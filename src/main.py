"""Phase 1 entry point.

Wires the Polymarket and Kalshi collectors against shared QuestDB +
Redis clients and runs them as concurrent asyncio tasks. On crash a
collector restarts after 30s (REQ-PMC-010). SIGINT/SIGTERM trigger
graceful shutdown.

Run interactively:
    source .venv/bin/activate
    set -a; source .env; set +a
    python -m src.main

Or as the systemd service (env is loaded from /etc/systemd via EnvironmentFile).
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Awaitable, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()  # no-op if env already set by systemd

from src.config import get_config  # noqa: E402
from src.ingestion.kalshi import KalshiCollector  # noqa: E402
from src.ingestion.kalshi_api import KalshiAPI  # noqa: E402
from src.ingestion.polymarket import PolymarketCollector  # noqa: E402
from src.ingestion.polymarket_api import PolymarketAPI  # noqa: E402
from src.utils.db import QuestDBClient  # noqa: E402
from src.utils.redis_client import RedisClient  # noqa: E402

logger = logging.getLogger("pm-platform")
RESTART_DELAY_SECONDS = 30


def _setup_logging() -> None:
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)sZ %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


async def run_with_restart(
    name: str,
    start: Callable[[], Awaitable[None]],
    stop_event: asyncio.Event,
) -> None:
    """Run a collector's start() repeatedly. On crash, log it and sleep
    RESTART_DELAY_SECONDS before retrying. Returns once stop_event is set."""
    while not stop_event.is_set():
        try:
            await start()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("%s crashed; restarting in %ds", name, RESTART_DELAY_SECONDS)
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=RESTART_DELAY_SECONDS
                )
                return
            except asyncio.TimeoutError:
                continue
        else:
            # Clean exit (rare — start() normally runs forever)
            return


def _normalise_auth_mode(raw: str) -> str:
    valid = ("auto", "bearer", "basic", "rsa")
    if raw in valid:
        return raw
    logger.warning("Invalid KALSHI_AUTH_MODE=%r; falling back to 'auto'", raw)
    return "auto"


async def main_async() -> int:
    _setup_logging()
    cfg = get_config()
    logger.info("Starting pm-platform")

    db = QuestDBClient()
    db.ensure_tables()
    redis_client = RedisClient()

    pm_api = PolymarketAPI(
        max_requests_per_second=cfg.ingestion.polymarket.max_requests_per_second,
    )
    kalshi_api = KalshiAPI(
        api_key=os.environ.get("KALSHI_API_KEY"),
        api_secret=os.environ.get("KALSHI_API_SECRET"),
        auth_mode=_normalise_auth_mode(os.environ.get("KALSHI_AUTH_MODE", "auto")),
        max_requests_per_second=cfg.ingestion.kalshi.max_requests_per_second,
    )

    pm = PolymarketCollector(pm_api, db, redis_client, cfg)
    kalshi = KalshiCollector(kalshi_api, db, redis_client, cfg)

    stop_event = asyncio.Event()

    def _shutdown(signame: str) -> None:
        if not stop_event.is_set():
            logger.info("Received %s; shutting down", signame)
            stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown, sig.name)
        except NotImplementedError:
            pass

    # Polymarket historical backfill before live polling.
    try:
        await pm.backfill_history()
    except Exception:
        logger.exception(
            "Polymarket backfill failed; continuing into live polling anyway"
        )

    tasks = [
        asyncio.create_task(
            run_with_restart("polymarket", pm.start, stop_event), name="pm"
        ),
        asyncio.create_task(
            run_with_restart("kalshi", kalshi.start, stop_event), name="kalshi"
        ),
    ]

    await stop_event.wait()
    logger.info("Stopping collectors...")
    await asyncio.gather(pm.stop(), kalshi.stop(), return_exceptions=True)
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    await pm_api.close()
    await kalshi_api.close()
    await redis_client.close()
    db.close()
    logger.info("Clean shutdown complete")
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    sys.exit(main())
