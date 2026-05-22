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
from src.ingestion.alchemy import AlchemyPolygonAPI  # noqa: E402
from src.ingestion.kalshi import KalshiCollector  # noqa: E402
from src.ingestion.kalshi_api import KalshiAPI  # noqa: E402
from src.ingestion.market_context import MarketContextCollector  # noqa: E402
from src.ingestion.polymarket import PolymarketCollector  # noqa: E402
from src.ingestion.polymarket_api import PolymarketAPI  # noqa: E402
from src.ingestion.wallet_tracer import WalletTracer  # noqa: E402
from src.utils.db import QuestDBClient  # noqa: E402
from src.utils.redis_client import RedisClient  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXCHANGE_WALLETS_PATH = PROJECT_ROOT / "config" / "exchange_wallets.json"

logger = logging.getLogger("pm-platform")
RESTART_DELAY_SECONDS = 30


def _setup_logging() -> None:
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)sZ %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    # httpx and httpcore log every HTTP request at INFO; with backfill
    # making thousands of calls, that drowns out our own application
    # logs. Bump them to WARNING so we only see actual problems.
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


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

    # Wallet tracer is feature-flagged. When disabled, the anomaly
    # detector will operate in degraded mode per REQ-ANM-003.
    wallet_tracer: WalletTracer | None = None
    alchemy_api: AlchemyPolygonAPI | None = None
    if cfg.feature_flags.wallet_tracing_enabled:
        alchemy_key = os.environ.get("ALCHEMY_API_KEY")
        if not alchemy_key:
            logger.warning(
                "wallet_tracing_enabled=true but ALCHEMY_API_KEY is unset; "
                "skipping WalletTracer"
            )
        else:
            alchemy_api = AlchemyPolygonAPI(api_key=alchemy_key)
            wallet_tracer = WalletTracer(
                alchemy_api, db, redis_client, cfg, EXCHANGE_WALLETS_PATH
            )

    # Market Context Service — Phase 3 prep, off by default.
    market_context: MarketContextCollector | None = None
    if cfg.feature_flags.market_context_enabled:
        market_context = MarketContextCollector(db, redis_client, cfg)

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

    # Kalshi has no backfill phase, so start it immediately. Polymarket
    # backfill (REQ-PMC-004) runs blocking before its live polling starts,
    # but should NOT block the Kalshi pipeline.
    tasks = [
        asyncio.create_task(
            run_with_restart("kalshi", kalshi.start, stop_event), name="kalshi"
        ),
    ]
    if wallet_tracer is not None:
        tasks.append(
            asyncio.create_task(
                run_with_restart("wallet_tracer", wallet_tracer.start, stop_event),
                name="wallet_tracer",
            )
        )
    if market_context is not None:
        tasks.append(
            asyncio.create_task(
                run_with_restart("market_context", market_context.start, stop_event),
                name="market_context",
            )
        )

    try:
        await pm.backfill_history()
    except Exception:
        logger.exception(
            "Polymarket backfill failed; continuing into live polling anyway"
        )

    tasks.append(
        asyncio.create_task(
            run_with_restart("polymarket", pm.start, stop_event), name="pm"
        )
    )

    await stop_event.wait()
    logger.info("Stopping collectors...")
    stops = [pm.stop(), kalshi.stop()]
    if wallet_tracer is not None:
        stops.append(wallet_tracer.stop())
    if market_context is not None:
        stops.append(market_context.stop())
    await asyncio.gather(*stops, return_exceptions=True)
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    await pm_api.close()
    await kalshi_api.close()
    if alchemy_api is not None:
        await alchemy_api.close()
    await redis_client.close()
    db.close()
    logger.info("Clean shutdown complete")
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    sys.exit(main())
