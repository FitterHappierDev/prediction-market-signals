"""HUMAN STEP 0.11 — exercise the foundation modules end to end.

Usage (from the repo root, with the venv active):

    source .venv/bin/activate
    set -a; source .env; set +a
    python scripts/verify_foundation.py

The script:
1. Loads config and prints a handful of representative values.
2. Sanity-checks the time helpers (current UTC, equity vs crypto market hours).
3. Connects to QuestDB, runs ensure_tables(), and lists the resulting tables.
4. Pings Redis, publishes a test stream message, and round-trips a hash.
5. Sends a test Slack alert if SLACK_WEBHOOK_URL is set in the environment.
"""

from __future__ import annotations

import asyncio
import os
import sys

from src.config import get_config
from src.utils.db import QuestDBClient
from src.utils.notifications import send_alert
from src.utils.redis_client import RedisClient
from src.utils.time_utils import (
    is_market_open,
    minutes_between,
    now_utc,
    trading_minutes_to_years,
)


def banner(title: str) -> None:
    print()
    print("=" * 60)
    print(title)
    print("=" * 60)


def check_config() -> None:
    banner("[1] src/config.py")
    cfg = get_config()
    print(f"  polymarket.poll_interval_seconds      = {cfg.ingestion.polymarket.poll_interval_seconds}")
    print(f"  detection.anomaly.alert_critical      = {cfg.detection.anomaly.alert_threshold_critical}")
    print(f"  models.stage1.min_training_samples    = {cfg.models.stage1.min_training_samples}")
    print(f"  risk.sizing.category_caps.fed_rate    = {cfg.risk.sizing.category_caps.fed_rate}")
    print(f"  feature_flags.wallet_tracing_enabled  = {cfg.feature_flags.wallet_tracing_enabled}")


def check_time_utils() -> None:
    banner("[2] src/utils/time_utils.py")
    now = now_utc()
    print(f"  now_utc()                             = {now.isoformat()}")
    print(f"  is_market_open('equity', now)         = {is_market_open('equity', now)}")
    print(f"  is_market_open('crypto', now)         = {is_market_open('crypto', now)}")
    print(f"  minutes_between(now, now)             = {minutes_between(now, now)}")
    print(f"  trading_minutes_to_years(60,'equity') = {trading_minutes_to_years(60, 'equity'):.6f}")


def check_db() -> None:
    banner("[3] src/utils/db.py — ensure_tables + sanity SELECT")
    db = QuestDBClient()
    db.ensure_tables()
    tables = db.query("SHOW TABLES")
    print(f"  Tables in QuestDB ({len(tables)}):")
    if not tables.empty:
        # SHOW TABLES returns a single 'table_name' (or similar) column;
        # take whichever column is at index 0 to be robust to naming drift.
        for value in tables.iloc[:, 0]:
            print(f"    - {value}")
    db.close()


async def _redis_round_trip() -> None:
    r = RedisClient()
    print(f"  ping()                                = {await r.ping()}")
    msg_id = await r.publish_to_stream(
        "test:hello", {"value": 42, "ts": now_utc().isoformat()}
    )
    print(f"  publish_to_stream id                  = {msg_id}")
    await r.set_hash("test:hash", {"k": "v", "n": 7}, ttl_seconds=60)
    print(f"  get_hash                              = {await r.get_hash('test:hash')}")
    await r.close()


def check_redis() -> None:
    banner("[4] src/utils/redis_client.py — ping + publish + hash round-trip")
    asyncio.run(_redis_round_trip())


def check_notifications() -> None:
    banner("[5] src/utils/notifications.py")
    if os.environ.get("SLACK_WEBHOOK_URL"):
        print("  SLACK_WEBHOOK_URL set; sending test alert (info severity)")
        asyncio.run(
            send_alert("PM platform foundation verification - Slack test.", "info")
        )
        print("  ...check your Slack channel.")
    else:
        print("  SLACK_WEBHOOK_URL not set in env; skipping live send")


def main() -> int:
    try:
        check_config()
        check_time_utils()
        check_db()
        check_redis()
        check_notifications()
    except Exception:
        import traceback

        traceback.print_exc()
        return 1

    print()
    print("All foundation modules imported and exercised cleanly.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
