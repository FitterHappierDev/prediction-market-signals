"""Print the actual JSON shape of one Kalshi orderbook response.

Used to verify whether _midpoint_from_orderbook is reading the right
fields. Picks any market in the first Economics event with nested
markets and prints the full orderbook response.

Usage:
    source .venv/bin/activate
    set -a; source .env; set +a
    python scripts/inspect_kalshi_orderbook.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.ingestion.kalshi_api import KalshiAPI  # noqa: E402


async def main() -> int:
    api = KalshiAPI(
        api_key=os.environ.get("KALSHI_API_KEY"),
        api_secret=os.environ.get("KALSHI_API_SECRET"),
    )
    try:
        async for event in api.iter_events(
            category="Economics",
            status="open",
            with_nested_markets=True,
            page_size=10,
        ):
            markets = event.get("markets") or []
            if not markets:
                continue
            for m in markets:
                ticker = m.get("ticker")
                print(f"Probing orderbook for ticker={ticker}")
                ob = await api.get_orderbook(ticker)
                print("Top-level keys:", sorted(ob.keys()) if isinstance(ob, dict) else type(ob))
                print(json.dumps(ob, indent=2)[:1200])
                return 0
    finally:
        await api.close()
    print("No markets found.")
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
