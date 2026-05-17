"""Probe Kalshi /markets to identify how to classify markets.

Prints:
1. All fields of the first market (so we can see what Kalshi actually
   returns).
2. Unique values for any category-like fields across the first ~500
   markets (so we can map Kalshi categories to our category enum).

Usage:
    source .venv/bin/activate
    set -a; source .env; set +a
    python scripts/inspect_kalshi_fields.py
"""

from __future__ import annotations

import asyncio
import os
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.ingestion.kalshi_api import KalshiAPI  # noqa: E402


CATEGORY_LIKE_FIELDS = (
    "category",
    "subtitle",
    "series_ticker",
    "event_ticker",
    "market_type",
)


async def main() -> int:
    api = KalshiAPI(
        api_key=os.environ.get("KALSHI_API_KEY"),
        api_secret=os.environ.get("KALSHI_API_SECRET"),
    )

    samples = []
    async for raw in api.iter_markets(status="open", page_size=200):
        samples.append(raw)
        if len(samples) >= 500:
            break
    await api.close()

    if not samples:
        print("No markets returned.")
        return 1

    print("=" * 60)
    print("[1] Full field list of the first market")
    print("=" * 60)
    for k in sorted(samples[0].keys()):
        v = samples[0][k]
        r = repr(v)
        print(f"  {k:30s} {r[:120]}")

    print()
    print("=" * 60)
    print(f"[2] Top values per category-like field (across {len(samples)} markets)")
    print("=" * 60)
    for field in CATEGORY_LIKE_FIELDS:
        counts: Counter = Counter()
        for m in samples:
            v = m.get(field)
            if v is None:
                continue
            # Normalise event/series tickers to their prefix (Kalshi
            # tickers look like FED-26MAY-T5.00, KXNFLGAME-..., etc.)
            if field in ("event_ticker", "series_ticker", "market_type"):
                prefix = str(v).split("-")[0]
                counts[prefix] += 1
            else:
                counts[str(v)[:60]] += 1
        if not counts:
            print(f"  {field}: (not present on any sampled market)")
            continue
        print(f"  {field}:")
        for value, n in counts.most_common(15):
            print(f"    {n:5d}  {value}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
