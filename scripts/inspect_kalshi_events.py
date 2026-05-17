"""Probe Kalshi /events to find the categorisation signal.

/markets responses have no `category` field — the categorisation lives
on the parent /events resource. This script:
1. Prints the full field list of one event.
2. Counts unique categories across 500 events.
3. Tries category-filtered queries to confirm Kalshi supports it.

Usage:
    source .venv/bin/activate
    set -a; source .env; set +a
    python scripts/inspect_kalshi_events.py
"""

from __future__ import annotations

import asyncio
import os
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.ingestion.kalshi_api import KalshiAPI, KalshiAPIError  # noqa: E402


async def main() -> int:
    api = KalshiAPI(
        api_key=os.environ.get("KALSHI_API_KEY"),
        api_secret=os.environ.get("KALSHI_API_SECRET"),
    )

    print("=" * 60)
    print("[1] First page of /events")
    print("=" * 60)
    try:
        page = await api._request("GET", "/events", params={"status": "open", "limit": 10})
    except KalshiAPIError as e:
        print(f"ERROR: {e}")
        await api.close()
        return 1

    events = page.get("events", []) or []
    print(f"Returned {len(events)} events; top-level keys: {sorted(page.keys())}")
    if events:
        print("\nFirst event field list:")
        for k in sorted(events[0].keys()):
            v = events[0][k]
            r = repr(v)
            print(f"  {k:30s} {r[:120]}")

    # Page through ~500 events and count categories
    print()
    print("=" * 60)
    print("[2] Category distribution across first ~500 open events")
    print("=" * 60)
    categories: Counter = Counter()
    seen = 0
    cursor = page.get("cursor")
    for e in events:
        seen += 1
        cat = e.get("category") or e.get("subtitle") or "(no category)"
        categories[str(cat)[:60]] += 1
    while cursor and seen < 500:
        try:
            page = await api._request(
                "GET", "/events", params={"status": "open", "limit": 200, "cursor": cursor}
            )
        except KalshiAPIError as e:
            print(f"ERROR on page after seen={seen}: {e}")
            break
        for e in page.get("events", []) or []:
            seen += 1
            cat = e.get("category") or e.get("subtitle") or "(no category)"
            categories[str(cat)[:60]] += 1
            if seen >= 500:
                break
        cursor = page.get("cursor")
        if not cursor:
            break

    print(f"Seen {seen} events. Top categories:")
    for value, n in categories.most_common(20):
        print(f"  {n:5d}  {value}")

    # Try the category filter directly
    print()
    print("=" * 60)
    print("[3] Probe category-filtered /events")
    print("=" * 60)
    for cat in ("Politics", "Economics", "Financials", "politics", "economics"):
        try:
            r = await api._request(
                "GET", "/events", params={"status": "open", "category": cat, "limit": 5}
            )
            events_returned = len(r.get("events", []) or [])
            print(f"  category={cat!r:14s} -> {events_returned} events")
        except KalshiAPIError as e:
            print(f"  category={cat!r:14s} -> ERROR: {e}")

    await api.close()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
