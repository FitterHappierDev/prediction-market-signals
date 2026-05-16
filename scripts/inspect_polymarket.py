"""Inspect live Polymarket API responses for field-name verification.

Run BEFORE deploying the full collector. Hits the Gamma + CLOB APIs once
each, prints the raw response of one active market, runs `parse_market`
on it, and tries an order book + trades fetch for its YES token.

Output is read directly to confirm Polymarket's current field naming
matches the assumptions in src/ingestion/polymarket.py.

Usage:
    source .venv/bin/activate
    python scripts/inspect_polymarket.py
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.ingestion.polymarket_api import PolymarketAPI, PolymarketAPIError  # noqa: E402
from src.ingestion.polymarket import parse_market  # noqa: E402


async def main() -> int:
    api = PolymarketAPI()

    # ----- 1. List one page of markets ---------------------------------------
    print("=" * 60)
    print("[1] Gamma /markets — first market in the active list")
    print("=" * 60)
    try:
        first = None
        async for raw in api.iter_markets(active=True, closed=False, page_size=5):
            first = raw
            break
    except PolymarketAPIError as e:
        print(f"ERROR listing markets: {e}")
        await api.close()
        return 1

    if not first:
        print("No active markets returned.")
        await api.close()
        return 1

    print("Raw fields (sorted):")
    for k in sorted(first.keys()):
        v = first[k]
        repr_val = repr(v)
        if len(repr_val) > 100:
            repr_val = repr_val[:97] + "..."
        print(f"  {k}: {repr_val}")

    # ----- 2. Run our parser on it -------------------------------------------
    print()
    print("=" * 60)
    print("[2] parse_market() result")
    print("=" * 60)
    parsed = parse_market(first)
    if parsed is None:
        print("parse_market returned None — no usable market_id found")
        await api.close()
        return 1
    for field in (
        "market_id",
        "yes_token_id",
        "title",
        "category",
        "bet_type",
        "end_date",
        "resolved",
        "volume_total_usd",
    ):
        print(f"  {field:22s} = {getattr(parsed, field)!r}")

    # ----- 3. CLOB book + trades for the YES token ---------------------------
    if not parsed.yes_token_id:
        print("\nNo YES token id parsed — skipping CLOB calls.")
        await api.close()
        return 0

    print()
    print("=" * 60)
    print(f"[3] CLOB /book for token {parsed.yes_token_id[:20]}...")
    print("=" * 60)
    try:
        book = await api.get_order_book(parsed.yes_token_id)
        # Print top-level keys and a few bid/ask entries
        print(f"Top-level keys: {sorted(book.keys()) if isinstance(book, dict) else type(book)}")
        if isinstance(book, dict):
            bids = book.get("bids") or []
            asks = book.get("asks") or []
            print(f"  bids ({len(bids)}): {bids[:3]}")
            print(f"  asks ({len(asks)}): {asks[:3]}")
    except PolymarketAPIError as e:
        print(f"ERROR: {e}")

    print()
    print("=" * 60)
    print(f"[4] CLOB /trades for token {parsed.yes_token_id[:20]}...")
    print("=" * 60)
    try:
        trades = await api.get_trades(parsed.yes_token_id, limit=3)
        print(f"Got {len(trades)} trades")
        for i, t in enumerate(trades):
            print(f"  --- trade {i} ---")
            for k in sorted(t.keys()):
                v = t[k]
                repr_val = repr(v)
                if len(repr_val) > 100:
                    repr_val = repr_val[:97] + "..."
                print(f"    {k}: {repr_val}")
    except PolymarketAPIError as e:
        print(f"ERROR: {e}")

    await api.close()
    print()
    print("Done. Confirm the field names above match what the collector expects:")
    print("  market_id      <- conditionId | condition_id | id")
    print("  yes_token_id   <- clobTokenIds[0]")
    print("  trade.id       <- id | trade_id | transactionHash")
    print("  trade.size     <- size  (USD via size * price)")
    print("  trade.taker    <- taker | maker")
    print("  trade.timestamp<- timestamp | ts")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
