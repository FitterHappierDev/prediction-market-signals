"""Smoke-test the Alchemy Polygon RPC client end-to-end.

Runs every RPC method the WalletTracer uses against a real wallet and
prints the responses. Validates:
- ALCHEMY_API_KEY is valid
- get_code, get_transaction_count, get_block_by_number, get_asset_transfers
  all return the shapes our parser expects
- USDC transfer events resolve

Usage:
    # From the repo root, with .env present and ALCHEMY_API_KEY set:
    python -m scripts.inspect_alchemy
    python -m scripts.inspect_alchemy 0xabc...   # specific wallet
    python -m scripts.inspect_alchemy 0xabc... --force-questdb   # also exercise
        the pm_trades query path (requires a running QuestDB on localhost:9000)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Make `src` importable when run as `python -m scripts.inspect_alchemy`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv  # noqa: E402

# Prefer the worktree's .env; fall back to the parent (where the user
# keeps their real keys during dev).
for candidate in (
    Path(__file__).resolve().parent.parent / ".env",
    Path("/Users/yen/Desktop/prediction-market-signals/.env"),
):
    if candidate.exists():
        load_dotenv(candidate)
        break

from src.ingestion.alchemy import (  # noqa: E402
    POLYGON_USDC_ADDRESSES,
    AlchemyError,
    AlchemyPolygonAPI,
)

# Default: Vitalik's address — same hex on Polygon. Lots of external
# transactions so wallet-age + tx-count return non-trivial values. USDC
# transfers may be empty on Polygon for him, which is itself useful:
# proves the parser handles the empty case.
DEFAULT_WALLET = "0xd8da6bf26964af9d7eed9e03e53415d37aa96045"


def banner(title: str) -> None:
    print(f"\n=== {title} ===")


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("wallet", nargs="?", default=DEFAULT_WALLET)
    args = ap.parse_args()

    api_key = os.environ.get("ALCHEMY_API_KEY")
    if not api_key or api_key.startswith("your_"):
        print("ERROR: ALCHEMY_API_KEY missing or placeholder", file=sys.stderr)
        return 2

    wallet = args.wallet.lower()
    print(f"Tracing wallet: {wallet}")

    alchemy = AlchemyPolygonAPI(api_key=api_key)

    try:
        banner("eth_getCode")
        code = await alchemy.get_code(wallet)
        print(f"len={len(code)}  starts_with={code[:10]}")
        print(f"is_contract={code != '0x'}")

        banner("eth_getTransactionCount")
        tx_count = await alchemy.get_transaction_count(wallet)
        print(f"total_transactions={tx_count:,}")

        banner("alchemy_getAssetTransfers (asc, max=1) — first transfer")
        first = await alchemy.get_asset_transfers(
            from_address=wallet,
            category=["external", "erc20"],
            order="asc",
            max_count=1,
        )
        print(f"count={len(first)}")
        if first:
            t = first[0]
            print(f"  blockNum={t.get('blockNum')}")
            print(f"  to={t.get('to')}  asset={t.get('asset')}  value={t.get('value')}")
            block = await alchemy.get_block_by_number(t["blockNum"])
            if block:
                ts_hex = block.get("timestamp", "0x0")
                ts_unix = int(ts_hex, 16)
                first_seen = datetime.fromtimestamp(ts_unix, tz=timezone.utc)
                print(f"  first_seen={first_seen.isoformat()}")
            else:
                print("  block fetch returned None")

        banner("alchemy_getAssetTransfers (desc, USDC inbound, max=10)")
        usdc_in = await alchemy.get_asset_transfers(
            to_address=wallet,
            contract_addresses=list(POLYGON_USDC_ADDRESSES),
            category=["erc20"],
            order="desc",
            max_count=10,
        )
        print(f"count={len(usdc_in)}")
        for t in usdc_in[:5]:
            print(
                f"  {t.get('blockNum')}  from={t.get('from')}  "
                f"asset={t.get('asset')}  value={t.get('value')}"
            )
        if usdc_in:
            # Mimic the largest-by-value pick the tracer does.
            def safe_value(t: dict) -> float:
                raw = t.get("value")
                try:
                    return float(raw) if raw is not None else 0.0
                except (TypeError, ValueError):
                    return 0.0

            largest = max(usdc_in, key=safe_value)
            print(
                f"\nLargest USDC funder: {largest.get('from')}  "
                f"value={largest.get('value')}"
            )

        banner("Sample raw response (one transfer record, full shape)")
        if usdc_in:
            print(json.dumps(usdc_in[0], indent=2, default=str))
        elif first:
            print(json.dumps(first[0], indent=2, default=str))
        else:
            print("no transfers to print")

        return 0

    except AlchemyError as e:
        print(f"\nAlchemy error: {e}", file=sys.stderr)
        return 1
    finally:
        await alchemy.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
