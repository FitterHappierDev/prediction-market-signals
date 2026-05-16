"""Probe the Kalshi /markets pagination in isolation.

Verifies that KalshiAPI.iter_markets actually produces results (a timer
shows how fast each page returns). If iter_markets hangs forever in the
real collector but works here, the bug is in our collector wiring; if
it hangs here too, it's an API/auth issue.

Usage:
    source .venv/bin/activate
    set -a; source .env; set +a
    python scripts/inspect_kalshi_iter.py
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.ingestion.kalshi_api import KalshiAPI, KalshiAPIError  # noqa: E402


async def main() -> int:
    api = KalshiAPI(
        api_key=os.environ.get("KALSHI_API_KEY"),
        api_secret=os.environ.get("KALSHI_API_SECRET"),
    )
    print(f"auth_mode = {api.auth_mode}")
    print(f"KALSHI_API_KEY length    = {len(os.environ.get('KALSHI_API_KEY') or '')}")
    print(f"KALSHI_API_SECRET length = {len(os.environ.get('KALSHI_API_SECRET') or '')}")
    print()

    start = time.time()
    count = 0
    last_print = start
    try:
        async for raw in api.iter_markets(status="open", page_size=200):
            count += 1
            if count <= 3:
                print(f"  market {count}: {raw.get('ticker')} - {(raw.get('title') or '')[:60]}")
            now = time.time()
            if now - last_print >= 5:
                print(f"  ... {count} markets after {now - start:.1f}s")
                last_print = now
            if count >= 1000:
                print(f"  stopping at {count} (probe cap)")
                break
    except KalshiAPIError as e:
        print(f"KalshiAPIError after {count} markets: {e}")
    except Exception as e:
        print(f"Unexpected {type(e).__name__} after {count} markets: {e}")
    finally:
        await api.close()

    elapsed = time.time() - start
    print()
    print(f"Total: {count} markets in {elapsed:.1f}s "
          f"({count / elapsed:.1f} markets/sec)" if elapsed > 0 else f"Total: {count}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
