"""Probe Kalshi auth modes against the live API.

Kalshi has shipped multiple auth schemes; this script tries each one
against `GET /markets?status=open&limit=1` and reports which (if any)
return HTTP 200. Use the winning mode by setting `KALSHI_AUTH_MODE` to
`bearer`, `basic`, or `rsa` in main.py.

Usage:
    source .venv/bin/activate
    set -a; source .env; set +a
    python scripts/inspect_kalshi.py
"""

from __future__ import annotations

import asyncio
import base64
import os
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
PATH = "/markets"
PARAMS = {"status": "open", "limit": 1}


def _make_basic(key: str, secret: str) -> dict[str, str]:
    token = base64.b64encode(f"{key}:{secret}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def _make_bearer(key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {key}"}


def _make_rsa(key: str, pem: str) -> dict[str, str] | None:
    if "BEGIN" not in pem:
        return None
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding

    pk = serialization.load_pem_private_key(pem.encode(), password=None)
    ts = str(int(time.time() * 1000))
    full_path = f"/trade-api/v2{PATH}"
    message = f"{ts}GET{full_path}".encode()
    sig = pk.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": key,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        "KALSHI-ACCESS-TIMESTAMP": ts,
    }


async def probe(name: str, headers: dict[str, str] | None) -> tuple[str, int, str]:
    if headers is None:
        return name, -1, "skipped (auth mode not applicable)"
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(f"{KALSHI_BASE}{PATH}", params=PARAMS, headers=headers)
    body = (r.text or "")[:120].replace("\n", " ")
    return name, r.status_code, body


async def main() -> int:
    key = os.environ.get("KALSHI_API_KEY", "")
    secret = os.environ.get("KALSHI_API_SECRET", "")
    if not key:
        print("ERROR: KALSHI_API_KEY not set; source your .env first.")
        return 1

    print(f"key length:    {len(key)}")
    print(f"secret length: {len(secret)}")
    print(f"secret looks like PEM: {'BEGIN' in secret}")
    print()

    cases = [
        ("anonymous", {}),
        ("bearer(key)", _make_bearer(key)),
        ("bearer(secret)", _make_bearer(secret)) if secret else ("bearer(secret)", None),
        ("basic(key:secret)", _make_basic(key, secret) if secret else None),
        ("rsa-signed", _make_rsa(key, secret) if secret else None),
    ]

    print(f"{'Mode':22s}  Status  Body (first 120 chars)")
    print("-" * 80)
    for name, headers in cases:
        n, status, body = await probe(name, headers)
        marker = "✓" if status == 200 else " "
        print(f"{marker} {n:20s}  {status:>5}   {body}")

    print()
    print("Whichever row shows 200 is the auth mode to use.")
    print("Set KALSHI_AUTH_MODE in .env (or main.py) to: bearer | basic | rsa")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
