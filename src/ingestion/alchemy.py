"""Alchemy Polygon RPC client — JSON-RPC over HTTPS.

The ONLY module that knows Alchemy's URL and request shapes. If Alchemy
changes their API, fix it here.

Used by the Wallet Tracer (REQ-WLT-001/020/021) to fetch:
- transaction counts and code (eth_getTransactionCount, eth_getCode)
- block timestamps (eth_getBlockByNumber)
- token transfer history (alchemy_getAssetTransfers — enhanced API)

Retry policy mirrors src/ingestion/polymarket_api.py:
- HTTP 429: respect Retry-After header; fall back to 60s
- HTTP 5xx / timeout / network: exp backoff 1s -> 2s -> ... cap 60s
- HTTP 4xx (non-429): raise AlchemyError immediately (request bug)
- JSON-RPC error response: raise AlchemyError (no retry — server reached us)

Throttling: single tunable knob (`set_rate`) defaulting to 5 r/s, which
is well below Alchemy's free-tier ~25 CU/s after accounting for per-call
compute-unit cost (asset transfer queries cost more than basic eth_*).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

POLYGON_RPC_BASE = "https://polygon-mainnet.g.alchemy.com/v2"

# Polymarket-relevant USDC contracts on Polygon. USDC.e is the bridged
# token Polymarket uses for collateral; native Circle USDC was launched
# later and is occasionally seen in funding paths. Query both.
POLYGON_USDC_ADDRESSES: tuple[str, ...] = (
    "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",  # USDC.e (bridged, Polymarket collateral)
    "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",  # Native USDC (Circle)
)

DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_RETRIES = 5
DEFAULT_RPS = 5.0


class AlchemyError(Exception):
    """Non-retriable Alchemy error (4xx, JSON-RPC error response, or
    exhausted retries)."""


class AlchemyPolygonAPI:
    def __init__(
        self,
        api_key: str,
        max_requests_per_second: float = DEFAULT_RPS,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        if not api_key:
            raise ValueError("AlchemyPolygonAPI requires a non-empty api_key")
        self._url = f"{POLYGON_RPC_BASE}/{api_key}"
        self._min_interval = 1.0 / max_requests_per_second
        self._last_request_at = 0.0
        self._timeout = timeout
        self._max_retries = max_retries
        self._client: httpx.AsyncClient | None = None
        self._lock = asyncio.Lock()
        self._next_id = 0

    async def _client_or_create(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self._timeout,
                headers={
                    "User-Agent": "pm-platform/0.1 (+github.com/FitterHappierDev)",
                    "Content-Type": "application/json",
                },
            )
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def set_rate(self, requests_per_second: float) -> None:
        self._min_interval = 1.0 / requests_per_second

    async def _throttle(self) -> None:
        async with self._lock:
            now = asyncio.get_running_loop().time()
            wait = self._min_interval - (now - self._last_request_at)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request_at = asyncio.get_running_loop().time()

    def _next_rpc_id(self) -> int:
        self._next_id += 1
        return self._next_id

    async def _rpc(self, method: str, params: list[Any]) -> Any:
        client = await self._client_or_create()
        body = {
            "jsonrpc": "2.0",
            "id": self._next_rpc_id(),
            "method": method,
            "params": params,
        }

        backoff = 1.0
        last_err: Exception | None = None

        for attempt in range(1, self._max_retries + 1):
            await self._throttle()
            try:
                resp = await client.post(self._url, json=body)
            except (httpx.TimeoutException, httpx.NetworkError) as e:
                last_err = e
                logger.warning("Alchemy %s attempt %d: %s", method, attempt, e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", "60"))
                logger.warning("Alchemy 429 (Retry-After=%.1fs) on %s", retry_after, method)
                await asyncio.sleep(retry_after)
                continue

            if 500 <= resp.status_code < 600:
                last_err = AlchemyError(f"{resp.status_code} on {method}")
                logger.warning("Alchemy %s %d attempt %d", method, resp.status_code, attempt)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            if 400 <= resp.status_code < 500:
                raise AlchemyError(
                    f"Alchemy {resp.status_code} on {method}: {resp.text[:200]}"
                )

            payload = resp.json()
            if "error" in payload:
                # JSON-RPC application-level error. Some are retryable
                # ("rate limit exceeded", "503") but most are not.
                err = payload["error"]
                code = err.get("code")
                message = err.get("message", "")
                if code in (-32005, 429) or "rate limit" in message.lower():
                    logger.warning("Alchemy rpc-rate-limit %s: %s", method, message)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    continue
                raise AlchemyError(f"Alchemy RPC error on {method}: {err}")

            return payload.get("result")

        raise AlchemyError(f"Alchemy max retries on {method}") from last_err

    # ----- typed wrappers ----------------------------------------------------

    async def get_transaction_count(self, address: str) -> int:
        result = await self._rpc(
            "eth_getTransactionCount", [address.lower(), "latest"]
        )
        return int(result, 16)

    async def get_code(self, address: str) -> str:
        """Returns the hex bytecode at `address`. `'0x'` means EOA."""
        return await self._rpc("eth_getCode", [address.lower(), "latest"])

    async def get_block_by_number(self, block_num_hex: str) -> dict[str, Any] | None:
        """`block_num_hex` is the 0x-prefixed hex string Alchemy returns
        from getAssetTransfers (e.g., '0x12a05f'). Pass `False` for the
        second arg so the response excludes full transaction bodies."""
        return await self._rpc("eth_getBlockByNumber", [block_num_hex, False])

    async def get_asset_transfers(
        self,
        *,
        from_address: str | None = None,
        to_address: str | None = None,
        contract_addresses: list[str] | None = None,
        category: list[str] | None = None,
        max_count: int = 20,
        order: str = "desc",
        from_block: str = "0x0",
        to_block: str = "latest",
    ) -> list[dict[str, Any]]:
        """Enhanced API: returns a list of transfer events matching the
        filter. See https://docs.alchemy.com/reference/alchemy-getassettransfers

        The API caps `max_count` at 1000 per call; we typically use 20.
        Always sets `excludeZeroValue=true` and `withMetadata=false`.
        """
        params: dict[str, Any] = {
            "category": category or ["external", "erc20"],
            "maxCount": hex(max_count),
            "order": order,
            "fromBlock": from_block,
            "toBlock": to_block,
            "excludeZeroValue": True,
            "withMetadata": False,
        }
        if from_address:
            params["fromAddress"] = from_address.lower()
        if to_address:
            params["toAddress"] = to_address.lower()
        if contract_addresses:
            params["contractAddresses"] = [a.lower() for a in contract_addresses]

        result = await self._rpc("alchemy_getAssetTransfers", [params])
        if not isinstance(result, dict):
            return []
        return result.get("transfers", []) or []
