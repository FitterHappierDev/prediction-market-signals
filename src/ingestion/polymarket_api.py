"""Polymarket HTTP API client — the ONLY module that knows Polymarket's
URLs and response shapes. If Polymarket changes their API, fix it here.

Two upstream APIs:
- Gamma (https://gamma-api.polymarket.com)
    metadata + price history
- CLOB (https://clob.polymarket.com)
    live order book + trade tape

Retry policy (REQ-PMC-020):
- HTTP 429: respect `Retry-After`; fall back to 60s
- HTTP 5xx / timeout / network: exponential backoff 1s -> 2s -> ... cap 60s,
  max 10 retries
- HTTP 4xx (non-429): raise PolymarketAPIError immediately (do NOT retry —
  indicates a request bug, not a transient outage)

Rate limiting: a single tunable knob (`set_rate`) used to slow down during
backfill (2 r/s) versus steady-state (4 r/s).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

import httpx

logger = logging.getLogger(__name__)

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
# CLOB /trades requires HMAC L2 auth; the public trade tape lives here:
DATA_BASE = "https://data-api.polymarket.com"

DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_MAX_RETRIES = 10
DEFAULT_RPS = 4.0


class PolymarketAPIError(Exception):
    """Raised for non-retriable HTTP errors and exhausted retries."""


class PolymarketAPI:
    def __init__(
        self,
        max_requests_per_second: float = DEFAULT_RPS,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        self._min_interval = 1.0 / max_requests_per_second
        self._last_request_at = 0.0
        self._timeout = timeout
        self._max_retries = max_retries
        self._client: httpx.AsyncClient | None = None
        self._lock = asyncio.Lock()  # serialises the throttle

    async def _client_or_create(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self._timeout,
                headers={"User-Agent": "pm-platform/0.1 (+github.com/FitterHappierDev)"},
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

    async def _request(self, method: str, url: str, **kwargs: Any) -> Any:
        client = await self._client_or_create()
        backoff = 1.0
        last_err: Exception | None = None

        for attempt in range(1, self._max_retries + 1):
            await self._throttle()
            try:
                resp = await client.request(method, url, **kwargs)
            except (httpx.TimeoutException, httpx.NetworkError) as e:
                last_err = e
                logger.warning("PM %s %s attempt %d: %s", method, url, attempt, e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", "60"))
                logger.warning("PM %s 429 (Retry-After=%.1fs)", url, retry_after)
                await asyncio.sleep(retry_after)
                continue

            if 500 <= resp.status_code < 600:
                last_err = PolymarketAPIError(f"{resp.status_code} on {url}")
                logger.warning("PM %s %d attempt %d", url, resp.status_code, attempt)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            if 400 <= resp.status_code < 500:
                raise PolymarketAPIError(
                    f"PM {resp.status_code} on {url}: {resp.text[:200]}"
                )

            resp.raise_for_status()
            return resp.json()

        raise PolymarketAPIError(f"PM max retries on {url}") from last_err

    # ----- Gamma API ---------------------------------------------------------

    async def iter_markets(
        self,
        active: bool = True,
        closed: bool = False,
        page_size: int = 500,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield active markets via cursor-style offset pagination."""
        offset = 0
        while True:
            params = {
                "active": str(active).lower(),
                "closed": str(closed).lower(),
                "limit": page_size,
                "offset": offset,
            }
            page = await self._request("GET", f"{GAMMA_BASE}/markets", params=params)
            if not isinstance(page, list) or not page:
                return
            for market in page:
                yield market
            if len(page) < page_size:
                return
            offset += page_size

    async def get_prices_history(
        self,
        token_id: str,
        start_ts: int,
        end_ts: int,
        fidelity_minutes: int = 1,
    ) -> list[dict[str, Any]]:
        """Historical probability bars for a YES token. The CLOB endpoint
        takes the token_id (NOT the condition_id), with `fidelity` in
        minutes. Returns a list of {t: unix_seconds, p: price}.
        """
        params = {
            "market": token_id,
            "startTs": start_ts,
            "endTs": end_ts,
            "fidelity": fidelity_minutes,
        }
        result = await self._request(
            "GET", f"{CLOB_BASE}/prices-history", params=params
        )
        if isinstance(result, dict):
            return result.get("history", []) or []
        if isinstance(result, list):
            return result
        return []

    # ----- CLOB API ----------------------------------------------------------

    async def get_order_book(self, token_id: str) -> dict[str, Any]:
        return await self._request(
            "GET", f"{CLOB_BASE}/book", params={"token_id": token_id}
        )

    # ----- Data API ----------------------------------------------------------

    async def get_trades(
        self, market_id: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        """Public trade tape for a market. Uses the Data API (the CLOB
        equivalent requires HMAC auth).

        `market_id` is the `conditionId` (bytes32 hex) — NOT the token_id.
        """
        result = await self._request(
            "GET",
            f"{DATA_BASE}/trades",
            params={"market": market_id, "limit": limit},
        )
        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            return result.get("data") or result.get("trades") or []
        return []
