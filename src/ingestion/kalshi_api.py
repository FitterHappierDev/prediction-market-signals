"""Kalshi HTTP API client. The only module that knows Kalshi URLs and
response shapes — keep API-version churn isolated here.

Authentication: Kalshi has shipped several auth schemes over the years.
Supported modes (selected via `auth_mode`):

- `bearer`  — `Authorization: Bearer <api_key>` (token-only)
- `basic`   — HTTP Basic Auth with key:secret
- `rsa`     — RSA-PSS signed headers (KALSHI-ACCESS-KEY/SIGNATURE/TIMESTAMP)
              requires the secret to be a PEM-encoded RSA private key

`auto` (the default) inspects the secret format and picks `rsa` if it
looks like a PEM key, else `basic`.

Retry policy mirrors PolymarketAPI (REQ-PMC-020 / REQ-KAL-021).
"""

from __future__ import annotations

import asyncio
import base64
import logging
import time
from collections.abc import AsyncIterator
from typing import Any, Literal

import httpx

logger = logging.getLogger(__name__)

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_MAX_RETRIES = 10
DEFAULT_RPS = 10.0

AuthMode = Literal["auto", "bearer", "basic", "rsa"]


class KalshiAPIError(Exception):
    pass


class KalshiAPI:
    def __init__(
        self,
        api_key: str | None,
        api_secret: str | None,
        auth_mode: AuthMode = "auto",
        max_requests_per_second: float = DEFAULT_RPS,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        self._key = api_key or ""
        self._secret = api_secret or ""
        self._auth_mode: AuthMode = (
            self._infer_auth_mode() if auth_mode == "auto" else auth_mode
        )
        self._private_key = None
        if self._auth_mode == "rsa":
            self._private_key = self._load_rsa_key(self._secret)

        self._min_interval = 1.0 / max_requests_per_second
        self._last_request_at = 0.0
        self._timeout = timeout
        self._max_retries = max_retries
        self._client: httpx.AsyncClient | None = None
        self._lock = asyncio.Lock()

    @staticmethod
    def _load_rsa_key(pem: str):
        from cryptography.hazmat.primitives import serialization

        return serialization.load_pem_private_key(
            pem.encode() if isinstance(pem, str) else pem, password=None
        )

    def _infer_auth_mode(self) -> AuthMode:
        if self._secret and "BEGIN" in self._secret:
            return "rsa"
        return "basic"

    @property
    def auth_mode(self) -> AuthMode:
        return self._auth_mode

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

    def _auth_headers(self, method: str, path: str) -> dict[str, str]:
        if self._auth_mode == "bearer":
            return {"Authorization": f"Bearer {self._key}"}
        if self._auth_mode == "basic":
            token = base64.b64encode(f"{self._key}:{self._secret}".encode()).decode()
            return {"Authorization": f"Basic {token}"}
        if self._auth_mode == "rsa":
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives.asymmetric import padding

            ts = str(int(time.time() * 1000))
            message = f"{ts}{method.upper()}{path}".encode()
            sig = self._private_key.sign(
                message,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH,
                ),
                hashes.SHA256(),
            )
            return {
                "KALSHI-ACCESS-KEY": self._key,
                "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
                "KALSHI-ACCESS-TIMESTAMP": ts,
            }
        return {}

    async def _throttle(self) -> None:
        async with self._lock:
            now = asyncio.get_running_loop().time()
            wait = self._min_interval - (now - self._last_request_at)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request_at = asyncio.get_running_loop().time()

    async def _request(
        self, method: str, path: str, **kwargs: Any
    ) -> dict[str, Any]:
        url = f"{KALSHI_BASE}{path}"
        client = await self._client_or_create()
        backoff = 1.0
        last_err: Exception | None = None

        # Path for signing is just the URL path (no host).
        sign_path = path if path.startswith("/") else "/" + path

        for attempt in range(1, self._max_retries + 1):
            await self._throttle()
            headers = {**kwargs.pop("headers", {}), **self._auth_headers(method, f"/trade-api/v2{sign_path}")}
            try:
                resp = await client.request(method, url, headers=headers, **kwargs)
            except (httpx.TimeoutException, httpx.NetworkError) as e:
                last_err = e
                logger.warning("Kalshi %s %s attempt %d: %s", method, path, attempt, e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", "60"))
                logger.warning("Kalshi 429 on %s; waiting %.1fs", path, retry_after)
                await asyncio.sleep(retry_after)
                continue

            if 500 <= resp.status_code < 600:
                last_err = KalshiAPIError(f"{resp.status_code} on {path}")
                logger.warning("Kalshi %s %d attempt %d", path, resp.status_code, attempt)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            if 400 <= resp.status_code < 500:
                raise KalshiAPIError(
                    f"Kalshi {resp.status_code} on {path}: {resp.text[:200]}"
                )

            resp.raise_for_status()
            return resp.json() if resp.content else {}

        raise KalshiAPIError(f"Kalshi max retries on {path}") from last_err

    # ----- Public endpoints --------------------------------------------------

    async def iter_events(
        self,
        category: str | None = None,
        status: str = "open",
        with_nested_markets: bool = False,
        page_size: int = 100,
        max_pages: int = 200,
    ) -> AsyncIterator[dict[str, Any]]:
        """Cursor-paginated iteration over /events.

        Kalshi's category metadata lives on events, not markets — so this
        is the right place to filter by Politics/Economics/etc. Passing
        `with_nested_markets=True` makes the response include each
        event's markets inline, saving a per-event roundtrip.
        """
        cursor: str | None = None
        pages = 0
        total_yielded = 0
        while pages < max_pages:
            pages += 1
            params: dict[str, Any] = {"status": status, "limit": page_size}
            if category:
                params["category"] = category
            if with_nested_markets:
                params["with_nested_markets"] = "true"
            if cursor:
                params["cursor"] = cursor
            page = await self._request("GET", "/events", params=params)
            events = page.get("events") or []
            new_cursor = page.get("cursor")
            if pages == 1 or pages % 10 == 0:
                logger.info(
                    "Kalshi /events page %d (cat=%s): %d events, cursor=%r",
                    pages,
                    category or "*",
                    len(events),
                    (new_cursor or "")[:24],
                )
            if not events:
                break
            for event in events:
                yield event
                total_yielded += 1
            if not new_cursor or new_cursor == cursor:
                break
            cursor = new_cursor
        logger.info(
            "Kalshi /events (cat=%s): ended after %d pages, %d events yielded",
            category or "*",
            pages,
            total_yielded,
        )

    async def iter_markets(
        self,
        status: str = "open",
        page_size: int = 200,
        max_pages: int = 500,
    ) -> AsyncIterator[dict[str, Any]]:
        """Cursor-paginated iteration over /markets.

        Defensive termination: stops when the response has an empty markets
        list, no cursor, or returns the same cursor twice in a row. Also
        caps at `max_pages` so a misbehaving server can't trap us forever.
        Logs page progress every 20 pages.
        """
        cursor: str | None = None
        pages = 0
        total_yielded = 0
        while pages < max_pages:
            pages += 1
            params: dict[str, Any] = {"status": status, "limit": page_size}
            if cursor:
                params["cursor"] = cursor
            page = await self._request("GET", "/markets", params=params)
            markets = page.get("markets") or []
            new_cursor = page.get("cursor")
            if pages == 1 or pages % 20 == 0:
                logger.info(
                    "Kalshi /markets page %d: %d markets, cursor=%r",
                    pages,
                    len(markets),
                    (new_cursor or "")[:24],
                )
            if not markets:
                break
            for market in markets:
                yield market
                total_yielded += 1
            if not new_cursor or new_cursor == cursor:
                break
            cursor = new_cursor
        logger.info(
            "Kalshi /markets: ended after %d pages, %d markets yielded",
            pages,
            total_yielded,
        )

    async def get_orderbook(self, ticker: str) -> dict[str, Any]:
        return await self._request("GET", f"/markets/{ticker}/orderbook")

    async def get_trades(
        self, ticker: str, limit: int = 100, cursor: str | None = None
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"ticker": ticker, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        return await self._request("GET", "/markets/trades", params=params)
