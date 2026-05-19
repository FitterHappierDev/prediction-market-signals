"""Polygon Wallet Tracer — REQ-WLT-001/020/021.

Subscribes to Redis stream `wallets:trace_request`. For each address,
fetches on-chain metadata via Alchemy Polygon RPC and Polymarket trade
history via QuestDB, writes a profile to `pm_wallets`, caches it in
Redis hash `wallet:{address}` (TTL = retrace_interval_hours), and
publishes the result on `wallets:trace_response`.

Per profile, up to 4 RPC calls:
  1. eth_getCode (detect contract vs EOA)
  2. alchemy_getAssetTransfers ascending → first transaction blockNum
  3. eth_getBlockByNumber on that blockNum → wallet age
  4. alchemy_getAssetTransfers USDC desc → funding_source_1
  5. (optional 5th) alchemy_getAssetTransfers USDC desc on hop-1 →
     funding_source_2

Plus one QuestDB query for PM market count + volume.

Resource protection:
- Semaphore caps in-flight traces at max_concurrent_rpc (5).
- Per-wallet RPC budget of MAX_RPC_PER_WALLET (20) — prevents runaway.
- Hourly RPC budget of MAX_RPC_PER_HOUR (5000) — protects free-tier cost.
- Exhausted budgets → write trace_status='budget_exhausted' instead of
  blocking.

Exclusions (loaded from config/exchange_wallets.json):
- exchange addresses: funder is an exchange → mark funded_by_exchange,
  do NOT recurse for hop-2 (two users from Coinbase ≠ same entity).
- aggregator addresses: funder is a DEX/bridge → stop trace, set
  funding_source_2='unknown_via_aggregator'.

Graceful degradation:
- Any RPC failure → trace_status='rpc_failed', partial profile saved.
- Anomaly Detector treats missing fields per REQ-ANM-003 (gracefully).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from questdb.ingress import TimestampNanos

from src.config import Settings
from src.ingestion.alchemy import (
    POLYGON_USDC_ADDRESSES,
    AlchemyError,
    AlchemyPolygonAPI,
)
from src.utils.db import QuestDBClient
from src.utils.redis_client import RedisClient

logger = logging.getLogger(__name__)

TRACE_REQUEST_STREAM = "wallets:trace_request"
TRACE_RESPONSE_STREAM = "wallets:trace_response"
CONSUMER_GROUP = "wallet_tracer"
CONSUMER_NAME = "wallet_tracer_1"

MAX_RPC_PER_WALLET = 20
MAX_RPC_PER_HOUR = 5000

# Sentinel funding source when tracing hits a DEX/bridge — we cannot
# reliably continue past an aggregator, so the second hop is marked.
UNKNOWN_VIA_AGGREGATOR = "unknown_via_aggregator"

_ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")


def is_valid_address(addr: str) -> bool:
    return bool(_ADDRESS_RE.match(addr.lower() if addr else ""))


def _is_contract_code(code: str | None) -> bool:
    """eth_getCode returns '0x' for EOAs; any longer hex string is a
    deployed contract."""
    if not code:
        return False
    return code != "0x" and code != "0x0" and len(code) > 2


def _hex_to_int(h: str | None) -> int | None:
    if not h or not isinstance(h, str):
        return None
    try:
        return int(h, 16)
    except ValueError:
        return None


def _coerce_transfer_value(raw: Any) -> float:
    """getAssetTransfers returns `value` as a float (e.g. 1234.56). Be
    defensive — sometimes it's a string or null."""
    if raw is None:
        return 0.0
    if isinstance(raw, (int, float)):
        return float(raw)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


@dataclass
class WalletProfile:
    address: str
    first_seen: datetime | None
    funding_source_1: str | None
    funding_source_2: str | None
    total_transactions: int
    distinct_markets: int
    total_volume_usd: float
    is_contract: bool
    trace_status: str  # 'ok' | 'rpc_failed' | 'budget_exhausted' | 'contract' | 'zero_tx'
    funded_by_exchange: bool = False
    last_traced: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_redis_hash(self) -> dict[str, str]:
        """Stringified mapping for Redis HSET."""
        return {
            "address": self.address,
            "first_seen": self.first_seen.isoformat() if self.first_seen else "",
            "funding_source_1": self.funding_source_1 or "",
            "funding_source_2": self.funding_source_2 or "",
            "total_transactions": str(self.total_transactions),
            "distinct_markets": str(self.distinct_markets),
            "total_volume_usd": f"{self.total_volume_usd:.6f}",
            "is_contract": "1" if self.is_contract else "0",
            "trace_status": self.trace_status,
            "funded_by_exchange": "1" if self.funded_by_exchange else "0",
            "last_traced": self.last_traced.isoformat(),
        }


def _load_exclusion_lists(
    path: Path,
) -> tuple[frozenset[str], frozenset[str]]:
    """Returns (exchange_addresses, aggregator_addresses) — both
    lowercase frozensets."""
    if not path.exists():
        logger.warning(
            "Exchange wallets file not found at %s; sybil exclusion lists empty", path
        )
        return frozenset(), frozenset()
    with path.open() as f:
        data = json.load(f)
    exchanges = frozenset(
        (e.get("address") or "").lower() for e in data.get("exchange_addresses", [])
    )
    aggregators = frozenset(
        (e.get("address") or "").lower() for e in data.get("aggregator_addresses", [])
    )
    logger.info(
        "Loaded exclusions: %d exchange + %d aggregator addresses",
        len(exchanges),
        len(aggregators),
    )
    return exchanges, aggregators


class WalletTracer:
    def __init__(
        self,
        alchemy: AlchemyPolygonAPI,
        db: QuestDBClient,
        redis_client: RedisClient,
        cfg: Settings,
        exclusion_path: Path,
    ) -> None:
        self._alchemy = alchemy
        self._db = db
        self._redis = redis_client
        self._cfg = cfg

        self._exchanges, self._aggregators = _load_exclusion_lists(exclusion_path)

        self._semaphore = asyncio.Semaphore(
            cfg.ingestion.wallet_tracer.max_concurrent_rpc
        )
        self._hourly_count = 0
        self._hourly_window_started_at = time.monotonic()
        self._hourly_lock = asyncio.Lock()

        self._cache_ttl_seconds = (
            cfg.ingestion.wallet_tracer.retrace_interval_hours * 3600
        )

        self._stop = asyncio.Event()

    # ---- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        logger.info("WalletTracer starting; subscribing to %s", TRACE_REQUEST_STREAM)
        async for msg_id, payload in self._redis.subscribe_stream(
            stream=TRACE_REQUEST_STREAM,
            group=CONSUMER_GROUP,
            consumer=CONSUMER_NAME,
        ):
            if self._stop.is_set():
                break
            try:
                await self._handle_request(payload)
            except Exception:
                logger.exception("trace request failed: %s", payload)
            finally:
                await self._redis.ack(TRACE_REQUEST_STREAM, CONSUMER_GROUP, msg_id)

    async def stop(self) -> None:
        self._stop.set()

    # ---- request handling ---------------------------------------------------

    async def _handle_request(self, payload: dict[str, Any]) -> None:
        address = (payload.get("address") or "").lower()
        if not is_valid_address(address):
            logger.warning("Skipping invalid trace request: %r", payload)
            return

        force = bool(payload.get("force", False))
        if not force:
            cached = await self._redis.get_hash(f"wallet:{address}")
            if cached:
                logger.debug("Cache hit for %s", address)
                await self._publish_response(address, cached, source="cache")
                return

        async with self._semaphore:
            profile = await self._trace_one(address)

        await self._persist(profile)
        await self._publish_response(address, profile.to_redis_hash(), source="fresh")

    async def _publish_response(
        self, address: str, payload: dict[str, str], source: str
    ) -> None:
        await self._redis.publish_to_stream(
            TRACE_RESPONSE_STREAM,
            {"address": address, "source": source, **payload},
        )

    # ---- the trace itself ---------------------------------------------------

    async def _trace_one(self, address: str) -> WalletProfile:
        budget = _RPCBudget(MAX_RPC_PER_WALLET)
        profile = WalletProfile(
            address=address,
            first_seen=None,
            funding_source_1=None,
            funding_source_2=None,
            total_transactions=0,
            distinct_markets=0,
            total_volume_usd=0.0,
            is_contract=False,
            trace_status="ok",
        )

        # PM stats first — no RPC, sets distinct_markets and volume even
        # if the chain trace fails.
        try:
            markets, volume = self._query_pm_stats(address)
            profile.distinct_markets = markets
            profile.total_volume_usd = volume
        except Exception:
            logger.exception("pm_trades query failed for %s", address)

        # Contract check — short-circuits the rest. Contracts are
        # typically Polymarket proxy wallets; we record them but don't
        # try to trace funding (the deployer logic isn't useful for our
        # anomaly signals and would burn RPC).
        try:
            if not await self._reserve_rpc():
                profile.trace_status = "budget_exhausted"
                return profile
            budget.consume()
            code = await self._alchemy.get_code(address)
            if _is_contract_code(code):
                profile.is_contract = True
                profile.trace_status = "contract"
                return profile
        except AlchemyError as e:
            logger.warning("get_code failed for %s: %s", address, e)
            profile.trace_status = "rpc_failed"
            return profile

        # Transaction count — cheap.
        try:
            if budget.exhausted() or not await self._reserve_rpc():
                profile.trace_status = "budget_exhausted"
                return profile
            budget.consume()
            profile.total_transactions = await self._alchemy.get_transaction_count(
                address
            )
        except AlchemyError as e:
            logger.warning("get_transaction_count failed for %s: %s", address, e)
            profile.trace_status = "rpc_failed"
            return profile

        # Wallet age — first ascending external transfer's block.
        try:
            profile.first_seen = await self._fetch_first_seen(address, budget)
        except AlchemyError as e:
            logger.warning("first_seen failed for %s: %s", address, e)
            # Non-fatal — keep what we have, mark partial.
            profile.trace_status = "rpc_failed"

        if profile.total_transactions == 0:
            # Strongest freshness signal per REQ-WLT-001 edge case: brand
            # new wallet, set first_seen=now and don't try to trace
            # funding (there is none yet).
            profile.first_seen = datetime.now(timezone.utc)
            profile.trace_status = "zero_tx"
            return profile

        # Funding hop 1.
        try:
            hop1 = await self._fetch_largest_usdc_funder(address, budget)
        except AlchemyError as e:
            logger.warning("funding hop-1 failed for %s: %s", address, e)
            return profile

        if hop1 is None:
            return profile

        if hop1 in self._exchanges:
            profile.funded_by_exchange = True
            profile.funding_source_1 = hop1
            return profile

        if hop1 in self._aggregators:
            profile.funding_source_1 = hop1
            profile.funding_source_2 = UNKNOWN_VIA_AGGREGATOR
            return profile

        profile.funding_source_1 = hop1

        # Funding hop 2 — only if not exchange/aggregator.
        try:
            hop2 = await self._fetch_largest_usdc_funder(hop1, budget)
            if hop2 in self._aggregators:
                profile.funding_source_2 = UNKNOWN_VIA_AGGREGATOR
            else:
                profile.funding_source_2 = hop2
        except AlchemyError as e:
            logger.warning("funding hop-2 failed for %s (hop1=%s): %s", address, hop1, e)

        return profile

    async def _fetch_first_seen(
        self, address: str, budget: "_RPCBudget"
    ) -> datetime | None:
        if budget.exhausted() or not await self._reserve_rpc():
            return None
        budget.consume()
        transfers = await self._alchemy.get_asset_transfers(
            from_address=address,
            category=["external", "erc20"],
            order="asc",
            max_count=1,
        )
        if not transfers:
            return None
        block_num = transfers[0].get("blockNum")
        if not block_num:
            return None

        if budget.exhausted() or not await self._reserve_rpc():
            return None
        budget.consume()
        block = await self._alchemy.get_block_by_number(block_num)
        if not block or "timestamp" not in block:
            return None
        ts_unix = _hex_to_int(block["timestamp"])
        if ts_unix is None:
            return None
        return datetime.fromtimestamp(ts_unix, tz=timezone.utc)

    async def _fetch_largest_usdc_funder(
        self, address: str, budget: "_RPCBudget"
    ) -> str | None:
        if budget.exhausted() or not await self._reserve_rpc():
            return None
        budget.consume()
        transfers = await self._alchemy.get_asset_transfers(
            to_address=address,
            contract_addresses=list(POLYGON_USDC_ADDRESSES),
            category=["erc20"],
            order="desc",
            max_count=20,
        )
        if not transfers:
            return None
        # Pick the largest by value, return its `from`.
        largest = max(transfers, key=lambda t: _coerce_transfer_value(t.get("value")))
        from_addr = (largest.get("from") or "").lower()
        return from_addr if is_valid_address(from_addr) else None

    # ---- rate limiting ------------------------------------------------------

    async def _reserve_rpc(self) -> bool:
        """Returns True if one RPC slot was reserved in the current hourly
        window; False if the hourly cap is exhausted."""
        async with self._hourly_lock:
            now = time.monotonic()
            if now - self._hourly_window_started_at >= 3600:
                self._hourly_window_started_at = now
                self._hourly_count = 0
            if self._hourly_count >= MAX_RPC_PER_HOUR:
                logger.warning(
                    "Hourly RPC budget (%d) exhausted; deferring trace",
                    MAX_RPC_PER_HOUR,
                )
                return False
            self._hourly_count += 1
        return True

    # ---- QuestDB ------------------------------------------------------------

    def _query_pm_stats(self, address: str) -> tuple[int, float]:
        df = self._db.query(
            "SELECT count_distinct(market_id) AS markets, "
            "sum(size_usd) AS volume FROM pm_trades "
            f"WHERE source = 'polymarket' AND wallet_address = '{address}'"
        )
        if df.empty:
            return 0, 0.0
        markets = int(df.iloc[0].get("markets") or 0)
        volume_raw = df.iloc[0].get("volume")
        volume = float(volume_raw) if volume_raw is not None else 0.0
        return markets, volume

    async def _persist(self, profile: WalletProfile) -> None:
        # QuestDB row + Redis cache + publish.
        symbols = {"trace_status": profile.trace_status}
        columns: dict[str, Any] = {
            "wallet_address": profile.address,
            "funding_source_1": profile.funding_source_1 or "",
            "funding_source_2": profile.funding_source_2 or "",
            "total_transactions": profile.total_transactions,
            "distinct_markets": profile.distinct_markets,
            "total_volume_usd": profile.total_volume_usd,
            "is_contract": profile.is_contract,
        }
        if profile.first_seen is not None:
            columns["first_seen"] = profile.first_seen

        self._db.write_row(
            table="pm_wallets",
            symbols=symbols,
            columns=columns,
            at=TimestampNanos(int(profile.last_traced.timestamp() * 1e9)),
        )

        await self._redis.set_hash(
            f"wallet:{profile.address}",
            profile.to_redis_hash(),
            ttl_seconds=self._cache_ttl_seconds,
        )


@dataclass
class _RPCBudget:
    """Per-wallet RPC counter — hard cap so a single complex trace can't
    burn the hourly quota."""

    cap: int
    used: int = 0

    def exhausted(self) -> bool:
        return self.used >= self.cap

    def consume(self) -> None:
        self.used += 1
