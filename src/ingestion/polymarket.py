"""Polymarket Collector — live ingestion of market metadata, 1-minute
probability bars, and individual trades.

Implements REQ-PMC-001 (Market Discovery), REQ-PMC-002 (Probability
Ingestion), REQ-PMC-003 (Trade Ingestion), and REQ-PMC-005 (Market
Classification). Historical backfill (REQ-PMC-004) lives separately and
is wired by `main.py`.

Pipeline:
- A discovery task walks the Gamma /markets endpoint every two poll
  intervals, classifies each entry, and upserts into pm_markets.
- A polling task fans out per-market book + trades fetches every
  poll_interval_seconds. Each book poll updates the current minute's bar;
  each new trade is deduped, written to pm_trades, and published to
  pm:trades.
- A flush task fires every 5s, writes any bars whose minute is now in the
  past, and publishes them to pm:probabilities.

Bars without a probability value (e.g., a minute where only trades
happened but no book was readable) are dropped, per the REQ-PMC-002 edge
case ("do NOT write a bar with probability = 0 or NULL").
"""

from __future__ import annotations

import asyncio
import collections
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from questdb.ingress import TimestampNanos

from src.config import Settings
from src.ingestion.polymarket_api import PolymarketAPI, PolymarketAPIError
from src.utils.db import QuestDBClient
from src.utils.redis_client import RedisClient
from src.utils.time_utils import now_utc

logger = logging.getLogger(__name__)

# ---- Constants --------------------------------------------------------------

# Wallet addresses to exclude from trade ingestion (CLOB proxy / system
# contracts, not user wallets). Lowercase hex. Expand as Polymarket evolves.
CONTRACT_EXCLUSIONS: frozenset[str] = frozenset(
    {
        # Add known Polymarket CTF/exchange proxy addresses here as we
        # observe them in the trade tape. Empty for now — wallet_address
        # checks against this set on every trade.
    }
)

# REQ-PMC-005 — category keyword sets, evaluated in priority order.
CATEGORY_KEYWORDS: dict[str, tuple[str, ...]] = {
    "geopolitical": ("strike", "war", "ceasefire", "invasion", "sanctions", "military"),
    "fed_rate": ("fed", "fomc", "rate cut", "rate hike", "basis point", "interest rate"),
    "earnings": ("beat quarterly earnings", "miss quarterly earnings", "revenue", "eps"),
    "recession": ("recession", "gdp", "economic downturn", "contraction"),
    "crypto": ("bitcoin", "btc", "ethereum", "eth", "crypto", "token"),
    "political": ("election", "president", "governor", "senate", "congress", "legislation"),
}
CATEGORY_PRIORITY: tuple[str, ...] = (
    "geopolitical",
    "fed_rate",
    "earnings",
    "recession",
    "crypto",
    "political",
)

# REQ-PMC-005 — bet_type heuristics
EXPIRING_HINTS = re.compile(r"\$|above|below|hit|reach", re.IGNORECASE)

# Max distinct trade IDs we remember in-memory for de-dup. Larger than the
# expected per-poll trade rate × per-market count × seconds-of-history so
# that QuestDB-side dedup is just a backstop.
TRADE_DEDUP_SIZE = 50_000


def canonical_side(raw_side: str, raw_outcome: str) -> str:
    """Map Polymarket Data API's (side=BUY|SELL, outcome=Yes|No) tuple to the
    canonical `pm_trades.side` symbol used by the schema:
    buy_yes | buy_no | sell_yes | sell_no. Returns '' for unknown inputs."""
    s = (raw_side or "").lower()
    o = (raw_outcome or "").lower()
    if s in ("buy", "sell") and o in ("yes", "no"):
        return f"{s}_{o}"
    return ""


# ---- Helpers ----------------------------------------------------------------


class BoundedSeenSet:
    """FIFO-evicting bounded set used for trade-ID de-dup.

    Backed by a dict (insertion-ordered in 3.7+). When full, the oldest key
    is evicted on insert. Membership tests are O(1).
    """

    def __init__(self, max_size: int = TRADE_DEDUP_SIZE) -> None:
        self._d: dict[str, None] = {}
        self._max = max_size

    def add(self, key: str) -> bool:
        if key in self._d:
            return False
        if len(self._d) >= self._max:
            oldest = next(iter(self._d))
            del self._d[oldest]
        self._d[key] = None
        return True


@dataclass
class BarBuilder:
    """One in-progress minute bar."""

    probability: float | None = None
    bid: float | None = None
    ask: float | None = None
    volume_usd: float = 0.0
    trade_count: int = 0

    def update_book(self, prob: float, bid: float | None, ask: float | None) -> None:
        self.probability = prob
        self.bid = bid
        self.ask = ask

    def add_trade(self, size_usd: float) -> None:
        self.volume_usd += size_usd
        self.trade_count += 1


@dataclass
class ParsedMarket:
    """Subset of the raw Gamma market dict that the collector actually uses."""

    market_id: str
    yes_token_id: str | None
    title: str
    description: str
    category: str
    bet_type: str
    end_date: datetime | None
    resolved: bool
    outcome: str | None
    strike_price: float | None
    underlying_ticker: str | None
    volume_total_usd: float
    consecutive_skips: int = field(default=0)


def _parse_end_date(raw: Any) -> datetime | None:
    if not raw:
        return None
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(raw, tz=timezone.utc)
    if isinstance(raw, str):
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _extract_yes_token_id(raw: dict[str, Any]) -> str | None:
    """Locate the YES token id. Polymarket has shipped a few field shapes
    over time — try the common ones."""
    token_ids = raw.get("clobTokenIds") or raw.get("clob_token_ids")
    if isinstance(token_ids, str):
        try:
            token_ids = json.loads(token_ids)
        except json.JSONDecodeError:
            token_ids = None
    if isinstance(token_ids, list) and token_ids:
        return str(token_ids[0])
    return None


def classify_category(title: str, description: str) -> str:
    text = f"{title} {description}".lower()
    for cat in CATEGORY_PRIORITY:
        for kw in CATEGORY_KEYWORDS[cat]:
            if kw in text:
                return cat
    return "other"


def classify_bet_type(title: str, end_date: datetime | None) -> str:
    if end_date is None:
        return "triggered"
    horizon = end_date - now_utc()
    if horizon.days > 365:
        return "triggered"
    if EXPIRING_HINTS.search(title):
        return "expiring"
    return "preset"


def parse_market(raw: dict[str, Any]) -> ParsedMarket | None:
    """Map a raw Gamma /markets entry to a ParsedMarket. Returns None if
    the entry has no usable identifier."""
    market_id = (
        raw.get("conditionId")
        or raw.get("condition_id")
        or raw.get("id")
    )
    if not market_id:
        return None

    title = raw.get("question") or raw.get("title") or ""
    description = raw.get("description") or ""
    end_date = _parse_end_date(raw.get("endDate") or raw.get("end_date_iso"))

    return ParsedMarket(
        market_id=str(market_id),
        yes_token_id=_extract_yes_token_id(raw),
        title=title,
        description=description,
        category=classify_category(title, description),
        bet_type=classify_bet_type(title, end_date),
        end_date=end_date,
        resolved=bool(raw.get("closed") or raw.get("resolved")),
        outcome=raw.get("outcome") or raw.get("result"),
        strike_price=raw.get("strike_price") or raw.get("strikePrice"),
        underlying_ticker=raw.get("underlying_ticker") or raw.get("underlyingTicker"),
        volume_total_usd=float(raw.get("volume") or 0),
    )


def _midpoint_from_book(book: dict[str, Any]) -> tuple[float | None, float | None, float | None]:
    """Return (probability, best_bid, best_ask) from a CLOB book response.

    Handles empty / one-sided / crossed books per REQ-PMC-002 edge cases.
    """
    bids = book.get("bids") or []
    asks = book.get("asks") or []

    def _best(side: list[dict[str, Any]], best_of: str) -> float | None:
        prices = [float(o["price"]) for o in side if "price" in o]
        if not prices:
            return None
        return max(prices) if best_of == "max" else min(prices)

    best_bid = _best(bids, "max")
    best_ask = _best(asks, "min")

    if best_bid is not None and best_ask is not None:
        # Crossed market — use the tighter (ask) side and warn.
        if best_bid > best_ask:
            logger.warning("Crossed book: bid=%.4f ask=%.4f", best_bid, best_ask)
            return best_ask, best_bid, best_ask
        return (best_bid + best_ask) / 2.0, best_bid, best_ask
    if best_bid is not None:
        return best_bid, best_bid, None
    if best_ask is not None:
        return best_ask, None, best_ask
    return None, None, None


def _minute_unix(ts: datetime) -> int:
    return int(ts.timestamp()) // 60 * 60


# ---- Collector --------------------------------------------------------------


class PolymarketCollector:
    SOURCE = "polymarket"
    STREAM_PROBS = "pm:probabilities"
    STREAM_TRADES = "pm:trades"

    def __init__(
        self,
        api: PolymarketAPI,
        db: QuestDBClient,
        redis_client: RedisClient,
        cfg: Settings,
    ) -> None:
        self._api = api
        self._db = db
        self._redis = redis_client
        self._cfg = cfg

        self._poll_interval = cfg.ingestion.polymarket.poll_interval_seconds
        self._active: dict[str, ParsedMarket] = {}
        self._bars: dict[tuple[str, int], BarBuilder] = {}
        self._seen_trades = BoundedSeenSet()
        self._tasks: list[asyncio.Task] = []
        self._stop = asyncio.Event()

    async def start(self) -> None:
        """Spin up the discovery, polling, and flush tasks. Returns when
        stop() is called or all tasks crash."""
        self._tasks = [
            asyncio.create_task(self._discovery_loop(), name="pm-discovery"),
            asyncio.create_task(self._poll_loop(), name="pm-poll"),
            asyncio.create_task(self._flush_bars_loop(), name="pm-flush"),
        ]
        await asyncio.gather(*self._tasks, return_exceptions=True)

    # ----- historical backfill (REQ-PMC-004) ---------------------------------

    async def backfill_history(self) -> None:
        """Pull `backfill_days` of 1-minute prices for every active market.

        Idempotent at the QuestDB layer (commitLag dedupes on timestamp +
        market_id). Skipped if pm_probabilities is already non-empty.
        Runs at half the steady-state request rate to avoid throttling.
        """
        if not self._needs_backfill():
            logger.info("Polymarket backfill: pm_probabilities non-empty, skipping")
            return

        if not self._active:
            logger.info("Polymarket backfill: discovering markets first")
            await self._discover_once()

        steady_rate = self._cfg.ingestion.polymarket.max_requests_per_second
        self._api.set_rate(max(steady_rate / 2, 1.0))
        try:
            days = self._cfg.ingestion.polymarket.backfill_days
            end_ts = int(now_utc().timestamp())
            start_ts = end_ts - days * 86400
            markets = list(self._active.values())
            total = len(markets)
            logger.info(
                "Polymarket backfill: %d markets, %d days at half rate", total, days
            )
            for i, m in enumerate(markets, start=1):
                if m.yes_token_id is None:
                    continue
                try:
                    await self._backfill_one(m, start_ts, end_ts)
                except Exception:
                    logger.exception("Backfill failed for %s", m.market_id)
                if i % 50 == 0 or i == total:
                    logger.info(
                        "Polymarket backfill: %d/%d (%.1f%%)",
                        i,
                        total,
                        100.0 * i / total,
                    )
        finally:
            self._api.set_rate(steady_rate)
        logger.info("Polymarket backfill: complete")

    async def _backfill_one(
        self, m: ParsedMarket, start_ts: int, end_ts: int
    ) -> None:
        assert m.yes_token_id is not None
        history = await self._api.get_prices_history(
            m.yes_token_id, start_ts, end_ts, fidelity_minutes=1
        )
        for point in history:
            t = point.get("t")
            p = point.get("p")
            if t is None or p is None:
                continue
            try:
                ts = datetime.fromtimestamp(int(t), tz=timezone.utc)
                prob = max(0.0, min(1.0, float(p)))
            except (ValueError, OSError):
                continue
            self._db.write_row(
                "pm_probabilities",
                symbols={"market_id": m.market_id, "source": self.SOURCE},
                columns={
                    "probability": prob,
                    "bid": 0.0,
                    "ask": 0.0,
                    "volume_usd": 0.0,
                    "trade_count": 0,
                },
                at=TimestampNanos(int(ts.timestamp() * 1e9)),
            )

    def _needs_backfill(self) -> bool:
        try:
            df = self._db.query(
                "SELECT count() AS n FROM pm_probabilities WHERE source = 'polymarket'"
            )
        except Exception as e:
            logger.warning("Backfill needed-check failed (%s); will run backfill", e)
            return True
        if df.empty:
            return True
        return int(df.iloc[0]["n"]) == 0

    async def stop(self) -> None:
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        # Flush whatever bars remain in memory.
        self._flush_due_bars(force=True)

    # ----- discovery ---------------------------------------------------------

    async def _discovery_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._discover_once()
            except Exception:
                logger.exception("Polymarket market discovery failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._poll_interval * 2)
            except asyncio.TimeoutError:
                continue

    async def _discover_once(self) -> None:
        count = 0
        async for raw in self._api.iter_markets(active=True, closed=False):
            parsed = parse_market(raw)
            if parsed is None:
                continue
            if parsed.yes_token_id is None:
                logger.debug("Market %s has no clobTokenIds; skipping", parsed.market_id)
                continue
            self._write_market(parsed)
            self._active[parsed.market_id] = parsed
            count += 1
        logger.info("Polymarket discovery: %d active markets tracked", count)

    def _write_market(self, m: ParsedMarket) -> None:
        symbols = {
            "market_id": m.market_id,
            "source": self.SOURCE,
            "category": m.category,
            "bet_type": m.bet_type,
            "outcome": m.outcome or "",
            "underlying_ticker": m.underlying_ticker or "",
        }
        columns: dict[str, Any] = {
            "title": m.title,
            "resolved": m.resolved,
            "volume_total_usd": m.volume_total_usd,
        }
        if m.end_date is not None:
            # questdb-python wants a datetime (or TimestampMicros) for
            # TIMESTAMP columns; TimestampNanos is only valid for `at=`.
            columns["resolution_time"] = m.end_date
        if m.strike_price is not None:
            columns["strike_price"] = float(m.strike_price)
        self._db.write_row("pm_markets", symbols=symbols, columns=columns)

    # ----- polling -----------------------------------------------------------

    async def _poll_loop(self) -> None:
        while not self._stop.is_set():
            markets = list(self._active.values())
            if markets:
                await asyncio.gather(
                    *(self._poll_one_market(m) for m in markets),
                    return_exceptions=True,
                )
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._poll_interval)
            except asyncio.TimeoutError:
                continue

    async def _poll_one_market(self, m: ParsedMarket) -> None:
        try:
            await self._poll_probability(m)
            await self._poll_trades(m)
        except PolymarketAPIError as e:
            logger.warning("PM poll failed for %s: %s", m.market_id, e)
        except Exception:
            logger.exception("Unexpected error polling %s", m.market_id)

    async def _poll_probability(self, m: ParsedMarket) -> None:
        if m.yes_token_id is None:
            return
        try:
            book = await self._api.get_order_book(m.yes_token_id)
        except PolymarketAPIError:
            book = {}

        prob, bid, ask = _midpoint_from_book(book) if book else (None, None, None)

        # Fallback to last trade price (REQ-PMC-002 edge case).
        if prob is None:
            try:
                trades = await self._api.get_trades(m.market_id, limit=1)
                if trades:
                    last = trades[0]
                    prob = float(last.get("price", 0))
            except PolymarketAPIError:
                pass

        if prob is None:
            m.consecutive_skips += 1
            if m.consecutive_skips == 10:
                logger.error("pm_market_stale: %s (10 consecutive skips)", m.market_id)
            return

        m.consecutive_skips = 0
        prob = max(0.0, min(1.0, prob))

        bar_key = (m.market_id, _minute_unix(now_utc()))
        bar = self._bars.setdefault(bar_key, BarBuilder())
        bar.update_book(prob, bid, ask)

    async def _poll_trades(self, m: ParsedMarket) -> None:
        try:
            trades = await self._api.get_trades(m.market_id, limit=100)
        except PolymarketAPIError:
            return

        for raw_trade in trades:
            await self._ingest_trade(m, raw_trade)

    async def _ingest_trade(self, m: ParsedMarket, raw: dict[str, Any]) -> None:
        trade_id = str(
            raw.get("id") or raw.get("trade_id") or raw.get("transactionHash") or ""
        )
        if not trade_id or not self._seen_trades.add(trade_id):
            return

        size = float(raw.get("size") or 0)
        price = float(raw.get("price") or 0)
        size_usd = float(raw.get("size_usd") or (size * price))
        if size_usd <= 0:
            return  # dust / probe trade

        wallet = str(
            raw.get("proxyWallet") or raw.get("taker") or raw.get("maker") or ""
        ).lower()
        if wallet and wallet in CONTRACT_EXCLUSIONS:
            return

        ts_raw = raw.get("timestamp") or raw.get("ts")
        ts = self._parse_trade_timestamp(ts_raw) or now_utc()

        side = canonical_side(str(raw.get("side") or ""), str(raw.get("outcome") or ""))

        self._db.write_row(
            "pm_trades",
            symbols={"market_id": m.market_id, "source": self.SOURCE, "side": side},
            columns={
                "wallet_address": wallet,
                "price": price,
                "size_usd": size_usd,
                "trade_id": trade_id,
            },
            at=TimestampNanos(int(ts.timestamp() * 1e9)),
        )
        await self._redis.publish_to_stream(
            self.STREAM_TRADES,
            {
                "timestamp": ts.isoformat(),
                "market_id": m.market_id,
                "source": self.SOURCE,
                "wallet_address": wallet,
                "side": side,
                "price": price,
                "size_usd": size_usd,
                "trade_id": trade_id,
            },
        )

        bar_key = (m.market_id, _minute_unix(ts))
        bar = self._bars.setdefault(bar_key, BarBuilder())
        bar.add_trade(size_usd)

    @staticmethod
    def _parse_trade_timestamp(raw: Any) -> datetime | None:
        if raw is None:
            return None
        try:
            if isinstance(raw, (int, float)):
                v = float(raw)
                if v > 1e12:  # heuristic: ms vs s
                    v /= 1000.0
                return datetime.fromtimestamp(v, tz=timezone.utc)
            if isinstance(raw, str):
                if raw.isdigit():
                    return PolymarketCollector._parse_trade_timestamp(int(raw))
                return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except (ValueError, OSError):
            return None
        return None

    # ----- bar flushing ------------------------------------------------------

    async def _flush_bars_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._flush_due_bars(force=False)
            except Exception:
                logger.exception("Bar flush failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=5)
            except asyncio.TimeoutError:
                continue

    def _flush_due_bars(self, force: bool) -> None:
        cutoff = _minute_unix(now_utc())
        due_keys = [k for k in self._bars if force or k[1] < cutoff]
        for key in due_keys:
            bar = self._bars.pop(key)
            if bar.probability is None:
                continue
            market_id, minute_unix = key
            ts = datetime.fromtimestamp(minute_unix, tz=timezone.utc)
            self._db.write_row(
                "pm_probabilities",
                symbols={"market_id": market_id, "source": self.SOURCE},
                columns={
                    "probability": bar.probability,
                    "bid": bar.bid if bar.bid is not None else 0.0,
                    "ask": bar.ask if bar.ask is not None else 0.0,
                    "volume_usd": bar.volume_usd,
                    "trade_count": bar.trade_count,
                },
                at=TimestampNanos(int(ts.timestamp() * 1e9)),
            )
            asyncio.create_task(
                self._redis.publish_to_stream(
                    self.STREAM_PROBS,
                    {
                        "timestamp": ts.isoformat(),
                        "market_id": market_id,
                        "source": self.SOURCE,
                        "probability": bar.probability,
                        "bid": bar.bid,
                        "ask": bar.ask,
                        "volume_usd": bar.volume_usd,
                        "trade_count": bar.trade_count,
                    },
                )
            )
