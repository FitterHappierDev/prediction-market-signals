"""Kalshi Collector — discovery, 1-minute probability bars, trade tape.

Implements REQ-KAL-001 (Market Discovery), REQ-KAL-002 (Probability
Ingestion via REST polling), and REQ-KAL-003 (Trade Ingestion).
REQ-KAL-004 (cross-platform matching) is intentionally deferred —
P1 priority, easier once both sides have a day of history.

Pipeline mirrors PolymarketCollector:
- Discovery task walks /markets every two poll intervals, classifies via
  Kalshi's `category`/`subtitle` fields, upserts into pm_markets.
- Poll task fans out per-market orderbook + trade fetches.
- Flush task writes complete-minute bars to pm_probabilities and publishes
  to pm:probabilities (along with each trade to pm:trades).

Kalshi quirks:
- Probabilities are cents (0-100); divide by 100 for the canonical 0-1.
- Centralised exchange: no wallet addresses. `wallet_address` is left empty
  on every pm_trades row (per REQ-KAL-003).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from questdb.ingress import TimestampNanos

from src.config import Settings
from src.ingestion.kalshi_api import KalshiAPI, KalshiAPIError
from src.ingestion.polymarket import BarBuilder, BoundedSeenSet, _minute_unix
from src.utils.db import QuestDBClient
from src.utils.redis_client import RedisClient
from src.utils.time_utils import now_utc

logger = logging.getLogger(__name__)


# Kalshi categories we ingest. Everything else (Sports, Entertainment,
# Climate, ...) is dropped at the discovery stage so we never pay the
# per-market poll cost. REQ-KAL-001 maps them as follows:
TARGET_KALSHI_CATEGORIES: tuple[str, ...] = (
    "Politics",
    "Elections",
    "Economics",
    "Financials",
)


def derive_category_from_event(
    event_category: str | None, series_ticker: str | None
) -> tuple[str, str]:
    """Map a Kalshi event's (category, series_ticker) to our (category,
    bet_type) pair. Kalshi market responses don't carry category info —
    it lives on the parent event."""
    cat = (event_category or "").lower()
    series = (series_ticker or "").upper()
    if cat in ("politics", "elections"):
        return "political", "preset"
    if cat == "financials":
        return "earnings", "preset"
    if cat == "economics":
        # Sub-classify economics events from their series ticker prefix.
        if any(token in series for token in ("CPI", "UNEMP", "GDP", "INFL", "JOBS")):
            return "recession", "preset"
        return "fed_rate", "preset"
    return "other", "preset"


@dataclass
class KalshiMarket:
    market_id: str  # `kalshi:{ticker}`
    ticker: str
    title: str
    category: str
    bet_type: str
    close_time: datetime | None
    volume_total_usd: float
    consecutive_skips: int = field(default=0)


def _parse_iso(raw: Any) -> datetime | None:
    if not raw or not isinstance(raw, str):
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_market_with_event(
    raw_market: dict[str, Any], raw_event: dict[str, Any]
) -> KalshiMarket | None:
    """Construct a KalshiMarket from a market dict + its parent event
    dict. The event provides the category/series_ticker we need; the
    market provides ticker, close_time, volume, etc."""
    ticker = raw_market.get("ticker")
    if not ticker:
        return None
    category, bet_type = derive_category_from_event(
        raw_event.get("category"), raw_event.get("series_ticker")
    )
    if category == "other":
        return None
    title = (
        raw_market.get("title")
        or raw_market.get("yes_sub_title")
        or raw_event.get("title")
        or ticker
    )
    close_time = _parse_iso(
        raw_market.get("close_time") or raw_market.get("expiration_time")
    )
    return KalshiMarket(
        market_id=f"kalshi:{ticker}",
        ticker=ticker,
        title=title,
        category=category,
        bet_type=bet_type,
        close_time=close_time,
        volume_total_usd=float(raw_market.get("volume", 0) or 0),
    )


def _midpoint_from_orderbook(
    book: dict[str, Any],
) -> tuple[float | None, float | None, float | None]:
    """Parse Kalshi's fractional orderbook.

    Response shape from /markets/{ticker}/orderbook:
        {"orderbook_fp": {
            "yes_dollars": [[price_str, size_str], ...],  # bids on YES
            "no_dollars":  [[price_str, size_str], ...],  # bids on NO
        }}

    Prices are already in dollars (e.g. "0.0100" = 1 cent). The YES side
    is bids on YES (max = best bid). The YES ask is implied by the best
    NO bid: ask_yes = 1.0 - max(no_dollars).

    Caller should pass the inner object (book["orderbook_fp"]); we also
    accept the older `yes`/`no` keys for forward-compatibility.
    """
    yes_side = book.get("yes_dollars") or book.get("yes") or []
    no_side = book.get("no_dollars") or book.get("no") or []

    def _best_price(side: list) -> float | None:
        prices: list[float] = []
        for entry in side:
            if not entry:
                continue
            try:
                prices.append(float(entry[0]))
            except (TypeError, ValueError):
                continue
        return max(prices) if prices else None

    bid_yes = _best_price(yes_side)
    bid_no = _best_price(no_side)
    ask_yes = (1.0 - bid_no) if bid_no is not None else None

    if bid_yes is not None and ask_yes is not None:
        prob = (bid_yes + ask_yes) / 2.0
        return prob, bid_yes, ask_yes
    if bid_yes is not None:
        return bid_yes, bid_yes, None
    if ask_yes is not None:
        return ask_yes, None, ask_yes
    return None, None, None


class KalshiCollector:
    SOURCE = "kalshi"
    STREAM_PROBS = "pm:probabilities"
    STREAM_TRADES = "pm:trades"

    def __init__(
        self,
        api: KalshiAPI,
        db: QuestDBClient,
        redis_client: RedisClient,
        cfg: Settings,
    ) -> None:
        self._api = api
        self._db = db
        self._redis = redis_client
        self._cfg = cfg

        self._poll_interval = cfg.ingestion.kalshi.poll_interval_seconds
        self._active: dict[str, KalshiMarket] = {}
        self._bars: dict[tuple[str, int], BarBuilder] = {}
        self._seen_trades = BoundedSeenSet()
        self._tasks: list[asyncio.Task] = []
        self._stop = asyncio.Event()

    async def start(self) -> None:
        logger.info("KalshiCollector starting (poll_interval=%ds)", self._poll_interval)
        self._tasks = [
            asyncio.create_task(self._discovery_loop(), name="kalshi-discovery"),
            asyncio.create_task(self._poll_loop(), name="kalshi-poll"),
            asyncio.create_task(self._flush_bars_loop(), name="kalshi-flush"),
        ]
        results = await asyncio.gather(*self._tasks, return_exceptions=True)
        for name, result in zip(("discovery", "poll", "flush"), results):
            if isinstance(result, BaseException) and not isinstance(
                result, asyncio.CancelledError
            ):
                logger.warning("KalshiCollector %s task ended with: %r", name, result)
        logger.info("KalshiCollector.start() returning")

    async def stop(self) -> None:
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._flush_due_bars(force=True)

    # ----- discovery ---------------------------------------------------------

    async def _discovery_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._discover_once()
            except Exception:
                logger.exception("Kalshi discovery failed")
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self._poll_interval * 2
                )
            except asyncio.TimeoutError:
                continue

    async def _discover_once(self) -> None:
        logger.info("Kalshi discovery cycle starting")
        events_seen = 0
        markets_seen = 0
        kept = 0
        for kalshi_cat in TARGET_KALSHI_CATEGORIES:
            async for event in self._api.iter_events(
                category=kalshi_cat, status="open", with_nested_markets=True
            ):
                events_seen += 1
                for raw_market in event.get("markets") or []:
                    markets_seen += 1
                    parsed = parse_market_with_event(raw_market, event)
                    if parsed is None:
                        continue
                    self._write_market(parsed)
                    self._active[parsed.market_id] = parsed
                    kept += 1
        logger.info(
            "Kalshi discovery: %d events across %d categories, "
            "%d markets seen, %d kept",
            events_seen,
            len(TARGET_KALSHI_CATEGORIES),
            markets_seen,
            kept,
        )

    def _write_market(self, m: KalshiMarket) -> None:
        symbols = {
            "market_id": m.market_id,
            "source": self.SOURCE,
            "category": m.category,
            "bet_type": m.bet_type,
            "outcome": "",
            "underlying_ticker": "",
        }
        columns: dict[str, Any] = {
            "title": m.title,
            "resolved": False,
            "volume_total_usd": m.volume_total_usd,
        }
        if m.close_time is not None:
            # datetime is a supported column type; TimestampNanos only
            # works for the `at=` designated timestamp.
            columns["resolution_time"] = m.close_time
        self._db.write_row("pm_markets", symbols=symbols, columns=columns)

    # ----- polling -----------------------------------------------------------

    async def _poll_loop(self) -> None:
        while not self._stop.is_set():
            markets = list(self._active.values())
            if markets:
                await asyncio.gather(
                    *(self._poll_one(m) for m in markets), return_exceptions=True
                )
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._poll_interval)
            except asyncio.TimeoutError:
                continue

    async def _poll_one(self, m: KalshiMarket) -> None:
        try:
            await self._poll_probability(m)
            await self._poll_trades(m)
        except KalshiAPIError as e:
            logger.warning("Kalshi poll failed for %s: %s", m.ticker, e)
        except Exception:
            logger.exception("Unexpected error polling %s", m.ticker)

    async def _poll_probability(self, m: KalshiMarket) -> None:
        try:
            resp = await self._api.get_orderbook(m.ticker)
        except KalshiAPIError:
            return
        # Kalshi wraps the order book under `orderbook_fp` (fractional
        # precision). Older deployments used `orderbook`; fall back to
        # the top level only if neither key is present.
        if isinstance(resp, dict):
            book = resp.get("orderbook_fp") or resp.get("orderbook") or resp
        else:
            book = {}
        if not isinstance(book, dict):
            book = {}

        prob, bid, ask = _midpoint_from_orderbook(book)
        if prob is None:
            m.consecutive_skips += 1
            if m.consecutive_skips == 10:
                logger.error(
                    "pm_market_stale: %s (10 consecutive skips)", m.market_id
                )
            return
        m.consecutive_skips = 0
        prob = max(0.0, min(1.0, prob))

        bar_key = (m.market_id, _minute_unix(now_utc()))
        bar = self._bars.setdefault(bar_key, BarBuilder())
        bar.update_book(prob, bid, ask)

    async def _poll_trades(self, m: KalshiMarket) -> None:
        try:
            resp = await self._api.get_trades(m.ticker, limit=100)
        except KalshiAPIError:
            return
        trades = resp.get("trades", []) if isinstance(resp, dict) else resp or []
        for raw in trades:
            await self._ingest_trade(m, raw)

    async def _ingest_trade(self, m: KalshiMarket, raw: dict[str, Any]) -> None:
        trade_id = str(raw.get("trade_id") or raw.get("id") or "")
        if not trade_id or not self._seen_trades.add(trade_id):
            return

        # Kalshi exposes count (token quantity) plus yes/no price in cents.
        count = float(raw.get("count") or 0)
        yes_price_cents = float(raw.get("yes_price") or 0)
        no_price_cents = float(raw.get("no_price") or 0)
        outcome = (raw.get("taker_side") or raw.get("side") or "").lower()  # "yes" | "no"
        price_cents = yes_price_cents if outcome == "yes" else no_price_cents
        if count <= 0 or price_cents <= 0:
            return

        price = price_cents / 100.0
        size_usd = count * price

        ts = _parse_iso(raw.get("created_time")) or now_utc()
        side = f"buy_{outcome}" if outcome in ("yes", "no") else ""

        self._db.write_row(
            "pm_trades",
            symbols={
                "market_id": m.market_id,
                "source": self.SOURCE,
                "side": side,
            },
            columns={
                "wallet_address": "",  # centralized exchange — no wallet
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
                "wallet_address": "",
                "side": side,
                "price": price,
                "size_usd": size_usd,
                "trade_id": trade_id,
            },
        )

        bar_key = (m.market_id, _minute_unix(ts))
        bar = self._bars.setdefault(bar_key, BarBuilder())
        bar.add_trade(size_usd)

    # ----- bar flushing ------------------------------------------------------

    async def _flush_bars_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._flush_due_bars(force=False)
            except Exception:
                logger.exception("Kalshi bar flush failed")
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
