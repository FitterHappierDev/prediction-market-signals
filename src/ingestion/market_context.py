"""Market Context Service — traditional-asset OHLCV bars.

The "asset side" of PM→asset linkage analysis. Pulls daily bars from
Yahoo Finance via yfinance, writes to QuestDB pm_assets, and publishes
each bar to the pm:assets Redis stream so downstream linkage tests
can subscribe in real time.

Universe is hardcoded — 10 tickers chosen to cover the categories
Polymarket+Kalshi actually populate (political/Fed → bonds + dollar,
crypto → BTC/ETH, FX → EUR/USD + USD/JPY, geopolitical → VIX, equity
→ SPY, commodity → GLD). Expand by editing UNIVERSE.

Lifecycle:
  - On startup: backfill `backfill_days` of daily bars per ticker.
    Idempotent against pm_assets via QuestDB's commit-lag dedup
    (timestamp + ticker pair).
  - Steady state: hourly refresh of "last few days" daily bars per
    ticker, so we catch post-close updates and rerun any missed days.

yfinance is sync; we wrap the calls with asyncio.to_thread.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
from questdb.ingress import TimestampNanos

from src.config import Settings
from src.utils.db import QuestDBClient
from src.utils.redis_client import RedisClient
from src.utils.time_utils import now_utc

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class UniverseAsset:
    ticker: str          # yfinance ticker
    asset_class: str     # 'bond_etf' | 'equity_etf' | 'fx' | 'crypto' | 'commodity' | 'index'


# Macro-core universe — 10 tickers chosen to span the PM linkage
# categories we expect to test. Daily fetch is 10 calls; well under
# any yfinance soft-limit.
UNIVERSE: tuple[UniverseAsset, ...] = (
    UniverseAsset("TLT", "bond_etf"),         # 20y Treasuries — Fed/CPI linkage
    UniverseAsset("IEF", "bond_etf"),         # 7-10y Treasuries
    UniverseAsset("UUP", "fx"),               # USD index ETF
    UniverseAsset("GLD", "commodity"),        # gold
    UniverseAsset("^VIX", "index"),           # equity vol
    UniverseAsset("SPY", "equity_etf"),       # broad market
    UniverseAsset("BTC-USD", "crypto"),       # bitcoin spot
    UniverseAsset("ETH-USD", "crypto"),       # ethereum spot
    UniverseAsset("EURUSD=X", "fx"),          # EUR/USD spot
    UniverseAsset("USDJPY=X", "fx"),          # USD/JPY spot
)


def _df_to_bars(df: pd.DataFrame, ticker: str) -> list[dict[str, Any]]:
    """Convert a yfinance DataFrame to a list of bar dicts. Drops rows
    with NaN close (Yahoo occasionally returns gaps)."""
    bars: list[dict[str, Any]] = []
    if df is None or df.empty:
        return bars
    # yfinance returns a tz-aware DatetimeIndex; normalize to UTC.
    df = df.copy()
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")
    for ts, row in df.iterrows():
        close = row.get("Close")
        if pd.isna(close):
            continue
        try:
            bars.append({
                "timestamp": ts.to_pydatetime(),
                "open": float(row.get("Open") or close),
                "high": float(row.get("High") or close),
                "low": float(row.get("Low") or close),
                "close": float(close),
                "volume": float(row.get("Volume") or 0.0),
            })
        except (TypeError, ValueError):
            continue
    return bars


class MarketContextCollector:
    SOURCE = "yfinance"
    STREAM = "pm:assets"

    def __init__(
        self,
        db: QuestDBClient,
        redis_client: RedisClient,
        cfg: Settings,
    ) -> None:
        self._db = db
        self._redis = redis_client
        self._cfg = cfg
        self._backfill_days = cfg.ingestion.market_context.backfill_days
        self._poll_interval = cfg.ingestion.market_context.poll_interval_seconds
        self._stop = asyncio.Event()

    # ---- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        logger.info(
            "MarketContextCollector starting (universe=%d, backfill_days=%d, poll=%ds)",
            len(UNIVERSE),
            self._backfill_days,
            self._poll_interval,
        )
        try:
            await self._backfill()
        except Exception:
            logger.exception("Backfill failed; continuing into refresh loop")

        while not self._stop.is_set():
            try:
                await self._refresh_recent()
            except Exception:
                logger.exception("Refresh cycle failed")
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self._poll_interval
                )
            except asyncio.TimeoutError:
                continue

    async def stop(self) -> None:
        self._stop.set()

    # ---- ingest -------------------------------------------------------------

    async def _backfill(self) -> None:
        end = now_utc()
        start = end - timedelta(days=self._backfill_days)
        kept = 0
        for asset in UNIVERSE:
            try:
                bars = await self._fetch(asset, start, end, interval="1d")
            except Exception:
                logger.exception("Backfill fetch failed for %s", asset.ticker)
                continue
            self._write_bars(asset, bars)
            kept += len(bars)
            logger.info(
                "Backfilled %d daily bars for %s (%s)",
                len(bars), asset.ticker, asset.asset_class,
            )
        # Force-flush so backfill bars commit immediately (avoids the
        # sparse-table ILP eviction race we've seen elsewhere).
        self._db.flush()
        logger.info("MarketContext backfill done: %d bars across %d assets", kept, len(UNIVERSE))

    async def _refresh_recent(self) -> None:
        # Pull last 5 days to cover late-arriving end-of-day prints and
        # any weekend gaps. QuestDB dedup on (timestamp, asset_ticker)
        # makes the repeated writes harmless.
        end = now_utc()
        start = end - timedelta(days=5)
        kept = 0
        for asset in UNIVERSE:
            try:
                bars = await self._fetch(asset, start, end, interval="1d")
            except Exception:
                logger.exception("Refresh fetch failed for %s", asset.ticker)
                continue
            self._write_bars(asset, bars)
            kept += len(bars)
        self._db.flush()
        logger.debug("MarketContext refresh: %d bars across %d assets", kept, len(UNIVERSE))

    async def _fetch(
        self,
        asset: UniverseAsset,
        start: datetime,
        end: datetime,
        interval: str,
    ) -> list[dict[str, Any]]:
        # Import lazily so the rest of the platform is unaffected if
        # yfinance isn't installed in some environment.
        import yfinance as yf

        def _sync_fetch() -> pd.DataFrame:
            return yf.download(
                asset.ticker,
                start=start.date(),
                end=end.date() + timedelta(days=1),
                interval=interval,
                progress=False,
                auto_adjust=False,
                # yfinance sometimes returns a multi-index when given
                # multiple tickers; we always pass one so flatten the
                # columns afterwards if needed.
            )

        df = await asyncio.to_thread(_sync_fetch)
        # If the column header is a (field, ticker) MultiIndex (newer
        # yfinance), flatten to just the field name.
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return _df_to_bars(df, asset.ticker)

    # ---- persistence --------------------------------------------------------

    def _write_bars(
        self, asset: UniverseAsset, bars: list[dict[str, Any]]
    ) -> None:
        for b in bars:
            ts: datetime = b["timestamp"]
            self._db.write_row(
                table="pm_assets",
                symbols={
                    "asset_ticker": asset.ticker,
                    "asset_class": asset.asset_class,
                },
                columns={
                    "open": float(b["open"]),
                    "high": float(b["high"]),
                    "low": float(b["low"]),
                    "close": float(b["close"]),
                    "volume": float(b["volume"]),
                },
                at=TimestampNanos(int(ts.timestamp() * 1e9)),
            )
            # Publish each bar to Redis. Best-effort — failures are
            # logged at WARNING by the client itself and we continue.
            asyncio.create_task(
                self._redis.publish_to_stream(
                    self.STREAM,
                    {
                        "timestamp": ts.isoformat(),
                        "asset_ticker": asset.ticker,
                        "asset_class": asset.asset_class,
                        "close": b["close"],
                        "open": b["open"],
                        "high": b["high"],
                        "low": b["low"],
                        "volume": b["volume"],
                    },
                )
            )
