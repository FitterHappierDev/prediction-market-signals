"""Minute-resolution time helpers. All UTC internally.

The platform stores every timestamp as UTC microseconds (QuestDB default)
and expresses every duration as minutes (float). Boundaries with the
outside world (market hours, ISO 8601 strings) convert at the edge.
"""

from __future__ import annotations

from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

NY_TZ = ZoneInfo("America/New_York")

EQUITY_MARKET_OPEN = time(9, 30)
EQUITY_MARKET_CLOSE = time(16, 0)

# Minutes per calendar/trading year by asset class. Used to convert
# "minutes remaining" into the T input for barrier-option pricing.
TRADING_MINUTES_PER_YEAR: dict[str, float] = {
    "equity": 252 * 6.5 * 60,
    "crypto": 365 * 24 * 60,
}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def minutes_between(start: datetime, end: datetime) -> float:
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("Both datetimes must be timezone-aware (UTC)")
    return (end - start).total_seconds() / 60.0


def minute_floor(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def is_market_open(asset_class: str, dt: datetime | None = None) -> bool:
    if asset_class == "crypto":
        return True
    if dt is None:
        dt = now_utc()
    if dt.tzinfo is None:
        raise ValueError("dt must be timezone-aware")
    local = dt.astimezone(NY_TZ)
    if local.weekday() >= 5:
        return False
    t = local.time()
    return EQUITY_MARKET_OPEN <= t <= EQUITY_MARKET_CLOSE


def trading_minutes_to_years(minutes: float, asset_class: str) -> float:
    if asset_class not in TRADING_MINUTES_PER_YEAR:
        raise ValueError(f"Unknown asset_class: {asset_class!r}")
    return minutes / TRADING_MINUTES_PER_YEAR[asset_class]
