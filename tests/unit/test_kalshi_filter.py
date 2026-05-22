"""Regression tests for Kalshi discovery filter.

The unfiltered classifier produced 19K tracked markets on 2026-05-21,
which pushed the per-market poll cycle to ~80 minutes — useless for
minute-resolution linkage analysis. These tests pin the volume +
close-window filter that cut the live set back to a manageable size.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.ingestion.kalshi import KalshiMarket, should_keep_market


NOW = datetime(2026, 5, 21, 12, 0, tzinfo=timezone.utc)


def _market(
    volume_24h: float = 1000.0,
    close_time: datetime | None = NOW + timedelta(days=30),
    title: str = "Test market",
    lifetime_volume: float | None = None,
) -> KalshiMarket:
    return KalshiMarket(
        market_id="kalshi:TEST",
        ticker="TEST",
        title=title,
        category="fed_rate",
        bet_type="preset",
        close_time=close_time,
        volume_total_usd=lifetime_volume if lifetime_volume is not None else volume_24h * 10,
        volume_24h_usd=volume_24h,
    )


# ---- happy path ------------------------------------------------------------


def test_keeps_active_liquid_near_market():
    m = _market(volume_24h=1000.0, close_time=NOW + timedelta(days=10))
    keep, reason = should_keep_market(m, min_volume_usd=500, max_days_to_close=90, now=NOW)
    assert keep is True
    assert reason == "ok"


def test_keeps_market_with_no_close_time():
    m = _market(volume_24h=1000.0, close_time=None)
    keep, reason = should_keep_market(m, min_volume_usd=500, max_days_to_close=90, now=NOW)
    assert keep is True


# ---- drops -----------------------------------------------------------------


def test_drops_low_24h_volume():
    m = _market(volume_24h=100.0)
    keep, reason = should_keep_market(m, min_volume_usd=500, max_days_to_close=90, now=NOW)
    assert keep is False
    assert reason == "low_vol"


def test_drops_high_lifetime_but_low_24h():
    # A market with massive lifetime volume but dead today is not
    # useful for live linkage analysis — we want freshness.
    m = _market(volume_24h=100.0, lifetime_volume=1_000_000.0)
    keep, reason = should_keep_market(m, min_volume_usd=500, max_days_to_close=90, now=NOW)
    assert keep is False
    assert reason == "low_vol"


def test_drops_volume_exactly_below_threshold():
    m = _market(volume_24h=499.99)
    keep, _ = should_keep_market(m, min_volume_usd=500, max_days_to_close=90, now=NOW)
    assert keep is False


def test_keeps_volume_exactly_at_threshold():
    m = _market(volume_24h=500.0)
    keep, _ = should_keep_market(m, min_volume_usd=500, max_days_to_close=90, now=NOW)
    assert keep is True


def test_drops_far_future_market():
    # 2028 nomination markets sit flat for years — exactly what we want
    # to filter out.
    m = _market(close_time=NOW + timedelta(days=900))
    keep, reason = should_keep_market(m, min_volume_usd=500, max_days_to_close=90, now=NOW)
    assert keep is False
    assert reason == "far_close"


def test_drops_recently_expired():
    m = _market(close_time=NOW - timedelta(days=2))
    keep, reason = should_keep_market(m, min_volume_usd=500, max_days_to_close=90, now=NOW)
    assert keep is False
    assert reason == "expired"


def test_keeps_market_closing_today():
    m = _market(close_time=NOW + timedelta(hours=6))
    keep, _ = should_keep_market(m, min_volume_usd=500, max_days_to_close=90, now=NOW)
    assert keep is True


def test_keeps_market_closed_within_1d_grace():
    # Kalshi sometimes returns finalised markets via /events?status=open
    # for up to a day. We tolerate that window so the final outcome
    # bar still gets written.
    m = _market(close_time=NOW - timedelta(hours=12))
    keep, _ = should_keep_market(m, min_volume_usd=500, max_days_to_close=90, now=NOW)
    assert keep is True
