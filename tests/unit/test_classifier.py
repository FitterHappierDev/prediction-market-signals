"""Regression tests for category classification.

The previous implementation used substring matching, which caused
systematic false-positives:
  - "war" matched names like Warnock, Stewart, Edwards
  - "eth" matched Netherlands, ethics, Bethany
  - "btc" matched any hex string fragment

These tests pin the word-boundary behaviour so the bug doesn't return.
"""

from __future__ import annotations

from src.ingestion.polymarket import classify_category


# ---- regression: previously-broken cases -----------------------------------


def test_warnock_is_political_not_geopolitical():
    assert classify_category(
        "Will Raphael Warnock win the 2028 Democratic presidential nomination?", ""
    ) == "political"


def test_stewart_is_political_not_geopolitical():
    assert classify_category(
        "Will Jon Stewart win the 2028 Democratic presidential nomination?", ""
    ) == "political"


def test_netherlands_is_other_not_crypto():
    # No category keyword present → falls through to 'other'
    assert classify_category(
        "Will Netherlands win the 2026 FIFA World Cup?", ""
    ) == "other"


def test_ethics_substring_doesnt_match_crypto():
    assert classify_category(
        "Will the Senate ethics committee vote to expel Senator X?", ""
    ) == "political"  # caught by 'senate'


def test_warden_substring_doesnt_match_geopolitical():
    assert classify_category(
        "Will Warden be appointed to the Cabinet?", ""
    ) == "other"


# ---- happy-path: real matches still work -----------------------------------


def test_real_geopolitical_match():
    assert classify_category(
        "Will Israel launch a strike on Iran this month?", ""
    ) == "geopolitical"


def test_real_crypto_match_full_word():
    assert classify_category(
        "Will Bitcoin hit $1m this year?", ""
    ) == "crypto"


def test_real_crypto_match_btc_token():
    assert classify_category(
        "Will BTC close above $200K on Friday?", ""
    ) == "crypto"


def test_real_political_match():
    assert classify_category(
        "Will the Senate pass the infrastructure bill?", ""
    ) == "political"


def test_real_fed_rate_match_multi_word():
    assert classify_category(
        "Will the Fed cut interest rate by 25 basis points in June?", ""
    ) == "fed_rate"


def test_real_earnings_match_multi_word():
    assert classify_category(
        "Will AAPL beat quarterly earnings on revenue?", ""
    ) == "earnings"


def test_real_recession_match():
    assert classify_category(
        "Will the US enter a recession by Q4?", ""
    ) == "recession"


# ---- priority ordering ------------------------------------------------------


def test_geopolitical_wins_over_political():
    # "Will the war affect the election?" — both keywords present.
    # Per CATEGORY_PRIORITY, geopolitical wins.
    assert classify_category(
        "Will the war in Ukraine affect the 2026 midterm election?", ""
    ) == "geopolitical"


def test_fed_rate_wins_over_recession():
    # Both keywords present; fed_rate is higher priority than recession.
    assert classify_category(
        "Will the Fed cut rates to avoid recession?", ""
    ) == "fed_rate"


def test_unknown_falls_through_to_other():
    assert classify_category(
        "Will the new Playboi Carti album release before GTA VI?", ""
    ) == "other"


# ---- description field is also scanned --------------------------------------


def test_description_can_trigger_match():
    assert classify_category(
        "Some market title",
        "Resolves based on the next Fed FOMC meeting decision.",
    ) == "fed_rate"
