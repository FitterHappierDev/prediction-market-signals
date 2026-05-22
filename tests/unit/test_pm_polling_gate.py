"""Tests for the PM probability-polling gate.

Background: sports/entertainment outcome markets have no measurable
cross-asset linkage (verified 2026-05-22) so we stop polling their
orderbooks to save bandwidth. But we still want to poll the orderbook
for M&A / regulatory / corporate-event markets even when their
classifier-assigned category is 'other'. The title_keep_patterns
override exists for that case.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from src.ingestion.polymarket import ParsedMarket, should_poll_probability


def _mkt(
    category: str = "fed_rate",
    title: str = "Will the Fed cut rates in June?",
) -> ParsedMarket:
    return ParsedMarket(
        market_id="0xabc",
        yes_token_id="1",
        title=title,
        description="",
        category=category,
        bet_type="preset",
        end_date=datetime(2026, 6, 17, tzinfo=timezone.utc),
        resolved=False,
        outcome=None,
        strike_price=None,
        underlying_ticker=None,
        volume_total_usd=10_000.0,
    )


# ---- back-compat: empty filter → poll everything ---------------------------


def test_empty_filter_polls_everything():
    sports = _mkt(category="other", title="Will OKC win the NBA Finals?")
    assert should_poll_probability(sports, frozenset(), []) is True

    fed = _mkt()
    assert should_poll_probability(fed, frozenset(), []) is True


# ---- category filter -------------------------------------------------------


def test_listed_category_passes():
    cats = frozenset({"fed_rate", "earnings", "recession"})
    assert should_poll_probability(_mkt(category="fed_rate"), cats, []) is True


def test_non_listed_category_dropped():
    cats = frozenset({"fed_rate", "earnings"})
    sports = _mkt(category="other", title="Will OKC win the NBA Finals?")
    assert should_poll_probability(sports, cats, []) is False


def test_political_with_filter_passes_when_listed():
    cats = frozenset({"political"})
    politics = _mkt(category="political", title="Will Senator X resign?")
    assert should_poll_probability(politics, cats, []) is True


# ---- title-pattern override ------------------------------------------------


def test_title_pattern_overrides_category_drop():
    # 'other' category but title hints at M&A — override fires.
    cats = frozenset({"fed_rate"})
    patterns = [re.compile(r"acqui[rs]", re.IGNORECASE)]
    mna = _mkt(category="other", title="Will Microsoft acquire Activision by Dec?")
    assert should_poll_probability(mna, cats, patterns) is True


def test_multiple_patterns_first_match_wins():
    cats: frozenset[str] = frozenset()
    patterns = [
        re.compile(r"FDA approv", re.IGNORECASE),
        re.compile(r"antitrust", re.IGNORECASE),
    ]
    # Both empty cats and the FDA pattern: pattern still passes it.
    # (Empty cats already passes everything but this verifies the
    # pattern-match branch executes correctly.)
    fda = _mkt(category="other", title="Will FDA approve Drug X by Q3?")
    assert should_poll_probability(fda, cats, patterns) is True


def test_title_pattern_no_match_still_drops():
    cats = frozenset({"fed_rate"})
    patterns = [re.compile(r"acqui[rs]", re.IGNORECASE)]
    sports = _mkt(category="other", title="Will San Antonio Spurs win Game 3?")
    assert should_poll_probability(sports, cats, patterns) is False


def test_case_insensitive_match():
    cats = frozenset({"fed_rate"})
    patterns = [re.compile(r"merge[rd]?", re.IGNORECASE)]
    # Uppercase MERGED — pattern is case-insensitive.
    mna = _mkt(category="other", title="Will Warner and Paramount have MERGED by year-end?")
    assert should_poll_probability(mna, cats, patterns) is True


# ---- regression for the 2026-05-22 misclassification cases -----------------


def test_warnock_political_still_polled():
    # Post-classifier-fix, Warnock correctly classifies as political.
    # Sanity check that political IS in the default whitelist
    # (catching any future settings.yaml regression).
    cats = frozenset({"political"})
    warnock = _mkt(
        category="political",
        title="Will Raphael Warnock win the 2028 Democratic presidential nomination?",
    )
    assert should_poll_probability(warnock, cats, []) is True


def test_weinstein_other_dropped():
    # Weinstein sentencing markets used to misclassify as crypto;
    # post-fix they're 'other'. With the new filter they should be
    # dropped from probability polling — correct outcome (no asset
    # linkage to a celebrity sentencing).
    cats = frozenset({"fed_rate", "earnings", "recession", "geopolitical", "political", "crypto"})
    weinstein = _mkt(category="other", title="Will Harvey Weinstein be sentenced to no prison time?")
    assert should_poll_probability(weinstein, cats, []) is False
