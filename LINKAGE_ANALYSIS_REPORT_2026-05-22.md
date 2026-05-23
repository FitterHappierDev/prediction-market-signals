# In-Depth Report: Prediction-Market Linkage Analysis

**Snapshot date:** 2026-05-22 (after 13.3 days of Polymarket coverage, 5.9 days of Kalshi)
**Platform stage at time of writing:** Phase 1 (ingestion) + Phase 2.1 (wallet tracer) + Phase 3 prototype (asset ingest + Layer 1/2 linkage tests). Phase 2.2 (anomaly detector) and Phase 3-proper (full validator) not yet built.

> This document is a point-in-time snapshot. For ongoing platform state, see [`BUILD_PROGRESS.md`](BUILD_PROGRESS.md). Operational fixes landed in response to the findings here are summarized in the *Addendum* at the bottom.

---

## 1. Executive Summary

The platform's foundational bet is that **prediction-market probability dynamics contain information that arrives at traditional assets with a measurable lead-time, and that the lead can be statistically isolated and traded on**. We have now built enough infrastructure to begin testing this empirically and have run two formal tests using a 2-layer subset (cross-correlation + Granger causality) of the planned 5-layer linkage validator.

**Both tests returned the same verdict: in the PM markets we tested, the *asset* Granger-causes the prediction market — not the other way around.** The PM markets are *following* asset prices, not leading them. This is what we'd expect for "literal derivative" markets ("Will NDX close above X at 4pm?") but it is *not* the case the platform was designed to monetise.

However, this is a **selection-bias result, not a thesis refutation**. The two markets we tested were both explicit derivative-style markets — chosen because they had the cleanest paired data, not because they were the strongest candidates for the underlying thesis. The markets that *could* test the real thesis (event/sentiment markets where PM probability anticipates news that hasn't yet hit assets) exist in our data but in much smaller, harder-to-test form. One of them — a Kalshi market on Tulsi Gabbard's departure from DNI — moved from probability 0.38 → 0.94 in a single hour on May 22, the kind of event jump we want to detect. None of those event-style markets has yet been formally tested.

The data also surfaced several **operational truths** about the prediction-market landscape that affect strategy: Polymarket is dominated by sports and long-shot political nominations (no real macro coverage); Kalshi has the macro markets we need but consensus markets dominate (Fed June at 96.5% holds, no probability to correlate against); and on Polymarket, identifying genuinely informational wallets is harder than the original spec assumed because every user wallet is an EIP-1167 proxy funded by a single Polymarket relayer.

---

## 2. Data Inventory

After ~13 days of unattended ingestion:

| Table | Rows | Notes |
|---|---:|---|
| `pm_probabilities` | 2,471,326 | 1-min OHLCV-equivalent bars for both sources |
| `pm_trades` | 433,090 | Polymarket only; Kalshi trade tape silently broken (REQ-KAL-003) |
| `pm_markets` | 1,000,881 | Per-discovery-cycle upserts; ~280 LATEST-snapshot rows currently active |
| `pm_wallets` | 3 | Wallet tracer works; nothing's driving trace requests until Phase 2.2 lands |
| `pm_assets` | 350 | 10 tickers × ~33 days of daily OHLCV |
| `pm_anomaly_scores` | 0 | Phase 2.2 unbuilt |

**Coverage by source:**

| Source | Earliest | Latest | Bars | Markets ever tracked |
|---|---|---|---:|---:|
| Polymarket | 2026-05-09 16:04Z | 2026-05-22 23:02Z | 1,685,192 | 102 |
| Kalshi | 2026-05-17 02:33Z | 2026-05-22 23:02Z | 786,134 | 19,328 (post-filter: ~280 active) |

**Wallets (Polymarket trade tape):** 30,131 all-time / 29,718 in last 7 days. Almost no wallet has activity older than our coverage window — turnover is high, and the platform is bringing in new wallets continuously.

**Asset universe:** TLT, IEF, UUP, GLD, ^VIX, SPY, BTC-USD, ETH-USD, EURUSD=X, USDJPY=X — daily bars only, 30-day backfill via yfinance.

---

## 3. Market Composition

### Polymarket (102 active, post-classifier-fix)

| Category | Markets | Lifetime $vol |
|---|---:|---:|
| `other` (sports + entertainment) | **67** | $1,276M |
| `political` | 30 | $688M |
| `crypto` | 2 | $5.8M |
| `geopolitical` | 1 | $1.8M |
| `fed_rate` / `earnings` / `recession` | 0 | 0 |

The "other" category is overwhelmingly NBA Finals + FIFA World Cup + NHL Stanley Cup outcome markets, plus a long tail of entertainment ("Will GTA VI release before…", "Will Rihanna album release before…"). The 30 political markets are almost entirely 2028 Democratic-nomination horserace markets sitting at 0.7%–1.7% probability per candidate.

**Critical observation:** Polymarket has **zero markets** in the three macro categories where the platform's design assumed PM→asset linkages would be cleanest (Fed decisions, earnings, recession risk). This is a hard structural constraint on what Polymarket data alone can tell us.

**Bet-type breakdown:** 70 preset (calendar-resolution events), 29 triggered (no deadline), 1 expiring (price-threshold with deadline).

### Kalshi (~280 active, post-filter)

| Category | Active markets |
|---|---:|
| `fed_rate` (Economics fallback bucket) | 123 |
| `political` | 122 |
| `recession` | 19 |
| `earnings` | 10 |

Note: the `fed_rate` count is inflated by a known classifier bug — `derive_category_from_event` buckets *any* Kalshi Economics-category event into `fed_rate` if its series ticker doesn't match CPI/UNEMP/GDP/INFL/JOBS. So "When will SpaceX IPO?" and gas-price threshold markets land in `fed_rate`. The actual Fed-decision markets are a meaningful subset.

Kalshi is where the **real macro markets live** — CPI thresholds, FX threshold markets, China PMI, German Ifo, building permits, etc. — but the population is dominated by hourly/daily settlement contracts that resolve as soon as the corresponding data prints.

---

## 4. Trading Microstructure & Wallet Ecosystem

### Trade size distribution (Polymarket, last 72h, n=155,670)

| p10 | p50 | p90 | p99 | max | mean |
|---:|---:|---:|---:|---:|---:|
| $0.59 | $7.50 | $176 | $1,088 | **$80,060** | $79.47 |

Median trade is small ($7.50). But the right tail is extreme: 1% of trades exceed $1,088 and the single largest in the window was $80,060. **The top-10 single trades in the last 7 days are all $55K–$172K** — exactly the bet-size anomalies the unimplemented Phase 2.2 anomaly detector is designed to catch.

The largest single trade: $171,951 buy_no by wallet `0xb85a6156…` on 2026-05-18 on market `0xe20253…`. That's a wallet I have no profile on and that doesn't show up in the top-10 by 7d volume — suggesting it's either a one-shot bet or a wallet that came in for this single thesis. **This is exactly the kind of trade the platform was built to flag.**

### Direction balance (PM, 72h)

| Side | Trades | $vol |
|---|---:|---:|
| buy_no | 45,524 | **$5.42M** |
| sell_no | 41,273 | $4.21M |
| sell_yes | 44,026 | $0.98M |
| buy_yes | 24,847 | $1.77M |

Buy_no dominates dollar volume by 3:1 over buy_yes. This reflects the long-shot bias of the dominant sports/entertainment markets: when N teams compete, most outcomes are NO and the natural directional bet is "buy NO at low price."

### Wallet activity distribution (7d)

| Trade count | Wallets | Share |
|---|---:|---:|
| 1 trade | 7,944 | 27% |
| 2–10 | 17,472 | 59% |
| 11–100 | 4,102 | 14% |
| **>100** | **200** | **0.7%** |

A classic power-law / Pareto distribution. The top 200 wallets (0.7% of the population) drive most of the volume — but concentration is more moderate than expected: **top-20 wallets = 28.4% of 7d volume** ($9.77M of $34.4M total).

Top 5 wallets by 7d volume:

| Wallet | Trades | $vol 7d | Markets |
|---|---:|---:|---:|
| `0x1a96…` (traced) | 11,021 | **$1.47M** | 50 |
| `0x156b…` | 26,347 | $1.28M | 16 |
| `0xb3b3…` | 1,933 | $1.02M | 22 |
| `0x063723…` (traced) | 15,253 | $909K | 31 |
| `0x08fff5b9…` | 121 | $806K | 6 |

Two of the top-4 are wallets we've already traced. `0x08fff5b9…` is striking — only 121 trades but $806K volume = $6,660 average trade — a "few large bets" pattern very different from the high-frequency `0x156b…` (26K trades / $1.28M = $49 average — definitely a market-making or arbitrage bot).

---

## 5. Market Movement Survey — Where Probability Actually Moves

**Polymarket (102 markets, last 7d, sigma of probability):**

| Movement bucket | Markets | Share |
|---|---:|---:|
| Flat (σ < 0.01) | 83 | 81% |
| Small (0.01 ≤ σ < 0.05) | 14 | 14% |
| Moderate (0.05 ≤ σ < 0.10) | 3 | 3% |
| Large (σ ≥ 0.10) | 2 | 2% |

**Only 5 of 102 active PM markets have meaningfully moved in the last week.** The flat 83 include all 30 political 2028-nomination markets (pinned at 0.8%-1.7%) and most of the long-shot sports markets.

The 7d movers, ranked by sigma:

1. Oklahoma City Thunder NBA Finals (sigma 0.069, range 0.40→0.60)
2. San Antonio Spurs NBA Finals (0.059, range 0.22→0.40)
3. MegaETH airdrop by June 30 (0.056, range 0.16→0.33) ← only macro-ish one
4. Harvey Weinstein "no prison" sentencing (0.049, range 0.77→0.88)
5. NHL Stanley Cup teams (0.04 each)

**Kalshi (19K markets seen, last 7d):**

| Movement bucket | Markets |
|---|---:|
| Flat (σ < 0.01) | 11,881 |
| Small | 4,753 |
| Moderate | 1,267 |
| **Large (σ ≥ 0.10)** | **1,347** |

Kalshi has 1,347 markets with large probability movement in the last 7d — many more candidates than Polymarket. But most of these are **resolving point-in-time markets**: their 0.01 → 0.99 sweep is just the market collapsing to certainty as the data prints arrive, not informational dynamics over time.

The Kalshi macro markets with real movement in `fed_rate`/`recession`/`earnings`:

| Title | bars | σ |
|---|---:|---:|
| UST Par Yield Curve (30Y) Q2 above 5% | 3 | 0.56 |
| Truflation Deadlift Index thresholds | 2 | 0.49 |
| EUR/USD below 1.15 at 10am EDT (May 20) | 8 | 0.48 |
| USD/JPY below 157.5 at 10am EDT (May 20) | 8 | 0.47 |
| Truflation CPI YoY above 2.07% (May 20) | 4 | 0.47 |
| US building permits April > 1.5M | 33 | 0.43 |
| Germany unemployment rate May 2026 | 361 | 0.41 |
| China NBS PMI May > 47.0 | 33 | 0.41 |

Most have only a handful of bars (2-8) because they resolved quickly in our window. **Germany unemployment** (361 bars over 7d) and **China PMI** (33 bars, week-of-print) are the cleanest "many bars + macro release" pairs.

### One striking event move

**Kalshi: "Will Tulsi Gabbard leaves DNI before Aug 1, 2026?"** — hourly trajectory on 2026-05-22:

| Hour (UTC) | Probability |
|---|---:|
| 14:00 | 0.259 |
| 15:00 | 0.391 |
| 16:00 | 0.381 |
| **17:00** | **0.939** ← +56pp in one hour |
| 18:00 | 0.987 |
| 19:00 | 0.989 |
| 20:00 | 0.992 |
| 22:00 | 0.988 |
| 23:00 | 0.992 |

This is the *exact* event-driven probability move the platform was designed to capture: news breaks (likely an announcement of her departure), probability jumps from ~38% to ~94% in 60 minutes, settles at near-certainty. **If this kind of event correlates with a tradeable asset move (DXY? defense ETF? volatility?), that's the linkage edge the thesis predicts.** It went untested because we don't yet have a screener identifying these jumps in real time.

---

## 6. The Original Linkage Hypothesis

From [`PM_Platform_Technical_Design.md`](PM_Platform_Technical_Design.md) §3.8, the platform's core operating model is:

> Validated PM-asset linkages with decay profiles. When a PM market's probability moves, the linked asset's price moves directionally and predictably within a measurable lag window (typically minutes-to-hours).

The thesis has **three implicit sub-claims** that need to hold for the platform to generate alpha:

1. **PM markets contain information not yet in asset prices** — a real informational lead.
2. **That lead is statistically detectable** via cross-correlation, Granger causality, transfer entropy, event studies, mutual information.
3. **The lead is large enough to trade through** — i.e., greater than the latency between detection and order fill, after costs.

The mechanism for (1) is usually one of:
- **Insider knowledge**: someone with private info trades the PM market because it has fewer eyes than the asset market.
- **Concentrated belief**: PM aggregates many informed retail/expert opinions earlier than passive asset markets do.
- **Event-window asymmetry**: PM trades on news interpretation faster than asset markets that depend on broader liquidity.

For each of these mechanisms to work, the PM market must NOT be a derivative of the asset. If "Will SPX close above X at 4pm?" is the PM market and SPX is the asset, the PM is just a synthetic option — its probability *mechanically* tracks spot, with no informational lead.

---

## 7. Typology: Three Kinds of PM Markets

Working through our data, every PM market falls into one of three categories — and they have very different linkage profiles:

### Type A: Derivative markets

The PM is a synthetic option / forward on a tradeable asset. Probability is mathematically a function of current spot, time-to-expiry, and volatility expectations.

**Examples in our data:**
- "Will Nasdaq-100 close above $29,599.99 at 4pm EDT May 22?" — derivative of NDX
- "Will EUR/USD end Q2 2026 above 1.20?" — derivative of EUR/USD spot
- "Will EUR/USD open below 1.15 at 10am EDT May 20?" — derivative of EUR/USD spot
- "Will UST 30Y yield curve be above 5% Q2 2026?" — derivative of bond market

**Expected linkage profile:** very high correlation (~1.0 for short-expiry, ~0.4-0.7 for longer-dated). Granger should show asset→PM, not PM→asset. **No tradeable alpha** unless the PM is mispriced (in which case the trade is on the PM itself, not the asset).

### Type B: Event markets

The PM is a probability of a discrete, public event whose occurrence will move the asset.

**Examples in our data:**
- "Will Tulsi Gabbard leave DNI before Aug 1, 2026?" — could affect defense/intel-adjacent stocks, DXY on political-risk metric
- "Will China invade Taiwan before GTA VI?" — TWN, semiconductors, defense, oil
- "Will the Federal Reserve cut rates 25bps at June 2026 meeting?" — TLT, DXY, equity broadly
- "Will US building permits April > 1.5M?" — homebuilder stocks (XHB), 10-year yield
- "Will MegaETH perform an airdrop by June 30?" — could affect ETH ecosystem

**Expected linkage profile:** asymmetric. Before an event resolves, PM probability and asset can correlate weakly through shared sentiment. **After the event resolves, the asset moves on the realised outcome — and PM probability had a 0/1 final value before the asset closed**. If we can detect the PM probability jump *before* the asset reacts, that's alpha.

The Gabbard 38→94 jump is the textbook example. The question we haven't answered yet: did any tradeable asset (defense ETF? VIX? specific equity?) move correlatedly?

### Type C: Sentiment / consensus markets

The PM aggregates beliefs about a non-events-driven state of the world — its probability moves only when consensus shifts.

**Examples in our data:**
- "Will the US enter recession in 2026?" (we don't have this but it's typical)
- 2028 Democratic nomination horserace markets (sentiment about candidate strength)
- "Will Bitcoin hit $1m before GTA VI?" (long-tail crypto sentiment)

**Expected linkage profile:** noisy, low signal. Probability moves slowly with macro narrative; asset moves on many other things. **Marginally useful** for slow-moving thematic linkages, but not for high-frequency alpha.

---

## 8. Tests Conducted

**Method:**
1. **Layer 1 (cross-correlation):** Pearson r between PM probability and asset close, at lags -L to +L. Reports peak |r|, optimal lag, Bonferroni-corrected p-value.
2. **Layer 2 (Granger causality):** F-test on first-differenced series, both directions (PM→asset and asset→PM). Reports best lag, F-stat, p-value per direction.

Both implemented in [`scripts/run_linkage_xcorr.py`](scripts/run_linkage_xcorr.py).

### Test 1: Nasdaq-100 PM market vs ^NDX spot (Type A — derivative)

| Setup | Value |
|---|---|
| PM market | `kalshi:KXNASDAQ100U-26MAY22H1600-T29599.99` ("NDX > $29,599.99 at 4pm EDT May 22") |
| Asset | `^NDX` |
| Resolution | 1-min, intraday from yfinance |
| Overlap | 372 minute-bars (PM: 13:48Z–19:59Z May 22) |
| PM range | 0.010 → 0.705 (σ=0.179) |
| NDX range | 29,432 → 29,654 (σ=49.6) |

| Layer | Result |
|---|---|
| Layer 1 peak r | **+0.883 at lag +1 min** |
| p (Bonferroni) | < 10⁻¹²⁰ |
| Top 5 lags | -1, 0, +1, +2, +3 min — all >0.87 (effectively contemporaneous) |
| Layer 2 PM → NDX | F=1.91, p=**0.092** (not significant) |
| Layer 2 NDX → PM | F=18.18, **p<10⁻¹⁵** |
| Verdict | **Asset Granger-causes PM** — PM is mechanically reflecting NDX |

### Test 2: EUR/USD Q2 PM market vs EURUSD=X spot (Type A — derivative)

| Setup | Value |
|---|---|
| PM market | `kalshi:KXEURUSDQ-26JUN3010-T1.20` ("EUR/USD ≥ 1.20 at Q2 end") |
| Asset | `EURUSD=X` |
| Resolution | 1-min, intraday from yfinance |
| Overlap | 6,575 minute-bars |
| PM range | 0.085 → 0.470 (σ=0.064) |
| EUR/USD range | 1.1582 → 1.1665 (σ=0.0018) |

| Layer | Result |
|---|---|
| Layer 1 peak r | **+0.375 at lag +528 min (~9h)** |
| p (Bonferroni) | < 10⁻²¹⁵ |
| Top 5 lags | +527 to +531 min — sharp peak, 4-min spread |
| Layer 2 PM → EUR/USD | F=2.09, p=**0.079** (not significant) |
| Layer 2 EUR/USD → PM | F=1.93, **p=0.005** |
| Verdict | **Asset Granger-causes PM** — same as Test 1 |

---

## 9. The Headline Finding: PM Follows Asset

Both Type A tests gave the same verdict: **asset Granger-causes PM, not the reverse**. The PM markets we tested are *reactive* to spot, not predictive.

Interpretation by linkage strength:

- **NDX test (r≈0.88):** A 4pm-settlement market on the same trading day. PM probability is essentially a delta-and-time-decay function of current NDX. The +1 minute lag is just polling latency in our collector (Kalshi orderbook poll cycle is 15-26s). No surprise here — this is exactly what a synthetic option should look like, and exactly why we labeled it a "validation" test of the pipeline.

- **EUR/USD test (r≈0.375):** A Q2-end (June 30) settlement market. Six weeks out. The probability of "EUR/USD ≥ 1.20 by June 30" is a weak function of *current* spot (current spot is 1.166, target is 1.20 — far away) plus a strong function of *expected drift and volatility*. The +9-hour lag is interesting: probably reflects that the PM market sees most trade flow in US daytime, then EUR/USD moves on Asia/London opens 9 hours later. **This is a clock-time artifact, not informational lead.**

Both findings are **fully consistent with the platform's thesis** — the thesis was never "every PM market leads its asset"; it was "*some* PM markets — specifically event markets — have informational lead." We just haven't tested those yet.

---

## 10. What This Means for the Platform Thesis

**What the tests positively show:**
- The infrastructure works end-to-end (PM minute-bars + asset bars + correlation + Granger → tractable interpretable result).
- Layer 1 (cross-correlation) and Layer 2 (Granger causality) are sufficient to distinguish "follower" from "leader" PM markets.
- Derivative-type PM markets in our data are followers, as expected. This is a useful *negative* result — we now know not to waste analysis on these.

**What the tests negatively show:**
- We have not yet found a PM→asset linkage with informational lead. The two we tested were the wrong type.
- The Polymarket categories where the original thesis is most plausible (geopolitical, fed_rate, recession) have catastrophically thin coverage — 1, 0, and 0 markets respectively. The thesis cannot be tested on Polymarket alone.

**What the data suggests but we haven't formalised:**
- Kalshi event markets (Gabbard, building permits, China PMI, German Ifo) DO have real probability dynamics that resemble information arrival. These are untested.
- The 7d top-10 single trades on Polymarket are all $55K-$172K bets. The wallets making them are mostly off our radar (only 2 of the top-5 are in our wallet trace cache). The anomaly detector — when built — would flag these.
- High wallet turnover (30K wallets in 7d, only 0.7% with >100 trades) suggests Polymarket is **dominated by short-fuse retail flow**, not patient informational traders. The "wisdom-of-crowds" mechanism may not apply.

---

## 11. Untested Candidates with Real Potential

Markets I would test next, ranked by expected signal-to-noise:

### Tier 1 — Highest priority

| PM market | Asset candidate | Mechanism |
|---|---|---|
| Kalshi `KXGABBARDOUT-26-AUG01` (Gabbard DNI departure) | ITA (defense), DXY, ^VIX, specific defense contractors | Political-risk shock, intel-community continuity |
| Kalshi `KXBLDGPERM-26APR-T1500` (US building permits April >1.5M) | XHB (homebuilders), TLT, 10-year yield | Macro release, clear documented bond reaction |
| Kalshi `KXNBSPMI-26MAY-T47` (China NBS PMI May >47.0) | FXI/MCHI, copper (HG=F), AUD/USD | Real China activity proxy |
| Kalshi `KXDEIFO-26MAY22-T84.0` (Germany Ifo >84.0) | EURUSD=X, DAX/EU equities | European macro release |

### Tier 2 — Worth running once data accumulates

| PM market | Asset candidate | Mechanism |
|---|---|---|
| Kalshi recession-flagged markets (when they next resolve) | TLT, GLD, ^VIX | Cyclical risk-off |
| Kalshi UST yield curve markets | TLT, IEF, TIP | Direct rate expectations |
| Polymarket "Will China invade Taiwan…" (when volume picks up) | TWN, semis (SOXX), defense | Geopolitical tail risk |
| Polymarket MegaETH airdrop | ETH-USD, broader crypto | Crypto narrative |

### Tier 3 — Event-driven candidates we'd need to instrument

- Tulsi Gabbard 38→94 jump on May 22: did defense ETFs or specific contractors move in the same hour?
- Top-10 large-single-trade events: what markets, what direction, did paired assets respond?

---

## 12. Gaps That Block Better Tests

| Gap | Impact | Cost to close |
|---|---|---|
| **No PM markets resolved yet in our window** | Can't build training labels for Phase 3 ML | Wait — sports markets resolve through May/June |
| **No anomaly detector (Phase 2.2)** | Top-10 large trades aren't flagged in real time; no wallet-driven signal pipeline | ~1 session; tech spec is ready |
| **Asset data is daily-only, 30-day window** | Can't run minute-resolution event studies on past releases | Backfill 1-2 years of daily + 7-day rolling 1-min from yfinance |
| **Polymarket macro coverage is 0** | Can't test the dominant thesis on Polymarket | Structural — depends on Polymarket platform listing more macro markets |
| **Kalshi trade tape never written** | No anomaly signal from Kalshi; no wallet info either way | ~1 hour debugging; documented as P1 |
| **Kalshi consensus pinning** | The most-traded Kalshi macro markets (Fed) sit at consensus, no σ to correlate | Wait for surprise (Fed/CPI day); or scan for markets that moved AWAY from consensus |
| **No event-driven screener** | Gabbard-style jumps go unanalysed | ~1 session; SQL + simple thresholding |
| **Layer 3-5 of the validator unbuilt** (transfer entropy, event study, mutual info, decay-curve fit) | Granger is suggestive but not sufficient for high-stakes decisions | ~2 sessions |

---

## 13. Recommended Path Forward

In rough order of impact:

1. **Build the event-market screener** (~1 session). SQL query: every market with rolling-1h σ(probability) ≥ 0.05 AND a probability jump ≥ 30pp in any 60-min window. Output is a continually-updated short-list of "things just happened" candidates that a human can match against news + assets.

2. **Test the Gabbard event retrospectively** (~30 min). Pull defense ETFs (ITA, PPA) intraday for May 22; look at the 16:00–18:00 UTC window vs the Gabbard probability jump. This either validates or rules out the political-news → defense-asset linkage in one test.

3. **Backfill 1-year asset history** (~1 session). yfinance daily for 365 days lets us run Layer 4 event studies around past CPI/FOMC/jobs releases (which are visible in Kalshi history once we have it).

4. **Build Phase 2.2 anomaly detector** (~2 sessions). Then top-10 large trades get scored in real time. The platform finally has the "interesting trade just happened" signal it was designed around.

5. **Fix Kalshi trade tape** (~1 hour). Don't know what we don't know about Kalshi trader flow until this is on.

6. **Add Layer 3-5 to the validator** (~2 sessions). Transfer entropy distinguishes linear from non-linear dependence; event study measures abnormal returns in a window; mutual information catches non-linear linkages Granger would miss; decay-curve fit tells us the optimal entry window.

**What I would NOT do next:**
- Add more universe assets — 10 is plenty until we know which ones the existing markets actually link to.
- Try to refute / "rescue" the original thesis with more derivative-market tests. We know what those look like. The thesis lives or dies on event markets.
- Get distracted by the wallet anomaly detector before the event-screener exists. The detector's outputs need a consumer; the event-screener gives us the testbed.

---

**TL;DR:** The infrastructure works. Two formal tests run cleanly. Both tested the wrong kind of market — derivatives, which mechanically follow assets. The right kind of market (event-driven) exists in our data, has dramatic probability moves (Gabbard 38→94 in 1h), and is one screener away from being testable. Build the event-market screener next; it's the unlock.

---

# Addendum — what happened in response (2026-05-22 → 2026-05-23)

Three of the gaps identified above were closed in the same working session this report was written in. Documenting here so the report stands as a complete record.

### A1. Strategic re-orientation: three parallel tracks, not one

After the report was reviewed, the platform shifted from "single linkage thesis" to three parallel tracks (full rationale in [`BUILD_PROGRESS.md`](BUILD_PROGRESS.md) §"Strategy pivot"):

- **Track A**: continue the original cross-asset linkage work, but passively — build the event-market screener as a cheap experiment generator.
- **Track B**: wallet copy-trading on PM directly. Sports markets become the lab bench (fastest cycle times, most resolved markets, cleanest insider signals like injuries/lineups) — we don't trade sports, we train the wallet anomaly detector there and then deploy on macro/event markets.
- **Track C**: Kalshi mispricing arb on derivative markets via the Phase 3 Time Adjuster Mode B (REQ-TAJ-002). Trade Kalshi, not the asset, when asset moves and Kalshi probability hasn't caught up.

### A2. Polymarket probability-poll bandwidth gate (closes "Polymarket macro coverage is 0" partially)

The PM collector now gates the expensive per-market orderbook poll behind a category whitelist (`fed_rate`, `earnings`, `recession`, `geopolitical`, `political`, `crypto`) plus regex title overrides for M&A / regulatory / IPO event markets. Trade-tape ingestion continues across **all** active PM markets (preserves wallet activity context for Track B).

**Measured effect:** PM probability-bar writes dropped from ~8,400/hour to ~1,800/hour (**79% reduction**) within 30 minutes of the deploy. Sports/entertainment markets stopped receiving probability bars entirely; trade-tape ingestion uninterrupted.

Settings live in `config/settings.yaml: ingestion.polymarket.probability_polling_categories` and `…title_keep_patterns`. Unit tests in [`tests/unit/test_pm_polling_gate.py`](tests/unit/test_pm_polling_gate.py).

### A3. Kalshi trade-tape silent failure fixed (closes "Kalshi trade tape never written")

Root cause: family of field-name mismatches against Kalshi's actual `/markets/trades` schema. We were reading nonexistent `count`, `yes_price`, `no_price` fields (silently returning 0 for everything), ignoring `taker_book_side` (couldn't distinguish buys from sells), and only handling the older `taker_side` field name.

Fix correctly reads `count_fp`, `yes_price_dollars`, `no_price_dollars`, `taker_outcome_side`, `taker_book_side` with backwards-compat fall-back to the older names.

**Measured effect:** 0 ever → **19,857 Kalshi trades across 253 markets in the first 30 minutes** post-deploy (~660 trades/min). Track B and Track C both unblocked on the Kalshi side.

### A4. Operational follow-ups still open

- `pm_market_stale` log noise from Truflation `-26MAY22` markets sitting in KalshiCollector's in-memory `_active` set after their close_time has passed. Discovery filter catches them on next cycle but the in-memory set doesn't get pruned. ~30 min fix.
- Second-tier Kalshi filter for short-fuse high-velocity macro markets currently excluded for <$500/24h volume (these are individually small but cleanest informationally). ~30 min.
- Wallet performance tracker (Track B foundation): per-wallet rolling trade count, distinct markets, total volume, win rate. ~1 session.
- Phase 3 Time Adjuster Mode B (Track C foundation): theoretical-probability calculator + `information_premium` surface. ~1 session.
