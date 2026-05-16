# Prediction Market Signal Intelligence Platform — Technical Design Document

**Version:** 1.0  
**Date:** April 23, 2026  
**Status:** Draft — For Review and Build  

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture Diagram](#2-architecture-diagram)
3. [Component Specifications](#3-component-specifications)
4. [Data Models and Schemas](#4-data-models-and-schemas)
5. [Message Contracts](#5-message-contracts)
6. [API Specifications](#6-api-specifications)
7. [Algorithmic Specifications](#7-algorithmic-specifications)
8. [Infrastructure and Deployment](#8-infrastructure-and-deployment)
9. [Configuration and Feature Flags](#9-configuration-and-feature-flags)
10. [Monitoring and Observability](#10-monitoring-and-observability)
11. [Testing Strategy](#11-testing-strategy)

---

## 1. System Overview

### 1.1 Purpose

Ingest prediction market data in real time, detect anomalous insider-informed bets, statistically validate linkages to traditional assets, and execute trades on linked assets through a two-stage stacked model. The system does NOT trade prediction markets directly.

### 1.2 Design Principles

- **Minute-resolution throughout.** All time variables are stored and computed in minutes. No integer-hour approximations.
- **Backtest-live parity.** Every component that touches live data must accept historical data through the same interface. No separate backtest code path.
- **Fail-safe by default.** Every component has a defined failure mode that reduces exposure, never increases it.
- **Append-only data.** Raw ingested data is never modified or deleted. Derived features and model outputs are versioned.
- **Explicit over implicit.** No magic numbers. All thresholds, windows, and parameters are defined in a central config with documented rationale.

### 1.3 End-to-End Latency Budget

| Segment | Target | Max Acceptable |
|---|---|---|
| PM API → Signal Detector | 1,000 ms | 5,000 ms |
| Signal Detector → Stage 1 | 500 ms | 2,000 ms |
| Stage 1 → Linkage Lookup | 50 ms | 200 ms |
| Linkage Lookup → Stage 2 | 100 ms | 500 ms |
| Stage 2 → Risk Engine | 50 ms | 200 ms |
| Risk Engine → Broker API | 200 ms | 1,000 ms |
| Broker API → Fill Confirmation | 500 ms | 5,000 ms |
| **Total** | **~2,400 ms** | **~14,000 ms** |

Target: signal detection to order submission in under 5 seconds. Fill within 15 seconds. Well within the 5-minute minimum viable window for the fastest signal category (geopolitical).

---

## 2. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                              │
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │ Polymarket   │  │ Kalshi       │  │ Polygon RPC  │              │
│  │ Collector    │  │ Collector    │  │ Wallet Tracer│              │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘              │
│         │                 │                 │                       │
│         └────────┬────────┴────────┬────────┘                      │
│                  │                 │                                 │
│                  ▼                 ▼                                 │
│         ┌──────────────┐  ┌──────────────┐                         │
│         │ Redis Streams │  │   QuestDB    │                         │
│         │ (real-time)   │  │ (persistent) │                         │
│         └──────┬────────┘  └──────────────┘                         │
└────────────────┼────────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      DETECTION LAYER                                │
│                                                                     │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │
│  │ Anomaly Detector │  │ Time Adjuster    │  │ Signal Detector  │  │
│  │ (5-signal insider│  │ (bet-type-aware  │  │ (velocity, vol,  │  │
│  │  composite score)│  │  feature engine) │  │  cross-platform) │  │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘  │
│           └──────────┬──────────┴──────────┬──────────┘             │
│                      ▼                     ▼                        │
│              ┌──────────────┐     ┌──────────────────┐             │
│              │ Stage 1      │     │ Feature Store    │             │
│              │ Classifier   │     │ (Parquet + Redis)│             │
│              └──────┬───────┘     └──────────────────┘             │
└─────────────────────┼──────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      DECISION LAYER                                 │
│                                                                     │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │
│  │ Linkage Registry │  │ Stage 2          │  │ Risk Engine      │  │
│  │ (validated pairs │  │ Meta-Learner     │  │ (sizing, gates,  │  │
│  │  + decay profile)│  │ (trade decision) │  │  circuit breaker)│  │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘  │
│           │                     │                     │             │
│           └──────────┬──────────┴──────────┬──────────┘             │
│                      ▼                                              │
│              ┌──────────────┐                                       │
│              │ Order Router │                                       │
│              └──────┬───────┘                                       │
└─────────────────────┼──────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      EXECUTION LAYER                                │
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │ Alpaca       │  │ IBKR         │  │ CCXT         │              │
│  │ (equities +  │  │ (options +   │  │ (crypto      │              │
│  │  paper)      │  │  portfolio   │  │  spot/perps) │              │
│  │              │  │  margin)     │  │              │              │
│  └──────────────┘  └──────────────┘  └──────────────┘              │
└─────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      OBSERVABILITY LAYER                            │
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │ MLflow       │  │ Grafana      │  │ Health       │              │
│  │ (experiments │  │ (live PnL +  │  │ Monitor      │              │
│  │  + models)   │  │  dashboard)  │  │ (degradation │              │
│  │              │  │              │  │  detection)  │              │
│  └──────────────┘  └──────────────┘  └──────────────┘              │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Component Specifications

### 3.1 Polymarket Collector

**Purpose:** Ingest real-time market data, trades, and order book snapshots from Polymarket.

**Inputs:**
- Polymarket REST API (markets list, trade history, order book)
- Polymarket CLOB API (WebSocket for real-time trades if available; otherwise poll REST at 15s intervals)

**Outputs (to Redis Streams):**
- `pm:markets` — market metadata updates (new markets, resolution status)
- `pm:trades` — individual trade events
- `pm:probabilities` — 1-minute probability snapshots per market

**Outputs (to QuestDB):**
- `pm_probabilities` table — append-only 1-minute probability time series
- `pm_trades` table — append-only trade log
- `pm_markets` table — market metadata with resolution status

**Behavior:**
- On startup: backfill last 7 days of probability history for all active markets
- Steady state: poll every 15 seconds for trade updates; compute 1-minute probability bars; publish to Redis and write to QuestDB
- On market resolution: write resolution event to `pm_markets`; trigger downstream feature generation for that market
- On API failure: exponential backoff (1s, 2s, 4s, 8s, 16s, cap at 60s); log warning after 3 consecutive failures; emit health metric

**Rate Limits:**
- Polymarket REST: no published rate limit, but be conservative — max 4 requests/second
- Target data freshness: probability values no more than 20 seconds stale

**Key Implementation Detail:**
- Market IDs are `condition_id` (bytes32 hex string). Trade resolution uses UMA oracle. Track the `resolved` and `outcome` fields on market metadata.
- Probabilities are derived from CLOB midpoint: `prob = best_bid + (best_ask - best_bid) / 2` on the YES token. If no order book, use last trade price.


### 3.2 Kalshi Collector

**Purpose:** Ingest real-time market data from Kalshi for Fed, earnings, macro, and event markets.

**Inputs:**
- Kalshi public REST API (markets, order book, trades)
- Kalshi WebSocket feed (real-time trade stream, authenticated)

**Outputs:** Same stream/table structure as Polymarket Collector with `source = 'kalshi'` tag.

**Behavior:**
- Identical lifecycle to Polymarket Collector
- Authentication: API key required for WebSocket and some REST endpoints. Store in environment variable `KALSHI_API_KEY`.
- Kalshi uses `ticker` as market identifier (e.g., `FED-26APR-T3.375`). Map to a normalized `market_id` format shared with Polymarket.

**Key Implementation Detail:**
- Kalshi probabilities are derived from `yes_bid` / `yes_ask` fields (already in 0-100 cent format). Convert to 0-1 float.
- Kalshi has formal rate limits: 10 requests/second for public, 100/second for authenticated. Respect `X-RateLimit-Remaining` header.


### 3.3 Polygon Wallet Tracer

**Purpose:** Trace on-chain wallet metadata for Polymarket wallets that appear in anomaly detection.

**Inputs:**
- Wallet addresses flagged by Anomaly Detector (via Redis Stream `wallets:trace_request`)
- Polygon RPC endpoint (Alchemy, Infura, or public)

**Outputs (to QuestDB):**
- `pm_wallets` table — wallet age, funding sources, transaction count, first seen timestamp

**Behavior:**
- On-demand: only traces wallets when requested by Anomaly Detector (not all wallets)
- For each wallet: query first transaction timestamp (wallet age), trace USDC funding path back 2 hops (identify funder wallet), count total transactions, count distinct Polymarket markets participated in
- Cache results: wallet metadata changes slowly — re-trace at most once per 24 hours per wallet
- Batch requests to avoid RPC rate limits: max 5 concurrent RPC calls

**Key Implementation Detail:**
- Polymarket uses Polygon (PoS) USDC. The USDC contract address is `0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174`. Trace `Transfer` events to/from the wallet to identify funding sources.
- Use `eth_getBlockByNumber` with the wallet's first transaction to determine wallet creation date.


### 3.4 Anomaly Detector

**Purpose:** Score every active wallet-market pair for insider trading characteristics using the 5-signal composite methodology.

**Inputs:**
- `pm:trades` stream (real-time trade events)
- `pm_wallets` table (wallet metadata from Wallet Tracer)
- `pm_probabilities` table (for pre-resolution timing calculation)

**Outputs (to Redis Streams):**
- `anomaly:scores` — per-trade anomaly scores with breakdown by signal component
- `anomaly:alerts` — high-suspicion alerts (score > 4.5) with full context

**Outputs (to QuestDB):**
- `pm_anomaly_scores` table — append-only anomaly score history

**Parameters (from config):**

| Parameter | Default | Description |
|---|---|---|
| `WALLET_AGE_THRESHOLD_DAYS` | 7 | Wallets younger than this scored as fresh |
| `WALLET_MARKET_COUNT_THRESHOLD` | 5 | Wallets with fewer markets scored as low-activity |
| `WIN_RATE_PVALUE_THRESHOLD` | 0.001 | P-value below this = statistically improbable |
| `BET_SIZE_ZSCORE_THRESHOLD` | 3.0 | Bet size z-score above this = anomalous |
| `PRE_RESOLUTION_WINDOW_MINUTES` | 10 | Trades within this window of resolution flagged |
| `SYBIL_HOP_DEPTH` | 2 | USDC funding source trace depth |
| `ALERT_THRESHOLD_WATCH` | 4.5 | Composite score for WATCH alert |
| `ALERT_THRESHOLD_CRITICAL` | 7.0 | Composite score for CRITICAL alert |

**Composite Score Weights:**

| Signal | Weight | Score Range |
|---|---|---|
| Wallet Freshness + Concentration | 0.15 | 0–2.0 |
| Statistical Improbability (p-value) | 0.25 | 0–2.5 |
| Pre-Resolution Timing | 0.25 | 0–2.5 |
| Bet Size Anomaly | 0.15 | 0–1.5 |
| Sybil / Wallet Clustering | 0.20 | 0–2.0 |
| **Total** | **1.00** | **0–10.0** |

**Behavior:**
- Process every trade event from `pm:trades`
- For each trade: look up wallet metadata (request trace if not cached), compute each signal component, compute composite score
- If score > `ALERT_THRESHOLD_WATCH`: publish to `anomaly:alerts`
- If score > `ALERT_THRESHOLD_CRITICAL`: publish to `anomaly:alerts` with priority=CRITICAL and trigger Slack/Telegram notification
- Maintain a rolling window of per-wallet aggregate statistics (win rate, market count, total volume) in Redis hash `wallet:{address}`


### 3.5 Time Adjuster

**Purpose:** Transform raw probability changes into time-adjusted, bet-type-aware features that separate genuine information from mechanical time decay and noise.

**Inputs:**
- `pm:probabilities` stream (1-minute probability bars)
- `pm_markets` table (market metadata including bet type, resolution time, strike price if applicable)
- External price feeds for underlying assets (for expiring contract theoretical probability computation)

**Outputs (to Feature Store):**
- Time-adjusted feature vector per market per minute (written to Redis hash and Parquet)

**Three Processing Modes (by bet type):**

#### Mode A: Preset Endpoint
- **Examples:** "Will AAPL beat earnings Thursday?"
- **Baseline drift:** ~0 (probability doesn't mechanically drift)
- **Noise model:** Historical probability volatility surface indexed by `(category, minutes_remaining_bucket)`. Buckets: 0-5, 5-15, 15-30, 30-60, 60-240, 240-1440, 1440+
- **Output features:**
  - `noise_adjusted_signal` = prob_change_15m / vol_surface[category][minutes_remaining_bucket]
  - `time_confidence` = lookup from time_fraction_remaining (last 10% → 1.0, first 30% → 0.3)
  - `information_premium` = 0 (not applicable)
  - `excess_vs_time_decay` = prob_change_15m (no time decay to subtract)

#### Mode B: Expiring Before Outcome
- **Examples:** "Will BTC hit $150K by June 30?"
- **Requires:** current underlying price, strike price, implied volatility
- **Theoretical probability:** Barrier option approximation via Black-Scholes digital call: `P_theoretical = N(d2)` where `d2 = (ln(S/K) + (r - 0.5σ²)T) / (σ√T)`
- **Output features:**
  - `information_premium` = market_prob - theoretical_prob
  - `noise_adjusted_signal` = premium_change_15m / historical_premium_volatility
  - `time_confidence` = time_signal_quality × (1 - time_execution_risk)
  - `excess_vs_time_decay` = market_prob_change - theoretical_prob_change

#### Mode C: Outcome-Triggered (No Expiry)
- **Examples:** "Will there be a US recession?"
- **Baseline drift:** Empirically estimated from resolved markets in same category (median probability slope per minute)
- **Output features:**
  - `excess_vs_time_decay` = prob_change_15m - (empirical_drift_rate × 15)
  - `noise_adjusted_signal` = excess_vs_time_decay / noise_threshold
  - `time_confidence` = confidence_discount(0.7) × maturity_factor(age-dependent)
  - `information_premium` = 0 (not applicable)

**Volatility Surface Construction (built during Phase 3):**
- For each market category, collect all resolved markets' probability histories
- Bucket by minutes_remaining: compute the standard deviation of 15-minute probability changes within each bucket
- Store as a lookup table in config; update monthly during self-training loop

**Parameters:**

| Parameter | Default | Description |
|---|---|---|
| `PRESET_LATE_STAGE_THRESHOLD` | 0.10 | Time fraction below which = late stage (highest confidence) |
| `PRESET_EARLY_STAGE_THRESHOLD` | 0.70 | Time fraction above which = early stage (lowest confidence) |
| `EXPIRING_RISK_FREE_RATE` | 0.045 | Annual risk-free rate for barrier option model |
| `TRIGGERED_CONFIDENCE_DISCOUNT` | 0.70 | Categorical discount for no-expiry markets |
| `TRIGGERED_MATURITY_THRESHOLD_MINUTES` | 43200 | 30 days — below this, maturity factor < 1.0 |


### 3.6 Signal Detector

**Purpose:** Identify actionable prediction market probability shifts using multi-window velocity analysis with volume confirmation.

**Inputs:**
- `pm:probabilities` stream
- Time-adjusted features from Time Adjuster (via Feature Store)

**Outputs (to Redis Stream):**
- `signals:raw` — raw signal events with metadata

**Detection Logic:**
1. Compute probability changes over 4 windows: 5m, 15m, 60m, 240m
2. Compute velocity for each window: `velocity = abs(prob_change) / window_minutes`
3. Select the window with highest velocity (the "fastest window")
4. Classify signal speed by peak velocity:
   - `> 0.02 pts/min` → SPRINT (entry within 3 min)
   - `> 0.005 pts/min` → FAST (entry within 15 min)
   - `> 0.001 pts/min` → MODERATE (entry within 60 min)
   - `else` → SLOW (entry within 240 min)
5. Volume confirmation: 15-minute volume must exceed 1.5× the rolling 7-day average 15-minute volume
6. Cross-platform check: if Kalshi equivalent exists, directional alignment required (probability moving same direction on both platforms)
7. Minimum shift gate: the fastest window's absolute probability change must exceed `MIN_PROB_SHIFT` (default: 0.05 = 5 percentage points)

**Parameters:**

| Parameter | Default | Description |
|---|---|---|
| `MIN_PROB_SHIFT` | 0.05 | Minimum probability change to qualify as signal |
| `VOLUME_CONFIRMATION_RATIO` | 1.5 | Recent volume must exceed this × average |
| `CROSS_PLATFORM_REQUIRED` | false | If true, require Kalshi directional alignment |
| `SPRINT_VELOCITY_THRESHOLD` | 0.02 | Points per minute for SPRINT classification |
| `FAST_VELOCITY_THRESHOLD` | 0.005 | Points per minute for FAST classification |
| `MODERATE_VELOCITY_THRESHOLD` | 0.001 | Points per minute for MODERATE classification |


### 3.7 Stage 1 Classifier

**Purpose:** Binary classification — "Is this PM probability shift genuine information, or noise?" Outputs a calibrated probability.

**Inputs:**
- Raw signal event from Signal Detector
- Time-adjusted features from Feature Store
- Anomaly scores from Anomaly Detector

**Outputs (to Redis Stream):**
- `signals:classified` — signal event enriched with Stage 1 prediction

**Feature Vector (13 features):**

| # | Feature | Source | Description |
|---|---|---|---|
| 1 | `prob_delta_fastest_window` | Signal Detector | Magnitude of shift in fastest window |
| 2 | `prob_velocity` | Signal Detector | Points per minute |
| 3 | `volume_ratio` | Signal Detector | Recent / average volume |
| 4 | `volume_concentration` | Anomaly Detector | HHI of trade sizes (0-1) |
| 5 | `new_wallet_volume_pct` | Anomaly Detector | % of recent volume from wallets < 7 days |
| 6 | `max_suspicion_score` | Anomaly Detector | Highest composite score of any wallet in this market (last 4h) |
| 7 | `insider_wallet_count` | Anomaly Detector | Count of wallets with suspicion > 7.0 |
| 8 | `noise_adjusted_signal` | Time Adjuster | Z-score of shift vs expected noise |
| 9 | `time_confidence` | Time Adjuster | Lifecycle-stage confidence multiplier |
| 10 | `information_premium` | Time Adjuster | Market prob − theoretical prob (0 for non-expiring) |
| 11 | `excess_vs_time_decay` | Time Adjuster | Prob change beyond time decay |
| 12 | `cross_platform_divergence` | Signal Detector | Polymarket vs Kalshi spread |
| 13 | `minutes_remaining` | Time Adjuster | Minutes to resolution (inf for outcome-triggered) |

**Model Specification:**
- Algorithm: LightGBM binary classifier
- Hyperparameters: `n_estimators=200, max_depth=6, learning_rate=0.05, min_child_samples=20, subsample=0.8, colsample_bytree=0.8, class_weight='balanced'`
- Calibration: Platt scaling (sklearn `CalibratedClassifierCV` with `method='sigmoid'`)
- Training label: 1 if PM probability moved toward the eventual resolution (correct signal), 0 if it moved away (noise/wrong)
- Minimum training set: 200 labeled events before first deployment
- Retrained weekly (see self-training loop)

**Output Schema:**
```json
{
  "signal_id": "uuid",
  "market_id": "string",
  "timestamp": "iso8601",
  "signal_quality_prob": 0.73,
  "signal_quality_entropy": 0.28,
  "signal_speed": "FAST",
  "direction": 1,
  "recommended_entry_minutes": 15,
  "features": { ... }
}
```


### 3.8 Linkage Registry

**Purpose:** Store and serve statistically validated PM-asset linkages with decay profiles. This is a lookup table, not a model — it is updated offline by the Linkage Validator batch job.

**Storage:** JSON file on disk + Redis hash cache for fast lookup.

**Schema per linkage:**

```json
{
  "linkage_id": "fed_rate_cut::TLT",
  "pm_category": "fed_rate_decision",
  "pm_market_pattern": "fed.*rate.*cut|fomc.*rate",
  "asset_ticker": "TLT",
  "asset_class": "equity_etf",
  "direction": "same",
  "linkage_composite_score": 0.82,
  "layer_results": {
    "cross_correlation": { "optimal_lag_minutes": 185, "correlation": 0.34, "p_value": 0.002, "pass": true },
    "granger": { "optimal_lag_5m_bars": 37, "f_stat": 8.4, "p_value": 0.003, "pass": true },
    "transfer_entropy": { "net_te": 0.018, "direction": "PM_LEADS", "pass": true },
    "event_study": {
      "checkpoints_minutes": [5, 10, 15, 30, 60, 120, 240, 480],
      "car_bps": [2.1, 4.3, 5.8, 8.1, 11.4, 14.9, 17.6, 16.2],
      "hit_rates": [0.52, 0.55, 0.57, 0.60, 0.63, 0.66, 0.68, 0.65],
      "t_stats": [0.8, 1.4, 1.9, 2.5, 3.1, 3.6, 3.8, 3.4],
      "peak_checkpoint_minutes": 240,
      "peak_car_bps": 17.6,
      "peak_hit_rate": 0.68,
      "peak_t_stat": 3.8,
      "n_events": 47,
      "pass": true
    },
    "mutual_information": { "mi": 0.032, "pass": true }
  },
  "decay_profile": {
    "half_life_minutes": 210,
    "decay_type": "SLOW",
    "peak_correlation": 0.34,
    "residual_correlation": 0.05,
    "decay_constant_tau": 303,
    "optimal_entry_window_minutes": 63,
    "max_useful_lag_minutes": 630
  },
  "crowding_status": {
    "current_optimal_lag_minutes": 185,
    "lag_compression_rate": -3.2,
    "months_until_unviable": 56,
    "size_multiplier": 1.0,
    "last_checked": "2026-04-21T00:00:00Z"
  },
  "validated_at": "2026-04-21T00:00:00Z",
  "n_layers_passing": 5,
  "tradeable": true
}
```

**Lookup Interface:**
```python
class LinkageRegistry:
    def get_linkages_for_category(self, pm_category: str) -> list[Linkage]
    def get_linkage(self, pm_category: str, ticker: str) -> Linkage | None
    def is_tradeable(self, pm_category: str, ticker: str) -> bool
    def get_decay_profile(self, linkage_id: str) -> DecayProfile
    def get_all_tradeable(self) -> list[Linkage]
    def update_linkage(self, linkage: Linkage) -> None
```


### 3.9 Stage 2 Meta-Learner

**Purpose:** Combine Stage 1 signal quality, linkage statistics, and market context to predict the expected risk-adjusted return of acting on this signal. This is where the two-stage architecture earns its value.

**Inputs:**
- Classified signal from Stage 1
- Linkage record from Linkage Registry
- Current market context (from Market Context Service)
- Historical performance of this PM-asset pair (from Performance Tracker)

**Feature Vector (16 features):**

| # | Feature | Source | Description |
|---|---|---|---|
| 1 | `signal_quality_prob` | Stage 1 | Calibrated probability signal is correct (0-1) |
| 2 | `signal_quality_entropy` | Stage 1 | `-p*log(p) - (1-p)*log(1-p)` — model uncertainty |
| 3 | `linkage_composite_score` | Linkage Registry | 5-layer validation score (0-1) |
| 4 | `granger_p_value` | Linkage Registry | Causal link strength |
| 5 | `optimal_lag_minutes` | Linkage Registry | Expected lead time |
| 6 | `half_life_minutes` | Linkage Registry | Signal decay speed |
| 7 | `historical_event_car_bps` | Linkage Registry | Average CAR from event studies |
| 8 | `historical_hit_rate` | Linkage Registry | Historical accuracy of this pair |
| 9 | `vix_level` | Market Context | Current VIX (from CBOE/Yahoo) |
| 10 | `hmm_regime_state` | Navigator HMM | Current regime (0-5) |
| 11 | `asset_return_60m` | Market Context | Asset's recent 60-min return (already moved?) |
| 12 | `asset_bid_ask_spread_bps` | Market Context | Current spread (execution cost) |
| 13 | `minutes_to_market_close` | Market Context | Liquidity varies intraday |
| 14 | `pair_sharpe_30d` | Performance Tracker | Rolling 30-day Sharpe of this pair |
| 15 | `pair_hit_rate_30d` | Performance Tracker | Rolling 30-day hit rate |
| 16 | `category_sharpe_30d` | Performance Tracker | Category-level rolling Sharpe |

**Model Specification:**
- Algorithm: LightGBM regressor
- Target: risk-adjusted return = actual_return_at_peak_checkpoint / expected_risk
- Alternative framing: LightGBM classifier with target = "did trade exceed 0.5× transaction cost?" (binary)
- Hyperparameters: same as Stage 1 but with `objective='regression'` (or `binary`)
- Retrained weekly alongside Stage 1

**Output Schema:**
```json
{
  "signal_id": "uuid",
  "meta_prediction": 0.0018,
  "joint_confidence": 0.67,
  "trade_decision": "TRADE",
  "recommended_size_pct": 0.012,
  "recommended_hold_minutes": 240,
  "recommended_order_type": "LIMIT_AT_ASK",
  "features": { ... }
}
```


### 3.10 Risk Engine

**Purpose:** Apply hard risk gates, compute Half-Kelly position size, enforce portfolio-level constraints, and manage circuit breakers.

**Inputs:**
- Trade recommendation from Stage 2
- Current portfolio state (positions, exposure, recent PnL)

**Gates (evaluated in order — first failure blocks the trade):**

| # | Gate | Condition | On Fail |
|---|---|---|---|
| 1 | Signal Quality | `signal_quality_prob >= 0.50` | Block |
| 2 | Linkage Score | `linkage_composite_score >= 0.40` | Block |
| 3 | Joint Confidence | `sqrt(signal_quality × linkage_score) >= 0.55` | Block |
| 4 | Per-Trade Size Cap | `position_size <= CATEGORY_MAX_PCT × NAV` | Cap size |
| 5 | Aggregate PM Exposure | `total_pm_exposure + position_size <= 0.05 × NAV` | Block |
| 6 | Drawdown Cooldown | `pm_drawdown_30d < 0.02 × NAV` | Block |
| 7 | Market Hours | Asset market is open (or 24/7 for crypto) | Queue |
| 8 | Asset Liquidity | `asset_bid_ask_spread < MAX_SPREAD_BPS` | Block |

**Position Sizing:**
```
edge = signal_quality_prob - 0.50
kelly = edge / 0.50
half_kelly = kelly × 0.5
raw_size = half_kelly × NAV

# Apply category cap
category_caps = {
    "fed_rate": 0.03,
    "earnings": 0.01,
    "geopolitical": 0.02,
    "recession": 0.02,
    "crypto": 0.01,
    "political": 0.01
}
capped_size = min(raw_size, category_caps[category] × NAV)

# Apply crowding multiplier from Linkage Registry
final_size = capped_size × crowding_size_multiplier
```

**Circuit Breakers:**

| Breaker | Trigger | Action | Reset Condition |
|---|---|---|---|
| PM Drawdown | PM strategy loss > 2% NAV in 30d | Flatten all PM positions, halt PM trading | 30 calendar days |
| Model PSI | PSI > 0.25 on weekly check | Halt PM trading | Retrain + validate on 30d OOS |
| Category Brier | Brier > 0.20 for any category | Reduce category size 50% | Brier < 0.15 for 30 consecutive days |
| Feed Stall | No PM data for 5 minutes | Flatten all PM positions | Data feed restored + 15 min stability |

**Parameters:**

| Parameter | Default | Description |
|---|---|---|
| `MAX_PM_EXPOSURE_PCT` | 0.05 | Max aggregate PM-derived exposure as % of NAV |
| `MAX_SPREAD_BPS` | 20 | Max acceptable bid-ask spread on linked asset |
| `DRAWDOWN_LIMIT_PCT` | 0.02 | PM strategy drawdown limit as % of NAV |
| `DRAWDOWN_COOLDOWN_DAYS` | 30 | Days to wait after drawdown breach |
| `FEED_STALL_MINUTES` | 5 | Minutes without data before feed-stall breaker |


### 3.11 Order Router

**Purpose:** Translate trade decisions into broker-specific orders with regime-adaptive order types.

**Inputs:**
- Trade decision from Risk Engine (ticker, direction, size, hold minutes, order type)
- Current regime state from Navigator HMM

**Order Type Logic:**

| Signal Speed | Default Order Type | Override if VIX > 30 |
|---|---|---|
| SPRINT | Market order | Market order |
| FAST | Limit at ask (buy) / bid (sell) | Market order |
| MODERATE | Limit at mid-spread | Limit at ask/bid |
| SLOW | Limit at mid-spread | Limit at mid-spread |

**Broker Routing:**

| Asset Class | Paper | Live |
|---|---|---|
| US Equities + ETFs | Alpaca Paper | IBKR |
| US Options | Alpaca Paper | IBKR or Tastytrade |
| Crypto Spot | Exchange Testnet | Coinbase via CCXT |
| Crypto Perps | Hyperliquid Testnet | Hyperliquid via CCXT |

**Behavior:**
- Submit order via broker API
- Set timeout: if limit order not filled within `recommended_entry_minutes`, cancel and log as missed signal
- On fill: log to `pm_trades_executed` table with fill price, slippage, and latency
- On fill: set exit alarm at `recommended_hold_minutes` from entry
- At exit time: submit market order to close position; log exit fill


### 3.12 Health Monitor

**Purpose:** Run the three degradation detection algorithms and the weekly self-training loop.

**Sub-components:**

#### Alpha Crowding Monitor (runs monthly)
- For each active PM-asset linkage: re-run lead-lag correlation at minute resolution
- Compare current optimal lag to historical optimal lag
- Compute lag compression rate and months-until-unviable
- Update `crowding_status` in Linkage Registry

#### Calibration Monitor (runs weekly)
- For each PM signal category: compute 60-event rolling Brier score
- If Brier > 0.20: emit DEGRADED alert, trigger Risk Engine category size reduction

#### Model Staleness Monitor (runs weekly)
- Compute feature importance Spearman correlation (current vs retrained)
- Compute PSI on model output distribution
- Compute rolling accuracy trend (linear regression on weekly accuracy)
- If 2+ monitors flag: emit RETRAIN_REQUIRED alert, trigger circuit breaker

#### Self-Training Loop (runs weekly, Sunday night)
- Monday data: pull all resolved PM markets from past week
- Tuesday features: generate time-adjusted feature vectors, label with outcomes
- Wednesday retrain: retrain Stage 1 + Stage 2 on full dataset, purged walk-forward
- Thursday evaluation: compare new vs old on 4-week OOS; run all degradation checks
- Friday deployment: if new model outperforms on all OOS metrics, promote; else keep old

---

## 4. Data Models and Schemas

### 4.1 QuestDB Tables

```sql
-- 1-minute probability snapshots
CREATE TABLE pm_probabilities (
    timestamp TIMESTAMP,
    market_id SYMBOL,
    source SYMBOL,            -- 'polymarket' | 'kalshi'
    probability DOUBLE,       -- 0.0 to 1.0
    bid DOUBLE,
    ask DOUBLE,
    volume_usd DOUBLE,        -- Volume in this 1-minute bar
    trade_count INT
) TIMESTAMP(timestamp) PARTITION BY DAY;

-- Individual trades
CREATE TABLE pm_trades (
    timestamp TIMESTAMP,
    market_id SYMBOL,
    source SYMBOL,
    wallet_address VARCHAR,   -- NULL for Kalshi
    side SYMBOL,              -- 'buy_yes' | 'buy_no' | 'sell_yes' | 'sell_no'
    price DOUBLE,
    size_usd DOUBLE,
    trade_id VARCHAR
) TIMESTAMP(timestamp) PARTITION BY DAY;

-- Market metadata
CREATE TABLE pm_markets (
    market_id SYMBOL,
    source SYMBOL,
    title VARCHAR,
    category SYMBOL,          -- 'fed_rate' | 'earnings' | 'geopolitical' | etc.
    bet_type SYMBOL,          -- 'preset' | 'expiring' | 'triggered'
    created_at TIMESTAMP,
    resolution_time TIMESTAMP,-- NULL for outcome-triggered
    resolved BOOLEAN,
    outcome SYMBOL,           -- 'yes' | 'no' | NULL if unresolved
    strike_price DOUBLE,      -- NULL if not applicable
    underlying_ticker SYMBOL, -- NULL if not applicable
    volume_total_usd DOUBLE,
    last_updated TIMESTAMP
) TIMESTAMP(last_updated);

-- Wallet metadata
CREATE TABLE pm_wallets (
    wallet_address VARCHAR,
    first_seen TIMESTAMP,
    funding_source_1 VARCHAR, -- Immediate funder
    funding_source_2 VARCHAR, -- Funder's funder (2-hop)
    total_transactions INT,
    distinct_markets INT,
    total_volume_usd DOUBLE,
    last_traced TIMESTAMP
) TIMESTAMP(last_traced);

-- Anomaly scores (append-only)
CREATE TABLE pm_anomaly_scores (
    timestamp TIMESTAMP,
    market_id SYMBOL,
    wallet_address VARCHAR,
    composite_score DOUBLE,
    wallet_freshness_score DOUBLE,
    statistical_improbability_score DOUBLE,
    pre_resolution_score DOUBLE,
    bet_size_anomaly_score DOUBLE,
    sybil_cluster_score DOUBLE,
    alert_level SYMBOL        -- 'none' | 'watch' | 'critical'
) TIMESTAMP(timestamp) PARTITION BY DAY;

-- Executed trades (PM-derived strategy)
CREATE TABLE pm_trades_executed (
    timestamp TIMESTAMP,
    signal_id VARCHAR,
    market_id SYMBOL,
    pm_category SYMBOL,
    asset_ticker SYMBOL,
    direction SYMBOL,         -- 'long' | 'short'
    entry_price DOUBLE,
    exit_price DOUBLE,
    position_size_usd DOUBLE,
    signal_quality_prob DOUBLE,
    linkage_score DOUBLE,
    joint_confidence DOUBLE,
    entry_fill_latency_ms INT,
    exit_fill_latency_ms INT,
    slippage_bps DOUBLE,
    hold_minutes INT,
    pnl_usd DOUBLE,
    pnl_bps DOUBLE,
    entry_timestamp TIMESTAMP,
    exit_timestamp TIMESTAMP
) TIMESTAMP(timestamp) PARTITION BY MONTH;

-- Model performance tracking
CREATE TABLE pm_model_metrics (
    timestamp TIMESTAMP,
    model_version VARCHAR,
    stage SYMBOL,             -- 'stage1' | 'stage2'
    metric_name SYMBOL,       -- 'auc', 'accuracy', 'sharpe', 'psi', etc.
    metric_value DOUBLE,
    sample_size INT,
    window_days INT
) TIMESTAMP(timestamp);
```

### 4.2 Redis Key Patterns

```
# Real-time streams
pm:markets                    -- Market metadata events
pm:trades                     -- Trade events
pm:probabilities              -- 1-minute probability bars
signals:raw                   -- Raw detected signals
signals:classified            -- Stage 1 classified signals
anomaly:scores                -- Per-trade anomaly scores
anomaly:alerts                -- High-suspicion alerts
wallets:trace_request         -- Requests for wallet tracing

# Cached state (Redis Hashes)
wallet:{address}              -- Wallet aggregate statistics
market:{market_id}:prob       -- Latest probability + 15m/60m/240m history
market:{market_id}:meta       -- Market metadata cache
linkage:{pm_category}:{ticker} -- Linkage record cache

# Feature Store (Redis Hashes)
features:{market_id}:{minute} -- Time-adjusted feature vector

# System state
system:circuit_breakers       -- Active circuit breaker status
system:portfolio              -- Current positions + exposure
system:config                 -- Runtime config overrides
```

### 4.3 Feature Store (Parquet)

```
/data/features/
  ├── raw/
  │   ├── pm_probabilities/    -- Partitioned by date
  │   └── pm_trades/
  ├── computed/
  │   ├── time_adjusted/       -- Time-adjusted features per market per minute
  │   ├── anomaly_scores/      -- Anomaly scores per trade
  │   └── signal_events/       -- Detected signals with all features
  ├── training/
  │   ├── stage1/              -- Labeled training data for Stage 1
  │   ├── stage2/              -- Labeled training data for Stage 2
  │   └── linkage_validation/  -- Event study + correlation data per pair
  └── models/
      ├── stage1/              -- Versioned Stage 1 model artifacts
      ├── stage2/              -- Versioned Stage 2 model artifacts
      └── vol_surfaces/        -- Category × time-remaining volatility surfaces
```

---

## 5. Message Contracts

### 5.1 Redis Stream: `pm:probabilities`

```json
{
  "timestamp": "2026-04-23T14:30:00Z",
  "market_id": "0x1234abcd...",
  "source": "polymarket",
  "probability": 0.72,
  "bid": 0.71,
  "ask": 0.73,
  "volume_usd": 4520.50,
  "trade_count": 12
}
```

### 5.2 Redis Stream: `signals:raw`

```json
{
  "signal_id": "sig_20260423_143000_abcd",
  "timestamp": "2026-04-23T14:30:00Z",
  "market_id": "0x1234abcd...",
  "source": "polymarket",
  "pm_category": "earnings",
  "bet_type": "preset",
  "direction": -1,
  "prob_current": 0.58,
  "prob_delta_5m": -0.03,
  "prob_delta_15m": -0.07,
  "prob_delta_60m": -0.09,
  "prob_velocity": 0.0047,
  "signal_speed": "FAST",
  "volume_ratio": 3.2,
  "cross_platform_aligned": true,
  "minutes_remaining": 180,
  "time_adjusted_features": {
    "noise_adjusted_signal": -2.8,
    "time_confidence": 0.9,
    "information_premium": 0.0,
    "excess_vs_time_decay": -0.07
  },
  "anomaly_context": {
    "max_suspicion_score": 8.2,
    "insider_wallet_count": 2,
    "new_wallet_volume_pct": 0.45
  }
}
```

### 5.3 Redis Stream: `signals:classified`

Extends `signals:raw` with:

```json
{
  "...all fields from signals:raw...",
  "stage1_output": {
    "signal_quality_prob": 0.73,
    "signal_quality_entropy": 0.28,
    "model_version": "stage1_v12_20260421"
  }
}
```

### 5.4 Redis Stream: `anomaly:alerts`

```json
{
  "timestamp": "2026-04-23T14:29:45Z",
  "market_id": "0x1234abcd...",
  "wallet_address": "0x9876fedc...",
  "composite_score": 8.2,
  "alert_level": "critical",
  "signals": {
    "wallet_freshness": 1.8,
    "statistical_improbability": 2.3,
    "pre_resolution": 2.1,
    "bet_size_anomaly": 1.2,
    "sybil_cluster": 0.8
  },
  "trade_details": {
    "side": "buy_yes",
    "size_usd": 15000,
    "price": 0.42,
    "market_title": "Will AAPL beat Q2 earnings?"
  },
  "wallet_profile": {
    "age_days": 3,
    "total_markets": 2,
    "win_rate": 1.0,
    "funding_source": "0xaabb..."
  }
}
```

---

## 6. API Specifications

### 6.1 External APIs Consumed

| API | Auth | Base URL | Rate Limit | Key Endpoints Used |
|---|---|---|---|---|
| Polymarket CLOB | None (public) | `https://clob.polymarket.com` | ~4 req/s conservative | `GET /markets`, `GET /trades`, `GET /book` |
| Polymarket Gamma | None | `https://gamma-api.polymarket.com` | ~4 req/s | `GET /markets` (metadata, resolution) |
| Kalshi | API Key | `https://api.elections.kalshi.com/trade-api/v2` | 10 req/s public, 100 auth | `GET /markets`, `GET /markets/{ticker}/orderbook` |
| Polygon RPC | API Key | `https://polygon-rpc.com` | Depends on provider | `eth_getTransactionReceipt`, `eth_getLogs` |
| Alpaca | API Key | `https://paper-api.alpaca.markets` | 200 req/min | `POST /v2/orders`, `GET /v2/positions` |
| IBKR TWS | Local gateway | `https://localhost:5000` | N/A | TWS API via `ib_async` |

### 6.2 Internal REST API (optional — for dashboard/debugging)

If building a Streamlit or Grafana dashboard that needs to query system state:

```
GET /api/signals/recent?limit=50        -- Recent detected signals
GET /api/signals/{id}                   -- Signal detail with all features
GET /api/linkages                       -- All linkages with status
GET /api/linkages/{id}/decay            -- Decay profile for a linkage
GET /api/portfolio                      -- Current PM-derived positions
GET /api/health                         -- System health (all monitors)
GET /api/metrics/stage1                 -- Stage 1 model performance
GET /api/metrics/stage2                 -- Stage 2 model performance
GET /api/circuit-breakers               -- Active breakers
```

---

## 7. Algorithmic Specifications

### 7.1 Exponential Decay Fitting (Signal Half-Life)

**Input:** DataFrame of `(lag_minutes, abs_correlation)` pairs from lead-lag analysis  
**Output:** `half_life_minutes`, `decay_type`, `optimal_entry_window_minutes`

**Algorithm:**
1. Filter to positive lags only (PM leads asset)
2. Fit `corr(t) = A × exp(-t/τ) + baseline` via `scipy.optimize.curve_fit`
3. Initial guess: A=0.3, τ=120, baseline=0.01
4. Bounds: A ∈ [0, 1], τ ∈ [1, 5000], baseline ∈ [0, 0.5]
5. `half_life = τ × ln(2)`
6. `optimal_entry_window = half_life × 0.3`
7. `max_useful_lag = half_life × 3`

### 7.2 Barrier Option Theoretical Probability (for Expiring Contracts)

**Input:** current_price (S), strike_price (K), minutes_remaining, annualized_volatility (σ), risk_free_rate (r)  
**Output:** theoretical probability (0-1)

**Algorithm:**
1. Convert minutes to trading years: `T = minutes / (252 × 6.5 × 60)`
2. If T ≤ 0: return 1.0 if S ≥ K else 0.0
3. `d2 = (ln(S/K) + (r - 0.5σ²)T) / (σ√T)`
4. `P = Φ(d2)` where Φ is the standard normal CDF

### 7.3 Composite Suspicion Score

**Input:** wallet metadata, trade details, market state  
**Output:** composite score (0-10)

**Algorithm:**
1. **Wallet freshness** (max 2.0):
   - age < 1 day: 2.0
   - age < 3 days: 1.5
   - age < 7 days: 1.0
   - markets < 2: +0.5
   - markets < 5: +0.3
2. **Statistical improbability** (max 2.5):
   - Compute binomial p-value for observed win rate over N trades
   - p < 0.0001: 2.5
   - p < 0.001: 2.0
   - p < 0.01: 1.5
   - p < 0.05: 0.5
3. **Pre-resolution timing** (max 2.5):
   - Compute last-minute ratio = trades within 10 min of resolution / total trades
   - ratio > 0.8: 2.5
   - ratio > 0.5: 2.0
   - ratio > 0.3: 1.0
   - If current trade is within 10 min of resolution: +0.5
4. **Bet size anomaly** (max 1.5):
   - Compute z-score of trade size vs wallet's historical average
   - z > 5: 1.5
   - z > 3: 1.0
   - z > 2: 0.5
5. **Sybil clustering** (max 2.0):
   - Count wallets sharing the same 2-hop funding source AND betting same market same direction
   - cluster_size ≥ 4: 2.0
   - cluster_size = 3: 1.5
   - cluster_size = 2: 1.0

6. `composite = freshness×0.15 + improbability×0.25 + timing×0.25 + size×0.15 + sybil×0.20`

### 7.4 Joint Confidence and Half-Kelly Sizing

**Input:** signal_quality_prob (p_s), linkage_composite_score (l_s), category, NAV  
**Output:** position_size_usd

**Algorithm:**
1. `joint_confidence = sqrt(p_s × l_s)`
2. If `joint_confidence < 0.55`: return 0 (no trade)
3. `edge = p_s - 0.50`
4. `kelly = edge / 0.50`
5. `half_kelly = kelly × 0.5`
6. `raw_size = half_kelly × NAV`
7. `category_cap = CATEGORY_CAPS[category] × NAV`
8. `crowding_adj = raw_size × linkage.crowding_size_multiplier`
9. `final_size = min(crowding_adj, category_cap)`
10. `final_size = min(final_size, (MAX_PM_EXPOSURE - current_pm_exposure) × NAV)`
11. Return `max(0, final_size)`

### 7.5 Population Stability Index (PSI)

**Input:** expected distribution (training), actual distribution (recent)  
**Output:** PSI score (0 = identical, > 0.20 = significant shift)

**Algorithm:**
1. Bin both distributions into 10 equal-frequency bins (from expected)
2. Compute percentage of observations in each bin for both
3. Clip both to [0.001, ∞) to avoid log(0)
4. `PSI = Σ (actual_pct - expected_pct) × ln(actual_pct / expected_pct)`

---

## 8. Infrastructure and Deployment

### 8.1 Target Platform: AWS Cloud VM

All phases run on a single AWS EC2 instance in `us-east-1` (Northern Virginia) — lowest latency to Polymarket, Kalshi, NYSE/NASDAQ, and broker APIs.

```
Instance:     t4g.medium (Phase 1-2) → t4g.large (Phase 3-4) → m7g.large (Phase 5-6)
OS:           Ubuntu 24.04 LTS ARM64 (Graviton)
Python:       3.12 with venv
Storage:      100 GB gp3 EBS data volume mounted at /data
Networking:   Elastic IP, Security Group (SSH + Grafana + MLflow from your IP only)
Containers:   Docker Compose for QuestDB, Redis, Grafana, MLflow
App:          Python service managed by systemd
Cost:         ~$28/mo (Phase 1-2) → ~$58/mo (Phase 3-4) → ~$70/mo (Phase 5-6)
```

### 8.2 Docker Compose Services

```yaml
version: '3.8'

services:
  questdb:
    image: questdb/questdb:8.0
    ports:
      - "127.0.0.1:9000:9000"    # Web console — localhost only
      - "127.0.0.1:9009:9009"    # ILP ingestion
      - "127.0.0.1:8812:8812"    # PostgreSQL wire protocol
    volumes:
      - /data/questdb:/var/lib/questdb
    environment:
      - QDB_LINE_TCP_COMMIT_INTERVAL_DEFAULT=1000
      - QDB_LINE_TCP_MAINTENANCE_JOB_INTERVAL=60000
    restart: always
    deploy:
      resources:
        limits:
          memory: 2G
  
  redis:
    image: redis:7-alpine
    ports:
      - "127.0.0.1:6379:6379"
    command: redis-server --appendonly yes --maxmemory 1gb --maxmemory-policy allkeys-lru
    volumes:
      - /data/redis:/data
    restart: always
    deploy:
      resources:
        limits:
          memory: 1G
  
  grafana:
    image: grafana/grafana:11.0
    ports:
      - "0.0.0.0:3000:3000"     # Exposed — secured by AWS Security Group
    volumes:
      - /data/grafana:/var/lib/grafana
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_PASSWORD}
    restart: always
  
  mlflow:
    image: ghcr.io/mlflow/mlflow:2.16
    ports:
      - "0.0.0.0:5000:5000"     # Exposed — secured by AWS Security Group
    command: >
      mlflow server --host 0.0.0.0
      --backend-store-uri sqlite:///data/mlflow.db
      --default-artifact-root /data/artifacts
    volumes:
      - /data/mlflow:/data
    restart: always
```

Security: QuestDB and Redis bound to localhost only. Grafana and MLflow exposed but protected by AWS Security Group (your IP only).

### 8.3 Cloud Cost Summary

| Phase | EC2 Instance | EBS | S3 (backups) | Total/mo |
|---|---|---|---|---|
| Phase 1-2 | t4g.medium ($24) | 50 GB gp3 ($4) | — | ~$28 |
| Phase 3-4 | t4g.large ($49) | 100 GB gp3 ($8) | $1 | ~$58 |
| Phase 5-6 | m7g.large ($60) | 100 GB gp3 ($8) | $2 | ~$70 |
| + Tokyo crypto (optional) | t4g.small ($15) | 20 GB ($2) | — | +$17 |

### 8.3.1 Storage Layout

```
/data/                          ← EBS volume mounted here (persists if EC2 terminates)
├── questdb/                    ← ~6 GB/month growth
├── redis/                      ← AOF persistence (~1 GB)
├── grafana/                    ← Dashboard state
├── mlflow/                     ← Experiment tracking + model artifacts
├── parquet/                    ← Feature store + training data
│   ├── features/
│   └── models/
├── config/                     ← Symlinked from app directory
└── backups/                    ← Daily QuestDB snapshots (7-day local retention)
```

### 8.3.2 Systemd Service

The Python application runs as a systemd service for auto-start and crash recovery:

```ini
# /etc/systemd/system/pm-platform.service
[Unit]
Description=PM Signal Platform
After=docker.service
Requires=docker.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/prediction-market-signals
EnvironmentFile=/home/ubuntu/prediction-market-signals/.env
ExecStart=/home/ubuntu/prediction-market-signals/.venv/bin/python -m src.main
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

### 8.3.3 Development Workflow

Code on local machine → push to GitHub → pull on EC2 → restart service:

```bash
# Local: develop and push
git push origin main

# EC2: deploy
cd ~/prediction-market-signals && git pull && sudo systemctl restart pm-platform

# Access dashboards via SSH tunnel
ssh -L 3000:localhost:3000 -L 5000:localhost:5000 -L 9000:localhost:9000 ubuntu@ELASTIC_IP
```

### 8.4 Project Structure

```
prediction-market-signals/
├── README.md
├── docker-compose.yml
├── requirements.txt
├── config/
│   ├── settings.yaml          -- All parameters, thresholds, feature flags
│   ├── linkages.json           -- Linkage Registry (validated PM-asset pairs)
│   ├── vol_surfaces.json       -- Category × time volatility surfaces
│   └── grafana/                -- Dashboard provisioning
├── src/
│   ├── __init__.py
│   ├── main.py                 -- Entry point: start all services
│   ├── config.py               -- Load and validate settings.yaml
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── polymarket.py       -- Polymarket Collector
│   │   ├── kalshi.py           -- Kalshi Collector
│   │   ├── wallet_tracer.py    -- Polygon Wallet Tracer
│   │   └── market_context.py   -- VIX, asset prices, HMM regime
│   ├── detection/
│   │   ├── __init__.py
│   │   ├── anomaly.py          -- Anomaly Detector (5-signal composite)
│   │   ├── time_adjuster.py    -- Time Adjuster (3 bet type modes)
│   │   ├── signal_detector.py  -- Signal Detector (velocity + volume)
│   │   └── vol_surface.py      -- Volatility surface construction
│   ├── models/
│   │   ├── __init__.py
│   │   ├── stage1.py           -- Stage 1 Classifier (train + predict)
│   │   ├── stage2.py           -- Stage 2 Meta-Learner (train + predict)
│   │   ├── calibration.py      -- Platt scaling wrapper
│   │   └── features.py         -- Feature vector assembly for both stages
│   ├── linkage/
│   │   ├── __init__.py
│   │   ├── registry.py         -- Linkage Registry (load/save/lookup)
│   │   ├── validator.py        -- 5-layer statistical validation
│   │   ├── cross_correlation.py -- Layer 1: Lead-lag at minute resolution
│   │   ├── granger.py          -- Layer 2: Granger causality (5-min bars)
│   │   ├── transfer_entropy.py -- Layer 3: Non-linear information flow
│   │   ├── event_study.py      -- Layer 4: CAR at 8 checkpoints
│   │   ├── mutual_info.py      -- Layer 5: MI screening
│   │   └── decay.py            -- Exponential decay fitting
│   ├── risk/
│   │   ├── __init__.py
│   │   ├── engine.py           -- Risk Engine (gates + sizing)
│   │   ├── circuit_breakers.py -- Circuit breaker state machine
│   │   └── portfolio.py        -- Portfolio state tracking
│   ├── execution/
│   │   ├── __init__.py
│   │   ├── order_router.py     -- Order Router (broker dispatch)
│   │   ├── alpaca_adapter.py   -- Alpaca broker adapter
│   │   ├── ibkr_adapter.py     -- IBKR broker adapter
│   │   └── ccxt_adapter.py     -- CCXT crypto adapter
│   ├── monitoring/
│   │   ├── __init__.py
│   │   ├── health.py           -- Health Monitor (3 degradation types)
│   │   ├── crowding.py         -- Alpha crowding detection
│   │   ├── calibration.py      -- Brier score monitoring
│   │   ├── staleness.py        -- PSI + feature stability + accuracy trend
│   │   └── self_training.py    -- Weekly retrain pipeline
│   └── utils/
│       ├── __init__.py
│       ├── db.py               -- QuestDB client wrapper
│       ├── redis_client.py     -- Redis client + stream helpers
│       ├── notifications.py    -- Slack/Telegram alerting
│       └── time_utils.py       -- Minute-resolution time helpers
├── tests/
│   ├── unit/
│   │   ├── test_anomaly.py
│   │   ├── test_time_adjuster.py
│   │   ├── test_signal_detector.py
│   │   ├── test_risk_engine.py
│   │   └── test_sizing.py
│   ├── integration/
│   │   ├── test_ingestion_pipeline.py
│   │   ├── test_detection_pipeline.py
│   │   └── test_end_to_end.py
│   └── backtest/
│       ├── test_stage1_walkforward.py
│       ├── test_stage2_walkforward.py
│       └── test_linkage_validation.py
├── notebooks/
│   ├── 01_data_exploration.ipynb
│   ├── 02_vol_surface_construction.ipynb
│   ├── 03_linkage_discovery.ipynb
│   ├── 04_model_training.ipynb
│   └── 05_backtest_analysis.ipynb
└── scripts/
    ├── backfill_pm_data.py     -- Historical data backfill
    ├── build_vol_surface.py    -- Construct volatility surfaces
    ├── validate_linkages.py    -- Run 5-layer validation on all pairs
    ├── train_models.py         -- Train Stage 1 + Stage 2
    └── weekly_retrain.py       -- Full weekly self-training pipeline
```

---

## 9. Configuration and Feature Flags

### 9.1 settings.yaml Structure

```yaml
# ----- INGESTION -----
ingestion:
  polymarket:
    poll_interval_seconds: 15
    backfill_days: 7
    max_requests_per_second: 4
  kalshi:
    poll_interval_seconds: 15
    api_key_env: "KALSHI_API_KEY"
    max_requests_per_second: 10
  wallet_tracer:
    max_concurrent_rpc: 5
    retrace_interval_hours: 24
    hop_depth: 2

# ----- DETECTION -----
detection:
  anomaly:
    wallet_age_threshold_days: 7
    wallet_market_count_threshold: 5
    win_rate_pvalue_threshold: 0.001
    bet_size_zscore_threshold: 3.0
    pre_resolution_window_minutes: 10
    alert_threshold_watch: 4.5
    alert_threshold_critical: 7.0
  signal:
    min_prob_shift: 0.05
    volume_confirmation_ratio: 1.5
    cross_platform_required: false
    velocity_thresholds:
      sprint: 0.02
      fast: 0.005
      moderate: 0.001
  time_adjuster:
    preset_late_stage_threshold: 0.10
    preset_early_stage_threshold: 0.70
    expiring_risk_free_rate: 0.045
    triggered_confidence_discount: 0.70
    triggered_maturity_minutes: 43200

# ----- MODELS -----
models:
  stage1:
    algorithm: "lightgbm"
    n_estimators: 200
    max_depth: 6
    learning_rate: 0.05
    min_child_samples: 20
    subsample: 0.8
    colsample_bytree: 0.8
    min_training_samples: 200
    calibration_method: "sigmoid"
  stage2:
    algorithm: "lightgbm"
    objective: "regression"
    n_estimators: 200
    max_depth: 6
    learning_rate: 0.05
    min_training_samples: 150

# ----- RISK -----
risk:
  gates:
    min_signal_quality: 0.50
    min_linkage_score: 0.40
    min_joint_confidence: 0.55
    max_spread_bps: 20
  sizing:
    max_pm_exposure_pct: 0.05
    category_caps:
      fed_rate: 0.03
      earnings: 0.01
      geopolitical: 0.02
      recession: 0.02
      crypto: 0.01
      political: 0.01
  circuit_breakers:
    drawdown_limit_pct: 0.02
    drawdown_cooldown_days: 30
    feed_stall_minutes: 5
    psi_halt_threshold: 0.25
    brier_reduce_threshold: 0.20
    brier_recovery_days: 30

# ----- EXECUTION -----
execution:
  paper_mode: true
  brokers:
    equities: "alpaca"
    options: "alpaca"
    crypto: "ccxt"
  alpaca:
    api_key_env: "ALPACA_API_KEY"
    api_secret_env: "ALPACA_API_SECRET"
    base_url: "https://paper-api.alpaca.markets"
  order_timeout_minutes: 15

# ----- MONITORING -----
monitoring:
  health_check_interval_hours: 168  # Weekly
  crowding_check_interval_days: 30
  notifications:
    slack_webhook_env: "SLACK_WEBHOOK_URL"
    telegram_bot_token_env: "TELEGRAM_BOT_TOKEN"
    telegram_chat_id_env: "TELEGRAM_CHAT_ID"

# ----- LINKAGE VALIDATION -----
linkage:
  min_layers_passing: 3
  min_composite_score: 0.60
  min_events_for_event_study: 30
  granger_resample_minutes: 5
  granger_max_lag_minutes: 360
  cross_correlation_max_lag_minutes: 2880
  min_viable_lag_minutes: 5
```

### 9.2 Feature Flags

| Flag | Default | Description |
|---|---|---|
| `PAPER_MODE` | true | If true, all orders go to paper/testnet. Never false until Phase 6 go/no-go. |
| `STAGE2_ENABLED` | false | If false, skip Stage 2 and use Stage 1 directly. Enable in Phase 4. |
| `EXECUTION_ENABLED` | false | If false, log trade recommendations but don't submit orders. Enable in Phase 5. |
| `CRYPTO_ENABLED` | false | Enable crypto asset trading via CCXT. |
| `WALLET_TRACING_ENABLED` | true | Enable on-chain wallet analysis. Disable if RPC costs are too high. |
| `CROSS_PLATFORM_REQUIRED` | false | If true, require Kalshi directional alignment before signaling. |
| `SELF_TRAINING_ENABLED` | false | Enable weekly automated retraining. Enable in Phase 4. |

---

## 10. Monitoring and Observability

### 10.1 Grafana Dashboards

**Dashboard 1: Live Operations**
- PM API latency and data freshness (last update timestamp per source)
- Signal detection rate (signals/hour by category)
- Anomaly alert rate (alerts/hour by severity)
- Current PM-derived positions and unrealized PnL
- Circuit breaker status (green/red per breaker)

**Dashboard 2: Model Performance**
- Stage 1 AUC (rolling 30-day)
- Stage 2 Sharpe (rolling 30-day)
- Hit rate by signal category (rolling 30-day)
- PSI trend (weekly)
- Feature importance stability (weekly Spearman)

**Dashboard 3: Linkage Health**
- Per-linkage optimal lag trend (monthly)
- Per-category Brier score (rolling 60-event)
- Signal half-life trends (monthly)
- Active vs disabled linkages

### 10.2 Alerting Rules

| Alert | Condition | Channel | Action |
|---|---|---|---|
| Critical Insider | Anomaly score > 7.0 | Slack + Telegram | Review immediately |
| Feed Stall | No PM data for 5 min | Slack | Check API; flatten if persists |
| Model PSI Breach | PSI > 0.25 | Slack | Halt trading; retrain |
| Drawdown Breach | PM drawdown > 2% NAV | Slack + Telegram | Auto-flatten; 30-day cooldown |
| Stage 1 AUC Drop | AUC < 0.60 on weekly check | Slack | Retrain; reduce size |

---

## 11. Testing Strategy

### 11.1 Unit Tests (run on every commit)

- `test_anomaly.py`: verify composite score computation with known inputs; edge cases (new wallet, 100% win rate, pre-resolution timing)
- `test_time_adjuster.py`: verify all three bet type modes produce correct features; barrier option math matches known values
- `test_signal_detector.py`: verify velocity classification thresholds; volume confirmation logic
- `test_risk_engine.py`: verify all 8 gates; Half-Kelly sizing math; circuit breaker state transitions
- `test_sizing.py`: verify Kelly formula, category caps, crowding multiplier, aggregate exposure cap

### 11.2 Integration Tests (run weekly)

- `test_ingestion_pipeline.py`: verify Polymarket and Kalshi collectors write correct data to QuestDB and Redis
- `test_detection_pipeline.py`: end-to-end from raw trade → anomaly score → time-adjusted features → signal detection → Stage 1 classification
- `test_end_to_end.py`: full pipeline from PM data injection through to paper order submission (using mocked broker API)

### 11.3 Backtest Tests (run before model deployment)

- `test_stage1_walkforward.py`: purged walk-forward validation of Stage 1; verify AUC > 0.65 on all OOS folds
- `test_stage2_walkforward.py`: purged walk-forward of Stage 2; verify Sharpe > 0.5 on OOS
- `test_linkage_validation.py`: verify 5-layer validation pipeline produces correct results on known PM-asset pairs

### 11.4 Replay Tests

- Maintain a library of historical "known events" (e.g., Iran strike Feb 2026, Taylor Swift engagement Aug 2025, Fed rate decisions)
- Replay these through the full pipeline and verify the system detects the signal, correctly classifies it, identifies the correct linked asset, and would have generated a profitable trade recommendation
- This is the most important test category — it validates the entire system on ground truth

---

## Appendix A: Key Dependencies

```
# Core
python = "3.12"
lightgbm = "4.5"
scikit-learn = "1.5"
scipy = "1.14"
statsmodels = "0.14"
pandas = "2.2"
numpy = "1.26"
polars = "1.0"  # Optional: faster than pandas for large datasets

# Data
questdb = "2.0"  # QuestDB Python client (ILP)
redis = "5.0"    # Redis Python client
pyarrow = "17.0" # Parquet I/O
duckdb = "1.0"   # Analytical queries on Parquet

# ML / Experiment Tracking
mlflow = "2.16"
optuna = "4.0"   # Hyperparameter optimization (Phase 3+)

# Crypto / Blockchain
ccxt = "4.4"
web3 = "7.0"     # Polygon RPC interaction

# Broker APIs
alpaca-py = "0.30"
ib_async = "1.0"

# Monitoring
grafana-client = "4.0"  # Optional: programmatic dashboard management

# Notifications
slack-sdk = "3.30"
python-telegram-bot = "21.0"

# Testing
pytest = "8.3"
pytest-asyncio = "0.24"
```

---

## Appendix B: Phase-to-Component Mapping

| Phase | Components to Build | Components to Skip |
|---|---|---|
| 1 (Weeks 1-2) | Polymarket Collector, Kalshi Collector, QuestDB schema, Redis setup, config.py | Wallet Tracer, all Detection, all Models, all Execution |
| 2 (Weeks 3-4) | Anomaly Detector, Wallet Tracer, notifications | Time Adjuster, Signal Detector, Models, Execution |
| 3 (Weeks 5-8) | Time Adjuster, Signal Detector, Stage 1 Classifier, Linkage Validator (all 5 layers), Feature Store, vol_surface, decay profiler | Stage 2, Risk Engine, Order Router, Execution adapters |
| 4 (Weeks 9-12) | Stage 2 Meta-Learner, Risk Engine, self-training loop, Health Monitor | Order Router, Execution adapters (log recommendations only) |
| 5 (Weeks 13-16) | Order Router, Alpaca adapter (paper), full monitoring dashboards, circuit breakers | IBKR/CCXT adapters (paper only via Alpaca) |
| 6 (Weeks 17-20) | IBKR adapter (live), CCXT adapter (live), go/no-go evaluation | N/A — all components built |
