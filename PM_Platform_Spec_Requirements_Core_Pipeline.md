# Prediction Market Signal Intelligence Platform
# Specification Requirements — Core Pipeline
# Ingestion Layer + Detection Layer + Stage 1 Classifier

**Version:** 1.0  
**Date:** April 23, 2026  
**Scope:** Components required for Phases 1–3 of the build roadmap  
**Companion Document:** PM_Platform_Technical_Design.md  

---

## Document Conventions

- **REQ-XX-NNN**: Requirement ID. XX = component prefix, NNN = sequence number.
- **Priority**: P0 = must have for Phase gate, P1 = should have, P2 = nice to have.
- **Acceptance Criteria**: testable conditions that prove the requirement is met.
- Requirements reference parameters from `settings.yaml` using `${param.path}` notation.

---

## Table of Contents

1. [PMC — Polymarket Collector](#1-pmc--polymarket-collector)
2. [KAL — Kalshi Collector](#2-kal--kalshi-collector)
3. [WLT — Polygon Wallet Tracer](#3-wlt--polygon-wallet-tracer)
4. [ANM — Anomaly Detector](#4-anm--anomaly-detector)
5. [TAJ — Time Adjuster](#5-taj--time-adjuster)
6. [SIG — Signal Detector](#6-sig--signal-detector)
7. [SC1 — Stage 1 Classifier](#7-sc1--stage-1-classifier)
8. [Cross-Cutting Requirements](#8-cross-cutting-requirements)

---

## 1. PMC — Polymarket Collector

### 1.1 Functional Requirements

#### REQ-PMC-001: Market Discovery (P0)
**Description:** The collector SHALL discover all active Polymarket markets on startup and poll for new markets periodically.

**Acceptance Criteria:**
- On startup, fetches the complete list of active (non-resolved) markets from the Gamma API (`GET /markets?closed=false&active=true`)
- Stores each market in the `pm_markets` QuestDB table with all metadata fields populated
- After initial load, polls for new/updated markets every `${ingestion.polymarket.poll_interval_seconds}` seconds
- Newly created markets are detected and added within 2 poll cycles (30 seconds at default settings)

**Edge Cases:**
- API returns paginated results: collector MUST follow pagination links until all markets are retrieved. The Gamma API uses `offset` + `limit` parameters. Default page size is 100; collector should request 500 per page to minimize round trips.
- API returns a market with missing fields (e.g., no `end_date_iso`, no `outcomes`): log warning with market ID, store with NULL values for missing fields, flag as `bet_type = 'unknown'` — do NOT skip the market entirely.
- API returns a market that was previously marked as resolved: ignore the update (do not un-resolve a market).


#### REQ-PMC-002: Probability Ingestion (P0)
**Description:** The collector SHALL compute and store 1-minute probability bars for every active market.

**Acceptance Criteria:**
- For each active market, fetches the current order book from the CLOB API (`GET /book?token_id={yes_token_id}`) at the configured poll interval
- Computes probability as: `prob = (best_bid + best_ask) / 2` on the YES token outcome
- If order book is empty (no bids or no asks), falls back to last trade price from `GET /trades?asset_id={yes_token_id}&limit=1`
- Aggregates polls within each 1-minute window into a single probability bar: uses the last poll value within the minute as the bar's `probability`, sums `volume_usd`, counts `trade_count`
- Writes completed 1-minute bars to QuestDB `pm_probabilities` table within 5 seconds of the minute boundary
- Publishes each bar to Redis Stream `pm:probabilities` immediately upon computation

**Edge Cases:**
- Order book has bids but no asks (or vice versa): use the available side as the probability. If only bids exist, `prob = best_bid`. If only asks exist, `prob = 1 - best_ask_on_no_token` (if NO token book available) or `prob = best_ask` as approximation. Log info-level message.
- Order book has a crossed market (best_bid > best_ask): use `prob = best_ask` (the tighter side is more informative). Log warning.
- Both order book AND last trade are unavailable for a market: skip this market for this poll cycle. Do NOT write a bar with probability = 0 or probability = NULL. After 10 consecutive skips (2.5 minutes at default poll rate), log error and emit health metric `pm_market_stale`.
- Market resolves between polls: the next poll will return resolution data. Write a final probability bar with `probability = 1.0` (if resolved YES) or `probability = 0.0` (if resolved NO). Update `pm_markets` table with `resolved = true` and `outcome`.
- Probability falls outside [0, 1] due to calculation error: clamp to [0, 1] and log error. This should never happen but must be handled defensively.


#### REQ-PMC-003: Trade Ingestion (P0)
**Description:** The collector SHALL ingest individual trade events for all active markets.

**Acceptance Criteria:**
- Polls `GET /trades?asset_id={token_id}` for each active market at the configured interval
- Deduplicates trades by `trade_id` — each trade is written to QuestDB exactly once
- Writes to `pm_trades` table with all fields: timestamp, market_id, source='polymarket', wallet_address (the `maker` or `taker` address), side, price, size_usd, trade_id
- Publishes each new trade to Redis Stream `pm:trades`

**Edge Cases:**
- Trade has `size = 0` (dust trade): skip — do not write to DB or publish.
- Trade has a wallet address that is the Polymarket contract itself (not a user): skip. The CLOB proxy contract address should be in an exclusion list.
- Trades arrive out of order (trade with earlier timestamp arrives after a later one): write to QuestDB regardless (QuestDB handles out-of-order ingestion within its `commitLag` window). Set QuestDB `commitLag` to 60 seconds for the `pm_trades` table.
- Poll returns trades from a market that is no longer in the active market list: still process and store the trades (the market may have just resolved).
- Duplicate trade detected (same `trade_id` already in DB): skip silently. Do not log — this is expected behavior.


#### REQ-PMC-004: Historical Backfill (P0)
**Description:** On startup, the collector SHALL backfill historical data for the configured lookback period.

**Acceptance Criteria:**
- On first startup (empty `pm_probabilities` table), backfills `${ingestion.polymarket.backfill_days}` days of probability history for all active markets
- Uses the Gamma API `GET /markets/{id}/prices-history` endpoint with appropriate time range parameters
- Backfill runs at reduced request rate (2 req/s instead of 4) to avoid throttling
- Backfill is idempotent: running it again does not create duplicate rows (QuestDB deduplication on timestamp + market_id)
- Backfill completes before the collector enters steady-state polling. A startup flag `backfill_complete` gates the transition.
- Progress is logged: "Backfilling market {id}: {n}/{total} ({pct}%)"

**Edge Cases:**
- Backfill is interrupted mid-way (process crash): on restart, the collector detects partially backfilled state by querying `SELECT min(timestamp) FROM pm_probabilities WHERE market_id = '{id}'` and resumes from where it left off.
- API does not have history for the full requested period (new market, < 7 days old): backfill whatever is available. Do not treat as an error.
- Backfill discovers a market that has already resolved: backfill its full history anyway (resolved markets are valuable training data for Stage 1).


#### REQ-PMC-005: Market Classification (P1)
**Description:** The collector SHALL classify each market into a signal category and bet type.

**Acceptance Criteria:**
- On market discovery, classify `category` using keyword matching against the market title and description:
  - `fed_rate`: title contains "fed", "fomc", "rate cut", "rate hike", "basis point", "interest rate"
  - `earnings`: title contains "beat quarterly earnings", "miss quarterly earnings", "revenue", "EPS"
  - `geopolitical`: title contains "strike", "war", "ceasefire", "invasion", "sanctions", "military"
  - `recession`: title contains "recession", "GDP", "economic downturn", "contraction"
  - `crypto`: title contains "bitcoin", "BTC", "ethereum", "ETH", "crypto", "token"
  - `political`: title contains "election", "president", "governor", "senate", "congress", "legislation"
  - `other`: default if no keywords match
- Classify `bet_type` using market metadata:
  - `preset`: market has `end_date_iso` AND the end date corresponds to a known event (earnings date, FOMC date, sporting event). Heuristic: if the end date is within 1 hour of a known calendar event.
  - `expiring`: market has `end_date_iso` AND the title references a price threshold (contains "$", "above", "below", "hit", "reach"). Also: market has a non-null `strike_price` equivalent in its description.
  - `triggered`: market has no `end_date_iso` OR end date is > 365 days in the future
- Classification results are written to `pm_markets.category` and `pm_markets.bet_type`

**Edge Cases:**
- Market title matches multiple categories (e.g., "Will Fed rate cut cause recession?"): assign the first matching category in priority order: geopolitical > fed_rate > earnings > recession > crypto > political > other. The priority order reflects signal decay speed (faster signals first).
- Market title is in a non-English language: classify as `other`. Log info-level message.
- Market title is ambiguous (e.g., "Will X happen?"): classify as `other`. Do not guess.


### 1.2 Non-Functional Requirements

#### REQ-PMC-010: Availability (P0)
- The collector MUST run continuously (24/7) once started
- Automatic restart on crash via process supervisor (systemd or Docker `restart: always`)
- Mean time to recover from API outage: < 2 minutes (exponential backoff converges)

#### REQ-PMC-011: Latency (P0)
- Maximum data staleness: 20 seconds (probability values reflect market state within last 20 seconds)
- 1-minute bar publication: within 5 seconds of the minute boundary

#### REQ-PMC-012: Resource Usage (P1)
- Memory: < 500 MB steady state for up to 5,000 active markets
- CPU: < 0.5 cores steady state (bursty during backfill)
- Network: < 50 Mbps sustained

#### REQ-PMC-013: Logging (P0)
- Structured JSON logging with fields: `timestamp`, `level`, `component=pmc`, `market_id` (where applicable), `message`
- Log levels: DEBUG (every API call), INFO (market discovery, backfill progress, resolution events), WARNING (fallback to last trade, crossed markets), ERROR (consecutive poll failures, data quality issues)
- Log rotation: 7 days retention, max 500 MB per log file


### 1.3 Error Handling and Retry Logic

#### REQ-PMC-020: API Error Handling (P0)
**Retry Policy:**
- HTTP 429 (Rate Limited): wait for `Retry-After` header value, then retry. If no header, wait 60 seconds.
- HTTP 500/502/503/504 (Server Error): exponential backoff: 1s, 2s, 4s, 8s, 16s, cap at 60s. Max 10 retries, then skip this poll cycle and resume on next.
- HTTP 400 (Bad Request): do NOT retry. Log error with full request details. This indicates a code bug.
- HTTP 401/403 (Auth Error): do NOT retry. Log critical error. (Polymarket public API should never return these; if it does, something changed.)
- Connection timeout (10 second default): retry immediately once, then enter exponential backoff.
- DNS resolution failure: retry every 30 seconds for up to 5 minutes. If unresolved, emit critical health alert.

**Health Metrics:**
- `pmc_api_errors_total`: counter by error type (429, 5xx, timeout, dns)
- `pmc_api_latency_ms`: histogram of API response times
- `pmc_markets_active`: gauge of currently tracked markets
- `pmc_probabilities_written`: counter of bars written to QuestDB
- `pmc_last_successful_poll`: timestamp of last successful API response

#### REQ-PMC-021: QuestDB Write Error Handling (P0)
- If QuestDB is unreachable: buffer up to 1,000 bars in memory (FIFO queue). Continue polling. Retry QuestDB connection every 10 seconds. When connection is restored, flush buffer in order.
- If buffer exceeds 1,000 bars: drop oldest bars (log warning with count of dropped bars). This means QuestDB has been down for > 16 minutes (1,000 bars × 1 bar/min across all markets). Emit critical alert.
- If QuestDB write returns an error (malformed data): log the failed row, skip it, continue writing remaining rows.

#### REQ-PMC-022: Redis Stream Error Handling (P0)
- If Redis is unreachable: do NOT buffer. Redis Stream messages are ephemeral signals — if Redis is down, downstream consumers (Signal Detector, Anomaly Detector) cannot process them anyway. Log warning, continue writing to QuestDB. Resume Redis publishing when connection is restored.
- Redis stream maxlen: set `MAXLEN ~100000` on each stream to prevent unbounded memory growth.

---

## 2. KAL — Kalshi Collector

### 2.1 Functional Requirements

#### REQ-KAL-001: Market Discovery (P0)
**Description:** Identical to REQ-PMC-001 but for Kalshi markets.

**Acceptance Criteria:**
- On startup, fetches active markets from `GET /markets?status=open` with authentication
- Kalshi uses `ticker` as market identifier (e.g., `FED-26APR-T3.375`). The collector maps this to a normalized `market_id` format: `kalshi:{ticker}`
- Stores in `pm_markets` with `source = 'kalshi'`
- Kalshi markets include structured metadata: `category`, `sub_category`, `settlement_timer_seconds`, `expected_expiration_time`. Map these to our `category` and `bet_type` fields using the mapping:
  - Kalshi `category = 'Economics'` + `sub_category = 'Fed Funds Rate'` → our `category = 'fed_rate'`, `bet_type = 'preset'`
  - Kalshi `category = 'Economics'` + `sub_category` contains 'CPI' or 'Unemployment' → our `category = 'recession'`
  - Kalshi `category = 'Financials'` + `sub_category` contains 'Earnings' → our `category = 'earnings'`, `bet_type = 'preset'`
  - Kalshi `category = 'Politics'` → our `category = 'political'`
  - Default: `category = 'other'`

**Edge Cases:**
- Kalshi API requires pagination via `cursor` parameter, not `offset`. Follow cursor-based pagination.
- Kalshi returns markets in `finalized` status that were previously `open`: update `pm_markets` to `resolved = true`. Kalshi uses `result` field ('yes'/'no') for outcome.
- Rate limit (10 req/s public, 100 req/s authenticated): respect `X-RateLimit-Remaining` header. If remaining < 2, sleep until reset timestamp.


#### REQ-KAL-002: Probability Ingestion (P0)
**Description:** Compute and store 1-minute probability bars from Kalshi order book data.

**Acceptance Criteria:**
- Fetch order book via `GET /markets/{ticker}/orderbook` at configured poll interval
- Kalshi probabilities are in cents (0-100). Convert: `prob = yes_price / 100.0`
- If using WebSocket (authenticated): subscribe to `orderbook_delta` channel for real-time updates. Fall back to REST polling if WebSocket disconnects.
- Same 1-minute bar aggregation logic as PMC (REQ-PMC-002)
- Write to QuestDB `pm_probabilities` with `source = 'kalshi'`

**Edge Cases:**
- Kalshi order book uses `yes_price` and `no_price` which should sum to 100 (minus fees). If they sum to < 95 or > 105, the book is stale or broken — log warning, skip this poll.
- Kalshi WebSocket drops: reconnect with exponential backoff (1s, 2s, 4s). Fall back to REST polling during reconnection. Resume WebSocket when reconnected.
- Kalshi WebSocket sends heartbeat messages: acknowledge them but do not process as data.
- Auth token expires during long-running session: Kalshi tokens expire after a configurable period. Implement token refresh: re-authenticate via `POST /login` with stored credentials when receiving 401.


#### REQ-KAL-003: Trade Ingestion (P1)
**Description:** Ingest individual trade events from Kalshi.

**Acceptance Criteria:**
- Kalshi does not expose individual wallet addresses (it's a centralized exchange). Set `wallet_address = NULL` for all Kalshi trades.
- Fetch trades via `GET /markets/{ticker}/trades` with `cursor` pagination
- Deduplicate by Kalshi's trade ID
- Same storage pattern as PMC trades

**Edge Cases:**
- Kalshi trade API may lag behind order book API (trades appear after book moves): this is acceptable. Store trades as they arrive.


#### REQ-KAL-004: Cross-Platform Market Matching (P1)
**Description:** The Kalshi collector SHALL identify equivalent markets on Polymarket for cross-platform divergence detection.

**Acceptance Criteria:**
- For each Kalshi market, attempt to find a matching Polymarket market by:
  1. Exact category + date match (e.g., Kalshi `FED-26MAY` matches Polymarket "Fed decision in May" market)
  2. Keyword overlap in market titles (cosine similarity of tokenized titles > 0.7)
- Store the mapping in a Redis hash: `market_xref:{kalshi_ticker}` → `polymarket_market_id`
- Cross-reference is rebuilt on each market discovery cycle

**Edge Cases:**
- Multiple Polymarket markets match a single Kalshi market: select the one with highest volume.
- No Polymarket match found: set `xref = null`. Do not block any downstream processing.
- Match is ambiguous (similarity score between 0.5 and 0.7): store as `tentative_match`, require manual confirmation before use in cross-platform divergence.


### 2.2 Error Handling

#### REQ-KAL-020: Authentication Failure (P0)
- On 401/403: attempt token refresh (re-login) up to 3 times
- If all refresh attempts fail: log critical error, switch to "degraded mode" (no Kalshi data, PM signals rely on Polymarket only)
- Do NOT crash the entire system because Kalshi auth fails — Polymarket Collector continues independently

#### REQ-KAL-021: API Error Handling (P0)
- Same retry policy as REQ-PMC-020
- Additional: Kalshi returns `429` with a `Retry-After` header in seconds. Always respect this value (typically 1-10 seconds).

---

## 3. WLT — Polygon Wallet Tracer

### 3.1 Functional Requirements

#### REQ-WLT-001: On-Demand Wallet Tracing (P0)
**Description:** When requested by the Anomaly Detector, trace a Polymarket wallet's on-chain metadata.

**Acceptance Criteria:**
- Listens on Redis Stream `wallets:trace_request`
- For each request (a wallet address), retrieves:
  1. **Wallet age**: timestamp of the wallet's first-ever transaction on Polygon. Query: `eth_getTransactionCount` first, then binary-search block range with `eth_getBlockByNumber` to find the first transaction block.
  2. **Funding sources (2-hop)**: query USDC Transfer events (`topic0 = Transfer`) where `to = wallet_address`. The `from` address of the largest USDC transfer is `funding_source_1`. Repeat for `funding_source_1` to get `funding_source_2`.
  3. **Transaction count**: `eth_getTransactionCount(wallet_address, 'latest')`
  4. **Distinct Polymarket markets**: count unique `condition_id` values in the wallet's Polymarket CLOB trades (from `pm_trades` table, not from RPC)
  5. **Total volume**: sum of `size_usd` from `pm_trades` for this wallet
- Stores results in `pm_wallets` QuestDB table
- Caches results in Redis hash `wallet:{address}` with TTL of `${ingestion.wallet_tracer.retrace_interval_hours}` hours
- Responds on Redis Stream `wallets:trace_response` with the wallet profile

**Edge Cases:**
- Wallet address is a contract (not an EOA): detect by calling `eth_getCode(address)`. If code length > 2 (not `0x`), mark as `is_contract = true`. Contracts are typically Polymarket proxy wallets — trace the contract's deployer address instead.
- Wallet has zero transactions (newly created, never used outside Polymarket): set `first_seen = current_time`, `total_transactions = 0`. This is the strongest freshness signal.
- USDC transfer path involves a DEX aggregator or bridge contract (common for cross-chain funding): trace stops at the aggregator. Set `funding_source_1 = aggregator_address` and `funding_source_2 = 'unknown_via_aggregator'`. Do NOT attempt to trace through DEX routers — the logic is too complex and unreliable.
- Wallet has > 10,000 USDC transfers (whale or market maker): cap the funding source search at the 20 largest transfers. Log info: "High-activity wallet, funding analysis limited to top 20 transfers."
- RPC node returns stale data (block number behind chain head by > 100 blocks): detect by comparing returned block number to expected block number (Polygon produces blocks every ~2 seconds). If stale, switch to backup RPC endpoint.


#### REQ-WLT-002: Batch Wallet Screening (P2)
**Description:** Periodically re-trace all wallets that have been active in the last 7 days to update their profiles.

**Acceptance Criteria:**
- Runs daily at 03:00 UTC (low-activity period)
- Queries `pm_wallets` for wallets with `last_traced < now() - ${retrace_interval_hours} hours` AND has trades in last 7 days
- Processes at max `${ingestion.wallet_tracer.max_concurrent_rpc}` concurrent requests
- Updates existing records (does not create duplicates)


### 3.2 Error Handling

#### REQ-WLT-020: RPC Error Handling (P0)
- RPC timeout (30 second default): retry once with 5 second delay, then skip this wallet. Set wallet profile to `trace_status = 'failed'`. The Anomaly Detector must handle missing wallet profiles gracefully (see REQ-ANM-003).
- RPC rate limit (429 or provider-specific): exponential backoff, cap at 60 seconds. Max 5 retries per wallet.
- RPC returns inconsistent data (e.g., transaction count decreases): log error, use the higher value. This indicates RPC node inconsistency.
- All RPC endpoints fail: emit health alert `wlt_rpc_unavailable`. The system continues without wallet tracing — Anomaly Detector falls back to trade-data-only scoring (see REQ-ANM-003).

#### REQ-WLT-021: Resource Protection (P0)
- Max concurrent RPC calls: `${ingestion.wallet_tracer.max_concurrent_rpc}` (default: 5)
- Max RPC calls per wallet trace: 20 (prevents runaway tracing on complex wallets)
- Max total RPC calls per hour: 5,000 (prevents cost overruns on paid RPC providers)
- If hourly limit reached: queue remaining trace requests for next hour

---

## 4. ANM — Anomaly Detector

### 4.1 Functional Requirements

#### REQ-ANM-001: Per-Trade Anomaly Scoring (P0)
**Description:** For every trade event, compute a composite suspicion score.

**Acceptance Criteria:**
- Subscribes to Redis Stream `pm:trades` (consumer group: `anomaly_detector`)
- For each trade:
  1. Look up wallet profile from Redis cache `wallet:{address}`
  2. If wallet not in cache: publish trace request to `wallets:trace_request`, process trade with partial scoring (see REQ-ANM-003)
  3. Compute all 5 signal component scores using algorithms from tech design Section 7.3
  4. Compute weighted composite score
  5. Write to `pm_anomaly_scores` QuestDB table
  6. Publish to Redis Stream `anomaly:scores`
  7. If score > `${detection.anomaly.alert_threshold_watch}`: publish to `anomaly:alerts` with `alert_level = 'watch'`
  8. If score > `${detection.anomaly.alert_threshold_critical}`: publish to `anomaly:alerts` with `alert_level = 'critical'`, trigger notification (Slack/Telegram)
- Processing latency: < 500ms per trade from receipt to score publication

**Edge Cases:**
- Trade is from Kalshi (no wallet address): skip wallet-based signals (freshness, sybil). Compute only bet_size_anomaly and pre_resolution_timing based on aggregate market data. Maximum possible score for Kalshi trades is 4.0 (no wallet signals contribute). This means Kalshi trades will rarely trigger CRITICAL alerts, which is expected — Kalshi insider detection relies on statistical patterns, not wallet analysis.
- Wallet has only 1 prior trade (insufficient history for statistical tests): set `statistical_improbability_score = 0` (insufficient data, not suspicious). Set `bet_size_anomaly_score` based on comparison to market median instead of wallet history.
- Market has not yet resolved (pre_resolution_timing cannot be computed): set `pre_resolution_score = 0` for live scoring. This score is only fully computed retroactively after resolution for training data generation.
- Trade size is $0.01 (minimum possible): skip anomaly scoring entirely. This is a probe/test trade.


#### REQ-ANM-002: Rolling Wallet Statistics (P0)
**Description:** Maintain aggregate statistics per wallet for efficient scoring.

**Acceptance Criteria:**
- For each wallet, maintain in Redis hash `wallet:{address}:stats`:
  - `total_trades`: int
  - `total_markets`: int (distinct market_ids)
  - `total_volume_usd`: float
  - `win_count`: int (trades on the correct side of a resolved market)
  - `loss_count`: int
  - `avg_trade_size_usd`: float
  - `trade_size_variance`: float (for z-score computation)
  - `last_trade_timestamp`: ISO 8601
- Updated on every trade event
- Win/loss counts are updated when a market resolves (retroactive update)

**Edge Cases:**
- Market resolves as `N/A` (invalid resolution, dispute): do not count toward win or loss. Mark all trades on this market as `outcome = 'void'`.
- Wallet address appears with different casing (Ethereum addresses are case-insensitive but hex characters may vary): normalize all addresses to lowercase before lookup.


#### REQ-ANM-003: Graceful Degradation Without Wallet Data (P0)
**Description:** When wallet tracing is unavailable or incomplete, the Anomaly Detector MUST still produce valid (reduced) anomaly scores.

**Acceptance Criteria:**
- If wallet profile is missing (not in cache, trace not yet completed):
  - Set `wallet_freshness_score = 1.0` (assume moderately suspicious — the wallet is unknown)
  - Set `sybil_cluster_score = 0.0` (cannot compute without funding source)
  - Set `statistical_improbability_score = 0.0` (no history)
  - Compute `bet_size_anomaly_score` using market-level median trade size as baseline
  - Compute `pre_resolution_timing_score` normally (depends on market timing, not wallet)
  - Maximum possible composite score for unknown wallet: `1.0×0.15 + 0×0.25 + timing×0.25 + size×0.15 + 0×0.20 = 0.15 + up to 2.5 + up to 1.5 = 4.15`
  - This means an unknown wallet can trigger a WATCH alert (threshold 4.5 = close but unlikely) but not a CRITICAL alert without further wallet data
  - When wallet trace completes asynchronously, re-score recent trades (last 60 minutes) from this wallet and update scores in QuestDB

**Edge Cases:**
- Wallet Tracer is completely disabled (`${WALLET_TRACING_ENABLED} = false`): all wallets treated as unknown. System operates in "trade-data-only" mode. Reduced sensitivity is acceptable — document this in health dashboard.
- Wallet trace request queue backs up (> 100 pending requests): log warning, continue processing with partial scores. Do not block trade processing waiting for trace responses.


#### REQ-ANM-004: Sybil Cluster Detection (P1)
**Description:** Identify groups of wallets controlled by the same entity.

**Acceptance Criteria:**
- When a wallet trace completes (funding sources known), check if any other known wallets share the same `funding_source_1` or `funding_source_2`
- If matches found AND those wallets are betting on the same market in the same direction within a 60-minute window:
  - Group them into a cluster
  - Store cluster in Redis: `sybil_cluster:{funding_source}` → set of wallet addresses
  - Set `sybil_cluster_score` based on cluster size (algorithm per tech design Section 7.3)
- Clusters expire after 7 days of inactivity (no new trades from any member)

**Edge Cases:**
- Funding source is a well-known exchange hot wallet (Coinbase, Binance, etc.): exclude from clustering. Maintain a list of known exchange addresses (at least top 20 exchanges). Two wallets funded from Coinbase are not necessarily the same entity.
- Cluster grows to > 20 wallets: cap the cluster_score at its maximum (2.0) but continue tracking. Log warning: "Large sybil cluster detected: {count} wallets from {funding_source}."
- Two clusters merge (wallet A was in cluster X, but a new funding trace shows wallet A's funder is also in cluster Y): merge the clusters. Use the larger cluster's ID.


### 4.2 Non-Functional Requirements

#### REQ-ANM-010: Throughput (P0)
- MUST process at least 100 trades per second (peak Polymarket volume during major events)
- Processing latency: < 500ms from trade receipt to anomaly score publication (p99)

#### REQ-ANM-011: State Recovery (P0)
- On restart, wallet statistics (`wallet:{address}:stats`) persist in Redis (AOF enabled)
- On restart, the consumer group position on `pm:trades` stream is recovered (Redis consumer group tracks last-acknowledged message)
- No trades are lost on restart (at-least-once processing; duplicates are acceptable and handled by idempotent scoring)

---

## 5. TAJ — Time Adjuster

### 5.1 Functional Requirements

#### REQ-TAJ-001: Mode A — Preset Endpoint Processing (P0)
**Description:** For markets with a known, fixed resolution time tied to a scheduled event, compute time-adjusted features that account for the characteristic noise profile at each lifecycle stage.

**Acceptance Criteria:**
- Triggered when a new 1-minute probability bar arrives for a market with `bet_type = 'preset'`
- Computes `minutes_remaining = (resolution_time - current_time) / 60`
- Computes `time_fraction_remaining = minutes_remaining / total_market_duration_minutes`
- Looks up `expected_noise_std` from the volatility surface: `vol_surfaces[category][time_bucket]` where time_bucket is determined by `minutes_remaining`:
  - Buckets: `[0-5, 5-15, 15-30, 30-60, 60-240, 240-1440, 1440+]`
- Computes `noise_adjusted_signal = prob_change_15m / max(expected_noise_std, 0.001)`
- Computes `time_confidence`:
  - `time_fraction_remaining < ${detection.time_adjuster.preset_late_stage_threshold}` → 1.0
  - `time_fraction_remaining < 0.30` → 0.8
  - `time_fraction_remaining < 0.70` → 0.6
  - `time_fraction_remaining >= ${detection.time_adjuster.preset_early_stage_threshold}` → 0.3
- Sets `information_premium = 0.0` (not applicable for preset endpoints)
- Sets `excess_vs_time_decay = prob_change_15m` (no time decay to subtract)
- Writes feature vector to Feature Store (Redis hash + Parquet)

**Edge Cases:**
- `prob_change_15m` is exactly 0.0: output features are all zero. This is correct — no signal.
- `minutes_remaining < 0` (current time is past resolution time but market hasn't resolved yet): set `minutes_remaining = 0`, `time_fraction_remaining = 0`, `time_confidence = 1.0`. This happens when resolution is delayed (e.g., earnings report late). Continue processing.
- Volatility surface has no entry for this category (new category, no historical data): use a default noise_std of 0.02 (2 percentage points). Log warning: "No vol surface for category {cat}, using default."
- `total_market_duration_minutes = 0` (market created and resolving in the same minute): set `time_fraction_remaining = 0`, `time_confidence = 1.0`.


#### REQ-TAJ-002: Mode B — Expiring Contract Processing (P0)
**Description:** For markets with a price threshold and deadline, compute the information premium that separates market intelligence from mechanical time decay.

**Acceptance Criteria:**
- Triggered when a new probability bar arrives for a market with `bet_type = 'expiring'`
- Requires `strike_price` and `underlying_ticker` from market metadata, AND current underlying price from Market Context Service
- Computes theoretical probability using barrier option formula (tech design Section 7.2):
  - `T = minutes_remaining / (252 * 6.5 * 60)` for equity underlyings
  - `T = minutes_remaining / (365 * 24 * 60)` for crypto (24/7 markets)
  - Use `${detection.time_adjuster.expiring_risk_free_rate}` for risk-free rate
  - Implied volatility: estimate from the option chain if available, otherwise use 30-day realized vol of the underlying × 1.2 (vol premium factor)
- Computes `information_premium = current_market_prob - theoretical_prob`
- Computes `excess_vs_time_decay = market_prob_change_15m - theoretical_prob_change_15m`
- Computes `time_signal_quality` and `time_execution_risk` per minute_remaining thresholds:
  - < 60 min: quality=0.9, risk=0.8
  - < 1440 min (1 day): quality=0.7, risk=0.3
  - < 10080 min (1 week): quality=0.5, risk=0.1
  - else: quality=0.3, risk=0.05
- `time_confidence = time_signal_quality × (1 - time_execution_risk)`
- Computes `noise_adjusted_signal = premium_change_15m / max(historical_premium_volatility, 0.001)`

**Edge Cases:**
- Underlying price is unavailable (Market Context Service is down): cannot compute theoretical probability. Fall back to Mode A (preset endpoint) behavior — treat as if no time decay model is available. Set `information_premium = NaN`, use raw probability change. Log warning.
- Strike price is not parseable from market metadata (ambiguous description): classify as `bet_type = 'triggered'` instead and process with Mode C. Log warning.
- Theoretical probability computes to > 1.0 or < 0.0 (numerical instability in Black-Scholes with extreme inputs): clamp to [0.001, 0.999]. This can happen with very short time remaining and price near strike.
- Underlying price is exactly equal to strike price: `d2 = 0`, `theoretical_prob = 0.5` (correct). No special handling needed.
- Implied volatility estimate is unreasonably high (> 500% annualized) or low (< 5%): clamp to [5%, 500%] and log warning.


#### REQ-TAJ-003: Mode C — Outcome-Triggered Processing (P0)
**Description:** For markets with no expiry, subtract the empirical drift rate to isolate information-driven probability changes.

**Acceptance Criteria:**
- Triggered when a new probability bar arrives for a market with `bet_type = 'triggered'`
- Looks up `empirical_drift_rate` for this category from pre-computed drift table (`config/drift_rates.json`)
  - Drift rate units: probability change per minute (typically very small, e.g., 0.000001)
  - If no drift rate available for this category: use 0.0 (assume no drift)
- Computes `baseline_drift_15m = empirical_drift_rate × 15`
- Computes `information_change_15m = prob_change_15m - baseline_drift_15m`
- Computes `noise_adjusted_signal = information_change_15m / max(noise_threshold, 0.001)` where noise_threshold is the historical standard deviation of 15-minute probability changes for this category
- Computes `time_confidence = ${detection.time_adjuster.triggered_confidence_discount} × maturity_factor` where:
  - `market_age_minutes < 10080` (7 days): maturity_factor = 0.3
  - `market_age_minutes < 43200` (30 days): maturity_factor = 0.6
  - else: maturity_factor = 1.0
- Sets `information_premium = 0.0`
- Sets `excess_vs_time_decay = information_change_15m`

**Edge Cases:**
- Market age is 0 minutes (just created): set `maturity_factor = 0.1` (very low confidence). First few minutes of any market are price discovery, not signal.
- Probability is at 0.99 or 0.01 (near certainty): the market is effectively resolved. Any shift away from the extreme is highly informative — do NOT discount. Adjust noise_threshold to reflect that near-extreme probabilities have lower typical noise.
- Drift rate is negative for a category (probability tends to decrease over time): this is valid for some categories (e.g., "Will X happen in 2026?" probability decreases as year passes without the event). Use the negative drift rate as-is.


#### REQ-TAJ-004: Volatility Surface Construction (P1)
**Description:** Build and maintain the category × time-remaining volatility surface used by Mode A.

**Acceptance Criteria:**
- Built from historical resolved markets: for each category, collect all 15-minute probability changes, bucket by minutes_remaining, compute standard deviation per bucket
- Minimum 30 data points per bucket for statistical reliability. If fewer, merge with adjacent bucket.
- Stored in `config/vol_surfaces.json`
- Rebuilt monthly during the self-training loop (or on-demand via `scripts/build_vol_surface.py`)
- On startup, loaded into memory. If file missing, use default surface (all buckets = 0.02)

**Edge Cases:**
- A category has zero resolved markets (brand new category): use the `other` category's surface as default.
- A bucket has extreme outlier volatility (> 10× adjacent buckets): clip to 3× the median of adjacent buckets. This prevents a single extreme event from distorting the entire surface.


### 5.2 Non-Functional Requirements

#### REQ-TAJ-010: Latency (P0)
- Feature computation: < 50ms per market per minute (must not be a bottleneck)
- Total Time Adjuster throughput: at least 5,000 markets × 1 computation/minute = ~83 computations/second

#### REQ-TAJ-011: Determinism (P0)
- Given the same inputs (probability history, market metadata, volatility surface), the Time Adjuster MUST produce identical outputs. No randomness. This is critical for backtest-live parity.

---

## 6. SIG — Signal Detector

### 6.1 Functional Requirements

#### REQ-SIG-001: Multi-Window Velocity Analysis (P0)
**Description:** Detect actionable prediction market signals using probability velocity across multiple time windows.

**Acceptance Criteria:**
- Triggered every minute for every active market (pulls latest features from Feature Store)
- Computes probability changes over 4 windows: 5m, 15m, 60m, 240m
- Computes velocity for each: `velocity = abs(prob_change) / window_minutes`
- Selects the fastest window (highest velocity)
- Classifies signal speed per velocity thresholds (tech design Section 3.6)
- Applies minimum shift gate: `abs(prob_change_fastest_window) >= ${detection.signal.min_prob_shift}`
- If gate passes: publish signal to Redis Stream `signals:raw`

**Edge Cases:**
- Market has < 5 minutes of history (just discovered): can only compute 5m window. Use it alone. Set other windows to `None`.
- Market has < 15 minutes of history: compute 5m and partial 15m. 60m and 240m are `None`.
- All windows show zero change: no signal. Skip.
- Multiple windows show significant velocity: report the fastest (shortest window with highest velocity). In the signal output, include ALL window velocities — Stage 1 may find the combination informative.
- Probability change is positive in 5m window but negative in 60m window (reversal): this is NOT a signal. The velocity is high but the direction is uncertain. Skip. Require directional consistency: the 5m and 15m windows must agree in direction. The 60m and 240m windows are context but not gating.


#### REQ-SIG-002: Volume Confirmation (P0)
**Description:** Signals must be accompanied by above-average volume.

**Acceptance Criteria:**
- Compute `recent_volume = sum(volume_usd) over last 15 minutes`
- Compute `avg_volume_15m = sum(volume_usd over last 7 days) / (7 × 24 × 4)` (number of 15-minute windows in 7 days)
- `volume_ratio = recent_volume / max(avg_volume_15m, 1.0)`
- Signal passes volume gate if `volume_ratio >= ${detection.signal.volume_confirmation_ratio}`

**Edge Cases:**
- Market is < 7 days old (insufficient history for average): use available history. If < 1 day old, set `avg_volume_15m` to the average of all available 15-minute windows.
- Market has zero volume in the last 7 days (completely dead): `avg_volume_15m = 0`, which means `volume_ratio = infinity`. This is misleading — a single tiny trade would trigger. Solution: require `recent_volume >= $10` absolute minimum in addition to the ratio check. Markets with < $10 volume in 15 minutes are not liquid enough to be informative.
- Volume is high but all from a single trade: the volume confirmation passes, but note `volume_concentration` (HHI) will be high. This is useful context for Stage 1 (a single large bet from one wallet is different from many small bets).


#### REQ-SIG-003: Cross-Platform Divergence Check (P1)
**Description:** When a cross-platform match exists, check if Kalshi and Polymarket agree on direction.

**Acceptance Criteria:**
- Look up cross-reference: `market_xref:{market_id}` from Redis
- If match exists and `${detection.signal.cross_platform_required} = true`:
  - Compute probability change on the matched platform over the same fastest window
  - `cross_platform_aligned = sign(pm_change) == sign(kalshi_change)`
  - If not aligned: suppress signal (do not emit to `signals:raw`)
- If match exists and `cross_platform_required = false`:
  - Include `cross_platform_aligned` as a feature in the signal output but do NOT gate on it
  - Also include `cross_platform_divergence = abs(polymarket_prob - kalshi_prob)` — this is informative for Stage 1
- If no match exists: set `cross_platform_aligned = null`, `cross_platform_divergence = null`

**Edge Cases:**
- Cross-reference exists but the matched market has not been polled yet in this cycle (stale data): use the last available probability from the matched platform. If > 5 minutes stale, set `cross_platform_aligned = null`.
- Polymarket and Kalshi have the same event but slightly different resolution criteria: the cross-reference may be valid but probabilities may legitimately differ. Do not treat legitimate divergence as misalignment — divergence > 15% is flagged as a `DIVERGENCE_SIGNAL` (a potential cross-platform arbitrage opportunity), not as suppression.


#### REQ-SIG-004: Signal Deduplication (P0)
**Description:** Prevent emitting duplicate signals for the same probability shift as it develops over consecutive minutes.

**Acceptance Criteria:**
- After emitting a signal for a market, set a cooldown period: no new signal for this market for `cooldown_minutes = max(recommended_entry_minutes × 2, 30)` minutes
- During cooldown, if the probability continues to shift in the same direction AND the velocity increases by > 50%: emit an `ACCELERATION` update signal (same `signal_id`, updated velocity and magnitude)
- During cooldown, if the probability reverses direction: clear the cooldown immediately (the original signal may be invalidated)
- Store active cooldowns in Redis: `signal_cooldown:{market_id}` with TTL

**Edge Cases:**
- Market resolves during cooldown period: clear cooldown. The signal can be retroactively labeled (correct/incorrect) immediately.
- System restarts during a cooldown period: cooldowns are in Redis with TTL — they survive restart.

---

## 7. SC1 — Stage 1 Classifier

### 7.1 Functional Requirements

#### REQ-SC1-001: Signal Classification (P0)
**Description:** For each detected signal, produce a calibrated probability that the PM signal is "correct" (probability shifted toward the eventual outcome).

**Acceptance Criteria:**
- Subscribes to Redis Stream `signals:raw` (consumer group: `stage1_classifier`)
- For each signal:
  1. Assemble the 13-feature vector (tech design Section 3.7)
  2. Run inference: `raw_prob = model.predict_proba(features)[0, 1]`
  3. Apply Platt scaling calibration: `calibrated_prob = calibrator.predict_proba(raw_prob.reshape(-1,1))[0, 1]`
  4. Compute entropy: `entropy = -p*log2(p) - (1-p)*log2(1-p)` where p = calibrated_prob
  5. Enrich the signal with `signal_quality_prob`, `signal_quality_entropy`, `model_version`
  6. Publish to Redis Stream `signals:classified`
  7. Write to `pm_anomaly_scores` table (or a separate `pm_signal_classifications` table)
- Inference latency: < 10ms per signal (LightGBM is extremely fast)

**Edge Cases:**
- Model is not yet trained (`min_training_samples` not reached): do NOT block the pipeline. Publish the signal to `signals:classified` with `signal_quality_prob = 0.5` (maximum uncertainty), `signal_quality_entropy = 1.0`, `model_version = 'untrained'`. Downstream Stage 2 and Risk Engine will handle accordingly (joint confidence will be low, trade unlikely).
- Feature value is NaN (e.g., `information_premium = NaN` because underlying price was unavailable): LightGBM handles NaN natively (treats as missing). However, log a warning: "Feature {name} is NaN for signal {id}."
- Feature value is infinite (e.g., `volume_ratio = inf` because average volume was 0): replace with a large sentinel value (1000.0 for ratios, 9999 for minutes). Log warning.
- All `anomaly_context` features are zero (no insider signals): this is the common case. Most signals are NOT insider-informed. The model should still produce a valid prediction based on the remaining features (velocity, volume, time adjustment).


#### REQ-SC1-002: Model Training (P0)
**Description:** Train the Stage 1 classifier on historical labeled data.

**Acceptance Criteria:**
- Training data: historical signals with known outcomes (market resolved, we know if the probability shift was correct)
- Minimum training set: `${models.stage1.min_training_samples}` labeled events
- Training procedure:
  1. Load labeled data from `data/training/stage1/` Parquet files
  2. Split using purged walk-forward: 80% train, 20% test, with 7-day purge gap between train and test periods
  3. Train LightGBM with configured hyperparameters
  4. Calibrate with Platt scaling on a held-out 20% calibration set (separate from test)
  5. Evaluate on test set: compute AUC, accuracy, precision, recall, F1, Brier score
  6. Log all metrics to MLflow
  7. Save model artifacts to `data/models/stage1/{version}/`
  8. Save calibrator alongside model
- Gate: model is deployed only if test AUC > 0.60

**Edge Cases:**
- Training data is imbalanced (70% positive / 30% negative or vice versa): `class_weight='balanced'` handles this in LightGBM. Additionally, stratified splitting ensures both classes are represented in all folds.
- Training data has a temporal pattern (all recent signals are different from early signals): the purged walk-forward handles this — the test set is always the most recent period.
- Training fails to converge (LightGBM reports no improvement after 50 rounds): reduce `learning_rate` by 50%, increase `n_estimators` to 400, retrain. If still no convergence, log error and keep the previous model version.
- Previous model file is corrupted: detect by attempting to load and predict on a known test vector. If prediction fails, log error, fall back to "untrained" mode (output 0.5).


#### REQ-SC1-003: Model Versioning (P0)
**Description:** Maintain a versioned history of trained models with metadata.

**Acceptance Criteria:**
- Model version format: `stage1_v{N}_{YYYYMMDD}` (e.g., `stage1_v12_20260421`)
- Each version directory contains:
  - `model.lgb` — serialized LightGBM model
  - `calibrator.pkl` — serialized Platt scaling calibrator
  - `metadata.json` — training date, training set size, hyperparameters, test metrics
  - `feature_importances.json` — feature importance rankings
- Active model version stored in Redis: `system:active_model:stage1` → version string
- Previous 5 versions retained for rollback
- Rollback procedure: update Redis key to previous version, reload model in classifier process

**Edge Cases:**
- Disk full (cannot save new model): log critical error. Do NOT overwrite the active model. Emit alert.
- MLflow is unavailable: save model artifacts locally but skip MLflow logging. Do not block training.


#### REQ-SC1-004: Label Generation (P0)
**Description:** Automatically generate training labels when prediction markets resolve.

**Acceptance Criteria:**
- When a market resolution event is detected (market transitions to `resolved = true`):
  1. Query all signals that were emitted for this market (from `pm_anomaly_scores` or `pm_signal_classifications`)
  2. For each signal, determine if it was "correct":
     - Signal `direction = +1` (probability increasing) AND `outcome = 'yes'` → label = 1 (correct)
     - Signal `direction = +1` AND `outcome = 'no'` → label = 0 (incorrect)
     - Signal `direction = -1` AND `outcome = 'no'` → label = 1 (correct)
     - Signal `direction = -1` AND `outcome = 'yes'` → label = 0 (incorrect)
  3. Write labeled examples to `data/training/stage1/` Parquet files, partitioned by month
  4. Log: "Generated {N} training labels from market {market_id} resolution ({outcome})"

**Edge Cases:**
- Market resolves as `N/A` or is voided: do NOT generate labels. These are unusable training data.
- A market had 50+ signals (highly active market with many shifts): all signals are valid training examples. Do not subsample.
- Signal was emitted very close to resolution (< 5 minutes before): still generate the label, but add a feature `was_near_resolution = true` so the model can learn that near-resolution signals have different characteristics.
- The same market had signals in both directions (probability went up, then down): both are valid training examples with potentially different labels. A signal that predicted "up" is correct if the market resolved YES, regardless of whether a later signal predicted "down."


### 7.2 Non-Functional Requirements

#### REQ-SC1-010: Inference Latency (P0)
- < 10ms per signal (p99)
- Model must be loaded into memory on startup, not loaded from disk per-inference

#### REQ-SC1-011: Model Hot-Swap (P1)
- When a new model version is deployed (Redis key updated), the classifier process should detect the change and reload the model within 30 seconds, without dropping any signals
- Hot-swap procedure: load new model into a shadow variable, run a validation prediction, swap the active reference, release the old model

#### REQ-SC1-012: Throughput (P0)
- Must handle at least 50 signals per second (peak during major events)
- This is well within LightGBM's capabilities (typically 100K+ predictions/second)

---

## 8. Cross-Cutting Requirements

### 8.1 Data Integrity

#### REQ-XC-001: Append-Only Raw Data (P0)
- Raw ingested data (`pm_probabilities`, `pm_trades`, `pm_markets`) is NEVER modified or deleted
- Derived data (anomaly scores, features, classifications) may be recomputed but historical versions are retained

#### REQ-XC-002: Idempotent Processing (P0)
- Every component MUST handle duplicate messages gracefully
- Processing the same trade event twice produces the same anomaly score (no double-counting)
- Processing the same probability bar twice does not create duplicate entries in QuestDB (QuestDB deduplicates on timestamp + market_id within the commit lag window)

#### REQ-XC-003: Clock Synchronization (P0)
- All components use UTC exclusively. No local time zones anywhere in the codebase.
- Timestamps are ISO 8601 with timezone designator: `2026-04-23T14:30:00Z`
- System clock must be synchronized via NTP (< 1 second drift from authoritative time)
- All features involving time differences (minutes_remaining, market_age, etc.) use the PM event's timestamp, not the processing time, to ensure backtest-live parity


### 8.2 Observability

#### REQ-XC-010: Structured Logging (P0)
- All components log in structured JSON format
- Required fields: `timestamp`, `level`, `component`, `message`
- Optional fields: `market_id`, `signal_id`, `wallet_address`, `error_type`, `latency_ms`
- Levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
- Component prefixes: `pmc`, `kal`, `wlt`, `anm`, `taj`, `sig`, `sc1`

#### REQ-XC-011: Health Endpoint (P0)
- Each component exposes a health check function that returns:
  - `status`: `healthy`, `degraded`, `unhealthy`
  - `last_successful_operation`: timestamp
  - `error_count_5m`: errors in last 5 minutes
  - `component_specific_metrics`: (varies by component)
- Health checks are aggregated by the Health Monitor (built in Phase 4)


### 8.3 Configuration

#### REQ-XC-020: Centralized Configuration (P0)
- All parameters are defined in `config/settings.yaml`
- No magic numbers in code — every threshold, window size, and limit references a config parameter
- Config is loaded once on startup and cached in memory
- Runtime config overrides stored in Redis `system:config` — these take precedence over file-based config
- Config changes require a service restart (hot-reload is P2)

#### REQ-XC-021: Environment Variables for Secrets (P0)
- API keys, webhook URLs, and database credentials are NEVER in `settings.yaml`
- All secrets loaded from environment variables (naming convention: `PM_PLATFORM_{SERVICE}_{KEY}`)
- Required env vars documented in `.env.example`


### 8.4 Testing

#### REQ-XC-030: Unit Test Coverage (P0)
- Minimum 80% line coverage for all core pipeline components
- Every edge case documented in this spec MUST have a corresponding unit test
- Tests run in < 60 seconds total (no external dependencies — all APIs and databases mocked)

#### REQ-XC-031: Integration Test Suite (P1)
- Integration tests use Docker Compose to stand up QuestDB + Redis
- Tests inject known data through the pipeline and verify end-to-end output
- Test scenarios include:
  1. Normal signal detection (happy path)
  2. Market resolution with label generation
  3. API failure and recovery
  4. Wallet tracing unavailable (degraded mode)
  5. Cross-platform divergence detection
  6. Circuit breaker activation

#### REQ-XC-032: Replay Test Library (P1)
- Maintain a set of historical "golden" events with known outcomes:
  - Iran strike (Feb 2026): insider wallet activity → geopolitical signal → oil/VIX movement
  - Fed rate decision (Dec 2025): Kalshi/Polymarket convergence → TLT movement
  - Earnings miss (example from Wolfe Research): earnings PM shift → stock decline
- Each golden event has: raw trade data, expected anomaly scores, expected signal classification, expected linked asset movement
- Replay tests run the full pipeline on golden event data and assert outputs match expected values within tolerance

---

## Appendix A: Requirement Traceability to Build Phases

| Phase | Requirements Covered | Gate Criteria |
|---|---|---|
| Phase 1 (Weeks 1-2) | REQ-PMC-001 through 004, REQ-KAL-001 through 003, REQ-XC-001 through 003, REQ-XC-020-021 | Data flowing into QuestDB, 7+ days of history, 1-min resolution confirmed |
| Phase 2 (Weeks 3-4) | REQ-WLT-001, REQ-ANM-001 through 004, REQ-XC-010-011 | Anomaly scoring live, alerts firing to Slack/Telegram, manual outcome tracking started |
| Phase 3 (Weeks 5-8) | REQ-TAJ-001 through 004, REQ-SIG-001 through 004, REQ-SC1-001 through 004, REQ-XC-030-032 | Stage 1 AUC > 0.65 on OOS, 2+ linkages validated, vol surface constructed |

---

## Appendix B: Open Questions for Build Phase

1. **Polymarket CLOB API stability**: The CLOB API has been known to change endpoints without notice. Build the collector with an API abstraction layer that can be updated without modifying business logic.

2. **Kalshi WebSocket availability**: WebSocket access requires authentication and may have availability issues. The REST fallback must be the primary path, with WebSocket as an optimization.

3. **Polygon RPC provider selection**: Alchemy free tier (300M compute units/month) vs Infura free tier (100K requests/day) vs public endpoint (unreliable). Start with Alchemy free tier; budget $49/month for Alchemy Growth if free tier is insufficient.

4. **QuestDB vs TimescaleDB**: The tech design specifies QuestDB. If QuestDB proves difficult to integrate with the Python ecosystem, TimescaleDB (PostgreSQL extension) is a drop-in alternative with better tooling support. Decision point: end of Phase 1.

5. **Redis Streams vs simple pub/sub**: Redis Streams provide consumer groups and message persistence. If the overhead is not needed in early phases, simple pub/sub with in-memory buffering is simpler. Decision point: beginning of Phase 2.
