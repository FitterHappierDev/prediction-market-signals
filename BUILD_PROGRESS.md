# Build Progress — PM Signal Platform

**Last session:** 2026-05-19
**Phase:** 1 gate passed; Phase 2 starting
**Live status:** Polymarket + Kalshi minute bars + Polymarket trade tape writing to QuestDB. Systemd service has been up 52h with 0 restarts.

---

## Where to pick up

Phase 1 gate is passed (see *Phase 1 gate result* below). Begin Phase 2:

1. **AI STEP 2.1 — `src/ingestion/wallet_tracer.py`** (REQ-WLT-001/002): on-demand Polygon RPC traces via Alchemy. Listens on Redis stream `wallets:trace_request`, writes `pm_wallets`, caches `wallet:{address}`.
2. **AI STEP 2.2/2.3 — `src/detection/anomaly.py`** (REQ-ANM-001 through 004): 5-signal composite scoring + sybil clustering per tech design §7.3.
3. **AI STEP 2.4 — Notifications upgrade**: Slack block-kit + Telegram Markdown formatting for CRITICAL alerts (skeleton at [src/utils/notifications.py:40](src/utils/notifications.py:40)).
4. **HUMAN STEP 2.7** — start the manual outcome-tracking spreadsheet when CRITICAL alerts fire.

Tech design and spec reference:
- `PM_Platform_Technical_Design.md` — section 3.3 (Wallet Tracer), 3.4 (Anomaly Detector), 7.3 (composite score algorithm)
- `PM_Platform_Spec_Requirements_Core_Pipeline.md` — REQ-WLT-* and REQ-ANM-*

---

## Phase 1 gate result (2026-05-19)

Validated via SSH to EC2 after 52h continuous operation:

| Criterion | Required | Actual | Status |
|---|---|---|---|
| Service unattended | ≥24h | 52h, 0 restarts since 2026-05-17 03:08 UTC | ✅ |
| Both sources flowing in last hour | yes | PM: 4,280 bars / 102 markets; Kalshi: 8,812 bars / 8,812 markets | ✅ |
| PM ≥7 days history | ≥7d | 9.6 days (2026-05-09 → 2026-05-19) | ✅ |
| Kalshi from-now history | ~2d | 2.2 days | ✅ |
| PM trades flowing | yes | 1,396 trades / hour | ✅ |
| Latest bar freshness | <few min | 2:46 old vs wall clock | ✅ |
| EC2 clock sync | NTP | 268 ns offset (AWS Time Sync) | ✅ |

### Operational issues uncovered (NOT gate blockers, but address before Phase 3)

1. **Kalshi market count exploded to 17,110 over 24h** (vs documented ~50). At 4 r/s, full poll cycle is ~71 min — each Kalshi market gets one bar every ~71 minutes, not every minute. PM is healthy (102 markets, ~26s cycle). Likely cause: `derive_category_from_event` at [src/ingestion/kalshi.py:52](src/ingestion/kalshi.py:52) is too permissive — every event under Politics/Elections/Economics/Financials is tracked, and Kalshi opens many granular sub-markets (state-level, daily settlements, etc.). Filter more aggressively before Phase 3 needs Kalshi training data.
2. **Zero Kalshi trades ever written** (total count = 0). REQ-KAL-003 docstring at [src/ingestion/kalshi.py](src/ingestion/kalshi.py) claims it's implemented but the poll path is silently failing or never wired into the loop. P1 priority; Phase 3 training labels benefit from Kalshi trade context but Phase 2's anomaly detector is PM-only (Kalshi has no wallet addresses).

### Side-quest from this session: regaining SSH access

Home IP rotated from `97.113.95.181` → `97.113.147.5`. SSH timed out because **both** the AWS Security Group **and** UFW on the EC2 had the old IP whitelisted. Path that worked:

1. Update AWS SG `pm-platform-sg` inbound rules to new IP — fixed network-level block but not host-level.
2. UFW still blocked port 22 (Docker bypasses UFW so Grafana/MLflow on 3000/5000 still worked — that's the diagnostic clue).
3. EC2 Instance Connect failed too (UFW also blocked the EIC IP range).
4. Enabled **SSM DHMC** (Default Host Management Configuration) in the EC2 → Connect → SSM tab; agent came online without per-instance IAM role.
5. From the SSM browser shell, `sudo ufw allow from {new IP} ... && sudo ufw delete allow from {old IP} ...` on ports 22/3000/5000.

Moral: when home IP rotates, update **both** the SG **and** UFW. SSM DHMC is the rescue path if you forget.

---

## Infrastructure

| Component | Where | Notes |
|---|---|---|
| EC2 instance | AWS `us-east-2` (Ohio, not `us-east-1` as the build guide assumed) | t4g.medium, 4 GB RAM |
| Elastic IP | `18.225.93.218` | UFW + SG locked to home IP `97.113.95.181` |
| EBS data volume | mounted `/data`, 100 GB | survives instance termination |
| Swap | 4 GB on /swapfile | added because t4g.medium is tight |
| Docker stack | QuestDB 8.0 (3 GB), Redis 7 (512 MB), Grafana 11, MLflow 2.16 | `restart: always`; data under `/data/{questdb,redis,grafana,mlflow}` |
| Python venv | `/home/ubuntu/prediction-market-signals/.venv` | Python 3.12, deps pinned to `~=` ranges |
| systemd service | `pm-platform.service` | `sudo systemctl status pm-platform.service` |
| Daily backup cron | 03:00 UTC | `scripts/backup.sh`; S3 sync if `S3_BACKUP_BUCKET` is set |

### Access from your Mac

SSH:
```bash
ssh -i /Users/yen/Downloads/pm-platform-key.pem ubuntu@18.225.93.218
```

SSH tunnel for QuestDB web console (best data browser):
```bash
ssh -i /Users/yen/Downloads/pm-platform-key.pem -N \
    -L 9000:localhost:9000 -L 8812:localhost:8812 \
    ubuntu@18.225.93.218
# Then open http://localhost:9000
```

Grafana (already exposed to home IP): http://18.225.93.218:3000 — admin / `$GRAFANA_PASSWORD`
MLflow (same): http://18.225.93.218:5000

---

## Code layout

```
src/
├── main.py                    # asyncio runner, SIGINT/SIGTERM, 30s crash-restart
├── config.py                  # Pydantic v2 hierarchy mirroring config/settings.yaml
├── ingestion/
│   ├── polymarket_api.py      # Gamma + CLOB + Data API HTTP wrapper, retries, throttling
│   ├── polymarket.py          # Collector: discovery, 1-min bars, trade tape, backfill (REQ-PMC-001/002/003/004/005)
│   ├── kalshi_api.py          # Kalshi /events + /markets/* wrapper, auth modes (auto/bearer/basic/rsa)
│   └── kalshi.py              # Collector: walks /events?category=X to filter to Politics/Elections/Economics/Financials (REQ-KAL-001/002/003)
└── utils/
    ├── db.py                  # QuestDB client: ILP/TCP writes, HTTP reads, transactional buffer
    ├── redis_client.py        # Async wrapper for streams + hashes
    ├── time_utils.py          # UTC-only helpers, equity vs crypto hours
    └── notifications.py       # Async Slack + Telegram fanout

scripts/
├── provision_server.sh        # idempotent EC2 setup (REQ-INF-001)
├── harden_server.sh           # UFW + fail2ban + key-only SSH (REQ-INF-004)
├── backup.sh                  # daily QuestDB tar + S3 sync (REQ-INF-003)
├── pm-platform.service        # systemd unit
├── verify_foundation.py       # Phase 0 smoke test (HUMAN STEP 0.11)
├── inspect_polymarket.py      # one-shot probe of Gamma/CLOB/Data API shapes
├── inspect_kalshi.py          # auth-mode probe
├── inspect_kalshi_iter.py     # /markets pagination probe
├── inspect_kalshi_fields.py   # /markets field-name dump
├── inspect_kalshi_events.py   # /events shape + category enum probe
└── inspect_kalshi_orderbook.py# /orderbook shape probe

config/
└── settings.yaml              # central config (symlinked from /data/config on EC2)
```

The whole `config/` directory is a symlink to `/data/config/` on EC2 (per the v1.1 design). This fights with git on every config commit — see *Pitfalls* below.

---

## Phase 1 acceptance check

Run any time after the service has been up ~24h:

```bash
# Latest bar in the last few seconds:
curl -sG "http://localhost:9000/exec" --data-urlencode "query=SELECT max(timestamp) FROM pm_probabilities" | python3 -m json.tool

# Historical depth (should span >=7 days for Polymarket from backfill, ~24h for Kalshi):
curl -sG "http://localhost:9000/exec" --data-urlencode "query=SELECT source, min(timestamp), max(timestamp), count() bars FROM pm_probabilities GROUP BY source" | python3 -m json.tool

# Both sources flowing in the last hour:
curl -sG "http://localhost:9000/exec" --data-urlencode "query=SELECT source, count() bars, count(DISTINCT market_id) markets FROM pm_probabilities WHERE timestamp > dateadd('h',-1,now()) GROUP BY source" | python3 -m json.tool

# Trades in the last hour:
curl -sG "http://localhost:9000/exec" --data-urlencode "query=SELECT source, count() trades FROM pm_trades WHERE timestamp > dateadd('h',-1,now()) GROUP BY source" | python3 -m json.tool
```

Phase 1 is complete when all four pass and the systemd service has been up for 24h without manual intervention.

---

## Operational runbook

```bash
# Status
sudo systemctl status pm-platform.service
journalctl -u pm-platform.service -f          # tail
journalctl -u pm-platform.service --since '10 min ago' --no-pager

# Restart after a code update:
cd ~/prediction-market-signals
git pull origin main
# (if git complains about config/settings.yaml — see "config symlink" pitfall)
sudo systemctl restart pm-platform.service

# Stop the runner (for maintenance):
sudo systemctl stop pm-platform.service

# Restart the Docker stack (data persists on /data):
sudo docker compose restart

# Check disk usage:
sudo du -sh /data/* 2>/dev/null
df -h /data

# Manual ad-hoc run for debugging:
tmux new -s pm
source .venv/bin/activate
set -a; source .env; set +a
python -m src.main 2>&1 | tee /tmp/main.log
# Ctrl-b d to detach
```

---

## Pitfalls hit and how we resolved them (so we don't relitigate)

1. **`config/` symlink fights with git.** `provision_server.sh` replaces `config/` with a symlink to `/data/config/`. After that, every `git pull` that touches a file under `config/` complains "Your local changes to config/settings.yaml would be overwritten." Workaround that worked:
   ```bash
   git checkout HEAD -- config/settings.yaml
   git pull origin main
   ```
   Long-term: change the design to keep `config/` as a real directory and only symlink generated artefacts (`linkages.json`, `vol_surfaces.json`) into `/data/`. Not yet done.

2. **Ubuntu 24.04 dropped `awscli` from apt.** Provisioning installs AWS CLI v2 via the official installer instead. Fixed in `provision_server.sh`.

3. **Grafana container UID 472 needs to own `/data/grafana`.** Provisioning chowns it.

4. **QuestDB self-imposes a RAM cap below the Docker limit.** Set `QDB_CAIRO_RAM_USAGE_LIMIT_PERCENT=95` in `docker-compose.yml`. Without that, QuestDB exit-127s during heavy backfill writes even though the container has headroom. Verified with cgroup `memory.events` (no OOM kills) — it's QuestDB's internal accounting, not the kernel.

5. **QuestDB ILP/TCP intentionally disconnects idle clients every 60s** (`QDB_LINE_TCP_MAINTENANCE_JOB_INTERVAL=60000`). The recurring "Broken pipe" warnings in the logs are this, not crashes. `src/utils/db.py` handles them with a transactional reconnect + buffer-replay.

6. **`questdb-python` Sender column types.** `TimestampNanos` is only valid for the `at=` designated timestamp; TIMESTAMP-typed columns must be plain `datetime` (or `TimestampMicros`). Got bitten when writing `resolution_time` to `pm_markets`.

7. **Don't `flush()` after every `row()`.** Was causing per-row TCP traffic that QuestDB resets under load. Trust auto-flush. Mark the buffer-replay transactional so rows aren't lost on flush failure.

8. **Polymarket CLOB `/trades` requires HMAC auth.** Use `data-api.polymarket.com/trades?market=<conditionId>` instead — public trade tape, no auth.

9. **Polymarket Data API trade fields.** `proxyWallet` (not `taker`/`maker`); side is the tuple `(side=BUY|SELL, outcome=Yes|No)`, mapped to canonical `buy_yes|sell_no|...`.

10. **Kalshi market responses don't have a `category` field at all.** The category lives on the *event* (`/events?category=Politics&status=open`). Iterating 100k+ /markets and filtering client-side is wrong; walk /events for the four target categories and use `with_nested_markets=true` to get markets inline.

11. **Kalshi categories `Politics` and `Elections` are separate** — both map to our `political`. Don't forget `Elections` (262 events vs Politics's 86 in the sampled 500).

12. **Kalshi orderbook shape.** Response is `{"orderbook_fp": {"yes_dollars": [["0.0100","4042.50"], ...], "no_dollars": [...]}}`. Prices are already in dollars (string), not cents. Don't divide by 100.

13. **Kalshi auth modes**: API key auto-detection picks `basic` for these credentials (no PEM header in the secret). For Phase 1 read-only endpoints (markets/events/orderbook/trades), even anonymous works — but auth mode is correctly inferred so the system Just Works.

14. **httpx logs every HTTP request at INFO.** Drowns out our application logs during backfill. `main.py` sets `httpx`/`httpcore` loggers to WARNING.

15. **QuestDB ILP-over-TCP loses sender-buffered rows on idle disconnect.** With TCP transport, the questdb-python Sender holds rows in an in-memory auto-flush buffer that only flushes on subsequent `row()` calls. Sparse-write tables (pm_wallets, pm_markets) can sit unflushed past QuestDB's 60s `LINE_TCP_MAINTENANCE_JOB_INTERVAL` idle disconnect — at which point the sender is dropped and those rows are lost. Switched the transport to ILP-over-HTTP (`http::addr=…:9000`) at [src/utils/db.py:162](src/utils/db.py:162): each flush is an atomic HTTP POST, no persistent connection means no broken-pipe race. Cost is a small per-flush HTTP overhead, irrelevant at our throughput.

16. **ILP-over-HTTP requires WAL tables.** QuestDB rejects HTTP writes to non-WAL tables with `error: cannot insert in non-WAL table`. A table is WAL only if its DDL declares `PARTITION BY ... WAL` (or just `PARTITION BY ...` — newer QuestDB defaults that to WAL). The original DDL for `pm_markets`, `pm_wallets`, `pm_model_metrics` had no `PARTITION BY`, so they were non-WAL and rejected HTTP writes after we switched transport. Added `PARTITION BY MONTH WAL` to all three DDLs. **Migration: `ALTER TABLE … SET TYPE WAL` fails on non-partitioned tables** (`Cannot convert non-partitioned table`), so the existing tables had to be dropped and recreated. Safe in this case — pm_markets repopulates from the next 30s discovery cycle, pm_wallets / pm_model_metrics were empty.

17. **QuestDB returns 4xx with the error message in the JSON body.** Our `QuestDBClient.query()` was treating 4xx as a transient HTTP error: retry 3 times, then raise `RuntimeError("QuestDB query failed after 3 retries")` with the real message in `__cause__`. The migration handler in `_run_migrations` checks `str(e)` for "already exists" / "duplicate" and re-raised on miss — crashing the service. Fix at [src/utils/db.py:252](src/utils/db.py:252): on 4xx, read the JSON `error` field (or fall back to body text) and raise `RuntimeError` with that message immediately. Idempotent migrations now correctly detect "column already exists" and skip.

18. **Polymarket proxy wallets are funded by more than one relayer.** Found in Phase 2.1 testing: 0x1a96... and 0x4d97... (the relayer we've added to the exclusion list) handle most bettors, but 0x063723… traces back to a different hop-1 funder (0x3a3bd7bb9528e159577f7c2e685cc81a765002e2). Probably another Polymarket factory or proxy deployer; needs Polygonscan confirmation before adding to `config/exchange_wallets.json`.

---

## Known limitations / TODO

- **No Kalshi historical backfill.** Polymarket gets 7 days from `/prices-history`; Kalshi only starts collecting from "now". For Phase 3's training data needs we'll want a Kalshi backfill path.
- **REQ-KAL-004 (cross-platform market matching)** is unimplemented. P1, easier once both sides have a day of history.
- **REQ-XC-010 structured JSON logging** is not implemented. Currently plain text via journald.
- **`config/` symlink design (pitfall #1)** should be refactored so git pulls don't fight us.
- **Polymarket `category="other"` markets are still ingested** (~50% of the 100 active). Phase 2's anomaly detector could ignore them at signal time, or we filter them at discovery. TBD.
- **Polymarket discovery returns only 100 active markets** even with `limit=500`. Gamma may be capping the response — worth checking if cursor pagination is supposed to be used here too.
- **Polling cadence vs market count**: with 100 PM + ~50 Kalshi markets at 4 r/s per source, each full poll cycle takes 50-60s — overshooting the 15s `poll_interval_seconds` target. Once anomaly/signal detection demands tighter freshness, either raise the rate or tier markets by liquidity. **Update 2026-05-19:** measured Kalshi count was 17,110 over 24h (not 50), pushing the cycle to ~71 min. **Update 2026-05-22:** discovery filter (min 24h volume $500, max 90 days to close) now keeps ~1,062 Kalshi markets per cycle. Per-market cycle dropped from ~71 min to ~4-5 min. Filters live in `settings.yaml: ingestion.kalshi.{min_volume_usd, max_days_to_close}`.
- **Kalshi Economics-category default is `fed_rate`** — buckets SpaceX IPO and gas-price markets there too. The `derive_category_from_event` fallback at [src/ingestion/kalshi.py:52](src/ingestion/kalshi.py:52) needs better sub-classification (parse series_ticker prefix / event title) before fed_rate ↔ TLT linkage tests can rely on it.
- **Sparse-table writes to pm_markets are partially evicted by the QuestDB ILP auto-flush buffer.** Observed 2026-05-22 after Kalshi filter: discovery cycle writes 1,062 rows, but only ~270 land in the LATEST snapshot before the next QuestDB 60s idle disconnect drops the rest. Same root cause as the pm_wallets eviction (pitfall fixed via explicit `db.flush()` in wallet_tracer). The fix for pm_markets is to call `self._db.flush()` once per discovery cycle after the loop completes — TODO. Mitigates by not affecting pm_probabilities (high-write-rate hits the row-count threshold).
- **Kalshi trade tape never written** (REQ-KAL-003 silently broken). Discovered during Phase 1 gate check; details in *Phase 1 gate result → operational issues*. P1 — fix before Phase 3.
- **Polymarket wallets in `pm_trades` are EIP-1167 proxies, not user EOAs.** Discovered 2026-05-19 while probing wallet `0x1a967b272e98b708fb26a947a50c23785ac29797` (top 24h volume): `eth_getCode` returned the standard `0x363d3d37...` minimal-proxy bytecode, `eth_getTransactionCount=1` (only the deployment), and every inbound USDC.e came from a single Polymarket relayer (`0x4d97dcd97ec945f40cf65f87097ace5ea0476045`). Implications for the anomaly detector:
  - **Wallet age still works** — proxy deployment timestamp = user's first PM interaction (the freshness signal we want).
  - **`total_transactions` is uninformative** — always ~1 for proxies. Use `distinct_markets` / `total_volume_usd` from `pm_trades` instead for activity-based statistical tests (REQ-ANM-001 step 2).
  - **Sybil clustering via on-chain funding is largely defeated** — all proxies share the same relayer funder. The relayer is in the `exchange_addresses` exclusion list so we don't false-cluster all PM bettors. A proper sybil signal would require resolving each proxy to its owner EOA (Polymarket factory deployment event, or storage-slot read) — deferred until we observe whether the other 4 signals are enough.

---

## Commit summary (this session)

In chronological order, all on `main`:

| Commit | Summary |
|---|---|
| `cd8b6cc` | Initial commit (`.gitignore` only) |
| `e57ee6e` | Amended initial commit author to FitterHappierDev |
| `2c6306d` | Scaffold project structure, configs, EC2 provisioning scripts |
| `71cc5a6` | Fix awscli install on Ubuntu 24.04 (use AWS CLI v2 installer) |
| `709017e` | Fix Grafana permission on /data/grafana; drop obsolete compose version |
| `f246f1e` | Add foundation modules (config, db, redis, time, notifications) |
| `9c76e4b` | Add scripts/verify_foundation.py for HUMAN STEP 0.11 |
| `2aaf953` | Make verify_foundation.py runnable from any cwd |
| `142328f` | Add Polymarket collector (REQ-PMC-001/002/003/005) + inspection script |
| `f7a351d` | Switch Polymarket trade tape from CLOB to public Data API |
| `3419a7b` | Map Polymarket Data API trade fields (proxyWallet, side+outcome) |
| `0fb2558` | Complete Phase 1: PM backfill, Kalshi collector, main.py runner |
| `736b322` | Use datetime (not TimestampNanos) for TIMESTAMP column values |
| `a47c3c1` | Quiet httpx/httpcore per-request logs |
| `8b98121` | Bump QuestDB to 3G, shrink Redis to 512M |
| `9e23e02` | Run Kalshi collector in parallel with Polymarket backfill |
| `9dcf3ab` | Stop per-row ILP flush; transactional buffer replay; throttle Kalshi |
| `91d6312` | Add Kalshi market-iteration probe |
| `c441869` | Add lifecycle logging to KalshiCollector |
| `b6af0be` | (superseded) Filter uncategorised Kalshi markets at parse time |
| `c14381a` | Defensive Kalshi pagination + per-page logging |
| `0793ee0` | Probe Kalshi /markets field names + category distributions |
| `823e059` | Probe Kalshi /events for category signal |
| `c8ef131` | Walk Kalshi /events (not /markets) for category-aware discovery |
| `b96bb65` | Probe one Kalshi orderbook response |
| `44316f9` | Match Kalshi orderbook_fp shape; stop dividing in-dollar prices by 100 |

---

## Useful data queries

(See the conversation transcript for full recipes; here are the must-haves.)

```sql
-- Markets we're tracking (most recent snapshot per market):
SELECT title, category, bet_type, source, resolution_time, volume_total_usd
FROM pm_markets
LATEST ON last_updated PARTITION BY market_id
WHERE category != 'other'
ORDER BY volume_total_usd DESC LIMIT 25;

-- Hottest markets in the last hour:
SELECT market_id, source, sum(volume_usd) usd, sum(trade_count) trades
FROM pm_probabilities
WHERE timestamp > dateadd('h', -1, now())
GROUP BY market_id, source
ORDER BY usd DESC LIMIT 20;

-- One market's probability curve (then click "Chart" in the console):
SELECT timestamp, probability, bid, ask, volume_usd
FROM pm_probabilities
WHERE market_id = 'kalshi:KXMIDTERMMOV-TN09D-P55'
ORDER BY timestamp;

-- Most-active wallets (Phase 2 preview):
SELECT wallet_address, count() trades, count(DISTINCT market_id) markets, sum(size_usd) total_usd
FROM pm_trades
WHERE source = 'polymarket' AND wallet_address != ''
  AND timestamp > dateadd('h', -24, now())
GROUP BY wallet_address
ORDER BY total_usd DESC LIMIT 25;
```

---

## Phase 2 plan (next session)

Build order, mirroring `PM_Platform_Updates_v1.1.md` AI STEPS 2.1–2.4:

1. **AI STEP 2.1 — Wallet Tracer (`src/ingestion/wallet_tracer.py`).** Subscribes to Redis stream `wallets:trace_request`. For each address: query Polygon via Alchemy RPC for first transaction (wallet age), USDC funding sources (2-hop), transaction count. Cache results in Redis `wallet:{address}` hash with TTL. Implements REQ-WLT-001/002 + retry/rate-limit policies REQ-WLT-020/021.

2. **AI STEP 2.2 — Anomaly Detector (`src/detection/anomaly.py`).** Subscribes to `pm:trades`. Computes 5-signal composite per trade (tech design §7.3): wallet freshness, statistical improbability, pre-resolution timing, bet-size anomaly, sybil cluster. Writes scores to `pm_anomaly_scores`, fires Slack/Telegram on `composite > 7.0`. Implements REQ-ANM-001 through REQ-ANM-004.

3. **AI STEP 2.3 — Sybil clustering** (within the anomaly module). Reverse-index wallets by 2-hop USDC funder; cluster wallets sharing a funder that bet the same market in the same direction within 60 min.

4. **AI STEP 2.4 — Alert formatting.** Update `notifications.py` to emit Slack block-kit / Telegram Markdown for CRITICAL alerts (market title, wallet truncated, composite + per-component breakdown, trade details, wallet profile).

5. **HUMAN STEPS 2.5/2.6/2.7** — deploy, wait 48 h, begin manual outcome tracking spreadsheet (HUMAN STEP 2.7 is the leading indicator that anomaly detection is finding real things).

Phase 2 gate criteria (from build guide): anomaly scoring live, alerts firing, scores-to-trades ratio ≈ 1:1, traced wallet count > 0, manual outcome tracking started.

### What needs to exist before starting Phase 2

- Phase 1 acceptance check above passes
- A few hours of `pm_trades` rows accumulated so the anomaly detector has wallets to look at on first boot
- An `.env` with a valid `ALCHEMY_API_KEY` (already present and verified)
- Exchange hot-wallet exclusion list — pre-populate `src/ingestion/wallet_tracer.py` with top-20 Polygon exchange wallets (Coinbase, Binance, etc.) so sybil clustering doesn't false-positive on exchange-funded users

---

## Things that didn't make it into Phase 1 but should before Phase 3

- Refactor the `config/` symlink (pitfall #1) so it stops fighting git
- Structured JSON logging (REQ-XC-010 / REQ-PMC-013)
- Some lightweight unit tests for `parse_market_with_event`, `derive_category_from_event`, `_midpoint_from_orderbook`, `canonical_side` (the parsing functions that have been gotcha-prone)
- Polymarket discovery: figure out if Gamma's `active=true&closed=false&limit=500` is actually capped at 100, or if there's a cursor we're missing
