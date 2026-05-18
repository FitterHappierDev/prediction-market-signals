# Build Progress — PM Signal Platform

**Last session:** 2026-05-17
**Phase:** 1 complete, 2 not yet started
**Live status:** Polymarket + Kalshi minute bars + Polymarket trade tape writing to QuestDB. Systemd service runs the collector unattended.

---

## Where to pick up

The platform's ingestion layer is live. Both data sources are flowing into QuestDB at minute resolution. Next session, the natural sequence is:

1. **Verify the 24-hour gate (HUMAN STEP 1.8 from `PM_Platform_Updates_v1.1.md`).** Re-run the validation queries in *Phase 1 acceptance check* below; if all four pass, declare Phase 1 done.
2. **Phase 2 — Anomaly Detection Layer:**
   - `src/ingestion/wallet_tracer.py` (REQ-WLT-001/002) — on-demand Polygon RPC traces
   - `src/detection/anomaly.py` (REQ-ANM-001 through REQ-ANM-004) — 5-signal composite scoring + sybil clustering
   - Slack/Telegram alert formatting upgrade
3. Begin manual outcome tracking when CRITICAL alerts fire (HUMAN STEP 2.7).

Tech design and spec reference:
- `PM_Platform_Technical_Design.md` — section 3.3 (Wallet Tracer), 3.4 (Anomaly Detector), 7.3 (composite score algorithm)
- `PM_Platform_Spec_Requirements_Core_Pipeline.md` — REQ-WLT-* and REQ-ANM-*

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

---

## Known limitations / TODO

- **No Kalshi historical backfill.** Polymarket gets 7 days from `/prices-history`; Kalshi only starts collecting from "now". For Phase 3's training data needs we'll want a Kalshi backfill path.
- **REQ-KAL-004 (cross-platform market matching)** is unimplemented. P1, easier once both sides have a day of history.
- **REQ-XC-010 structured JSON logging** is not implemented. Currently plain text via journald.
- **`config/` symlink design (pitfall #1)** should be refactored so git pulls don't fight us.
- **Polymarket `category="other"` markets are still ingested** (~50% of the 100 active). Phase 2's anomaly detector could ignore them at signal time, or we filter them at discovery. TBD.
- **Polymarket discovery returns only 100 active markets** even with `limit=500`. Gamma may be capping the response — worth checking if cursor pagination is supposed to be used here too.
- **Polling cadence vs market count**: with 100 PM + ~50 Kalshi markets at 4 r/s per source, each full poll cycle takes 50-60s — overshooting the 15s `poll_interval_seconds` target. Once anomaly/signal detection demands tighter freshness, either raise the rate or tier markets by liquidity.

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
