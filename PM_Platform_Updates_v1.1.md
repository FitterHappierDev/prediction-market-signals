# PM Platform — Document Updates (v1.1)
# Infrastructure: Cloud VM | Build Guide: Human vs AI Steps

**Date:** April 23, 2026  
**Changes:** Infrastructure updated from local workstation to AWS Cloud VM. Build guide rewritten with explicit HUMAN vs AI step separation.

---

# PART 1: Technical Design Updates

## Replaces: Section 8 (Infrastructure and Deployment)

---

## 8. Infrastructure and Deployment

### 8.1 Target Platform: AWS Cloud VM

All phases run on a single AWS EC2 instance in `us-east-1` (Northern Virginia). This region provides the lowest latency to Polymarket (Polygon blockchain validators), Kalshi (Chicago), NYSE/NASDAQ, and Alpaca/IBKR API servers.

**Instance sizing by phase:**

| Phase | Instance Type | vCPU | RAM | Cost/mo | Storage |
|---|---|---|---|---|---|
| Phase 1-2 | `t4g.medium` | 2 | 4 GB | ~$24 | 50 GB gp3 ($4) |
| Phase 3-4 | `t4g.large` | 2 | 8 GB | ~$49 | 100 GB gp3 ($8) |
| Phase 5-6 | `m7g.large` | 2 | 8 GB | ~$60 | 100 GB gp3 ($8) |
| Burst (training) | `m7g.xlarge` | 4 | 16 GB | ~$120 | 100 GB gp3 ($8) |

**Why `t4g` / `m7g` (ARM Graviton):** 20-30% cheaper than x86 equivalents for CPU-bound workloads. All our dependencies (Python, LightGBM, QuestDB, Redis) have ARM builds. If any dependency lacks ARM support, substitute `t3` / `m7i` (x86 equivalent, ~20% more expensive).

**Burst strategy:** Run `t4g.large` ($49/mo) for steady-state ingestion and detection. When running model training or linkage validation (Phase 3+), temporarily resize to `m7g.xlarge` via AWS CLI (`aws ec2 modify-instance-attribute`), run the compute, resize back down. Training runs take 10-30 minutes — you pay for the larger instance only during those minutes.

### 8.2 AWS Infrastructure Components

```
┌─────────────────────────────────────────────────────────┐
│                   AWS us-east-1                          │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │        EC2: t4g.large (primary)                 │    │
│  │                                                 │    │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────────┐    │    │
│  │  │ QuestDB  │ │  Redis   │ │   Grafana    │    │    │
│  │  │ (Docker) │ │ (Docker) │ │   (Docker)   │    │    │
│  │  └──────────┘ └──────────┘ └──────────────┘    │    │
│  │  ┌──────────┐ ┌──────────────────────────┐     │    │
│  │  │  MLflow  │ │  Python App (systemd)    │     │    │
│  │  │ (Docker) │ │  collectors + detectors  │     │    │
│  │  └──────────┘ │  + models + monitoring   │     │    │
│  │               └──────────────────────────┘     │    │
│  │                                                 │    │
│  │  EBS gp3: 100 GB (QuestDB + Parquet + models)  │    │
│  └─────────────────────────────────────────────────┘    │
│                                                         │
│  ┌────────────────────────┐                             │
│  │ Elastic IP             │  ← stable public IP for     │
│  │ (attached to EC2)      │    SSH + Grafana access     │
│  └────────────────────────┘                             │
│                                                         │
│  ┌────────────────────────┐                             │
│  │ Security Group         │                             │
│  │ Inbound:               │                             │
│  │   22 (SSH) ← your IP  │                             │
│  │   3000 (Grafana) ← IP │                             │
│  │   5000 (MLflow) ← IP  │                             │
│  │ Outbound: all          │                             │
│  └────────────────────────┘                             │
│                                                         │
│  ┌────────────────────────┐                             │
│  │ S3 Bucket (optional)   │  ← model artifacts +       │
│  │ pm-platform-backups    │    daily QuestDB snapshots  │
│  └────────────────────────┘                             │
│                                                         │
│  Optional (Phase 6, crypto only):                       │
│  ┌────────────────────────────────────────┐             │
│  │ EC2 t4g.small in ap-northeast-1       │             │
│  │ Tokyo — Hyperliquid proximity ($15/mo)│             │
│  └────────────────────────────────────────┘             │
└─────────────────────────────────────────────────────────┘
```

### 8.3 Docker Compose Services

```yaml
version: '3.8'

services:
  questdb:
    image: questdb/questdb:8.0
    ports:
      - "127.0.0.1:9000:9000"    # Web console — localhost only
      - "127.0.0.1:9009:9009"    # ILP ingestion
      - "127.0.0.1:8812:8812"    # PostgreSQL wire protocol (for Grafana)
    volumes:
      - /data/questdb:/var/lib/questdb
    environment:
      - QDB_LINE_TCP_COMMIT_INTERVAL_DEFAULT=1000
      - QDB_LINE_TCP_MAINTENANCE_JOB_INTERVAL=60000
      - QDB_SHARED_WORKER_COUNT=2
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
      - "0.0.0.0:3000:3000"     # Exposed for remote access (secured by SG)
    volumes:
      - /data/grafana:/var/lib/grafana
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_PASSWORD}
      - GF_SERVER_ROOT_URL=http://${SERVER_IP}:3000
    restart: always

  mlflow:
    image: ghcr.io/mlflow/mlflow:2.16
    ports:
      - "0.0.0.0:5000:5000"     # Exposed for remote access (secured by SG)
    command: >
      mlflow server
      --host 0.0.0.0
      --backend-store-uri sqlite:///data/mlflow.db
      --default-artifact-root /data/artifacts
    volumes:
      - /data/mlflow:/data
    restart: always
```

**Security notes:**
- QuestDB and Redis ports bound to `127.0.0.1` — not exposed to the internet
- Grafana and MLflow exposed on `0.0.0.0` but protected by AWS Security Group (only your IP allowed)
- All inter-service communication is localhost (single VM, no network hops)

### 8.4 Storage Layout

```
/data/                          ← Separate EBS volume mounted here
├── questdb/                    ← QuestDB data directory (~70 GB/year)
├── redis/                      ← Redis AOF persistence (~1 GB)
├── grafana/                    ← Grafana dashboards + config
├── mlflow/                     ← MLflow DB + model artifacts
├── parquet/                    ← Feature store + training data
│   ├── features/
│   │   ├── raw/
│   │   ├── computed/
│   │   └── training/
│   └── models/
│       ├── stage1/
│       └── stage2/
├── config/                     ← Symlinked from app directory
└── backups/                    ← Daily QuestDB snapshots (before S3 sync)

/home/ubuntu/prediction-market-signals/   ← Application code
├── src/
├── config/ → /data/config               ← Symlink to data volume
├── data/ → /data/parquet                 ← Symlink to data volume
├── docker-compose.yml
├── requirements.txt
└── .env                                  ← Secrets (never committed)
```

**Why a separate EBS volume for `/data`:** If the EC2 instance is terminated or replaced, the EBS volume persists. You can attach it to a new instance and resume without data loss. The root volume (OS + code) is disposable and reproducible.

### 8.5 Backup Strategy

| What | Frequency | Method | Retention |
|---|---|---|---|
| QuestDB tables | Daily 03:00 UTC | `SNAPSHOT` → `/data/backups/` → `aws s3 sync` | 30 days |
| Parquet training data | Weekly | `aws s3 sync /data/parquet/training/ s3://bucket/training/` | 90 days |
| Model artifacts | On each training run | MLflow logs to `/data/mlflow/artifacts/` → S3 sync weekly | All versions |
| Redis AOF | Continuous | Redis AOF persistence to `/data/redis/` | Current state |
| Config files | On change | `git push` (code repo) | Git history |

### 8.6 System Service (systemd)

The Python application runs as a systemd service for automatic start on boot and restart on crash:

```ini
# /etc/systemd/system/pm-platform.service
[Unit]
Description=Prediction Market Signal Platform
After=docker.service
Requires=docker.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/prediction-market-signals
Environment=PYTHONPATH=/home/ubuntu/prediction-market-signals
EnvironmentFile=/home/ubuntu/prediction-market-signals/.env
ExecStart=/home/ubuntu/prediction-market-signals/.venv/bin/python -m src.main
Restart=always
RestartSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

### 8.7 SSH Access and Development Workflow

Development happens on your local machine. Code is pushed to GitHub. The EC2 instance pulls from GitHub and restarts the service.

```bash
# On your local machine (develop + test)
git push origin main

# On EC2 (deploy)
cd ~/prediction-market-signals
git pull origin main
pip install -r requirements.txt
sudo systemctl restart pm-platform
journalctl -u pm-platform -f   # Watch logs
```

For interactive debugging and notebook work: SSH tunnel to access Grafana, MLflow, and QuestDB console from your local browser:

```bash
ssh -L 3000:localhost:3000 -L 5000:localhost:5000 -L 9000:localhost:9000 ubuntu@${SERVER_IP}
# Then open http://localhost:3000 (Grafana), etc.
```

### 8.8 Cost Summary

| Phase | EC2 | EBS | S3 | Total/mo |
|---|---|---|---|---|
| 1-2 | $24 (t4g.medium) | $4 (50 GB) | $0 | **~$28** |
| 3-4 | $49 (t4g.large) | $8 (100 GB) | $1 | **~$58** |
| 5-6 | $60 (m7g.large) | $8 (100 GB) | $2 | **~$70** |
| + Tokyo (optional) | +$15 (t4g.small) | +$4 | $0 | **+$19** |

---

# PART 2: Spec Requirements Updates

## Add: Section 9 — Infrastructure Requirements

These requirements are written for Claude Code to implement via shell commands and file creation.

---

### REQ-INF-001: EC2 Instance Provisioning Script (P0)

**Description:** A shell script that provisions the EC2 instance with all required system packages, Docker, and application dependencies.

**Acceptance Criteria:**
- Script file: `scripts/provision_server.sh`
- Runs on a fresh Ubuntu 24.04 ARM64 (Graviton) EC2 instance
- Installs: Docker CE, Docker Compose v2, Python 3.12, pip, git, htop, tmux, jq, awscli
- Creates `/data` directory and symlinks
- Sets up the systemd service file for the Python application
- Sets up a daily cron job for QuestDB backup + S3 sync
- Sets up log rotation for application logs
- Does NOT handle AWS account creation, EC2 launch, Security Group, or Elastic IP — those are HUMAN steps
- Idempotent: running twice produces the same result

**Implementation prompt for Claude Code:**
> Write a bash script `scripts/provision_server.sh` that provisions an Ubuntu 24.04 ARM64 EC2 instance. The script should:
> 1. Update apt and install: docker.io, docker-compose-v2, python3.12, python3.12-venv, python3-pip, git, htop, tmux, jq, awscli, build-essential
> 2. Add ubuntu user to docker group
> 3. Enable and start Docker
> 4. Create directory structure: /data/{questdb,redis,grafana,mlflow,parquet/{features/{raw,computed,training},models/{stage1,stage2}},config,backups}
> 5. Set ownership of /data to ubuntu:ubuntu
> 6. Clone the git repo to /home/ubuntu/prediction-market-signals (or skip if exists)
> 7. Create symlinks: config/ → /data/config, data/ → /data/parquet
> 8. Create Python venv at .venv, install requirements.txt
> 9. Copy the systemd service file to /etc/systemd/system/pm-platform.service, enable it
> 10. Create cron job: daily at 03:00 UTC, run /home/ubuntu/prediction-market-signals/scripts/backup.sh
> 11. Set up logrotate for /var/log/pm-platform/*.log (7 days, 500MB max, compress)
> The script must be idempotent (safe to run multiple times). Use set -euo pipefail. Log progress with echo statements.


### REQ-INF-002: Docker Compose Configuration (P0)

**Description:** Docker Compose file for all infrastructure services, configured for cloud deployment.

**Acceptance Criteria:**
- File: `docker-compose.yml`
- Services: QuestDB, Redis, Grafana, MLflow (as specified in Section 8.3 above)
- QuestDB and Redis ports bound to 127.0.0.1 (not exposed to internet)
- Grafana and MLflow on 0.0.0.0 (protected by Security Group)
- Memory limits set on all containers (QuestDB: 2G, Redis: 1G)
- All data volumes point to /data/ (the EBS mount)
- Grafana password loaded from environment variable
- restart: always on all services

**Implementation prompt for Claude Code:**
> Write the docker-compose.yml file for the PM platform with four services: QuestDB 8.0, Redis 7-alpine, Grafana 11.0, MLflow 2.16. Use the exact configuration from the tech design Section 8.3. Bind QuestDB and Redis to 127.0.0.1 only. Bind Grafana and MLflow to 0.0.0.0. Set memory limits. All volumes under /data/. Include deploy.resources.limits.memory for each service.


### REQ-INF-003: Backup Script (P0)

**Description:** Automated daily backup of QuestDB data and weekly sync to S3.

**Acceptance Criteria:**
- Script file: `scripts/backup.sh`
- Creates a dated QuestDB snapshot: `questdb-backup-YYYYMMDD.tar.gz` from `/data/questdb/`
- Retains only the last 7 daily backups locally (delete older)
- If AWS CLI is configured and S3 bucket exists: sync `/data/backups/` to `s3://${S3_BUCKET}/backups/`
- If S3 is not configured: skip S3 sync, log info message (backup still runs locally)
- Log output to `/var/log/pm-platform/backup.log`

**Implementation prompt for Claude Code:**
> Write a bash script `scripts/backup.sh` that: 1) Creates a timestamped tar.gz of /data/questdb/ into /data/backups/questdb-backup-$(date +%Y%m%d).tar.gz. 2) Deletes backups older than 7 days from /data/backups/. 3) If the environment variable S3_BACKUP_BUCKET is set, runs `aws s3 sync /data/backups/ s3://$S3_BACKUP_BUCKET/backups/ --delete`. 4) Logs all output to /var/log/pm-platform/backup.log with timestamps. Use set -euo pipefail.


### REQ-INF-004: Security Hardening (P1)

**Description:** Basic security configuration for the cloud VM.

**Acceptance Criteria:**
- Script file: `scripts/harden_server.sh`
- Configures UFW firewall: allow SSH (22), Grafana (3000), MLflow (5000) from a specified IP only; deny all other inbound
- Disables password SSH authentication (key-only)
- Enables automatic security updates (unattended-upgrades)
- Sets up fail2ban for SSH brute force protection

**Implementation prompt for Claude Code:**
> Write a bash script `scripts/harden_server.sh` that accepts one argument: the allowed IP address (CIDR notation, e.g., 203.0.113.0/32). The script should: 1) Install and enable UFW. 2) Default deny incoming, allow outgoing. 3) Allow SSH (22), 3000, 5000 from the provided IP only. 4) Enable UFW. 5) Disable PasswordAuthentication in /etc/ssh/sshd_config, restart sshd. 6) Install and enable unattended-upgrades. 7) Install and configure fail2ban for sshd with maxretry=5, bantime=3600. The script must be idempotent.


### REQ-INF-005: Health Check Endpoint (P1)

**Description:** A lightweight HTTP health check that AWS can use for monitoring, and you can use for quick status checks.

**Acceptance Criteria:**
- Runs on port 8080 (localhost only — not exposed via Security Group)
- `GET /health` returns JSON with status of each component:
  ```json
  {
    "status": "healthy",
    "components": {
      "questdb": {"status": "healthy", "last_write": "2026-04-23T14:30:00Z"},
      "redis": {"status": "healthy", "connected": true},
      "polymarket_collector": {"status": "healthy", "last_poll": "2026-04-23T14:30:12Z", "markets_active": 1247},
      "kalshi_collector": {"status": "healthy", "last_poll": "2026-04-23T14:30:08Z", "markets_active": 312},
      "anomaly_detector": {"status": "healthy", "scores_last_hour": 4521},
      "signal_detector": {"status": "healthy", "signals_last_hour": 3},
      "stage1_classifier": {"status": "healthy", "model_version": "stage1_v12_20260421"}
    },
    "uptime_seconds": 86421,
    "timestamp": "2026-04-23T14:30:15Z"
  }
  ```
- If any component is unhealthy, top-level status is "degraded"
- If QuestDB or Redis is down, top-level status is "unhealthy"

**Implementation prompt for Claude Code:**
> Build a lightweight health check HTTP server using Python's built-in http.server (no additional dependencies). Run on port 8080, bind to 127.0.0.1. Single endpoint GET /health. Check QuestDB connectivity (try a simple SELECT), Redis connectivity (PING), and each running component's last-activity timestamp from Redis hashes. Return JSON. Start this server as an additional asyncio task in src/main.py.

---

# PART 3: Build Guide v2 — With Human vs AI Steps

## Replaces: Entire PM_Platform_Build_Guide_v1.md

---

## Step 0: Account Setup and Cloud Provisioning

### HUMAN STEP 0.1: Create AWS Account and Launch EC2

**You must do this manually. AI tools cannot create cloud accounts or access AWS console.**

1. **Create or log into your AWS account** at https://aws.amazon.com
2. **Create an SSH key pair:**
   - AWS Console → EC2 → Key Pairs → Create Key Pair
   - Name: `pm-platform-key`
   - Type: RSA, Format: .pem
   - Download and save the .pem file. Run: `chmod 400 pm-platform-key.pem`
3. **Create a Security Group:**
   - AWS Console → EC2 → Security Groups → Create
   - Name: `pm-platform-sg`
   - Inbound rules:
     - SSH (22): your home IP only (find at https://whatismyip.com, use /32)
     - Custom TCP (3000): your home IP only (Grafana)
     - Custom TCP (5000): your home IP only (MLflow)
   - Outbound: All traffic (default)
4. **Launch EC2 instance:**
   - AMI: Ubuntu Server 24.04 LTS (ARM64 / Graviton)
   - Instance type: `t4g.medium` (Phase 1-2) — you'll resize later
   - Key pair: `pm-platform-key`
   - Security group: `pm-platform-sg`
   - Storage: 50 GB gp3 root volume + 100 GB gp3 additional volume (mount point: `/data`)
   - Enable "Delete on termination" for root volume only (NOT the 100 GB data volume)
5. **Allocate and associate an Elastic IP** (so your IP doesn't change on restart):
   - EC2 → Elastic IPs → Allocate → Associate with your instance
   - Note the Elastic IP: `	18.225.93.218` (you'll use this everywhere)
6. **SSH in to verify:**
   ```bash
   ssh -i pm-platform-key.pem ubuntu@18.225.93.218
   ```
7. **Mount the data volume** (first time only):
   ```bash
   # Find the device name (usually /dev/nvme1n1 or /dev/xvdf)
   lsblk
   # Format it (ONLY on first mount — this erases data)
   sudo mkfs.ext4 /dev/nvme1n1
   # Create mount point and mount
   sudo mkdir -p /data
   sudo mount /dev/nvme1n1 /data
   sudo chown ubuntu:ubuntu /data
   # Add to fstab for auto-mount on reboot
   echo '/dev/nvme1n1 /data ext4 defaults,nofail 0 2' | sudo tee -a /etc/fstab
   ```

### HUMAN STEP 0.2: Create Service Accounts and Get API Keys

**You must create these accounts manually and obtain API keys. Store them securely.**

| Service | Sign Up URL | What You Need | Free Tier |
|---|---|---|---|
| **Kalshi** | https://kalshi.com/sign-up | Email + ID verification → API key from Settings | Free (trading account required) |
| **Alpaca** | https://alpaca.markets/signup | Email → Paper trading API key + secret from Dashboard | Free paper trading |
| **Alchemy** (Polygon RPC) | https://www.alchemy.com/signup | Email → Create app (Polygon Mainnet) → API key | 300M compute units/mo free |
| **Slack Webhook** | https://api.slack.com/messaging/webhooks | Create Slack app → Incoming Webhook → URL | Free |
| **Telegram Bot** | Message @BotFather on Telegram | `/newbot` → get token. Send message to bot → get chat_id via `https://api.telegram.org/bot{TOKEN}/getUpdates` | Free |
| **GitHub** | https://github.com (if not already) | Create repo: `prediction-market-signals` (private) | Free |

**Optional (Phase 3+):**

| Service | URL | What You Need | Cost |
|---|---|---|---|
| **S3 Bucket** | AWS Console → S3 | Create bucket: `pm-platform-backups-{your-id}` in us-east-1 | ~$1-2/mo |
| **ORATS** | https://orats.com | API key for options data (Phase 7+) | $79-159/mo |

### HUMAN STEP 0.3: Create the .env File

On your **local machine**, create the `.env` file with your API keys:

```bash
# .env — NEVER commit this file to git
KALSHI_API_KEY=your_kalshi_api_key
KALSHI_API_SECRET=your_kalshi_secret
ALPACA_API_KEY=your_alpaca_key
ALPACA_API_SECRET=your_alpaca_secret
ALCHEMY_API_KEY=your_alchemy_key
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz
TELEGRAM_BOT_TOKEN=123456:ABC-DEF...
TELEGRAM_CHAT_ID=your_chat_id
GRAFANA_PASSWORD=choose_a_strong_password
S3_BACKUP_BUCKET=pm-platform-backups-yourid
SERVER_IP=your_elastic_ip
```

### HUMAN STEP 0.4: Create GitHub Repo and Push Initial Structure

```bash
# On your local machine
mkdir prediction-market-signals && cd prediction-market-signals
git init
echo ".env" >> .gitignore
echo "__pycache__/" >> .gitignore
echo ".venv/" >> .gitignore
echo "data/" >> .gitignore
echo "*.pyc" >> .gitignore
git add .gitignore
git commit -m "Initial commit"
git remote add origin git@github.com:yourusername/prediction-market-signals.git
git push -u origin main
```

---

### AI STEP 0.5: Generate Project Structure and Config Files

**Prompt for Claude Code:**
> Create the full project directory structure for the prediction-market-signals project as specified in the tech design Section 8.4. Create all directories and `__init__.py` files. Create `requirements.txt` with pinned dependencies. Create `config/settings.yaml` with the complete configuration from tech design Section 9.1. Create `.env.example` with placeholder values for all required environment variables. Create the `docker-compose.yml` for cloud deployment with QuestDB, Redis, Grafana, MLflow — bind QuestDB and Redis to 127.0.0.1, Grafana and MLflow to 0.0.0.0, all data volumes under /data/, memory limits (QuestDB 2G, Redis 1G). Create the systemd service file at `scripts/pm-platform.service`.

### AI STEP 0.6: Generate Server Provisioning Script

**Prompt for Claude Code:**
> Write `scripts/provision_server.sh` implementing REQ-INF-001 from the spec. Ubuntu 24.04 ARM64. Install Docker, Python 3.12, all system deps. Create /data directory structure. Set up symlinks. Create venv. Install systemd service. Set up cron for backup. Must be idempotent.

### AI STEP 0.7: Generate Security Hardening Script

**Prompt for Claude Code:**
> Write `scripts/harden_server.sh` implementing REQ-INF-004. Accept one argument: allowed IP in CIDR. Configure UFW, disable password SSH, set up unattended-upgrades and fail2ban. Must be idempotent.

### AI STEP 0.8: Generate Backup Script

**Prompt for Claude Code:**
> Write `scripts/backup.sh` implementing REQ-INF-003. Daily QuestDB backup, 7-day local retention, optional S3 sync if S3_BACKUP_BUCKET env var is set.

### HUMAN STEP 0.9: Deploy to EC2

```bash
# On your local machine — push all generated files
git add -A
git commit -m "Add project structure, configs, and provisioning scripts"
git push origin main

# SSH into EC2
ssh -i pm-platform-key.pem ubuntu@YOUR_ELASTIC_IP

# Clone repo
git clone git@github.com:yourusername/prediction-market-signals.git
cd prediction-market-signals

# Copy .env from local machine (via scp from another terminal)
# scp -i pm-platform-key.pem .env ubuntu@YOUR_ELASTIC_IP:~/prediction-market-signals/.env

# Run provisioning
chmod +x scripts/provision_server.sh
sudo ./scripts/provision_server.sh

# Run security hardening
chmod +x scripts/harden_server.sh
sudo ./scripts/harden_server.sh YOUR_HOME_IP/32

# Start Docker services
docker compose up -d

# Verify services
docker compose ps          # All should show "running"
curl localhost:9000         # QuestDB console
redis-cli ping              # PONG
curl localhost:3000         # Grafana (also from your browser via Elastic IP)
curl localhost:5000         # MLflow
```

### AI STEP 0.10: Build Foundation Modules

**Prompt for Claude Code:**
> Build `src/config.py`: Pydantic Settings config loader for settings.yaml. Nested BaseModel hierarchy matching the YAML. Validation: thresholds positive, probabilities 0-1, minutes non-negative. Singleton `get_config()`. Support Redis runtime overrides from `system:config` hash.

**Prompt for Claude Code:**
> Build `src/utils/db.py`: QuestDB client wrapper using `questdb` package. ILP for writes (TCP to localhost:9009), HTTP for reads (localhost:9000). Methods: `write_row`, `write_batch`, `query` (returns DataFrame), `ensure_tables` (create all tables from tech design Section 4.1). Connection retry: 3 attempts, 5s delay. Buffer up to 1000 rows if QuestDB unreachable.

**Prompt for Claude Code:**
> Build `src/utils/redis_client.py`: Redis client wrapper. Methods: `publish_to_stream` (XADD with MAXLEN ~100000), `subscribe_stream` (XREADGROUP consumer loop with XACK), `get_hash`, `set_hash` (with optional TTL). On disconnect: log warning, don't buffer streams, resume on reconnect.

**Prompt for Claude Code:**
> Build `src/utils/time_utils.py`: All UTC, all minutes. Functions: `now_utc()`, `minutes_between(start, end) -> float`, `minute_floor(dt)`, `is_market_open(asset_class)` (9:30-16:00 ET for equities, always True for crypto), `trading_minutes_to_years(minutes, asset_class)`.

**Prompt for Claude Code:**
> Build `src/utils/notifications.py`: Alert sender for Slack (webhook POST) and Telegram (bot API POST). `send_alert(message, severity, channel)`. Critical → both. Info/warning → Slack only. Async fire-and-forget, never block. Load creds from env vars. Format: market title, wallet (truncated), score, breakdown.

### HUMAN STEP 0.11: Verify Foundation

```bash
# SSH into EC2, activate venv
cd ~/prediction-market-signals
source .venv/bin/activate

# Test config
python -c "from src.config import get_config; cfg = get_config(); print(f'Poll interval: {cfg.ingestion.polymarket.poll_interval_seconds}s')"

# Test QuestDB
python -c "from src.utils.db import QuestDBClient; db = QuestDBClient(); db.ensure_tables(); print('Tables created')"

# Test Redis
python -c "from src.utils.redis_client import RedisClient; r = RedisClient(); r.publish_to_stream('test', {'hello': 'world'}); print('Redis OK')"

# Test notifications (optional — sends a real alert)
python -c "from src.utils.notifications import send_alert; import asyncio; asyncio.run(send_alert('Test alert from PM platform', 'info'))"
```

---

## Phase 1: Ingestion Layer (Weeks 1-2)

### AI STEP 1.1: Build Polymarket Collector

**Prompt for Claude Code:**
> Build `src/ingestion/polymarket.py` implementing REQ-PMC-001 through REQ-PMC-005 from the spec document. [Include the full prompt from the original build guide Step 1.1 — the API endpoints, data mapping, class structure, retry logic, classification rules, and all edge cases.]

### AI STEP 1.2: Build Polymarket Probability + Trade Ingestion

**Prompt for Claude Code:**
> [Include the full prompt from original build guide Step 1.2 — poll_cycle, probability computation, 1-minute bars, trade deduplication, all edge cases.]

### AI STEP 1.3: Build Polymarket Historical Backfill

**Prompt for Claude Code:**
> [Include the full prompt from original build guide Step 1.3 — backfill_history, idempotency, resume from interruption.]

### HUMAN STEP 1.4: Create Kalshi Account and Get API Key

If not already done in Step 0.2, create Kalshi account now. You need it before the Kalshi Collector can run.

1. Go to https://kalshi.com/sign-up
2. Complete identity verification (required for API access)
3. Navigate to Settings → API → Generate API Key
4. Add to `.env` on the EC2 instance:
   ```bash
   echo 'KALSHI_API_KEY=your_key' >> .env
   echo 'KALSHI_API_SECRET=your_secret' >> .env
   ```

### AI STEP 1.5: Build Kalshi Collector

**Prompt for Claude Code:**
> Build `src/ingestion/kalshi.py` implementing REQ-KAL-001 through REQ-KAL-004. [Include full prompt from original build guide Step 1.4.]

### AI STEP 1.6: Build Main Entry Point

**Prompt for Claude Code:**
> Build `src/main.py` that starts both collectors as concurrent asyncio tasks. SIGINT/SIGTERM graceful shutdown. If either crashes, log and restart after 30s.

### HUMAN STEP 1.7: Deploy and Run Phase 1

```bash
# On EC2
cd ~/prediction-market-signals
git pull origin main
pip install -r requirements.txt

# Start manually first to watch logs
python -m src.main

# Once confirmed working (data flowing for 30+ minutes), deploy as service
sudo systemctl start pm-platform
sudo systemctl enable pm-platform
journalctl -u pm-platform -f   # Watch logs
```

### HUMAN STEP 1.8: Phase 1 Gate Validation (After 24 Hours)

```bash
# SSH in, check data
source .venv/bin/activate
python << 'EOF'
from src.utils.db import QuestDBClient
db = QuestDBClient()

# Latest data freshness
latest = db.query("SELECT max(timestamp) FROM pm_probabilities")
print(f"Latest bar: {latest}")

# Historical depth
depth = db.query("SELECT min(timestamp), max(timestamp) FROM pm_probabilities")
print(f"Date range: {depth}")

# Both sources
sources = db.query("""
    SELECT source, count() as bars, count(distinct market_id) as markets 
    FROM pm_probabilities WHERE timestamp > dateadd('h', -1, now()) GROUP BY source
""")
print(f"Sources:\n{sources}")

# Trades
trades = db.query("""
    SELECT source, count() as trades FROM pm_trades 
    WHERE timestamp > dateadd('h', -1, now()) GROUP BY source
""")
print(f"Trades:\n{trades}")
EOF
```

**Phase 1 complete when:** Both sources flowing, 7+ days history, 1-minute resolution, 24 hours unattended.

---

## Phase 2: Anomaly Detection Layer (Weeks 3-4)

### HUMAN STEP 2.0: Verify Alchemy RPC Access

Before building the Wallet Tracer, verify your Alchemy API key works:

```bash
curl -s "https://polygon-mainnet.g.alchemy.com/v2/${ALCHEMY_API_KEY}" \
  -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' | jq
```

Should return a JSON response with the current Polygon block number.

### AI STEP 2.1: Build Wallet Tracer

**Prompt for Claude Code:**
> Build `src/ingestion/wallet_tracer.py` implementing REQ-WLT-001 and REQ-WLT-002. [Include full prompt from original build guide Step 2.1 — USDC contracts, Transfer topic, funding source tracing, exchange hot wallet exclusion list, max concurrent RPC, all edge cases.]

### AI STEP 2.2: Build Anomaly Detector

**Prompt for Claude Code:**
> Build `src/detection/anomaly.py` implementing REQ-ANM-001 through REQ-ANM-004. [Include full prompt from original build guide Step 2.2 — five signal components with exact scoring, composite weights, alerting thresholds, degraded mode, wallet stats, address normalization.]

### AI STEP 2.3: Build Sybil Clustering

**Prompt for Claude Code:**
> Extend anomaly detector with sybil clustering per REQ-ANM-004. Reverse funding index, exchange exclusion, 60-minute window, cluster expiry after 7 days.

### AI STEP 2.4: Wire Up Notifications

**Prompt for Claude Code:**
> Update `src/utils/notifications.py` with Slack block kit formatting and Telegram markdown for critical anomaly alerts. Include: market title, wallet (first 6 + last 4 chars), composite score, all 5 component scores, trade details (side, size, price).

### HUMAN STEP 2.5: Deploy Phase 2

```bash
# On EC2
git pull origin main
sudo systemctl restart pm-platform
journalctl -u pm-platform -f
```

### HUMAN STEP 2.6: Phase 2 Gate Validation (After 48 Hours)

```python
# Check anomaly scoring
scores = db.query("SELECT count() FROM pm_anomaly_scores")
trades = db.query("SELECT count() FROM pm_trades WHERE source = 'polymarket'")
print(f"Scores: {scores}, Polymarket trades: {trades}")
# Ratio should be close to 1:1

# Check alerts
alerts = db.query("""
    SELECT alert_level, count() FROM pm_anomaly_scores 
    WHERE alert_level != 'none' GROUP BY alert_level
""")
print(f"Alerts:\n{alerts}")

# Check wallet profiles
wallets = db.query("SELECT count() FROM pm_wallets")
print(f"Traced wallets: {wallets}")
```

### HUMAN STEP 2.7: Begin Manual Outcome Tracking

**This is a HUMAN step that cannot be automated yet.** When a CRITICAL alert fires:
1. Note the market title, wallet, and composite score
2. Identify the linked traditional asset (use the linkage table from the strategy memo)
3. Note the asset price at alert time
4. Check the asset price at +1h, +4h, +8h, +24h
5. Record in a spreadsheet: date, market, alert score, asset, price_at_alert, price_at_1h, price_at_4h, price_at_8h, price_at_24h, directional_hit (yes/no)

This manual tracking builds the evidence base you'll need to validate linkages in Phase 3.

---

## Phase 3: Signal Detection + Stage 1 (Weeks 5-8)

### AI STEP 3.1: Build Volatility Surface Constructor

**Prompt for Claude Code:**
> Build `src/detection/vol_surface.py` and `scripts/build_vol_surface.py` implementing REQ-TAJ-004. [Full prompt from original build guide Step 3.1.]

### HUMAN STEP 3.1b: Run Volatility Surface Build

```bash
python scripts/build_vol_surface.py
# Verify output
cat config/vol_surfaces.json | python -m json.tool
```

### AI STEP 3.2: Build Time Adjuster

**Prompt for Claude Code:**
> Build `src/detection/time_adjuster.py` implementing REQ-TAJ-001 through REQ-TAJ-003. [Full prompt from original build guide Step 3.2.]

### AI STEP 3.3: Build Signal Detector

**Prompt for Claude Code:**
> Build `src/detection/signal_detector.py` implementing REQ-SIG-001 through REQ-SIG-004. [Full prompt from original build guide Step 3.3.]

### AI STEP 3.4: Build Feature Assembly

**Prompt for Claude Code:**
> Build `src/models/features.py`. [Full prompt from original build guide Step 3.4.]

### AI STEP 3.5: Build Label Generator

**Prompt for Claude Code:**
> Build `src/models/label_generator.py` and `scripts/generate_labels.py` implementing REQ-SC1-004. [Full prompt from original build guide Step 3.5.]

### AI STEP 3.6: Build Stage 1 Classifier

**Prompt for Claude Code:**
> Build `src/models/stage1.py` and `scripts/train_models.py` implementing REQ-SC1-001 through REQ-SC1-003. [Full prompt from original build guide Step 3.6.]

### AI STEP 3.7: Build Stage 1 Inference Service

**Prompt for Claude Code:**
> Build the Stage1InferenceService. [Full prompt from original build guide Step 3.7.]

### AI STEP 3.8: Build Linkage Validator

**Prompt for Claude Code:**
> Build `src/linkage/validator.py` and all 5 layer modules + decay profiler + `scripts/validate_linkages.py`. [Full prompt from original build guide Step 3.8.]

### AI STEP 3.9: Update Main Entry Point

**Prompt for Claude Code:**
> Update `src/main.py` to run all Phase 3 components. [Full prompt from original build guide Step 3.9.]

### HUMAN STEP 3.10: Generate Training Data and Train Model

```bash
# Generate labels from resolved markets
python scripts/generate_labels.py

# Check training data size
python -c "
import pyarrow.parquet as pq
df = pq.read_table('data/training/stage1/').to_pandas()
print(f'Training samples: {len(df)}')
print(f'Label distribution: {df[\"label\"].value_counts().to_dict()}')
"

# If >= 200 samples, train the model
python scripts/train_models.py
```

### HUMAN STEP 3.11: Resize EC2 for Training (If Needed)

If model training is slow on `t4g.medium`:

```bash
# From your local machine (not the EC2 instance)
aws ec2 stop-instances --instance-ids i-YOUR_INSTANCE_ID
aws ec2 modify-instance-attribute --instance-id i-YOUR_INSTANCE_ID --instance-type m7g.xlarge
aws ec2 start-instances --instance-ids i-YOUR_INSTANCE_ID
# SSH back in, run training, then resize back down
aws ec2 stop-instances --instance-ids i-YOUR_INSTANCE_ID
aws ec2 modify-instance-attribute --instance-id i-YOUR_INSTANCE_ID --instance-type t4g.large
aws ec2 start-instances --instance-ids i-YOUR_INSTANCE_ID
```

### HUMAN STEP 3.12: Run Linkage Validation

```bash
python scripts/validate_linkages.py

# Review results
python -c "
import json
with open('config/linkages.json') as f:
    linkages = json.load(f)
for lid, l in linkages.items():
    status = '✅' if l.get('tradeable') else '❌'
    score = l.get('linkage_composite_score', 0)
    print(f'{status} {lid}: score={score:.2f}, layers={l.get(\"n_layers_passing\",0)}/5')
    if l.get('decay_profile'):
        dp = l['decay_profile']
        print(f'   Half-life: {dp.get(\"half_life_minutes\",\"?\"):.0f} min ({dp.get(\"decay_type\",\"?\")})')
"
```

### HUMAN STEP 3.13: Deploy Phase 3 and Run

```bash
git pull origin main
sudo systemctl restart pm-platform
journalctl -u pm-platform -f
```

### HUMAN STEP 3.14: Phase 3 Gate Validation (After 2+ Weeks)

```python
# 1. Stage 1 AUC
metrics = db.query("SELECT * FROM pm_model_metrics WHERE stage='stage1' ORDER BY timestamp DESC LIMIT 5")
print(f"Stage 1 metrics:\n{metrics}")
# AUC must be > 0.65

# 2. Classified signals flowing
import redis
r = redis.Redis()
print(f"Classified signals: {r.xlen('signals:classified')}")

# 3. At least 2 tradeable linkages
import json
with open('config/linkages.json') as f:
    tradeable = [l for l in json.load(f).values() if l.get('tradeable')]
print(f"Tradeable linkages: {len(tradeable)}")
assert len(tradeable) >= 2

# 4. Training data growing
import pyarrow.parquet as pq
training = pq.read_table('data/training/stage1/').to_pandas()
print(f"Training samples: {len(training)}")
assert len(training) >= 200
```

**Phase 3 complete when all four checks pass. You are now ready to spec and build the Decision Layer (Stage 2 + Risk Engine), informed by real data from your live pipeline.**

---

## Appendix: Common Pitfalls

1. **Polymarket API instability.** Build HTTP calls behind an abstraction layer. Only one method needs updating when the API changes.

2. **QuestDB timestamp precision.** QuestDB defaults to microsecond. API timestamps may be seconds or milliseconds. Always convert to microseconds. Mismatched precision breaks deduplication.

3. **Redis Stream memory.** Set `MAXLEN ~100000` on every XADD. The `~` means approximate trimming (more efficient than exact).

4. **Wallet address normalization.** Always `.lower()` before lookup or comparison.

5. **LightGBM feature order.** Positional indices, not names, at inference. Feature order must match training exactly. Adding/removing features requires retraining.

6. **Time zone confusion.** Polymarket = epoch seconds (UTC). Kalshi = ISO 8601. Market hours = ET. QuestDB = UTC. Work in UTC internally, convert only at boundaries.

7. **Backtest-live divergence.** `prob_change_15m` must be `prob(t) - prob(t-15)`, NEVER `prob(t+15) - prob(t)`.

8. **Premature optimization.** Do not optimize for speed in Phase 1-2. < 100 events/second. Single Python process handles this trivially.

9. **EC2 instance stops.** If your EC2 instance stops (manually or AWS maintenance), the EBS data volume persists but the Elastic IP may detach if not properly associated. Verify association after any stop/start.

10. **Docker disk space.** Docker images and layers accumulate. Run `docker system prune` monthly to reclaim space. Set up a cron job: `0 4 1 * * docker system prune -f >> /var/log/pm-platform/docker-cleanup.log 2>&1`
