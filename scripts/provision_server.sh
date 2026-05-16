#!/usr/bin/env bash
#
# Provisions a fresh Ubuntu 24.04 ARM64 (Graviton) EC2 instance for the
# Prediction Market Signal Platform. Idempotent: safe to re-run.
#
# Usage:  sudo ./scripts/provision_server.sh
# Optional env: REPO_URL=<git-url>  (only needed if the repo isn't already
# cloned at /home/ubuntu/prediction-market-signals)
#
# Implements REQ-INF-001.

set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
    echo "ERROR: Must be run as root. Use: sudo $0" >&2
    exit 1
fi

REPO_USER="ubuntu"
REPO_HOME="/home/${REPO_USER}"
REPO_DIR="${REPO_HOME}/prediction-market-signals"
DATA_DIR="/data"
LOG_DIR="/var/log/pm-platform"
SYSTEMD_UNIT="/etc/systemd/system/pm-platform.service"

step() { printf '\n==> %s\n' "$*"; }

# --- 1. Base packages ---------------------------------------------------------
step "Updating apt and installing base packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    git \
    htop \
    tmux \
    jq \
    unzip \
    build-essential \
    docker.io \
    docker-compose-v2 \
    python3.12 \
    python3.12-venv \
    python3.12-dev \
    python3-pip

# --- 1b. AWS CLI v2 (not in Ubuntu 24.04 apt repos) -------------------------
if ! command -v aws >/dev/null 2>&1; then
    step "Installing AWS CLI v2"
    AWSCLI_TMP="$(mktemp -d)"
    curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "${AWSCLI_TMP}/awscli.zip"
    unzip -q "${AWSCLI_TMP}/awscli.zip" -d "${AWSCLI_TMP}"
    "${AWSCLI_TMP}/aws/install" --update
    rm -rf "${AWSCLI_TMP}"
else
    step "AWS CLI already installed: $(aws --version 2>&1)"
fi

# --- 2. Docker group for ubuntu ----------------------------------------------
step "Adding ${REPO_USER} to docker group"
usermod -aG docker "${REPO_USER}"

# --- 3. Enable and start Docker ----------------------------------------------
step "Enabling and starting Docker"
systemctl enable --now docker

# --- 4. /data directory tree -------------------------------------------------
step "Creating ${DATA_DIR} directory tree"
mkdir -p \
    "${DATA_DIR}/questdb" \
    "${DATA_DIR}/redis" \
    "${DATA_DIR}/grafana" \
    "${DATA_DIR}/mlflow" \
    "${DATA_DIR}/parquet/features/raw" \
    "${DATA_DIR}/parquet/features/computed" \
    "${DATA_DIR}/parquet/features/training" \
    "${DATA_DIR}/parquet/models/stage1" \
    "${DATA_DIR}/parquet/models/stage2" \
    "${DATA_DIR}/config" \
    "${DATA_DIR}/backups"

# --- 5. Ownership ------------------------------------------------------------
step "Setting ${DATA_DIR} ownership to ${REPO_USER}:${REPO_USER}"
chown -R "${REPO_USER}:${REPO_USER}" "${DATA_DIR}"

# Service-specific UIDs inside their containers:
#   grafana:11.0.0   runs as UID/GID 472
#   questdb:8.0.0    runs as UID/GID 10001 (creates its own files at startup)
#   redis:7-alpine   runs as UID/GID 999   (creates its own files at startup)
#   mlflow           runs as root          (no chown needed)
# Grafana is the strict one — it needs the host bind mount writable on first boot.
chown -R 472:472 "${DATA_DIR}/grafana"

# --- 6. Clone repo (skip if exists) ------------------------------------------
if [[ -d "${REPO_DIR}/.git" ]]; then
    step "Repo already present at ${REPO_DIR}, skipping clone"
elif [[ -n "${REPO_URL:-}" ]]; then
    step "Cloning ${REPO_URL} to ${REPO_DIR}"
    sudo -u "${REPO_USER}" git clone "${REPO_URL}" "${REPO_DIR}"
else
    echo "ERROR: ${REPO_DIR} does not exist and REPO_URL is not set." >&2
    echo "Either clone the repo first as ${REPO_USER}, or:" >&2
    echo "  sudo -E REPO_URL=git@github.com:owner/repo.git $0" >&2
    exit 1
fi

# --- 7. Symlinks: config/ -> /data/config, data/ -> /data/parquet ------------
# Seed /data/config from the repo (one-time, only if /data/config is empty).
# Note: after this, the repo's `config/` directory becomes a symlink, so
# `git status` will show the symlink as an untracked change. Future
# `git pull` updates to files inside config/ still apply because git
# transparently follows the directory symlink.
step "Setting up symlinks"
if [[ -d "${REPO_DIR}/config" ]] && [[ ! -L "${REPO_DIR}/config" ]]; then
    if [[ -z "$(ls -A "${DATA_DIR}/config" 2>/dev/null || true)" ]]; then
        echo "    Seeding ${DATA_DIR}/config from ${REPO_DIR}/config"
        cp -a "${REPO_DIR}/config/." "${DATA_DIR}/config/"
        chown -R "${REPO_USER}:${REPO_USER}" "${DATA_DIR}/config"
    fi
    rm -rf "${REPO_DIR}/config"
    sudo -u "${REPO_USER}" ln -s "${DATA_DIR}/config" "${REPO_DIR}/config"
fi
if [[ ! -e "${REPO_DIR}/data" ]] && [[ ! -L "${REPO_DIR}/data" ]]; then
    sudo -u "${REPO_USER}" ln -s "${DATA_DIR}/parquet" "${REPO_DIR}/data"
fi

# --- 8. Python venv + dependencies -------------------------------------------
step "Creating venv at ${REPO_DIR}/.venv and installing requirements"
if [[ ! -d "${REPO_DIR}/.venv" ]]; then
    sudo -u "${REPO_USER}" python3.12 -m venv "${REPO_DIR}/.venv"
fi
sudo -u "${REPO_USER}" "${REPO_DIR}/.venv/bin/pip" install --upgrade pip wheel setuptools
sudo -u "${REPO_USER}" "${REPO_DIR}/.venv/bin/pip" install -r "${REPO_DIR}/requirements.txt"

# --- 9. Systemd service ------------------------------------------------------
step "Installing systemd service"
install -m 0644 "${REPO_DIR}/scripts/pm-platform.service" "${SYSTEMD_UNIT}"
systemctl daemon-reload
systemctl enable pm-platform.service
# Deliberately not starting the service here; HUMAN STEP 1.7 starts it
# after the operator has verified data flow manually.

# --- 10. Daily backup cron at 03:00 UTC --------------------------------------
step "Installing daily backup cron job for ${REPO_USER}"
BACKUP_CMD="${REPO_DIR}/scripts/backup.sh"
CRON_LINE="0 3 * * * ${BACKUP_CMD}"
( sudo -u "${REPO_USER}" crontab -l 2>/dev/null | grep -vF "${BACKUP_CMD}" || true
  echo "${CRON_LINE}"
) | sudo -u "${REPO_USER}" crontab -

# --- 11. Log directory + logrotate -------------------------------------------
step "Creating ${LOG_DIR} and logrotate config"
mkdir -p "${LOG_DIR}"
chown "${REPO_USER}:${REPO_USER}" "${LOG_DIR}"
cat > /etc/logrotate.d/pm-platform <<EOF
${LOG_DIR}/*.log {
    daily
    rotate 7
    maxsize 500M
    missingok
    compress
    delaycompress
    notifempty
    copytruncate
    su ${REPO_USER} ${REPO_USER}
}
EOF

step "Provisioning complete."
cat <<EOF

Next steps:
  1. Confirm .env exists at ${REPO_DIR}/.env (scp from your local machine)
  2. Start infra:        cd ${REPO_DIR} && docker compose up -d
  3. Verify containers:  docker compose ps
  4. Start the app:      sudo systemctl start pm-platform
  5. Tail logs:          journalctl -u pm-platform -f

Reminder: a fresh group membership for 'docker' takes effect on next login.
EOF
