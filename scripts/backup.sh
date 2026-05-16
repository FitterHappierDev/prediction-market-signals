#!/usr/bin/env bash
#
# Daily QuestDB backup, with 7-day local retention and optional S3 sync.
# Designed to be invoked by cron as the `ubuntu` user.
#
# Implements REQ-INF-003.
#
# Note: this is a directory-level tar of /data/questdb while QuestDB may
# still be running. QuestDB durably commits ILP data so a hot tar is usable,
# but for stricter consistency consider running `SNAPSHOT PREPARE;` / `SNAPSHOT
# COMPLETE;` against the QuestDB SQL endpoint before/after the tar.

set -euo pipefail

REPO_DIR="/home/ubuntu/prediction-market-signals"
BACKUP_DIR="/data/backups"
QUESTDB_DIR="/data/questdb"
LOG_DIR="/var/log/pm-platform"
LOG_FILE="${LOG_DIR}/backup.log"
DATE_STAMP="$(date -u +%Y%m%d)"
BACKUP_FILE="${BACKUP_DIR}/questdb-backup-${DATE_STAMP}.tar.gz"

# Pull S3_BACKUP_BUCKET (and anything else) from the project .env so the
# cron environment matches the systemd service.
ENV_FILE="${REPO_DIR}/.env"
if [[ -f "${ENV_FILE}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
    set +a
fi

mkdir -p "${LOG_DIR}" "${BACKUP_DIR}"

# All subsequent stdout/stderr appended to the log file, line-prefixed
# with a UTC timestamp.
exec > >(awk 'BEGIN { OFS=" " } { print strftime("%Y-%m-%dT%H:%M:%SZ", systime(), 1), $0; fflush(); }' >> "${LOG_FILE}") 2>&1

echo "=== backup.sh start (host=$(hostname)) ==="

if [[ ! -d "${QUESTDB_DIR}" ]]; then
    echo "ERROR: QuestDB data dir ${QUESTDB_DIR} missing; nothing to back up."
    exit 1
fi

echo "Creating ${BACKUP_FILE}"
tar -czf "${BACKUP_FILE}" -C /data questdb
echo "Created: $(ls -lh "${BACKUP_FILE}" | awk '{print $5, $9}')"

echo "Pruning local backups older than 7 days"
find "${BACKUP_DIR}" -maxdepth 1 -name 'questdb-backup-*.tar.gz' -type f -mtime +7 -print -delete || true

if [[ -n "${S3_BACKUP_BUCKET:-}" ]]; then
    echo "Syncing ${BACKUP_DIR}/ to s3://${S3_BACKUP_BUCKET}/backups/"
    aws s3 sync "${BACKUP_DIR}/" "s3://${S3_BACKUP_BUCKET}/backups/" --delete
else
    echo "S3_BACKUP_BUCKET not set; skipping S3 sync"
fi

echo "=== backup.sh complete ==="
