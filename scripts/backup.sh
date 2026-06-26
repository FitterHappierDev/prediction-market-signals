#!/usr/bin/env bash
#
# Daily QuestDB backup, with 7-day local retention and optional S3 sync.
# Designed to be invoked by cron as the `ubuntu` user.
#
# Implements REQ-INF-003.
#
# Wraps the tar in `SNAPSHOT PREPARE;` / `SNAPSHOT COMPLETE;` so the
# archive is point-in-time consistent and tar doesn't fight live writes
# (previously: "file changed as we read it" warnings → tar exit 1 →
# `set -e` killed the script before prune / S3, retention never ran).
# Medallion-navigator now owns long-term S3 retention via a 03:30 cron
# that ships these tars to s3://medallion-navigator-backups.

set -euo pipefail

REPO_DIR="/home/ubuntu/prediction-market-signals"
BACKUP_DIR="/data/backups"
QUESTDB_DIR="/data/questdb"
LOG_DIR="/var/log/pm-platform"
LOG_FILE="${LOG_DIR}/backup.log"
DATE_STAMP="$(date -u +%Y%m%d)"
BACKUP_FILE="${BACKUP_DIR}/questdb-backup-${DATE_STAMP}.tar.gz"
QDB_HTTP="http://localhost:9000/exec"

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

# ----- SNAPSHOT wrap --------------------------------------------------------
#
# SNAPSHOT PREPARE checkpoints WAL state and tells QuestDB to hold off on
# applying further writes to the on-disk partition files until COMPLETE.
# Reads + ingestion to the WAL continue normally — only the on-disk apply
# is paused. Subsequent tar therefore sees a stable snapshot.
#
# CRITICAL: if PREPARE succeeds, COMPLETE MUST run, otherwise QuestDB is
# locked in a "snapshot pending" state forever, new WAL segments pile up,
# and storage grows. The trap below ensures COMPLETE fires on every exit
# path (success, failure, signal) when SNAPSHOT_ACTIVE=1.
#
# Tested QuestDB versions: 8.0+. Older versions had subtle semantics
# around partial snapshots; if we ever downgrade, revisit.

SNAPSHOT_ACTIVE=0

snapshot_prepare() {
    echo "SNAPSHOT PREPARE"
    curl -sf -G "${QDB_HTTP}" --data-urlencode "query=SNAPSHOT PREPARE" -o /dev/null
}

snapshot_complete() {
    echo "SNAPSHOT COMPLETE"
    # || true so we don't mask the prior error during cleanup. If COMPLETE
    # itself fails, the next run's PREPARE will fail loudly anyway.
    curl -sf -G "${QDB_HTTP}" --data-urlencode "query=SNAPSHOT COMPLETE" -o /dev/null || true
}

cleanup() {
    if [[ "${SNAPSHOT_ACTIVE}" == "1" ]]; then
        echo "(trap) releasing snapshot"
        snapshot_complete
        SNAPSHOT_ACTIVE=0
    fi
}
trap cleanup EXIT

snapshot_prepare
SNAPSHOT_ACTIVE=1

echo "Creating ${BACKUP_FILE}"
tar -czf "${BACKUP_FILE}" -C /data questdb
echo "Created: $(ls -lh "${BACKUP_FILE}" | awk '{print $5, $9}')"

snapshot_complete
SNAPSHOT_ACTIVE=0

# ----- retention + offload --------------------------------------------------

echo "Pruning local backups older than 7 days"
find "${BACKUP_DIR}" -maxdepth 1 -name 'questdb-backup-*.tar.gz' -type f -mtime +7 -print -delete || true

if [[ -n "${S3_BACKUP_BUCKET:-}" ]]; then
    echo "Syncing ${BACKUP_DIR}/ to s3://${S3_BACKUP_BUCKET}/backups/"
    aws s3 sync "${BACKUP_DIR}/" "s3://${S3_BACKUP_BUCKET}/backups/" --delete
else
    # Empty bucket = expected steady state. Medallion-navigator's 03:30
    # cron offloads /data/backups to s3://medallion-navigator-backups —
    # PM running its own --delete sync would fight medallion's retention.
    # See BUILD_PROGRESS.md "Operational decision: S3 backup ownership".
    echo "S3_BACKUP_BUCKET not set; skipping S3 sync (medallion offload owns durable archive)"
fi

echo "=== backup.sh complete ==="
