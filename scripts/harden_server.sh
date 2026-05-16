#!/usr/bin/env bash
#
# Basic security hardening for the PM Signal Platform EC2 instance.
# Idempotent: safe to re-run.
#
# Usage:  sudo ./scripts/harden_server.sh <ALLOWED_IP_CIDR>
# Example: sudo ./scripts/harden_server.sh 203.0.113.42/32
#
# WARNING: This locks SSH/Grafana/MLflow to a single source IP. If you give
# it the wrong IP, you will lock yourself out of the instance. AWS Security
# Group rules still apply on top — fix is via the AWS console if needed.
#
# Implements REQ-INF-004.

set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
    echo "ERROR: Must be run as root. Use: sudo $0 <ALLOWED_IP_CIDR>" >&2
    exit 1
fi

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <ALLOWED_IP_CIDR>" >&2
    echo "Example: $0 203.0.113.42/32" >&2
    exit 1
fi

ALLOWED_IP="$1"

# Validate CIDR-ish shape (rudimentary): a.b.c.d/N
if ! [[ "${ALLOWED_IP}" =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/[0-9]{1,2}$ ]]; then
    echo "ERROR: '${ALLOWED_IP}' does not look like CIDR notation (expected a.b.c.d/N)." >&2
    exit 1
fi

step() { printf '\n==> %s\n' "$*"; }

# --- 1. UFW ------------------------------------------------------------------
step "Installing and configuring UFW for ${ALLOWED_IP}"
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y --no-install-recommends ufw

ufw default deny incoming
ufw default allow outgoing

# Allow SSH/Grafana/MLflow from the operator IP only. UFW deduplicates
# identical rules, so re-running this script does not stack them.
ufw allow from "${ALLOWED_IP}" to any port 22   proto tcp comment 'SSH'
ufw allow from "${ALLOWED_IP}" to any port 3000 proto tcp comment 'Grafana'
ufw allow from "${ALLOWED_IP}" to any port 5000 proto tcp comment 'MLflow'

ufw --force enable
ufw status verbose

# --- 2. Disable password SSH auth -------------------------------------------
step "Disabling SSH password authentication"
SSHD_CONFIG="/etc/ssh/sshd_config"
# Replace whatever value the directive currently has (or add it if absent).
if grep -qE '^[#[:space:]]*PasswordAuthentication' "${SSHD_CONFIG}"; then
    sed -ri 's|^[#[:space:]]*PasswordAuthentication[[:space:]]+.*$|PasswordAuthentication no|' "${SSHD_CONFIG}"
else
    echo 'PasswordAuthentication no' >> "${SSHD_CONFIG}"
fi
# Also strip any override files (Ubuntu 24.04 ships /etc/ssh/sshd_config.d/*).
for f in /etc/ssh/sshd_config.d/*.conf; do
    [[ -f "$f" ]] || continue
    sed -ri 's|^[#[:space:]]*PasswordAuthentication[[:space:]]+.*$|PasswordAuthentication no|' "$f"
done
sshd -t
systemctl restart ssh

# --- 3. Unattended security upgrades -----------------------------------------
step "Enabling unattended-upgrades"
apt-get install -y --no-install-recommends unattended-upgrades
cat > /etc/apt/apt.conf.d/20auto-upgrades <<'EOF'
APT::Periodic::Update-Package-Lists "1";
APT::Periodic::Unattended-Upgrade "1";
APT::Periodic::AutocleanInterval "7";
EOF
systemctl enable --now unattended-upgrades

# --- 4. fail2ban for sshd ----------------------------------------------------
step "Installing and configuring fail2ban (sshd jail)"
apt-get install -y --no-install-recommends fail2ban
cat > /etc/fail2ban/jail.local <<'EOF'
[DEFAULT]
banaction = ufw

[sshd]
enabled  = true
maxretry = 5
bantime  = 3600
findtime = 600
EOF
systemctl enable --now fail2ban
systemctl restart fail2ban

step "Hardening complete."
echo
echo "Verify before logging out:"
echo "  - Open a SECOND ssh session from ${ALLOWED_IP} to confirm key-based auth still works."
echo "  - 'sudo ufw status' should list rules limited to ${ALLOWED_IP}."
echo "  - 'sudo fail2ban-client status sshd' should show the sshd jail active."
