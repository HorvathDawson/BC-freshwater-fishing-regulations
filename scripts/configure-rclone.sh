#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────
#  configure-rclone.sh — Configure rclone for Cloudflare R2 access
#
#  Usage:
#    ./scripts/configure-rclone.sh
#
#  This script will prompt you for your Cloudflare R2 credentials and
#  create an rclone remote named "r2" for uploading data files.
#
#  You'll need:
#    - Cloudflare Account ID (from dashboard URL or API)
#    - R2 Access Key ID (from R2 > Manage R2 API Tokens)
#    - R2 Secret Access Key (from R2 > Manage R2 API Tokens)
# ────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Colours ───────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ── Check rclone is installed ─────────────────────────────────────────
if ! command -v rclone &>/dev/null; then
    err "rclone is not installed."
    info "Install rclone:"
    echo "  Linux/WSL: curl https://rclone.org/install.sh | sudo bash"
    echo "  macOS:     brew install rclone"
    echo "  Windows:   winget install Rclone.Rclone"
    exit 1
fi

# ── Check if r2 remote already exists ─────────────────────────────────
if rclone listremotes 2>/dev/null | grep -q "^r2:$"; then
    warn "An 'r2' remote already exists."
    read -p "Do you want to reconfigure it? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        info "Keeping existing configuration."
        exit 0
    fi
    info "Deleting existing r2 remote..."
    rclone config delete r2
fi

# ── Prompt for credentials ────────────────────────────────────────────
echo ""
info "=== Cloudflare R2 Configuration ==="
echo ""
echo "You'll need credentials from Cloudflare Dashboard:"
echo "  1. Go to: https://dash.cloudflare.com/ → R2 → Manage R2 API Tokens"
echo "  2. Create a token with 'Object Read & Write' permissions"
echo "  3. Note your Account ID from the dashboard URL"
echo ""

read -p "Cloudflare Account ID: " ACCOUNT_ID
read -p "R2 Access Key ID: " ACCESS_KEY_ID
read -sp "R2 Secret Access Key: " SECRET_ACCESS_KEY
echo ""

if [[ -z "$ACCOUNT_ID" || -z "$ACCESS_KEY_ID" || -z "$SECRET_ACCESS_KEY" ]]; then
    err "All fields are required."
    exit 1
fi

# ── Create rclone config ──────────────────────────────────────────────
info "Creating rclone 'r2' remote..."

rclone config create r2 s3 \
    provider=Cloudflare \
    access_key_id="$ACCESS_KEY_ID" \
    secret_access_key="$SECRET_ACCESS_KEY" \
    endpoint="https://${ACCOUNT_ID}.r2.cloudflarestorage.com" \
    acl=private \
    --non-interactive

# ── Verify connection ─────────────────────────────────────────────────
info "Testing connection..."
if rclone lsd r2: --s3-no-check-bucket &>/dev/null 2>&1; then
    ok "Connection successful!"
    echo ""
    info "Available buckets:"
    rclone lsd r2: --s3-no-check-bucket 2>/dev/null || true
else
    warn "Could not list buckets (this may be normal if you have no buckets yet)"
fi

echo ""
ok "rclone 'r2' remote configured successfully!"
echo ""
info "You can now run:"
echo "  ./scripts/seed-r2.sh --file <f>  # Upload a single file"
echo "  ./scripts/seed-r2.sh --dry-run   # Preview upload"
echo "  ./scripts/seed-r2.sh             # Upload all data"
