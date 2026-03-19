#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────
#  promote-r2.sh — Copy data from staging R2 bucket to production R2
#
#  Usage:
#    ./scripts/promote-r2.sh              # copy staging → production
#    ./scripts/promote-r2.sh --dry-run    # preview only
#    ./scripts/promote-r2.sh --clean      # delete production files not in staging
#
#  Prerequisites:
#    - rclone configured with an "r2" remote
# ────────────────────────────────────────────────────────────────────────
set -euo pipefail

SRC="r2:bc-fishing-regulations-staging"
DST="r2:bc-fishing-regulations"
RCLONE_FLAGS="--s3-no-check-bucket --progress --checksum --transfers 32"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

if ! command -v rclone &>/dev/null; then
  err "rclone is not installed."
  exit 1
fi

case "${1:-}" in
  --dry-run)
    info "=== Dry run: staging → production ==="
    rclone copy "$SRC" "$DST" $RCLONE_FLAGS --dry-run
    ;;
  --clean)
    warn "This will DELETE production files that don't exist in staging."
    read -rp "Continue? [y/N] " confirm
    if [[ "$confirm" =~ ^[Yy]$ ]]; then
      info "=== Syncing staging → production (with delete) ==="
      rclone sync "$SRC" "$DST" $RCLONE_FLAGS
      ok "Production bucket synced to match staging (old files removed)."
    else
      info "Aborted."
    fi
    ;;
  --help|-h)
    echo "Usage: $0 [--dry-run | --clean | --help]"
    echo ""
    echo "  (no args)     Copy staging → production (additive, no deletes)"
    echo "  --dry-run     Preview what would be copied"
    echo "  --clean       Sync with delete (removes prod files not in staging)"
    exit 0
    ;;
  *)
    info "=== Copying staging → production ==="
    rclone copy "$SRC" "$DST" $RCLONE_FLAGS
    ok "Staging data promoted to production."
    ;;
esac
