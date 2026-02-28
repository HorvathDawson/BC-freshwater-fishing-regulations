#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────
#  deploy-data.sh — Upload data files to Cloudflare R2
#
#  Usage:
#    ./scripts/deploy-data.sh              # Upload ALL data files to R2
#    ./scripts/deploy-data.sh --tiles      # Upload only .pmtiles files
#    ./scripts/deploy-data.sh --json       # Upload only .json data files
#    ./scripts/deploy-data.sh --file <f>   # Upload a single specific file
#
#  Prerequisites:
#    - rclone installed and configured with an "r2" remote
#      (see webapp/DEPLOYMENT.md for setup instructions)
# ────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBAPP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR="$WEBAPP_DIR/public/data"
R2_BUCKET="r2:bc-fishing-regulations"
RCLONE_FLAGS="--s3-no-check-bucket --progress"

# ── Colours ───────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ── Pre-flight checks ────────────────────────────────────────────────
if ! command -v rclone &>/dev/null; then
    err "rclone is not installed. Install it: curl https://rclone.org/install.sh | sudo bash"
    exit 1
fi

if ! rclone lsd r2: --s3-no-check-bucket &>/dev/null 2>&1; then
    err "rclone 'r2' remote is not configured. See DEPLOYMENT.md for setup."
    exit 1
fi

# ── Upload helpers ────────────────────────────────────────────────────
upload_file() {
    local file="$1"
    local basename
    basename="$(basename "$file")"
    local size
    size="$(du -h "$file" | cut -f1)"
    info "Uploading $basename ($size) ..."
    rclone copy "$file" "$R2_BUCKET/" $RCLONE_FLAGS
    ok "$basename uploaded."
}

upload_tiles() {
    info "=== Uploading PMTiles ==="
    for f in "$DATA_DIR"/*.pmtiles; do
        [ -f "$f" ] && upload_file "$f"
    done
}

upload_json() {
    info "=== Uploading JSON data ==="
    for f in "$DATA_DIR"/*.json; do
        [ -f "$f" ] && upload_file "$f"
    done
}

upload_all() {
    upload_tiles
    upload_json
}

# ── Main ──────────────────────────────────────────────────────────────
case "${1:-}" in
    --tiles)
        upload_tiles
        ;;
    --json)
        upload_json
        ;;
    --file)
        if [ -z "${2:-}" ]; then
            err "Usage: $0 --file <path-to-file>"
            exit 1
        fi
        if [ ! -f "$2" ]; then
            err "File not found: $2"
            exit 1
        fi
        upload_file "$2"
        ;;
    --help|-h)
        echo "Usage: $0 [--tiles | --json | --file <path> | --help]"
        echo ""
        echo "  (no args)     Upload all data files (pmtiles + json)"
        echo "  --tiles       Upload only .pmtiles files"
        echo "  --json        Upload only .json data files"
        echo "  --file <f>    Upload a single file"
        echo "  --help        Show this help"
        exit 0
        ;;
    *)
        upload_all
        ;;
esac

echo ""
ok "Data upload complete! Verify at: https://bc-fishing-r2.horvath-dawson.workers.dev/"
