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

# Check by listing the specific bucket (lsd fails if token lacks ListBuckets permission)
if ! rclone ls "$R2_BUCKET" --max-depth 0 &>/dev/null 2>&1; then
    err "Cannot access R2 bucket. Check rclone 'r2' config. See DEPLOYMENT.md for setup."
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

# ── Write & upload data_version.json ─────────────────────────────────
# Called automatically before any upload so end-users always get a fresh
# version string on their next page load, which cache-busts the PMTiles URLs.
write_version() {
    local version
    version="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    local version_file="$DATA_DIR/data_version.json"
    echo "{\"v\":\"$version\"}" > "$version_file"
    info "Wrote $version_file (version: $version)"
}

# ── Main ──────────────────────────────────────────────────────────────
case "${1:-}" in
    --tiles)
        write_version
        upload_tiles
        upload_file "$DATA_DIR/data_version.json"
        ;;
    --json)
        write_version
        upload_json
        upload_file "$DATA_DIR/data_version.json"
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
        echo "  --file <f>    Upload a single file (no version bump)"
        echo "  --help        Show this help"
        exit 0
        ;;
    *)
        write_version
        upload_all
        upload_file "$DATA_DIR/data_version.json"
        ;;
esac

echo ""
ok "Data upload complete! Verify at: https://bc-fishing-r2.horvath-dawson.workers.dev/"
