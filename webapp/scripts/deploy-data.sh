#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────
#  deploy-data.sh — Upload deploy/ directory to Cloudflare R2
#
#  The deploy/ directory is the single source of truth for all files
#  served by the R2 Worker: tier0.json, pmtiles, admin_visibility.json,
#  shards, and data_version.json.
#
#  Usage:
#    ./scripts/deploy-data.sh              # Upload entire deploy/ to R2
#    ./scripts/deploy-data.sh --file <f>   # Upload a single specific file
#    ./scripts/deploy-data.sh --dry-run    # Show what would be uploaded
#
#  Prerequisites:
#    - rclone installed and configured with an "r2" remote
#      (see webapp/DEPLOYMENT.md for setup instructions)
#    - Pipeline has been run to populate deploy/
# ────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBAPP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_DIR="$(cd "$WEBAPP_DIR/.." && pwd)"
DEPLOY_DIR="$PROJECT_DIR/output/pipeline/deploy"
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

if [ ! -d "$DEPLOY_DIR" ]; then
    err "Deploy directory not found: $DEPLOY_DIR"
    err "Run the pipeline first: python -m pipeline --step all"
    exit 1
fi

# Sanity check: tier0.json and at least one shard dir should exist
if [ ! -f "$DEPLOY_DIR/tier0.json" ]; then
    err "tier0.json not found in deploy/ — pipeline may not have completed."
    exit 1
fi

# Check by listing the specific bucket (lsd fails if token lacks ListBuckets permission)
if ! rclone ls "$R2_BUCKET" --max-depth 0 &>/dev/null 2>&1; then
    err "Cannot access R2 bucket. Check rclone 'r2' config. See DEPLOYMENT.md for setup."
    exit 1
fi

# ── Upload helper for single files ───────────────────────────────────
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

# ── Write data_version.json into deploy/ ─────────────────────────────
# Generates a fresh timestamp so end-users get cache-busted PMTiles URLs.
write_version() {
    local version
    version="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    local version_file="$DEPLOY_DIR/data_version.json"
    echo "{\"v\":\"$version\"}" > "$version_file"
    info "Wrote data_version.json (version: $version)"
}

# ── Upload deploy/ directory ─────────────────────────────────────────
# Uses rclone copy (additive) — won't delete files in R2 not present
# locally. Shard paths are version-prefixed (shards/v7/) so old orphans
# from previous versions are harmless and can be cleaned up manually.
upload_deploy() {
    info "=== Uploading deploy/ to R2 ==="

    local file_count
    file_count="$(find "$DEPLOY_DIR" -type f ! -path '*/_tile_temp/*' | wc -l)"
    info "Files to upload: $file_count"

    rclone copy "$DEPLOY_DIR" "$R2_BUCKET/" \
        $RCLONE_FLAGS \
        --exclude "_tile_temp/**" \
        --exclude "layer_manifest.json" \
        --checksum \
        --transfers 32

    ok "Deploy complete ($file_count files)."
}

# ── Main ──────────────────────────────────────────────────────────────
case "${1:-}" in
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
    --dry-run)
        write_version
        info "=== Dry run: showing what would be uploaded ==="
        rclone copy "$DEPLOY_DIR" "$R2_BUCKET/" \
            $RCLONE_FLAGS \
            --exclude "_tile_temp/**" \
            --exclude "layer_manifest.json" \
            --checksum \
            --dry-run
        ;;
    --help|-h)
        echo "Usage: $0 [--file <path> | --dry-run | --help]"
        echo ""
        echo "  (no args)     Upload entire deploy/ directory to R2"
        echo "  --file <f>    Upload a single file (no version bump)"
        echo "  --dry-run     Show what would be uploaded without uploading"
        echo "  --help        Show this help"
        exit 0
        ;;
    *)
        write_version
        upload_deploy
        ;;
esac

echo ""
ok "Data upload complete! Verify at: https://bc-fishing-r2.horvath-dawson.workers.dev/"
