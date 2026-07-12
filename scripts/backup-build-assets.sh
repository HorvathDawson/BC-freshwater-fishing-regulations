#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────
#  backup-build-assets.sh — Mirror expensive-to-regenerate build inputs to R2
#
#  Backs up the artifacts that are slow or costly to recreate (LLM parsing
#  output, the extracted synopsis rows, and the processed DFO tidal boundary)
#  into a dedicated "build assets" bucket so a future rebuild can pull them
#  instead of re-running extraction / parsing / geometry processing.
#
#  What gets backed up (into r2:bc-fishing-build-assets):
#    • parsing/     ← output/pipeline/parsing/            (LLM parse output)
#    • extraction/  ← output/pipeline/extraction/synopsis_raw_data.json
#    • DFO_TIDAL_BOUNDARY.gpkg ← server-side copy from the prod data bucket
#
#  Deliberately NOT backed up here:
#    • bc.pmtiles   — large OSM basemap, regenerated/hosted separately
#
#  Usage:
#    ./scripts/backup-build-assets.sh              # run the backup
#    ./scripts/backup-build-assets.sh --dry-run    # preview only
#
#  Prerequisites:
#    - rclone configured with an "r2" remote (see STAGING.md).
#    - The destination bucket must already exist. The scoped R2 API token
#      cannot create buckets, so create it once in the Cloudflare dashboard:
#        Workers & Pages → R2 → Create bucket → "bc-fishing-build-assets"
#    - Pipeline has been run so output/pipeline/{parsing,extraction} exist
#      (only needed for those two backups; the tidal copy is server-side).
# ────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PROD_BUCKET="r2:bc-fishing-regulations"
BACKUP_BUCKET="r2:bc-fishing-build-assets"

PARSING_DIR="$ROOT/output/pipeline/parsing"
EXTRACTION_FILE="$ROOT/output/pipeline/extraction/synopsis_raw_data.json"
TIDAL_FILENAME="DFO_TIDAL_BOUNDARY.gpkg"

RCLONE_FLAGS="--s3-no-check-bucket --checksum --transfers 8"
DRY_RUN=""

# ── Colours / logging ─────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ── Arg parsing ───────────────────────────────────────────────────────
case "${1:-}" in
  --dry-run) DRY_RUN="--dry-run"; info "Dry run — nothing will be written." ;;
  --help|-h)
    echo "Usage: $0 [--dry-run | --help]"
    echo "  Backs up parsing output, extraction input, and the DFO tidal"
    echo "  boundary to $BACKUP_BUCKET (bc.pmtiles is intentionally excluded)."
    exit 0
    ;;
  "") ;;
  *) err "Unknown argument: $1 (see --help)"; exit 1 ;;
esac

# ── Pre-flight ────────────────────────────────────────────────────────
if ! command -v rclone &>/dev/null; then
  err "rclone is not installed. Install it: curl https://rclone.org/install.sh | sudo bash"
  exit 1
fi

# The scoped token can't list/create buckets, so probe the destination by
# listing it. "directory not found" / NoSuchBucket means it doesn't exist yet.
if ! rclone lsf "$BACKUP_BUCKET" --s3-no-check-bucket &>/dev/null; then
  err "Destination bucket '$BACKUP_BUCKET' is not reachable."
  err "Create it once in the Cloudflare dashboard (R2 → Create bucket):"
  err "    bc-fishing-build-assets"
  err "then re-run this script."
  exit 1
fi

# ── 1. Parsing output ─────────────────────────────────────────────────
if [[ -d "$PARSING_DIR" ]]; then
  info "Backing up parsing output → $BACKUP_BUCKET/parsing/"
  rclone copy "$PARSING_DIR" "$BACKUP_BUCKET/parsing/" \
    $RCLONE_FLAGS $DRY_RUN --exclude ".DS_Store" --exclude "**/.DS_Store"
  ok "Parsing output backed up."
else
  warn "Parsing dir not found ($PARSING_DIR) — skipping. Run the pipeline first."
fi

# ── 2. Extraction input ───────────────────────────────────────────────
if [[ -f "$EXTRACTION_FILE" ]]; then
  info "Backing up extraction input → $BACKUP_BUCKET/extraction/"
  rclone copy "$EXTRACTION_FILE" "$BACKUP_BUCKET/extraction/" \
    $RCLONE_FLAGS $DRY_RUN
  ok "Extraction input backed up."
else
  warn "Extraction file not found ($EXTRACTION_FILE) — skipping."
fi

# ── 3. DFO tidal boundary (server-side copy from prod) ────────────────
# Copying remote→remote on the same R2 account is server-side: no bytes
# travel through this machine.
info "Mirroring $TIDAL_FILENAME from prod → $BACKUP_BUCKET/ (server-side)"
if rclone copyto "$PROD_BUCKET/$TIDAL_FILENAME" "$BACKUP_BUCKET/$TIDAL_FILENAME" \
    $RCLONE_FLAGS $DRY_RUN; then
  ok "Tidal boundary backed up."
else
  warn "Could not copy $TIDAL_FILENAME from prod — is it present in $PROD_BUCKET?"
fi

ok "Build-assets backup complete → $BACKUP_BUCKET"
