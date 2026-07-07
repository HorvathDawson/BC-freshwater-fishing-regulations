#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────
#  seed-r2.sh — Upload deploy/ directory to Cloudflare R2 via rclone
#
#  Usage:
#    ./scripts/seed-r2.sh                          # staging (default)
#    DEPLOY_ENV=production ./scripts/seed-r2.sh    # production
#    ./scripts/seed-r2.sh --file <path>            # single file
#    ./scripts/seed-r2.sh --dry-run                # preview only
#
#  Prerequisites:
#    - rclone installed and configured with an "r2" remote
#      (see STAGING.md for setup instructions)
#    - Pipeline has been run to populate deploy/
# ────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEPLOY_DIR="$ROOT/output/pipeline/deploy"
BATHY_PDF_DIR="$ROOT/data/bathymetry_pdfs"
RCLONE_FLAGS="--s3-no-check-bucket --progress"

DEPLOY_ENV="${DEPLOY_ENV:-staging}"

case "$DEPLOY_ENV" in
  staging)    BUCKET="r2:bc-fishing-regulations-staging" ;;
  production) BUCKET="r2:bc-fishing-regulations" ;;
  *) echo "ERROR: Unknown DEPLOY_ENV=$DEPLOY_ENV" >&2; exit 1 ;;
esac

# ── Colours ───────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ── Pre-flight checks ────────────────────────────────────────────────
if ! command -v rclone &>/dev/null; then
  err "rclone is not installed. Install it: curl https://rclone.org/install.sh | sudo bash"
  exit 1
fi

if [[ ! -d "$DEPLOY_DIR" ]]; then
  err "Deploy directory not found: $DEPLOY_DIR"
  err "Run the pipeline first: python -m pipeline --step all"
  exit 1
fi

if [[ ! -f "$DEPLOY_DIR/tier0.json" ]]; then
  err "tier0.json not found in deploy/ — pipeline may not have completed."
  exit 1
fi

# ── Write data_version.json ──────────────────────────────────────────
write_version() {
  local version
  version="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "{\"v\":\"$version\"}" > "$DEPLOY_DIR/data_version.json"
  info "Wrote data_version.json (version: $version)"
}

# ── Upload full deploy/ ──────────────────────────────────────────────
upload_deploy() {
  info "=== Uploading deploy/ to $BUCKET ==="

  local file_count
  file_count="$(find "$DEPLOY_DIR" -type f ! -path '*/_tile_temp/*' | wc -l)"
  info "Files to upload: $file_count"

  rclone copy "$DEPLOY_DIR" "$BUCKET/" \
    $RCLONE_FLAGS \
    --exclude "_tile_temp/**" \
    --exclude "layer_manifest.json" \
    --checksum \
    --transfers 32

  ok "Deploy complete ($file_count files → $BUCKET)."
}

# ── Upload bathymetry survey-map PDFs ────────────────────────────────
# The PDFs live in data/bathymetry_pdfs/ (populated by data/fetch_data.py),
# separate from the regenerated deploy/ tree, and are mirrored to the bucket
# under bathymetry/ so the web app can serve depth maps from our own domain.
# Idempotent + checksum-based, so unchanged PDFs are skipped on re-runs.
upload_bathymetry_pdfs() {
  if [[ ! -d "$BATHY_PDF_DIR" ]]; then
    info "No bathymetry PDF dir ($BATHY_PDF_DIR) — skipping map upload."
    return
  fi

  local count
  count="$(find "$BATHY_PDF_DIR" -type f -name '*.pdf' | wc -l | tr -d ' ')"
  if [[ "$count" -eq 0 ]]; then
    info "No bathymetry PDFs found in $BATHY_PDF_DIR — skipping map upload."
    return
  fi

  info "=== Uploading $count bathymetry PDF(s) → $BUCKET/bathymetry/ ==="
  rclone copy "$BATHY_PDF_DIR" "$BUCKET/bathymetry/" \
    $RCLONE_FLAGS \
    --include "*.pdf" \
    --checksum \
    --transfers 32
  ok "Bathymetry maps uploaded ($count files → $BUCKET/bathymetry/)."
}

# ── Upload single file ───────────────────────────────────────────────
upload_file() {
  local file="$1"
  local size
  size="$(du -h "$file" | cut -f1)"
  info "Uploading $(basename "$file") ($size) → $BUCKET"
  rclone copy "$file" "$BUCKET/" $RCLONE_FLAGS
  ok "$(basename "$file") uploaded."
}

# ── Main ──────────────────────────────────────────────────────────────
case "${1:-}" in
  --file)
    if [[ -z "${2:-}" ]]; then err "Usage: $0 --file <path>"; exit 1; fi
    if [[ ! -f "$2" ]]; then err "File not found: $2"; exit 1; fi
    upload_file "$2"
    ;;
  --dry-run)
    write_version
    info "=== Dry run: showing what would be uploaded ==="
    rclone copy "$DEPLOY_DIR" "$BUCKET/" \
      $RCLONE_FLAGS \
      --exclude "_tile_temp/**" \
      --exclude "layer_manifest.json" \
      --checksum \
      --dry-run
    if [[ -d "$BATHY_PDF_DIR" ]]; then
      info "=== Dry run: bathymetry PDFs → $BUCKET/bathymetry/ ==="
      rclone copy "$BATHY_PDF_DIR" "$BUCKET/bathymetry/" \
        $RCLONE_FLAGS \
        --include "*.pdf" \
        --checksum \
        --dry-run
    fi
    ;;
  --help|-h)
    echo "Usage: $0 [--file <path> | --dry-run | --help]"
    echo "  DEPLOY_ENV=staging|production (default: staging)"
    echo ""
    echo "  (no args)     Upload entire deploy/ directory to R2 + bathymetry PDFs"
    echo "  --file <f>    Upload a single file"
    echo "  --dry-run     Show what would be uploaded (deploy/ + bathymetry PDFs)"
    exit 0
    ;;
  *)
    write_version
    upload_deploy
    upload_bathymetry_pdfs
    ;;
esac

echo ""
ok "Done! Verify at the R2 worker URL."
