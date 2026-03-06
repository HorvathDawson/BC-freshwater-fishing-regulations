#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────
#  update-basemap.sh — Download a fresh Protomaps basemap extract for BC
#
#  Extracts tiles from the latest Protomaps daily planet build, clipped
#  to the same bounding box used by the frontend (Map.tsx BC_BOUNDS).
#
#  Usage:
#    ./scripts/update-basemap.sh                # Download latest daily build
#    ./scripts/update-basemap.sh <YYYYMMDD>     # Download a specific date's build
#    ./scripts/update-basemap.sh --upload        # Download + upload to R2
#    ./scripts/update-basemap.sh <YYYYMMDD> --upload
#
#  Prerequisites:
#    - pmtiles CLI (https://github.com/protomaps/go-pmtiles)
#      Install: go install github.com/protomaps/go-pmtiles/cmd/pmtiles@latest
#      Or download a release binary from GitHub and put it on PATH.
# ────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBAPP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT="$WEBAPP_DIR/public/data/bc.pmtiles"

# ── Bounding box — must match BC_BOUNDS in Map.tsx ────────────────────
# SW: -148.0, 45.0  |  NE: -108.0, 63.5
BBOX="-148.0,45.0,-108.0,63.5"
MAXZOOM=15

# ── Colours ───────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ── Pre-flight ────────────────────────────────────────────────────────
PMTILES=""
if command -v pmtiles &>/dev/null; then
    PMTILES="pmtiles"
elif [ -x "/mnt/c/Users/DawsonHorvath/Documents/Workspace/pmtiles/pmtiles.exe" ]; then
    PMTILES="/mnt/c/Users/DawsonHorvath/Documents/Workspace/pmtiles/pmtiles.exe"
fi

if [ -z "$PMTILES" ]; then
    err "pmtiles CLI not found."
    echo ""
    echo "  Install options:"
    echo "    go install github.com/protomaps/go-pmtiles/cmd/pmtiles@latest"
    echo "    Or download from: https://github.com/protomaps/go-pmtiles/releases"
    echo ""
    exit 1
fi

# ── Parse args ────────────────────────────────────────────────────────
UPLOAD=false
BUILD_DATE=""

for arg in "$@"; do
    case "$arg" in
        --upload) UPLOAD=true ;;
        *)        BUILD_DATE="$arg" ;;
    esac
done

# Default to today's date if not specified
if [ -z "$BUILD_DATE" ]; then
    BUILD_DATE="$(date +%Y%m%d)"
fi

SOURCE_URL="https://build.protomaps.com/${BUILD_DATE}.pmtiles"

# ── Extract ───────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║   Protomaps Basemap Extract — British Columbia   ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
info "Source:   $SOURCE_URL"
info "pmtiles:  $PMTILES"
info "Bbox:     $BBOX"
info "Maxzoom:  $MAXZOOM"
info "Output:   $OUTPUT"
echo ""

# Back up existing file
if [ -f "$OUTPUT" ]; then
    BACKUP="${OUTPUT}.bak"
    info "Backing up existing bc.pmtiles → bc.pmtiles.bak"
    mv "$OUTPUT" "$BACKUP"
fi

# If using a Windows .exe under WSL, convert the output path for Windows
PMTILES_OUTPUT="$OUTPUT"
if [[ "$PMTILES" == *.exe ]]; then
    PMTILES_OUTPUT="$(wslpath -w "$OUTPUT")"
fi

info "Extracting tiles (this may take several minutes)..."
if ! "$PMTILES" extract "$SOURCE_URL" "$PMTILES_OUTPUT" --bbox="$BBOX" --maxzoom="$MAXZOOM"; then
    err "Extraction failed."
    # Restore backup if it exists
    if [ -f "${OUTPUT}.bak" ]; then
        warn "Restoring previous bc.pmtiles from backup."
        mv "${OUTPUT}.bak" "$OUTPUT"
    fi
    exit 1
fi

# Remove backup on success
rm -f "${OUTPUT}.bak"

SIZE=$(du -h "$OUTPUT" | cut -f1)
ok "Extraction complete: $OUTPUT ($SIZE)"

# ── Optional upload ───────────────────────────────────────────────────
if [ "$UPLOAD" = true ]; then
    echo ""
    info "Uploading to R2..."
    bash "$SCRIPT_DIR/deploy-data.sh" --file "$OUTPUT"
fi

echo ""
ok "Done! Basemap updated to $BUILD_DATE build."
