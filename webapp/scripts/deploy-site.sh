#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────
#  deploy-site.sh — Build and deploy the webapp to Cloudflare Pages
#
#  Usage:
#    ./scripts/deploy-site.sh              # Build + deploy (production)
#    ./scripts/deploy-site.sh --build-only # Build only, no deploy
#    ./scripts/deploy-site.sh --deploy-only# Deploy existing dist/ (skip build)
#
#  Prerequisites:
#    - Node.js + npm installed
#    - Authenticated wrangler (npx wrangler login)
# ────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBAPP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_NAME="bc-fishing-regulations"

# ── Colours ───────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

cd "$WEBAPP_DIR"

# ── Build ─────────────────────────────────────────────────────────────
do_build() {
    info "Installing dependencies..."
    npm install --silent

    info "Building webapp (production)..."
    npm run build
    ok "Build complete → dist/"

    # Verify no oversized files
    local max_size=$((25 * 1024 * 1024))  # 25 MiB Pages limit
    while IFS= read -r -d '' file; do
        local size
        size=$(stat -c%s "$file" 2>/dev/null || stat -f%z "$file" 2>/dev/null)
        if [ "$size" -gt "$max_size" ]; then
            err "File too large for Pages (>25 MiB): $file ($(du -h "$file" | cut -f1))"
            err "Upload it to R2 instead: ./scripts/deploy-data.sh --file $file"
            exit 1
        fi
    done < <(find dist/ -type f -print0)
    ok "All files under 25 MiB Pages limit."
}

# ── Deploy ────────────────────────────────────────────────────────────
do_deploy() {
    if [ ! -d "dist" ]; then
        err "dist/ directory not found. Run build first."
        exit 1
    fi

    info "Deploying to Cloudflare Pages ($PROJECT_NAME)..."
    npx wrangler pages deploy dist \
        --project-name="$PROJECT_NAME" \
        --commit-dirty=true
    ok "Deployment complete!"
    echo ""
    echo "  Production:  https://canifishthis.ca"
    echo "  Pages URL:   https://$PROJECT_NAME.pages.dev"
}

# ── Main ──────────────────────────────────────────────────────────────
case "${1:-}" in
    --build-only)
        do_build
        ;;
    --deploy-only)
        do_deploy
        ;;
    --help|-h)
        echo "Usage: $0 [--build-only | --deploy-only | --help]"
        echo ""
        echo "  (no args)       Build and deploy"
        echo "  --build-only    Build only (no deploy)"
        echo "  --deploy-only   Deploy existing dist/ (skip build)"
        echo "  --help          Show this help"
        exit 0
        ;;
    *)
        do_build
        do_deploy
        ;;
esac
