#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────
#  deploy-site.sh — Build and deploy the webapp to Cloudflare Workers
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
REPO_ROOT="$(cd "$WEBAPP_DIR/.." && pwd)"

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
}

# ── Deploy ────────────────────────────────────────────────────────────
do_deploy() {
    if [ ! -d "dist" ]; then
        err "dist/ directory not found. Run build first."
        exit 1
    fi

    info "Deploying to Cloudflare Workers (static assets)..."
    cd "$REPO_ROOT"
    npx wrangler deploy
    cd "$WEBAPP_DIR"
    ok "Deployment complete!"
    echo ""
    echo "  Production:  https://canifishthis.ca"
    echo "  Workers URL: https://bc-fishing-regulations.horvath-dawson.workers.dev"
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
