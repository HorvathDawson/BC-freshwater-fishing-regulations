#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────
#  deploy-all.sh — Full deployment: data → worker → site
#
#  Usage:
#    ./scripts/deploy-all.sh               # Deploy everything
#    ./scripts/deploy-all.sh --skip-data   # Skip R2 data upload
# ────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }

echo "╔══════════════════════════════════════════════════╗"
echo "║   Can I Fish This? — Full Deployment             ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# Step 1: Upload data to R2
if [ "${1:-}" != "--skip-data" ]; then
    info "Step 1/3: Uploading data to R2..."
    bash "$SCRIPT_DIR/deploy-data.sh"
    echo ""
else
    info "Step 1/3: Skipping data upload (--skip-data)"
    echo ""
fi

# Step 2: Deploy R2 worker
info "Step 2/3: Deploying R2 CORS worker..."
bash "$SCRIPT_DIR/deploy-worker.sh"
echo ""

# Step 3: Build + deploy site
info "Step 3/3: Building and deploying site..."
bash "$SCRIPT_DIR/deploy-site.sh"
echo ""

echo "╔══════════════════════════════════════════════════╗"
echo "║   Deployment Complete!                           ║"
echo "║                                                  ║"
echo "║   🌐  https://canifishthis.ca                    ║"
echo "║   🌐  https://www.canifishthis.ca                ║"
echo "╚══════════════════════════════════════════════════╝"
