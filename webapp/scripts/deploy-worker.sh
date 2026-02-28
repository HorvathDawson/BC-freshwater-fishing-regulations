#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────
#  deploy-worker.sh — Deploy the R2 CORS worker
#
#  Usage:
#    ./scripts/deploy-worker.sh
#
#  Deploys the Cloudflare Worker that serves R2 files with
#  proper CORS headers and Range request support (for PMTiles).
# ────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKER_DIR="$(cd "$SCRIPT_DIR/../r2-worker" && pwd)"

# ── Colours ───────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'

info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

cd "$WORKER_DIR"

info "Deploying R2 worker (bc-fishing-r2)..."
npx wrangler deploy
ok "R2 worker deployed!"
echo ""
echo "  Worker URL: https://bc-fishing-r2.horvath-dawson.workers.dev"
