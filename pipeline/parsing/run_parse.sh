#!/usr/bin/env bash
# One-command setup + run for the Claude agent parser.
#
#   bash pipeline/parsing/run_parse.sh                 # export -> dispatch (Opus) -> review -> ingest
#   REGISTRY=output/v2/full/registry.json bash pipeline/parsing/run_parse.sh
#
# Uses your Claude Pro/Max login (run `claude` once to log in). Env knobs:
#   REGISTRY     path to registry.json         (default: output/v2/full/registry.json)
#   BATCH_SIZE   rows per batch                 (default: 40)
#   MODEL        CLI model alias               (default: opus)
#   CONCURRENCY  parallel batch subagents       (default: 3)
#   CLAUDE_BIN   path to the claude CLI         (default: claude)
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
export PYTHONPATH="$PWD"
PY=".venv/bin/python"

REGISTRY="${REGISTRY:-output/v2/full/registry.json}"
BATCH_SIZE="${BATCH_SIZE:-40}"
MODEL="${MODEL:-opus}"
CONCURRENCY="${CONCURRENCY:-3}"
CLAUDE_BIN="${CLAUDE_BIN:-claude}"

echo "== 1/5  Preflight =="
if ! command -v "$CLAUDE_BIN" >/dev/null 2>&1; then
  echo "  ✗ '$CLAUDE_BIN' not found. Install:  npm install -g @anthropic-ai/claude-code"
  echo "    then log in once with your Pro/Max account:  claude"
  exit 1
fi
"$CLAUDE_BIN" --version >/dev/null 2>&1 && echo "  ✓ claude CLI: $($CLAUDE_BIN --version 2>/dev/null | head -1)"
if [ ! -f "$REGISTRY" ]; then
  echo "  ✗ registry not found: $REGISTRY"
  echo "    build it first:  $PY -m pipeline.build --full --out output/v2/full"
  exit 1
fi
echo "  ✓ registry: $REGISTRY"

echo "== 2/5  Export batches =="
$PY -m pipeline.parsing.batch_exporter --registry "$REGISTRY" --batch-size "$BATCH_SIZE"

echo "== 3/5  Dispatch to Claude ($MODEL, x$CONCURRENCY) + independent review =="
$PY -m pipeline.parsing.dispatch --model "$MODEL" --concurrency "$CONCURRENCY" \
    --review --dangerously-skip-permissions --claude-bin "$CLAUDE_BIN"

RESP="$($PY - <<'PYEOF'
from pipeline.parsing.batch_exporter import default_work_dir
print(default_work_dir() / "responses")
PYEOF
)"

echo "== 4/5  Validate (dry-run ingest) =="
$PY -m pipeline.parsing.ingest "$RESP"/*.json --dry-run

echo "== 5/5  Apply =="
read -r -p "  Write EntryFiles from the above? [y/N] " ans
if [ "${ans:-N}" = "y" ] || [ "${ans:-N}" = "Y" ]; then
  $PY -m pipeline.parsing.ingest "$RESP"/*.json
  echo "  ✓ EntryFiles written to pipeline/parsing/entries/"
else
  echo "  skipped apply. Re-run:  $PY -m pipeline.parsing.ingest $RESP/*.json"
fi
