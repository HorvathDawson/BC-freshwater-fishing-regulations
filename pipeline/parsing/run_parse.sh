#!/usr/bin/env bash
# One-command tiered parse cascade for the Claude agent parser.
#
# ⛔ HUMAN-ONLY: this script spends credits (it dispatches batches to the `claude` CLI). Claude/agents
#    must NOT run it — only give the command. A human runs it in their own terminal to watch progress.
#
#   bash pipeline/parsing/run_parse.sh
#   REGISTRY=output/v2/full/registry.json bash pipeline/parsing/run_parse.sh
#
# Cost cascade — cheap models do the bulk, the expensive one touches only the hard minority:
#   1. PARSE     every batch on $MODEL (sonnet), single-shot (no in-agent tool loop)
#   2. ESCALATE  re-parse validation failures on $ESCALATE_MODEL (opus)
#   3. REVIEW    an independent reviewer on $REVIEW_MODEL (haiku) flags confident-but-wrong parses
#   4. ESCALATE  re-parse review-flagged (high/med) + any still-invalid on $ESCALATE_MODEL (opus)
#   5. VALIDATE + APPLY  (dry-run ingest, then confirm)
#
# Every stage is resumable: re-run this script and completed batches/reviews are skipped. Uses your
# Claude Pro/Max login (run `claude` once to log in). Env knobs:
#   REGISTRY        path to registry.json      (default: output/v2/full/registry.json)
#   BATCH_SIZE      rows per batch             (default: 30)
#   MODEL           parse model               (default: sonnet)
#   REVIEW_MODEL    reviewer model            (default: haiku — cheap semantic net)
#   ESCALATE_MODEL  re-parse model for failures/flags (default: opus)
#   CONCURRENCY     parallel batch subagents   (default: 3)
#   CLAUDE_BIN      path to the claude CLI     (default: claude)
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
export PYTHONPATH="$PWD"
PY=".venv/bin/python"

REGISTRY="${REGISTRY:-output/v2/full/registry.json}"
BATCH_SIZE="${BATCH_SIZE:-30}"
MODEL="${MODEL:-sonnet}"
REVIEW_MODEL="${REVIEW_MODEL:-haiku}"
ESCALATE_MODEL="${ESCALATE_MODEL:-opus}"
CONCURRENCY="${CONCURRENCY:-3}"
CLAUDE_BIN="${CLAUDE_BIN:-claude}"
DISPATCH=($PY -m pipeline.parsing.dispatch --concurrency "$CONCURRENCY" --claude-bin "$CLAUDE_BIN")

echo "== 1/7  Preflight =="
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
echo "  ✓ registry: $REGISTRY   parse=$MODEL review=$REVIEW_MODEL escalate=$ESCALATE_MODEL"

echo "== 2/7  Export batches (resume: only rows missing from EntryFiles) =="
# --skip-existing makes export ROW-GRANULAR: rows already in pipeline/parsing/entries are dropped, so
# only entries that were deleted (or never parsed) are batched. Deleting a bad entry re-parses just it,
# not its whole 30-row batch. Layout is keyed to current EntryFiles — don't ingest a partial run then
# re-export; finish the run first.
$PY -m pipeline.parsing.batch_exporter --registry "$REGISTRY" --batch-size "$BATCH_SIZE" --skip-existing

echo "== 3/7  Parse ($MODEL, single-shot) =="
"${DISPATCH[@]}" --model "$MODEL"

echo "== 4/7  Escalate validation failures ($ESCALATE_MODEL) =="
"${DISPATCH[@]}" --model "$ESCALATE_MODEL" --redo-invalid

echo "== 5/7  Review ($REVIEW_MODEL) =="
"${DISPATCH[@]}" --model "$MODEL" --review --review-model "$REVIEW_MODEL"

echo "== 6/7  Escalate review-flagged + any still-invalid ($ESCALATE_MODEL) =="
"${DISPATCH[@]}" --model "$ESCALATE_MODEL" --redo-flagged --redo-invalid

RESP="$($PY - <<'PYEOF'
from pipeline.parsing.batch_exporter import default_work_dir
print(default_work_dir() / "responses")
PYEOF
)"

echo "== 7/7  Validate (dry-run ingest) + apply =="
$PY -m pipeline.parsing.ingest "$RESP"/*.json --dry-run
read -r -p "  Write EntryFiles from the above? [y/N] " ans
if [ "${ans:-N}" = "y" ] || [ "${ans:-N}" = "Y" ]; then
  $PY -m pipeline.parsing.ingest "$RESP"/*.json
  echo "  ✓ EntryFiles written to pipeline/parsing/entries/"
else
  echo "  skipped apply. Re-run:  $PY -m pipeline.parsing.ingest $RESP/*.json"
fi
