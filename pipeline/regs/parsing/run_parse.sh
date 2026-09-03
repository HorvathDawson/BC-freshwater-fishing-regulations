#!/usr/bin/env bash
# THE single entry point for the parse pipeline. Every workflow is a subcommand here — the Python
# modules are building blocks it calls (see pipeline/regs/parsing/README.md for the dataflow).
#
# ⛔ HUMAN-ONLY (credits): `parse`, `parse-missing`, `review`, `repass` dispatch batches to the `claude`
#    CLI and spend the user's credits. Claude/agents must NOT run those — only hand the user the command.
#    `prune` and `status` are local (no credits) and safe for anyone to run.
#
#   `parse`          full cascade: parse missing + escalate + review + review-escalate + ingest.
#   `parse-missing`  parse ONLY missing rows + escalate invalid + ingest. NO review (saves credits).
#
#   bash pipeline/regs/parsing/run_parse.sh <parse|parse-missing|review|repass|prune|status> [rereview]
#
# Env knobs: REGISTRY, BATCH_SIZE, MODEL (parse), REVIEW_MODEL, ESCALATE_MODEL, CONCURRENCY, CLAUDE_BIN.
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
EXPORT=($PY -m pipeline.regs.parsing.batch_exporter --registry "$REGISTRY" --batch-size "$BATCH_SIZE")
DISPATCH=($PY -m pipeline.regs.parsing.dispatch --concurrency "$CONCURRENCY" --claude-bin "$CLAUDE_BIN")
RESP="$($PY -c 'from pipeline.regs.parsing.io import default_work_dir; print(default_work_dir()/"responses")')"

# NO DEFAULT. `parse` spends credits and its first act is an export that DELETES the work dir's
# responses, so a bare `run_parse.sh` — typed to see the usage text — silently destroys the raw
# output of the previous run. It did exactly that once. An argument is required; bare prints usage.
CMD="${1:-}"

_need_claude() {
  command -v "$CLAUDE_BIN" >/dev/null 2>&1 || {
    echo "  ✗ '$CLAUDE_BIN' not found. Install: npm install -g @anthropic-ai/claude-code; then: claude"; exit 1; }
  "$CLAUDE_BIN" --version >/dev/null 2>&1 && echo "  ✓ claude CLI: $($CLAUDE_BIN --version 2>/dev/null | head -1)"
}
_need_registry() {
  [ -f "$REGISTRY" ] || { echo "  ✗ registry not found: $REGISTRY — build it: $PY -m pipeline.atlas.build --full --out output/v2/full"; exit 1; }
  echo "  ✓ registry: $REGISTRY"
}
_apply_ingest() {   # dry-run ingest, then confirm-apply. $@ = extra ingest flags
  $PY -m pipeline.regs.parsing.ingest "$RESP"/*.json --dry-run "$@"
  read -r -p "  Write EntryFiles from the above? [y/N] " ans
  if [ "${ans:-N}" = "y" ] || [ "${ans:-N}" = "Y" ]; then
    $PY -m pipeline.regs.parsing.ingest "$RESP"/*.json "$@" && echo "  ✓ EntryFiles updated"
  else
    echo "  skipped apply. Re-run: $PY -m pipeline.regs.parsing.ingest $RESP/*.json $*"
  fi
}

# REPAIR TOOLS — deliberately NOT part of the cascade. Each rewrites already-ingested rules, so
# running one automatically after every parse would quietly paper over a bad parse instead of
# surfacing it: the parse would look fine and the prompt would never get fixed. The parser is
# supposed to emit correct, split, standard-form rules (prompts/PARSE_PROMPT.md +
# prompts/RULE_STANDARDS.md) and the reviewer is supposed to catch it when it does not. Reach for
# these only to repair an existing corpus, and read the --dry-run first:
#
#   $PY -m pipeline.regs.parsing.backfill_rule_subjects --dry-run   # `details` that lost its subject
#   $PY -m pipeline.regs.parsing.split_bundled_gear     --dry-run   # one rule carrying several restrictions
#   $PY -m pipeline.regs.parsing.normalize_details      --dry-run   # off-standard wording / restriction_type

case "$CMD" in

  parse)   # export un-parsed rows, parse (tiered cascade), review-escalate, ingest
    echo "== parse: preflight =="; _need_claude; _need_registry
    echo "  parse=$MODEL review=$REVIEW_MODEL escalate=$ESCALATE_MODEL"
    echo "== export (resume: only rows missing from EntryFiles) =="; "${EXPORT[@]}" --skip-existing
    echo "== parse ($MODEL, single-shot) ==";                "${DISPATCH[@]}" --model "$MODEL"
    echo "== escalate validation failures ($ESCALATE_MODEL) =="; "${DISPATCH[@]}" --model "$ESCALATE_MODEL" --redo-invalid
    echo "== review ($REVIEW_MODEL) ==";                     "${DISPATCH[@]}" --model "$MODEL" --review --review-model "$REVIEW_MODEL"
    echo "== escalate review-flagged + still-invalid ($ESCALATE_MODEL) =="; "${DISPATCH[@]}" --model "$ESCALATE_MODEL" --redo-flagged --redo-invalid
    echo "== validate + apply =="; _apply_ingest
    ;;

  parse-missing)   # parse ONLY rows missing from EntryFiles, then ingest. NO review (saves credits).
    echo "== parse-missing: preflight =="; _need_claude; _need_registry
    echo "  parse=$MODEL escalate=$ESCALATE_MODEL (review SKIPPED — run 'review' later)"
    echo "== export (resume: only rows missing from EntryFiles) =="; "${EXPORT[@]}" --skip-existing
    echo "== parse ($MODEL, single-shot) ==";                "${DISPATCH[@]}" --model "$MODEL"
    echo "== escalate validation failures ($ESCALATE_MODEL) =="; "${DISPATCH[@]}" --model "$ESCALATE_MODEL" --redo-invalid
    echo "== validate + apply =="; _apply_ingest
    echo "  ✓ parse-missing done (no review run). Review later: bash pipeline/regs/parsing/run_parse.sh review"
    ;;

  review)  # review current entries in place (locked + not); stamp parse_review. No re-parse.
    #   review            every entry WITHOUT a verdict yet (resumes a partial run)
    #   review hardest    the unreviewed half, worst-first by rule count (HARDEST=0.5 to change)
    #   review clean      wipe every verdict and review everything again
    #
    # Resume is keyed on the ENTRY (`parse_review.verdict`), never on the batch file. Reviewing a
    # SUBSET renumbers the batches, so a stale `reviews/batch_007.review.json` from a previous run
    # would be read as this run's batch 7 — the same desync that cost a parse once. The review files
    # are transient (the verdict is stamped onto the entry at the end), so they are cleared every
    # run and the entry is the only thing that remembers.
    echo "== review: preflight =="; _need_claude; _need_registry
    SELECT=(--unreviewed)
    if [ "${2:-}" = "clean" ]; then
      echo "== clean: wiping ALL prior verdicts (entry parse_review) =="
      $PY -m pipeline.regs.parsing.ingest --clear-reviews
      SELECT=()
    elif [ "${2:-}" = "hardest" ]; then
      SELECT=(--unreviewed --hardest "${HARDEST:-0.5}")
      echo "  (hardest: the unreviewed ${HARDEST:-0.5} by rule count — run 'review' again for the rest)"
    fi
    rm -f output/parse/reviews/*.review.json 2>/dev/null || true   # transient; verdicts live on entries
    echo "== export (selected entries -> batches) =="; "${EXPORT[@]}" "${SELECT[@]+"${SELECT[@]}"}"
    echo "== synth responses from current entries =="; $PY -m pipeline.regs.parsing.synth_responses
    echo "== review ($REVIEW_MODEL) =="; "${DISPATCH[@]}" --review --review-model "$REVIEW_MODEL"
    echo "== stamp parse_review onto entries (content-safe) =="; $PY -m pipeline.regs.parsing.ingest --apply-reviews
    echo "  ✓ review done. Remaining unreviewed: run 'run_parse.sh review' again."
    echo "    Flagged entries: run 'run_parse.sh repass' to re-parse them."
    ;;

  repass)  # re-parse ONLY the review-flagged entries (MODEL selectable), with reviewer hints
    echo "== repass: preflight =="; _need_claude; _need_registry
    echo "  re-parsing review-flagged entries on: $MODEL"
    rm -rf output/parse                                    # fresh work dir (progress lives in EntryFiles)
    echo "== export flagged =="; "${EXPORT[@]}" --flagged
    echo "== parse ($MODEL, force) ==";                     "${DISPATCH[@]}" --model "$MODEL" --force
    echo "== escalate validation failures ($ESCALATE_MODEL) =="; "${DISPATCH[@]}" --model "$ESCALATE_MODEL" --redo-invalid
    echo "== validate + apply =="; _apply_ingest
    ;;

  prune)   # drop stale bare-item_id entries superseded by per-row entries (local; no credits)
    $PY -m pipeline.regs.parsing.prune_superseded "${@:2}"
    ;;

  status)  # local snapshot: entry counts + review/flag state (no credits)
    $PY - <<'PYEOF'
from pipeline.regs.parsing import io
regs = io.region_ids()
by = io.read_entries_dir()
from collections import Counter
verd = Counter((e.get("parse_review") or {}).get("verdict", "") for e in by.values())
locked = sum(1 for e in by.values() if e.get("locked"))
flagged = sum(1 for e in by.values()
              if (e.get("parse_review") or {}).get("verdict") == "changes_requested" and not e.get("locked"))
print(f"entries: {len(by)} across regions {regs}")
print(f"  locked (curated): {locked}")
print(f"  parse_review verdicts: {dict(verd)}")
print(f"  flagged & unlocked (repass candidates): {flagged}")
PYEOF
    ;;

  ""|-h|--help|help|*)
    [ -n "$CMD" ] && echo "  ✗ unknown subcommand: $CMD"
    echo "usage: bash pipeline/regs/parsing/run_parse.sh <parse|parse-missing|review|repass|prune|status>"
    echo "  parse-missing             parse ONLY missing rows + ingest, NO review (saves credits)"
    echo "  review [hardest|clean]    (no arg) = every entry with no verdict yet (resumes);"
    echo "                            hardest  = the unreviewed half, worst-first by rule count"
    echo "                                       (HARDEST=0.25 for a quarter, HARDEST=200 for a count);"
    echo "                            clean    = wipe every verdict and review everything again"
    exit 1
    ;;
esac
