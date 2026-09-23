#!/usr/bin/env bash
# THE single entry point for the parse pipeline. Every workflow is a subcommand here — the Python
# modules are building blocks it calls (see pipeline/regs/parsing/README.md for the dataflow).
#
# ⛔ HUMAN-ONLY (credits): `parse`, `review`, `repass` dispatch batches to the `claude`
#    CLI and spend the user's credits. Claude/agents must NOT run those — only hand the user the command.
#    `prune` and `status` are local (no credits) and safe for anyone to run.
#
#
#   `all`        parse then review, back to back — the usual full run.
#   `parse`      parse the water-specific tables into the catalogue format (pipeline/docs/18).
#   `parse-dry`  export batches only — NO dispatch, no credits. Read the prompt first.
#   `review`     an agent second pass over what the parse produced (strict checklist).
#   `repass`     re-parse ONLY the review-flagged entries, with the findings as hints.
#
#   bash pipeline/regs/parsing/run_parse.sh <parse|parse-dry|review|repass|prune|status>
#
# Env knobs: REGISTRY, BATCH_SIZE, MODEL (parse), REVIEW_MODEL, ESCALATE_MODEL, CONCURRENCY, CLAUDE_BIN.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
export PYTHONPATH="$PWD"
PY=".venv/bin/python"

REGISTRY="${REGISTRY:-data/generated/atlas/full/registry.json}"
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
  [ -f "$REGISTRY" ] || { echo "  ✗ registry not found: $REGISTRY — build it: $PY -m pipeline.atlas.build --full --out data/generated/atlas/full"; exit 1; }
  echo "  ✓ registry: $REGISTRY"
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





  prune)   # drop stale bare-item_id entries superseded by per-row entries (local; no credits)
    $PY -m pipeline.regs.parsing.prune_superseded "${@:2}"
    ;;

  parse)   # parse the water-specific tables into the catalogue format (pipeline/docs/18)
    echo "== catalogue parse: preflight =="; _need_claude; _need_registry
    echo "  model=$MODEL   format=type+conditions, label generated"
    echo "== export (catalogue prompts) =="
    "${EXPORT[@]}" --skip-existing
    echo "== parse ($MODEL) ==";  "${DISPATCH[@]}" --model "$MODEL"
    echo "== validate + apply (nothing partial is written) =="
    $PY -m pipeline.regs.parsing.ingest_catalogue \
        --batch "$RESP"/../batches/batch_*.json \
        --response "$RESP"/batch_*.json \
        --out data/curated/regulations/entries/catalogue
    ;;

  parse-dry)  # export batches only — NO dispatch, no credits. Read the prompt first.
    echo "== catalogue export only (no credits spent) =="; _need_registry
    "${EXPORT[@]}" --skip-existing
    echo "batches written. Read one before spending anything:"
    echo "  less \"$RESP\"/../batches/batch_000.prompt.txt"
    ;;

  all)  # parse, then review, in one go. The two most-used steps; repass stays separate because
        # it escalates to a pricier model and you want to see the findings before spending that.
    echo "== parse + review =="
    "$0" parse
    "$0" review
    echo
    echo "== both done =="
    "$0" status
    echo
    echo "Re-parse whatever the reviewer flagged (escalates to $ESCALATE_MODEL):"
    echo "  bash pipeline/regs/parsing/run_parse.sh repass"
    ;;

  review)  # an agent second pass over parsed entries, against CATALOGUE_REVIEW_PROMPT.md
    echo "== review: preflight =="; _need_claude
    echo "  reviewer=$REVIEW_MODEL   checklist=prompts/CATALOGUE_REVIEW_PROMPT.md"
    echo "== review ($REVIEW_MODEL) =="
    "${DISPATCH[@]}" --model "$MODEL" --review --review-model "$REVIEW_MODEL"
    echo
    echo "Findings are stamped on the entries. Re-parse the flagged ones with:"
    echo "  bash pipeline/regs/parsing/run_parse.sh repass"
    ;;

  repass)  # re-parse ONLY the review-flagged entries, with the reviewer's findings as hints
    echo "== repass: preflight =="; _need_claude; _need_registry
    echo "  model=$ESCALATE_MODEL (flagged entries only)"
    "${EXPORT[@]}" --only-flagged
    "${DISPATCH[@]}" --model "$ESCALATE_MODEL" --redo-flagged --redo-invalid
    echo "== validate + apply =="
    $PY -m pipeline.regs.parsing.ingest_catalogue \
        --batch "$RESP"/../batches/batch_*.json \
        --response "$RESP"/batch_*.json \
        --out data/curated/regulations/entries/catalogue
    ;;

  status)  # where you left off: entry counts, review state, and WHAT TO RUN NEXT (no credits)
    $PY - <<'PYEOF'
from pipeline.regs.parsing import io
regs = io.region_ids()
by = io.read_entries_dir()
from collections import Counter
verd = Counter((e.get("parse_review") or {}).get("verdict", "") for e in by.values())
flagged = sum(1 for e in by.values()
              if (e.get("parse_review") or {}).get("verdict") == "changes_requested")
print(f"entries: {len(by)} across regions {regs}")
print(f"  parse_review verdicts: {dict(verd)}")
print(f"  flagged (repass candidates): {flagged}")

# WHERE YOU LEFT OFF. A parse stops on a credit limit and the scrollback is gone by the next
# session, so the number that actually matters — how many synopsis rows still have no entry — is
# printed here rather than remembered.
try:
    from pipeline.regs.parsing.rows import load_synopsis_rows
    from pipeline.regs.parsing.batch_exporter import _row_entry_id

    class _M:
        def __init__(self, water):
            self.water = water

    rows = {_row_entry_id(r, _M(r["water"])) for r in load_synopsis_rows()}
    parsed = {k for k in by if not str(k).startswith("z")}
    left = len(rows - parsed)
    print()
    print(f"synopsis rows: {len(rows)}   parsed: {len(rows & parsed)}   REMAINING: {left}")
    if left:
        print(f"  ~{-(-left // 30)} batch(es) at BATCH_SIZE=30.  RESUME (spends credits):")
        print("     bash pipeline/regs/parsing/run_parse.sh all      # parse, then review")
        print("     bash pipeline/regs/parsing/run_parse.sh parse    # parse only, cheaper")
        print("  Nothing already parsed is re-sent: the export skips every row in EntryFiles.")
    else:
        print("  every row has an entry. Next:  run_parse.sh review")
except Exception as exc:  # noqa: BLE001
    print(f"\n(could not compute remaining rows: {exc})")
PYEOF
    ;;

  ""|-h|--help|help|*)
    [ -n "$CMD" ] && echo "  ✗ unknown subcommand: $CMD"
    echo "usage: bash pipeline/regs/parsing/run_parse.sh <all|parse|parse-missing|review|repass|prune|status>"
    echo "  all                       parse, then review — the usual full run"
    echo "  parse-missing             parse ONLY missing rows + ingest, NO review (saves credits)"
    echo "  review [hardest|clean]    (no arg) = every entry with no verdict yet (resumes);"
    echo "                            hardest  = the unreviewed half, worst-first by rule count"
    echo "                                       (HARDEST=0.25 for a quarter, HARDEST=200 for a count);"
    echo "                            clean    = wipe every verdict and review everything again"
    exit 1
    ;;
esac
