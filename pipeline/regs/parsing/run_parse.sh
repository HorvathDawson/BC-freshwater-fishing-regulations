#!/usr/bin/env bash
# THE single entry point for the parse pipeline. Every workflow is a subcommand here — the Python
# modules are building blocks it calls (see pipeline/regs/parsing/README.md for the dataflow).
#
# ⛔ HUMAN-ONLY (credits): `all`, `parse`, `review`, `repass` dispatch batches to the `claude`
#    CLI and spend the user's credits. Claude/agents must NOT run those — only hand the user the command.
#    `parse-dry` and `status` are local (no credits) and safe for anyone to run.
#
#   `all`        parse then review, back to back — the usual full run.
#   `parse`      parse the water-specific tables into the catalogue format (pipeline/docs/18).
#   `parse-dry`  export batches only — NO dispatch, no credits. Read the prompt first.
#   `review`     an agent second pass over the last parse's batches (strict checklist). Findings
#                are written to the work dir's reviews/ — never onto the entries.
#   `repass`     re-parse ONLY the review-flagged entries, with the findings as hints.
#   `status`     entry counts, review findings, and what to run next.
#
#   bash pipeline/regs/parsing/run_parse.sh <all|parse|parse-dry|review|repass|status>
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
case "$CMD" in
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
    ;;

  review)  # an agent second pass over parsed entries, against CATALOGUE_REVIEW_PROMPT.md
    echo "== review: preflight =="; _need_claude
    echo "  reviewer=$REVIEW_MODEL   checklist=prompts/CATALOGUE_REVIEW_PROMPT.md"
    echo "== review ($REVIEW_MODEL) =="
    "${DISPATCH[@]}" --model "$MODEL" --review --review-model "$REVIEW_MODEL"
    echo
    "$0" status
    echo
    echo "Findings are in the work dir's reviews/ (not on the entries). Re-parse the flagged ones with:"
    echo "  bash pipeline/regs/parsing/run_parse.sh repass"
    ;;

  repass)  # re-parse ONLY the review-flagged entries, with the reviewer's findings as hints
    # The export reads the findings from reviews/ BEFORE it replaces the batches (and exits
    # non-zero, spending nothing, when there are none). The new batches ARE the flagged set, so
    # `--force` re-parses every one of them. Ingest will not overwrite an entry a curator has
    # edited since it was ingested: it reports it as KEPT (see ingest_catalogue --replace-edited).
    echo "== repass: preflight =="; _need_claude; _need_registry
    echo "  model=$ESCALATE_MODEL (flagged entries only)"
    "${EXPORT[@]}" --flagged
    "${DISPATCH[@]}" --model "$ESCALATE_MODEL" --force
    echo "== validate + apply =="
    $PY -m pipeline.regs.parsing.ingest_catalogue \
        --batch "$RESP"/../batches/batch_*.json \
        --response "$RESP"/batch_*.json \
        --out data/curated/regulations/entries/catalogue
    ;;

  status)  # where you left off: entry counts, review state, and WHAT TO RUN NEXT (no credits)
    $PY - <<'PYEOF'
from collections import Counter
from pipeline.regs.parsing import io
regs = io.region_ids()
by = io.read_entries_dir()
work = io.default_work_dir()
print(f"entries: {len(by)} across regions {regs}")

# REVIEWS LIVE IN THE WORK DIR, per batch; they are never written onto the entries.
reviews = io.read_reviews(work)
states = Counter(r["state"] for r in reviews.values())
print(f"reviews ({work / 'reviews'}): {len(reviews)} batch(es) {dict(states)}")
if states.get("failed"):
    print("  ⚠ failed = the reviewer's reply was unreadable; re-review those batches "
          "(dispatch --review --rereview --only <ids>)")
if states.get("stale"):
    print("  ⚠ stale = the batch was re-parsed after its review; the finding is about another parse")
try:
    flagged = io.read_review_findings(work)
    print(f"  flagged entries (high/medium — repass candidates): {len(flagged)}")
    if flagged:
        print("     bash pipeline/regs/parsing/run_parse.sh repass    # spends credits")
except ValueError as exc:
    print(f"  ✗ cannot join the reviews to entries: {exc}")

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
        print("  Nothing already parsed is re-sent: the export skips every row already in the catalogue.")
    else:
        print("  every row has an entry. Next:  run_parse.sh review")
except Exception as exc:  # noqa: BLE001
    print(f"\n(could not compute remaining rows: {exc})")
PYEOF
    ;;

  ""|-h|--help|help|*)
    case "$CMD" in ""|-h|--help|help) ;; *) echo "  ✗ unknown subcommand: $CMD" ;; esac
    echo "usage: bash pipeline/regs/parsing/run_parse.sh <all|parse|parse-dry|review|repass|status>"
    echo "  all        parse, then review — the usual full run                      (credits)"
    echo "  parse      parse the rows not yet in the catalogue, then ingest          (credits)"
    echo "  parse-dry  export the batches only; read a prompt before spending anything"
    echo "  review     review the last parse's batches; findings go to reviews/     (credits)"
    echo "  repass     re-parse the review-flagged entries on \$ESCALATE_MODEL       (credits)"
    echo "  status     entries, review findings, rows remaining, and what to run next"
    exit 1
    ;;
esac
