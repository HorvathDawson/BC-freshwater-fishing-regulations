#!/usr/bin/env bash
#
# TIER 2, the FULL set (P00, 2026-10-20): run once per phase or before a merge, never per edit or per ticket.
# It is HEAVY: run it through handoff/tools/heavy.sh (min free 40 %), one heavy job at a time, with the owner's go-ahead.
#
#   heavy.sh test-full 40 -- ./scripts/test-full.sh
#
#   1. pnpm -C app check                                    every app gate + vitest (≈ 17 s)
#   2. pytest (the fast suite, `-m "not slow"`)             ≈ 8.5 min serial (2026-10-20: 3572 passed)
#   3. pytest -m slow                                       264 tests: determinism and full-build guards (AGENTS 18)
#   4. check.mjs, every round                               the whole prototype suite
#   5. card gate on the shipped answers                     problems: 0 (rule 9)
#   6. the generated-file --checks                          answers model, calendar, gauge policy
#
# Not run here, because each has its own procedure: the golden re-sync (port/sync.mjs, when the prototype changes),
# verify_map / pinreplay / quotes (after a sync), the perf gates, and the comprehension-reader gate (manual,
# owner-launched).
# Every step runs unfiltered (no rtk). If a step fails, fix it, rerun only the failing files, then run Tier 2 once more.
set -uo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python
t0=$SECONDS; failed=()
step() { local name=$1 s=$SECONDS; shift; echo "── $name"; if "$@"; then echo "   ok ($((SECONDS - s)) s)"; else
  echo "   FAILED ($((SECONDS - s)) s)"; failed+=("$name"); fi; }

step "app check" pnpm -C app check
step "pytest fast" "$PY" -m pytest -q
step "pytest slow" "$PY" -m pytest -q -m slow
step "check.mjs" node app/design/regs-v11/tools/check.mjs
step "card gate" "$PY" -m pipeline.deliver.answers.card_check
# the release (P08): parity of every single-file read against the bundle, the query plans and the perf budgets
REL=$(/bin/ls -dt data/release/r-* 2>/dev/null | head -1)
if [ -n "$REL" ]; then
  step "release parity + perf" env RELEASE_DIR="$REL" BUNDLE=data/generated/bundle/bundle.sqlite \
       pnpm -C app exec vitest run release-perf
  step "release: every water placeable" env RELEASE_DIR="$REL" TIER2=1 pnpm -C app exec vitest run release-placeable
else
  echo "── release parity + perf: no data/release/r-* yet (run python -m pipeline.deliver release)"; failed+=("release (none built)")
fi
step "answers model --check" "$PY" -m pipeline.deliver.answers.model --check
step "calendar --check" "$PY" -m pipeline.tools.emit_calendar --check
step "gauge policy --check" "$PY" -m pipeline.tools.emit_gauge_policy --check
step "pack --check" "$PY" -m pipeline.tools.emit_pack --check

echo "full: $((SECONDS - t0)) s"
if ((${#failed[@]})); then echo "FAILED: ${failed[*]}"; exit 1; fi
echo "full: all passed"
