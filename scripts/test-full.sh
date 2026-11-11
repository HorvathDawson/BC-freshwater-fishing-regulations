#!/usr/bin/env bash
#
# TIER 2, the FULL set (P00, 2026-10-20): run once per phase or before a merge, never per edit or per ticket.
# It is HEAVY: run it through scripts/heavy.sh (min free 40 %), one heavy job at a time, with the owner's go-ahead.
#
#   scripts/heavy.sh test-full 40 -- ./scripts/test-full.sh
#
#   1. REGS_STORE_REQUIRED=1 pnpm -C app check              every app gate + vitest, the parity strict (≈ 17 s)
#   2. pytest (the fast suite, `-m "not slow"`)             in chunks, one process each (scripts/pytest-chunked.sh, T638:
#                                                           one process grows to ~111 GB and the kernel kills it)
#   3. pytest -m slow                                       the same, chunked: determinism and full-build guards (AGENTS 18)
#   4. card gate on the shipped answers                     problems: 0 (rule 9)
#   5. the generated-file --checks                          python -m pipeline.tools.emit --check (all four)
#
# Not run here, because each has its own procedure: the golden re-sync (app/tools/regs-recut/sync.mjs, after a release),
# verify_map (run by the re-cut), the perf gates, and the comprehension-reader gate (manual,
# owner-launched).
# Every step runs unfiltered (no rtk). If a step fails, fix it, rerun only the failing files, then run Tier 2 once more.
set -uo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python
# T638 item 3: gating runs never trust old bytecode (matcher.py edited twice in one second at the same size kept its
# old .pyc): one fresh PYTHONPYCACHEPREFIX per run, shared by its processes, removed at exit
PYTHONPYCACHEPREFIX=$(mktemp -d "${TMPDIR:-/tmp}/pycache.XXXXXX"); export PYTHONPYCACHEPREFIX
trap 'rm -rf "$PYTHONPYCACHEPREFIX"' EXIT
t0=$SECONDS; failed=()
step() { local name=$1 s=$SECONDS; shift; echo "── $name"; if "$@"; then echo "   ok ($((SECONDS - s)) s)"; else
  echo "   FAILED ($((SECONDS - s)) s)"; failed+=("$name"); fi; }

# (T432) STRICT: REGS_STORE_REQUIRED=1 turns every release-backed parity skip into a failure. Without it the golden parity
# passes green on zero cases whenever the release on disk is not the one taps.json was cut from (after every release,
# until sync.mjs re-cuts the goldens); the failure names `node app/tools/regs-recut/sync.mjs`.
step "app check (strict parity)" env REGS_STORE_REQUIRED=1 pnpm -C app check
step "pytest fast (chunked)" bash scripts/pytest-chunked.sh
step "pytest slow (chunked)" bash scripts/pytest-chunked.sh -- -m slow
step "card gate" "$PY" -m pipeline.deliver.answers.card_check
# the release (P08): parity of every single-file read against the bundle, the query plans and the perf budgets
# THE release on disk: `release.manifest.latest`, the one answer every reader asks (T133)
REL=$("$PY" -c 'from pipeline.deliver.release.manifest import latest; print(latest("data/release") or "")')
if [ -n "$REL" ]; then
  step "release parity + perf" env RELEASE_DIR="$REL" BUNDLE=data/generated/bundle/bundle.sqlite \
       pnpm -C app exec vitest run release-perf
  step "release: every water placeable" env RELEASE_DIR="$REL" TIER2=1 pnpm -C app exec vitest run release-placeable
else
  echo "── release parity + perf: no data/release/r-* yet (run python -m pipeline.deliver release)"; failed+=("release (none built)")
fi
step "generated files --check" "$PY" -m pipeline.tools.emit --check

echo "full: $((SECONDS - t0)) s"
if ((${#failed[@]})); then echo "FAILED: ${failed[*]}"; exit 1; fi
echo "full: all passed"
