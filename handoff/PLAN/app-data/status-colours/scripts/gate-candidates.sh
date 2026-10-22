#!/usr/bin/env bash
# T47: run the house style check and the existing colour gates on every candidate, then put today's palette back.
#
#   bash handoff/PLAN/app-data/status-colours/scripts/gate-candidates.sh [ids...]     # default: A B C
#
# Per candidate: apply-palette.mjs -> tools/build-style.mjs (house floors) -> tools/check-style.mjs -> the colour gates
# (tools/cvd.test.ts, tools/water-status-cvd.test.ts, tools/check-colours.test.ts, tools/regs-tokens.test.ts,
# packages/map/src). Output: one block per candidate in $OUT (default: stdout). Leaves the theme files as they were.
set -u
ROOT="$(cd "$(dirname "$0")/../../../../.." && pwd)"
S="$ROOT/handoff/PLAN/app-data/status-colours/scripts"
IDS=("$@"); [ ${#IDS[@]} -eq 0 ] && IDS=(A B C)
restore() { node "$S/apply-palette.mjs" --restore >/dev/null; (cd "$ROOT/app" && node tools/build-style.mjs >/dev/null 2>&1); }
trap restore EXIT
for id in "${IDS[@]}"; do
  echo "== $id"
  node "$S/apply-palette.mjs" "$id" >/dev/null || { echo "apply failed"; continue; }
  (cd "$ROOT/app" && node tools/build-style.mjs 2>&1 | grep -v '^✓' | tail -12; node tools/check-style.mjs 2>&1 | tail -2)
  (cd "$ROOT/app" && npx vitest run tools/cvd.test.ts tools/water-status-cvd.test.ts tools/check-colours.test.ts \
     tools/regs-tokens.test.ts packages/map/src 2>&1 | grep -E 'Test Files|Tests |FAIL|AssertionError|expected' | head -30)
  node "$S/apply-palette.mjs" --restore >/dev/null
done
