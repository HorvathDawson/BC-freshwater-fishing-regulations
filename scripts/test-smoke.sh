#!/usr/bin/env bash
#
# TIER 1, the SMOKE set (P00, 2026-10-20): run once per ticket commit. Target ≤ 2 min wall clock on the laptop.
#
#   ./scripts/test-smoke.sh
#
#   1. pnpm -C app check            every app gate + the whole vitest suite (≈ 17 s; vitest alone ≈ 5 s)
#   2. pytest -m smoke              one fast no-data test per pipeline test file (pipeline/tests/smoke.txt)
#   3. check.mjs ONLY=smoke         one page check per prototype round (needs Chrome; CHROME=… to point at it)
#
# Tier 0 (while iterating) is the one test file / vitest filter / ONLY=<round> you touched. Tier 2 is
# scripts/test-full.sh, run once per phase or before a merge. Every step runs unfiltered (no rtk), so the counts
# printed here are the real ones (memory rtk-pytest-false-pass). Exits non-zero on the first failing step.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python
t0=$SECONDS
step() { local s=$SECONDS; echo "── $1"; shift; "$@"; echo "   ($((SECONDS - s)) s)"; }

step "app check" pnpm -C app check
step "pytest smoke" "$PY" -m pytest -q -m smoke
step "check.mjs smoke" env ONLY=smoke node app/design/regs-v11/tools/check.mjs

echo "smoke: all passed in $((SECONDS - t0)) s"
