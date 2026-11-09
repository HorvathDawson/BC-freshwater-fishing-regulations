#!/usr/bin/env bash
#
# FAST-FAIL (T483, AGENTS 20a): the cheap checks that stop a doomed long job in a minute or two. Target <= 2 min.
#
#   ./scripts/fast-fail.sh            run every item; exits non-zero on the FIRST failure (its output is shown whole)
#
# Run it before any long job (build, release, APK, emulator, full suite) and while a build waits. `python -m pipeline
# release --staging` and deploy/staging/build-apk.sh run it first; their `--no-fast-fail <reason>` escape prints the
# reason. Output is unfiltered (never through rtk: it has reported false passes, memory rtk-pytest-false-pass).
#
# "CHANGED" = the working tree (uncommitted + untracked) and the commits not yet on origin/redesign/stream-sections,
# i.e. everything since `git merge-base HEAD origin/redesign/stream-sections`. FAST_FAIL_BASE=<rev> names another base.
#
# THE LIST GROWS (AGENTS 20a): a failure that showed up late (after a long job, at review, on the device) and that a
# cheap check could have caught is added here, in the same change as its fix, with a "caught:" line naming it.
# Checks that need a release or an out-dir live where those are known: `pipeline/release.py` preflight (branch pushed,
# T484) and `pipeline.deliver all --out-dir` (scratch release root holds tiles + basemap).
# caught (T512, 2026-11-08): Play builds 20 and 21 shipped the STOCK MapLibre (no MLN_LOCAL_MAVEN), found only from the
# owner's phone trace: `deploy/staging/build-apk.sh --play` checks it before the heavy lock (needs the flags it is given)
# and checks the AAB's libmaplibre.so build id after the build.
# caught (2026-11-09), outside this script, where the context is: the staging site older than the release's min app
# (`deliver.release.site_gate`, run by `pipeline release` preflight and deploy/staging/upload.sh before any upload); the
# live reach baseline lost to a `pipeline build` outside a release (`pipeline build` keeps LATEST's copy before reach
# reruns, `release.keep_baseline`); `--accept-diff` compared by bytes (now by content, `release.diff_content_sha`);
# `session.sh --build` with MLN_LOCAL_MAVEN and no TESTBED_APK_TAG failing after the heavy lock (heavy.sh runs a script's
# `heavy-precheck` before the lock).
set -euo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python
DEV=redesign/stream-sections
t0=$SECONDS
step() { local s=$SECONDS; echo "── $1"; shift; "$@"; echo "   ok ($((SECONDS - s)) s)"; }

if [[ -n ${FAST_FAIL_BASE:-} ]]; then
  BASE=$(git rev-parse --verify "$FAST_FAIL_BASE^{commit}")
else
  git rev-parse -q --verify "origin/$DEV" >/dev/null \
    || { echo "fast-fail: no origin/$DEV to diff against — git fetch origin $DEV, or FAST_FAIL_BASE=<rev>" >&2; exit 1; }
  BASE=$(git merge-base HEAD "origin/$DEV")
fi
# changed / added paths since BASE (working tree included), plus untracked files; deleted paths are left out
CHANGED=$( { git diff --name-only --diff-filter=ACMR "$BASE"; git ls-files --others --exclude-standard; } | sort -u)
echo "fast-fail: base $(git rev-parse --short "$BASE"), $(grep -c . <<<"$CHANGED" || true) changed path(s)"

# caught: merge/cherry-pick resolvers leaving conflict markers in files that then built (memory merge-resolver-markers)
conflict_markers() {
  if git grep -n -I --untracked -E '^(<<<<<<<|>>>>>>>)( |$)'; then
    echo "fast-fail: conflict markers above — resolve them" >&2; return 1
  fi
}
step "no conflict markers" conflict_markers

# caught: ticket rows without files / files without Status lines surfacing only in `pnpm check` after the work (T84, T437)
step "ticket ledger" node app/tools/check-tickets.mjs

# caught 2026-11-07: three test files merged with no smoke.txt id (test_dfo_tidal_water, test_lake_through_river,
# test_tile_smooth), found only by the Tier 1 run after the merges; the guard is T432's test_smoke_coverage (< 1 s)
step "smoke.txt covers every pipeline test file" "$PY" -m pytest -q -p no:cacheprovider pipeline/tests/test_smoke_coverage.py

# caught (2026-11-09): a committed `pytest.skip` failed test_no_silent_skips only in the full suite. A changed pipeline
# test file runs that guard here (~1 s)
silent_skips() {
  grep -qE '^pipeline/(.*/)?(test_[^/]*|conftest)\.py$|^conftest\.py$' <<<"$CHANGED" \
    || { echo "   (no pipeline test file changed)"; return 0; }
  "$PY" -m pytest -q -p no:cacheprovider pipeline/tests/test_no_silent_skips.py
}
step "no silent skips (a pipeline test file changed)" silent_skips

# caught: a syntax error in a changed pipeline module surfacing only when a build stage imported it, an hour in
py_compiles() {
  local files=() f
  while IFS= read -r f; do [[ $f == *.py && -f $f ]] && files+=("$f"); done <<<"$CHANGED"
  (( ${#files[@]} )) || { echo "   (no Python changed)"; return 0; }
  "$PY" - "${files[@]}" <<'EOF'
import sys
bad = 0
for p in sys.argv[1:]:
    try:
        compile(open(p, encoding="utf-8").read(), p, "exec")
    except SyntaxError as e:
        bad += 1
        print(f"fast-fail: {p}:{e.lineno}: {e.msg}", file=sys.stderr)
sys.exit(1 if bad else 0)
EOF
}
step "changed Python compiles" py_compiles

# caught (2026-11-09): T517 bumped `inseason-links/3` in Python and core but the feeds worker still required /2, found
# only on the deployed worker; every Python-written format the app / worker reads must have ONE value on both sides
step "format pairs (Python writer = app / worker reader)" "$PY" scripts/check-format-pairs.py

# caught (2026-11-09): after LATEST moved, `pnpm check` failed in conformance until regs-recut/sync.mjs re-cut the goldens;
# taps.json meta.store must be the store of LATEST's water file, or this names the sync command
step "goldens cut from LATEST" "$PY" scripts/check-goldens-paired.py

# caught 2026-11-07: app/tools/pin-study.mjs held hex colours; the colour gate flagged it only after the build (T481)
step "colours" node app/tools/check-colours.mjs

# caught 2026-11-07: uncommitted Python changed the generated answers, found stale only at the end of a release
step "answers not stale" "$PY" -m pipeline.deliver.answers.model --check

# caught: type errors reaching an APK / web build (2026-11-07: TS6133 unused createHorizontalLinePattern, 1b84c5ae6)
step "app typecheck" pnpm -C app run -s typecheck

# caught: a change breaking the tests of the files it touched, found only by the full suite or `pnpm check` later.
# The changed test files, plus each changed source's own sibling test (X.ts -> X.test.ts/.tsx). Not `vitest related`:
# its import graph pulls most of the suite in for one core file (> 2 min); the full suite stays `pnpm check` (Tier 1).
vitest_changed() {
  local tests=() f b t
  while IFS= read -r f; do
    [[ $f == app/* && $f != */node_modules/* && -f $f && $f =~ \.(ts|tsx|mjs|js)$ ]] || continue
    if [[ $f =~ \.test\.(ts|tsx)$ ]]; then tests+=("${f#app/}"); continue; fi
    b=${f%.*}
    for t in "$b.test.ts" "$b.test.tsx"; do [[ -f $t ]] && tests+=("${t#app/}"); done
  done <<<"$CHANGED"
  (( ${#tests[@]} )) || { echo "   (no changed app file has a test)"; return 0; }
  tests=($(printf '%s\n' "${tests[@]}" | sort -u))
  echo "   vitest: ${#tests[@]} test file(s): ${tests[*]}"
  (cd app && pnpm exec vitest run "${tests[@]}")
}
step "vitest on changed app files" vitest_changed

echo "fast-fail: all passed in $((SECONDS - t0)) s"
