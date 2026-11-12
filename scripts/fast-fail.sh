#!/usr/bin/env bash
#
# FAST-FAIL (T483, AGENTS 20a): the cheap checks that stop a doomed long job in a minute or two. Target <= 2 min.
#
#   ./scripts/fast-fail.sh            run every item; exits non-zero on the FIRST failure (its output is shown whole)
#   FAST_FAIL_ONLY=<regex> …          only the steps whose name matches (rerun one failed step); says PARTIAL, not a pass
#
# Run it before any long job (build, release, APK, emulator, full suite) and while a build waits. (T535) scripts/heavy.sh
# runs it before every heavy job, during the lock wait (FAST_FAIL_DONE=<HEAD sha> when the caller already ran it;
# HEAVY_NO_FAST_FAIL=<reason> skips it, printed). `python -m pipeline release --staging` and deploy/staging/build-apk.sh
# run it themselves; their `--no-fast-fail <reason>` escape prints the reason. Output is unfiltered (never through rtk: it has reported false passes, memory rtk-pytest-false-pass).
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
# T638 item 3: gating runs never trust old bytecode (matcher.py edited twice in one second at the same size kept its
# old .pyc): one fresh PYTHONPYCACHEPREFIX per run, shared by its processes, removed at exit
PYTHONPYCACHEPREFIX=$(mktemp -d "${TMPDIR:-/tmp}/pycache.XXXXXX"); export PYTHONPYCACHEPREFIX
trap 'rm -rf "$PYTHONPYCACHEPREFIX"' EXIT
t0=$SECONDS
ran=0; not_run=0
step() {
  if [[ -n ${FAST_FAIL_ONLY:-} && ! $1 =~ $FAST_FAIL_ONLY ]]; then not_run=$((not_run + 1)); return 0; fi
  local s=$SECONDS; ran=$((ran + 1)); echo "── $1"; shift; "$@"; echo "   ok ($((SECONDS - s)) s)"
}

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
# (T533) say what "changed" is, so a run against a release / APK base shows what it checks
if [[ -n $CHANGED ]]; then head -12 <<<"$CHANGED" | sed 's/^/   changed: /'; (( $(grep -c . <<<"$CHANGED") > 12 )) && echo "   changed: …"; fi

# caught: merge/cherry-pick resolvers leaving conflict markers in files that then built (memory merge-resolver-markers)
conflict_markers() {
  if git grep -n -I --untracked -E '^(<<<<<<<|>>>>>>>)( |$)'; then
    echo "fast-fail: conflict markers above — resolve them" >&2; return 1
  fi
}
step "no conflict markers" conflict_markers

# caught (T536, 2026-11-10): heavy.sh freed its lock while a stopped wrapper's job ran on, and read an empty memory
# reading as 0 % free (killing a healthy job). Its shell test (fake jobs, stubbed memory_pressure, scratch lanes, ~10 s)
# runs when heavy.sh or the test changes
heavy_test() {
  grep -qE '^scripts/(heavy\.sh|tests/heavy\.test\.sh)$' <<<"$CHANGED" || { echo "   (heavy.sh unchanged)"; return 0; }
  bash scripts/tests/heavy.test.sh
}
step "heavy.sh shell test (heavy.sh changed)" heavy_test

# caught (T538, 2026-11-10): build-apk.sh only warned on an app version with no changelog entry and published empty
# notes ("No notes were published"); 0.1 s (also in pnpm check:fast)
step "app version has its changelog entry (newest)" node scripts/check-changelog.mjs

# (T535) CI's cheap steps, each read from .github/workflows/pipeline-ci.yml by its name and run as written there, so
# the list has one home (< 1 s each, measured 2026-11-10). caught: these failed only on CI after the push.
ci_step() {   # ci_step <step name>: run that pipeline-ci.yml step's `run` in a per-run temp file (`python` = the venv's)
  local f rc
  f=$(mktemp "${TMPDIR:-/tmp}/fast-fail-ci.XXXXXX")
  "$PY" -c 'import sys, yaml
steps = [s for j in yaml.safe_load(open(".github/workflows/pipeline-ci.yml"))["jobs"].values() for s in j["steps"]
         if s.get("name") == sys.argv[1]]
assert len(steps) == 1, f"pipeline-ci.yml: step {sys.argv[1]!r} found {len(steps)} times"
sys.stdout.write(steps[0]["run"])' "$1" > "$f"
  rc=$?
  if (( rc == 0 )); then PATH="$PWD/.venv/bin:$PATH" bash -e "$f"; rc=$?; fi
  rm -f "$f"; return $rc
}
step "CI: every curated path resolves" ci_step "Every curated path resolves"
step "CI: committed match artifacts in step with their rosters" ci_step "Committed match artifacts are in step with their rosters"
step "CI: hand-edited waters files are sound" ci_step "Hand-edited waters files are sound"
step "CI: generated files are current" ci_step "Generated files are current (gauge policy, calendar, release contract, answers model)"

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

# caught (T667, 2026-11-11): 158 test files read the live tree past the helper or typed one build's section number,
# part number or digest, so every rebuild and release meant hand edits to tests and 13 were red from build to promote.
# A test file off its anchor fails here with what to write instead (< 2 s, every test file, no baseline; the app half
# is `pnpm anchors` in check:fast below)
step "test anchors (pipeline: no test file off its anchor)" "$PY" -m pipeline.tests.anchors

# caught (2026-11-09, CI run 42723032360): `REPO_ROOT / "data" / "release"` (T517 name_check) failed CI's "No hardcoded
# curated or source paths" gate after the push; that gate is a grep (< 1 s), run here from the workflow itself
# (T535: through ci_step, a per-run mktemp file; the one fixed temp path let two lanes run each other's half-written copy)
step "no hardcoded curated or source paths (CI's gate)" ci_step "No hardcoded curated or source paths"

# caught: a syntax error in a changed pipeline module surfacing only when a build stage imported it, an hour in
py_compiles() {
  local files=() f
  while IFS= read -r f; do [[ $f == *.py && -f $f ]] && files+=("$f"); done <<<"$CHANGED"
  (( ${#files[@]} )) || { echo "   (no Python changed)"; return 0; }
  echo "   compiling ${#files[@]} changed Python file(s)"
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

# caught (2026-11-09, T545): two of the owner's DFO marks (Chilliwack/Vedder, saved in one group) never reached
# dfo_salmon/places/region-2.json and read "saved"; no build or release looked. Read-only, ~2 s: every mark in force is
# in its curated file, or is on the dated allow-list (`pipeline/regs/review/apply.py` NOT_LANDED_OK)
step "review marks have landed in their curated files" "$PY" -m pipeline.regs.review.apply --check --waiting-ok

# caught (2026-11-10, T546/T632): OKANAGAN RIVER OXBOWS pinned 26 wsc ids no registry item carries; the row bound the
# rest with nothing said. Owner 2026-11-11: no build runs with a dead override id (the reach run refuses it too).
# Reads the live atlas's registry.json (~2 s); a missing build raises naming the command that makes it
step "every curated override id names a water" "$PY" -m pipeline.regs.matching.matcher

# (T535) every `pnpm check` gate but the vitest suite, from its one home `app/package.json` `check:fast` (~21 s for 17
# gates, measured 2026-11-10: colours 6 s, promises 4 s, typecheck:seo 3 s, knip 2 s, the rest < 1 s each). It holds
# what fast-fail ran by hand before: the ticket ledger (T84, T437), colours (2026-11-07 pin-study hex, T481), answers
# not stale (2026-11-07, found at the end of a release), the app typecheck (2026-11-07 TS6133, 1b84c5ae6). The whole
# vitest suite stays `pnpm check` (Tier 1); the changed files' tests run below.
step "app gates (pnpm check:fast: every pnpm check gate but vitest)" pnpm -C app run -s check:fast

# caught (2026-11-09, CI run 42717971280): 15 tests red on CI only — tests without a `needs_*` marker read the local
# atlas build (T522's deliver chain resolved it before its stub; the review 503 test read the registry first) and every
# local run passed because the build is here. The changed pipeline test files run as CI runs them: the data tiers
# deselected and this machine's builds hidden (`pipeline/tests/ci_no_build.py`)
# (T535) the marker expression has ONE home, CI's "Tests" step (`pytest -q -m "<markers>"`), read from there
CI_MARKERS=$("$PY" -c 'import re, yaml
s = [s for j in yaml.safe_load(open(".github/workflows/pipeline-ci.yml"))["jobs"].values() for s in j["steps"]
     if s.get("name") == "Tests"]
assert len(s) == 1, "Tests step not found"
m = re.search(r"-m \"([^\"]+)\"", s[0]["run"])
assert m, "no -m in the Tests step"
print(m.group(1))') || { echo "fast-fail: cannot read the marker expression from pipeline-ci.yml's Tests step" >&2; exit 1; }
# (T533) a changed pipeline SOURCE file (not a test) can break any test. With FAST_FAIL_BASE (release preflight and
# build-apk.sh pass the commit the last shipped thing was built from) the whole no-data tier then runs, as CI's "Tests"
# step does, in chunks (scripts/pytest-chunked.sh: one process for the suite grows to ~111 GB) — minutes, once per ship.
# On the default base (the unpushed commits, every lane's everyday run) only the changed test files run, and it says so.
as_ci() {
  local tests src
  src=$(grep -E '^pipeline/.*\.py$' <<<"$CHANGED" | grep -vE '(^|/)(test_[^/]*|conftest)\.py$' | grep -vE '^pipeline/tests/' || true)
  if [[ -n $src && -n ${FAST_FAIL_BASE:-} ]]; then
    echo "   $(grep -c . <<<"$src") pipeline source file(s) changed ($(head -3 <<<"$src" | paste -sd' ' -)…): the whole no-data tier, as CI"
    bash scripts/pytest-chunked.sh -- -p pipeline.tests.ci_no_build -m "$CI_MARKERS"
    return
  fi
  [[ -n $src ]] && echo "   $(grep -c . <<<"$src") pipeline source file(s) changed: the whole no-data tier runs only with a ship base (FAST_FAIL_BASE); here the changed tests"
  tests=$(grep -E '^pipeline/tests/(.*/)?test_[^/]*\.py$' <<<"$CHANGED" | while IFS= read -r f; do [[ -f $f ]] && echo "$f"; done || true)
  [[ -n $tests ]] || { echo "   (no pipeline test file changed)"; return 0; }
  echo "   as CI: $(grep -c . <<<"$tests") test file(s): $(paste -sd' ' - <<<"$tests" | cut -c1-300)"
  # exit 5 = every test in them deselected (all data-tier): nothing CI runs, nothing to fail
  "$PY" -m pytest -q -p no:cacheprovider -p pipeline.tests.ci_no_build -m "$CI_MARKERS" $tests || [[ $? == 5 ]]
}
step "changed pipeline tests pass as CI runs them (no local builds)" as_ci

# caught (2026-11-09, T529): T517's re-cut made /changes 180 KB gzip (budget 30 KB), found only after a 15 min SEO build;
# the rules-key diff of LATEST against the newest kept snapshot is ~7 s (skipped with no kept snapshot or no LATEST folder)
changes_budget() {
  local r; r=$(cat data/release/LATEST 2>/dev/null) || { echo "   (no LATEST)"; return 0; }
  [ -d "data/release/$r" ] && [ -d app/dist/seo-snapshots ] || { echo "   (no release folder or kept snapshot)"; return 0; }
  node app/tools/build-seo.mjs --release-dir "data/release/$r" --check-changes
}
step "/changes within budget (LATEST vs kept snapshot)" changes_budget

# (T535) the review app was outside every gate: a frontend break was found when the owner restarted the app. Its
# frontend typechecks (tsc --noEmit, ~2.5 s) when a curation-review/frontend file changed
review_frontend() {
  grep -qE '^curation-review/frontend/.*\.(ts|tsx)$|^curation-review/frontend/(package\.json|tsconfig[^/]*\.json)$' <<<"$CHANGED" \
    || { echo "   (no review frontend file changed)"; return 0; }
  curation-review/frontend/node_modules/.bin/tsc --noEmit -p curation-review/frontend
}
step "review app frontend typechecks (a frontend file changed)" review_frontend

# (T535) ... and its backend's tests run as CI runs them when a curation-review/backend file changed: the
# test_curation_review_* files, one process each (~30 s; scripts/pytest-chunked.sh, T638: one process grew to 60 GB)
review_backend() {
  grep -qE '^curation-review/backend/' <<<"$CHANGED" || { echo "   (no review backend file changed)"; return 0; }
  bash scripts/pytest-chunked.sh --only '/test_curation_review' -- -p pipeline.tests.ci_no_build -m "$CI_MARKERS"
}
step "review app backend tests (a backend file changed)" review_backend

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
  # (T542) the scenarios name ids and labels in the app's screens: a changed screen re-checks them
  grep -qE '^app/(apps|packages/ui-native)/' <<<"$CHANGED" && tests+=("tools/testbed-static.test.ts")
  (( ${#tests[@]} )) || { echo "   (no changed app file has a test)"; return 0; }
  tests=($(printf '%s\n' "${tests[@]}" | sort -u))
  echo "   vitest: ${#tests[@]} test file(s): ${tests[*]}"
  (cd app && pnpm exec vitest run "${tests[@]}")
}
step "vitest on changed app files" vitest_changed

if [[ -n ${FAST_FAIL_ONLY:-} ]]; then
  echo "fast-fail: PARTIAL (FAST_FAIL_ONLY=$FAST_FAIL_ONLY): $ran step(s) passed, $not_run not run — not a full pass, $((SECONDS - t0)) s"
  exit 0
fi
echo "fast-fail: all passed in $((SECONDS - t0)) s"
