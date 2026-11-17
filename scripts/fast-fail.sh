#!/usr/bin/env bash
#
# FAST-FAIL (T483, AGENTS 20a): the cheap checks that stop a doomed long job in a minute or two. Target <= 2 min.
#
#   ./scripts/fast-fail.sh            run every item; exits non-zero on the FIRST failure (its output is shown whole)
#   FAST_FAIL_ONLY=<regex> …          only the steps whose name matches (rerun one failed step); says PARTIAL, not a pass
#   FAST_FAIL_ALL=1 …                 (T666 item 3) go on past a failed step and end with the list of EVERY step that
#                                     failed (exit 1); `pipeline release` preflight runs it so, and reads the list from
#                                     FAST_FAIL_REPORT=<file> (one failed step name per line). caught (build 27,
#                                     2026-11-11): six release runs, each stopped by the next single failure.
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
DEV=redesign/stream-sections
# the test environment (T667 slice 7): PY, one fresh PYTHONPYCACHEPREFIX per run (T638 item 3), CI's marker expression
. scripts/test-env.sh
test_env_pycache
t0=$SECONDS
ran=0; not_run=0; failed=()
step() {
  if [[ -n ${FAST_FAIL_ONLY:-} && ! $1 =~ $FAST_FAIL_ONLY ]]; then not_run=$((not_run + 1)); return 0; fi
  local s=$SECONDS name=$1 rc; ran=$((ran + 1)); echo "── $1"; shift
  if [[ -n ${FAST_FAIL_ALL:-} ]]; then
    # a subshell OUTSIDE any `if` / `||`, so `set -e` still stops the step at its own first failing command
    set +e; ( set -e; "$@" ); rc=$?; set -e
    if (( rc )); then
      failed+=("$name"); echo "   FAILED (rc=$rc, $((SECONDS - s)) s) — going on (FAST_FAIL_ALL): the list is at the end"
      return 0
    fi
  else
    "$@"
  fi
  echo "   ok ($((SECONDS - s)) s)"
}
finish() {
  local f
  [[ -n ${FAST_FAIL_REPORT:-} ]] && printf '%s\n' ${failed[@]+"${failed[@]}"} > "$FAST_FAIL_REPORT"
  if (( ${#failed[@]} )); then
    echo "fast-fail: ${#failed[@]} step(s) FAILED of $ran run ($((SECONDS - t0)) s) — each one's output is above:" >&2
    for f in "${failed[@]}"; do echo "  - $f" >&2; done
    echo "  rerun one: FAST_FAIL_ONLY='<part of its name>' ./scripts/fast-fail.sh" >&2
    exit 1
  fi
  if [[ -n ${FAST_FAIL_ONLY:-} ]]; then
    echo "fast-fail: PARTIAL (FAST_FAIL_ONLY=$FAST_FAIL_ONLY): $ran step(s) passed, $not_run not run — not a full pass, $((SECONDS - t0)) s"
    exit 0
  fi
  echo "fast-fail: all passed in $((SECONDS - t0)) s"
}
# (T666) the runner's own test (pipeline/tests/test_release_follows.py): two stand-in steps instead of the list, so
# "stops at the first" and "lists every failure" are proved without running a real check
if [[ -n ${FAST_FAIL_SELFTEST:-} ]]; then
  selftest_fails() { echo "   selftest: before"; false; echo "   selftest: NOT REACHED"; }
  step "selftest: a step that fails" selftest_fails
  step "selftest: a step that passes" true
  step "selftest: a second step that fails" false
  finish; exit 0
fi

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

# caught (T540, 2026-11-12): STATUS.md grew to 155 KB with nobody moving the old days out; the fix is one command
# (`scripts/status_archive.py --move`); < 0.1 s
step "STATUS.md under its size limit (older days archived)" python3 scripts/status_archive.py --check

# caught (T536, 2026-11-10): heavy.sh freed its lock while a stopped wrapper's job ran on, and read an empty memory
# reading as 0 % free (killing a healthy job). Its shell test (fake jobs, stubbed memory_pressure, scratch lanes, ~10 s)
# runs when heavy.sh or the test changes
heavy_test() {
  grep -qE '^scripts/(heavy\.sh|tests/heavy\.test\.sh)$' <<<"$CHANGED" || { echo "   (heavy.sh unchanged)"; return 0; }
  bash scripts/tests/heavy.test.sh
}
step "heavy.sh shell test (heavy.sh changed)" heavy_test

# (T666 item 6) the bounded workflow-start retry the staging upload uses: its shell test (a stand-in gh, ~2 s) runs when
# the script or its test changes
start_workflow_test() {
  grep -qE '^scripts/(start-workflow\.sh|tests/start-workflow\.test\.sh)$' <<<"$CHANGED" || { echo "   (start-workflow.sh unchanged)"; return 0; }
  bash scripts/tests/start-workflow.test.sh
}
step "start-workflow.sh shell test (start-workflow.sh changed)" start_workflow_test

# caught (T538, 2026-11-10): build-apk.sh only warned on an app version with no changelog entry and published empty
# notes ("No notes were published"); 0.1 s (also in pnpm check:fast)
step "app version has its changelog entry (newest)" node scripts/check-changelog.mjs

# caught (T555 item 4, 2026-11-11): the release's app-version gate knew paint and salmon only; water/3 and geo/2 shipped
# on a minAppVersion set by hand. The gate is now derived from what each app version reads (app_reads.json): a format
# moved with no app version bump, or a version with no row, fails here; 0.2 s (also in pnpm check:fast)
step "app version recorded in the app-reads table (the release gate's source)" "$PY" -m pipeline.tools.emit.app_reads --check

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
# test_tile_smooth), found only by the Tier 1 run after the merges; the guard was T432's test_smoke_coverage. Since T667 D6
# (2026-11-14) the set is derived (pipeline/tests/smoke.py), so a new file is in it by construction; this pins the rule (~2 s)
step "smoke set is derived by rule" "$PY" -m pytest -q -p no:cacheprovider pipeline/tests/test_smoke_coverage.py

# caught (2026-11-09): a committed `pytest.skip` failed test_no_silent_skips only in the full suite. A changed pipeline
# test file runs that guard here (~1 s)
silent_skips() {
  grep -qE '^pipeline/(.*/)?(test_[^/]*|conftest)\.py$|^conftest\.py$' <<<"$CHANGED" \
    || { echo "   (no pipeline test file changed)"; return 0; }
  "$PY" -m pytest -q -p no:cacheprovider pipeline/tests/test_no_silent_skips.py
}
step "no silent skips (a pipeline test file changed)" silent_skips

# caught 2026-11-14 (CI red on b07996827): pipeline/deliver/release/live_counts.py read the curated tree and wrote a
# file, unclassified; its lane ran only the tests of the files it changed. A changed pipeline / review-app Python file
# runs the one-writer scan (AGENTS 36b, ~2 s)
curated_writer() {
  grep -qE '^(pipeline|curation-review/backend)/.*\.py$' <<<"$CHANGED" \
    || { echo "   (no pipeline Python file changed)"; return 0; }
  "$PY" -m pytest -q -p no:cacheprovider pipeline/tests/test_curated_writer.py
}
step "one writer for curated data (a pipeline Python file changed)" curated_writer

# caught 2026-11-14 (CI red on acd561469): pipeline/tools/mini_release.py split and joined watershed codes by hand
# (`"-".join`, `.split("-")`) where pipeline.common.utils.wsc owns them. A changed pipeline Python file runs the scan (~1 s)
wsc_scan() {
  grep -qE '^(pipeline|curation-review)/.*\.py$' <<<"$CHANGED" \
    || { echo "   (no pipeline Python file changed)"; return 0; }
  "$PY" -m pytest -q -p no:cacheprovider pipeline/tests/test_wsc_relation.py
}
step "watershed codes read only in wsc.py (a pipeline Python file changed)" wsc_scan

# caught 2026-11-14 (T667 slice 8): the tile export drew the configured gpkg's areas and the bundle read HYDAT, the
# gpkg, clim.json and the DFO feed from the live tree whatever it was given — found only by running the chain on the
# invented world. A changed atlas / deliver / gauges module or mini fixture runs the mini chain twice (~10 s, no data)
# caught 2026-11-15 (CI red; T788 item 3): a pipeline/regs change (the DFO typing, the catalogue) moved the mini release
# and nothing here ran it — regs is in the trigger
mini_chain() {
  grep -qE '^pipeline/(atlas|deliver|gauges|regs|tools/mini_release)[/.]|^pipeline/tests/fixtures/mini/|^app/packages/data/dev/mini-release/' <<<"$CHANGED" \
    || { echo "   (no atlas / deliver / gauges / regs / mini file changed)"; return 0; }
  "$PY" -m pytest -q -p no:cacheprovider pipeline/tests/test_mini_release.py pipeline/tests/test_mini_deliver_inputs.py \
    pipeline/tests/test_export_claims.py
  # (T667 slice 8) and the committed copy the app's tests read still matches a rebuild (~8 s, by content)
  "$PY" -m pipeline.tools.mini_release --check
}
step "the mini chain on invented data (an atlas / deliver / gauges / mini file changed)" mini_chain

# caught 2026-11-14 (CI red on 931f14e75): test_liveness's scan of every pipeline file refused mini_release reading
# a station's `active` / `realtime` itself; the lane ran only the tests importing the modules it changed, and a scan
# imports nothing. A changed pipeline Python file runs every test file that walks the pipeline's sources (found by
# the same grep each time, so a new scanner joins by itself), CI's markers (~40 s). Two are left out: their time is
# the review app's own fixtures, not the scan (test_review_screen 83 s, test_review_marks 15 s), and CI runs them.
source_scanners() {
  grep -qE '^pipeline/.*\.py$' <<<"$CHANGED" || { echo "   (no pipeline Python file changed)"; return 0; }
  local files
  files=$(git grep -l -E 'rglob\("\*\.py"\)|glob\("\*\*/\*\.py"\)|_py_sources\(|walk\(.*pipeline' -- 'pipeline/tests/test_*.py' \
    | grep -vE 'test_review_(screen|marks)\.py$')
  # shellcheck disable=SC2086
  local markers; markers=$(no_data_markers) || return 1
  "$PY" -m pytest -q -p no:cacheprovider -m "$markers" $files
}
step "every source scanner (a pipeline Python file changed)" source_scanners

# caught 2026-11-15 (CI red on 189458c93 and 67c9b32f1): T742 B dropped `historic_only`'s groups argument and T742 C made
# `search_splits` read the atlas registry; test_marks_never_orphaned still passed the groups and
# test_review_checklist's cut-search test (data-free) now read the build. The lanes ran `test_review_*` / `test_dfo_*`
# directly in the DATA tier, so the build was visible and the first file was not in the run at all. A changed review
# module (pipeline/regs/review, the review app's backend) runs EVERY test file that reads review code (found by the
# same grep each time, so a new one joins by itself) under CI's markers, which hide this machine's builds (tiers.py)
review_tests() {
  grep -qE '^(pipeline/regs/review|curation-review/backend)/.*\.py$' <<<"$CHANGED" \
    || { echo "   (no review module changed)"; return 0; }
  local files markers
  files=$(git grep -l -E 'pipeline\.regs\.review|regs\.review import|curation-review' -- 'pipeline/tests/test_*.py')
  markers=$(no_data_markers) || return 1
  # shellcheck disable=SC2086
  "$PY" -m pytest -q -p no:cacheprovider -m "$markers" $files
}
step "every review test, CI's tier (a review module changed)" review_tests

# caught 2026-11-14 (CI red on b07996827): scripts/test-env.sh hard-coded .venv/bin/python, which a hosted runner has
# not; nothing ran the contract's test before the push. A changed scripts/test-*.sh, test-env runner or tiers file runs
# it (~1 s; it also proves the source stops naming PIPELINE_PY when no interpreter is found)
test_env_contract() {
  grep -qE '^scripts/(test-[^/]*|pytest-chunked|fast-fail|fresh-copy-tests)\.sh$|^pipeline/tests/(tiers|test_test_env|conftest)\.py$|^conftest\.py$|^\.github/workflows/pipeline-ci\.yml$' <<<"$CHANGED" \
    || { echo "   (no test-environment file changed)"; return 0; }
  "$PY" -m pytest -q -p no:cacheprovider pipeline/tests/test_test_env.py
}
step "test-environment contract (a test-env file changed)" test_env_contract

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
# caught (2026-11-13, T708): a cuts.json label naming no place outside its brackets stopped the atlas build after 9 min
# (`splits.label_problem`); the corpus test runs in a second
step "every curated cut label names a place" "$PY" -m pytest -q -p no:cacheprovider \
  pipeline/tests/test_splits.py::test_every_curated_label_names_a_place

# caught (T556, 2026-11-11): the live feeds' formats were hand-copied per side (the 2026-11-09 failure above), and no
# test held the worker's DFO parser / fingerprint / row typing to the Python's since T431. The formats are generated
# from one table, and the Python's reading of the saved DFO pages is a committed vector file the worker's test must
# reproduce: a stale generated file or vector file fails here; 0.2 s + 1.5 s (also in `python -m pipeline.tools.emit --check`)
step "feed formats generated from the one table" "$PY" -m pipeline.tools.emit.feed_formats --check
step "DFO parity vectors are what the Python reads (the worker's test reproduces them)" "$PY" -m pipeline.tools.emit.dfo_vectors --check
# (T810 V-py) the same pin for the trout table: the committed vectors are the committed trout fixture's cells (a vector
# edited by hand, or a fixture re-cut without them, fails here; scripts/trout-refresh.sh writes both)
step "trout table vectors are the trout fixture's cells (the app's reader reproduces them)" "$PY" -m pipeline.tools.emit.trout_vectors --check

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
# caught 2026-11-15 (CI red on e0785054a): T750 removed WaterMapView's Chooser (tools/back-layers.test.ts still listed it)
# and T737 added two swallowing catches (tools/failures.test.ts pins each file's count); both lanes ran their targeted
# tests + `pnpm check:fast`, which ran no vitest, and the two inventories import none of what they scan, so no sibling
# test finds them. check:fast now ends with `pnpm inventories` (those two files, ~3 s), whatever changed.
step "app gates (pnpm check:fast: every pnpm check gate but vitest)" pnpm -C app run -s check:fast

# caught (2026-11-09, CI run 42717971280): 15 tests red on CI only — tests without a `needs_*` marker read the local
# atlas build (T522's deliver chain resolved it before its stub; the review 503 test read the registry first) and every
# local run passed because the build is here. The changed pipeline test files run as CI runs them: the data tiers
# deselected and this machine's builds hidden (a no-data tier always hides them: pipeline/tests/tiers.py, T667 slice 7)
# (T535) the marker expression has ONE home, CI's "Tests" step (`pytest -q -m "<markers>"`), read from there
CI_MARKERS=$(no_data_markers) || exit 1
# (T533) a changed pipeline SOURCE file (not a test) can break any test. With FAST_FAIL_BASE (release preflight and
# build-apk.sh pass the commit the last shipped thing was built from) the whole no-data tier then runs, as CI's "Tests"
# step does, in chunks (scripts/pytest-chunked.sh: one process for the suite grows to ~111 GB) — minutes, once per ship.
# On the default base (the unpushed commits, every lane's everyday run) only the changed test files run, and it says so.
as_ci() {
  local tests src
  src=$(grep -E '^pipeline/.*\.py$' <<<"$CHANGED" | grep -vE '(^|/)(test_[^/]*|conftest)\.py$' | grep -vE '^pipeline/tests/' || true)
  if [[ -n $src && -n ${FAST_FAIL_BASE:-} ]]; then
    echo "   $(grep -c . <<<"$src") pipeline source file(s) changed ($(head -3 <<<"$src" | paste -sd' ' -)…): the whole no-data tier, as CI"
    bash scripts/pytest-chunked.sh -- -m "$CI_MARKERS"
    return
  fi
  [[ -n $src ]] && echo "   $(grep -c . <<<"$src") pipeline source file(s) changed: the whole no-data tier runs only with a ship base (FAST_FAIL_BASE); here the changed tests"
  tests=$(grep -E '^pipeline/tests/(.*/)?test_[^/]*\.py$' <<<"$CHANGED" | while IFS= read -r f; do [[ -f $f ]] && echo "$f"; done || true)
  [[ -n $tests ]] || { echo "   (no pipeline test file changed)"; return 0; }
  echo "   as CI: $(grep -c . <<<"$tests") test file(s): $(paste -sd' ' - <<<"$tests" | cut -c1-300)"
  # exit 5 = every test in them deselected (all data-tier): nothing CI runs, nothing to fail
  "$PY" -m pytest -q -p no:cacheprovider -m "$CI_MARKERS" $tests || [[ $? == 5 ]]
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
  bash scripts/pytest-chunked.sh --only '/test_curation_review' -- -m "$CI_MARKERS"
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
  # caught (2026-11-12, T681): a literal `borderRadius: 0` in shell/MapLegend.tsx failed tools/one-button.test.ts only in
  # CI — the tools/ gates that SCAN ui-native's source off disk import nothing of it, so no sibling rule finds them. A
  # changed ui-native file re-runs every such gate (22 files, ~6 s).
  if grep -qE '^app/packages/ui-native/src/' <<<"$CHANGED"; then
    while IFS= read -r t; do tests+=("$t"); done < <(cd app && grep -l "packages/ui-native/src" tools/*.test.ts \
      | xargs grep -l "readFileSync\|readdirSync")
  fi
  # caught (2026-11-14, T734): two failures only CI saw. tools/failures.test.ts (no silent fallback) scans every app
  # source file off disk and pins each file's unhandled-catch count, so a catch removed in packages/map broke it; and
  # changelog.ts has no sibling test (its order is pinned in core's appUpdate.test.ts: 0.7.10 sorted as a string).
  grep -qE '^app/(apps|packages)/.*\.(ts|tsx)$' <<<"$CHANGED" && tests+=("tools/failures.test.ts")
  # caught 2026-11-15 (CI red on e0785054a): tools/back-layers.test.ts inventories every back surface in apps/mobile, ui,
  # ui-native and map off disk; only a ui-native change ran it above. Any of its roots changed runs it
  grep -qE '^app/(apps/mobile|packages/(ui|ui-native|map))/src/.*\.(ts|tsx)$' <<<"$CHANGED" && tests+=("tools/back-layers.test.ts")
  grep -qE '^app/packages/core/src/changelog\.ts$' <<<"$CHANGED" && tests+=("packages/core/src/appUpdate.test.ts")
  (( ${#tests[@]} )) || { echo "   (no changed app file has a test)"; return 0; }
  tests=($(printf '%s\n' "${tests[@]}" | sort -u))
  echo "   vitest: ${#tests[@]} test file(s): ${tests[*]}"
  (cd app && pnpm exec vitest run "${tests[@]}")
}
step "vitest on changed app files" vitest_changed

finish
