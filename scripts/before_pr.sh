#!/usr/bin/env bash
#
# BEFORE THE PULL REQUEST (T1041, owner 2026-11-23 "Both"): the two suites the pre-push hook leaves to the PR, once,
# before the PR is opened (and before a push that updates it after a change of substance):
#
#   scripts/before_pr.sh        app-ci's whole vitest suite AND pipeline-ci's no-data tier, both through scripts/heavy.sh,
#                               at the same time on two heavy lanes; ends with ONE line:
#                               before-pr: PASS|FAIL (app suite rc=<n>, pipeline tier rc=<n>, <s> s)
#
# Why: the hook (`.githooks/pre-push`) must stay short (git holds its connection to GitHub open while it runs; a ~10 min
# tier outlived it, 2026-11-22), so it runs only the tests the change reaches (`before_push.py --changed`, T1041). The
# whole suites catch what an import graph cannot (a fixture or a pinned JSON read off disk, a conftest); on 2026-11-23
# both red CI runs (#63 app vitest, #67 the no-data tier) were checks no local run had made, each an hour or two.
#
# The lanes: the tier on HEAVY_LOCK (heavy.sh's default lane unless set), the app suite on BEFORE_PR_APP_LOCK
# (default ~/.claude/heavy-job-2.lock). Each waits for its lane and for free memory (heavy.sh's 30 % start gate). Each
# one's output goes to its own log (named at the end; a failed one's last lines are printed). Unfiltered, never rtk.
# BEFORE_PR_SELFTEST="<app rc> <tier rc>" stands the two suites in for exits of those codes (pipeline/tests/
# test_before_pr.py proves the verdict line with it; nothing else sets it).
set -uo pipefail
cd "$(dirname "$0")/.."
. scripts/test-env.sh   # PY, test_env_pycache, no_data_markers (CI's "Tests" step, the one home of the markers)
test_env_pycache
markers=$(no_data_markers) || exit 1
here=$(basename "$PWD")
logs=$(mktemp -d "${TMPDIR:-/tmp}/before-pr.XXXXXX")
app_log=$logs/app-suite.log tier_log=$logs/pipeline-tier.log
if [[ -n ${BEFORE_PR_SELFTEST:-} ]]; then
  read -r app_rc tier_rc <<<"$BEFORE_PR_SELFTEST"
  app_cmd=(bash -c "echo selftest app; exit $app_rc"); tier_cmd=(bash -c "echo selftest tier; exit $tier_rc")
else
  # the label names the checkout: heavy.sh refuses a second job with the same label + command (two lanes at once)
  app_cmd=(env "HEAVY_LOCK=${BEFORE_PR_APP_LOCK:-$HOME/.claude/heavy-job-2.lock}" bash scripts/heavy.sh
           "before-pr-app-$here" 30 -- pnpm -C app exec vitest run)
  tier_cmd=(bash scripts/heavy.sh "before-pr-tier-$here" 30 -- bash scripts/pytest-chunked.sh -- -q -m "$markers")
fi
t0=$SECONDS
echo "before-pr: the whole app suite and the pipeline no-data tier, at once (logs: $logs)"
"${app_cmd[@]}" >"$app_log" 2>&1 & app_pid=$!
"${tier_cmd[@]}" >"$tier_log" 2>&1 & tier_pid=$!
# a stopped script stops both (their heavy.sh wrappers stop their jobs' process groups)
trap 'kill $app_pid $tier_pid 2>/dev/null; exit 143' INT TERM
wait "$app_pid"; app_rc=$?
wait "$tier_pid"; tier_rc=$?
for pair in "app suite:$app_rc:$app_log" "pipeline tier:$tier_rc:$tier_log"; do
  IFS=: read -r what rc log <<<"$pair"
  if (( rc )); then echo "── $what FAILED (rc=$rc): the last lines of $log"; tail -40 "$log"; fi
done
secs=$((SECONDS - t0))
if (( app_rc == 0 && tier_rc == 0 )); then
  rm -rf "$logs"
  echo "before-pr: PASS (app suite rc=0, pipeline tier rc=0, $secs s)"
  exit 0
fi
echo "before-pr: FAIL (app suite rc=$app_rc, pipeline tier rc=$tier_rc, $secs s) — logs: $app_log $tier_log" >&2
exit 1
