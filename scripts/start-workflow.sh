#!/usr/bin/env bash
#
# START A GITHUB WORKFLOW AND NAME ITS RUN (T666 item 6): `gh workflow run`, then the id of the run it made, on stdout.
# A start GitHub refuses is RETRIED, a bounded number of times, each try said out loud (stderr).
#
#   scripts/start-workflow.sh <owner/repo> <ref> <workflow file> <workflow name prefix> [gh workflow run args…]
#   e.g. scripts/start-workflow.sh me/repo my-branch feeds-runtiming.yml "feeds — run timing" -f upload=true -f tiles=t-1
#
# caught (build 27, 2026-11-11): GitHub refused to start the run-timing workflow ("startup failure", a generic error; it
# recovered in ~30 minutes) and the only retry was the whole release command. deploy/staging/upload.sh calls this.
#
# WHAT IS RETRIED — only a start that made nothing run:
#   - `gh workflow run` itself failed (GitHub refused the dispatch), or
#   - the run it made ended at once with conclusion `startup_failure` (it ran no job).
# NOT retried: a dispatch GitHub accepted whose run did not show within the poll window — a second dispatch could then
# run the workflow twice; it stops (exit 75) and says to look at the Actions page first. A run that starts and later
# FAILS is the caller's to watch (`gh run watch --exit-status`), never retried here.
#
#   WF_START_TRIES   how many starts at most (default 3)
#   WF_START_WAIT    seconds between tries (default 120)
#   WF_POLLS         how many times to look for the new run after a dispatch (default 30)
#   WF_POLL_WAIT     seconds between looks (default 4)
#   WF_SETTLE        seconds to give a new run before asking whether it failed at startup (default 8)
# Exit: 0 with the run id on stdout; 75 when no run was started (each try's reason is on stderr); 2 on bad usage.
set -euo pipefail
(( $# >= 4 )) || { echo "usage: $0 <owner/repo> <ref> <workflow file> <workflow name prefix> [gh workflow run args…]" >&2; exit 2; }
REPO=$1 REF=$2 WF=$3 NAME=$4; shift 4
TRIES=${WF_START_TRIES:-3} WAIT=${WF_START_WAIT:-120} POLLS=${WF_POLLS:-30} POLL_WAIT=${WF_POLL_WAIT:-4} SETTLE=${WF_SETTLE:-8}
[[ $TRIES =~ ^[1-9][0-9]*$ ]] || { echo "start-workflow: WF_START_TRIES must be a count ≥ 1, not '$TRIES'" >&2; exit 2; }

since() {   # a minute back, UTC: the Mac and GitHub clocks differ a little (BSD date, else GNU)
  date -u -v-60S +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '60 seconds ago' +%Y-%m-%dT%H:%M:%SZ
}

for (( try = 1; try <= TRIES; try++ )); do
  t0=$(since)
  why=""
  if ! out=$(gh workflow run "$WF" -R "$REPO" --ref "$REF" "$@" 2>&1); then
    why="GitHub refused the dispatch: $(tr '\n' ' ' <<<"$out" | cut -c1-300)"
  else
    run=""
    for (( i = 0; i < POLLS; i++ )); do
      run=$(gh run list -R "$REPO" --branch "$REF" --event workflow_dispatch --limit 10 --json databaseId,createdAt,workflowName \
        --jq "[.[] | select(.workflowName | startswith(\"$NAME\")) | select(.createdAt >= \"$t0\")][0].databaseId // empty")
      [[ -n $run ]] && break
      sleep "$POLL_WAIT"
    done
    if [[ -z $run ]]; then
      echo "start-workflow: GitHub accepted the dispatch of $WF but no run showed in $((POLLS * POLL_WAIT)) s. NOT retried" \
           "(a second dispatch could run it twice): look at https://github.com/$REPO/actions, then rerun" >&2
      exit 75
    fi
    sleep "$SETTLE"
    concl=$(gh run view "$run" -R "$REPO" --json conclusion --jq '.conclusion // ""')
    if [[ $concl != startup_failure ]]; then
      (( try > 1 )) && echo "start-workflow: $WF started on try $try of $TRIES (run $run)" >&2
      echo "$run"; exit 0
    fi
    why="run $run ended with a startup failure (it ran no job): https://github.com/$REPO/actions/runs/$run"
  fi
  if (( try < TRIES )); then
    echo "start-workflow: $WF did not start (try $try of $TRIES): $why — trying again in $WAIT s" >&2
    sleep "$WAIT"
  else
    echo "start-workflow: $WF did not start (try $try of $TRIES): $why" >&2
  fi
done
echo "start-workflow: $WF was NOT started after $TRIES tries — nothing ran. GitHub's side: wait and rerun" \
     "(WF_START_TRIES / WF_START_WAIT set the bound)" >&2
exit 75
