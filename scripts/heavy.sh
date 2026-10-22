#!/bin/bash
# heavy.sh — run ONE memory-heavy job at a time, with a memory guard.
#
#   heavy.sh <label> <min_free_pct_to_start> -- <command...>
#
# - A machine-wide lock (mkdir is atomic): only one heavy job runs at a time; others wait.
# - Refuses to start until system free memory >= <min_free_pct_to_start> % (memory_pressure -Q).
# - While it runs, kills the job's whole process group if free memory falls below KILL_PCT (default 6 %),
#   so the Mac and VS Code never go down (the 2026-10-06 crash: overlapping atlas/reach/pytest/16-proc jobs).
# Heavy = atlas build, reach run, pipeline.deliver, full pytest suites, golden regeneration, answers build.
set -u
label=$1; need=$2; shift 2; [[ $1 == -- ]] && shift
LOCK=${HEAVY_LOCK:-$HOME/.claude/heavy-job.lock}
KILL_PCT=${KILL_PCT:-6}
free_pct() { memory_pressure -Q 2>/dev/null | awk -F': ' '/free percentage/ {gsub("%","",$2); print int($2)}'; }

PAUSE=${HEAVY_PAUSE:-$HOME/.claude/heavy-job.pause}
while [[ -e $PAUSE ]]; do echo "[heavy:$label] PAUSED ($(cat "$PAUSE")) — remove $PAUSE to resume"; sleep 60; done
while ! mkdir "$LOCK" 2>/dev/null; do
  holder=$(cat "$LOCK/owner" 2>/dev/null)
  pid=${holder%% *}
  if [[ -n $pid ]] && ! kill -0 $pid 2>/dev/null; then rm -rf "$LOCK"; continue; fi   # stale lock
  echo "[heavy:$label] waiting for lock held by: $holder"; sleep 20
done
echo "$$ $label $(date '+%F %T')" > "$LOCK/owner"
trap 'rm -rf "$LOCK"' EXIT INT TERM

while (( $(free_pct) < need )); do echo "[heavy:$label] free $(free_pct)% < $need% — waiting"; sleep 20; done
echo "[heavy:$label] start (free $(free_pct)%): $*"

set -m                      # job control: the command gets its own process group
"$@" &
job=$!
while kill -0 $job 2>/dev/null; do
  f=$(free_pct)
  if (( f < KILL_PCT )); then
    echo "[heavy:$label] KILLED: system free memory ${f}% < ${KILL_PCT}%" >&2
    kill -TERM -$job 2>/dev/null; sleep 5; kill -KILL -$job 2>/dev/null
    wait $job 2>/dev/null; exit 137
  fi
  sleep 5
done
wait $job; rc=$?
echo "[heavy:$label] done rc=$rc (free $(free_pct)%)"
exit $rc
