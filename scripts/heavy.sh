#!/bin/bash
# heavy.sh — run ONE memory-heavy job at a time per lane, with a memory guard.
#
#   heavy.sh [--again] <label> <min_free_pct_to_start> -- <command...>
#   heavy.sh --status                 what holds each lane: holder, command, elapsed, free memory, last log lines
#
# - A lock per lane (mkdir is atomic). Two lanes exist, nothing else is accepted (owner 2026-11-09: max 2):
#   ~/.claude/heavy-job.lock (the default) and ~/.claude/heavy-job-2.lock (HEAVY_LOCK=…/heavy-job-2.lock).
# - Refuses to start until system free memory >= <min_free_pct_to_start> % (memory_pressure -Q).
# - While it runs, stops the job's whole process group if free memory falls below KILL_PCT (default 6 %), or if the
#   group's PHYSICAL FOOTPRINT (`footprint`, compressed pages included) passes HEAVY_MAX_FOOTPRINT_GB (default: the
#   machine's RAM). T638: the full pytest suite grew to 111 GB, mostly compressed, while memory_pressure still read
#   83-86 % free, and the kernel killed it; free % alone cannot see that. Both guards keep the Mac and VS Code up
#   (the 2026-10-06 crash: overlapping atlas/reach/pytest/16-proc jobs).
# Heavy = atlas build, reach run, pipeline.deliver, full pytest suites, golden regeneration, answers build.
# - RE-ENTRANT for the lock holder: a heavy.sh run INSIDE a heavy job (build-apk.sh wrapped in heavy.sh calls heavy.sh
#   itself) sees HEAVY_HELD naming the lock's owner and runs its command straight away under the outer job's lock and
#   memory guard — it never waits on the lock its own parent holds (the 2026-10-31 build-apk deadlock).
#
# T536 (2026-11-10):
# - STOPPING THE WRAPPER STOPS THE JOB. INT/TERM: TERM to the job's process group, HEAVY_STOP_GRACE s (10), KILL, then
#   the lock goes. A SIGKILLed wrapper cannot clean up: the owner file records the job's process group, and a waiter
#   takes the lock only when the wrapper AND that group are gone.
# - A MEMORY READING THAT IS NOT 0-100 IS UNREADABLE, never 0: at the start it waits and says so; while running it
#   warns, and stops the job only after HEAVY_UNREADABLE_MAX (12) unreadable samples in a row (exit 74, its own message).
# - The owner file is line 1 `<wrapper pid> <label> <date>` (as before) then `pgid:`, `cmd:`, `cwd:`, `log:`, `started:`.
#   `log:` is HEAVY_LOG, else the file stdout goes to. A heartbeat line every HEAVY_HEARTBEAT s (300). A second call
#   with the same label + command while one holds or waits on either lane is refused (exit 3) unless `--again`.
# - The lock is removed only by its owner; a stale lock is taken over under a mutex (`<lock>.takeover`) by `mv` then
#   delete, so two waiters take it once.
# Exit codes: the job's own; 137 killed for memory (free % or footprint); 74 memory unreadable; 143 stopped (INT/TERM);
# 2 usage / unknown lane; 3 duplicate.
set -u
LANES_DIR=${HEAVY_LANES_DIR:-$HOME/.claude}      # tests point this at a scratch dir; nothing else should
LANES=("$LANES_DIR/heavy-job.lock" "$LANES_DIR/heavy-job-2.lock")
KILL_PCT=${KILL_PCT:-6}
POLL=${HEAVY_POLL:-5}                  # seconds between memory samples while the job runs
WAIT_POLL=${HEAVY_WAIT_POLL:-20}       # seconds between lock / start-gate checks
FP_EVERY=${HEAVY_FOOTPRINT_EVERY:-30}  # seconds between footprint samples
BEAT=${HEAVY_HEARTBEAT:-300}
GRACE=${HEAVY_STOP_GRACE:-10}
UNREADABLE_MAX=${HEAVY_UNREADABLE_MAX:-12}
MAX_FP_GB=${HEAVY_MAX_FOOTPRINT_GB:-$(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1073741824 ))}

usage() { echo "usage: heavy.sh [--again] <label> <min_free_pct> -- <command...>  |  heavy.sh --status" >&2; exit 2; }

# free memory %, or NOTHING when the reading is not an integer 0-100 (memory_pressure failed or changed its wording)
free_pct() {
  local v
  v=$(memory_pressure -Q 2>/dev/null | awk -F': ' '/free percentage/ {gsub("%","",$2); print $2; exit}')
  [[ $v =~ ^[0-9]+$ ]] && (( 10#$v <= 100 )) && echo $((10#$v))
}
# physical footprint of a process group, whole GB (rounded down); nothing when it cannot be read
group_footprint_gb() {
  local pids
  pids=$(ps -axo pid=,pgid= | awk -v g="$1" '$2 == g {printf "-p %s ", $1}')
  [[ -n $pids ]] || return 0
  # shellcheck disable=SC2086
  footprint -f bytes --noCategories $pids 2>/dev/null \
    | awk '/phys_footprint:/ {s += $2; n++} END {if (n) printf "%d\n", s / 1073741824}'
}
owner_field() { sed -n "s/^$2: //p" "$1/owner" 2>/dev/null | head -1; }   # owner_field <lock> <key>
owner_pid() { head -1 "$1/owner" 2>/dev/null | cut -d' ' -f1; }
alive() { [[ -n ${1:-} ]] && kill -0 "$1" 2>/dev/null; }
group_alive() { [[ -n ${1:-} ]] && kill -0 -- "-$1" 2>/dev/null; }
mtime() { stat -f %m "$1" 2>/dev/null || echo 0; }
# a lock is stale when its wrapper is gone AND the job's process group is gone (a SIGKILLed wrapper leaves its job
# running: that lock is still held). A lock dir with no owner file for over a minute is a wrapper that died at mkdir.
lock_stale() {
  local lock=$1 pid
  [[ -d $lock ]] || return 1
  if [[ ! -s $lock/owner ]]; then (( $(date +%s) - $(mtime "$lock") > 60 )); return; fi
  pid=$(owner_pid "$lock")
  ! alive "$pid" && ! group_alive "$(owner_field "$lock" pgid)"
}
elapsed() {
  local s=$1 now; now=$(date +%s); s=$(( now - s ))
  printf '%dh%02dm' $(( s / 3600 )) $(( s % 3600 / 60 ))
}

status() {
  local lock pid pgid started log q f
  f=$(free_pct)
  echo "free memory: ${f:-UNREADABLE (memory_pressure -Q)}%   pause file: $([[ -e ${HEAVY_PAUSE:-$HOME/.claude/heavy-job.pause} ]] && echo PRESENT || echo none)"
  for lock in "${LANES[@]}"; do
    echo "── $lock"
    if [[ ! -d $lock ]]; then echo "   free"
    else
      pid=$(owner_pid "$lock"); pgid=$(owner_field "$lock" pgid); started=$(owner_field "$lock" started)
      if [[ -z $started ]]; then   # an owner file written before T536: `<pid> <label> <YYYY-MM-DD HH:MM:SS>`
        started=$(date -j -f '%Y-%m-%d %H:%M:%S' "$(head -1 "$lock/owner" 2>/dev/null | cut -d' ' -f3-4)" +%s 2>/dev/null)
      fi
      echo "   holder: $(head -1 "$lock/owner" 2>/dev/null || echo '(no owner file)')"
      echo "   elapsed: $([[ -n $started ]] && elapsed "$started" || echo '?')   wrapper $pid: $(alive "$pid" && echo alive || echo GONE)" \
           "  job group ${pgid:-?}: $(if [[ -z $pgid ]]; then echo 'not recorded'; elif group_alive "$pgid"; then echo alive; else echo GONE; fi)"
      lock_stale "$lock" && echo "   STALE: the next waiter takes it over"
      if [[ -z $(owner_field "$lock" cmd) ]]; then
        echo "   (owner file from a heavy.sh before T536: no command, group or log recorded; ps -p $pid shows the wrapper)"
      else
        echo "   cmd: $(owner_field "$lock" cmd)"
        echo "   cwd: $(owner_field "$lock" cwd)"
        [[ -s $lock/beat ]] && echo "   last beat: $(cat "$lock/beat")"
        log=$(owner_field "$lock" log)
        echo "   log: $log"
        if [[ -f $log ]]; then tail -n "${HEAVY_STATUS_LINES:-5}" "$log" | sed 's/^/   | /'; fi
      fi
    fi
    for q in "$lock.queue"/*; do
      [[ -f $q ]] || continue
      if alive "${q##*/}"; then echo "   queued: ${q##*/} $(head -1 "$q")"; else rm -f "$q"; fi
    done
  done
}

[[ ${1:-} == --status ]] && { status; exit 0; }
again=0
[[ ${1:-} == --again ]] && { again=1; shift; }
(( $# >= 3 )) || usage
label=$1; need=$2; shift 2; [[ $1 == -- ]] && shift
(( $# )) || usage
[[ $need =~ ^[0-9]+$ ]] && (( need <= 100 )) || { echo "[heavy:$label] min free % must be 0-100, got '$need'" >&2; exit 2; }
LOCK=${HEAVY_LOCK:-${LANES[0]}}
if [[ $LOCK != "${LANES[0]}" && $LOCK != "${LANES[1]}" ]]; then
  echo "[heavy:$label] unknown lane $LOCK — only ${LANES[0]} or ${LANES[1]} (max 2 heavy lanes)" >&2; exit 2
fi
if [[ -n ${HEAVY_HELD:-} && -d $LOCK && "$(owner_pid "$LOCK")" == "$HEAVY_HELD" ]]; then
  echo "[heavy:$label] inside heavy job $HEAVY_HELD — running under its lock: $*"
  exec "$@"
fi
# PRECHECK BEFORE THE LOCK (T483 fast-fail; caught 2026-11-09: `session.sh --build` with MLN_LOCAL_MAVEN and no
# TESTBED_APK_TAG failed only after waiting for the lock). A script whose text says `heavy-precheck: yes` is run once
# with HEAVY_PRECHECK=1 first: it checks its flags and environment and exits (0 = go on). A failure ends here, unlocked.
pre=$1; [[ $pre == bash || $pre == sh ]] && pre=${2:-}
if [[ -f $pre ]] && grep -q 'heavy-precheck: yes' "$pre"; then
  HEAVY_PRECHECK=1 "$@" || { rc=$?; echo "[heavy:$label] precheck failed (rc=$rc) — the lock was not taken" >&2; exit $rc; }
fi

cmd="$label :: $(printf '%q ' "$@")"
# DUPLICATE (chat 9: two identical quiet background jobs each ran whole): the same label + command holding or waiting
# on either lane is refused unless --again
if (( ! again )); then
  for l in "${LANES[@]}"; do
    if [[ -d $l && "$(owner_field "$l" cmd)" == "$cmd" ]] && ! lock_stale "$l"; then
      echo "[heavy:$label] refused: the same label + command already holds $l ($(head -1 "$l/owner")); heavy.sh --status;" \
           "--again runs it a second time" >&2; exit 3
    fi
    for q in "$l.queue"/*; do
      [[ -f $q ]] && alive "${q##*/}" && [[ "$(sed -n 2p "$q")" == "$cmd" ]] || continue
      echo "[heavy:$label] refused: the same label + command is already waiting on $l (pid ${q##*/}); --again runs it a second time" >&2
      exit 3
    done
  done
fi

if [[ -n ${HEAVY_LOG:-} ]]; then log=$HEAVY_LOG
else
  log=$(lsof -a -p $$ -d 1 -Fn 2>/dev/null | sed -n 's/^n//p' | head -1)
  [[ -n $log && -f $log ]] || log="(stdout is not a file; HEAVY_LOG=<path> names one)"
fi

QUEUE="$LOCK.queue/$$"
job=""; nap_pid=""; held=0
release() {
  [[ -n $nap_pid ]] && kill "$nap_pid" 2>/dev/null
  rm -f "$QUEUE"
  # only this run's lock: a lock taken over after it went stale belongs to someone else now
  (( held )) && [[ "$(owner_pid "$LOCK")" == "$$" ]] && rm -rf "$LOCK"
}
stop_job() {   # TERM the job's group, wait up to GRACE s, then KILL
  alive "$job" || group_alive "$job" || return 0
  kill -TERM -- "-$job" 2>/dev/null
  local t=0
  while group_alive "$job" && (( t < GRACE * 2 )); do sleep 0.5; t=$((t + 1)); done
  if group_alive "$job"; then echo "[heavy:$label] job group $job ignored TERM for ${GRACE}s — KILL" >&2; kill -KILL -- "-$job" 2>/dev/null; fi
  wait "$job" 2>/dev/null
}
on_stop() {
  echo "[heavy:$label] stopped (signal): stopping the job${job:+ (group $job)} before the lock goes" >&2
  stop_job; exit 143
}
nap() { sleep "$1" & nap_pid=$!; wait "$nap_pid" 2>/dev/null; nap_pid=""; }   # a signal ends the wait at once
trap release EXIT
trap on_stop INT TERM HUP

mkdir -p "$LOCK.queue" && printf '%s\n%s\n' "$(date '+%F %T')" "$cmd" > "$QUEUE"
PAUSE=${HEAVY_PAUSE:-$HOME/.claude/heavy-job.pause}
while [[ -e $PAUSE ]]; do echo "[heavy:$label] PAUSED ($(cat "$PAUSE")) — remove $PAUSE to resume"; nap 60; done
while ! mkdir "$LOCK" 2>/dev/null; do
  if lock_stale "$LOCK"; then
    # ONE waiter takes a stale lock: the takeover mutex, a re-check under it, then mv (atomic) and delete
    if mkdir "$LOCK.takeover" 2>/dev/null; then
      if lock_stale "$LOCK"; then
        gone=$(head -1 "$LOCK/owner" 2>/dev/null)
        if mv "$LOCK" "$LOCK.stale.$$" 2>/dev/null; then
          rm -rf "$LOCK.stale.$$"
          echo "[heavy:$label] stale lock taken over (holder and its job are gone: ${gone:-no owner file})"
        fi
      fi
      rmdir "$LOCK.takeover" 2>/dev/null
      continue
    elif (( $(date +%s) - $(mtime "$LOCK.takeover") > 60 )); then
      rmdir "$LOCK.takeover" 2>/dev/null   # a waiter died holding the takeover mutex (held for milliseconds)
      continue
    fi
  fi
  holder=$(head -1 "$LOCK/owner" 2>/dev/null)
  if [[ -n $holder ]] && ! alive "${holder%% *}"; then
    echo "[heavy:$label] waiting: wrapper of '$holder' is gone but its job (group $(owner_field "$LOCK" pgid)) still runs — not taking the lock"
  else
    echo "[heavy:$label] waiting for lock held by: $holder"
  fi
  nap "$WAIT_POLL"
done
held=1
rm -f "$QUEUE"
write_owner() {
  printf '%s\npgid: %s\ncmd: %s\ncwd: %s\nlog: %s\nstarted: %s\n' "$$ $label $(date '+%F %T')" "${job}" "$cmd" "$PWD" \
    "$log" "$started" > "$LOCK/owner.tmp" && mv "$LOCK/owner.tmp" "$LOCK/owner"
}
started=$(date +%s); write_owner
export HEAVY_HELD=$$        # a nested heavy.sh in the command runs under this lock (re-entrant, above)

while :; do
  f=$(free_pct)
  if [[ -z $f ]]; then echo "[heavy:$label] free memory UNREADABLE (memory_pressure -Q) — waiting, not starting blind"
  elif (( f < need )); then echo "[heavy:$label] free $f% < $need% — waiting"
  else break; fi
  nap "$WAIT_POLL"
done
echo "[heavy:$label] start (free $f%, lane ${LOCK##*/}, footprint limit ${MAX_FP_GB} GB): $*"

set -m                      # job control: the command gets its own process group (pgid = its pid)
"$@" &
job=$!
set +m
write_owner
unread=0; last_fp=-$FP_EVERY; last_beat=$SECONDS; fp=""
while alive "$job"; do
  f=$(free_pct)
  if [[ -z $f ]]; then
    unread=$((unread + 1))
    echo "[heavy:$label] WARNING: free memory unreadable ($unread/$UNREADABLE_MAX in a row) — the guard is blind" >&2
    if (( unread >= UNREADABLE_MAX )); then
      echo "[heavy:$label] STOPPED: free memory unreadable for $unread samples (memory_pressure -Q); the job cannot run unguarded" >&2
      stop_job; exit 74
    fi
  else
    unread=0
    if (( f < KILL_PCT )); then
      echo "[heavy:$label] KILLED: system free memory ${f}% < ${KILL_PCT}%" >&2
      stop_job; exit 137
    fi
  fi
  if (( SECONDS - last_fp >= FP_EVERY )); then
    last_fp=$SECONDS
    fp=$(group_footprint_gb "$job")
    if [[ -n $fp ]] && (( MAX_FP_GB > 0 && fp > MAX_FP_GB )); then
      echo "[heavy:$label] KILLED: the job's physical footprint ${fp} GB > ${MAX_FP_GB} GB (HEAVY_MAX_FOOTPRINT_GB)" >&2
      stop_job; exit 137
    fi
  fi
  if (( SECONDS - last_beat >= BEAT )); then
    last_beat=$SECONDS
    beat="$(date '+%F %T') running $(elapsed "$started"), free ${f:-?}%, footprint ${fp:-?} GB"
    echo "[heavy:$label] $beat"; echo "$beat" > "$LOCK/beat"
  fi
  nap "$POLL"
done
wait "$job"; rc=$?
echo "[heavy:$label] done rc=$rc in $(elapsed "$started") (free $(free_pct)%)"
exit $rc
