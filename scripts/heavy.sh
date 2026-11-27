#!/bin/bash
# heavy.sh — run ONE memory-heavy job at a time per lane, with a memory guard.
#
#   heavy.sh [--again] <label> <min_free_pct_to_start> -- <command...>
#   heavy.sh --status                 what holds each lane: holder, command, elapsed, free memory, last log lines
#
# - A lock per lane (mkdir is atomic). Three lanes exist, nothing else is accepted (owner 2026-11-09: max 2; owner
#   2026-11-14, five iOS simulator lanes behind one lock: "Maybe we can run 2 at same time might have enough
#   resources" -> a third lane, for a second simulator job): ~/.claude/heavy-job.lock (the default),
#   ~/.claude/heavy-job-2.lock and ~/.claude/heavy-job-3.lock (HEAVY_LOCK=…/heavy-job-<n>.lock).
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
# - (T788 #11) FREE DISK, beside the memory gate: the disk filled twice (2026-11-15: 1.3 GB free, $TMPDIR held 151 GB of
#   test leftovers; 2026-11-20: 190 MB free, 82 GB of merged lane worktrees) and every shell then refused to start. The
#   job starts only with HEAVY_MIN_DISK_GB (20) free on BOTH the volume it runs in ($PWD) and $TMPDIR's, else it is
#   refused at once (exit 73: waiting frees nothing), naming where to look. While it runs, below HEAVY_KILL_DISK_GB (3)
#   it is stopped (exit 73): stopping it frees its temp space before the disk fills. An unreadable `df` refuses the
#   start and warns while running (the memory guard still holds).
# - (T1225) FIRST COME, FIRST SERVED per lane. Every waiter has a queue entry `<lock>.queue/<pid>` (line 1 the date it
#   queued, line 2 the job, as before; line 3 `since: <epoch>`, new). A waiter tries the lock only when it is FIRST IN
#   LINE: the oldest LIVE entry (a tie: the lower pid). An entry whose pid is gone, stopped, a zombie, or reused (the
#   process started after the entry was written) is skipped, never waited for. The first in line polls every
#   HEAVY_HEAD_POLL s (2), the others every HEAVY_WAIT_POLL. The memory gate is unchanged: it is taken WITH the lock, so
#   a first-in-line job waiting for free memory holds the lane and the jobs behind it wait, smaller ones too (a strict
#   line; nothing overtakes). A heavy.sh from before T1225 still takes the lock whenever its own poll finds it free (it
#   cannot be made to wait its turn); it is shown as `older heavy.sh` in --status, and when one is first in line the
#   others wait for it. --status lists each lane's line in order (label, wait), says when a holder is still waiting
#   for free memory (the owner file's `gate:` line, its start gate; no job group yet), and marks a holder past
#   HEAVY_LONG_HOLD s (3600) as a long holder: a server that never ends does not belong on a heavy lock.
# Exit codes: the job's own; 137 killed for memory (free % or footprint); 74 memory unreadable; 73 disk short; 143
# stopped (INT/TERM); 2 usage / unknown lane; 3 duplicate.
set -u
LANES_DIR=${HEAVY_LANES_DIR:-$HOME/.claude}      # tests point this at a scratch dir; nothing else should
LANES=("$LANES_DIR/heavy-job.lock" "$LANES_DIR/heavy-job-2.lock" "$LANES_DIR/heavy-job-3.lock")
KILL_PCT=${KILL_PCT:-6}
POLL=${HEAVY_POLL:-5}                  # seconds between memory samples while the job runs
WAIT_POLL=${HEAVY_WAIT_POLL:-20}       # seconds between lock / start-gate checks
FP_EVERY=${HEAVY_FOOTPRINT_EVERY:-30}  # seconds between footprint samples
BEAT=${HEAVY_HEARTBEAT:-300}
GRACE=${HEAVY_STOP_GRACE:-10}
UNREADABLE_MAX=${HEAVY_UNREADABLE_MAX:-12}
MAX_FP_GB=${HEAVY_MAX_FOOTPRINT_GB:-$(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1073741824 ))}
MIN_DISK_GB=${HEAVY_MIN_DISK_GB:-20}
KILL_DISK_GB=${HEAVY_KILL_DISK_GB:-3}
DISK_EVERY=${HEAVY_DISK_EVERY:-30}    # seconds between free-disk samples while the job runs
# (T1225) the first in line polls faster (never slower than WAIT_POLL), and says it waits once per WAIT_POLL
HEAD_POLL=${HEAVY_HEAD_POLL:-$(awk -v w="$WAIT_POLL" 'BEGIN { print (w + 0 < 2) ? w : 2 }')}
SAY_EVERY=$(awk -v w="$WAIT_POLL" -v h="$HEAD_POLL" 'BEGIN { n = (h + 0 > 0) ? int(w / h + 0.999) : 1; print (n < 1) ? 1 : n }')
LONG_HOLD=${HEAVY_LONG_HOLD:-3600}    # (T1225) --status marks a holder past this many seconds as a long holder

usage() { echo "usage: heavy.sh [--again] <label> <min_free_pct> -- <command...>  |  heavy.sh --status" >&2; exit 2; }

# free memory %, or NOTHING when the reading is not an integer 0-100 (memory_pressure failed or changed its wording)
free_pct() {
  local v
  v=$(memory_pressure -Q 2>/dev/null | awk -F': ' '/free percentage/ {gsub("%","",$2); print $2; exit}')
  [[ $v =~ ^[0-9]+$ ]] && (( 10#$v <= 100 )) && echo $((10#$v))
}
# (T788 #11) free disk, whole GB (rounded down), of the fuller of the volumes holding $PWD and $TMPDIR, as
# "<GB> <mount>"; NOTHING when df fails or prints no number (unreadable, never 0)
free_disk() {
  local out
  out=$(df -Pk "$PWD" "${TMPDIR:-/tmp}" 2>/dev/null) || return 0
  awk 'NR > 1 && $4 ~ /^[0-9]+$/ {g = int($4 / 1048576); if (n == 0 || g < min) {min = g; m = $6}; n++}
       END {if (n) print min, m}' <<<"$out"
}
disk_hint() {
  echo "free space: \`du -k -d 1 \$TMPDIR | sort -n | tail\` (old release-open-*, regs-web-*, tiles-lowband-* are" \
       "test and build leftovers), and merged, idle lane worktrees under .claude/worktrees (\`git worktree remove\`)"
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
# (T1225) a lane's LIVE waiters in the order they are served, one line each: "<pid> <since epoch> <new|old> <job>".
# Order: the entry's `since:` epoch (an older heavy.sh writes none: its line-1 date, to the second), a tie by the lower
# pid. Live: the pid runs, is not stopped (T) or a zombie (Z), and started no later than its entry was written (a
# reused pid started after it). One ps call for the whole line. A dead entry is skipped, never waited for.
queue_list() {
  local q pid d c s since kind pids="" rows="" now live
  for q in "$1.queue"/*; do
    [[ -f $q ]] || continue
    pid=${q##*/}; [[ $pid =~ ^[0-9]{1,6}$ ]] || continue
    d="" c="" s=""
    { IFS= read -r d; IFS= read -r c; IFS= read -r s; } < "$q" 2>/dev/null
    if [[ $s == "since: "* ]]; then since=${s#since: }; kind=new
    else since=$(date -j -f '%Y-%m-%d %H:%M:%S' "$d" +%s 2>/dev/null); kind=old; fi
    [[ $since =~ ^[0-9]+$ ]] || since=$(mtime "$q")
    rows+="R $pid $since $kind $c"$'\n'; pids+=${pids:+,}$pid
  done
  [[ -n $pids ]] || return 0
  # the clock is read right BEFORE ps: a stall between them makes a start look earlier (kept), never later (dropped)
  now=$(date +%s); live=$(ps -o pid=,stat=,etime= -p "$pids" 2>/dev/null)
  { awk 'NF { print "P", $0 }' <<<"$live"; printf '%s' "$rows"; } \
    | awk -v now="$now" '
        function secs(e,   d, n, p, i, t) {   # ps etime: [[dd-]hh:]mm:ss
          d = 0; if (i = index(e, "-")) { d = substr(e, 1, i - 1); e = substr(e, i + 1) }
          n = split(e, p, ":"); t = 0; for (i = 1; i <= n; i++) t = t * 60 + p[i]; return d * 86400 + t }
        $1 == "P" { if ($3 !~ /^[TZ]/) start[$2] = now - secs($4); next }
        $1 == "R" && ($2 in start) && start[$2] <= $3 + 2 { print substr($0, 3) }' \
    | sort -n -k2,2 -k1,1
}

status() {
  local lock pid pgid started log q f d n qpid since kind job gate
  f=$(free_pct)
  d=$(free_disk)
  echo "free memory: ${f:-UNREADABLE (memory_pressure -Q)}%   free disk: $(if [[ -n $d ]]; then echo "${d%% *} GB on ${d#* }"; else echo "UNREADABLE (df)"; fi)   pause file: $([[ -e ${HEAVY_PAUSE:-$HOME/.claude/heavy-job.pause} ]] && echo PRESENT || echo none)"
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
      # (T1225) the memory gate is taken after the lock: a holder with no job group yet is waiting for free memory
      gate=$(owner_field "$lock" gate)
      if [[ -z $pgid && -n $gate ]] && alive "$pid"; then
        echo "   NOT STARTED: waiting for free memory >= $gate % (now ${f:-UNREADABLE} %)"
      fi
      # (T1225) a guard for a holder that never ends (2026-11-26: `expo start` held a lane 20 minutes, 11 jobs behind)
      if [[ -n $started ]] && (( $(date +%s) - started > LONG_HOLD )); then
        echo "   LONG HOLDER: held $(elapsed "$started"), past $(printf '%dh%02dm' $(( LONG_HOLD / 3600 )) $(( LONG_HOLD % 3600 / 60 ))) (HEAVY_LONG_HOLD); a server" \
             "that never ends does not belong on a heavy lock"
      fi
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
    for q in "$lock.queue"/*; do   # a dead waiter's entry goes (as before T1225)
      [[ -f $q ]] && ! alive "${q##*/}" && rm -f "$q"
    done
    # (T1225) the line, in the order it is served: place, pid, label, how long it has waited
    n=0
    while IFS=' ' read -r qpid since kind job; do
      [[ -n $qpid ]] || continue
      n=$((n + 1))
      echo "   queued $n: $qpid ${job%% :: *}  waiting $(elapsed "$since") (since $(date -r "$since" '+%F %T'))$(
        [[ $kind == old ]] && echo "  [older heavy.sh: takes the lock out of turn]")"
    done <<<"$(queue_list "$lock")"
  done
}

[[ ${1:-} == --status ]] && { status; exit 0; }
again=0
[[ ${1:-} == --again ]] && { again=1; shift; }
(( $# >= 3 )) || usage
label=$1; need=$2; shift 2; [[ $1 == -- ]] && shift
(( $# )) || usage
[[ $need =~ ^[0-9]+$ ]] && (( need <= 100 )) || { echo "[heavy:$label] min free % must be 0-100, got '$need'" >&2; exit 2; }
# (owner 2026-11-21: "We should use lower memory threshold") the start gate is capped: a lane asked for 90 % free (it meant
# minutes) and waited an hour at 80 %, which this Mac rarely passes. The kill guards below (KILL_PCT, the footprint cap)
# are what keep the machine up once a job runs; the start gate only spaces jobs out. HEAVY_MAX_GATE overrides the cap.
MAX_GATE=${HEAVY_MAX_GATE:-30}
if (( need > MAX_GATE )); then echo "[heavy:$label] start gate $need % lowered to $MAX_GATE % (the cap; HEAVY_MAX_GATE)" >&2; need=$MAX_GATE; fi
LOCK=${HEAVY_LOCK:-${LANES[0]}}
known=0; for l in "${LANES[@]}"; do [[ $LOCK == "$l" ]] && known=1; done
if (( ! known )); then
  echo "[heavy:$label] unknown lane $LOCK — only ${LANES[*]} (max ${#LANES[@]} heavy lanes)" >&2; exit 2
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
# (T788 #11) FREE DISK, also before the lock (after the script's own precheck, so its words come first): refused at
# once, not waited for (nothing frees itself)
[[ $MIN_DISK_GB =~ ^[0-9]+$ && $KILL_DISK_GB =~ ^[0-9]+$ ]] || {
  echo "[heavy:$label] HEAVY_MIN_DISK_GB / HEAVY_KILL_DISK_GB must be whole GB, got '$MIN_DISK_GB' / '$KILL_DISK_GB'" >&2; exit 2; }
disk=$(free_disk)
if [[ -z $disk ]]; then
  echo "[heavy:$label] refused: free disk UNREADABLE (df -Pk $PWD ${TMPDIR:-/tmp}) — not starting blind" >&2; exit 73
elif (( ${disk%% *} < MIN_DISK_GB )); then
  echo "[heavy:$label] refused: ${disk%% *} GB free on ${disk#* } < $MIN_DISK_GB GB (HEAVY_MIN_DISK_GB); $(disk_hint)" >&2; exit 73
fi

cmd="$label :: $PWD :: $(printf '%q ' "$@")"
# DUPLICATE (chat 9: two identical quiet background jobs each ran whole): the same label + command FROM THE SAME
# DIRECTORY holding or waiting on any lane is refused unless --again. (T995, 2026-11-22) The directory is part of the
# job: two worktrees' pre-push hooks run the same command on different trees, and the second was refused.
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
  rm -f "$QUEUE" "$LOCK.queue/.$$.tmp"
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

# (T1225) the entry is written whole (a dot file, then mv: a reader never sees half of it); lines 1-2 are what an older
# heavy.sh reads (its --status the date, its duplicate check the job), line 3 the order this one serves by
queued_at=$(date +%s)
mkdir -p "$LOCK.queue" && printf '%s\n%s\nsince: %s\n' "$(date -r "$queued_at" '+%F %T')" "$cmd" "$queued_at" \
  > "$LOCK.queue/.$$.tmp" && mv "$LOCK.queue/.$$.tmp" "$QUEUE"
PAUSE=${HEAVY_PAUSE:-$HOME/.claude/heavy-job.pause}
while [[ -e $PAUSE ]]; do echo "[heavy:$label] PAUSED ($(cat "$PAUSE")) — remove $PAUSE to resume"; nap 60; done
# (T1225) FIRST COME, FIRST SERVED: only the first in line (queue_list) tries the lock; a stale lock is cleared by any
# waiter (under the takeover mutex, as before), then taken by the first in line. A run that cannot see its own entry in
# the line (ps unreadable) says so and tries the lock as a heavy.sh before T1225 did: in no order, never stuck.
said="" loops=0 blind=0
while :; do
  line=$(queue_list "$LOCK")
  first=${line%%$'\n'*}; first_pid=${first%% *}
  place=$(awk -v me="$$" '$1 == me { print NR; exit }' <<<"$line"); in_line=$(grep -c . <<<"$line")
  if [[ -z $place ]]; then
    (( blind )) || echo "[heavy:$label] WARNING: this run is not in the line it reads ($LOCK.queue, ps -o etime): trying" \
                       "the lock in no order, as before T1225" >&2
    blind=1
    mkdir "$LOCK" 2>/dev/null && break
  elif [[ $first_pid == "$$" ]]; then
    mkdir "$LOCK" 2>/dev/null && break
  fi
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
  at=${place:+ (in line: $place of $in_line)}
  holder=$(head -1 "$LOCK/owner" 2>/dev/null)
  if [[ ! -d $LOCK ]]; then
    ahead=$(awk -v me="$$" '$1 == me { exit } { n++; if (n == 1) { split($0, f, " :: "); sub(/^[0-9]+ [0-9]+ [a-z]+ /, "", f[1]); w = $1 " (" f[1] ")"; o = $3 } }
                            END { print w (o == "old" ? ", an older heavy.sh: it takes the lock on its own poll" : "") }' <<<"$line")
    msg="waiting for my turn: the lock is free, first in line is pid $ahead$at"
  elif [[ -n $holder ]] && ! alive "${holder%% *}"; then
    msg="waiting: wrapper of '$holder' is gone but its job (group $(owner_field "$LOCK" pgid)) still runs — not taking the lock$at"
  else
    msg="waiting for lock held by: $holder$at"
  fi
  if [[ $first_pid == "$$" ]]; then
    # the first in line polls every HEAD_POLL s and says so when it changes, or once per WAIT_POLL
    if [[ $msg != "$said" ]] || (( loops % SAY_EVERY == 0 )); then echo "[heavy:$label] $msg"; said=$msg; fi
    loops=$((loops + 1)); nap "$HEAD_POLL"
  else
    echo "[heavy:$label] $msg"; said=$msg; loops=0; nap "$WAIT_POLL"
  fi
done
held=1
rm -f "$QUEUE"
write_owner() {
  # (T1225) `gate:` is the start gate's free %, for --status; an older heavy.sh and pipeline/release.py read line 1
  # and keyed lines only, so a new key is invisible to them
  printf '%s\npgid: %s\ncmd: %s\ncwd: %s\nlog: %s\nstarted: %s\ngate: %s\n' "$$ $label $(date '+%F %T')" "${job}" "$cmd" \
    "$PWD" "$log" "$started" "$need" > "$LOCK/owner.tmp" && mv "$LOCK/owner.tmp" "$LOCK/owner"
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
# (T1161) the Mac stays awake while the job runs (no idle sleep; none on power either); caffeinate ends with the job
# (-w <pid>). macOS only: Linux has no caffeinate and no such sleep to hold off, so nothing to do there. Locking,
# queueing, the memory gates and the exit code are untouched. It is not part of the job's process group.
if [[ $(uname -s) == Darwin ]]; then
  if command -v caffeinate >/dev/null 2>&1; then
    caffeinate -i -s -w "$job" >/dev/null 2>&1 &
    echo "[heavy:$label] keeping the Mac awake while the job runs (caffeinate -i -s -w $job); keep the lid open, on power"
  else
    echo "[heavy:$label] WARNING: caffeinate not found: the Mac may sleep during this job" >&2
  fi
fi
write_owner
unread=0; last_fp=-$FP_EVERY; last_disk=-$DISK_EVERY; last_beat=$SECONDS; fp=""
while alive "$job"; do
  if (( SECONDS - last_disk >= DISK_EVERY )); then
    last_disk=$SECONDS
    disk=$(free_disk)
    if [[ -z $disk ]]; then
      echo "[heavy:$label] WARNING: free disk unreadable (df) — the disk guard is blind this sample" >&2
    elif (( ${disk%% *} < KILL_DISK_GB )); then
      echo "[heavy:$label] STOPPED: ${disk%% *} GB free on ${disk#* } < $KILL_DISK_GB GB (HEAVY_KILL_DISK_GB): stopping the" \
           "job frees its temp space before the disk fills; $(disk_hint)" >&2
      stop_job; exit 73
    fi
  fi
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
