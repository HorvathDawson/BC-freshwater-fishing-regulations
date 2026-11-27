#!/usr/bin/env bash
# Shell test for scripts/heavy.sh (T536, T638): fake jobs, a stubbed `memory_pressure` and `footprint`, scratch lanes.
# No real build runs and the live lanes (~/.claude/heavy-job*.lock) are never touched: every call gets
# HEAVY_LANES_DIR=<scratch>. Run by scripts/fast-fail.sh when heavy.sh or this file changes (~1 min).
# (T1225) The first-come tests (13-17) also run the heavy.sh from before T1225 (HEAVY_OLD, else `git show
# 118d8e4f8:scripts/heavy.sh`) beside this one, on the same scratch lanes, for the mixed-version cases.
#
#   bash scripts/tests/heavy.test.sh [path/to/heavy.sh]
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
HEAVY=$(cd "$(dirname "${1:-$ROOT/scripts/heavy.sh}")" && pwd)/$(basename "${1:-$ROOT/scripts/heavy.sh}")
T=$(mktemp -d "${TMPDIR:-/tmp}/heavy-test.XXXXXX")
mkdir -p "$T/bin"
fails=0; t0=$SECONDS
cleanup() {   # only what this test started: the job groups whose pids it recorded
  local p
  for p in "$T"/*.pid; do [[ -f $p ]] && kill -KILL -- "-$(cat "$p")" 2>/dev/null; done
  rm -rf "$T"
}
trap cleanup EXIT

cat > "$T/bin/memory_pressure" <<EOF
#!/bin/bash
echo "The system has 51539607552 (3145728 pages with a page size of 16384)."
echo "System-wide memory free percentage: \$(cat "$T/free")%"
EOF
cat > "$T/bin/footprint" <<EOF
#!/bin/bash
echo "Auxiliary data:"
echo "    phys_footprint: \$(cat "$T/fp") B"
EOF
# (T788 #11) df -Pk <cwd> <tmpdir>: the first volume has $T/disk KB available, the second $T/disk_tmp (else the same);
# an empty $T/disk is a df that fails
cat > "$T/bin/df" <<EOF
#!/bin/bash
a=\$(cat "$T/disk"); b=\$(cat "$T/disk_tmp" 2>/dev/null || echo "\$a")
[[ -n \$a ]] || { echo "df: stub: no such volume" >&2; exit 1; }
echo "Filesystem 1024-blocks Used Available Capacity Mounted on"
echo "/dev/stub1 999999999 1 \$a 1% /System/Volumes/Data"
echo "/dev/stub2 999999999 1 \$b 1% /private/tmp-vol"
EOF
# (T1161) a fake `caffeinate`: records its arguments, holds nothing (the real one would hold this Mac awake)
cat > "$T/bin/caffeinate" <<EOF
#!/bin/bash
echo "\$*" >> "$T/caffeinate.args"
EOF
# job.sh <name> [stubborn]: records its pid (= its process group), logs start/end, holds a child sleeping
cat > "$T/job.sh" <<EOF
#!/bin/bash
echo \$\$ > "$T/\$1.pid"
echo "start \$1" >> "$T/events"
echo "hello-log \$1"
if [[ \${2:-} == stubborn ]]; then trap '' TERM; while :; do sleep 0.2; done; fi
sleep "\${HOLD:-30}" & wait
echo "end \$1" >> "$T/events"
EOF
chmod +x "$T/bin/"* "$T/job.sh"
echo 80 > "$T/free"; echo 1000 > "$T/fp"; echo $((100 * 1048576)) > "$T/disk"

# $T/H = heavy.sh with the scratch lanes and fast timings. A script that EXECs (a function would run in a subshell
# when backgrounded), so `"$T/H" … &` puts heavy.sh's own pid in $! and a signal to it reaches the wrapper.
cat > "$T/H" <<EOF
#!/bin/bash
exec env -u HEAVY_HELD PATH="$T/bin:\$PATH" HEAVY_LANES_DIR="$T" HEAVY_LOCK="\${LANE:-$T/heavy-job.lock}" \\
  HEAVY_PAUSE="$T/no-pause" HEAVY_POLL=0.2 HEAVY_WAIT_POLL=0.2 HEAVY_FOOTPRINT_EVERY=0 HEAVY_DISK_EVERY=0 HEAVY_STOP_GRACE=1 \\
  HEAVY_UNREADABLE_MAX=3 bash "$HEAVY" "\$@"
EOF
chmod +x "$T/H"
H() { "$T/H" "$@"; }
# (T1225) $T/O = the heavy.sh from before T1225, run the same way: old and new copies run side by side for days
if [[ -n ${HEAVY_OLD:-} ]]; then cp "$HEAVY_OLD" "$T/old-heavy.sh"
else git -C "$ROOT" show 118d8e4f8:scripts/heavy.sh > "$T/old-heavy.sh" \
  || { echo "heavy.test: FAIL cannot read the old heavy.sh (git show 118d8e4f8:scripts/heavy.sh; HEAVY_OLD=<path>)"; exit 1; }
fi
sed "s|bash \"$HEAVY\"|bash \"$T/old-heavy.sh\"|" "$T/H" > "$T/O" && chmod +x "$T/O"
grep -q old-heavy.sh "$T/O" || { echo "heavy.test: FAIL could not make the old-script wrapper $T/O"; exit 1; }
# hold.sh <name>: holds the lock until $T/go.<name> exists; logs start and end
cat > "$T/hold.sh" <<EOF
#!/bin/bash
echo \$\$ > "$T/\$1.pid"
echo "start \$1" >> "$T/events"
until [[ -e "$T/go.\$1" ]]; do sleep 0.05; done
echo "end \$1" >> "$T/events"
EOF
chmod +x "$T/hold.sh"
until_() { local i=0; until eval "$1"; do sleep 0.1; i=$((i + 1)); (( i > ${2:-100} )) && return 1; done; }
ok() { echo "  ok   $1"; }
bad() { echo "  FAIL $1"; fails=$((fails + 1)); }
check() { local name=$1; shift; if "$@"; then ok "$name"; else bad "$name"; fi; }
alive() { kill -0 "$1" 2>/dev/null; }
reset() { rm -rf "$T"/heavy-job*.lock* "$T"/*.pid "$T/events" "$T"/*.out "$T"/go.* "$T/disk_tmp" "$T/caffeinate.args"; echo 80 > "$T/free"; echo 1000 > "$T/fp"
          echo $((100 * 1048576)) > "$T/disk"; }
L=$T/heavy-job.lock

echo "heavy.sh test: $HEAVY"

echo "0. (T1161) the Mac is kept awake while the job runs: caffeinate -i -s -w <the job's pid>; the job's exit code stays"
reset
if [[ $(uname -s) != Darwin ]]; then
  echo "  skip (printed): not macOS; heavy.sh holds nothing awake there"
else
  cat > "$T/rc7.sh" <<EOF
#!/bin/bash
echo \$\$ > "$T/rc7.pid"; sleep 1; exit 7
EOF
  chmod +x "$T/rc7.sh"
  "$T/H" awake 10 -- "$T/rc7.sh" > "$T/aw.out" 2>&1; rc=$?
  j=$(cat "$T/rc7.pid")
  check "caffeinate gets -i -s -w <job pid>" [ "$(cat "$T/caffeinate.args")" = "-i -s -w $j" ]
  check "says so in the output" grep -q 'keeping the Mac awake while the job runs' "$T/aw.out"
  check "the job's exit code is unchanged (7)" [ $rc = 7 ]
fi

echo "1. stop mid-job (TERM to the wrapper): job gone, lock gone"
reset
"$T/H" stop 10 -- "$T/job.sh" a > "$T/a.out" 2>&1 & w=$!
until_ "[[ -f $T/a.pid ]]"; j=$(cat "$T/a.pid")
kill -TERM $w; wait $w; rc=$?
check "wrapper exits 143" [ $rc = 143 ]
check "job process gone" bash -c "! kill -0 $j 2>/dev/null"
check "job's child sleep gone" bash -c "! pgrep -g $j >/dev/null"
check "lock gone" [ ! -d "$L" ]

echo "2. a job ignoring TERM is KILLed after the grace"
reset
"$T/H" stubborn 10 -- "$T/job.sh" b stubborn > "$T/b.out" 2>&1 & w=$!
until_ "[[ -f $T/b.pid ]]"; j=$(cat "$T/b.pid")
kill -TERM $w; wait $w
check "stubborn job gone" bash -c "! kill -0 $j 2>/dev/null"
check "says KILL" grep -q 'ignored TERM' "$T/b.out"
check "lock gone" [ ! -d "$L" ]

echo "3. SIGKILLed wrapper: the lock is not taken while its job lives"
reset
"$T/H" killed 10 -- "$T/job.sh" c > "$T/c.out" 2>&1 & w=$!
until_ "[[ -f $T/c.pid ]]"; j=$(cat "$T/c.pid")
check "owner file records the job's group" grep -q "^pgid: $j$" "$L/owner"
kill -KILL $w; wait $w 2>/dev/null
"$T/H" next 10 -- "$T/job.sh" d > "$T/d.out" 2>&1 & w2=$!
until_ "grep -q 'still runs' $T/d.out" 30
check "waiter says the job still runs" grep -q 'still runs — not taking the lock' "$T/d.out"
check "waiter did not start its job" [ ! -f "$T/d.pid" ]
kill -TERM -- "-$j"
until_ "[[ -f $T/d.pid ]]" 50
check "lock taken after the orphan job ended" [ -f "$T/d.pid" ]
kill -TERM $w2; wait $w2

echo "4. unreadable memory at the start gate waits (never starts blind), then starts"
reset; echo "" > "$T/free"
"$T/H" blind 10 -- "$T/job.sh" e > "$T/e.out" 2>&1 & w=$!
until_ "grep -q UNREADABLE $T/e.out" 30
check "says unreadable" grep -q 'free memory UNREADABLE' "$T/e.out"
check "job not started" [ ! -f "$T/e.pid" ]
echo 150 > "$T/free"; sleep 0.5
check "150 % is unreadable too" [ ! -f "$T/e.pid" ]
echo 80 > "$T/free"
until_ "[[ -f $T/e.pid ]]" 30
check "starts once readable" [ -f "$T/e.pid" ]
kill -TERM $w; wait $w

echo "5. unreadable mid-run: warns, stops after N samples with exit 74"
reset
"$T/H" midblind 10 -- "$T/job.sh" f > "$T/f.out" 2>&1 & w=$!
until_ "[[ -f $T/f.pid ]]"; j=$(cat "$T/f.pid")
echo "" > "$T/free"
wait $w; rc=$?
check "exit 74" [ $rc = 74 ]
check "warned first" grep -q 'unreadable (1/3 in a row)' "$T/f.out"
check "own message" grep -q 'STOPPED: free memory unreadable for 3 samples' "$T/f.out"
check "not reported as low memory" bash -c "! grep -q 'free memory 0%' '$T/f.out'"
check "job gone" bash -c "! kill -0 $j 2>/dev/null"
check "lock gone" [ ! -d "$L" ]

echo "6. low free memory kills (exit 137)"
reset
"$T/H" low 10 -- "$T/job.sh" g > "$T/g.out" 2>&1 & w=$!
until_ "[[ -f $T/g.pid ]]"; echo 3 > "$T/free"; wait $w; rc=$?
check "exit 137 + message" bash -c "[ $rc = 137 ] && grep -q 'KILLED: system free memory 3%' '$T/g.out'"

echo "7. physical footprint over the limit kills (T638; free % still reads 80)"
reset
HEAVY_MAX_FOOTPRINT_GB=1 "$T/H" fat 10 -- "$T/job.sh" h > "$T/h.out" 2>&1 & w=$!
until_ "[[ -f $T/h.pid ]]"; j=$(cat "$T/h.pid"); echo $((3 * 1073741824)) > "$T/fp"; wait $w; rc=$?
check "exit 137 + footprint message" bash -c "[ $rc = 137 ] && grep -q 'physical footprint 3 GB > 1 GB' '$T/h.out'"
check "job gone" bash -c "! kill -0 $j 2>/dev/null"

echo "8. duplicates: same label + command holding or queued is refused; --again queues"
reset
"$T/H" dup 10 -- "$T/job.sh" i > "$T/i.out" 2>&1 & w=$!
until_ "[[ -f $T/i.pid ]]"
H dup 10 -- "$T/job.sh" i > "$T/i2.out" 2>&1; rc=$?
check "holding: refused, exit 3" bash -c "[ $rc = 3 ] && grep -q 'already holds' '$T/i2.out'"
LANE=$T/heavy-job-2.lock H dup 10 -- "$T/job.sh" i > "$T/i3.out" 2>&1; rc=$?
check "refused on the other lane too" [ $rc = 3 ]
mkdir -p "$T/wt2"
(cd "$T/wt2" && exec env LANE="$T/heavy-job-2.lock" "$T/H" dup 10 -- "$T/job.sh" i > "$T/i5.out" 2>&1) & w6=$!
sleep 1
check "(T995) the same command from another checkout is its own job" bash -c "! grep -q refused '$T/i5.out'"
kill -TERM $w6 2>/dev/null; wait $w6 2>/dev/null
"$T/H" --again dup 10 -- "$T/job.sh" i > "$T/i4.out" 2>&1 & w4=$!
until_ "grep -q 'waiting for lock' $T/i4.out" 30
check "--again waits instead" grep -q 'waiting for lock' "$T/i4.out"
"$T/H" other 10 -- "$T/job.sh" k > "$T/k.out" 2>&1 & w5=$!
until_ "grep -q 'waiting for lock' $T/k.out" 30
H other 10 -- "$T/job.sh" k > "$T/k2.out" 2>&1; rc=$?
check "queued: refused, exit 3" bash -c "[ $rc = 3 ] && grep -q 'already waiting' '$T/k2.out'"
kill -TERM $w4 $w5 $w; wait $w4 $w5 $w 2>/dev/null
check "a stopped waiter leaves no queue entry" bash -c "! ls '$L.queue'/* >/dev/null 2>&1"

echo "9. a stale lock is taken over ONCE by two waiters"
reset
dead=$(bash -c 'echo $$'); mkdir "$L"; echo "$dead old 2026-01-01 00:00:00" > "$L/owner"
HOLD=0.5 "$T/H" w1 10 -- "$T/job.sh" w1 > "$T/w1.out" 2>&1 & a=$!
HOLD=0.5 "$T/H" w2 10 -- "$T/job.sh" w2 > "$T/w2.out" 2>&1 & b=$!
wait $a $b
n=$(cat "$T/w1.out" "$T/w2.out" | grep -c 'stale lock taken over')
check "taken over once (got $n)" [ "$n" = 1 ]
check "jobs never overlapped" bash -c "[[ \$(awk '{print \$1}' '$T/events' | paste -sd' ' -) == 'start end start end' ]]"
check "lock gone" [ ! -d "$L" ]

echo "9b. the third lane is a lane (owner 2026-11-14: a second simulator job at once)"
reset
LANE=$T/heavy-job-3.lock H third 10 -- true > "$T/t3.out" 2>&1; rc=$?
check "third lane runs a job" [ $rc = 0 ]
check "third lane left no lock" [ ! -d "$T/heavy-job-3.lock" ]

echo "10. unknown lane refused; nested call runs under the holder's lock"
reset
LANE=$T/other.lock H x 10 -- true > "$T/x.out" 2>&1; rc=$?
check "unknown lane exit 2" bash -c "[ $rc = 2 ] && grep -q 'unknown lane' '$T/x.out'"
H outer 10 -- env PATH="$T/bin:$PATH" HEAVY_LANES_DIR="$T" HEAVY_LOCK="$L" bash "$HEAVY" inner 10 -- echo nested-ok > "$T/n.out" 2>&1
check "nested ran" grep -q 'inside heavy job .* running under its lock' "$T/n.out"

echo "11. --status names holder, command, log tail; lock removed only by its owner"
reset
HEAVY_LOG="$T/s.out" "$T/H" stat 10 -- "$T/job.sh" s > "$T/s.out" 2>&1 & w=$!
until_ "grep -q hello-log $T/s.out"
H --status > "$T/st.out" 2>&1
check "status: holder" grep -q "holder: $w stat" "$T/st.out"
check "status: command" grep -q "cmd: stat :: .*job.sh s" "$T/st.out"
check "status: log tail" grep -q '| hello-log s' "$T/st.out"
check "status: job alive" grep -q "job group $(cat "$T/s.pid"): alive" "$T/st.out"
# simulate a takeover: another run now owns the lock; this wrapper must not remove it when it ends
{ echo "99999999 someone 2026-01-01 00:00:00"; sed 1d "$L/owner"; } > "$L/owner.x" && mv "$L/owner.x" "$L/owner"
kill -TERM $w; wait $w
check "lock not removed by a non-owner" [ -d "$L" ]

echo "12. (T788 #11) free disk: short at the start is refused at once (73), before the lock; short mid-run stops the job"
reset; echo $((5 * 1048576)) > "$T/disk"
H disk 10 -- "$T/job.sh" dk > "$T/dk.out" 2>&1; rc=$?
check "start refused, exit 73" [ $rc = 73 ]
check "  … names the volume, the floor and where to look" \
  grep -q 'refused: 5 GB free on /System/Volumes/Data < 20 GB (HEAVY_MIN_DISK_GB); free space: .*release-open-.*worktrees' "$T/dk.out"
check "  … job not started, no lock, no queue entry" bash -c "[ ! -f '$T/dk.pid' ] && [ ! -d '$L' ] && ! ls '$L.queue'/* >/dev/null 2>&1"
reset; echo $((100 * 1048576)) > "$T/disk"; echo $((2 * 1048576)) > "$T/disk_tmp"
H disktmp 10 -- "$T/job.sh" dt > "$T/dt.out" 2>&1; rc=$?
check "the \$TMPDIR volume counts too (the fuller one decides)" \
  bash -c "[ $rc = 73 ] && grep -q '2 GB free on /private/tmp-vol' '$T/dt.out'"
reset; : > "$T/disk"
H diskblind 10 -- "$T/job.sh" db > "$T/db.out" 2>&1; rc=$?
check "an unreadable df refuses the start (never 0, never blind)" \
  bash -c "[ $rc = 73 ] && grep -q 'free disk UNREADABLE' '$T/db.out' && [ ! -f '$T/db.pid' ]"
reset
"$T/H" diskfill 10 -- "$T/job.sh" df1 > "$T/df1.out" 2>&1 & w=$!
until_ "[[ -f $T/df1.pid ]]"; j=$(cat "$T/df1.pid"); echo $((1 * 1048576)) > "$T/disk"; wait $w; rc=$?
check "mid-run below the kill floor: exit 73 + message" \
  bash -c "[ $rc = 73 ] && grep -q 'STOPPED: 1 GB free on /System/Volumes/Data < 3 GB (HEAVY_KILL_DISK_GB)' '$T/df1.out'"
check "  … job gone, lock gone" bash -c "! kill -0 $j 2>/dev/null && [ ! -d '$L' ]"
H --status > "$T/st2.out" 2>&1
check "--status shows free disk, once" grep -q 'free disk: 1 GB on /System/Volumes/Data   pause file' "$T/st2.out"

# ── (T1225) first come, first served ────────────────────────────────────────────────────────────────────────────────
events() { paste -sd' ' - < "$T/events"; }
queued() { H --status 2>/dev/null | grep -c '^   queued'; }
go() { local n; for n in "$@"; do touch "$T/go.$n"; done; }
started_() { until_ "grep -q 'start $1' $T/events 2>/dev/null"; }
# inq <pid>: wait until that waiter's queue entry exists, so the next one cannot pass it under load (then a 1 s gap:
# the order is kept to the second, a tie going to the lower pid)
inq() { until_ "[[ -f $L.queue/$1 ]]" 100; [[ -f $L.queue/$1 ]] || echo "  (waiter $1 never queued)"; }

echo "13. (T1225) five waiters get the lock in the order they queued; --status lists them in that order, with labels"
reset
"$T/H" holder 10 -- "$T/hold.sh" h > "$T/h.out" 2>&1 & pids=($!)
started_ h
for n in w1 w2 w3 w4 w5; do
  sleep 1.05   # the order is kept to the second; a tie goes to the lower pid
  "$T/H" "lbl-$n" 10 -- "$T/hold.sh" "$n" > "$T/$n.out" 2>&1 & pids+=($!); inq $!
done
until_ "[[ \$(queued) == 5 ]]" 50
H --status > "$T/st13.out" 2>&1
got=$(grep '^   queued' "$T/st13.out" | sed -E 's/^   queued ([0-9]): [0-9]+ ([a-z0-9-]+)  waiting [0-9]+h[0-9]{2}m \(since .*/\1 \2/' | paste -sd' ' -)
check "status: the line in order, each with its label and wait (got: $got)" \
  [ "$got" = "1 lbl-w1 2 lbl-w2 3 lbl-w3 4 lbl-w4 5 lbl-w5" ]
go h w1 w2 w3 w4 w5
wait "${pids[@]}"
check "served first come, first served (got: $(events))" \
  [ "$(events)" = "start h end h start w1 end w1 start w2 end w2 start w3 end w3 start w4 end w4 start w5 end w5" ]
check "the line is empty afterwards" bash -c "! ls '$L.queue'/* >/dev/null 2>&1"

echo "14. (T1225) a dead, a stopped and a reused-pid entry never block the lane"
reset
"$T/H" holder 10 -- "$T/hold.sh" h > "$T/h.out" 2>&1 & hp=$!
started_ h
then_=$(( $(date +%s) - 100 ))
dead=$(bash -c 'echo $$')
printf '%s\n%s\nsince: %s\n' "$(date -r "$then_" '+%F %T')" "dead :: / :: x" "$then_" > "$L.queue/$dead"
sleep 30 & reused=$!   # a live pid that started AFTER its entry was written: the entry's waiter is dead
printf '%s\n%s\nsince: %s\n' "$(date -r "$then_" '+%F %T')" "reused :: / :: x" "$then_" > "$L.queue/$reused"
"$T/H" stopped 10 -- "$T/hold.sh" s > "$T/s.out" 2>&1 & sp=$!
until_ "[[ -f $L.queue/$sp ]]"; kill -STOP "$sp"
sleep 1.05
"$T/H" x 10 -- "$T/hold.sh" x > "$T/x.out" 2>&1 & xp=$!
until_ "[[ -f $L.queue/$xp ]]"
H --status > "$T/st14.out" 2>&1
check "status: the live waiter is first; the dead, stopped and reused entries are not in the line" \
  bash -c "grep -q '^   queued 1: $xp x ' '$T/st14.out' && [[ \$(grep -c '^   queued' '$T/st14.out') == 1 ]]"
go h x
until_ "grep -q 'end x' $T/events" 50
check "the live waiter ran past them (got: $(events))" [ "$(events)" = "start h end h start x end x" ]
kill -CONT "$sp"; kill -TERM "$sp"; wait "$sp" "$hp" "$xp" 2>/dev/null; kill "$reused" 2>/dev/null; wait "$reused" 2>/dev/null

echo "15. (T1225) the first in line waits for memory holding the lane: a smaller job behind it does not overtake"
reset
"$T/H" holder 10 -- "$T/hold.sh" h > "$T/h.out" 2>&1 & hp=$!
started_ h
"$T/H" big 30 -- "$T/hold.sh" big > "$T/big.out" 2>&1 & bp=$!; inq $bp
sleep 1.05
"$T/H" small 0 -- "$T/hold.sh" small > "$T/small.out" 2>&1 & smp=$!; inq $smp
until_ "[[ \$(queued) == 2 ]]" 50
echo 10 > "$T/free"; go h
until_ "grep -q 'free 10% < 30% — waiting' $T/big.out" 50
sleep 1
H --status > "$T/st15.out" 2>&1
check "big holds the lane waiting for memory; small is queued 1 behind it, not started" \
  bash -c "grep -q 'holder: $bp big' '$T/st15.out' && grep -q '^   queued 1: $smp small ' '$T/st15.out' && ! grep -q 'start small' '$T/events'"
check "status says why big has not started: NOT STARTED, its gate and the free memory now" \
  grep -q '^   NOT STARTED: waiting for free memory >= 30 % (now 10 %)$' "$T/st15.out"
echo 80 > "$T/free"; go big small
wait "$hp" "$bp" "$smp"
check "then big, then small (got: $(events))" [ "$(events)" = "start h end h start big end big start small end small" ]

echo "16. (T1225) mixed versions on one lane (old = the heavy.sh before T1225)"
reset   # a: new holder, old waiters: they take it on their own polls, one at a time
"$T/H" holder 10 -- "$T/hold.sh" h > "$T/h.out" 2>&1 & hp=$!
started_ h
"$T/O" o1 10 -- "$T/hold.sh" o1 > "$T/o1.out" 2>&1 & o1=$!
"$T/O" o2 10 -- "$T/hold.sh" o2 > "$T/o2.out" 2>&1 & o2=$!
until_ "[[ \$(queued) == 2 ]]" 50
"$T/O" --status > "$T/st16o.out" 2>&1
check "a: the old --status reads the new holder and lists the old waiters" \
  bash -c "grep -q 'holder: $hp holder' '$T/st16o.out' && [[ \$(grep -c '^   queued: ' '$T/st16o.out') == 2 ]]"
H --status > "$T/st16n.out" 2>&1
check "a: the new --status marks them as older heavy.sh" [ "$(grep -c 'older heavy.sh' "$T/st16n.out")" = 2 ]
go h o1 o2; wait "$hp" "$o1" "$o2"
check "a: both old waiters ran, never overlapping (got: $(events))" \
  bash -c "[[ \$(awk '{print \$1}' '$T/events' | paste -sd' ' -) == 'start end start end start end' ]]"
reset   # b: old holder, new waiters: served in their order; the old script reads their entries (status, duplicates)
"$T/O" holder 10 -- "$T/hold.sh" h > "$T/h.out" 2>&1 & hp=$!
started_ h
pids=()
for n in n1 n2 n3; do sleep 1.05; "$T/H" "lbl-$n" 10 -- "$T/hold.sh" "$n" > "$T/$n.out" 2>&1 & pids+=($!); inq $!; done
until_ "[[ \$(queued) == 3 ]]" 50
"$T/O" --status > "$T/st16b.out" 2>&1
check "b: the old --status lists the new entries by their date line" \
  bash -c "[[ \$(grep -cE '^   queued: [0-9]+ [0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:]{8}\$' '$T/st16b.out') == 3 ]]"
"$T/O" lbl-n2 10 -- "$T/hold.sh" n2 > "$T/dup.out" 2>&1; rc=$?
check "b: the old duplicate check still refuses a new waiter's twin (exit 3)" \
  bash -c "[ $rc = 3 ] && grep -q 'already waiting' '$T/dup.out'"
go h n1 n2 n3; wait "$hp" "${pids[@]}"
check "b: new waiters behind an old holder go in order (got: $(events))" \
  [ "$(events)" = "start h end h start n1 end n1 start n2 end n2 start n3 end n3" ]
reset   # c: an old waiter first in line, polling slowly (3 s): the new one behind it waits its turn on a free lock
sed 's/HEAVY_WAIT_POLL=0.2/HEAVY_WAIT_POLL=3/' "$T/O" > "$T/O3" && chmod +x "$T/O3"
"$T/H" holder 10 -- "$T/hold.sh" h > "$T/h.out" 2>&1 & hp=$!
started_ h
"$T/O3" o1 10 -- "$T/hold.sh" o1 > "$T/o1.out" 2>&1 & o1=$!; inq $o1
sleep 1.05
"$T/H" n1 10 -- "$T/hold.sh" n1 > "$T/n1.out" 2>&1 & n1=$!; inq $n1
until_ "[[ \$(queued) == 2 ]]" 50
go h o1 n1; wait "$hp" "$o1" "$n1"
check "c: old waiter first, then the new one (got: $(events))" [ "$(events)" = "start h end h start o1 end o1 start n1 end n1" ]
check "c: the new one said who it waited for" grep -q 'first in line is pid [0-9]* (o1), an older heavy.sh' "$T/n1.out"
reset   # d: a new waiter first, an old one behind (it may jump the line, as today): no deadlock, no overlap
"$T/O" holder 10 -- "$T/hold.sh" h > "$T/h.out" 2>&1 & hp=$!
started_ h
"$T/H" n1 10 -- "$T/hold.sh" n1 > "$T/n1.out" 2>&1 & n1=$!; inq $n1
sleep 1.05
"$T/O" o1 10 -- "$T/hold.sh" o1 > "$T/o1.out" 2>&1 & o1=$!; inq $o1
until_ "[[ \$(queued) == 2 ]]" 50
go h o1 n1; wait "$hp" "$o1" "$n1"
check "d: both ran, never overlapping (got: $(events))" \
  bash -c "[[ \$(awk '{print \$1}' '$T/events' | paste -sd' ' -) == 'start end start end start end' ]]"

echo "17. (T1225) --status marks a holder past HEAVY_LONG_HOLD as a long holder"
reset
"$T/H" server 10 -- "$T/hold.sh" srv > "$T/srv.out" 2>&1 & sv=$!
started_ srv; sleep 1.1
HEAVY_LONG_HOLD=0 H --status > "$T/st17.out" 2>&1
check "long holder flagged, with the reason" \
  grep -q 'LONG HOLDER: held 0h00m, past 0h00m (HEAVY_LONG_HOLD); a server that never ends does not belong on a heavy lock' "$T/st17.out"
H --status > "$T/st17b.out" 2>&1
check "not flagged under the default (1 h)" bash -c "! grep -q 'LONG HOLDER' '$T/st17b.out'"
go srv; wait "$sv"

echo "heavy.test: $fails failed in $((SECONDS - t0)) s"
(( fails == 0 ))
