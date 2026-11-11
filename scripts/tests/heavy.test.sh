#!/usr/bin/env bash
# Shell test for scripts/heavy.sh (T536, T638): fake jobs, a stubbed `memory_pressure` and `footprint`, scratch lanes.
# No real build runs and the live lanes (~/.claude/heavy-job*.lock) are never touched: every call gets
# HEAVY_LANES_DIR=<scratch>. Run by scripts/fast-fail.sh when heavy.sh or this file changes (~40 s).
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
echo 80 > "$T/free"; echo 1000 > "$T/fp"

# $T/H = heavy.sh with the scratch lanes and fast timings. A script that EXECs (a function would run in a subshell
# when backgrounded), so `"$T/H" … &` puts heavy.sh's own pid in $! and a signal to it reaches the wrapper.
HEAD_SHA=$(git -C "$ROOT" rev-parse HEAD)
cat > "$T/H" <<EOF
#!/bin/bash
exec env -u HEAVY_HELD PATH="$T/bin:\$PATH" HEAVY_LANES_DIR="$T" HEAVY_LOCK="\${LANE:-$T/heavy-job.lock}" \\
  HEAVY_PAUSE="$T/no-pause" HEAVY_POLL=0.2 HEAVY_WAIT_POLL=0.2 HEAVY_FOOTPRINT_EVERY=0 HEAVY_STOP_GRACE=1 \\
  HEAVY_UNREADABLE_MAX=3 FAST_FAIL_DONE="\${FAST_FAIL_DONE:-$HEAD_SHA}" bash "$HEAVY" "\$@"
EOF
chmod +x "$T/H"
H() { "$T/H" "$@"; }
until_() { local i=0; until eval "$1"; do sleep 0.1; i=$((i + 1)); (( i > ${2:-100} )) && return 1; done; }
ok() { echo "  ok   $1"; }
bad() { echo "  FAIL $1"; fails=$((fails + 1)); }
check() { local name=$1; shift; if "$@"; then ok "$name"; else bad "$name"; fi; }
alive() { kill -0 "$1" 2>/dev/null; }
reset() { rm -rf "$T"/heavy-job*.lock* "$T"/*.pid "$T/events" "$T"/*.out; echo 80 > "$T/free"; echo 1000 > "$T/fp"; }
L=$T/heavy-job.lock

echo "heavy.sh test: $HEAVY"

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

echo "heavy.test: $fails failed in $((SECONDS - t0)) s"
(( fails == 0 ))
