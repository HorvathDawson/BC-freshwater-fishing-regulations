# shellcheck shell=bash
# (T1058) A GRADLE BUILD THAT ENDS — SOURCED by deploy/staging/build-apk.sh for both of its Gradle builds.
#
#   gradle_watched <android dir> <gradle args…>   ./gradlew --no-daemon <args> in <android dir>, its output shown as
#                                                 it comes; returns Gradle's exit code, or 124 when it was stopped
#
# caught (0.9.0 production build, 2026-11-24 09:14): Gradle printed BUILD SUCCESSFUL, its daemon stopped before it sent
# the client the build's result (a Metaspace expiry at the end of the build, plugins/with-gradle-memory.js), and the
# `gradlew` client waited on the closed connection for over an hour, holding the heavy lock; heavy.sh's guards watch
# memory and disk, and an idle client uses neither. Now:
#   - BUILD SUCCESSFUL / BUILD FAILED printed and the client still running GRADLE_DONE_GRACE_S (120) later: stopped,
#     said, and the build FAILS (exit 124). Its result never reached the client, so nothing after it is trusted.
#   - still running GRADLE_MAX_S (3600) after it started (a normal build here takes 1-7 min): stopped, said, 124.
#   Stopping = TERM to the client and every process under it (the PIDs this call started, walked from the client),
#   KILL after 10 s. The Kotlin compile daemons are left alone: another build may share one, and each stops itself once
#   its Gradle is gone. Never by name, never `gradlew --stop` (it stops every Gradle daemon of the version on the machine).
#   - --no-daemon: Gradle forks a SINGLE-USE daemon for the build (the project's jvmargs differ from the client's) that
#     exits with it, so no idle 3 GiB daemon stays behind for 3 hours outside the heavy lock (2026-11-24: one did, with
#     three Kotlin daemons, after the 11:13 build). A daemon kept between builds bought little: every build-apk build is
#     a fresh worktree.
#   - a machine-wide `org.gradle.jvmargs` (~/.gradle/gradle.properties) overrides the project's: said, not hidden.
GRADLE_DONE_GRACE_S=${GRADLE_DONE_GRACE_S:-120}
GRADLE_MAX_S=${GRADLE_MAX_S:-3600}
GRADLE_POLL_S=${GRADLE_POLL_S:-5}
GRADLE_STOP_GRACE_S=${GRADLE_STOP_GRACE_S:-10}

# _gw_alive <pid>: still running (bash reaps its own background child as it exits, keeping the code for `wait`)
_gw_alive() { kill -0 "$1" 2>/dev/null; }

# _gw_tree <pid>: the pid and every process under it, parents first
_gw_tree() {
  local c
  echo "$1"
  for c in $(pgrep -P "$1" 2>/dev/null); do _gw_tree "$c"; done
}

# _gw_stop <pid>: TERM the tree under <pid> (Kotlin compile daemons left out), KILL what is left after the grace
_gw_stop() {
  local p pids=() i
  for p in $(_gw_tree "$1"); do
    [[ $(ps -o command= -p "$p" 2>/dev/null) == *KotlinCompileDaemon* ]] && continue
    pids+=("$p")
  done
  echo "gradle-watch: stopping pid(s) ${pids[*]} (this build's Gradle client and what it started)" >&2
  kill -TERM "${pids[@]}" 2>/dev/null || true
  for ((i = 0; i < GRADLE_STOP_GRACE_S; i++)); do
    _gw_alive "$1" || break
    sleep 1
  done
  for p in "${pids[@]}"; do _gw_alive "$p" && kill -KILL "$p" 2>/dev/null; done
  return 0
}

gradle_watched() {
  local dir=$1; shift
  local log pid tailpid start=$SECONDS done_at= why= rc=0 f line
  f=${GRADLE_USER_HOME:-$HOME/.gradle}/gradle.properties
  if [[ -f $f ]] && line=$(grep -E '^[[:space:]]*org\.gradle\.jvmargs[[:space:]]*=' "$f" | tail -1) && [[ -n $line ]]; then
    echo "gradle-watch: $f sets ${line#"${line%%[![:space:]]*}"} — it OVERRIDES the project's (plugins/with-gradle-memory.js)"
  fi
  log=$(mktemp "${TMPDIR:-/tmp}/gradle-watch.XXXXXX")
  (cd "$dir" && exec ./gradlew --no-daemon "$@") >"$log" 2>&1 </dev/null &
  pid=$!
  tail -n +1 -f "$log" &
  tailpid=$!
  while _gw_alive "$pid"; do
    sleep "$GRADLE_POLL_S"
    _gw_alive "$pid" || break
    if [[ -z $done_at ]] && grep -qE '^BUILD (SUCCESSFUL|FAILED) in ' "$log"; then done_at=$SECONDS; fi
    if [[ -n $done_at ]] && (( SECONDS - done_at >= GRADLE_DONE_GRACE_S )); then
      why="Gradle printed \"$(grep -E '^BUILD (SUCCESSFUL|FAILED) in ' "$log" | tail -1)\" $((SECONDS - done_at)) s ago and its client (pid $pid) has not exited: its result never reached it (T1058, a daemon that stopped under it). The build is NOT trusted"
      break
    fi
    if (( SECONDS - start >= GRADLE_MAX_S )); then
      why="Gradle (pid $pid) ran $((SECONDS - start)) s, past GRADLE_MAX_S=$GRADLE_MAX_S, without finishing (T1058)"
      break
    fi
  done
  if [[ -n $why ]]; then
    echo "gradle-watch: $why" >&2
    _gw_stop "$pid"
    wait "$pid" 2>/dev/null || true
    rc=124
  else
    wait "$pid" || rc=$?
  fi
  sleep 1                                     # tail prints the last lines
  kill "$tailpid" 2>/dev/null || true
  wait "$tailpid" 2>/dev/null || true
  if (( rc == 124 )); then echo "gradle-watch: this build FAILED (stopped); its output: $log" >&2
  else rm -f "$log"; fi                       # shown above as it came
  return "$rc"
}
