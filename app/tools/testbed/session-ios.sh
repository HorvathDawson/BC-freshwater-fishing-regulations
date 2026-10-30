#!/bin/bash
# (T313) ONE iOS TEST-BED BATCH under the heavy lock: boot an iPhone simulator, install the simulator build
# (deploy/staging/build-ios-sim.sh → app/dist/ios-sim/canifishthis.app), run one capped command, shut the simulator
# down again (only when this session booted it). Run it ONLY through scripts/heavy.sh:
#
#   scripts/heavy.sh testbed-ios 30 -- bash app/tools/testbed/session-ios.sh [--build] [--fresh] [--max-min 30] -- <command…>
#   e.g. … -- node app/tools/testbed/run-ios.mjs all
#
#   --build      build the app first (build-ios-sim.sh --testbed, re-entrant under the same lock)
#   --fresh      uninstall first: the next launch is a first open (downloads the whole staging release)
#   --max-min N  the command's wall-clock cap (default 30)
#   TESTBED_SIM  the device name to use (default "iPhone 17"; the newest runtime that has it)
#
# `xcrun simctl` runs `xcodebuild -runFirstLaunch` first when CoreSimulator is older than Xcode wants, and that waits
# for an admin password (hangs, unattended). Until the owner has run it once, a PATH shim sends `xcrun simctl` (ours and
# Maestro's) to the CoreSimulator framework's own simctl. Kills only what it started. Never pkill.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
OUT="$HERE/out"; mkdir -p "$OUT"
APP="$ROOT/app/dist/ios-sim/canifishthis.app"
NAME=${TESTBED_SIM:-iPhone 17}
BUILD=0; FRESH=0; MAX_MIN=30; CMD=()
while (( $# )); do
  case $1 in
    --build) BUILD=1 ;;
    --fresh) FRESH=1 ;;
    --max-min) MAX_MIN=$2; shift ;;
    --) shift; CMD=("$@"); break ;;
    *) echo "session-ios.sh: unknown $1" >&2; exit 2 ;;
  esac; shift
done
log() { echo "[testbed-ios $(date +%T)] $*"; }
source "$HERE/lib/xcrun-shim.sh"
export PATH="$HOME/.maestro/maestro/bin:$PATH"
export JAVA_HOME=${JAVA_HOME:-"/Applications/Android Studio.app/Contents/jbr/Contents/Home"}
export TESTBED_PLATFORM=ios
SIMCTL="xcrun simctl"

if (( BUILD )); then
  log "building (build-ios-sim.sh --testbed)"
  "$ROOT/deploy/staging/build-ios-sim.sh" --testbed || { log "build failed"; exit 1; }
fi
[[ -d $APP ]] || { log "no $APP: run deploy/staging/build-ios-sim.sh (or --build)"; exit 1; }

# the simulator: an already booted one of that name is used and left running; else the newest runtime's, booted by us
UDID=$($SIMCTL list devices available -j | python3 -c '
import json,sys,re
d=json.load(sys.stdin)["devices"]; name=sys.argv[1]
rt=sorted((k for k in d if "iOS" in k), key=lambda k:[int(x) for x in re.findall(r"\d+",k.split("iOS")[-1])], reverse=True)
hit=[(x["state"],x["udid"]) for k in rt for x in d[k] if x["name"]==name]
booted=[u for s,u in hit if s=="Booted"]
print((booted or [u for s,u in hit] or [""])[0])' "$NAME")
[[ -n $UDID ]] || { log "no simulator named \"$NAME\""; exit 1; }
BOOTED_BY_US=0
cleanup() {
  if (( BOOTED_BY_US )); then log "shutting down $UDID"; $SIMCTL shutdown "$UDID" >/dev/null 2>&1; fi
  rm -f "$OUT/UDID"
}
trap cleanup EXIT INT TERM
if $SIMCTL list devices | grep -q "$UDID) (Booted)"; then log "$NAME $UDID already booted (not ours: left running)"
else $SIMCTL boot "$UDID" && BOOTED_BY_US=1; log "booted $NAME $UDID"; fi
$SIMCTL bootstatus "$UDID" -b >/dev/null
echo "$UDID" > "$OUT/UDID"
export TESTBED_UDID=$UDID
(( FRESH )) && { $SIMCTL uninstall "$UDID" ca.canifishthis.app 2>/dev/null; log "uninstalled (first open next)"; }
$SIMCTL install "$UDID" "$APP" && log "installed $APP ($(cat "$ROOT/app/dist/ios-sim/sha" 2>/dev/null | cut -c1-9))"

(( ${#CMD[@]} )) || { log "no command: booted/installed only"; exit 0; }
log "running (cap ${MAX_MIN} min): ${CMD[*]}"
set -m
"${CMD[@]}" &
job=$!
end=$(( $(date +%s) + MAX_MIN * 60 ))
while kill -0 $job 2>/dev/null; do
  if (( $(date +%s) > end )); then log "cap reached: stopping the batch"; kill -TERM -$job 2>/dev/null; sleep 5; kill -KILL -$job 2>/dev/null; fi
  sleep 5
done
wait $job; rc=$?
log "command rc=$rc"; exit $rc
