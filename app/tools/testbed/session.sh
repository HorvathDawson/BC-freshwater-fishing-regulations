#!/bin/bash
# (T255) ONE TEST-BED BATCH under the heavy lock: boot the emulator, build (or reuse) the test-bed APK, install it, run
# one scenario batch (a capped command), shut down. Run it ONLY through scripts/heavy.sh (the emulator and Gradle are heavy):
#
#   scripts/heavy.sh testbed 30 -- bash app/tools/testbed/session.sh [--build] [--max-min 45] -- <command…>
#
#   --build        rebuild the APK (else reuse app/tools/testbed/out/app-testbed.apk when it exists)
#   --max-min N    the command's wall-clock cap (default 45): past it the command is stopped and the session ends
#   -- <command>   run it with the emulator up (e.g. `node app/tools/testbed/run.mjs feeds offline`), then shut down
#
# BOUNDED, NEVER IDLE (coordinator 2026-10-29): one scenario batch per heavy.sh job; the lock is released between
# batches so the atlas rebuilds and other heavy jobs are never held behind an emulator nobody is using.
#
# Kills only what it started: its own emulator PID, the Gradle daemon it used (`gradlew --stop`). Never pkill.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
OUT="$HERE/out"; mkdir -p "$OUT"
APK="$OUT/app-testbed.apk"
AVD=${TESTBED_AVD:-n1_pixel}
BUILD=0; MAX_MIN=45; CMD=()
while (( $# )); do
  case $1 in
    --build) BUILD=1 ;;
    --max-min) MAX_MIN=$2; shift ;;
    --) shift; CMD=("$@"); break ;;
    *) echo "session.sh: unknown $1" >&2; exit 2 ;;
  esac; shift
done
export JAVA_HOME=${JAVA_HOME:-"/Applications/Android Studio.app/Contents/jbr/Contents/Home"}
export ANDROID_HOME=${ANDROID_HOME:-$HOME/Library/Android/sdk}
export PATH="$JAVA_HOME/bin:$ANDROID_HOME/platform-tools:$ANDROID_HOME/emulator:$HOME/.maestro/maestro/bin:$PATH"
log() { echo "[testbed $(date +%T)] $*"; }

EMU_PID=""
cleanup() {
  log "shutting down"
  [[ -f $OUT/gradle-used ]] && (cd "$ROOT/app/apps/mobile/android" && ./gradlew --stop >/dev/null 2>&1; rm -f "$OUT/gradle-used")
  if [[ -n $EMU_PID ]]; then
    adb -s "$SERIAL" emu kill >/dev/null 2>&1; sleep 5
    kill -0 "$EMU_PID" 2>/dev/null && kill "$EMU_PID"
  fi
  rm -f "$OUT/SERIAL"
}
trap cleanup EXIT INT TERM

# 1. THE APK — a release build (JS bundled: no Metro, no dev LogBox toasts over the tab bar) reading STAGING on the T251
#    test channel, environment switch on (the Updates log). arm64 only (the emulator's ABI on Apple silicon).
if (( BUILD )) || [[ ! -f $APK ]]; then
  log "building the APK"
  cd "$ROOT/app/apps/mobile"
  ../../node_modules/.bin/expo prebuild --platform android --no-install >"$OUT/prebuild.log" 2>&1 || { log "prebuild failed (out/prebuild.log)"; exit 1; }
  # the JS bundle task does not see EXPO_PUBLIC_* as inputs: a bundle made with another env must not be reused
  rm -rf android/app/build/generated/assets/createBundleReleaseJsAndAssets android/app/build/generated/res/createBundleReleaseJsAndAssets \
         android/app/build/intermediates/assets/release
  touch "$OUT/gradle-used"
  t0=$(date +%s)
  ( cd android && EXPO_PUBLIC_APP_ENV=staging EXPO_PUBLIC_ENV_SWITCH=1 EXPO_PUBLIC_UPDATE_CHANNEL=test \
      ./gradlew app:assembleRelease --max-workers=4 -PreactNativeArchitectures=arm64-v8a -x lint ) >"$OUT/gradle.log" 2>&1 \
    || { log "gradle failed (out/gradle.log)"; tail -30 "$OUT/gradle.log"; exit 1; }
  cp android/app/build/outputs/apk/release/app-release.apk "$APK"
  (cd android && ./gradlew --stop >/dev/null 2>&1); rm -f "$OUT/gradle-used"
  log "APK built in $(( $(date +%s) - t0 )) s: $APK ($(du -h "$APK" | cut -f1))"
  cd "$ROOT"
fi

# 2. THE EMULATOR — headless, its own console port so a stray emulator elsewhere is never ours
PORT=${TESTBED_PORT:-5580}
SERIAL="emulator-$PORT"
if adb devices | grep -q "^$SERIAL"; then log "$SERIAL already up (not ours: left running at the end)"; else
  emulator -avd "$AVD" -port "$PORT" -no-window -no-audio -no-boot-anim -no-snapshot-save -gpu host >"$OUT/emulator.log" 2>&1 &
  EMU_PID=$!
  log "emulator pid $EMU_PID on $SERIAL"
fi
echo "$SERIAL" >"$OUT/SERIAL"
adb -s "$SERIAL" wait-for-device
until [[ "$(adb -s "$SERIAL" shell getprop sys.boot_completed 2>/dev/null | tr -d '\r')" == 1 ]]; do sleep 3; done
log "booted"
adb -s "$SERIAL" shell cmd connectivity airplane-mode disable >/dev/null 2>&1   # the AVD was left in airplane mode by N1
adb -s "$SERIAL" shell svc wifi enable; adb -s "$SERIAL" shell svc data enable
adb -s "$SERIAL" install -r "$APK" >/dev/null && log "installed $APK"

# 3. THE BATCH (capped; no command: build/boot/install only, then shut down)
(( ${#CMD[@]} )) || { log "no command: built/booted/installed only"; exit 0; }
log "running (cap ${MAX_MIN} min): ${CMD[*]}"
set -m
TESTBED_SERIAL=$SERIAL "${CMD[@]}" &
job=$!
end=$(( $(date +%s) + MAX_MIN * 60 ))
while kill -0 $job 2>/dev/null; do
  if (( $(date +%s) > end )); then log "cap reached: stopping the batch"; kill -TERM -$job 2>/dev/null; sleep 5; kill -KILL -$job 2>/dev/null; fi
  sleep 5
done
wait $job; rc=$?
log "command rc=$rc"; exit $rc
