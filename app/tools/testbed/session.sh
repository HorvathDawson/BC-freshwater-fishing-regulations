#!/bin/bash
# (T255) ONE TEST-BED BATCH under the heavy lock: boot the emulator, build (or reuse) the test-bed APK, install it, run
# one scenario batch (a capped command), shut down. Run it ONLY through scripts/heavy.sh (the emulator and Gradle are heavy):
#
#   scripts/heavy.sh testbed 30 -- bash app/tools/testbed/session.sh [--build] [--max-min 45] -- <command…>
#
#   --build        rebuild the APK (else reuse app/tools/testbed/out/app-testbed.apk when it exists)
#   --max-min N    the command's wall-clock cap (default 45): past it the command is stopped and the session ends
#   --local        (T354/T360) build/run an APK reading THIS machine's data server (`pnpm tiles`, :39217, as 10.0.2.2:
#                  the emulator's own name for this machine)
#   --opengl       (T360) build with MapLibre's OpenGL variant instead of Vulkan (app.json changed for the build only)
#                  instead of staging: for a release that is promoted locally but not uploaded (SALMON_NEEDS)
#   --crash-stub   (T242) the APK's crash reports go to http://localhost:39256/crash on the phone, `adb reverse`d to this
#                  machine, where `crash-stub/server.mjs` (the worker's own handler, in memory) takes them: never staging
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
AVD=${TESTBED_AVD:-n1_pixel}
BUILD=0; MAX_MIN=45; CMD=(); ENV=staging; VARIANT=; STUB=; STUB_PORT=${TESTBED_STUB_PORT:-39256}
while (( $# )); do
  case $1 in
    --build) BUILD=1 ;;
    --crash-stub) STUB=1 ;;
    --local) ENV=local ;;
    --opengl) VARIANT=opengl ;;
    --max-min) MAX_MIN=$2; shift ;;
    --) shift; CMD=("$@"); break ;;
    *) echo "session.sh: unknown $1" >&2; exit 2 ;;
  esac; shift
done
APK="$OUT/app-testbed$([[ $ENV == local ]] && echo -local)${VARIANT:+-$VARIANT}${STUB:+-stub}${TESTBED_APK_TAG:+-$TESTBED_APK_TAG}.apk"   # (T420) TESTBED_APK_TAG: an experiment build beside the others
export JAVA_HOME=${JAVA_HOME:-"/Applications/Android Studio.app/Contents/jbr/Contents/Home"}
export ANDROID_HOME=${ANDROID_HOME:-$HOME/Library/Android/sdk}
export PATH="$JAVA_HOME/bin:$ANDROID_HOME/platform-tools:$ANDROID_HOME/emulator:$HOME/.maestro/maestro/bin:$PATH"
log() { echo "[testbed $(date +%T)] $*"; }
# (T420) MLN_LOCAL_MAVEN builds against the locally patched MapLibre (app/tools/maplibre-patch/README.md): its APK must
# carry its own name (never replace the stock one), and the patched SDK is the Vulkan variant only
if [[ -n ${MLN_LOCAL_MAVEN:-} ]]; then
  [[ -n ${TESTBED_APK_TAG:-} ]] || { log "MLN_LOCAL_MAVEN needs TESTBED_APK_TAG (e.g. fsdelta) so the stock APK is kept"; exit 1; }
  [[ -z $VARIANT ]] || { log "MLN_LOCAL_MAVEN is the Vulkan SDK: drop --$VARIANT"; exit 1; }
fi
# heavy-precheck: yes — scripts/heavy.sh runs this far with HEAVY_PRECHECK=1 BEFORE it waits for the lock, so a bad flag
# or env (above) fails at once (T483; caught 2026-11-09: MLN_LOCAL_MAVEN without TESTBED_APK_TAG failed after the wait)
[[ -n ${HEAVY_PRECHECK:-} ]] && { log "precheck ok (flags + env)"; exit 0; }
source "$HERE/lib/xcrun-shim.sh"   # (T313) Maestro's startup `xcrun simctl list -j` must not hang on Xcode's first launch

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
  if [[ -n $VARIANT ]]; then
    MJSON="$ROOT/app/apps/mobile/app.json"
    cp "$MJSON" "$OUT/app.json.keep"; trap 'cp "$OUT/app.json.keep" "$MJSON" 2>/dev/null; cleanup' EXIT INT TERM
    sed -i '' "s/\"nativeVariant\": \"vulkan\"/\"nativeVariant\": \"$VARIANT\"/" app.json
    grep -q "\"nativeVariant\": \"$VARIANT\"" app.json || { log "--$VARIANT: app.json not changed"; exit 1; }
  fi
  ../../node_modules/.bin/expo prebuild --platform android --no-install --clean >"$OUT/prebuild.log" 2>&1 || { log "prebuild failed (out/prebuild.log)"; exit 1; }
  if [[ -n $VARIANT ]]; then cp "$OUT/app.json.keep" "$MJSON"; trap cleanup EXIT INT TERM; fi   # restored: the plain trap again
  # --local reads http://localhost:39217: a release build refuses cleartext unless the manifest allows it. Patched AFTER
  # prebuild (which regenerates android/), so it never reaches a store build.
  if [[ $ENV == local || -n $STUB ]]; then
    sed -i '' 's/<application /<application android:usesCleartextTraffic="true" /' android/app/src/main/AndroidManifest.xml
    grep -q 'usesCleartextTraffic="true"' android/app/src/main/AndroidManifest.xml || { log "cleartext patch failed"; exit 1; }
  fi
  # (T242) --crash-stub: crash reports go to the local stub (crash-stub/server.mjs, the worker's own handler), never staging
  [[ -n $STUB ]] && export EXPO_PUBLIC_CRASH_URL="http://localhost:$STUB_PORT/crash"
  # (T360) every test-bed APK is profileable from the shell, so Perfetto's heapprofd can say where native memory goes
  # (`app/tools/testbed/heapprof.sh`). Patched after prebuild like the cleartext line: never in a store build.
  sed -i '' 's#</application>#<profileable android:shell="true"/></application>#' android/app/src/main/AndroidManifest.xml
  grep -q 'profileable android:shell="true"' android/app/src/main/AndroidManifest.xml || { log "profileable patch failed"; exit 1; }
  # the JS bundle task does not see EXPO_PUBLIC_* as inputs: a bundle made with another env must not be reused
  rm -rf android/app/build/generated/assets/createBundleReleaseJsAndAssets android/app/build/generated/res/createBundleReleaseJsAndAssets \
         android/app/build/intermediates/assets/release
  touch "$OUT/gradle-used"
  CHANNEL=test
  # --local: the emulator's name for this machine, and no update channel (the test channel is staging's `test/` prefix)
  if [[ $ENV == local ]]; then export EXPO_PUBLIC_DATA_BASE=http://10.0.2.2:39217; CHANNEL=; fi
  t0=$(date +%s)
  ( cd android && EXPO_PUBLIC_APP_ENV=$ENV EXPO_PUBLIC_ENV_SWITCH=1 EXPO_PUBLIC_UPDATE_CHANNEL=$CHANNEL EXPO_PUBLIC_PAN_PROBE=${TESTBED_PROBE:-1} EXPO_PUBLIC_TEST_REPORTS=1 EXPO_PUBLIC_MAP_LAB=1 \
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
  # (T420) TESTBED_EMU_ARGS: extra emulator flags (e.g. `-cores 2`); TESTBED_EMU_ECORES=1: the emulator runs at background
  # QoS (macOS `taskpolicy -b`: the efficiency cores only), a slower CPU for the whole phone — the low-end stress run
  ${TESTBED_EMU_ECORES:+taskpolicy -b} emulator -avd "$AVD" -port "$PORT" -no-window -no-audio -no-boot-anim -no-snapshot-save -gpu host \
    ${TESTBED_EMU_ARGS:-} >"$OUT/emulator.log" 2>&1 &
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
# (T242) the stub APK's crash address is the phone's localhost:<port>, carried to this machine's stub
if [[ -n $STUB ]]; then adb -s "$SERIAL" reverse "tcp:$STUB_PORT" "tcp:$STUB_PORT" >/dev/null && log "crash stub: phone localhost:$STUB_PORT → this machine"; fi
export TESTBED_STUB_PORT=$STUB_PORT TESTBED_STUB=${STUB:-}
if [[ $ENV == local ]]; then
  curl -sf -o /dev/null http://localhost:39217/release/LATEST || { log "--local: no data server on :39217 (pnpm tiles)"; exit 1; }
  log "local data: http://10.0.2.2:39217 (this machine)"
fi

# 3. THE BATCH (capped; no command: build/boot/install only, then shut down)
(( ${#CMD[@]} )) || { log "no command: built/booted/installed only"; exit 0; }
log "running (cap ${MAX_MIN} min): ${CMD[*]}"
set -m
TESTBED_SERIAL=$SERIAL TESTBED_ENV=$ENV "${CMD[@]}" &
job=$!
end=$(( $(date +%s) + MAX_MIN * 60 ))
while kill -0 $job 2>/dev/null; do
  if (( $(date +%s) > end )); then log "cap reached: stopping the batch"; kill -TERM -$job 2>/dev/null; sleep 5; kill -KILL -$job 2>/dev/null; fi
  sleep 5
done
wait $job; rc=$?
log "command rc=$rc"; exit $rc
