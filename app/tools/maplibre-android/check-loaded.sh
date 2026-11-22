#!/bin/bash
# Inside a test-bed session (emulator up, APK installed): prove the app runs the pinned upstream libmaplibre.so (pin.env,
# T978) and take one map screenshot. Run as session.sh's command (session.sh builds against the pinned repo by default):
#
#   scripts/heavy.sh testbed 30 -- bash app/tools/testbed/session.sh --build -- \
#     bash app/tools/maplibre-android/check-loaded.sh "" <out dir>
#
# <m2 dir> "" = the pinned repo, cache/maplibre/<MLN_VERSION>; another repo (an experiment AAR) is named instead.
# Checks: the INSTALLED APK's arm64 libmaplibre.so has the same GNU build id as the one in the local AAR, which names the
# upstream commit it was built from (its version and SOURCE.txt), and the running app process has that lib's bytes of
# base.apk mapped executable (needs `su`: the emulator's userdebug image). Writes <out dir>/map.png and check.txt.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=pin.env
source "$HERE/pin.env"
M2=${1:-$(cd "$HERE/../../.." && pwd)/cache/maplibre/$MLN_VERSION}; OUT=${2:?usage: check-loaded.sh <m2 dir or ""> <out dir>}
mkdir -p "$OUT"; TMP=$(mktemp -d "$OUT/tmp.XXXX")
S=${TESTBED_SERIAL:?run inside session.sh}
APP=ca.canifishthis.app
READELF=$(ls -d "${ANDROID_HOME:-$HOME/Library/Android/sdk}"/ndk/*/toolchains/llvm/prebuilt/darwin-x86_64/bin/llvm-readelf | tail -1)
buildid() { "$READELF" -n "$1" | awk '/Build ID/ {print $3}'; }
say() { echo "$*" | tee -a "$OUT/check.txt"; }
: >"$OUT/check.txt"

AAR=$(ls "$M2"/org/maplibre/gl/android-sdk-vulkan/*/android-sdk-vulkan-*.aar)
unzip -q -o "$AAR" jni/arm64-v8a/libmaplibre.so -d "$TMP/aar"
say "local AAR:      $(basename "$AAR")  build id $(buildid "$TMP/aar/jni/arm64-v8a/libmaplibre.so")"
say "its source:     $(cat "$M2/SOURCE.txt" 2>/dev/null || echo "no SOURCE.txt (not made by build.sh)")"
if [[ $(basename "$AAR") == "android-sdk-vulkan-$MLN_VERSION.aar" ]]; then say "pinned:         yes ($MLN_VERSION = upstream $MLN_COMMIT)"
else say "pinned:         NO, an experiment AAR (pin.env names $MLN_VERSION)"; fi

adb -s "$S" pull "$(adb -s "$S" shell pm path $APP | sed -n 's/^package://p' | grep base.apk | tr -d '\r')" "$TMP/base.apk" >/dev/null
unzip -q -o "$TMP/base.apk" lib/arm64-v8a/libmaplibre.so -d "$TMP/apk"
say "installed APK:  build id $(buildid "$TMP/apk/lib/arm64-v8a/libmaplibre.so")"
[[ $(buildid "$TMP/apk/lib/arm64-v8a/libmaplibre.so") == $(buildid "$TMP/aar/jni/arm64-v8a/libmaplibre.so") ]] \
  || { say "FAIL: the installed APK does not carry the local AAR's libmaplibre.so"; exit 1; }

adb -s "$S" shell am force-stop $APP
adb -s "$S" shell am start -W -n $APP/.MainActivity >/dev/null
sleep 20
adb -s "$S" shell am start -a android.intent.action.VIEW -d "'canifishthis://testbed/camera?lon=${CHECK_LON:--122.78}&lat=${CHECK_LAT:-49.28}&zoom=${CHECK_ZOOM:-12}'" $APP >/dev/null
sleep 15
PID=$(adb -s "$S" shell pidof $APP | tr -d '\r')
[[ -n $PID ]] || { say "FAIL: the app is not running"; exit 1; }
# the APK stores its libs uncompressed and the linker maps them straight from base.apk: find libmaplibre.so's byte
# range inside the APK and look for an executable base.apk mapping that starts inside it
MAPS=$(adb -s "$S" shell "su 0 cat /proc/$PID/maps" | tr -d '\r')
HIT=$(python3 -I -c 'import struct, sys, zipfile
apk = sys.argv[1]
i = zipfile.ZipFile(apk).getinfo("lib/arm64-v8a/libmaplibre.so")
assert i.compress_type == 0, "libmaplibre.so is compressed in the APK"
with open(apk, "rb") as f:
    f.seek(i.header_offset); n, m = struct.unpack("<HH", f.read(30)[26:30])
lo = i.header_offset + 30 + n + m; hi = lo + i.file_size
for line in sys.stdin:
    p = line.split()
    if len(p) >= 6 and "x" in p[1] and p[5].endswith("base.apk") and lo <= int(p[2], 16) < hi:
        print(f"bytes {lo}-{hi} of base.apk; mapped: {line.strip()}"); break' "$TMP/base.apk" <<<"$MAPS")
say "libmaplibre.so executable mapping: ${HIT:-<none>}"
[[ -n $HIT ]] || { say "FAIL: no executable mapping of the APK's libmaplibre.so in the app process"; exit 1; }
adb -s "$S" exec-out screencap -p >"$OUT/map.png"
say "screenshot: $OUT/map.png"
rm -rf "$TMP"
say "PASS"
