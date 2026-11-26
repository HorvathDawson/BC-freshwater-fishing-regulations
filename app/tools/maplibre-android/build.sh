#!/bin/bash
# Build the MapLibre Native Android Vulkan release AAR (arm64-v8a + x86_64) of the PINNED upstream commit (pin.env) with NO
# patch, and publish it to the local Maven repo every Android build reads (README.md).
#
# (T1170) WITHOUT --compile it compiles NOTHING: it downloads the stored build that maplibre-android.lock.json records for
# the current key (stored.py: built once per key by .github/workflows/maplibre-android.yml, a GitHub Release asset),
# verifies its sha256 and size and unpacks it to the repo below. An unpublished record, another key (the pin or a setting
# moved), a failed download or a wrong file is a stop with its cause, never a local build. Compiling here is ONLY the
# explicit flag (the runner's build; a pin move before the workflow has stored it). HEAVY: only through heavy.sh:
#
#   scripts/heavy.sh mln-aar 30 -- bash app/tools/maplibre-android/build.sh --compile <work dir>
#
# <work dir> (outside git's view, e.g. the session scratchpad; never commit what it holds) gets:
#   src/        maplibre-native at MLN_COMMIT with its submodules (cloned on the first run, reused after)
#   build.log
# The repo goes to cache/maplibre/<MLN_VERSION>/ of the checkout this runs from (MLN_OUT overrides): the path
# deploy/staging/build-apk.sh and app/tools/testbed/session.sh default to.
# Env: MLN_JOBS (compile jobs, default 4), MLN_ABIS (default "arm64-v8a x86_64"), MLN_OUT.
# Measured (T978, 2026-11-22, 4 jobs, Mac at load ~20-35): full build 703 s; clone + submodules ~1-2 min.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
# shellcheck source=pin.env
source "$HERE/pin.env"
# (T1170) these four are in the stored build's key (stored.py reads these lines), and each is checked below
NDK=28.2.13676358               # platform/android/buildSrc/src/main/kotlin/Versions.kt at MLN_COMMIT
CMAKE=3.31.6                    # Versions.kt asks for 3.24.0+; the SDK's 3.22.1 is too old
GRADLE=9.5.1                    # platform/android/gradle/wrapper/gradle-wrapper.properties at MLN_COMMIT
JDK=21                          # the JDK major (Android Studio's JBR on the Mac; Temurin on the runner)
COMPILE=
if [[ ${1:-} == --compile ]]; then COMPILE=1; shift; fi
WORK=${1:?usage: build.sh [--compile] <work dir>}
mkdir -p "$WORK"; WORK=$(cd "$WORK" && pwd)
SRC=$WORK/src
LOG=$WORK/build.log
OUT=${MLN_OUT:-$ROOT/cache/maplibre/$MLN_VERSION}
ABIS=${MLN_ABIS:-"arm64-v8a x86_64"}

# 0. (T1170) the stored build, unless --compile: stored.py says every refusal and exits 1
if [[ -z $COMPILE ]]; then exec python3 "$HERE/stored.py" fetch --work "$WORK" --out "$OUT"; fi

export JAVA_HOME=${JAVA_HOME:-"/Applications/Android Studio.app/Contents/jbr/Contents/Home"}
export ANDROID_HOME=${ANDROID_HOME:-$HOME/Library/Android/sdk}
for need in "ndk/$NDK" "cmake/$CMAKE"; do
  [[ -d $ANDROID_HOME/$need ]] || { echo "missing $ANDROID_HOME/$need: sdkmanager --install \"${need/\//;}\""; exit 1; }
done
jdk=$("$JAVA_HOME/bin/java" -version 2>&1 | sed -n 's/.*version "\([0-9]*\).*/\1/p' | head -1)
[[ $jdk == "$JDK" ]] || { echo "build.sh: JAVA_HOME=$JAVA_HOME is JDK ${jdk:-unreadable}, not $JDK (the key names $JDK)"; exit 1; }

# 1. the source at MLN_COMMIT (cloned on the first run)
if [[ ! -d $SRC/.git ]]; then
  git clone -q --filter=blob:none --no-checkout https://github.com/maplibre/maplibre-native.git "$SRC"
  git -C "$SRC" checkout -q "$MLN_COMMIT"
fi
got=$(git -C "$SRC" rev-parse HEAD)
[[ $got == "$MLN_COMMIT" ]] || { echo "$SRC is at $got, not the pin $MLN_COMMIT: git -C $SRC fetch origin $MLN_COMMIT && git -C $SRC checkout $MLN_COMMIT, or a new work dir"; exit 1; }
# its submodules, nested ones included, EVERY run (T978 review: an init cut short and run again was skipped when this
# ran on the first clone only). vcpkg and the doxygen theme are Windows/docs only: never fetched.
# shellcheck disable=SC2046
git -C "$SRC" submodule update --init --recursive --depth 1 --jobs 8 \
  $(git -C "$SRC" submodule status | awk '{print $2}' | grep -vE 'vcpkg|doxygen')

# the NDK above must be the one the pinned commit asks for (a pin move can change it: pin.env says so)
VERSIONS_KT=$SRC/platform/android/buildSrc/src/main/kotlin/Versions.kt
grep -q "ndkVersion = \"$NDK\"" "$VERSIONS_KT" || { echo "build.sh: NDK=$NDK, but $VERSIONS_KT asks for: $(grep -o 'ndkVersion = "[^"]*"' "$VERSIONS_KT"); set NDK (and check CMAKE) here"; exit 1; }
WRAPPER=$SRC/platform/android/gradle/wrapper/gradle-wrapper.properties
grep -q "gradle-$GRADLE-bin.zip" "$WRAPPER" || { echo "build.sh: GRADLE=$GRADLE, but $WRAPPER names: $(grep distributionUrl "$WRAPPER"); set GRADLE here"; exit 1; }

# 2. NO PATCH (T978): the tree must be exactly upstream's — no changed, staged or UNTRACKED file (an added source file
#    is a patch too; build outputs are upstream-ignored), submodules included — the local version file aside
dirty=$(git -C "$SRC" status --porcelain --untracked-files=all --ignore-submodules=none -- . ':(exclude)platform/android/VERSION')
[[ -z $dirty ]] || { echo "$SRC has local changes (no patch is ever applied, T978):"; echo "$dirty" | head -20
  echo "fix: git -C $SRC checkout -- . (or a new work dir)"; exit 1; }

# 3. the local version (build-only: it feeds the published artifact's version and the SDK's version string)
echo "$MLN_VERSION" > "$SRC/platform/android/VERSION"
# (T1170) CMake named, not picked: Versions.kt's "3.24.0+" lets AGP take the newest the SDK holds (a runner holds several).
# local.properties is upstream-ignored, so the no-patch check above does not see it
printf 'sdk.dir=%s\ncmake.dir=%s\n' "$ANDROID_HOME" "$ANDROID_HOME/cmake/$CMAKE" > "$SRC/platform/android/local.properties"

# 4. build + publish the vulkanRelease variant to OUT (no signing: a configured repo URL without a key skips it)
mkdir -p "$OUT"
cd "$SRC/platform/android"
t0=$(date +%s)
./gradlew --init-script "$HERE/jobpool.init.gradle" --max-workers=1 \
  -Pmaplibre.abis="$ABIS" -PpublicationRepositoryUrl="file://$OUT" \
  :MapLibreAndroid:assembleVulkanRelease :MapLibreAndroid:publishVulkanreleasePublicationToConfiguredRepository \
  >"$LOG" 2>&1 || { tail -40 "$LOG"; exit 1; }
./gradlew --stop >/dev/null 2>&1 || true
AAR=$OUT/org/maplibre/gl/android-sdk-vulkan/$MLN_VERSION/android-sdk-vulkan-$MLN_VERSION.aar
[[ -f $AAR ]] || { echo "no AAR at $AAR after the build ($LOG)"; exit 1; }
printf 'maplibre-native %s (upstream, no patch), version %s, built %s from %s\n' \
  "$MLN_COMMIT" "$MLN_VERSION" "$(date -u +%FT%TZ)" "$SRC" >"$OUT/SOURCE.txt"
echo "built in $(( $(date +%s) - t0 )) s: $AAR"
