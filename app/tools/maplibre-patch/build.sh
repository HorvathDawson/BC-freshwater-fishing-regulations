#!/bin/bash
# Build the patched MapLibre Native Android Vulkan release AAR (arm64-v8a + x86_64) and publish it to a local Maven
# repo the app's opt-in wiring reads (README.md). HEAVY: run it only through scripts/heavy.sh:
#
#   scripts/heavy.sh mln-aar 30 -- bash app/tools/maplibre-patch/build.sh <work dir>
#
# <work dir> (outside the repo, e.g. the session scratchpad) gets:
#   src/   maplibre-native at TAG with its submodules, the patch applied (cloned on the first run, reused after)
#   m2/    the local Maven repo: org/maplibre/gl/android-sdk-vulkan/<VERSION>/   <- MLN_LOCAL_MAVEN points here
#   build.log
# Env: MLN_JOBS (compile jobs, default 4), MLN_ABIS (default "arm64-v8a x86_64"), MLN_PATCH + MLN_VERSION (the lean patch).
set -euo pipefail

TAG=android-v13.6.1
VERSION=${MLN_VERSION:-13.6.1-fsdelta}   # the local artifact's version: never collides with a Maven Central one
PATCH_NAME=${MLN_PATCH:-feature-state-deltas.patch}   # or feature-state-deltas-lean.patch (MLN_VERSION=13.6.1-fslean)
NDK=28.2.13676358               # platform/android/buildSrc/src/main/kotlin/Versions.kt at TAG
CMAKE=3.31.6                    # Versions.kt asks for 3.24.0+; the SDK's 3.22.1 is too old
HERE=$(cd "$(dirname "$0")" && pwd)
WORK=${1:?usage: build.sh <work dir>}
mkdir -p "$WORK"; WORK=$(cd "$WORK" && pwd)
SRC=$WORK/src
LOG=$WORK/build.log
ABIS=${MLN_ABIS:-"arm64-v8a x86_64"}

export JAVA_HOME=${JAVA_HOME:-"/Applications/Android Studio.app/Contents/jbr/Contents/Home"}
export ANDROID_HOME=${ANDROID_HOME:-$HOME/Library/Android/sdk}
for need in "ndk/$NDK" "cmake/$CMAKE"; do
  [[ -d $ANDROID_HOME/$need ]] || { echo "missing $ANDROID_HOME/$need: sdkmanager --install \"${need/\//;}\""; exit 1; }
done

# 1. source at TAG with submodules (vcpkg and the doxygen theme are Windows/docs only: skipped)
if [[ ! -d $SRC/.git ]]; then
  git clone -q --depth 1 --branch "$TAG" https://github.com/maplibre/maplibre-native.git "$SRC"
  (cd "$SRC" && git -c submodule.fetchJobs=8 submodule update --init --depth 1 \
     $(git submodule status | awk '{print $2}' | grep -vE 'vcpkg|doxygen'))
  (cd "$SRC/vendor/maplibre-tile-spec" && git submodule update --init --depth 1 cpp/vendor/earcut cpp/vendor/fsst cpp/vendor/json)
  (cd "$SRC/vendor/freetype" && git submodule update --init --depth 1)
fi
[[ $(cd "$SRC" && git describe --tags --exact-match) == "$TAG" ]] || { echo "$SRC is not at $TAG"; exit 1; }

# 2. the patch (applied once; a tree that has it applied already passes the reverse check)
cd "$SRC"
if git apply --check "$HERE/$PATCH_NAME" 2>/dev/null; then
  git apply "$HERE/$PATCH_NAME"
elif ! git apply --check --reverse "$HERE/$PATCH_NAME" 2>/dev/null; then
  echo "$PATCH_NAME neither applies nor is applied in $SRC"; exit 1
fi

# 3. the local version (build-only: VERSION feeds the published artifact's version and the SDK's version string)
echo "$VERSION" > platform/android/VERSION

# 4. build + publish the vulkanRelease variant to $WORK/m2 (no signing: a configured repo URL without a key skips it)
cd platform/android
t0=$(date +%s)
./gradlew --init-script "$HERE/jobpool.init.gradle" --max-workers=1 \
  -Pmaplibre.abis="$ABIS" -PpublicationRepositoryUrl="file://$WORK/m2" \
  :MapLibreAndroid:assembleVulkanRelease :MapLibreAndroid:publishVulkanreleasePublicationToConfiguredRepository \
  >"$LOG" 2>&1 || { tail -40 "$LOG"; exit 1; }
./gradlew --stop >/dev/null 2>&1
echo "built in $(( $(date +%s) - t0 )) s"
ls -l "$WORK/m2/org/maplibre/gl/android-sdk-vulkan/$VERSION/"
