#!/bin/bash
# Upstream tests for the feature-state patch, on this Mac (Metal, CMake + Ninja). HEAVY: run only through scripts/heavy.sh:
#
#   scripts/heavy.sh mln-tests 30 -- bash app/tools/maplibre-patch/tests.sh <work dir>
#
# <work dir> is build.sh's (src/ = maplibre-native at the tag, patch applied); builds into <work dir>/build-macos.
# Runs: the feature-state render tests (upstream's + the three the patch adds) and the unit tests that touch feature
# state or tile/source updates. Results: <work dir>/tests.log; the render report is src/metrics/macos-xcode11-release/.
set -euo pipefail
WORK=$(cd "${1:?usage: tests.sh <work dir>}" && pwd)
SRC=$WORK/src
BUILD=$WORK/build-macos
LOG=$WORK/tests.log
JOBS=${MLN_JOBS:-4}
SDK_CMAKE=${ANDROID_HOME:-$HOME/Library/Android/sdk}/cmake/3.31.6/bin   # cmake + ninja (build.sh needs them anyway)
export PATH="$SDK_CMAKE:$PATH"

cmake -S "$SRC" -B "$BUILD" -G Ninja -DCMAKE_MAKE_PROGRAM="$SDK_CMAKE/ninja" \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_OSX_DEPLOYMENT_TARGET=14.3 \
  -DMLN_WITH_METAL=ON -DMLN_DARWIN_USE_LIBUV=OFF -DMLN_WITH_GLFW=OFF -DMLN_WITH_WERROR=OFF >"$LOG" 2>&1 || { tail -30 "$LOG"; exit 1; }
t0=$(date +%s)
cmake --build "$BUILD" -j "$JOBS" --target mbgl-render-test-runner mbgl-test-runner >>"$LOG" 2>&1 || { tail -30 "$LOG"; exit 1; }
echo "built in $(( $(date +%s) - t0 )) s" | tee -a "$LOG"

cd "$SRC"
set +e
"$BUILD/mbgl-render-test-runner" --manifestPath metrics/macos-xcode11-release-style.json -f 'feature-state/' 2>&1 | tee -a "$LOG" | tail -25
render=${PIPESTATUS[0]}
"$BUILD/mbgl-test-runner" --gtest_filter='Query.*:*FeatureState*:GeoJSON*:Source.*:VectorTile*:GeometryTile*:CustomGeometryTile*' 2>&1 | tee -a "$LOG" | tail -25
unit=${PIPESTATUS[0]}
echo "render-tests exit $render, unit tests exit $unit" | tee -a "$LOG"
(( render == 0 && unit == 0 ))
