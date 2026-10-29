#!/bin/bash
# STAGING ONLY: build a release arm64 APK of the app reading STAGING data (T232 env, switch on, T251 version stamp) and
# publish it to bc-fishing-regulations-staging under app/android/ (served by the staging data worker):
#   https://data-staging.canifishthis.ca/app/android/index.html   (link, version, date, size, install steps)
#   .../app/android/latest.apk  and  canifishthis-staging-<version>-<sha>.apk
# Signed with Expo's debug keystore (android/app/debug.keystore, gitignored; same key every build, so an update installs
# over the previous one). Sideload only. HEAVY: wrapped in scripts/heavy.sh (waits for the machine lock).
# Usage: deploy/staging/build-apk.sh [--no-upload]
set -euo pipefail
cd "$(dirname "$0")/../.."
if [[ -z ${BUILD_APK_INNER:-} ]]; then
  export BUILD_APK_INNER=1
  exec scripts/heavy.sh build-apk 20 -- "$0" "$@"
fi
BUCKET=bc-fishing-regulations-staging            # hardcoded: never a production bucket
BASE=https://data-staging.canifishthis.ca/app/android
export JAVA_HOME=${JAVA_HOME:-"/Applications/Android Studio.app/Contents/jbr/Contents/Home"}
export ANDROID_HOME=${ANDROID_HOME:-$HOME/Library/Android/sdk}
export EXPO_PUBLIC_APP_ENV=staging EXPO_PUBLIC_ENV_SWITCH=1
M=app/apps/mobile
VER=$(node -p "require('./$M/app.json').expo.version")
CODE=$(node -p "require('./$M/app.json').expo.android.versionCode")
SHA=$(git rev-parse --short HEAD)
export EXPO_PUBLIC_GIT_SHA=$(git rev-parse HEAD)
DATE=$(date -u +%F)
OUT=app/dist/apk; mkdir -p "$OUT"
NAME="canifishthis-staging-$VER-$SHA.apk"
cd "$M"
[[ -d android ]] || npx expo prebuild --platform android
echo "sdk.dir=$ANDROID_HOME" > android/local.properties
(cd android && ./gradlew assembleRelease -PreactNativeArchitectures=arm64-v8a --max-workers=4 -Dorg.gradle.workers.max=4)
cd - >/dev/null
cp "$M/android/app/build/outputs/apk/release/app-release.apk" "$OUT/$NAME"
cp "$OUT/$NAME" "$OUT/latest.apk"
SIZE=$(stat -f%z "$OUT/$NAME"); MB=$(awk "BEGIN{printf \"%.1f\", $SIZE/1048576}")
cat > "$OUT/index.html" <<HTML
<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>canifishthis Android (staging)</title>
<body style="font:16px/1.5 system-ui;max-width:34em;margin:2em auto;padding:0 16px">
<h1>canifishthis for Android <small>(staging)</small></h1>
<p><a href="latest.apk" style="font-size:1.3em"><b>Download the APK</b></a><br>
Version $VER (build $CODE), commit $SHA, built $DATE, $MB MB, arm64 phones.<br>
Pinned copy: <a href="$NAME">$NAME</a></p>
<p>This build reads <b>staging</b> data (data-staging.canifishthis.ca) and has the environment switch on About.</p>
<h2>Install</h2>
<ol><li>Open this page in Chrome on the phone and tap Download.</li>
<li>When asked, allow Chrome to <b>install unknown apps</b> (Settings, Apps, Chrome, Install unknown apps).</li>
<li>Open the downloaded file and tap Install. If Play Protect warns, choose "Install anyway" (the app is not from the Play Store).</li>
<li>Updates: install a newer APK from this page over the old one; your saved spots stay. The app also updates its data by itself.</li></ol>
</body>
HTML
[[ ${1:-} == --no-upload ]] && { echo "built $OUT/$NAME ($MB MB)"; exit 0; }
W="npx --prefix app wrangler r2 object put --remote"
APK=application/vnd.android.package-archive
$W "$BUCKET/app/android/$NAME" --file "$OUT/$NAME" --content-type $APK --cache-control "public, max-age=31536000, immutable"
$W "$BUCKET/app/android/latest.apk" --file "$OUT/latest.apk" --content-type $APK --cache-control "no-cache"
$W "$BUCKET/app/android/index.html" --file "$OUT/index.html" --content-type "text/html; charset=utf-8" --cache-control "no-cache"
curl -sI "$BASE/latest.apk" | head -8
echo "published $BASE/index.html  ($NAME, $MB MB)"
