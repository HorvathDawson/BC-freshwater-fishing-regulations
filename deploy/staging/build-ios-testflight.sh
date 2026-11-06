#!/bin/bash
# (T88) A SIGNED iOS DEVICE BUILD, uploaded to TestFlight. THE OWNER RUNS THIS (it needs the $99 Apple Developer account);
# Claude only wrote it and checked `--dry-run`. The iOS twin of build-apk.sh --play (Google Play internal testing).
#
# Builds COMMITTED HEAD only: a temporary git worktree (other lanes' uncommitted edits never enter it), `pnpm install
# --offline`, `expo prebuild -p ios`, pods, then `xcodebuild archive` for generic iOS with AUTOMATIC signing driven by an
# App Store Connect API key (`-allowProvisioningUpdates`: Xcode makes/renews the distribution certificate and the App
# Store profile itself; no Xcode login, no hand-made profiles), then `-exportArchive` with destination `upload`: the build
# goes straight to App Store Connect, where it appears in TestFlight after Apple's processing (~5–30 min).
# HEAVY: wrapped in scripts/heavy.sh (waits for the machine lock).
#
# Usage: deploy/staging/build-ios-testflight.sh [--production] [--no-upload | --unsigned] [--dry-run]
#   (default)     staging data, the environment switch on — the build internal testers get (as the Play internal track)
#   --production  production data, no switch — the build that is later submitted for App Review (needs
#                 CONFIRM_PROD=canifishthis.ca, T433)
#   --no-upload   export the signed .ipa to app/dist/ios/ instead of uploading (Transporter or `xcrun altool` by hand)
#   --unsigned    a device (arm64, iphoneos) Release archive with NO signing, team or upload: proves the device build
#                 compiles and links without the account (Claude's check); the archive cannot be installed or uploaded
#   --dry-run     print the steps and the secrets it would read; builds nothing, needs no account
#
# Secrets (never committed), in ~/.canifishthis/:
#   ios-team-id           the 10-character Team ID (developer.apple.com → Membership details)
#   asc-api-key.json      {"key_id": "…", "issuer_id": "…", "key_path": "/Users/…/AuthKey_<key_id>.p8"} — App Store Connect →
#                         Users and Access → Integrations → App Store Connect API → a Team key with the "App Manager" role
#   testflight-last-build (written here) the last CFBundleVersion uploaded; each upload must be higher
# Before the first run, in App Store Connect: My Apps → + → New App, platform iOS, bundle id ca.canifishthis.app (register
# it at developer.apple.com → Identifiers if it is not offered), the name, primary language English (Canada), SKU any.
#
# The build number: max(app.json ios.buildNumber, testflight-last-build) + 1, set in the WORKTREE's app.json only (never
# committed: Play's versionCode keeps its own count); the version (CFBundleShortVersionString) is app.json's expo.version.
set -euo pipefail
cd "$(dirname "$0")/../.."
REPO=$PWD
has() { [[ " $ARGS " == *" $1 "* ]]; }
ARGS="$*"
if has --dry-run; then DRY=1; else DRY=; fi
# (T433) a production build from a staging/ script asks the same confirmation as deploy/prod/
if has --production && [[ "${CONFIRM_PROD:-}" != canifishthis.ca ]]; then
  echo "build-ios-testflight.sh --production: set CONFIRM_PROD=canifishthis.ca to build production" >&2; exit 2
fi
if [[ -z ${BUILD_IOS_INNER:-} && -z $DRY ]]; then
  export BUILD_IOS_INNER=1
  exec scripts/heavy.sh build-ios-testflight 20 -- "$0" "$@"
fi
SECRETS=$HOME/.canifishthis
M=app/apps/mobile
if has --production; then
  export EXPO_PUBLIC_APP_ENV=production; unset EXPO_PUBLIC_ENV_SWITCH; ENV=production
else
  export EXPO_PUBLIC_APP_ENV=staging EXPO_PUBLIC_ENV_SWITCH=1; ENV=staging
fi
export EXPO_PUBLIC_CONTACT=${EXPO_PUBLIC_CONTACT:-support@canifishthis.ca}
# a store build never carries the test bed's switches
unset EXPO_PUBLIC_PAN_PROBE EXPO_PUBLIC_TEST_REPORTS EXPO_PUBLIC_UPDATE_CHANNEL

need() { [[ -s $1 ]] || { echo "missing $1 — $2" >&2; exit 2; }; }
TEAM_FILE=$SECRETS/ios-team-id KEY_FILE=$SECRETS/asc-api-key.json LAST_FILE=$SECRETS/testflight-last-build
APP_BUILD=$(node -p "require('./$M/app.json').expo.ios.buildNumber")
LAST=$( [[ -s $LAST_FILE ]] && cat "$LAST_FILE" || echo 0 )
BUILD_NO=$(( (APP_BUILD > LAST ? APP_BUILD : LAST) + 1 ))
VER=$(node -p "require('./$M/app.json').expo.version")
SHA_FULL=$(git rev-parse HEAD)
if [[ -n $DRY ]]; then
  echo "[dry-run] $ENV build of committed HEAD $SHA_FULL: version $VER, build $BUILD_NO (app.json $APP_BUILD, last uploaded $LAST)"
  for f in "$TEAM_FILE" "$KEY_FILE"; do [[ -s $f ]] && echo "[dry-run] found $f" || echo "[dry-run] MISSING $f (see the header)"; done
  echo "[dry-run] git worktree add --detach \$WT $SHA_FULL; (cd \$WT/app && pnpm install --offline --frozen-lockfile)"
  echo "[dry-run] set expo.ios.buildNumber=$BUILD_NO in the worktree's app.json; expo prebuild -p ios --no-install; pod install"
  echo "[dry-run] xcodebuild archive -workspace ios/<Name>.xcworkspace -scheme <Name> -configuration Release -destination generic/platform=iOS"
  echo "          DEVELOPMENT_TEAM=<team> CODE_SIGN_STYLE=Automatic -allowProvisioningUpdates -authenticationKey{Path,ID,IssuerID}"
  echo "[dry-run] xcodebuild -exportArchive (method app-store-connect, destination $(has --no-upload && echo 'export → app/dist/ios/' || echo 'upload → App Store Connect / TestFlight'))"
  exit 0
fi
if has --unsigned; then
  SIGN=(CODE_SIGNING_ALLOWED=NO CODE_SIGNING_REQUIRED=NO CODE_SIGN_IDENTITY=)
else
  need "$TEAM_FILE" "the Apple Team ID"
  need "$KEY_FILE" "the App Store Connect API key description"
  TEAM=$(tr -d '[:space:]' < "$TEAM_FILE")
  KEY_ID=$(node -p "require('$KEY_FILE').key_id"); ISSUER=$(node -p "require('$KEY_FILE').issuer_id"); KEY_PATH=$(node -p "require('$KEY_FILE').key_path")
  need "$KEY_PATH" "the .p8 private key named by asc-api-key.json"
  AUTH=(-allowProvisioningUpdates -authenticationKeyPath "$KEY_PATH" -authenticationKeyID "$KEY_ID" -authenticationKeyIssuerID "$ISSUER")
  SIGN=(DEVELOPMENT_TEAM="$TEAM" CODE_SIGN_STYLE=Automatic "${AUTH[@]}")
fi

echo "building committed HEAD $SHA_FULL ($ENV) as $VER ($BUILD_NO) — uncommitted working-tree edits are NOT included"
WT=${TMPDIR:-/tmp}/build-ios-tf-$$
cleanup() { cd "$REPO" && git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; git worktree prune; }
trap cleanup EXIT
git worktree add --detach "$WT" "$SHA_FULL"
cd "$WT"
export EXPO_PUBLIC_GIT_SHA=$SHA_FULL
OUT=$REPO/app/dist/ios; mkdir -p "$OUT"
LOG=$OUT/build.log; : > "$LOG"
(cd app && pnpm install --offline --frozen-lockfile)
node -e "const f='$M/app.json',j=require('./'+f);j.expo.ios.buildNumber='$BUILD_NO';require('fs').writeFileSync(f,JSON.stringify(j,null,2)+'\n')"
cd "$M"
npx expo prebuild --platform ios --no-install
echo "export NODE_BINARY=$(command -v node)" > ios/.xcode.env.local
(cd ios && LANG=en_US.UTF-8 pod install) 2>&1 | tee -a "$LOG" | tail -5
WS=$(ls -d ios/*.xcworkspace | head -1); SCHEME=$(basename "$WS" .xcworkspace)
ARCHIVE=$WT/$M/ios/build/$SCHEME.xcarchive
t0=$(date +%s)
xcodebuild archive -workspace "$WS" -scheme "$SCHEME" -configuration Release -destination 'generic/platform=iOS' \
  -archivePath "$ARCHIVE" "${SIGN[@]}" >>"$LOG" 2>&1 \
  || { echo "xcodebuild archive failed ($LOG):" >&2; grep -E "error:|BUILD FAILED|ARCHIVE FAILED" "$LOG" | head -30 >&2; exit 1; }
if has --unsigned; then
  APP=$(ls -d "$ARCHIVE"/Products/Applications/*.app | head -1)
  echo "unsigned device archive built in $(( $(date +%s) - t0 )) s: $(lipo -archs "$APP/$(basename "$APP" .app)") $(plutil -extract CFBundleVersion raw "$APP/Info.plist") (not installable, not uploaded)"
  exit 0
fi
DEST=upload; has --no-upload && DEST=export
cat > "$WT/ExportOptions.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>method</key><string>app-store-connect</string>
  <key>destination</key><string>$DEST</string>
  <key>teamID</key><string>$TEAM</string>
  <key>signingStyle</key><string>automatic</string>
  <key>uploadSymbols</key><true/>
  <key>manageAppVersionAndBuildNumber</key><false/>
</dict></plist>
PLIST
xcodebuild -exportArchive -archivePath "$ARCHIVE" -exportOptionsPlist "$WT/ExportOptions.plist" -exportPath "$OUT/export" \
  "${AUTH[@]}" >>"$LOG" 2>&1 \
  || { echo "xcodebuild -exportArchive failed ($LOG):" >&2; grep -E "error:|EXPORT FAILED|Upload failed" "$LOG" | head -30 >&2; exit 1; }
# the archive's dSYMs are kept beside the build (crash symbolication, T352's source-map rule for native code)
SYM=$REPO/data/sourcemaps/$VER-b$BUILD_NO-${SHA_FULL:0:7}/ios; mkdir -p "$SYM"; cp -R "$ARCHIVE/dSYMs/." "$SYM/" 2>/dev/null || true
if [[ $DEST == upload ]]; then
  echo "$BUILD_NO" > "$LAST_FILE"
  echo "uploaded $VER ($BUILD_NO) to App Store Connect in $(( $(date +%s) - t0 )) s — TestFlight shows it after Apple's processing"
else
  echo "exported $(ls "$OUT"/export/*.ipa) in $(( $(date +%s) - t0 )) s (not uploaded; testflight-last-build unchanged)"
fi
