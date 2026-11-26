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
# Usage: deploy/staging/build-ios-testflight.sh [--production] [--no-upload | --unsigned] [--no-distribute] [--dry-run]
#          [--precheck]   (T978: only the precheck — every missing secret, tool or link listed with its fix — then stop)
#   (default)     staging data, the environment switch on — the build internal testers get (as the Play internal track)
#   --production  production data, no switch — the build that is later submitted for App Review (needs
#                 CONFIRM_PROD=canifishthis.ca, T433)
#   --no-upload   export the signed .ipa to app/dist/ios/ instead of uploading (Transporter or `xcrun altool` by hand)
#   --unsigned    a device (arm64, iphoneos) Release archive with NO signing, team or upload: proves the device build
#                 compiles and links without the account (Claude's check); the archive cannot be installed or uploaded
#   --no-distribute  (T918) upload only: the build is NOT added to the External TestFlight group (below)
#   --dry-run     print the steps and the secrets it would read; builds nothing, needs no account
#   --no-fast-fail=<reason> --no-ci-check=<reason> --no-notes=<reason>   (T993 plan 5) the escapes of the gates below,
#                 each printed; a bare one is refused; --no-rc-check=<reason> (T1062) is the release
#                 candidate check's
#
# (T993 plan 5, T661) THE GATES, before the heavy lock, the same as build-apk.sh's (deploy/lib/build-links.sh): the
# precheck lists every failure together (the tools, the secrets, the gitignored links, and — not for --unsigned —
# app.json's version having its changelog entry, the newest, and (T1062) on release/x.y.z HEAD being the newest
# release candidate's tree apart from the build numbers); then CI on HEAD (scripts/ci_status.py; --strict for
# --production), then scripts/fast-fail.sh, whose "changed" is everything since the last upload (app/dist/ios/last-sha).
# heavy.sh runs no fast-fail of its own: this script does.
#
# Secrets (never committed), in ~/.canifishthis/:
#   ios-team-id           the 10-character Team ID (developer.apple.com → Membership details)
#   asc-api-key.json      {"key_id": "…", "issuer_id": "…", "key_path": "/Users/…/AuthKey_<key_id>.p8"} — App Store Connect →
#                         Users and Access → Integrations → App Store Connect API → a Team key with the "Admin" role (an "App Manager" key archives but cannot sign for the store: "Cloud signing permission error", seen 2026-11-14)
#   testflight-last-build (written here) the last CFBundleVersion uploaded; each upload must be higher
#   testflight-external-group (T918, optional) one line: the External TestFlight group's name; without it the app's only
#                         external group is used (several: refused, naming them)
#   app-store-url         (T888, optional) the app's App Store page, https://apps.apple.com/app/id<number> (App Store
#                         Connect → the app → App Information → Apple ID). Baked in as EXPO_PUBLIC_APP_STORE_URL: without
#                         it the iPhone build has NO update check (no "Version X is out" popup, no App update in the bell).
# Before the first run, in App Store Connect: My Apps → + → New App, platform iOS, bundle id ca.canifishthis.app (register
# it at developer.apple.com → Identifiers if it is not offered), the name, primary language English (Canada), SKU any.
#
# The build number: max(app.json ios.buildNumber, testflight-last-build) + 1, set in the WORKTREE's app.json only (never
# committed: Play's versionCode keeps its own count); the version (CFBundleShortVersionString) is app.json's expo.version.
#
# (T888, T894) THE iPHONE'S VERSION FILES, after a successful upload: app/ios/<version>.json {version, build, sha, date,
# notes} — ONE PER VERSION — then app/ios/appstore.json (the same content: the last upload, what 0.8.1 / 0.8.2-before-T894
# installs read), in the bucket of the build's environment (staging: bc-fishing-regulations-staging, --production:
# bc-fishing-regulations). `notes` = this version's changelog bullets for the iPhone (core `notesOn`: an Android-only note
# is left out). The app reads <data origin>/app/ios/<the version the App Store offers>.json (core storeVersionFileUrl), so a
# TestFlight upload ahead of the store's version never hides the store version's notes. The twin of build-apk.sh's
# play/<versionCode>.json. A failed put says so and exits 1 (the build IS uploaded): redo both with
#   for f in <version>.json appstore.json; do npx --prefix app wrangler r2 object put --remote <bucket>/app/ios/$f \
#     --file app/dist/ios/appstore.json --content-type application/json --cache-control no-cache; done
#
# (T918) THE EXTERNAL GROUP, after the version files: deploy/staging/testflight-distribute.mjs waits for Apple's processing
# (every 30 s, up to 45 min, printed), answers export compliance from app.json, sets "What to Test" from this version's
# iPhone notes, adds the build to the External group and submits it for Beta App Review when Apple asks for one (the first
# build of a version); the last line says review pending / approved. Skipped with --no-distribute (and for --no-upload,
# --unsigned, --dry-run). A failure says so and exits 1 (the build IS uploaded and its files published): rerun with
#   node deploy/staging/testflight-distribute.mjs --version <version> --build <build> [--group "<name>"]
# (every step is a no-op when already done). It holds the heavy lane while it waits (light work: a poll every 30 s).
set -euo pipefail
cd "$(dirname "$0")/../.."
REPO=$PWD
has() { [[ " $ARGS " == *" $1 "* ]]; }
ARGS="$*"
if has --dry-run; then DRY=1; else DRY=; fi
if has --production; then PROD=1; else PROD=; fi
# (T433) a production build from a staging/ script asks the same confirmation as deploy/prod/
if has --production && ! has --precheck && [[ "${CONFIRM_PROD:-}" != canifishthis.ca ]]; then
  echo "build-ios-testflight.sh --production: set CONFIRM_PROD=canifishthis.ca to build production" >&2; exit 2
fi
SECRETS=$HOME/.canifishthis
M=app/apps/mobile
# (T993 plan 5) the gates and their escapes, shared with build-apk.sh
source deploy/lib/build-links.sh
build_reasons build-ios-testflight "$@"
if [[ -z ${BUILD_IOS_INNER:-} ]]; then
  # fast-fail's "changed" = everything since the last upload was built from, said (build_ff_base)
  build_ff_base build-ios-testflight "$(cat app/dist/ios/last-sha 2>/dev/null || true)" "the last TestFlight upload (app/dist/ios/last-sha)"
fi
# (T978) THE PRECHECK, before the heavy lock: every missing link (secrets, tools, the gitignored node_modules wrangler
# runs from) listed together with its fix, then one exit. Was: the first missing secret ended the run, after the lock
# wait. `--precheck` runs only this; a dry run lists it and goes on.
if [[ -z ${BUILD_IOS_INNER:-} ]]; then
  set +e
  pf=()
  bad() { pf+=("$*"); echo "  FAIL $*"; }
  ok() { echo "  ok   $*"; }
  echo "build-ios-testflight: precheck (before the heavy lock)"
  for t in xcodebuild pod node; do command -v $t >/dev/null && ok "$t" || bad "no $t on PATH (Xcode, CocoaPods: brew install cocoapods, Node)"; done
  # (T993 plan 5) what fast-fail and the CI check read (.venv, app/node_modules, the review app's when it is typechecked)
  build_links_common
  build_links_frontend "$FFB"
  if has --unsigned; then ok "changelog: not asked (--unsigned: nothing is uploaded)"; else build_check_changelog; fi
  if has --unsigned; then ok "release candidate: not asked (--unsigned: nothing is uploaded)"; else build_check_rc; fi
  if ! has --unsigned; then
    for f in ios-team-id asc-api-key.json; do
      [[ -s $SECRETS/$f ]] && ok "$SECRETS/$f" || bad "no $SECRETS/$f (this script's header says what it holds)"
    done
    if [[ -s $SECRETS/asc-api-key.json ]]; then
      KP=$(node -p "require('$SECRETS/asc-api-key.json').key_path" 2>/dev/null)
      [[ -n $KP && -s $KP ]] && ok "the .p8 key ($KP)" || bad "the .p8 private key asc-api-key.json names (key_path '${KP:-none}') is not there"
    fi
  fi
  if ! has --no-upload && ! has --unsigned; then
    if [[ -f app/node_modules/.modules.yaml ]]; then
      [[ $(npx --prefix app wrangler whoami 2>&1) == *"You are logged in"* ]] && ok "Cloudflare (wrangler) login" \
        || bad "wrangler is not logged in (npx --prefix app wrangler login): the version files would fail AFTER the upload"
    fi
  fi
  [[ -s $SECRETS/app-store-url ]] && ok "$SECRETS/app-store-url" \
    || echo "  note no $SECRETS/app-store-url: the build has NO update check (the header); not a stop"
  set -e
  if (( ${#pf[@]} )); then
    if [[ -n $DRY ]]; then echo "[dry-run] WOULD STOP: ${#pf[@]} precheck failure(s)"
    else echo "build-ios-testflight: precheck FAILED (${#pf[@]}) — the lock was not taken, nothing was built" >&2; exit 1; fi
  else echo "build-ios-testflight: precheck ok"; fi
  has --precheck && exit 0
  # (T993 plan 5) CI on HEAD, then fast-fail, before the lock (build-links.sh build_gates)
  build_gates build-ios-testflight ${PROD:+--strict}
fi
if [[ -z ${BUILD_IOS_INNER:-} && -z $DRY ]]; then
  export BUILD_IOS_INNER=1 BUILD_PRECHECK_SHA=$(git rev-parse HEAD)
  exec scripts/heavy.sh build-ios-testflight 20 -- "$0" "$@"
fi
if has --production; then
  export EXPO_PUBLIC_APP_ENV=production; unset EXPO_PUBLIC_ENV_SWITCH; ENV=production; BUCKET=bc-fishing-regulations
else
  export EXPO_PUBLIC_APP_ENV=staging EXPO_PUBLIC_ENV_SWITCH=1 EXPO_PUBLIC_MAP_LAB=1; ENV=staging
  BUCKET=bc-fishing-regulations-staging
fi
# (T888) the App Store page: the iPhone's update channel (core appUpdateChannel); none without it
STORE_FILE=$SECRETS/app-store-url
if [[ -s $STORE_FILE ]]; then export EXPO_PUBLIC_APP_STORE_URL=$(tr -d '[:space:]' < "$STORE_FILE")
else unset EXPO_PUBLIC_APP_STORE_URL
  echo "build-ios-testflight: no $STORE_FILE — this build has NO update check (no new-version popup or bell row; see the header)" >&2
fi
# (T888) this version's changelog bullets for the iPhone, as JSON (build-apk.sh's notes_json, the iPhone's list)
ios_notes() { node -e 'import(process.argv[1]).then((m)=>{const e=m.CHANGELOG.find((x)=>x.version===process.argv[2]);process.stdout.write(JSON.stringify(e?m.notesOn(e,"ios"):[]))})' "$1/app/packages/core/src/changelog.ts" "$2"; }
export EXPO_PUBLIC_CONTACT=${EXPO_PUBLIC_CONTACT:-support@canifishthis.ca}
# a store build never carries the test bed's switches
unset EXPO_PUBLIC_PAN_PROBE EXPO_PUBLIC_TEST_REPORTS EXPO_PUBLIC_UPDATE_CHANNEL
# (T739) the App Store export signs `aps-environment` as production: the phone's push token is registered as production
# (src/pushToken.ios.ts), so the feeds worker sends it to APNs' production host
export EXPO_PUBLIC_APNS_ENV=production

need() { [[ -s $1 ]] || { echo "missing $1 — $2" >&2; exit 2; }; }
TEAM_FILE=$SECRETS/ios-team-id KEY_FILE=$SECRETS/asc-api-key.json LAST_FILE=$SECRETS/testflight-last-build
APP_BUILD=$(node -p "require('./$M/app.json').expo.ios.buildNumber")
LAST=$( [[ -s $LAST_FILE ]] && cat "$LAST_FILE" || echo 0 )
BUILD_NO=$(( (APP_BUILD > LAST ? APP_BUILD : LAST) + 1 ))
VER=$(node -p "require('./$M/app.json').expo.version")
# (T894) the version names its file (app/ios/<version>.json): only a plain x.y.z, the form the app reads (storeVersionFileUrl)
[[ $VER =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]] || { echo "build-ios-testflight: app.json's version '$VER' is not x.y.z" >&2; exit 2; }
SHA_FULL=$(git rev-parse HEAD)
[[ -n $DRY ]] || build_same_head build-ios-testflight "$SHA_FULL"
if [[ -n $DRY ]]; then
  echo "[dry-run] $ENV build of committed HEAD $SHA_FULL: version $VER, build $BUILD_NO (app.json $APP_BUILD, last uploaded $LAST)"
  for f in "$TEAM_FILE" "$KEY_FILE"; do [[ -s $f ]] && echo "[dry-run] found $f" || echo "[dry-run] MISSING $f (see the header)"; done
  echo "[dry-run] git worktree add --detach \$WT $SHA_FULL; (cd \$WT/app && pnpm install --offline --frozen-lockfile)"
  echo "[dry-run] set expo.ios.buildNumber=$BUILD_NO in the worktree's app.json; expo prebuild -p ios --no-install; pod install"
  echo "[dry-run] xcodebuild archive -workspace ios/<Name>.xcworkspace -scheme <Name> -configuration Release -destination generic/platform=iOS"
  echo "          DEVELOPMENT_TEAM=<team> CODE_SIGN_STYLE=Automatic -allowProvisioningUpdates -authenticationKey{Path,ID,IssuerID}"
  echo "[dry-run] xcodebuild -exportArchive (method app-store-connect, destination $(has --no-upload && echo 'export → app/dist/ios/' || echo 'upload → App Store Connect / TestFlight'))"
  echo "[dry-run] update check: ${EXPO_PUBLIC_APP_STORE_URL:-NONE (no $STORE_FILE)}"
  has --no-upload || echo "[dry-run] after the upload: put $BUCKET/app/ios/$VER.json, then $BUCKET/app/ios/appstore.json, with notes $(ios_notes "$REPO" "$VER")"
  if has --no-upload || has --no-distribute; then echo "[dry-run] TestFlight External group: skipped ($(has --no-upload && echo --no-upload || echo --no-distribute))"
  else echo "[dry-run] then (T918) node deploy/staging/testflight-distribute.mjs --version $VER --build $BUILD_NO: wait for processing, add to the External group, Beta App Review if Apple asks"; fi
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
  echo "$SHA_FULL" > "$OUT/last-sha"   # (T993 plan 5) the FAST_FAIL_BASE of the next upload
  echo "uploaded $VER ($BUILD_NO) to App Store Connect in $(( $(date +%s) - t0 )) s — TestFlight shows it after Apple's processing"
  # (T888) the iPhone's version file (the header): this version's iPhone bullets, read once the App Store offers it
  NOTES=$(ios_notes "$WT" "$VER") \
    || { echo "build-ios-testflight: could not read $VER's iPhone notes; appstore.json was NOT published (the build IS uploaded): redo the put in the header" >&2; exit 1; }
  [[ $NOTES == "[]" ]] && echo "build-ios-testflight: no changelog entry for $VER — appstore.json has no notes (the app says none were published)" >&2
  printf '{"version":"%s","build":%s,"sha":"%s","date":"%s","notes":%s}\n' "$VER" "$BUILD_NO" "${SHA_FULL:0:7}" "$(date -u +%F)" "$NOTES" \
    > "$OUT/appstore.json"
  # (T894) this version's own file first (what the app reads, by the version the App Store offers), then appstore.json
  for f in "$VER.json" appstore.json; do
    (cd "$REPO" && npx --prefix app wrangler r2 object put --remote "$BUCKET/app/ios/$f" --file "$OUT/appstore.json" \
       --content-type "application/json" --cache-control "no-cache") \
      || { echo "build-ios-testflight: app/ios/$f was NOT published (the build IS uploaded): redo the puts in the header$(has --no-distribute || echo ", then: node deploy/staging/testflight-distribute.mjs --version $VER --build $BUILD_NO")" >&2; exit 1; }
  done
  echo "published $BUCKET/app/ios/$VER.json and $BUCKET/app/ios/appstore.json"
  # (T918) the External TestFlight group (the header)
  if has --no-distribute; then
    echo "TestFlight External group: skipped (--no-distribute); later: node deploy/staging/testflight-distribute.mjs --version $VER --build $BUILD_NO"
  else
    node "$WT/deploy/staging/testflight-distribute.mjs" --version "$VER" --build "$BUILD_NO" \
      || { echo "build-ios-testflight: the build was NOT given to the External TestFlight group (it IS uploaded and its version files are published): rerun" >&2
           echo "  node deploy/staging/testflight-distribute.mjs --version $VER --build $BUILD_NO" >&2; exit 1; }
  fi
else
  echo "exported $(ls "$OUT"/export/*.ipa) in $(( $(date +%s) - t0 )) s (not uploaded; testflight-last-build unchanged)"
fi
