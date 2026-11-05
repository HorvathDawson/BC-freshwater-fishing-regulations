# T390 — The app notices it is not the latest version and offers an Update button

Status: PARTIAL — 0798fd27b, c62aeb4b8 (code + tests merged; left: the emulator/phone checks with the next APK, below).

The app should know when a newer version of itself exists and let the reader update from inside it, with one button.

- **Android:** Google Play's in-app updates (Play Core "flexible" update: the download runs in the background, then a
  "Restart to update" button; "immediate" only when the data needs it). A well-established Expo-compatible package
  (owner rule: a better established package beats a minimal one). It works only for installs from Play (internal,
  Closed, production); a sideloaded staging APK points to the staging APK page instead.
- **iOS** (later, with T88): there is no in-app install, so the button opens the App Store page. The newer version
  comes from the store lookup.
- **When the data needs it:** a release whose `minAppVersion` is above this app (the staging releases since 0.5.0) is
  refused today and the app keeps its old data silently for the reader. That case must say so ("New data needs the latest
  app") with the same Update button. It is the strong prompt; a plain newer version is the gentle one.
- **Where:** the update notice the app already shows (the bell / updates sheet), never a modal on open; asked again at
  most once a day; offline, it says nothing.
- Failures are shown + `reportFailure` (T368); one load path.

Check first: what the update sheet says today when a release is refused for `minAppVersion` (is the reader told at all?).
Test: a release with `minAppVersion` above the app (deploy/staging/test-updates.mjs `min-app <semver>`) shows the
strong prompt; the Play flow on an internal-testing install (emulator with Play services or the owner's phone).

## Check first (answered 2026-11-04)

Before T390 the reader WAS told, but quietly: the bell's "Data update" row said "New regulations need a newer version of
the app." with "Update the app" (store link in production, the APK page in staging; no button at all when no link was
set), it counted on the badge, and More → Updates said "Update the app to get new regulations". A first start with no
data showed that line with no button. Nothing checked whether a newer app existed.

## Done (0798fd27b, c62aeb4b8)

- **Package: `expo-in-app-updates` 0.13.0** (pinned; reasons in `app/deps.md`). Compared with
  `sp-react-native-in-app-updates` 2.0.0: that one needs `react-native-device-info` (pinned 10.3.0, 2022) and
  `react-native-siren` as extra native modules; this one is an Expo module (new-arch native, autolinked, no config
  plugin), has no runtime deps, ships a web stub, has more use (472k vs 261k downloads/month) and released 2026-10-20.
  Built against SDK 58: the SDK 57 core APIs it uses exist (checked in source), the first APK build is the proof.
- **core `appUpdate.ts`**: `appUpdateChannel` (play / app-store / apk-page / none, from the build's
  `EXPO_PUBLIC_APP_DISTRIBUTION`), `shouldCheckAppUpdate` (once a device day, never offline), `apkIsNewer` (the staging
  page's `latest.json`), `appUpdateVM` (gentle "A new version of the app is available." / strong "New data needs the
  latest app.", one Update button, what it did or why it failed). Words: `APP_UPDATE_WORDS`. `releaseNoticeVM` no longer
  carries app-too-old: the strong prompt is the bell's "App update" section, first in the sheet.
- **ui `useAppUpdate`**: one load path (on opening, back online, day turn); the answer kept in `app-update.json`;
  failed check/start shown + `reportFailure` (the connection's shown, never reported; offline says nothing).
- **Bell**: `InSeasonNotice` "App update" section; both prompts count on the badge and lead the bell's label. Never a
  modal. A first start refused for an old app shows the same Update button (`boot-app-update`).
- **apps/mobile `appUpdater.native.ts`**: Play = in-app update (flexible; immediate for the strong prompt when Play allows
  it); apk-page = `latest.json` + opens the page; **iOS is a stub until T88**: no channel until
  `EXPO_PUBLIC_APP_STORE_URL` names the listing (then the store lookup + the listing). `appUpdater.web.ts`: none (no
  package in the web bundle).
- **build-apk.sh**: `EXPO_PUBLIC_APP_DISTRIBUTION=apk-page` (sideload) / `play` (`--play`); publishes
  `app/android/latest.json` {version, build, sha, date} beside the page.
- Tests: `core/src/appUpdate.test.ts`, `ui/src/appUpdate.render.test.tsx`, `ui-native/src/InSeason.test.tsx` (T390 block).

## Deviation (owner may want to know)

The package completes a FLEXIBLE update itself once the download finishes (Play installs and restarts the app) — there
is no separate "Restart to update" button. The reader has pressed Update first, so it is asked for; a two-step flow
would need a patch to the package's Kotlin (`installStateListener`).

## Test bed — with the next APK (held for the batch)

1. Sideload staging APK: `node deploy/staging/test-updates.mjs min-app 99.0.0`, open the app → bell badge; sheet's first
   section "App update / New data needs the latest app." + "Get the new test build" → opens the APK page.
   `node deploy/staging/test-updates.mjs reset` after.
2. Sideload staging APK older than the published `latest.json` (or edit it on staging) → gentle prompt, once a day
   (second opening the same day does not refetch: Updates log / network).
3. Airplane mode → no App update section; back online → checked.
4. Play internal-testing install (owner's phone or an emulator with Play) one build behind → gentle prompt; Update starts
   Play's flexible download, the app restarts into the new build. With `min-app` above it → Update starts the
   immediate flow.
5. The first APK build itself proves `expo-in-app-updates` 0.13.0 builds on SDK 57.

## Owner 2026-11-05: 0.6.0 did not notice 0.6.1 (uploaded ~15 min earlier)
Why (code as built): (1) `useAppUpdate` asks at most once per DEVICE DAY — a check earlier that day saved "current";
(2) on Play the check is Play's in-app update API, which says "available" only after the phone's Play Store refreshes
(minutes to hours after an upload); (3) the background "new app version" notice runs only when that alert is on, on
Android's WorkManager schedule. Our own `play.json` already said 0.6.1.
**Fix:** the foreground check reads OUR version file (`play.json` / `latest.json`, tiny, our server) on every opening and
every return to the app — no daily cap for it; when it names a newer version, the bell says so with its notes at once,
and the button uses Play's in-app update once Play agrees (until then: "Play is still preparing it — or open the Play
listing"). Play's own check keeps its daily cap. Test: a version file newer than the install shows on the next resume.
