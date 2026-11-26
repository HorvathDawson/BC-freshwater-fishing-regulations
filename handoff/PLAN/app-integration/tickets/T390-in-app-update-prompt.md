# T390 — The app notices it is not the latest version and offers an Update button

Status: PARTIAL — 0798fd27b, c62aeb4b8, a2b614425, 0b96e8848 (code + tests merged; shipped in 0.6.2 and 0.6.5); left (Claude): the "Test bed" checks 1-4 below on the current APK, including a Play internal install one build behind (old owner to-do lists it as "9. Claude: the in-app update prompt checked (T390, stalled since Nov 6) and T514.").

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T390 | The app notices a newer version of itself and offers an Update button (Play in-app update; strong prompt when a release needs a newer app) | PARTIAL (code + tests merged; left (Claude): test-bed checks 1-4 on the current APK) | 0798fd27b, c62aeb4b8, a2b614425, 0b96e8848 | owner 2026-11-04 (after 0.5.0); expo-in-app-updates; iOS stub until T88; left: emulator/phone checks with the next APK (ticket test bed) |

Source: owner 2026-11-04 (after 0.5.0). The app should know when a newer version of itself exists and let the reader
update from inside it, with one button: Android = Play in-app updates (Play installs only; a sideloaded staging APK points
to the staging APK page); iOS (later, with T88) = the App Store page; a release whose `minAppVersion` is above this app
gets the strong prompt ("New data needs the latest app"), a plain newer version the gentle one; shown in the bell /
updates sheet, never a modal on open; offline says nothing; failures shown + `reportFailure` (T368); one load path.

Done (0798fd27b, c62aeb4b8, a2b614425, 0b96e8848; shipped 0.6.2 / 0.6.5): `expo-in-app-updates` 0.13.0 (pinned, reasons
in `app/deps.md`); core `appUpdate.ts` (`appUpdateChannel` play / app-store / apk-page / none from
`EXPO_PUBLIC_APP_DISTRIBUTION`, `appUpdateVM`, `APP_UPDATE_WORDS`); ui `useAppUpdate` (asks on mount, resume, back online,
a tapped notice; no day cap); bell `InSeasonNotice` "App update" section, first start refused shows the same button
(`boot-app-update`); `apps/mobile/src/appUpdater.native.ts` (iOS stub until T88: no channel until
`EXPO_PUBLIC_APP_STORE_URL`); build-apk.sh publishes `app/android/latest.json`. Tests: `core/src/appUpdate.test.ts`,
`ui/src/appUpdate.render.test.tsx`, `ui-native/src/InSeason.test.tsx` (T390 block).
- Owner 2026-11-05 (0.6.0 did not notice 0.6.1): the check reads OUR version file (`play.json` / `latest.json`) on every
  opening and return; if Play does not offer it yet the button opens the Play listing ("Play is still preparing this
  version for your phone. The Play listing is open.", start state `listing`).
- Owner 2026-11-06, chose "only when Play has it": the Play check is Play's own `checkForUpdate`; `play.json` only names
  the version and its bullets once Play offers a build; a Play build has no background new-version notice
  (`notify.native.ts`). 0.6.3/0.6.4 installs keep the old behaviour until they update.
- Deviation: the package completes a FLEXIBLE update itself once the download finishes (Play installs and restarts) — no
  separate "Restart to update" button; a two-step flow would need a patch to the package's Kotlin
  (`installStateListener`).

## Work — test bed on the current APK

1. Sideload staging APK: `node deploy/staging/test-updates.mjs min-app 99.0.0`, open the app → bell badge; sheet's first
   section "App update / New data needs the latest app." + "Get the new test build" → opens the APK page.
   `node deploy/staging/test-updates.mjs reset` after.
2. Sideload staging APK older than the published `latest.json` (or edit it on staging) → gentle prompt (on every opening
   and return since 11-05: Updates log / network).
3. Airplane mode → no App update section; back online → checked.
4. Play internal-testing install (owner's phone or an emulator with Play) one build behind → gentle prompt once Play
   offers the build; Update starts Play's flexible download, the app restarts into the new build. With `min-app` above
   it → Update starts the immediate flow.
