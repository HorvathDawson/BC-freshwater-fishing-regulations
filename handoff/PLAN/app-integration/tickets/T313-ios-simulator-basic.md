# T313 — iOS: build for the simulator and run the basic tests

Status: IN PROGRESS — build + 5/8 scenarios pass; one final run (with perf) held behind the APK 0.4.0 chain (owner 2026-10-31: "Xcode is installed. Could you set it up and do basic iOS tests and make sure it works as expected?").

## Work
1. `expo prebuild --platform ios` in a temp worktree (like build-apk.sh), pods, build a Debug/Release app for the iOS
   Simulator (no Apple developer account needed). MapLibre RN iOS build (see T217 binding patch).
2. iOS fallbacks for Android-only pieces: exit-info (crash reason) → no-op on iOS; bg-download → iOS background URLSession
   path (expo-file-system) already used; notification permission; location (expo-location iOS Info.plist strings).
3. Test bed on iOS: Maestro on the simulator (or simctl + screenshots): install/first open, map + tap a water → regs,
   offline, satellite, location (simctl location set), walkthrough, PDF open. Screenshots side by side with Android.
4. Script it (e.g. `deploy/staging/build-ios-sim.sh`, `app/tools/testbed` iOS target) so it repeats; heavy jobs via heavy.sh.

## Acceptance
iOS simulator build launches against staging; the basic scenarios pass or each failure is ticketed; screenshot pairs.

Depends: T217, T88. Size: M–L. Owner: no (App Store needs a $99 account later).

## Progress (2026-10-30, paused for a machine restart)
- `deploy/staging/build-ios-sim.sh`: committed HEAD in a temp worktree → pnpm offline → `expo prebuild -p ios` → pods →
  `xcodebuild` Release, iphonesimulator, arm64, no signing; STAGING + env switch (`--testbed` adds the test-bed flags).
  **Built in 120 s** (ba2217d58): `app/dist/ios-sim/canifishthis.app` (+ zip). The project is `CanIFish` (from expo.name).
- iOS fixes (app.json): the unused Always-location and motion usage strings dropped; prompts say "Can I Fish?" (the
  Xcode product name `CanIFish` showed via `$(PRODUCT_NAME)`). `exit-info` / `bg-download` already
  `requireOptionalNativeModule` (null on iOS): no code change.
- Test bed iOS target: `app/tools/testbed/session-ios.sh` (boot "iPhone 17" iOS 26.5, install, capped command, shut
  down only what it booted), `run-ios.mjs` (first-open, map, tap-water + edge-back, offline via "Simulate offline",
  satellite, location via `simctl location`/`privacy`, walkthrough, pdf), `lib/device-ios.mjs`, flows `tap-point`,
  `edge-back`, `ios-alert`.
- Found: Xcode 27.2 was installed without its first launch; `xcrun simctl` then hangs on `xcodebuild -runFirstLaunch`
  (admin password), and Maestro calls it at start even for Android — every Android flow hung. Owner ran the first
  launch; `lib/xcrun-shim.sh` (sourced by both sessions) stays as insurance.
- RESUME (one heavy job):
  `scripts/heavy.sh testbed-ios 25 -- bash -c 'deploy/staging/build-ios-sim.sh && bash app/tools/testbed/session-ios.sh --fresh --max-min 40 -- node app/tools/testbed/run-ios.mjs all --out app/tools/testbed/out/runs/ios-t313'`
  then shots → handoff/PLAN/app-integration/T313-shots/ beside the Android ones (T255/T277/T293/T303/T215-shots).

## Performance (owner 2026-10-30: "make sure it is performant on iOS too in testing")
Run the same timing scenarios as Android (test bed run timing / T317 pan numbers) on the iOS run: cold start → map
drawn, search → water page, water page open from a map tap, offline install, pan/fling frame times. Report beside the
Android numbers. Caveat in the report: the Simulator runs on the Mac's CPU/GPU, so its numbers flag regressions and
iOS-only stalls, not real-phone speed. A real iPhone run (free personal-team signing, 7-day install) is the follow-up.

## Runs (2026-10-30, iPhone 17 sim, iOS 26.5, staging release r-038e6818e80f)
Reports: `app/tools/testbed/out/runs/ios-t313{b,c,d}/index.html` (b: first run after the URLSession fix; d: latest).

| scenario | t313d | note |
|---|---|---|
| first-open | PASS | first open (492 MB) 153 s in b, 70 s to the map in d; About says "Data source: Staging" |
| map | PASS | waters coloured, labels, hatches (shot T313-map) |
| tap-water | 2/3 | search → page PASS; framed + map tap → Cultus page PASS; edge-back from the water's page FAILED once (y 50 %, 400 ms): the next run tries three heights/speeds — ticket if none go back |
| offline | 2/3 | Simulate offline on → Check now "failed (offline)", regs from the installed set PASS; "put back off" was a test bug (fixed) |
| satellite | PASS | imagery drawn (light grey 35 % → 4 %), Map base put back |
| location | PASS (1 skip) | revoke → "Location is off for this app…"; grant → blue dot centred, "Follow my location"; "At my location" row not in iOS search (skip, check) |
| walkthrough | FAIL → T332 | 16 of 25 walked; step 17's card over the Layers sheet is not in the accessibility tree |
| pdf | PASS | the rule book opens in iOS Preview (QuickLook: "synopsis-open", Back) — file:// via Linking works |

### Triage of ios-t313b (from the artifacts)
- Search matcher: test. iOS reads a result row as one label ("Cultus Lake, Lake · Region 2 …"), so `Lake · …` never
  matched from the start; now `.*Lindell Beach.*`.
- About "staging": test. The walkthrough card (opened by itself after the map drew) covered About; its Skip is labelled
  for VoiceOver, so the text "Skip" never matched → skipped by `id: guide-skip` (dismiss-first-run.yaml, Android too).
- Simulate offline: test. Menu pages are Modals from below (no edge swipe), the runner fell back to a cold start, which
  resets the (unkept) switch; the second press turned it on again. Menu pages now left by their "‹ Back"; the switch read first.
- Walkthrough 16/25: real, **T332**.
- PDF in b: test (a resumed walk reopened the Layers sheet; appUp now closes it). Passes in d.

### iOS bugs found and fixed here
- **First open failed** ("The map data could not be loaded."): expo-file-system's iOS DownloadTask defaults to a
  BACKGROUND URLSession (out of process; nothing on disk until the end; lost on a kill). Release files + rule book now
  ask for a foreground session (Android ignores the option) — b8e33ca80.
- **Menu pages under the status bar** ("‹ Back" under the clock: About, Updates log, …): the Modal's SafeAreaView
  measured mid-slide. iOS pads by the root provider's insets — 404a65da0 (needs the next run's shot to confirm).
- Permission prompts said "CanIFish" and carried unused Always-location / motion strings — fixed in app.json.

## Performance scenario (ready, not yet run)
`run-ios.mjs perf` on a `build-ios-sim.sh --probes` build (EXPO_PUBLIC_PAN_PROBE=1: the app's `[open]` `[pan]`
`[stall]` `[target]` lines reach the unified log, streamed to `app-os.log`): cold start ×3 → map drawn (screenshot poll,
±0.3 s) + RSS; Cultus framed, the map tapped at the app's `[target]` ×3, timed by `[open]` (first frame, SQL); 8 travelling
swipes → JS lag / stalls / tiles / feature-state acks + RSS peak; `T313-perf.json`. Native map frame times are not read
on iOS (no gfxinfo; Instruments is the follow-up). Simulator numbers = regressions / iOS-only stalls, not phone speed.

## RESUME (after the APK 0.4.0 chain releases the lock; one heavy job)
`scripts/heavy.sh testbed-ios 25 -- bash -c 'deploy/staging/build-ios-sim.sh --probes && bash app/tools/testbed/session-ios.sh --max-min 50 -- node app/tools/testbed/run-ios.mjs all --out app/tools/testbed/out/runs/ios-t313e'`
then: shots → T313-shots/ beside Android (T255 run-final 002/151, T277 satellite-online-head, T303 phone-granted-03,
T293, T215), edge-back verdict (ticket if it fails), perf numbers beside T245-perf.json, close.
