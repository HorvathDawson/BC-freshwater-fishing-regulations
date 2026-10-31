# T313 — iOS: build for the simulator and run the basic tests

Status: IN PROGRESS — build works, scenarios not yet run (owner 2026-10-31: "Xcode is installed. Could you set it up and do basic iOS tests and make sure it works as expected?").

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
