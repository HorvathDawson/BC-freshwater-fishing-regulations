# T718 — iOS: build 0.7.9 for the simulator and record the baseline (IOS-1)

Status: IN PROGRESS (iOS chat, main lane).

Source: owner 2026-11-14, verbatim: "Maybe we could make a handoff to start improving the ios versions? Making sure it is
rock solid too?" and "We want to do as much work as possible without the Apple account". Stub IOS-1 in
`handoff/PLAN/ios/START-HERE-ios.md` (approved to start there, "Unattended day"). Simulator only; no Apple account.

## Work
**IOS-1. Build and run on the simulator; record the baseline.** Goal: know today's truth on 0.7.9. Do: `heavy.sh` ->
`deploy/staging/build-ios-sim.sh --testbed`, then `session-ios.sh --fresh --max-min 40 -- node app/tools/testbed/run-ios.mjs all`;
fix test-bed drift only (not app behaviour yet). Acceptance: report with the 11 existing scenarios, build time, first-open
time, memory table, shots; every failure listed with cause (app vs test bed). Size: M (one heavy run + drift fixes).
  Sub-steps: (a) `rtk git pull`, check `heavy.sh --status`; (b) build with `--testbed`, then again with `--probes` for perf/memory;
  (c) run all 11 scenarios, triage each failure app vs test bed; (d) fix bed drift (hard-coded tab/back coordinates in
  `run-ios.mjs`, Maestro ids); (e) record minimum iOS from the generated Podfile; (f) shots into `handoff/PLAN/ios/shots/`;
  (g) list every Android scenario with no iOS twin (feeds IOS-3..6). Mirrors: Android `run.mjs all`.

## Acceptance
report with the 11 existing scenarios, build time, first-open time, memory table, shots; every failure listed with cause (app vs test bed).

Depends: none. Size: M. Owner: no.

## Results
- **Build (2026-11-14, HEAD 8ba815a23, 0.7.9 build 31):** `deploy/staging/build-ios-sim.sh --testbed` passed first time, no
  fix needed after two weeks of app changes. `xcodebuild` 122 s; the whole job (worktree, pnpm, prebuild, pods, compile)
  under 10 minutes on heavy lane 2. Xcode 27.2, iphonesimulator, arm64, Release, no signing.
- **Minimum iOS: 16.4** (`MinimumOSVersion` in the built `Info.plist`; not set in `app.json`, so it is what Expo SDK 58 /
  RN 0.88 give). Answers the handoff's "unknown". Owner decision 2 keeps this as the default.
- Display name in the build: "Can I Fish?" (`CFBundleDisplayName`), version 0.7.9, build 31.
- Rotation: `expo.orientation` is already `"portrait"` in `app.json` (checked 2026-11-14); T726 confirms it on the build.

### (g) Android scenarios with no iOS twin (test-bed lane, 2026-11-14, read from the code)
Twins today (`run-ios.mjs`): install → `first-open` (no mid-download kill), offline, satellite, crash, memory,
walkthrough, locate → `location` (partial), perf (JS lag only, no frame times), regs-open + tap-stalls (new, T719/T720).
Every Android scenario also leans on `scenarios/common.mjs` `coldStart`/`toMap`/`openLog`, which send `input keyevent
4`: none runs unchanged until those take a device Back (`run-ios.mjs` `toMap` is the iOS one). Helpers below are
iOS device/runner pieces to add; "ctx" = runner pieces `run-ios.mjs` lacks (`ctx.until`, `ctx.hasId`, `ctx.timer`,
`ctx.updates` = the test channel, `ctx.sync`).

| scenario | verdict | what it needs on iOS |
|---|---|---|
| near-cold | portable once Back + ctx | Back, `ctx.until/hasId`; logcat → `logcatJs` (done) |
| gauge-offline, report | helper | `net('offline')` → the app's "Simulate offline" (Updates log); report reads the worker host-side |
| feeds, retire-swap | helper | `ctx.updates` (host R2 writes; the `--testbed` iOS build reads channel `test`), `openLog` via Back |
| update-freeze | helper | `ctx.updates`, a swipe-at flow, `logcat -v epoch` → `logTime` |
| big-update | part Android-only | metered wait: the simulator cannot be on mobile data; minAppVersion / sha-mismatch parts portable with `ctx.updates` |
| bg-install | Android-only | WorkManager background download + adb; iOS uses a foreground URLSession: own scenario in IOS-5 (T722) |
| spots | helper | `addPhoto` → `simctl addmedia`, offline → Simulate offline, `ctx.sync`; `geo` exists |
| saved-spots, two-days, route-scroll | helper | `lib/screen.mjs` reads uiautomator nodes (bounds, label fit): an iOS node reader from `maestro hierarchy` frames; route-scroll a swipe-at flow |
| water-nav, part-steps | helper | node reader (uiautomator dump), tap-at (`ios-tap-at`), Back; water-nav's crash buffer → DiagnosticReports (runner has it) |
| switch, regs-switch | helper | node reader, tap-at, `screenrecord` → `simctl io recordVideo` |
| step-shots | helper | `cmd uimode` → `simctl ui appearance`, `am start` link → `simctl openurl`, node reader |
| pane (paneOpen, mapIdle) | helper | tap-at, Back, `screenrecord` → `simctl io recordVideo` (pane-frames.py reads any video) |
| back-flash | iOS twin to write | Android system Back (keyevent 4) is the subject; iOS: `water-back` / edge swipe (T721) |
| edge | iOS twin to write | Android edge swipe; iOS EdgeBack grid via swipe flows (T721, `edge-back*.yaml`) |
| feel, glide | helper | timed swipes (`input swipe` with ms): Maestro `swipe` with `duration`; camera probe is JS (portable) |
| photo-zoom | part Android-only | pinch is raw multi-touch as root; double-tap / drag / swipe parts portable via Maestro; photo via `simctl addmedia` |
| gestures, zoom-flick | Android-only | raw multi-touch written to /dev/input as root (pinch, two-finger tap); no iOS multi-touch injector here (Maestro has none, idb not installed) |
| pan, switch-pan | Android-only | `dumpsys gfxinfo` frame stats; iOS frame times need Instruments (`xctrace`), not wired |
| heapprof | Android-only | Perfetto heapprofd; iOS stand-in is `footprint` (the `memory` twin) |
| crash-bed | part portable | JS rejection / uncaught error parts portable (crash stub on localhost, the simulator shares it); native SIGSEGV part = iOS `crash` |
