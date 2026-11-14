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

### Baseline triage (test-bed lane, 2026-11-14; build 8ba815a23 `--testbed`, iPhone 17 sim, iOS 26.5)
Runs: `ios-baseline` (main lane, `all`, cut by its 40 min cap during `pdf`), then `ios-rest`, `ios-benches` and `ios-open`
(lane 2) for what failed or never ran. Shots: `handoff/PLAN/ios/shots/baseline/` (one per scenario, JPEG under 80 KB).

| scenario | result | cause | number |
|---|---|---|---|
| first-open | PASS | | 397 s to the map (split below) |
| map | PASS | | 5.4 % coloured, 39 % light grey |
| tap-water | PASS | | edge drag: 50 % / 400 ms stayed, 30 % / 900 ms went back (as at 0.5.0) |
| offline | 2 of 3 (steps 2 and 3 pass after the fixes; step 1 still fails in 3 re-runs) | **test bed:** (1) step 3: the shared flow's `tapOn "MAP\|Map"` hit another "Map" on iOS and the Cultus page reopened → `ios-open-updates-log.yaml`, no text tap (6572ee881); (2) step 1: the Developer card's log reaches the accessibility tree only after the flow's 15 s wait at a cold start (the screenshot shows it) → wait on the tree (1f3376368). Still failing (ios-open2): even 30 s more, the log never entered the tree on that first open, while step 3's open later in the same run read it. **Open: test bed, or an app accessibility gap in the test-only Developer card;** the next move is a scroll or a re-open inside the card | |
| satellite | PASS | | light grey 10.6 % → 5.6 % |
| location | 2 PASS, 1 SKIP | **app (parity):** no "At my location" row in iOS search (START-HERE flagged it) | 949 blue px at the centre |
| walkthrough | PASS | | 41 of 41 |
| pdf | PASS (re-run) | baseline: cut by the session cap | "Page 11 of 88" in PDFKit |
| crash | FAIL step 2 | **app or server, unconfirmed:** the next start read the record (`1 new … (CRASH_NATIVE), 1 bad`) but `sendExit` returned false on every start (POST /crash to the staging worker was made: CFNetwork lines). The app does not log the HTTP status. Likely the worker's per-IP limit (40 crash POSTs a day: many crash tests from this Mac today), not proven | |
| perf | PASS | | cold start → map 597 / 583 / 595 ms; tap → first frame 123 / 54 / 64 ms; pan lag max 18 ms, 0 stalls |
| memory | PASS (re-run) | baseline: never ran. **Test bed:** the rule-book link (now "This water in the rule book p.7 ↗") and the Salmon toggle's words had changed (59aaa4de8) | peak footprint 274 MB (0.5.0: 290); table in T725 |

**First open, 397 s (0.5.0: 168 s), split from the app's own log (`ios-baseline/app-os.log`):**
- **Release size: 479,277,504 bytes = 479 MB (457 MiB)**, from the app's `[download] mobile data: go (479277504 bytes)`.
  So "about 480 MB" is right; "about 500 MB" overstates it. Pieces: basemap 225.4 MB, atlas 197.2 MB, rule book 11.7 MB,
  the rest (regulations data etc.) ~45 MB.
- launch → map drawn **327 s**: 25 s before the big files started (the smaller files are not logged one by one; JS
  stalls up to 826 ms in that window); big files 257 s. **On average 479 MB in 282 s = 1.70 MB/s** (0.5.0: 492 MB in
  153 s = 3.2 MB/s). A single curl from this Mac
  to staging at 13:00 read 2.3 MB/s, so most of the doubling is the line today, not the app.
- **44 s after the last byte** hashing (`[setup] installed 44158 ms after the last byte`): the following hash (T272)
  could not keep up (atlas 40.6 s, basemap 44.1 s "to catch up").
- open + first draw 1 s. The remaining **~70 s are the test bed** (Maestro: the first-run notice, the walkthrough
  dismissal and `hierarchy` reads after the map was already up).
- **App finding (iOS only): 4,999 `TypeError: Cannot convert BigInt to number` lines during the download**, ~20 a second,
  from 10:22:37.850 to 10:26:54.569. Those are the exact times the three big files were downloading. expo-file-system's iOS
  `DownloadTask` gives `bytesWritten` as Int64, which reaches JS as a BigInt, and `install.native.ts:137`
  `onProgress: (p) => onBytes?.(have + p.bytesWritten)` adds it to a number. **Likely effect: the first-open progress on
  iOS does not move** (not seen: no shot during the download). Fix (shared `packages/data`, not this lane's):
  `Number(p.bytesWritten)`. Android is unaffected (its value is a number).

**Test bed cost:** every Maestro call (a flow or a `hierarchy` read) takes 10-25 s on this simulator (a JVM start plus the
XCTest driver), so the 11 scenarios need about 60 min, more than the 40 min cap given. Run them in two batches.
**Also seen:** the Developer card's log is missing from the accessibility tree for 15 s or more after a cold start, while
it is on screen. That card is test builds only, so it has no reader impact.
