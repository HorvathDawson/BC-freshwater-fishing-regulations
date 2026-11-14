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
