# T720 — iOS: no-stall bench `tap-stalls` on the simulator (IOS-3)

Status: DONE — 49b7226b6, 68bf25229. The iOS `tap-stalls` bench is ported and passes on the simulator at the Android 300 ms limit (worst stall 227 ms).

Closed by the 2026-11-20 audit: acceptance 'bench passes on the simulator with the same threshold' met: Phase 2 run `ios-benches` 5 of 5 steps PASS (68bf25229).

Source: owner 2026-11-14, verbatim: "Maybe we could make a handoff to start improving the ios versions? Making sure it is
rock solid too?" and "We want to do as much work as possible without the Apple account". Stub IOS-3 in
`handoff/PLAN/ios/START-HERE-ios.md` (approved to start there, "Unattended day"). Simulator only; no Apple account.

## Work
**IOS-3. No-stall bench on iOS (`tap-stalls`).** Goal: no JS stall of 300 ms or more at cold start, Salmon colouring,
Run timing tab, the bell. Acceptance: bench passes on the simulator with the same threshold. Size: S-M (needs `[stall]`
lines in the `--probes` build).
  Sub-steps: port `scenarios/stalls.mjs`; make `--probes` print `[stall]`/`[work]`; cover cold start, Salmon colouring, Run
  timing tab, the bell; fail at 300 ms; fix any iOS-only stall. Mirrors: `tap-stalls`.

## Acceptance
bench passes on the simulator with the same threshold.

Depends: T718. Size: S-M. Owner: no.

## Results

### Phase 1 (2026-11-14, iOS test-bed lane, code only: nothing run on the simulator yet)
**Probe lines on iOS:** `[stall]` (every JS hold > 50 ms) and `[work]` (>= 20 ms) come from `apps/mobile/index.js` /
`@app/data` openProbe on `EXPO_PUBLIC_PAN_PROBE=1`; `deploy/staging/build-ios-sim.sh` sets it for BOTH `--testbed` and
`--probes`, so no build change is needed. They reach the unified log as `[com.facebook.react.log:javascript]` lines
(ios-t313g, a 0.5.0 Release sim build: 357 `[stall]`, 210 `[work]`, 2400 `[heap]`). `run-ios.mjs` streams the log to
`app-os.log`; `lib/device-ios.mjs` now reads it the way the Android scenarios read logcat: `logcatClear()` (a mark in
the file, never a delete) and `logcatJs()` (JS lines since the mark).
**Ported:** `app/tools/testbed/scenarios-ios/stalls.mjs`, run by name `node app/tools/testbed/run-ios.mjs tap-stalls`
(not in `all`). Same steps and limit as `scenarios/stalls.mjs`: cold start watched 20 s, Salmon colouring
(`regs-paint-salmon`), Run timing tab (`tab` Runs), the bell (`open-bell`, closed by Done), back to Standard; fails at
`STALL_MAX` 300 ms. Extra guard: no `[heap]` line during the cold start fails the step (a non-probe build or no log
stream is not a pass). Twin, not shared: the Android file uses `input keyevent 4` and `net`. **Unify after the Android
tap/back work lands.**
**To prove (phase 2):** `HEAVY_LOCK=$HOME/.claude/heavy-job-2.lock scripts/heavy.sh testbed-ios 30 -- bash
app/tools/testbed/session-ios.sh --max-min 15 -- node app/tools/testbed/run-ios.mjs tap-stalls`.

### Phase 2 (2026-11-14): measured, PASS (run `ios-benches`, build 8ba815a23 `--testbed`, iPhone 17 sim)
`node app/tools/testbed/run-ios.mjs tap-stalls`: 5 of 5 steps pass; the limit is 300 ms, the same as Android.

| step | stalls > 50 ms (ms) | worst | work logged |
|---|---|---|---|
| cold start, 20 s watched | 227, 110, 134 | 227 | decodes, all sliced: paint 444, method 377, salmon-6 335, salmon-5 233, salmon-7a 205, salmon-1/2/3/7b 91-103 |
| Salmon colouring | none | 0 | none |
| Run timing tab | 169 | 169 | none |
| the bell | none | 0 | none |
| back to Standard | none | 0 | none |

- No iOS-only stall at or over 300 ms, so nothing was fixed. The 227 ms at the cold start falls inside the sliced decodes:
  the slices yield, so the longest single hold is the stall line, not the 444 ms wall time.
- Simulator caveat: these are Mac CPU times; a phone is slower by an amount not measured for iOS. The 227 ms cold-start
  stall is the one to re-check on a real iPhone (IOS-11).
