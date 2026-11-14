# T725 — iOS: memory table and cold start re-measured on 0.7.9 (IOS-8)

Status: OPEN.

Source: owner 2026-11-14, verbatim: "Maybe we could make a handoff to start improving the ios versions? Making sure it is
rock solid too?" and "We want to do as much work as possible without the Apple account". Stub IOS-8 in
`handoff/PLAN/ios/START-HERE-ios.md` (approved to start there, "Unattended day"). Simulator only; no Apple account.

## Work
**IOS-8. Memory and cold start on a real-class device.** Goal: the footprint table re-run on 0.7.9; the memory-warning
trim measured; cold start to map. Acceptance: table in T363 shape; a verdict on the oldest supported iPhone (needs the
minimum-version decision and, ideally, a real older phone). Size: S-M.
  Sub-steps: re-run `memory.mjs` on 0.7.9 and diff with T363; confirm the `memoryWarning` trim (`simctl` memory warning) frees
  what it claims; cold start x3 via `perf`; record minimum-iOS number; no device claims. Mirrors: `memory`, `perf`,
  T360 table.

## Acceptance
table in T363 shape; a verdict on the oldest supported iPhone (needs the minimum-version decision and, ideally, a real older phone).

Depends: T718. Size: S-M. Owner: yes (decision 2: minimum iOS). Default: what Expo SDK 58 gives, number recorded.

## Results

### 2026-11-14, iOS test-bed lane: 0.7.9 on the simulator (build 8ba815a23 `--testbed`, iPhone 17, iOS 26.5)
Run `ios-rest` (`run-ios.mjs memory perf`), the T363 method (`footprint` of the simulator process, MB; JS from `[heap]`).
Simulator = the Mac's CPU/GPU: graphics reads 0 here (the Mac's GPU), so these are the CPU-side numbers only.

Two runs of 8ba815a23, `ios-rest` (12:27) and `ios-benches` (13:20), MB, shown as rest / benches.

| step | footprint 0.5.0 (T363) | footprint 0.7.9 | malloc 0.7.9 | RSS 0.7.9 | JS heap 0.5.0 → 0.7.9 | JS external |
|---|---|---|---|---|---|---|
| boot | 147 | 164 / 135 | 76 / 45 | 402 / 481 | 40 → 60 / 56 | 47 / 44 |
| 8 pans | 170 | 194 / 154 | 83 / 48 | 478 / 521 | 44 → 60 / 64 | 53 / 51 |
| satellite on, 4 pans | 178 | 205 / 164 | 85 / 49 | 520 / 546 | 52 → 72 / 72 | 61 / 59 |
| water page (Cultus) | 232 | 230 / 213 | 67 / 48 | 626 / 654 | 108 → 116 / 120 | 33 / 33 |
| rule book open / closed | 290 / 241 | - ; 271 / 222 | - ; 110 / 54 | - ; 854 / 839 | 108 → 120 | 33 |
| rule book 2nd open / closed | 238 / 241 | - ; 218 / 222 | - ; 56 / 53 | - ; 867 / 851 | 108 → 120 | 33 |
| Conditions | 265 | 242 / 230 | 71 / 53 | 652 / 869 | 108 → 132 / 132 | 44 / 44 |
| Map tab after Conditions | 270 | 243 / 273 | 67 / 95 | 657 / 771 | 116 → 136 / 132 | 31 / 32 |
| Salmon | 273 | 248 / 274 | 67 / 94 | 662 / 781 | 116 → 136 / 132 | 33 / 33 |
| after a memory warning | 273 | 248 / 271 | 68 / 92 | 662 / 780 | 116 → 136 / 132 | 33 / 34 |

- **Peak footprint 274 MB on 0.7.9, against 290 MB on 0.5.0** (0.5.0's peak was the rule book open). The two 0.7.9 runs
  differ by up to 40 MB on the same step (boot 164 vs 135), so read a single step as about ±30 MB. The JS heap is 16-28 MB
  above 0.5.0 throughout: more release data is held since 0.6.x.
- **The rule book does not leak:** open 271, closed 222; a second open and close ends at 222 again (0.5.0: 241 / 241).
- **The memory-warning trim is NOT tested.** `simctl spawn <udid> notifyutil -p UISimulatedMemoryWarningNotification`
  (the scenario's way) did not reach the app: no `[trim]` line in the app log in either run (`memoryTrim.native.ts` logs one per
  warning), and nothing was freed. The Simulator's Debug → Simulate Memory Warning is a GUI menu item; the headless
  simulator the test bed boots has no window to send it to. No command-line way was found. A phone (or a Simulator
  window driven by hand) is the check.
- **Cold start → map drawn (`perf`, 3 runs): 597 / 583 / 595 ms** (screenshot polling, ±0.3 s). 0.5.0: ~0.61 s warm.
  RSS 444-447 MB.
- Map tap → page first frame (`perf`, Hope Slough at the start view): 123 ms cold, 54 / 64 ms warm (sql 13 / 3 / 3).
- Pan (8 swipes): JS lag max 18 ms, 0 stalls > 50 ms, RSS peak 591 MB (0.5.0: lag 17 ms, 0 stalls, peak 609-613 MB).
- Minimum iOS: 16.4 (T718). No device verdict: simulator only.
