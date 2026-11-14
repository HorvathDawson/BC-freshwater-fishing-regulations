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
(filled in as the work runs)
