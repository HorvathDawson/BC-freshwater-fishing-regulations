# T725 — iOS: memory table and cold start re-measured on 0.7.9 (IOS-8)

Status: DONE — 68bf25229, 83653e704. Measured on 0.7.9; lane L6: the memory-warning trim proven on the simulator (the warning now reaches the app, `[trim]` 14 databases, 263 → 257 MB); the oldest iPhone: iOS 16.4+ kept (owner 2026-11-27)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T725 | iOS: memory table and cold start re-measured on 0.7.9 (IOS-8) | DONE (trim proven on the simulator; iOS 16.4+ kept, owner 2026-11-27) | 68bf25229, 83653e704 | — |

Source: owner 2026-11-14, verbatim: "Maybe we could make a handoff to start improving the ios versions? Making sure it is
rock solid too?" and "We want to do as much work as possible without the Apple account". Stub IOS-8 in
`docs/agents/START-HERE-ios.md` (approved to start there, "Unattended day"). Simulator only; no Apple account.

## What
**IOS-8. Memory and cold start on a real-class device.** The footprint table re-run on 0.7.9 (T363 shape; method: `footprint`
of the simulator process, `run-ios.mjs memory perf`; graphics reads 0 on the simulator, CPU-side numbers only); the
memory-warning trim measured; cold start to map; a verdict on the oldest supported iPhone.

Done (measured, simulator, iPhone 17, iOS 26.5):
- 0.7.9 (8ba815a23): peak footprint 274 MB against 290 MB on 0.5.0; two runs differ by up to 40 MB on one step (read a
  single step as about ±30 MB); JS heap 16-28 MB above 0.5.0 throughout. The rule book does not leak (open 271, closed 222,
  again 222). Cold start → map drawn 597 / 583 / 595 ms (0.5.0 ~0.61 s); map tap → page first frame 123 ms cold, 54 / 64
  warm; pan: JS lag max 18 ms, 0 stalls > 50 ms, RSS peak 591 MB. Minimum iOS: 16.4 (T718).
- After T717 (081d93f90, ad-hoc signed, every change since 8ba815a23, not like for like): water page 279 MB (+49 to +66);
  peak 389 MB (the rule book's second open). **New, to look at:** with the page up, closing the rule book frees only
  10 MB (348 → 338; 389 → 343) where the baseline freed about 50 MB; malloc stays about 150 MB against 54; once the page is
  left the footprint drops to 158 MB. About 180 MB is held while a page is up and the book has been opened; the test bed
  cannot split it between PDFKit and the kept map, Instruments (Allocations) on the simulator would.
- Memory-warning trim proven (lane L6, 83653e704, 2026-11-20): `app/tools/testbed/lib/sim-memory-warning.js` calls
  CoreSimulator's `-[SimDevice simulateMemoryWarning]` by UDID (JXA; the old `simctl spawn … notifyutil` never reached the
  app; System Events is not authorised, `-1743`); `device-ios.mjs` `memoryWarning()` uses it; the `memory` scenario's
  warning step now FAILS without the app's `[trim]` line. Run on iPhone 17e: `[trim] iOS memory warning, 14 database(s)
  shrunk, 0 idle closed`; footprint 263 → 257 MB, malloc 86.3 → 78.3, JS external 60.9 → 33.3 (JS heap 124 unchanged).
  The rule book steps failed in that bench (its `scroll-to` did not find the link on this build): no PDF numbers.

## Work (left)
- The verdict on the oldest supported iPhone (owner, decision 2; the simulator says nothing about a phone's memory limit;
  ideally a real older phone). No device claims.

## Acceptance
table in T363 shape; a verdict on the oldest supported iPhone (needs the minimum-version decision and, ideally, a real older phone).

Depends: T718. Size: S-M. Owner: yes (decision 2: minimum iOS). Default: what Expo SDK 58 gives, number recorded.

## Owner

- [x] 2026-11-14 — The oldest iPhone / iPad the app supports: today's build needs iOS 16.4+, iPhone only. Keep, or widen? Default: keep. → "lets see if older phones work at all with emulation"; then, after the scoping (the floor is Expo SDK 58's 16.4, which covers iPhone 8 / X and newer; only the iOS 26.5 simulator is installed; night notes `owner-questions/T725/`): "Keep iOS 16.4+, close it (Recommended)" (2026-11-27)
