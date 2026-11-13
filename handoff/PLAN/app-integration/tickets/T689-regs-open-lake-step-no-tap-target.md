# T689 — Test bed `regs-open`: the lake step ("Cultus") finds no tap target, twice in a row

Status: IN PROGRESS — the probe is fixed in the code (606037505); left (Claude): the `regs-open` 10/10 rerun on the
next test-bed APK (no APK may be built in the lane that fixed it).

Source: emulator smoke on build 29 (APK stamp 87d2eeac0), 2026-11-12: first run 9/10, rerun the same evening 9/10 (run
`app/tools/testbed/out/runs/2026-11-13T02-37-38`, shot 010; first run in `bcfish-session-backup-2026-11-12/smoke-b29/`).
The other scenario that failed first time, `water-nav` (step 7, "Maestro Android driver did not start up in time"), PASSED 7/7
on the rerun: a flake of the driver start-up, no ticket.

## What is seen
- Step 10, "lake: search Cultus, the map framed on it, the search closed", fails with "the app named no tap target for the
  water (no [target] line)". The map is framed on Cultus Lake correctly (shot 010: the lake drawn khaki, "Not fishable" in the
  key, an orange outline). The small and Fraser steps pass every time.

## Found (2026-11-12, third run on build 29, lane 2, logcat kept)
- **A test-bed probe defect, not a user-facing one.** The logcat holds `[target] {"none":true,…}`: the probe ran, searched
  400 points and accepted none. Since T478 (6b5f9039e, 2026-11-07) `findTarget` skipped every point with ANY still water under
  the tap box, and a lake is all still water, so no lake could ever be a target. The last regs-open run before T478
  (2026-11-06T16-46-03) passed the lake step (target at 567,1136 px); the first run after it (build 29) fails. The scenario's
  message ("no [target] line") was wrong too: it said the same thing for "searched, found none".
- **Hand taps open the lake.** Four taps on Cultus Lake on the same screen (build 29), each `[tap] … hit: {layer: "lake",
  id: 1360469}` then `[open]` with the page "Cultus Lake · Regulations" drawn in 44–58 ms. A person tapping a lake, or
  opening one from search, is not affected. Only `regs-open` has a lake case; `water-nav`, `part-steps`, `pane` target rivers.
- **"Not fishable" is not what the lake is drawn as.** Cultus Lake has no closure in r-0a9930fb8826 (row `r2:cultus_lake@2-3`:
  wild trout over 50 cm release, 1 bull trout over 60 cm, kokanee release, vessel rules): it is "Own rules". The fill is the
  own-rules orange #D08404 at the lake opacity 0.6 over the basemap's cyan water (128,210,221): computed (176,163,91), on
  screen (174,166,97). The "Not fishable" swatch (#B1AF8B at 0.9, sewage/tailings/dumps only) is a FIXED row of the key and is
  shown on every map. The same colour is on the 2026-11-04 and 2026-11-06 Cultus shots: not new. That an own-rules lake reads
  khaki, close to the "Not fishable" swatch, is a look question (raised in the lane report for the owner), not a data defect.

## Fix
- `isTapTarget` (`app/packages/map/src/sections.ts`): a point is the lit water's when the tap there answers a lit section and
  every still-water feature under the box is lit too. Another water's still water still skips the point (T478 kept: a lit
  river at a lake's tip); the lit lake's own water no longer does. `Map.native.tsx` `findTarget` uses it (the web map has no
  target probe: nothing to change there). Test: `sections.test.ts` "(T689) the test bed's tap target" (fails before).
- `regs-open.mjs`: the failure says which of the two it was (no line: nothing lit / map not drawn; `{"none":true}`: searched
  and found none).

## Acceptance
- `regs-open` passes 10/10 on the next test-bed APK (left: Claude, on the next APK).
