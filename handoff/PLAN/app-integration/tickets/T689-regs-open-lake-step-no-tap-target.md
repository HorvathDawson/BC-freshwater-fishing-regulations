# T689 — Test bed `regs-open`: the lake step ("Cultus") finds no tap target, twice in a row

Status: OPEN.

Source: emulator smoke on build 29 (APK stamp 87d2eeac0), 2026-11-12: first run 9/10, rerun the same evening 9/10 (run
`app/tools/testbed/out/runs/2026-11-13T02-37-38`, shot 010; first run in `bcfish-session-backup-2026-11-12/smoke-b29/`).
The other scenario that failed first time, `water-nav` (step 7, "Maestro Android driver did not start up in time"), PASSED 7/7
on the rerun: a flake of the driver start-up, no ticket.

## What is seen
- Step 10, "lake: search Cultus, the map framed on it, the search closed", fails with "the app named no tap target for the
  water (no [target] line)". The map is framed on Cultus Lake correctly (shot 010: the lake drawn khaki, "Not fishable" in the
  key, an orange outline). The small and Fraser steps pass every time.
- It repeats exactly, so it is not a flake. Whether it is the app or the test bed is not decided.

## Where to look
- `app/packages/map/src/Map.native.tsx` `findTarget`: it returns with NO line when the lit set is empty or the map has no
  size yet (`if (!ids.size || !w || !h || !rendered.current) return;`), and logs `[target] {"none":true}` only after the grid
  found nothing; the scenario treats both as "no target". Since T478 the grid also skips every point with still water under
  it, and a lake is all still water: a lake whose every point is still water can never name a target. Check which of the three
  happens for Cultus (the logcat of the run holds it), and whether the lake is lit at all after the search.
- Separately, ask why a large, ordinary lake reads "Not fishable" in the key in November (the khaki fill). Cultus Lake has
  closures on parts; if the whole polygon reads not fishable that is a data/colour question for the lake, not a test-bed one.

## Acceptance
- `regs-open` passes 10/10 on the next APK, or the lake step is changed to what a lake can answer (a test bed change, with
  the reason written in the scenario). A test that fails before: the probe on a lake-only box.

Size: S. Owner: no.
