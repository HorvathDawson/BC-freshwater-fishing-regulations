# T417 — Watershed conditions: no flicker between watersheds; pick the watershed most on screen

Status: DONE in code 2026-11-05 — 44ecbaad6 (0.6.1 batch; phone check with the next APK). Owner report: 2026-11-05 20:21
on 0.6.0 build 9, report dbc65e804d2692ba (More screen, regulations map, z8.6 at 49.134,-122.776).

"When moving between watersheds it flickers when switching the schedule view for the watershed conditions. Instead it
should load the new one in the background and only move over after it is ready. Also it should use which watershed is
most in screen, so it won't show none when you are centered next to the watershed on an ocean for example."

## Work
1. Keep the current watershed's schedule on screen until the next one has loaded, then swap (no empty/loading frame).
2. Choose the watershed with the largest share of the visible map (not the one under the centre point); "none" only when
   no watershed is on screen. One load path (memory one-load-path-per-screen): the same result from pan, search or tap.
3. Measure on the emulator: pan from the Fraser lowland onto the Strait; no blank frame, the watershed stays.
Size: S–M. Owner: phone check on the next APK.

## Done 2026-11-05

**No flicker (`@app/ui` `basinDays.ts`).** The flicker was `useBasinDays` returning `loading` from the moment the map's
group changed until the new group's days were read. It now keeps the last group's days on screen — under that group's
own name and title, never the new one's — and swaps in the render the new days arrive. `none` (no group on the map) is
said at once; a different quantity (flow ↔ level) is still a new question (the old days would answer another one). The
days answer now carries its quantity too: the render right after a new question still held the last answer, which could
pass as the new one for one frame.

**The group most on screen (`@app/data` `BasinGrid.mostIn`, `source.basinIn`; `@app/ui` `useViewBasin`).** The browse
group is the one with the largest share of the visible map: every grid cell the bounds touch, weighted by cos(latitude);
ties go to the group under the centre, then the lower id; `null` only when no group holds any cell. Both maps now report
their visible bounds with the camera (`Camera.bounds` on `onMoved`: native from the region event, web `getBounds()`).
Pan, search and tap all end in that same settle, so there is one load path. Before the map first reports (the start
camera), the box is the start point. `useCentreBasin`/`noteCentre`/`centreBasin` were renamed to
`useViewBasin`/`noteView`/`viewBasin`. The browse "none" line is now "No watershed group is on the map".

**Measured on the live grid (r-5a5999c94588 `conditions`, phone view ~411 × 700 dp):**

| view | under the centre | most on screen |
|---|---|---|
| owner's report, z8.6 49.134,-122.776 | Lower Fraser | Lower Fraser |
| panned west onto the Strait, z8.6 49.15,-123.25 | none | Squamish |
| further onto the Strait, z8.6 49.05,-123.45 | none | Cowichan |
| Strait, z10 49.1,-123.35 | none | Lower Fraser |
| open Pacific, z8.6 48,-130 | none | none |

`mostIn` over a province-wide box (z5): 0.16 ms.

**Tests.** `data/basinGrid.test.ts` (+3: largest share, not the centre; ground-area weighting; a bad box throws),
`data/release/source.test.ts` (+1 basinIn), `ui/basin.render.test.tsx` (useViewBasin: asked by bounds, the owner's
sea case, off/on, no groups), `ui/basinDays.render.test.tsx` (NO FLICKER: every render from the first ready one is
ready and shows its own group's days, across a slow switch; none at once; quantity is a new question), the map adapter
contract (onMoved carries the bounds).

**Not verified on a device.** The emulator check in step 3 was not run. The test bed needs a fresh test-bed APK built
with this code (`session.sh --build`), and this lane was told not to build an APK. To run it with the next APK: on
Conditions, pan from the Fraser lowland onto the Strait. The day list should never blank, and it should stay on a group.
