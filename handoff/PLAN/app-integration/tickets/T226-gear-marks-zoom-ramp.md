# T226 — Bait-ban / fly-only marks: fade in with zoom

Status: DONE (6db63d0e) — owner 2026-10-28: the T213 marks (ink edge under the status line, dashed for a bait ban) look very
aggressive zoomed out.

## Change
`app/packages/map/src/paint.ts` `gearPaint` (web + native share it; contract.ts repaints through it):
- opacity: top-level zoom `interpolate`, the day's `case` inside each stop (zoom may only feed a top-level interpolate);
- edge / ring width: the stream's own width curve plus `2 * GEAR_EDGE_PX * rampAt(edge, z)`; lake ring a zoom curve;
- dash: `step` on zoom, twice as long below z11 (a thin line with a 1.2 dash is shimmer).
- Two ramps in `GEAR_RAMPS`, picked by `GEAR_RAMP`:
  `gentle` (default): opacity 0 at z8, .3 z9, .6 z11, 1 z14; edge 35% -> 65% (z11) -> 100% (z14).
  `late`: nothing before z9.5, half at z10.5, T213's look from z12.
- Colours/tokens unchanged (`color.gear.edge`, `dash.gear.bait`); no-access dots and selection halo untouched. At
  full strength (z14+) the look is exactly T213's.

## Shots
`handoff/PLAN/app-integration/T226-shots/{before,gentle,late}-kootenay-z{7,9,11,13,15}-{light,dark}-{desktop,phone}.png`
(Elk Valley / Region 4, :39218). No fly-only stream was found in the Vedder area on the current day, so fly-only
(solid) is covered by the same code path, not a shot.

## Tests
`method-paint.test.ts` updated (+ ramp test); `app/packages/map` + `app/tools` suites green.
