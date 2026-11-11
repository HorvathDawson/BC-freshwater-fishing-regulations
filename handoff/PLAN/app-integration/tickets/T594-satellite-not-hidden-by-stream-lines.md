# T594 — Satellite view is not hidden by the stream lines

Status: IN REVIEW — dcfbf8f29 (look C built, web + phone from the one style source); the owner looks at the shots and picks the opacity

Source: owner problem report 2026-11-10 07:31 UTC (worker report 442a0ecc, build 24, z9.6 near Chilliwack): "we need to
figure out a way to have better satellite where it isn't blocked by the streams lines." (The same report's other half,
Indigenous land names, is T570.)

## Today
- With satellite on, the regulation-coloured stream lines draw at full weight over the imagery, so the water itself
  cannot be seen. Not yet read in code: which line layers stay on over imagery and at what width/opacity.

## Work
1. Find the line layers drawn over imagery (web and phone share the style).
2. Propose two or three looks with shots at z10, z13, z15 on one river and one small stream: e.g. thinner lines,
   casing only (two thin edges, clear middle), or lines fading out as the zoom closes in. Owner picks.
3. Build the pick on both platforms from the one style source.

## Acceptance
- With satellite on, the river is visible under or between the lines at close zoom, and the colour still reads.

Depends: —. Size: S–M. Owner: yes (pick the look).

## What draws over the imagery today (read in the running web build, z9.6 to z16)
Satellite changes NOTHING about our layers: `setBasemap` (`app/packages/map/src/adapters/contract.ts`) only shows the
`imagery` raster and repaints the labels (`color.ui.imagery.*`); every water layer keeps its plain-map paint.
- `stream` (line, status colour, opacity ~1 at z12+): width = base + Strahler order x slope, `[z4 .5+.1o, z8 .2+.16o, z10 .3+.33o,
  z11 .5+.55o, z12 1+1o, z14 1.5+1.5o, z16 2+2o]` px, closed x1.3 at z7-11. The Fraser (order 10) is ~11 px at z12, ~25 px at
  z14, ~42 px at z16: a band that covers the whole channel. A small creek (order 3-4) is 4-10 px at z14-16.
- `lake`, `lake_flowing` (river polygons: the Fraser's channel), `wetland_flowing`: fill at `opacity.lake` 0.6 light (dark 0.85, cvd
  0.9), so imagery under every lake and polygon river is tinted over at all zooms; `wetland` 0.45 hatched. `lake__edge`: status
  line, width by sqrt(area) 0.75-6 px.
- On top of the line: `stream-closed` (dashed paper stripe), gear edges, `stream-noaccess` dots; `selection-casing` when tapped.

## Looks for the owner to pick
Applied live (map.setPaintProperty on the running web build, nothing committed), satellite on, 390 px wide, today's trout colouring.
Shots: `handoff/PLAN/app-integration/T594-shots/<look>-z<zoom>-<place>.jpg`, look = today | A | B | C | D, zoom 10 / 13 / 15 /
16, place = fraser (-121.822, 49.216) | creek (Liumchen Creek, -121.917, 49.058) | lake (Cultus Lake, -121.99, 49.05).
Hue never changes (open orange, base blue, closed dark red): only how much of the water the colour covers.

- **today**: full-width line, 0.6 fill. At z13 the Fraser is a ~24 px orange bar over its main channel.
- **A, thinner lines**: stream width x0.55 (floor 0.9 px), lake/river fills x0.5. Cheapest. Judged: the Fraser still hides a
  third of its channel at z15-16 and a creek goes to a 2 px thread; does not meet the ask. Not recommended.
- **B, casing only**: every status line becomes two 1.4 px edges with the middle clear (`line-gap-width` = W - 2.8, `line-width` =
  (W - gap)/2, same colour and opacity expressions), lake / river fills x0.2, lake edge x1.8. Reads best close in (bars, pools, gravel
  bars all visible; both banks stay coloured, so open / closed still reads and the closed stripe still sits on the middle). At
  overview zooms the edges are hairlines and the colour weakens.
- **C, closing in (B from z12)**: solid line and 0.6 fill at overview zooms as today, the casing opens between z11 and z13 (gap and
  fill fade with zoom: `t` = 0 at z11, 0.5 at z12, 1 at z14 because the stream curve has stops at 11, 12, 14), fills x(1 - 0.8t), lake
  edge x(1 + 0.8t). Same close-in picture as B; the province-scale map keeps today's strong colour. Replaces nothing in the tap
  path: the layers and features stay, only paint changes.
- **D, centre hairline (replaced a "fade to nothing", which would lose the answer)**: line narrows from full width at z11 to at most
  1.6 px by z14 and stays there; fills as C. Clearest water, but one thin line carries the colour: weakest for colour-blind readers
  and the line is a tight tap target. Shown for comparison.

Exact source change (the style cannot know the ground, so all of it is an imagery branch of `setBasemap`, set with
`h.setPaint` the way the labels already are, and put back on "map"; the numbers live as new tokens so `layers.source.json` stays the one
source):
- B and C: for each of `stream`, `stream-closed`, `stream-gear-fly`, `stream-gear-bait`, `stream-noaccess`, `stream-runs-pass`:
  `line-gap-width` = zoom curve with `t(z) * max(0, W - 2e)` and `line-width` = `(W - gap) / 2`, `W` = the layer's own `line-width`
  stop output, `e` = new token `width.imagery.edge` = 1.4, `t` = 1 for B, `[11 0, 12 .5, 14 1]` for C (new `ramp.imagery.open`).
- B and C: `lake`, `lake_flowing`, `wetland_flowing` `fill-opacity` x `[11 1, 13 .2]` (C) or x0.2 (B), new token `opacity.imagery.fill`
  = 0.2; `lake__edge` `line-width` x `[11 1, 13 1.8]` (C) or x1.8 (B). `wetland` unchanged.
- A: `line-width` stop outputs `max(0.9, 0.55 W)`, fills x0.5. D: `line-width` stop outputs `(1-s)W + s min(1.6, W)`, `s` = ramp z11 to z14.
Values are exactly what the live-applied script used (scratch only, not committed); the numbers above are all of it.

Pinned values the pick moves (none run yet; this is a read of the tests):
- `tools/status-colours-distinct.test.ts`: has a satellite row ("a 0.6-coverage line over conifer / shadow imagery", `LINE = 0.6`)
  and gates `opacity.lake` (0.4 / 0.5 / 0.55 failed there on the plain ground). B / C / D lower line coverage and fill opacity ON IMAGERY,
  so the gate needs an imagery row for casing coverage and the lake-fill rule must say it is for the plain ground only. Colour-blind
  check not done: the edges are 1.4 px, so closed (dark red) vs own (orange) over dark forest is the pair to re-measure.
- `tools/handover.test.ts` is NOT moved (it pins handover z, floors and minzoom, not paint). `tools/midzoom.test.ts` (stream
  line-width at z8-11) and `parity.test.ts` (stream `line-width`, selection casing) read the plain-map paint, which is unchanged;
  new imagery paint needs its own test beside them, and `pnpm style` (`style.json` / `style.sha256`) only moves if tokens are added to
  the source.
- Taps: paint only, features stay; the casing's clear middle is still hit by the tap query only if its padding reaches the line
  (not measured: check on the phone shell before shipping B / C / D).

Recommendation: **C**. It clears the water exactly where the owner looks for pools (z13+), keeps today's strong colour at province
zoom where B's hairlines lose the answer, keeps both banks coloured and the closed stripe, and is one casing mechanism over every
stream layer plus a fill fade, with no new layers. Take D only if the owner wants a single thin line at close zoom.

## Owner pick 2026-11-11
Look C (two thin edges), as long as it does not block small streams; try lower opacity when zoomed in too. Measure tap target and the colour-blind gate before it ships; send before/after shots with the question.

## Built 2026-11-11 (dcfbf8f29): look C, satellite only
What a reader sees over satellite (the street map is painted exactly as before; `tools/imagery-casing.test.ts` proves it):
- **z10 and out**: unchanged, today's solid line and lake fill.
- **z11-z14**: the line opens: at z12 half open, from z14 two 1.5 px edges in the status colour with the water clear
  between (`line-gap-width`). The Fraser at z15-16 is two orange edges with its gravel bars and water visible between.
- **Small streams** (the owner's condition): the clear middle is never narrower than a creek's own width close in
  (`IMAGERY_GAP_FLOOR`: 2 px at z13, 3 at z14, 5 at z15, 8 at z16, ~12 m of ground), so a creek a few pixels wide sits
  BETWEEN the edges, never under one; the band is wider than today's line there instead of narrower. Chosen over a
  centre hairline because a hairline sits ON the creek.
- **Marks**: the gear edges and the selection outline sit just outside the coloured edges (as wide as before each side),
  a passing run's dash and closed's paper dash break the edges themselves (closed still reads as broken, nothing over
  the water). The no-access dots are thin already and unchanged.
- **Lakes / river polygons / flowing wetlands**: fill fades to 0.2 of itself by z13; the lake outline (which carries the
  status) 1.8x as heavy there.
- **Opacity**: the edges are full strength today (`opacity.imagery.edge` = 1). Lower opacity close in FAILS the
  colour-blind gate at a thin edge (below); 0.9 passes only with 2 px edges.

Measured:
- **Tap**: MapLibre hit-tests a line with a gap over its whole band (gap + both edges), and the band is never narrower
  than today's line at any zoom, order or selection state (`imagery-casing.test.ts`, evaluated with MapLibre's own
  evaluator). In the running web build: a point query at the line's centre (the clear middle) hits the stream at every
  sampled midpoint (Fraser z10 103/103, z13 55/55; creek z14 30/30, z16 8/8; the streams in the Cultus Lake view z13 33/33), the same as today;
  the 24 px tap box hits the same counts. The phone uses the same query geometry (`queryRenderedFeatures`), not run on
  the emulator.
- **Colour-blind gate** (`tools/status-colours-distinct.test.ts`, new row "as edges over satellite": coverage
  e/(e+1) x opacity, held to the old satellite line's floors, over the imagery grounds plus dark forest #0a1b11 and the
  Fraser's brown water #a7a385 sampled from the shots): the ticket's 1.4 px edge FAILED (closed/own 17.2 < 17.5 light and
  cvd, closed/base 33.8 < 35); raised to **1.5 px at full opacity: passes**. 0.7 opacity fails at any width up to 4 px
  (1.5 px: closed/own 13.5 < 17.5); 0.9 passes at 2 px. Mutation pins: 1.4 px and 0.7 fail.
- Native: `line-gap-width` (data-driven since Android SDK 5.0) and `let`/`var`/`max`/`case` (6.0) are supported by
  MapLibre Native 13.6.1; the phone's style (`nativeStyle`, satellite) passes the style-spec validator in every mode.

Shots (web, 390 px, satellite, today's Trout colouring) in `handoff/PLAN/app-integration/T594-shots/final/`:
`today-` vs `new-` `z10-fraser`, `z13-fraser`, `z15-fraser`, `z16-fraser`, `z14-creek`, `z16-creek` (Liumchen Creek),
`z13-lake`, `z15-lake` (Cultus); opacity at z15: `opacity-z15-fraser-o1.0-edge1.5.jpg` (built),
`opacity-z15-fraser-o0.9-edge2.jpg` (passes the gate), `opacity-z15-fraser-o0.7-edge1.5-fails-cvd.jpg` (fails it).
Owner question: keep 1.5 px at full strength, or 2 px at 0.9? (0.7 would need the colour-blind floors lowered.)
Not checked: the look on the phone itself (emulator or Pixel); a creek under heavy canopy is not visible in the imagery
either way.
