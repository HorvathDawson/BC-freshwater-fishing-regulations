# T881 — Conditions: the gauge map only where it draws a gauge; one map size, fitted

Status: OPEN (built; lands after the owner sees the pictures). Lane `app/t881-gauge-map`, from `app/integrate-081-web`.

Owner 2026-11-18 ~21:05, verbatim, with a shot of Babine Lake — Main Body → Conditions: "Lakes or other things without
a meaningful map here don't need to include gauge map.. Also we should choose one map size maybe larger fit only"

The screen: "NO ESTIMATE / No gauge on this water is close enough…", then the gauge map holding only the "you are here"
pin over the lake, with Fit / Larger map / +/- buttons, then the watershed flow card.

## The rule

The route map's pins are the placed gauges (`donorView` `placed`), a gauged lake's own station (T817 `own`) and "you are
here". `donorView` (`ui/panel/donor.ts`) now returns `map: null` unless the map draws at least one gauge
(`drawsAGauge = placed.length > 0 || own !== null`). Cases where the map would hold "you are here" alone, all now no map:

- no member at all: a lake or creek no gauge is near enough to ("no-station", Babine Lake — Main Body);
- a refusal whose every member is not reporting or not a real-time gauge (those are not framed, so not pinned);
- an answer whose members have no known place (the rows under it still name each gauge; the map's "not placed" note
  goes with the map).

A gauged lake's own station keeps its map (Harrison Lake, T817).

## One size

`DonorMap.tsx`: one size, `ROUTE_MAP_SIZE = "large"` (the old Larger map: 0.95 × width, 380–620 px); `"small"` is the
old default, kept in `ROUTE_MAP_SIZES` so going back is one word. The Fit and Larger map buttons are gone; the map opens
on the camera that frames the water (or the tap) and its gauges, as before. +/- zoom, pinch and pan stay. The words
(`ROUTE_MAP_WORDS` fit / fitLabel / larger / smaller) and MiniMap's `fit` prop (no other caller) are removed.

Note for the owner: the frame fits the tighter axis. On an east–west valley (Vedder) the zoom is the same as the small
map's and the extra height is map below the routes.

Kept: the walkthrough's Conditions step points at the headline, horizons, reading, quantity, span and chart, never at
this map; the test bed's route map scenarios (`route-scroll`, `gestures`) find the map as a MapLibre view under 60 % of
the screen, which 380 px still is, and read no Fit / Larger map control.

## Pictures (390 wide, dark, staging web export)

Scratchpad: `t881-before-390-dark-babine.png` / `t881-after-390-dark-babine.png` (map gone),
`t881-{before,after}-390-dark-vedder.png` (map present, larger, no buttons),
`t881-{before,after}-390-dark-mamquam.png` (one gauge upstream, Mamquam River Above Ring Creek).

## Tests

`ui/panel/noGaugeMap.test.ts` (the three no-gauge cases and the one-gauge control; mutation: drop `&& drawsAGauge` →
three fail), `ui/panel/ownGauge.test.ts` (a lake without its own station: no map), `ui-native/DonorMap.test.tsx` (no
buttons, opened on the VM's camera, the one size), `core/routeMap.test.ts`, `ui/conditions.render.test.tsx`,
`ui/panel.place.test.ts`.
