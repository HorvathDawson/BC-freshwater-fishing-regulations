# T145 — Spot markers: smaller, capped size, deselect on tap off, a better look

Status: DONE — 2cb2ea91, a73d0561, review fixes in the commit that adds "Adversarial review" below (lane-T145; web shots in handoff/PLAN/app-integration/T145-shots/; native is drawn by the same
code but was not run on a device: rides T50)

Source: Owner 2026-10-23 (app walk). "the new spot markers look really bad and take up a ton of space and dont deselect if click off them etc.
they get way too big too." Rework of T131.

## Work
Cap the marker size at every zoom (no growth past a small pin), a compact look in the app's tokens (light + dark; not
app/design/riffle.html), tapping the map off a spot clears the selection (web + native), selected vs unselected clear but
small. Shots before/after at low and high zoom, light and dark. Size: S–M.

## As built
- `@app/core` spotPins.ts: `SPOT_PIN` is now hit 44 (invisible target, unchanged), face 12, groupFace 18, frame 2,
  ring 4, **max 26**, groupPx 32 (was 40), labelZoom 12 (was 11), labelMaxWidth 120, labelHeight 15, labelGap 3.
  `spotFace(m)` returns `{ face, extent }` from the mark's kind and selection only. It takes no zoom, so no zoom can
  grow a mark. A test walks z4 to the camera's deepest, selected or not, alone or grouped, and gets the extents
  {12, 18, 20, 26}, all ≤ max. New `spotUnder(marks, lat, lon, zoom)`: the mark whose 44 px target holds a tap (the
  nearer of two).
- `@app/ui-native` SpotPin: a 12 px SQUARE of the spot mark framed 2 px in the spot ground (it reads on water, land and
  imagery; square, so it is never a gauge's round dot or a boundary mark's ring). Selected: the same face ringed in the
  mark, with a 2 px gap and a 2 px ring, 20 px in all. Group: an 18 px square with its count in the ground colour. The
  name is one line of 11 px bold on a ground strip with no frame, cut with an ellipsis at 120 px. The slab shadow, the
  stacked second face, the framed 12 px label and the icon inside the face are gone. Same tokens as T131
  (`color.ui.spot` / `color.ui.spot-ground`; the theme.test ΔE gate is unchanged and passes).
- Deselect: `@app/ui` mapRail `deselect()` forgets the pin last opened and clears the spot Show selected. Show's
  framing and the route stay. `mapPointTap(rail, closeSearch|null)` is the ONE decision for a map tap off every spot:
  in pick mode it places the spot; otherwise it closes a wide search when one is open and clears the selection;
  undefined when there is nothing to do. The Shell hands it to the Map tab, the Conditions map and a water's map view
  (`onMapPoint`).
- Web: the spot layer is DOM over the canvas, so a tap on a spot never reaches the map's click. Native
  (`Map.native.tsx` `onPress`): a tap that `spotUnder` puts on a spot returns before `onMapPoint` and the feature
  press, so the spot's annotation press is the only thing it does. It neither clears the selection it makes nor opens
  the water under it.
- Tests (whole values): spotPins.test (policy, spotFace, the cap at every zoom, spotUnder, the selected extent placing
  names), SpotPin.test (face/ring/count/name read back from the DOM, light + dark, sizes = spotFace ≤ max),
  mapRail.render.test (deselect; mapPointTap's five cases), Spots.shell.test (through the real Shell: tap off clears on
  the Map tab, the Conditions map, after Show, and on a water's map; a pin tap selects again).
- Mutation pins, each failing the named tests:
  M1 browse map `onMapPoint` reverted to the old expression → Spots.shell T145 map/conditions/Show;
  M2 water map's `onMapPoint` dropped → Spots.shell T145 water;
  M3 deselect keeps Show's spot → mapRail.render T145 + Spots.shell Show;
  M4 no selected ring in `spotFace` → spotPins (name placement, spotFace, cap) + SpotPin selected/sizes;
  M5 group face 30 px → spotPins policy/spotFace/CAP + SpotPin group/sizes;
  M6 spotUnder half-target = 44 → spotPins spotUnder;
  M7 mapPointTap drops the search close when a pin is selected → mapRail.render mapPointTap.
- Shots (own Expo web server, phone 390×844, Show on Tamihi run then zoom out): `before-*` / `after-*` for
  {light, dark} × {z14-selected, z12-names, z9-grouped}, plus `after-*-z14-tapped-off` (the ring gone after a tap off
  the spot; the DOM ring width read 2px → 0px in both themes). Checked by hand on web: a pin tap opens the spot, and
  back shows it selected.

## Checks
`REGS_STORE_REQUIRED=1 pnpm -C app check` via heavy.sh, once: boundaries, platform, style, colours, deps and typecheck
all passed. Tests: 7300 passed, 6 failed. One failure was mine (map spots.test: a group now parts at z9.5, not z10);
it is fixed in the follow-up commit, and the 25 affected files (303 tests) pass unfiltered. The other 5 are
conformance regs tests (blob/card/inputs/settle/store): the main checkout's `data/generated/bundle/regs.sqlite` is
store f977e8646d7ddcd8, but taps.json and the live release were cut from db98ffb0563359a2. A concurrent
`pipeline build` held the heavy lock just before this check ran. This lane touched no data or conformance file.

## Owner questions (defaults built)
- Q-T145-1 Shape: a small square (brutalist corners; never a gauge dot or a boundary ring). A round or teardrop pin
  instead? Default: square.
- Q-T145-2 Selected = a ring 8 px wider; unselected and selected are the same colour (ink in light, light in dark).
  Want the selected one in a hue of its own? It would need a new token past the ΔE gate. Default: ring only.
- Q-T145-3 Names from z12 (was z11), one line cut at 120 px. Default: as built.
- Q-T145-4 The invisible tap target stays 44 px (T131 acceptance, WCAG). Close to a pin it takes the tap from the water
  under it. Smaller (32 px)? Default: 44.

## Adversarial review (2026-10-23)
Tried against the owner's complaint in a browser on my own Expo web server (:39477, stopped after; live tiles and
release from :39217, read only), four spots seeded into the web store (two close ones, one name long enough to cut):
- **Too big at zoom:** at the camera's deepest (z18, "Zoom in" disabled, 10 m scale) every face measured 12 px, the
  selected ring 20 px. At z12 the names measured 62/75/120/120 px; the long one is 296 px of text cut at 120. Holds.
- **Tap off deselects:** on the Map tab after Show, a tap on bare land took the ring 2px → 0px and opened nothing. On
  the Conditions map, after a pin tap and back (ring 2px), a tap off → 0px. A tap on the Vedder River opened the water,
  and back from it showed the pin unselected. A tap 16 px right / 14 px below the face (inside the invisible target, off
  the face) opened the spot; back showed it selected. Shots: `T145-shots/review-light-conditions-z18-tapped-off.png`,
  `review-light-conditions-z12.png`.
- Mutations checked by hand: M1 (Shell's browse `onMapPoint` reverted) fails Spots.shell map/conditions/Show; M3
  (deselect keeps Show's spot) fails mapRail.render T145 + Spots.shell Show. Both as claimed.

Found and fixed (tests only; no product code changed):
- **R1 — spotUnder's "the nearer of two" test passed for the wrong reason.** Its taps at 10 and 26 px each sat inside
  ONE target only (targets 36 px apart overlap only from 14 to 22 px), so a first-match `spotUnder` passed it (run:
  18/18 green with `if (best === null)` in place of `if (d < bestD)`). Now it taps at 16 and 20 px, asserts both sit in
  both targets, and expects s1 then b. Mutation pin M8: first-match `spotUnder` → spotPins "two marks whose targets
  overlap" fails (17/18).
- **R2 — the native guard (`Map.native.tsx` `onPress` returning on `spotUnder`) had no test at all**: deleting the line
  failed nothing. New adapter-contract "native only" test: at z14 a tap on the mark and one 20 px off it report no
  point, query nothing and open nothing; a tap 30 px off reports the point and opens the stream under it. Mutation pin
  M9: guard line deleted → that test fails. (It still says nothing about whether MapLibre Native fires the map's
  `onPress` for a tap on a view annotation — that is the device run, T50.)

Seen, not changed (already owner questions or prior tickets):
- On web a name is `pointerEvents: none`, so a tap on the part of a long name OUTSIDE the 44 px target falls through to
  the map: it clears the selection and may open the water under it (T131 design: "the face is the target").
- The Tamihi pin sits on a gauge pill ("p5th") on the Conditions map at z12: Q-T131-5, unchanged.
- `mapPointTap` returns a new function each render while a pin is selected, so the web map re-binds its click
  listener per Shell render. Cheap, correct; not changed.

Review checks: `REGS_STORE_REQUIRED=1 pnpm -C app check` via heavy.sh once (temporary `.venv` symlink, removed):
boundaries … typecheck passed; tests 7301 passed, 6 failed. Five are the same conformance store/oracle mismatch as
above (f977e8646d7ddcd8 vs db98ffb0563359a2), not this lane. The sixth, check-colours "no colour literal outside the
token files", hit ENOENT on a `_plat.ts` that check-boundaries / no-localhost tests write and delete while it walked;
run alone it passes 20/20.

## Left / follow-ups
- (review) Flaky suite: tools/check-colours.test walks packages/ui-native/src while check-boundaries / no-localhost
  tests create and delete a temporary `_plat.ts` there; a parallel run can ENOENT. Not this lane; needs a ticket.
- Native not run on a device or emulator (rides T50): the `spotUnder` guard assumes MapLibre Native reports `onPress`
  for a tap on a view annotation. If it does not, the guard is harmless.
- T131's Q-T131-4 (a blocked selected name is dropped) and Q-T131-5 (a pin over a gauge pill) still stand, smaller now.

## Rebuild
None: app code only. Ships with the next app build/deploy; no bundle, tile or data rebuild.
