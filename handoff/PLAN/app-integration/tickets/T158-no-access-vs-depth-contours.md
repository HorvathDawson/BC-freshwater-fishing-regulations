# T158 — Dark no-access ice cyan nearly matches the depth contours

Status: DONE — 32b4f4af (lane-T157); adversarial review a78dabfd

Source: T146 adversarial review, 2026-10-23: dark no-access #50E4FF is ΔE 4.7 from the depth-contour colour, so contours
vanish on a no-access lake when the Depth layer is on (e.g. Coquitlam Lake). Add the pair to the colour gate and move
one of them (contours are the cheaper move). Size: S.

## As built
- **The gate** (`app/tools/status-colours-distinct.test.ts`, "depth contours are seen on every lake (T158)"): the
  contour is a 0.8 px line laid OVER the lake fill, so it is measured that way — a 0.6-coverage line over each fill,
  the fill drawn at `opacity.lake` over the basemap's water (the same `over` / grounds the T47 gate uses) — against
  every token the lake layer paints in the colourings contours are drawn over: `plain`, `surveyed` (Depth), `status`
  and `profile` (Regulations), categories and `missing`, read from `layers.source.json` itself (a category added there
  is held the day it is added). Floors: normal vision ≥ 15, every reader (protan/deutan/tritan, Viénot and Machado)
  ≥ 12. Pair named in the ticket, as drawn: dark contour #4FD0E8 on a no-access lake 1.0 / 1.0.
- **Measuring every fill found the same fault in the Depth colouring itself**, every theme: a contour on a digitised
  lake (the only lakes that have contours) was 5.7 (dark), 4.8 (light), 9.2 (cvd) in normal vision — cyan on teal.
  So the move is in all three themes, not dark only.
- **The move:** `color.contour.line` → `#000040` (a deep navy) in light, dark and cvd. Searched over the sRGB cube
  (step 4) for the fills above: every colour that clears both floors on every fill is near-black; #000040 gives the
  worst pair dark lake.fill 17 / 13, light closed 25 / 23, cvd closed 27 / 20 (normal / every reader).
- `pnpm style:build` regenerated `style.meta.json` + `style.sha256`.

## Tests (mutation → test that fails; each run)
- The dark contour back to #4FD0E8 → "fails the dark contour cyan that T146's ice-cyan no public access swallowed"
  (whole list pinned: digitised 5.7 / 4.0, no_access 1.0 / 1.0, unmapped 10.7 every-reader).
- The light contour back to #2FA9C9 → "fails the light contour cyan on the Depth colouring's own digitised fill" (4.8).
- A contour painted in no_access's own colour → "fails a contour painted in the lake fill it lies on" (all themes).
- The three "stand off every lake fill they are drawn on — light / dark / cvd" pass on the built palette.

## Shots (`../T158-shots/`, `contour-shots.mjs`; this worktree's Expo on :39571 against the owner's :39217, GET only)
**The current tile set carries NO contours** (`data/generated/tiles/layers/` has no `contour.geojsonl`; the switch
draws nothing anywhere, measured: 0 source features around Coquitlam at z11–12.6). So the shots add stand-in contours
— Coquitlam Lake's own rendered outline scaled toward its centre — on a line layer copying the `contour` layer's
resolved paint (#000040, 0.8 px), with the Depth contours switch on as a reader turns it on:
- `after--coquitlam-depth--dark.png` / `--light.png` — the navy contours read on the ice-cyan no-access lake.
- `before--coquitlam-depth--dark.png` / `--light.png` — the same lines in the old cyans: gone in dark, faint in light.

## Owner questions (default built)
- Q1 Contours are now a deep navy in every theme (they were cyan). Default built: navy everywhere, the only colour
  family that is seen on every lake fill. Alternative: keep cyan in light/cvd and accept the Depth colouring's
  4.8 / 9.2 (contours nearly invisible on a surveyed lake there).

## Checks
- `vitest tools/status-colours-distinct.test.ts tools/style-spec tools/style-valid tools/check-colours tools/cvd`: 63
  passed. `check-colours.mjs`, `style` gates clean. Full `pnpm -C app check`: see T157.

## Rebuild
App build only (style tokens). No tiles, no bundle. The contours themselves only show once a tile build includes the
`contour` layer again.

## Left / follow-ups
- **Stocked is not held**: the cvd stocking ramp runs navy → pale yellow, and no one line colour stands off both ends
  (best 17.5 normal / 8.3 every reader); light and dark clear it with #000040 (measured, not gated).
- **No contour layer in the current tiles** (above): the Depth contours switch is offered and draws nothing. Worth a
  ticket of its own (tile build / bathymetry fetch).

## Adversarial review (2026-10-23, lane-T157)
- Re-ran `status-colours-distinct` (with `check-boundaries`: 51 passed). The `contour` layer is a static line in
  `color.contour.line` at `width.lake.edge` with no opacity of its own, so a full-strength line at the gate's coverage
  over the lake fill at `opacity.lake` is the right model; no other app surface paints the token (the Layers sheet's
  Depth swatches are `survey`, not the contour).
- `contourGrounds` reads the lake layer's `plain` / `surveyed` / `status` / `profile` modes and its list is pinned whole, so
  a category added there fails the pin until it is held. The Conditions view paints lakes `standing` and never offers
  the contours switch (`overlay.contours` views: map, water), as the test says.
- The mutation pins (old dark cyan, old light cyan, contour = no_access) are whole lists; the style was regenerated and
  `pnpm check`'s style gate is clean (sha 8f1f7fc1d362). Nothing to fix.
- Left as written: Stocked in cvd is not held, and the tiles carry no contour layer (both already follow-ups).
- (Resumed after a machine restart) re-confirmed `data/generated/tiles/layers/` holds no `contour.geojsonl`, so Coquitlam
  with Depth on cannot be shown with real contours; the builder's stand-in shots are the only picture, the gate is the
  evidence. No second copy of the old cyans (#4FD0E8 / #2FA9C9) anywhere in app source, tokens or design.

