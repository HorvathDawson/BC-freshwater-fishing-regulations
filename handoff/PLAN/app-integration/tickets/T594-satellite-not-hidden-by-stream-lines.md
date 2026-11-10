# T594 — Satellite view is not hidden by the stream lines

Status: OPEN

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
