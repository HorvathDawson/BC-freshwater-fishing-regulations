# T448 — Native map: cap the drawing density like the web (pixel ratio ≤ 2)

Status: OPEN.

Source: T420 (2026-11-06, 2b891823f): the web map now draws at `min(devicePixelRatio, 2)` (`packages/map/src/web/
pixelRatio.ts`) — 28–32 % less frame time at z10–z12 on a Pixel 9 Chrome proxy (412×915 @ 2.625). MapLibre Native on
the same phone also draws at 2.625; the binding exposes no pixel ratio.

## Work
Expose `MapLibreMapOptions.pixelRatio` (Android) / the iOS equivalent in the binding patch, set the same rule in the
native half (`Map.native.tsx`; rule 34a: the native file, not a shared branch), measure on the stressed emulator (pan
scripts, T420 tables) and say what it costs in sharpness (shots at 2.625 vs 2).
Size: S–M.
