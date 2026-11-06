# T444 — Crash: "water map: part 0 of gnis:3062 is not in its group" (Chilliwack map view)

Status: OPEN.

Source: staging worker `GET /crashes` (read 2026-11-06), kind 446ddf96b4937153: 0.6.2 (build 11, d6deee4), Android 37
(owner's phone), route water, water gnis:8634 (Chilliwack River), view map, part 1; non-fatal JS error thrown in
`waterMapStartIx` ← `useWaterMap` ← `WaterMapView`. gnis:3062 is another water (likely the one the Chilliwack continues
as / from — the Vedder), so a frame for one water reached the map of another: a step / cross (▲/▼ into the water it
continues as, T345/T369 `crossAction`) or a stale frame on re-render.

## Work
1. Reproduce: Chilliwack → Map → part 1 → step across into the next water and back, quickly (web + emulator); also
   a reload mid-step. The release-backed WaterMapView tests are the place to pin it.
2. Fix the frame/water mismatch at its source (one load path, T369); keep the throw (no silent fallback), but it must
   be unreachable from any way in.
Size: S.
