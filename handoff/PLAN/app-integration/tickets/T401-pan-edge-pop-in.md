# T401 — Streams pop in at the screen edges while panning (phone)

Status: PARTIAL — owner 2026-11-09 on build 24: creeks still pop during the glide, "minor and not too bad but noticeable. not urgent but we could improve"; left (Claude): option 3 render margin, later (owner: not urgent)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T401 | Streams pop in at the edges while panning: MapLibre Native has no viewport prefetch; parent tiles lack small streams. Measure, then prefetch-delta patch or render margin (no tile buffer rebuild) | PARTIAL — owner 2026-11-09 on build 24: creeks still pop during the glide, "minor and not too bad but noticeable. not urgent but we could improve"; left: option 3 render margin, later | c1c838f58 deb635819 29b1e561b | owner 2026-11-04 (0.5.0 phone); 11-06 stressed emulator: frames missing tiles while moving 22.6→17.6 % (z13 travelling), 26.4→16.1 % (circling) after the T420 tile-event fix … (detail: ticket file) |

Source: owner 2026-11-04, 0.5.0 Play internal: "panning doesn't load enough of a buffer of streams outside the
view"; 2026-11-09 on build 24 (Status above).

## What
MapLibre Native loads only the tiles that cover the viewport; its only prefetch is parent tiles (`prefetchZoomDelta`,
default 4). The parent tile drawn while a new one loads holds only the bigger streams (`MAGNITUDE_LADDER`: a
magnitude-5 creek starts at z12), so when the real tile lands the small streams appear — the pop. The atlas has no
tippecanoe `--buffer` set (default 5/256): the buffer clips lines at tile edges, it does not load neighbours, so a
bigger buffer would NOT fix this — no tiles rebuild needed for it. The pop is DURING the glide on a slow CPU: at rest
the edge strip is 0 % missing (2026-11-05, test bed `edge`).

Done:
- Pan probe reads MapLibre's per-frame fully/partially rendered (c1c838f58). Normal emulator ~5 % of moving frames lack
  a tile; stressed emulator (2 efficiency cores) 20–36 %.
- T420 tile-event fix (deb635819): stressed emulator, frames missing tiles while moving: Coquitlam z13 26.4 → 16.1 %
  (circling), 22.6 → 17.6 % (travelling); Fraser Valley z11 9.9 → 14.9 %, 22.6 → 19.4 % (noise is a few points). The
  rest is MapLibre parsing new tiles behind the glide.
- Option 2, prefetch zoom delta (29b1e561b): the binding patch's `prefetchZoomDelta` prop (Android,
  `MLRNMapView.applyPrefetchZoomDelta` → `MapLibreMap.setPrefetchZoomDelta`, android-v13.6.1; −1 = MapLibre's own 4);
  `Map.native.tsx` passes `PREFETCH_ZOOM_DELTA = -1` (unchanged behaviour). iOS `MLNMapView` has only `prefetchesTiles`
  (on/off). Deltas 2 and 3 vs 4 (3 runs each, runs `app/tools/testbed/out/runs/t401-*`): within run-to-run noise
  (±10 points missing, ±35 ms p50), no gain, no cost; not shipped. The probe cannot credit a richer placeholder
  (`TilePyramid::isLoaded` waits on the prefetched parents too); screenrecord on the 2-core emulator stalls the app.
- Build 22 (2026-11-08) carries the lean AAR, the binding patch (`maxPixelRatio` + `prefetchZoomDelta`), prefetch −1.

## Work (left, owner: not urgent)
- Option 3, render margin: the native map view drawn larger than the screen (tiles beyond the edge load early) — costs
  GPU/memory.
- If the owner still sees bare streams on the Pixel 9: a delta-2 APK side by side (the emulator cannot judge
  placeholder richness); narrower experiment: MapLibre's PER-SOURCE delta (`Source.setPrefetchZoomDelta`, not exposed)
  for the streams source alone.

Depends: T312 (map feel). Size: S–M. Owner: choose after step 1.

## Ledger detail (moved out of the ledger row, T540, 2026-11-22)
owner 2026-11-04 (0.5.0 phone); 11-06 stressed emulator: frames missing tiles while moving 22.6→17.6 % (z13 travelling), 26.4→16.1 % (circling) after the T420 tile-event fix deb635819; not gone — prefetch-delta patch next if the owner still sees it; option 2 (29b1e561b): binding `prefetchZoomDelta` prop, deltas 2/3 vs 4 within run-to-run noise (3 runs each) → default kept; left: owner phone A/B (old owner to-do build 22), option 3; 11-08 build 22 command checked: lean SDK + prefetch default

## Owner

- [ ] 2026-11-08 — Phone A/B of the pan-edge pop-in, option 3 (the run-to-run noise showed no win, so the default is kept; see Work): say if you want option 3 on your phone.
