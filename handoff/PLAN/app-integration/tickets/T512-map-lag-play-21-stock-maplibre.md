# T512 — Laggy main map on the Pixel 9 (Play 0.6.9 build 21): the build shipped the stock MapLibre again

Status: IN REVIEW — GUARD (`build-apk.sh --play` refuses stock MapLibre); build 22 went to Play through it; owner: OWNER-TODO "Phone check with build 22" (main map lag on the Pixel).
Earlier: IN REVIEW — GUARD; owner runs the next Play build with the patched MapLibre. Owner 2026-11-08 (Pixel 9 "tokay", trace `~/Downloads/trace-tokay-CP3A.261005.005-2026-11-08-12-15-47.perfetto-trace`): "laggy map … worst on the main map page. Conditions is very fast, and when switching between maps it gets worse." Follow-up of T420.

## The trace (Perfetto trace processor; app `ca.canifishthis.app` versionCode 21; 11 s, ~8.5 s of it panning)
- **The map's own render thread is the whole problem.** MapLibre's Vulkan render thread (`RenderThread 29`) ran
  **7.95 s of CPU in 8.2 s** (2.5–10.7 s), 6.2 s of it on the prime core (CPU 7): 100 % of a core, never asleep. The map's
  surface presented **55 frames** in that time: ~94 ms apart in the first 2 s of panning, then **142–209 ms** (5–7 fps)
  — **~147 ms of render-thread CPU per map frame**. GPU is idle (QueueSubmit/QueuePresentKHR ≤ 0.4 ms).
- Not the UI, JS or GC: main thread 0.58 ms per `Choreographer#doFrame` (max 10 ms; 33 HWUI frames, 3 janky), JS thread
  0.5 s in total, GC 53 ms (one concurrent collection). The 4 map tile workers 0.86–1.07 s each (in parallel, not the limit).
- **One map alive**: one MapLibre render thread, one app SurfaceView layer, one Vulkan context (2 `mali-event-hand`
  threads: the HWUI one and the map's). No second map rendering behind — the T498/T508 hold is not in this trace.
- Same signature as the owner's 0.6.8 (build 18, stock) trace of 2026-11-07 (render thread 9.4 s / 12 s, 113 frames ≈
  83 ms CPU per frame); build 21's frames cost ~1.8× more.

## Cause: build 21 (and 20) carry the STOCK MapLibre, not T420's lean patch
`libmaplibre.so` GNU build id in each Play AAB (`app/dist/aab/`):
| build | libmaplibre build id | MapLibre |
|---|---|---|
| 18 (0.6.8) | aab47813 | stock 13.6.1 |
| **19 (0.6.9)** | **7bf3dc29** | **lean patch** (`13.6.1-fslean`) |
| 20 (0.6.9, ab9e25a5c) | aab47813 | stock |
| 21 (0.6.9, 6ef99a4e5) — the phone | aab47813 | stock |
The staging APKs (bdde6e668, e7eb12a8f) are stock too. Builds 20 and 21 were made without `MLN_LOCAL_MAVEN` (the lean
repo sat in an old session's /private/tmp scratchpad), so they took Maven Central's SDK — while play.json for 21 still
says "The map pans more smoothly, and stays smooth the longer you use it".

Stock MapLibre re-applies EVERY feature state ever set to EVERY rendered tile on EVERY frame (T420 "Correction"), so:
- **the Map tab is the slow one**: its stream/lake colours are per-section feature state read by many paint properties;
  Conditions colours from fewer state keys (`standing`) — less re-evaluated per frame;
- **it gets worse with every switch**: each Map ↔ Conditions ↔ Runs switch is a style swap after which the engine
  relabels every held tile (new ids and keys), and MapLibre's store never shrinks (a clear writes nulls; states survive a
  swap). T420 measured the growth on stock (Coquitlam z13 p50 99 → 148 ms after 160 flings) and its absence with the patch.

Ranked: (1) stock MapLibre in builds 20/21 — proven by the build ids, matches every symptom; (2) the T508 hold keeping
maps alive — not supported (one map, one surface in the trace; the browse map has stayed mounted behind a water's pane,
opacity 0, since 2026-10-29, before T498); the emulator run below counts render threads and surfaces after N switches.

## Done (2026-11-08)
- **Guard** (`deploy/staging/build-apk.sh`): `--play` (not `--production`, where app.config.js refuses the local SDK) needs
  `MLN_LOCAL_MAVEN` or `--stock-maplibre=<reason>` before the heavy lock, and after the build refuses an AAB whose arm64
  `libmaplibre.so` build id is not the local AAR's. The lean repo now lives at `cache/maplibre/fslean-m2` (gitignored,
  survives a reboot). Dry runs: refused without it (rc 2), accepted with it, bare `--stock-maplibre` refused. The id check
  read on the real AABs: 21 → aab47813, 19 → 7bf3dc29 = the AAR's.
- Test bed `switch-pan` (`app/tools/testbed/scenarios/switch-pan.mjs`): the Map and Conditions tabs' travelling pan fresh
  and after N switches (Map → Conditions → Map + the Fraser opened into its Map view and closed), with render threads,
  map workers, SurfaceView layers and PSS beside each pan.
- **Emulator A/B** (stressed as T420: efficiency cores, `-cores 2`; Coquitlam z13; 16 travelling flings; both APKs from
  the same tree, versionCode 21; runs `app/tools/testbed/out/runs/t512-{lean-a,stock-a}/`). Map surface p50 / p95 ms
  (frames) after N switches, each switch = Map → Conditions → Map + the Fraser opened into its Map view and closed:
  | build · tab | 0 | 4 | 8 | 16 |
  |---|---|---|---|---|
  | lean · Map | 34.1 / 134 (484) | 39.2 / 121 (398) | 33.6 / 95 (510) | 34.8 / 93 (472) |
  | lean · Conditions | 33.4 / 87 (616) | 33.4 / 84 (624) | — | 32.7 / 80 (644) |
  | stock · Map | 59.6 / 158 (299) | 84.3 / 171 (205) | 83.6 / 154 (231) | not run |
  | stock · Conditions | 52.9 / 129 (445) | 54.4 / 127 (427) | 71.0 / 147 (353) | not run |
  Stock is ~1.75× slower fresh and its Map tab grows +40 % within 4 switches; lean is flat to 16. Alive throughout, both
  builds: 1 map SurfaceView, the same render-thread / worker names (5 / 14) — no map left alive by the hold. Stopped
  at stock 8 switches (owner 2026-11-08: the patch worked before; ship it).

## Owner
- Next Play build: `MLN_LOCAL_MAVEN=$PWD/cache/maplibre/fslean-m2 deploy/staging/build-apk.sh --play` (build 22).
- Production (`--production`) still refuses the patched SDK (T420: shipping it to production is its own decision).
