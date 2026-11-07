# Map performance: handoff for a focused chat (2026-11-06)

You own one job: make the map feel smooth on the owner's Pixel 9. Read `AGENTS.md` first (every repo rule). Then read
the tickets named below. They hold the full measurements; this page is the map to them.

## What the owner feels now (0.6.7, Pixel 9, Play internal)
1. **The pan hitches while new tiles load.** The fling glide itself is smooth (T452 is DONE). Quote: "the map loading
   still causes stuttering". This is the main target.
2. **It lags when zoomed out (z5–z9).** It's better after a893b06fe but not gone.
3. **Streams pop in at the screen edges during a glide** (T401).
4. **Streams flicker when switching to the Map tab** from Conditions or Runs (T451). Runs ↔ Conditions is fine.
   ⚠ T451 is being worked by another lane right now (the step-screen agent, with T443/T459). Check the STATUS.md top
   entry. If that lane hasn't closed T451, leave it alone and coordinate through the owner.

The mobile web lags only in Chrome on the Pixel 9; desktop is fine. The web is already capped at pixel ratio 2.

## Tickets to read (in this order)
- `app-integration/tickets/T420-map-pan-lag-pixel-9.md`: the main ticket. It has every table so far: the stress method,
  the CPU split, the tile-ids fix, the web proxy, the per-layer cost, the zoomed-out polygon costs, and the 4 ideas not
  yet tried (the "Next" list near the end).
- `T401-pan-edge-pop-in.md`: the prefetch delta was measured with no gain and not shipped. The per-source delta was not
  tried.
- `T448-native-pixel-ratio-cap.md`: the native surface was capped at 823×1567 (26c59849f). Its gain can only show on a
  real GPU. iOS is not done.
- `T451-map-tab-switch-flicker.md`, `T452-fling-glide-stutter.md`, `T419-…` (stream simplification), `T360-…` (memory).

## What is already established (don't redo)
- **Both maps draw on the GPU.** The lag is CPU work beside the drawing.
- **On a normal emulator the map runs at 60 fps.** The lag reproduces only on the stressed emulator (2 efficiency cores).
  That emulator's GPU is the Mac's, passed through `goldfish_pipe`. So fill and GPU costs **cannot** be judged on it,
  only CPU costs.
- **The app's own JS costs are fixed:**
  - tile ids are sent once per watch (deb635819): p50 83 → 50 ms, tile events 958 → 37.
  - one style per tab switch (e63c921c3).
- **What's left on the stressed emulator:** ~50 ms p50 and 16–20 % of moving frames missing a tile. That's MapLibre's own
  tile parsing and layout on a slow CPU.
- **Web proxy (SwiftShader, Pixel 9 viewport):**
  - no single one of our style layers costs anything measurable.
  - the whole basemap costs ~9 ms.
  - zoomed out, the basemap's green `landuse` fills are the cost; we already turned off `fill-antialias` on them.
- **Prefetch delta 2 or 3 vs 4:** no difference beyond noise.
- **Noise:** the same APK varies by ±10 points (missing tiles) and ±35 ms (p50) between sessions. So use 3+ runs, or
  interleaved A/B, and give ranges, never a single run.

## Leads not yet tested (ranked by my guess at payoff; measure first)
1. **Symbol placement during the pan.** The style has ~26 symbol layers (labels, icons). MapLibre re-runs collision
   placement on the render thread while the camera moves. Try:
   - fewer symbol layers at stream zooms, merging same-source labels.
   - `symbol-sort`/`text-allow-overlap` effects.
   - ⚠ Any change to what shows is a proposal for the owner. Send side-by-side shots, don't ship it.
2. **Buckets per tile.** MapLibre builds one bucket per distinct (source-layer, filter, layout) group. The stream source
   has ~7 line layers plus overlays (gear, no-access, closed, runs, the hatch).
   - Count the buckets per stream tile at z11–z13.
   - See whether layers can share layout so they share a bucket (paint-only differences group together).
   - Or cut the layer count.
3. **Stream tile weight at z11–z13**, the zooms panned most: features and vertices per tile, the attributes carried, and
   the `part`/section ids. Smaller tiles mean less parse. Tile changes need a tile build (heavy.sh) and the render gate.
4. **Render backend on the Pixel 9: Vulkan vs OpenGL ES.** The test bed already builds an `-opengl` APK. A side-by-side
   APK pair is a cheap owner check.
5. **The per-source prefetch delta** for the streams source only (`Source.setPrefetchZoomDelta`, not exposed yet), and
   the ambient tile cache size.
6. **Zoomed out:** the T420 "Next" list:
   - basemap `landuse` SIMPLIFY 4.0 below z10.
   - `landuse_park` fading in later.
   - dropping the 4 % region/MU tints.
   All three are look changes, so the owner decides.
7. **React/JS during a gesture:** prove that `Map.native.tsx` doesn't re-render mid-pan and that the JS thread doesn't
   contend. Use a Hermes profile on the stressed emulator; the JS share was 27 → 21 % after the fix.
8. **A real trace.** A Perfetto system trace from the Pixel 9 during a pan would end the guessing about the GPU. This
   needs the owner to run one command over USB.
   - Owner rule: reproduce on the emulator, not their phone. So a trace is an ASK, only if the emulator can't separate
     the leads.
   - Give a one-line `adb`/`perfetto` command plus a config file.

## Tools you have
- **Test bed:** `app/tools/testbed/README.md`. Run it as
  `scripts/heavy.sh testbed 30 -- bash app/tools/testbed/session.sh --build -- node app/tools/testbed/run.mjs <scenario>`.
- **Scenarios:**
  - `pan`: the `[drawn]` probe gives frames missing tiles and rest → full.
  - `perf`: budgets.
  - `glide`, `feel`, `switch` (T451), `memory`, `heapprof`.
- **Stress knobs:** `TESTBED_EMU_ECORES=1 TESTBED_EMU_ARGS="-cores 2"`, `PAN_AT=lon,lat,zoom`, `PAN_MS=120` (a fling),
  `PAN_COLD=1`.
- **Standard places:**
  - Coquitlam z13 (dense).
  - Fraser Valley z11 (`-122.05,49.25`).
  - Lower Mainland z7/z9 (`-122.5,49.3`).
- **Old runs** are in `app/tools/testbed/out/runs/` (git-ignored; compare against `t420-after2-*`, `t448-*`, `t401-*`).
- **CPU split:** `simpleperf -e cpu-clock` (the emulator is rooted).
- **Frames:** `dumpsys gfxinfo` / SurfaceFlinger.
- **The native binding** is patched in `app/patches/@maplibre__maplibre-react-native@11.5.0.patch` (Android
  `MLRNVectorSource.kt`, `MLRNMapView`; iOS `MLRNVectorSource.m` …). New native knobs go there. Build both platforms;
  the iOS sim build is `deploy/staging/build-ios-sim.sh --testbed`.
- **Map code:** `app/packages/map/src/`:
  - `Map.native.tsx` / `Map.web.tsx`.
  - `native/useColouring.ts`, `native/panProbe.ts`.
  - `runtime-style.ts`, `pixel-ratio.ts`, `web/pixelRatio.ts`.
- **Layers** come only from `app/packages/map/style/layers.source.json`, then `pnpm style:build` (rule 28, tokens only).
- **Web proxy:** headless Chromium at the Pixel 9 viewport (412×915 @ 2.625), SwiftShader. The earlier scripts were
  scratch and not kept. If you rebuild one, keep it under `app/tools/`.

## Rules that bite here
- **Heavy jobs one at a time** through `scripts/heavy.sh`. Only one emulator session at a time: another lane may be
  using the test bed, so check before starting.
- **Kill only PIDs you started.** Never pkill by pattern; the owner's tile server is on :39217.
- **Commits:** `git commit -- <your paths>` only (other lanes commit in parallel); branch `redesign/stream-sections`.
- **No silent fallbacks** (rule 46), e.g. a capability check that quietly lowers quality must report itself.
- **Any change that alters what the map shows is a proposal.** Send the owner before/after images with SendUserFile;
  they can't open repo paths.
- **APK:** the next one (0.6.8) bundles other lanes' work. Don't build it alone. Hand perf changes to the main chat or
  the owner for that batch, or ask the owner before a perf-only A/B APK (`deploy/staging/build-apk.sh --play`).
- **Records:** keep T420 (and T401/T448 if touched) updated with before/after tables and commit hashes. Put a short
  entry at the top of `handoff/PLAN/STATUS.md`.

## Done means
- Same scripts, 3+ runs, before/after ranges on the stressed emulator, for each lever shipped.
- A clear split, written into T420, of what the emulator can prove and what only the phone can.
- An owner phone check list in `OWNER-TODO.md`: what to pan, where, and what should feel different. Optionally A/B APK
  pairs for GPU-side levers (Vulkan/GL, pixel cap, prefetch).
