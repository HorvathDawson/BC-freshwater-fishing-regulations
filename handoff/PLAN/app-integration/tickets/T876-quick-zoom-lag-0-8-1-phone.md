# T876 — Quick zoom in and out lags more on the phone (0.8.1)

Status: OPEN (app chat). Trace read 2026-11-18 ~20:45; cause not proven yet, bench next.

Owner 2026-11-18 20:37, verbatim: "More laggy quick zoom in and out."
Trace: his Pixel 9 ("tokay", Android CP3A.261005.005), System Tracing, captured 20:35:
`/Users/dawson.horvath/.claude/uploads/b8ddfca9-44eb-400e-a9bf-bd89b4b0f359/88da0716-trace-tokay-CP3A.261005.005-2026-11-18-20-35-10.perfetto-trace`
(9.6 s; read with `app/tools/perfetto/` queries, scripts in the app chat's scratchpad `t876/scripts/`).

## What the trace shows

- **Build**: `ca.canifishthis.app` versionCode 36 (0.8.1). Map mode can't be read from the trace (no logs, no JS names).
- **Gesture**: six quick two-finger pinches between 2.0 and 6.2 s, then two taps.
- **Map frames** (MapLibre's Vulkan thread `RenderThread 72`, `QueuePresentKHR`): 116 frames over ~5 s (~23 fps);
  57 gaps > 16.7 ms, 15 > 50 ms, **13 > 100 ms, worst 384 ms**. One pinch (5.56–5.94 s) showed 1 frame in 376 ms.
  The app's UI layer had 55 frames and 2 janky ones, so the lag is in the map.
- **Where the time goes**:
  1. **Map render thread: 4.41 s CPU in ~5 s** (72 % on the big core, cpu7). Every long gap is ONE unbroken burst of
     render-thread work (99 % of the gap), mostly while the tile workers are idle (e.g. 5.63 s: 381 ms, workers 0.3 ms;
     3.01 s: 271 ms, workers 4 ms). So it is not tile parsing or JS. The work is on the render thread: uploading
     buckets, symbol placement, or applying feature-state batches. The trace has no CPU samples and MapLibre has no
     trace markers, so it can't say which. Present/dequeue waits are ~0.8 s of it.
  2. **Tile workers 1–4: 5.2 s CPU** in bursts of up to 2.3 cores as each pinch crosses zoom levels. They compete
     for cores 4–7 but don't stall frames themselves.
  3. JS 0.60 s, UI 0.67 s (input 188 ms), one 39 ms GC: small.
- **Not the device**: no thermal limits (all cdev 0; cpu7 reached 3.1 GHz). No download: OkHttp ~9 ms, PMTiles
  reader 67 ms. kswapd ran only before the gesture.
- **Not the data**: r-fc137c4040fa ships the same atlas (`4ae7b62955bc9990.9674331adca9`, 197 MB) as 0.8.0's releases
  r-86d43303bee2 / r-f1f2e8da9666.
- **Not MapLibre**: 0.8.0 and 0.8.1 were built with the same patched AAR (build id `7bf3dc29141e…`).
- For scale, the 2026-11-07 pan trace (0.6.8, stock MapLibre, `app/tools/testbed/out/traces/pixel9-068-…`) used
  ~83 ms of render-thread CPU per frame with 50 gaps > 100 ms. Today's trace uses ~38 ms per frame, so the map is
  better overall; what the owner feels is the zoom stalls.

## Suspect

The only change between 0.8.0 and 0.8.1 on this path is the app's map code. Likeliest: **4c005321e** (T848/T855
stream split). Every stream feature now sits in TWO line layers, `stream` and `stream-heavy`. Each layer's opacity is
data-driven by feature-state (`["case", heavy, 0, v]` / `["case", heavy, v, 0]`), and each layer has its own colour.
On MapLibre Native, a data-driven opacity of 0 still uploads and draws the feature. Each feature-state batch (one per
burst of new tiles, `useColouring` `flush`) re-evaluates and re-uploads both layers' colour and opacity, and on
Salmon that colour is the large `sp` lists. On the web the split was a clear gain because it took feature-state off
nine mark widths. On the phone, with the T420 delta patch, that part was already cheap, so the extra layer may cost
more than it saves. Its fix 2 (`visibility: none` marks) should help, not hurt. T831's trace sources are empty off
the route map.

## Next

1. A/B on the emulator, same view and gesture: the 0.8.0 APK (7ed1598e6) against 0.8.1 (84b1eb4f5), and 0.8.1 with
   `splitStream` bypassed on native only (`runtime-style.ts`: `stream` = `streamPaint`, no `stream-heavy`), on Trout
   and Salmon. Script six fast pinches in and out (`zoom-flick`'s `touchscreen` events; FLICK_AT=-121.95,49.17,11).
   Record a Perfetto trace with `linux.perf` callstack sampling on the render thread. Measure map frames from
   `QueuePresentKHR` (gaps > 100 ms, worst) and name the render-thread functions from the AAR's unstripped `.so`.
2. If the split is the cause: on native, keep `stream` as one layer whose width reads feature-state only in
   `stream-heavy`'s guard (no second full-coverage layer), or filter `stream-heavy` to the lit/closed ids
   (a style filter, not a feature-state opacity). Check the look on pictures before landing.
3. Cheap try either way: the T420 lab send gap (`stateGapMs`, `lab.ts`: 0 today, 250 the other setting), so a
   pinch sends a few feature-state batches instead of one per tile burst.

## 2026-11-18 ~20:51: not a 0.8.1 regression (owner)
Owner (to the main chat), verbatim: "Also the speed on this release is better so it wasn't this build it was always a bit
of an issue". So this is an after-launch performance ticket, not a blocker for the production phone builds. The emulator
A/B (0.8.0 / 0.8.1 / split off on native / stateGapMs=250) runs bounded; a fix that helps quick pinch-zoom without losing
the zoom-out gains lands as an ordinary change after launch.
