# Colour latency: why a pan draws grey first, and what fixes it

2026-10-21. The owner, 2026-10-20: "when we pan it takes a second to color from default". Could colouring be
GPU-accelerated? This is a measurement report. No production code changed: the prototypes live in `harness/`.

## The answer in four lines

- **The GPU already does the colouring.** Feature-state and data-driven colour both go into paint attribute arrays the
  GPU reads. The grey comes from **when** today's path hands out the state: it waits for MapLibre's `idle`, which
  fires only after every tile has loaded and the 300 ms symbol fade has finished, and then goes through a React
  round trip. A newly arrived tile draws grey for about 0.5 s.
- **Web: colour each tile in its own `sourcedata` event, inside the adapter (A).** The tile's first frame then has the
  right colour: 0 grey frames in 36 of 36 measured pans, under 1 ms of work per pan, no long tasks.
- **Store the section's PROFILE in the feature-state, and put the day in the style (the P variants).** This is
  base+deltas as the paint index already holds it. A section's state is set once per session. A day change is one
  `setPaintProperty` with a match of about 115 labels (443 bytes, 0.5–3 ms) and **zero** feature-state calls.
- **Native: OPEN — decide after N2.** The binding has no per-tile event. The ring around the user (E1P) proposed here
  is **not supported as stated**: the independent verification found the E numbers **4.6–6× too low** (see
  "Corrections" below). Options for N2 to settle: a native tile-loaded event or a bulk feature-state module (AP on
  native, no index), or E with a zoom floor. The "one approach for both platforms (E1AP)" line is withdrawn.

**SHIPPED (web), 2026-10-21: AP** — see "AP in the real app" at the end: 0 grey frames after a pan at z8, z11 and z13,
desktop and phone, against 8 of 9 pans grey for ~0.5 s before.

## Today, in the real app (expo web dev build, `app_run.py`)

| config | pans grey after end | pan end → every visible section coloured, median / max | push starts after pan end | worst grey share of visible sections |
|---|---|---|---|---|
| desktop 1280×800, Metal | 8/8* | 512 / 561 ms | 521 ms | 97% |
| phone 390×844, Metal, 4G + 4× CPU | 8/8* | 547 / 857 ms | 535 ms | 95% |

\* The 9th pan returns to an area already coloured. The push itself takes 0.0–0.8 ms for 50–540 calls. All of the
latency is in waiting for `idle`.

## The options, measured (harness: the app's real style, the live atlas and release paint, MapLibre GL JS 5.24.0)

Each run has 12 camera steps at (−121.0, 52.0) z11: 9 one-viewport pans, a zoom out, a zoom in and a 3-pan flick.
"Grey" means a rendered frame in which at least one visible stream, lake or wetland section did not yet have its
status. The probe checks this with `queryRenderedFeatures` on every `render`. The timings come from separate runs
without the probe. The status index was decoded as `statusIndex.ts` decodes it. Day change: day 294 → day 120.

### Desktop 1280×800, Metal GPU

| option | pans grey while moving / after end | pan end → all coloured | colour work per pan, median / max | long tasks over 12 steps, sum / max | frame gap p95 / max | day change: sync ms, feature-state calls | heap MB after pans / day |
|---|---|---|---|---|---|---|---|
| today (idle → set; no React here) | 10 / 9 of 12 | 518 / 534 ms | 6.1 / 10.5 ms | 0 / 0 | 9 / 14 | 10, 324 (visible only) | 58 / 51 |
| **A** per tile, sync | **0 / 0** | **0** | 0.2 / 0.6 ms | 0 / 0 | 9 / 14 | 11, 4,421 | 45 / 46 |
| **AP** A + profile state | **0 / 0** | **0** | 0.1 / 0.7 ms | 0 / 0 | 10 / 16 | 13, **0** (setPaint 0.5 ms) | 38 / 54 |
| B `match` on `["id"]`, 621–889k ids | 0 / 0 | 0 | — | **8,498 / 195** | 9 / 198 | 703, 0 (setPaint 588–683 ms) | **2,516** / 679 |
| D sparse set-all (1.63M calls) | 0 / 0 | 0 | — | **10,148 / 1,460** | 135 / 1,468 | 314, 1.63M | 343 / 687 |
| D full set-all, streams only (cost reference) | n/a | n/a | — | 5,930 / 793 | 132 / 800 | 96, 543k | 166 / 473 |
| E0 viewport, no margin | 3 / 1 | 0 | 1.1 / 2.2 ms | 0 / 0 | 10 / 16 | 11, 2,011 | 45 / 46 |
| **E1** viewport + 1 viewport each side | **0 / 0** | **0** | 2.2 / 4.9 ms | 0 / 0 | 11 / 33 | 14, 6,262 | 61 / 60 |
| E2 viewport + 2 each side | 0 / 0 | 0 | 4.2 / 7.0 ms | 0 / 0 | 12 / 59 | 14, 13,633 | 61 / 65 |
| E1A E1 + A | 0 / 0 | 0 | 2.5 / 4.9 ms | 0 / 0 | 11 / 30 | 11, 10,683 | 62 / 63 |
| **E1P** E1 + profile state | **0 / 0** | **0** | 2.5 / 4.0 ms | 0 / 0 | 10 / 32 | 14, **0** (setPaint 0.5 ms) | 56 / 68 |
| E1AP | 0 / 0 | 0 | 2.6 / 5.2 ms | 0 / 0 | 10 / 32 | 11, **0** (setPaint 0.4 ms) | 63 / 75 |

### Phone 390×844 @2x, Metal GPU, 4G (120 ms RTT, 9 Mbit/s) + 4× CPU throttle

| option | grey moving / after | pan end → all coloured | work per pan | long tasks sum / max | frame gap p95 / max | day change: sync ms, calls | heap MB |
|---|---|---|---|---|---|---|---|
| today (idle) | 10 / 9 of 12 | 525 / 1,243 ms | 11.3 / 21.8 ms | 0 / 0 | 11 / 13 | 38, 150 | 24 / 47 |
| **A** | **0 / 0** | **0** | 0.1 / 0.8 ms | 0 / 0 | 11 / 14 | 38, 1,309 | 39 / 25 |
| **AP** | **0 / 0** | **0** | 0.1 / 1.0 ms | 0 / 0 | 11 / 16 | 44, **0** (setPaint 3 ms) | 46 / 36 |
| B | 0 / 0 | 0 | — | **9,816 / 701** | 11 / 622 | 2,439, 0 (setPaint 2.4–3.0 s) | **1,577 / 2,334** |
| D sparse | 0 / 0 | 0 | — | **12,026 / 1,378** | 11 / 1,384 | 1,108, 1.63M | 261 / 570 |
| E0 | 3 / 2 | 0 | 3.3 / 7.5 ms | 0 / 0 | 11 / 16 | 37, 603 | 41 / 27 |
| **E1** | **0 / 0** | **0** | 5.6 / 9.4 ms | 0 / 0 | 11 / 19 | 43, 2,564 | 49 / 28 |
| E2 | 0 / 0 | 0 | 6.8 / 11.0 ms | 0 / 0 | 12 / 30 | 44, 5,800 | 48 / 36 |
| E1A | 0 / 0 | 0 | 4.3 / 5.7 ms | 0 / 0 | 11 / 19 | 41, 3,873 | 40 / 27 |
| **E1P** | **0 / 0** | **0** | 3.8 / 6.5 ms | 0 / 0 | 11 / 22 | 41, **0** (setPaint 2.4 ms) | 47 / 40 |
| E1AP | 0 / 0 | 0 | 4.1 / 8.8 ms | 0 / 0 | 11 / 19 | 39, **0** (setPaint 3.4 ms) | 50 / 40 |

### Phone 390×844, SwiftShader (CPU GL, `--use-angle=swiftshader`)

Headless Chromium with no flags already gets SwiftShader (`ANGLE (Google, Vulkan 1.3.0 (SwiftShader…))`). Metal
needs `--use-angle=metal --enable-gpu --ignore-gpu-blocklist`. The verdicts are the same on both: today 10 / 9 grey
pans, 514 / 534 ms. A, AP, E1, E1A, E1P and E1AP had 0 / 0. E0 had 3 / 2. B had 3,050 ms of long tasks and a 1.7 GB
heap. D sparse had 3,759 ms of long tasks. Frame gaps are longer (p95 13–18 ms) because the GPU is a CPU.

Tables: `harness/table.py runs/ results.json`. Per-run summaries are in `runs/`.

## Why B, C and D lose

- **B (status in the style on `["id"]`).** One day is a match with **621,517 ids (4.45 MB)**. Day 120 has
  **889,093 ids (6.30 MB)**. `setPaintProperty` takes 0.6 s on desktop and 2.2–3.0 s on the phone profile. Every
  newly loaded tile re-parses the expression in the worker. That gives 8.5–9.8 s of long tasks over 12 steps, `idle`
  taking 1–4.9 s, and a heap of **1.6–2.5 GB**. A `step` over contiguous handle runs (Bs) needs about 261k stops per
  day; we did not run it uncapped. With caps: 30k stops = 235 KB, setPaint 89 ms; 100k stops = 857 KB, setPaint
  255 ms, 3.6 s of long tasks, 1.2 GB. Handles are not spatially ordered, so runs stay short. On native, N1 measured
  225 ms for a style swap with **83** ids.
- **C (region base + overrides).** The tiles carry **no** region or MU code on stream, lake or wetland features
  (`tile-contract.json`: `section_id, name, ord` / `…, water, area_m2` / `…, water`). Suppose we added the regs
  store's `section_place` (23 region sets; geometry-like). Then the base colour of each region set would be its
  commonest status that day (`count_region_overrides.py`). The overrides would still be **527,070 / 534,646 /
  673,905** sections (min / median / max over the year), against 621,517 / 628,412 / 889,093 non-base sections
  without the region code. That is only a 15–24% cut. The reason: **"own" (a named water's own rules) is
  290–572k sections every day.** Region-wide closures (up to 598k closed) are the only part a region code removes.
  C is B at about 0.8× the size, so it fails for the same reasons, and it would need a tile rebuild. Rejected.
- **D (set everything once per day).** Sparse means state only for the non-base sections, plus a style fallback to
  base: **1.63M calls** over 3 source-layers. The loop finishes in 173 ms (564 ms on the phone profile), but
  MapLibre applies the state lazily into every tile it loads. Over the pans that comes to 10–12 s of long tasks, one
  task up to 1.5 s, and a 260–690 MB heap. A day change takes 0.3–1.1 s of sync work. On native, 1.63M bridge calls
  at about 0.1 ms each is about 2.7 minutes. Rejected.
- **The status index already IS base + deltas (the paint file).** 115 day-profiles × 366 days, plus runs of handles
  → profile. 1,061,214 sections are in no run, which means base all year. That is why P works: the per-section fact
  (the profile) never changes within a release. Only the profile → status-of-the-day table changes, and that table is
  the ~115-label style expression. From the per-day counts (`count_overrides.py`, `overrides_by_day.json`): status
  changes between consecutive days on only **39 days a year** (median 0 sections, max 371,234).

## E: a ring around the user (the owner's idea)

> **CORRECTION (independent verification, 2026-10-21).** Every E row in the tables above, and the E costs below, used
> a cell index built from **z11 tiles only** (`build-cell-index.mjs`), and z11 tiles are thinned: one z11 cell
> (335/676) carries 94 ids in its own tile but 444 in its z14 children. The harness index held 67k (cell, id) pairs
> for the test area; a complete one holds **431k** — the E figures are **4.6–6× too low**. Re-run with the complete
> index: E1P calls per step **9,446 / 2,809** median (desktop / phone; was 1,627 / 671), **42,415 / 12,982** max (was
> 7,515 / 2,389), start-up 19.6k calls (was 4.3k). "No long tasks" no longer holds: E1P **256 ms** of long tasks on
> desktop (max 193 ms, the zoom-in), 62 ms on the phone; E1 357 ms. Still 0 grey steps. On native at N1's 0.1 ms per
> call that is **~280 ms median and ~1.3 s max per step** at z11 (not 67 / 240 ms), ~0.5 s at start-up, and at z9 /
> z8 a phone ring is ~57k / ~230k calls (~5.7 s / ~23 s). **Native E1P is not supported as stated**; it would need a
> zoom floor or a zoom-split index (hard: tippecanoe drops by density, not by section). The province index size below
> (~2.1M pairs, 1.11 cells per section) was right.

- **The buffer MapLibre gives you is about none.** GL JS requests only the tiles that cover the viewport. It keeps
  recently seen tiles in an LRU cache but does not prefetch a ring, so `querySourceFeatures` cannot see ahead. Native
  adds only `prefetchZoomDelta`, which fetches LOWER-zoom parents; those carry the big rivers, not the small streams.
  So E needs its own **section → cell index**. The harness builds one for the test area from the z11 tiles' MVT ids
  (`harness/build-cell-index.mjs`, ids read straight from the protobuf with no geometry decode).
- **Province index size** (`cell_index_size.py` on a z14 patch of 6,561 tiles and 44,157 sections): 1.03–1.13 cells
  per section at z10–z12. That is **about 2.1M (cell, id) pairs, about 5 MB raw** before gzip. It is derived from
  geometry, so it belongs with the TILES (same vintage, built by the tile pipeline). A data release never touches it.
  If handles were assigned in spatial (Hilbert) order, the index would shrink to a short ranges table. That change
  would be made in the pipeline.
- **Margin.** With no margin (E0), 3 of 12 steps showed grey while the camera moved: the zoom-out and the flick reach
  past the viewport before `move` fires. Margins of 1 and 2 viewports gave 0 grey frames everywhere. Cost per step:
  E1 is 1.6k (desktop) / 671 (phone) feature-state calls median, 7.5k / 2.4k max (the zoom-out), dropped cells
  included. E2 roughly doubles that. **Choose E1.**
- **E + A.** On web, A alone already gives 0. A is the safety net that makes E's margin irrelevant there, and it
  costs about 0.3 ms more per step.

## Recommendation

**Shared core (both platforms):**
1. The adapter keeps the decoded status index in memory. Lookups go handle → profile, synchronously, with no React.
2. Feature-state `{p: profile}` (on Android as a **string** label, e.g. `"p17"`, because Android `<Layer paint>`
   rejects numeric `match` labels). It is set once per section per session and never re-pushed for a day change.
3. The status paint becomes `["match", ["coalesce", ["feature-state","p"], -1], -1, unmapped, [own…], own,
   [closed…], closed, [none…], unmapped, base]`, rebuilt for the chosen day. That is ≤115 labels and about 450
   bytes. The style stays small, the tiles stay geometry-only, and a data release needs no tile rebuild.

**Web: AP**, with E1 optional. In the adapter, `map.on('sourcedata', e => e.tile && …)` sets `p` for the tile's
stream, lake and wetland ids, synchronously, before the tile's first draw. The `idle` → `onVisible` → `statusData` →
`setFeatureState` loop goes away for colouring; it can stay for anything else that needs the visible list. Caveat:
the cheap way to get a tile's ids uses GL JS internals (`tile.latestFeatureIndex.loadVTLayers()`, or the raw MVT bytes
through `harness/mvt-ids.js`). Those must be pinned to 5.24 behind a test. The public route,
`querySourceFeatures` per source-layer, is heavier and was not measured.

**Native: OPEN — decide after N2** (a native tile event or bulk state, or a zoom floor). The text below is the
original proposal, kept for the record; its numbers are corrected above (E is 4.6–6× costlier than stated).

~~**Native: E1P**~~ (ring of one viewport on each side, P state, day in the style), from the province section → cell
index shipped with the tiles. Estimate from N1's 0.1 ms per call: ~~about 67 ms median and 240 ms max~~ (corrected:
~280 ms median, ~1.3 s max at z11) of async TurboModule work per pan for a phone viewport. A day change costs one paint-property set (N1 measured style
operations at about 225 ms for a whole style swap; a single paint property should cost less) and **no**
feature-state calls. Without P, a day change would be about 2.6k calls, about 260 ms. Native reads the atlas from
local disk, so a tile arrives in tens of ms. The ring has to be set before that, which is why the margin matters more
on native than on web.

~~**If one approach must serve both: E1P**, plus A on web as a safety net (E1AP).~~ Withdrawn: see the correction.

### What needs the emulator (N2) to confirm
- That MapLibre Native applies a feature-state set **before** a feature's tile loads (GL JS does: `SourceFeatureState`
  initialises each new tile from the stored state). E depends on this.
- First-frame correctness during a fast fling at z13–14 with E1 (ring calls in flight while local tiles load), counted
  per frame with `onDidFinishRenderingFrame` + `queryRenderedFeatures`.
- The cost of `setPaintProperty` (or a `<Layer paint>` prop change) for the 115-label string `match` on Android.
- Whether the binding exposes any source or tile-loaded event. If it does, AP works on native too, without the index.

## Caveats
- **State, not pixels.** The probe reads each rendered feature's feature-state, not the painted colour. GL JS
  coalesces state into the paint arrays in the same frame's source update, before drawing, so a frame where the state
  is right is drawn in the right colour. That is MapLibre's design, not a pixel test.
- **One place, one zoom.** Everything ran at (−121.0, 52.0) z11 ± 1 with 12 steps. The E cell index covers only the
  test area and is built from z11 tiles; the province size is extrapolated from a z14 patch. Denser areas (the Fraser
  Valley) will have more ids per step.
- **The "today" row in the harness has no React.** It is a `setTimeout(0)` stand-in. The real-app rows above include
  React and are about the same, because the wait is `idle` (tiles + the 300 ms fade), not the push.
- **Not a phone.** 4× CPU throttle and 4G emulation on an M5 Pro, in headless Chromium. Native numbers are
  estimates from the N1 emulator spike.
- Memory is `performance.memory.usedJSHeapSize` (JS heap only; GPU buffers and worker heaps are not counted).

## Files
- `harness/`: `harness.js` (the page and every option: `?mode=none|idle|A|AP|Ds|Dfull|Bm|Bs|E0|E1|E2|E1A|E1P|E1AP`,
  `&probe=1`), `server.mjs` (serves the live tiles and paint read-only on its own port), `run.py` (Playwright
  driver), `app_run.py` (today's path in the real app), `dump-style.ts` (the app's real runtime style),
  `build-cell-index.mjs` + `mvt-ids.js` (E's index), `table.py`.
- `count_overrides.py` → `overrides_by_day.json`; `count_region_overrides.py` → `region_overrides_by_day.json` (C);
  `cell_index_size.py` (E index size); `results.json` and `runs/` (per-run summaries).
- `harness/app_measure.py` + `harness/app_table.py` (before/after in the real app, below); `after/` (runs + shots).

## Other corrections from the verification
- **A day change under P reloads every atlas tile.** In 5.24, setting a paint property that reads feature-state marks
  the source for reload (`Style._updateLayer`). The 0.5–3 ms `setPaintProperty` is the sync part only; `idle` comes
  ~320–360 ms later (today's path: 12–43 ms). The verifier's pixel test: the picture settles 45–55 ms after the change,
  so it is not a stale window, but it grows with the number of loaded tiles.
- **The probe cannot see a P day change** (it derives "want" and "effective" from the same table), and skips tidal /
  outside sections. Day changes are pinned instead by unit tests (core `dayProfiles` ≡ `statusOn` on the pipeline's
  fixture; the profile expression whole) and, in the real app below, by the settled pixels.
- The recommended expression mixed a numeric fallback with string labels; the shipped one coalesces to `""` (a match
  takes one label type), and labels are strings (`p17`, `b` for unlisted) for Android.

## AP in the real app (shipped, web) — before / after

Commit 1340c7b7. `harness/app_measure.py` on the expo web dev build of this branch (release r-6960fe8201e3, tiles
d768d6e135e62b99.c9f9604f12bf, own servers on :39471/:39472, Metal), 9 one-viewport pans at (−121.0, 52.0) at each of
z8, z11 and z13, then three day changes ("One month earlier") and a theme change (Layers → Palette → Dark). "Grey" = a
rendered frame with a visible stream / lake / lake_flowing / wetland_flowing section that has no colour state
(`status` before, `p` after). Work = the time inside the colouring per pan (`setFeatureState` time, or the map's own
`__colourCost` record of id extraction + labelling, whichever is larger). Before = 1014fec7, the idle → React path.

| run | zoom | pans grey moving / after | grey frames after | pan end → all coloured ms, median / max | work per pan ms, median / max | setFeatureState per pan, median / max | long tasks ms |
|---|---|---|---|---|---|---|---|
| before, desktop 1280×800 | z8 | 8 / 8 of 9 | 484 | 504 / 578 | 0.7 / 1.2 | 2,853 / 4,905 | 0 |
| before, desktop | z11 | 8 / 8 | 490 | 527 / 563 | 0.4 / 1.1 | 855 / 1,629 | 0 |
| before, desktop | z13 | 8 / 8 | 481 | 510 / 552 | 0.1 / 0.3 | 315 / 486 | 0 |
| **after, desktop** | z8 | **0 / 0** | **0** | **0** | 0.2 / 2.1 | 308 / 574 | 0 |
| **after, desktop** | z11 | **0 / 0** | **0** | **0** | 0.2 / 1.3 | 245 / 898 | 0 |
| **after, desktop** | z13 | **0 / 0** | **0** | **0** | 0.2 / 0.3 | 96 / 135 | 0 |
| before, phone 390×844, 4G + 4× CPU | z8 | 8 / 8 | 624 | 574 / 889 | 2.0 / 4.0 | 1,620 / 3,000 | 108 |
| before, phone | z11 | 8 / 8 | 504 | 536 / 867 | 0.6 / 1.4 | 528 / 900 | 0 |
| before, phone | z13 | 8 / 8 | 398 | 490 / 565 | 0 / 0.8 | 132 / 168 | 0 |
| **after, phone** | z8 | **0 / 0** | **0** | **0** | 0.8 / 1.7 | 180 / 321 | 0 |
| **after, phone** | z11 | **0 / 0** | **0** | **0** | 0.2 / 1.1 | 42 / 152 | 0 |
| **after, phone** | z13 | **0 / 0** | **0** | **0** | 0.8 / 1.6 | 28 / 73 | 0 |
| after, PUBLIC route, desktop | z8 / z11 / z13 | 0 / 0 each | 0 | 0 | 2.6 / 6.4 · 2.6 / 4.6 · 0.9 / 2.2 | as above | 0 |
| after, PUBLIC route, phone | z8 / z11 / z13 | 0 / 0 each | 0 | 0 | 6.1 / 13.1 · 2.6 / 5.8 · 2.4 / 3.8 | as above | 0 |

(The 9th pan returns to an area already coloured, hence 8 of 9 before. After AP a section is labelled once per
session, so the per-pan calls are only the sections never seen before.)

- **Day change** (×3): before 2,190 (desktop) / 705 (phone) `setFeatureState` calls, 0.5–1.8 ms; after **0 calls**,
  0.1–0.3 ms (desktop) / 0.7–1.4 ms (phone) sync for the new expression on 5 layers; idle +370–560 ms after (the
  tile reload above; before: 390–590 ms, which includes the next idle push); 0 grey frames; 0 long tasks.
- **Theme change** (Dark): before re-pushed 2,190 / 705 states; after **0 calls**, the rivers keep the profile paint
  (`paint profile` checked after the change — the day is remembered by the adapter, not lost to grey or plain).
- **Same picture.** The settled screenshots after the pans, the day changes and the theme change are **byte-identical**
  before and after (`after/status-*.png` vs `after/p-*.png`, 8 of 8 pairs): the profile path draws exactly the
  colours the status path drew.
- **Heap** (JS, with the per-frame probe running, so noisy): desktop start/after pans/after day/end 95 / 101 / 80 /
  193 MB after vs 94 / 106 / 89 / 83 before; phone 53 / 99 / 67 / 103 vs 43 / 78 / 118 / 150. No growth trend
  attributable to the labels; the de-dupe is a ~245 KB bitset per source-layer.
- **The public route** (`querySourceFeatures`, used where GL 5.24's internals are absent; forced with the dev flag
  `__colourPublic`): still 0 grey, at ~2.5–6 ms of work per pan (up to 13 ms on the throttled phone at z8) — a sound
  fallback, ~10× the internals route. It cannot see tiles revived from GL's out-of-view cache, so it also rescans on
  `moveend`.
- **Not measured here:** a real phone (4× CPU + 4G on an M5 Pro), a release swap in the running app (pinned by
  `profile-paint.test.ts`, call by call), native (open until N2).

## Conditions: the same engine (2026-10-21)

Owner: "the painting for regulations is way faster now very good. but we need to do the same painting speed up for the
conditions map too" — and "it should be a single pathway ideally".

**What shipped (web).** `profile-paint.ts` became `colouring.ts`: ONE engine (`Colouring`) with two CHANNELS. Both share
the per-tile labelling in `sourcedata` (one feature-state call per section carrying every channel's label it lacks), the
bitset de-dupe, the release-swap / withdraw sequence (plain → clear → restore highlight + pushed data → forget →
relabel → install), the proven-pair gate (no input, no label) and the public-route fallback.

| channel | static label (per release) | what changes | how a change reaches the map |
|---|---|---|---|
| `status` (Map tab) | `p` = the section's day-profile (`@app/core` `profileLabel`) | the day | the style: one `match` over ~115 labels (`dayProfiles`), one paint property, 0 feature-state |
| `standing` (Conditions) | the section's CLASS (`@app/data` `StandingClasses.classOf`) | the feed, the horizon, the quantity | one `Colouring.set`: the loaded tiles' sections relabelled with their class's value in feature-state `standing` (the key the `standing` mode always read); the basins by id |

- **Classes, not panels.** A section's standing is `answerFrom(panel, feed, quantity, horizon)`, and the weights depend
  on the section's own target area, so a per-panel value is the wrong answer for most of a panel's sections. On
  r-7b6e802bf22b: 249,415 panelled sections, 3,281 panels, **18,548 (panel, target area) classes** (+ a class per gauged
  lake station no panel speaks for). The one reading is `sectionStanding` (`panelStanding` = `answerFrom`'s
  contributions and `estimate` without the sheet's rows); the native path's `usePanelStandings` / `useBasinStandings`
  call the same functions; `conformance/src/release.test.ts` holds every class value to `answerFrom` for every section
  of the fixture release, both readers, every horizon and quantity. (The estimate stays in `@app/data` beside
  `answerFrom`, where it was — core holds `estimate` and `basinStanding`, data the join.)
- **Why the values are not in the style.** Measured (the paused run on 8ff6c170, `conditions/runs/table-*.json`,
  `shots/table-*`): a style table (`["at", class, ["literal", 18,548 values]]`, ~227 KB per paint, 6 layers) gave 0 grey
  pans too, but a horizon change cost **52–61 ms of long tasks on desktop and 280–315 ms on the phone profile**, settled
  +75–116 / +376–671 ms: GL 5.24 re-lays out every loaded tile on any data-driven paint change (`_updateLayer`;
  `setGlobalStateProperty` takes the same path). A per-panel feature-state set is not possible (state is per feature).
  Relabelling the loaded sections costs 0.6–1.5 ms desktop / 1.8–4.3 ms phone for 104–422 states (below), so it wins.
  Values are computed lazily per class (a horizon change asks the classes the loaded sections use) and memoised per
  (feed index, quantity, horizon).
- **The classes' build** (once, first Conditions visit; paint + flow-area + the lake road from conditions/core): rewritten
  to typed arrays with no per-section string, tuple or Set — 10–11 ms vs 42–57 ms for 249k synthetic sections (node,
  M5); the same 22,050 classes and lookups.
- **Feed independent of tiles.** Tiles carry geometry + section ids; the classes come from the release; the feed's
  standings are applied at run time and the map re-asks the feed on its own schedule (`FEED_POLL_MS` = the feed's 5-minute
  cache) — the old path re-read the feed only on the next pan.

### Measured — `harness/conditions_measure.py`, Conditions tab, the app's HOME (−121.85, 49.15)

Before = 09232f48 (idle → `onVisible` → React → `usePanelStandings` → `setData`), after = this change; same live tiles
(d768d6e135e62b99.c9f9604f12bf), release r-7b6e802bf22b and feed; own servers :39640 (data, read-only) / :39641 / :39642;
expo web dev build, Metal. Phone = 390×844 @2x, 4G + 4× CPU. 9 one-viewport pans per zoom, then the horizon chips (+1d,
+3d, Now) and the quantity (Level, Flow). "Grey" = a rendered frame drawing a section WITHOUT the value it has once the
map settles (sections nothing speaks for are never counted). The after runs predate one later edit (the ground moved
into `applyLook`; `CI=1` Metro does not watch files), which touches only the look effect; the lake-mode run below is on
the final code.

| run | zoom | pans grey moving / after | grey frames after | pan end → coloured ms, median / max | work per pan ms, median / max | state calls per pan, median / max |
|---|---|---|---|---|---|---|
| before, desktop | z8 (field) | 0 / 0 of 9 | 0 | 0 | 0.2 / 0.7 | 195 / 739 |
| before, desktop | z11 | **8 / 8** | 493 | **517 / 559** | 0.1 / 0.3 | 62 / 142 |
| before, desktop | z13 | **6 / 6** | 375 | **525 / 553** | 0 / 0.2 | 20 / 51 |
| **after, desktop** | z8 | **0 / 0** | **0** | **0** | 0.4 / 1.0 | 195 / 739 |
| **after, desktop** | z11 | **0 / 0** | **0** | **0** | 0.2 / 0.4 | 28 / 94 |
| **after, desktop** | z13 | **0 / 0** | **0** | **0** | 0 / 0.3 | 16 / 28 |
| before, phone | z8 | 0 / 0 | 0 | 0 | 0 / 2.1 | 0 / 365 |
| before, phone | z11 | **8 / 8** | 486 | **591 / 635** | 0 / 1.1 | 21 / 55 |
| before, phone | z13 | **3 / 3** | 229 | 0 / 885 | 0 / 0.7 | 5 / 23 |
| **after, phone** | z8 | **0 / 0** | **0** | **0** | 0 / 3.5 | 0 / 365 |
| **after, phone** | z11 | **0 / 0** | **0** | **0** | 0 / 1.0 | 8 / 39 |
| **after, phone** | z13 | **0 / 0** | **0** | **0** | 0 / 0.6 | 5 / 17 |

(z8 is the basin field: both paths set 190 basin states by id, so it was never grey. No long tasks during any pan.)

| change | before, desktop | after, desktop | before, phone | after, phone |
|---|---|---|---|---|
| horizon (+1d, +3d, Now): last colour change after the click | +26–35 ms (22 calls) | **first frame** (104–422 states, engine 0.6–1.5 ms) | +111–180 ms (6 calls) | **first frame** (61–297 states, engine 1.8–4.3 ms) |
| horizon: long tasks | 0 | 0 | 58–80 ms | **0** |
| quantity (Level, Flow) | +33–35 ms, 0 long tasks | first frame, 0 | +130–200 ms, 92–102 ms long tasks | first frame, 72–75 ms long tasks (React: the gauge dots, the legend) |

"First frame" = the first frame GL drew after the click already had the new values (the engine runs before GL's next
render). The after path sets more states than the old visible push (every loaded tile, cached ones too) and is still
sub-5 ms of work.

**Correct, not just non-grey** (`harness/conditions_compare.py`, every rendered section's value at settle after every
pan and change, before vs after): desktop **568 / 568 equal** (650 with no value on both), phone **245 / 245 equal**
(243 none on both). No section changed value, gained or lost one. Screenshots: `conditions/shots/before-*` /
`after-*` (same steps).

**The feed alone** (`harness/conditions_feed_swap.py`, `conditions/runs/feed-swap.json`): the data server served ONLY a
modified `/feeds/live/index.json` (every percentile p → 1 − p); nothing republished. The map recoloured on the app's own schedule (295 s: the feed's 5-minute cache, then the poll):
**76 / 76** rendered sections took exactly 100 − their value (53 visibly changed; the rest sit at 50 or on the −1
sentinel, which the flip leaves alone); the ONLY request after the swap was `/feeds/live/index.json` — **no tile, no
release byte**; **0** `setPaintProperty` calls and the stream's paint unchanged; the colouring's record shows ONE
`standing` relabel (146 states, 1.6 ms). Unit-pinned in `colouring.test.ts` and, through the real `Map.web.tsx` over a
recording maplibre-gl, `one-engine.render.test.tsx` ("A NEW FEED ALONE": feature-state on the loaded sections only, no
paint, no clear, the same map instance).

**Idle** (`harness/idle_paints.py`, 45 s left alone on each tab after settling): 0 paint, 0 layout, 0 feature-state
calls on both tabs. (The paused run's feed-swap record held 71,940 `setPaintProperty` calls over five minutes on
Conditions; its cause was not re-traced here. The likely one — the look effect re-running on every Shell render with an
inline `onError` in its deps — is gone: that dep is now a ref, and the swap below made 0 paint calls.)

### Mutation runs (each made the named test fail, then reverted)
- standing labelled by a second `Colouring` instance in `Map.web.tsx` → `one-engine.render.test` "labelled ONCE … in one
  call" fails (two calls per section / no standing at the tile event).
- the standing channel's `relabels` off → "A NEW FEED ALONE" fails (no recolour).
- the ground back in its own `whenStyled` effect → "THE GROUND goes with the look" fails (the toggle is dropped).
- `tools/one-colouring-engine.test.ts` pins the source shape (one `new Colouring(`, one `c.set(`, both channels through
  `toChannel`, the Shell only through `useConditionsColour`) and that every section colour mode reads a key an engine
  channel sets or a known pushed key.

### The two map views, audited for one pathway

| | Map tab | Conditions | now |
|---|---|---|---|
| section colouring | engine, `status` channel | idle → React → `setData` | **engine, `standing` channel** |
| basin field | — | `setData` after a React round trip | engine `setChannel("standing")`, by id |
| lake outline follows the lake | `followersOf` in the contract | same | same (one `setLayerMode`) |
| flow arrows follow the stream | follower | same | same |
| wash | `applyLook` by the stream's mode | same | same |
| "no baseline" dash | built only if the map was CREATED in `standing` (a map opened on the Map tab never dashed on Conditions) | | **always built, painted by the adapter with the stream's mode** |
| the look re-run | every Shell render (inline `onError` in deps: ~800 no-op paints per feed tick / horizon) | same | keyed on the look alone |
| basemap / satellite | own effect behind `whenStyled`: **dropped when toggled mid tile burst** (found here: the radio said Satellite, `imagery` stayed `none`) | same | **part of `applyLook`** on both maps (native already applied it after the look) |
| idle → `onVisible` | every pan re-rendered the Shell for nothing (web) | colouring | web: Conditions only, for the legend's marks (`mapModes.asksVisible`); native: both tabs (its pushed colours) |
| scale, compass, gestures | one `Map` component | same | same |

Remaining differences, each a property of the view, not of a pathway: the gauge dots (Conditions, or the temperature
choice); hidden `mu`/`region` and shown flow arrows (the style's `views[].hide/show`); the selected highlight and the
search fit (Map tab only — Conditions must not inherit a search); the tap (opens a water vs. a Conditions section, none
below the handover); the Layers button (Map tab) vs. the Showing control and horizon chips (Conditions). The route
overlay, up/down gauges and 'here' pin are on the Conditions sheet's route map (`DonorMap` → `MiniMap`, `trace`), which
colours ONLY the route's sections — since the engine polish (below) through the SAME engine's `standing` channel, with
the route as its classes; it was pushed `data` before.

### Lake modes (Stocked, Depth) and the basemap toggle

They switch through the same pathway as status and standing: the Layers sheet's choice → `mapModes` → `applyLook` →
`setLayerMode` (the lake's edge follows) + the `depth` group; the basemap now inside `applyLook` too. **Their values
come from nowhere today, and nothing is baked into the tiles**: the lake features carry `section_id, name, water,
area_m2` (`pipeline/deliver/tiles/tile-contract.json`), so no tile change is needed; the release ships no stocking or
chart data (`release/source.ts`: "charts, stocking — no producer ships them"; `lakeInfo` null, `stockingHistory` empty);
no code sets feature-state `stocked` or `survey`. Measured on the live map (`harness/lake_modes_measure.py`,
`conditions/runs/lake-modes-desktop.json`): after Stocked or Depth the rendered lakes hold only `p` — both modes paint
`missing` (plain water-blue, the edge with it). Switch cost (desktop, after the fix): Stocked / Depth / Plain drawn on
the first frame, 146 paint + layout calls (one look pass), 0 long tasks, idle +330–394 ms; Satellite / Map 0 long tasks,
imagery visible on the first frame, idle +1.9 s / +0.3 s (the imagery tiles from the network). Before the fix the
Satellite toggle was DROPPED in this run (`imagery` stayed `none`). Pan colouring for them is nil (nothing to label).
PIPELINE NEED: a per-section (or per-item) stocking date and survey state in the release; then each is an engine
channel like `standing` (label = the static fact, e.g. last-stocked day or `digitised`/`sheet`; the band by day in
the style, or a daily relabel) — no adapter code, no tile rebuild.
**DONE 2026-10-21** (`handoff/PLAN/app-data/lake-values/README.md`): the release file `lakes` (`lake_facts.bin`, per lake
section: sheet / digitised, last stocked day; FFSBC roster + both sheet rosters) and two engine channels, `survey` and
`stocked` (the band for today, relabelled on a new day), handed in as `MapProps.lakes`. Live on the web: the lakes carry
`stocked` / `survey` from a release that ships the file and nothing from one that does not, on the same tiles.

### Native (N2) — superseded by "Native: the engine on a phone" below

Unchanged in kind: `Map.native.tsx` gets the visible sections' standings pushed as `data` in the `standing` mode, now
through `useConditionsColour` (the same `sectionStanding`), and the marsh a stream flows through (`wetland_flowing`)
is pushed too — the Shell never had, so a slough drawn as a wetland polygon stayed unmeasured on a phone. The ground
is set by `applyLook` (it was a separate call after it). For the engine on a phone it needs what AP needs: a per-tile
loaded event, or a bulk feature-state call, from the binding (open, as above); then `coloursEachTile` flips.

### Files (Conditions)
`harness/data_server.mjs` (live tiles + release + feed, read-only, with a feed-index override), `conditions_measure.py`,
`conditions_compare.py`, `conditions_feed_swap.py`, `lake_modes_measure.py`, `idle_paints.py`, `conditions_table.py`;
`conditions/runs/` (before/after per profile, the settle dumps, feed-swap, lake modes, the table variant) and
`conditions/shots/` (before-*, after-*, feed-*, table-*).

### Not done / not verified
- A real phone, native (emulator), a release swap in the running app (pinned call by call in `colouring.test.ts`).
- At mount (and a release swap) the two channels install in turn, so a held tile's sections get two state calls, not one.
- The style-table variant is measured and NOT shipped (`runs/table-*`, `shots/table-*`, from the paused run).

## Engine polish (2026-10-21): the look keyed on the look, one pathway on the web, the phone's plan

**(1) A horizon change no longer re-runs the look.** `useMapModes` memoised on the horizon, so a horizon click handed
the map a new `modes` object, and `Map.web`'s look effect (keyed on prop identities, `hide={[]}` among them) re-ran the
whole pass: 139 `setPaintProperty` per click, every one a no-op. Now `useMapModes` keeps `modes` / `activeGroups` as
the same objects while their content is the same, and the look effect is keyed on the look's CONTENT (`lookKey`). Pushed
`data` moved to an effect of its own. Pinned: `ui/src/mapModes.render.test.tsx` (same objects across a horizon change;
a real mode change is a new object), `one-engine.render.test.tsx` "A HORIZON CHANGE" (through the real `Map.web.tsx`:
the next standings as feature-state ONLY, zero paint and layout calls; a real mode change still repaints).

**(2) One pathway on the web.** The Conditions route map now colours through the engine: `ui/src/routeColour.ts` hands
the map the route as the standing channel's classes (a chain section's class is its place on the chain, any other
section has none, so the engine leaves it alone) and `usePanelStandings`' values over the chain (the same
`sectionStanding`). The labels are kept through a horizon change (the panel answers no rows while it reloads; the last
route of the section is kept), so a horizon is a relabel of the route's sections, never a clear. Pushed route `data` is
the phone's alone (`useConditionsView(..., { perTile })`, from `coloursEachTile(Platform.OS)` in `ConditionsPanel`).
`Map.web.tsx` now REFUSES `p` / `standing` / `status` in pushed `data` (`withoutEngineKeys` in `colouring.ts`, on both
paths into `setData`: the data effect and the engine's restore), reporting each refused layer through `onError`.
`tools/one-colouring-engine.test.ts` now also pins: the refused keys are the channels' own plus `status`; every
`setData` in `Map.web.tsx` goes through `withoutEngineKeys`; each producer of pushed colours (`statusData`,
`pushedStandings`, `pushedRoute`) is called once, only off `perTile`; the sheet passes `perTile` and both halves.

**Re-measured on the final code** (`harness/conditions_measure.py --path state --changes-only`, which now also counts
`setPaintProperty` / `setLayoutProperty` per step; own servers :39660 data / :39661 expo web, release r-7b6e802bf22b,
live tiles and feed, HOME at z11; `conditions/runs/polish-desktop.json`, `polish-phone.json`):

| change | desktop 1280×800: paint / layout calls | engine (ms, states) | long tasks | phone 390×844, 4G, 4× CPU: paint / layout | engine (ms, states) | long tasks |
|---|---|---|---|---|---|---|
| +1 day | **0 / 0** (was 139 paint) | 0.9, 209 | 0 | **0 / 0** | 3.6, 100 | 53 ms |
| +3 days | **0 / 0** | 0.7, 209 | 0 | **0 / 0** | 4.1, 100 | 0 |
| Now | **0 / 0** | 0.5, 209 | 0 | **0 / 0** | 2.5, 100 | 0 |
| Level | 0 / 0 | 0.8, 209 | 0 | 0 / 0 | 2.6, 100 | 75 ms |
| Flow | 0 / 0 | 0.5, 209 | 0 | 0 / 0 | 1.4, 100 | 69 ms |

Every change drew its new colours on the first frame (`settledMs` 0). The phone's long tasks are React re-rendering the
sheet, legend and gauge dots (the engine's own work is the 1.4–4.1 ms column); the first +1d on the phone also takes
the first forecast table. Feature-state calls per change were 335–339 desktop / 226–230 phone — more than the engine's
own (209 / 100): the remainder are the map's other per-feature states (not traced in this pass; no paint among them).

**(3) Native: the engine on a phone — NOT implemented, the plan.** `@maplibre/maplibre-react-native` 11.5.0 offers only
`VectorSource.setFeatureState(featureId, sourceLayer, state)` — one async bridge call per feature — with no tile-loaded
event and no bulk call, so the engine cannot label a tile before its first frame there, and a JS-side engine pushing
per feature would be the pushed path again. The phone stays on push (`useConditionsColour` with `perTile: false`) and
is HELD TO THE ENGINE'S VALUES: `ui-native/src/NativeAgrees.test.tsx` renders both paths over one fixture release and
feed and requires, for every section on every pushed source-layer and every basin, at every quantity and horizon, that
the phone's pushed `standing` is exactly the web engine's label (`standingInput`) — absent where the engine leaves it
unlabelled, the sentinel where it is the sentinel (mutation: a pushed value clamped at 0 fails it). The plan, for when
an emulator run is allowed:

1. A small native module beside the binding (Expo module, Android + iOS), `ColouringModule`, holding per source-layer
   the CHANNEL INPUT the engine holds: the labelling as a typed array `classOf[handle] -> class` (int32, the release's
   `StandingClasses` / the day-profile index — shipped once per release, ~8 MB for 1.96M handles, from the same release
   file the web reads) and the values as `Float32Array` per class (status: the day's profile table; standing: the class
   table), set from JS with one call each (`setLabels`, `setValues`).
2. In the module, a tile-loaded hook: Android `MapView.addOnDidFinishLoadingTileListener`-equivalent via the core's
   `MapObserver::onTileAction(TileOperation::EndParse)` (believed present in the MapLibre Native core the binding ships —
NOT verified against 11.5.0's native sources in this pass); on it, walk
   the tile's features of `stream` / `lake` / `wetland` and call the core's `setFeatureState` for each, natively —
   no bridge per feature, before the tile's first frame where the observer fires on the render thread's parse.
3. A values change (horizon, feed, day): the module relabels the loaded tiles' features from the new table, one native
   pass (the web engine's `set` with `relabels`); a labelling change: clear + relabel, the web sequence
   (`colouring.ts` steps 1–5), the `selected` / `water` highlight put back by the module's restore callback into JS.
4. `Map.native.tsx` then takes `status` / `standing` exactly as `Map.web.tsx` does, through the same `toChannel`
   shape, and `coloursEachTile` returns true for it; the push producers become dead and are deleted with
   `withoutEngineKeys` applied on native too, and `NativeAgrees.test.tsx` becomes the module's contract test.
5. Measure on the emulator with the N2 harness: grey frames after a pan, the cost of a horizon change, bridge calls.
   Risk to check first: whether `onTileAction` is reachable from the binding's map view without forking the binding
   (if not, a patch-package on 11.5.0, or an upstream PR adding a tile event).

**Not done / not verified (polish):** the route map was not driven in a browser in this pass (pinned through the real
`Map.web.tsx` by `one-engine.render.test.tsx` "THE ROUTE MAP" and the hook tests); the pans were not re-run (only the
look changed, which a pan does not touch); the extra non-engine feature-state calls per change were not traced.
