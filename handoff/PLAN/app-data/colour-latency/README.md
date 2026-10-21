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
