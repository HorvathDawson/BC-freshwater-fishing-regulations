# T848 — The web map lags zooming out over Intata Reach (the Nechako Reservoir lakes)

Status: OPEN, cause found (owner 2026-11-18 ~13:15; target 0.8.1, the web site can ship it earlier).

Source: owner 2026-11-18, verbatim: "Here is a trace from zooming out in map on a high detail big lake and it lags out
bad … Be nice to fix this in 0.8.1 if it is still an issue in 0.8.0". In the dialog: where = "In Chrome (staging web)";
which lake = "Intata Reach area with the multiple lakes there".

## What the owner's trace shows (Pixel 9, Traceur, ~6.8 s)

The app was a cached background process (0 ms CPU). Chrome's renderer main thread ran ~100% of a core for ~3.5 s
(2.83 s CPU) with a DedicatedWorker adding 341 ms in bursts; 34 frames in 6.8 s, worst 44 ms. That is the MapLibre GL
JS web map working on the main thread. No URLs or Chrome categories, so no JS function names.

## Next

Reproduce on https://staging.canifishthis.ca: zoom out over Intata Reach, record a Chrome performance trace (CDP), and
name the main-thread work (symbol placement / collision, layout of big lake polygons and labels, style evaluation,
our own JS on camera change). Then fix and measure before/after.

## 2026-11-18 ~13:40: cause found (headless Chrome on staging, 390x844 dpr 2, CPU 4x, easeTo z12 -> z7 over 5 s)

| run | long tasks | worst frame | main-thread JS | feature-state work |
|---|---|---|---|---|
| Intata Reach | 10 / 1,075 ms | 300 ms | 3.46 s | 1.22 s |
| Peace (control) | 3 / 345 ms | 167 ms | 2.54 s | 0.64 s |
| Ocean (control) | 1 / 102 ms | 117 ms | 1.56 s | 0.17 s |
| Intata, stream line-width without feature-state | 0 | 50 ms | 2.59 s | 0.25 s |

At 6x: Intata 18 long tasks / 2,170 ms (worst 700 ms); with the width change 1 / 64 ms.

Cause: our colouring labels each loaded tile's sections (`Colouring.label` -> `setFeatureState`); MapLibre then
re-evaluates every feature-state paint property for every labelled feature, every frame (`Tile.setFeatureState`
1,216 ms: `coalesceChanges` 915, `initializeTileState` 308; `updatePaintArrays` 1,189 ms, mostly expression `max`/`*`/`+`).
The costly one is the stream `line-width`: a ~2.7 KB zoom curve with `max`, `in` and `feature-state`, evaluated twice
per feature, shared by 9 stream layers. Placement, the worker and our own JS are small. Intata is ~2x Peace because it
has more sections (1,929 state calls vs 726) and 3.4x the vertices at z7; lake outlines ~12k vertices per z8 tile.

Fixes, ranked:
1. Stream `line-width` free of feature-state (`app/packages/map/src/paint.ts`): the highlight floor
   `max(v, case(selected|part|water …))` (~462-476) moves to the selected water's own layers (water-copy line + casing,
   selection-casing); the T470 "closed is heavier" factor (`closedShown`, ~440-448) moves to the `stream-closed`
   overlay. Measured: long tasks to zero. Helps the phone map too (same style). Look must stay the same: check the
   highlight and the closed weight on pictures before landing.
2. Overlays a mode doesn't show set `visibility: none` (no bucket); the selection ring from the water-copy source
   (`layers.source.json`, `pnpm style:build`). ~0.25 s left after fix 1.
3. (Not recommended) defer labelling while the camera moves: grey tiles while moving, a spike at moveend.
4. (Not recommended) harder lake simplification at z<=8: reverses T489, not the bottleneck.

Scripts, traces and summaries: the app chat's scratchpad `t848/` (zoom.mjs with PRE/DURING, analyze.mjs, buckets.mjs).
Plan: fix 1 (+2 if cheap) after 0.8.0 ships, with before/after pictures; the web site can take it before the app's 0.8.1.

## Done (branch worktree-agent-a1e41c47a10b4ccef, 2026-11-18, 0.8.1 lane A; not merged)

Fix 1, reshaped: the floor and the closed factor could not simply move to the copy / casing / stripe — the copy is drawn
only below the atlas's own zoom, and a thinner `stream` inside a floor-wide casing shows a band of highlight. So:
- `stream` is drawn at the width zoom and order give (`paint.ts` `streamWidths().base`: no feature-state), and hidden
  where a section is lit (selected / part / water) or closed;
- a new runtime layer directly over it, `stream-heavy`, draws exactly those sections with the stream's own colour and
  opacity at the width the stream had (`drawn`, `paintFor`'s unchanged width) — `splitStream`;
- every mark sized from the stream's width reads the drawn width only where it can show and differs (`markWidth`: the
  casing behind `selected|part|inseason`, the gear edges behind `lit`, closed's stripe the factor always + the floor where
  lit, the issue stripe behind `gi`), the base width in a mode that never shows it. Both platforms (runtime style + adapter).
Fix 2: the stream marks a MODE never shows are `visibility: none` (`markShownIn`: nobaseline, no-access dots, gear
edges and rings, runs dash, closed stripe, issue stripe) on the first frame and in the adapter; by mode only, never by
day, so a day arriving never flips visibility. The selection ring from the copy source was not done.

Look: identical. Proven by evaluation (`app/tools/stream-heavy.test.ts`: width, colour, opacity of the stream as drawn
and every mark's width wherever it shows, all themes and modes, against the one-layer paint; midzoom, imagery-casing and
water-focus read the stream as drawn) and by pictures: staging page vs staging with this tree's paint injected (its own
day), light + dark, z9 + z12, with selected / part / water / closed (lit and not) / fly / bait states written on the
sections: 0 map pixels differ (only the search box's rotating placeholder). Pictures: the lane's scratchpad
`t848/before`, `t848/after`, `t848/diff`.

Measured (staging, Intata Reach z12 -> z7 in 5 s, 390x844 dpr 2, CPU 4x; this tree's paint injected): long tasks
10 / 1,045-1,082 ms -> 1-2 / 133-153 ms; worst frame 250-450 ms -> 133-183 ms (in the zoom 167 -> 67 ms). Salmon map
(T855), same view: zoom-out 16-19 / 2.4-2.6 s -> 6 / ~525 ms, pan 600 px 5-6 / 1.0-1.2 s -> 2-3 / ~285 ms; the switch
Trout -> Salmon is ~380 ms to idle either way.
