# T443 — Stepping ▲/▼ between a big part and a tiny one jumps the camera (disorienting)

Status: DONE — owner phone check 2026-11-08 passed (0.6.9). Was: IN REVIEW — 7f36eb73a, 55c7f54, c30bee60d, c0c9647d7 (0.6.7 items: one framing per part, one label at Landstrom Bar); owner phone check left

Source: owner problem report b48c0e449372c34b, 2026-11-06, app 0.6.2 (staging worker `GET /reports`): "When navigating
a river like the Fraser regulation map and you click up or downstream and it goes from a big section to a tiny section or
reverse it is very disorienting. Fraser is a good example in region 2.. we should have a minimum zoom in maybe and just
make it overall cleaner."

## Work
1. Reproduce on the Fraser (Region 2) water map: step ▲/▼ across its parts, record each fit's zoom (web + emulator).
2. Bound the camera change per step: a zoom floor/ceiling for a part fit (do not zoom in past what keeps the part's
   neighbours in view; do not zoom out to the whole river for one big part), and an animated transition that keeps
   the shared boundary on screen. One rule for every water (no per-water special case).
3. Shots of before/after on the Fraser and Coquitlam steps.
Size: S–M. Owner: look after.

## Measured (2026-11-06, r-67b456b5e64b, 412×600 map, 48 px padding)
Fraser (gnis:39325), 19 map parts mouth up, each part's fit zoom: 8.1 · 14 · 9.6 · 11.3 · 13.4 · 12.5 · 9.1 · 14 · 9.0 · 14
· 12.6 · 12.5 · 8.8 · 9.7 · 9.9 · 9.0 · 7.6 · 9.7 · 5.5 — one ▲ can move the camera 5 zoom levels (×32). Coquitlam:
12.4 · 11.7 · 10.0.

## Done (2026-11-06, owner chose "cap + gentle zoom-in")
`@app/core` `PART_FIT_MAX_ZOOM` = 12, `STEP_ZOOM_IN` = 2, `fitZoomLimit`; the water map's `fit` carries both
(`maxZoom`, `zoomInAtMost`), both maps widen the box to the limit (`fitAtMost`). The zoom-in bound applies only to a
fit that FOLLOWS another on the same map: the props are the same whatever the way in (rule: one load path), and a reload
or a first open frames the part whole. Zooming out to fit a big part is never bounded. Fraser steps from the mouth now
land ≤ 2 levels in each time and never past z12. Left: owner look on the phone (Fraser R2, Coquitlam).

## Owner on 0.6.6 (2026-11-06): the small Fraser part was too small
"so small labels overlap and you can't see it; should allow zooming in a bit more." New floor (`PART_MIN_FILL` = 1):
neither bound may leave a part under half the screen across (one level out from the zoom that frames it), up to z14
(`fitZoomOf` + `fitZoomLimit(fit, current, fitZoom)`, both maps). Fraser from the mouth, step by step, before → after:
tiny parts 10.1 / 11.1 / 11.0 → 13 / 13 / 13; parts of z12–13.4 → 12–12.4; big parts unchanged. The tiny parts still
take a big step in (≈ 5 levels from z8–9): seeing the part won over the smaller step.

## Owner phone check (2026-11-06, 0.6.7)
"Fraser is framed well." Two new items:
1. **The boundary names AT LANDSTROM BAR overlap each other** (corrected 2026-11-06: the owner wrote "landstorm bar",
   meaning the place, not a bar). The release has TWO Fraser marks there with the SAME label "Landstrom Bar sign zone
   boundary", ~50 m apart (r-67b456b5e64b water-5 `part_mark` ord 9871: mark 2 at −121.4650, 49.3730, part 2 → part 6;
   mark 9 at −121.4632, 49.3725, part 6 → part 2). Part 6 "Within Landstrom Bar sign zone" is a short zone inside part 2
   ("2 stretches"), and both ends of the zone are drawn with one label. Fix: two marks that carry the same label and
   sit closer than a label's width at the zoom are drawn as ONE label (the zone's name), or each end names its side
   (lower/upper end). Never two overlapping copies. Check every water for same-label marks closer than ~200 m (count
   them) and add a render/collision test with this pair. Also make sure the step bar's own names never overlap at
   360 dp (wrap/ellipsis, full name on the a11y label).
2. **The first open of a water's Map tab frames the part differently from the same part reached by ▲ then ▼.** The
   first framing must equal the step framing (memory: one load path per screen, T369): the first open must go through
   the same framing function as a step, with the same padding, floor (half the screen, ≤ z14) and insets. Find the
   difference (likely the first fit runs before the bar/sheet insets are measured, or uses the whole water's bounds).
   Test: open → camera A; ▲ then ▼ → camera B; A == B (centre within 1 dp, zoom within 0.01).

## 0.6.7 items done (2026-11-06, c30bee60d, c0c9647d7)
1. **Landstrom Bar.** The zone's two ends carry the same words 138 m apart, both labelled. Fix (`@app/ui` waterMap
   `oneLabelPerPlace`): within 250 m of a labelled mark with the same words a mark keeps its ring and its own
   screen-reader words but draws no label; the open mark's label wins. The release holds 12 such pairs in 9 waters
   (5–197 m; Clayhurst / Mount Griffin / Lasqueti Ecological Reserves, Capilano / Greater Victoria watershed boundaries,
   MU 5-6/5-11, Nanaimo rifle range, a hatchery fence, Landstrom Bar). Test: the Fraser pair on the fixture release
   (`waterMap.test.ts`, mutation-checked). The step bar's own names already clamp to two lines with an ellipsis and the
   full words as the label; a render test pins that (`WaterMapView.test.tsx`), and the shot at 360 dp with the release's
   longest pair (Puntledge part 4, 141 characters) is `app/tools/testbed/out/t443/step-bar-360dp-puntledge.png`.
2. **First open ≠ ▲ then ▼.** Three causes. (a) The step bound (at most 2 levels in from the camera) made a part's frame
   depend on the part before it: 61 of the release's 968 ▲-then-▼ round trips came back up to a level further out.
   Removed: a part's frame is the cap and the floor only (`fitZoomLimit(fit, fitZoom)`). (b) The native map framed the
   first fit before its size was known / before the bar under it grew; it now frames again when its size changes
   (as the web's refit does), unless the reader moved it. (c) On the test bed a first fit sent before the native map's
   first style loaded was lost to its initial camera (the water's map opened on the previous water's place); the fit
   now waits for the first style. Note: a SEARCH result opens the Map on the whole water (T203, by design), so "first
   open" here is the Map tab entered from another view.
**Measured** (test bed `step-shots`, Map tab entered from another view; `[cam]`): before (c30bee60d build, no fit gate)
Coquitlam part 2: A z9.885 vs B z10.900, centres 178.6 dp apart (and a Chilliwack open left on the previous water's
place); after: Fraser part 19 and Coquitlam part 2 — centres 0.00 dp apart, zoom 0.000 (spec: ≤ 1 dp, ≤ 0.01).
Shots: `app/tools/testbed/out/t443/` — `coquitlam-A-first-open.png` / `coquitlam-B-step-there-and-back.png`,
`fraser-landstrom-bar.png` (one label), `step-bar-360dp-puntledge.png`.
Seen, not in scope: at the Fraser's top part (z5.5) the marks round Williams Lake (different words, km apart at that
zoom) still overlap — a label-collision rule across different words would be its own ticket.

