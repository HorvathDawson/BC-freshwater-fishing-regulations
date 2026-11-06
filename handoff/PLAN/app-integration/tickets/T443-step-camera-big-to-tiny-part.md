# T443 — Stepping ▲/▼ between a big part and a tiny one jumps the camera (disorienting)

Status: IN REVIEW (7f36eb73a; owner look on the phone).

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
