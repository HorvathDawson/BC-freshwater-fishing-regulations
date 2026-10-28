# T91 — Atnarko "No Fishing from Tenas Lake to … near Atnarko Park campsite": where the stretch really ends

Status: OPEN. Owner needed (local knowledge).

Source: memory `ambiguous-cut-picks-silently`; owner said the campsite boundary is further downstream than the curated
split. Untouched since (last commits on `atnarko_park_campsite`: 296bfc14 and older). The run records `ambiguous_cut`
for `south_atnarko_river__tenas_lake`: 630.47 used, 2380.4 the other candidate. `atnarko_bella_coola_rivers.r2` binds
22 Atnarko + 1,662 tributary sections.

## Work
Show both split points on a map; the owner confirms where the signs are; fix the curated split; reach rerun; binding diff.

## Acceptance
The owner agrees with the closed stretch on the map; a test pins its ends.

Depends: could be the first item in T66's review tool; touches T59. Size: S.
Owner: yes. Default: keep the current binding until the owner confirms.

## Owner call (prepared 2026-10-28, read only — nothing curated changed)
r2 binds two spans: Atnarko (blk 360879335) from the campsite signs split @ 19630.02 m up to the South Atnarko
confluence @ 58205.71 m (38.6 km), plus the South Atnarko (blk 360878501) from its mouth up to Tenas Lake. Two
separate questions, so two picks:

**1. The campsite end (the owner's concern) — Atnarko River, measures from its mouth at the Talchako:**

| Point | blk @ m | lat, lon (on the FWA line) | Map |
|---|---|---|---|
| Curated split "Atnarko Park campsite boundary signs" (pin -125.91077, 52.40597, 280 m off the river; snapped) | 360879335 @ 19630.0 | 52.40347, -125.91107 | [Google](https://www.google.com/maps?q=52.40347,-125.91107) |
| Atnarko Campground (OSM node 5127542071, BC Parks Tweedsmuir South; 52.40435, -125.90488) — nearest river point | 360879335 @ 19936 (209 m from the river) | — | [Google](https://www.google.com/maps?q=52.40435,-125.90488) |
| Fisheries Pool Campground (OSM node 5650741936, BC Parks; 52.37715, -126.08491) — the next campground downstream | 360879335 @ ~847 (124 m from the river) | — | [Google](https://www.google.com/maps?q=52.37715,-126.08491) |

The curated cut already sits ~306 m DOWNSTREAM of the Atnarko Campground. The only other campground below it is
Fisheries Pool, ~18.8 km further down, ~850 m above the Talchako confluence (Stuie) — the place DFO's own salmon
notice puts its "fishing boundary sign near Tweedsmuir Park Lodge" (dfo-split-hunt `5b:atnarko-river:downstream-of-the-fishing-boundary`).
Moving the end there would close ~19 km more of the Atnarko year-round. No source found on the web for the sign
position (search 2026-10-28). Only the owner can say where the signs stand.

**2. The Tenas Lake end — `ambiguous_cut` on `south_atnarko_river__tenas_lake` (South Atnarko measures from its mouth):**

| Candidate | blk @ m | lat, lon | Map |
|---|---|---|---|
| USED: Tenas Lake outlet (lake's downstream edge) | 360878501 @ 630.47 | 52.16918, -125.71451 | [Google](https://www.google.com/maps?q=52.16918,-125.71451) |
| Other: Tenas Lake inlet (upstream edge) | 360878501 @ 2380.4 | 52.15430, -125.70761 | [Google](https://www.google.com/maps?q=52.15430,-125.70761) |

Recommendation: keep the OUTLET. "From Tenas Lake to …" runs downstream from the lake, and r1 already closes Tenas Lake
itself only Apr 1–June 30 — taking the inlet would put the lake under a year-round closure the book does not state.
Pinning the lake end to `lake_outlet` in the catalogue removes the ambiguity (a T185 proposal, not done here).

**How to pick in the review app (no new UI needed):** open
`http://localhost:5183/#r5:atnarko_bella_coola_rivers_includes_tributaries_except_burnt@5-11+5-6+5-8`, rule
`atnarko_bella_coola_rivers.r2`, Place = n with a proposed extent: `between` — point 1 = `sign` (click where the signs
stand; or `split` = `atnarko_bella_coola_rivers_includes_tributaries_exce__atnarko_park_campsite_boundary_signs` to
keep it), point 2 = `lake_outlet` Tenas Lake. The mark is the only write; T185 lands it, then reach rerun + binding diff.
