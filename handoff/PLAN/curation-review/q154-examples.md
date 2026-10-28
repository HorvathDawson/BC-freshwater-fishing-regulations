# Q154: how small are the "lakes" a river runs through?

Owner, 2026-10-28: "how small are we talking? lakes are still lakes unless it's a pool labelled a lake".
Read-only measurement. Nothing in the data changed and the switch was not flipped (`RIVER_WIDENINGS` is still off).

## 1. How the set was found

I ran `river_widenings` itself (`pipeline/gauges/consume/panels.py`) on the live atlas (`data/generated/atlas/full`,
2026-10-28). It picks every lake polygon that meets all of these:
- no water item owns it
- it is not a "not fishing" polygon
- a stream on the polygon's own watershed code flows into it, and a stream on that code flows out

**136,080 polygons.** It is the same set, polygon for polygon, on the snapshot T174 measured. The set is bigger
than T174's "34,283 unowned lakes get a panel": most of these polygons sit where no gauge's answer reaches, so
switching them on changes nothing there.

- Every one is an FWA lake (`WATERBODY_TYPE L`), except 114 in the man-made layer (`X`, reservoirs and dugouts).
- **None has a name.** Neither the graph node nor the FWA polygon carries a GNIS name. A named lake is a water item,
  and the selection skips owned polygons, so a named lake can never be caught.
- Most sit on small creeks. By Strahler order of the creek through them: order 1–2: 81k, order 3: 37k,
  order 4: 14k, order 5 and up: 3.7k.
- By region: R7 50k, R6 46k, R5 20k, then R3, R1, R4, R2 and R8.

## 2. Sizes and shapes

Measures (from the FWA polygon, BC Albers):
- **area**: hectares.
- **avg width** = 2 × area ÷ perimeter. For a long strip this is its width. For a round pond it gives the radius,
  so a round pond reads about half as wide as it is.
- **length** = perimeter ÷ 2. For a strip this is its length along the flow.
- **elongation** = long side ÷ short side of the smallest rectangle around the polygon.
- **river width (modelled)**: FWA streams are single lines with no width, so this is estimated. The upstream
  catchment comes from the repo's own magnitude-to-area model (`AreaModel`, the one the gauges use). A Pacific
  Northwest bankfull-width curve then turns that into a width: W ≈ 2.5 × A^0.38 m, with A in km². **It is rough,
  within a factor of 2 either way**, and it does not know about dams or braids.
- **width ratio** = avg width ÷ modelled river width. Near 1 means the polygon is barely wider than the river,
  which is pool-like. Much more than 1 means a real basin.

| area band | polygons | median avg width | median length | median elongation | median width ratio | median order |
|---|---|---|---|---|---|---|
| < 0.1 ha | 26,595 | 12 m | 52 m | 1.7 | 3.1 | 2 |
| 0.1–0.5 ha | 57,184 | 21 m | 104 m | 1.8 | 4.9 | 2 |
| 0.5–1 ha | 17,937 | 35 m | 200 m | 2.0 | 7.7 | 2 |
| 1–5 ha | 23,201 | 54 m | 362 m | 2.0 | 12.1 | 2 |
| 5–20 ha | 8,256 | 111 m | 801 m | 2.0 | 20.7 | 3 |
| > 20 ha | 2,907 | 204 m | 1.8 km | 2.2 | 31.2 | 3 |

So 75% are under 1 ha. The typical one is a pond 20 m or so across on a creek a few metres wide: about 5 times
the creek's width and round rather than stretched along the flow. From the imagery, those are mostly beaver ponds,
muskeg ponds and potholes: small, but ponds, not pools in a river. A tail of 2,907 polygons over 20 ha (up to
190 ha) are plainly lakes that have no name.

## 3. Examples

To make the calls below I drew each FWA outline (red) and the FWA stream lines (blue) on ESRI World Imagery, the
app's satellite layer, using the tile code from `handoff/PLAN/sloughs/v2/automark/imagery.py`, and looked at the
images. The pixel-colour "water %" from that script did not hold up (it calls a glacial-green lake "vegetation"),
so I did not use it. "Caught" means caught by the cutoff proposed in section 4. Regions come from the polygon's
management units.

| # | water | reg | ha | avg width / length | elong. | order, modelled river width | ratio | caught? | my call (from the imagery) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | unnamed creek ([map](https://www.google.com/maps/@49.55842,-123.65438,18z/data=!3m1!1e3)) | 2 | 0.07 | 10 / 75 m | 3.9 | 4, 8 m | 1.2 | yes | no open water visible under the forest; a mapped widening, not a lake |
| 2 | unnamed creek ([map](https://www.google.com/maps/@53.32156,-121.74536,18z/data=!3m1!1e3)) | 7 | 0.05 | 11 / 50 m | 1.9 | 1, 3 m | 3.8 | no | dark beaver-type pond in a gully; a small pond, kept as a lake |
| 3 | Brecon Creek ([map](https://www.google.com/maps/@59.39717,-132.85615,18z/data=!3m1!1e3)) | 6 | 0.04 | 11 / 39 m | 1.1 | 3, 8 m | 1.4 | yes | a dark pool inside the creek channel; **pool** |
| 4 | unnamed creek ([map](https://www.google.com/maps/@52.57904,-125.23263,18z/data=!3m1!1e3)) | 5 | 0.05 | 9 / 52 m | 3.2 | 3, 7 m | 1.2 | yes | wider dark stretch of a meandering meadow creek; **pool / widening** |
| 5 | Bridge River ([map](https://www.google.com/maps/@50.78750,-122.21911,17z/data=!3m1!1e3)) | 3 | 0.49 | 36 / 133 m | 1.2 | 8, 58 m | 0.6 | yes | the plunge pool right below the dam (by its location, Terzaghi) at the head of the lower river; **pool labelled a lake** |
| 6 | Thudaka Creek ([map](https://www.google.com/maps/@57.62756,-126.59309,18z/data=!3m1!1e3)) | 7 | 0.18 | 21 / 83 m | 1.7 | 7, 32 m | 0.7 | yes | eddy pool on the outside of a bend; **pool** |
| 7 | unnamed alpine creek ([map](https://www.google.com/maps/@54.06161,-127.61544,17z/data=!3m1!1e3)) | 6 | 0.21 | 14 / 150 m | 5.4 | 4, 10 m | 1.4 | yes | narrow strip of water along the creek in a snowy valley; **widening** |
| 8 | unnamed creek ([map](https://www.google.com/maps/@52.36447,-120.97955,17z/data=!3m1!1e3)) | 5 | 0.36 | 28 / 128 m | 1.7 | 3, 6 m | 4.8 | no | drained beaver pond, now a wet meadow; pond, not river |
| 9 | Nancut Creek ([map](https://www.google.com/maps/@54.57918,-125.16175,16z/data=!3m1!1e3)) | 7 | 0.51 | 22 / 228 m | 4.1 | 5, 17 m | 1.3 | yes | the creek's own channel drawn as a polygon; **river** |
| 10 | Gote Creek ([map](https://www.google.com/maps/@59.55891,-122.02056,15z/data=!3m1!1e3)) | 7 | 0.92 | 13 / 687 m | 3.7 | 3, 10 m | 1.3 | yes | a winding strip that IS the creek channel; **river** |
| 11 | unnamed creek ([map](https://www.google.com/maps/@59.48619,-131.99416,17z/data=!3m1!1e3)) | 6 | 0.67 | 43 / 157 m | 1.5 | 4, 10 m | 4.3 | no | round pond where three creeks meet; a small lake |
| 12 | unnamed creek ([map](https://www.google.com/maps/@59.94155,-122.82667,16z/data=!3m1!1e3)) | 7 | 0.70 | 24 / 288 m | 4.0 | 3, 7 m | 3.6 | no | a dry cut-off meander (oxbow) the line clips at one end; not a widening, not much of a lake |
| 13 | Hutchison Creek ([map](https://www.google.com/maps/@59.76864,-126.64944,16z/data=!3m1!1e3)) | 7 | 1.03 | 33 / 312 m | 4.1 | 6, 20 m | 1.6 | no (just over 1 ha) | a strip along the creek; widening. **A miss by the size cap** |
| 14 | unnamed creek ([map](https://www.google.com/maps/@58.38404,-121.74233,16z/data=!3m1!1e3)) | 7 | 1.88 | 65 / 287 m | 1.6 | 2, 4 m | 15.8 | no | dark round open water; **real lake** |
| 15 | Nazko River ([map](https://www.google.com/maps/@52.55316,-123.48105,14z/data=!3m1!1e3)) | 5 | 5.79 | 52 / 1.1 km | 3.7 | 6, 26 m | 2.0 | no | a slow, wide reach of the river; arguably river. **A miss by the size cap** |
| 16 | unnamed, near Hutchison Creek ([map](https://www.google.com/maps/@59.97215,-126.66152,14z/data=!3m1!1e3)) | 7 | 11.8 | 44 / 2.7 km | 1.8 | 6, 23 m | 1.9 | no | **real lake** with a long channel tail, and the tail drags the average width down. Without a size cap the ratio would catch it |
| 17 | Kechika River ([map](https://www.google.com/maps/@58.91258,-127.31063,14z/data=!3m1!1e3)) | 7 | 7.19 | 79 / 914 m | 1.8 | 8, 74 m | 1.1 | no | **oxbow lake** off the river; ratio ≈ 1 because the big river's modelled width is large. Why the cap |
| 18 | Columbia River ([map](https://www.google.com/maps/@50.99726,-116.47403,15z/data=!3m1!1e3)) | 4 | 7.26 | 112 / 647 m | 2.3 | 8, 57 m | 2.0 | no | backwater pond in the Columbia wetlands, off the main channel; a lake, not the river |
| 19 | unnamed creek ([map](https://www.google.com/maps/@52.14609,-122.41510,14z/data=!3m1!1e3)) | 5 | 21.0 | 79 / 2.7 km | 2.5 | 2, 4 m | 19.3 | no | **real lake** |
| 20 | Tuya River ([map](https://www.google.com/maps/@59.00520,-130.63862,12z/data=!3m1!1e3)) | 6 | 189.6 | 206 / 9.2 km | 2.4 | 7, 30 m | 6.9 | no | **real lake**, 190 ha with no name |

I also drew 8 random members of the caught set. None was a real lake: 3 tiny pools inside creek channels, 2 strips
along a channel, 2 with no water visible under the trees, and 1 small dark beaver pond on a creek (the closest call).

## 4. Proposed cutoff

**Treat a polygon as river (a widening) only when it is under 1 ha AND its average width is at most 2 × the
river's modelled width.** It must also already be in the `river_widenings` set (unnamed, unowned, on the river's own
code, flowed into and out of), and it must not be a man-made polygon.

- **Catches 10,650 of the 136,080 polygons (7.8%).** By order: 49 on order 1, 767 on order 2, 3,700 on order 3,
  4,151 on order 4, 1,672 on order 5, 311 on order 6 and up. Small creeks have 1–3 m channels and FWA rarely maps a
  polygon that narrow, so on them nearly every pond stays a lake, which is what you asked for.
- **Sections that change: 11,423**, measured. I rebuilt the panels from the T174 snapshot with the switch on for
  only the caught set and compared them with the shipped panels. That is +10,887 sections answering Flow and +6,242
  answering Level. 2,227 of the caught polygons get a panel. For comparison, the switch fully on gives 96,596
  changed sections, the same as T174's number. One section loses its Flow answer and one loses its Level answer
  (fully on loses 4 and 14). I did not chase those.
- **Why the 1 ha cap.** Width alone catches real lakes on big rivers: an oxbow lake on the Kechika (#17), a Columbia
  backwater pond (#18), and a lake with a long channel tail (#16). In each, the big river's modelled width or a thin
  tail makes the ratio look river-like. With width alone (≤ 2×, no cap) the catch is 10,785, and all 135 extra
  polygons are over 1 ha.
- **What it misses.** Real widenings over 1 ha, like Hutchison Creek (#13) and the Nazko reach (#15). Raising the
  cap to 5 ha would add 130 polygons, a mix of widenings like these and big-river oxbows and backwaters. I would
  leave it at 1 ha.
- **Named lakes are never caught**, by construction (see section 1). Neither is anything over 1 ha.
- **Caveat.** The river width is modelled, not measured, and it does not know about regulated flow. The Bridge River
  pool (#5) has ratio 0.6 because the model assumes a natural river of that catchment. A round pond's average width
  reads at half its diameter, which pushes round ponds toward "caught". The 8-sample check suggests this is
  acceptable, but it is a sample, not a census.

Scripts and images (scratch, not committed):
`/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/scratchpad/q154/`
(`q154_select.py`, `shapes.py`, `dist.py`, `measure_cut.py`, `render.py`, `img/`).
