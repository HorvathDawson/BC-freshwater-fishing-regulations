# T664-pending — curated edits for the next side build (proposals, NOT applied)

Made 2026-11-11 (Claude). Nothing here is in `data/curated` yet. Each patch applies with `git apply <file>` on the
tree as of this commit (`git apply --check` passed). The order to apply them is in `../next-rebuild.md`.

| file | what | applied when |
|---|---|---|
| `nine-cuts.cuts.json.patch` | the 9 new cuts for the 9 DFO marks on `NOT_LANDED_OK` (`cuts.json`, +202 lines) | before the side atlas build |
| `nine-cuts.dfo-places.patch` | the 9 marks' extents on their DFO places (`dfo_salmon/places/region-2/3/6.json`) | after promote, by `apply_proposals` (this patch is what it will write) |
| `t664-references.patch` | `r8:okanagan_river_oxbows@8-1` `matched` 53 → 9 ids; in-season notice `is-bb5426c61ff3` 53 → 9 extents (+ a history row) | after the side atlas build, before the reach run |

Both cut patches come from the review tool itself (`python -m pipeline.regs.review.proposals side <the 9 mark proposals>`),
built from the owner's saved marks in memory. No mark was edited or added.

## T664 — what the merge does (measured in memory on the live atlas `full`, no write)

- Registry: 21,331 → 21,287 items. The 45 STREAM items named "Okanagan River Oxbows" become one:
  `wsc:300-432687-459615` (80 sections). Absorbed (44, now its `aliases`): the other 30 `wsc:300-432687-…` line items
  and 14 polygon items (`wbk:328947490, 328985785, 329460316, 329460457, 329461320, 329461445, 329461800, 329461866,
  329461918, 329462064, 329462179, 329462406, 329462410, 329462452`).
- Kept apart (default, owner call below): 8 oxbow polygons that are still LAKE or WETLAND items — `wbk:328944522,
  328948249, 328957729, 329459157, 329459219, 329459234` (wetland), `328985789, 329460279` (lake).
- Oxbow rule `okanagan_river_oxbows.r1`: live 89 sections = survivor's 80 + the 8 kept-apart polygons + `lake:329461947`
  (bound by the reach's river-widening step, not by membership: the 459615 line threads it). Merged set minus live: none.
  Expected after the build: 89 → 89, identical.
- No boundary dropped (no lake edge onto a polygon the item now holds); the 5 hillside cuts stay on the survivor.
- Curated references to absorbed ids: only `region-8.json` (the entry's `matched`) and `inseason/notice_waters.json`
  (`is-bb5426c61ff3`). None in DFO places, gauges, cuts or `book_name_links.json` (the owner's hand match names raw
  watershed codes and waterbody keys; they still resolve, since the survivor's `ref_ids` hold every member's).

## The nine cuts (the owner's saved marks are the authority)

blk and measure: the mark's point projected onto the FWA line of the water (`bc_fisheries_data.gpkg`), offset applied.
"Section now" is the live section the cut falls in.

| cut id | water | blk | cut at (m) | anchor | section now | conf. | owner mark(s) (Dawson) |
|---|---|---|---|---|---|---|---|
| nicomekl_river__23_m_below_the_nicomekl_flood_dam_d23m | Nicomekl River gnis:21100 | 356353722 | 4778.3 | dam point, 23 m downstream (dam at 4801.3 = the `dyke_gates` cut) | 356353722:46 | high | 2:nicomekl-river:within-23-m-downstream-of-the-floo (11-09) |
| serpentine_river__23_m_below_the_serpentine_flood_dam_d23m | Serpentine River gnis:24978 | 356351273 | 3471.0 | dam point, 23 m downstream (dam at 3494.0 = `dyke_gates`) | 356351273:0 | high | 2:serpentine-river:within-23-m-downstream-of-the-floo (11-10) |
| nakina_river__sloko_river_u300m | Nakina River gnis:16492 | 360874795 | 15041.8 | Sloko confluence (14741.8) + 300 m up | 360874795:0 | high | 6:nakina-river:that-portion-of-the-nakina-river-f (11-09) |
| nakina_river__sloko_river_u1300m | Nakina River gnis:16492 | 360874795 | 16041.8 | Sloko confluence + 1,300 m up | 360874795:0 | high | same mark |
| thompson_river__signs_just_below_goldpan_provincial_park | Thompson River gnis:39492 | 356364690 | 26051.6 | point | 356364690:23466 | medium | 3:thompson-river:from-kamloops-lake-outlet-downstre~2; 3:thompson-river:from-the-white-triangle-fishing-bo (11-09) |
| thompson_river__easterly_border_of_the_skihist_ecological_reserve_along_the | Thompson River gnis:39492 | 356364690 | 7143.1 | point | 356364690:0 | medium | 3:thompson-river:from-the-white-triangle-fishing-bo (11-09) |
| harrison_river__hwy_7_bridge_over_the_harrison | Harrison River gnis:15333 | 356362488 | 5954.6 | point | 356362488:2297 | medium | 2:harrison-river:from-the-highway-7-bridge-downstre; 2:harrison-river:from-the-outlet-of-harrison-lake-d (11-09) |
| nicomen_slough__hwy_7_nicomen_slough_bridge_at_dewdney | Nicomen Slough gnis:21105 | 355991886 | 3368.9 | point | 355991886:0 | medium | 2:nicomen-including-dewdney-slough:from-the-highway-7-bridge-at-dewdn (11-10) |
| nicomen_slough__siddle_creek | Nicomen Slough gnis:21105 | 355991886 | 20557.3 | point | 355991886:18999 | medium | 2:nicomen-including-dewdney-slough:from-the-confluence-of-siddle-bell (11-10) |

The extents the marks set (the dfo-places patch): Nicomekl / Serpentine `between dyke_gates and d23m`; Nakina `between
u300m and u1300m`; Thompson `between kamloops_lake and goldpan` and `between goldpan and skihist`; Harrison `between
harrison_lake and the bridge` and `downstream_of the bridge`; Nicomen `downstream_of` each of its two cuts.

**Where a mark is ambiguous (read before applying; none changes a cut):**
- Nakina: DFO says "from the centre line to the northern bank". A cut cannot take half the width, so the binding is the
  whole channel between the two signs. The mark says between the cuts; the half-width is lost.
- Nicomekl and Serpentine: the 23 m pieces are short but above the 5 m sliver limit. The places are `dormant`.
- Nicomen "(including Dewdney) Slough": both marks bind `gnis:21105` only. Whether Dewdney Slough is a separate water
  that should also be named was not checked.
- Thompson Skihist: DFO gives 50°15'23.0"N 121°30'55.0"W; the mark's point is about 38 m north of that. The cut
  snaps onto the line at 7,143 m either way.
- The two Thompson marks share one cut at the Goldpan signs, and the two Harrison marks share one at the bridge. That
  is how the DFO wording reads.

## Owner calls (defaults taken, listed)
1. The 8 still-water oxbow polygons stay separate items, so search shows 1 + 8 hits, not 1. Folding them in
   (`one_water.ONE_WATER_TAKES_STILL_WATER = True`) makes them streams. That moves zone rules: lake rules come off and
   stream rules go on, including the spring stream closure the oxbow row exempts.
2. The survivor id is `wsc:300-432687-459615`: the first line item by id.
