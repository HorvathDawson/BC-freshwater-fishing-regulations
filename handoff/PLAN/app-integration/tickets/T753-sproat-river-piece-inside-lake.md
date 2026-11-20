# T753 — A piece of the Sproat River drawn inside Sproat Lake

Status: DONE — a458c1fd4, 3bbb20082. Same-lake loops leave their river and join their lake (Kotcho stays the river's); promoted 2026-11-15 and in every release since (r-46e4355a7277 on).

Closed by the 2026-11-20 audit: both steps the owner picked ("yes do both steps") landed on redesign/stream-sections (a458c1fd4, 3bbb20082). STATUS 2026-11-15 (chat 13 end) says lake loops landed, with atlas/tiles/reach promoted. curation-review/rebuild-finish-2026-11-15.md verifies 7 loops moved and Kotcho kept. Shipped with r-46e4355a7277 and later releases.

Owner, 2026-11-15, on the review app's DFO Region 1 place "Sproat River from Sproat Lake to fishing boundary signs
300 m downstream of Hwy 4": "there is a tiny section of sprout river in the middle of the lake... if anything it
should be part of the lake....".

## What the piece is (verified by query)

- Two short lines on FWA blue line **354087683**. The blue line has **no GNIS name**. It carries FWA watershed code
  `930-139262-982023`, which is the Sproat River's own code. It is a secondary-flow line (edge type 1100) that
  leaves Sproat Lake, runs through a 4.2 ha unnamed pond (wbk **329016376**) and goes back into Sproat Lake:

  | measure | edge | in | length |
  |---|---|---|---|
  | 0–898 | 1450 | Sproat Lake (329016196) | 898 m |
  | **898–999** | 1100 | no polygon: lake → pond | **101 m**, section `354087683:898`, release sid **356** |
  | 999–1295 | 1300 | the pond 329016376 | 296 m (a lake section, in no item) |
  | **1295–1361** | 1100 | no polygon: pond → lake | **66 m**, section `354087683:1295`, release sid **357** |
  | 1361–2465 | 1450 | Sproat Lake | 1104 m |

- Where: about -125.017, 49.259 and -125.022, 49.261. The pond is 59 m from the lake, so these are two short channels
  across a spit. The nearest piece of the river's own bed outside the lake is the outlet, **8.5 km** east. The river's
  main line through the lake (blk 354153364, edge 1200) passes 1.1 km away.
- Registry (`data/generated/atlas/full/registry.json`): `gnis:25640` Sproat River has `section_ids`
  `354153364:0, 354087683:898, 354087683:1295, 354153364:1924`. Its `ref_ids` include `blk:354087683` and
  `wsc:930-139262-982023`, and its boundaries include `sproat_river__lake_329016376`, which is the pond.
- **Why it joined the river:** it is not the through-river name. The memory note "lake must not answer to its
  river" is about lake *polygons*. This is a nameless line that shares the river's watershed code.
  `registry/build.py` `_effective_gnis` gives such a line the river's gnis, either through an inherited
  side-channel name or through `_wsc_gnis_map`. That rule exists so that a braid stays with its river. Here the
  "braid" is a loop off the lake. I inferred which of the two paths applies; both lead to `gnis:25640`.
- **Cut:** there is no curated cut on the piece. `cuts.json` has only the Hwy 4 signs split for this river. The dot
  the owner saw is almost certainly drawn by the review app (inferred from the code, not run).
  `curation-review/backend/dfo.py` `_splits_fc` draws every boundary of the water. For the pond boundary
  `sproat_river__lake_329016376`, `_split_feature` puts the dot at the pond's representative point, which is in the
  narrows. For `sproat_river__sproat_lake`, `_lake_ends` adds an "end" at every river section end that touches the
  lake, and the side-channel ends at measures 898 and 1361 count. The line is dashed because the tiles draw a line
  with no main flow as a side channel (`deliver/tiles/export.py` ~L686).

## What the app and the review app show today

- Release `r-f749475d0962` (verified). In the `water` db, item_ord 6026 has **part 0** "From the Stamp River
  confluence up to the fishing boundary signs 300 m downstream of Hwy 4". Its `part_line` rows include blk
  354087683 at 898–999 and 1295–1360, and `part_section` puts sids 356 and 357 in part 0. Tapping the narrows shows
  the **lower Sproat River**, 8 km downstream. In the tile `stream` layer they are features 356 and 357,
  `name: "Sproat River"`, drawn at z13 only.
- Rules (verified, `reaches/full/rule_section.jsonl.gz`):
  - `sproat_river.r2` and `r2x` (the bait ban, `op: whole`) bind all 4 sections, including the two pieces.
  - The closure `sproat_river.r1` (between Sproat Lake and the signs) binds only `354153364:1924` and correctly
    misses them.
  - The zone `salmon_stamp.r1` covers them too.
  - Sproat Lake's own rows do **not** apply there: `r1:sproat_lake@1-7` (cutthroat catch and release, no wild
    rainbow over 50 cm, single barbless hook). An angler in the narrows gets river rules instead of lake rules.
- Review app: the DFO proposal is `between(sproat_river__sproat_lake, …hwy_4)`, which binds the main line only. The
  map still draws the pond's cut dot and the dashed piece, because the piece belongs to the water.

## How common (verified by query; the "far" test is a heuristic)

Method (no rebuild): I took every `gnis:` stream item in the atlas registry and each run of its sections on a blue
line with no GNIS anywhere. I kept the runs that have a lake polygon directly below and directly above them, and
whose blue line starts and ends inside lake polygons (FWA `streams` / `lakes`, read-only). Then I measured the
distance to the item's named line outside lakes.

- Lake-to-lake nameless lines on gazetted rivers: **114 runs on 62 items** (38.0 km). Most of them are floodplain
  braids through tiny ponds right beside the river, which is legitimate.
- **200 m or more from the river's own bed: 59 runs on 30 items, 27.6 km.** All 59 sections are in the release
  `item_section`. 15 of these runs (11 items) leave a lake and come back into the same one, which is the Sproat
  pattern: Sproat, Nanika, Kotcho, Stalk Lakes, Tatlatui (Firesteel), Isintok, Moat (Cruickshank) and others.
  6 of the 30 items have their own regulation rows (Sproat, Little Rancheria, Fiftynine, Halfway, Cruickshank,
  Kloiya).
- Top 20 by length (metres, item, blk m0–m1, release sid, lon/lat, distance from the river):

  | m | water | blk m0–m1 | sid | at | from river |
  |---|---|---|---|---|---|
  | 4792 | Taylor Creek | 355996166 336–5128 | 61337 | -121.125, 51.481 | 1113 m |
  | 2697 | Hackett River | 360242625 28–2725 | 773456 | -131.588, 58.100 | 817 m |
  | 2291 | Duteau Creek | 355994475 416–2707 | 60613 | -119.074, 50.098 | 583 m |
  | 1580 | Halfway River | 359004597 696–2276 | 450833 | -122.382, 56.556 | 270 m |
  | 1412 | Five Mile Creek | 355993608 1768–3180 | 60273 | -121.868, 52.207 | 1199 m |
  | 1221 | Five Mile Creek | 355993608 86–1307 | 60272 | -121.885, 52.213 | 2787 m |
  | 1109 | Cranberry Creek | 356366621 602–1711 | 317475 | -118.064, 50.811 | 1319 m |
  | 823 | Klicho Creek | 359001826 1269–2092 | 449620 | -121.224, 58.831 | 536 m |
  | 809 | Eightythree Creek | 355995204 823–1632 | 60918 | -121.080, 51.428 | 615 m |
  | 735 | Rapid River | 359012426 41–776 | 454140 | -128.986, 59.283 | 219 m |
  | 662 | Klicho Creek | 359001826 2439–3101 | 449623 | -121.223, 58.839 | 484 m |
  | 622 | Teslin River | 360213343 85–707 | 762197 | -131.993, 59.395 | 431 m |
  | 602 | Metlahdoa Creek | 359009904 1095–1697 | 453108 | -121.392, 58.794 | 220 m |
  | 570 | Klicho Creek | 359001826 581–1151 | 449618 | -121.213, 58.828 | 381 m |
  | 543 | Halfway River | 359004597 66–609 | 450832 | -122.375, 56.549 | 756 m |
  | 525 | Klicho Creek | 359001826 4053–4578 | 449627 | -121.234, 58.850 | 229 m |
  | 518 | Klicho Creek | 359001826 3143–3661 | 449624 | -121.226, 58.843 | 563 m |
  | 480 | Taylor Creek | 355996166 5160–5640 | 61338 | -121.142, 51.500 | 601 m |
  | 460 | Klinkit Creek | 360222098 535–995 | 765059 | -131.146, 59.553 | 416 m |
  | 408 | Gladys River | 360213266 984–1392 | 762176 | -132.874, 59.497 | 274 m |

  Sproat's two pieces (101 m and 66 m, 8.5 km from the river) rank about 39th by length. The other large-lake loops
  are the Nanika (86 m, Nanika Lake, 9.9 km from the river) and the Kotcho (53 m, Kotcho Lake, 9.0 km).
- Not checked: whether some of the 200 m+ cases are real channels of the river, for example on a river that runs
  through a chain of lakes. Look at the 30 on the map before sweeping them.

## Options

**(a) Give such pieces to the lake.** The registry would group a nameless line whose blue line leaves and re-enters
the same lake into that lake's item.
- What it changes: Sproat sids 356 and 357 move from `gnis:25640` to `wbk:329016196` and take the lake's rules.
  This is the owner's reading.
- Rebuild: atlas (the registry is an atlas stage), side build plus `build_parity`, then promote, then reach, tiles and
  bundle together. The tile features carry the item name and ord.
- Risk: lake items are one polygon section today, so every reader that assumes this has to accept line sections
  (the lake picker, km, "whole lake", painting). A loop into an **unnamed** lake has no item to join, so it falls back
  to (b). It only covers the 15 same-lake loops; a line between two different lakes needs a choice of lake.

**(b) Drop them from the river item and leave them nameless.** This is the same pattern as
`DITCHES_LEAVE_RIVER_ITEMS`: a policy flag plus a predicate in `registry/build.py`, applied after `drop_ditches`, with
a mutation pin. The predicate: no gnis of its own, names inherited only, both ends are lake boundaries, and the line
never meets the item's named line outside a lake.
- What it changes: the 59 pieces (30 items) carry zone rules only, as every nameless line does. The river's tap card
  stops reaching into the lake.
- Rebuild: the same as (a).
- Risk: low. It reuses a proven shape, and the readers already handle nameless pieces. It does not give the lake's
  rules (single barbless at Sproat): the narrows answer with the region default.

**(c) Leave the data; hide it in the review app's proposal only.** The fix would go in `dfo.py` `_splits_fc` /
`_lake_ends`.
- What it changes: only the review map.
- Rebuild: none.
- Risk: the app still shows the lower-river rules in the narrows, which is the real defect, and the review app
  would differ from what ships (AGENTS 16).

**Recommend (b) now, then (a) for the same-lake loops into a named lake as a follow-on.**
- (b) removes the wrong answer everywhere with one flag. (a) adds the lake's rules where the owner wants them, but
  needs readers to accept a lake item that has lines.
- Before promoting, review the 30-item list on the map.

## Notes for whoever implements

- No section ids change (no new cuts) and no item id changes (each of the 30 keeps its named line), so no entry
  resync is expected. If any affected item turns out to consist only of such pieces, its id disappears. Then follow
  "registry change → entry resync" (rebuild, `prune_remapped`, `backfill_matched`, and hand `parse-missing` to the
  owner).
- `data/curated/gauges/matches.json` (frozen) contains none of the 59 pieces' blks. Gauge 08HB008 sits on the
  `sproat_river__sproat_lake` boundary, which is unaffected. After the rebuild, check that no gauged section moved.
- `item_boundaries` takes the Sproat Lake boundary's `place` as the lowest (blk, measure) **as strings**, so today
  it is `354087683:898`, the side line. That only orders clash suffixes, but it goes away with either fix.
- Scratch evidence (not committed): `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/51a6025f-10ca-4232-b730-e92b1263ce2f/scratchpad/D2.json`
  holds all 114 runs with distances, and `step2.py` to `step5.py` are the queries.

## Owner answer 2026-11-15
"yes do both steps but make sure it is done in a clean way" (both = option (b) take the pieces out of the river, then
(a) give same-lake loops to the lake). Build lane started; the atlas rebuild that carries it is the one held for the
owner's new cuts.

## Built (2026-11-15, branch `data/t753-lake-loops`)

One rule, `pipeline/atlas/registry/lake_loops.py` (`LAKE_LOOPS_LEAVE_RIVER_ITEMS` = option b,
`LAKE_LOOPS_JOIN_THEIR_LAKE` = option a, both True). It runs in `atlas/build.py` `water_registry` right after the
widening-pond fold, and `atlas/sidecars.py` runs the same pass. It is not run beside `drop_ditches`. The reason is that
the lake's holder is only final after the flowing and widening folds: a loop off a river's own polygon or widening is
that river's braid, and a pass beside the ditch rule could not see that. The build writes `lake_loop_report.json`.
`POLICY_VERSION` is now 40 (it marks the binding diff), and AGENTS rule 12 and `pipeline/docs/MAP.md` have a line each.

**Criteria (per blue line, read off the graph's flow edges; no names, no length or distance limit):**
1. No piece on the blue line has a gnis or a name of its own. Every name tuple is `side_channel` (the ditch rule's test).
2. Every piece flows into exactly one lake node and out of exactly one lake node. Consecutive pieces meet in the same
   lake, and the line starts and ends in the same lake X. Edges, not bounds: FWA starts the Sproat line inside the
   lake, so the piece's lower bound is `None`. Reading the bounds moved nothing on real data.
3. Where the pieces go depends on what holds X:
   - a lake or wetland item: they join it;
   - no item: they join no item;
   - a stream item: they stay, because it is a braid of that river.

   A pond the loop passes through that belongs to another water also means the pieces stay.
4. An item made only of such pieces keeps them.

**The pond stays its own unnamed water.** Only the line pieces move. Giving the pond to the lake would change the rules
on the pond itself; that is left for the owner.

**Dry run on the promoted atlas** (graph.pkl + registry.json, read only, via heavy.sh): 8 lines (11 pieces, 755 m on
8 waters) move, and all 8 join a named lake. The list is in `T753-files/lake_loop_dry_run.json` (moved, near misses with
reasons, bindings). It is a proposal for the owner, not applied data.

| water left | lake joined | pieces | m |
|---|---|---|---|
| Sproat River | Sproat Lake | 354087683:898, :1295 | 166 |
| Cruickshank River | Moat Lake | 354087804:134, :214 | 70 |
| Firesteel River | Tatlatui Lake | 359018360:34, :179 | 214 |
| Nanika River | Nanika Lake | 360240946:664 | 85 |
| Kotcho River | Kotcho Lake | 359013342:6118 | 52 |
| Tsintsunko Creek | Tsintsunko Lake | 355997590:1138 | 97 |
| Isintok Creek | Isintok Lake | 356369274:212 | 58 |
| Eneas Creek | Garnet Lake | 356369362:73 | 14 |

**Against the ticket:** the ticket counted 15 runs on 11 items (29 runs on 21 items without its 200 m filter). The rule
moves 8 lines, all inside the 29. None of the 29 is unexplained. The rest are listed as not moved:
- **Lake held by the river's own polygon or widening (a braid): 8.** Burnstead, Fiftynine, Gladys, Four Mile,
  Northwest, Chasm, Schooler, Antigonish.
- **A stream joins the loop or it splits to a stream: 4.** Stalk, Iverson and Jennings have a nameless creek flowing
  in. Otter's loop also flows out to a stream.
- **A curated name on the piece: 1.** Little Rancheria's piece carries "Rancheria River" (source: regulation).

Near misses in all: 462 (365 leave the lakes, 38 named, 32 between two different lakes, 19 joined by a stream, 8 braids).

**What binds differently** (live reach run, dry): these rules leave the pieces:
- the river's rows: `sproat_river.r2`/`r2x`, `cruickshank_river.r1`, `eneas_creek.r1`, Williston r4 on Firesteel,
  Bulkley r1/r3 and Skeena r1 on the Nanika piece;
- the zone stream rules (stream closures, bait bans, stream quotas, steelhead).

These arrive: the lake's rows (`sproat_lake.r1-r3`, `tatlatui_lake.r1`) and the zone lake rules (set lining, lake
quotas, `bait.r5`). Section ids and item ids do not change: no cut is made, and every river keeps sections.

The river loses only:
- the pond-edge boundaries `sproat_river__lake_329016376`, `cruickshank_river__lake_329097416` and
  `firesteel_river__lake_329139181`. None is named in the curated tree, the DFO suggestions, the review app or the
  marks;
- the `blk:` ref of each moved line.

Gauge `matches.json` names none of these blue lines. The marks name none of them.

**The rebuild must verify:**
1. `lake_loop_report.json` equals this list.
2. The binding diff matches the table above.
3. DFO places: `1:sproat-river:whole` drops 4 to 2 sections; `6:morice-lake:including-tributaries` and the region
   defaults may drop the Nanika and other pieces.
4. Promote's moved-boundary check is clean, and no gauged section moved.
5. Readers handle a lake item that holds line sections (an audit read the code, nothing was run):
   - `bundle/spans.py` writes no `section_span` row for them (stream items only), so the parts treat them as polygon
     parts (no km);
   - the tiles draw them in the `stream` layer under the lake's name;
   - `area_ha` is unaffected;
   - the review app's `_lake_ends` reads only items with lake boundaries, so it is not hit.

Tests: `pipeline/tests/test_lake_loops.py` (18, toy graphs, mutation pins for both flags, idempotence) and the policy pin
in `test_reach_builder.py`.

**Open for the owner:**
- Should the pond a loop passes through (Sproat's 4.2 ha pond, Moat's, Tatlatui's) also go to the lake?
- Should a loop that a nameless creek flows into (Stalk, Iverson, Jennings) move too? The rule leaves it because the
  creek's receiving water would change.

## Kotcho Lake follow-up (2026-11-15, 3bbb200)

The first promoted build gave Kotcho Lake its 51.8 m loop (`359013342:6118`). The lake's polygon is in the Kotcho Lake
Ecological Reserve and the loop is not, so the lake fell into two parts with different rules and the answers step
refused it (`promote-2026-11-15.md` §6). Fixed in the general rule, criterion 5 (`LOOPS_NEVER_DIVIDE_A_LAKE`): a loop
joins its lake only when every piece is in exactly the lake's CUT areas (`areas.json` `cut` clean / first_last, the
lake's `outside_items` taken out). Otherwise it stays with its river, listed as `AREAS_DIFFER` with both area sets. A
lake is never divided into areas, so this follows the repo's lake-split deferral. Membership-only areas (`cut: false`,
provincial parks and the like) do not hold a loop. They are listed on the moved row: Sproat Lake's two shore parks.
Built and promoted locally: 7 lines move (Sproat, Moat/Cruickshank, Tatlatui/Firesteel, Nanika, Tsintsunko, Isintok,
Garnet/Eneas), and Kotcho is not moved (`rebuild-finish-2026-11-15.md`).

## Closed 2026-11-20

Closed 2026-11-20 on the owner's word ("Couldn't we just assume most of these are done already?"): the two optional loop points (ponds a loop passes, nameless-creek loops) are taken as fine as built. Status stays DONE.
