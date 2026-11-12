# Next rebuild: results of the side build (T664 + T665 + the nine DFO cuts)

2026-11-11, the rebuild lane. Runbook: `next-rebuild.md`. **The side build is done and measured. Nothing is promoted.**

| | |
|---|---|
| Side build | `data/generated/side/next-rebuild/` : `atlas/full_next` (the candidate), `reach/` (the reach run on it), `curated/` (the copy it read), `logs/` |
| Built from | code `56b18e8bb`; the live curated tree + three edits on the copy (nine cuts, the oxbow `one_water_takes` list, the id resync) |
| Atlas | 32 min, rc 0, `COMPLETE.json` written; every input hash current against the copy |
| Reach run | 576 s, rc 0, POLICY_VERSION 37; 3,376 bound / 25 unresolved (same as build 27); straddle and unplaced gates pass |
| Unexplained | **none**. One binding change is explained but is a real 33 m gain (Ucona River, below) |
| Promote needs | the owner's go, the four curated patches applied live, and `--accept-boundary-loss` (net −31, listed below) |

The atlas was built twice. The first build (kept as `atlas/full_next.first`, 3.5 GB, can be deleted) showed a defect in
the T665 rule, the rule was fixed and tested, and the second build is the candidate. See "What the first build found".

## 1. Parity against the live atlas (build 27), every difference by cause

| | live | side | difference | cause |
|---|---|---|---|---|
| items | 21,331 | 21,279 | −52, 0 added | **T664**: the 52 absorbed oxbow ids (44 stream items + the 8 lake / wetland polygons the owner ruled are streams). Nothing else removed |
| items changed | | 175 | 92 waters + 83 areas | T665 158, DFO cut 10, DFO cut + T665 4, T664 1 (the survivor), length cap 2 (Thompson, Nakina: below) |
| sections | 1,958,835 | 1,958,536 | 503 ids gone, 204 new, on 535 blue lines | **T665** 529 lines (a T660 shore cut moved, dropped or added); **DFO cuts** 6 lines. No lake section changed. 0 lines unexplained |
| boundaries (registry) | 27,522 | 27,491 | **−31** | lost ids: 77 `shore:` (**T665**), 2 `length:` (**DFO cuts**), 1 lake edge (**T664**); new ids: 40 `shore:`, 9 curated (**DFO cuts**) |
| shore cuts (all lines) | 532 | 226 | 168 moved to the polygon exit, 30 kept in place (held), 334 dropped, 28 on new lines | **T665** |

- The two lost `length:` cuts (`length:356364690:23466` on the Thompson, `length:360874795:17123` on the Nakina) are
  the 25 km length cap's own: with the new curated cuts every piece there is under the cap (Thompson 7,143 to 26,051 m
  = 18.9 km; Nakina 16,041 to 38,173 m = 22.1 km), so the cap makes no cut. No rule names a `length:` id.
- The lost lake edge is `okanagan_river_oxbows__lake_328985789`: the item's edge onto a polygon it now holds.
- Promote's own list: no boundary id a curated rule names is gone or moved; no curated item id is gone or absorbed
  (after the resync, section 6).
- **`--accept-boundary-loss` is needed**, for a net loss of 31 registry boundaries, not the "363 dropped shore cuts"
  the T665 lane wrote down: promote counts boundaries in `registry.json`, and only 92 of build 27's 532 shore cuts sit
  on a named water. Expected losses, exactly: the 77 `shore:` ids, the 2 `length:` ids and the 1 lake edge above. Any
  other lost id is a real stop.

## 2. The nine DFO cuts

All nine landed on their line within 0.05 m of the README's measure, each on its water's item, each with a new section
starting at the cut.

| cut | blue line | built at (m) | README (m) | new section |
|---|---|---|---|---|
| Nicomekl, 23 m below the flood dam | 356353722 | 4,778.27 | 4,778.3 | 356353722:4778 |
| Serpentine, 23 m below the flood dam | 356351273 | 3,471.01 | 3,471.0 | 356351273:3471 |
| Nakina, Sloko + 300 m | 360874795 | 15,041.79 | 15,041.8 | 360874795:15041 |
| Nakina, Sloko + 1,300 m | 360874795 | 16,041.79 | 16,041.8 | 360874795:16041 |
| Thompson, signs below Goldpan | 356364690 | 26,051.64 | 26,051.6 | 356364690:26051 |
| Thompson, Skihist reserve border | 356364690 | 7,143.15 | 7,143.1 | 356364690:7143 |
| Harrison, Hwy 7 bridge | 356362488 | 5,954.56 | 5,954.6 | 356362488:5954 |
| Nicomen Slough, Hwy 7 bridge at Dewdney | 355991886 | 3,368.94 | 3,368.9 | 355991886:3368 |
| Nicomen Slough, Siddle Creek | 355991886 | 20,557.30 | 20,557.3 | 355991886:20557 |

Rules on those six rivers keep their water: 159 rules change ids there (145 zone and provincial, 14 water rows:
Harrison r1 to r3, Nicomekl r2 r3 r5 r6, Nicomen Slough r1, Serpentine r2 r3, Thompson below Kamloops Lake r1 r2 r6,
Nakina r1) and none changes place. The nine DFO places are not bound yet (step 8 of the runbook). A dry run of that
step against the side atlas (`review.apply dfo --dry-run --build …`, nothing written) says all nine "would apply", and
its diff is line for line `T664-pending/nine-cuts.dfo-places.patch` (96 change lines).

**`NOT_LANDED_OK`**: nothing comes off before promote. After promote and step 8 the nine `dfo|…` keys come off; the
two T359 tidal keys (`tidal|tidal:354151091|`, `tidal|tidal:360848786|`) stay.

## 3. T664: the Okanagan River Oxbows are one water

- One item answers to the name: `wsc:300-432687-459615`, kind stream, 88 sections (48 line pieces + 40 polygons), 70
  aliases. Build 27 has 53 (45 stream + 8 still water). Search will show ONE result.
- The 8 polygons the owner ruled are streams (`wbk:328944522, 328948249, 328957729, 328985789, 329459157, 329459219,
  329459234, 329460279`) joined it through the id list on the name record (`one_water_takes`, patch
  `T664-pending/oxbow-still-water.name_variants.patch`), not through a blanket switch. `ONE_WATER_TAKES_STILL_WATER`
  stays off. How Nicomen Slough's polygons became stream polygons is different on both counts: its four unnamed
  polygons were taken by the flowing fold (a "slough" name), and the other 14 oxbow polygons by the owner's slough
  marks (`slough_calls.json`, written from marks only). These 8 were never in that queue.
- **The oxbow rule: 89 sections before, 89 after, the same set** (`okanagan_river_oxbows.r1`).

Per-rule change on the 8 polygons (the expected, accepted consequence):

| rule | before | after |
|---|---|---|
| `zp:bait` r5 (lakes) | on the 2 lake polygons | off |
| `zp:barbless_single_hook_streams` r2 (lakes) | on the 2 lake polygons | off |
| `z8:spring_stream_closure` r1 | on none | **on all 8** |
| `z8:single_barbless_hook` r1 | on none | on all 8 |
| `z8:daily_quotas` species_quotas r6, trout_char_quota r3 to r6 (streams) | on none | on all 8 |
| `zp:bait` r4, `zp:barbless_single_hook_streams` r1 and r3 (streams) | on none | on all 8 |
| `okanagan_river_oxbows.r1` (the exemption from the spring closure) | on all 8 | on all 8 |

- **The spring stream closure now lands on the 8 polygons, and the oxbow row's exemption is bound there too.** Seven
  of the eight carry exactly the rule set of an oxbow line section (90 rules); the eighth (`lake:328985789`) carries one
  more, `zp:superior_closures` r3, which it had before. So the lift is the same as on the rest of the water by
  construction (one reader, same rule set). Not run through the bundle's reader: no deliver was built.
- `lake:329461947` (the ninth polygon of the old 89) is unchanged: 83 rules before and after, still lake rules, still
  bound by the oxbow rule through river widening. The data: an unnamed FWA lake polygon, no item of its own, on the
  oxbow channel 356529638 between 929 and 1,026 m. FWA does not name it an oxbow, so it was left. Owner item below.

## 4. T665: tidal counts against the dry run

| | dry run 2 (owner's map) | side build | why different |
|---|---|---|---|
| lines wholly inside, tidal whole | 215 not already tidal, 67.7 km | **215, 67.7 km** | same |
| shore cuts | 201 | **226** | −3 Fraser side channels that are tidal water already; +4 creeks into them; +30 held lines that keep their build 27 cut; −5 the Brem River group; −1 Kainet Creek (held) |
| held lines | 61 | **93** | +2 up the channel, +1 braid, +29 not on the owner's map (below) |
| tidal water the cuts are decided on | 70 sections (the graph's reading) | 100 sections = the reach's own | point a, below |
| became tidal (reach run) | | 20.4 km of creek below 184 cuts + the 215 lines | |
| tidal in build 27, fresh now | | 15 strips, 0.3 km in all, longest 65 m | the cut moved down from the T660 crossing top to the polygon exit |

Nothing that was tidal under a curated limit changed: the reach counts 100 sections for it against build 27's 124, on
the same 85 blue lines; the other 24 ids were pieces T660 cuts made inside tidal water (13 on lines tidal whole, 11
below a limit).

Items: 82 change their tidal sections; 60 go from fresh to partly tidal; **one becomes wholly tidal, Brae Slough**
(`blk:356232808`, an owner-found name; dropped from search by D5, its found-name row kept). `T665-orphans.json` is
rewritten from the side build (82 items, 156 references in curated files; none is an id change).

## 5. Per-rule binding diff (reach run on the side atlas against build 27's)

By id, 269 of 3,376 rules differ. Ids are not places, so the diff was also taken **by place** (what each rule covers on
each changed blue line, as metres along it): **183 rules cover different water, 86 only changed ids** (38 the nine
cuts, 39 T665, 9 T664).

Every piece of water a rule gained or lost, by cause:

| cause | rule × piece | what |
|---|---|---|
| T665, tidal now | 23,073 lost | below a polygon cut, or a line wholly in tidal water |
| T665, tidal in build 27 and fresh now | 1,476 gained | the 15 strips above (0.3 km) |
| T664 | 80 gained, 4 lost | the 8 polygons (table above) |
| **explained, but a real gain** | **1** | **Ucona River, 0 to 33 m, joins `strathcona_park_waters.r1`** |
| unexplained | 0 | |

The 183 rules: 94 zone, 76 provincial, 13 water rows. The water rows:

| rule | lost (tidal now) | gained (fresh again) | where |
|---|---|---|---|
| `skeena_river.r1` (tributaries) | 28 | 8 | 33 unnamed creeks, MacMillan, Boneyard and Kildala creeks |
| `atnarko_bella_coola_rivers` r3 r4 r9 r10 r13 | 12 each | | 12 lines of the Bella Coola wholly inside the polygon at its mouth |
| `nass_river.r1` | 3 | 1 | unnamed |
| `san_juan_river.r2` | 3 | | lines of the San Juan wholly inside |
| `brunette_river_s_tributaries.r1` | 2 | | Nelson Creek + 1 unnamed |
| `salmon_river.r2` / `r3` | 1 | 1 | Salmon River (Langley) |
| `cougar_canyon_creek.r1` | 1 | | Cougar Canyon Creek |
| `strathcona_park_waters.r1` | | 1 (fresh before too) | Ucona River |

**The Ucona River, 33 m.** Build 27 cut the Ucona 33 m up (a T660 shore cut into fresh water). The park rule bound the
piece above the cut and not the 33 m below it. That cut is dropped (owner D1), the creek is one piece again, and the
piece is in the park by overlap, so the rule covers the first 33 m too. Cause known (T665, D1); it is a widening, so it
is listed and not waved through. Owner item below.

DFO places: 29 of 352 change ids; 15 change place, all tidal (407 pieces tidal now, 32 fresh again); no place changes
outcome. Unresolved rules: the same 25. Unplaced pieces: the same 2 accepted ones (Noons Creek, Seymour River) and the
known Skeena DFO place (T631 item 4).

## 6. Id resync (registry change → entry resync), as a dry run with a patch

- Entry ids carry no item id; `prune_remapped` no longer exists. Nothing is orphaned by id.
- `backfill_matched --dry-run` on the side registry lists six entries; five are the same standing hand calls it lists
  on the live registry (Nelson Creek, Stave, Sumallo, Seton, Shuswap), and the sixth is the oxbow entry.
- On the copy: `--restamp r8:okanagan_river_oxbows@8-1` (matched 53 → 1) and `T664-pending/resync_notice.py`
  (in-season `is-bb5426c61ff3` 53 → 1 extent, a `set` history row). The matcher passes on the side registry.
- **The reviewable patch is `T664-pending/t664-references.patch`** (remade: region-8.json and notice_waters.json,
  `git apply --check` passes on the live tree). Live curated files were not touched; the copy differs from live in
  exactly four files (`cuts.json`, `name_variants.json`, `region-8.json`, `notice_waters.json`).
- **The guard that was missing (T631 item 3) is in**: `atlas.promote` lists every curated item id the candidate lost or
  absorbed and refuses while there is one (`--accept-dead-ids`). On the live tree it lists the 52 absorbed oxbow ids
  (region-8.json and notice_waters.json); on the resynced copy, none. No id is gone outright.
- `parse-missing` (human only): nothing to parse is expected, since no entry id moved. Command for the owner:
  `bash pipeline/regs/parsing/run_parse.sh parse-missing`.

## 7. Points a to e

**a. The atlas's tidal set (70 sections against the reach's 124).** Fixed, with a gate. The registry's waters are now
made once before the shore cuts (`build.water_registry`) and the reach's own two functions decide the tidal water
(`shore.reach_tidal`); `build_registry` asks again on the registry it writes and stops the build on a difference. In
this build: `100 section(s), the same set`. The 10 cuts of the dry run's list: the 3 atlas-only cuts are gone (those
side channels are tidal water); of the 7 reach-only, 4 are made (354135384 at 252 m, 356070273 at 6 m, Crescent Slough
356326465 at 27 m, 356349946 at 195 m), 2 are held (Cohilukthan Slough 880 m and Deas Slough 225 m up the channel,
over the owner's 100 m), and 356342435 is not cut (it joins Cohilukthan above its kept cut).

**b. The held lines.** They were NOT going to stay as build 27 has them: 26 of the 61 carry a T660 cut in build 27 (19
with a tidal piece below), and the polygon rule alone dropped those cuts. Fixed: `shore.HELD_KEEPS_ITS_CUT`. Measured
on the side atlas: **93 of 93 held lines have the same sections as build 27, and the same cut or none** (30 with a cut).

**c. Braid sweep, sliver / straddle / unplaced gates, the sidecar writer.**
- Braid sweep: it would have stopped the build on Kainet Creek (360871314, exit 16 m up; its side channel joins on
  both sides and the sweep's line misses it). Held, as in build 27. In the build: sweep cases `refused: another
  water` 3, `refused: one side` 7, nothing to cut, 0 networks straddle.
- Sliver gate: passes (no piece under 5 m; the shortest tidal piece is 356070273, 5.95 m).
- Straddle and unplaced gates (reach run): pass.
- `sidecars.py` writer for `inside_tidal.json`: not needed and not added. The sidecar pass never changes a section,
  so it leaves the build's file in place (`--out` links it). The messages that pointed at a writer are corrected, and
  `--tidal-only` now warns when a `split` / `whole` / `cut` limit changed since the atlas was built.
- Found on the way: the registry made before the cuts must leave `outside_area_items` out (no area item exists before
  membership; it would have stopped every full build). Fixed and tested.

**d. Dewdney Slough.** The owner's reading holds: one water, two names. FWA has no water named Dewdney Slough (streams,
lakes, wetlands); the only Dewdney water is Dewdney Creek (`gnis:23609`, MU 2-17, elsewhere). The registry and
`name_variants.json` have none. DFO's Region 2 table prints "Dewdney Slough - See Nicomen Slough" and "Nicomen
(including Dewdney) Slough". The Highway 7 bridge at Dewdney crosses Nicomen Slough (`gnis:21105`, blue line 355991886
at 3,369 m). The marks are complete as they are. Proposed, not applied and not in this build:
`T664-pending/dewdney-slough.name_variants.patch` ("Dewdney Slough" as a search name on `gnis:21105`).

**e. Orphans.** Section 6. T665 moves no id; T664 absorbs 52, resynced by the patch.

## What the first build found (why there were two)

The T665 rule asked "does every outlet of the piece below the cut end in tidal water" of the line's lowest SECTION.
On a line that is not cut yet that section is the whole lower river, with every side channel that leaves it anywhere.
The dry runs ran on build 27's graph, which T660 had already cut, and did not see it. In the first build:
- 6 lines were not cut that should be (McDonald Slough, Glen-Lyon Creek, Power House Creek, Wahoo Creek, 2 unnamed):
  5 of them are tidal crossings in build 27 and would have gone back to fresh water.
- 10 creeks were cut for nothing (they join a held slough above its kept cut).
- The item "Power House Creek" was renamed "Byrne Slough" (an item's name is its most common piece name; one piece
  fewer flipped it). It is back in the second build. Ticket T677.

Fixed (`shore.outlet_edges`, `edge_leave_measures`: by where each edge leaves the line and where it joins the next),
with tests. The precise rule then showed 29 rivers and creeks going tidal that the owner's pages listed as "not cut:
would stay freshwater" (same artifact, on the dry run). Those are held, not cut. The second build's cuts were
predicted in memory on the first atlas before building: 226 cuts and 215 whole lines, and the build made exactly those.

## Owner items

1. **Go for promote.** Commands below. Net boundary loss 31, as listed in section 1.
2. **Held lines: 93, of which 32 are new and not on a map page yet.**
   - Up the channel (same 100 m measure): Cohilukthan Slough 356285962 (880 m), Deas Slough 356358883 (225 m). Both
     are among the 33 mouth-tick creeks; the dry-run table showed them "not cut".
   - Braid across the exit: Kainet Creek 360871314 (16 m).
   - 29 the rule makes tidal that the dry-run pages showed as "not cut" (`shore.HELD_NOT_ON_THE_OWNERS_MAP`, metres
     that would go tidal): Kwatna 3,125; Quaal 2,357; Ksi X'anmas 1,641 + 310; Bella Coola 1,320; Kitimat 1,251;
     Necleetsconnay 1,073 + 617; Khutze 1,068; Moyeha 959; Wathl Creek 928; Tom Browne Creek 855; Kemano 813; San
     Juan 695 (keeps its build 27 cut at 37 m); Mussel 635 + 142; Koksilah 542; Zeballos 474 + 195; Bonsall Creek 461;
     Theodosia 441; Stagoo Creek 399; Brittain 279; Nieumiamus Creek 120; unnamed 360883402 1,681, 356341234 264
     (keeps its cut at 193 m), 360233262 172, 354150843 127, 354113387 12. 17 of the 27 without a cut carry your
     `mouth` row. If you tick them, 31 creeks into them are cut too, and 24 braid cases at their polygon exits need a call.
3. **Four creeks newly cut** (they join Fraser side channels that are tidal water): 354135384 (252 m), 356070273
   (6 m), Crescent Slough 356326465 (27 m, one of the 33), 356349946 (195 m). Say if any should be held.
4. **Shown as cut on the dry-run map, not cut in the build**: Brem River 360854323 (102 m) and three unnamed creeks
   into it (360743108, 360827624, 360883856), and 360802896. The Brem's line ends at the sea with no water mapped
   below it, so there is nothing the rule can call tidal; the dry run's "tidal below" came from the artifact above.
5. **Ucona River, 33 m**: the first 33 m now fall under the Strathcona Park waters rule (section 5). Accept, or keep a
   cut there.
6. **`lake:329461947`**: an unnamed polygon on an oxbow channel, still a lake by kind, bound by the oxbow rule but not
   by the stream rules. Fold it in too? It does not show in search either way.
7. **Dewdney Slough as a search name** (point d): accept the patch or not.
8. Standing: the `mouth`-rows patch (`T665-tidal-limits-mouth-rows.patch`, not applied; the build is the same either
   way), the Nakina half-width, the T664 survivor id.

## Where the side build is, and the promote commands

`data/generated/side/next-rebuild/atlas/full_next` (3.5 GB), `…/reach`, `…/curated`. Measurements:
`…/measure_atlas.json`, `measure_reach.json`, `measure_geo.json` (by place), `measure_tidal_pieces.json`,
`binding_diff.json`, `logs/parity.txt`, `logs/apply_dfo_dryrun.txt`.

Promote is runbook steps 7 to 9, run without `CURATED_ROOT`:
```
SIDE=$PWD/data/generated/side/next-rebuild
mkdir -p $SC/backup && cp data/curated/regulations/synopsis/region-*.json data/curated/inseason/notice_waters.json \
    data/curated/waters/cuts.json data/curated/waters/name_variants.json $SC/backup/
git apply handoff/PLAN/app-integration/T664-pending/nine-cuts.cuts.json.patch
git apply handoff/PLAN/app-integration/T664-pending/oxbow-still-water.name_variants.patch
git apply handoff/PLAN/app-integration/T664-pending/t664-references.patch
diff -r data/curated $SIDE/curated                                            # must print nothing
.venv/bin/python -m pipeline.atlas.inputs --build $SIDE/atlas/full_next       # current with its 10 inputs
.venv/bin/python -m pipeline.atlas.promote $SIDE/atlas/full_next --dry-run    # section 1's numbers; no dead or absorbed id
.venv/bin/python -m pipeline.atlas.promote $SIDE/atlas/full_next --accept-boundary-loss
git commit -m "…" -- data/curated/waters/cuts.json data/curated/waters/name_variants.json \
    data/curated/regulations/synopsis/region-8.json data/curated/inseason/notice_waters.json
```
Then step 8 (land the nine marks, take their keys off `NOT_LANDED_OK`), step 9 (`scripts/heavy.sh build 50 --
.venv/bin/python -m pipeline build`, re-cut the app fixtures, the release).

## Not verified

- No tiles, bundle, export or app build on the side atlas: labels, the one oxbow card and search result, the lift on
  the 8 polygons through the reader, the bundle's tidal proof, the DFO drowned check, the in-season re-resolve.
- The held lines are compared by section ids (whole-metre starts) and cut measures, not by geometry.
- The slow atlas tests (`-m slow`: `test_no_slivers.py` and the others that read a built atlas) were not run on the
  side atlas, and the full suite was not run. Two tests fail in other lanes' areas and are not from this work:
  `test_dataflow_gates.py::test_gate_4_one_calendar_spec` (app/tools/testbed) and
  `test_release.py::test_the_apps_paint_fixture_is_current`.
- T606 item 2 (lake clash suffix numbering, lake names) is not in this build.
- RULINGS.md and MAP.md still do not carry the shore rule (a T665 leftover).
- The "up the channel" measure is the T665 lane's distance-to-edge proxy; it was reused, not re-derived.
- The measurement scripts are kept with the side build (`data/generated/side/next-rebuild/tools.tar`, packed 2026-11-11 so no `.py` sits under `data/` for the CI path gate: `measure_atlas.py`,
  `measure_reach.py`, `measure_geo.py`, the in-memory dry runs), not in the repo.
