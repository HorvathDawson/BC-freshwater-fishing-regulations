# Confluence sweep, and the owner's round 1-6 answers applied (2026-11-15 evening)

Owner, 2026-11-15 ~20:05: "Iook ofr other ones like he skeena one to make sure we have them all". Then his answers,
rounds 1-6 (OWNER-TODO "2026-11-15 evening — owner answers by question dialog"), applied in Part 2.

Tags: **[ran]** a script or command over the files named; **[read]** read in code or data; **[inferred]** my reading.
Inputs: live atlas `data/generated/atlas/full` (`splits.resolved.json`, `split_unplaced.json`), FWA streams in
`data/source/bc_fisheries_data.gpkg`, `waters/cuts.json` before tonight's edit (491 cuts), DFO places files (all
states), the 63 cached DFO pages in `cache/dfo_salmon/history/`, the synopsis region files,
`dfo-suggestions.json` and `dfo-split-hunt.json` (read only). Scripts: session scratchpad `csw/` (gather.py,
blks.py, conf.py, mouths.py, pts.py, sheldens.py, edit_cuts.py, remark_nanaimo.py, thompson_ca.py, channels.py).

"Main line" = the FWA blue line whose BLUE_LINE_KEY equals its WATERSHED_KEY (the river's own line; side channels
and braids carry the river's code but another key). "Real mouth" = the first point of the tributary's own main line,
projected onto the parent's main line.

## Part 1. The sweep

### Counts [ran]

| looked at | how many | result |
|---|---|---|
| (a) DFO wordings with confluence / mouth / outlet / inlet / junction / forks / enters / joins / meets words: places files, every state | 78 places (active + dormant) | 2 gaps (the Skeena pair) |
| (a) the same words in the 63 cached DFO pages, wordings not in the places files | 651 rows, 10 distinct wordings | 0 gaps (all are covered by a stored wording's cut, an open end at the line's own mouth, or a sign zone) |
| (a) DFO wordings that name another stream without those words ("from Gosnell Creek to Lamprey Creek", "upstream of Parker Creek", "downstream of the Fleet River", "downstream of Howson Creek") | 36 rows | 0 gaps (Parker, Lamprey, Gosnell, Howson, Fleet all have a main-line confluence cut) |
| (a) synopsis rules whose `extent_text` / `verbatim` carry the words | 114 rules | 0 gaps (the rest are lake mouths drawn as undrawn parts, or descriptions of a landmark) |
| (b) curated confluence-anchored cuts (`anchor.type: confluence`, offsets included) | 106 | 106 resolve; 106 have their point on the parent's main line; 105 within 20 m of the real mouth (+ offset); 1 at 58 m |
| (b) point-anchored cuts named for a confluence or mouth | 14 | all on the right line; 3 cannot be checked by FWA name (below) |
| (b) `split_unplaced.json` of the live atlas | 3 rows | gauges only (08HD024, 08ME002, 08MH032); **no curated cut unplaced** |
| (c) DFO suggestions with a new confluence end | 7 | 1 on a side line (the Skeena/Lakelse one, now replaced by a main-line cut) |
| (c) split-hunt cuts anchored on a confluence | 3 (Nicomen/Siddle, Nakina/Sloko ×2) | all on their water's own line |

### The table: every finding (gaps, side lines, off-mouth, other landmarks)

| # | water / pair | where it comes from | finding [ran] | result |
|---|---|---|---|---|
| 1 | Skeena ← Lakelse | DFO `6:skeena-river:from-lakelse-river-mouth-upstream` (dormant, unmarked) | no cut. The Lakelse (blk 360879877) ends on a Skeena side channel (blk 360223771, m 532), **428 m** from the Skeena main line (blk 360887278). The suggestion put the end on that side line | **added** `skeena_river__lakelse_river_into_skeena_river` (main line, m ~108,507) |
| 2 | Skeena ← Babine | DFO `6:skeena-river:from-the-confluence-with-the-babin` (dormant, unmarked) | no cut. Babine mouth on the main line (0 m off), m ~335,400. Nearest cut: `skeena_river__above_babine_river` = gauge 08EB005 at m 337,743, **2.34 km above** | **added** `skeena_river__babine_river_into_skeena_river` |
| 3 | Skeena, Hwy 16 bridge at Ferry Island (the other end of #1) | same place | no cut | **added** `skeena_river__highway_16_bridge_at_ferry_island` |
| 4 | Skeena "to a point above the confluence with the Babine" | DFO `…upstream-of-cnr-bridge-at-terrace` (+ `~2`, `mainstem-waters-only…~2`) | ends on gauge 08EB005, 2.34 km above the mouth | kept: the words say "a point above", not the confluence; the gauge is named "Skeena River above Babine River" [inferred] |
| 5 | Bella Coola ← Talchako | synopsis `atnarko_bella_coola_rivers.r12` "Atnarko … to the confluence with Talchako River" | resolves 58 m above the Talchako's mouth, at the top of the Bella Coola line (where the Atnarko line starts) | kept: for a rule ON THE ATNARKO the Atnarko's own end is the right point [inferred] |
| 6 | Morice ← Gosnell; Skeena ← Exchamsiks, ← Kitsumkalum (+1500), ← Zymoetz (+1500) | several | the tributary ends on a side channel 219 / 1,252 / 287 / 273 m off the main line; the cut is on the main line abreast of the mouth (projection) | right: the same reading as #1 |
| 7 | Nass ← Meziadin | DFO uses `meziadin_river__meziadin_river_into_nass_river`, synopsis `nass_river.r2` uses `nass_river__meziadin_river_into_nass_river` | two ids, the same point (Nass m 189,238.68) | harmless (a coincident cut is an alias); worth merging some day |
| 8 | Nass "signs near the confluence with the Meziadin", Ecstall "signs near confluence with Johnston Creek" | DFO | the confluence cut stands for the signs | kept (owner kept Ecstall, round 5 #3) |
| 9 | Morice "from Gosnell Creek to Lamprey Creek" | DFO, mark 2739 | signs cut 100 m below Gosnell | kept: owner round 2 "Keep the signs point (Recommended)" |
| 10 | Braid-sweep cuts on other codes | `splits.resolved.json` | 3 confluence cuts also cut small TRIBUTARY lines of another watershed code, not side channels: `morice_river__lamprey_creek_into_morice_river` (blks 360584360, 360773773, 360813099; the last ~2.7 km from the mouth), `skeena_river__exchamsiks_river_into_skeena_river` (360863785), `peace_river__halfway_river_into_peace_river_d5000m` (359240659). Their main-line points are right | a code question for the braid sweep, not a data gap; not changed |
| 11 | Point cuts at a mouth with no FWA name to check against | `nicomen_slough__siddle_creek` (owner's own pin), `brunette_river__salamander_creek_into_brunette_river` (an added stream), `asher_creek__south_fork` (user pin) | not checkable by name | kept |
| 12 | Inch Creek "to the confluence with Norrish Creek", Harrison / Alouette / Nechako / Chehalis / Babine "to the confluence with X" | DFO | the end is the line's own mouth: an open end, no cut needed | right |
| 13 | Babine Lake "400 m radius of the mouth of Pinkut Creek" (+ the tributary list) | DFO, dormant, unmarked | circles, not cuts (entry B / closed areas) | not this sweep's kind |

Everything else checked out: all the other DFO and synopsis confluence ends (Quinsam, Boulder, Little Nitinat,
Parker, Morrison ±100, Ash, Slesse −100, Hyde (8.6 m), Siddle, Seton, Campbell Creek −100, Crag, Meziadin ×3,
Morice→Bulkley ±, Lamprey, Gosnell, Howson, Nilkitkwa, Mill, Zymagotitz, Kitsumkalum/Glacier, Thompson→Fraser,
Spuzzum, Chilcotin, Sakwi, Muchalat, Saunders, Little Oyster, White, Lardeau, Goat, Talchako, Shegunia, Sedan,
Chimdemash, North Anzac, Thorne/Kemess ±500, Macleod, Halfway ±5000, Sloko +300/+1300, Fleet, Johnston) sit on the
parent's main line within 20 m of the real mouth plus the authored offset (`csw/conf_summary.json`).

## Part 2. The owner's answers, applied

| # | answer (owner 2026-11-15) | done | where |
|---|---|---|---|
| 1 | Skeena: "Make them (Recommended)" (+ "make sure we have them all") | **done**: 3 cuts in the `SKEENA RIVER (mainstem only)` waterbody. Lakelse and Babine are confluence anchors (`applies_to` wsc 400; the resolver takes the main channel among the parent lines within `proximity_m` 500, and the Lakelse mouth is 428 m off the main line, so it lands on the main line). The bridge is the hunt's `skeena_dudley_little` point (BC MoT road structure "DUDLEY LITTLE MAIN"), pinned to the main line (`applies_to` blk 360887278) because that point is 143 m off it and would otherwise be free to land on a Ferry Island channel. No other gap found, so nothing more added | `waters/cuts.json` |
| 1b | (while this lane ran) the owner marked both Skeena places: `marks.jsonl:2751` (Babine → CNR bridge) and `:2752` (Lakelse → Ferry Island bridge), each with new splits; his Lakelse one names the side line (blk 360223771) as the suggestion did | the 3 cuts' anchors were written exactly as his marks propose them (tributary wsc `400-174068` / `400-536025`, the bridge at the snapped main-line point -128.565054, 54.512035), so `proposals build` takes them as the existing cuts (`place_groups` dup, checked: all 3 match) and no second cut is proposed. The Lakelse anchor is a confluence anchor, so the atlas puts it on the main line whatever blk the mark's note shows | `cuts.json` |
| 2 | Puntledge tailrace: "Okay please fix them" | **done**: anchor moved to the tailrace outlet (OSM way 1178621508, 49.6885474,-125.0342327, measure ~4429.3); the 50 m upstream offset kept → ~4479 (was 4451.9). Same id; listed in the file's ODbL `_licence.ids` | `waters/cuts.json` |
| 3 | Nanaimo: "Okay please fix them" | **done**: new cut `nanaimo_river__500_m_below_power_line_at_bore_hole` (the power-line point, 500 m downstream: ~12,485 on blk 354154938). Mark 2442 re-pointed by ONE appended mark, **line 2746** (verdict verified, unchanged; note "cut matched …; saved by Claude at the owner's request 2026-11-15"; `dfo.mark_for` + `marks.append`). It waits on the rebuild | `cuts.json`, `marks.jsonl:2746` |
| 4 | Sproat: round 5 "1 … goes ahead" | **done**: his note (mark 2468) says which way: "50m more downstream … bottom of pool", for every extent using the cut. `sproat_river__fishing_boundary_signs_300_m_downstream_of_hwy_4` offset 300 → 350 m downstream (~1874, was 1924.1). Same id; label left as DFO's "300 m" | `cuts.json` |
| 5 | Hell's Gate label | **done**: label "top of Hell's Gate (1.5 km above the Kitsumkalum)", id unchanged | `cuts.json` |
| 6 | Sheldens Creek: "You can hunt for it I think it should be close to tidal" | **hunted, nothing changed** (below) | — |
| 7 | Shuswap: "Use the real gauge point … and the other coord as the other point" | **stopped** (no writer; and his own save 2742 holds the wrong end; below) | — |
| 8 | Thompson three areas: "Okay please fix them" | **2 of 3 accepted**: `closed_area` marks for **Deadman (line 2748)** and **Ashcroft (line 2749)**, built the way the app's `POST /api/marks` does (`reuse.post_mark`: `closed_areas.prints_of` + `dfo.stamp_piece` on the live place `…downstre~2` + `Mark.model_validate`), appended with `marks.append`, note "accepted by Claude at the owner's request 2026-11-15". Their new cuts all stamp on the Thompson main line (blk 356364690). **Juniper held**: its upper signs are marked in the proposal itself as "the owner's choice (two DFO descriptions 2.8 km apart)", so it is a position he has not picked | `marks.jsonl:2748-2749` |
| 9 | Side channels: "Add all five (Recommended)" | **done** for the water records: `2:fraser-river` + blk:355992187 (Herrling), blk:355991777 (Seabird N), blk:355994562 (Jeperson braids); `2:squamish-river-including-powerhouse-channel` + blk:360219234 (Powerhouse Channel), through `dfo_salmon.entries.save` (8 lines changed). **Synopsis Fraser r4**: + `whole blk:355994562` (the same mechanical addition; backed up first, A2). **Maria Slough**: already in the water record (gnis:13499); it is dropped only by the item-restricted marks (`between[gnis:39325]`), which a water-record edit cannot reach: listed below | `places/region-2.json`, `synopsis/region-2.json` |
| 10 | "Raise to 420 kB (Recommended)" | **done**: `salmon-6` 420,000 in `pipeline/deliver/release/build.py` `SIZE_GATES` (dated comment), T686 has the shrink-later line | code |

### 6. Sheldens Creek, Spur 19 Bridge: the hunt [ran]

Sources: the repo's FTEN road layer (`forest_service_roads`, permit R06206) and FWA streams; one public place-name
lookup (BC Geographical Names, "Sheldens" / "Sheldons"; nothing about the owner or project sent).
- **Is the line right?** BC Geographical Names has no "Sheldens Creek", only **Sheldens Bay** (adopted 1954, centre
  -131.75667, 53.15028, Moresby Island). Our line blk 360846083 (wsc 950-016367, 3.69 km, order 2) ends **564 m from
  the bay's centre**: it is the largest stream into Sheldens Bay (the others are under 2.3 km and order 1, except
  360735679, 2.29 km, which ends 851 m away at the bay's south side). So "the creek into Sheldens Bay" = our line is a
  good reading [inferred]. Every SP 19 road is named "… TO SHELDONS".
- **The roads named SP 19** (all R06206, active since 1993):

| candidate | road | where it meets water | coord (lon, lat) | on our line? |
|---|---|---|---|---|
| A (today's cut) | SP 19(1) TO SHELDONS | crosses blk 360846083 at m 3,402 (290 m below its top, 3.4 km from the bay) | -131.78224, 53.12968 | yes; far from tidal, the place he marked "completely the wrong spot" |
| B | SP 19R TO SHELDONS (the last section, heading east to the coast) | the road ENDS 39 m from the top of blk 360843127 (wsc 950-016367-137183, a 764 m tributary that joins our line at m 507, ~0.5 km above the bay) | -131.76932, 53.15602 (stream end); road end -131.76903, 53.15652 | on its tributary; the closest to tidal water of all the SP 19 roads; a road ending at a stream fits "bridge **site**" [inferred] |
| C | SP 19(2) TO SHELDONS | ends 19 m from the top of blk 360862400 (another small stream) | -131.78788, 53.15456 | no |
| D | SP 19H TO SHELDONS | passes 53 m from blk 360368447 (a tributary of our line, its m 722) | -131.78656, 53.13752 | on a tributary, inland |

  No SP 19 road crosses the lower 3 km of our line in the FTEN data (nearest: SP 19R, 330 m from our line's m 715).
- Suggested for the owner: **B** fits his "close to tidal" best, but it sits on a tributary, not on the line we took for
  Sheldens Creek. Either (i) the place is our line upstream of where that tributary joins (m 507), or (ii) the
  tributary 360843127 is the creek DFO means. Not decidable from data; his call.

### 7. Shuswap: why it stopped [read]

- `pipeline/regs/dfo_salmon/closures.py` `main` writes only `dfo_closure_entries.json` (entry B's entries). The
  placement file `dfo_closed_area_geometry.json` has no generator for stretches: `closure_mouths.main` rewrites its
  CIRCLE proposals only; the Shuswap and Thompson stretches were added by hand (1d02f6e88, a T573 commit). So the brief's
  writer cannot express it.
- **And the owner already saved it, with the wrong end replaced**: `marks.jsonl:2742` (closed_area
  `8:shuswap-river#200d03a74aaf`, verified) holds points `gauge__08LC003` → `shuswap_closed_wtfb_as_printed`. The WTFB
  point as printed (50°18.181' N) IS the gauge's place (27 m from 08LC003), so that saved stretch is ~27 m long.
- What his words need: points `{kind: split, split_id: gauge__08LC003}` and `{kind: new, split_id:
  shuswap_closed_station_as_printed}` (DFO's other printed coordinate, 50°19.105' N 118°48.857' W = -118.8142833,
  50.3184167, km 133.08), about 2.05 km. Exactly needed: (1) in `dfo_closed_area_geometry.json`, piece `200d03a74aaf`:
  `points` = those two (the `shuswap_closed_station_as_printed` new split is already in `new_splits`); (2) he re-accepts
  it in the app (a new closed_area mark replaces 2742). Step (1) is a hand edit of a generated file his running app
  reads, so it is left to the main chat's call.

### 9. Maria Slough and the restricted Fraser marks

Maria Slough (gnis:13499) and the gazetted Jeperson line (gnis:11481) are already in `2:fraser-river`. A mark whose
extent names `items: ["gnis:39325"]` binds the Fraser item only, so it drops them and also the three channels added
tonight. Which live Fraser places carry such a restriction is in fraser-channels-2026-11-15.md §1 (the dormant
`…upstream-of-the-cp`; the live `~2` is unrestricted). Fix: the owner re-marks those places without the item
restriction, or the review-app lane makes "restricted to a river" take the river's own channel items. Not done here.

## REBUILD INPUT 2 (for the next side build; NOT run here)

**Cuts added, moved or renamed tonight** (all in `data/curated/waters/cuts.json`, 491 → 495 cuts; backup
`data/generated/curated_writes/backups/waters/cuts.json/20261116T031943497728Z-9be4bad22afb.json`):

| cut | change | expected resolve |
|---|---|---|
| `skeena_river__lakelse_river_into_skeena_river` | new | blk 360887278 m ~108,507 (main line; the braid sweep may also cut side channel 360223771 near m 532) |
| `skeena_river__babine_river_into_skeena_river` | new | blk 360887278 m ~335,400 |
| `skeena_river__highway_16_bridge_at_ferry_island` | new | blk 360887278 m ~131,651 (514 m below the CNR bridge cut at 132,165) |
| `nanaimo_river__500_m_below_power_line_at_bore_hole` | new | blk 354154938 m ~12,485 |
| `puntledge_river__fishing_boundary_signs_50_m_upstream_of_bc_hydro_puntledge_generating_station_tailrace` | moved ~27 m up | blk 354154204 4451.9 → ~4479.3 |
| `sproat_river__fishing_boundary_signs_300_m_downstream_of_hwy_4` | moved 50 m down | blk 354153364 1924.1 → ~1874 |
| `skeena_river__kitsumkalum_river_into_skeena_river_u1500m` | label only | unchanged (123,961.2) |

Plus the closed-area new cuts from marks 2748 / 2749 (Deadman upper signs: Hwy 1 bridge at Savona −1,000 m; Bonaparte
mouth; Hwy 97C bridge in Ashcroft), which `proposals build` turns into cut proposals (T573, fe11e0af7): add their ids
to the side list if `proposals build` lists them.

**Reach-only changes (no atlas cut):** `places/region-2.json` water records (Fraser +3 channel items, Squamish
+Powerhouse Channel); synopsis `fraser_river_upstream_of_mission.r4` + blk:355994562.

**Parity must show:** new boundary ids = the 4 new cuts (+ any closed-area cuts landed), no boundary lost; changed
sections only on blks 360887278 (and braid-swept Skeena side channels at the Lakelse mouth / Ferry Island),
354154938, 354154204, 354153364. **Binding diff:** Puntledge r4 and the DFO 50 m place ~+27 m at their top;
Sproat places and synopsis rules on the Sproat cut +50 m at the lower end; Nanaimo `from-500-m-below…` +500 m once mark
2746 lands; Fraser R2 DFO places on the unrestricted water record + Herrling / Seabird N / Jeperson braids (~20 km);
Squamish `…:whole` + 2.97 km; Fraser r4 + Jeperson braids (5 sections); the two Skeena places (marks 2751/2752) bind
once landed: Lakelse cut ~108,507 → Ferry Island ~131,651 and CNR bridge 132,165 → Babine ~335,400, all on blk 360887278.
`apply --check` now also lists `closed_areas` STALE (marks 2742, 2748, 2749 not yet written to
`dfo_salmon/closed_areas.json`): `python -m pipeline.regs.review.apply closed_areas` lands them (it would also land
2742's ~27 m Shuswap stretch, Q2). Anything else changing is not from tonight.

**Commands** (repo root; as cut-cleanup §5, with these cuts already in `cuts.json`):
```
SIDE=$PWD/data/generated/side/rebuild-input-2; rm -rf $SIDE; mkdir -p $SIDE/logs
rtk git status; node app/tools/check-tickets.mjs; scripts/fast-fail.sh; scripts/heavy.sh --status
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.proposals build      # lists the cuts still to land (closed-area ones)
.venv/bin/python -m pipeline.tools.audit.check_curated --waters
scripts/heavy.sh atlas-side 40 -- .venv/bin/python -u -m pipeline.atlas.build --full --out $SIDE/atlas/full_next > $SIDE/logs/atlas.log 2>&1
.venv/bin/python -m pipeline.atlas.promote $SIDE/atlas/full_next --dry-run > $SIDE/logs/parity.txt     # also lists split_unplaced
scripts/heavy.sh reach-side 40 -- env PYTHONPATH="$PWD" .venv/bin/python -m pipeline.atlas.reach.cli --build $SIDE/atlas/full_next --out $SIDE/reach > $SIDE/logs/reach.log 2>&1
.venv/bin/python -m pipeline.atlas.reach.binding_diff data/generated/reaches/full $SIDE/reach --json $SIDE/binding_diff.json
# STOP: read parity + binding diff against the lists above; owner go
.venv/bin/python -m pipeline.atlas.promote $SIDE/atlas/full_next
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.apply dfo --dry-run      # 2746 (Nanaimo) waits on the new cut
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.apply --check
scripts/heavy.sh build 40 -- .venv/bin/python -m pipeline build --hold-latest
```

**The owner re-ticks / decides after it:**
- The two Skeena places: he marked them tonight (2751, 2752, "unsure": T and R implied); they land on the new cuts
  after the side build (`apply dfo --side-checked`); then a full tick on each.
- Puntledge: entry mark 1263 (r4 print) and DFO 2447 (the tailrace end moved ~27 m).
- Sproat: the three Sproat places (`1:sproat-river:*`) and the Region 1 synopsis Sproat entry, whose reach moved 50 m.
- Fraser R2 places on the water record and Squamish `…:whole` (they gain the channels).
- Shuswap: re-accept the closed stretch once its points are gauge → 50°19.105' N (2742 holds a ~27 m stretch).
- Thompson Juniper: pick its upper signs (the two DFO descriptions 2.8 km apart).

## Questions (plain words)

1. **Sheldens Creek:** the Spur 19 road that ends nearest the sea (SP 19R) ends on a small side creek about 0.5 km above
   Sheldens Bay, not on the creek we drew. Is the place "the creek above where that side creek joins", or is that side
   creek Sheldens Creek? Or point at the bridge on the map. Default: today's cut stays.
2. **Shuswap:** your saved closed stretch (today) runs from the gauge to the printed sign point, which is the same spot as
   the gauge, so it is only ~27 m long. OK to change the proposal to gauge → the other printed point (2 km), for you to
   accept again?
3. **Thompson Juniper:** which upper signs, of DFO's two descriptions 2.8 km apart? Deadman and Ashcroft are saved.
4. **Maria Slough and the restricted Fraser marks:** re-mark those Fraser places without "Fraser only", so the sloughs
   and side channels come in? Or should "Fraser only" always take the Fraser's own channels (an app change)?
5. **Code question (no answer needed now):** three confluence cuts also cut small tributary creeks next to them (Morice
   at Lamprey, one ~2.7 km away; Skeena at Exchamsiks; Peace at Halfway). Worth a look in the braid sweep.
