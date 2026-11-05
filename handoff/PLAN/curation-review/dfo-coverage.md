# DFO extent coverage — every historic location (owner 2026-10-27)

352 historic DFO salmon locations (all scrapes, active + dormant); 173 have a curated extent; the other **179** are below — each with a proposal + confidence + evidence in the review app's DFO mode (`accept suggestion` pre-fills the editor; **Save extent** — your mark — is the only write). Nothing touched splits.json, the catalogue or marks.jsonl.

Regenerate: `python -m pipeline.regs.dfo_salmon.suggest` (matcher → split hunt `dfo-split-hunt.json` → this pass, `pipeline/regs/dfo_salmon/coverage.py`).

## Outcome

| outcome | high | medium | low | total |
|---|---|---|---|---|
| matched to EXISTING cuts | 32 | 16 | 4 | 52 |
| the whole water (no cut) | | | | 47 |
| NEW split proposed | 24 | 21 | 20 | 65 |
| unresolvable (why below) | | | | 15 |

Clusters of rewordings with an unplaced member: **25** (covering 58 locations, placed members included). Unplaced members: inherit a curated extent 6, adopt the cluster's better suggestion 1, already agree 20, canonical 20, member (own reading kept — differs in meaning) 5.

## Why each was unplaced

| why | n | meaning |
|---|---|---|
| review_pending | 80 | existing cuts (or the whole water) say it — never curated, owner review pending |
| needs_split | 57 | the water lacks a cut it needs — a NEW split is proposed |
| variant | 15 | a rewording of another unplaced location (one decision covers the cluster) |
| lake_area | 12 | a part of a lake (a line across it / a radius) — not a cut on a line; lake splits are deferred |
| variant_of_placed | 6 | a rewording of a location that already has a curated extent |
| side_channel | 3 | a side channel FWA does not draw — needs an added stream or an area |
| tributary_set | 3 | tributaries only — a mark cannot say tributaries-only |
| unknown_landmark | 2 | the notice's landmark is named nowhere public |
| no_water | 1 | the location's water is not matched to the registry |

## Unresolvable

| location | why | evidence / route |
|---|---|---|
| `2:sakinaw-lake:easterly-of-a-line-drawn-from-a-bo` | lake_area | a part of a lake (a line across it): not a cut on a stream line. The notice's own boundary sign is at 49°41.50'N 123°58.45'W (49.69167,-123.97417) — curate as a lake area by hand. Owner rule (lake-splits-deferred): a lake divided into closure areas is deferred — curate later as a lake split / sign-zone area. |
| `2:sakinaw-lake:the-body-of-water-known-as-bear-ba` | lake_area | Bear Bay, a part of the lake east of a north–south line from 49°40.05'N 124°02.13'W (49.66750,-124.03550): a lake area, not a line cut — curate by hand. Owner rule (lake-splits-deferred): a lake divided into closure areas is deferred — curate later as a lake split / sign-zone area. |
| `5a:quesnel-lake:in-the-waters-of-horsefly-bay-loca` | lake_area | Horsefly Bay inside a line of signs across its entrance: a lake area, not a stream cut — curate by hand (no published coordinate found). Owner rule (lake-splits-deferred): a lake divided into closure areas is deferred — curate later as a lake split / sign-zone area. |
| `5a:quesnel-lake:that-portion-of-quesnel-lake-known` | lake_area | Horsefly Bay (lake area) — as above. Owner rule (lake-splits-deferred): a lake divided into closure areas is deferred — curate later as a lake split / sign-zone area. |
| `6:babine-lake:within-a-400-m-radius-of-the-mouth` | lake_area | a 400 m radius around the Pinkut Creek mouth on the lake: a lake area — curate by hand. Owner rule (lake-splits-deferred): a lake divided into closure areas is deferred — curate later as a lake split / sign-zone area. |
| `6:babine-lake:within-a-400m-radius-of-the-mouth` | lake_area | a 400 m radius around the Pinkut Creek mouth: a lake area — curate by hand. Owner rule (lake-splits-deferred): a lake divided into closure areas is deferred — curate later as a lake split / sign-zone area. |
| `8:mabel-lake:northerly-of-a-line-drawn-from-a-w` | lake_area | lake areas (a line from Mabel Lake Provincial Park's north edge to a western point; signs ~1 km from Wap Creek) — curate by hand. Owner rule (lake-splits-deferred): a lake divided into closure areas is deferred — curate later as a lake split / sign-zone area. |
| `8:osoyoos-lake:in-the-waters-in-the-portion-of-os` | lake_area | Osoyoos Lake north of the Hwy 3 bridge (the North Basin): a lake part — the bridge (Hwy 3, Osoyoos narrows) is a line across the lake, curate as a lake split by hand. Owner rule (lake-splits-deferred): a lake divided into closure areas is deferred — curate later as a lake split / sign-zone area. |
| `8:osoyoos-lake:in-the-waters-in-the-portion-of-os~2` | lake_area | same North Basin lake part — curate by hand. Owner rule (lake-splits-deferred): a lake divided into closure areas is deferred — curate later as a lake split / sign-zone area. |
| `6:sheldens-creek:upstream-of-white-triangular-fishi` | no_water | (T164 2026-10-29) the WATER is the open question, not the cut. Proposed match: the unnamed FWA stream blk 360846083 (code 950-016367) into Sheldens Bay, Moresby Island (mouth 53.14874,-131.76677) — the forest roads around it are all named for it (FTEN R06206 'SP 19(1) TO SHELDONS', 'SP 19H TO SHELDONS', SHEL100–SHEL103). The cut: FTEN road 'SP 19(1) TO SHELDONS' (R06206 sec 331, ACTIVE) crosses that line at 53.12968,-131.78224, FWA measure 3402 m (exact road×stream intersection) — the Spur 19 bridge site, medium. The name is a REVIEW row in the Names mode (candidate `blk:360846083~sheldens-creek`, never auto-accepted): accepting it names the line, the DFO matcher then binds the water, and this cut becomes placeable here. |
| `2:stave-river:in-that-portion-of-the-stave-river` | side_channel | Ruskin + Northrop spawning channels — both side channels off the FWA Stave (Ruskin: OSM 1485091713, inlet 49.1950,-122.4085; Northrop: OSM 72823766, intake 49.1938,-122.4120, joins the mainstem ≈ 49.1892,-122.4121). |
| `2:stave-river:that-portion-of-the-stave-river-kn` | side_channel | the Ruskin Spawning Channel is a side channel on the east bank, not on the FWA Stave line. Mapped (OSM 1485091713 'Ruskin Channel'): inlet near the dam 49.1950,-122.4085, boat-ramp crossing ≈ 49.1921,-122.4070, joins the Stave ≈ 49.1864,-122.4092 — needs an added channel or an area, by hand. |
| `2:stave-river:the-ruskin-spawning-channel-on-the` | side_channel | same Ruskin Spawning Channel (side channel, not on the FWA Stave): inlet 49.1950,-122.4085, boat-ramp crossing ≈ 49.1921,-122.4070 (OSM 1485091713). |
| `6:kasiks-river:downstream-of-fishing-boundary-sig` | unknown_landmark | the Kasiks 'Upper Pool' — not found (as above). |
| `6:kasiks-river:upstream-of-fishing-boundary-signs` | unknown_landmark | 'the Upper Pool' is named nowhere public (DFO pages, FWA, FISS: no lake or falls in the lower 12 km). Ask DFO Terrace or a local guide. |

## Caveats on a proposal (read before accepting)

- `1:nanaimo-river:in-subarea-17-14-tidal-portions-of` — tidal Subarea 17-14: the reach below Cedar Bridge is tidal water — check the extent should bind the freshwater line at all (the tidal boundary rule, T212).
- `1:nanaimo-river:in-subarea-17-14-tidal-portions-of~2` — tidal Subarea 17-14 — as the sibling row: check the freshwater extent should bind tidal water (T212).
- `5b:bella-coola-river-not-including-at:all-tributaries-to-the-bella-coola` — TRIBUTARIES ONLY (not the Bella Coola mainstem; the water excludes the Atnarko). A mark can only say whole + tributaries — on save, note tributaries_only.
- `6:babine-lake:babine-lake-excluding-tributaries` — the lake LESS 400 m radii around the named creek mouths — those carve-outs are not geometry: keep them as the binding's notes / spatial_caveat (DFO doc 19 §3.1), the map shows the whole lake.
- `6:babine-lake:babine-lake-excluding-tributaries~2` — the lake LESS 400 m radii around the named creek mouths and the part east of a line from Gullwing Creek to the south shore — those carve-outs are not geometry: keep them as the binding's notes / spatial_caveat (DFO doc 19 §3.1), the map shows the whole lake.
- `6:babine-lake:babine-lake-excluding-tributaries~3` — the lake LESS 400 m radii around the named creek mouths and the part east of a line from Gullwing Creek to the south shore — those carve-outs are not geometry: keep them as the binding's notes / spatial_caveat (DFO doc 19 §3.1), the map shows the whole lake.
- `6:bulkley-river:all-tributaries-of-the-bulkley-riv` — TRIBUTARIES ONLY, not the Bulkley mainstem, and not the Suskwa. A mark can only say whole + tributaries — on save, note 'tributaries_only, exclude Suskwa' (the binding's tributaries_only / tributary_excludes).
- `6:bulkley-river:all-tributaries-of-the-bulkley-riv~2` — TRIBUTARIES ONLY, less the Morice (and its tributaries), the Suskwa (and its tributaries) and Two Mile Creek. A mark can only say whole + tributaries — on save, note tributaries_only + those three excludes.

## Clusters

Canonical first; **placed** = has a curated extent; role of each unplaced member.

- `1:nanaimo-river:from-the-cedar-road-bridge-crossin` (placed) · `1:nanaimo-river:cedar-road-bridge-upstream-approx` (inherits)
- `1:nanaimo-river:in-subarea-17-14-tidal-portions-of` (canonical) · `1:nanaimo-river:in-subarea-17-14-tidal-portions-of~2` (member)
- `1:sproat-river:from-sproat-lake-to-approx-300m-be` (placed) · `1:sproat-river:sproat-river-from-sproat-lake-to-f` (inherits)
- `1:stamp-river:stamp-river-downstream-of-girl-gui` (placed) · `1:stamp-river:downstream-of-signs-at-girl-guide` (inherits)
- `1:stamp-river:from-the-confluence-of-the-ash-riv~2` (placed) · `1:stamp-river:from-the-confluence-of-the-ash-riv` (inherits)
- `1:stamp-river:from-the-inlet-upstream-of-roberst` (canonical) · `1:stamp-river:hatchery-lagoon-from-fishing-bound` (agrees)
- `1:stamp-river:from-the-powerline-crossing-downst` (canonical) · `1:stamp-river:powerline-crossing-from-boundary-s` (agrees)
- `2:alouette-river-and-tributaries:downstream-of-a-line-between-two-f` (canonical) · `2:alouette-river-and-tributaries:downstream-of-a-line-between-2-fis` (agrees)
- `2:chilliwack-vedder-river-including:from-a-line-between-2-fishing-boun~3` (canonical) · `2:chilliwack-vedder-river-including:from-a-line-between-2-fishing-boun` (agrees) · `2:chilliwack-vedder-river-including:from-a-line-between-2-fishing-boun~2` (agrees) · `2:chilliwack-vedder-river-including:upstream-from-a-line-between-2-fis~2` (agrees)
- `2:fraser-river:mainstem-waters-from-the-downstrea` (canonical) · `2:fraser-river:from-the-downstream-side-of-the-cp` (member) · `2:fraser-river:from-the-downstream-side-of-the-cp~2` (member) · `2:fraser-river:mainstem-waters-downstream-of-the` (adopts)
- `2:fraser-river:mainstem-waters-upstream-of-the-cp~2` (canonical) · `2:fraser-river:mainstem-waters-upstream-of-the-cp` (agrees)
- `2:serpentine-river:downstream-of-168th-street-at-both` (canonical) · `2:serpentine-river:downstream-of-168th-street` (agrees)
- `2:stave-river:downstream-of-b-c-hydro-dam-to-the~2` (canonical) · `2:stave-river:downstream-of-b-c-hydro-dam-to-the` (agrees)
- `2:stave-river:the-ruskin-spawning-channel-on-the` (canonical) · `2:stave-river:in-that-portion-of-the-stave-river` (agrees) · `2:stave-river:that-portion-of-the-stave-river-kn` (agrees)
- `3:thompson-river:from-kamloops-lake-outlet-downstre~2` (canonical) · `3:thompson-river:from-kamloops-lake-outlet-downstre` (agrees)
- `5a:quesnel-lake:that-portion-of-quesnel-lake-known` (canonical) · `5a:quesnel-lake:in-the-waters-of-horsefly-bay-loca` (agrees)
- `5a:quesnel-river:that-portion-of-the-quesnel-river` (canonical) · `5a:quesnel-river:between-the-johnson-subdivision-br` (agrees)
- `6:babine-lake:babine-lake-excluding-tributaries~3` (canonical) · `6:babine-lake:babine-lake-excluding-tributaries` (member) · `6:babine-lake:babine-lake-excluding-tributaries~2` (agrees)
- `6:babine-lake:within-a-400m-radius-of-the-mouth` (canonical) · `6:babine-lake:within-a-400-m-radius-of-the-mouth` (agrees)
- `6:bulkley-river:all-tributaries-of-the-bulkley-riv~2` (canonical) · `6:bulkley-river:all-tributaries-of-the-bulkley-riv` (member)
- `6:nass-river:mainstem-waters-downstream-of-the` (canonical) · `6:nass-river:mainstem-waters-downstream-of-the~2` (agrees)
- `6:skeena-river:upstream-of-cnr-bridge-at-terrace~2` (placed) · `6:skeena-river:from-the-confluence-with-the-babin` (inherits) · `6:skeena-river:mainstem-waters-only-upstream-of-c~2` (placed) · `6:skeena-river:upstream-of-cnr-bridge-at-terrace` (inherits)
- `7:nechako-river:from-the-downstream-edge-of-the-fo` (canonical) · `7:nechako-river:upstream-from-the-boundary-signs-a` (agrees)
- `8:osoyoos-lake:in-the-waters-in-the-portion-of-os~2` (canonical) · `8:osoyoos-lake:in-the-waters-in-the-portion-of-os` (agrees)
- `8:shuswap-river:middle-between-shuswap-falls-and-m~2` (canonical) · `8:shuswap-river:middle-between-shuswap-falls-and-m` (agrees)

## Every location

| location | status | why | proposal | conf |
|---|---|---|---|---|
| `1:cayeghle-river:including-colonial-river` | active | review_pending | the whole water | medium |
| `1:cowichan-river:downstream-of-the-weir-on-cowichan` | dormant | review_pending | between Cowichan Lake and CNR Mile 66 Trestle | high |
| `1:cowichan-river:from-cnr-trestle-at-mile-70-2-to-s` | dormant | needs_split | between CNR Bridge (mile 70.2) and Skutz Falls | high |
| `1:cowichan-river:including-tributaries-and-cowichan` | dormant | review_pending | the whole water | medium |
| `1:koksilah-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `1:little-qualicum-river:from-the-falls-in-little-qualicum` | active | needs_split | between Little Qualicum Falls and hatchery fence | medium |
| `1:nanaimo-river:cedar-road-bridge-upstream-approx` | dormant | variant_of_placed | between Cedar Road bridge and Hwy 19 bridge | high |
| `1:nanaimo-river:in-subarea-17-14-tidal-portions-of` | active | review_pending | downstream of Cedar Road bridge | high |
| `1:nanaimo-river:in-subarea-17-14-tidal-portions-of~2` | active | needs_split | between Cedar Road bridge and signs 400 m below the Cedar Road bridge | medium |
| `1:nanaimo-river:including-tributaries-except-in-th` | active | review_pending | the whole water with its tributaries | medium |
| `1:puntledge-river:downstream-from-a-fishing-boundary` | dormant | review_pending | downstream of signs 75 m downstream of Puntledge River hatchery fence | medium |
| `1:puntledge-river:from-the-weir-at-the-lower-hatcher` | dormant | needs_split | between Puntledge lower hatchery weir and signs 75 m below the Puntledge lower hatchery weir | medium |
| `1:qualicum-river:all-open-portions-of-the-qualicum` | active | review_pending | the whole water | medium |
| `1:quatse-river:100m-upstream-and-downstream-of-th` | active | needs_split | between 100 m above the Quatse River Hatchery weir and 100 m below the Quatse River Hatchery weir | low |
| `1:san-juan-river:that-portion-of-the-san-juan-river` | dormant | needs_split | between San Juan hatchery counting fence and signs 150 m below the San Juan hatchery fence | low |
| `1:san-juan-river:waters-of-the-san-juan-river-and-i` | dormant | review_pending | downstream of Fleet River → San Juan River | high |
| `1:sproat-river:sproat-river-from-sproat-lake-to-f` | dormant | variant_of_placed | between sproat_river__sproat_lake and fishing boundary signs 300 m downstream of Hwy 4 | medium |
| `1:stamp-river:between-fishing-boundary-signs-app` | dormant | review_pending | between fishing boundary signs 200 m upstream of Stamp Falls and signs 500 m downstream of Stamp Falls | high |
| `1:stamp-river:downstream-of-signs-at-girl-guide` | dormant | variant_of_placed | downstream of Girl Guide Falls signs | medium |
| `1:stamp-river:from-the-confluence-of-the-ash-riv` | dormant | variant_of_placed | upstream of Ash River → Stamp River | high |
| `1:stamp-river:from-the-inlet-upstream-of-roberst` | active | needs_split | between Robertson Creek Hatchery lagoon inlet and Robertson Creek Hatchery lagoon outlet | low |
| `1:stamp-river:from-the-powerline-crossing-downst` | active | needs_split | between Stamp River powerline crossing (BC Hydro 1L127) and Robertson Creek Hatchery lagoon inlet | low |
| `1:stamp-river:from-the-southern-portion-of-the-r` | dormant | needs_split | between Robertson Creek Hatchery lagoon inlet and Robertson Creek Hatchery lagoon outlet | low |
| `1:stamp-river:hatchery-lagoon-from-boundary-sign` | dormant | needs_split | between Robertson Creek Hatchery lagoon inlet and Stamp River cable car crossing | low |
| `1:stamp-river:hatchery-lagoon-from-fishing-bound` | dormant | variant | between Robertson Creek Hatchery lagoon inlet and Robertson Creek Hatchery lagoon outlet | low |
| `1:stamp-river:powerline-crossing-from-boundary-s` | dormant | variant | between Stamp River powerline crossing (BC Hydro 1L127) and Robertson Creek Hatchery lagoon inlet | low |
| `1:stamp-river:upstream-of-signs-located-at-girl` | dormant | review_pending | upstream of Girl Guide Falls signs | medium |
| `2:alouette-river-and-tributaries:above-216th-street-including-the-n` | dormant | review_pending | upstream of 216 Street bridge | low |
| `2:alouette-river-and-tributaries:downstream-of-a-line-between-2-fis` | dormant | variant | downstream of Allco Park boundary signs (Allco hatchery fence) | medium |
| `2:alouette-river-and-tributaries:downstream-of-a-line-between-two-f` | dormant | needs_split | downstream of Allco Park boundary signs (Allco hatchery fence) | medium |
| `2:alouette-river-and-tributaries:north-alouette-and-tributaries` | active | review_pending | the whole water | high |
| `2:birkenhead-river:from-the-birkenhead-bridge-located` | dormant | needs_split | between Birkenhead Bridge (Portage Road) and Birkenhead canyon (foot) | medium |
| `2:brunette-river:downstream-of-cariboo-road` | dormant | needs_split | downstream of Cariboo Road bridge | high |
| `2:capilano-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `2:chapman-creek:upstream-of-tidal-water-fishing-bo` | active | needs_split | between Chapman Creek Hwy 101 bridge (tidal signs below it) and 100 m below the Chapman Creek falls (below the power line) | low |
| `2:chehalis-river:downstream-of-the-logging-bridge-2` | active | review_pending | downstream of main logging road bridge ~2.4km d/s Chehalis Lk | high |
| `2:chilliwack-vedder-river-including:from-a-line-between-2-fishing-boun` | dormant | variant | downstream of fishing boundary signs 100 m below the Slesse Creek confluence | high |
| `2:chilliwack-vedder-river-including:from-a-line-between-2-fishing-boun~2` | dormant | variant | downstream of fishing boundary signs 100 m below the Slesse Creek confluence | high |
| `2:chilliwack-vedder-river-including:from-a-line-between-2-fishing-boun~3` | active | review_pending | downstream of fishing boundary signs 100 m below the Slesse Creek confluence | high |
| `2:chilliwack-vedder-river-including:upstream-from-a-line-between-2-fis` | dormant | review_pending | upstream of fishing boundary signs 100 m below the Slesse Creek confluence | high |
| `2:chilliwack-vedder-river-including:upstream-from-a-line-between-2-fis~2` | dormant | variant | downstream of fishing boundary signs 100 m below the Slesse Creek confluence | high |
| `2:fraser-river:from-the-downstream-side-of-the-cp` | dormant | needs_split | between the CPR Bridge at Mission and Highway 1 bridge at Hope (Bill Hartley / Fraser-Hope) | low |
| `2:fraser-river:from-the-downstream-side-of-the-cp~2` | dormant | needs_split | between the CPR Bridge at Mission and Alexandra Bridge (Hwy 1) | low |
| `2:fraser-river:from-the-highway-1-bridge-at-hope` | dormant | needs_split | between Highway 1 bridge at Hope (Bill Hartley / Fraser-Hope) and Alexandra Bridge (Hwy 1) | medium |
| `2:fraser-river:mainstem-waters-downstream-of-the` | active | variant | between the CPR Bridge at Mission and Highway 1 bridge at Hope (Bill Hartley / Fraser-Hope) | low |
| `2:fraser-river:mainstem-waters-from-the-downstrea` | dormant | needs_split | between the CPR Bridge at Mission and Highway 1 bridge at Hope (Bill Hartley / Fraser-Hope) | low |
| `2:fraser-river:mainstem-waters-upstream-of-the-cp` | dormant | variant | upstream of the CPR Bridge at Mission | low |
| `2:fraser-river:mainstem-waters-upstream-of-the-cp~2` | active | review_pending | upstream of the CPR Bridge at Mission | low |
| `2:fraser-river:upstream-from-the-highway-1-bridge` | dormant | needs_split | upstream of Highway 1 bridge at Hope (Bill Hartley / Fraser-Hope) | high |
| `2:harrison-river:from-the-highway-7-bridge-downstre` | active | needs_split | downstream of Hwy 7 bridge over the Harrison | high |
| `2:harrison-river:from-the-outlet-of-harrison-lake-d` | active | needs_split | between Harrison Lake and Hwy 7 bridge over the Harrison | high |
| `2:harrison-river:from-the-outlet-of-harrison-lake-d~2` | dormant | review_pending | downstream of Harrison Lake | high |
| `2:inch-creek:from-the-hatchery-outlet-to-the-co` | dormant | needs_split | downstream of Inch Creek Hatchery outlet | medium |
| `2:kanaka-creek:downstream-of-the-112th-street-bri` | active | review_pending | downstream of 112 Avenue bridge | medium |
| `2:kanaka-creek:upstream-of-the-112th-street-bridg` | dormant | review_pending | upstream of 112 Avenue bridge | medium |
| `2:little-campbell-river:downstream-of-12th-avenue-includin` | active | review_pending | downstream of 12th Avenue | high |
| `2:little-campbell-river:downstream-of-a-line-between-2-fis` | active | review_pending | between two white triangular fishing boundary signs (200 m upstream) and pedestrian bridge at the foot of Stayte Road | medium |
| `2:nicomekl-river:downstream-of-208th-street` | active | review_pending | downstream of 208th Street | high |
| `2:nicomekl-river:within-23-m-downstream-of-the-floo` | dormant | needs_split | between dyke gates and 23 m below the Nicomekl flood dam | high |
| `2:nicomen-including-dewdney-slough:from-the-confluence-of-siddle-bell` | active | needs_split | downstream of Siddle (Bell's) Creek mouth | high |
| `2:nicomen-including-dewdney-slough:from-the-highway-7-bridge-at-dewdn` | active | needs_split | downstream of Hwy 7 Nicomen Slough Bridge at Dewdney | high |
| `2:pitt-river:pitt-river-and-tributaries` | dormant | review_pending | the whole water with its tributaries | high |
| `2:pitt-river:upper-and-lower-river-including-tr` | dormant | review_pending | the whole water with its tributaries | high |
| `2:pitt-river:upper-river-upstream-of-pitt-lake` | dormant | review_pending | upstream of Pitt Lake | high |
| `2:sakinaw-lake:easterly-of-a-line-drawn-from-a-bo` | dormant | lake_area | — | none |
| `2:sakinaw-lake:the-body-of-water-known-as-bear-ba` | dormant | lake_area | — | none |
| `2:serpentine-river:downstream-of-168th-street` | dormant | variant | downstream of 168th Street at Bothwell Park | high |
| `2:serpentine-river:downstream-of-168th-street-at-both` | active | review_pending | downstream of 168th Street at Bothwell Park | high |
| `2:serpentine-river:within-23-m-downstream-of-the-floo` | dormant | needs_split | between dyke gates and 23 m below the Serpentine flood dam | high |
| `2:seymour-river:from-the-seymour-river-dam-to-the` | dormant | review_pending | downstream of Seymour Lake | medium |
| `2:stave-river:downstream-of-b-c-hydro-dam-to-the` | dormant | variant | between Hayward Lake and CPR railway bridge over the Stave (Ruskin) | high |
| `2:stave-river:downstream-of-b-c-hydro-dam-to-the~2` | active | needs_split | between Hayward Lake and CPR railway bridge over the Stave (Ruskin) | high |
| `2:stave-river:in-that-portion-of-the-stave-river` | dormant | side_channel | — | none |
| `2:stave-river:that-portion-of-the-stave-river-kn` | dormant | side_channel | — | none |
| `2:stave-river:the-ruskin-spawning-channel-on-the` | dormant | side_channel | — | none |
| `3:fraser-river:in-that-portion-of-the-fraser-rive` | active | needs_split | between confluence of Seton River and signs ~4 km below Lillooet | low |
| `3:fraser-river:mainstem-waters-of-the-fraser-rive` | dormant | review_pending | the whole water without its tributaries | medium |
| `3:south-thompson-river:from-the-green-can-buoy-near-outle` | active | needs_split | between Little Shuswap Lake and confluence of Campbell Creek | medium |
| `3:thompson-river:from-kamloops-lake-outlet-downstre` | dormant | variant | between Kamloops Lake and signs just below Goldpan Provincial Park | medium |
| `3:thompson-river:from-kamloops-lake-outlet-downstre~2` | active | needs_split | between Kamloops Lake and signs just below Goldpan Provincial Park | medium |
| `3:thompson-river:from-the-white-triangle-fishing-bo` | active | needs_split | between signs just below Goldpan Provincial Park and easterly border of the Skihist Ecological reserve along the Thompson River locat | medium |
| `5a:quesnel-lake:in-the-waters-of-horsefly-bay-loca` | dormant | lake_area | — | none |
| `5a:quesnel-lake:that-portion-of-quesnel-lake-known` | active | lake_area | — | none |
| `5a:quesnel-river:between-the-johnson-subdivision-br` | dormant | variant | between Johnson Subdivision Bridge and powerlines | high |
| `5a:quesnel-river:that-portion-of-the-quesnel-river` | active | needs_split | between Johnson Subdivision Bridge and powerlines | high |
| `5b:atnarko-river:downstream-of-the-fishing-boundary` | active | needs_split | downstream of signs near Tweedsmuir Park Lodge (Stuie) | medium |
| `5b:bella-coola-river-not-including-at:all-tributaries-to-the-bella-coola` | active | tributary_set | the whole water | medium |
| `5b:dean-river:from-signs-located-approx-500-m-up` | active | review_pending | between signs ~0.5 km above the canyon (500 m upstream) and signs ~100 m below the canyon (100 m downstream) | high |
| `5b:dean-river:from-the-confluence-with-crag-cree` | active | review_pending | between Crag Creek → Dean River and mouth | low |
| `5b:dean-river:upstream-of-the-confluence-with-cr` | active | review_pending | upstream of Crag Creek → Dean River | high |
| `5b:long-lake:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:babine-lake:babine-lake-excluding-tributaries` | dormant | lake_area | the whole water | medium |
| `6:babine-lake:babine-lake-excluding-tributaries~2` | dormant | lake_area | the whole water | medium |
| `6:babine-lake:babine-lake-excluding-tributaries~3` | active | lake_area | the whole water | medium |
| `6:babine-lake:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:babine-lake:within-a-400-m-radius-of-the-mouth` | dormant | lake_area | — | none |
| `6:babine-lake:within-a-400m-radius-of-the-mouth` | dormant | lake_area | — | none |
| `6:babine-river:from-the-nilkitkwa-river-confluenc` | dormant | review_pending | between Nilkitkwa River → Babine River and signs 80 m downstream of adult fish counting fence | medium |
| `6:babine-river:from-the-nilkitkwa-river-confluenc~2` | dormant | review_pending | downstream of Nilkitkwa River → Babine River | high |
| `6:bear-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:bish-creek:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:brim-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:bulkley-river:all-tributaries-of-the-bulkley-riv` | active | tributary_set | the whole water | medium |
| `6:bulkley-river:all-tributaries-of-the-bulkley-riv~2` | active | tributary_set | the whole water | medium |
| `6:bulkley-river:downstream-of-the-morice-river-con` | active | review_pending | downstream of Morice River → Bulkley River | high |
| `6:cranberry-river:including-tributaries-note-the-sec` | active | review_pending | the whole water with its tributaries | high |
| `6:dala-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:ecstall-river:upstream-of-fishing-boundary-signs` | active | needs_split | upstream of confluence of Johnston Creek | medium |
| `6:exchamsiks-river:upstream-of-highway-16-bridge` | active | needs_split | upstream of Hwy 16 bridge over the Exchamsiks | high |
| `6:exstew-river:upstream-of-highway-16-bridge` | active | needs_split | upstream of Hwy 16 bridge over the Exstew | high |
| `6:gitnadoix-river:upstream-of-powerline-crossing-nea` | active | needs_split | upstream of Gitnadoix powerline crossing near the mouth | low |
| `6:illiance-river:upstream-of-white-triangular-fishi` | active | needs_split | upstream of signs near the Illiance mouth | low |
| `6:ishkheenickh-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:kasiks-river:downstream-of-fishing-boundary-sig` | active | unknown_landmark | — | none |
| `6:kasiks-river:upstream-of-fishing-boundary-signs` | active | unknown_landmark | — | none |
| `6:kasiks-river:upstream-of-highway-16-bridge` | active | needs_split | upstream of Hwy 16 bridge over the Kasiks | high |
| `6:khutze:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:khutzeymateen-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:khyex-river:upstream-of-highway-16-bridge` | active | needs_split | upstream of Hwy 16 bridge over the Khyex | high |
| `6:kiltuish-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:kispiox-river:downstream-of-fishing-boundary-sig` | active | needs_split | downstream of signs near Kispiox River Resort | low |
| `6:kiteen-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:kitimat-river:downstream-of-highway-37-bridge` | active | review_pending | downstream of Hwy 37 bridge | high |
| `6:kitimat-river:on-west-bank-between-signs-at-kiti` | active | review_pending | between Kitimat Hatchery outfall — u/s sign and Kitimat Hatchery outfall — d/s sign | high |
| `6:kitimat-river:upstream-of-highway-37-bridge` | active | review_pending | upstream of Hwy 37 bridge | high |
| `6:kitsault-river:upstream-of-white-triangular-fishi` | active | needs_split | upstream of signs near the Kitsault mouth | low |
| `6:kitseguecla-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:kitsumkalum-lake:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:kitsumkalum-river:downstream-of-fishing-boundary-sig` | active | needs_split | downstream of signs below the Kitsumkalum lower canyon | medium |
| `6:kitsumkalum-river:downstream-of-railway-bridge` | active | needs_split | downstream of CN railway bridge over the Kitsumkalum | high |
| `6:kitsumkalum-river:upstream-of-fishing-boundary-signs` | active | needs_split | upstream of signs below the Kitsumkalum lower canyon | medium |
| `6:kitwanga-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:kloiya-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:kwinimass-river:downstream-of-lower-bridge-abutmen` | active | review_pending | downstream of lower bridge abutments | medium |
| `6:kwinimass-river:upstream-of-lower-bridge-abutments` | active | review_pending | upstream of lower bridge abutments | medium |
| `6:lakelse-river:above-logging-road-bridge-near-the` | active | needs_split | upstream of Whitebottom Rd bridge (Lakelse, near the mouth) | medium |
| `6:lakelse-river:below-logging-road-bridge-near-the` | active | needs_split | downstream of Whitebottom Rd bridge (Lakelse, near the mouth) | medium |
| `6:lakelse-river:downstream-of-the-cnr-bridge` | active | review_pending | downstream of CNR bridge | medium |
| `6:meziadin-lake:excluding-tributaries` | active | review_pending | the whole water without its tributaries | high |
| `6:meziadin-lake:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:meziadin-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:nakina-river:that-portion-of-the-nakina-river-f` | dormant | needs_split | between sign ~300 m above the Sloko confluence and sign ~1300 m above the Sloko confluence | medium |
| `6:nass-river:mainstem-waters-downstream-of-the` | dormant | review_pending | downstream of Meziadin River → Nass River | high |
| `6:nass-river:mainstem-waters-downstream-of-the~2` | active | variant | downstream of Meziadin River → Nass River | high |
| `6:nass-river:mainstem-waters-near-the-meziadin` | active | review_pending | between Meziadin River → Nass River and Hwy 37 bridge | medium |
| `6:nass-river:portions-from-the-canyon-city-brid` | active | needs_split | between Canyon City (Gitwinksihlkw) bridge and WSC 08DB001 station line (Nass above Shumal Creek) | high |
| `6:pinkut-creek:downstream-of-fishing-boundary-sig` | active | needs_split | downstream of signs 25 m below the Pinkut fish fence | medium |
| `6:quaal-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:redsand-lake:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:scotia-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:shegunia-river:between-fishing-boundary-signs-loc` | active | needs_split | between signs above the Shegunia logging road bridge and signs below the Shegunia logging road bridge | low |
| `6:sheldens-creek:upstream-of-white-triangular-fishi` | active | no_water | — | none |
| `6:skeena-river:from-lakelse-river-mouth-upstream` | dormant | needs_split | between confluence of Lakelse River and Skeena River Overpass (Dudley Little Bridge) at Ferry Island | high |
| `6:skeena-river:from-the-confluence-with-the-babin` | dormant | variant_of_placed | between CNR Railway Bridge at Terrace and gauge__08EB005 | high |
| `6:skeena-river:upstream-of-cnr-bridge-at-terrace` | dormant | variant_of_placed | between CNR Railway Bridge at Terrace and gauge__08EB005 | high |
| `6:stikine-river:including-tributaries-see-the-tahl` | active | review_pending | the whole water with its tributaries | high |
| `6:suskwa-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:sustut-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:swift-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:swift-river:including-tributaries-anglers-may` | dormant | review_pending | the whole water with its tributaries | high |
| `6:tahltan-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:taku-river:including-tributaries-see-the-naki` | active | review_pending | the whole water with its tributaries | high |
| `6:tatsatua-creek:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:tseax-river:downstream-of-nisga-a-highway-brid` | active | review_pending | downstream of Nass Road Bridge | medium |
| `6:tseax-river:upstream-of-nisga-a-highway-bridge` | active | review_pending | upstream of Nass Road Bridge | medium |
| `6:wahoo-river:including-tributaries` | active | review_pending | the whole water with its tributaries | high |
| `6:wilauks-creek:upstream-of-white-triangular-fishi` | active | needs_split | upstream of signs near the Wilauks mouth | low |
| `6:zymagotitz-river:upstream-of-highway-16-bridge` | active | needs_split | upstream of Hwy 16 bridge over the Zymagotitz | high |
| `6:zymoetz-river:note-section-of-river-from-highway` | dormant | needs_split | downstream of Hwy 16 bridge over the Zymoetz (Copper) | medium |
| `6:zymoetz-river:upstream-of-highway-16-bridge` | active | needs_split | upstream of Hwy 16 bridge over the Zymoetz (Copper) | high |
| `7:nechako-river:from-the-downstream-edge-of-the-fo` | active | review_pending | downstream of Foothills Boulevard Bridge in Prince George | high |
| `7:nechako-river:upstream-from-the-boundary-signs-a` | dormant | variant | downstream of Foothills Boulevard Bridge in Prince George | high |
| `8:mabel-lake:northerly-of-a-line-drawn-from-a-w` | active | lake_area | — | none |
| `8:osoyoos-lake:in-the-waters-in-the-portion-of-os` | dormant | lake_area | — | none |
| `8:osoyoos-lake:in-the-waters-in-the-portion-of-os~2` | active | lake_area | — | none |
| `8:shuswap-river:lower-50-m-upstream-of-the-trinity` | active | review_pending | between signs 50 m upstream of Trinity Bridge and signs 50 m downstream of Trinity Bridge | high |
| `8:shuswap-river:lower-upstream-from-white-triangul` | active | review_pending | between Mara Bridge and Mabel Lake | medium |
| `8:shuswap-river:middle-between-shuswap-falls-and-m` | dormant | variant | between Shuswap Falls (Wilsey Dam) and Mabel Lake | high |
| `8:shuswap-river:middle-between-shuswap-falls-and-m~2` | active | needs_split | between Shuswap Falls (Wilsey Dam) and Mabel Lake | high |
