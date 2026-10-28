# DFO split hunt (owner 2026-10-27)

Hand research for the DFO locations the matcher left with no suggestion (68) or one end open (15).
Every cut is a PROPOSAL: open the location in the review app's DFO mode — the hunted cuts show
with their confidence, description, sources and notes; **accept suggestion** pre-fills the
editor (the research travels in each new split's note); **Save extent** (your mark) is the only
write. Nothing touched splits.json, the catalogue or marks.jsonl. Input:
`handoff/PLAN/curation-review/dfo-split-hunt.json`; regenerate with
`python -m pipeline.regs.dfo_salmon.suggest`.

## Counts (91 locations)

| locations | high | medium | low | not found |
|---|---|---|---|---|
| 91 | 26 | 34 | 16 | 15 |

A location's confidence is its weakest end. Distinct hunted cuts: high 22, medium 14, low 17.

## Found

| location | extent | conf | cuts (conf · snap) |
|---|---|---|---|
| `1:cowichan-river:from-cnr-trestle-at-mile-70-2-to-s` | between CNR Bridge (mile 70.2) and Skutz Falls | high | Skutz Falls (high · 0 m) |
| `1:cowichan-river:from-the-white-triangle-boundary-s` | downstream of white triangle boundary sign located in Sandy Pool Regional Park | high | curated cuts only |
| `1:cowichan-river:including-tributaries-and-cowichan` | the whole water | medium | curated cuts only |
| `1:nanaimo-river:from-500-m-below-the-lower-power-l` | between 500 m below the power line at Bore Hole and Boulder Creek → Nanaimo River | medium | 500 m below the power line at Bore Hole (medium · 0 m) |
| `1:puntledge-river:from-the-weir-at-the-lower-hatcher` | between Puntledge lower hatchery weir and signs 75 m below the Puntledge lower hatchery weir | medium | Puntledge lower hatchery weir (high · 3 m); signs 75 m below the Puntledge lower hatchery weir (medium · 3 m) |
| `1:quatse-river:100m-upstream-and-downstream-of-th` | between 100 m above the Quatse River Hatchery weir and 100 m below the Quatse River Hatchery weir | low | 100 m above the Quatse River Hatchery weir (low · 36 m); 100 m below the Quatse River Hatchery weir (low · 36 m) |
| `1:san-juan-river:that-portion-of-the-san-juan-river` | between San Juan hatchery counting fence and signs 150 m below the San Juan hatchery fence | low | San Juan hatchery counting fence (low · 4 m); signs 150 m below the San Juan hatchery fence (low · 4 m) |
| `1:stamp-river:except-in-those-areas-and-times-li` | the whole water | medium | curated cuts only |
| `1:stamp-river:from-the-inlet-upstream-of-roberst` | between Robertson Creek Hatchery lagoon inlet and Robertson Creek Hatchery lagoon outlet | low | Robertson Creek Hatchery lagoon inlet (low · 1 m); Robertson Creek Hatchery lagoon outlet (low · 5 m) |
| `1:stamp-river:from-the-powerline-crossing-downst` | between Stamp River powerline crossing (BC Hydro 1L127) and Robertson Creek Hatchery lagoon inlet | low | Stamp River powerline crossing (BC Hydro 1L127) (medium · 0 m); Robertson Creek Hatchery lagoon inlet (low · 1 m) |
| `1:stamp-river:from-the-southern-portion-of-the-r` | between Robertson Creek Hatchery lagoon inlet and Robertson Creek Hatchery lagoon outlet | low | Robertson Creek Hatchery lagoon inlet (low · 1 m); Robertson Creek Hatchery lagoon outlet (low · 5 m) |
| `1:stamp-river:hatchery-lagoon-from-boundary-sign` | between Robertson Creek Hatchery lagoon inlet and Stamp River cable car crossing | low | Robertson Creek Hatchery lagoon inlet (low · 1 m); Stamp River cable car crossing (low · 4 m) |
| `1:stamp-river:hatchery-lagoon-from-fishing-bound` | between Robertson Creek Hatchery lagoon inlet and Robertson Creek Hatchery lagoon outlet | low | Robertson Creek Hatchery lagoon inlet (low · 1 m); Robertson Creek Hatchery lagoon outlet (low · 5 m) |
| `1:stamp-river:powerline-crossing-from-boundary-s` | between Stamp River powerline crossing (BC Hydro 1L127) and Robertson Creek Hatchery lagoon inlet | low | Stamp River powerline crossing (BC Hydro 1L127) (medium · 0 m); Robertson Creek Hatchery lagoon inlet (low · 1 m) |
| `2:alouette-river-and-tributaries:downstream-of-a-line-between-2-fis` | downstream of Allco Park boundary signs (Allco hatchery fence) | medium | Allco Park boundary signs (Allco hatchery fence) (medium · 7 m) |
| `2:alouette-river-and-tributaries:downstream-of-a-line-between-two-f` | downstream of Allco Park boundary signs (Allco hatchery fence) | medium | Allco Park boundary signs (Allco hatchery fence) (medium · 7 m) |
| `2:alouette-river-and-tributaries:north-alouette-and-tributaries` | the whole water | high | curated cuts only |
| `2:alouette-river-and-tributaries:upstream-of-the-216th-street-bridg` | between 216 Street bridge and Allco Park boundary signs (Allco hatchery fence) | medium | Allco Park boundary signs (Allco hatchery fence) (medium · 7 m) |
| `2:birkenhead-river:from-the-birkenhead-bridge-located` | between Birkenhead Bridge (Portage Road) and Birkenhead canyon (foot) | medium | Birkenhead Bridge (Portage Road) (high · 1 m); Birkenhead canyon (foot) (medium · 1 m) |
| `2:brunette-river:downstream-of-cariboo-road` | downstream of Cariboo Road bridge | high | Cariboo Road bridge (high · 1 m) |
| `2:chapman-creek:upstream-of-tidal-water-fishing-bo` | between Chapman Creek Hwy 101 bridge (tidal signs below it) and 100 m below the Chapman Creek falls (below the power line) | low | Chapman Creek Hwy 101 bridge (tidal signs below it) (low · 0 m); 100 m below the Chapman Creek falls (below the power line) (low · 19 m) |
| `2:fraser-river:from-the-downstream-side-of-the-cp` | between the CPR Bridge at Mission and Highway 1 bridge at Hope (Bill Hartley / Fraser-Hope) | high | Highway 1 bridge at Hope (Bill Hartley / Fraser-Hope) (high · 114 m) |
| `2:fraser-river:from-the-downstream-side-of-the-cp~2` | between the CPR Bridge at Mission and Alexandra Bridge (Hwy 1) | medium | Alexandra Bridge (Hwy 1) (medium · 77 m) |
| `2:fraser-river:from-the-highway-1-bridge-at-hope` | between Highway 1 bridge at Hope (Bill Hartley / Fraser-Hope) and Alexandra Bridge (Hwy 1) | medium | Highway 1 bridge at Hope (Bill Hartley / Fraser-Hope) (high · 114 m); Alexandra Bridge (Hwy 1) (medium · 77 m) |
| `2:fraser-river:mainstem-waters-from-the-downstrea` | between the CPR Bridge at Mission and Highway 1 bridge at Hope (Bill Hartley / Fraser-Hope) | high | Highway 1 bridge at Hope (Bill Hartley / Fraser-Hope) (high · 114 m) |
| `2:fraser-river:upstream-from-the-highway-1-bridge` | upstream of Highway 1 bridge at Hope (Bill Hartley / Fraser-Hope) | high | Highway 1 bridge at Hope (Bill Hartley / Fraser-Hope) (high · 114 m) |
| `2:harrison-river:from-the-highway-7-bridge-downstre` | downstream of Hwy 7 bridge over the Harrison | high | Hwy 7 bridge over the Harrison (high · 165 m) |
| `2:harrison-river:from-the-outlet-of-harrison-lake-d` | between Harrison Lake and Hwy 7 bridge over the Harrison | high | Hwy 7 bridge over the Harrison (high · 165 m) |
| `2:inch-creek:from-the-hatchery-outlet-to-the-co` | downstream of Inch Creek Hatchery outlet | medium | Inch Creek Hatchery outlet (medium · 396 m) ⚠ |
| `2:little-campbell-river:downstream-of-a-line-between-2-fis` | between two white triangular fishing boundary signs (200 m upstream) and pedestrian bridge at the foot of Stayte Road | medium | curated cuts only |
| `2:nicomekl-river:within-23-m-downstream-of-the-floo` | between dyke gates and 23 m below the Nicomekl flood dam | high | 23 m below the Nicomekl flood dam (high · 1 m) |
| `2:nicomen-including-dewdney-slough:from-the-confluence-of-siddle-bell` | downstream of Siddle (Bell's) Creek mouth | high | Siddle (Bell's) Creek mouth (high · 21 m) |
| `2:nicomen-including-dewdney-slough:from-the-highway-7-bridge-at-dewdn` | downstream of Hwy 7 Nicomen Slough Bridge at Dewdney | high | Hwy 7 Nicomen Slough Bridge at Dewdney (high · 17 m) |
| `2:serpentine-river:within-23-m-downstream-of-the-floo` | between dyke gates and 23 m below the Serpentine flood dam | high | 23 m below the Serpentine flood dam (high · 1 m) |
| `2:seymour-river:from-the-seymour-river-dam-to-the` | downstream of Seymour Lake | medium | curated cuts only |
| `2:stave-river:downstream-of-b-c-hydro-dam-to-the` | between Hayward Lake and CPR railway bridge over the Stave (Ruskin) | high | CPR railway bridge over the Stave (Ruskin) (high · 7 m) |
| `2:stave-river:downstream-of-b-c-hydro-dam-to-the~2` | between Hayward Lake and CPR railway bridge over the Stave (Ruskin) | high | CPR railway bridge over the Stave (Ruskin) (high · 7 m) |
| `3:fraser-river:in-that-portion-of-the-fraser-rive` | between confluence of Seton River and signs ~4 km below Lillooet | low | signs ~4 km below Lillooet (low · 9 m) |
| `3:south-thompson-river:from-the-green-can-buoy-near-outle` | between Little Shuswap Lake and confluence of Campbell Creek | medium | curated cuts only |
| `3:thompson-river:from-kamloops-lake-outlet-downstre` | between Kamloops Lake and signs just below Goldpan Provincial Park | medium | signs just below Goldpan Provincial Park (medium · 29 m) |
| `3:thompson-river:from-kamloops-lake-outlet-downstre~2` | between Kamloops Lake and signs just below Goldpan Provincial Park | medium | signs just below Goldpan Provincial Park (medium · 29 m) |
| `3:thompson-river:from-the-white-triangle-fishing-bo` | between signs just below Goldpan Provincial Park and easterly border of the Skihist Ecological reserve along the Thompson River locat | medium | signs just below Goldpan Provincial Park (medium · 29 m) |
| `5b:atnarko-river:downstream-of-the-fishing-boundary` | downstream of signs near Tweedsmuir Park Lodge (Stuie) | medium | signs near Tweedsmuir Park Lodge (Stuie) (medium · 260 m) |
| `5b:bella-coola-river-not-including-at:all-tributaries-to-the-bella-coola` | the whole water | medium | curated cuts only |
| `6:babine-lake:babine-lake-excluding-tributaries` | the whole water | medium | curated cuts only |
| `6:babine-lake:babine-lake-excluding-tributaries~2` | the whole water | medium | curated cuts only |
| `6:babine-lake:babine-lake-excluding-tributaries~3` | the whole water | medium | curated cuts only |
| `6:babine-river:from-the-nilkitkwa-river-confluenc` | between Nilkitkwa River → Babine River and signs 80 m downstream of adult fish counting fence | medium | curated cuts only |
| `6:bulkley-river:all-tributaries-of-the-bulkley-riv` | the whole water | medium | curated cuts only |
| `6:bulkley-river:all-tributaries-of-the-bulkley-riv~2` | the whole water | medium | curated cuts only |
| `6:exchamsiks-river:upstream-of-highway-16-bridge` | upstream of Hwy 16 bridge over the Exchamsiks | high | Hwy 16 bridge over the Exchamsiks (high · 42 m) |
| `6:exstew-river:upstream-of-highway-16-bridge` | upstream of Hwy 16 bridge over the Exstew | high | Hwy 16 bridge over the Exstew (high · 38 m) |
| `6:gitnadoix-river:upstream-of-powerline-crossing-nea` | upstream of Gitnadoix powerline crossing near the mouth | low | Gitnadoix powerline crossing near the mouth (low · 324 m) ⚠ |
| `6:illiance-river:upstream-of-white-triangular-fishi` | upstream of signs near the Illiance mouth | low | signs near the Illiance mouth (low · 0 m) |
| `6:kasiks-river:upstream-of-highway-16-bridge` | upstream of Hwy 16 bridge over the Kasiks | high | Hwy 16 bridge over the Kasiks (high · 30 m) |
| `6:khyex-river:upstream-of-highway-16-bridge` | upstream of Hwy 16 bridge over the Khyex | high | Hwy 16 bridge over the Khyex (high · 48 m) |
| `6:kispiox-river:downstream-of-fishing-boundary-sig` | downstream of signs near Kispiox River Resort | low | signs near Kispiox River Resort (low · 116 m) |
| `6:kitsault-river:upstream-of-white-triangular-fishi` | upstream of signs near the Kitsault mouth | low | signs near the Kitsault mouth (low · 0 m) |
| `6:kitsumkalum-river:downstream-of-fishing-boundary-sig` | downstream of signs below the Kitsumkalum lower canyon | medium | signs below the Kitsumkalum lower canyon (medium · 2 m) |
| `6:kitsumkalum-river:downstream-of-railway-bridge` | downstream of CN railway bridge over the Kitsumkalum | high | CN railway bridge over the Kitsumkalum (high · 27 m) |
| `6:kitsumkalum-river:upstream-of-fishing-boundary-signs` | upstream of signs below the Kitsumkalum lower canyon | medium | signs below the Kitsumkalum lower canyon (medium · 2 m) |
| `6:lakelse-river:above-logging-road-bridge-near-the` | upstream of Whitebottom Rd bridge (Lakelse, near the mouth) | medium | Whitebottom Rd bridge (Lakelse, near the mouth) (medium · 14 m) |
| `6:lakelse-river:below-logging-road-bridge-near-the` | downstream of Whitebottom Rd bridge (Lakelse, near the mouth) | medium | Whitebottom Rd bridge (Lakelse, near the mouth) (medium · 14 m) |
| `6:nakina-river:that-portion-of-the-nakina-river-f` | between sign ~300 m above the Sloko confluence and sign ~1300 m above the Sloko confluence | medium | sign ~300 m above the Sloko confluence (medium · 0 m); sign ~1300 m above the Sloko confluence (medium · 0 m) |
| `6:nass-river:portions-from-the-canyon-city-brid` | between Canyon City (Gitwinksihlkw) bridge and WSC 08DB001 station line (Nass above Shumal Creek) | high | Canyon City (Gitwinksihlkw) bridge (high · 35 m); WSC 08DB001 station line (Nass above Shumal Creek) (high · 122 m) |
| `6:pinkut-creek:downstream-of-fishing-boundary-sig` | downstream of signs 25 m below the Pinkut fish fence | medium | signs 25 m below the Pinkut fish fence (medium · 5 m) |
| `6:shegunia-river:between-fishing-boundary-signs-loc` | between signs above the Shegunia logging road bridge and signs below the Shegunia logging road bridge | low | signs above the Shegunia logging road bridge (low · 1 m); signs below the Shegunia logging road bridge (low · 1 m) |
| `6:skeena-river:from-lakelse-river-mouth-upstream` | between confluence of Lakelse River and Skeena River Overpass (Dudley Little Bridge) at Ferry Island | high | Skeena River Overpass (Dudley Little Bridge) at Ferry Island (high · 143 m) |
| `6:tseax-river:downstream-of-nisga-a-highway-brid` | downstream of Nass Road Bridge | medium | curated cuts only |
| `6:tseax-river:upstream-of-nisga-a-highway-bridge` | upstream of Nass Road Bridge | medium | curated cuts only |
| `6:wilauks-creek:upstream-of-white-triangular-fishi` | upstream of signs near the Wilauks mouth | low | signs near the Wilauks mouth (low · 0 m) |
| `6:zymagotitz-river:upstream-of-highway-16-bridge` | upstream of Hwy 16 bridge over the Zymagotitz | high | Hwy 16 bridge over the Zymagotitz (high · 26 m) |
| `6:zymoetz-river:note-section-of-river-from-highway` | downstream of Hwy 16 bridge over the Zymoetz (Copper) | medium | Hwy 16 bridge over the Zymoetz (Copper) (high · 27 m) |
| `6:zymoetz-river:upstream-of-highway-16-bridge` | upstream of Hwy 16 bridge over the Zymoetz (Copper) | high | Hwy 16 bridge over the Zymoetz (Copper) (high · 27 m) |
| `8:shuswap-river:middle-between-shuswap-falls-and-m` | between Shuswap Falls (Wilsey Dam) and Mabel Lake | high | Shuswap Falls (Wilsey Dam) (high · 22 m) |
| `8:shuswap-river:middle-between-shuswap-falls-and-m~2` | between Shuswap Falls (Wilsey Dam) and Mabel Lake | high | Shuswap Falls (Wilsey Dam) (high · 22 m) |

## Still not found

| location | why |
|---|---|
| `2:sakinaw-lake:easterly-of-a-line-drawn-from-a-bo` | a part of a lake (a line across it): not a cut on a stream line. The notice's own boundary sign is at 49°41.50'N 123°58.45'W (49.69167,-123.97417) — curate as a lake area by hand. Owner rule (lake-splits-deferred): a lake divided into closure areas is deferred — curate later as a lake split / sign-zone area. |
| `2:sakinaw-lake:the-body-of-water-known-as-bear-ba` | Bear Bay, a part of the lake east of a north–south line from 49°40.05'N 124°02.13'W (49.66750,-124.03550): a lake area, not a line cut — curate by hand. Owner rule (lake-splits-deferred): a lake divided into closure areas is deferred — curate later as a lake split / sign-zone area. |
| `2:stave-river:in-that-portion-of-the-stave-river` | Ruskin + Northrop spawning channels — both side channels off the FWA Stave (Ruskin: OSM 1485091713, inlet 49.1950,-122.4085; Northrop: OSM 72823766, intake 49.1938,-122.4120, joins the mainstem ≈ 49.1892,-122.4121). |
| `2:stave-river:that-portion-of-the-stave-river-kn` | the Ruskin Spawning Channel is a side channel on the east bank, not on the FWA Stave line. Mapped (OSM 1485091713 'Ruskin Channel'): inlet near the dam 49.1950,-122.4085, boat-ramp crossing ≈ 49.1921,-122.4070, joins the Stave ≈ 49.1864,-122.4092 — needs an added channel or an area, by hand. |
| `2:stave-river:the-ruskin-spawning-channel-on-the` | same Ruskin Spawning Channel (side channel, not on the FWA Stave): inlet 49.1950,-122.4085, boat-ramp crossing ≈ 49.1921,-122.4070 (OSM 1485091713). |
| `5a:quesnel-lake:in-the-waters-of-horsefly-bay-loca` | Horsefly Bay inside a line of signs across its entrance: a lake area, not a stream cut — curate by hand (no published coordinate found). Owner rule (lake-splits-deferred): a lake divided into closure areas is deferred — curate later as a lake split / sign-zone area. |
| `5a:quesnel-lake:that-portion-of-quesnel-lake-known` | Horsefly Bay (lake area) — as above. Owner rule (lake-splits-deferred): a lake divided into closure areas is deferred — curate later as a lake split / sign-zone area. |
| `6:babine-lake:within-a-400-m-radius-of-the-mouth` | a 400 m radius around the Pinkut Creek mouth on the lake: a lake area — curate by hand. Owner rule (lake-splits-deferred): a lake divided into closure areas is deferred — curate later as a lake split / sign-zone area. |
| `6:babine-lake:within-a-400m-radius-of-the-mouth` | a 400 m radius around the Pinkut Creek mouth: a lake area — curate by hand. Owner rule (lake-splits-deferred): a lake divided into closure areas is deferred — curate later as a lake split / sign-zone area. |
| `6:kasiks-river:downstream-of-fishing-boundary-sig` | the Kasiks 'Upper Pool' — not found (as above). |
| `6:kasiks-river:upstream-of-fishing-boundary-signs` | 'the Upper Pool' is named nowhere public (DFO pages, FWA, FISS: no lake or falls in the lower 12 km). Ask DFO Terrace or a local guide. |
| `6:sheldens-creek:upstream-of-white-triangular-fishi` | the water is not matched to the registry (FWA has no 'Sheldens Creek'). Best lead: the creek into Sheldens Bay, Moresby Island (mouth 53.14874,-131.76677, FWA code 950-016367); Spur 19 (FTEN 'SP 19(1) TO SHELDONS') crosses it ≈ 53.12968,-131.78224, ~3.1 km above the mouth (medium). Match the water first (T164 dossier), then this cut. |
| `8:mabel-lake:northerly-of-a-line-drawn-from-a-w` | lake areas (a line from Mabel Lake Provincial Park's north edge to a western point; signs ~1 km from Wap Creek) — curate by hand. Owner rule (lake-splits-deferred): a lake divided into closure areas is deferred — curate later as a lake split / sign-zone area. |
| `8:osoyoos-lake:in-the-waters-in-the-portion-of-os` | Osoyoos Lake north of the Hwy 3 bridge (the North Basin): a lake part — the bridge (Hwy 3, Osoyoos narrows) is a line across the lake, curate as a lake split by hand. Owner rule (lake-splits-deferred): a lake divided into closure areas is deferred — curate later as a lake split / sign-zone area. |
| `8:osoyoos-lake:in-the-waters-in-the-portion-of-os~2` | same North Basin lake part — curate by hand. Owner rule (lake-splits-deferred): a lake divided into closure areas is deferred — curate later as a lake split / sign-zone area. |

## Synopsis-side cuts (owner 2026-10-27: guessed LOW)

Not DFO locations — the synopsis rules' missing cuts (split-name-audit.md); for the owner to place.

- **r2:chapman_creek.r2** — "from the falls to 100 m downstream (falls are located approximately 550 m upstream of the power line crossing)": between the cascade below the power line (49.4639,-123.7265) and 100 m below it — LOW (owner 2026-10-27). Sources: https://www.openstreetmap.org/way/258997740 ; https://www.vancouvertrails.com/trails/chapman-falls/
- **r1:deena_creek.r1** — "upstream of fishing boundary signs at second bridge approximately 5 km upstream of the tidal boundary": upstream_of the 2nd road crossing on Deena Creek, FWA measure 8.8 km (53.11879,-132.20619) — LOW. Crossings: 1.5 km (53.1406,-132.1451), 8.8 km, 10.3 km (53.1176,-132.2266), 14.0 km; the estuary runs to ~3.5 km, so the 2nd bridge is ~5 km above it. Sources: https://openmaps.gov.bc.ca/geo/pub/wfs (DataBC FWA streams blk 360863736; DRA / FTEN roads)
- **r1:mamin_river.r3** — "upstream of fishing boundary signs on third bridge approximately 10 km upstream of the tidal boundary": upstream_of the 3rd road crossing on the Mamin, FWA measure 16.7 km (53.53846,-132.32589) — LOW. Crossings: 4.6 km (New Mainline, 53.6016,-132.2910), 11.2 km (Mamin Main, 53.5681,-132.3263), 16.7 km, 28.2 km; '10 km above tidal' fits only if the tidal boundary is ~6–7 km up. Sources: https://openmaps.gov.bc.ca/geo/pub/wfs (DataBC FWA streams; DRA / FTEN roads)
