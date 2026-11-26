# Unmatched lakes — research (2026-10-28)

Research only. Nothing is bound. The owner accepts a lead with a mark. The machine-readable copies are:
- `t89-awaiting-water.json`: each proposal's `lead` text is rewritten and a `research` block is added, so the review app plots the new wbk.
- `unmatched-research.json`: the Kootenay pointer rows.

## Scope: the 19 `not_found: true` rows in overrides.json

| group | rows | treatment |
|---|---|---|
| T89 awaiting water | 6: Frog, Hidden, Secret, Square (5-6); Redfern (5-15); Squirrel (R6 6-1) | researched below |
| Intentional duplicates / cross-lists | 8: R5 copies of the MU 6-1 lakes (Toms, Basalt, Gatcho, Naglico, Pettry, Chipmunk, Squirrel), Endako R. (7A) | skipped (R5 Squirrel shares the R6 row's verdict) |
| Kootenay pointer rows (no rules of their own) | 5: Arrow Lakes, Arrow Lakes' tribs, Seven Mile Reservoir, Seven Mile Res. tribs, Waneta Res. tribs | identified; no binding needed |

## Summary

| entry | verdict | FWA lake | confidence | rules it would bind |
|---|---|---|---|---|
| r5:hidden_lake@5-6 | **found**: unnamed lake, alias "Hidden" | wbk 329021830 (01338ATNA, 24 ha, -125.6081 52.1129) | high | No Ice Fishing; trout quota 1; bait ban; single barbless |
| r5:secret_lake@5-6 | **found**: unnamed lake, alias "Secret" | wbk 329021850 (01370ATNA, 17.5 ha, -125.6168 52.0999) | high | same 4 |
| r5:square_lake@5-6 | **found**: Whitton Lake (local name "Square") | wbk 329021786 = registry `wbk:329021786`, gnis 39187 (01503ATNA, 238 ha) | high | trout quota 2; bait ban; single barbless |
| r5:frog_lake@5-6 | unresolved | weak: wbk 328962870, 329021897 or 329021982 (Stewart Lake drainage) | low | same 4 as Hidden |
| r5:redfern_lake@5-15 | unresolved | weak: wbk 329354762 (head of Ruth Redfern Cr.), but FISS calls it "Goat Lake" | low | trout quota 2; bait ban; single barbless |
| r6:squirrel_lake@6-1 | unresolved | none | none | trout quota 2; bait ban; single barbless |
| Arrow Lakes (+ tribs) | pointer | Upper Arrow wbk:328961689 + Lower Arrow wbk:328961720 | high | none (cross-reference) |
| Seven Mile Res. (+ tribs), Waneta Res. tribs | pointer | Pend d'Oreille River (the river row says it "Includes the reservoirs behind Waneta Dam and Seven Mile Dam") | high | none (cross-reference) |

Fix path for the three found lakes: write a `name_variants.json` record for each (book name, REGION 5, MU 5-6, mapped to the wbk), rebuild the registry, then shrink `AWAITING_WATER_ROWS`. This is the T89 `how_to_fix`. The book has no Whitton Lake row, so Square → Whitton collides with nothing.

## Main source

The province's FISS data on openmaps WFS was the key source. Most of it was missing from earlier searches:
- `WHSE_FISH.FISS_LAKE_SURVEYS_SVW` has a `SOURCE_REF` survey title that carries local names, e.g. "Reconnaissance Survey of Unnamed (Secret) Lake".
- `WHSE_FISH.FISS_FISH_OBSRVTN_PNT_SP` has the same titles.
- `WHSE_FISH.ACAT_REPORT_POINT_PUB_SVW` holds the EcoCat report titles.

Each record carries the 50K waterbody identifier, e.g. `01338ATNA`. This equals the FWA lake's `WATERBODY_KEY_50K` + `WATERSHED_GROUP_CODE_50K`, so the match is exact rather than "nearest".

Query used: `https://openmaps.gov.bc.ca/geo/pub/wfs?service=WFS&version=2.0.0&request=GetFeature&typeName=WHSE_FISH.FISS_LAKE_SURVEYS_SVW&outputFormat=json&CQL_FILTER=WATERBODY_IDENTIFIER LIKE '%ATNA'`

In 1997, AIM Ecological Consultants surveyed a set of lakes south and west of Charlotte Lake (MU 5-6) under their local names:
- Cowboy (Maydoe)
- Banana
- Crazy Bear (Ginny)
- Whitton (Square)
- Un-named (Hidden)
- Unnamed (Secret)
- Unnamed (No Name), which is the registry's "Cruise Lake" wbk 329021826
- Cohen
- Stewart

The 5-6 rows in the book follow that list almost one for one.

## Evidence

### Hidden Lake (5-6): high
- FISS lake survey, BC Lakes Database survey_id 58 (Region 5 Williams Lake, 1993): "UNTITLED REPORT: LIMNOLOGY DATA FOR HIDDEN LAKE". WATERBODY_IDENTIFIER 01338ATNA, GAZETTED_NAME null.
- FDIS rchsrvy_id 30392 (AIM, 1997-09-26): "Project ID/Name: 495/Reconnaissance Survey of Un-named (Hidden) Lake - 1997". Same id, 01338ATNA.
- EcoCat 32595 "Limnology Sample - Unnamed (Hidden) (01338ATNA)": https://a100.gov.bc.ca/pub/acat/public/viewReport.do?reportId=32595
- EcoCat 32579: https://a100.gov.bc.ca/pub/acat/public/viewReport.do?reportId=32579
- FWA: WATERBODY_KEY 329021830 has 50K key 1338 / ATNA, 24.1 ha, and sits inside MU 5-6. It drains to Stewart Lake (50K WSC 910-290700-99900-80000-0660).
- The registry has no Hidden Lake in 5-6. The 5-15 Hidden Lake (wbk 329354780, 00697MAHD) is a different water.

### Secret Lake (5-6): high
- FDIS rchsrvy_id 33227 (AIM, 1997-09-27): "Project ID/Name: 523/Reconnaissance Survey of Unnamed (Secret) Lake - 1997", 01370ATNA.
- EcoCat 49174 "A Reconnaissance Survey of Unnamed Lake Alis Secret". Its attachments include bathymetric maps titled "Secret Lake", "Secret Lake 2" and "Secret Lake 3": https://a100.gov.bc.ca/pub/acat/public/viewReport.do?reportId=49174
- FWA: WATERBODY_KEY 329021850 has 50K key 1370 / ATNA, 17.5 ha. It is on the same stream as Hidden, 1.4 km south of it.

### Square Lake (5-6): high
- FDIS rchsrvy_id 27660 (AIM, 1997-09-23): "Project ID/Name: 522/Reconnaissance Survey of Whitton (Square) Lake - 1997". 01503ATNA, GAZETTED_NAME WHITTON LAKE.
- EcoCat 33962 "A Reconnaissance Survey of Whitton Lake, 1997 01503ATNA": https://a100.gov.bc.ca/pub/acat/public/viewReport.do?reportId=33962
- Registry: `wbk:329021786` Whitton Lake, mus [5-6], gnis 39187.

### Frog Lake (5-6): unresolved, low
- No FISS, EcoCat, BCGNIS or web record puts a Frog Lake anywhere near MU 5-6.
  - BC-wide FROG hits are all elsewhere: Vancouver Island 00284SALM / 00767SALM, Thompson LNTH / LNIC, and Frog River FROG.
- Frog shares the exact rule set of Hidden and Secret (No Ice Fishing; quota 1). Both of those are AIM-era lakes in the Stewart Lake drainage, so Frog is probably another lake there.
- Unnamed lakes in that drainage with 1994 Region 5 "LAKE SURVEY DATA" records:
  - wbk 328962870 (01256ATNA, 10.9 ha, -125.4608 52.1781)
  - wbk 329021897 (01267ATNA, 8.6 ha)
  - wbk 329021982 (01272ATNA, 4.4 ha)
- None of these is named. Do not bind on this.
- A next step that could work: the 1994 survey forms, or the 11 MB scanned EcoCat 49174 report (it could not be OCR'd here). Alternatively, ask Region 5 (Williams Lake) fisheries.

### Redfern Lake (5-15): unresolved, low
- BCGNIS has a single Redfern feature in the Cariboo: Ruth Redfern Creek (names/22824, -120.583 52.07). The only Redfern Lake (names/23417) is in Redfern-Keily Park, MU 7-42.
- Ruth Redfern Creek flows through Deception Lake (wbk 329354827). Deception has its own 5-15 row with the identical rules. The creek rises at the unnamed lake wbk 329354762 (00034MAHD, 33 ha).
  - FISS (survey_id 5484) and EcoCat 51923 call that lake "UNNAMED LAKE (GOAT LAKE)". That argues against Redfern.
- Every lake survey in the Canim/Deception/McNeil block was checked (Katherine, Skinny, Goat, No Name, Hidden, Elkin file, …). None says Redfern.
- The book may have coined the name from the creek, or it may be a typo.

### Squirrel Lake (6-1): unresolved
- BC has one Squirrel Lake (BCGNIS 39822, -120.765 51.896), and it is in MU 5-1. It is the registry's Paddy/Squirrel Lake (wbk 329354753; WSA map 01446MAHD; FISS 1987 "A Reconnaissance Survey of Squirrel Lake"). Paddy Lake has its own 5-1 row, so it is not this entry.
- The other 6-1 rows (Chipmunk, Toms, Pettry, Naglico, Basalt) were all identified from the 1993 Region 5 winter-limnology series in EUCL. That series also has unnamed "Lake #19–#25" near Chipmunk/Toms, but no record names Squirrel.
- Also checked, with no hit:
  - every FISS lake record from 52.88 to 53.2N, 124.9 to 125.9W;
  - the EcoCat EUCL, Kluskus and Euchiniko titles;
  - the Eliguk Lake Lodge and Furthermost Outpost sites.

### Kootenay pointer rows: identified, no binding needed
- ARROW LAKES ("See Upper Arrow Lake") is Upper Arrow wbk:328961689 plus Lower Arrow wbk:328961720 (BCGNIS 43779 "Arrow Lakes", recorded). Both registry items carry the variant "Arrow Reservoir". The tributaries row points to both lakes' tributary rows.
- SEVEN MILE RESERVOIR, its tributaries, and WANETA RESERVOIR'S TRIBUTARIES all point to the Pend d'Oreille River. That row says it "Includes the reservoirs behind Waneta Dam and Seven Mile Dam".
  - There is no FWA lake polygon or BCGNIS name for Seven Mile Reservoir.
  - The registry's "Waneta Reservoir" (wbk:329262640) is only a 19 ha manmade polygon at the border.

## Searched (all entries)
- FISS lake surveys and fish observations, BC-wide, by name.
- Every record in ATNA, KLIN and EUCL.
- Bbox sweeps around Ruth Redfern Creek and Eliguk.
- EcoCat titles, by name and by watershed group.
- BC Geographical Names, all statuses.
- WSA bathymetry index (`data/source/wsa_bathymetry_maps.csv`).
- `name_variants.json`.
- FWA lakes and streams (`bc_fisheries_data.gpkg`).
- Web searches (standard and extended): lodges, Backroad Mapbooks, older synopsis copies.
