# Locators to curate — regulation reference

*Companion to `complex_regulations.md`, organized for MANUAL POINT CURATION. Every regulation whose
TITLE or any specific rule needs a spatial locator, grouped by the **split anchor kind** you would hand-author
in `stream_sections/splits.json`. Generated from `output/pipeline/matching/match_table.json` (names) +
`output/pipeline/parsing/synopsis_parsed.json` (rules), 1:1 by index. 622 distinct locator strings across
360 regulations.*

## How to read this
- **Anchor kind** = the `anchor.type` (or new operator) that expresses this locator. `point`/`confluence`/`line`/`lake` exist today; `area_boundary`, `lake_io`, and `buffer` are Phase-5 additions (see `docs/15`).
- **src** = `name` (title / title parenthetical), `entry` (entry_location_text), `rule` (a rule location_text), `except` (a rule exception clause).
- Work top-down: `coordinate` rows are turnkey (coords printed); `falls`/`dam`/`bridge` rows resolve against obstacle/infra points; `map_or_vague` rows have no clean anchor and need a human call.
- One row per distinct (regulation, locator string). A single reg can appear in several buckets (its rules differ). Buckets are assigned by a priority classifier — skim adjacent buckets for edge cases.

## Summary — count by anchor kind

| Anchor kind | Strings | Maps to |
|---|--:|---|
| `coordinate` | 5 | point (exact coord — easiest) |
| `falls_canyon_obstacle` | 81 | point via obstacles layer (FISS_OBSTACLES) |
| `dam_weir_fence` | 43 | point (infrastructure) |
| `bridge_road_km` | 116 | point (bridge/road/km marker) |
| `confluence_tributary` | 127 | confluence (tributary mouth) |
| `lake_reach` | 58 | lake (often already split) |
| `lake_inlet_outlet` | 5 | NEW lake_io op (lake_inlets ∪ lake_outlets) |
| `radius_buffer` | 23 | NEW buffer op (point+radius) |
| `line_between_signs` | 31 | line (author 2 endpoints) / area for lakes |
| `area_park_polygon` | 18 | area_boundary (polygon) |
| `boundary_signs_generic` | 12 | point (locate signs from map) |
| `except_negative` | 19 | EXCEPT set-difference / negative member |
| `map_or_vague` | 26 | manual / map-only (no clean anchor) |
| `other_reach` | 58 | reach (generic) |

## coordinate  (5)  →  point (exact coord — easiest)

| Regulation | R/MU | src | locator text |
|---|---|---|---|
| ALOUETTE RIVER | REGION 2 2-8 | rule | upstream of the fishing boundary signs located at 49° 14.790'N and 122° 32.080'W, near the southern boundary (chain-link |
| GOLD RIVER | REGION 1 1-9 | rule | between the cascade falls (located approximately 6.5 km upstream of Muchalat Inlet, UTM 709137E, 5512420N) and fishing b |
| MORICE RIVER | REGION 6 6-9 | rule | from fishing boundary signs near outlet of Morice Lake (UTM: 602539.02E, 5997165.96N) to the fishing boundary signs appr |
| MORICE RIVER | REGION 6 6-9 | rule | from fishing boundary signs approximately 2 km downstream (the "Dunes", UTM: 603741.00E, 5998628.00N) of Morice Lake to  |
| SAKINAW LAKE | REGION 2 2-5 | rule | easterly of a line drawn from a boundary sign located at the north side of the Sakinaw Lake boat launch southwesterly to |

## falls_canyon_obstacle  (81)  →  point via obstacles layer (FISS_OBSTACLES)

| Regulation | R/MU | src | locator text |
|---|---|---|---|
| AMOR DE COSMOS CREEK | REGION 1 1-10 | rule | from upper falls downstream 1 km to (Bear River) logging road bridge 3 km from tidewater |
| AMOR DE COSMOS CREEK | REGION 1 1-10 | rule | from mouth to falls about 4 km upstream |
| ASH RIVER | REGION 1 1-7 | rule | from Dickson Lake to signs 200 m downstream of Lanternman Falls |
| ASH RIVER | REGION 1 1-7 | rule | from Dickson Falls downstream 30 m to signs |
| ATNARKO/BELLA COOLA RIVERS [Includes Tributaries] EXCE | REGION 5 5-6,5-8,5-11 | name | ATNARKO/BELLA COOLA RIVERS [Includes Tributaries] EXCEPT: Burnt Bridge Creek upstream of Sitkatapa Creek, Hunlen Creek u |
| ATNARKO/BELLA COOLA RIVERS [Includes Tributaries] EXCE | REGION 5 5-6,5-8,5-11 | entry | EXCEPT: Burnt Bridge Creek upstream of Sitkatapa Creek, Hunlen Creek upstream of Hunlen Falls, and Young Creek upstream  |
| BLUE LEAD CREEK | REGION 5 5-15 | rule | downstream of falls situated approximately 2 km from Quesnel Lake |
| BONAPARTE RIVER | REGION 3 3-30 | rule | downstream of falls at Bonaparte fishway |
| BREM RIVER | REGION 2 2-13 | rule | from the upstream edge of 1st falls (situated approximately 1.5 km from the river's mouth) to a point 100 m downstream |
| BREM RIVER | REGION 2 2-13 | rule | upstream of 2nd set of falls (situated approximately 2.5 km upstream of Hillis Creek confluence) |
| CAMPBELL RIVER | REGION 1 1-10 | rule | between Elk Falls and John Hart Dam Power Station |
| CAYOOSH CREEK | REGION 3 3-16 | rule | downstream of falls |
| CELISTA CREEK | REGION 3 3-36 | rule | downstream of the falls |
| CHAPMAN CREEK | REGION 2 2-5 | rule | from the falls to 100 m downstream (falls are located approximately 550 m upstream of the power line crossing) |
| CHEMAINUS RIVER | REGION 1 1-5 | rule | from Copper Canyon Falls downstream 100 m to the fishing boundary signs |
| CHESLATTA RIVER (downstream of falls) | REGION 6 6-4 | name | downstream of falls |
| CLEARWATER RIVER | REGION 3 3-40,3-46 | rule | from Falls Creek to Mahood River |
| CLEARWATER RIVER | REGION 3 3-40,3-46 | rule | downstream of Falls Creek |
| COFFEE CREEK | REGION 4 4-18 | rule | downstream of fishing boundary signs at falls approximately 10 km from Kootenay Lake |
| CRANBERRY RIVER | REGION 6 6-15 | rule | between fishing boundary signs upstream of and downstream of Cranberry River Canyon |
| CRAZY CREEK | REGION 3 3-35 | rule | downstream of the falls |
| CRAZY CREEK | REGION 3 3-35 | rule | upstream of the falls |
| DEAN RIVER | REGION 5 5-9 | rule | from Crag Creek to fishing boundary signs approximately 500 m upstream of canyon |
| DEAN RIVER | REGION 5 5-9 | rule | from fishing boundary signs approximately 500 m upstream of canyon to signs 100 m downstream of canyon |
| DEAN RIVER | REGION 5 5-9 | rule | from fishing boundary signs approximately 100 m downstream of canyon to tidal boundary |
| DEAN RIVER | REGION 5 5-9 | rule | between signs 0.5 km and 3.5 km upstream of canyon |
| DEAN RIVER | REGION 5 5-9 | rule | From Crag Creek to signs 500 m upstream of the canyon |
| DEAN RIVER | REGION 5 5-9 | rule | From signs 100 m downstream of canyon to tidal boundary |
| DINOSAUR LAKE (Reservoir Downstream of W.A.C. Bennett  | REGION 7B 7-31 | rule | from W.A.C. Bennett Dam to 100 m south of Gething Creek and between the anti-vortex dyke and Peace Canyon Dam |
| FORDING RIVER (downstream of Josephine Falls) | REGION 4 4-23 | name | downstream of Josephine Falls |
| FORDING RIVER (upstream of Josephine Falls) | REGION 4 4-23 | name | upstream of Josephine Falls |
| HALFWAY RIVER | REGION 4 4-31 | rule | downstream of falls approximately 11 km from Arrow Lake |
| HEBER RIVER | REGION 1 1-9 | rule | upstream of top of lower canyon, located 1.3km upstream of the Gold River confluence |
| HEBER RIVER | REGION 1 1-9 | rule | downstream of top of lower canyon, located 1.3km upstream of the Gold River confluence |
| HEBER RIVER | REGION 1 1-9 | rule | downstream of Saunders Creek to the top of the lower canyon, located 1.3km upstream of the Gold River confluence |
| HORSEFLY RIVER (from Quesnel Lake to Horsefly River Fa | REGION 5 5-2 | name | from Quesnel Lake to Horsefly River Falls |
| HUNLEN CREEK (upstream of Hunlen Falls) | REGION 5 5-11 | name | upstream of Hunlen Falls |
| HUNLEN CREEK (upstream of Hunlen Falls) | REGION 5 5-11 | rule | Downstream of Hunlen Falls |
| ILLECILLEWAET RIVER | REGION 4 4-33 | rule | downstream of Albert Canyon |
| ISKUT RIVER | REGION 6 6-21 | rule | downstream of Forest Kerr Canyon |
| KANAKA CREEK | REGION 2 2-8 | rule | from Cliff Park Falls to 112th Avenue |
| KOKISH RIVER | REGION 1 1-11 | rule | from boundary signs in Kokish canyon to Ida Lake |
| KOKISH RIVER | REGION 1 1-11 | rule | from the log boom located approximately 100 m upstream of the IPP intake to signs at the tail of the canyon pool located |
| KUSKANAX CREEK | REGION 4 4-31 | rule | downstream of falls 1 km upstream of Gardiner Creek |
| LIUMCHEN CREEK | REGION 2 2-3 | rule | downstream of the lower falls |
| LODGEPOLE CREEK (downstream of falls near the km 26 po | REGION 4 4-2 | name | downstream of falls near the km 26 post on Lodgepole Road |
| LODGEPOLE CREEK (upstream of falls) | REGION 4 4-2 | name | upstream of falls |
| LYNN CREEK | REGION 2 2-8 | rule | between fishing boundary signs situated approximately 200 m upstream of and 150 m downstream of Twin Falls Bridge |
| MAHOOD RIVER | REGION 3 3-46 | rule | Downstream of Goodwin Falls |
| MAHOOD RIVER | REGION 3 3-46 | rule | Upstream of Goodwin Falls |
| MCLEOD RIVER | REGION 7A 7-24 | rule | from Carp Lake to War Falls |
| MCRAE CREEK | REGION 8 8-15 | rule | downstream of falls situated approximately 4 km upstream of Christina Lake |
| MISSION CREEK | REGION 8 8-10 | rule | from falls at Gallagher Canyon to Okanagan Lake |
| MOFFAT CREEK | REGION 5 5-2 | rule | downstream of falls 8 km from Horsefly River |
| MURRAY RIVER | REGION 7B 7-21 | rule | from Kinuseo Falls to signs about 2 km downstream |
| NORNS (Pass) CREEK | REGION 4 4-15 | rule | downstream of falls approximately 2 km from Columbia River |
| PEACE RIVER (From Hwy. 29 bridge to the Site C dam) | REGION 7B 7-31 | rule | between Peace Canyon Dam and Hwy 29 Bridge |
| PEACHLAND CREEK | REGION 8 8-8 | rule | from Hardy Falls to Okanagan Lake |
| PTARMIGAN CREEK | REGION 7A 7-5 | rule | from falls to Quarry Bridge |
| PUNTLEDGE RIVER | REGION 1 1-6 | rule | downstream of the BC Hydro diversion dam (approximately 3.5 km downstream of Comox Lake) to the base of Stotan Falls (ap |
| QUATSE RIVER | REGION 1 1-13 | rule | upstream of the Quatse River fishway (approximately 1.4 km upstream of Dick Booth Creek) |
| QUINSAM RIVER | REGION 1 1-6 | rule | from the falls situated downstream of Middle Quinsam Lake to the fishing boundary signs at power line crossing (approxim |
| SEYMOUR RIVER | REGION 3 3-36 | rule | downstream of the falls |
| SLOCAN RIVER | REGION 4 4-17 | except | EXCEPT Koch Creek[Includes Tributaries] upstream of falls located approximately 700 m downstream of the Little Slocan Fo |
| SLOCAN RIVER | REGION 4 4-17 | except | EXCEPT Koch Creek[Includes Tributaries] upstream of falls and Little Slocan Lake's tributaries |
| SOMASS RIVER | REGION 1 1-7 | rule | between the tidal boundary at Papermill Dam to boundary signs approximately 1.0 km upstream (Falls Road Gravel Pit and t |
| SOOKE RIVER | REGION 1 1-2 | rule | downstream of Sooke River Falls |
| SOOKE RIVER | REGION 1 1-2 | rule | from the base of the lower "potholes" falls to signs approximately 100 m downstream |
| STAMP RIVER | REGION 1 1-7 | rule | between fishing boundary signs 200 m upstream of and 500 m downstream of Stamp Falls |
| STAMP RIVER | REGION 1 1-7 | rule | upstream of signs at "Girl Guide Falls" (approximately 250 m upstream of the mouth of Beaver Creek) |
| STAMP RIVER | REGION 1 1-7 | rule | downstream of signs at "Girl Guide Falls" (approximately 250 m upstream of the mouth of Beaver Creek) |
| STELLAKO RIVER | REGION 6 6-4,7-12 | rule | from François Lake to the falls |
| TAMIHI CREEK | REGION 2 2-3 | rule | downstream of the falls situated approximately 200 m upstream of Chilliwack River |
| TAMIHI CREEK | REGION 2 2-3 | rule | upstream of the falls situated approximately 200 m upstream of Chilliwack River |
| TOQUART RIVER | REGION 1 1-8 | rule | upstream of the boundary sign located near the falls approximately 800 m downstream of Toquart Lake (including the Upper |
| TROUT CREEK | REGION 8 8-8 | rule | from the trestle in Trout Creek Canyon to Okanagan Lake |
| WAP CREEK | REGION 8 8-24 | rule | downstream of Frog Falls |
| WAP CREEK | REGION 8 8-24 | rule | upstream of Frog Falls |
| WOODBURY CREEK | REGION 4 4-18 | rule | downstream of falls at small hydro structure approximately 800 m upstream of Hwy 31 bridge |
| ZYMOETZ (Copper) RIVER | REGION 6 6-9 | rule | between fishing boundary signs in Zymoetz Canyon |
| ZYMOETZ (Copper) RIVER | REGION 6 6-9 | rule | upstream of fishing boundary sign at the transmission line crossing (located downstream of Zymoetz Canyon) |

## dam_weir_fence  (43)  →  point (infrastructure)

| Regulation | R/MU | src | locator text |
|---|---|---|---|
| BABINE RIVER | REGION 6 6-8 | rule | from the juvenile fish counting weir located at the outlet of Nilkitkwa Lake to the Nilkitkwa River confluence |
| BABINE RIVER | REGION 6 6-8 | rule | between fishing boundary signs approximately 100 m upstream of and 80 m downstream of the adult fish counting fence |
| BABINE RIVER | REGION 6 6-8 | rule | from the adult fish counting fence (described above) downstream to the Babine River's confluence with the Skeena River |
| BABINE RIVER | REGION 6 6-8 | rule | from the juvenile fish counting weir located at the outlet of Nilkitkwa Lake downstream to the Babine River's confluence |
| BRIDGE RIVER | REGION 3 3-33 | rule | from Terzaghi Dam to Yalakom River |
| BRUNETTE RIVER | REGION 2 2-8 | rule | upstream of Burnaby Lake or from Cariboo Dam to Salamander Creek |
| CAMPBELL RIVER | REGION 1 1-10 | rule | From John Hart Dam Power Station to power line crossing approximately 200 m upstream of Quinsam River confluence |
| CAPILANO RIVER | REGION 2 2-8 | rule | upstream of fishing boundary signs at footbridge situated approximately 100 m downstream of the fish fence |
| COLUMBIA RIVER | REGION 4 4-8,4-15,4-26,4-34,4-38 | rule | from Revelstoke Dam downstream to Hwy 1 bridge in Revelstoke |
| COLUMBIA RIVER | REGION 4 4-8,4-15,4-26,4-34,4-38 | rule | from Keenleyside Dam to the Washington state border |
| COLUMBIA RIVER | REGION 4 4-8,4-15,4-26,4-34,4-38 | rule | From Keenleyside Dam downstream to the Washington state border and connected reaches: the Kootenay River (Columbia River |
| COWICHAN RIVER (see map below) | REGION 1 1-4 | rule | from weir (dam) at Cowichan Lake's outlet to Greendale Trestle |
| DINOSAUR LAKE (Reservoir Downstream of W.A.C. Bennett  | REGION 7B 7-31 | name | Reservoir Downstream of W.A.C. Bennett Dam |
| DUNCAN RIVER | REGION 4 4-19 | rule | downstream of Duncan Dam to the confluence of the Duncan River and the Lardeau River |
| ELK RIVER (downstream of Elko Dam) | REGION 4 4-2 | name | downstream of Elko Dam |
| ELK RIVER (upstream of Elko Dam) | REGION 4 4-2,4-23 | name | upstream of Elko Dam |
| ELK RIVER (upstream of Elko Dam) | REGION 4 4-2,4-23 | rule | from Lower Elk Lake to Forsyth Cr, from Line Creek Bridge to CPR Bridge at Sparwood, from Hwy 3 bridge at Hosmer to the  |
| GREAT CENTRAL LAKE | REGION 1 1-7 | rule | from the dam to fishing boundary signs approximately 50 m upstream (southwest) of the Ash Main Bridge |
| JOHN HART LAKE'S TRIBUTARIES | REGION 1 1-10 | rule | channel downstream of Ladore Dam |
| KEOGH RIVER | REGION 1 1-13 | rule | downstream of lower fish counting fence near tidewater |
| KITIMAT RIVER (Angling regulations for the Kitimat Riv | REGION 6 6-3 | rule | on the west half of river between fishing boundary signs near Kitimat Hatchery outfall |
| KOOTENAY RIVER (downstream of Idaho border) | REGION 4 4-7,4-8 | rule | Downstream from the Idaho border to CPR Bridge near Creston and from Corra Linn Dam to the Columbia River |
| KOOTENAY RIVER (downstream of Idaho border) | REGION 4 4-7,4-8 | rule | from the Brilliant Dam to the confluence with the Columbia River |
| KOOTENAY RIVER (downstream of Idaho border) | REGION 4 4-7,4-8 | rule | From the Brilliant Dam to the confluence with the Columbia River |
| LAKE REVELSTOKE | REGION 4 4-38,4-39 | rule | from Mica Dam to fishing boundary signs at the narrows immediately downstream of the mouth of Bigmouth Creek |
| LITTLE QUALICUM RIVER | REGION 1 1-6 | rule | from the hatchery fence to signs approximately 35 m downstream |
| OKANAGAN RIVER | REGION 8 8-1 | rule | from Okanagan Lake Dam downstream to McIntyre Dam and downstream of Drop Structure No. 1 |
| OKANAGAN RIVER OXBOWS | REGION 8 8-1 | rule | downstream of the McIntyre Dam and upstream of Vaseux Lake |
| PEACE RIVER (Downstream of boundary signs 1,200m downs | REGION 7B 7-31 | name | Downstream of boundary signs 1,200m downstream of the Site C dam |
| PEACE RIVER (From Hwy. 29 bridge to the Site C dam) | REGION 7B 7-31 | name | From Hwy. 29 bridge to the Site C dam |
| PEACE RIVER (From Site C dam to boundary signs 1,200m  | REGION 7B 7-31 | name | From Site C dam to boundary signs 1,200m downstream |
| PEND D'OREILLE RIVER (Includes the reservoirs behind W | REGION 4 4-8 | name | Includes the reservoirs behind Waneta Dam and Seven Mile Dam |
| PINKUT CREEK | REGION 6 6-6 | rule | downstream of the fish fence |
| PUNTLEDGE RIVER | REGION 1 1-6 | rule | from fishing boundary signs located 50 m upstream of the BC Hydro generating station tailrace to signs located 75 m down |
| PUNTLEDGE RIVER | REGION 1 1-6 | rule | upstream of the BC Hydro diversion dam (approximately 3.5 km downstream of Comox Lake) |
| QUALICUM RIVER | REGION 1 1-6 | rule | downstream of boundary signs located approximately 100 m downstream of the hatchery counting fence |
| QUALICUM RIVER | REGION 1 1-6 | rule | from the upper hatchery weir (located 125 m downstream of the E&N Trestle) to boundary sign: located approximately 100 m |
| QUINSAM RIVER | REGION 1 1-6 | rule | from the fishing boundary signs at power line crossing (approximately 25 m upstream of Quinsam Hatchery weir) to fishing |
| SETON RIVER (includes BC Hydro Power Canal upstream of | REGION 3 3-16 | name | includes BC Hydro Power Canal upstream of the dam up to signs located on Seton Lake |
| STAMP RIVER | REGION 1 1-7 | rule | from the confluence with Ash River upstream to the Great Central Lake dam |
| STAVE RIVER | REGION 2 2-8 | rule | in the Ruskin spawning channel, from the inlet near the dam downstream to the boat ramp crossing |
| SWIFT CREEK | REGION 7A 7-2 | rule | from upstream side of weir to CNR Bridge in Valemount |
| VASEUX LAKE (including two lagoons on the west side of | REGION 8 8-1 | name | including two lagoons on the west side of Okanagan River upstream of McIntyre Dam |

## bridge_road_km  (116)  →  point (bridge/road/km marker)

| Regulation | R/MU | src | locator text |
|---|---|---|---|
| "HIGH" LAKE (unnamed lake approx. 4 km north of Bridge | REGION 5 5-1 | name | unnamed lake approx. 4 km north of Bridge Lake |
| ADAM RIVER (except Eve River) | REGION 1 1-10 | rule | upstream of Eve River, to Hwy 19 bridge |
| ALEXANDER CREEK (downstream of the easternmost Hwy 3 b | REGION 4 4-23 | name | downstream of the easternmost Hwy 3 bridge |
| ALEXANDER CREEK (upstream of the easternmost Hwy 3 bri | REGION 4 4-23 | name | upstream of the easternmost Hwy 3 bridge |
| ALOUETTE RIVER | REGION 2 2-8 | rule | upstream of 216th Street |
| ARTLISH RIVER | REGION 1 1-12 | rule | upstream of the boundary signs at the bridge crossing approximately 10 km from the mouth |
| BEAR (Mahood) CREEK | REGION 2 2-4 | rule | upstream of 152nd Street (Johnson Road) |
| BRIDGE RIVER | REGION 3 3-33 | rule | downstream of Hwy 40 bridge (approximately 6 km north of Lillooet) |
| BULKLEY RIVER | REGION 6 6-9 | rule | from Morice River to CNR Bridge at Barrett |
| BURTON CREEK | REGION 4 4-15 | rule | from Woden Creek to Hwy 6 bridge |
| CAMPBELL RIVER | REGION 1 1-10 | rule | from the boundary sign at the end of Maple Street downstream to the boundary sign at the cement block |
| CAMPBELL RIVER | REGION 1 1-10 | rule | downstream of power line crossing approximately 200 m upstream of Quinsam River |
| CAMPBELL RIVER | REGION 2 2-4 | rule | between two white triangular fishing boundary signs downstream to pedestrian bridge at the foot of Stayte Road |
| CARIBOU CREEK | REGION 4 4-15 | rule | from Rodd Creek to Hwy 6 bridge |
| CHEHALIS RIVER | REGION 2 2-19 | rule | from boundary signs at outlet of Chehalis Lake to main logging road bridge approximately 2.4 km downstream of Chehalis L |
| CHEHALIS RIVER | REGION 2 2-19 | rule | downstream of the main logging road bridge situated approximately 2.4 km downstream of Chehalis Lake |
| CHILKO RIVER | REGION 5 5-5 | rule | upstream of bridge at Henry's Crossing |
| CHILLIWACK / VEDDER RIVERS (does not include Sumas Riv | REGION 2 2-4 | rule | downstream of Tamihi Rapids Bridge to Vedder Crossing Bridge |
| CHILLIWACK / VEDDER RIVERS (does not include Sumas Riv | REGION 2 2-4 | rule | Downstream of Vedder Crossing Bridge |
| CHOWADE RIVER | REGION 7B 7-43 | rule | upstream of the Horseshoe Road Bridge |
| CLEARWATER RIVER | REGION 3 3-40,3-46 | rule | Downstream of old Clearwater Bridge |
| CLUXEWE RIVER | REGION 1 1-13 | rule | upstream of the West Main logging road bridge (approximately 7.5 km upstream of the Hwy 19 bridge) |
| COAL CREEK (downstream of Old MF&M Railway bridge 7 km | REGION 4 4-23 | name | downstream of Old MF&M Railway bridge 7 km upstream of Elk River |
| COPPER CREEK | REGION 1 6-12 | rule | from Skidegate Lake to signs at second bridge 6 km upstream of tidal boundary |
| COQUIHALLA RIVER | REGION 2 2-17 | rule | upstream of the northern entrance to the upper most railway tunnel |
| COQUIHALLA RIVER | REGION 2 2-17 | rule | downstream of the southern entrance to the lower most railway tunnel |
| COQUIHALLA RIVER | REGION 2 2-17 | rule | at Othello Tunnels from the northern entrance to the upper most railway tunnel to the southern entrance of the lower mos |
| COQUITLAM RIVER | REGION 2 2-8 | rule | upstream of Mary Hill Bypass Bridge |
| COQUITLAM RIVER | REGION 2 2-8 | rule | from Lougheed Highway Bridge to Mary Hill Bypass Bridge |
| COWICHAN RIVER (see map below) | REGION 1 1-4 | rule | upstream of CNR Trestle (Mile 66) |
| COWICHAN RIVER (see map below) | REGION 1 1-4 | rule | downstream of the CNR Mile 66 Trestle |
| COWICHAN RIVER (see map below) | REGION 1 1-4 | rule | from signs at Greendale Trestle to CNR Bridge (mile 70.2) |
| CROOKED RIVER | REGION 7A 7-24 | rule | downstream (north) of the 200 Road Bridge |
| CROOKED RIVER | REGION 7A 7-24 | rule | upstream (south) of the 200 Road Bridge |
| DEENA CREEK | REGION 1 6-12 | rule | upstream of fishing boundary signs at second bridge approximately 5 km upstream of the tidal boundary |
| ELK RIVER'S TRIBUTARIES (see exceptions) | REGION 4 4-2,4-23 | except | EXCEPT Coal Creek downstream of old MF&M Railway Bridge 7 km upstream of Elk River |
| EVE RIVER | REGION 1 1-10 | rule | upstream of the fishing boundary signs (near the South Main Bridge crossing) located approximately 5.4 km downstream of  |
| FINDLAY CREEK | REGION 4 4-26 | rule | from Doctor Creek Bridge to Lavington Creek Bridge |
| FRASER RIVER | REGION 3 3-14 | rule | from the lower entrance of the fish ladder at Bridge River Rapids to the BC Railway Bridge approximately 2 km north of L |
| FRASER RIVER | REGION 3 3-14 | rule | From Hwy 99 bridge at Lillooet downstream approximately 1.2 km to BC Hydro's tail race outflow channel |
| FRASER RIVER | REGION 3 3-14 | rule | From the confluence with Thompson River to the CNR Bridge approximately 1 km downstream |
| FRASER RIVER (upstream of the CPR Bridge at Mission) | REGION 2 2-4 | name | upstream of the CPR Bridge at Mission |
| HAYS CREEK (in Prince Rupert) | REGION 6 6-14 | rule | upstream of fishing boundary signs downstream of lower culvert near fish cannery in Prince Rupert |
| HORSEFLY RIVER (from Quesnel Lake to Horsefly River Fa | REGION 5 5-2 | rule | from Woodjam Bridge to Quesnel Lake |
| HYLAND CREEK | REGION 2 2-4 | rule | upstream of 152nd Street (Johnson Road) |
| KITIMAT RIVER (Angling regulations for the Kitimat Riv | REGION 6 6-3 | rule | in tributaries and upstream of Hwy 37 bridge |
| KOOTENAY RIVER (downstream of Idaho border) | REGION 4 4-7,4-8 | rule | from CPR Bridge near Creston downstream 2 km to navigation dolphin |
| KSI SII AKS RIVER (formerly Tseax River) | REGION 6 6-14 | rule | upstream of Nass Road Bridge |
| KSI X'ANMAS RIVER (formerly Kwinamass River) | REGION 6 6-14 | rule | upstream from the lower bridge abutments |
| LAKELSE RIVER | REGION 6 6-10 | rule | from the outlet of Lakelse Lake to the power line crossing, located 3.5 km upstream of the Lakelse River mouth |
| LAKELSE RIVER | REGION 6 6-10 | rule | between Lakelse Lake and CNR Bridge |
| LUSSIER RIVER | REGION 4 4-21 | rule | downstream of Premier Lake Bridge crossing |
| LUSSIER RIVER | REGION 4 4-21 | rule | between Premier Lake Bridge crossing and Mutton Creek |
| MAMIN RIVER | REGION 1 6-13 | rule | upstream of fishing boundary signs on third bridge approximately 10 km upstream of the tidal boundary |
| MARA LAKE | REGION 8 8-26 | rule | south of the CPR bridge |
| MCARTHUR ISLAND SLOUGH | REGION 3 3-28 | rule | from westerly entrance to 12th Street entrance to Park |
| MICHEL CREEK (downstream of the easternmost Hwy 3 brid | REGION 4 4-23 | name | downstream of the easternmost Hwy 3 bridge |
| MICHEL CREEK (upstream of the easternmost Hwy 3 bridge | REGION 4 4-23 | name | upstream of the easternmost Hwy 3 bridge |
| MOHUN CREEK | REGION 1 1-10 | rule | from Menzies Bay logging mainline bridge crossing to Morton Lake |
| MOYIE RIVER | REGION 4 4-5 | rule | from bridge at south end of Moyie Lake to U.S. border |
| NAHATLATCH RIVER | REGION 3 3-15 | rule | from Frances Lake downstream approximately 400 m to fishing boundary signs at the logging bridge |
| NANAIMO RIVER | REGION 1 1-5 | rule | from power line crossing at "Bore Hole" upstream to fishing boundary signs at the mouth of Boulder Creek |
| NANAIMO RIVER | REGION 1 1-5 | rule | from the Cedar Road Bridge upstream to the Hwy 19 bridge |
| NANAIMO RIVER | REGION 1 1-5 | rule | upstream of the Hwy 1 bridge |
| NASS RIVER | REGION 6 6-30 | rule | from white triangular fishing boundary signs located downstream of the Meziadin River confluence, and upstream to the Hw |
| NATHAN (Beaver) CREEK | REGION 2 2-4 | rule | upstream of 272nd Street (Jackman Road) |
| NATHAN (Beaver) CREEK | REGION 2 2-4 | rule | downstream of 272nd Street (Jackman Road) |
| NECHAKO RIVER | REGION 7A 7-12 | rule | from said sign downstream to Hwy 27 bridge |
| NECHAKO RIVER | REGION 7A 7-12 | rule | downstream of Foothills Boulevard Bridge in Prince George |
| NICOMEKL RIVER | REGION 2 2-4 | rule | upstream of 208th Street (Berry Road) |
| NICOMEKL RIVER | REGION 2 2-4 | rule | downstream of 208th Street |
| NITINAT RIVER | REGION 1 1-4 | rule | between fishing boundary signs approximately 100 m upstream of and downstream of "Red Rock Pool," approximately 2 km (by |
| NITINAT RIVER | REGION 1 1-4 | rule | between boundary signs approximately 50 m upstream of and downstream of the Nitinat River Bridge |
| NOONS CREEK | REGION 2 2-8 | rule | upstream of railway bridge |
| NORTH ALOUETTE RIVER | REGION 2 2-8 | rule | upstream of 216th Street (Fifth Ave) |
| PINE RIVER | REGION 7B 7-32 | rule | upstream of the Hasler Road Bridge |
| PITT RIVER | REGION 2 2-8 | rule | in the Lower Pitt River (CPR Bridge upstream to Pitt Lake) |
| POWERS CREEK | REGION 8 8-11 | rule | downstream of Hwy 97 bridge to Okanagan Lake |
| PRUDHOMME LAKE (south of the Hwy 16 bridge) | REGION 6 6-14 | name | south of the Hwy 16 bridge |
| QUESNEL RIVER | REGION 5 5-2 | rule | from 50 m upstream of Likely Bridge to 50 m downstream of Likely Bridge |
| QUESNEL RIVER | REGION 5 5-2 | rule | from the boundary signs approximately 1.8 km east of the Likely Bridge downstream to Morehead Creek |
| ROSEMOND LAKE | REGION 8 8-26 | rule | south of the CPR Bridge |
| SALMON RIVER | REGION 2 2-4 | rule | upstream of 232nd Street (Livingstone Road) |
| SALMON RIVER | REGION 2 2-4 | rule | downstream of 232nd Street (Livingstone Road) |
| SALMON RIVER | REGION 3 3-26 | rule | downstream of Hwy 97 bridge at Falkland |
| SAND CREEK (downstream of Hwy 3) | REGION 4 4-22 | name | downstream of Hwy 3 |
| SERPENTINE RIVER | REGION 2 2-4 | rule | upstream of 168th Street at Bothwell Park |
| SERPENTINE RIVER | REGION 2 2-4 | rule | downstream of 168th Street at Bothwell Park |
| SHORTS CREEK | REGION 8 8-11 | rule | from Westside Road Bridge to Okanagan Lake |
| SHUSWAP RIVER | REGION 8 8-26 | rule | from Mara Lake upstream to Mara Bridge |
| SHUSWAP RIVER | REGION 8 8-26 | rule | 50 m upstream and 50 m downstream of Trinity Bridge |
| SHUSWAP RIVER | REGION 8 8-26 | rule | from Mara Bridge upstream to Sugar Lake |
| SILVERHOPE (Silver) CREEK | REGION 2 2-2 | rule | from Silver Lake down to the Bailey Bridge situated approximately 8 km upstream of Hwy 1 |
| SILVERHOPE (Silver) CREEK | REGION 2 2-2 | rule | downstream of Bailey Bridge situated approximately 8 km upstream of Hwy 1 |
| SKOOKUMCHUCK CREEK | REGION 4 4-20 | rule | from a point on the creek closest to km 38 on the Skookumchuck Forest Service Road to Buhl Creek |
| SPROAT RIVER | REGION 1 1-7 | rule | from Sproat Lake to fishing boundary signs approximately 300 m downstream of Hwy 4 |
| ST. LEON CREEK | REGION 4 4-31 | rule | downstream of barrier approximately 1 km upstream of the Hwy 23 bridge |
| STELLAKO RIVER | REGION 6 6-4,7-12 | rule | between fishing boundary signs approximately 250 m and 4 km downstream of the bridge near the François Lake outlet |
| SUSTUT RIVER | REGION 6 6-18 | rule | upstream of BCR Bridge at Bear River mouth |
| TAHLTAN RIVER | REGION 6 6-22 | rule | from boundary signs located approximately 400 m upstream from the Tahltan RIver Bridge on the Telegraph Creek Road to th |
| TEEPEE LAKE (adjacent to West Road River) | REGION 5 5-13 | name | adjacent to West Road River |
| THOMPSON RIVER (downstream of signs at Kamloops Lake o | REGION 3 3-13,3-14,3-18 | rule | from the CNR Bridge downstream of Deadman River to CNR Bridge upstream of Bonaparte River |
| TLELL RIVER | REGION 1 6-13 | rule | downstream of tidal boundary sign located 1.5 km upstream of Hwy 16 Bridge |
| TOQUART RIVER | REGION 1 1-8 | rule | upstream of the Toquart mainline logging bridge when open |
| TREPANIER RIVER | REGION 8 8-8 | rule | from Hwy 97C to Okanagan Lake |
| TROUT CREEK | REGION 8 8-8 | rule | Upstream of the trestle |
| UPPER ARROW LAKE (drawdown area) | REGION 4 4-31,4-32 | rule | between Hwy 1 bridge in Revelstoke and the power line crossing at Akolkolex Narrows |
| WEST ROAD ("Blackwater") RIVER'S TRIBUTARIES | REGION 6 6-1 | name | WEST ROAD RIVER'S TRIBUTARIES |
| WHITE RIVER | REGION 1 1-10 | rule | upstream of the Sayward Road Bridge crossing |
| WIGWAM RIVER (downstream of the access road adjacent t | REGION 4 4-2 | name | downstream of the access road adjacent to km 42 on the Bighorn (Ram |
| WIGWAM RIVER (downstream of the access road adjacent t | REGION 4 4-2 | entry | downstream of the access road adjacent to km 42 on the Bighorn (Ram) Forest Service Road |
| WIGWAM RIVER (upstream of the Forest Service recreatio | REGION 4 4-2 | name | upstream of the Forest Service recreation site adjacent to km 42 on the Bighorn (Ram |
| WILLISTON LAKE (in Zone A) (includes waters 500 m east | REGION 7A 7-30,7-37,7-38 | name | includes waters 500 m east/upstream of the Causeway Road |
| WILLISTON LAKE (in Zone A) (includes waters 500 m east | REGION 7A 7-30,7-37,7-38 | entry | in Zone A (includes waters 500 m east/upstream of the Causeway Road) |
| WILLISTON LAKE (in Zone A) (includes waters 500 m east | REGION 7A 7-30,7-37,7-38 | rule | 500 m upstream and downstream of Causeway Road |
| YOUNG CREEK (upstream of Hwy 20) | REGION 5 5-11 | name | upstream of Hwy 20 |

## confluence_tributary  (127)  →  confluence (tributary mouth)

| Regulation | R/MU | src | locator text |
|---|---|---|---|
| (Lower) CAMPBELL LAKE'S TRIBUTARIES | REGION 1 1-6 | name | CAMPBELL LAKE'S TRIBUTARIES |
| ANZAC RIVER | REGION 7A 7-23 | rule | upstream of the North Anzac River confluence |
| ARROW LAKES' TRIBUTARIES | REGION 4  | name | ARROW LAKES' TRIBUTARIES |
| ATNARKO/BELLA COOLA RIVERS [Includes Tributaries] EXCE | REGION 5 5-6,5-8,5-11 | rule | on Atnarko River, from Goat Creek to the confluence with Talchako River |
| ATNARKO/BELLA COOLA RIVERS [Includes Tributaries] EXCE | REGION 5 5-6,5-8,5-11 | rule | downstream of Young Creek |
| ATTICHIKA CREEK | REGION 7A 7-39 | rule | 500 m upstream and downstream of the Thorn Creek confluence or the Kemess Creek confluence |
| BRUNETTE RIVER'S TRIBUTARIES | REGION 2 2-8 | name | BRUNETTE RIVER'S TRIBUTARIES |
| BULKLEY RIVER | REGION 6 6-9 | rule | upstream of Morice/Bulkley River confluence |
| BURNABY LAKE'S TRIBUTARIES | REGION 2 2-8 | name | BURNABY LAKE'S TRIBUTARIES |
| BURNT BRIDGE CREEK (upstream of Sitkatapa Creek) | REGION 5 5-11 | name | upstream of Sitkatapa Creek |
| BURNT BRIDGE CREEK (upstream of Sitkatapa Creek) | REGION 5 5-11 | rule | Downstream of Sitkatapa Creek |
| BUTTLE LAKE'S TRIBUTARIES | REGION 1 1-9 | name | BUTTLE LAKE'S TRIBUTARIES |
| CAMPBELL RIVER | REGION 1 1-10 | rule | in any tributaries |
| CAYCUSE RIVER | REGION 1 1-3 | rule | upstream of and including Hatton Creek |
| CHEHALIS LAKE'S TRIBUTARIES | REGION 2 2-19 | name | CHEHALIS LAKE'S TRIBUTARIES |
| CHEMAINUS RIVER | REGION 1 1-5 | rule | downstream of Bannon Creek |
| CHEMAINUS RIVER | REGION 1 1-5 | rule | upstream of Bannon Creek |
| CHILCOTIN RIVER | REGION 5 5-12,5-13,5-14 | rule | upstream of Chilko River |
| CHILCOTIN RIVER | REGION 5 5-12,5-13,5-14 | rule | downstream of Chilko River |
| CHILCOTIN RIVER | REGION 5 5-12,5-13,5-14 | rule | Downstream of Chilko River |
| CHILKO LAKE'S tributary streams | REGION 5 5-4 | name | CHILKO LAKE'S tributary streams |
| CHILKO RIVER | REGION 5 5-5 | rule | upstream of Brittany Creek |
| CLEARWATER RIVER | REGION 3 3-40,3-46 | rule | from Mahood River to North Thompson River |
| COLDWATER RIVER'S TRIBUTARIES | REGION 3 3-13 | name | COLDWATER RIVER'S TRIBUTARIES |
| COLUMBIA LAKE'S TRIBUTARIES | REGION 4 4-25 | name | COLUMBIA LAKE'S TRIBUTARIES |
| CONNOR LAKE'S TRIBUTARIES | REGION 4 4-23 | name | CONNOR LAKE'S TRIBUTARIES |
| COWICHAN RIVER (see map below) | REGION 1 1-4 | rule | in tributaries upstream of and including Holt Creek |
| CULTUS CREEK | REGION 4 4-7 | rule | downstream of Laib Creek |
| DEAN RIVER | REGION 5 5-9 | rule | upstream of Iltasyuko River |
| DEAN RIVER | REGION 5 5-9 | rule | from Iltasyuko River to Crag Creek |
| DEAN RIVER | REGION 5 5-9 | rule | From Anahim Lake to Iltasyuko River |
| DUNCAN LAKE'S TRIBUTARIES | REGION 4 4-27 | name | DUNCAN LAKE'S TRIBUTARIES |
| DUTCH CREEK | REGION 4 4-26 | rule | from Ben Able Creek to Columbia Lake and/or River |
| ELK RIVER'S TRIBUTARIES (see exceptions) | REGION 4 4-2,4-23 | name | ELK RIVER'S TRIBUTARIES |
| FLATHEAD RIVER'S TRIBUTARIES | REGION 4 4-1 | name | FLATHEAD RIVER'S TRIBUTARIES |
| FRASER RIVER | REGION 3 3-14 | rule | upstream of Thompson River |
| FRASER RIVER | REGION 3 3-14 | rule | from Hell's Gate to the confluence with the Thompson River |
| FRASER RIVER | REGION 3 3-14 | rule | upstream of the confluence with the Thompson River |
| FRASER RIVER | REGION 3 3-14 | rule | from the confluence with Spuzzum Creek (Region 3 boundary) to Hells Gate |
| FRASER RIVER | REGION 5 5-2 | rule | in the Fraser River mainstem downstream of the confluence with the Chilcoltin River |
| FRASER RIVER | REGION 5 5-2 | rule | in the Fraser River Watershed upstream of Williams Lake River |
| FRASER RIVER | REGION 5 5-2 | rule | from the confluence with the Chilcotin River downstream to the Region 5 boundary |
| FRASER RIVER | REGION 7A 7-9 | rule | upstream of Cottonwood River |
| FRASER RIVER (upstream of the CPR Bridge at Mission) | REGION 2 2-4 | rule | in the area bounded by a line commencing at a fishing boundary sign located at the eastern end of Landstrom Bar (Scale B |
| GLACIER (Redslide) CREEK (unnamed tributary to Nanika  | REGION 6 6-9 | name | unnamed tributary to Nanika River |
| GOAT RIVER | REGION 4 4-6 | except | EXCEPT Kitchener Creek, see Kitchener Creek, a tributary |
| GOAT RIVER | REGION 7A 7-5 | rule | upstream of the Macleod Creek confluence |
| GOLD RIVER | REGION 1 1-9 | rule | upstream of the Muchalat River |
| GOLD RIVER | REGION 1 1-9 | rule | downstream of the Muchalat River |
| GORDON RIVER | REGION 1 1-3 | rule | upstream of Bugaboo Creek |
| GRANBY RIVER | REGION 8 8-15 | rule | Upstream of Burrell Creek |
| GRANBY RIVER | REGION 8 8-15 | rule | Downstream of Burrell Creek |
| GRANBY RIVER'S TRIBUTARIES | REGION 8 8-15 | name | GRANBY RIVER'S TRIBUTARIES |
| HALFWAY RIVER | REGION 7B 7-34 | rule | from confluence with Peace River to fishing boundary signs approximately 5 km upstream |
| HARRIS CREEK | REGION 1 1-3 | rule | upstream of and including Hemmingsen Creek |
| HELLROARING CREEK | REGION 4 4-20 | rule | downstream of Angus Creek |
| JOHN HART LAKE'S TRIBUTARIES | REGION 1 1-10 | name | JOHN HART LAKE'S TRIBUTARIES |
| JORDAN RIVER | REGION 4 4-39 | rule | upstream of Kirkup Creek |
| JORDAN RIVER | REGION 4 4-39 | rule | Upstream of Kirkup Creek |
| KETTLE RIVER'S TRIBUTARIES | REGION 8 8-14 | name | KETTLE RIVER'S TRIBUTARIES |
| KINBASKET (McNaughton) LAKE'S TRIBUTARIES | REGION 4 4-36 | name | KINBASKET LAKE'S TRIBUTARIES |
| KITIMAT RIVER (Angling regulations for the Kitimat Riv | REGION 6 6-3 | name | Angling regulations for the Kitimat River are currently under review. Please check the in-season regulation change websi |
| KITSUMKALUM (Kalum) RIVER | REGION 6 6-15 | rule | from the outlet of Kitsumkalum Lake to Glacier Creek confluence |
| KOKISH RIVER | REGION 1 1-11 | rule | between signs at the IPP tail race confluence downstream approximately 500 m to signs |
| KOOTENAY LAKE'S TRIBUTARIES | REGION 4 4-7,4-19 | name | KOOTENAY LAKE'S TRIBUTARIES |
| KOOTENAY RIVER (upstream of Koocanusa Reservoir) | REGION 4 4-2,4-21,4-22,4-24,4-25,4-35 | rule | Upstream of Koocanusa Reservoir to White River |
| KOOTENAY RIVER (upstream of Koocanusa Reservoir) | REGION 4 4-2,4-21,4-22,4-24,4-25,4-35 | rule | Upstream of White River |
| LAKE REVELSTOKE'S TRIBUTARIES | REGION 4 4-38 | name | LAKE REVELSTOKE'S TRIBUTARIES |
| LARDEAU RIVER | REGION 4 4-29,4-30 | rule | downstream of fishing boundary signs at Trout Lake outlet, to fishing boundary signs approximately 600m downstream near  |
| LITTLE QUALICUM RIVER | REGION 1 1-6 | rule | All tributaries |
| LITTLE SLOCAN LAKE'S TRIBUTARIES | REGION 4 4-16 | name | LITTLE SLOCAN LAKE'S TRIBUTARIES |
| LOWER ARROW LAKE'S TRIBUTARIES | REGION 4 4-14 | name | LOWER ARROW LAKE'S TRIBUTARIES |
| LUSSIER RIVER | REGION 4 4-21 | rule | downstream of Mutton Creek |
| MAHOOD LAKE (see map on page 28 for area closure) | REGION 3 3-46 | rule | within the fishing boundary signs at the western tip of the lake near the mouth of Canim River |
| MEZIADIN RIVER | REGION 6 6-16 | rule | from fishing boundary signs at outlet of Meziadin Lake to Nass River |
| MITCHELL RIVER | REGION 5 5-15 | rule | from Michell Lake to Cameron Creek |
| MITCHELL RIVER | REGION 5 5-15 | rule | downstream of Cameron Creek |
| MITCHELL RIVER | REGION 5 5-15 | rule | downstream of Cameron Creek (including Cameron Creek) |
| MORICE RIVER | REGION 6 6-9 | rule | from Gosnell Creek to Lamprey Creek |
| MOYIE RIVER | REGION 4 4-5 | rule | Irishman Creek (Moyie River tributary) |
| NIMPKISH RIVER | REGION 1 1-11 | rule | upstream of Davie River |
| NITINAT RIVER | REGION 1 1-4 | rule | upstream of Parker Creek |
| OYSTER RIVER | REGION 1 1-6 | rule | upstream of the confluence with Little Oyster River |
| PEACE RIVER (From Hwy. 29 bridge to the Site C dam) | REGION 7B 7-31 | rule | from mouth of Halfway River to fishing boundary signs approximately 5 km upstream and 5 km downstream |
| PEND D'OREILLE RIVER'S TRIBUTARIES (except Salmo River | REGION 4 4-8 | name | except Salmo River[Includes Tributaries] |
| PEND D'OREILLE RIVER'S TRIBUTARIES (except Salmo River | REGION 4 4-8 | name | PEND D'OREILLE RIVER'S TRIBUTARIES |
| PREMIER LAKE'S TRIBUTARIES | REGION 4 4-21 | name | PREMIER LAKE'S TRIBUTARIES |
| PUNTLEDGE RIVER | REGION 1 1-6 | rule | between fishing boundary signs approximately 100 m upstream and downstream of the confluence with Morrison Creek |
| QUESNEL RIVER | REGION 5 5-2 | rule | upstream of Cariboo River |
| QUESNEL RIVER | REGION 5 5-2 | rule | downstream of Morehead Creek |
| RANCHERIA RIVER'S TRIBUTARIES | REGION 6 6-25 | name | RANCHERIA RIVER'S TRIBUTARIES |
| REVELSTOKE LAKE'S TRIBUTARIES | REGION 4 4-38 | name | REVELSTOKE LAKE'S TRIBUTARIES |
| SALMO RIVER | REGION 4 4-8 | rule | Sheep Creek to South Salmo River |
| SALMO RIVER'S TRIBUTARIES | REGION 4 4-8 | name | SALMO RIVER'S TRIBUTARIES |
| SALMON RIVER | REGION 1 1-10 | rule | upstream of Kay Creek |
| SALMON RIVER | REGION 1 1-10 | rule | upstream of confluence with White River |
| SALMON RIVER | REGION 1 1-10 | rule | from estuary to confluence with White River |
| SAN JUAN RIVER | REGION 1 1-3 | rule | upstream of Fleet River |
| SEVEN MILE RESERVOIR'S TRIBUTARIES | REGION 4 4-8 | name | SEVEN MILE RESERVOIR'S TRIBUTARIES |
| SKEENA RIVER (mainstem only) | REGION 6 6-10 | rule | from Exchamsiks River to 1.5 km upstream of Kitsumkalum River (known as "Skeena River 2") |
| SKEENA RIVER (mainstem only) | REGION 6 6-10 | rule | 1.5 km upstream of Zymoetz River (known as "Skeena River Section 4") |
| SKEENA RIVER (mainstem only) | REGION 6 6-10 | rule | Shegunia River confluence to Sedan Creek confluence |
| SKEENA RIVER (mainstem only) | REGION 6 6-10 | rule | Chimdemash Creek confluence to 1.5 km upstream of Zymoetz River confluence |
| SKEENA RIVER/KISPIOX RIVER CONFLUENCE | REGION 6 6-8 | name | SKEENA RIVER/KISPIOX RIVER CONFLUENCE |
| SKEENA RIVER/KISPIOX RIVER CONFLUENCE | REGION 6 6-8 | rule | within 3 white fishing boundary signs located at the confluence of the Skeena and Kispiox rivers |
| SLOCAN LAKE'S TRIBUTARIES | REGION 4 4-17 | name | SLOCAN LAKE'S TRIBUTARIES |
| SNOW CREEK | REGION 4 4-15 | rule | downstream of Hail Creek |
| SQUAMISH POWERHOUSE CHANNEL | REGION 2 2-6 | rule | upstream of Ashlu Creek |
| SQUAMISH RIVER'S TRIBUTARIES | REGION 2 2-6 | name | SQUAMISH RIVER'S TRIBUTARIES |
| ST. MARY RIVER | REGION 4 4-20 | rule | on all tributaries |
| STAVE RIVER | REGION 2 2-8 | rule | in the Northrop Spawning Channel, from the intake downstream to where the channel joins the Stave River main |
| TANYA LAKE'S TRIBUTARIES | REGION 5 5-10 | name | TANYA LAKE'S TRIBUTARIES |
| THOMPSON RIVER (downstream of signs at Kamloops Lake o | REGION 3 3-13,3-14,3-18 | name | downstream of signs at Kamloops Lake outlet to the confluence with Fraser River |
| TROUT LAKE'S TRIBUTARIES | REGION 4 4-30 | name | TROUT LAKE'S TRIBUTARIES |
| TSITIKA RIVER | REGION 1 1-10 | rule | upstream of Catherine Creek |
| TSITIKA RIVER | REGION 1 1-10 | rule | downstream of Catherine Creek |
| UPPER ARROW LAKE'S TRIBUTARIES | REGION 4 4-31 | name | UPPER ARROW LAKE'S TRIBUTARIES |
| WAHLEACH ("Jones") LAKE'S TRIBUTARIES | REGION 2 2-3 | name | WAHLEACH LAKE'S TRIBUTARIES |
| WANETA RESERVOIR'S TRIBUTARIES | REGION 4 4-8 | name | WANETA RESERVOIR'S TRIBUTARIES |
| WEAVER LAKE and WEAVER CREEK | REGION 2 2-19 | rule | from fishing boundary signs at log booms on Weaver Lake downstream to where Sakwi Creek enters Weaver Creek |
| WEST KETTLE RIVER'S tributaries | REGION 8 8-12 | name | WEST KETTLE RIVER'S tributaries |
| WHITE RIVER (see also east White & North White Rivers) | REGION 4 4-24 | rule | Upstream of and including North White River |
| WHITE RIVER (see also east White & North White Rivers) | REGION 4 4-24 | rule | downstream of North White River |
| WILLISTON LAKE (in Zone A) (includes waters 500 m east | REGION 7A 7-30,7-37,7-38 | rule | from tributaries |
| WILSON CREEK | REGION 4 4-17 | rule | downstream of Burkitt Creek |
| ZYMOETZ (Copper) RIVER | REGION 6 6-9 | rule | Upstream of Limonite Creek (Zymoetz River A) |
| ZYMOETZ (Copper) RIVER | REGION 6 6-9 | rule | Downstream of Limonite Creek (Zymoetz River B) |

## lake_reach  (58)  →  lake (often already split)

| Regulation | R/MU | src | locator text |
|---|---|---|---|
| "BLUEY LAKE POTHOLES" | REGION 8 8-6 | rule | all unnamed lakes within 2 km of Bluey Lake |
| "GRIZZLY" LAKE (unnamed lake approx. 4.5 km upstream o | REGION 5 5-15 | name | unnamed lake approx. 4.5 km upstream of Maeford Lake |
| "SEELEY" CREEK (outlet of Seeley Lake) | REGION 6 6-9 | name | outlet of Seeley Lake |
| ADAMS RIVER (downstream of Adams Lake) | REGION 3 3-37 | name | downstream of Adams Lake |
| ADAMS RIVER (upstream of Adams Lake) | REGION 3 3-37 | name | upstream of Adams Lake |
| ASH RIVER | REGION 1 1-7 | rule | from Elsie Lake to Dickson Lake |
| ASHER CREEK | REGION 4 4-30 | rule | downstream of South Fork (approximately 5 km from Trout Lake) |
| ATNARKO/BELLA COOLA RIVERS [Includes Tributaries] EXCE | REGION 5 5-6,5-8,5-11 | rule | from Tenas Lake to fishing boundary signs near Atnarko Park campsite |
| BIG BAR CREEK | REGION 3 3-31 | rule | downstream of Big Bar Lake |
| BONAPARTE RIVER | REGION 3 3-30 | rule | downstream of Bonaparte Lake |
| BRIDGE RIVER | REGION 3 3-33 | rule | upstream of Downton Lake (reservoir) |
| CHILKO LAKE | REGION 5 5-4 | rule | on Big Lagoon (west side of lake) |
| CHIMNEY CREEK | REGION 5 5-2 | rule | downstream of Brunson Lake |
| COLUMBIA RIVER | REGION 4 4-8,4-15,4-26,4-34,4-38 | rule | from Mud Lake to Columbia Lake |
| DAVIE RIVER | REGION 1 1-11 | rule | downstream of Schoen Lake |
| DEADMAN RIVER | REGION 3 3-29 | rule | downstream of Mowich Lake |
| DOWNTON LAKE (Reservoir) | REGION 3 3-33 | name | Reservoir |
| EAGLE RIVER | REGION 3 3-34 | rule | downstream of Griffin Lake |
| ECHOES LAKE (near Kimberley) | REGION 4 4-20 | rule | from both lakes |
| FORSYTH CREEK | REGION 4 4-23 | rule | from Connor Lake downstream 3 km |
| FRANÇOIS LAKE | REGION 6 6-4 | entry | at the outlet of François Lake described on map 1 |
| GUICHON CREEK | REGION 3 3-18 | rule | downstream of Mamit Lake |
| HARRISON RIVER (from the Fraser River upstream to Harr | REGION 2 2-18 | name | from the Fraser River upstream to Harrison Lake |
| IDLEWILD LAKE (old Cranbrook Reservoir) | REGION 4 4-3 | name | old Cranbrook Reservoir |
| JEWEL CREEK | REGION 8 8-14 | rule | from Jewel Lake downstream approximately 1.5 km to fishing boundary signs |
| KOOTENAY RIVER (downstream of Idaho border) | REGION 4 4-7,4-8 | rule | from Idaho border near Creston to Kootenay Lake |
| KOOTENAY RIVER (upstream of Koocanusa Reservoir) | REGION 4 4-2,4-21,4-22,4-24,4-25,4-35 | name | upstream of Koocanusa Reservoir |
| LIARD RIVER WATERSHED (see map on page 63) | REGION 7B 7-53 | rule | from all lakes and streams |
| MAHOOD LAKE (see map on page 28 for area closure) | REGION 3 3-46 | rule | within fishing boundary signs at the western tip of the lake |
| MCKINLEY CREEK | REGION 5 5-2 | rule | downstream of McKinley Lake |
| MOBERLY RIVER | REGION 7B 7-32 | rule | downstream of Moberly Lake |
| NAHATLATCH RIVER | REGION 3 3-15 | rule | Downstream of Nahatlatch Lake (including Hannah and Frances lakes; except as noted upstream of ) |
| NAHATLATCH RIVER | REGION 3 3-15 | rule | downstream of Nahatlatch Lake |
| NAHMINT RIVER | REGION 1 1-7 | rule | Nahmint River (upstream and downstream of the lake) |
| NAHMINT RIVER | REGION 1 1-7 | rule | upstream of Nahmint Lake |
| NANAIMO RIVER | REGION 1 1-5 | rule | upstream of the westernmost of the two Nanaimo Lakes, known locally as "Second" Lake |
| NICOLA RIVER | REGION 3 3-13 | rule | upstream of Nicola Lake |
| NICOLA RIVER | REGION 3 3-13 | rule | downstream of Nicola Lake |
| PAUL CREEK (downstream of Paul Lake) | REGION 3 3-27 | name | downstream of Paul Lake |
| PENNASK CREEK | REGION 3 3-12 | rule | upstream of Pennask Lake |
| PITT LAKE | REGION 2 2-8 | rule | north of fishing boundary signs (east and west shores) near the head of the lake |
| PITT RIVER | REGION 2 2-8 | rule | upstream of Pitt Lake |
| PREMIER LAKE | REGION 4 4-21 | rule | south of fishing boundary signs on the lake shore |
| RUBY CREEK | REGION 2 2-5 | rule | from Ruby Lake to fishing boundary signs approximately 100 m downstream |
| SETON RIVER (includes BC Hydro Power Canal upstream of | REGION 3 3-16 | rule | downstream of Seton Lake |
| SETON RIVER (includes BC Hydro Power Canal upstream of | REGION 3 3-16 | rule | Downstream of Seton Lake |
| SEYMOUR RIVER | REGION 2 2-8 | rule | downstream of Seymour Lake |
| SHANNON LAKE (netted off portion on the south end of t | REGION 8 8-11 | entry | netted off portion on the south end of the lake |
| SHUMWAY LAKE | REGION 3 3-20 | rule | north of fishing boundary signs located at south end of lake |
| SHUSWAP LAKE (see maps on page 28) (includes Little Sh | REGION 3 3-26 | name | includes Little Shuswap Lake, that part of South Thompson River between Shuswap Lake and Little Shuswap Lake, Seymour, A |
| SHUSWAP RIVER | REGION 8 8-26 | rule | Upstream of Sugar Lake |
| SILVERHOPE (Silver) CREEK | REGION 2 2-2 | rule | upstream of Silver Lake |
| THOMPSON RIVER (downstream of signs at Kamloops Lake o | REGION 3 3-13,3-14,3-18 | rule | Downstream of signs at Kamloops Lake |
| THOMPSON RIVER (upstream of Kamloops Lake) | REGION 3 3-28 | name | upstream of Kamloops Lake |
| UNNAMED LAKES (located immediately north and south of  | REGION 8 8-6 | entry | located immediately north and south of Bluey Lake |
| WHATSHAN RIVER | REGION 4 4-32 | rule | upstream of Whatshan Lake |
| YAKOUN RIVER | REGION 1 6-13 | rule | from Yakoun Lake downstream approximately 13 km to fishing boundary signs |
| ZYMOETZ (Copper) RIVER | REGION 6 6-9 | rule | from McDonell Lake downstream approximately 3 km to fishing boundary signs |

## lake_inlet_outlet  (5)  →  NEW lake_io op (lake_inlets ∪ lake_outlets)

| Regulation | R/MU | src | locator text |
|---|---|---|---|
| HEALY LAKE'S OUTLET STREAM | REGION 1 1-5 | name | HEALY LAKE'S OUTLET STREAM |
| TATSATUA CREEK (formerly known as Tatsamenie Lake's ou | REGION 6 6-26 | name | formerly known as Tatsamenie Lake's outlet streams |
| WHITESWAN LAKE'S INLET & OUTLET STREAMS | REGION 4 4-24 | name | WHITESWAN LAKE'S INLET & OUTLET STREAMS |
| WHITESWAN LAKE'S INLET & OUTLET STREAMS | REGION 4 4-24 | except | EXCEPT the outlet stream downstream of the falls 2.4 km downstream of Whiteswan Lake |
| WHITETAIL LAKE'S INLET & OUTLET STREAMS | REGION 4 4-26 | name | WHITETAIL LAKE'S INLET & OUTLET STREAMS |

## radius_buffer  (23)  →  NEW buffer op (point+radius)

| Regulation | R/MU | src | locator text |
|---|---|---|---|
| ANZAC RIVER | REGION 7A 7-23 | rule | within 500 m radius of the Upper Anzac bridge |
| BABINE LAKE | REGION 6 6-6 | rule | within a 400 m radius of the mouth of Pinkut Creek |
| BUCKINGHORSE LAKE | REGION 6 6-20 | rule | within 100 m of outlet |
| BULKLEY RIVER | REGION 6 6-9 | rule | in Moricetown Canyon or within 100 m downstream |
| COWICHAN LAKE (including Bear Lake) | REGION 1 1-4 | rule | within 60 m of shore |
| DAVIS BAY (in Finlay Reach of Williston Lake) | REGION 7A 7-37 | rule | within a 500 m radius of the Davis Forest Service Road Bridge |
| KINBASKET (McNaughton) LAKE | REGION 4 4-36 | rule | within 200 m of Bush-Sullivan Road Bridge in Bush Arm |
| KLAHOWYA LAKE | REGION 6 6-20 | rule | within 100 m of outlet |
| LEIGHTON LAKE | REGION 3 3-18 | rule | within 100 m of the mouth of the inlet stream |
| LEIGHTON LAKE | REGION 3 3-18 | rule | within 100 m of the Tunkwa Creek outlet |
| LETAIN LAKE | REGION 7B 7-52 | rule | within 100 m of fishing boundary sign at outlet |
| LOON LAKE | REGION 3 3-30 | rule | within 500 m of outlet stream at southwest end of lake as marked by fishing boundary signs |
| MAHOOD LAKE (see map on page 28 for area closure) | REGION 3 3-46 | rule | within 200 m of the Mahood River outlet |
| MAHOOD LAKE (see map on page 28 for area closure) | REGION 3 3-46 | rule | within 200 m of the mouth of the Mahood River outlet |
| MITCHELL LAKE | REGION 5 5-15 | rule | within 100 m radius of the weir at the lake's outlet |
| MITCHELL RIVER | REGION 5 5-15 | rule | within 100 m radius of the weir at the outlet of Michell Lake |
| RAINBOW LAKES | REGION 7B 7-52 | rule | within 100 m of fishing boundary sign at outlet |
| RUBY LAKE | REGION 2 2-5 | rule | in the outlet bay within 100 m of the head of Ruby Creek |
| SILVERTHORNE (Erickson) LAKE | REGION 6 6-9 | rule | within 50 m of the outlet |
| TUPPER RIVER | REGION 7B 7-20 | rule | within 100 m downstream of outlet weir at Swan Lake |
| WAHPEETO CREEK | REGION 1 1-14 | rule | within 100 m downstream of the falls situated approximately 4.5 km upstream of Wakeman River |
| WHITE LAKE | REGION 3 3-26 | rule | within 400 m of the mouth of Cedar Creek as designated by signs |
| WOLVERINE LAKE | REGION 7B 7-52 | rule | within 100 m of fishing boundary sign at outlet |

## line_between_signs  (31)  →  line (author 2 endpoints) / area for lakes

| Regulation | R/MU | src | locator text |
|---|---|---|---|
| "ANDERSON" LAKE | REGION 1 1-3 | entry | Unnamed lake in the Walbran Creek Watershed approximately 7 km west/southwest of Mt. Walbran |
| "GEESE" LAKE (2 km northeast of Eliguk Lake) | REGION 5 5-12 | name | 2 km northeast of Eliguk Lake |
| "GRASSY" LAKE (unnamed lake approx. 1 km southwest of  | REGION 5 5-1 | name | unnamed lake approx. 1 km southwest of West King Lake |
| "LITTLE BISHOP" LAKE (approx. 1.7 km northeast of Bish | REGION 5 5-13 | name | approx. 1.7 km northeast of Bishop Lake |
| ADAMS LAKE | REGION 3 3-37 | rule | north of a line drawn due west from mouth of Momich River |
| ALOUETTE LAKE |  2-8 | rule | at S. end of lake, S. of a line drawn from the BC Parks boat ramp to signs on the E. side of the lake |
| ALOUETTE LAKE | REGION 2 2-8 | rule | at south end of lake, south of a line drawn from the BC Parks boat ramp to signs on the east side of the lake |
| BABINE LAKE | REGION 6 6-6 | rule | east of a line from Gullwing Creek to the south shore of Babine Lake |
| CANIM LAKE (see map on page 42) | REGION 5 5-1 | rule | within the waters of the small bay at the mouth of Eagle Creek northerly of a line drawn between two boundary signs loca |
| CHILLIWACK / VEDDER RIVERS (does not include Sumas Riv | REGION 2 2-4 | rule | upstream from a line between two fishing boundary signs on either side of the Chilliwack River 100 m downstream of the c |
| CHILLIWACK / VEDDER RIVERS (does not include Sumas Riv | REGION 2 2-4 | rule | downstream of a line between two fishing boundary signs on either side of the Chilliwack River 100m downstream of the co |
| CHRISTINA LAKE | REGION 8 8-15 | rule | north of a line between Bald and Knob Points |
| COLUMBIA RIVER | REGION 4 4-8,4-15,4-26,4-34,4-38 | rule | from a line between the old Robson Ferry landing and a sign on the south river bank, downstream approximately 950 m to t |
| COLUMBIA RIVER | REGION 4 4-8,4-15,4-26,4-34,4-38 | rule | from Keenleyside Dam to a line between the old Robson Ferry landing and a sign on the south river bank |
| DRAGON LAKE | REGION 5 5-2 | rule | southeast of a line between fishing boundary signs on opposite shores of the bay at the mouth of Hallis Creek |
| FISH LAKE (unnamed lake approx. 2 km northwest of McCl | REGION 5 5-6 | name | unnamed lake approx. 2 km northwest of McClinchy Lake |
| GREEN LAKE | REGION 5 5-1 | rule | northeast of line between boundary signs on opposite shores of the bay at the mouth of Watch Creek |
| HELENE LAKE | REGION 6 6-6 | rule | northwest of a line between fishing boundary signs on opposite shores of the outlet bay |
| LOON LAKE | REGION 3 3-30 | rule | northeast of fishing boundary signs near the mouth of Thunder Creek and the public access site |
| MABEL LAKE | REGION 8 8-24 | rule | south of a line between fishing boundary signs on the lakeshore approximately 800 m north of Shuswap River inlet |
| NATION ARM (Williston Lake) | REGION 7A 7-30 | rule | west of a line between two fishing boundary signs approximately 500 m downstream (east) of the Nation River Bridge on th |
| NULKI LAKE | REGION 7A 7-12 | rule | west of a line between fishing boundary signs on lakeshore near mouth of Corkscrew Creek |
| QUESNEL LAKE | REGION 5 5-15 | rule | southwest of a line between fishing boundary signs on opposite shores of Horsefly Bay |
| QUESNEL LAKE | REGION 5 5-15 | rule | in North Arm, north of a line between Watt and Service Creeks |
| ROCHE LAKE | REGION 3 3-20 | rule | south of a line bearing true 244° from a point on the southern tip of the largest island in the south arm of Roche Lake  |
| SHUSWAP LAKE (see maps on page 28) (includes Little Sh | REGION 3 3-26 | rule | in the waters lying west of a line between signs at Henstridge Road and Wharf Road to a line between signs on the south  |
| SHUSWAP LAKE (see maps on page 28) (includes Little Sh | REGION 3 3-26 | rule | east of a line between fishing boundary signs on Murdock and Semaphore points, to Hwy 1 bridge |
| SHUSWAP LAKE (see maps on page 28) (includes Little Sh | REGION 3 3-26 | rule | in Salmon Arm Bay, west of line between Engineer's Point and Sunnybrae Point |
| TAKYSIE LAKE | REGION 6 6-4 | rule | northwest of a line between fishing boundary signs on opposite shores immediately north of Takysie Lake Settlement |
| TROUT LAKE | REGION 4 4-30 | rule | northwest of a line between fishing boundary signs on opposite shores approximately 1.5 km southeast the city of Trout L |
| WOLF LAKE (situated approx. 2.3 km northeast of Lorin  | REGION 5 5-1 | name | situated approx. 2.3 km northeast of Lorin Lake |

## area_park_polygon  (18)  →  area_boundary (polygon)

| Regulation | R/MU | src | locator text |
|---|---|---|---|
| (Lower) CAMPBELL LAKE'S TRIBUTARIES | REGION 1 1-6 | rule | including Campbell River between Strathcona Dam and (Lower) Campbell Lake |
| ADAMS RIVER (downstream of Adams Lake) | REGION 3 3-37 | rule | between fishing boundary signs in the vicinity of the public salmon viewing platforms in Tsutswecew Provincial Park |
| ATNARKO/BELLA COOLA RIVERS [Includes Tributaries] EXCE | REGION 5 5-6,5-8,5-11 | rule | upstream of Tweedsmuir Provincial Park plus Tenas Lake |
| ATNARKO/BELLA COOLA RIVERS [Includes Tributaries] EXCE | REGION 5 5-6,5-8,5-11 | rule | downstream of eastern boundary of Tweedsmuir Provincial Park |
| BAKER CREEK | REGION 5 5-13 | rule | upstream of Pinnacles Provincial Park |
| BAKER CREEK | REGION 5 5-13 | rule | downstream of Pinnacles Provincial Park |
| BEAR LAKE (Crooked River Provincial Park) | REGION 7A 7-16 | name | Crooked River Provincial Park |
| CAMPBELL RIVER | REGION 1 1-10 | rule | from Strathcona Dam downstream 100 m |
| CRESTON VALLEY WILDLIFE MANAGEMENT AREA (CVWMA) WATERS | REGION 4 4-6 | rule | within the CVWMA, including Six Mile Lake, Leach Lake, Kootenay River and Canal |
| ENGLISHMAN RIVER | REGION 1 1-5 | rule | from lower falls in Englishman River Park to signs approximately 100 m downstream |
| ENGLISHMAN RIVER | REGION 1 1-5 | rule | downstream of the lower falls in Englishman River Falls Provincial Park to the Top Bridge crossing at the end of Allsbro |
| HART LAKE (Crooked River Provincial Park) | REGION 7A 7-16 | name | Crooked River Provincial Park |
| LITTLE QUALICUM RIVER | REGION 1 1-6 | rule | from the falls in Little Qualicum Falls Provincial Park downstream to the hatchery fence |
| PITT RIVER | REGION 2 2-8 | rule | within Garibaldi Park |
| SQUARE LAKE (located in Crooked River Provincial Park) | REGION 7A 7-16 | name | located in Crooked River Provincial Park |
| STRATHCONA PARK WATERS | REGION 1 1-9 | name | STRATHCONA PARK WATERS |
| STRATHCONA PARK WATERS | REGION 1 1-9 | rule | on any water within Strathcona Park |
| WOOD RIVER | REGION 4 4-40 | rule | within Hamber Provincial Park |

## boundary_signs_generic  (12)  →  point (locate signs from map)

| Regulation | R/MU | src | locator text |
|---|---|---|---|
| CHICHOUYENILY CREEK | REGION 7A 7-30 | rule | downstream of fishing boundary signs near its mouth |
| CHUCKWALLA RIVER | REGION 5 5-7 | rule | between fishing boundary signs at Ten Mile Pool |
| CULTUS LAKE | REGION 2 2-3 | rule | at north end, as buoyed and signed |
| FRASER RIVER | REGION 3 3-14 | rule | between fishing boundary signs approximately 6.5 km downstream of Boston Bar to signs 2.8 km downstream of Hell's Gate |
| GAGNON CREEK | REGION 7A 7-30 | rule | downstream of fishing boundary signs near its mouth |
| HARRISON LAKE | REGION 2 2-18 | rule | at south end, as buoyed and signed |
| NECHAKO RIVER | REGION 7A 7-12 | rule | from Cheslatta River to a boundary sign 5 km downstream |
| ROSS LAKE (Boundary between Ross Lake and Skagit River | REGION 2 2-2 | name | Boundary between Ross Lake and Skagit River is market by signs |
| SKAGIT RIVER (boundary between Skagit River and Ross L | REGION 2 2-2 | name | boundary between Skagit River and Ross Lake is marked by signs |
| THOMPSON RIVER (downstream of signs at Kamloops Lake o | REGION 3 3-13,3-14,3-18 | rule | Upstream of boundary signs 1 km downstream of Martel |
| WESTON CREEK | REGION 7A 7-30 | rule | downstream of signs near its mouth |
| WHITE RIVER | REGION 1 1-10 | rule | between fishing boundary signs at the salmon viewing pool |

## except_negative  (19)  →  EXCEPT set-difference / negative member

| Regulation | R/MU | src | locator text |
|---|---|---|---|
| "BLUEY LAKE POTHOLES" | REGION 8 8-6 | except | except Bluey Lake itself and Kentucky Lake |
| ADAM RIVER (except Eve River) | REGION 1 1-10 | name | except Eve River |
| CAMPBELL RIVER | REGION 1 1-10 | except | except Quinsam River |
| CHILLIWACK / VEDDER RIVERS (does not include Sumas Riv | REGION 2 2-4 | entry | does not include Sumas River |
| COLUMBIA LAKE'S TRIBUTARIES | REGION 4 4-25 | except | except Dutch Creek |
| CRESTON VALLEY WILDLIFE MANAGEMENT AREA (CVWMA) WATERS | REGION 4 4-6 | except | EXCEPT Duck Lake (see separate entry) |
| ELK RIVER'S TRIBUTARIES (see exceptions) | REGION 4 4-2,4-23 | name | see exceptions |
| FINDLAY CREEK | REGION 4 4-26 | except | except Lavington Creek |
| FRASER RIVER | REGION 3 3-14 | except | except as noted below |
| KOOTENAY LAKE - UPPER WEST ARM (for location see map o | REGION 4 4-7 | except | EXCEPT Apr 1-Apr 3 and July 1-July 2 only, when daily quota = 5 |
| MCLEOD RIVER | REGION 7A 7-24 | except | excluding War Lake |
| NAHATLATCH RIVER | REGION 3 3-15 | except | except as noted upstream of |
| NILKITKWA LAKE | REGION 6 6-8 | except | EXCEPT dead fin fish may be used as bait when set lining |
| SALMO RIVER'S TRIBUTARIES | REGION 4 4-8 | except | EXCEPT bull trout catch and release |
| SQUAMISH RIVER'S TRIBUTARIES | REGION 2 2-6 | except | EXCEPT: Ashlu Creek, Cheakamus, Elaho and Mamquam Rivers, and the Squamish Powerhouse Channel |
| ST. MARY RIVER | REGION 4 4-20 | except | except Joseph Creek |
| STRATHCONA PARK WATERS | REGION 1 1-9 | except | except Gold, Upper Campbell and Buttle lakes |
| TCHESINKUT LAKE | REGION 6 6-4 | except | EXCEPT during months of February and July (when regional quotas apply) |
| WAP CREEK | REGION 8 8-24 | except | excluding Wap Lake |

## map_or_vague  (26)  →  manual / map-only (no clean anchor)

| Regulation | R/MU | src | locator text |
|---|---|---|---|
| ALOUETTE LAKE |  2-8 | rule | in swimming areas |
| ALOUETTE RIVER | REGION 2 2-8 | rule | on mainstem |
| ATNARKO/BELLA COOLA RIVERS [Includes Tributaries] EXCE | REGION 5 5-6,5-8,5-11 | rule | on mainstems of Atnarko River and Bella Coola River |
| BEAVER LAKE | REGION 1 1-1 | rule | on parts |
| BRANNEN LAKE | REGION 1 1-5 | rule | on parts |
| CANIM LAKE (see map on page 42) | REGION 5 5-1 | name | see map on page 42 |
| CHILLIWACK / VEDDER RIVERS (does not include Sumas Riv | REGION 2 2-4 | name | see map on page 24 |
| CHUCKWALLA RIVER | REGION 5 5-7 | rule | entire river |
| COURTENAY RIVER | REGION 1 1-6 | rule | on part |
| COWICHAN LAKE (including Bear Lake) | REGION 1 1-4 | rule | on parts |
| COWICHAN RIVER (see map below) | REGION 1 1-4 | name | see map below |
| COWICHAN RIVER (see map below) | REGION 1 1-4 | rule | on parts |
| ELK LAKE | REGION 1 1-1 | rule | on parts |
| FRASER RIVER (upstream of the CPR Bridge at Mission) | REGION 2 2-4 | rule | Fraser River Mainstem |
| KITSUMKALUM (Kalum) RIVER | REGION 6 6-15 | rule | for the entire river |
| KOOTENAY LAKE - LOWER WEST ARM (for location see map o | REGION 4 4-7 | name | for location see map on page 34 |
| KOOTENAY LAKE - MAIN BODY (for location see map on pag | REGION 4 4-19 | name | for location see map on page 34 |
| KOOTENAY LAKE - UPPER WEST ARM (for location see map o | REGION 4 4-7 | name | for location see map on page 34 |
| LIARD RIVER WATERSHED (see map on page 63) | REGION 7B 7-53 | name | see map on page 63 |
| LONG LAKE (Nanaimo) | REGION 1 1-5 | rule | on parts |
| MAHOOD LAKE (see map on page 28 for area closure) | REGION 3 3-46 | name | see map on page 28 for area closure |
| NANAIMO RIVER | REGION 1 1-5 | rule | on parts |
| OSOYOOS LAKE | REGION 8 8-1 | rule | in 5 signed swimming areas |
| SALMO RIVER | REGION 4 4-8 | rule | Remainder of mainstem |
| SHUSWAP LAKE (see maps on page 28) (includes Little Sh | REGION 3 3-26 | name | see maps on page 28 |
| WEST ROAD ("Blackwater") RIVER | REGION 5 5-12,5-13 | rule | in mainstem (only) |

## other_reach  (58)  →  reach (generic)

| Regulation | R/MU | src | locator text |
|---|---|---|---|
| ARROW PARK (Mosquito) CREEK | REGION 4 4-32 | name | ARROW PARK CREEK |
| BAKER CREEK | REGION 5 5-13 | rule | downstream of Park |
| BIG LAKE (approx. 10 km west of 100 Mile House) | REGION 5 5-2 | name | approx. 10 km west of 100 Mile House |
| BIG LAKE (approx. 30 km west of Likely) | REGION 5 5-2 | name | approx. 30 km west of Likely |
| BOWRON LAKE Park waters other than Bowron Lake | REGION 5 5-16 | name | BOWRON LAKE Park waters other than Bowron Lake |
| BULL RIVER | REGION 4 4-22 | rule | Other parts |
| BUTTLE LAKE'S TRIBUTARIES | REGION 1 1-9 | rule | Thelwood Creek |
| CAMPBELL RIVER | REGION 2 2-4 | rule | upstream of 12th Avenue |
| CAMPBELL RIVER | REGION 2 2-4 | rule | downstream of 12th Avenue |
| CHILKOOT TRAIL NATIONAL HISTORIC PARK WATERS | REGION 6 6-28 | name | CHILKOOT TRAIL NATIONAL HISTORIC PARK WATERS |
| COLUMBIA LAKE | REGION 4 4-25 | rule | near eastern shore and at south end |
| COLUMBIA RIVER | REGION 4 4-8,4-15,4-26,4-34,4-38 | rule | in wetlands |
| COLUMBIA RIVER | REGION 4 4-8,4-15,4-26,4-34,4-38 | rule | in main channel from Fairmont to Donald |
| DEAN RIVER | REGION 5 5-9 | rule | all parts |
| DEER LAKE (Sasquatch Park) | REGION 2 2-18 | name | Sasquatch Park |
| ELK RIVER (upstream of Elko Dam) | REGION 4 4-2,4-23 | rule | All other parts |
| FINDLAY CREEK | REGION 4 4-26 | rule | other parts |
| FRASER RIVER | REGION 3 3-14 | rule | downstream of Hell's Gate |
| FRASER RIVER | REGION 3 3-14 | rule | from Hells Gate upstream to the Region 3 boundary |
| FRASER RIVER (upstream of the CPR Bridge at Mission) | REGION 2 2-4 | rule | in the non-tidal portion of the Fraser River in Region 2 |
| FRASER RIVER (upstream of the CPR Bridge at Mission) | REGION 2 2-4 | rule | in the Jesperson's Side Channel, Herrling Side Channel, and Seabird Island north Side Channel |
| GOAT RIVER | REGION 4 4-6 | rule | Leadville Creek Cameron Creek |
| GOLD RIVER | REGION 1 1-9 | except | but not including the Muchalat or Heber Rivers |
| HARRISON RIVER (from the Fraser River upstream to Harr | REGION 2 2-18 | rule | in small bays along the river as signed |
| HATZIC LAKE AND SLOUGH | REGION 2 2-8 | rule | in Hatzic Lake |
| HEFFLEY LAKE (parts of ) | REGION 3 3-27 | entry | parts of |
| ISKUT RIVER | REGION 6 6-21 | rule | between Natadesleen Lake and Kinaskan Lake |
| JERRY SULINA PARK POND | REGION 2 2-8 | name | JERRY SULINA PARK POND |
| JORDAN RIVER | REGION 4 4-39 | rule | from Kirkup Creek downstream, including Kirkup Creek |
| KEMESS CREEK | REGION 7A 7-39 | rule | from Attichka Creek to a point 500 m upstream |
| KEOGH RIVER | REGION 1 1-13 | rule | in all parts |
| KIKOMUN CREEK PARK (all lakes in the park) | REGION 4 4-22 | name | all lakes in the park |
| KIKOMUN CREEK PARK (all lakes in the park) | REGION 4 4-22 | name | KIKOMUN CREEK PARK |
| KOOTENAY RIVER (downstream of Idaho border) | REGION 4 4-7,4-8 | name | downstream of Idaho border |
| KOOTENAY RIVER (upstream of Koocanusa Reservoir) | REGION 4 4-2,4-21,4-22,4-24,4-25,4-35 | rule | upstream of the Montana border |
| LIGHTNING LAKE (Manning Park) | REGION 2 2-1 | name | Manning Park |
| LITTLE LAC DES ROCHES (at west end of Lac Des Roches) | REGION 3 3-30 | entry | at west end of Lac Des Roches |
| LOST LAKE (near Taweel Lake) | REGION 3 3-39 | entry | near Taweel Lake |
| MARBLE ("Link") RIVER (only between Victoria and Alice | REGION 1 1-13 | name | only between Victoria and Alice lakes |
| NICOMEKL RIVER | REGION 2 2-4 | rule | upstream of dyke gates |
| NILKITKWA LAKE | REGION 6 6-8 | rule | between Babine and Nilkitkwa Lakes |
| PITT RIVER | REGION 2 2-8 | rule | at Grant Narrows |
| POWELL LAKE | REGION 2 2-12 | rule | in One Mile Bay |
| PREMIER LAKE | REGION 4 4-21 | rule | south half only |
| SAKINAW LAKE | REGION 2 2-5 | rule | in "Bear Bay" |
| SARDIS PARK POND | REGION 2 2-4 | name | SARDIS PARK POND |
| SERPENTINE RIVER | REGION 2 2-4 | rule | upstream of dyke gates |
| SEYMOUR RIVER | REGION 1 1-14 | except | unless fishing for steelhead |
| SHUSWAP LAKE (see maps on page 28) (includes Little Sh | REGION 3 3-26 | rule | in the entire area north of Albas |
| SHUSWAP LAKE (see maps on page 28) (includes Little Sh | REGION 3 3-26 | except | anglers fishing from the community pier in the city of Salmon Arm are exempt from the closure |
| SKEENA RIVER (mainstem only) | REGION 6 6-10 | rule | in Skeena River Section 4 |
| SUMALLO RIVER (includes "Cedar" Lake, at Sunshine Vall | REGION 2 2-2 | entry | includes "Cedar" Lake, at Sunshine Valley |
| THORN CREEK | REGION 7A 7-39 | rule | from Attichika Creek to a point 500 m upstream |
| TROUT CREEK (Wells Gray Park) | REGION 3 3-46 | name | Wells Gray Park |
| TROUT LAKE (Sasquatch Park) | REGION 2 2-18 | name | Sasquatch Park |
| WEAVER LAKE and WEAVER CREEK | REGION 2 2-19 | rule | on Weaver Lake |
| WHITE RIVER (see also east White & North White Rivers) | REGION 4 4-24 | rule | on all parts |
| WILLISTON LAKE (in Zone B) | REGION 7B 7-31,7-36 | entry | in Zone B |