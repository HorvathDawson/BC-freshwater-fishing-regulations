# T55 v2 census: FWA lake / manmade / wetland polygons

Read-only census, one row per WATERBODY_KEY (721,352 waterbodies from 763,015 polygon rows). Sources: bc_fisheries_data.gpkg, bundle.sqlite, section_handles.txt, tiles/layers lake+wetland. "water rule" = the polygon's own section carries a rule from a water entry (r*), by reach; zone rules (z*) do not count. "drawn" = the polygon is a feature in the lake or wetland tile layer. "river through" = a NAMED Construction-Main-Flow (edge 1200) line inside whose blue line carries a stream item. Lat/lon in examples are representative points inside the polygon.

### Census by feature code

| layer | fc_label | n | drawn | lake_item | wetland_item | stream_item | rule_own | rule_from_stream | rule_from_lake | rule_area_only | named_fwa | river_through |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| lakes | Lake-Definite | 337245 | 337243 | 7425 | 0 | 131 | 837 | 4 | 89 | 38227 | 6960 | 12284 |
| wetlands | Marsh | 206432 | 206432 | 0 | 49 | 1 | 1 | 0 | 8 | 371 | 41 | 50 |
| wetlands | Swamp | 126938 | 126938 | 0 | 5 | 0 | 0 | 0 | 0 | 93 | 5 | 13 |
| lakes | Lake-Intermittent | 32717 | 32717 | 101 | 0 | 5 | 9 | 3 | 2 | 4490 | 95 | 268 |
| lakes | Lake-Indefinite | 16050 | 16050 | 61 | 0 | 5 | 1 | 0 | 0 | 5074 | 61 | 838 |
| manmade | Reservoir-Definite | 1480 | 1480 | 20 | 0 | 0 | 3 | 1 | 0 | 56 | 15 | 47 |
| manmade | Settling Basin-Sewage | 214 | 214 | 0 | 0 | 0 | 0 | 0 | 0 | 12 | 0 | 1 |
| wetlands | Flooded Land-Inundated | 156 | 156 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 4 |
| manmade | Mine-Tailing Pond | 59 | 59 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 4 |
| manmade | Reservoir-Intermittent | 34 | 34 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 1 |
| manmade | Reservoir-Indefinite | 15 | 15 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 2 |
| manmade | Canal | 8 | 8 | 0 | 0 | 4 | 0 | 2 | 0 | 0 | 3 | 0 |
| lakes | Reservoir-Definite | 2 | 1 | 2 | 0 | 0 | 1 | 0 | 0 | 0 | 2 | 1 |
| manmade | Dump | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

rule_own = a reach rule from a book entry for this polygon's own item; rule_from_stream / rule_from_lake = a reach rule from another water's entry (e.g. a river entry binding a lake polygon on its course); rule_area_only = only watershed / area entries (e.g. "Liard River watershed"), which bind every polygon inside.

### Line class by feature code (lakes + manmade)

| fc_label | lake_arm | main_connector | main_flow_unnamed | no_lines | only_1400_1450 | river_through | secondary_flow |
|---|---|---|---|---|---|---|---|
| Canal | 0 | 4 | 0 | 0 | 1 | 0 | 3 |
| Dump | 0 | 0 | 0 | 0 | 2 | 0 | 0 |
| Lake-Definite | 62 | 0 | 255040 | 2 | 67206 | 12284 | 2651 |
| Lake-Indefinite | 0 | 0 | 10924 | 0 | 4209 | 838 | 79 |
| Lake-Intermittent | 0 | 0 | 18424 | 0 | 13921 | 268 | 104 |
| Mine-Tailing Pond | 0 | 0 | 17 | 0 | 38 | 4 | 0 |
| Reservoir-Definite | 1 | 0 | 215 | 1 | 1214 | 48 | 3 |
| Reservoir-Indefinite | 0 | 0 | 2 | 0 | 11 | 2 | 0 |
| Reservoir-Intermittent | 0 | 0 | 11 | 0 | 22 | 1 | 0 |
| Settling Basin-Sewage | 0 | 0 | 10 | 0 | 203 | 1 | 0 |


## A. Lake-Indefinite and Lake-Intermittent that the app treats as lakes

All: 48,767 (16,050 indefinite, 32,717 intermittent); drawn 48,767; lake item 162; stream item 10; FWA-named 156; regulated by a named water's rule 15 (own 10, from a river entry 3, from a lake entry 2).

Named, a lake item, or carrying a water rule, by line class:

| fc_label | item_kind | main_flow_unnamed | only_1400_1450 | river_through | secondary_flow |
|---|---|---|---|---|---|
| Lake-Indefinite | lake | 41 | 7 | 13 | 0 |
| Lake-Indefinite | stream | 2 | 0 | 1 | 0 |
| Lake-Intermittent | lake | 61 | 29 | 11 | 0 |
| Lake-Intermittent | stream | 0 | 0 | 0 | 3 |

All Lake-Indefinite/Intermittent by line class (drawn as lake polygons whatever their item):

| fc_label | main_flow_unnamed | only_1400_1450 | river_through | secondary_flow |
|---|---|---|---|---|
| Lake-Indefinite | 10924 | 4209 | 838 | 79 |
| Lake-Intermittent | 18424 | 13921 | 268 | 104 |

Main flow (1200) inside vs lake-arm (1475) only, among those the app names/regulates as a lake item:

| group | n |
|---|---|
| lake_items | 162 |
| main_flow_any | 126 |
| named_main_flow | 24 |
| river_through | 24 |
| river_through_regulated | 0 |
| lake_arm_any | 0 |
| secondary_any | 0 |
| no_lines | 0 |

Examples: Lake-Indefinite/Intermittent lake items with a river through (largest):

| wbk | fc_label | n1 | item_name | item_kind | area_m2 | elong | mean_width_m | lineclass | main_flow_name | river_item | regulated | rule_entries | lat | lon |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 329400596 | Lake-Indefinite | Stevens Lakes | Stevens Lakes | lake | 1397564 | 23.03 | 236 | river_through | Snookwa Creek | Snookwa Creek | False |  | 51.95 | -120 |
| 329400604 | Lake-Indefinite | Stevens Lakes | Stevens Lakes | lake | 497452 | 10.93 | 196 | river_through | Snookwa Creek | Snookwa Creek | False |  | 51.97 | -120 |
| 329596036 | Lake-Intermittent | Billy Lake | Billy Lake | lake | 454764 | 11.44 | 183 | river_through | Billy Creek | Billy Creek | False |  | 52.33 | -124 |
| 329400609 | Lake-Indefinite | Stevens Lakes | Stevens Lakes | lake | 360190 | 32.26 | 102 | river_through | Snookwa Creek | Snookwa Creek | False |  | 51.98 | -120 |
| 329097639 | Lake-Intermittent | Bendixon Lake | Bendixon Lake | lake | 321049 | 1.94 | 268 | river_through | Bendixon Creek | Bendixon Creek | False |  | 52.89 | -122 |
| 329276414 | Lake-Indefinite | Le Blanc Lake | Le Blanc Lake | lake | 246356 | 14.97 | 120 | river_through | Pyper Creek | Pyper Creek | False |  | 52.06 | -124 |
| 329520029 | Lake-Intermittent | Kidd Lake | Kidd Lake | lake | 184056 | 1.8 | 206 | river_through | Otter Creek | Otter Creek | True | r8:kidd_lake@8-6 | 49.92 | -121 |
| 329064583 | Lake-Indefinite | Toboggan Lake | Toboggan Lake | lake | 153898 | 8.56 | 120 | river_through | Glass Creek | Glass Creek | False |  | 54.87 | -127 |
| 329069277 | Lake-Intermittent | Bednorski Lake | Bednorski Lake | lake | 151123 | 2.07 | 182 | river_through | Haha Creek | Haha Creek | True | r4:bednorski_lake@4-3 | 49.44 | -116 |
| 329069279 | Lake-Intermittent | Haha Lake | Haha Lake | lake | 146389 | 1 | 196 | river_through | Haha Creek | Haha Creek | True | r4:haha_lake@4-3 | 49.43 | -115 |

Examples: Lake-Indefinite/Intermittent lake items with no named line through (headwater / isolated basins, oxbows?):

| wbk | fc_label | n1 | item_name | item_kind | area_m2 | elong | mean_width_m | lineclass | main_flow_name | river_item | regulated | rule_entries | lat | lon |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 329328384 | Lake-Intermittent | Boundary Lake | Boundary Lake | lake | 2444079 | 5.75 | 555 | main_flow_unnamed |  |  | False |  | 56.34 | -120 |
| 329276282 | Lake-Intermittent | Kliyul Lake | Kliyul Lake | lake | 1479054 | 14.28 | 301 | main_flow_unnamed |  |  | False |  | 51.82 | -123 |
| 329095507 | Lake-Indefinite | Memory Lake | Memory Lake | lake | 988400 | 3.89 | 401 | main_flow_unnamed |  |  | False |  | 49.58 | -125 |
| 328995580 | Lake-Indefinite | Moosecall Lake | Moosecall Lake | lake | 852327 | 3.45 | 385 | main_flow_unnamed |  |  | False |  | 55.93 | -122 |
| 329430975 | Lake-Indefinite | Wutlus Lake | Wutlus Lake | lake | 584828 | 1 | 390 | main_flow_unnamed |  |  | False |  | 52.98 | -123 |
| 329532448 | Lake-Intermittent | Loch Lomond | Loch Lomond | lake | 485450 | 16.22 | 163 | main_flow_unnamed |  |  | False |  | 49.6 | -123 |
| 329241966 | Lake-Indefinite | Fiddler Lake | Fiddler Lake | lake | 441109 | 3.01 | 287 | main_flow_unnamed |  |  | False |  | 54.72 | -129 |
| 329566988 | Lake-Indefinite | Raindrop Lake | Raindrop Lake | lake | 260030 | 1 | 262 | main_flow_unnamed |  |  | False |  | 50.37 | -124 |
| 329343293 | Lake-Indefinite | Indigo Lake | Indigo Lake | lake | 246916 | 3.46 | 207 | main_flow_unnamed |  |  | False |  | 54.37 | -123 |
| 329069263 | Lake-Intermittent | Twin Lake | Twin Lake | lake | 234096 | 10.46 | 136 | main_flow_unnamed |  |  | False |  | 49.22 | -115 |

Examples: Lake-Indefinite/Intermittent with any Lake Arm (1475) line:

_(none)_

Examples: Lake-Indefinite/Intermittent, NOT items, river through, elongated (>= 4), drawn as lake:

| wbk | fc_label | n1 | item_name | item_kind | area_m2 | elong | mean_width_m | lineclass | main_flow_name | river_item | regulated | rule_entries | lat | lon |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 329220249 | Lake-Intermittent |  |  | none | 1918692 | 18.27 | 307 | river_through | McBeath Creek | McBeath Creek | False |  | 51.27 | -117 |
| 329026703 | Lake-Intermittent |  |  | none | 903168 | 10.05 | 273 | river_through | Byron Creek | Byron Creek | False |  | 54.86 | -127 |
| 329316094 | Lake-Intermittent |  |  | none | 229417 | 13.9 | 120 | river_through | Conant Creek | Conant Creek | False |  | 50.27 | -121 |
| 328964302 | Lake-Intermittent |  |  | none | 225832 | 12.51 | 124 | river_through | Eleven Mile Creek | Eleven Mile Creek | False |  | 51.24 | -122 |
| 329079735 | Lake-Intermittent |  |  | none | 194357 | 6.78 | 148 | river_through | Brittany Creek | Brittany Creek | False |  | 51.78 | -124 |
| 328988518 | Lake-Indefinite |  |  | none | 177104 | 26.65 | 78.6 | river_through | Shekilie River | Shekilie River | False |  | 59.36 | -120 |
| 328989278 | Lake-Indefinite |  |  | none | 139234 | 5.84 | 132 | river_through | Wapiti River | Wapiti River | False |  | 54.45 | -121 |
| 329276526 | Lake-Intermittent |  |  | none | 127027 | 7.24 | 116 | river_through | Minton Creek | Minton Creek | False |  | 51.8 | -123 |
| 329276534 | Lake-Intermittent |  |  | none | 121176 | 4.26 | 137 | river_through | Minton Creek | Minton Creek | False |  | 51.79 | -123 |
| 329529512 | Lake-Intermittent |  |  | none | 120621 | 6.84 | 116 | river_through | Will Creek | Will Creek | False |  | 57.53 | -129 |

Examples: Lake-Indefinite/Intermittent, not items, a RIVER ENTRY's rule binds the polygon (regulated as part of the river, drawn as lake):

_(none)_

Counts: not-an-item and a river entry binds it: 0; of those river_through: 0.


## B. Sewage settling basins, tailing ponds, dumps

| fc_label | n | drawn | has_ruleset | any_item | named | regulated | rule_area | water_rule_trib | gauge | lines_inside | river_through |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Dump | 2 | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| Mine-Tailing Pond | 59 | 59 | 59 | 0 | 0 | 0 | 0 | 0 | 0 | 59 | 4 |
| Settling Basin-Sewage | 214 | 214 | 214 | 0 | 0 | 0 | 12 | 0 | 0 | 214 | 1 |

12 have an item, a name, a non-zone water rule (any source, reach or trib) or a gauge:

| wbk | fc_label | n1 | item_name | item_kind | regulated | rule_area | rule_entries | water_rule_trib | gauge | tile_water | tile_minzoom | lat | lon |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 329311812 | Settling Basin-Sewage |  |  | none | False | True |  | False |  | lake | 12 | 58.83 | -123 |
| 328977504 | Settling Basin-Sewage |  |  | none | False | True |  | False |  | lake | 12 | 58.83 | -123 |
| 329311850 | Settling Basin-Sewage |  |  | none | False | True |  | False |  | lake | 12 | 58.79 | -123 |
| 328977517 | Settling Basin-Sewage |  |  | none | False | True |  | False |  | lake | 12 | 58.81 | -123 |
| 329312106 | Settling Basin-Sewage |  |  | none | False | True |  | False |  | lake | 12 | 58.81 | -123 |
| 328977523 | Settling Basin-Sewage |  |  | none | False | True |  | False |  | lake | 12 | 58.81 | -123 |
| 329375568 | Settling Basin-Sewage |  |  | none | False | True |  | False |  | lake | 12 | 58.66 | -123 |
| 328977529 | Settling Basin-Sewage |  |  | none | False | True |  | False |  | lake | 12 | 58.81 | -123 |
| 329375939 | Settling Basin-Sewage |  |  | none | False | True |  | False |  | lake | 13 | 58.74 | -123 |
| 329312761 | Settling Basin-Sewage |  |  | none | False | True |  | False |  | lake | 13 | 58.83 | -123 |
| 329312806 | Settling Basin-Sewage |  |  | none | False | True |  | False |  | lake | 13 | 58.83 | -123 |
| 329376416 | Settling Basin-Sewage |  |  | none | False | True |  | False |  | lake | 14 | 58.74 | -123 |

Examples: largest drawn ones:

| wbk | fc_label | n1 | item_name | item_kind | area_m2 | elong | mean_width_m | lineclass | main_flow_name | river_item | regulated | rule_entries | lat | lon | tile_minzoom |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 329412991 | Mine-Tailing Pond |  |  | none | 2356485 | 4.83 | 579 | main_flow_unnamed |  |  | False |  | 52.55 | -122 | 8 |
| 329459124 | Mine-Tailing Pond |  |  | none | 2310344 | 4.54 | 585 | river_through | MacDonald Creek | MacDonald Creek | False |  | 49.86 | -120 | 8 |
| 329563839 | Mine-Tailing Pond |  |  | none | 1593937 | 3.16 | 539 | river_through | Pukaist Creek | Pukaist Creek | False |  | 50.56 | -121 | 8 |
| 329026696 | Mine-Tailing Pond |  |  | none | 1053109 | 5.56 | 369 | only_1400_1450 |  |  | False |  | 54.98 | -126 | 9 |
| 329563841 | Mine-Tailing Pond |  |  | none | 927299 | 5.96 | 338 | river_through | Alkali Creek | Alkali Creek | False |  | 50.65 | -121 | 9 |
| 329064479 | Mine-Tailing Pond |  |  | none | 859910 | 1.81 | 444 | only_1400_1450 |  |  | False |  | 54.2 | -126 | 9 |
| 329563849 | Settling Basin-Sewage |  |  | none | 371556 | 1.31 | 302 | only_1400_1450 |  |  | False |  | 50.69 | -120 | 9 |
| 329563859 | Mine-Tailing Pond |  |  | none | 293169 | 9.33 | 160 | main_flow_unnamed |  |  | False |  | 50.51 | -121 | 9 |
| 328991047 | Settling Basin-Sewage |  |  | none | 269195 | 18.36 | 115 | only_1400_1450 |  |  | False |  | 50.69 | -120 | 10 |
| 329354794 | Settling Basin-Sewage |  |  | none | 229714 | 4.33 | 187 | main_flow_unnamed |  |  | False |  | 52.09 | -121 | 10 |


## C. Canals and ditches

Manmade canal polygons: 8

| wbk | fc_label | n1 | item_name | item_kind | book_entries | lineclass | regulated | rule_entries | lat | lon |
|---|---|---|---|---|---|---|---|---|---|---|
| 329246329 | Canal |  |  | none |  | secondary_flow | False |  | 49.46 | -117 |
| 329707189 | Canal | Vedder Canal | Vedder River | stream | r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4/r2:vedder_river@2-4 | main_connector | True | r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4 | 49.11 | -122 |
| 329707192 | Canal | Sumas Lake Canal | Sumas Lake Canal | stream |  | main_connector | False |  | 49.09 | -122 |
| 328988335 | Canal |  | Seton Canal | stream | r3:seton_river_includes_bc_hydro_power_canal_upstream_of_the_da@3-16 | secondary_flow | True | r3:seton_river_includes_bc_hydro_power_canal_upstream_of_the_da@3-16 | 50.67 | -122 |
| 328997872 | Canal | Sumas River | Sumas River | stream |  | main_connector | False |  | 49.11 | -122 |
| 329292019 | Canal |  |  | none |  | secondary_flow | False |  | 49.12 | -123 |
| 329708509 | Canal |  |  | none |  | main_connector | False |  | 50.25 | -117 |
| 329506580 | Canal |  |  | none |  | only_1400_1450 | False |  | 50.67 | -122 |

Canal-coded polygons in any layer (feature codes mixed per key):

| layer | item_kind | n |
|---|---|---|
| manmade | none | 4 |
| manmade | stream | 4 |

Ditch/canal LINES (streams layer GA08800110 / GA03950000): 2,864 lines on 1,572 blue lines, 1,475 km. Named blue lines: 112.

| fc | stream_item | blks | km | named | with_sections | water_rule | book_entry | entry_by_name |
|---|---|---|---|---|---|---|---|---|
| GA03950000 | False | 20 | 20 | 0 | 11 | 0 | 0 | 0 |
| GA03950000 | True | 1 | 2 | 0 | 1 | 1 | 1 | 0 |
| GA03950000,GA08800110 | False | 1 | 5 | 1 | 0 | 0 | 0 | 1 |
| GA08800110 | False | 1357 | 1160 | 1 | 1072 | 0 | 0 | 1 |
| GA08800110 | True | 193 | 288 | 110 | 193 | 23 | 30 | 14 |

Named ditch/canal blue lines that are NOT a stream item (longest):

| blk | name | fc | edges | n | m | sections | items | item_kinds | entry_by_name | lat | lon |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 356328804 | Still Creek | GA03950000,GA08800110 | 2000 | 7 | 5212 | 0 |  |  | r2:still_creek@2-8 | 49 | -123 |
| 356013125 | Nelson Creek | GA08800110 | 2000 | 1 | 1052 | 0 |  |  | r2:nelson_creek@2-8 | 49 | -123 |

Ditch/canal blue lines a book entry names (via item or by name):

| blk | name | fc | n | m | items | item_kinds | water_rule | book_entries | entry_by_name |
|---|---|---|---|---|---|---|---|---|---|
| 356558770 | Deep Creek | GA08800110 | 24 | 15718 | Deep Creek | stream | False |  | r6:deep_creek@6-15 |
| 356538976 |  | GA08800110 | 6 | 7139 | French Slough | stream | True | r4:french_slough@4-7 |  |
| 356328804 | Still Creek | GA03950000,GA08800110 | 7 | 5212 |  |  | False |  | r2:still_creek@2-8 |
| 354149285 | Bear Creek | GA08800110 | 2 | 3050 | Bear Creek | stream | False |  | r2:bear_mahood_creek@2-4 |
| 356360324 | Monte Creek | GA08800110 | 8 | 2984 | Monte Creek | stream | True | r3:monte_creek@3-20 | r3:monte_creek@3-20 |
| 355995609 |  | GA08800110 | 8 | 2824 | Serpentine River | stream | True | r2:serpentine_river@2-4 |  |
| 356366076 |  | GA03950000 | 6 | 2273 | Kootenay River | stream | True | r4:kootenay_river_downstream_of_idaho_border@4-7+4-8/r4:kootenay_river_upstream_of_koocanusa_reservoir@4-2+4-21+4-22+4-24+4-25+4-35 |  |
| 356365726 |  | GA08800110 | 5 | 2267 | Kootenay River | stream | True | r4:kootenay_river_downstream_of_idaho_border@4-7+4-8/r4:kootenay_river_upstream_of_koocanusa_reservoir@4-2+4-21+4-22+4-24+4-25+4-35 |  |
| 356365735 |  | GA08800110 | 4 | 1983 | Kootenay River | stream | True | r4:kootenay_river_downstream_of_idaho_border@4-7+4-8/r4:kootenay_river_upstream_of_koocanusa_reservoir@4-2+4-21+4-22+4-24+4-25+4-35 |  |
| 356565201 | Joseph Creek | GA08800110 | 2 | 1631 | Joseph Creek | stream | False | r4:joseph_creek@4-3 | r4:joseph_creek@4-3 |
| 355997046 |  | GA08800110 | 5 | 1580 | Salmon River | stream | False | r3:salmon_river@3-26 |  |
| 355993717 |  | GA08800110 | 2 | 1576 | Serpentine River | stream | True | r2:serpentine_river@2-4 |  |
| 356366893 |  | GA08800110 | 2 | 1561 | Trout Creek | stream | True | r8:trout_creek@8-8 |  |
| 355993718 |  | GA08800110 | 4 | 1556 | Serpentine River | stream | True | r2:serpentine_river@2-4 |  |
| 355996889 |  | GA08800110 | 2 | 1413 | Serpentine River | stream | True | r2:serpentine_river@2-4 |  |
| 356566082 | Kitchener Creek | GA08800110 | 10 | 1308 | Kitchener Creek | stream | True | r4:kitchener_meadow_creek@4-6 | r4:kitchener_meadow_creek@4-6 |
| 355992376 |  | GA08800110 | 6 | 1150 | Mahood Creek | stream | False | r2:bear_mahood_creek@2-4 |  |
| 359004004 |  | GA08800110 | 3 | 1128 | Halfway River | stream | True | r7:halfway_river@7-34 |  |
| 356366338 |  | GA08800110 | 2 | 1090 | Kootenay River | stream | True | r4:kootenay_river_downstream_of_idaho_border@4-7+4-8/r4:kootenay_river_upstream_of_koocanusa_reservoir@4-2+4-21+4-22+4-24+4-25+4-35 |  |
| 356013125 | Nelson Creek | GA08800110 | 1 | 1052 |  |  | False |  | r2:nelson_creek@2-8 |
| 355995401 |  | GA08800110 | 2 | 942 | Lillooet River | stream | True | r2:lillooet_lake_lillooet_river@2-9 |  |
| 356360953 | Summit Creek | GA08800110 | 5 | 903 | Summit Creek | stream | False |  | r4:summit_creek@4-7 |
| 354136336 | Bear Creek | GA08800110 | 1 | 885 | Bear Creek | stream | False |  | r2:bear_mahood_creek@2-4 |
| 355998550 |  | GA08800110 | 2 | 831 | Nicomekl River | stream | True | r2:nicomekl_river@2-4 |  |
| 356317779 | Marshall Creek | GA08800110 | 1 | 737 | Marshall Creek | stream | True | r2:lonzo_marshall_creek@2-4/r2:marshall_creek@2-4 | r2:marshall_creek@2-4 |
| 355992379 |  | GA08800110 | 1 | 719 | Mahood Creek | stream | False | r2:bear_mahood_creek@2-4 |  |
| 354088905 |  | GA08800110 | 1 | 690 | Quinsam River | stream | True | r1:quinsam_river@1-6 |  |
| 355992377 |  | GA08800110 | 1 | 581 | Mahood Creek | stream | False | r2:bear_mahood_creek@2-4 |  |
| 356341287 | Clinton Creek | GA08800110 | 3 | 561 | Clinton Creek | stream | True | r3:clinton_creek@3-30 | r3:clinton_creek@3-30 |
| 354146159 | Colquitz River | GA08800110 | 1 | 540 | Colquitz River | stream | True | r1:colquitz_river@1-1 | r1:colquitz_river@1-1 |
| 356360570 | Willow River | GA08800110 | 1 | 461 | Willow River | stream | True | r7:willow_river@7-9 | r7:willow_river@7-9 |
| 355992378 |  | GA08800110 | 1 | 422 | Mahood Creek | stream | False | r2:bear_mahood_creek@2-4 |  |
| 356001942 |  | GA08800110 | 1 | 404 | Serpentine River | stream | True | r2:serpentine_river@2-4 |  |
| 356300497 | Ptarmigan Creek | GA08800110 | 1 | 388 | Ptarmigan Creek | stream | False |  | r7:ptarmigan_creek@7-5 |
| 356002170 |  | GA08800110 | 1 | 329 | Salmon River | stream | False | r3:salmon_river@3-26 |  |
| 356222061 | Fergus Creek | GA08800110 | 2 | 311 | Fergus Creek | stream | True | r2:fergus_creek@2-4 | r2:fergus_creek@2-4 |
| 356535230 | Norbury Creek | GA08800110 | 1 | 304 | Norbury Creek | stream | True | r4:norbury_little_bull_creek@4-22 | r4:norbury_little_bull_creek@4-22 |
| 356365516 |  | GA08800110 | 1 | 114 | Kootenay River | stream | True | r4:kootenay_river_downstream_of_idaho_border@4-7+4-8/r4:kootenay_river_upstream_of_koocanusa_reservoir@4-2+4-21+4-22+4-24+4-25+4-35 |  |


## D. Reservoirs

| fc_label | item_kind | n | drawn | named | river_through | river_regulated | regulated | rule_from_stream | book_entry |
|---|---|---|---|---|---|---|---|---|---|
| Reservoir-Definite | lake | 22 | 21 | 17 | 9 | 3 | 5 | 1 | 5 |
| Reservoir-Definite | none | 1460 | 1460 | 0 | 39 | 7 | 0 | 0 | 0 |
| Reservoir-Indefinite | none | 15 | 15 | 0 | 2 | 1 | 0 | 0 | 0 |
| Reservoir-Intermittent | none | 34 | 34 | 0 | 1 | 0 | 0 | 0 | 0 |

| fc_label | lake_arm | main_flow_unnamed | no_lines | only_1400_1450 | river_through | secondary_flow |
|---|---|---|---|---|---|---|
| Reservoir-Definite | 1 | 215 | 1 | 1214 | 48 | 3 |
| Reservoir-Indefinite | 0 | 2 | 0 | 11 | 2 | 0 |
| Reservoir-Intermittent | 0 | 11 | 0 | 22 | 1 | 0 |

Examples: reservoir lake items with a regulated river running through ("dammed area"):

| wbk | fc_label | n1 | item_name | item_kind | area_m2 | elong | mean_width_m | lineclass | main_flow_name | river_item | regulated | rule_entries | lat | lon | through_ratio | book_entries |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 329657308 | Reservoir-Definite | Dinosaur Lake | Dinosaur Lake | lake | 8603816 | 84.15 | 316 | river_through | Peace River | Peace River | True | r7:dinosaur_lake_reservoir_downstream_of_w_a_c_bennett_dam@7-31 | 55.97 | -122 | 0.8 | r7:dinosaur_lake_reservoir_downstream_of_w_a_c_bennett_dam@7-31 |
| 329459141 | Reservoir-Definite | Thirsk Lake | Thirsk Lake | lake | 484450 | 6.98 | 230 | river_through | Trout Creek | Trout Creek | True | r8:thirsk_lake@8-8 | 49.71 | -120 | 0.62 | r8:thirsk_lake@8-8 |
| 329262640 | Reservoir-Definite |  | Waneta Reservoir | lake | 190943 | 4.52 | 168 | river_through | Pend-d'Oreille River | Pend-d'Oreille River | True | r4:pend_d_oreille_river_includes_the_reservoirs_behind_waneta_d@4-8 | 49 | -118 | 0.75 | r4:pend_d_oreille_river_includes_the_reservoirs_behind_waneta_d@4-8/r4:waneta_reservoir@4-8 |

Examples: reservoirs NOT items that a river entry's rule binds:

_(none)_

Examples: reservoirs with a river through and NO item (drawn as plain lake):

| wbk | fc_label | n1 | item_name | item_kind | area_m2 | elong | mean_width_m | lineclass | main_flow_name | river_item | regulated | rule_entries | lat | lon | through_ratio |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 329316106 | Reservoir-Definite |  |  | none | 178210 | 8.24 | 131 | river_through | Kanevale Creek | Kanevale Creek | False |  | 49.93 | -121 | 0.58 |
| 329586405 | Reservoir-Definite |  |  | none | 131350 | 4.91 | 136 | river_through | South Hawks Creek | South Hawks Creek | False |  | 52.21 | -122 | 0.72 |
| 329069292 | Reservoir-Definite |  |  | none | 117005 | 9.41 | 101 | river_through | Bull River | Bull River | False |  | 49.5 | -115 | 0.9 |
| 329262674 | Reservoir-Definite |  |  | none | 85911 | 4.2 | 116 | river_through | Cambridge Creek | Cambridge Creek | False |  | 49.06 | -118 | 0.85 |
| 329123476 | Reservoir-Definite |  |  | none | 66441 | 1.31 | 128 | river_through | Greenhills Creek | Greenhills Creek | False |  | 50.05 | -115 | 0.54 |
| 329664094 | Reservoir-Definite |  |  | none | 49573 | 6.52 | 75.6 | river_through | Shuswap River | Shuswap River | False |  | 50.29 | -119 | 0.75 |
| 329065075 | Reservoir-Definite |  |  | none | 39376 | 1 | 99.8 | river_through | Lu Creek | Lu Creek | False |  | 54.21 | -126 | 0.81 |
| 329481406 | Reservoir-Definite |  |  | none | 29406 | 2.26 | 79.1 | river_through | Wiggins Creek | Wiggins Creek | False |  | 52.29 | -122 | 0.69 |
| 329055368 | Reservoir-Definite |  |  | none | 23215 | 3.81 | 61.8 | river_through | Clinton Creek | Clinton Creek | False |  | 51.13 | -122 | 0.39 |
| 329481655 | Reservoir-Definite |  |  | none | 19563 | 3.31 | 59.1 | river_through | Wiggins Creek | Wiggins Creek | False |  | 52.28 | -122 | 0.8 |

Examples: reservoirs already folded into a stream item:

_(none)_


## E. Lake-arm evidence across the 865 candidates and the owner's marks

Candidates joined to the census: 865 of 865.

| class | blueline_1000s | lake_arm | main_connector | main_flow_unnamed | no_lines | only_1400_1450 | river_through | secondary_flow | All |
|---|---|---|---|---|---|---|---|---|---|
| LAKE | 9 | 2 | 0 | 44 | 0 | 3 | 158 | 17 | 233 |
| LIKELY_STREAM | 7 | 0 | 0 | 16 | 1 | 0 | 119 | 93 | 236 |
| STREAM_BY_BOOK | 0 | 0 | 0 | 3 | 0 | 2 | 25 | 35 | 65 |
| UNSURE | 150 | 0 | 1 | 44 | 0 | 12 | 98 | 26 | 331 |
| All | 166 | 2 | 1 | 107 | 1 | 17 | 400 | 171 | 865 |

Presence of each line code (count of polygons with any line of that edge type) by candidate class:

| class | n | any_1200 | named_1200 | river_through | lake_arm_1475 | main_conn_1250 | secondary_1300 | side_1350 | e1400 | e1450 | bl_1000s | regulated | rule_from_stream |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| LAKE | 233 | 204 | 158 | 158 | 2 | 0 | 17 | 0 | 3 | 90 | 11 | 6 | 0 |
| LIKELY_STREAM | 236 | 135 | 119 | 119 | 0 | 0 | 92 | 1 | 0 | 128 | 8 | 0 | 0 |
| STREAM_BY_BOOK | 65 | 28 | 25 | 25 | 0 | 0 | 34 | 1 | 2 | 22 | 0 | 0 | 0 |
| UNSURE | 331 | 142 | 98 | 98 | 0 | 1 | 26 | 0 | 12 | 69 | 153 | 40 | 0 |

Same, by owner verdict:

| verdict | n | any_1200 | named_1200 | river_through | lake_arm_1475 | main_conn_1250 | secondary_1300 | side_1350 | e1400 | e1450 | bl_1000s | regulated | rule_from_stream |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| not_stream | 5 | 4 | 1 | 1 | 0 | 0 | 1 | 0 | 0 | 3 | 0 | 0 | 0 |
| note-only | 1 | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| stream | 89 | 42 | 39 | 39 | 0 | 0 | 40 | 1 | 2 | 30 | 3 | 0 | 0 |
| unsure | 3 | 3 | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 3 | 0 | 0 | 0 |

Province-wide, for scale (lakes layer):

| fc_label | n | any_1200 | named_1200 | river_through | lake_arm_1475 | main_conn_1250 | secondary_1300 | side_1350 | e1400 | e1450 | bl_1000s | regulated | rule_from_stream |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Lake-Definite | 337245 | 267385 | 12284 | 12284 | 88 | 1 | 2651 | 0 | 67204 | 48815 | 1 | 922 | 4 |
| Lake-Indefinite | 16050 | 11762 | 838 | 838 | 0 | 0 | 79 | 0 | 4209 | 1551 | 0 | 1 | 0 |
| Lake-Intermittent | 32717 | 18692 | 268 | 268 | 0 | 0 | 104 | 0 | 13921 | 1556 | 0 | 14 | 3 |
| Reservoir-Definite | 2 | 2 | 1 | 1 | 1 | 0 | 0 | 0 | 0 | 2 | 0 | 1 | 0 |

Owner marks (98) by line class:

| verdict | blueline_1000s | main_flow_unnamed | no_lines | only_1400_1450 | river_through | secondary_flow | All |
|---|---|---|---|---|---|---|---|
| not_stream | 0 | 3 | 0 | 0 | 1 | 1 | 5 |
| note-only | 0 | 0 | 0 | 0 | 0 | 1 | 1 |
| stream | 3 | 3 | 1 | 2 | 39 | 41 | 89 |
| unsure | 0 | 0 | 0 | 0 | 3 | 0 | 3 |
| All | 3 | 6 | 1 | 2 | 43 | 43 | 98 |

Owner marks by FWA feature code:

| verdict | Canal | Flooded Land-Inundated | Lake-Definite | Lake-Indefinite | Lake-Intermittent | Marsh | All |
|---|---|---|---|---|---|---|---|
| not_stream | 0 | 0 | 5 | 0 | 0 | 0 | 5 |
| note-only | 0 | 0 | 1 | 0 | 0 | 0 | 1 |
| stream | 2 | 1 | 79 | 2 | 2 | 3 | 89 |
| unsure | 0 | 0 | 3 | 0 | 0 | 0 | 3 |
| All | 2 | 1 | 88 | 2 | 2 | 3 | 98 |

Median metrics by verdict:

| verdict | n | elong | width | area | through | river_through | len1200 | len1475 | len1300 |
|---|---|---|---|---|---|---|---|---|---|
| not_stream | 5 | 6.97 | 50.8 | 41794 | 0 | 0.2 | 0.8 | 0 | 0.2 |
| note-only | 1 | 1.7 | 9.8 | 415 | 0 | 0 | 0 | 0 | 1 |
| stream | 89 | 11.36 | 23.1 | 6443 | 0 | 0.44 | 0.47 | 0 | 0.45 |
| unsure | 3 | 14.54 | 45.4 | 35129 | 0.81 | 1 | 1 | 0 | 0 |

Every non-stream / unsure / note mark:

| id | verdict | note | stream_name | fc_label | item_kind | item_name | lineclass | elong | mean_width_m | area_m2 | main_flow_name | rule_entries | lines_sig | lat | lon |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 329292341 | stream | part of williamson slough nearby | Fraser River | Lake-Definite | none |  | secondary_flow | 15.47 | 22.2 | 8651 |  |  | {"WA24111130/1300": [1, 373]} | 49.1 | -123 |
| 329292598 | not_stream |  | Coquitlam River | Lake-Definite | none |  | main_flow_unnamed | 6.97 | 21 | 4032 |  |  | {"WA24111110/1200": [1, 182]} | 49.55 | -123 |
| 329291987 | stream | this polygon is fuller slough | Ewen Slough | Lake-Definite | none |  | only_1400_1450 | 11.61 | 61.5 | 51811 |  |  | {"WA24111170/1400": [1, 797]} | 49.1 | -123 |
| 329037255 | not_stream |  | Big Bar Creek | Lake-Definite | none |  | river_through | 14.99 | 50.8 | 43944 | Big Bar Creek |  | {"WA24111110/1200": [4, 811], "WA24111190/1450": [3, 84]} | 51.35 | -122 |
| 329247848 | not_stream |  | Kootenay River | Lake-Definite | none |  | main_flow_unnamed | 10.21 | 69.8 | 59999 |  |  | {"WA24111110/1200": [2, 801], "WA24111190/1450": [1, 27]} | 51.07 | -116 |
| 329070300 | unsure | im like a tiny bit unsure but stream is stricter regs. iut is a dammed area | Quinsam River | Lake-Definite | none |  | river_through | 14.54 | 27.2 | 12290 | Quinsam River |  | {"WA24111110/1200": [3, 417], "WA24111190/1450": [2, 42]} | 49.93 | -126 |
| 328966554 | unsure | dammed area probnably a lake? | South Nanaimo River | Lake-Definite | none |  | river_through | 11.75 | 113 | 176499 | South Nanaimo River |  | {"WA24111110/1200": [4, 1262], "WA24111190/1450": [3, 242]} | 49.06 | -124 |
| 329070659 | not_stream |  | Elk River | Lake-Definite | none |  | main_flow_unnamed | 4.91 | 21.8 | 3374 |  |  | {"WA24111110/1200": [2, 128], "WA24111190/1450": [1, 11]} | 49.74 | -126 |
| 329016376 | not_stream | this could actually be labelled as sproat lake too | Sproat River | Lake-Definite | none |  | secondary_flow | 3.05 | 88.1 | 41794 |  |  | {"WA24111130/1300": [1, 296]} | 49.26 | -125 |
| 329097416 | note-only | similar this can be on moat lake | Cruickshank River | Lake-Definite | none |  | secondary_flow | 1.7 | 9.8 | 415 |  |  | {"WA24111130/1300": [1, 37]} | 49.68 | -125 |
| 329567618 | stream | likely a slough here?  | Tahumming River | Lake-Definite | none |  | secondary_flow | 10.13 | 23.1 | 6548 |  |  | {"WA24111130/1300": [1, 256]} | 50.58 | -124 |
| 329055141 | unsure | probably a lake | Bonaparte River | Lake-Definite | none |  | river_through | 14.95 | 45.4 | 35129 | Bonaparte River |  | {"WA24111110/1200": [2, 37], "WA24111190/1450": [1, 676]} | 51.35 | -121 |

Median metrics by candidate class:

| class | n | elong | width | through | river_through | len1475 |
|---|---|---|---|---|---|---|
| LAKE | 233 | 2.25 | 27.5 | 0.53 | 0.68 | 0.01 |
| LIKELY_STREAM | 236 | 12.96 | 27.8 | 0.06 | 0.5 | 0 |
| STREAM_BY_BOOK | 65 | 11.74 | 22.1 | 0 | 0.38 | 0 |
| UNSURE | 331 | 11.27 | 53.6 | 0 | 0.3 | 0 |


## F. Flooded land and Lake-Intermittent on a river: stream candidates

| fc_label | item_kind | river_regulated | n | drawn | in_candidates | regulated | rule_area |
|---|---|---|---|---|---|---|---|
| Flooded Land-Inundated | none | False | 155 | 155 | 1 | 0 | 0 |
| Flooded Land-Inundated | none | True | 1 | 1 | 1 | 0 | 0 |
| Lake-Intermittent | lake | False | 11 | 11 | 0 | 4 | 0 |
| Lake-Intermittent | none | False | 257 | 257 | 1 | 0 | 18 |

Flooded land by line class:

| lineclass | item_kind | n |
|---|---|---|
| blueline_1000s | none | 80 |
| main_flow_unnamed | none | 39 |
| no_lines | none | 19 |
| only_1400_1450 | none | 14 |
| river_through | none | 4 |

Examples: flooded land with a river through (largest):

| wbk | fc_label | n1 | item_name | item_kind | area_m2 | elong | mean_width_m | lineclass | main_flow_name | river_item | regulated | rule_entries | lat | lon | through_ratio |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 328944409 | Flooded Land-Inundated |  |  | none | 131396 | 38.2 | 57.2 | river_through | Beedy Creek | Beedy Creek | False |  | 52.46 | -122 | 0.07 |
| 329229787 | Flooded Land-Inundated |  |  | none | 103086 | 15.95 | 75.7 | river_through | Kitseguecla River | Kitseguecla River | False |  | 54.93 | -128 | 0.93 |
| 329098901 | Flooded Land-Inundated |  |  | none | 12648 | 3.35 | 47.3 | river_through | Bellos Creek | Bellos Creek | False |  | 53.13 | -122 | 0.81 |
| 329548600 | Flooded Land-Inundated |  |  | none | 6057 | 8.73 | 23.6 | river_through | Brodman Creek | Brodman Creek | False |  | 53.8 | -123 | 0.95 |

Examples: Lake-Intermittent with a regulated river through AND bound by that river entry rule, not yet a stream item (largest):

_(none)_

Not yet in the 865 candidates: Lake-Intermittent / Flooded land, regulated river through, not a stream item:

0 waterbodies.

_(none)_
