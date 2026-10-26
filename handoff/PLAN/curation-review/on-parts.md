# "On parts" rules — held on the whole water (owner 2026-10-26)

Made by `python -m pipeline.regs.parsing.drop_on_parts` (dry run, then applied 2026-10-26 to exactly the 33 below).
Where the book says a rule holds "on parts" of a water and never says which, the rule holds on the WHOLE water for now. The tool drops `undrawn_part`, the undrawn-part `review_reason` and a matching `unresolved_locators` entry. Needs a reach + deliver rebuild.

## Applied: 33 rules

| entry | rule | verbatim | undrawn part (dropped) | reason left |
|---|---|---|---|---|
| `r1:beaver_lake@1-1` | `beaver_lake.r3` | no towing on parts | on parts | — |
| `r1:brannen_lake@1-5` | `brannen_lake.r1` | Speed restriction on parts (60 km/h) | on parts | — |
| `r1:courtenay_river@1-6` | `courtenay_river.r2` | speed restriction on part (8 km/h) | on part | — |
| `r1:cowichan_lake_including_bear_lake@1-4` | `cowichan_lake.r4` | Speed restriction on parts (8 km/h) | on parts | — |
| `r1:cowichan_river_see_map_below@1-4` | `cowichan_river.r8` | Speed restriction (8 km/h) | on parts | — |
| `r1:cowichan_river_see_map_below@1-4` | `cowichan_river.r9` | engine power restriction - 7.5 kW (10 hp) on parts | on parts | — |
| `r1:elk_lake@1-1` | `elk_lake.r2` | Engine power restriction on parts - 7.5 kW (10 hp) | on parts | — |
| `r1:elk_lake@1-1` | `elk_lake.r3` | no vessels on parts | on parts | — |
| `r1:elk_lake@1-1` | `elk_lake.r4` | no powered boats on parts | on parts | — |
| `r1:elk_lake@1-1` | `elk_lake.r5` | no towing on parts | on parts | — |
| `r1:long_lake_nanaimo@1-5` | `long_lake.r1` | Speed restriction on parts (60 km/h) | on parts | — |
| `r1:nanaimo_river@1-5` | `nanaimo_river.r6` | Engine power restriction on parts - 7.5 kW (10 hp) | on parts | — |
| `r1:prospect_lake@1-2` | `prospect_lake.r2` | speed restriction on parts (8 and 60 km/h) | on parts | — |
| `r1:prospect_lake@1-2` | `prospect_lake.r3` | speed restriction on parts (8 and 60 km/h) | on parts | — |
| `r1:quennell_lake@1-5` | `quennell_lake.r2` | speed restriction on parts (8 km/h) | on parts | — |
| `r1:shawnigan_lake@1-2` | `shawnigan_lake.r2` | Speed restrictions on parts (8 and 65 km/h) | on parts | — |
| `r1:shawnigan_lake@1-2` | `shawnigan_lake.r3` | Speed restrictions on parts (8 and 65 km/h) | on parts | — |
| `r1:stamp_river@1-7` | `stamp_river.r5` | Engine power restriction 7.5 kW (10 hp), on parts | on parts | — |
| `r2:coquitlam_river@2-8` | `coquitlam_river.r2` | No powered boats on parts | on parts | — |
| `r2:cultus_lake@2-3` | `cultus_lake.r3` | no vessels on parts | on parts | — |
| `r2:green_lake@2-9` | `green_lake.r5` | speed restriction on parts (10 km/h) | on parts | — |
| `r2:green_lake@2-9` | `green_lake.r6` | no towing on parts | on parts | — |
| `r2:hatzic_lake_and_slough@2-8` | `hatzic_lake_and_slough.r2` | speed restriction on parts (8 and 50 km/h) | on parts | — |
| `r2:hatzic_lake_and_slough@2-8` | `hatzic_lake_and_slough.r3` | speed restriction on parts (8 and 50 km/h) | on parts | — |
| `r2:nicomekl_river@2-4` | `nicomekl_river.r5` | no towing on parts | on parts | — |
| `r2:nicomen_slough@2-8` | `nicomen_slough.r1` | Speed restriction on parts (8 km/h) | on parts | — |
| `r2:strawberry_slough@2-8` | `strawberry_slough.r1` | Speed restriction on parts (8 km/h) | on parts | — |
| `r3:heffley_lake_parts_of@3-27` | `heffley_lake.r1` | No towing | parts of the lake (not specified in the book) | — |
| `r3:heffley_lake_parts_of@3-27` | `heffley_lake.r2` | speed restriction (15 km/h) | parts of the lake (not specified in the book) | — |
| `r5:chilko_river@5-5` | `chilko_river.r6` | speed restriction (5 km/h) on part | on part | — |
| `r5:williams_lake@5-2` | `williams_lake.r1` | Speed restriction on parts (8 km/h) | on parts | — |
| `r8:osoyoos_lake@8-1` | `osoyoos_lake.r6` | Speed restriction on parts | on parts | the book prints this speed restriction with no km/h |
| `r8:wood_lake@8-22` | `wood_lake.r5` | Speed restriction (10 km/h) on parts | on parts | — |

Several waters now carry two limits of one kind on the whole water (Prospect, Shawnigan, Hatzic: "8 and 60/65/50 km/h") — the book does not say where each holds.

## Kept undrawn, close wording (names a kind of place, not a part): 10

Not applied — the owner's call.

| entry | rule | undrawn part |
|---|---|---|
| `r2:alouette_lake@2-8` | `alouette_lake.r2` | in swimming areas, as buoyed and signed |
| `r2:harrison_river_from_the_fraser_river_upstream_to_harrison_la@2-18` | `harrison_river.r3` | in small bays along the river as signed |
| `r4:columbia_river@4-15+4-26+4-34+4-38+4-8` | `columbia_river.r12` | in wetlands |
| `r8:kalamalka_lake@8-22` | `kalamalka_lake.r3` | various locations (as buoyed and signed) |
| `r8:kalamalka_lake@8-22` | `kalamalka_lake.r4` | various locations (as buoyed and signed) |
| `r8:okanagan_lake@8-10` | `okanagan_lake.r4` | various locations (as buoyed and signed) |
| `r8:okanagan_lake@8-10` | `okanagan_lake.r5` | various locations (as buoyed and signed) |
| `r8:osoyoos_lake@8-1` | `osoyoos_lake.r7` | in 5 signed swimming areas |
| `r8:skaha_lake@8-9` | `skaha_lake.r5` | various locations (as buoyed and signed) |
| `r8:skaha_lake@8-9` | `skaha_lake.r6` | various locations (as buoyed and signed) |

## Kept undrawn, names a place: 90

| entry | rule | undrawn part |
|---|---|---|
| `r1:cowichan_lake_including_bear_lake@1-4` | `cowichan_lake.r5` | within 60 m of shore |
| `r1:deena_creek@6-12` | `deena_creek.r1` | upstream of fishing boundary signs at second bridge approximately 5 km upstream of the tidal boundary |
| `r1:great_central_lake@1-7` | `great_central_lake.r1` | from the dam to fishing boundary signs approximately 50 m upstream (southwest) of the Ash Main Bridge |
| `r1:mamin_river@6-13` | `mamin_river.r3` | upstream of fishing boundary signs on third bridge approximately 10 km upstream of the tidal boundary |
| `r1:white_river@1-10` | `white_river.r1` | between fishing boundary signs at the salmon viewing pool |
| `r2:alouette_lake@2-8` | `alouette_lake.r3` | at south end of lake, south of a line drawn from the BC Parks boat ramp to signs on the east side of the lake |
| `r2:chapman_creek@2-5` | `chapman_creek.r2` | from the falls to 100 m downstream (falls are located approximately 550 m upstream of the power line crossing) |
| `r2:cultus_lake@2-3` | `cultus_lake.r4` | at north end, as buoyed and signed |
| `r2:harrison_lake@2-18` | `harrison_lake.r3` | at south end, as buoyed and signed |
| `r2:pitt_lake@2-8` | `pitt_lake.r3` | north of fishing boundary signs (east and west shores) near the head of the lake |
| `r2:pitt_lake@2-8` | `pitt_lake.r4` | north of fishing boundary signs (east and west shores) near the head of the lake |
| `r2:pitt_river@2-8` | `pitt_river.r2` | at Grant Narrows |
| `r2:powell_lake@2-12` | `powell_lake.r2` | in One Mile Bay |
| `r2:ruby_lake@2-5` | `ruby_lake.r1` | in the outlet bay within 100 m of the head of Ruby Creek |
| `r2:sakinaw_lake@2-5` | `sakinaw_lake.r1` | easterly of a line drawn from a boundary sign located at the north side of the Sakinaw Lake boat launch southwesterly to a boundary sign located at 49o 41.50'N and 123o 58.45'W |
| `r2:sakinaw_lake@2-5` | `sakinaw_lake.r2` | in "Bear Bay", easterly of a line drawn from a boundary sign located at 49o 40.05'N and 124o 02.13'W due south to a boundary sign on the opposite shore |
| `r2:stave_river@2-8` | `stave_river.r1` | Ruskin spawning channel, from the inlet near the dam downstream to the boat ramp crossing |
| `r2:stave_river@2-8` | `stave_river.r2` | Northrop Spawning Channel, from the intake downstream to where the channel joins the Stave River main, including its tributaries containg the fishway |
| `r3:adams_lake@3-37` | `adams_lake.r1` | north of a line drawn due west from mouth of Momich River |
| `r3:leighton_lake@3-18` | `leighton_lake.r1` | within 100 m of the mouth of the inlet stream |
| `r3:leighton_lake@3-18` | `leighton_lake.r2` | within 100 m of the Tunkwa Creek outlet |
| `r3:loon_lake@3-30` | `loon_lake.r1` | northeast of fishing boundary signs near the mouth of Thunder Creek and the public access site |
| `r3:loon_lake@3-30` | `loon_lake.r2` | within 500 m of outlet stream at southwest end of lake as marked by fishing boundary signs |
| `r3:mahood_lake_see_map_on_page_28_for_area_closure@3-46` | `mahood_lake.r1` | within the fishing boundary signs at the western tip of the lake near the mouth of Canim River |
| `r3:mahood_lake_see_map_on_page_28_for_area_closure@3-46` | `mahood_lake.r2` | within fishing boundary signs at the western tip of the lake |
| `r3:mahood_lake_see_map_on_page_28_for_area_closure@3-46` | `mahood_lake.r3` | within fishing boundary signs at the western tip of the lake |
| `r3:mahood_lake_see_map_on_page_28_for_area_closure@3-46` | `mahood_lake.r4` | within fishing boundary signs at the western tip of the lake |
| `r3:mahood_lake_see_map_on_page_28_for_area_closure@3-46` | `mahood_lake.r5` | within 200 m of the Mahood River outlet |
| `r3:mahood_lake_see_map_on_page_28_for_area_closure@3-46` | `mahood_lake.r6` | within 200 m of the mouth of the Mahood River outlet |
| `r3:mahood_lake_see_map_on_page_28_for_area_closure@3-46` | `mahood_lake.r7` | within 200 m of the mouth of the Mahood River outlet |
| `r3:mahood_lake_see_map_on_page_28_for_area_closure@3-46` | `mahood_lake.r8` | within 200 m of the mouth of the Mahood River outlet |
| `r3:mcarthur_island_slough@3-28` | `mcarthur_island_slough.r1` | from westerly entrance to 12th Street entrance to Park |
| `r3:roche_lake@3-20` | `roche_lake.r4` | south of a line bearing true 244° from a point on the southern tip of the largest island in the south arm of Roche Lake to the westerly shoreline and conversely 64° to the easterly shoreline |
| `r3:shumway_lake@3-20` | `shumway_lake.r1` | north of fishing boundary signs located at south end of lake |
| `r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26` | `shuswap_lake.r1` | waters lying west of a line between signs at Henstridge Road and Wharf Road to a line between signs on the south and north shores of Little Shuswap Lake (map A) |
| `r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26` | `shuswap_lake.r2` | waters lying west of a line between signs at Henstridge Road and Wharf Road to a line between signs on the south and north shores of Little Shuswap Lake (map A) |
| `r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26` | `shuswap_lake.r3` | entire area north of Albas (map B) |
| `r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26` | `shuswap_lake.r4` | east of a line between fishing boundary signs on Murdock and Semaphore points, to Hwy 1 bridge (map C) |
| `r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26` | `shuswap_lake.r5` | Salmon Arm Bay, west of line between Engineer's Point and Sunnybrae Point |
| `r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26` | `shuswap_lake.r5x` | from the community pier in the city of Salmon Arm |
| `r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26` | `shuswap_lake.r11x` | from the community pier in the city of Salmon Arm |
| `r3:white_lake@3-26` | `white_lake.r1` | within 400 m of the mouth of Cedar Creek as designated by signs |
| `r4:columbia_lake@4-25` | `columbia_lake.r3` | near eastern shore and at south end, as buoyed and signed |
| `r4:columbia_river@4-15+4-26+4-34+4-38+4-8` | `columbia_river.r11` | from Mud Lake to Columbia Lake |
| `r4:kinbasket_mcnaughton_lake@4-36` | `kinbasket_lake.r1` | within 200 m of Bush-Sullivan Road Bridge in Bush Arm |
| `r4:lake_revelstoke@4-38+4-39` | `lake_revelstoke.r1` | from Mica Dam to fishing boundary signs at the narrows immediately downstream of the mouth of Bigmouth Creek |
| `r4:premier_lake@4-21` | `premier_lake.r1` | south of fishing boundary signs on the lake shore |
| `r4:premier_lake@4-21` | `premier_lake.r4` | south half |
| `r4:premier_lake@4-21` | `premier_lake.r5` | south half |
| `r4:trout_lake@4-30` | `trout_lake.r1` | northwest of a line between fishing boundary signs on opposite shores approximately 1.5 km southeast the city of Trout Lake |
| `r4:upper_arrow_lake_drawdown_area@4-31+4-32` | `upper_arrow_lake_drawdown_area.r1b` | the drawdown area at the north end of the lake, down to the power line crossing at Akolkolex Narrows (approximately 1.5 km downstream of the Akolkolex River confluence) |
| `r4:upper_arrow_lake_drawdown_area@4-31+4-32` | `upper_arrow_lake_drawdown_area.r2b` | the drawdown area at the north end of the lake, down to the power line crossing at Akolkolex Narrows (approximately 1.5 km downstream of the Akolkolex River confluence) |
| `r4:upper_arrow_lake_drawdown_area@4-31+4-32` | `upper_arrow_lake_drawdown_area.r3b` | the drawdown area at the north end of the lake, down to the power line crossing at Akolkolex Narrows (approximately 1.5 km downstream of the Akolkolex River confluence) |
| `r5:canim_lake_see_map_on_page_42@5-1` | `canim_lake.r1` | within the waters of the small bay at the mouth of Eagle Creek northerly of a line drawn between two boundary signs located on opposite shores of the bay |
| `r5:chilko_lake@5-4` | `chilko_lake.r6` | Big Lagoon (west side of lake) |
| `r5:dragon_lake@5-2` | `dragon_lake.r1` | southeast of a line between fishing boundary signs on opposite shores of the bay at the mouth of Hallis Creek |
| `r5:green_lake@5-1` | `green_lake.r1` | northeast of line between boundary signs on opposite shores of the bay at the mouth of Watch Creek |
| `r5:mitchell_lake@5-15` | `mitchell_lake.r1` | within 100 m radius of the weir at the lake's outlet |
| `r5:quesnel_lake@5-15` | `quesnel_lake.r1` | southwest of a line between fishing boundary signs on opposite shores of Horsefly Bay |
| `r5:quesnel_lake@5-15` | `quesnel_lake.r2` | in North Arm, north of a line between Watt and Service Creeks |
| `r6:babine_lake@6-6` | `babine_lake.r1` | east of a line from Gullwing Creek to the south shore of Babine Lake |
| `r6:babine_lake@6-6` | `babine_lake.r2` | within a 400 m radius of the mouth of Pinkut Creek |
| `r6:buckinghorse_lake@6-20` | `buckinghorse_lake.r1` | within 100 m of outlet |
| `r6:cranberry_river@6-15` | `cranberry_river.r1` | between fishing boundary signs upstream of and downstream of Cranberry River Canyon |
| `r6:fran_ois_lake@6-4` | `francois_lake.r1` | the area at the outlet of François Lake described on map 1 |
| `r6:fran_ois_lake@6-4` | `francois_lake.r2` | the area at the outlet of François Lake described on map 1 |
| `r6:fran_ois_lake@6-4` | `francois_lake.r3` | the area at the outlet of François Lake described on map 1 |
| `r6:fran_ois_lake@6-4` | `francois_lake.r4` | the area at the outlet of François Lake described on map 1 |
| `r6:fran_ois_lake@6-4` | `francois_lake.r5` | the area at the outlet of François Lake described on map 1 |
| `r6:fran_ois_lake@6-4` | `francois_lake.r6` | the area at the outlet of François Lake described on map 1 |
| `r6:fran_ois_lake@6-4` | `francois_lake.r7` | the area at the outlet of François Lake described on map 1 |
| `r6:fulton_river@6-8` | `fulton_river.r3` | opposite the spawning channels, as signed |
| `r6:helene_lake@6-6` | `helene_lake.r1` | northwest of a line between fishing boundary signs on opposite shores of the outlet bay |
| `r6:klahowya_lake@6-20` | `klahowya_lake.r1` | within 100 m of outlet |
| `r6:silverthorne_erickson_lake@6-9` | `silverthorne_lake.r1` | within 50 m of the outlet |
| `r6:takysie_lake@6-4` | `takysie_lake.r1` | northwest of a line between fishing boundary signs on opposite shores immediately north of Takysie Lake Settlement |
| `r7:davis_bay_in_finlay_reach_of_williston_lake@7-37` | `davis_bay.r1` | within a 500 m radius of the Davis Forest Service Road Bridge |
| `r7:dinosaur_lake_reservoir_downstream_of_w_a_c_bennett_dam@7-31` | `dinosaur_lake.r1` | from W.A.C. Bennett Dam to 100 m south of Gething Creek and between the anti-vortex dyke and Peace Canyon Dam |
| `r7:letain_lake@7-52` | `letain_lake.r1` | within 100 m of fishing boundary sign at outlet |
| `r7:nation_arm_williston_lake@7-30` | `nation_arm.r1` | west of a line between two fishing boundary signs approximately 500 m downstream (east) of the Nation River Bridge on the Finlay Forest Service Road |
| `r7:nation_arm_williston_lake@7-30` | `nation_arm.r2` | west of a line between two fishing boundary signs approximately 500 m downstream of the Nation River Bridge on the Finlay Forest Service Road |
| `r7:nulki_lake@7-12` | `nulki_lake.r1` | west of a line between fishing boundary signs on lakeshore near mouth of Corkscrew Creek |
| `r7:rainbow_lakes@7-52` | `rainbow_lakes.r1` | within 100 m of fishing boundary sign at outlet |
| `r7:williston_lake_in_zone_a_includes_waters_500_m_east_upstream@7-30+7-37+7-38` | `williston_lake.r2` | 500 m upstream and downstream of Causeway Road |
| `r7:wolverine_lake@7-52` | `wolverine_lake.r1` | within 100 m of fishing boundary sign at outlet |
| `z7b:peace_river_navigation` | `peace_river_navigation.r1` | between 2 km upstream of the Site C dam site and the downstream construction bridge |
| `r8:christina_lake@8-15` | `christina_lake.r1` | north of a line between Bald and Knob Points |
| `r8:mabel_lake@8-24` | `mabel_lake.r1` | south of a line between fishing boundary signs on the lakeshore approximately 800 m north of Shuswap River inlet |
| `r8:mara_lake@8-26` | `mara_lake.r2` | south of the CPR bridge |
| `r8:rosemond_lake@8-26` | `rosemond_lake.r1` | south of the CPR Bridge |
