# DFO extent suggestions (T164 / T210 follow-up)

Suggestions only — nothing applied. Open a location in the review app's DFO mode: the
suggestion is shown and drawn (violet); **accept suggestion** pre-fills the extent editor;
**Save extent** records it as your mark. Regenerate with
`python -m pipeline.regs.dfo_salmon.suggest` (reads registry, splits.json, marks, FWA gpkg).

200 locations without a curated extent:

- matched to existing cuts / your proposals: **67**
- whole water (no cut needed): **43**
- with a NEW split — high: **4**
- with a NEW split — medium: **3**
- with a NEW split — low: **0**
- partial (one end unresolved): **15**
- no suggestion: **68**

| location | suggestion | conf | reason |
|---|---|---|---|
| `1:campbell-river:downstream-of-the-confluence-with` | downstream of Quinsam River → (proposed) | high | high: confluence of Quinsam River: the proposed cut `n2` (Quinsam River → (proposed)) |
| `1:campbell-river:downstream-of-the-confluence-with~2` | between Quinsam River → (proposed) and Maple Street boundary sign | high | high: confluence of Quinsam River: the proposed cut `n2` (Quinsam River → (proposed)); high: "maple, street" -> the curated cut `campbell_river__maple_street_bo |
| `1:campbell-river:from-a-fishing-boundary-sign-at-th` | between Maple Street boundary sign and cement block boundary sign | high | high: "maple, street" -> the curated cut `campbell_river__maple_street_boundary_sign` (Maple Street boundary sign); high: "block, cement" -> the curated cut `ca |
| `1:cowichan-river:downstream-of-the-weir-on-cowichan` | between Cowichan Lake and CNR Mile 66 Trestle | high | high: Cowichan Lake: the curated cut `cowichan_river__cowichan_lake` (Cowichan Lake); high: "66, mile, trestle" -> the curated cut `cowichan_river__cnr_trestle` |
| `1:cowichan-river:from-66-mile-trestle-downstream-to` | between CNR Mile 66 Trestle and white triangle boundary sign located in Sandy Pool Regional Park | high | high: "66, mile, trestle" -> the curated cut `cowichan_river__cnr_trestle` (CNR Mile 66 Trestle); high: "park, pool, regional, sandy" -> the proposed cut `n1` ( |
| `1:cowichan-river:from-the-white-triangle-boundary-s` | downstream of white triangle boundary sign located in Sandy Pool Regional Park | high | high: "park, pool, regional, sandy" -> the proposed cut `n1` (white triangle boundary sign located in Sandy Pool Regional Park); the other end is the water's mo |
| `1:little-qualicum-river:from-the-hatchery-fence-to-signs-l` | between hatchery fence and signs 35 m downstream of hatchery fence | medium | medium: "fence, hatchery" -> the curated cut `little_qualicum_river__hatchery_fence` (hatchery fence); medium: "35m-down, downstream" -> the curated cut `little |
| `1:nanaimo-river:cedar-road-bridge-upstream-approx` | between Cedar Road bridge and Hwy 19 bridge | high | high: "bridge, cedar, road" -> the curated cut `nanaimo_river__cedar_road_bridge` (Cedar Road bridge); high: "19, bridge, hwy" -> the curated cut `nanaimo_river |
| `1:nanaimo-river:from-500-m-below-the-lower-power-l` | between power line at Bore Hole and Boulder Creek → Nanaimo River | medium | medium: "bore, hole, powerline" -> the curated cut `nanaimo_river__power_line_at_bore_hole` (power line at Bore Hole); the notice's place is 500 m below it; hig |
| `1:nanaimo-river:from-the-cedar-road-bridge-crossin` | between Cedar Road bridge and Hwy 19 bridge | high | high: "bridge, cedar, road" -> the curated cut `nanaimo_river__cedar_road_bridge` (Cedar Road bridge); high: "19, bridge, hwy" -> the curated cut `nanaimo_river |
| `1:nanaimo-river:from-the-power-line-crossing-at-bo` | between power line at Bore Hole and Boulder Creek → Nanaimo River | high | high: "bore, hole, powerline" -> the curated cut `nanaimo_river__power_line_at_bore_hole` (power line at Bore Hole); high: confluence of Boulder Creek: the cura |
| `1:nanaimo-river:from-the-upstream-side-of-the-ceda` | between Cedar Road bridge and Hwy 1 bridge | medium | high: "bridge, cedar, road" -> the curated cut `nanaimo_river__cedar_road_bridge` (Cedar Road bridge); medium: "1, hwy" -> the curated cut `nanaimo_river__hwy_1 |
| `1:puntledge-river:downstream-from-a-fishing-boundary` | downstream of signs 75 m downstream of Puntledge River hatchery fence | medium | medium: "75m-down, downstream, hatchery" -> the curated cut `puntledge_river__signs_75_m_downstream_of_puntledge_river_hatchery_fence` (signs 75 m downstream of |
| `1:puntledge-river:downstream-from-the-base-of-stotan` | downstream of base of Stotan Falls | high | high: "falls, stotan" -> the curated cut `puntledge_river__base_of_stotan_falls` (base of Stotan Falls) |
| `1:puntledge-river:from-signs-located-50m-upstream-of` | between fishing boundary signs 50 m upstream of BC Hydro Puntledge generating-station tailrace and signs 75 m downstream of Puntledge River hatchery fence | high | high: "50m-up, generating, hydro, station, tailrace, upstream" -> the curated cut `puntledge_river__fishing_boundary_signs_50_m_upstream_of_bc_hydro_puntledge_g |
| `1:puntledge-river:morrison-creek-closure-between-fis` | between Morrison Creek → Puntledge River (100 m upstream) and Morrison Creek → Puntledge River (100 m downstream) | high | high: confluence of Morrison Creek: the curated cut `puntledge_river__morrison_creek_into_puntledge_river_u100m` (Morrison Creek → Puntledge River (100 m upstre |
| `1:qualicum-river:downstream-of-the-upper-weir-locat` | between upper hatchery weir (125 m downstream) and boundary signs = QFN | medium | high: "125m-down, downstream, weir" -> the curated cut `qualicum_river__upper_hatchery_weir_125_m_downstream` (upper hatchery weir (125 m downstream)); medium:  |
| `1:qualicum-river:those-waters-upstream-of-the-big-q` | upstream of upper hatchery weir (125 m downstream) | high | high: "125m-down, downstream, hatchery, weir" -> the curated cut `qualicum_river__upper_hatchery_weir_125_m_downstream` (upper hatchery weir (125 m downstream)) |
| `1:qualicum-river:waters-below-the-qfn-boundary-fenc` | between boundary signs = QFN and Hwy 19A bridge | medium | medium: "qfn" -> the curated cut `qualicum_river__boundary_signs_qfn` (boundary signs = QFN); high: "19a, bridge, hwy" -> the curated cut `qualicum_river__hwy_1 |
| `1:san-juan-river:waters-of-the-san-juan-river-and-i` | downstream of Fleet River → San Juan River | high | high: confluence of Fleet River: the curated cut `san_juan_river__fleet_river_into_san_juan_river` (Fleet River → San Juan River) |
| `1:sproat-river:from-sproat-lake-to-approx-300m-be` | between Sproat Lake and fishing boundary signs 300 m downstream of Hwy 4 | high | high: Sproat Lake: the curated cut `sproat_river__sproat_lake` (Sproat Lake); high: "300m-down, 4, downstream, hwy" -> the curated cut `sproat_river__fishing_bo |
| `1:sproat-river:sproat-river-from-sproat-lake-to-f` | between Sproat Lake and fishing boundary signs 300 m downstream of Hwy 4 | high | high: Sproat Lake: the curated cut `sproat_river__sproat_lake` (Sproat Lake); high: "300m-down, 4, downstream, hwy" -> the curated cut `sproat_river__fishing_bo |
| `1:stamp-river:between-fishing-boundary-signs-app` | between fishing boundary signs 200 m upstream of Stamp Falls and signs 500 m downstream of Stamp Falls | high | high: "200m-up, falls, stamp, upstream" -> the curated cut `stamp_river__fishing_boundary_signs_200_m_upstream_of_stamp_falls` (fishing boundary signs 200 m ups |
| `1:stamp-river:downstream-of-signs-at-girl-guide` | downstream of Girl Guide Falls signs | medium | medium: "falls, girl, guide" -> the curated cut `stamp_river__girl_guide_falls_signs` (Girl Guide Falls signs); the notice's place is 250 m above it |
| `1:stamp-river:from-approx-200-m-upstream-of-the` | between fishing boundary signs 200 m upstream of Stamp Falls and signs 500 m downstream of Stamp Falls | high | high: "200m-up, falls, stamp, upstream" -> the curated cut `stamp_river__fishing_boundary_signs_200_m_upstream_of_stamp_falls` (fishing boundary signs 200 m ups |
| `1:stamp-river:from-the-confluence-of-the-ash-riv` | between Ash River → Stamp River and Great Central Lake dam | high | high: confluence of Ash River: the curated cut `stamp_river__ash_river_into_stamp_river` (Ash River → Stamp River); high: Great Central Lake: the curated cut `s |
| `1:stamp-river:from-the-confluence-of-the-ash-riv~2` | between Ash River → Stamp River and Great Central Lake dam | high | high: confluence of Ash River: the curated cut `stamp_river__ash_river_into_stamp_river` (Ash River → Stamp River); high: Great Central Lake: the curated cut `s |
| `1:stamp-river:stamp-river-downstream-of-girl-gui` | downstream of Girl Guide Falls signs | high | high: "falls, girl, guide" -> the curated cut `stamp_river__girl_guide_falls_signs` (Girl Guide Falls signs) |
| `1:stamp-river:upstream-of-signs-located-at-girl` | upstream of Girl Guide Falls signs | medium | medium: "falls, girl, guide" -> the curated cut `stamp_river__girl_guide_falls_signs` (Girl Guide Falls signs); the notice's place is 250 m above it |
| `2:alouette-river-and-tributaries:above-216th-street-including-the-n` | upstream of 216 Street bridge | low | low: "216, street" -> the curated cut `alouette_river__216_street_bridge` (216 Street bridge); as close: `alouette_river__216th_street` (216th Street) |
| `2:alouette-river-and-tributaries:downstream-of-the-216th-street-bri` | downstream of 216 Street bridge | medium | medium: "216, bridge, street" -> the curated cut `alouette_river__216_street_bridge` (216 Street bridge); the other end is the water's mouth, so downstream of i |
| `2:chehalis-river:downstream-of-the-logging-bridge-2` | downstream of main logging road bridge ~2.4km d/s Chehalis Lk | high | high: "2400m-down, bridge, chehalis, downstream, lake, logging" -> the curated cut `chehalis_river__main_logging_road_bridge_2_4km_d_s_chehalis_lk` (main loggin |
| `2:chilliwack-vedder-river-including:from-a-line-between-2-fishing-boun` | downstream of fishing boundary signs 100 m below the Slesse Creek confluence | medium | medium: "confluence, slesse" -> the curated cut `chilliwack_vedder_rivers__boundary_signs_below_slesse_creek` (fishing boundary signs 100 m below the Slesse Cre |
| `2:chilliwack-vedder-river-including:from-a-line-between-2-fishing-boun~2` | downstream of fishing boundary signs 100 m below the Slesse Creek confluence | medium | medium: "confluence, slesse" -> the curated cut `chilliwack_vedder_rivers__boundary_signs_below_slesse_creek` (fishing boundary signs 100 m below the Slesse Cre |
| `2:chilliwack-vedder-river-including:from-a-line-between-2-fishing-boun~3` | downstream of fishing boundary signs 100 m below the Slesse Creek confluence | high | high: "100m-down, confluence, slesse" -> the curated cut `chilliwack_vedder_rivers__boundary_signs_below_slesse_creek` (fishing boundary signs 100 m below the S |
| `2:chilliwack-vedder-river-including:upstream-from-a-line-between-2-fis` | upstream of fishing boundary signs 100 m below the Slesse Creek confluence | high | high: "100m-down, confluence, slesse" -> the curated cut `chilliwack_vedder_rivers__boundary_signs_below_slesse_creek` (fishing boundary signs 100 m below the S |
| `2:chilliwack-vedder-river-including:upstream-from-a-line-between-2-fis~2` | downstream of fishing boundary signs 100 m below the Slesse Creek confluence | high | high: "100m-down, confluence, slesse" -> the curated cut `chilliwack_vedder_rivers__boundary_signs_below_slesse_creek` (fishing boundary signs 100 m below the S |
| `2:fraser-river:mainstem-waters-upstream-of-the-cp` | upstream of the CPR Bridge at Mission | high | high: "bridge, cpr, mission" -> the curated cut `fraser_river__the_cpr_bridge_at_mission` (the CPR Bridge at Mission) |
| `2:harrison-river:from-the-outlet-of-harrison-lake-d~2` | downstream of Harrison Lake | high | high: Harrison Lake: the curated cut `harrison_river__harrison_lake` (Harrison Lake); the other end is the water's mouth, so downstream of it |
| `2:kanaka-creek:downstream-of-the-112th-street-bri` | downstream of 112 Avenue bridge | medium | medium: "112, bridge" -> the curated cut `kanaka_creek__112_avenue_bridge` (112 Avenue bridge) |
| `2:kanaka-creek:upstream-of-the-112th-street-bridg` | upstream of 112 Avenue bridge | medium | medium: "112, bridge" -> the curated cut `kanaka_creek__112_avenue_bridge` (112 Avenue bridge) |
| `2:little-campbell-river:downstream-of-12th-avenue-includin` | downstream of 12th Avenue | high | high: "12, avenue" -> the curated cut `campbell_river__12th_avenue` (12th Avenue) |
| `2:little-campbell-river:downstream-of-a-line-between-2-fis` | downstream of pedestrian bridge at the foot of Stayte Road | low | low: only "road, stayte" in common with `campbell_river__pedestrian_bridge_at_the_foot_of_stayte_road` (pedestrian bridge at the foot of Stayte Road) |
| `2:nicomekl-river:downstream-of-208th-street` | downstream of 208th Street | high | high: "208, street" -> the curated cut `nicomekl_river__208th_street` (208th Street) |
| `2:pitt-river:upper-river-upstream-of-pitt-lake` | upstream of Pitt Lake | high | high: Pitt Lake: the curated cut `pitt_river__pitt_lake` (Pitt Lake) |
| `2:serpentine-river:downstream-of-168th-street` | downstream of 168th Street at Bothwell Park | low | low: only "168, street" in common with `serpentine_river__168th_street_at_bothwell_park` (168th Street at Bothwell Park) |
| `2:serpentine-river:downstream-of-168th-street-at-both` | downstream of 168th Street at Bothwell Park | high | high: "168, bothwell, park, street" -> the curated cut `serpentine_river__168th_street_at_bothwell_park` (168th Street at Bothwell Park) |
| `5b:atnarko-river:downstream-of-the-fishing-boundary` | downstream of Tweedsmuir Park eastern boundary | low | low: only "park, tweedsmuir" in common with `atnarko_bella_coola_rivers_includes_tributaries_exce__tweedsmuir_park_eastern_boundary` (Tweedsmuir Park eastern bo |
| `5b:dean-river:from-signs-located-approx-500-m-up` | between signs ~0.5 km above the canyon (500 m upstream) and signs ~100 m below the canyon (100 m downstream) | high | high: "500m-up, canyon, upstream" -> the curated cut `dean_river__signs_0_5_km_above_the_canyon_500_m_upstream` (signs ~0.5 km above the canyon (500 m upstream) |
| `5b:dean-river:from-the-confluence-with-crag-cree` | downstream of Crag Creek → Dean River | high | high: confluence of Crag Creek: the curated cut `dean_river__crag_creek_into_dean_river` (Crag Creek → Dean River); the other end is the water's mouth, so downs |
| `5b:dean-river:upstream-of-the-confluence-with-cr` | upstream of Crag Creek → Dean River | high | high: confluence of Crag Creek: the curated cut `dean_river__crag_creek_into_dean_river` (Crag Creek → Dean River) |
| `6:babine-river:from-the-nilkitkwa-river-confluenc~2` | downstream of Nilkitkwa River → Babine River | high | high: confluence of Nilkitkwa River: the curated cut `babine_river__nilkitkwa_river_into_babine_river` (Nilkitkwa River → Babine River); the other end is the wa |
| `6:bulkley-river:downstream-of-the-morice-river-con` | downstream of Morice River → Bulkley River | high | high: confluence of Morice River: the curated cut `bulkley_river__morice_river_into_bulkley_river` (Morice River → Bulkley River) |
| `6:kitimat-river:downstream-of-highway-37-bridge` | downstream of Hwy 37 bridge | high | high: "37, bridge, hwy" -> the curated cut `kitimat_river__hwy_37_bridge` (Hwy 37 bridge) |
| `6:kitimat-river:on-west-bank-between-signs-at-kiti` | between Kitimat Hatchery outfall — u/s sign and Kitimat Hatchery outfall — d/s sign | high | high: "hatchery, kitimat, outfall, upstream" -> the curated cut `kitimat_river__kitimat_hatchery_outfall_u_s_sign` (Kitimat Hatchery outfall — u/s sign); high:  |
| `6:kitimat-river:upstream-of-highway-37-bridge` | upstream of Hwy 37 bridge | high | high: "37, bridge, hwy" -> the curated cut `kitimat_river__hwy_37_bridge` (Hwy 37 bridge) |
| `6:kwinimass-river:downstream-of-lower-bridge-abutmen` | downstream of lower bridge abutments | medium | medium: "abutments, bridge" -> the curated cut `ksi_x_anmas_river__lower_bridge_abutments` (lower bridge abutments) |
| `6:kwinimass-river:upstream-of-lower-bridge-abutments` | upstream of lower bridge abutments | medium | medium: "abutments, bridge" -> the curated cut `ksi_x_anmas_river__lower_bridge_abutments` (lower bridge abutments) |
| `6:lakelse-river:downstream-of-the-cnr-bridge` | downstream of CNR bridge | medium | medium: "bridge, cnr" -> the curated cut `lakelse_river__cnr_bridge` (CNR bridge) |
| `6:nass-river:mainstem-waters-downstream-of-the` | downstream of Meziadin River → Nass River | high | high: confluence of Meziadin River: the curated cut `meziadin_river__meziadin_river_into_nass_river` (Meziadin River → Nass River) |
| `6:nass-river:mainstem-waters-downstream-of-the~2` | downstream of Meziadin River → Nass River | medium | medium: confluence of Meziadin River: the curated cut `meziadin_river__meziadin_river_into_nass_river` (Meziadin River → Nass River) — the notice's signs are NE |
| `6:nass-river:mainstem-waters-near-the-meziadin` | between Meziadin River → Nass River and Hwy 37 bridge | medium | medium: confluence of Meziadin River: the curated cut `meziadin_river__meziadin_river_into_nass_river` (Meziadin River → Nass River) — the notice's signs are NE |
| `6:pinkut-creek:downstream-of-fishing-boundary-sig` | downstream of fish fence | medium | medium: "fence, fish" -> the curated cut `pinkut_creek__fish_fence` (fish fence); the notice's place is 25 m below it |
| `7:nechako-river:from-the-downstream-edge-of-the-fo` | downstream of Foothills Boulevard Bridge in Prince George | high | high: "boulevard, bridge, foothills" -> the curated cut `nechako_river__foothills_boulevard_bridge_in_prince_george` (Foothills Boulevard Bridge in Prince Georg |
| `7:nechako-river:upstream-from-the-boundary-signs-a` | downstream of Foothills Boulevard Bridge in Prince George | high | high: "boulevard, bridge, foothills" -> the curated cut `nechako_river__foothills_boulevard_bridge_in_prince_george` (Foothills Boulevard Bridge in Prince Georg |
| `8:shuswap-river:lower-50-m-upstream-of-the-trinity` | between signs 50 m upstream of Trinity Bridge and signs 50 m downstream of Trinity Bridge | high | high: "50m-up, bridge, trinity, upstream" -> the curated cut `shuswap_river__signs_50_m_upstream_of_trinity_bridge` (signs 50 m upstream of Trinity Bridge); hig |
| `8:shuswap-river:lower-upstream-from-white-triangul` | between Mara Bridge and Mabel Lake | medium | medium: "bridge, mara" -> the curated cut `shuswap_river__mara_bridge` (Mara Bridge); high: Mabel Lake: the curated cut `shuswap_river__mabel_lake` (Mabel Lake) |
| `1:cowichan-river:from-cnr-trestle-at-mile-70-2-to-s` | between CNR Bridge (mile 70.2) and Skutz Falls | medium | medium: "70.2, cnr, mile" -> the curated cut `cowichan_river__cnr_bridge` (CNR Bridge (mile 70.2)); medium: Skutz Falls: the OSM waterfall 0 m from the water |
| `1:little-qualicum-river:from-the-falls-in-little-qualicum` | between Little Qualicum Falls and hatchery fence | medium | medium: Little Qualicum Falls: the OSM waterfall 0 m from the water; medium: "fence, hatchery" -> the curated cut `little_qualicum_river__hatchery_fence` (hatch |
| `5a:quesnel-river:between-the-johnson-subdivision-br` | between Johnson Subdivision Bridge and powerlines | high | high: the notice's coordinate, 26 m from the water; high: the notice's coordinate, 29 m from the water |
| `5a:quesnel-river:that-portion-of-the-quesnel-river` | between Johnson Subdivision Bridge and powerlines | high | high: the notice's coordinate, 26 m from the water; high: the notice's coordinate, 29 m from the water |
| `6:ecstall-river:upstream-of-fishing-boundary-signs` | upstream of confluence of Johnston Creek | medium | medium: Johnston Creek (gnis:2031) flows straight into Ecstall River (FWA watershed code); the notice's place is NEAR the confluence; its mouth 0 m from the wat |
| `6:skeena-river:from-the-confluence-with-the-babin` | between confluence of Babine River and CNR Railway Bridge at Terrace | high | high: Babine River (gnis:17687) flows straight into Skeena River (FWA watershed code); its mouth 0 m from the water's line; high: "bridge, cnr, railway, terrace |
| `6:skeena-river:upstream-of-cnr-bridge-at-terrace` | between CNR Railway Bridge at Terrace and confluence of Babine River | high | high: "bridge, cnr, terrace" -> the curated cut `skeena_river__cnr_railway_bridge_terrace` (CNR Railway Bridge at Terrace); high: Babine River (gnis:17687) flow |
| `1:cayeghle-river:including-colonial-river` | whole | medium | "including Colonial River": the whole water, all its items |
| `1:cowichan-river:including-tributaries-and-cowichan` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `1:koksilah-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `1:little-qualicum-river:all-open-portions-of-the-little-qu` | whole | medium | the notice names no reach: the whole water |
| `1:little-qualicum-river:all-waters-unless-indicated-below` | whole | medium | the notice names no reach: the whole water |
| `1:nanaimo-river:including-tributaries-except-in-th` | whole + tributaries | medium | the whole water with its tributaries (the exception is another location) |
| `1:qualicum-river:all-open-portions-of-the-qualicum` | whole | medium | the notice names no reach: the whole water |
| `2:alouette-river-and-tributaries:north-alouette-and-tributaries` | whole + tributaries | medium | "North Alouette and tributaries": North Alouette River with its tributaries |
| `2:capilano-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `2:pitt-river:pitt-river-and-tributaries` | whole + tributaries | high | "Pitt River and tributaries": the whole water with its tributaries |
| `2:pitt-river:upper-and-lower-river-including-tr` | whole + tributaries | high | "Upper and lower river, including tributaries": the whole water with its tributaries |
| `3:fraser-river:mainstem-waters-of-the-fraser-rive` | whole | medium | the mainstem only: the whole water without its tributaries |
| `5b:long-lake:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:babine-lake:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:bear-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:bish-creek:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:brim-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:cranberry-river:including-tributaries-note-the-sec` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:dala-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:ishkheenickh-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:khutze:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:khutzeymateen-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:kiltuish-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:kiteen-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:kitseguecla-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:kitsumkalum-lake:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:kitwanga-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:kloiya-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:meziadin-lake:excluding-tributaries` | whole | high | "excluding tributaries": the whole water, without its tributaries |
| `6:meziadin-lake:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:meziadin-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:quaal-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:redsand-lake:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:scotia-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:stikine-river:including-tributaries-see-the-tahl` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:suskwa-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:sustut-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:swift-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:swift-river:including-tributaries-anglers-may` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:tahltan-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:taku-river:including-tributaries-see-the-naki` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:tatsatua-creek:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `6:wahoo-river:including-tributaries` | whole + tributaries | high | "including tributaries": the whole water with its tributaries |
| `1:stamp-river:from-the-southern-portion-of-the-r` | between confluence of Robertson Creek and the Hatchery Lagoon to the northern shoreline at the inlet to the Hatchery Lagoon, then south of a line drawn from the northern shoreline at the inlet to the Hatchery Lagoon to the northern shoreline at the outlet to the Hatchery Lagoon, then west of a line drawn from the northern shoreline at the outlet to the Hatchery Lagoon to the southern shoreline at the outlet to the Hatchery Lagoon | none | high: Robertson Creek (gnis:25870) flows straight into Stamp River (FWA watershed code); its mouth 0 m from the water's line; none: no curated cut, proposal or  |
| `2:alouette-river-and-tributaries:upstream-of-the-216th-street-bridg` | between 216 Street bridge and signs at Allco park | none | medium: "216, bridge, street" -> the curated cut `alouette_river__216_street_bridge` (216 Street bridge); none: no curated cut, proposal or data point names thi |
| `2:fraser-river:from-the-downstream-side-of-the-cp` | between the CPR Bridge at Mission and the Highway 1 bridge at Hope | none | high: "bridge, cpr, mission" -> the curated cut `fraser_river__the_cpr_bridge_at_mission` (the CPR Bridge at Mission); none: no curated cut, proposal or data po |
| `2:fraser-river:from-the-downstream-side-of-the-cp~2` | between the CPR Bridge at Mission and the Highway 1 Bridge at Hope and from the Highway 1 Bridge at Hope to the downstream side of the Alexandra Bridge | none | high: "bridge, cpr, mission" -> the curated cut `fraser_river__the_cpr_bridge_at_mission` (the CPR Bridge at Mission); none: no curated cut, proposal or data po |
| `2:fraser-river:mainstem-waters-from-the-downstrea` | between the CPR Bridge at Mission and the Highway 1 Bridge at Hope, BC | none | high: "bridge, cpr, mission" -> the curated cut `fraser_river__the_cpr_bridge_at_mission` (the CPR Bridge at Mission); none: no curated cut, proposal or data po |
| `2:harrison-river:from-the-outlet-of-harrison-lake-d` | between Harrison Lake and the Hwy 7 bridge | none | high: Harrison Lake: the curated cut `harrison_river__harrison_lake` (Harrison Lake); none: no curated cut, proposal or data point names this place |
| `3:fraser-river:in-that-portion-of-the-fraser-rive` | between confluence of Seton River and the fishing boundary signs located on both sides of the river approximately 4 km downstream of the town of Lillooet | none | high: Seton River (gnis:6291) flows straight into Fraser River (FWA watershed code); its mouth 0 m from the water's line; none: no curated cut, proposal or data |
| `3:south-thompson-river:from-the-green-can-buoy-near-outle` | between the green can buoy near outlet of Little River and confluence of Campbell Creek | none | none: no curated cut, proposal or data point names this place; medium: Campbell Creek (gnis:17762) flows straight into South Thompson River (FWA watershed code) |
| `3:thompson-river:from-kamloops-lake-outlet-downstre` | between Kamloops Lake and the fishing boundary signs just downstream of Gold Pan Provincial Park | none | high: Kamloops Lake: the curated cut `thompson_river__kamloops_lake` (Kamloops Lake); none: no curated cut, proposal or data point names this place |
| `3:thompson-river:from-kamloops-lake-outlet-downstre~2` | between Kamloops Lake and the fishing boundary signs just downstream of Gold Pan Provincial Park | none | high: Kamloops Lake: the curated cut `thompson_river__kamloops_lake` (Kamloops Lake); none: no curated cut, proposal or data point names this place |
| `3:thompson-river:from-the-white-triangle-fishing-bo` | between the white triangle fishing boundary signs just downstream of Gold Pan Provincial Park and easterly border of the Skihist Ecological reserve along the Thompson River locat | none | none: no curated cut, proposal or data point names this place; high: the notice's coordinate, 38 m from the water |
| `6:babine-river:from-the-nilkitkwa-river-confluenc` | between Nilkitkwa River → Babine River and the fishing boundary signs below the Babine Fence | none | high: confluence of Nilkitkwa River: the curated cut `babine_river__nilkitkwa_river_into_babine_river` (Nilkitkwa River → Babine River); none: no curated cut, p |
| `6:skeena-river:from-lakelse-river-mouth-upstream` | between confluence of Lakelse River and the Skeena River Overpass (New Hwy Bridge) at Ferry Island | none | high: Lakelse River (gnis:4903) flows straight into Skeena River (FWA watershed code); its mouth 0 m from the water's line; none: no curated cut, proposal or da |
| `8:shuswap-river:middle-between-shuswap-falls-and-m` | between Shuswap Falls and Mabel Lake | none | none: no curated cut, proposal or data point names this place; high: Mabel Lake: the curated cut `shuswap_river__mabel_lake` (Mabel Lake) |
| `8:shuswap-river:middle-between-shuswap-falls-and-m~2` | between Shuswap Falls and Mabel Lake | none | none: no curated cut, proposal or data point names this place; high: Mabel Lake: the curated cut `shuswap_river__mabel_lake` (Mabel Lake) |
| `1:puntledge-river:from-the-weir-at-the-lower-hatcher` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `1:quatse-river:100m-upstream-and-downstream-of-th` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `1:san-juan-river:that-portion-of-the-san-juan-river` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `1:stamp-river:except-in-those-areas-and-times-li` | — | none | none: no curated cut, proposal or data point names this place; the notice reads as downstream_of |
| `1:stamp-river:from-the-inlet-upstream-of-roberst` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `1:stamp-river:from-the-powerline-crossing-downst` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `1:stamp-river:hatchery-lagoon-from-boundary-sign` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `1:stamp-river:hatchery-lagoon-from-fishing-bound` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `1:stamp-river:powerline-crossing-from-boundary-s` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `2:alouette-river-and-tributaries:downstream-of-a-line-between-2-fis` | — | none | none: no curated cut, proposal or data point names this place |
| `2:alouette-river-and-tributaries:downstream-of-a-line-between-two-f` | — | none | none: no curated cut, proposal or data point names this place |
| `2:birkenhead-river:from-the-birkenhead-bridge-located` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `2:brunette-river:downstream-of-cariboo-road` | — | none | none: no curated cut, proposal or data point names this place |
| `2:chapman-creek:upstream-of-tidal-water-fishing-bo` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `2:fraser-river:from-the-highway-1-bridge-at-hope` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `2:fraser-river:upstream-from-the-highway-1-bridge` | — | none | none: no curated cut, proposal or data point names this place |
| `2:harrison-river:from-the-highway-7-bridge-downstre` | — | none | none: no curated cut, proposal or data point names this place; the other end is the water's mouth, so downstream of it |
| `2:inch-creek:from-the-hatchery-outlet-to-the-co` | — | none | none: no curated cut, proposal or data point names this place; the other end is the water's mouth, so downstream of it |
| `2:nicomekl-river:within-23-m-downstream-of-the-floo` | — | none | none: no curated cut, proposal or data point names this place |
| `2:nicomen-including-dewdney-slough:from-the-confluence-of-siddle-bell` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `2:nicomen-including-dewdney-slough:from-the-highway-7-bridge-at-dewdn` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `2:sakinaw-lake:easterly-of-a-line-drawn-from-a-bo` | — | none | a part of a lake (a line across it / a radius): not a cut on a line — curate by hand |
| `2:sakinaw-lake:the-body-of-water-known-as-bear-ba` | — | none | a part of a lake (a line across it / a radius): not a cut on a line — curate by hand |
| `2:serpentine-river:within-23-m-downstream-of-the-floo` | — | none | none: no curated cut, proposal or data point names this place |
| `2:seymour-river:from-the-seymour-river-dam-to-the` | — | none | none: no curated cut, proposal or data point names this place; the other end is the water's mouth, so downstream of it |
| `2:stave-river:downstream-of-b-c-hydro-dam-to-the` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `2:stave-river:downstream-of-b-c-hydro-dam-to-the~2` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `2:stave-river:in-that-portion-of-the-stave-river` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `2:stave-river:that-portion-of-the-stave-river-kn` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `2:stave-river:the-ruskin-spawning-channel-on-the` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `5a:quesnel-lake:in-the-waters-of-horsefly-bay-loca` | — | none | a part of a lake (a line across it / a radius): not a cut on a line — curate by hand |
| `5a:quesnel-lake:that-portion-of-quesnel-lake-known` | — | none | a part of a lake (a line across it / a radius): not a cut on a line — curate by hand |
| `5b:bella-coola-river-not-including-at:all-tributaries-to-the-bella-coola` | — | none | tributaries only (not the mainstem): an extent fix cannot say it — curate by hand |
| `6:babine-lake:babine-lake-excluding-tributaries` | — | none | a part of a lake (a line across it / a radius): not a cut on a line — curate by hand |
| `6:babine-lake:babine-lake-excluding-tributaries~2` | — | none | a part of a lake (a line across it / a radius): not a cut on a line — curate by hand |
| `6:babine-lake:babine-lake-excluding-tributaries~3` | — | none | a part of a lake (a line across it / a radius): not a cut on a line — curate by hand |
| `6:babine-lake:within-a-400-m-radius-of-the-mouth` | — | none | a part of a lake (a line across it / a radius): not a cut on a line — curate by hand |
| `6:babine-lake:within-a-400m-radius-of-the-mouth` | — | none | a part of a lake (a line across it / a radius): not a cut on a line — curate by hand |
| `6:bulkley-river:all-tributaries-of-the-bulkley-riv` | — | none | tributaries only (not the mainstem): an extent fix cannot say it — curate by hand |
| `6:bulkley-river:all-tributaries-of-the-bulkley-riv~2` | — | none | tributaries only (not the mainstem): an extent fix cannot say it — curate by hand |
| `6:exchamsiks-river:upstream-of-highway-16-bridge` | — | none | none: no curated cut, proposal or data point names this place |
| `6:exstew-river:upstream-of-highway-16-bridge` | — | none | none: no curated cut, proposal or data point names this place |
| `6:gitnadoix-river:upstream-of-powerline-crossing-nea` | — | none | none: no curated cut, proposal or data point names this place |
| `6:illiance-river:upstream-of-white-triangular-fishi` | — | none | none: no curated cut, proposal or data point names this place |
| `6:kasiks-river:downstream-of-fishing-boundary-sig` | — | none | none: no curated cut, proposal or data point names this place |
| `6:kasiks-river:upstream-of-fishing-boundary-signs` | — | none | none: no curated cut, proposal or data point names this place |
| `6:kasiks-river:upstream-of-highway-16-bridge` | — | none | none: no curated cut, proposal or data point names this place |
| `6:khyex-river:upstream-of-highway-16-bridge` | — | none | none: no curated cut, proposal or data point names this place |
| `6:kispiox-river:downstream-of-fishing-boundary-sig` | — | none | none: no curated cut, proposal or data point names this place |
| `6:kitsault-river:upstream-of-white-triangular-fishi` | — | none | none: no curated cut, proposal or data point names this place |
| `6:kitsumkalum-river:downstream-of-fishing-boundary-sig` | — | none | none: no curated cut, proposal or data point names this place |
| `6:kitsumkalum-river:downstream-of-railway-bridge` | — | none | none: no curated cut, proposal or data point names this place |
| `6:kitsumkalum-river:upstream-of-fishing-boundary-signs` | — | none | none: no curated cut, proposal or data point names this place |
| `6:lakelse-river:above-logging-road-bridge-near-the` | — | none | none: no curated cut, proposal or data point names this place |
| `6:lakelse-river:below-logging-road-bridge-near-the` | — | none | none: no curated cut, proposal or data point names this place |
| `6:nakina-river:that-portion-of-the-nakina-river-f` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `6:nass-river:portions-from-the-canyon-city-brid` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `6:shegunia-river:between-fishing-boundary-signs-loc` | — | none | none: no curated cut, proposal or data point names this place; none: no curated cut, proposal or data point names this place |
| `6:sheldens-creek:upstream-of-white-triangular-fishi` | — | none | the location's water is not matched to the registry |
| `6:tseax-river:downstream-of-nisga-a-highway-brid` | — | none | none: no curated cut, proposal or data point names this place |
| `6:tseax-river:upstream-of-nisga-a-highway-bridge` | — | none | none: no curated cut, proposal or data point names this place |
| `6:wilauks-creek:upstream-of-white-triangular-fishi` | — | none | none: no curated cut, proposal or data point names this place |
| `6:zymagotitz-river:upstream-of-highway-16-bridge` | — | none | none: no curated cut, proposal or data point names this place |
| `6:zymoetz-river:note-section-of-river-from-highway` | — | none | a note on the water's name, not a place |
| `6:zymoetz-river:upstream-of-highway-16-bridge` | — | none | none: no curated cut, proposal or data point names this place |
| `8:mabel-lake:northerly-of-a-line-drawn-from-a-w` | — | none | a part of a lake (a line across it / a radius): not a cut on a line — curate by hand |
| `8:osoyoos-lake:in-the-waters-in-the-portion-of-os` | — | none | a part of a lake (a line across it / a radius): not a cut on a line — curate by hand |
| `8:osoyoos-lake:in-the-waters-in-the-portion-of-os~2` | — | none | a part of a lake (a line across it / a radius): not a cut on a line — curate by hand |
