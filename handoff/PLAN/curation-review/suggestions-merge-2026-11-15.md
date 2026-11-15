# Targeted merge of data/generated/review/dfo-suggestions.json (2026-11-15)

Only the rows below change: each replaced by the new suggester's row, computed on the same data (head 6c8fd7fa5, the owner's marks as of the copy) as the old code. Every other row and every top-level key is the current file byte for byte; a row the current file lacks is added.

## A. tributaries only / a tributary left out (plain in DFO's words) — 6 rows

### `1:somass-river-tributaries:whole`

- words: Somass River tributaries — 
- before: null new cuts [] (confidence None, row absent)
- after:  {"op": "whole", "items": [], "points": [], "tributaries": true, "tributaries_only": true, "tributary_excludes": null, "words": "the extent as curated — the tributaries only"} new cuts [] (confidence medium, state tributary_call)

### `1:stamp-river-tributaries:whole`

- words: Stamp River tributaries — 
- before: null new cuts [] (confidence None, row absent)
- after:  {"op": "whole", "items": [], "points": [], "tributaries": true, "tributaries_only": true, "tributary_excludes": null, "words": "the extent as curated — the tributaries only"} new cuts [] (confidence medium, state tributary_call)

### `5b:bella-coola-river-not-including-at:all-tributaries-to-the-bella-coola`

- words: Bella Coola River (not including Atnarko River) — All tributaries to the Bella Coola River.
- before: {"op": "whole", "items": ["gnis:11611"], "points": [], "tributaries": true, "tributaries_only": null, "tributary_excludes": null, "words": "the whole water"} new cuts [] (confidence medium, row present)
- after:  {"op": "whole", "items": ["gnis:11611"], "points": [], "tributaries": true, "tributaries_only": true, "tributary_excludes": ["gnis:17209"], "words": "the extent as curated — the tributaries only; not Atnarko River"} new cuts [] (confidence medium, state tributary_call)
- CONFLICT (owner's call): the water's heading takes the tributaries in; this place's own words say tributaries only (not the water itself)

### `5b:bella-coola-river-not-including-at:whole`

- words: Bella Coola River (not including Atnarko River) — 
- before: {"op": "whole", "items": ["gnis:11611"], "points": [], "tributaries": true, "tributaries_only": null, "tributary_excludes": null, "words": "the whole water and its tributaries, NOT Atnarko River: add gnis:17209 to tributary_excludes"} new cuts [] (confidence high, row present)
- after:  {"op": "whole", "items": ["gnis:11611"], "points": [], "tributaries": true, "tributaries_only": null, "tributary_excludes": ["gnis:17209"], "words": "the extent as curated — not Atnarko River"} new cuts [] (confidence medium, state tributary_call)

### `6:bulkley-river:all-tributaries-of-the-bulkley-riv`

- words: Bulkley River — all tributaries of the Bulkley River other than the Suskwa River.
- before: {"op": "whole", "items": ["gnis:2195"], "points": [], "tributaries": true, "tributaries_only": null, "tributary_excludes": null, "words": "the whole water"} new cuts [] (confidence medium, row present)
- after:  {"op": "whole", "items": ["gnis:2195"], "points": [], "tributaries": true, "tributaries_only": true, "tributary_excludes": ["gnis:2637"], "words": "the whole water — the tributaries only; not Suskwa River"} new cuts [] (confidence medium, state unbound)

### `6:bulkley-river:all-tributaries-of-the-bulkley-riv~2`

- words: Bulkley River — all tributaries of the Bulkley River other than Morice River and tributaries, Suskwa River and tributaries, and Two Mile Creek
- before: {"op": "whole", "items": ["gnis:2195"], "points": [], "tributaries": true, "tributaries_only": null, "tributary_excludes": null, "words": "the whole water"} new cuts [] (confidence medium, row present)
- after:  {"op": "whole", "items": ["gnis:2195"], "points": [], "tributaries": true, "tributaries_only": true, "tributary_excludes": ["gnis:14918", "gnis:2637", "gnis:30327"], "words": "the whole water — the tributaries only; not Morice River, Suskwa River, Two Mile Creek"} new cuts [] (confidence medium, state unbound)

## B. the place's own words take the tributaries in or leave them out — 9 rows

### `2:fraser-river:mainstem-waters-downstream-of-the`

- words: Fraser River — mainstem waters downstream of the CPR Bridge at Mission to the Highway 1 Bridge at Hope
- before: {"op": "between", "items": [], "points": [{"kind": "split", "split_id": "fraser_river__the_cpr_bridge_at_mission"}, {"kind": "new", "split_id": "fraser_hope_bridge"}], "tributaries": false, "tributaries_only": null, "tributary_excludes": null, "words": "between the CPR Bridge at Mission and Highway 1 bridge at Hope (Bill Hartley / Fraser-Hope)"} new cuts ['fraser_hope_bridge'] (confidence high, row present)
- after:  {"op": "between", "items": [], "points": [{"kind": "split", "split_id": "fraser_river__the_cpr_bridge_at_mission"}, {"kind": "new", "split_id": "fraser_hope_bridge"}], "tributaries": false, "tributaries_only": null, "tributary_excludes": null, "words": "between the CPR Bridge at Mission and Highway 1 bridge at Hope (Bill Hartley / Fraser-Hope)"} new cuts ['fraser_hope_bridge'] (confidence high, state needs_cut)

### `2:fraser-river:mainstem-waters-from-the-downstrea`

- words: Fraser River — mainstem waters from the downstream side of the CPR Bridge at Mission, BC to the Highway 1 Bridge at Hope, BC
- before: {"op": "between", "items": [], "points": [{"kind": "split", "split_id": "fraser_river__the_cpr_bridge_at_mission"}, {"kind": "new", "split_id": "fraser_hope_bridge"}], "tributaries": false, "tributaries_only": null, "tributary_excludes": null, "words": "between the CPR Bridge at Mission and Highway 1 bridge at Hope (Bill Hartley / Fraser-Hope)"} new cuts ['fraser_hope_bridge'] (confidence high, row present)
- after:  {"op": "between", "items": [], "points": [{"kind": "split", "split_id": "fraser_river__the_cpr_bridge_at_mission"}, {"kind": "new", "split_id": "fraser_hope_bridge"}], "tributaries": false, "tributaries_only": null, "tributary_excludes": null, "words": "between the CPR Bridge at Mission and Highway 1 bridge at Hope (Bill Hartley / Fraser-Hope)"} new cuts ['fraser_hope_bridge'] (confidence high, state needs_cut)

### `2:stave-river:downstream-of-b-c-hydro-dam-to-the~2`

- words: Stave River — downstream of B.C. Hydro Dam to the CPR Railway Bridge; except you shall not fish for salmon in that portion on the east side of the Stave River, known as the R
- before: {"op": "between", "items": ["gnis:14589"], "points": [{"kind": "split", "split_id": "stave_river__hayward_lake"}, {"kind": "new", "split_id": "stave_cpr_bridge"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "between Hayward Lake and CPR railway bridge over the Stave (Ruskin)"} new cuts ['stave_cpr_bridge'] (confidence high, row present)
- after:  {"op": "between", "items": ["gnis:14589"], "points": [{"kind": "split", "split_id": "stave_river__hayward_lake"}, {"kind": "split", "split_id": "stave_river__cpr_bridge_tidal_boundary"}], "tributaries": false, "tributaries_only": null, "tributary_excludes": null, "words": "between stave_river__hayward_lake and CPR Bridge (tidal boundary)"} new cuts [] (confidence high, state tributary_call)

### `6:babine-lake:babine-lake-excluding-tributaries`

- words: Babine Lake — Babine Lake excluding tributaries and those waters within a 400 m radius of the following tributary streams: Morrison Creek, Six Mile Creek, Pierre Creek, Pendl
- before: {"op": "whole", "items": ["wbk:329026676"], "points": [], "tributaries": false, "tributaries_only": null, "tributary_excludes": null, "words": "the whole water"} new cuts [] (confidence medium, row present)
- after:  {"op": "whole", "items": ["wbk:329026676"], "points": [], "tributaries": false, "tributaries_only": null, "tributary_excludes": null, "words": "the whole water"} new cuts [] (confidence medium, state needs_cut)
- CONFLICT (owner's call): the water's heading takes the tributaries in; this place's own words say main stem / no tributaries

### `6:babine-lake:babine-lake-excluding-tributaries~2`

- words: Babine Lake — Babine Lake excluding tributaries and those waters within a 400 m radius of the following tributary streams: Also closed east of a line from Gullwing Creek to s
- before: {"op": "whole", "items": ["wbk:329026676"], "points": [], "tributaries": false, "tributaries_only": null, "tributary_excludes": null, "words": "the whole water"} new cuts [] (confidence medium, row present)
- after:  {"op": "whole", "items": ["wbk:329026676"], "points": [], "tributaries": false, "tributaries_only": null, "tributary_excludes": null, "words": "the whole water"} new cuts [] (confidence medium, state needs_cut)
- CONFLICT (owner's call): the water's heading takes the tributaries in; this place's own words say main stem / no tributaries

### `6:babine-lake:babine-lake-excluding-tributaries~3`

- words: Babine Lake — Babine Lake excluding tributaries and those waters within a 400 m radius of the following tributary streams: Also closed east of a line from Gullwing Creek to s
- before: {"op": "whole", "items": ["wbk:329026676"], "points": [], "tributaries": false, "tributaries_only": null, "tributary_excludes": null, "words": "the whole water"} new cuts [] (confidence medium, row present)
- after:  {"op": "whole", "items": ["wbk:329026676"], "points": [], "tributaries": false, "tributaries_only": null, "tributary_excludes": null, "words": "the whole water"} new cuts [] (confidence medium, state needs_cut)
- CONFLICT (owner's call): the water's heading takes the tributaries in; this place's own words say main stem / no tributaries

### `6:nass-river:mainstem-waters-downstream-of-the`

- words: Nass River — mainstem waters downstream of the confluence with the Meziadin River
- before: {"op": "downstream_of", "items": ["gnis:3206"], "points": [{"kind": "split", "split_id": "meziadin_river__meziadin_river_into_nass_river"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "downstream of Meziadin River → Nass River"} new cuts [] (confidence high, row present)
- after:  {"op": "downstream_of", "items": ["gnis:3206"], "points": [{"kind": "split", "split_id": "meziadin_river__meziadin_river_into_nass_river"}], "tributaries": false, "tributaries_only": null, "tributary_excludes": null, "words": "downstream of Meziadin River → Nass River"} new cuts [] (confidence high, state needs_cut)

### `6:nass-river:mainstem-waters-downstream-of-the~2`

- words: Nass River — mainstem waters downstream of the fishing boundary signs near the confluence with the Meziadin River
- before: {"op": "downstream_of", "items": ["gnis:3206"], "points": [{"kind": "split", "split_id": "meziadin_river__meziadin_river_into_nass_river"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "downstream of Meziadin River → Nass River"} new cuts [] (confidence high, row present)
- after:  {"op": "downstream_of", "items": ["gnis:3206"], "points": [{"kind": "split", "split_id": "meziadin_river__meziadin_river_into_nass_river"}], "tributaries": false, "tributaries_only": null, "tributary_excludes": null, "words": "downstream of Meziadin River → Nass River"} new cuts [] (confidence high, state needs_cut)

### `6:nass-river:mainstem-waters-near-the-meziadin`

- words: Nass River — mainstem waters near the Meziadin River confluence, from white triangular fishing boundary signs located downstream of the Meziadin River confluence,and upstrea
- before: {"op": "between", "items": ["gnis:3206"], "points": [{"kind": "split", "split_id": "meziadin_river__meziadin_river_into_nass_river"}, {"kind": "split", "split_id": "nass_river__hwy_37_bridge"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "between Meziadin River → Nass River and Hwy 37 bridge"} new cuts [] (confidence medium, row present)
- after:  {"op": "between", "items": ["gnis:3206"], "points": [{"kind": "split", "split_id": "meziadin_river__meziadin_river_into_nass_river"}, {"kind": "split", "split_id": "nass_river__hwy_37_bridge"}], "tributaries": false, "tributaries_only": null, "tributary_excludes": null, "words": "between Meziadin River → Nass River and Hwy 37 bridge"} new cuts [] (confidence medium, state needs_cut)

## C. inherited from the WATER's heading / its "(including tributaries)" flag — 15 rows

### `1:cowichan-river:from-cnr-trestle-at-mile-70-2-to-s`

- words: Cowichan River — from CNR Trestle at mile 70.2 to Skutz Falls
- before: {"op": "between", "items": ["gnis:12227"], "points": [{"kind": "split", "split_id": "cowichan_river__cnr_bridge"}, {"kind": "new", "split_id": "cowichan_skutz_falls"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "between CNR Bridge (mile 70.2) and Skutz Falls"} new cuts ['cowichan_skutz_falls'] (confidence high, row present)
- after:  {"op": "between", "items": ["gnis:12227"], "points": [{"kind": "split", "split_id": "cowichan_river__cnr_bridge"}, {"kind": "new", "split_id": "cowichan_skutz_falls"}], "tributaries": true, "tributaries_only": null, "tributary_excludes": null, "words": "between CNR Bridge (mile 70.2) and Skutz Falls"} new cuts ['cowichan_skutz_falls'] (confidence high, state needs_cut)

### `1:stamp-river:powerline-crossing-from-boundary-s`

- words: Stamp River — Powerline Crossing - from boundary signs on either side of the Stamp River at the powerline crossing downstream approx. 500m to boundary signs on either side of
- before: {"op": "between", "items": ["gnis:14533"], "points": [{"kind": "new", "split_id": "stamp_powerline"}, {"kind": "new", "split_id": "stamp_lagoon_inlet"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "between Stamp River powerline crossing (BC Hydro 1L127) and Robertson Creek Hatchery lagoon inlet"} new cuts ['stamp_powerline', 'stamp_lagoon_inlet'] (confidence low, row present)
- after:  {"op": "between", "items": ["gnis:14533"], "points": [{"kind": "split", "split_id": "stamp_river__stamp_river_powerline_crossing_b"}, {"kind": "split", "split_id": "stamp_river__robertson_creek_hatchery_lagoon_inlet"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "between Stamp River powerline crossing (BC Hydro 1L127) and Robertson Creek Hatchery lagoon inlet"} new cuts [] (confidence medium, state needs_cut)

### `3:thompson-river:from-kamloops-lake-outlet-downstre`

- words: Thompson River — From Kamloops Lake outlet downstream to the fishing boundary signs just downstream of Gold Pan Provincial Park, except:
- before: {"op": "between", "items": ["gnis:39492"], "points": [{"kind": "split", "split_id": "thompson_river__kamloops_lake"}, {"kind": "new", "split_id": "thompson_goldpan_signs"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "between Kamloops Lake and signs just below Goldpan Provincial Park"} new cuts ['thompson_goldpan_signs'] (confidence medium, row present)
- after:  {"op": "between", "items": ["gnis:39492"], "points": [{"kind": "split", "split_id": "thompson_river__kamloops_lake"}, {"kind": "split", "split_id": "thompson_river__signs_just_below_goldpan_provincial_park"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "between thompson_river__kamloops_lake and signs just below Goldpan Provincial Park"} new cuts [] (confidence high, state needs_cut)

### `5b:atnarko-river:downstream-of-the-fishing-boundary`

- words: Atnarko River — downstream of the fishing boundary sign located near Tweedsmuir Park Lodge.
- before: {"op": "downstream_of", "items": ["gnis:17209"], "points": [{"kind": "new", "split_id": "atnarko_lodge_sign"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "downstream of signs near Tweedsmuir Park Lodge (Stuie)"} new cuts ['atnarko_lodge_sign'] (confidence medium, row present)
- after:  {"op": "downstream_of", "items": ["gnis:17209"], "points": [{"kind": "new", "split_id": "atnarko_lodge_sign"}], "tributaries": true, "tributaries_only": null, "tributary_excludes": null, "words": "downstream of signs near Tweedsmuir Park Lodge (Stuie)"} new cuts ['atnarko_lodge_sign'] (confidence medium, state needs_cut)

### `6:ecstall-river:upstream-of-fishing-boundary-signs`

- words: Ecstall River — upstream of fishing boundary signs near confluence with Johnston Creek.
- before: {"op": "upstream_of", "items": ["gnis:3986"], "points": [{"kind": "new", "split_id": "sg1"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "upstream of confluence of Johnston Creek"} new cuts ['sg1'] (confidence medium, row present)
- after:  {"op": "upstream_of", "items": ["gnis:3986"], "points": [{"kind": "new", "split_id": "ecstall_river__johnston_creek"}], "tributaries": true, "tributaries_only": null, "tributary_excludes": null, "words": "upstream of Johnston Creek → (proposed) (medium confidence)"} new cuts [] (confidence medium, state needs_cut)

### `6:exstew-river:upstream-of-highway-16-bridge`

- words: Exstew River — upstream of Hwy #16 bridge.
- before: {"op": "upstream_of", "items": ["gnis:3498"], "points": [{"kind": "new", "split_id": "exstew_hwy16"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "upstream of Hwy 16 bridge over the Exstew"} new cuts ['exstew_hwy16'] (confidence high, row present)
- after:  {"op": "upstream_of", "items": ["gnis:3498"], "points": [{"kind": "new", "split_id": "exstew_hwy16"}], "tributaries": true, "tributaries_only": null, "tributary_excludes": null, "words": "upstream of Hwy 16 bridge over the Exstew"} new cuts ['exstew_hwy16'] (confidence high, state needs_cut)

### `6:kasiks-river:upstream-of-highway-16-bridge`

- words: Kasiks River — upstream of Hwy #16 bridge.
- before: {"op": "upstream_of", "items": ["gnis:4078"], "points": [{"kind": "new", "split_id": "kasiks_hwy16"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "upstream of Hwy 16 bridge over the Kasiks"} new cuts ['kasiks_hwy16'] (confidence high, row present)
- after:  {"op": "upstream_of", "items": ["gnis:4078"], "points": [{"kind": "new", "split_id": "kasiks_hwy16"}], "tributaries": true, "tributaries_only": null, "tributary_excludes": null, "words": "upstream of Hwy 16 bridge over the Kasiks"} new cuts ['kasiks_hwy16'] (confidence high, state needs_cut)

### `6:khyex-river:upstream-of-highway-16-bridge`

- words: Khyex River — upstream of Hwy #16 bridge.
- before: {"op": "upstream_of", "items": ["gnis:4076"], "points": [{"kind": "new", "split_id": "khyex_hwy16"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "upstream of Hwy 16 bridge over the Khyex"} new cuts ['khyex_hwy16'] (confidence high, row present)
- after:  {"op": "upstream_of", "items": ["gnis:4076"], "points": [{"kind": "new", "split_id": "khyex_hwy16"}], "tributaries": true, "tributaries_only": null, "tributary_excludes": null, "words": "upstream of Hwy 16 bridge over the Khyex"} new cuts ['khyex_hwy16'] (confidence high, state needs_cut)

### `6:kispiox-river:downstream-of-fishing-boundary-sig`

- words: Kispiox River — downstream of fishing boundary signs near Kispiox River Resort.
- before: {"op": "downstream_of", "items": ["gnis:2308"], "points": [{"kind": "new", "split_id": "kispiox_resort_signs"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "downstream of signs near Kispiox River Resort"} new cuts ['kispiox_resort_signs'] (confidence low, row present)
- after:  {"op": "downstream_of", "items": ["gnis:2308"], "points": [{"kind": "new", "split_id": "kispiox_resort_signs"}], "tributaries": true, "tributaries_only": null, "tributary_excludes": null, "words": "downstream of signs near Kispiox River Resort"} new cuts ['kispiox_resort_signs'] (confidence low, state needs_cut)

### `6:kitsumkalum-river:downstream-of-fishing-boundary-sig`

- words: Kitsumkalum River — downstream of fishing boundary signs below lower canyon.
- before: {"op": "downstream_of", "items": ["gnis:3942"], "points": [{"kind": "new", "split_id": "kitsumkalum_lower_canyon"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "downstream of signs below the Kitsumkalum lower canyon"} new cuts ['kitsumkalum_lower_canyon'] (confidence medium, row present)
- after:  {"op": "downstream_of", "items": ["gnis:3942"], "points": [{"kind": "new", "split_id": "kitsumkalum_lower_canyon"}], "tributaries": true, "tributaries_only": null, "tributary_excludes": null, "words": "downstream of signs below the Kitsumkalum lower canyon"} new cuts ['kitsumkalum_lower_canyon'] (confidence medium, state needs_cut)

### `6:kitsumkalum-river:downstream-of-railway-bridge`

- words: Kitsumkalum River — downstream of railway bridge.
- before: {"op": "downstream_of", "items": ["gnis:3942"], "points": [{"kind": "new", "split_id": "kitsumkalum_cn_bridge"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "downstream of CN railway bridge over the Kitsumkalum"} new cuts ['kitsumkalum_cn_bridge'] (confidence high, row present)
- after:  {"op": "downstream_of", "items": ["gnis:3942"], "points": [{"kind": "new", "split_id": "kitsumkalum_cn_bridge"}], "tributaries": true, "tributaries_only": null, "tributary_excludes": null, "words": "downstream of CN railway bridge over the Kitsumkalum"} new cuts ['kitsumkalum_cn_bridge'] (confidence high, state needs_cut)

### `6:kitsumkalum-river:upstream-of-fishing-boundary-signs`

- words: Kitsumkalum River — upstream of fishing boundary signs below lower canyon.
- before: {"op": "upstream_of", "items": ["gnis:3942"], "points": [{"kind": "new", "split_id": "kitsumkalum_lower_canyon"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "upstream of signs below the Kitsumkalum lower canyon"} new cuts ['kitsumkalum_lower_canyon'] (confidence medium, row present)
- after:  {"op": "upstream_of", "items": ["gnis:3942"], "points": [{"kind": "new", "split_id": "kitsumkalum_lower_canyon"}], "tributaries": true, "tributaries_only": null, "tributary_excludes": null, "words": "upstream of signs below the Kitsumkalum lower canyon"} new cuts ['kitsumkalum_lower_canyon'] (confidence medium, state needs_cut)

### `6:lakelse-river:above-logging-road-bridge-near-the`

- words: Lakelse River — above logging road bridge near the mouth.
- before: {"op": "upstream_of", "items": ["gnis:4903"], "points": [{"kind": "new", "split_id": "lakelse_whitebottom_bridge"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "upstream of Whitebottom Rd bridge (Lakelse, near the mouth)"} new cuts ['lakelse_whitebottom_bridge'] (confidence medium, row present)
- after:  {"op": "upstream_of", "items": ["gnis:4903"], "points": [{"kind": "new", "split_id": "lakelse_whitebottom_bridge"}], "tributaries": true, "tributaries_only": null, "tributary_excludes": null, "words": "upstream of Whitebottom Rd bridge (Lakelse, near the mouth)"} new cuts ['lakelse_whitebottom_bridge'] (confidence medium, state needs_cut)

### `6:lakelse-river:below-logging-road-bridge-near-the`

- words: Lakelse River — below logging road bridge near the mouth.
- before: {"op": "downstream_of", "items": ["gnis:4903"], "points": [{"kind": "new", "split_id": "lakelse_whitebottom_bridge"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "downstream of Whitebottom Rd bridge (Lakelse, near the mouth)"} new cuts ['lakelse_whitebottom_bridge'] (confidence medium, row present)
- after:  {"op": "downstream_of", "items": ["gnis:4903"], "points": [{"kind": "new", "split_id": "lakelse_whitebottom_bridge"}], "tributaries": true, "tributaries_only": null, "tributary_excludes": null, "words": "downstream of Whitebottom Rd bridge (Lakelse, near the mouth)"} new cuts ['lakelse_whitebottom_bridge'] (confidence medium, state needs_cut)

### `6:zymagotitz-river:upstream-of-highway-16-bridge`

- words: Zymagotitz River — upstream of Highway #16 bridge
- before: {"op": "upstream_of", "items": ["gnis:3812"], "points": [{"kind": "new", "split_id": "zymagotitz_hwy16"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "upstream of Hwy 16 bridge over the Zymagotitz"} new cuts ['zymagotitz_hwy16'] (confidence high, row present)
- after:  {"op": "upstream_of", "items": ["gnis:3812"], "points": [{"kind": "new", "split_id": "zymagotitz_hwy16"}], "tributaries": true, "tributaries_only": null, "tributary_excludes": null, "words": "upstream of Hwy 16 bridge over the Zymagotitz"} new cuts ['zymagotitz_hwy16'] (confidence high, state needs_cut)

## D. mouth / tidal ends: the existing tidal boundary or an open end, never a new cut — 6 rows

### `1:puntledge-river:from-the-weir-at-the-lower-hatcher`

- words: Puntledge River — from the weir at the lower hatchery to fishing boundary signs approximately 75m downstream of the weir.
- before: {"op": "between", "items": ["gnis:27060"], "points": [{"kind": "new", "split_id": "puntledge_lower_weir"}, {"kind": "new", "split_id": "puntledge_lower_weir_d75"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "between Puntledge lower hatchery weir and signs 75 m below the Puntledge lower hatchery weir"} new cuts ['puntledge_lower_weir', 'puntledge_lower_weir_d75'] (confidence medium, row present)
- after:  {"op": "between", "items": ["gnis:27060"], "points": [{"kind": "split", "split_id": "puntledge_river__signs_75_m_downstream_of_puntledge_river_hatchery_fence"}, {"kind": "new", "split_id": "puntledge_lower_weir_d75"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "between signs 75 m downstream of Puntledge River hatchery fence and signs 75 m below the Puntledge lower hatchery weir"} new cuts ['puntledge_lower_weir_d75'] (confidence medium, state needs_cut)

### `1:stamp-river:from-the-southern-portion-of-the-r`

- words: Stamp River — From the southern portion of the Robertson Creek Hatchery Lagoon, east of a line drawn from the southern shoreline at the inlet to the Hatchery Lagoon to the no
- before: {"op": "between", "items": ["gnis:14533"], "points": [{"kind": "new", "split_id": "stamp_lagoon_inlet"}, {"kind": "new", "split_id": "stamp_lagoon_outlet"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "between Robertson Creek Hatchery lagoon inlet and Robertson Creek Hatchery lagoon outlet"} new cuts ['stamp_lagoon_inlet', 'stamp_lagoon_outlet'] (confidence low, row present)
- after:  {"op": "between", "items": ["gnis:14533"], "points": [{"kind": "split", "split_id": "stamp_river__robertson_creek_hatchery_lagoon_inlet"}, {"kind": "new", "split_id": "stamp_lagoon_outlet"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "between Robertson Creek Hatchery lagoon inlet and Robertson Creek Hatchery lagoon outlet"} new cuts ['stamp_lagoon_outlet'] (confidence low, state needs_cut)

### `1:stamp-river:hatchery-lagoon-from-boundary-sign`

- words: Stamp River — Hatchery Lagoon - from boundary signs on either side of the Stamp River located at the inlet of the Stamp River hatchery lagoon continuing downstream approx. 25
- before: {"op": "between", "items": ["gnis:14533"], "points": [{"kind": "new", "split_id": "stamp_lagoon_inlet"}, {"kind": "new", "split_id": "stamp_cable_car"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "between Robertson Creek Hatchery lagoon inlet and Stamp River cable car crossing"} new cuts ['stamp_lagoon_inlet', 'stamp_cable_car'] (confidence low, row present)
- after:  {"op": "between", "items": ["gnis:14533"], "points": [{"kind": "split", "split_id": "stamp_river__robertson_creek_hatchery_lagoon_inlet"}, {"kind": "new", "split_id": "stamp_cable_car"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "between Robertson Creek Hatchery lagoon inlet and Stamp River cable car crossing"} new cuts ['stamp_cable_car'] (confidence low, state needs_cut)

### `1:stamp-river:hatchery-lagoon-from-fishing-bound`

- words: Stamp River — Hatchery Lagoon - from fishing boundary signs on either side of the Stamp River located at the inlet to the Stamp River Hatchery lagoon, downstream to the marke
- before: {"op": "between", "items": ["gnis:14533"], "points": [{"kind": "new", "split_id": "stamp_lagoon_inlet"}, {"kind": "new", "split_id": "stamp_lagoon_outlet"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "between Robertson Creek Hatchery lagoon inlet and Robertson Creek Hatchery lagoon outlet"} new cuts ['stamp_lagoon_inlet', 'stamp_lagoon_outlet'] (confidence low, row present)
- after:  {"op": "between", "items": ["gnis:14533"], "points": [{"kind": "split", "split_id": "stamp_river__robertson_creek_hatchery_lagoon_inlet"}, {"kind": "new", "split_id": "stamp_lagoon_outlet"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "between Robertson Creek Hatchery lagoon inlet and Robertson Creek Hatchery lagoon outlet"} new cuts ['stamp_lagoon_outlet'] (confidence low, state needs_cut)

### `6:illiance-river:upstream-of-white-triangular-fishi`

- words: Illiance River — upstream of white triangular fishing boundary signs located near mouth of river.
- before: {"op": "upstream_of", "items": ["gnis:4205"], "points": [{"kind": "new", "split_id": "mouth_signs_illiance"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "upstream of signs near the Illiance mouth"} new cuts ['mouth_signs_illiance'] (confidence low, row present)
- after:  {"op": "whole", "items": ["gnis:4205"], "points": [], "tributaries": true, "tributaries_only": null, "tributary_excludes": null, "words": "the whole water"} new cuts [] (confidence medium, state needs_cut)

### `6:wilauks-creek:upstream-of-white-triangular-fishi`

- words: Wilauks Creek — upstream of white triangular fishing boundary signs located near mouth of creek.
- before: {"op": "upstream_of", "items": ["gnis:3787"], "points": [{"kind": "new", "split_id": "mouth_signs_wilauks"}], "tributaries": null, "tributaries_only": null, "tributary_excludes": null, "words": "upstream of signs near the Wilauks mouth"} new cuts ['mouth_signs_wilauks'] (confidence low, row present)
- after:  {"op": "whole", "items": ["gnis:3787"], "points": [], "tributaries": true, "tributaries_only": null, "tributary_excludes": null, "words": "the whole water"} new cuts [] (confidence medium, state needs_cut)

Rows changed by this merge: 36 (= the targets: True). A FULL regenerate instead would change 145 rows, 109 of them not targets (the current file predates the places: rows of places bound since, re-read hunts).
current sha256 4770d994fcccc0b07479ca8d4d38d99ce41f069fd6d5062ca339f579c63232bc
merged  sha256 730237cd38ca9ee3c75fcfa439a412496bf50243a36c0839e5f494fcb9e6cc46
