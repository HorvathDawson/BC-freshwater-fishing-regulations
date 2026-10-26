# Split-name audit (2026-10-26)

Owner, on Wahpeeto Creek: the closure "within 100 m downstream of the falls situated approximately
4.5 km upstream of Wakeman River" was held as an undrawn note. Meanwhile a curated split
`wahpeeto_creek__within_100_m_downstream`, labelled "within (100 m downstream)", sat on the water and
no rule used it. The id and label were cut from the regulation's phrase, so the parser had nothing to
match. Also, only the 100 m-below point existed, never the falls.

The fix goes into the next rebuild. Nothing is built yet, so `splits.resolved.json` and the bundle
still carry the old ids until then.

## Counts

| | |
|---|---|
| Curated splits before → after | 409 → 410 (one added: the Wahpeeto falls) |
| Bound by a catalogue rule | 353 |
| Bound by DFO salmon entries only | 15 |
| Unused, but they answer to a bound cut-point (alias, same place) | 12 |
| Unused by any rule | 30 (listed below) |
| Bad names: id or label is a phrase fragment, names no place | 22 → all fixed (21 renamed, 1 relabelled) |
| Rules with an undrawn or unplaced part, or "no bindable split" | 136 (135 `undrawn_part`, 31 `unresolved_locators`, 92 "no split" reasons) |
| …of those on a stream, where a cut would draw it | 18, listed under "Needs a split" |
| …the rest | lake polygons ("on parts", "north of a line…", "as buoyed"), which a stream split can't draw, plus "on part(s)" where the book doesn't say which part |
| Re-extented rules | 1: `wahpeeto_creek.r1` |
| Marks affected | none. The 9 marks in `marks.jsonl` are on Hatzic, Dragon, Iskut, Sproat, Trepanier, Halfway (R4) and Peachland, and no renamed split or edited entry is on those waters. |

## Renamed splits (high confidence, applied)

Every reference was rewritten: the catalogue (19 entries in regions 1, 2, 4, 5, 6 and 7),
`dfo_salmon/region-1.json` (Somass), and `test_rulings_pinned.py` (Anzac). That test still accepts the
old id until the bundle is rebuilt. Each renamed split's `_note` records its old id and label. The
anchors are unchanged, so `anchor_digest` is unchanged too.

| Old id | New id | New label |
|---|---|---|
| wahpeeto_creek__within_100_m_downstream | wahpeeto_creek__100_m_below_falls_4_5_km_above_wakeman | 100 m below the falls (4.5 km above Wakeman River) |
| mitchell_river__within_100_m_downstream | mitchell_river__100_m_below_mitchell_lake_weir | 100 m below the Mitchell Lake outlet weir |
| mitchell_river__within_100_m_upstream | mitchell_river__100_m_above_mitchell_lake_weir | 100 m above the Mitchell Lake outlet weir |
| tupper_river__within_100_m_downstream | tupper_river__100_m_below_swan_lake_weir | 100 m below the Swan Lake outlet weir |
| anzac_river__within_500_m_downstream | anzac_river__500_m_below_upper_anzac_bridge | 500 m below the Upper Anzac bridge |
| anzac_river__within_500_m_upstream | anzac_river__500_m_above_upper_anzac_bridge | 500 m above the Upper Anzac bridge |
| tahltan_river__th_750_m_upstream | tahltan_river__landslide_750_m_above_tahltan_bridge | landslide ~750 m above Tahltan River Bridge |
| bulkley_river__in_moricetown_canyon_or_within_100_m_downstream | bulkley_river__100_m_below_moricetown_canyon | 100 m below Moricetown/Witset Canyon |
| murray_river__signs_about_2000_m_downstream | murray_river__signs_2_km_below_kinuseo_falls | signs ~2 km below Kinuseo Falls |
| nechako_river__boundary_sign_5000_m_downstream | nechako_river__sign_5_km_below_cheslatta_river | boundary sign 5 km below Cheslatta River |
| tlell_river__tidal_boundary_sign_1500_m_upstream | tlell_river__tidal_sign_1_5_km_above_hwy_16_bridge | tidal boundary sign 1.5 km above Hwy 16 Bridge |
| kokish_river__log_boom_90_m_upstream | kokish_river__log_boom_above_ipp_intake | log boom ~100 m above the IPP intake |
| heber_river__top_of_lower_canyon_1300_m_upstream | heber_river__top_of_lower_canyon | top of the lower canyon (1.3 km above Gold River) |
| woodbury_creek__falls_at_small_hydro_structure_800_m_upstream | woodbury_creek__falls_at_small_hydro_above_hwy_31 | falls at the small hydro structure (800 m above Hwy 31 bridge) |
| ruby_creek__ruby_lake_to_fishing_boundary_signs_100_m_downstream | ruby_creek__signs_100_m_below_ruby_lake | fishing boundary signs 100 m below Ruby Lake |
| chichouyenily_creek__fishing_boundary_signs_near_its_mouth_400_m_upstream | chichouyenily_creek__signs_400_m_above_williston_lake | fishing boundary signs near the mouth (400 m above Williston Lake) |
| weston_creek__signs_near_its_mouth_400_m_upstream | weston_creek__signs_400_m_above_williston_lake | (same) |
| gagnon_creek__fishing_boundary_signs_near_its_mouth_400_m_upstream | gagnon_creek__signs_400_m_above_williston_lake | (same) |
| somass_river__boundary_signs_1km_u_s | somass_river__signs_1_km_above_papermill_dam | fishing boundary signs ~1 km above Papermill Dam |
| thorn_creek__point | thorn_creek__500_m_above_attichika_creek | 500 m above Attichika Creek |
| yakoun_river__yakoun_lake_14533_m_downstream | yakoun_river__signs_13_km_below_yakoun_lake | fishing boundary signs ~13 km below Yakoun Lake |
| brem_river__the_upstream_edge_of_1st_falls_100_m_downstream | brem_river__100_m_below_1st_falls | 100 m below the 1st falls |
| pitt_river__garibaldi_pitt (id kept) | — | Garibaldi Park boundary (was "within Garibaldi Park") |

## Added split and re-extented rule

- **New `wahpeeto_creek__falls_4_5_km_above_wakeman`** ("the falls (4.5 km above Wakeman River)"). It's
  a point at FISS 29119 (-126.46897, 51.16963), the same coordinate the 100 m-below cut was already
  offset from. The location comes from data, not a guess. FWA puts it about 4.1 km up the creek, and
  the book says "approximately 4.5 km".
- **`wahpeeto_creek.r1`** changes from `whole` plus an undrawn part to
  `between [wahpeeto_creek__falls_4_5_km_above_wakeman, wahpeeto_creek__100_m_below_falls_4_5_km_above_wakeman]`.
  Its `undrawn_part`, `unresolved_locators` and `review_reason` are removed.

## Matcher fix (systemic)

The parser picks a rule's cut by reading each cut's **label** in the boundary menu
(`parse_context.render_boundary_menu`). The 22 bad labels came from the retired structural-split step,
which cut ids and labels out of the regulation's phrase. One offset point had no cut at its own anchor;
Wahpeeto was the only case. Among the other 47 such points, every one that a two-cut reach needs
already has its partner.

- New `pipeline/atlas/splits/splits.label_problem(label)` decides whether a label names a place. It
  refuses a phrase fragment ("within …", "in … or within", "signs about"), a reach ("Ruby Lake to
  signs"), and a label made only of distances, directions or sign words ("boundary sign (5000 m
  downstream)", "th", "point", "~1km u/s").
- `load_split_defs` now refuses a splits.json with such a label, and the error says why for each one.
  So a bad name can't reach the menu again.
- Tests in `pipeline/tests/test_splits.py` cover the refused and accepted labels, the loader refusal
  (a mutation), and a check that every curated label names a place.
- This doesn't touch normalisation or `run_parse.sh`. Nothing was run against the parser.

## Needs a split (rule left undrawn; DFO review mode / T191 fix-extent can place it)

| Rule | Missing cut(s) |
|---|---|
| r2:chapman_creek.r2 | the falls (~550 m above the power line crossing) and 100 m below them. No FISS or OSM falls point on Chapman Creek. |
| r7:halfway_river.r1 (gnis:36111) | signs ~5 km up the Halfway from the Peace; the rule is `downstream_of` that cut. See the medium list. |
| r1:deena_creek.r1 | signs at the second bridge, ~5 km above the tidal boundary |
| r1:mamin_river.r3 | signs on the third bridge, ~10 km above the tidal boundary |
| r1:white_river.r1 | signs at the salmon viewing pool (both ends) |
| r6:cranberry_river.r1 | signs above and below Cranberry River Canyon |
| r2:pitt_river.r2 | Grant Narrows (both ends of the speed zone) |
| r2:stave_river.r1 / .r2 | Ruskin and Northrop spawning channels (intake, boat-ramp crossing, junction) |
| z2:stave_river_marking.r1 | Ruskin Dam (`downstream_of`; it's over-broad now) |
| r4:columbia_river.r11 | Mud Lake: no boundary on the Columbia for it |
| r6:fulton_river.r3 | the signed stretch opposite the spawning channels |
| r3:mcarthur_island_slough.r1 | the westerly and 12th Street park entrances |

These are left as notes because a split can't draw them: "on part(s)" where the book doesn't say
which part (Courtenay r2, Cowichan r8/r9, Nanaimo r6, Coquitlam r2, Nicomekl r5, Chilko r6), and about
100 lake-polygon rules, which need sub-lake areas, not stream splits.

## Medium / low confidence (listed, not applied)

- **Medium.** `halfway_river__halfway_river_into_peace_river` (unused) and `…_u5000m` (an alias of
  the Peace's own cut) target `wsc 200-948755`, the **Peace**, not the Halfway. They look like they
  were authored for r7:halfway_river.r1 but anchored on the wrong river. Fix: retarget a 5 km cut onto
  the Halfway (gnis:36111) and delete these two.
- **Low.** `gagnon_creek.r1` binds `gagnon_creek__lake_329465442` where the text names the signs. The
  renamed signs split answers to the same cut-point (an alias), so the drawing is the same. It could
  bind the signs id for clarity.
- **Low.** About 25 labels name a place but keep the old "(N m downstream)" parenthetical, or their id
  still carries a bare distance. Examples: `ash_river__dickson_falls_30_m_downstream` "Dickson Falls
  (30 m downstream)", Dean River "signs ~0.5 km above the canyon (500 m upstream)", Gold River "the
  cascade falls (80 m downstream)". All are bound. Renaming them is cosmetic.
- **Low.** `crazy_creek__the_falls` ("the falls") is OSM node 1489693219, ~0.8 km up the creek. It's
  correct, just generic.
- **Low.** r5:mitchell_lake.r1 ("100 m radius of the weir at the lake's outlet"): the lake half of the
  radius needs a sub-lake area.

## Unused by any rule (30)

- **Unused by design** (rulings Q18 and T179: one cut for "upstream of / downstream of the Park"):
  `baker_creek__pinnacles_park_upstream_boundary`,
  `atnarko_bella_coola_rivers_includes_tributaries_exce__tweedsmuir_park_upstream_boundary`.
- **Authored names for gauge or other positions:** `skeena_river__above_babine_river` (gauge 08EB005),
  `bulkley_river__moricetown_witset_canyon_bottom`.
- **No rule text describes them** (orphans from earlier DFO or other work; delete or keep for DFO):
  `burton_creek__hwy_6_bridge`, `chehalis_river__hatchery_outlet` (only a note in `overrides.json`
  and DFO R2 names it as an override's upstream bound; nothing binds it, so check that override),
  `chilliwack_vedder_rivers__vedder_crossing_bridge_200_m_{downstream,upstream}`,
  `clearwater_river__clearwater_river_into_north_thompson_river`,
  `columbia_river__kootenay_river_into_columbia_river`,
  `columbia_river__pend_d_oreille_river_into_columbia_river`,
  `de_boville_slough__hyde_creek_into_de_boville_slough`, `dutch_creek__dutch_creek_into_columbia_river`,
  `halfway_river__halfway_river_into_peace_river` (see medium),
  `kootenay_river__navigation_dolphin_2000_m_downstream` (not in the registry),
  `ksi_gingolx__kincolith_river_bridge`, `morice_river__gosnell_creek_into_thautil_river`,
  `morice_river__thautil_river_into_morice_river`, `pallant_creek__bridge_near_mouth`,
  `pend_d_oreille_river__{seven_mile_dam,seven_mile_reservoir_head,waneta_dam}`,
  `pitt_river__garibaldi_pitt`, `qualicum_river__hwy_19a_bridge`,
  `skeena_river_kispiox_river_confluence__kispiox_river_into_skeena_river` (the rule binds the sign
  zone instead), `squamish_powerhouse_channel__ashlu_creek_into_squamish_river`,
  `squamish_river__powerline_crossing_at_9_mile`, `telkwa_river__howson_creek_into_telkwa_river`,
  `wood_river__hamber_prov_park_boundary` (the rule uses the park area), `yakoun_river__6_mile_bridge`
  (only a note in DFO R6 names it).

## After the next rebuild

- Wahpeeto r1 should draw about 100 m of Wahpeeto Creek below FISS 29119.
- The bundle's `split` table carries the new ids. `test_rulings_pinned` reads either the new or the old
  id.
- Run `python -m pytest pipeline/tests/test_splits.py pipeline/tests/test_t89_rules_that_bind_no_water.py`
  against the new bundle.
