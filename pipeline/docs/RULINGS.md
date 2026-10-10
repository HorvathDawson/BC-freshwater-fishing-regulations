# Interpretation rulings

This is the authoritative list of every ruling the user (the owner) has made about how the
regulations are read. When code, a doc or a memory note disagrees with this file, this file wins.
If you need a reading that is not here, ask the user. Do not infer one.

**Sources.** These rulings were copied on 2026-10-08 from places outside git:
- the handoff `DECISIONS.md`;
- the USER ANSWERS sections of `QUESTIONS.md`;
- `RULES/READY.md`;
- the answers 2.1/2.2 scope notes;
- the agent memory (`zone-rules-are-the-base.md` and related notes);
- the "Reading the regulations" section of `AGENTS.md` (rules 45-61).

AGENTS.md stays the place for the *mechanics*. This file is the place for *what the book means*.

**Pages.** Pages are the book's PRINTED page numbers (the footer number). For the repo's
synopsis PDF, the printed page is the PDF page minus 2 up to p.40, and the PDF page minus 6 after
that. Printed landmarks:
- provincial pages: 4-10;
- Region 1 box: 13;
- Region 2: 21;
- Region 3: 28;
- Region 4: 34;
- Region 5: 42;
- Region 6: 49;
- Zone 7A: 58;
- Zone 7B: 64;
- Region 8: 68;
- definitions: 80.

Never cite a PDF page without writing "PDF p.".

**Columns.**
- **Ruling** is one sentence.
- **Date** is the date of the ruling. A later date supersedes an earlier one.
- **Example** names a water and what the ruling means there.
- **Enforced in** gives the constant, function or test that holds the ruling. Every name was
  checked with grep on 2026-10-08. `read` is `pipeline/deliver/bundle/read.py`. `catalogue` is
  `pipeline/regs/parsing/catalogue.py`. Test files are under `pipeline/tests/`.

**Status markers.**
- **⚠ NOT ENFORCED**: nothing in code or tests holds the ruling.
- **◐ PARTIAL**: something holds part of the ruling, or only the curated data holds it and no test
  pins it.
- **⚡ CONFLICT**: the ruling disagrees with another ruling or with the code. The full list is in
  §15.

---

## 1. Zone and base rules (Z)

| ID | Ruling | Date | Example | Page | Enforced in |
|---|---|---|---|---|---|
| Z1 | The provincial and zone rules are the base of every answer and the most important thing to get right; a section's answer is its region's base plus the named water rows that reach it, and every difference must be traceable to one named row. | 2026-09-17 / 2026-09-24 | A Region 4 lake with no row of its own answers exactly Region 4's table; the Kootenay Lake rainbow 10 is a difference traced to the Kootenay Lake row. | 4-10, 13-68 | `read.effective_rules` (the reference reader). **◐ PARTIAL**: the base-plus-deltas test was removed with the table on 2026-09-22, and no test checks every difference against a named row any more. |
| Z2 | A region-wide rule that says "except certain waters / excluding X Lake" binds the WHOLE region; the excepted water's own row overrides it, and the exclusion is not encoded. | 2026-09-24 | Region 2's bass rule binds Mill Lake too; Mill Lake's row speaks over it. Zone B kokanee and the Peace River work the same way. | 21, 64 | Curated data; `test_zone_decisions.py::test_the_2020_restructure_holds_the_whole_region`. ◐ (no test pins Mill Lake itself) |
| Z3 | A water takes the zone rules of the region it lies in; a "See X" pointer row never moves it into X's region. | 2026-09-25 | Mara Lake keeps its own region's base, although its row says "See Shuswap Lake". | 68 | `test_region_homes.py::test_mara_lake_takes_both_regions_bases_and_keeps_region_8s_row`, `test_see_pointers.py::test_every_pointer_lands` |
| Z4 | A river piece takes the zone rules of the region it mostly lies in (its home, by length); a lake that straddles regions gets BOTH bases, and the stricter rule wins. | 2026-09-29 | Ahbau Lake and Mara Lake straddle two regions; on each, the stricter of the two tables applies. | — | `region_home.json` sidecar / `section_home`; `test_region_homes.py::test_ahbau_and_mara_the_most_strict_applies` |
| Z5 | Water rows are region-agnostic: a row and its tributary walk cross region lines. Only zone and watershed rows are clipped to their region. The Fraser has one row per region. | 2026-09-26 | The Similkameen row reaches its Region 3 part. The Fraser's Region 5 row stops at Region 5. | — | `test_placement_rulings.py::test_a_water_row_walks_across_the_region_line`, `::test_a_per_region_row_still_stops_at_its_line` |
| Z6 | A whole-watershed row binds the FWA basin intersected with its region, not just a stream walk. It walks the whole watershed first, then clips to the region. A codeless lake takes the basin of the named-watershed polygon that contains it. | 2026-09-24 | "Fraser watershed" in Region 6 binds every Region 6 water in the Fraser basin, lakes included. | 49 | `test_watershed_parts.py::test_lakes_come_with_the_watershed_even_codeless_ones` |
| Z7 | Region 1's stream bait ban covers Region 1 minus Haida Gwaii; Haida Gwaii streams get only the Haida Gwaii line. | 2026-09-24 | A Yakoun tributary carries the Haida Gwaii bait line, never Region 1's. | 13 | `test_reach_area_bounds.py::test_each_side_of_the_strait_gets_its_own_bait_ban_and_only_that` (slow) |
| Z8 | In Zone 7B, bull trout are 0 (released) region-wide; the Liard watershed row, and any row that prints its own bull trout limit, override that. The Zone B "1 bull trout (Oct 16-Aug 14, 30-50 cm)" clause is held by the Liard row. | 2026-09-24 | Kakwa Lake: the zone's bull trout release speaks; the Liard watershed row's bull trout quota speaks inside the Liard. | 64 | `catalogue.WINDOWS_HELD_ELSEWHERE`; `test_zone_decisions.py::test_zone_b_bull_trout_are_catch_and_release_and_the_liard_row_speaks` |
| Z9 | A zone release limited to a kind of water ("from streams") displaces its own region's quotas for that fish on that kind of water. | 2026-09-29 | Region 3 "Bull trout from streams, Aug 1-Oct 31": no stream keeps a bull trout then. | 28, 34 | `read.released_on_water`; `test_zone_release_by_water.py` |
| Z10 | An area closure includes a lake that lies only partly inside it, unless that lake has its own row. A lake that lies mostly outside the area is taken out by name. | 2026-09-29 | Peaceful Lake stays inside the Malcolm Knapp closure. Goose Lake (2.2% inside) is taken out, and so are Kootenay Lake (Creston Valley WMA) and Bennett Lake (Chilkoot Trail). | — | `Extent.outside_items` / `areas.json` `outside_items`. ◐ (curated data; no test names Peaceful Lake or Goose Lake) |
| Z11 | When the book's geography puts a water OUTSIDE an area its polygon touches, that is stated once on the area itself, never on a rule. | 2026-10-03 | Kennedy Lake is outside Pacific Rim. | — | `registry.outside_area_items`; `test_phase3_rulings.py::test_kennedy_lake_is_outside_pacific_rim` |
| Z12 | National parks are closed by default ("prohibited unless opened under the National Parks Fishing Regulations"), and provincial licences do not apply inside them. | 2026-10-06 | Yoho, Kootenay, Glacier and Mt Revelstoke all show closed; provincial licences are "displaced" there. | 9 | `test_zone_decisions.py::test_national_parks_close_by_default_and_provincial_licences_do_not_bind_there`; `test_requirements_g5.py`. The proviso is DISPLAYED (Q41, answers 2.3, 2026-10-09): a full closure with an advisory proviso (`condition_of`) says `display.UNLESS_OPENED` — "Closed unless opened by Parks Canada — a national park fishing permit is required." (`display.rules[].unless_opened`), and a display frame names the closing rules whose proviso is the answer there (`unless_opened`, `display.unless_opened_here`); the three park RESERVES (`zp:national_park_reserves.r1`) close the same fish with no proviso, so they read plainly closed. `test_answers_23.py` (`::test_built_a_national_park_says_unless_opened_and_a_park_reserve_is_plainly_closed`). |

## 2. Quotas, sizes and competition (Q)

| ID | Ruling | Date | Example | Page | Enforced in |
|---|---|---|---|---|---|
| Q1 | Competition is decided PER FISH, among the rules in force on the date asked. A dated water rule gives way to its region outside its dates. A lift is in force only while its lifter is. | 2026-09-25 | Tchesinkut Lake: in February and July the region's rules speak, not the lake's dated rule. | — | `read.effective_rules`; `test_competition.py::test_tchesinkut_february_and_july_are_the_regions` |
| Q2 | For a given fish, a rule that NAMES that fish beats a rule that names a GROUP containing it, even when the group rule is at a more specific place. A water row that explicitly names the species beats the zone. At the same naming level, water beats zone, and zone beats province. | 2026-09-25 | Kakwa Lake: the zone's bull trout release beats the lake's "Trout/char 2". A row's "Bull trout quota = 1" beats the zone's bull trout rule. | — | `test_competition.py::test_a_water_row_naming_bull_trout_beats_the_zone`, `::test_kakwa_and_cecilia_bull_trout_are_released_other_trout_keep_the_lakes_two` |
| Q3 | A water row's release, or another stricter rule for a species, displaces the zone's rules for that species, even when the zone rule is conditioned. | 2026-09-25 | A lake's "rainbow trout catch and release" silences the zone's hatchery-only rainbow count there. | — | `test_zone_decisions.py::test_a_water_quota_never_sits_under_a_smaller_conditioned_zone_count` |
| Q4 | When a water row prints a LARGER number for a fish, it replaces the zone's number for that fish. When water and zone print the exact same statement, the water's number wins. A different, smaller statement sits BESIDE the zone's. | 2026-09-26 (corrects 2026-09-24) | Kootenay Lake rainbow 10 replaces Region 4's 5. The Dean's steelhead 1 counts toward Region 5's 5 trout/char. | 37, 42 | `test_competition.py::test_a_larger_water_number_for_a_fish_replaces_the_zones_for_that_fish`, `test_kootenay_lake_rainbow_10_replaces_region_4s_5`, `test_the_dean_still_sits_beside_region_5` |
| Q5 | A larger water quota also overrides the zone's "1 over 50 cm". Only a row that prints "(any size)" also carries an interpretation caution: does it mean no size limit, or only no minimum? | 2026-09-28 | Rows printing "(any size)" get the caution; rows that print their own sizes, or no size, do not. | — | `test_water_dates_any_size_chinook.py::test_the_caution_only_where_the_row_prints_any_size` |
| Q6 | A water row that prints its OWN dates for a fish overrides a dated zone rule for that fish on the overlapping days only. An undated water quota never silences a dated zone release unless the two are the exact same statement. | 2026-09-25 / 2026-09-28 | Cheslatta and Murray lakes, lake trout, Nov 1-30: the lake's 3 applies. Shuswap Lake's "char 1" does not silence Region 3's lake trout release, Oct 15-Jan 31. | 28, 49 | `test_displacement_rulings.py::test_cheslatta_keeps_region_6s_quotas_on_nov_1_to_30`; `test_trout_scope_and_dated_releases.py::test_the_exact_same_statement_on_the_same_dates_replaces_it` |
| Q7 | A water row's own dated quota also overrides a water-kind zone release ("from streams") on the overlap. | 2026-09-28 | Michel Creek's own dated release replaces Region 4's stream release on its dates. | 34 | `test_displacement_rulings.py::test_a_water_rows_own_dates_override_a_stream_release`, `::test_michel_creeks_own_dated_release_replaces_region_4s_stream_release` |
| Q8 | A rule that only lifts never competes. A rule's conditions (origin, `while`, kind of water, size-only) are part of its competition key. | 2026-09-25 | A hatchery-only quota and a wild release do not compete as one subject. | — | `test_zone_decisions.py::test_a_rule_that_only_lifts_never_competes`, `::test_a_rules_conditions_are_in_its_key` |
| Q9 | MOOT SIZE CLAUSE (Option A): a zone size-only clause ("none under 60 cm") is HIDDEN under any outright release or closure of that fish in force that covers every origin the clause covers, from any source (water, zone or superior authority). | 2026-10-05 | Bonaparte Lake, Nov 1, lake trout: only the release shows. Eleven Mile Creek, Aug 1: bull trout. | — | `read.MOOT_SIZE_CLAUSE_HIDDEN`; `test_moot_size_clause.py` |
| Q10 | WATER CLOSURE DOMINANT: a water's OWN full closure in force silences every keeping rule for the fish it covers (zone, area rows, walked rows, possession, annual, size). Only a superior authority's rule and the closure's own lifts still stand. | 2026-10-06 | Denetiah Creek, Jul 1-15: only its closure shows, not the Liard row's bull trout "1 in possession". | — | `read.WATER_CLOSURE_DOMINANT`; `test_reader_answers.py::test_denetiah_creek_its_own_closure_is_all_that_speaks_for_bull_trout` |
| Q11 | A closure bound to PART of a water dominates that part's sections exactly as a whole-water closure would. | 2026-10-06 (C10) | The Chilliwack above the Slesse signs. | 22 | `test_part_runs.py::test_chilliwack_closed_above_the_slesse_signs` |
| Q12 | These rulings were made 2026-10-03 and built in Phase 3:<br>• **RU-3**: a dated release or closure in the same row silences that row's undated counted quota on its dates.<br>• **RU-4**: a zone release with no water kind and no `while` empties its own table's keepers.<br>• **RU-5**: a water's take-0 size band displaces a zone size clause that lies wholly inside it.<br>• **RU-7**: a full closure displaces the keepers it beats.<br>• **RU-8**: when two regions print the identical statement, it shows once. | 2026-10-03 | Quatse: its dated closure silences its own quota. Zone 7B: the grayling release, on its dates, empties the table's "2 per day". | 64 | `read.effective_rules`; `test_competition.py` |
| Q13 | A fish exactly ON a printed size bound is legal, under both "none under X" and "no … over X". The book's "X cm or more" and "X cm or less" keep X inside the band (`closed: true`). | 2026-10-07 (Q9) | A lake trout of exactly 30.0 cm under "none under 30 cm" may be kept. Under "40 cm or more", a 40 cm fish is denied. | 80 | `catalogue.EXACT_BOUND_IS_LEGAL`; `test_rules_round.py::test_q9_none_under_x_keeps_x`, `::test_q9_the_books_or_more_or_less_bands_are_closed`. ⚡ see C-3 |
| Q14 | A size limit is `lengths`: ordered ranges, and the first match wins. A length that no range covers is not spoken about. | 2026-09-22 | Ruby Lake: `[{min_cm: 40, take: 0}, {max_cm: 40}]` denies 40 cm and up first. | — | `test_lengths.py` |
| Q15 | The possession quota is 2× the daily quota, except at your place of ordinary residence; that exception is part of the definition of every possession quota. | 2026-10-07 (Q39) | Fish in your home freezer do not count toward possession. | 80 | One advisory under `quota_defaults` (`quota_defaults.r4`, RR23) plus `glossary.py` wording; every possession line the answers say carries it in one wording, `display.POSSESSION_HOME` ("Possession: twice the daily quota (fish at home don’t count)."; "Have no more than 1 Arctic grayling in possession (fish at home don’t count).") — answers 2.3, 2026-10-09; `test_answers_23.py::test_every_possession_line_says_fish_at_home_dont_count`, `::test_the_possession_exception_is_worded_once`. The export's `label` is unchanged (RR23). |
| Q16 | Region 1/2 "no more than 1 over 50 cm (2 hatchery steelhead over 50 cm allowed)" means at most 2 fish over 50 cm in total, of which at most 1 may be something other than a steelhead. | 2026-10-07 (Q44) | 2 hatchery steelhead and 1 rainbow, all over 50 cm, is one fish too many. | 13, 21 | `z1`/`z2` `r2b`; `test_rules_round.py::test_q44_at_most_two_over_50_in_total` |
| Q17 | A count limit that several kinds share is shared across all of them. | 2026-10-08 (decision B) | Region 6 "1 trout from streams Jul 1-Oct 31": on the Babine and the Skeena, 1 in total, not 1 of each kind. | 49 | `rows._shared_counts`; `test_answers_v22.py::test_b_region_6_stream_trout_cap_is_one_for_all_trout_kinds_together` |
| Q18 | "All other species" or "all species" in a water's release means game fish only. A bare "catch and release" covers all game fish, and crayfish may still be kept. | 2026-09-25 | A lake's "catch and release" does not stop crayfish trapping. | 80 | `test_catalogue.py::test_all_other_species_is_game_fish`, `::test_a_bare_catch_and_release_is_game_fish_and_crayfish_may_be_kept` |
| Q19 | "Kokanee 5 (none from streams)" means kokanee must be released on streams. It does not mean "no targeting". | 2026-10-07 (Q42) | Kokanee caught in a stream must be released. | — | `test_catalogue.py::test_kokanee_streams_is_species_scoped_not_a_water_closure` |
| Q20 | Only lines that print dates are dated. A permanent line in a row with dated lines stays permanent. | 2026-10-07 (Q23) | Adams Lake: the bull trout and lake trout quota is permanent. Adams River: the "Bull trout 1 (none under 80 cm)" with dates applies only on those dates; outside them the zone applies. | 29 | Curated data; the guide example `adams_lake_release_over_its_1` (`pipeline/tools/guide_examples.py`). ◐ |
| Q21 | Lonzo Creek is a stream, so it keeps the zone's "2 from streams"; its own window lifts only the release of hatchery fish under 30 cm. | 2026-10-07 (Q22) | Lonzo Creek, Jul 1: 2 trout/char from streams. | 24 | `test_rules_round.py::test_fix_lonzo_window_lifts_the_hatchery_under_30_release` |
| Q22 | The Kootenay Lake West Arms' combined kokanee pool of 5 is a note on each arm. It is not modelled across entries. | 2026-10-06 (F10) | The Lower West Arm card shows the note. | 37 | By ruling, a note only. |

## 3. Closures and lifts (L)

| ID | Ruling | Date | Example | Page | Enforced in |
|---|---|---|---|---|---|
| L1 | STRICT LIFT: a zone closure and a water row BOTH apply, and the water is closed on the UNION of their closed dates. A row's own closed season never shortens the zone's. A row beats a zone closure ONLY where (a) the book prints an exemption, or (b) the row prints a dated catch and release, opening or quota INSIDE the closure. A (b) lift holds on exactly those dates and for exactly those fish. Every lift must trace to (a) or (b). | 2026-10-05 | Thompson below Kamloops Lake: closed Oct 1-May 31 ∪ Jan 1-Jun 30. Nicola below the lake: trout catch and release Jan 1-Feb 28 only; whitefish stay closed. | 28, 30 | `pipeline/deliver/bundle/rules.py` lifts; `test_lift_decisions.py::test_the_nicola_below_its_lake_is_trout_catch_and_release_on_jan_15_whitefish_closed` |
| L2 | The p.28 list naming the Stein, and the Nahatlatch below its lake, is a STEELHEAD closure list, not a list of exceptions. Both rivers are closed to Jun 30. | 2026-10-05 | `stein_river.r1x` and `nahatlatch_river.r2x` were removed. | 28, 31 | `test_lift_decisions.py::test_the_stein_is_closed_on_june_15_by_region_3s_spring_closure`, `::test_the_nahatlatch_below_its_lake_is_closed_on_june_15` |
| L3 | "Open all year" lifts only FULL blanket closures (spring, winter, summer), never a species closure. Region 6's steelhead closure (May 15-Jun 15) holds on every Region 6 river and stream except the mainstems of the Skeena, Nass, Iskut, Stikine and Taku. | 2026-09-24 / 2026-10-05 (G4) | Skeena mainstem: open for steelhead on May 20. A Skeena tributary: closed. | 49 | `rules.is_blanket_closure`; `test_zone_decisions.py::test_open_all_year_lifts_the_blanket_closure_and_never_a_species_one`; `test_lift_decisions.py::test_the_skeena_mainstem_is_open_for_steelhead_on_may_20_and_a_tributary_is_closed` |
| L4 | The steelhead mainstem exemption is read day by day. | 2026-09-24 | The Skeena above Cedarvale opens Jun 1, after the winter closure ends May 31. | 49 | Same tests as L3 |
| L5 | An "equivalent" lift in another region matches the KIND of closure the row names. | 2026-09-26 | "Exempt from spring closure" lifts other regions' spring closures, never their winter closures. | — | `rules._equivalent_closures`; `test_a_spring_exemption_lifts_the_other_regions_spring_closure_never_its_winter_one` |
| L6 | (G3) A printed exemption carries to the same kind of closure in a neighbouring region ONLY where the water has no entry of its own in that region. | 2026-10-05 | The Fraser and the Canim never carry their exemptions. The West Road's Region 6 and Zone 7A mainstem pieces keep their lift. | — | `rules.equivalent_regions`, `own_entry_regions`; `test_lift_decisions.py::test_an_exemption_carries_only_where_the_water_has_no_entry_of_its_own` |
| L7 | No lift reaches a tributary, in any region. | 2026-10-05 | West Road: the mainstem is open Jun 15; its tributaries take the spring closure. | — | `test_lift_decisions.py::test_west_roads_tributaries_are_closed_on_june_20_and_its_mainstem_is_open` (see also T3) |
| L8 | (G5) A row's DATED bait ban REPLACES its zone's all-year bait ban. | 2026-10-05 | Quatse, Somass, Sproat and Stamp (Region 1). | 13-19 | Export gotcha `dated_bait_ban_replaces_zone`; `test_displacement_rulings.py::test_region_1_rows_with_a_dated_bait_ban_are_its_exceptions` ⚡ see C-5 |
| L9 | Duck Lake's creeks stay closed Apr 1-Jun 14: the lake row's bass catch and release does not lift the stream closure. | 2026-10-05 | A Duck Lake creek on May 20: closed. | 36 | `test_lift_decisions.py::test_duck_lakes_creeks_are_closed_to_bass_on_may_20_and_the_lake_is_catch_and_release` |
| L10 | Beaver Creek is always closed to bass, lake and stream alike; the entry does not lift the closure. | 2026-10-05 | — | 42 | `test_displacement_rulings.py::test_the_beaver_creek_chain_lakes_keep_their_bass_closure` |
| L11 | A water row that NAMES a fish its zone closes lifts that species closure on that water. It never lifts:<br>• an all-fish seasonal closure;<br>• a closure that prints its own exemption list;<br>• a single-fish closure, when the row's quota is for a group. | 2026-10-08 (confirmed as built) | Creston bass, and French and Lewis/Cameron sloughs, are lifted. A trout/char quota of 1 does not reopen steelhead. | — | `names_the_fish` (`rules.py`, `read.py`); `test_competition.py::test_a_group_row_never_reopens_a_named_closure`; `test_zone_decisions.py::test_a_closure_printing_its_own_exemptions_takes_no_derived_lift` |
| L12 | A derived lift never reopens more than the lifter covers, and a lift limited to one origin lifts only that origin. | 2026-09-26 / 2026-10-06 | Kitimat's hatchery steelhead 2 does not lift Region 6's steelhead closure for wild fish. | 49 | `test_competition.py::test_kitimat_steelhead_closure_stands_for_wild_and_hatchery`; `effective_rules(origin=…)` |
| L13 | Region 5's spring closure "EXCEPT … other streams listed in the tables" exempts EVERY stream that has a row in the tables. | 2026-09-29 | A Region 5 river with its own row is open in spring. | 42 | `test_displacement_rulings.py::test_region_5_listed_streams_are_out_of_the_spring_closure` |
| L14 | The Stellako stays under the Fraser-watershed spring closure: closed Nov 15-Jun 30. | 2026-10-02 | — | 58 | Curated data. ◐ (no test pins the dates) |
| L15 | Bowron Lake Park waters carry no spring-closure lift: the rows print only quotas and gear. | 2026-10-07 (Q26) | The park's streams take Region 5's spring closure. | 43 | `test_rules_round.py::test_q26_bowron_park_waters_lift_no_spring_closure` |
| L16 | Chimney Creek is open all year below Brunson Lake; upstream it keeps the spring closure. | 2026-10-07 (Q27) | — | 44 | Curated data. ◐ (no test) |
| L17 | The Seton River's "Exempt from spring closure" applies to the ENTIRE river, the BC Hydro power canal included. Only "no trout under 25 cm" and "No Fishing Apr 1-May 31" are limited to below Seton Lake. | 2026-10-07 (Q1/Q25) | Seton Portage (between Anderson and Seton lakes) is open in spring. | 31 | `test_rules_round.py::test_q25_seton_canal_is_part_of_the_seton_row` |
| L18 | A closure ("No fishing") is not a release. A closure means no gear in the water. A release means you may fish but must let the fish go. | 2026-10-05 | — | 80 | `rules.closure_grade` |
| L19 | "No gear in the water during a closure" applies in EVERY region. | 2026-10-06 (C11) | — | 4-10 | `test_answers_gear.py::test_no_gear_in_the_water_during_a_closure_speaks_in_every_region` |
| L20 | Every entry whose own closure overlaps a zone seasonal closure gets a reader-facing gotcha saying that both hold. | 2026-10-05 | Nahatlatch, Thompson, Stein, Guichon, Mahood, Stellako, Bridge, West Road. | — | Export gotcha `closures_combine`; `export_ui_rules.closures_combine_problems` |
| L21 | "Open June 16-Apr 30" is stored as a closure for the days it leaves out, and those printed open dates lift the winter closure. | 2026-09-23 / 2026-10-05 | The Fulton River is closed May 1-Jun 15. | 51 | `test_displacement_rulings.py::test_fulton_river_is_open_june_16_to_april_30` |

## 4. Tributaries and ✱ (T)

| ID | Ruling | Date | Example | Page | Enforced in |
|---|---|---|---|---|---|
| T1 | "Tributaries" means STREAMS only; "watershed" includes lakes. | 2026-09-24 | A lake on a tributary is not covered by a "including tributaries" rule. | 80 | `test_tributary_lakes.py::test_the_rule_collects_the_streams_only` |
| T2 | A row whose NAME carries the global ✱ includes tributaries for EVERY rule, even when a clause repeats the ✱. Only a row WITHOUT the global ✱ narrows the walk to the clause that prints one. | 2026-10-07 (reverses RR1) | San Juan, Inland, Sooke, Bull River and Kitsumkalum walk every rule. Oyster r2, Anderson r4 and Dinosaur r1 walk on their starred clause only. | 4 | `test_rules_round.py::test_q4_a_global_star_row_with_a_repeated_inline_star_walks_every_rule`, `::test_q4_no_global_star_the_starred_clause_alone_walks`, `::test_q4_every_global_star_row_walks_every_rule_it_does_not_scope_in_words` |
| T3 | No exemption printed on a ✱ row reaches its tributaries: the ✱ carries the row's closures, quotas and gear, never its lifts. | 2026-10-07 (Q3) | A Quinsam tributary is closed Jul 15-Aug 31. The rule covers Nitinat, Duncan, Lardeau, Dutch, Hevenor, Fulton, Babine's Rainbow Alley and the Similkameen. | 4, 17-18, 36-38, 51 | `test_rules_round.py::test_q3_a_lift_on_a_starred_row_stays_on_its_water`, `::test_q3_every_lift_on_a_starred_row_is_listed`; gotcha `exemption_stays_on_its_water` |
| T4 | A tributary with its own row gets BOTH its own rules and the rules it inherits. On one competition key its own row speaks, but an inherited CLOSURE still closes it; only a lift removes a closure. | 2026-10-07 (Q5, confirmed) | Granby and West Kettle are closed Jul 25-Sept 15 under the Kettle's tributary line; Granby's own quota beats the inherited rainbow release. | 69-70 | `read.OWN_ROW_BEATS_INHERITED`; `test_rules_round.py::test_q5_own_row_beats_the_inherited_release`, `::test_q5_an_inherited_closure_still_closes` |
| T5 | Squamish: the creeks flowing into the excepted rivers (Ashlu, Cheakamus, Elaho, Mamquam, Powerhouse Channel) are still Squamish tributaries and stay CLOSED. | 2026-10-07 (Q6) | A creek into the Elaho is closed. | 26 | `test_rules_round.py::test_q6_squamish_excepted_rivers_creeks_stay_closed`; gotcha `excepted_rivers_creeks_stay_closed` |
| T6 | Campbell River "No Fishing in any tributaries (except Quinsam River), Dec 1-May 31" binds the tributaries of the Campbell from Strathcona Dam to the sea, Quinsam excepted. | 2026-10-07 (Q7) | Creeks into the river below Strathcona Dam are closed Dec 1-May 31. | 14 | `test_rules_round.py::test_q7_campbell_tributaries_from_strathcona_dam` ⚡ see C-1 |
| T7 | Michel Creek: the Elk exception is lower Michel ITSELF. Its own tributaries stay Elk River tributaries and keep the Elk's closure (`walk_past`). | 2026-10-07 (corrects the round-1 answer) | Lower Michel's tributaries are closed Sept 1-Oct 31. | 36 | `test_rules_round.py::test_q8_lower_michel_itself_is_carved_out` |
| T8 | A closure's tributaries are closed like any other tributaries, unless the book excepts them. A spot closure ("500 m radius") also closes the tributaries joining within the spot. | 2026-09-30 | Atnarko r2 does not close the Hotnarko, which the book excepts. | 46 | `test_placement_rulings.py::test_the_atnarko_excepts_its_three_waters_on_every_walking_rule`. ◐ (spot closures: as built, no test) |
| T9 | Joining water at a confluence cut:<br>• **no row of its own**: it goes with the cut on BOTH sides;<br>• **a row of its own**: it is left out;<br>• **signs at the confluence**: it is pulled in like a `between`, up to its first lake. | 2026-09-30 / 2026-10-03 | Bannon Creek takes both Chemainus closures. The Morice, Lardeau and Iltasyuko are left out. The Nass takes the Meziadin up to Meziadin Lake. | — | `reach.build._stem_to_first_lake`; `test_placement_rulings.py::test_bannon_creek_has_no_row_so_it_takes_both_chemainus_closures`, `::test_the_bulkley_closure_stays_out_of_the_morice`; `test_phase3_rulings.py::test_the_nass_closure_takes_the_meziadin_to_its_lake_and_no_further` |
| T10 | An undrawn-part rule never walks tributaries. | 2026-10-07 (review 2) | Dinosaur r1 no longer closes 1,109 km of tributaries. | 64 | `classify.UNDRAWN_PART_DOES_NOT_WALK`; `test_rules_round.py::test_an_undrawn_part_rule_does_not_walk`; gotcha `undrawn_part_tributaries` |
| T11 | A tributary walk continues across water outside B.C. and resumes where the stream re-enters; the pieces outside B.C. carry no rules. | 2026-10-06 (A3/B4) | Chilliwack tributary 356233151 keeps Chilliwack/Vedder r1 above its 424 m in Washington. | — | `tributaries.ORDER_ZERO_IS_UNKNOWN`; `test_reach_tributaries.py::test_a_walk_continues_across_water_outside_bc_and_resumes_inside` |
| T12 | The Williston Lake "Zone B" row's tributary walk stays inside Zone B: an exception to region-agnostic walks, because the row's own words scope it. | 2026-09-24 | — | 64 | `test_placement_rulings.py::test_a_row_named_for_its_zone_says_so_in_its_extents` |

## 5. Trout and char (C)

| ID | Ruling | Date | Example | Page | Enforced in |
|---|---|---|---|---|---|
| C1 | The book's species list is closed: 22 fish in TROUT, CHAR, WHITEFISH, BASS and OTHER. A bull trout IS a Dolly Varden. Chinook is the one salmon the book names; it sits in SALMON and is never a game fish. | 2026-09-25 | "Bull trout" is stored as `DV`, and `BT` is refused. | 80 | `catalogue.BOOK_FAMILIES`; `test_catalogue.py::test_all_game_fish_is_the_closed_list_and_excludes_salmon` |
| C2 | "Trout" INCLUDES char unless (a) the line excludes char in so many words, or (b) the same row or zone table gives char a RELATED rule of its own. Related means: the same aspect (retention, size or gear), a compatible kind of water, and days that meet. A combined "trout/char" never excludes char. | 2026-10-07 (supersedes 2026-09-28) | Region 1's "2 from streams (must be hatchery)" is trout only, because of "All char". "Wild trout/char quota = 2 (no wild trout over 40 cm)": the 40 cm cap covers char. "No trout under 30 cm" beside "bull trout release Aug 1-Oct 31" covers bull trout. | 80 | `catalogue.related_rules`, `catalogue.rule_aspects`, `catalogue.trout_scope_problems`; `test_rules_round.py::test_q45_*` (4 tests) |
| C3 | A user-facing note must say that "trout" includes char. | 2026-09-28 | — | 80 | `glossary.py` term. ◐ (no test pins the note) |

## 6. Steelhead (S)

| ID | Ruling | Date | Example | Page | Enforced in |
|---|---|---|---|---|---|
| S1 | Steelhead regulations come only from the book. The provincial and zone steelhead rules bind STREAMS in Regions 1, 2, 3, 5 and 6. | 2026-10-01 | Region 4 streams carry no zone steelhead rule. | 7-8 | `test_steelhead_streams.py::test_every_provincial_and_zone_steelhead_rule_binds_streams_only` |
| S2 | A STEELHEAD ROW is any row that PRINTS steelhead. That means a rule naming steelhead, or the Steelhead Stamp in any wording, including a waiver. A designation that prints no steelhead wording is not one. | 2026-10-02 | Kingcome, Wakeman, Seymour, Kakweiken and Ahnuhati are steelhead rows. Region 4's "Class II water when open" rows are not. | — | `steelhead.prints_steelhead`; `test_steelhead_waters.py::test_a_stamp_waiver_row_is_a_steelhead_row` |
| S3 | A LAKE is steelhead water ONLY as a steelhead row's own water. It never becomes one because another line of a steelhead row binds it, and never because of the curated list. | 2026-10-06 (D13) | Khartoum and Lois are steelhead water ("hatchery steelhead"). Tenas Lake, bound by the Atnarko's closure, is not. | 46 | `steelhead.LAKES_ONLY_BY_OWN_ROW`; `test_steelhead_streams.py::test_a_lake_whose_own_row_prints_steelhead_carries_the_set` |
| S4 | Being book-known does not spread along tributaries: there is no tributary walk for steelhead presence. | 2026-10-02 | — | — | `pipeline/atlas/reach/steelhead.py` (no walk) |
| S5 | The curated steelhead list binds no rule. It only marks its waters as `steelhead: known`, and big rainbow there are read as steelhead only where steelhead rules already apply. | 2026-10-03 (final) | The Okanagan River (Region 8) is known, but its rainbow rules answer for every rainbow. | — | `test_steelhead_waters.py::test_adding_any_water_to_the_list_changes_only_presence_and_anadromous` |
| S6 | `anadromous_rainbow` (a rainbow over 50 cm counts as a steelhead) holds exactly where the water is KNOWN, is a stream, and has steelhead rules that apply. | 2026-10-03 (final) | On the Chilliwack, a rainbow over 50 cm is a steelhead. | 22 | `test_competition.py::test_chilliwack_rainbow_over_50_cm_is_a_steelhead`; `test_bundle_licensing.py::test_anadromous_where_no_steelhead_rule_applies_is_refused` |
| S7 | Where no steelhead rule applies, the rainbow rules apply to a steelhead. | 2026-10-03 | Okanagan (Region 8) and the Fraser in Zone 7A: "ST" is answered as "RB". | — | `section_steelhead_rules` (`answers/common.py`); the `steelhead_rules: false` export flag |
| S8 | STAMP WAIVER: "(Steelhead Stamp not required)" means NO steelhead stamp on that designation's sections while it is in force, classified or provincial. The Classified Waters Licence and every steelhead rule still apply. A waiver "unless fishing for steelhead" lifts only the classified-water stamp. | 2026-10-02 | The Chilko needs no stamp. On the Seymour, Ecstall and Skeena, the provincial stamp still applies when fishing for steelhead. | — | `test_steelhead_stamp_waiver.py::test_a_conditional_waiver_does_not_lift_the_provincial_stamp`, `::test_the_lift_needs_both_the_consent_and_an_outright_waiver` |
| S9 | A zone trout/char size line leaves steelhead out ONLY where the same table prints its own steelhead quota. Region 6 prints none, so hatchery steelhead count toward Region 6's "1 over 50 cm". | 2026-10-08 (decision D) | Babine: a hatchery steelhead over 50 cm is the one fish over 50. | 49 | `catalogue.steelhead_scope_problems`; `test_answers_v22.py::test_d_region_6_one_over_50_counts_hatchery_steelhead`, `::test_d_region_2_one_over_50_leaves_steelhead_to_their_own_line` |
| S10 | Khartoum and Lois "Rainbow trout/hatchery steelhead quota = 6": wild steelhead count 0. | 2026-10-07 | — | 23 | `test_rules_round.py::test_fix_khartoum_lois_wild_steelhead_count_none` |
| S11 | Babine "Steelhead Stamp mandatory Sept 1-Oct 31" applies to ANY fishing in that period, and to steelhead fishing otherwise. A document's `when` is its broadest need. | 2026-10-08 (decision F) | Babine, Sep 20, fishing for trout: the stamp is needed. | 50 | `licence.py` L10 (`also_when`); `test_answers_v22.py::test_f_babine_stamp_is_for_any_fishing_in_its_period_and_for_steelhead_otherwise` |

## 7. Sturgeon (W)

| ID | Ruling | Date | Example | Page | Enforced in |
|---|---|---|---|---|---|
| W1 | White sturgeon may be fished (catch and release) ONLY in the licensed Fraser watershed: from the CPR Bridge at Mission up to and including the Williams Lake River, tributaries included. They are CLOSED everywhere else, and a region's "release all sturgeon" is that closure. | 2026-10-07 (Q40, replaces RR21) | Region 8: closed to white sturgeon. The Harrison and the Thompson system (inside the licensed area): catch and release. | 7 | `zp:white_sturgeon_licence.r4` / `r4x`; `test_rules_round.py::test_q40_white_sturgeon_closed_everywhere_but_the_licensed_fraser`; `test_watershed_parts.py::test_the_sturgeon_licence_is_mission_to_and_including_the_wlr` |
| W2 | Dead fin fish (head or headless body) may be used as sturgeon bait only where p.8 lists it. The list is the Region 2 Fraser, the Lower Pitt (CPR Bridge to Pitt Lake) and the Lower Harrison (Fraser River to Harrison Lake). | 2026-10-07 | — | 8 | `bait.r3`; `test_rules_round.py::test_dead_fin_fish_for_sturgeon_where_p8_lists_it` |

## 8. Gear and bait (G)

| ID | Ruling | Date | Example | Page | Enforced in |
|---|---|---|---|---|---|
| G1 | Bait is one domain: a bait ban bans every kind of bait. The only printed exemption is dead fin fish for sturgeon (W2). | 2026-09-24 | — | 8 | `test_zone_decisions.py::test_bait_is_one_domain_and_a_ban_meets_the_invertebrate_allowance` |
| G2 | Spear fishing is a METHOD with restrictions, not a retention zero. It is banned for game fish, allowed for burbot in some regions, and banned outright in Regions 1, 2 and 4. | 2026-09-24 | — | 8 | `test_zone_decisions.py::test_spear_fishing_is_a_method_rule_province_wide` |
| G3 | Kootenay Lake main body: unlimited rods ONLY from a boat. From shore, the province's 1 line applies. On other lakes, someone alone in a boat may use 2 lines. | 2026-10-06 (F12) | Kamloops Lake: 1 line, or 2 when alone in a boat. | 37 | `test_answers_gear.py::test_line_counts_kootenay_boat_unlimited_shore_province_other_lakes_two_alone_in_a_boat` |
| G4 | Set lining is allowed on Region 6 and Zone 7A lakes unless the table says otherwise, so a fly-only rule does not forbid it. | 2026-10-07 (Q32) | Nilkitkwa Lake: set lining is allowed. | 53 | `test_rules_round.py::test_q32_nilkitkwa_fly_only_does_not_bind_set_lining` |
| G5 | Coquihalla fly-only applies only upstream of the uppermost railway tunnel, Jul 1-Oct 31. | 2026-10-07 (Q17) | The lower river's Nov-Mar catch-and-release fishery is not fly-only. | 22 | `test_rules_round.py::test_q17_coquihalla_fly_only_is_upstream_of_the_tunnel_jul_to_oct` |
| G6 | A fish snagged, wilfully or accidentally, must be released. | 2026-10-07 (Q38) | A trout foul-hooked while casting must be released. | 8, 80 | `zp:prohibited_methods.r3` is `caught: [foul_hooked]` (2026-10-09; the interim `while: snagging`, RR22, is gone and refused): `catalogue.CAUGHT_HOW`, a condition on the FISH — never an outright release or a closure (`rules.release_origins`, `rules.CLOSURE_CONDITIONS`), its own statement and key (`daily@caught=foul_hooked`), so it decides no quota for a fish hooked in the mouth. The answers say "Any fish snagged — even by accident — must be released." (`display.caught_sentence`, kind `caught`) and list it in every gear answer (`gear.caught`). `test_snag_duty.py`. |
| G7 | "Single barbless hook" means one barbless hook. Barbless is a restriction (`only: [barbless]`), never a permission, and the "single" is kept. | 2026-09-23 | Naglico, Ogston and Sayres lakes: `points_per_hook max 1`. | — | Gear model (`test_gear_model.py`); curated data. |
| G8 | Boat rules: "parts of" a lake is a note on the whole lake; and a speed limit "on part" of a river is a note on the whole river. A towing or engine rule follows its printed reach. | 2026-10-07 (Q28-Q31) | Heffley Lake towing, Chilko 5 km/h, Columbia no towing Fairmont-Donald, Salmon River (both clauses downstream of the Hwy 97 bridge). | 30-35, 44 | `test_rules_round.py::test_q29_heffley_parts_are_a_note_on_the_whole_lake`, `::test_q28_chilko_5_kmh_is_a_note_on_the_whole_river`. ◐ (Columbia and Salmon: curated data only) |

## 9. Licences (Li)

| ID | Ruling | Date | Example | Page | Enforced in |
|---|---|---|---|---|---|
| Li1 | Licensing NEVER affects whether a water is open, closed or restricted. | 2026-09-23 | A Class II water is coloured by its rules, never by its licence. | — | Export guide prose (`export_ui_rules.py`); the status index reads only `closure_grade`. ◐ (by construction; no test) |
| Li2 | The angler is always unknown: answers are conditional ("if you are a non-resident…"), never a default profile. Every profile ships, Métis included. | 2026-09-23 / 2026-10-05 | — | 4-6 | `test_answers_licence.py::test_profiles_are_sixty_and_indexed_by_arithmetic` |
| Li3 | When a youth's catch counts toward the accompanying adult's quota, that is a NOTE on the licensing record, not quota arithmetic. An under-16 non-resident fishes either accompanied by a licensed adult, or as that adult would. Both paths show on the tile and in "You need". | 2026-09-23 / 2026-10-08 (decision A) | Kootenay Lake, an under-16 non-resident. | 4-6 | `test_licensing_model.py::test_the_under_16_non_resident_is_an_accompaniment_path` |
| Li4 | The classified period reuses `When`. "Class II water WHEN OPEN" needs no field, because a closed water needs no licence. | 2026-09-23 | — | — | `test_part_runs.py::test_every_designation_says_its_period`, `::test_a_dated_period_is_its_own_when` |
| Li5 | Classified Waters: each section has ONE unit, taken from the FIRST classified water it flows into. A tributary whose own row prints no designation is not classified, and neither is anything above it. A joining water with no row inherits the designation ("when in doubt, classified"). | 2026-09-30 / 2026-10-03 | Gosnell takes the Morice's designation, not the Bulkley's. The Endako is not classified. Limonite inherits both Zymoetz units. | — | `licensing.first_classified_downstream`; `test_licensing_first_classified.py`; `test_phase3_rulings.py::test_no_section_carries_two_classified_waters_units`, `::test_gosnell_creek_and_the_endako_are_not_classified` |
| Li6 | The basic licence is always needed, and Classified Waters adds its extras; a document needed to fish is `base` even while the water is classified. | 2026-10-06 (D15, L8) | — | 4-6 | `licence.documents`; `test_answers_licence.py::test_live_profiles_answer_as_the_book_says` |
| Li7 | Where the book offers another licence (the Yukon angling licence), the two are ALTERNATIVES, never both. | 2026-10-08 (decision C) | Teslin Lake: a B.C. licence OR a Yukon licence. | — | `licence.py` L9; `test_answers_v22.py::test_c_teslin_basic_or_yukon_never_both` |
| Li8 | A requirement for the other KIND of water is dropped, and a superior authority's requirement displaces the provincial ones. | 2026-10-06 (G5, adopted) | Yoho: "provincial licences are not valid here". | 9 | `read.requirements_in_force`; `test_requirements_g5.py` |
| Li9 | A row that prints "(also in M.U. X)" in its own name binds in that MU's region too. | 2026-10-07 (Q24 / RR18 revised) | Canim (Region 3): its single barbless hook rule reaches the Region 5 part. | 29 | `outside.region_limit`; `test_rules_round.py::test_q24_a_row_printing_also_in_mu_binds_in_that_region` |
| Li10 | The West Road "Class II" keeps its designation, although the row prints no Classified Waters glyph. | 2026-10-07 (Q35) | — | 45 | Curated data. ◐ |

## 10. Waters and kinds (K)

| ID | Ruling | Date | Example | Page | Enforced in |
|---|---|---|---|---|---|
| K1 | SLOUGHS, CANALS AND CHANNELS ARE STREAMS for every stream regulation. The water kind is decided ONCE, in the registry, from the head noun of the name. | 2026-10-03 | Bear Creek Reservoir, Corn Creek Marsh and River Lakes stay lakes. | — | `pipeline/common/water_kind.py::flows`; `atlas/registry/flowing.py`; `test_one_water_kind.py` |
| K2 | The Vedder Canal is a stream (it joins the Vedder River). Lake rules never apply to it, but the region's trout/char total still does. | 2026-10-03 / 2026-10-08 | Region 2's "4, max 2 from streams": the canal takes 2; the rest are from lakes. | 21-22 | `test_answers_v22.py::test_e_the_vedder_canal_is_the_vedder_river_a_stream_and_no_lake_rule_binds_it` |
| K3 | A slough threaded by a creek of ANOTHER name does not join it. Nicomen Slough and Six Mile Slough are waters of their own. | 2026-10-03 | The Hansen and Lewis sloughs. | — | `flowing.THREADED_JOINS`; `test_registry_flowing.py::test_rule_3_threaded_by_one_stream_is_reported_and_not_joined` |
| K4 | A lake fully covered by its parts is not searchable as a whole: search finds the parts. | 2026-10-03 | Kootenay, Williston, Shannon. | — | `test_export_ui_rules.py::test_a_lake_cut_into_parts_is_read_through_its_parts` |
| K5 | A lake divided into internal closure areas (a lake split) is DEFERRED, never curated as a stream cut. | 2026-08 | Shuswap Lake's p.28 maps A/B/C. | 28 | Curation process (the review app's `deferred` status). ◐ |
| K6 | A braid's surviving channel is chosen by principle: FWA's mainstem first, then the highest magnitude and order, then length. Never by id order. | 2026-10-06 (B8) | — | — | `nests.SURVIVOR_BY_PRINCIPLE`; `test_prune.py` |
| K7 | TIDAL water is a different regulation system. It carries no provincial rules or licences, only the note "tidal water: see the federal (DFO) tidal waters regulations or the Fishing BC app; a federal Tidal Waters Sport Fishing Licence is required". | 2026-10-06 (D12) | Nitinat Lake is tidal and is the only tidal row. | 21 | `licence.TIDAL_IS_DOCUMENTED`; `test_answers_tidal.py`; `test_placement_rulings.py::test_nitinat_lake_is_tidal_and_the_only_tidal_row` ⚡ see C-2 |
| K8 | The Fraser below the Mission CPR bridge is approved AS BUILT (Region 2 freshwater zone rules) until tidal water is specced. | 2026-10-07 | A steelhead at New Westminster on Sep 1 reads "release". | 21, 23 | **⚠ NOT ENFORCED** as tidal: the tidal spec is ON HOLD. ⚡ see C-2 |

## 11. Extents and boundaries (E)

| ID | Ruling | Date | Example | Page | Enforced in |
|---|---|---|---|---|---|
| E1 | Never widen. A rule's extents are its own, never inherited from its entry. A place the builder cannot bind leaves the rule UNBOUND, with a typed reason; silent widening is a defect even "in the safe direction". | 2026-09-08 / 2026-09-24 | A 500 m closure is never applied to a whole lake arm. | — | `RuleBinding.__post_init__`; `validate_catalogue.default_extents` |
| E2 | When the rule names a part of its own water that nothing can draw, it is an `undrawn_part`. It is shown as a prominent "not yet mapped" note and never colours the whole water. | 2026-09-24 | The Kinbasket 200 m bridge closure. | — | `test_undrawn_part.py` |
| E3 | A COMPLEMENT ("other parts") is the rule's water minus its siblings' sections. If a sibling does not bind, the result is `complement_unknown`, never the whole water. | 2026-09-28 | Bull, Elk, Findlay. | — | `reach.build._build_rest` |
| E4 | Half of a river is `side`, not a part and not the whole: the other half follows the water's other rules. Half of a lake is an `undrawn_part`. | 2026-09-28 | Kitimat's "west half" closure. | 53 | `test_trout_scope_and_dated_releases.py::test_kitimats_west_half_closure_is_placed_and_says_so` |
| E5 | An area rule with an exception no cut can express stays unbound, or subtracts the excepted lakes by name. | 2026-09-24 / 2026-10-07 | "No powered boats … except Gold, Upper Campbell and Buttle lakes" in Strathcona Park (Great Central Lake too). | 14 | `classify.AREA_CARVE_OUTS_UNBIND`; `test_rules_round.py::test_fix_strathcona_park_powered_boats_binds_less_the_three_lakes` |
| E6 | A row named for a PART of its water ("downstream of Paul Lake", "upstream of the CPR Bridge at Mission", "Fraser River upstream to Harrison Lake") binds that part, never the whole item. Every real case is fixed at the source. | 2026-10-06 (F11) | Paul Creek above Paul Lake takes the spring closure. Harrison's night closure is off the 1.2 km above Harrison Lake. | 23, 31 | `test_fix_round_rows.py` |
| E7 | A `between` whose sentence prints signs ends at the curated sign split, never at a hydrometric gauge. Surveyed signs win over the book's approximate distance. | 2026-10-07 | Yakoun: ~14.5 km of surveyed signs, not "approximately 13 km". Capilano binds from the footbridge. | 19, 26 | `test_rules_round.py::test_no_rule_bounds_a_reach_by_a_gauge_where_the_book_prints_signs`, `::test_fix_gauge_bounds_are_the_books_signs` |
| E8 | CLOSURE AREAS (national parks, ecological reserves, the Chilkoot) are cut CLEAN:<br>• a straddle is INSIDE;<br>• an exit needs a continuous run outside longer than 1,000 m (`rejoin_m`);<br>• the exit cut sits where the stream LAST LEFT;<br>• a run outside to the line's end exits however short (≥5 m);<br>• dips under 5 m are positional error and are ignored. | 2026-10-06 (A1, A2, Q43) | Krajina is inside from 207 m. The Beaverfoot stays inside Yoho; its longest wander outside is 968 m. | — | `clean_cut.decide_rejoin`, `clean_cut.stretches`; `test_clean_cut.py::test_a_straddle_is_inside_from_its_first_crossing`, `::test_an_exit_needs_a_continuous_run_out_longer_than_the_rejoin_distance` |
| E9 | REGIONS, MU groups, sign zones and restricted land keep their current cuts: first entry and last exit, and a straddling section belongs to both sides (it carries both regions). | 2026-10-06 (B7) | Contact Creek's 619 m also takes Region 6. | — | `area_splits.resolve_area_splits`; `test_border.py`, `test_anchors.py` |
| E10 | The B.C. outline is the EXACT union of the region units, never simplified. Every area is clipped to it, and B.C. rules stop at the border. | 2026-10-06 (B5, F19) | Inside B.C., no strip reads as outside. | — | `bc_boundary.load_outline`, `area_splits.clips_to_bc`; `test_bc_boundary.py` |
| E11 | Slivers are prevented at the source. There is no snap or merge, and the build gate only asserts. The atlas build is deterministic. | 2026-10-06 | — | — | `sliver_gate.check`; `test_no_slivers.py` (slow); `test_prune.py::test_the_braid_prune_does_not_depend_on_the_hash_seed` |
| E12 | Pieces outside B.C. get no rules. | 2026-10-06 (A3/B4) | — | — | `test_bundle_licensing.py::test_a_rule_set_on_water_outside_bc_stops_the_build` |
| E13 | Per-water extent rulings from the 2026-10-07 round. | 2026-10-07 | • **Baker Creek** (Q18): one split at the park's DOWNSTREAM crossing.<br>• **Shuswap River R8** (Q19): catch and release on the whole river; "Mara Bridge to Sugar Lake" scopes only the exemption.<br>• **Goat River** (Q20): catch and release on the whole mainstem.<br>• **Wap Lake** (Q21): outside the Frog Falls closure.<br>• **Region 5 Fraser closures** (Q46): clipped to Region 5.<br>• **Fraser night closure** (Q12): covers the river's side channels, but not separately named sloughs.<br>• **Rancheria**: its tributaries mean the basin's streams. | 37, 43, 71, 23 | `test_rules_round.py::test_q18_baker_creek_one_split_at_the_parks_downstream_crossing`, `::test_q19_shuswap_river_catch_and_release_on_the_whole_river`, `::test_q21_wap_lake_is_outside_the_frog_falls_closure`, `::test_q46_region_5_fraser_closures_are_clipped_to_region_5`, `::test_the_rancheria_alternative_reaches_the_basin_streams`. ◐ (Goat and the Q12 side channels have no test) |
| E14 | Hays Creek: the culvert is essentially the mouth, and the split exists. | 2026-08-11 | — | — | Curated split. ◐ |

## 12. Dates, weekdays and hours (D)

| ID | Ruling | Date | Example | Page | Enforced in |
|---|---|---|---|---|---|
| D1 | A `;` ends a dating run: a date after a semicolon scopes only its own clause, while an "and" or comma chain shares the date. | 2026-09-25 | "Trout/char catch and release; bait ban, June 15-Oct 31" dates only the bait ban. | — | `test_rule_dates.py::test_a_date_after_a_semicolon_is_only_its_own_clauses` |
| D2 | A range printed "to Feb 28" runs through Feb 29 in leap years. | 2026-10-06 (C9) | Nicola below the lake: Feb 29 is catch and release, not closed. | 31 | `catalogue.range_days`; `test_feb29.py` |
| D3 | A season the parser could not read (`unparsed`) is the opposite of an absent season: absent means all year. Ranges may wrap the year end, and the stored days are the days the rule HOLDS. | 2026-09-23 | Fulton (L21). | — | The `When` model; `test_rule_dates.py` |
| D4 | A rule held on some weekdays or some hours DECIDES at those moments. A day is closed only when every moment of it is closed, so a night closure closes its hours, never the day. | 2026-10-08 (D2/G5) | Kootenay Lake Lower West Arm kokanee: release Mon-Fri, 5 a day Sat-Sun. Fraser above Mission: no fishing from 1 h after sunset to 1 h before sunrise. | 23, 37 | `calendar.Moment`, `calendar.moments`, `read.in_force`; `test_moments.py::test_the_lower_west_arm_keeps_5_kokanee_at_weekends_and_releases_them_on_weekdays`, `::test_a_night_closure_closes_its_hours_never_the_day` |

## 13. Display (P)

| ID | Ruling | Date | Example | Page | Enforced in |
|---|---|---|---|---|---|
| P1 | The page decides nothing. It reads the answers file, and anything the pipeline cannot decide is a named build error or a documented state, never a fallback. | 2026-10-06 | — | — | `test_answers_v22.py::test_d_the_page_decides_no_exemption`; `test_dataflow_gates.py` |
| P2 | Guide prose examples must come from a worked case, or be tested against the reader. `guide.cases` are sample waters, not a test oracle. | 2026-10-05 | — | — | `export_ui_rules.guide_example_problems`; `pipeline/tools/guide_examples.py` |
| P3 | Where steelhead are known but no steelhead rule applies, the page says: "Steelhead have been recorded here, but no steelhead rule applies: treat any rainbow, however big, as a rainbow trout." | 2026-10-03 | — | — | The export's `steelhead_rules: false` flag |
| P4 | The required gotchas are: closures that combine, a dated bait ban that replaces the zone's, an exemption that stays on its own water, closed creeks off excepted rivers, tributaries of undrawn parts, and waters not located. | 2026-10-05 / 2026-10-07 | — | — | `export_ui_rules`: `closures_combine`, `dated_bait_ban_replaces_zone`, `exemption_stays_on_its_water`, `excepted_rivers_creeks_stay_closed`, `undrawn_part_tributaries`, `not_located` |
| P5 | Angler-facing text carries no developer instructions; the tidal guide is in angler words only. | 2026-10-08 (B17) | — | — | `test_moments.py::test_no_shipped_angler_text_carries_a_developer_instruction` |
| P6 | Page decisions from 2026-10-08 (G-J):<br>• "today" is the real date;<br>• the closure banner lists every closure of the run;<br>• the bait rule is shown once;<br>• a glossary of plain terms ships in the answers file. | 2026-10-08 | Cowichan's banner lists all three back-to-back closures. | 15 | `glossary.py`; `page_v36.js` |
| P7 | Gear and licence may share one UI tab, but their data sections stay separate, with the same keys. | 2026-10-06 (E17/E18) | — | — | The answers schema |

## 14. Other (O)

| ID | Ruling | Date | Example | Page | Enforced in |
|---|---|---|---|---|---|
| O1 | A "See X" row is a pointer (`see`), never a rule: the target entry carries the rules. | 2026-09-25 | "See Lonzo Creek"; Nahatlatch Lake's "see page 28 for bull trout" (Q37). | — | `test_see_pointers.py::test_a_pointer_row_is_a_see_and_never_an_advisory_rule` |
| O2 | These are out of scope by ruling: mercury advisory scoping and the Family Fishing Weekend waiver. | 2026-09-24 | — | — | By ruling, not modelled. |
| O3 | The user approves every removal of a closure and every deletion. | standing | — | — | Process (handoff `READY` files list closures removed). |

---

## 15. Contradictions and tensions (⚡)

- **C-1. Campbell tributaries (T6): RESOLVED 2026-10-08.** The user ruled "ALL TRIBUTARIES": every
  tributary tree from Strathcona Dam to the sea, Quinsam excepted. That is what RR8 builds.
- **C-2. Tidal (K7 and K8).** K7 says tidal water carries no provincial rules or licences. The book
  (p.21) puts the Fraser below the Mission CPR bridge in tidal water. Yet K8 approves Region 2
  freshwater rules there "as built" until the tidal spec, which is ON HOLD. The four user testers
  hit this on the default Fraser part (TRIAGE item 1). The early memory note "not wanted:
  tidal-boundary rules (geometry handles)" also predates K7.
- **C-3. Exact size bounds and the display (Q13): ACCEPTED 2026-10-08.** "None over 40 cm" against a
  closed "40 cm or more" band is an accepted ambiguity: the user says no one can measure a fish that
  precisely. The display wording stays as it is.
- **C-4. Stale ruling citation.**
  `test_zone_decisions.py::test_steelhead_over_50_are_not_in_the_trout_one_over_50` justifies
  Region 1's CHAR exclusion by the withdrawn 2026-09-28 "mentions char" ruling. The assertion still
  holds under C2: "All char" is a related retention rule. The comment cites a superseded ruling.
- **C-5. A dated bait ban vs an inherited one: RULED 2026-10-08, ENFORCED 2026-10-09.** The user ruled that
  inherited rules behave like zone rules: any regulation under the water's own row replaces them. So
  a row's dated bait ban REPLACES an inherited all-year ban, exactly as it replaces a zone ban (L8).
  Granby: its own Apr 1-Oct 31 ban replaces the Kettle's inherited all-year ban. Enforced by
  `read.OWN_ROW_REPLACES_INHERITED` (`effective_rules` step 3b): a non-quota, non-closure own rule of the
  same type and dimension replaces the inherited one on every day; quotas keep Q5, closures combine (L20).
  Changed 3 answers (Granby above and below Burrell Creek, West Kettle). Tests:
  `test_rules_round.py::test_c5_granbys_dated_bait_ban_replaces_the_inherited_all_year_ban`,
  `::test_c5_an_inherited_closure_is_not_replaced_by_the_own_row`.

## 16. Not enforced, or held only by curated data

- **⚠ NOT ENFORCED:**
  - K8, Fraser below Mission as tidal;
  - Z1, the base-plus-deltas attribution test (removed with the table in 2026-09).
- **◐ PARTIAL, curated data with no test pinning it:**
  - Z2 Mill Lake;
  - Z10 Peaceful/Goose;
  - Q20 Adams;
  - L14 Stellako dates;
  - L16 Chimney;
  - T8 spot closures;
  - G8 Columbia/Salmon;
  - Li1;
  - Li10;
  - K5;
  - E13 Goat and Q12 side channels;
  - E14;
  - C3.

## 17. Superseded rulings

| Superseded | By | What changed |
|---|---|---|
| 2026-09-24 "a water quota sits beside the zone's unless the exact same statement" (as first recorded) | Q4, 2026-09-26 | A larger water number for a fish REPLACES the zone's. |
| 2026-09-28 "a row that MENTIONS char separately excludes char from its trout lines" | C2, 2026-10-07 | Only explicit exclusion, or a RELATED char rule, takes char out. RR11, RR12 (`ROW_NAMED_FISH_SIZE`) and RR27 were withdrawn. |
| 2026-09-30 "Nahatlatch below the lake is open from June 1" | L1/L2, 2026-10-05 | The strict lift ruling: `r2x` was removed, and the river is closed to Jun 30. |
| 2026-10-05 "add a Region 3 spring-closure lift for the Similkameen's tributaries" | T3, 2026-10-07 | No exemption on a ✱ row reaches its tributaries. |
| 2026-10-01 "steelhead: known includes the tributaries of a steelhead-row water" | S4, 2026-10-02 | No tributary walk. |
| 2026-10-02 "any waterbody bound by any rule of a steelhead row gets the whole set" (Tenas Lake) | S3, 2026-10-06 | A lake is steelhead water only as its own row's water. |
| 2026-10-02 "FINAL: the curated list never changes the anadromous flag" | S5/S6, 2026-10-03 | The list makes waters known; anadromous follows where steelhead rules apply. |
| 2026-10-05 "the size clause stays beside a release" | Q9, 2026-10-05 (Option A) | A moot size clause is hidden. |
| 2026-10-06 "clean cut at the single crossing in the middle of a straddle" | E8, 2026-10-06 | A straddle is inside, with a 1 km rejoin distance. |
| 2026-09-08 "straddling sections: mark one side, so the section resolves to one region" | E9, 2026-10-06 | Regions keep first/last cuts, and a straddling section belongs to both sides. |
| Early "not wanted: sloughs-as-streams" | K1, 2026-10-03 | Sloughs, canals and channels are streams. |
| 2026-10-07 RR1 "an inline ✱ narrows the row's ✱ to its clause" (Inland, San Juan, Sooke, Bull, Kitsumkalum) | T2, 2026-10-07 | A global-✱ row walks every rule. |
| 2026-10-07 round-1 Q8 "the Elk exception applies below the easternmost Hwy 3 bridge" | T7, 2026-10-07 | Lower Michel itself is excepted; its tributaries stay Elk tributaries. |
| 2026-10-07 RR21 "white sturgeon open in all of Regions 1-3 by their own tables" | W1, 2026-10-07 | Closed everywhere outside the licensed Fraser. |
| 2026-10-07 Q5 recommendation "exclude a tributary with its own row from the walk" | T4, 2026-10-07 | Additive: both its own rules and the inherited ones apply. |
| 2026-09-22 "length bounds inclusive" (for take-0 bands) | Q13, 2026-10-07 | A fish exactly on the bound is legal; "or more/or less" bands are closed. |
| 2026-09-15 workaround "a closure may not lift itself" (`lifts.self_lifting` guard) | L3, 2026-10-05 | The table was removed; the Region 6 steelhead closure's exemption now binds only the five mainstems. |
| 2026-10-06 rows F1b "a general cap excepts RB on steelhead water" | S9, 2026-10-08 | Region 6 counts hatchery steelhead and excepts nothing. Region 2 keeps `except: [ST]`. |
