# T100 — Lakes divided into closure areas (RULINGS K5): Shuswap's maps A/B/C and the "widened" rules

Status: DONE (owner Q43 default built: whole-water binding + area caveat) — da79ce4d, 1cccc29e, the ticket commit;
adversarial review fix (Mahood outlet cites no map; 19 rules cite a map, not 23) — see "Adversarial review".
Lane lane-T100 (worktree wf_144b677f-89c-4). Rebuild: deliver (export + answers + store; bundle, reach and atlas
unchanged) and a regs fixture re-cut after it.

Source: RULINGS K5 (2026-08: "a lake divided into internal closure areas is DEFERRED, never curated as a stream cut",
◐ held only by the review app's `deferred` status); memory `lake-splits-deferred`; data/curated/waters/
sub_lake_areas.json: 67 rules name a place inside a water and bind to ALL of it (marked `widened`); only Kootenay,
Williston and Shannon are split into parts.

## Work
List every `widened` rule and every deferred lake-split row (Shuswap p.28 A/B/C, Ross, Mahood, Skagit/Ross …) with what
the app says today (the whole lake) vs the book. Propose the model: a lake part (added_lakes ring, as Kootenay) or a
caveat line on the whole lake ("applies only in area A — see the map on p.28"). Build the agreed one; reach + deliver.

## Acceptance
Every widened rule is either split to its area or shows the area caveat on the page; a test pins the count of widened
rules (a new one fails until handled); Shuswap shows its areas correctly.

Depends: T97 (curation review may carry it); a reach rerun + deliver (atlas rebuild if parts are added). Size: M–L.
Owner: yes (Q43, the model). Default: keep binding the whole lake, add the area caveat line so the page never overstates.

## As built (2026-10-23)

Model: owner Q43 default. Every widened rule keeps binding the WHOLE water (no lake parts, no atlas change) and carries
an AREA CAVEAT that is pipeline data; the app prints it as stored and words nothing.

- `pipeline/tools/export_ui_rules.py`: `not_yet_mapped(r, verbatim, entry)` gains `map` and cites it in `says`.
  `book_map(part, verbatim, entry)` is the one place that decides which book map draws a part: a map named in the
  part or the rule's line ("(map A)", "described on map 1"), and its PRINTED page from "see map(s) … on page N" in the
  rule's line, else in the row heading, else the row's own single page where the row says the map is "below". An
  unidentified part ("on parts") never cites a map. `undrawn_map_problems` (in the export's problem gate) refuses a
  part that names a map with no page found, and a cited page that `guide.pdf_pages` does not map.
- `pipeline/deliver/answers/display.py`: `applies_here` ends a widened rule's `applies` with the export's `says`;
  `book_pages` adds the map's page after the row's (Shuswap: row p.32, maps p.28). `encode.py` SPEC text says so.
- App: no code change. The rule sheet / sources / decided lines print `applies`; the Keep in-part block
  (`caveats.ts inPartOf`) already listed these rules at the top of the water and its book link reads `facts.book`,
  which now carries the map page.
- Tests: `pipeline/tests/test_t100_area_caveat.py` pins the count of widened rules per row (`WIDENED`: 135 rules on 87
  rows; a new one fails until looked at) and the map table (`MAPS`: 19 rules after review), whole caveat strings, applies/book, the
  refusal gate, and the export as built (`needs_bundle`). `test_export_ui_rules.py` reads the cited sentence on a
  mapped part. Mutations listed in the test's docstring.

The ticket's "67 rules" (and the `_` text of `data/curated/waters/sub_lake_areas.json`) is stale: the corpus now holds
135 widened rules on 87 rows (98 name their part — 81 distinct parts on 64 rows —, 19 of those cite a book map, 37 are
vessel rules "on parts" / "various locations" that the book never locates; corrected in review from "75" and "23"). One licensing record is in a part (the Creston Valley WMA permit, Kootenay Lake Main Body); it
carries the caveat already and cites no map.

Deferred lake-split rows (memory lake-splits-deferred): Shuswap (p.32, maps A/B/C + Salmon Arm Bay on p.28) and Mahood
(p.31, area closure on p.28) are widened rows and now cite their maps (Mahood: only the western-tip rules r1-r4; its
Mahood River outlet rules r5-r8 are off the p.28 map and cite none). Ross Lake and Skagit River (p.25) have no
widened rule: each row binds its own whole water by the book (the boundary between them is the signs), so nothing
changes there. Kootenay, Williston and Shannon are already split into parts (`sub_lake_areas.json`) and are not
widened.

### Shuswap: app today vs the book vs now (printed p.32 = PDF 34; maps p.28 = PDF 30)
- Book: map A (west of Henstridge Rd/Wharf Rd to Little Shuswap Lake) closed Mar 15-May 14, then rainbow trout + char
  release; map B (entire area north of Albas) closed Mar 15-May 14; map C (Murdock/Semaphore points to the Hwy 1
  bridge) closed Mar 15-May 14; Salmon Arm Bay closed Sept 1-Dec 31 (map on p.28), the community pier exempt.
- App today: all 7 bind the whole lake. The Keep in-part block lists them as "not yet mapped"; the rule sheet's
  `applies` says only "This water's own row in the … Synopsis: Shuswap Lake." (reads as the whole lake) and the book
  link opens p.32 only.
- Now: `applies` = that sentence + "Not yet mapped: this applies only in one part of this water — entire area north of
  Albas — which is not drawn on the map yet; see map B on p.28 of the regulations. It does not apply to the rest of
  the water."; book = p.32 then p.28.

### Measured (scratch deliver of this lane's code from the live bundle; bundle, verdicts, status index untouched)
- (Builder's run; the review's re-measure is under "Adversarial review": 19 export rules, 12 books.)
- Export: exactly 23 rules change, only `not_yet_mapped` (gains `map`, `says` cites it), e.g. Shuswap r1 "… Little
  Shuswap Lake (map A) — which is not drawn on the map yet." -> "… Little Shuswap Lake — which is not drawn on the map
  yet; see map A on p.28 of the regulations."; Mahood r1-r8 "see the map on p.28", Canim r1 "see the map on p.42",
  François r1-r7 "see map 1 on p.51". Guide: 2 dictionary texts (rule and licensing `not_yet_mapped` gain `map?`). The
  guide's `encoding`/`rule.provenance.region` text also differs from live, but that is T109 (1ca9bc00) landing on
  the dev branch after this lane's base, not this change.
- Answers: only `sections.display.rules` (+ its spec text). 135 rules change `applies` (every widened rule, the caveat
  appended), 16 of them also `book` (Mahood 8 and Shuswap 7 add p.28 [PDF 30]; Canim adds p.42 [PDF 48]; François's
  map is on its own row page p.51, so its book is unchanged). Example Mahood r1: book [[31,33]] -> [[31,33],[28,30]].
- card_check on the scratch answers: 0 problems (16,004 groups, 7,159 cards, 2,266 keys). Store built and proved.
- Lane merges cleanly into redesign/stream-sections (`git merge-tree`).

### Owner questions (defaults built)
- Q43 (the model): whole-water binding + caveat line (built) vs lake parts as `added_lakes` rings like Kootenay
  (atlas rebuild). Default built: caveat. Shuswap's maps A/B/C are the best candidates if parts are wanted later.
- The 37 rules "on parts" cite nothing (the book never says which parts): they keep the existing "the
  regulations do not identify which parts" sentence. Default: keep.
- The in-part block's own where-line still reads "(map A in the regulations)" without the page; the page is in its
  book link. Default: leave the app as is (no app change in this ticket).

## Every widened rule (135 on 87 rows), with the book's words and its caveat

App today, for every rule below: it binds the whole water; the Keep in-part block shows it "not yet mapped" with the
part; the rule sheet's `applies` read only "This water's own row … : <water>" and its book link only the row page.
Now: `applies` ends with the caveat (citing the map where marked), book adds the map page. Pages: printed (PDF).

**BEAVER LAKE** — `r1:beaver_lake@1-1`, p.14 (PDF 16), 1 rule(s)

- `beaver_lake.r3` book: "no towing on parts" — part: *on parts* — caveat: part not identified ('on parts')

**BRANNEN LAKE** — `r1:brannen_lake@1-5`, p.14 (PDF 16), 1 rule(s)

- `brannen_lake.r1` book: "Speed restriction on parts (60 km/h)" — part: *on parts* — caveat: part not identified ('on parts')

**COURTENAY RIVER** — `r1:courtenay_river@1-6`, p.15 (PDF 17), 1 rule(s)

- `courtenay_river.r2` book: "speed restriction on part (8 km/h)" — part: *on part* — caveat: part not identified ('on parts')

**COWICHAN LAKE (including Bear Lake)** — `r1:cowichan_lake_including_bear_lake@1-4`, p.15 (PDF 17), 2 rule(s)

- `cowichan_lake.r4` book: "Speed restriction on parts (8 km/h)" — part: *on parts* — caveat: part not identified ('on parts')
- `cowichan_lake.r5` book: "overall 10 km/h speed restriction within 60 m of shore" — part: *within 60 m of shore* — caveat names the part

**COWICHAN RIVER (see map below)** — `r1:cowichan_river_see_map_below@1-4`, p.15 (PDF 17), 2 rule(s)

- `cowichan_river.r8` book: "Speed restriction (8 km/h)" — part: *on parts* — caveat: part not identified ('on parts')
- `cowichan_river.r9` book: "engine power restriction - 7.5 kW (10 hp) on parts" — part: *on parts* — caveat: part not identified ('on parts')

**DEENA CREEK** — `r1:deena_creek@6-12`, p.15 (PDF 17), 1 rule(s)

- `deena_creek.r1` book: "No Fishing upstream of fishing boundary signs at second bridge approximately 5 km upstream of the tidal boundary" — part: *upstream of fishing boundary signs at second bridge approximately 5 km upstream of the tidal boundary* — caveat names the part

**ELK LAKE** — `r1:elk_lake@1-1`, p.15 (PDF 17), 4 rule(s)

- `elk_lake.r2` book: "Engine power restriction on parts - 7.5 kW (10 hp)" — part: *on parts* — caveat: part not identified ('on parts')
- `elk_lake.r3` book: "no vessels on parts" — part: *on parts* — caveat: part not identified ('on parts')
- `elk_lake.r4` book: "no powered boats on parts" — part: *on parts* — caveat: part not identified ('on parts')
- `elk_lake.r5` book: "no towing on parts" — part: *on parts* — caveat: part not identified ('on parts')

**GREAT CENTRAL LAKE** — `r1:great_central_lake@1-7`, p.16 (PDF 18), 1 rule(s)

- `great_central_lake.r1` book: "No Fishing Jan 1-Apr 30, from the dam to fishing boundary signs approximately 50 m upstream (southwest) of the Ash Main Bridge" — part: *from the dam to fishing boundary signs approximately 50 m upstream (southwest) of the Ash Main Bridge* — caveat names the part

**LONG LAKE (Nanaimo)** — `r1:long_lake_nanaimo@1-5`, p.17 (PDF 19), 1 rule(s)

- `long_lake.r1` book: "Speed restriction on parts (60 km/h)" — part: *on parts* — caveat: part not identified ('on parts')

**MAMIN RIVER** — `r1:mamin_river@6-13`, p.17 (PDF 19), 1 rule(s)

- `mamin_river.r3` book: "No Fishing upstream of fishing boundary signs on third bridge approximately 10 km upstream of the tidal boundary, Feb 1-Apr 30" — part: *upstream of fishing boundary signs on third bridge approximately 10 km upstream of the tidal boundary* — caveat names the part

**NANAIMO RIVER** — `r1:nanaimo_river@1-5`, p.17 (PDF 19), 1 rule(s)

- `nanaimo_river.r6` book: "Engine power restriction on parts - 7.5 kW (10 hp)" — part: *on parts* — caveat: part not identified ('on parts')

**PROSPECT LAKE** — `r1:prospect_lake@1-2`, p.17 (PDF 19), 2 rule(s)

- `prospect_lake.r2` book: "speed restriction on parts (8 and 60 km/h)" — part: *on parts* — caveat: part not identified ('on parts')
- `prospect_lake.r3` book: "speed restriction on parts (8 and 60 km/h)" — part: *on parts* — caveat: part not identified ('on parts')

**QUENNELL LAKE** — `r1:quennell_lake@1-5`, p.18 (PDF 20), 1 rule(s)

- `quennell_lake.r2` book: "speed restriction on parts (8 km/h)" — part: *on parts* — caveat: part not identified ('on parts')

**SHAWNIGAN LAKE** — `r1:shawnigan_lake@1-2`, p.18 (PDF 20), 2 rule(s)

- `shawnigan_lake.r2` book: "Speed restrictions on parts (8 and 65 km/h)" — part: *on parts* — caveat: part not identified ('on parts')
- `shawnigan_lake.r3` book: "Speed restrictions on parts (8 and 65 km/h)" — part: *on parts* — caveat: part not identified ('on parts')

**STAMP RIVER** — `r1:stamp_river@1-7`, p.18 (PDF 20), 1 rule(s)

- `stamp_river.r5` book: "Engine power restriction 7.5 kW (10 hp), on parts" — part: *on parts* — caveat: part not identified ('on parts')

**WAHPEETO CREEK** — `r1:wahpeeto_creek@1-14`, p.19 (PDF 21), 1 rule(s)

- `wahpeeto_creek.r1` book: "No Fishing within 100 m downstream of the falls situated approximately 4.5 km upstream of Wakeman River" — part: *within 100 m downstream of the falls situated approximately 4.5 km upstream of Wakeman River* — caveat names the part

**WHITE RIVER** — `r1:white_river@1-10`, p.19 (PDF 21), 1 rule(s)

- `white_river.r1` book: "No Fishing between fishing boundary signs at the salmon viewing pool" — part: *between fishing boundary signs at the salmon viewing pool* — caveat names the part

**ALOUETTE LAKE** — `r2:alouette_lake@2-8`, p.22 (PDF 24), 2 rule(s)

- `alouette_lake.r2` book: "No vessels in swimming areas, as buoyed and signed" — part: *in swimming areas, as buoyed and signed* — caveat names the part
- `alouette_lake.r3` book: "speed restriction (8 km/h) at south end of lake, south of a line drawn from the BC Parks boat ramp to signs on the east side of the lake" — part: *at south end of lake, south of a line drawn from the BC Parks boat ramp to signs on the east side of the lake* — caveat names the part

**CHAPMAN CREEK** — `r2:chapman_creek@2-5`, p.22 (PDF 24), 1 rule(s)

- `chapman_creek.r2` book: "No Fishing from the falls to 100 m downstream (falls are located approximately 550 m upstream of the power line crossing)" — part: *from the falls to 100 m downstream (falls are located approximately 550 m upstream of the power line crossing)* — caveat names the part

**COQUITLAM RIVER** — `r2:coquitlam_river@2-8`, p.23 (PDF 25), 1 rule(s)

- `coquitlam_river.r2` book: "No powered boats on parts" — part: *on parts* — caveat: part not identified ('on parts')

**CULTUS LAKE** — `r2:cultus_lake@2-3`, p.23 (PDF 25), 2 rule(s)

- `cultus_lake.r3` book: "no vessels on parts" — part: *on parts* — caveat: part not identified ('on parts')
- `cultus_lake.r4` book: "speed restriction (5 km/h) at north end, as buoyed and signed" — part: *at north end, as buoyed and signed* — caveat names the part

**GREEN LAKE** — `r2:green_lake@2-9`, p.23 (PDF 25), 2 rule(s)

- `green_lake.r5` book: "speed restriction on parts (10 km/h)" — part: *on parts* — caveat: part not identified ('on parts')
- `green_lake.r6` book: "no towing on parts" — part: *on parts* — caveat: part not identified ('on parts')

**HARRISON LAKE** — `r2:harrison_lake@2-18`, p.23 (PDF 25), 1 rule(s)

- `harrison_lake.r3` book: "speed restriction (10 km/h) at south end, as buoyed and signed" — part: *at south end, as buoyed and signed* — caveat names the part

**HARRISON RIVER (from the Fraser River upstream to Harrison Lake)** — `r2:harrison_river_from_the_fraser_river_upstream_to_harrison_la@2-18`, p.23 (PDF 25), 1 rule(s)

- `harrison_river.r3` book: "Speed restriction or no powered boats in small bays along the river as signed" — part: *in small bays along the river as signed* — caveat names the part

**HATZIC LAKE AND SLOUGH** — `r2:hatzic_lake_and_slough@2-8`, p.23 (PDF 25), 2 rule(s)

- `hatzic_lake_and_slough.r2` book: "speed restriction on parts (8 and 50 km/h)" — part: *on parts* — caveat: part not identified ('on parts')
- `hatzic_lake_and_slough.r3` book: "speed restriction on parts (8 and 50 km/h)" — part: *on parts* — caveat: part not identified ('on parts')

**NICOMEKL RIVER** — `r2:nicomekl_river@2-4`, p.25 (PDF 27), 1 rule(s)

- `nicomekl_river.r5` book: "no towing on parts" — part: *on parts* — caveat: part not identified ('on parts')

**NICOMEN SLOUGH** — `r2:nicomen_slough@2-8`, p.25 (PDF 27), 1 rule(s)

- `nicomen_slough.r1` book: "Speed restriction on parts (8 km/h)" — part: *on parts* — caveat: part not identified ('on parts')

**PITT LAKE** — `r2:pitt_lake@2-8`, p.25 (PDF 27), 2 rule(s)

- `pitt_lake.r3` book: "Wild trout/char catch and release, single barbless hook north of fishing boundary signs (east and west shores) near the head of the lake" — part: *north of fishing boundary signs (east and west shores) near the head of the lake* — caveat names the part
- `pitt_lake.r4` book: "single barbless hook north of fishing boundary signs (east and west shores) near the head of the lake" — part: *north of fishing boundary signs (east and west shores) near the head of the lake* — caveat names the part

**PITT RIVER** — `r2:pitt_river@2-8`, p.25 (PDF 27), 1 rule(s)

- `pitt_river.r2` book: "speed restriction at Grant Narrows (10 km/h)" — part: *at Grant Narrows* — caveat names the part

**POWELL LAKE** — `r2:powell_lake@2-12`, p.25 (PDF 27), 1 rule(s)

- `powell_lake.r2` book: "no powered boats in One Mile Bay" — part: *in One Mile Bay* — caveat names the part

**RUBY LAKE** — `r2:ruby_lake@2-5`, p.25 (PDF 27), 1 rule(s)

- `ruby_lake.r1` book: "No Fishing in the outlet bay within 100 m of the head of Ruby Creek" — part: *in the outlet bay within 100 m of the head of Ruby Creek* — caveat names the part

**SAKINAW LAKE** — `r2:sakinaw_lake@2-5`, p.25 (PDF 27), 2 rule(s)

- `sakinaw_lake.r1` book: "No Fishing easterly of a line drawn from a boundary sign located at the north side of the Sakinaw Lake boat launch southwesterly to a boundary sign located a…" — part: *easterly of a line drawn from a boundary sign located at the north side of the Sakinaw Lake boat launch southwesterly to a boundary sign located at 49o 41.50'N and 123o 58.45'W* — caveat names the part
- `sakinaw_lake.r2` book: "No Fishing in "Bear Bay", easterly of a line drawn from a boundary sign located at 49o 40.05'N and 124o 02.13'W due south to a boundary sign on the opposite …" — part: *in "Bear Bay", easterly of a line drawn from a boundary sign located at 49o 40.05'N and 124o 02.13'W due south to a boundary sign on the opposite shore* — caveat names the part

**STAVE RIVER** — `r2:stave_river@2-8`, p.26 (PDF 28), 2 rule(s)

- `stave_river.r1` book: "No Fishing in the Ruskin spawning channel, from the inlet near the dam downstream to the boat ramp crossing" — part: *Ruskin spawning channel, from the inlet near the dam downstream to the boat ramp crossing* — caveat names the part
- `stave_river.r2` book: "No Fishing in the Northrop Spawning Channel, from the intake downstream to where the channel joins the Stave River main, including its tributaries containg t…" — part: *Northrop Spawning Channel, from the intake downstream to where the channel joins the Stave River main, including its tributaries containg the fishway* — caveat names the part

**STRAWBERRY SLOUGH** — `r2:strawberry_slough@2-8`, p.26 (PDF 28), 1 rule(s)

- `strawberry_slough.r1` book: "Speed restriction on parts (8 km/h)" — part: *on parts* — caveat: part not identified ('on parts')

**ADAMS LAKE** — `r3:adams_lake@3-37`, p.29 (PDF 31), 1 rule(s)

- `adams_lake.r1` book: "No Fishing north of a line drawn due west from mouth of Momich River, Mar 15-June 30" — part: *north of a line drawn due west from mouth of Momich River* — caveat names the part

**HEFFLEY LAKE (parts of )** — `r3:heffley_lake_parts_of@3-27`, p.30 (PDF 32), 2 rule(s)

- `heffley_lake.r1` book: "No towing" — part: *parts of the lake (not specified in the book)* — caveat names the part
- `heffley_lake.r2` book: "speed restriction (15 km/h)" — part: *parts of the lake (not specified in the book)* — caveat names the part

**LEIGHTON LAKE** — `r3:leighton_lake@3-18`, p.30 (PDF 32), 2 rule(s)

- `leighton_lake.r1` book: "No Fishing within 100 m of the mouth of the inlet stream, May 1-Nov 30" — part: *within 100 m of the mouth of the inlet stream* — caveat names the part
- `leighton_lake.r2` book: "No Fishing within 100 m of the Tunkwa Creek outlet, May 1-Nov 30" — part: *within 100 m of the Tunkwa Creek outlet* — caveat names the part

**LOON LAKE** — `r3:loon_lake@3-30`, p.30 (PDF 32), 2 rule(s)

- `loon_lake.r1` book: "No Fishing northeast of fishing boundary signs near the mouth of Thunder Creek and the public access site" — part: *northeast of fishing boundary signs near the mouth of Thunder Creek and the public access site* — caveat names the part
- `loon_lake.r2` book: "No Ice Fishing within 500 m of outlet stream at southwest end of lake as marked by fishing boundary signs" — part: *within 500 m of outlet stream at southwest end of lake as marked by fishing boundary signs* — caveat names the part

**MAHOOD LAKE (see map on page 28 for area closure)** — `r3:mahood_lake_see_map_on_page_28_for_area_closure@3-46`, p.31 (PDF 33), 8 rule(s)

- `mahood_lake.r1` book: "No Fishing within the fishing boundary signs at the western tip of the lake near the mouth of Canim River, Jan 1-June 30" — part: *within the fishing boundary signs at the western tip of the lake near the mouth of Canim River* — caveat cites the map on p.28 (PDF 30)
- `mahood_lake.r2` book: "Trout/char catch and release" — part: *within fishing boundary signs at the western tip of the lake* — caveat cites the map on p.28 (PDF 30)
- `mahood_lake.r3` book: "bait ban" — part: *within fishing boundary signs at the western tip of the lake* — caveat cites the map on p.28 (PDF 30)
- `mahood_lake.r4` book: "single barbless hook" — part: *within fishing boundary signs at the western tip of the lake* — caveat cites the map on p.28 (PDF 30)
- `mahood_lake.r5` book: "No Fishing within 200 m of the Mahood River outlet, Jan 1-June 30" — part: *within 200 m of the Mahood River outlet* — caveat names the part (the p.28 map shows only the western tip; corrected in review)
- `mahood_lake.r6` book: "Trout/char catch and release" — part: *within 200 m of the mouth of the Mahood River outlet* — caveat names the part (the p.28 map shows only the western tip; corrected in review)
- `mahood_lake.r7` book: "bait ban" — part: *within 200 m of the mouth of the Mahood River outlet* — caveat names the part (the p.28 map shows only the western tip; corrected in review)
- `mahood_lake.r8` book: "single barbless hook" — part: *within 200 m of the mouth of the Mahood River outlet* — caveat names the part (the p.28 map shows only the western tip; corrected in review)

**MCARTHUR ISLAND SLOUGH** — `r3:mcarthur_island_slough@3-28`, p.31 (PDF 33), 1 rule(s)

- `mcarthur_island_slough.r1` book: "No powered boats from westerly entrance to 12th Street entrance to Park" — part: *from westerly entrance to 12th Street entrance to Park* — caveat names the part

**ROCHE LAKE** — `r3:roche_lake@3-20`, p.32 (PDF 34), 1 rule(s)

- `roche_lake.r4` book: "Electric motor only - max 7.5 kW, south of a line bearing true 244° from a point on the southern tip of the largest island in the south arm of Roche Lake to …" — part: *south of a line bearing true 244° from a point on the southern tip of the largest island in the south arm of Roche Lake to the westerly shoreline and conversely 64° to the easterly shoreline* — caveat names the part

**SHUMWAY LAKE** — `r3:shumway_lake@3-20`, p.32 (PDF 34), 1 rule(s)

- `shumway_lake.r1` book: "No Fishing north of fishing boundary signs located at south end of lake, Apr 1-Oct 31" — part: *north of fishing boundary signs located at south end of lake* — caveat names the part

**SHUSWAP LAKE (see maps on page 28) (includes Little Shuswap Lake, that part of South Thompson River between Shuswap Lake and Little Shuswap Lake, Seymour, Anstey and Salmon arms and Mara Lake unless stated otherwise)** — `r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26`, p.32 (PDF 34), 7 rule(s)

- `shuswap_lake.r1` book: "No Fishing Mar 15-May 14" — part: *waters lying west of a line between signs at Henstridge Road and Wharf Road to a line between signs on the south and north shores of Little Shuswap Lake (map A)* — caveat cites map A on p.28 (PDF 30)
- `shuswap_lake.r2` book: "rainbow trout and char catch and release, May 15-Mar 14" — part: *waters lying west of a line between signs at Henstridge Road and Wharf Road to a line between signs on the south and north shores of Little Shuswap Lake (map A)* — caveat cites map A on p.28 (PDF 30)
- `shuswap_lake.r3` book: "No Fishing in the entire area north of Albas, Mar 15-May 14" — part: *entire area north of Albas (map B)* — caveat cites map B on p.28 (PDF 30)
- `shuswap_lake.r4` book: "No Fishing east of a line between fishing boundary signs on Murdock and Semaphore points, to Hwy 1 bridge, Mar 15-May 14" — part: *east of a line between fishing boundary signs on Murdock and Semaphore points, to Hwy 1 bridge (map C)* — caveat cites map C on p.28 (PDF 30)
- `shuswap_lake.r5` book: "No fishing in Salmon Arm Bay, west of line between Engineer's Point and Sunnybrae Point (see map on page 28), for the conservation of chinook and coho salmon…" — part: *Salmon Arm Bay, west of line between Engineer's Point and Sunnybrae Point* — caveat cites the map on p.28 (PDF 30)
- `shuswap_lake.r5x` book: "anglers fishing from the community pier in the city of Salmon Arm are exempt from the closure" — part: *from the community pier in the city of Salmon Arm* — caveat cites the maps on p.28 (PDF 30)
- `shuswap_lake.r11x` book: "anglers fishing from the community pier in the city of Salmon Arm are exempt from the bait ban" — part: *from the community pier in the city of Salmon Arm* — caveat cites the maps on p.28 (PDF 30)

**WHITE LAKE** — `r3:white_lake@3-26`, p.32 (PDF 34), 1 rule(s)

- `white_lake.r1` book: "No Fishing within 400 m of the mouth of Cedar Creek as designated by signs, Dec 1-June 30" — part: *within 400 m of the mouth of Cedar Creek as designated by signs* — caveat names the part

**COLUMBIA LAKE** — `r4:columbia_lake@4-25`, p.35 (PDF 37), 1 rule(s)

- `columbia_lake.r3` book: "speed restriction (10 km/h) near eastern shore and at south end, as buoyed and signed" — part: *near eastern shore and at south end, as buoyed and signed* — caveat names the part

**COLUMBIA RIVER** — `r4:columbia_river@4-15+4-26+4-34+4-38+4-8`, p.35 (PDF 37), 2 rule(s)

- `columbia_river.r11` book: "Speed restriction (10 km/h) from Mud Lake to Columbia Lake" — part: *from Mud Lake to Columbia Lake* — caveat names the part
- `columbia_river.r12` book: "no power boats in wetlands" — part: *in wetlands* — caveat names the part

**KINBASKET (McNaughton) LAKE** — `r4:kinbasket_mcnaughton_lake@4-36`, p.37 (PDF 39), 1 rule(s)

- `kinbasket_lake.r1` book: "No Fishing within 200 m of Bush-Sullivan Road Bridge in Bush Arm" — part: *within 200 m of Bush-Sullivan Road Bridge in Bush Arm* — caveat names the part

**LAKE REVELSTOKE** — `r4:lake_revelstoke@4-38+4-39`, p.38 (PDF 40), 1 rule(s)

- `lake_revelstoke.r1` book: "No Fishing from Mica Dam to fishing boundary signs at the narrows immediately downstream of the mouth of Bigmouth Creek" — part: *from Mica Dam to fishing boundary signs at the narrows immediately downstream of the mouth of Bigmouth Creek* — caveat names the part

**PREMIER LAKE** — `r4:premier_lake@4-21`, p.39 (PDF 41), 3 rule(s)

- `premier_lake.r1` book: "No Fishing south of fishing boundary signs on the lake shore, May 15-June 20" — part: *south of fishing boundary signs on the lake shore* — caveat names the part
- `premier_lake.r4` book: "No towing" — part: *south half* — caveat names the part
- `premier_lake.r5` book: "speed restriction (15 km/h)" — part: *south half* — caveat names the part

**TROUT LAKE** — `r4:trout_lake@4-30`, p.40 (PDF 42), 1 rule(s)

- `trout_lake.r1` book: "No Fishing northwest of a line between fishing boundary signs on opposite shores approximately 1.5 km southeast the city of Trout Lake, Feb 1-May 31" — part: *northwest of a line between fishing boundary signs on opposite shores approximately 1.5 km southeast the city of Trout Lake* — caveat names the part

**UPPER ARROW LAKE (drawdown area)** — `r4:upper_arrow_lake_drawdown_area@4-31+4-32`, p.40 (PDF 42), 3 rule(s)

- `upper_arrow_lake_drawdown_area.r1b` book: "trout/char daily quota = 2" — part: *the drawdown area at the north end of the lake, down to the power line crossing at Akolkolex Narrows (approximately 1.5 km downstream of the Akolkolex River confluence)* — caveat names the part
- `upper_arrow_lake_drawdown_area.r2b` book: "no bull trout under 60 cm and only 1 bull trout over 60 cm" — part: *the drawdown area at the north end of the lake, down to the power line crossing at Akolkolex Narrows (approximately 1.5 km downstream of the Akolkolex River confluence)* — caveat names the part
- `upper_arrow_lake_drawdown_area.r3b` book: "single barbless hook" — part: *the drawdown area at the north end of the lake, down to the power line crossing at Akolkolex Narrows (approximately 1.5 km downstream of the Akolkolex River confluence)* — caveat names the part

**CANIM LAKE (see map on page 42)** — `r5:canim_lake_see_map_on_page_42@5-1`, p.43 (PDF 49), 1 rule(s)

- `canim_lake.r1` book: "No Fishing within the waters of the small bay at the mouth of Eagle Creek northerly of a line drawn between two boundary signs located on opposite shores of …" — part: *within the waters of the small bay at the mouth of Eagle Creek northerly of a line drawn between two boundary signs located on opposite shores of the bay* — caveat cites the map on p.42 (PDF 48)

**CHILKO LAKE** — `r5:chilko_lake@5-4`, p.43 (PDF 49), 1 rule(s)

- `chilko_lake.r6` book: "No powered boats on Big Lagoon (west side of lake)" — part: *Big Lagoon (west side of lake)* — caveat names the part

**CHILKO RIVER** — `r5:chilko_river@5-5`, p.44 (PDF 50), 1 rule(s)

- `chilko_river.r6` book: "speed restriction (5 km/h) on part" — part: *on part* — caveat: part not identified ('on parts')

**DRAGON LAKE** — `r5:dragon_lake@5-2`, p.44 (PDF 50), 1 rule(s)

- `dragon_lake.r1` book: "No Fishing southeast of a line between fishing boundary signs on opposite shores of the bay at the mouth of Hallis Creek, Apr 1-May 31" — part: *southeast of a line between fishing boundary signs on opposite shores of the bay at the mouth of Hallis Creek* — caveat names the part

**GREEN LAKE** — `r5:green_lake@5-1`, p.45 (PDF 51), 1 rule(s)

- `green_lake.r1` book: "No Fishing northeast of line between boundary signs on opposite shores of the bay at the mouth of Watch Creek, Mar 1-May 31" — part: *northeast of line between boundary signs on opposite shores of the bay at the mouth of Watch Creek* — caveat names the part

**MITCHELL LAKE** — `r5:mitchell_lake@5-15`, p.46 (PDF 52), 1 rule(s)

- `mitchell_lake.r1` book: "No Fishing within 100 m radius of the weir at the lake's outlet" — part: *within 100 m radius of the weir at the lake's outlet* — caveat names the part

**QUESNEL LAKE** — `r5:quesnel_lake@5-15`, p.46 (PDF 52), 2 rule(s)

- `quesnel_lake.r1` book: "No Fishing southwest of a line between fishing boundary signs on opposite shores of Horsefly Bay, Mar 1-May 31" — part: *southwest of a line between fishing boundary signs on opposite shores of Horsefly Bay* — caveat names the part
- `quesnel_lake.r2` book: "No Fishing in North Arm, north of a line between Watt and Service Creeks, Mar 1-May 31" — part: *in North Arm, north of a line between Watt and Service Creeks* — caveat names the part

**WILLIAMS LAKE** — `r5:williams_lake@5-2`, p.47 (PDF 53), 1 rule(s)

- `williams_lake.r1` book: "Speed restriction on parts (8 km/h)" — part: *on parts* — caveat: part not identified ('on parts')

**BABINE LAKE** — `r6:babine_lake@6-6`, p.50 (PDF 56), 2 rule(s)

- `babine_lake.r1` book: "No Fishing east of a line from Gullwing Creek to the south shore of Babine Lake" — part: *east of a line from Gullwing Creek to the south shore of Babine Lake* — caveat names the part
- `babine_lake.r2` book: "No Fishing within a 400 m radius of the mouth of Pinkut Creek" — part: *within a 400 m radius of the mouth of Pinkut Creek* — caveat names the part

**BUCKINGHORSE LAKE** — `r6:buckinghorse_lake@6-20`, p.50 (PDF 56), 1 rule(s)

- `buckinghorse_lake.r1` book: "No Fishing within 100 m of outlet, May 15-July 31" — part: *within 100 m of outlet* — caveat names the part

**CRANBERRY RIVER** — `r6:cranberry_river@6-15`, p.50 (PDF 56), 1 rule(s)

- `cranberry_river.r1` book: "No Fishing between fishing boundary signs upstream of and downstream of Cranberry River Canyon" — part: *between fishing boundary signs upstream of and downstream of Cranberry River Canyon* — caveat names the part

**FRANÇOIS LAKE** — `r6:fran_ois_lake@6-4`, p.51 (PDF 57), 7 rule(s)

- `francois_lake.r1` book: "No Fishing Nov 15-May 31" — part: *the area at the outlet of François Lake described on map 1* — caveat cites map 1 on p.51 (PDF 57)
- `francois_lake.r2` book: "char daily quota = 2" — part: *the area at the outlet of François Lake described on map 1* — caveat cites map 1 on p.51 (PDF 57)
- `francois_lake.r3` book: "no more than 1 over 50 cm" — part: *the area at the outlet of François Lake described on map 1* — caveat cites map 1 on p.51 (PDF 57)
- `francois_lake.r4` book: "rainbow trout catch and release" — part: *the area at the outlet of François Lake described on map 1* — caveat cites map 1 on p.51 (PDF 57)
- `francois_lake.r5` book: "bait ban" — part: *the area at the outlet of François Lake described on map 1* — caveat cites map 1 on p.51 (PDF 57)
- `francois_lake.r6` book: "single barbless hook" — part: *the area at the outlet of François Lake described on map 1* — caveat cites map 1 on p.51 (PDF 57)
- `francois_lake.r7` book: "no set lining" — part: *the area at the outlet of François Lake described on map 1* — caveat cites map 1 on p.51 (PDF 57)

**FULTON RIVER** — `r6:fulton_river@6-8`, p.51 (PDF 57), 1 rule(s)

- `fulton_river.r3` book: "Angling permitted in Fulton River opposite the spawning channels, as signed" — part: *opposite the spawning channels, as signed* — caveat names the part

**HELENE LAKE** — `r6:helene_lake@6-6`, p.51 (PDF 57), 1 rule(s)

- `helene_lake.r1` book: "No Fishing northwest of a line between fishing boundary signs on opposite shores of the outlet bay, Apr 1-June 30" — part: *northwest of a line between fishing boundary signs on opposite shores of the outlet bay* — caveat names the part

**KLAHOWYA LAKE** — `r6:klahowya_lake@6-20`, p.52 (PDF 58), 1 rule(s)

- `klahowya_lake.r1` book: "No Fishing within 100 m of outlet, May 15-July 31" — part: *within 100 m of outlet* — caveat names the part

**SILVERTHORNE (Erickson) LAKE** — `r6:silverthorne_erickson_lake@6-9`, p.53 (PDF 59), 1 rule(s)

- `silverthorne_lake.r1` book: "No Fishing within 50 m of the outlet, May 1-June 30" — part: *within 50 m of the outlet* — caveat names the part

**TAKYSIE LAKE** — `r6:takysie_lake@6-4`, p.53 (PDF 59), 1 rule(s)

- `takysie_lake.r1` book: "No Fishing northwest of a line between fishing boundary signs on opposite shores immediately north of Takysie Lake Settlement, Mar 1-May 31" — part: *northwest of a line between fishing boundary signs on opposite shores immediately north of Takysie Lake Settlement* — caveat names the part

**DAVIS BAY (in Finlay Reach of Williston Lake)** — `r7:davis_bay_in_finlay_reach_of_williston_lake@7-37`, p.59 (PDF 65), 1 rule(s)

- `davis_bay.r1` book: "No Fishing within a 500 m radius of the Davis Forest Service Road Bridge" — part: *within a 500 m radius of the Davis Forest Service Road Bridge* — caveat names the part

**DINOSAUR LAKE (Reservoir Downstream of W.A.C. Bennett Dam)** — `r7:dinosaur_lake_reservoir_downstream_of_w_a_c_bennett_dam@7-31`, p.65 (PDF 71), 1 rule(s)

- `dinosaur_lake.r1` book: "No Fishing from W.A.C. Bennett Dam to 100 m south of Gething Creek and between the anti-vortex dyke and Peace Canyon Dam" — part: *from W.A.C. Bennett Dam to 100 m south of Gething Creek and between the anti-vortex dyke and Peace Canyon Dam* — caveat names the part

**HALFWAY RIVER** — `r7:halfway_river@7-34`, p.65 (PDF 71), 1 rule(s)

- `halfway_river.r1` book: "No Fishing" — part: *from confluence with Peace River to fishing boundary signs approximately 5 km upstream* — caveat names the part

**LETAIN LAKE** — `r7:letain_lake@7-52`, p.65 (PDF 71), 1 rule(s)

- `letain_lake.r1` book: "No Fishing" — part: *within 100 m of fishing boundary sign at outlet* — caveat names the part

**NATION ARM (Williston Lake)** — `r7:nation_arm_williston_lake@7-30`, p.60 (PDF 66), 2 rule(s)

- `nation_arm.r1` book: "No Fishing west of a line between two fishing boundary signs approximately 500 m downstream (east) of the Nation River Bridge on the Finlay Forest Service Ro…" — part: *west of a line between two fishing boundary signs approximately 500 m downstream (east) of the Nation River Bridge on the Finlay Forest Service Road* — caveat names the part
- `nation_arm.r2` book: "Bull trout catch and release west of a line between two fishing boundary signs approximately 500 m downstream of the Nation River Bridge on the Finlay Forest…" — part: *west of a line between two fishing boundary signs approximately 500 m downstream of the Nation River Bridge on the Finlay Forest Service Road* — caveat names the part

**NULKI LAKE** — `r7:nulki_lake@7-12`, p.60 (PDF 66), 1 rule(s)

- `nulki_lake.r1` book: "No Fishing west of a line between fishing boundary signs on lakeshore near mouth of Corkscrew Creek, Apr 1-May 31" — part: *west of a line between fishing boundary signs on lakeshore near mouth of Corkscrew Creek* — caveat names the part

**RAINBOW LAKES** — `r7:rainbow_lakes@7-52`, p.66 (PDF 72), 1 rule(s)

- `rainbow_lakes.r1` book: "No Fishing within 100 m of fishing boundary sign at outlet, May 15-July 31" — part: *within 100 m of fishing boundary sign at outlet* — caveat names the part

**WILLISTON LAKE (in Zone A) (includes waters 500 m east/upstream of the Causeway Road)** — `r7:williston_lake_in_zone_a_includes_waters_500_m_east_upstream@7-30+7-37+7-38`, p.61 (PDF 67), 1 rule(s)

- `williston_lake.r2` book: "Bait ban 500 m upstream and downstream of Causeway Road July 1-Oct 30" — part: *500 m upstream and downstream of Causeway Road* — caveat names the part

**WOLVERINE LAKE** — `r7:wolverine_lake@7-52`, p.66 (PDF 72), 1 rule(s)

- `wolverine_lake.r1` book: "No Fishing within 100 m of fishing boundary sign at outlet, May 15-July 31" — part: *within 100 m of fishing boundary sign at outlet* — caveat names the part

**Peace River navigation** — `z7b:peace_river_navigation`, p.64 (PDF 70), 1 rule(s)

- `peace_river_navigation.r1` book: "PEACE RIVER The Peace River is no longer navigable past the Site C construction site. Avoid boat travel between 2 km upstream of the dam site (warning signs …" — part: *between 2 km upstream of the Site C dam site and the downstream construction bridge* — caveat names the part

**CHRISTINA LAKE** — `r8:christina_lake@8-15`, p.69 (PDF 75), 1 rule(s)

- `christina_lake.r1` book: "No Fishing north of a line between Bald and Knob Points, Mar 1-May 31" — part: *north of a line between Bald and Knob Points* — caveat names the part

**KALAMALKA LAKE** — `r8:kalamalka_lake@8-22`, p.70 (PDF 76), 2 rule(s)

- `kalamalka_lake.r3` book: "Speed restrictions or no vessels (as buoyed and signed), various locations" — part: *various locations (as buoyed and signed)* — caveat: part not identified ('on parts')
- `kalamalka_lake.r4` book: "Speed restrictions or no vessels (as buoyed and signed), various locations" — part: *various locations (as buoyed and signed)* — caveat: part not identified ('on parts')

**MABEL LAKE** — `r8:mabel_lake@8-24`, p.70 (PDF 76), 1 rule(s)

- `mabel_lake.r1` book: "No Fishing south of a line between fishing boundary signs on the lakeshore approximately 800 m north of Shuswap River inlet, Jan 1-May 31" — part: *south of a line between fishing boundary signs on the lakeshore approximately 800 m north of Shuswap River inlet* — caveat names the part

**MARA LAKE** — `r8:mara_lake@8-26`, p.70 (PDF 76), 1 rule(s)

- `mara_lake.r2` book: "No powered boats south of the CPR bridge" — part: *south of the CPR bridge* — caveat names the part

**OKANAGAN LAKE** — `r8:okanagan_lake@8-10`, p.70 (PDF 76), 2 rule(s)

- `okanagan_lake.r4` book: "Speed restrictions or no vessels (as buoyed and signed), various locations" — part: *various locations (as buoyed and signed)* — caveat: part not identified ('on parts')
- `okanagan_lake.r5` book: "Speed restrictions or no vessels (as buoyed and signed), various locations" — part: *various locations (as buoyed and signed)* — caveat: part not identified ('on parts')

**OSOYOOS LAKE** — `r8:osoyoos_lake@8-1`, p.70 (PDF 76), 2 rule(s)

- `osoyoos_lake.r6` book: "Speed restriction on parts" — part: *on parts* — caveat: part not identified ('on parts')
- `osoyoos_lake.r7` book: "no vessels (see definition on page 4) in 5 signed swimming areas" — part: *in 5 signed swimming areas* — caveat names the part

**ROSEMOND LAKE** — `r8:rosemond_lake@8-26`, p.70 (PDF 76), 1 rule(s)

- `rosemond_lake.r1` book: "No powered boats south of the CPR Bridge" — part: *south of the CPR Bridge* — caveat names the part

**SKAHA LAKE** — `r8:skaha_lake@8-9`, p.71 (PDF 77), 2 rule(s)

- `skaha_lake.r5` book: "Speed restrictions or no vessels (as buoyed and signed), various locations" — part: *various locations (as buoyed and signed)* — caveat: part not identified ('on parts')
- `skaha_lake.r6` book: "Speed restrictions or no vessels (as buoyed and signed), various locations" — part: *various locations (as buoyed and signed)* — caveat: part not identified ('on parts')

**WOOD LAKE** — `r8:wood_lake@8-22`, p.71 (PDF 77), 1 rule(s)

- `wood_lake.r5` book: "Speed restriction (10 km/h) on parts" — part: *on parts* — caveat: part not identified ('on parts')

## Adversarial review (2026-10-23)

Read against the printed synopsis (p.28 = PDF 30 rendered, p.42 = PDF 48, p.51 = PDF 57) and re-measured on a fresh
scratch deliver (export + answers + card_check of the reviewed code from the live bundle, diffed read-only).

Findings:
1. **DEFECT, fixed — Mahood's outlet rules cited a map that does not draw the outlet.** `book_map` took the row
   heading's "(see map on page 28 for area closure)" for EVERY identified part on the row. The p.28 Mahood map shows
   only the western tip (Canim River mouth, the Jan 1-June 30 sign line); "within 200 m of the Mahood River outlet"
   (r5-r8) is the lake's far end, off the map. The app would have told anglers "see the map on p.28" for a place
   that map does not show, and opened p.28 from their book link. Fix: `export_ui_rules.ROW_MAP_DOES_NOT_DRAW`
   (row-name prefix + part pattern, each verified against the printed map); `book_map` returns None for a part in it.
   Tests: `MAPS` drops Mahood r5-r8; `test_a_part_off_the_rows_map_cites_nothing` (Mahood outlet cites nothing, its
   western tip still cites p.28, and every exclusion still matches a widened rule so none goes stale).
2. **Test gap, fixed — the export gate wiring was untested.** Removing `undrawn_map_problems(doc)` from `problems()`
   passed every test. Added `test_the_export_gate_refuses_an_unmapped_map_page` (needs_bundle): a cited page outside
   `guide.pdf_pages` is refused by `problems()` itself.
3. **Wrong measurement, corrected in this ticket** — "75 name their part" was 98 rules (81 distinct parts on 64 rows);
   "23 cite a map" is 19 after fix 1 (24 in the commit message / test docstring was also wrong); answers `book`
   changes 12, not 16.

Checked, no defect:
- Second copies: `applies` / `book` are composed only in `display.applies_here` / `book_pages` (one caller,
  `display.rules`); the app prints `facts.applies` / `applies_trib` / `facts.book` (sheets.ts, decided.ts,
  sources.ts, other.ts) and words nothing. The licensing in-part box (`licence.ts`) reads `part`, not `says`.
- `applies_trib` never carries the caveat, correctly: no widened rule reaches a tributary (bundle `ruleset`:
  0 rows `via='trib'` for a rule with `undrawn_part`), and live answers ship no `applies_trib` on them.
- Mutations (each fails the named test, run against the live bundle): `book_map` -> None (7 fail), heading not read
  (7), "(map A)" tag kept (3), `applies_here` drops the caveat (1), `book_pages` ignores the map (1), the gate's page
  check off (1), `ROW_MAP_DOES_NOT_DRAW` emptied (3), gate unwired (1, after fix 2; 0 before).
- Printed pages: Shuswap p.32 -> PDF 34, maps p.28 -> PDF 30, Canim p.43/42 -> PDF 49/48, François p.51 -> PDF 57.
  Map A/B/C areas and Salmon Arm Bay match the p.28 drawings; Canim's "NO FISHING ZONE" at Eagle Creek is on p.42;
  "MAP 1 FRANÇOIS LAKE - Outlet" is on p.51.

Re-measured (scratch deliver of the fixed code vs live, read-only):
- Export: 19 rules change, only `not_yet_mapped` (map 1 on p.51 x7, the map on p.28 x5 [Mahood r1-r4 + Salmon Arm
  Bay], map A x2, map B, map C, the maps on p.28 x2 [the Salmon Arm pier exemptions], the map on p.42 [Canim]).
  Mahood r5-r8 carry no `map`: "… within 200 m of the Mahood River outlet — which is not drawn on the map yet. It does
  not apply to the rest of the water."
- Answers: only `sections.display.rules` (+ spec text): `applies` 135, `book` 12 (Mahood r1-r4 and Shuswap's 7 add
  [28,30]; Canim adds [42,48]).
- card_check on the scratch answers: 0 problems (16,004 groups, 7,159 cards, 2,266 keys).

Owner questions (defaults built):
- Q-T100-a: the Salmon Arm community-pier exemptions (r5x, r11x) cite "the maps on p.28" from the row heading. The
  pier is not marked on the Salmon Arm Bay map, though the City of Salmon Arm at the bay's head is. Default: keep the
  cite (it locates the bay the pier is on); add the pier to `ROW_MAP_DOES_NOT_DRAW` if you would rather it cite none.

Rebuild: unchanged — deliver (export + answers + store), then a regs fixture re-cut.
