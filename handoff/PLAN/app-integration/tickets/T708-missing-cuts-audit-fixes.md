# T708 — Missing-cuts audit fixes: Pitt "at Grant Narrows", Ruby Creek, Gagnon signs, Deena/Mamin bridges, Tlell note, end names, unmatched lakes

Status: OPEN.

Source: `handoff/PLAN/research/missing-cuts-audit.md` (2026-11-13, release r-adc4e248a163) and the owner's answers
the same day; follow-up research 2026-11-13 (this ticket, read-only, no data changed). Before closed testing.
Pictures (Esri imagery, scratch): `deena_cut.jpg`, `mamin_cut.jpg`, `mcrae_cut.jpg` from the follow-up lane; Gagnon:
`~/bcfish-session-backup-2026-11-12/lake-ends/gagnon_creek.jpg` (shows the old 400 m cut).

## Owner decisions (2026-11-13)

1. **Pitt River `pitt_river.r2`** "speed restriction at Grant Narrows (10 km/h)": treat it like the other vessel
   "on parts" rules (stays on the water as they do), and its label keeps the place: "… at Grant Narrows". Today the
   label reads only "Speed restriction (10 km/h)". The 30 "on part(s)" vessel rules stay as they are.
2. **Ruby Creek `ruby_creek.r1`**: defect. "From Ruby Lake to signs ~100 m downstream" is a `between`
   (Ruby Lake cut 10.59 km, sign cut 10.49 km), not `upstream_of` the sign (which also takes the creek above Ruby Lake
   up to Ambrose Lake + 6 tributary sections). PROPOSAL; the owner has not objected.
3. **Yakoun**: correct as placed. **McRae Creek**: check (picture). **Lakelse**: add "signs" to the mouth's name.
   **Kootenay `kootenay_river_lower.r1`**: fine if the lower boundary carries its proper name.
4. **Columbia River r12 "in wetlands"**: a future item to look at better (not this ticket's work).
5. **Decided later the same day** (owner answers on the follow-up report):
   - **Deena**: "deena proposed is good": the cut moves to crossing 2 (8.83 km on the line); stays `low`.
   - **Mamin**: "same with mamin crossing": the cut moves to crossing 3 (16.71 km); stays `low`.
   - **McRae**: "seems good. i think i spent time confirming before": no change, closed.
   - **Gagnon**: "should cover a little bit above the unnamed lake too. so make it 200m above the unnamed lake or so":
     the sign cut goes ~200 m above the unnamed lake's inlet (≈ 3,859 m on the line); the exemption covers the 74 m
     channel, the whole unnamed lake and that ~200 m of creek (Work 3).
   - **Unmatched lakes**: the owner goes through Hidden, Secret, Square (→ Whitton) and Frog with evidence pictures
     (`~/bcfish-session-backup-2026-11-12/audit-cuts/`: `hidden_lake.jpg`, `secret_lake.jpg`,
     `square_whitton_lake.jpg`, `frog_lake_leads.jpg`). Then: "add the 3 high confidence lakes. frog seems unsure
     still … will look harder for frog and redfern later": Hidden, Secret, Square → Whitton DECIDED (Work 8).
6. **Answered 2026-11-13** (were the open questions):
   - **Haida Gwaii tidal boundary (audit Q1)**: stays at the mouth for Copper, Deena and Mamin unless a written
     boundary is listed elsewhere (standing order: DFO written boundary → points-of-land line → polygon). Check
     (read-only, `data/source/dfo_tidal_areas/2026-10-28` and `2026-11-11`, SOR/2007-77, curated
     `waters/tidal_limits.json`, `tidal_split_authority.json`, `cuts.json`): **no written tidal boundary for any of
     the three.** DFO lists only "The tidal portion of all streams flowing into Areas 1, 2 and 102". Near-misses, not
     tidal boundaries: "Diina (Deena Creek): That portion of subarea 2-1 that lies inside a line commencing at
     53° 09.415' N, 132° 06.870' W …" is a sub-area of the X̲aana K̲aahlii (Skidegate Inlet) Marine Refuge
     (`fm-gp/rec/tidal-maree/a-s2-eng.html`); "Copper River estuary … shoreward of a straight line between two
     boundary signs located on either side of Copper River estuary" is a gear-restriction area in the 2026-10-28
     copy only (gone from 2026-11-11). Mamin: nothing. No change.
   - **Stave River Ruskin channel (Q6)**: the whole described channel; `stave_river.r1` stays on all 4 pieces. No change.
   - **McArthur Island Slough (class a item 2)**: the whole slough; the stretch stays (3 sections). The card must keep
     the book's place words ("from westerly entrance to 12th Street entrance to Park"); today's label reads only
     "No powered boats".

## Work

1. **Pitt r2**: give the rule its place words ("at Grant Narrows") so the label says it; extent as the "on parts"
   rules have it.
2. **Ruby Creek r1**: `between` [`ruby_creek__signs_100_m_below_ruby_lake`, `ruby_creek__ruby_lake`] (both exist);
   diff the rule's bindings (100 m piece only).
3. **Gagnon Creek r1** — DECIDED (owner 2026-11-13, Owner decisions 5): a sign cut ~200 m above the unnamed lake's
   inlet (≈ 3,859 m; the inlet is 3,659 m; that stretch is the creek's own double-line polygon, wbk 328948699).
   `downstream_of` it then binds the 74 m channel, the unnamed lake wbk 329465442 (the walk/extent must take the lake:
   lakes are never cut) and the ~200 m of creek. Mouth stays Williston and the lake stays its own (P57). What
   changes: RULINGS P57 (its "signs ARE the mouth … covers no water" sentences; the mouth part of it stands), the cut
   and its note in `cuts.json` (`gagnon_creek__fishing_boundary_signs_at_the_mouth` → a point cut with the book's
   words), `pipeline/tests/test_t89_rules_that_bind_no_water.py` (gagnon r1 leaves BY_DESIGN: 8 → 7, ALLOWED 29 → 28
   in `test_the_allow_list_is_exact`), and the creek's part names (one part "The mouth (at Williston Lake) → Source"
   becomes mouth → signs → Source). Atlas rebuild. The earlier options, kept for the record — facts:
   - FWA line (blk 359565112): 0–1,761 m runs INSIDE Williston Lake's full-pool polygon (lake connector, the
     reservoir arm; the drawdown zone has no stream piece of its own); 1,761–1,835 m a 74 m channel; 1,835–3,659 m
     through the 58 ha unnamed lake wbk 329465442 (behind an embankment); creek above. First road crossings:
     Finlay-Causeway FSR 5,362 m, Gagnon Creek Bridge (highway) 5,715 m. No record of where the signs stand.
   - Before P57 the cut was `gagnon_creek__signs_400_m_above_williston_lake` at route measure ~2,161 m (Williston edge
     + 400 m): inside the unnamed lake, so it aliased onto that lake's outlet edge (1,835 m) and the exemption held on
     the 74 m channel only (the picture above).
   - Options: (a) restore that cut: exemption = the 74 m channel; (b) also give the exemption the unnamed lake
     (the 400 m point lies in it; lakes are never cut, so all or none of its 58 ha); (c) 400 m measured from the line's
     measure 0 lies inside Williston: no creek water, nothing binds.
   - P57 ("mouth is still Williston") to keep true: the creek's mouth stays the Williston Zone A edge (the part's
     lower end stays "The mouth (at Williston Lake)"), the unnamed lake stays its own lake (`waters/pond_calls.json`).
     What changes with it: RULINGS P57's "signs ARE the mouth" sentence, the cut note in `cuts.json`, and
     `test_t89_rules_that_bind_no_water.py` BY_DESIGN (gagnon r1 then binds; the count 8 → 7).
4. **Deena `deena_creek.r1`, Mamin `mamin_river.r3`** (owner: the cut ON A BRIDGE where one exists, old or broken
   ones too, near the book's distance along the stream OR as the bird flies; stays low confidence). Road crossings
   from the BC Digital Road Atlas + FTEN (all statuses) + PSCIS; distances from the FWA tidal crossing:

   | water | crossing | on line | above tidal, stream | bird | road |
   |---|---|---|---|---|---|
   | Deena (tidal 0.31 km) | 1 | 1.52 km | 1.2 km | 0.6 km | paved resource road (OSM bridge) |
   | | **2 — proposed** | **8.83 km** | 8.5 km | **5.3 km** | loose resource road (DRA), 53.11879,-132.20619 |
   | | 3 | 10.27 km | 10.0 km | 6.6 km | overgrown road (DRA) |
   | | 4 | 14.03 km | 13.7 km | 8.1 km | FTEN R13249 "121E-100 TO FORK05" |
   | | current cut | 5.31 km | 5.0 km | 3.1 km | no road: a point at the book's distance |
   | Mamin (tidal 2.08 km) | 1 | 4.58 km | 2.5 km | 1.9 km | Datlamen Main / "New Mainline" |
   | | 2 = current cut | 11.16 km | 9.1 km | 5.8 km | Mamin Main |
   | | **3 — proposed** | **16.71 km** | 14.6 km | **9.0 km** | DRA "unsigned", loose, 53.53846,-132.32589 |
   | | 4–6 | 28.2–30.6 km | 26–29 km | 14.5–16.5 km | Mamin Main / MAMIN16 |

   Counting bridges AND reading the book's km as the bird flies fit both rows (2nd crossing ~5 km, 3rd ~9 km);
   Copper Creek, already accepted, fits the same reading (2nd crossing South Bay Main: 7.7 km stream, 5.3 km bird,
   book "6 km"). DECIDED (owner 2026-11-13): Deena → crossing 2 (8.83 km), Mamin → crossing 3 (16.71 km), both kept
   `low`; re-anchor `deena_creek__second_bridge_5_km_above_tidal` / `mamin_river__third_bridge_mamin_main` (new
   labels name the crossing), atlas rebuild, diff the two rules' bindings. Unconfirmed:
   no source says which roads have bridges vs culverts, and an unmapped crossing would change the count.
5. **McRae Creek `mcrae_creek.r1`** ("falls ~4 km upstream of Christina Lake"): the cut is the FISS 4 m falls
   (24834) at 2.33 km on the line = 2.05 km above the lake shore by stream, 1.4 km bird. FISS has a 2 m falls at
   2.47 km and nothing near 4 km (4.0 km above the shore is km 4.28). CLOSED, no change (owner 2026-11-13: "seems
   good. i think i spent time confirming before").
6. **Tlell `tlell_river.r5`** (tidal note). App facts: the Tlell River's own page shows it nowhere (part 1, 4 sections,
   source → tidal sign: r1, r2, licensing only; part 2, the tidal piece below the sign: no rules, the tidal guide
   line). The note sits in one rule set (bundle set via `trib`, 13 sections): Geikie Creek's upper part (1 section,
   searchable by name) and 12 unnamed tributary lines (map tap only). None of the 13 is tidal: none in the bundle's
   `tidal` table, all freshwater streams 0.7–8.6 km long (11 drain to Geikie Creek, 1 joins the Tlell directly below
   the sign). Proposal (owner's view): take the note off every one that is not fully tidal, i.e. all 13 (the rule
   is the row's `includes_tributaries` walking from a reach that the tidal trim emptied). The tidal part already says
   "tidal water" by itself.
7. **End names.** Kootenay: the closure's lower end is the auto lake-edge split
   `kootenay_river__kootenay_lake_main_body` ("Kootenay Lake — Main Body"); label "No fishing — between CPR and
   Kootenay Lake — Main Body". The curated `kootenay_river__navigation_dolphin_2000_m_downstream` (cuts.json, anchored
   on the retired whole-lake wbk 328974235) resolves nowhere (not in `splits.resolved.json` nor the bundle). The name
   is set by a `cuts.json` label: re-anchor the dolphin split on Kootenay Lake — Main Body's inlet edge (wbk -20) with
   the book's words ("navigation dolphin, 2 km below CPR Bridge") and point r1 at it; check which name wins when two
   cuts share the place (`sectionizer._coincident`: the standing boundary keeps its name). "CPR" may become "CPR
   Bridge near Creston". Lakelse: the lower part runs "power line crossing → mouth"; `mouth` is a fixed token worded
   "the mouth"/"Mouth" in `pipeline/deliver/answers/display/part.py` (`end_name`, `_end_short`); no per-water mouth
   name exists. Ways: a cut at the designated mouth with a label carrying "signs" (audit Q4; note `_SIGN_WORDS`
   strips a LEADING "signs at/near" from short names, so the words must not start with "signs"), or a per-water
   mouth-name field (new).
8. **Unmatched waters** (all regions, lakes and streams): exactly 6 entries match no water and hold rules
   (21 rules, `no_sections_for_items`): Frog, Hidden, Secret, Square (5-6, p.45–47), Redfern (5-15, p.46), Squirrel
   (R6 6-1, p.53; the R5 copy is a `see`). Every other entry with no water is an area row, a pointer or a
   cross-listed duplicate. Evidence pictures (Esri imagery, MU line labelled):
   `~/bcfish-session-backup-2026-11-12/audit-cuts/hidden_lake.jpg`, `secret_lake.jpg`, `square_whitton_lake.jpg`,
   `frog_lake_leads.jpg`.

   **DECIDED (owner 2026-11-13: "add the 3 high confidence lakes")** — one `data/curated/waters/name_variants.json`
   record each, in the file's existing form (as Cruise Lake / "Fish" Lake, the same 1997 survey series):
   ```json
   {"target": {"wbks": ["329021830"]}, "names": [{"name": "HIDDEN LAKE", "source": "regulation", "note": "Unnamed lake in MU 5-6, 3.2 km SW of Stewart Lake. BC Lakes Database: WBID 01338ATNA (FISS 1993-03-12 'Limnology data for Hidden Lake'; AIM 1997 'Reconnaissance Survey of Un-named (Hidden) Lake', EcoCat 32595, 32579). Owner accepted 2026-11-13 (T708).; REGION 5 - Cariboo; MU 5-6"}]}
   {"target": {"wbks": ["329021850"]}, "names": [{"name": "SECRET LAKE", "source": "regulation", "note": "Unnamed lake in MU 5-6, 0.7 km south of Hidden Lake on the same creek. BC Lakes Database: WBID 01370ATNA (AIM 1997 'Reconnaissance Survey of Unnamed (Secret) Lake'; EcoCat 49174, charts titled 'Secret Lake'). Owner accepted 2026-11-13 (T708).; REGION 5 - Cariboo; MU 5-6"}]}
   {"target": {"wbks": ["329021786"]}, "names": [{"name": "SQUARE LAKE", "source": "regulation", "note": "Local name of Whitton Lake (gnis 39187) in MU 5-6, above Cohen Lake. BC Lakes Database: WBID 01503ATNA (AIM 1997 'Reconnaissance Survey of Whitton (Square) Lake'; EcoCat 33962). The book has no Whitton row. Owner accepted 2026-11-13 (T708).; REGION 5 - Cariboo; MU 5-6"}], "_gnis": ["39187"]}
   ```
   Evidence per lake: Hidden — unnamed, 24.1 ha, 52.1132,-125.6079; Secret — unnamed, 17.5 ha, 52.1005,-125.6163;
   Square — Whitton Lake, 237.7 ha, 52.0318,-125.5638. Each survey's 50K code equals the FWA lake's own
   `WATERBODY_KEY_50K` + group code (an exact join, not nearest). No other FWA lake in MU 5-6 carries these names.
   Follow-on (the lane that applies them):
   - `data/curated/regulations/book_name_links.json`: the three `not_found: true` rows for HIDDEN / SECRET / SQUARE
     LAKE (Region 5, MU 5-6) say "do not bind"; remove them (or the variants never reach the entries).
   - Registry rebuild (atlas build, `--atlas` side build + parity, then promote); re-match the entries (`matched`).
   - The reach run's unresolved rules drop by 11 (Hidden 4, Secret 4, Square 3): 29 → 18 (→ 17 with Gagnon r1, Work 3).
   - `pipeline/tests/test_t89_rules_that_bind_no_water.py`: take the three rows out of `AWAITING_WATER_ROWS`;
     `test_the_allow_list_is_exact` pins `len(AWAITING_WATER) == 21` → 10 and `len(ALLOWED_UNRESOLVED) == 29` → 18
     (17 with Gagnon; `BY_DESIGN` 8 → 7). The docstring's "seven rows" text follows. No other test names these entries
     (`fixtures/review_tiers_sample.json` is a frozen fixture). The binding diff vs the live release lists 11 rules
     newly bound on 3 lakes.

   **Still unmatched** (stay in `AWAITING_WATER_ROWS`; Frog and Redfern: owner will look harder later):
   - **Frog Lake** (p.45, MU 5-6; "No Ice Fishing; trout daily quota = 1; bait ban, single barbless hook"; no locating
     words, the same in the 2008-09 synopsis). The Cariboo 2008 harmonization table (gov.bc.ca
     `RegChanges-Update05dec08.pdf`) put Frog, Hidden and Secret together in suite R3 (quota 1 was already theirs;
     2008 added the ice closure), so Frog is most likely another lake of the Hidden/Secret group near Stewart Lake.
     Leads (unnamed, each with only a 1994 Region 5 "LAKE SURVEY DATA" record; picture `frog_lake_leads.jpg`):
     A wbk 328962870, 10.9 ha, 01256ATNA, 52.1769,-125.4629 (1.8 km W of Charlotte Lake); B wbk 329021897, 8.6 ha,
     01267ATNA, 52.1670,-125.4710; C wbk 329021982, 4.4 ha, 01272ATNA, 52.1622,-125.4823 (A and C on the Stewart Lake
     creek, B on a side creek). Other unnamed lakes with 1994 records nearby: 01504ATNA (wbk 329021836, 22.1 ha;
     wbk 329021863, 14.1 ha), 01538ATNA (wbk 329022115, 2.7 ha), 01542ATNA (wbk 329022199, 2.2 ha), all east of
     Whitton Lake; 01479ATNA (no FWA lake ≥ 2 ha found). Tried, nothing: FWA names province-wide (BC's Frog lakes
     are in 1-10, 3-28/29, 7-30, 7-52), BCGNIS, FISS lake surveys + fish observations + EcoCat titles (incl.
     "(Frog)"), every FISS/EcoCat record in the Charlotte–Stewart block, the repo's stocking records (FIDQ, 64,991
     releases: none), the 2008 table and the 2008-09 synopsis. None of the 1997 AIM survey titles is Frog. Would
     settle it: the 1994 survey forms for those codes or the Williams Lake fisheries office's lake file, the scanned
     EcoCat 49174 report, or an older synopsis with a locating note.
   - **Redfern Lake** (p.46, MU 5-15; "Trout daily quota = 2; bait ban, single barbless hook"; no locating words, same
     row in 2008-09). BC's only Redfern Lake (gnis 28852, 498.6 ha) is in MU 7-42 (Redfern-Keily Park). In the
     Cariboo only Ruth Redfern Creek (MU 5-15, blk 356363780) carries the name; it runs through Deception Lake (its
     own 5-15 row, same rules) and rises at unnamed lake wbk 329354762 (32.8 ha, 00034MAHD, 52.1514,-120.5416), which
     FISS (survey 5484) and EcoCat 51923 call "Unnamed Lake (Goat Lake)" — against Redfern. Tried: FISS/EcoCat by
     name, every survey in the Canim/Deception/McNeil block, BCGNIS. Would settle it: the regional office, or a
     book or gazetteer record tying "Redfern" to a lake on Ruth Redfern Creek.
   - **Squirrel Lake** (R6 p.53 + R5 p.47 copy, MU 6-1; "Trout daily quota = 2; bait ban, single barbless hook";
     2008 suite R2 with the other 6-1 lakes). BC's one Squirrel Lake (gnis 38786, wbk 329354753, 43.1 ha, 01446MAHD)
     is in MU 5-1 and is already bound by the Paddy Lake 5-1 row. The other 6-1 rows (Toms, Basalt, Gatcho, Naglico,
     Pettry, Chipmunk) came from the 1993 Region 5 winter-limnology series in EUCL, which also has unnamed "Lake
     #19–#25"; no record names Squirrel. Tried: FISS lake records 52.88–53.2 N / 124.9–125.9 W, EcoCat EUCL /
     Kluskus / Euchiniko titles, BCGNIS, web. Would settle it: the same office file for the 1993 series.

   **Deeper search (2026-11-13, owner "look harder")** — still no lake named; nothing to accept. Sources read:
   archived Region 5/6 synopsis tables 2004 (wlapwww.gov.bc.ca `fw/fish/pdf/region_5.pdf`, `region_6.pdf`, Wayback
   20040405), 2005-07, 2007-08, 2008-09, 2009-11; the 2003 regulation HTML pages and Cariboo/Skeena stocking lists
   (Wayback 2003, no lake tables / none of the names); BCGNIS all statuses (no Frog/Redfern/Squirrel lake in these
   MUs, none rescinded); every FISS lake survey + EcoCat report inside MUs 5-6, 5-15, 6-1; the scanned report covers
   and summary sheets of the unaliased surveys below; archive.org search (no scanned synopsis). All three rows are
   already in the 2004 edition with the same names and no locating words (earliest table found).
   - Frog: the 1993-03-12 Region 5 winter-limnology trip that recorded "Lake Name: HIDDEN" (EcoCat 32595 sheet) also
     sampled "UNNAMED (#2)" 00945KLIN = wbk 329238264 (9.3 ha, 51.8784,-124.9956) and "UNNAMED (#3)" 00949KLIN = wbk
     329238354 (4.8 ha, 51.8761,-125.0023), both just west of Agnus/Miner lakes. AIM's 1997 batch (which aliased
     Hidden, Secret, Square, Fish, No Name, Banana, Maydoe) left two lakes unaliased: "#30" 00907ATNA = wbk 329021857
     (16.3 ha, 52.3613,-125.5859) and "#31" 00876ATNA = wbk 329021820 (32.3 ha, 52.3754,-125.5779); their report
     covers and summary sheets carry no alias. With leads A–C these are seven possible lakes; none is named Frog
     anywhere. Confidence: none.
   - Redfern: "Goat" Lake (00034MAHD, wbk 329354762) had its own 5-15 row beside Redfern in 2004–2008-09, so
     Redfern is not Goat. The only other lakes in the Ruth Redfern Creek watershed are Deception (own row) and ponds
     ≤ 5 ha (largest wbk 329355014, 5.1 ha, 52.1614,-120.6007, 00011MAHD; no survey). AIM's 1998 unaliased 5-15 lake
     00681MAHD (wbk 329354774) reads "Local name: UNNAMED (#6327)" and drains to Hendrix Creek: not it. Not found.
   - Squirrel: NOT a misfile of the 5-1 lake — in 2004 Paddy Lake (5-1, FWA "Squirrel Lake") printed only "Trout daily
     quota = 2" while Squirrel (6-1) printed the 6-1 group's "quota = 4; bait ban, single barbless hook" (Toms,
     Chipmunk, Naglico), in both the Region 5 and Region 6 tables. The 6-1 group comes from the Region 5 limnology run
     of 1993-03-03 (sheets "Lake Name: TOMS", "CHIPMUNK", "#19"–"#25", MU "5-12 (6-01)"); Squirrel is likely one of
     the numbered ones: #19 00863EUCL wbk 329126790 (26.5 ha), #20 00725EUCL wbk 329126734 (53.8 ha), #21 00888EUCL
     wbk 328967975 (34.1 ha), #22 00913EUCL wbk 329126782 (28.9 ha), #23 00908EUCL wbk 329126768 (33.0 ha), #24
     00898EUCL wbk 329126717 (77.1 ha), #25 01094EUCL wbk 329126741 (48.1 ha), all 52.99–53.03 N, 125.0–125.4 W. No
     sheet names Squirrel. Not found.
   Would settle any of them: the Williams Lake (Region 5) office's lake files for the 1993 limnology runs, or an
   edition before 2004 (not online in any archive found).

   Full earlier evidence: `handoff/PLAN/curation-review/unmatched-lakes-research.md` (2026-10-28).

## Acceptance

Per item: the reach run's per-rule binding diff names only the intended moves (Ruby 100 m piece; Gagnon the chosen
part; Deena/Mamin the moved cuts; Tlell r5 0 tributary sections); the T89 allow-list shrinks by exactly the rules that
now bind; labels read "… at Grant Narrows", the Kootenay end in the book's words, the Lakelse mouth with "signs".

Depends: atlas rebuild for new/moved cuts (Deena, Mamin, Gagnon, Kootenay, Lakelse); reach + deliver for the rest.
Size: M. Owner: yes — Frog and Redfern (owner looks harder later), and the open
questions in Owner decisions 5. Default: nothing changes until accepted.
