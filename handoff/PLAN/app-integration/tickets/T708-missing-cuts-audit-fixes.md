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
     `square_whitton_lake.jpg`, `frog_lake_leads.jpg`); not yet accepted.
6. Open owner questions: Haida Gwaii tidal boundary at the mouth (Copper/Deena/Mamin, audit Q1); Stave River Ruskin
   channel (Q6); McArthur Island Slough (class a item 2).

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
   cross-listed duplicate. Proposals (owner accepts; fix path: a `name_variants.json` record per lake, rebuild the
   registry, shrink `AWAITING_WATER_ROWS`):

   | entry | proposal | evidence | confidence |
   |---|---|---|---|
   | r5:hidden_lake@5-6 | unnamed lake wbk 329021830 (24.1 ha, 52.1132,-125.6079) | FISS/EcoCat "Un-named (Hidden) Lake" 01338ATNA = its 50K key | high |
   | r5:secret_lake@5-6 | unnamed lake wbk 329021850 (17.5 ha, 52.1005,-125.6163) | FISS "Unnamed (Secret) Lake" 01370ATNA; EcoCat 49174 charts "Secret Lake" | high |
   | r5:square_lake@5-6 | Whitton Lake wbk 329021786 (registry item, 237.7 ha) | FISS "Whitton (Square) Lake" 01503ATNA; no Whitton row in the book | high |
   | r5:frog_lake@5-6 | not found | weak: wbk 328962870 / 329021897 / 329021982 (Stewart Lake drainage, unnamed) | — |
   | r5:redfern_lake@5-15 | not found | only Ruth Redfern Creek (5-15); its head lake wbk 329354762 is "Goat Lake" in FISS | — |
   | r6:squirrel_lake@6-1 | not found | BC's one Squirrel Lake (wbk 329354753) is in 5-1 and already Paddy Lake's row | — |

   Full evidence: `handoff/PLAN/curation-review/unmatched-lakes-research.md` (2026-10-28), re-checked 2026-11-13 on the
   current FWA (wbks, MUs and areas unchanged); new searches (FISS lake surveys, EcoCat titles incl. "(Frog)" /
   "(Squirrel)" / "(Redfern)" aliases, FWA names province-wide) found nothing more.
   More (2026-11-13): FISS also holds a 1993-03-12 Region 5 winter-limnology record "Limnology data for Hidden Lake"
   on 01338ATNA. The Cariboo region's 2008 harmonization table (env.gov.bc.ca RegChanges-Update05dec08.pdf) put
   Frog, Hidden and Secret together in suite R3 (quota 1 + the ice closure) and Square, Squirrel, Stewart in R2; the
   2008-09 Region 5 synopsis prints the same rows with no locating words. The repo's stocking records (FIDQ, 64,991
   releases) hold none of these lakes. No other FWA lake in MU 5-6 carries any of the four names. Frog would be
   settled by the Williams Lake office's lake file (the 1993/1994 survey forms on 01256/01267/01272 ATNA are not
   online as text) or the scanned EcoCat reports; none of the 1997 AIM survey titles is Frog.

## Acceptance

Per item: the reach run's per-rule binding diff names only the intended moves (Ruby 100 m piece; Gagnon the chosen
part; Deena/Mamin the moved cuts; Tlell r5 0 tributary sections); the T89 allow-list shrinks by exactly the rules that
now bind; labels read "… at Grant Narrows", the Kootenay end in the book's words, the Lakelse mouth with "signs".

Depends: atlas rebuild for new/moved cuts (Deena, Mamin, Gagnon, Kootenay, Lakelse); reach + deliver for the rest.
Size: M. Owner: yes — accept the Deena/Mamin bridges, the Gagnon option, the three lake matches, and the open
questions in Owner decisions 5. Default: nothing changes until accepted.
