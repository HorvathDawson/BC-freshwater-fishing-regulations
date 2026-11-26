# T708 — Missing-cuts audit fixes: Pitt "at Grant Narrows", Ruby Creek, Gagnon signs, Deena/Mamin bridges, Tlell note, end names, unmatched lakes

Status: PARTIAL (874bbd00a, 8937f6b73, d109408fd, 8a1ce30f1: all built and released, wording a-c in r-f749475d0962; owner: see ## Owner for Frog/Redfern/Squirrel).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T708 | Missing-cuts audit fixes (before closed testing): Pitt r2 "at Grant Narrows" label, Ruby Creek r1 between, Gagnon signs 200 m above the unnamed lake, Deena/Mamin cuts onto bridges, Kootenay dolphin re-anchor, 3 unmatched lakes matched; McArthur / Lakelse / Tlell wording | PARTIAL | 874bbd00a, 8937f6b73, d109408fd, 8a1ce30f1 | all built and released (r-a9914d6b25b7; the owner's wording answers a-c in r-f749475d0962); owner: see ## Owner (Frog/Redfern/Squirrel stay unplaced) |

Source: `handoff/PLAN/research/missing-cuts-audit.md` (2026-11-13, release r-adc4e248a163) and the owner's answers
the same day; follow-up research 2026-11-13 (this ticket, read-only, no data changed). Before closed testing.

Done (built and released, r-a9914d6b25b7; wording a-c in r-f749475d0962): Pitt r2 "— in part: at Grant Narrows"; Ruby r1
`between` (100 m piece); Gagnon r1 cut `gagnon_creek__signs_200_m_above_the_unnamed_lake` (binds the 74 m channel, the
unnamed lake and ~200 m of creek; RULINGS P57 rewritten); Deena → crossing 2 (53.11879,-132.20619), Mamin → crossing 3
(53.53846,-132.32589), both `low`; Kootenay dolphin re-anchored on wbk -20, r1 `between` [cpr, dolphin]; Hidden, Secret,
Square (→ Whitton) in `name_variants.json` (11 rules bound, unresolved 29 -> 17); (a) McArthur `undrawn_part`, (b) Lakelse
"Mouth (at the signs)" (d109408fd, `part.MOUTH_WORDS`), (c) Tlell r5 `includes_tributaries: false`; McRae closed, no change.
Answered, no change: Haida Gwaii tidal boundary stays at the mouth for Copper, Deena, Mamin (no written boundary); Stave
`stave_river.r1` stays on all 4 pieces.

Left as recorded, not built: the Kootenay dolphin cut is an ALIAS of the standing lake boundary (`sectionizer._coincident`),
so the PART end name stays "Kootenay Lake — Main Body"; changing which name wins is a sectionizer policy change. Gagnon's
cut label was OPEN for the owner (reads as a part end "Its mouth (placed 200 m above the unnamed lake)", fails
test_part_names_unnamed_lake; proposed e.g. "fishing boundary signs (200 m above the lake below Williston)", needs another
atlas build). Columbia River r12 "in wetlands": a future item to look at better (not this ticket's work).

## Work 8 (left): the unmatched lakes, Frog, Redfern, Squirrel
Exactly 6 entries matched no water and held rules (21 rules, `no_sections_for_items`); Hidden, Secret, Square are done. Left
(stay in `AWAITING_WATER_ROWS`; Owner decisions 5, 2026-11-13: "add the 3 high confidence lakes. frog seems unsure
still … will look harder for frog and redfern later"; then "look harder", "even deeper … old map books", and for Redfern
"might exist elsewhere": three rounds, no lake named, nothing to accept). Evidence pictures:
`~/bcfish-session-backup-2026-11-12/audit-cuts/frog_lake_leads.jpg`. Earlier evidence:
`handoff/PLAN/research/unmatched-lakes-research.md` (2026-10-28).
- **Frog Lake** (p.45, MU 5-6; "No Ice Fishing; trout daily quota = 1; bait ban, single barbless hook"; no locating
  words). The Cariboo 2008 harmonization table (gov.bc.ca `RegChanges-Update05dec08.pdf`) put Frog, Hidden and Secret
  together in suite R3, so Frog is most likely another lake of the Hidden/Secret group near Stewart Lake. 7 candidates,
  all unnamed and unmatched, none eliminated, confidence none: A wbk 328962870, 10.9 ha, 01256ATNA, 52.1769,-125.4629
  (1.8 km W of Charlotte Lake); B wbk 329021897, 8.6 ha, 01267ATNA, 52.1670,-125.4710; C wbk 329021982, 4.4 ha,
  01272ATNA, 52.1622,-125.4823 (A and C on the Stewart Lake creek, B on a side creek; each only a 1994 Region 5 "LAKE
  SURVEY DATA" record); 1993-03-12 run "UNNAMED (#2)" 00945KLIN = wbk 329238264 (9.3 ha, 51.8784,-124.9956) and "UNNAMED
  (#3)" 00949KLIN = wbk 329238354 (4.8 ha, 51.8761,-125.0023), west of Agnus/Miner lakes; AIM 1997 unaliased "#30"
  00907ATNA = wbk 329021857 (16.3 ha, 52.3613,-125.5859) and "#31" 00876ATNA = wbk 329021820 (32.3 ha, 52.3754,-125.5779).
  Other unnamed lakes with 1994 records nearby: 01504ATNA (wbk 329021836, 22.1 ha; wbk 329021863, 14.1 ha), 01538ATNA
  (wbk 329022115, 2.7 ha), 01542ATNA (wbk 329022199, 2.2 ha), all east of Whitton Lake; 01479ATNA (no FWA lake ≥ 2 ha).
- **Redfern Lake** (p.46, MU 5-15; "Trout daily quota = 2; bait ban, single barbless hook"; no locating words). BC's only
  Redfern Lake (gnis 28852, 498.6 ha) is in MU 7-42 (Redfern-Keily Park; Region 7B never printed a Redfern row; stocked
  once in 1984, 00436UPRO). In the Cariboo only Ruth Redfern Creek (MU 5-15, blk 356363780) carries the name; it runs
  through Deception Lake (its own 5-15 row, same rules: the only 5-15 row printing exactly Redfern's rules in every edition
  2004 → 2025) and rises at unnamed lake wbk 329354762 (32.8 ha, 00034MAHD, 52.1514,-120.5416), FISS survey 5484 / EcoCat
  51923 "Unnamed Lake (Goat Lake)": not Redfern (Goat had its own 5-15 row 2004–2008-09). Other lakes in the watershed:
  ponds ≤ 5 ha (largest wbk 329355014, 5.1 ha, 52.1614,-120.6007, 00011MAHD; no survey); AIM 1998 00681MAHD (wbk
  329354774, "UNNAMED (#6327)", drains to Hendrix Creek): not it. Reading: a small lake managed with Deception in the Ruth
  Redfern Creek / Deception area of 5-15, not the 7B lake; a misprinted MU would also need a misprinted region. Not proven.
- **Squirrel Lake** (R6 p.53 + R5 p.47 copy, MU 6-1; 2008 suite R2 with the other 6-1 lakes). BC's one Squirrel Lake
  (gnis 38786, wbk 329354753, 43.1 ha, 01446MAHD) is in MU 5-1, bound by the Paddy Lake 5-1 row; NOT a misfile: in 2004
  Paddy printed only "Trout daily quota = 2" while Squirrel printed the 6-1 group's "quota = 4; bait ban, single barbless
  hook" (Toms, Chipmunk, Naglico), in both regions' tables. The 6-1 group comes from the Region 5 limnology run of
  1993-03-03 (EUCL, MU "5-12 (6-01)"); in 6-1 four of its lakes carry a row (Toms 329126712, Chipmunk 329126718, Naglico
  329126698, Pettry 329126727), Wolf Lake 329126708 (112 ha) is gazetted with no row, and **7 are unnamed and unmatched**:
  #19 00863EUCL wbk 329126790 (26.5 ha, 53.0208,-125.0010), #20 00725EUCL 329126734 (53.8 ha, 53.0337,-125.0378), #21
  00888EUCL 328967975 (34.1 ha, 53.0168,-125.0423), #22 00913EUCL 329126782 (28.9 ha, 53.0101,-125.2405), #23 00908EUCL
  329126768 (33.0 ha, 53.0094,-125.2811), #24 00898EUCL 329126717 (77.1 ha, 53.0071,-125.3366), #25 01094EUCL 329126741
  (48.1 ha, 52.9898,-125.3511). No sheet names Squirrel; no sole or two-way candidate, so no pictures.

Searched, nothing found (so nobody repeats it): FWA names province-wide; BCGNIS all statuses; federal CGNDB (incl.
previously official); FISS lake surveys + fish observations + EcoCat titles (incl. "(Frog)") in MUs 5-6, 5-15, 6-1 and the
Charlotte–Stewart, Canim/Deception/McNeil and EUCL/Kluskus/Euchiniko blocks; the 1993 limnology sheets; provincial stocking
records (FIDQ, 64,991 releases, `aliases` field; none of the candidates was ever stocked); OpenStreetMap (none of the 21
candidate lakes carries a name); NRCan CanMatrix 1:50,000 093C/03, 093C/05, 093F/03, 093A/02, 092N/14–15 (#2, #3, #25 sit
in sheet margins, adjacent sheets not read); archived synopses 2004 (earliest found, same rows, no locator), 2005-07,
2007-08, 2008-09, 2009-11, the 2003 regulation pages, the 2008 harmonization documents; web (lodges, BC Adventure, Backroad
Mapbooks blog, Fishbrain, forums); archive.org. Not reached: GeoNames (needs an account), printed Backroad Mapbooks
indexes, UBC/UVic map-library scans, HathiTrust annual reports. Would settle any of them: the Williams Lake (Region 5)
office's lake files for the 1993 limnology runs / 1994 survey forms, the scanned EcoCat 49174 report, or an edition
before 2004 (not online in any archive found).

## Acceptance

Per item: the reach run's per-rule binding diff names only the intended moves (Ruby 100 m piece; Gagnon the chosen
part; Deena/Mamin the moved cuts; Tlell r5 0 tributary sections); the T89 allow-list shrinks by exactly the rules that
now bind; labels read "… at Grant Narrows", the Kootenay end in the book's words, the Lakelse mouth with "signs".

Depends: atlas rebuild for new/moved cuts (Deena, Mamin, Gagnon, Kootenay, Lakelse); reach + deliver for the rest.
Size: M. Owner: DECIDED 2026-11-13 by dialog, NOT YET IMPLEMENTED (a pipeline lane does these after the current release chain): (a) McArthur Island Slough shows the "in part" form "No powered boats — in part: from westerly entrance to 12th Street entrance to Park"; (b) Lakelse River: rename the end only, the lower stretch's end reads "Mouth (at the signs)", no new cut, no change to which water the rules cover; (c) Tlell r5: yes, take the tidal note off the 13 freshwater tributaries. Still open: Frog and Redfern (owner looks harder later), and the open
questions in Owner decisions 5. Default: nothing changes until accepted.

## Owner

- [ ] 2026-11-13 — Frog (5-6), Redfern (5-15) and Squirrel (6-1) lakes are still unmatched; you said "will look harder for frog and redfern later" (evidence in Work 8).
- [x] 2026-11-13 — Hidden, Secret and Square (= Whitton) Lake → "add the 3 high confidence lakes" (2026-11-13).
