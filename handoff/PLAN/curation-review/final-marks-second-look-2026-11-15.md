# Final DFO labels: the audit and the second look by eye (2026-11-15, after the owner's last saves)

Owner, 2026-11-15: "all dfo places are labelled now. go through them all and double check there is nothing weird".
Read-only: three checkers (Region 1; Regions 2, 3, 4, 5A, 5B, 7, 8; Region 6) read every place, live and dormant, against
DFO's words, the place files, the cuts and every mark note. Their full reports follow the summary, unedited.
Tags inside them: **[ran]** a script, **[read]** data or code read, **[inferred]** the checker's reasoning.

## The automatic audit (marks.jsonl at 2,769 lines, before the rebuild)

`python -m pipeline.regs.review.audit`: 251 candidates. A 1 (leftover link, Thompson dormant), B 4 (last save wrong, old
extent in force: Fraser Region 3 mainstem, Thompson dormant twin, Morice dormant, Sheldens), C 121 (114 of them "no
tributary choice anywhere", which resolves to off or to the water's flag), D 21 (rest-of), E 7, F 40 (side channels; see
the Regions 2-8 note: the reach step now adds a water's own channels, so most F rows are out of date), G 7 (group marks),
H 2 (one-bank: Kitimat, Nakina), I 13 (not landed: Kasiks x3, Shuswap x2, closed areas; all wait on the rebuild),
J 23 unmarked (18 are region / section / area defaults, 4 are lake parts deferred by decision, 1 Babine Lake), K 12.
It is re-run with `--since` after the rebuild.

## The second look: counts

| checker | places | clean | candidates | look wrong |
|---|---|---|---|---|
| Region 1 | 84 | 57 | 27 | 7 |
| Regions 2, 3, 4, 5A, 5B, 7, 8 | 110 | 77 | 33 (18 rows) | 4 (all waiting on the rebuild or the "wrong, no fix" rule) |
| Region 6 | 158 | 139 | 19 | 4 |
| all | 352 | 273 | 79 | 15 |

## What matters, in order

Fixed or carried by tonight's rebuild (no question):
1. Shuswap closed stretch was 20-27 m long (mark 2742). Now gauge 08LC003 to DFO's other printed point, 2,075 m
   (geometry file + mark 2770, saved by Claude at the owner's request; he re-ticks it).
2. Thompson below Kamloops Lake: Deadman and Ashcroft closed areas accepted (2748, 2749), three new cuts in the rebuild.
   Juniper still needs his pick of the upper signs.
3. Both Shuswap "(middle)" places and the three Kasiks places bind nothing until their new cuts are built (in the rebuild).
4. Nanaimo 500 m cut, Puntledge tailrace, Sproat +50 m, the three Skeena cuts: in the rebuild.

New findings (questions for the owner, in OWNER-TODO.md "2026-11-15 night: second look"):
5. **"Except in those areas and times listed below" places cover nothing or too little** (Somass 0 of 6 sections,
   Stamp 0 of 10 and about 2 of 10 after the decided fix, Little Qualicum 5 of 8): a sibling whose rule holds only part of
   the year takes its water out of the rest all year. Suggested: whole river, the narrower places answering first on
   their dates (as the Nitinat is set).
6. **Cowichan and Nanaimo stretches take in side streams their words do not name** (the water is "including
   tributaries" and a blank stretch inherits it): the Sandy Pool closure lands on 541 side-stream sections incl. the
   Koksilah; Nanaimo Cedar Road to Hwy 1 on Haslam Creek; the Bore Hole closure on Stark, Berkley, Elkhorn creeks.
7. **Morice, lake outlet signs to Gosnell Creek**: the stretch starts at the Dunes signs, about 2 km below the lake; the
   top 2 km has no salmon closure.
8. **Tatshenshini headwaters (the owner's saves 2745, 2767)**: the border point also pulled the upper Tatshenshini
   mainstem into both places; read from code, to be confirmed by the side reach run.
9. **Skeena "mainstem waters only"** (pink, printed under B(i)) is drawn on the whole river (his note 2758).
10. **Skeena CNR bridge to above the Babine, sockeye**: a year-round closure and the in-season openings cover the same
    53 sections; the openings win by id order only (taken up in the cascade review).
11. Babine Lake sockeye place (~3): no extent, shows only as a card note. Kitsault tidal boundary name (note 2682) not
    done. Stamp Falls cut names (site 17, Deer Creek) not done. Campbell River mouth: three places still "unsure" from
    Oct 26. Sproat and Stamp Falls end on a "wrong" save although the fix is in the file.

Standing, already decided or known: the four "wrong, no fix" places (rule not built), Kitimat west bank whole-width
(T754 not built), Sheldens Creek (hunt report in confluence-sweep-2026-11-15.md), lake parts deferred.

Code notes for the audit tool (not data): `audit.load` keeps the last `splits.resolved.json` row per cut id, which for 32
cut ids is a braid or tributary row on another line (sections E and K can be off; the "Region 3 – Region 8 boundary"
label the owner saw on the Fraser Region 3 mainstem is likely the same lookup); section F does not model
`reach/channels.py`.

---


---

# REPORT: Region 1 DFO places: second look by eye (2026-11-15)

Read-only. Nothing in the repo was changed. Tags: [ran] = a script I ran, [read] = read in data or code,
[inferred] = my reasoning.

Inputs: `data/curated/dfo_salmon/places/region-1.json`, `data/curated/review/marks.jsonl` (2,769 lines),
`data/curated/waters/cuts.json`, atlas `data/generated/atlas/full` (splits.resolved.json, registry.json,
tidal.resolved.json), the last reach run `data/generated/reaches/full` (dfo_salmon.jsonl.gz and
dfo_salmon_diagnostic.jsonl, written 19:04 tonight, BEFORE the evening's cut moves and the Puntledge weir save), the
DFO rule rows `data/generated/regs/dfo_salmon/rules/region-1.json`, and the audit `audit/audit-final.{md,json}`.

Scripts (beside this file): `r1_digest.py` (every place with its words, extent, cut positions, every mark) and its
output `r1_digest.txt`; `r1_cuts.txt` (cut positions and records on the Region 1 rivers); `r1_file_vs_mark.py`
(file vs last save).

## (a) Counts

- Places checked: **84** (58 live, 26 dormant). 79 have a mark; 5 have none (the region default, 2 San Juan,
  Somass tributaries, Stamp tributaries). [ran]
- File vs last save: all 79 match. No place holds an extent its last save does not say. [ran]
- Clean: **57**. Candidates: **27** places (7 look wrong, 20 minor or cannot tell).
- Last save not "right": 8 places (6 "not sure", 2 "wrong"). All 8 carry a fix that is now in the file. [ran]

## (b) Candidates

| place id | DFO wording (short) | mark line(s) | in force | verdict | reason (evidence) |
|---|---|---|---|---|---|
| `1:somass-river:except-in-those-areas-and-times-li` (live) | Except in those areas and times listed below (Chinook, Coho limits) | 2700 | rest | **looks wrong** | Covers nothing: the reach run kept 0 of 6 sections [read, diagnostic]. Its siblings include the live `1:somass-river:whole` (the whole-river bait ban, June 1 to Aug 24), which takes all 6 sections [read]. The decided fix (retired siblings stop taking water) does not help: this sibling is live [read, entries.py 970-994]. Result: Somass Chinook and Coho limits show nowhere [inferred]. |
| `1:stamp-river:except-in-those-areas-and-times-li` (live) | same words, Stamp (Chinook, Coho limits) | 2454-2466 | rest | **looks wrong** | Covers nothing now (0 of 10) [read]. After the decided fix it keeps about 2 of 10 sections: Girl Guide Falls to the Stamp Falls closure, and above that closure to the Ash [inferred]. Live siblings with rules only for part of the year still take their water out all year: below Girl Guide Falls (bait ban June 1 to Aug 24 only), above the Ash (closure Jan 1 to Apr 30 only), Stamp Falls (closure to Nov 15 only) [read, rule rows]. DFO says "areas **and times**". The owner's own Nitinat note (2619) asks for the same thing: the other parts should show through, not cut holes. |
| `1:little-qualicum-river:all-waters-unless-indicated-below` (live) | all waters unless indicated below (fly only, Chinook, Coho Sep-Nov) | 2472 | rest | **looks wrong** | The park-to-hatchery stretch is closed Dec 1 to May 31 only [read]. The rest takes it out all year (kept 5 of 8) [read]. So in Sep-Nov the fly-only rule and limits do not show there [inferred]. |
| `1:cowichan-river:from-the-white-triangle-boundary-s` (live) | from the Sandy Pool sign downstream to the tidal boundary (No fishing Sep 1-Nov 15) | 2439 | downstream of the sign; tributaries blank, so the water's "true" applies | **looks wrong** | The Cowichan water record has tributaries = true (since before the review began) [read, git history]. A blank place takes the water's flag [read, entries.py 930]. The run added 541 tributary sections, including the Koksilah River, Somenos, Averill and other creeks [ran]. The Koksilah has its own DFO place and rules. The words say nothing about tributaries. |
| `1:cowichan-river:from-66-mile-trestle-downstream-to` (live) | from 66 Mile Trestle downstream to the Sandy Pool sign | 13 (unsure, Oct 26) | between trestle and sign; tributaries from the water | **looks wrong** | Same cause: 41 tributary sections added (Holt, Inwood creeks among them) [ran]. Last save is still "not sure" from Oct 26. |
| `1:nanaimo-river:from-the-upstream-side-of-the-ceda` (live) | Cedar Road Bridge up to the Hwy 1 overpass (Coho 4/day hatchery, Chum) | 2445 | between; tributaries from the water (true) | **looks wrong** | 119 tributary sections added, Haslam and North Haslam creeks among them [ran]. Haslam Creek joins about 8.0 km up, inside this stretch [inferred, its watershed code]. The words do not take tributaries. This also takes Haslam Creek out of the Nanaimo rest (coho non-retention) [read, diagnostic removed 164]. |
| `1:nanaimo-river:from-500-m-below-the-lower-power-l` (live) | 500 m below the Bore Hole power line up to Boulder Creek (No fishing all year) | 2442, 2746 | between new 500 m cut and Boulder Creek; tributaries from the water | **looks wrong** | 35 tributary sections added (Stark, Berkley, Elkhorn creeks) [ran]. A year-round closure spread onto side creeks the words do not name. Separately known: the 500 m cut is not in the atlas yet, so this place and the Nanaimo rest draw nothing until the rebuild [read]. |
| `1:campbell-river:downstream-of-the-confluence-with` (live) | downstream of the Quinsam confluence (Coho, Pink) | 11 (unsure) | downstream of Quinsam cut | cannot tell | Last save "not sure" (placement "?") on Oct 26, never re-saved [read]. Extent reads right to me [inferred]. |
| `1:campbell-river:downstream-of-the-confluence-with~2` (live) | Quinsam confluence to the Maple Street sign (Chinook TBD) | 10 (unsure) | between Quinsam (4,200 m) and Maple Street (1,920 m) | cannot tell | Same: "not sure" since Oct 26 [read]. |
| `1:campbell-river:from-a-fishing-boundary-sign-at-th` (live) | Maple Street sign downstream to the cement block sign (No fishing Aug 1-Oct 31) | 12 (unsure) | between Maple Street and cement block (1,631 m) | cannot tell | "Not sure" since Oct 26. The cement block point is an estimate: its cut note says "LOW CONFIDENCE ~300m d/s of Maple St estimate; verify pin later" [read]. That point is also the Campbell's tidal line [read, tidal.resolved]. |
| `1:stamp-river:from-the-powerline-crossing-downst` (live) | powerline crossing down about 500 m to the lagoon inlet | 2461 (unsure) | between powerline (18,871) and inlet (18,376) | minor | Last save "not sure"; its new cuts landed [read]. 495 m, matches "about 500 m" [ran]. |
| `1:cowichan-river:from-cnr-trestle-at-mile-70-2-to-s` (dormant) | CNR trestle at mile 70.2 to Skutz Falls | 2600 (unsure) | between; tributaries true | minor | Last save "not sure"; tributaries saved true though the words are silent [read]. |
| `1:sproat-river:from-sproat-lake-to-approx-300m-be` (live) | Sproat Lake to about 300 m below Hwy 4, foot of the pool | 2453, 2468 (wrong) | between lake edge and the signs cut | minor | The owner's +50 m is done in cuts.json (offset 350, about 1,874 m) but not in the atlas yet [read]. The cut's name still says "300 m" [read]. Last save is "wrong" though the file now holds its fix: if the coming "wrong = do not ship" rule ignores the fix, this live closure would vanish [inferred]. |
| `1:stamp-river:from-approx-200-m-upstream-of-the` (live) | 200 m above to 500 m below the Stamp Falls fishway (50 m below site 17; 225 m below Deer Creek) | 2455, 2469 (wrong) | between 6,688 and 5,988 | minor | Owner asked to add "site 17" and "Deer Creek" to the two cut names: **not done** (labels unchanged) [read]. Deer Creek check: its code puts its mouth at about 6,150 m, so the lower cut is about 165 m below Deer Creek, DFO says 225 m: about 60 m short [inferred; the same method puts Girl Guide Falls 282 m above Beaver Creek, DFO says 275 m]. Same "wrong but fixed" risk as Sproat. |
| `1:somass-river-tributaries:whole` (dormant) | heading "Somass River tributaries" | none | whole Somass main river, tributaries blank | minor | Owner confirmed "tributaries only"; the place is not in that form and binds the Somass main river (5 sections) [read]. Note: in that form it takes in the Stamp and Sproat rivers and everything above them [inferred, watershed codes]. Unmarked. |
| `1:stamp-river-tributaries:whole` (dormant) | heading "Stamp River tributaries" | none | whole Stamp main river | minor | Same: not the tributaries-only form; binds the Stamp main river (10 sections) [read]. Unmarked. |
| `1:san-juan-river:that-portion-of-the-san-juan-river` (dormant) | hatchery counting fence down 150 m, plus Fairy Lake tributary at the pens | none | no extent | minor | Never marked, nothing drawn [read]. Only matters if DFO brings the row back. |
| `1:san-juan-river:waters-of-the-san-juan-river-and-i` (dormant) | San Juan and tributaries below the Fleet River | none | no extent | minor | Same [read]. |
| `1:qualicum-river:all-open-portions-of-the-qualicum` (live) | All open portions (hook rule) | 2651, 2652 | other_places (union of 3 siblings) | minor | The union includes the QFN fence to Hwy 19A stretch, which is closed all year [read, rule rows]. Harmless: a hook rule painted on closed water [inferred]. Owner's doubt in 2652 ("not sure how to represent this") is answered by this form. |
| `1:little-qualicum-river:all-open-portions-of-the-little-qu` (live) | all open portions (hook rule) | 2471, 2616 | other_places | minor | Same: the union includes the hatchery fence to 35 m stretch, closed all year [read]. |
| `1:little-qualicum-river:from-the-falls-in-little-qualicum` (live) | from the falls in the park down to the hatchery fence | 2440, 2467, 2617 | park downstream boundary (12,572 m) to hatchery fence (2,918 m) | minor | Owner asked for the park line (2440) and it is built from the park area [read]. DFO says "the falls"; the curated Upper Waterfall cut is 690 m higher [read]. Kept as his choice. |
| `1:nitinat-river:downstream-of-parker-creek-except` (live) | downstream of Parker Creek, except the areas and times below | 2619 | downstream of Parker Creek, whole | cannot tell | Owner note asks that the listed parts "show through". The data cannot say: all Nitinat places have precedence 3, and showing narrower places first is app behaviour I did not check [read]. |
| `1:stamp-river:hatchery-lagoon-from-boundary-sign` (dormant) | lagoon inlet down about 250 m to the cable car crossing | 2711 | inlet to lagoon outlet (522 m) | minor | Ends at the lagoon outlet, not a cable car crossing; 522 m vs about 250 m [ran]. Old wording only. |
| `1:stamp-river:from-the-confluence-of-the-ash-riv` (dormant) | Ash confluence up to the Great Central Lake dam (no "tributaries") | 2707 | upstream of Ash; tributaries true | minor | Saved with tributaries though these words lack them; its live twin (~2) says "including tributaries" [read]. |
| `1:cowichan-river:downstream-of-the-weir-on-cowichan` (dormant) | weir on Cowichan Lake to 66 Mile Trestle | 2599 | between lake edge and trestle; tributaries true | minor | Tributaries saved true; words silent [read]. Old wording only. |
| `1:nanaimo-river:cedar-road-bridge-upstream-approx` (dormant) | Cedar Road Bridge up about 400 m to the sign near the Hwy 19 bridge | 2663 | Cedar Road to Hwy 19 bridge (257 m) | minor | Ends at the bridge, not a sign about 400 m up; same cuts as the live twin [read]. |
| `1:nanaimo-river:from-the-power-line-crossing-at-bo` (dormant) | from the Bore Hole power line up to Boulder Creek | 2443 | own extent, plus a "same as" link to the 500 m place | minor | Holds its own extent and a link; the 500 m difference is in the words, so the two are not the same stretch [read]. |

Clean, and worth saying so:
- Puntledge "from the weir": weir cut (4,001 m) to the existing 75 m signs cut (3,926 m), 75 m. No new cut; no
  `puntledge_lower_weir_d75` exists in cuts.json [read]. Mark 2750 matches.
- Puntledge marks: 1263 is a rule-book mark (`r1:puntledge_river@1-6`), not a DFO place [read]. 2447 (tailrace to
  75 m signs, 526 m in the atlas; DFO "approx. 500 m") matches; the tailrace cut is moved in cuts.json, atlas pending
  [read]. 2687 (75 m signs down to Morrison Creek, then below it) abuts the Morrison closure exactly [read].
- Nanaimo 2746: the file points at the new 500 m cut (power-line point, 500 m downstream, about 12,485 m) [read].
- Qualicum weir cut: the point is the E&N trestle and the 125 m offset lands on the weir, as DFO says [read, cut note].
  The three Qualicum stretches abut (108 / 1,179 / 1,370 m).
- Quatse: the owner's weir point is the centre of the 200 m window (2452 done) [read].
- Stamp lagoon outlet moved to 17,854 m as asked in 2460 [read].
- Somass "0.5 km" vs the cut at 1,304 m: the cut note records it as settled ("THE POINT IS CORRECT — do not move
  it"), so not listed [read].
- Courtenay: whole Courtenay River (below the Puntledge/Tsolum), 2 sections [read].
- No `between` has swapped ends, zero length, or a cut on another river [ran].

## (c) Owner notes on Region 1 places

| mark line | place | note (quoted, short) | done? |
|---|---|---|---|
| 2440, 2467 | Little Qualicum, falls to hatchery | "need to make the provincial park split split properly using the admin area … do not add the manual split" | **done**: the cut is built from the park area (parks_bc, downstream side) [read] |
| 2452 | Quatse, 100 m each side of the weir | "will need to change the proposed location to be off this point offset up and down. i added weir point" | **done** [read] |
| 2453, 2468 | Sproat, lake to below Hwy 4 | "need to add another 50m or so to the offset … should apply to all extents that use the split" | **done in cuts.json**, not in the atlas yet; name still says 300 m [read] |
| 2455, 2469 | Stamp Falls 200 m / 500 m | "add this info to each split namer (50m downstream of site 17 …) and (225m downstream of … Deer Creek) also … double check the deer creek distance" | **not done** (names); Deer Creek check done here: about 60 m short [inferred] |
| 2457 | Stamp rest | "the rest of is not working properly. it is giving errors using that op but it is the needed one." | **partly**: no error now, but it covers nothing [read] |
| 2458 | Stamp rest | "it should be rest f I think...." | done (rest is in force) [read] |
| 2460 | Stamp lagoon | "the split proposed for the outlet of lagoon at wrong spot … we need to move the outlet" | **done** (17,854 m) [read] |
| 2619 | Nitinat below Parker Creek | "need to make sure the excep tthe other listed parts shows through" | **cannot tell** from the data (app ordering) [read] |
| 2652 | Qualicum all open portions | "im not sure how to represent this it is all open so 1 put it on all the other placed places" | done as "all the other places"; the union includes a closed stretch (minor) [read] |
| 2473, 2474 | Nanaimo tidal portions | "wholly in tidal water below the Cedar Road bridge … accept both" | done (no extent; tidal) [read] |
| 2746 | Nanaimo 500 m | "cut matched to …500_m_below_power_line_at_bore_hole … saved by Claude at the owner's request" | done in the file; atlas pending [read] |

Every other Region 1 note is boilerplate ("auto-approved", "T and R not asked", "confirmed as it is").

## (d) Questions for the owner

1. **Somass and Stamp "except in those areas and times".** On the Somass, the whole-river bait ban (June 1 to
   Aug 24) takes the whole river out of "everything else", so the Chinook and Coho limits show nowhere. On the Stamp,
   even after tonight's fix, the limits will not show below Girl Guide Falls (a summer-only bait ban), above the Ash
   (a winter-only closure), or at Stamp Falls after Nov 15. Should these two be the whole river, with the smaller
   places shown on top, the way you set the Nitinat? **Recommended: yes, whole river for both.**
2. **Little Qualicum "all waters unless indicated below".** The park-to-hatchery stretch is closed only Dec 1 to
   May 31, but it is cut out all year, so the September fly-only rule and limits do not show there. Same change as
   question 1? **Recommended: yes, whole river.**
3. **Side streams on the Cowichan and Nanaimo stretches.** Both rivers are set to "including tributaries" as a whole
   water, so every stretch on them takes in its side streams. The Cowichan stretch from Sandy Pool to the sea
   (closed Sep 1 to Nov 15) lands on the whole Koksilah River, which has its own rules. The Nanaimo stretch from Cedar
   Road to Highway 1 lands on Haslam Creek. The Nanaimo year-round closure below Bore Hole lands on Stark, Berkley and
   Elkhorn creeks. DFO's words for these stretches do not mention side streams. Make each of these stretches the main
   river only? **Recommended: yes; keep side streams only on the rows that say "including tributaries".**
4. **Campbell River near the mouth.** The three stretches (Quinsam confluence, Maple Street sign, cement block sign)
   still have "not sure" as your last word, from Oct 26. The cement block sign was placed by estimate, about 300 m
   below Maple Street, and it is also where the river turns tidal. Re-check these three in the review app?
   **Recommended: keep as drawn; re-check them with the satellite view when you can.**
5. **Sproat and Stamp Falls "wrong" saves.** Both now hold the fix you asked for (Sproat +50 m; Stamp names still to
   add). Re-save them as right after the rebuild, so a "wrong means do not ship" rule cannot hide two live closures?
   **Recommended: yes.**

Not questions (to do): add "site 17" and "Deer Creek" words to the two Stamp Falls cut names; rename the Sproat cut
from "300 m" to "about 350 m, foot of the pool"; build the tributaries-only form for the Somass and Stamp
tributaries rows (it will take in the Stamp and Sproat rivers under "Somass River tributaries").

## (e) Not checked

- No map look: no cut position was checked against imagery. Distances along a river come from atlas measures, and
  the Deer Creek, Beaver Creek, Haslam Creek and Koksilah positions from watershed-code proportions [inferred].
- The reach run is from 19:04, before tonight's cut moves and the Puntledge weir save; I did not re-run it.
- How the app orders or combines two DFO places on one section (matters for Nitinat and for question 1).
- The Stotan Falls "450 m below the Duncan Bay Mainline bridge" (no bridge cut exists).
- Whether the Courtenay and Cowichan lower ends are really freshwater (Cowichan's tidal line is the map mouth).
- The rule-book (synopsis) side of Region 1, and the region default row.

---

# REPORT: DFO places, Regions 2, 3, 4, 5A, 5B, 7 and 8: second look by eye (2026-11-15)

Read-only. Nothing in the repo was changed. Every row is a candidate for the owner, not a verdict.

Tags: **[ran]** a script I ran; **[read]** read in data or code; **[inferred]** my reasoning, not proven.

Inputs: `marks.jsonl` at 2,769 lines, the place files `region-{2,3,4,5a,5b,7,8}.json`, `cuts.json`,
`data/generated/atlas/full/splits.resolved.json` (river km = route measure; higher = further upstream), the closed-area
files, and tonight's audit (`audit-final.md`). Scripts: `r28/dump.py` (every place, its extent with cut positions, every
mark and note), `r28/geo.py` (cuts along a blue line). Output of the dump: `r28/dump.txt`.

## (a) Counts [ran]

| | places |
|---|---|
| checked (live and dormant, marked or not) | **110** (R2 70, R3 10, R4 1, R5A 5, R5B 13, R7 3, R8 8) |
| of which region / page defaults (unmarked, by design) | 8 |
| clean (words, extent and last mark agree; incl. the 8 defaults and 5 deferred lake parts) | **77** |
| candidates in the table below | **33** places in 18 rows: 4 places look wrong or wait (rows 1, 2, 3, 5, plus the Shuswap closed stretch), 8 cannot tell (rows 4, 6, 13, 14, 17), 21 minor (9 of them are never-marked dormant rows) |
| bind nothing (no extent, no link) | 14: 9 dormant R2 rows never marked, 5 lake parts / Shuswap middle (below) |

## (b) Candidates

| # | place | DFO wording (short) | mark line(s) | in force now | verdict | reason, evidence |
|---|---|---|---|---|---|---|
| 1 | `8:shuswap-river#200d03a74aaf` (closed stretch) and both Shuswap middle places | closed from the water survey station … to WTFB signs one km down stream | 2742 (closed area), 2743, 2744 | `closed_areas.json` holds gauge `gauge__08LC003` (km 135.155) → `shuswap_closed_wtfb_as_printed` (km 135.134) | **looks wrong** | The closed stretch is **about 20 m long** [ran]. The owner's round-4 answer was gauge → DFO's other printed point (50°19.105' N, `shuswap_closed_station_as_printed`, km 133.08, about 2.07 km) [read OWNER-TODO]. 2742 still names the wrong end; no later closed-area mark exists [ran]. The confluence-sweep lane stopped on this (no writer) [read]. |
| 2 | `8:shuswap-river:middle-between-shuswap-falls-and-m~2` (live) and its dormant twin | (middle) between Shuswap Falls and Mabel Lake except closed … | 2743, 2744 | **no extent**: both bind nothing | looks wrong for now (waits) | Both saves ask for Wilsey Dam (km 136.50) → Mabel Lake; the dam cut is not built (audit I) [read]. Until the rebuild, the live middle Shuswap shows nothing. |
| 3 | `3:thompson-river:from-kamloops-lake-outlet-downstre~2` (live) | Kamloops Lake outlet → signs below Gold Pan; no fishing year-round in 3 areas | 2518 (live), 2521 (dormant twin, wrong), 2748, 2749 | between Kamloops Lake and Goldpan signs (km 26.05), no closed areas bind | looks wrong for now | Deadman (2748, km 117.61 → 112.99) and Ashcroft (2749, km 80.68 → 78.34) are accepted but **not in `closed_areas.json`** (the audit's "file domain is stale": `apply closed_areas` not run) and their 3 new cuts are not built [read]. Juniper has no accepted placement. Until then ~2's rules cover all three no-fishing areas. |
| 4 | `3:fraser-river:mainstem-waters-of-the-fraser-rive` (dormant) | Mainstem waters of the Fraser River | 2577 verified, **2578 wrong** | between `area:3` (Fraser km 452.70, "Region 3 – Region 5 boundary") and Spuzzum (km 196.97); tributaries blank | cannot tell | The wrong save's reason is "ours: from Region 3 – Region 8 boundary". `area:3` has many crossings; a lookup that keeps the LAST row per split id (as `audit.load`'s `split_pos` does) returns blk 356570450 at km 8.15, labelled "Region 3 – Region 8 boundary" [ran]. So the label the owner saw is very likely another water's crossing, not this extent's [inferred]. The disputed extent still ships (dormant; the owner's "not ship a disputed extent" rule is not built) [read NEXT-SESSION]. |
| 5 | `3:thompson-river:from-kamloops-lake-outlet-downstre` (dormant) | … Gold Pan Provincial Park, except: | 2520 link, **2521 wrong** | rides ~2's extent (no own extent) | looks wrong (same as #3) | A wrong last save whose older link still ships [read audit A/B]. Fixed only by #3 landing. |
| 6 | `2:fraser-river:mainstem-waters-downstream-of-the` (live) + dormant `…from-the-downstrea`, `…upstream-of-the-cp`, `…upstream-from-the-highway-1-bridge` | mainstem waters … Mission … Hope / upstream of Mission | 2568, 2574, 2575, 2576 | extents restricted to `gnis:39325` | cannot tell | Audit F says these drop Herrling, Seabird N, Jeperson and Maria Slough. But `pipeline/atlas/reach/channels.py` (commit 9d8d2f381, wired at `reach/build.py:457`) widens an item-restricted DFO extent to the side channels of that item that are in the place's water record, at resolve time [read]. The water record now holds all five [read]. So the next reach run should take them in; the audit does not model this. Whether Maria Slough and the gazetted Jeperson line pass the "takes and returns water" test needs the graph, which I did not load. |
| 7 | `2:fraser-river:from-the-downstream-side-of-the-cp~2`, `…from-the-highway-1-bridge-at-hope` (dormant) | … to the downstream side of the Alexandra Bridge | 2572, 2573 | Mission/Hope → Alexandra (km 201.24) | minor | Runs 4.3 km past Spuzzum into Region 3, while the live Region 2 place stops at Spuzzum. The words name the Alexandra Bridge, so the difference is explained [read]. Known (earlier second look, issue 5). |
| 8 | `5b:dean-river:*` (3 places using the canyon cut) | from signs ~500 m upstream of the canyon … | 2588, 2589 | `dean_river__signs_0_5_km_above_the_canyon_500_m_upstream` | minor | This one cut id is placed on **two** lines: the Dean main line at km 5.243 and a Dean side line (blk 360227736, in the Dean item) at km 0.421 [ran]. Probably a braid cut on both channels. A last-wins lookup (the audit's) sees only the side line. The canyon window on the main line (km 3.75 → 5.24, 1.5 km) fits the words [ran]. |
| 9 | `2:alouette-river-and-tributaries:downstream-of-the-216th-street-bri` | downstream of 216th to the Pitt | 2463 | downstream of km 9.04, `gnis:9630`, tributaries on | minor (done by code) | The note asks to carve the North Alouette out. No carve-out is in the file, but the North Alouette is in the water record, and the tributary walk never enters the entry's own water items (`reach/build.py:1094-1098`, `reach/covered.py`) [read]. So it is not taken as a tributary [inferred]. |
| 10 | `2:alouette-river-and-tributaries:downstream-of-a-line-between-two-f` (dormant) | downstream of a line between two signs in Allco Park | 2508 unsure | downstream of the existing Allco signs cut (km 18.20) | minor | The save also carries an unused new cut 100 m higher (km 18.30). Both Allco twins have tributaries blank under an "and tributaries" heading (audit C2) [read]. Dormant. |
| 11 | `2:kanaka-creek:downstream-of-the-112th-street-bri` + dormant twin | downstream / upstream of the 112th Street bridge | 2534, 2535 | the cut is "112 Avenue bridge" (km 7.20) | minor | Words say Street, cut says Avenue [read]. Maple Ridge has no 112th Street, so this is probably DFO's slip [inferred]. |
| 12 | `2:little-campbell-river:downstream-of-a-line-between-2-fis` | signs → pedestrian bridge at Stayte Road | 2538 | km 0.96 → 0.76; tributaries blank → water default **on** | minor | The 200 m stretch takes its tributaries through the water default; the words are silent [read]. The cuts were kept as saved (owner decision). |
| 13 | `2:chehalis-river:downstream-of-the-logging-bridge-2` | logging bridge 2.4 km downstream of Chehalis Lake | 2478 | downstream of km 19.15 | cannot tell | The repo sidecars hold the Chehalis Lake end only as a lake alias, with no km, so the 2.4 km cannot be measured [ran]. The cut's note carries a wrong candidate point (50.5° N) and the word "MYSTERY"; its anchor is 49.387° N [read]. The owner verified it. |
| 14 | `2:capilano-river:including-tributaries` | including tributaries. | 2597 | downstream of the Cable Pool footbridge (km 5.50), tributaries on | cannot tell | The words name no stretch. The extent stops at the hatchery footbridge, not the dam [read]. Probably right (above it is closed water), but the words do not say so. |
| 15 | `8:shuswap-river:lower-upstream-from-white-triangul` | upstream from signs above Mara Bridge to Mabel Lake | 2689 | Mara Bridge itself (km 24.79) → Mabel Lake | minor | The signs "above" the bridge have no offset; the cut is the bridge [read]. |
| 16 | `2:fraser-river:mainstem-waters-upstream-of-the-cp~2` (live) | mainstem waters upstream of the CPR Bridge at Mission | 2569 | tributaries blank (place and water) | minor | Blank resolves to off (`bool(None)` in `dfo_salmon/entries.py` `to_reach_input`) [read]. Same result as "off"; the audit's C3 row has no effect. |
| 17 | `3:south-thompson-river:from-the-green-can-buoy-near-outle` | From the green can buoy … including Little Shuswap Lake, to … 100 m downstream of Campbell Creek | 2579 unsure | Little Shuswap Lake (whole) + Little Shuswap outlet → km 19.64 | cannot tell (small) | Done as asked. The last save is "unsure" but its own fix is in force. I could not place the green can buoy itself; it sits in the lake, which is taken whole [inferred]. |
| 18 | 9 dormant R2 rows with no extent (Birkenhead, Brunette, Inch, Pitt ×3, Sakinaw ×2, Seymour) | various | none | nothing | minor | Never marked, so they bind nothing. If DFO lists one again, the live feed paints nothing for it [read]. |

**Lake parts (check 5)** [read]: Quesnel Lake Horsefly Bay (live and dormant), Mabel Lake (live) and Osoyoos Lake North
Basin (live, plus a dormant twin linked to it) have no extent. None binds the whole lake. This matches the deferral.

**Looked at and clean** (short): Chilliwack/Vedder group (2500-2503; ~2's "upstream" wording is the owner's reading);
the Vedder Crossing window (two lines, 0.20 km up on the Chilliwack, 12.72 km on the Vedder, bridge at 12.95) [ran];
the Stave places: ~2 has tributaries off (2672 done); its excepted Ruskin and Northrop channels stay out because they are
not in the water record; the "in that portion" place 2522 binds both channels by item (`extent.py:1052`) [read].
Squamish whole with the Powerhouse channel in the water record (2764-2766). Nicomen: Siddle km 20.56, Hwy 7 km 3.37;
"Dewdney Slough" is a name on the Nicomen item [ran]. Harrison: two places abut at the Hwy 7 cut (km 5.95). Fraser
Seton → Lillooet signs: km 329.13 → 325.51, 3.6 km, kept as saved. Quesnel: both cuts are within about 30 m of DFO's
printed coordinates [ran]. Nechako twins: the same extent. Atnarko lodge sign: kept as saved. Bella Coola: "All tributaries"
is tributaries-only with the Atnarko excepted (2730); the whole river has the Atnarko excepted (2731) [read].
Dean canyon and the Crag Creek places abut. Nicomekl and Serpentine 23 m windows. Chapman, De Boville, Serpentine,
Nicomekl 208th, Long Lake, the whole rivers.

## (c) The owner's notes (check 2)

| line | place | note (quoted) | done? | evidence |
|---|---|---|---|---|
| 2463 | Alouette, downstream of 216th | "north alouette has its own section so it should be carved out on the regs here. or make sure the more specific ones land on top of this reg" | **done by code** (no carve-out in the file) | the walk never enters the water's own North Alouette item [read] |
| 2521 | Thompson, Kamloops Lake outlet (dormant) | "… no fishing is allowed in the following areas, year-round: Deadman … Juniper … Ashcroft … need to figure out how to represent all of these" | **not done** | 2 of 3 accepted (2748, 2749), not applied to `closed_areas.json`, cuts not built; Juniper open [read] |
| 2578 | Fraser R3 mainstem | "wrong: row 2 (from — ours: from Region 3 – Region 8 boundary) · make sure that when i checked fraser as water for the mainstem items it includes all channels too. like sea bird island etc." | channels: **done by code** for Region 2 (`channels.py`); row 2: **cannot tell** (likely a label artifact, #4) | [read], [ran] |
| 2579 | South Thompson | "make sure this includes little shuswap lake" | **done** | `whole[wbk:329534136]` in the file [read] |
| 2583 / 2730 | Bella Coola "All tributaries" | "this one should be tributaries only?" | **done** | `tributaries_only: true`, Atnarko excepted [read] |
| 2586 / 2731 | Bella Coola whole | "make sure this excludes atnarko" | **done** | `tributary_excludes: [whole gnis:17209]`; an exclude blocks the stream and everything above it (`reach/build.py:1084`) [read] |
| 2672 | Stave ~2 | "tributaries set to off explicitly" | **done** | tributaries false [read] |
| 2743 | Shuswap middle ~2 | "make sure the exception worked proper and the cut on gauge side is ther gauge itself" | gauge side: **done** (it is `gauge__08LC003`). The exception: **not done** (#1, a 20 m stretch) | [ran] |
| 2744 | Shuswap middle (dormant) | "make sure closed section is good here." | **not done** (#1) | [ran] |
| 2742 | Shuswap closed area | (no note) | holds the wrong second end against the owner's round-4 answer | [read] |
| 2748, 2749 | Thompson Deadman, Ashcroft | "accepted by Claude at the owner's request … placement as proposed" | saved, **not applied** | `closed_areas.json` has the Shuswap piece only [read] |
| 2764-2766 | Squamish whole | (boilerplate, then verified) | done | water record holds `blk:360219234` [read] |

Lines 2740-2769 in my regions: 2742, 2743, 2744, 2748, 2749, 2764, 2765, 2766. All are covered above.

## (d) Questions for the owner

1. **Shuswap River below Shuswap Falls, the closed stretch.** Your saved closed stretch still runs from the water survey
   gauge to a point 20 m away, not to the other printed coordinate about 2 km downstream. You said to use the gauge and
   the other printed point. Shall we re-save it that way for you, gauge to 50°19.105' N?
   *Recommended: yes, re-save it gauge → 50°19.105' N (2.07 km).* (The confluence-sweep report asks the same.)
2. **Thompson River at Juniper Beach.** Deadman and Ashcroft are saved, but Juniper has no closed stretch yet. Which
   upper end is right: 4 km above DFO's printed lower signs, or 1.5 km below Juniper Beach Park (only 1.2 km long)?
   *Recommended: 4 km above the printed lower signs, as DFO says "approximately 4 km".* (Also asked by that report.)
3. **Fraser River, Region 3 mainstem (Spuzzum up to the Region 3 / Region 5 line).** You marked it wrong because the
   start read "Region 3 – Region 8 boundary". The start actually used is the Region 3 / Region 5 line on the Fraser. The
   label you saw most likely came from another river's crossing. Is the stretch right as drawn?
   *Recommended: keep it; re-save it as right.*
4. **Kanaka Creek bridge.** DFO says "112th Street bridge"; the only crossing is 112 Avenue. Keep the 112 Avenue bridge?
   *Recommended: keep it.*

## (e) Not checked

- No reach run, no bundle, no graph. "In force" means the place files plus `splits.resolved.json`. So I could not
  confirm which Fraser channels `channels.py` will actually take in (#6), or what any place binds on the map.
- No outside maps. Landmark positions are from repo data only (Chehalis Lake end, the Mara signs offset, the Capilano
  dam, the Deadman CN bridge, the Atnarko lodge).
- I did not check whether the "whole" rivers (Clearwater, Bridge and others) leave their region; there is no region clip on
  a water place.
- Rule-scope marks (those with a `rule_id`) and synopsis rules on these waters were not reviewed; only DFO places and
  closed areas were.
- The review app's checklist label code was not traced (#4 is inferred from the same last-wins lookup).

---

# REPORT: Region 6 DFO places: second look by eye (2026-11-15, late)

Read-only. Nothing in the repo was changed. Scripts and dumps are in `second-look/r6/`:
`dump.py` (every place, its cuts with built positions, every mark and note), `file_vs_mark.py` (file vs last save,
using `pipeline/regs/review/audit.py` loaders), `reach_r6.txt` (what each place bound in the last reach run).

Tags: **[ran]** a script I ran; **[read]** read in data or code; **[inferred]** my reading.

Sources: `region-6.json` (20:37), `marks.jsonl` (2,769 lines, 20:38), atlas `splits.resolved.json` (16:52), reach run
`reaches/full/dfo_salmon.jsonl.gz` (19:04). **The reach run is older than tonight's last edits**: Blanchard (2767), the
three Tatshenshini creeks (2745) and the Kasiks marks have not been bound by any run yet.

## (a) Counts

| | |
|---|---|
| Places checked (live + dormant + 10 defaults) | 158 |
| Clean | 139 |
| Candidates | 19 places (16 rows below) |
| Looks wrong | 4 places (Morice outlet, Blanchard, Kwatini/Stanley/Goat, Skeena "mainstem waters only") |
| Known and still not done | 3 (Sheldens, Kitimat west bank, Kitsault label) |

File vs last save [ran]: 153 of 158 match. The 5 that do not: 3 Kasiks places (new cuts not built, so no extent in
the file), Morice dormant 2596 (wrong, old extent; dormant, so it carries no rules today), Sheldens 2517 (wrong,
old extent ships).

Section defaults [ran on the reach run]: B(i) and B(ii) do not overlap and together are exactly B. B, C, D, E and F do
not overlap each other. Together they cover all of A. Each default's extent says what its words say. 15 B(i) sections
lie just outside the Region 6 outline (Skeena headwater edge; harmless).

## (b) Candidates

| Place | DFO words (short) | Mark line(s) | In force | Verdict | Why (evidence) |
|---|---|---|---|---|---|
| 6:morice-river:from-fishing-boundary-signs-posted | from signs near the outlet of Morice Lake to Gosnell Creek | 2512 | between Gosnell confluence m 75,170 and the **Dunes** signs cut m 89,755 (`..._near_outlet_of_morice_lake_2000_m_downstream`) | **looks wrong** | The place's own binding note says: start at the OUTLET signs on the lake edge, "binding it to the Dunes split would drop the top 2 km" [read]. The cut used is the Dunes cut (its `_note`: "the Dunes") [read]. Main-line sections 89,754 and 91,225 (about 2.1 km below the lake) are in no Morice reach place [ran]. DFO closes all salmon there Jan 1 to Sep 30; today it gets only the bait ban and the B(i) defaults, which leave pink open Jun 16 to Sep 30 [read untangled text + inferred]. Not in the audit. |
| 6:tatshenshini-river:upstream-of-the-bc-yukon-border-al (Blanchard) | upstream of the border along the Haines Hwy; Blanchard River | 2767 (owner) | upstream_of `border:360886456:1` on items Blanchard **+ Tatshenshini**, tributaries on | **looks wrong** [inferred] | Before 2767 it was the whole Blanchard with tributaries. The border point is on the Tatshenshini, and the Blanchard joins it in the Yukon stretch, below that point [read tatshenshini-border report]. A reach resolves on the cut's own line, so the point does not limit the Blanchard but adds the whole upper Tatshenshini mainstem, and with tributaries on, all its tributaries too (incl. Kwatini, Stanley, Goat) [read same report: "on the Tatshenshini it adds the whole upper mainstem"; extent.py `_by_measure`]. That overlaps the two sibling places. Not yet bound by a reach run. The owner said he would do the Tatshenshini himself; the editor may have added the Tatshenshini item when he picked the point. |
| 6:tatshenshini-river:upstream-of-the-bc-yukon-border-al~2 (Kwatini, Stanley, Goat) | ...; Kwatini Creek, Stanley Creek and Goat Creek. No fishing for salmon, all year | 2745 (owner) | upstream_of `border:...:1` on the 3 creeks **+ Tatshenshini** | **looks wrong** [inferred] | Before 2745 it was the 3 creeks whole. Same effect as above: the upper Tatshenshini mainstem joins this place. This place is smaller than "mainstem and all other tributaries" (~3), so it answers first there [read salmon.py `chain_order`]. The upper mainstem would read closed all year, where DFO opens coho 2 per day [read]. |
| 6:skeena-river:mainstem-waters-only | mainstem waters only (listed under B(i): pink 2/day Jun 16 to Dec 31) | 2757, 2758 | whole Skeena mainstem, tidal line to the top | **looks wrong** (small effect) | DFO lists it under the upper Skeena, B(i) [read]. It binds 217 sections = all 124 below the CNR bridge + all 93 above [ran]. The owner's note 2758 says exactly this. Below the bridge, "mainstem downstream of CNR" is smaller and has pink all year, so it answers first: no fish changes today [read chain_order + inferred]. Fix: upstream of the CNR bridge. |
| 6:babine-lake:babine-lake-excluding-tributaries~3 | Babine Lake excluding tributaries and the 400 m circles; also east of the Gullwing line. Sockeye closure + in-season openings | none (unmarked) | no extent | cannot tell | It binds nothing on the map [ran: `no_extent`]. But a place with no extent rides its whole water as a card note (salmon.py 833-849) [read]. So the lake's sockeye openings show only as a caveat on the whole Babine Lake card, never painted. The 13 closed pieces (circles, Gullwing line) are "carried, not confirmed" and also ride the whole lake as notes [ran]. Not whole-lake binding; not drawn either. |
| 6:sheldens-creek:upstream-of-white-triangular-fishi | upstream of signs at the Spur 19 Bridge site (coho 0 per day) | 2470, 2516, 2517 | upstream of m 3,402, the top 290 m | known, **not done** | Last save 2517 is wrong ("spur 19 is at completely the wrong spot?"). The old extent still ships [ran, audit B]. The owner's 11-15 answer: hunt for it, "close to tidal". No new cut or proposal exists [read: proposals folder has only the 3 Kasiks patches]. |
| 6:kitimat-river:on-west-bank-between-signs-at-kiti | on west bank between signs at the hatchery outfall. No fishing for salmon, all year | 2662 | between m 4,296 and m 5,046, whole width | known, **not done** | Ships as a whole-width closure today [read; audit H]. The owner approved the one-bank design ("Build it this way"); it is not built yet. Nakina's centre-line place is dormant (no rules today), so it does no harm now. |
| 6:kitsault-river:upstream-of-white-triangular-fishi | upstream of signs near the mouth | 2682 | whole river | note **not done** (minor) | Note: "add that there are signs at tidal boundary to boundary name". The Kitsault tidal row is still `limit: mouth`, why "the mouth", alias `kitsault_river__tidal_boundary`; no sign words anywhere [read tidal_limits.json, tidal.resolved.json]. |
| 6:skeena-river:upstream-of-cnr-bridge-at-terrace~2 and 6:skeena-river:mainstem-waters-only-upstream-of-c~2 | CNR bridge to a point above the Babine: (a) "excluding the areas below", sockeye closed all year; (b) "mainstem only", sockeye in-season openings | 2762, 2763; 2761 | both between CNR m 132,165 and gauge 08EB005 m 337,743 | cannot tell | Same 53 sections [ran]. "Excluding the areas below" works: the Kispiox zone (2) and Kitwanga stretch (3) are inside and smaller, so they answer first [ran + read]. But between (a) and (b) the tie is broken by the internal name only (`chain_order`: size, then id). "mainstem-..." sorts before "upstream-..." so the in-season openings win on their days. Right result, by alphabet [inferred]. Owner note 2762/2763: "not sure if this will be represented right". |
| 6:kasiks-river: downstream-of-fishing-boundary-sig, upstream-of-fishing-boundary-signs, upstream-of-highway-16-bridge | below / above the signs below the Upper Pool; upstream of Hwy 16 bridge | 2768, 2769, 2741 | no extent (new cuts not built) | minor (pending) | Proposals are consistent: the two sign places use one cut (m 17,653, inferred point, medium confidence), the bridge m 194 [read patches]. Until built they bind nothing and ride the whole Kasiks as card notes [read]. |
| 6:kispiox-river:downstream-of-fishing-boundary-sig | downstream of signs near Kispiox River Resort | 2680 (unsure) | downstream of m 13,691 | cannot tell | The cut is the hunt's LOW-confidence guess: no business named "Kispiox River Resort" exists today; the Kispiox River Lodge was taken [read new_splits note]. |
| 6:shegunia-river:between-fishing-boundary-signs-loc | between signs above and below the logging road bridge | 2668 (unsure) | m 3,353 to m 3,453 | minor | The bridge is exact; the 50 m either side is a guess [read]. |
| 6:lakelse-river:above-logging-road-bridge-near-the | above logging road bridge near the mouth | 2694, 2696 | upstream of m 2,005, **no item limit** | minor | 2694 held it to the Lakelse item; 2696 (last) dropped the item. The sibling "below" keeps it. Bindings still tile the river exactly: above + below = whole, no overlap [ran]. |
| 6:endako-river:whole | (whole water) No fishing for salmon | 1213 | whole | minor | DFO lists it under E, but all 29 of its sections are in the Fraser watershed (F) [ran]. Same rule as F, so no effect. |
| 6:zymoetz-river:note-section-of-river-from-highway (dormant) | "Note: section from Hwy 16 bridge down to the confluence is the Zymoetz..." | 2691 | downstream of m 595 | minor | A note became a place with an extent. Dormant, so no rules today [read]. |
| 6:nass-river:portions-from-the-canyon-city-brid | Canyon City Bridge (5.75 km below Tseax) to the water survey line (5.78 km above) | 2717 (unsure) | m 66,227 to gauge 08DB001 m 78,039 | minor | 11.81 km apart along the line vs 11.53 km by the words; the hunt note said 11.58 [ran positions + read]. Ends are right (bridge, station). Tributaries blank (audit C5). |

Not candidates, checked and fine: Bulkley "all tributaries" x2 (tributaries only: no Bulkley mainstem section; "other
than Suskwa" binds 0 Suskwa sections; ~2 binds 0 Morice or Morice Lake sections, and is a subset of ~1, 5,528 fewer
[ran]). Pallant (the bridge is now the tidal boundary; "upstream of the bridge" = the whole non-tidal creek, same 4
sections as the dormant whole [ran + read]). Exchamsiks 2728 landed (cut m 227 in the file and the atlas) [read].
Stikine and Taku notes: done by the cascade (see d). Ecstall Johnston confluence cut is a confluence-anchored cut [read].

## (c) Skeena mainstem tiling (blue line 360887278, metres from the mouth)

Tidal boundary: m 6,405. CNR bridge at Terrace (B(i) / B(ii) line): m 132,165.

| Upstream order | Place | Status | Lower end | Upper end | Sections | Comment |
|---|---|---|---|---|---|---|
| 1 | mainstem downstream of CNR bridge (B(ii)) | live | tidal line 6,405 | CNR 132,165 | 124 | the whole B(ii) mainstem |
| 2 | from Lakelse mouth to Ferry Island bridge (B(ii)) | dormant | Lakelse confluence ~108,507 (not built) | Ferry Island Hwy 16 bridge ~131,651 (not built) | 0 | ends 514 m below the CNR bridge: correct side |
| 3 | Kitsumkalum mouth: Zymagotitz to top of Hell's Gate (B(ii)) | live | Zymagotitz 115,773 | 1.5 km above Kitsumkalum 123,961 | 4 | inside 1; smaller, so it answers first |
| 4 | mainstem only, Cedarvale to CNR (B(i)) | live | CNR 132,165 | Cedarvale 208,305 | 22 | |
| 5 | mainstem only, upstream of Cedarvale (B(i)) | live | Cedarvale 208,305 | top | 71 | 4 + 5 = 7 exactly, no overlap [ran] |
| 6 | Kitwanga mouth: Mill Creek to Hwy 37 bridge (B(i)) | live | Mill Creek 227,042 | Hwy 37 bridge 229,500 | 3 | inside 5 and 8 |
| 7 | Kispiox confluence sign zone (B(i)) + 2 dormant twins | live | polygon ~284,310 | ~284,780 | 2 | inside 8 |
| 8 | CNR to a point above the Babine, "excluding the areas below" (B(i)) + dormant twin | live | CNR 132,165 | gauge 08EB005 337,743 | 53 | same 53 as 9 |
| 9 | mainstem only, CNR to a point above the Babine (B(i)) | live | CNR 132,165 | gauge 337,743 | 53 | Babine confluence ~335,400; gauge 2.3 km above it: "a point above" |
| 10 | from Babine confluence down to CNR (B(i)) | dormant | CNR 132,165 | Babine ~335,400 (not built) | 0 | |
| 11 | upstream of the CNR bridge (B(i)) | live | CNR 132,165 | top | 93 | |
| 12 | mainstem waters only (B(i)) | live | tidal line | top | 217 | = 1 + 11; should be 11 only (candidate) |

Tiling [ran]: below the bridge, 1 covers the whole mainstem; above it, 11 does. 1 and 11 do not overlap. Nothing on the
mainstem is left to the B(i)/B(ii) defaults alone. Above the gauge (337,743) the sockeye rows stop, and the B(i)
default (sockeye closed) applies. That is what the words say. Zymoetz joins at about 138,470 (the 1.5 km cut is at
139,970), above the bridge, in B(i): correct.

Skeena tributaries with places (built positions on each river's own line):

| River | Places (ends in metres from its mouth) | Comment |
|---|---|---|
| Kispiox | whole; below the resort signs 13,691 | low-confidence point |
| Kitsumkalum | whole; below the CN bridge 23 (the 22 m piece, kept); below / above the lower-canyon signs 13,076; Kitsumkalum Lake + tributaries | below-signs and below-bridge share 1 section; smaller wins |
| Kitwanga | whole + tributaries | |
| Zymoetz | whole; above Hwy 16 bridge 595; dormant note below 595 | |
| Zymagotitz | whole; above Hwy 16 bridge 778 | |
| Lakelse | whole; below CNR bridge 16,654; below / above Whitebottom Rd bridge 2,005 | above + below = whole exactly |
| Exchamsiks | whole; above Hwy 16 bridge 227 | |
| Exstew | whole; above Hwy 16 bridge 413 | |
| Kasiks | whole (dormant); above Hwy 16 bridge 194; below / above Upper Pool signs 17,653 | all pending new cuts |
| Gitnadoix | whole (live and dormant) | kept: whole river |
| Khyex | above Hwy 16 bridge 1,517 | |
| Ecstall | whole; above Johnston Creek confluence 57,303 | |
| Bulkley | below the Morice confluence 164,908 (mainstem only); Forks zone 163,908 to 165,008 + Morice 0 to 100; tributaries-only x2 | |
| Morice | whole; Forks 0 to 100; confluence 0 to Bymac 4,754; above Bymac; below Lamprey 54,533; Gosnell signs 75,070 to Lamprey; Gosnell 75,170 to Dunes 89,755 | top 2.1 km to the lake uncovered (candidate); 100 m gap at Gosnell (signs point kept by the owner) |
| Telkwa | below Howson Creek 27,872 | |
| Suskwa | whole; whole + tributaries | |
| Babine River | whole; 2 dormant pieces (Nilkitkwa 94,174; fence signs 97,396) | |
| Babine Lake | whole + tributaries; ~3 no extent; 400 m circles carried | candidate |
| Shegunia | 3,353 to 3,453 | 50 m guess |
| Fulton / Pinkut | Fulton whole; Pinkut below 1,464 (25 m below the fence at 1,489) | |

## (d) The owner's notes on Region 6 places

| Line | Place | Note (quoted) | Done? |
|---|---|---|---|
| 2470 | Sheldens Creek | "add the Spur 19 bridge cut ...; the location is upstream of it" | done then; superseded by 2517 |
| 2517 | Sheldens Creek | "i think spur 19 is at completely the wrong spot?" | **not done**: old extent ships; no new point found yet |
| 2596 | Morice (dormant, chinook-only wording) | "this one has species in location if retired it is okay but otherwise it is an issue" | done: the place is dormant (retired); dormant places carry no rules today [read salmon.py 720] |
| 2627 | Stikine | "need to make sure the tributariy tahltan river gets its own regulation over this" | done by the cascade: the Tahltan (2,595 sections) lies inside the Stikine (69,237) and the smaller place answers first [ran + read `chain_order`]. On days the Tahltan has no rule (Sep to May), the Stikine's rules apply on it: that is DFO's cascade. |
| 2629 | Taku | "make sure the tributaries in here get their own regs (nakina and tatsatua)" | done the same way: Nakina (54) and Tatsatua (887) lie inside the Taku (23,390) [ran]. Nakina's own place has tributaries blank, so Nakina's side streams take the Taku's rules [inferred]. |
| 2671 | Ecstall | "make sure this split uses the proper confluence splits" | done: the cut is anchored on the Johnston Creek confluence (m 57,303). Only its id breaks the usual `_into_` naming [read]. |
| 2682 | Kitsault | "add that there are signs at tidal bopundary to boundary name" | **not done** [read] |
| 2728 | Exchamsiks | "Hwy 16 bridge split restored" | done: cut built at m 227, in the file [read] |
| 2732, 2733 | Bulkley "all tributaries" x2 | tributaries only, the named rivers excepted | done [ran]; still saved "unsure" by Claude, so the owner has not ticked them |
| 2734, 2735 | Illiance, Wilauks | whole river from the tidal boundary | done; the owner chose "Whole river is right" |
| 2736 | Gitnadoix | unused m 0 cut dropped; whole | done |
| 2737 to 2739 | Morice Gosnell to Lamprey | re-point undone, back to the signs point | done (2739 = 2591); owner: "Keep the signs point" |
| 2738 | Morice confluence to Bymac | lower end moved from 100 m up to the confluence | done; the words name the confluence. The Forks zone (smaller) still answers on the first 100 m [read] |
| 2758 | Skeena "mainstem waters only" | "this is true but it depends which section it is in b(i) or b(ii)" | **not done**: still the whole mainstem. It is listed under B(i), so it should be upstream of the CNR bridge |
| 2762, 2763 | Skeena CNR to above Babine, "excluding the areas below" | "not sure if this will be represented right" | works today, but only by name order (see b) |

Tonight's late lines 2740 to 2769 in Region 6 carry only 2758, 2762 and 2763 as notes; the rest are the standard
"T and R not asked" text [ran].

## (e) Questions for the owner

1. **Morice River, just below Morice Lake.** DFO closes salmon from the signs at the lake outlet down to Gosnell Creek.
   Our stretch starts at the Dunes signs, about 2 km below the lake, so the top 2 km is left out. Start it at the lake
   outlet? *Recommended: yes, start at the lake.*
2. **Tatshenshini headwaters (you said you would do these).** In your saves for the Blanchard River, and for Kwatini,
   Stanley and Goat Creeks, the border point on the Tatshenshini also pulled in the upper Tatshenshini itself. So both
   places now also cover the upper Tatshenshini, and the three creeks' all-year closure would close it. Should the
   Blanchard be the whole Blanchard with its tributaries, and the three creeks be the three creeks whole, leaving the
   upper Tatshenshini to "mainstem and all other tributaries"? *Recommended: yes.*
3. **Skeena "mainstem waters only" (pink 2 per day, Jun 16 to Dec 31).** DFO prints it under the Skeena above the CNR
   bridge at Terrace. We draw it on the whole Skeena down to the tidal line. Limit it to above the CNR bridge?
   *Recommended: yes (your note said so).*
4. **Babine Lake sockeye ("excluding tributaries, the 400 m circles and east of the Gullwing Creek line").** It has no
   drawing, so the lake's sockeye openings only show as a note on the lake's card, never on the map. Draw it as the
   whole lake with tributaries out, the circles and the Gullwing line kept as their own closed pieces?
   *Recommended: yes.*
5. **Skeena from Terrace to above the Babine, sockeye.** Two DFO rows cover the same stretch: a year-round closure and
   the in-season openings. The openings win today, but only because of how their internal names sort. Is "the
   in-season openings win" right? *Recommended: yes, and we make that order explicit instead of by name.*
6. **Kitsault River tidal boundary.** You asked to say there are signs there. Name it "tidal boundary (white triangular
   fishing boundary signs near the mouth)"? *Recommended: yes.*
7. **Kispiox River, "signs near the Kispiox River Resort".** No resort by that name exists today. We put the signs at
   the Kispiox River Lodge, about 13.7 km up. Keep it? *Recommended: keep until DFO Smithers can confirm.*

Already open, not asked again: Sheldens Creek (hunt for the Spur 19 site near the tidal boundary), Kitimat west bank
and Nakina centre line (one-bank design approved), Kasiks Upper Pool (medium-confidence proposal).

## (f) What I did not check

- No reach run after 19:04: the Blanchard and three-creek findings are read from the code and the T742 report, not run.
  A reach run (human or main chat) would confirm them.
- I did not load the graph, so the uncovered top 2 km of the Morice is from section ids, not from a fresh walk.
- I did not check any cut on the ground or on imagery (Kispiox resort, Shegunia 50 m, Kitsumkalum canyon, Lakelse
  bridge, Kasiks pool).
- I did not check the 13 Babine Lake closed pieces' shapes, or the Kispiox sign-zone polygon.
- I did not check the typed rules themselves, only the untangled text summary.
- Not checked: where the Kitwanga mouth falls between Mill Creek and the Hwy 37 bridge; whether "Two Mile Creek"
  (gnis:30327) is the Bulkley's Two Mile Creek.
- Side note for the main chat (code, not data): the audit's `load()` keeps the LAST row of `splits.resolved.json` per
  cut id. For 32 cut ids that row is a braid-crossing or tributary row on another line (e.g. Cedarvale reads as
  blk 360775818 m 392, Lamprey Creek as blk 360813099 m 2,678). Checks that use measures (E, K) can be off for those.
  Also two duplicate cut pairs at one spot: Meziadin confluence (`meziadin_river__...` and `nass_river__...`, m 189,239)
  and the Skeena gauge (`skeena_river__above_babine_river` and `gauge__08EB005`, m 337,743).
