# T1199 — Reserve and park cut rule: cut at real crossings, a 50 m edge band, no piece under 100 m, a stream that never crosses is never inside

Status: OPEN — ruled by the owner; the opens list answered 2026-11-27 ("Merge with the list; close both edge stretches (Recommended)"); merged in #372 (2026-11-27); left: the deliver fix for parts named "inside" after the cut (owner 2026-11-28: one rule across the board, ## Owner), in the 0.10.0 re-cut.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1199 | Reserve and park cut rule: cut at real crossings, a 50 m edge band, no piece under 100 m, a stream that never crosses is never inside | OPEN — merged (#372); in the 0.10.0 re-cut | — | the deliver fix (`fix/T1199-reviewed-inside-after-the-cut-rule`) with the across-the-board proof; the re-cut's area-cut gate proves the two accepted calls |

Source: the owner, 2026-11-26 (planning chat), verbatim. On the cut rule: "we need to figure out a way that everything
get simplified a lot.... like the skeena river er example is crazy... we want to maybe do braid simplification here
too? also we need to have just a couple proposals with all the info and then i can select how it is right now having
all the exampels seperate makes it hard to make a decision i think we need to make like a option A-... wehere each one
is different ways we could handle it." Edge water along a reserve or park: "Closed, like a boundary river
(Recommended)". A stream beside an area that never crosses: "Never inside (Recommended)". Excursions (before the
options): "we want to use a consistent logic as other parts. be intelligent about the split then where the river is cut
will define the section that falls inside the area when cut happens it gets inside area ... we need to be smart about
this and meassure it (also if a stream never enters a eco area at all then it will never get cut and will not get
considered inside. it must cross at least once or be very close. we need to think about this. we dont want tiny
artifacts left because of the 25m or whatever distance rule." Timing: "In the 0.10.0 re-cut (Recommended)". The
option: "D: simplify the water itself" (its loop removal is the trial T1242; this ticket is the part C and D share).
Late folds: "Hold the re-cut for all of them". The notes: `cut-area-inside-check`, `cut-rule-design`, `water-options`
(option C, the shared part). Related: T1149 (the park rule reaches outside), T1176, T1240-T1242.

## The rule (the owner's rulings made exact)
1. Cut at the real crossings of the reserve or park edge. A section is inside when the cut puts it inside.
2. An outside run between two inside pieces stays inside only if it never strays more than the **50 m** edge band.
   Tails within the band are joined.
3. **No piece under 100 m.**
4. A stream that never crosses the edge is never inside, however close it runs ("it must cross at least once").
5. Water running along the edge inside the band is closed, as a boundary river is.
6. Lakes partly in a cut area stay whole; they get the chip "Partly in <Area>" (T1191). Cutting lakes is a later release.

Measured (cut-rule lane's C2 at 50 m, province-wide): about 370 section ids change; 15.4 km open today closes and 13.4
km closed today opens; 0 m of water inside a closed area is left open; 0 pieces under 100 m (163 pieces under 50 m
today); the Skeena reserve goes from 10 cuts to 13 and the river beside the islands stays closed in one piece.
Bennett Lake stays out on purpose (`outside_items`, `region-6.json`; its own open entry).

## Work
1. Build the rule in the cutter (`clean_cut.py`, where the 1,000 m rejoin is today), with the measured numbers as the
   test pins, then rerun atlas onward inside the re-cut's from-nothing build (about a lane-day).
2. The opens list: every stretch that opens (13.4 km) with its map, sent to the owner BEFORE the re-cut closes, plus the
   binding diff (AGENTS rule 63) and the section-id renumber note (~370 ids; saved spots survive by their water id).
3. The side-channel folds that depend on this rule (Skeena p4 and its kin) go through T1240.

## Acceptance
The numbers above reproduce on the build; no piece under 100 m; a never-crossing stream is outside; a binding diff
per rule; the owner has seen the opens list.

Depends: the cut rule lands before T1240's folds can be checked. Severity: medium-high (answers change on named
rivers). Model: Opus. Size: M (about one lane-day plus the build). Built by the data and release chat.

## Build notes (lane E, 2026-11-26)
- Built as `clean_cut.decide_area`; side builds pass the area-cut gate (no piece under 100 m made by an area cut).
- A graze (an inside run under 5 m) is dropped BEFORE the band joins anything: a line beside the edge stays outside.
  Gate G5 refuses a member run with no 5 m inside run, unless the run lies wholly inside (`RUN_WHOLLY_INSIDE_M`:
  FWA's 1.5-3.8 m lines between lakes in a park). G6 stops the build on an open line hugging the edge for over 100 m
  with 2+ grazes (a boundary river drawn wiggling on the edge). The pre-check finds two, both opened by the graze fix:
  the Beaverfoot in Yoho (123 m within the band, 2 grazes up to 0.3 m) and a Klanawa ER creek (103 m, 2 grazes up to
  3.3 m). Owner 2026-11-27: "Merge with the list; close both edge stretches (Recommended)"; then, for the Klanawa creek shown its picture: "Stop at 370 m". ACCEPTED `closed` in
  `waters/area_edge_calls.json`: Beaverfoot 11,976.2-12,099.2 m (123.0 m); Klanawa 318.3-370.0 m (51.7 m, as today; 370.0-421.6 m
  stays open). Pre-check G1-G6 0, no stale row; against live: opens 12.50 km in 36 stretches, closes 14.74 km in 272.
- Strathcona's rule binds 9.7 m more (Megin River ER): the floor grew the reserve's 9.7 m mouth piece to 100 m, merged
  with park water; the park's overlap membership now holds the whole piece.
- Coast: moving the closure cuts after the coast cut hid four sea ends (a channel leaving the lowest piece 5-9 km up:
  Schmidt, Lost Shoe, Camper, Clanninick). Fixed in cce1cd50a (`shore.leaves_at_mouth`): the four rows are back
  byte-identical, and 488 more lines are now recorded "not cut: the mouth is the coast crossing"; coast cuts and
  tidal water unchanged; records only (no consumer reads row counts).
- 2026-11-27 (lane L, after #372): a deliver of main stopped at the answers step: "reviewed inside parts no longer said inside" (the Gordon River in Pacific Rim, the Okanagan River Oxbows at Hayne's Lease). The 100 m floor grew their 47 m and 37 m inside pieces to 100 m (46 % and 36 % inside the polygons); the area items still hold them, but `side_channels.inside_rows` measured the shapes. Fixed: in a cut area the part's lines are measured by the cut (E30), shapes only for lakes and membership-only areas; `REVIEWED_INSIDE` unchanged. `test_t1199_inside_follows_the_cut.py`.
- 2026-11-28 (lane L, review of the fix): only the clean-cut areas follow the cut, EXACTLY (all its lines held), both ways refused by name (RULINGS E35, his two sentences); a cut place with no water holds nothing; the Skeena's side channel by the reserve takes his yes ("Side channel inside Skeena River Eco Reserve", `REVIEWED_INSIDE`). Main-stem parts and whole waters: an open owner question (release audit). The Beaverfoot's "within Yoho" on two open stretches: his pick of 11-28, a second PR.
- Follow-up: two constants are both `STUB_M` (`coast.STUB_M` 25 m, `tolerances.STUB_M` 100 m); rename one later.

## Owner

- [x] 2026-11-26 — The list of what opens (about 13.4 km) and closes (about 15.4 km), with maps, shown before it merges. Nothing that opens water merges without him. Answered 2026-11-27: "Merge with the list; close both edge stretches (Recommended)"; the Klanawa creek, after its picture: "Stop at 370 m".
  - 2026-11-27: the lane's list (26 stretches, 12.43 km open, plus 252 m on 10 lines that only graze an area; 254 stretches, 14.74 km close; figures may move up to 0.25 km) was put to him. His answer, verbatim: "Hold for the loop-removal trial". Reading: the rule and its list wait for the T1242 trial; he sees both in one look.
- [x] 2026-11-28 — Main's deliver refused after #372 (parts the review named "inside" no longer said so); the Skeena side channel at the Skeena River Eco Reserve to read "Side channel inside Skeena River Eco Reserve" → "yes but we need to make sure the rule is consistent across the board here not just a ranom special case" (2026-11-28). Left: before merge, one stated rule, a whole-release list of mismatches both ways, no per-part exception, a test.
