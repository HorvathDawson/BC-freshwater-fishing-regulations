# T1199 — Reserve and park cut rule: cut at real crossings, a 50 m edge band, no piece under 100 m, a stream that never crosses is never inside

Status: OPEN — ruled by the owner; built by the data and release chat inside the 0.10.0 re-cut.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1199 | Reserve and park cut rule: cut at real crossings, a 50 m edge band, no piece under 100 m, a stream that never crosses is never inside | OPEN — in the 0.10.0 re-cut (owner 2026-11-26); owner: see ## Owner | — | build the rule in the cutter (`clean_cut.py`), then the opens list to the owner before the re-cut closes |

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
- Built as `clean_cut.decide_area` (commits 93b42f7ea, fccc926d5, 04b1ef0ff, and the review fix after them). On the
  side build: 1,151 closure cuts; no piece under 100 m an area cut made; the area-cut gate passes.
- Review fix: a run inside under 5 m (a graze) is dropped BEFORE the band joins anything, so a line beside the edge
  that grazes it stays outside. The gate's G5 now refuses a member run with no inside run of 5 m.
- `strathcona_park_waters.r1` binds 9.7 m more (Megin River ER, blk 354092551). The stage move does not cause this;
  the floor does. The reserve's 9.7 m mouth piece (inside the reserve, outside Strathcona Park) grew to 100 m, so it
  merged with 90.3 m of park water. The park's overlap membership (more than 1 m inside) then holds the whole 100 m
  piece, and the reserve's closure covers it too.
- Moving the closure cuts after the coast cut changed no coast cut. The 170 coast cuts are identical, and so is the
  tidal water: the section 354101341 0-290 was two sections, now one. `shore_cuts.json` drops four "not cut, the
  mouth is the coast crossing" rows (Schmidt, Lost Shoe, Camper and Clanninick creeks); the cause was not traced.

## Owner

- [ ] 2026-11-26 — The list of what opens (about 13.4 km) and closes (about 15.4 km), with maps, shown before it merges. Nothing that opens water merges without him.
