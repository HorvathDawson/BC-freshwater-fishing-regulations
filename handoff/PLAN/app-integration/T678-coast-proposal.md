# T678 — Tidal Coast Proposal (DRAFT, not published, not shown to the owner yet)

**Status 2026-11-12: ANSWERED** (owner, in chat). Build is ticket T685; rulings RULINGS E21–E24.

## Owner answers 2026-11-12
The owner asked twice for a single rule and a consistent definition.
1. **The rule, no DFO written boundary on the water.** (a) A water that reaches the sea is fresh to the shoreline; DFO's polygon never cuts. G1 (59) fresh; G2 (17) cuts dropped; G6 all ten estuaries E1–E10 (63) fresh to the shoreline (E1–E4 answered one by one, then "apply the single rule to all"); X2 Suter Brook Creek fresh to the shoreline (130 m drawn over the inlet is sea); X3 stays fresh; X4 loses its cut. (b) A creek entering a tidal part of a river is cut at the river's edge (polygon edge); only the piece inside the river's water is tidal; a creek entering a fresh part is untouched. G3 (18) cut at the river's edge; G4 (10) and G5 (3) keep today's cuts (they do enter tidal water and today's cut is already at the river's edge; the automatic "all fresh" reading is a flaw: it ignores the piece drawn across the river); X1, X5, X6 cut at the river's edge; X7 cut at the river's edge (168 m tidal, 53 m fresh).
2. **Cut placement question** (river polygon or FWA waterbody key): answered polygon edge; the build must confirm in code (T685).
3. **Fraser sloughs.** Same rule (b): S1, S2, S4, S5, S7, S11–S20 cut at the river's edge, slough fresh. Whole-water tidal by owner: Deas S3, Woodward S10, McDonald S8, Tamboline S9, London S6, recorded as owner `whole` rows in `data/curated/waters/tidal_limits.json` beside Gilmour, Tilbury, Williamson, Ewen.

**Status 2026-11-12: published** at https://claude.ai/artifact/2VRJAsyeeyMNmCiZpJewVA . 53 pictures are supporting files. The page carries proposals only; nothing is applied and every line stays held at today's result until the owner answers. Owner To-Do row `tidal-coast-proposal` added; the old tidal rows are marked replaced.

## What is left to do
1. Publish `index.html` as an Artifact titled "Tidal Coast Proposal" (static, icon "map"; images as supporting files `img/*.jpg`, 6.8 MB in all; load the artifact-design skill first). Put the URL where this file says PAGE_URL.
2. OWNER-TODO.md: replace the superseded tidal rows (46 estuaries; 61 + 32 held; four new cuts / Brem / Ucona; Fraser sloughs + 7 unnamed + Katzie; held-rivers page; tidal map pages) with one row pointing at the page, kept as superseded.
3. Owner To-Do artifact (collection `todo`): mark `estuaries-46-disagree`, `held-rivers-wait`, `rebuild-held-32-more`, `rebuild-new-cuts-brem-ucona`, `tidal-33-mouths`, `tidal-map-pages`, `fraser-side-channels-tidal` done with note "replaced by the Tidal Coast Proposal page" (all were version 1 when read); add `tidal-coast-proposal` (group "Look and pick", ticket T678, fields title/detail/group/ticket/order/added/done).
4. `node app/tools/check-tickets.mjs`, commit, push.

## Backup and rerun
Everything is in `/Users/dawson.horvath/bcfish-session-backup-2026-11-12/coast-proposal/` (outside the repo): `facts.py` (facts per line → `facts.json`), `build.py` (groups + pictures → `groups.json`, `img/`), `page.py` (→ `index.html`, `proposal.md`), `tiles/` (satellite tile cache), `todo-db/` (the Owner To-Do rows as read), `inputs/` (`mk.py` map helpers, `owner_list.json`, `dfo_written_boundaries.json`).
To rerun: the three scripts hard-code the old scratchpad paths at their top (`S`, `OUT`; `build.py` adds `S + "maps-1112"` to the import path for `mk.py`, and reads `S + "t678b/diff/owner_list.json"` and `S + "t665/dfo_written_boundaries.json"`). Point `OUT` at the backup folder and those three reads at `inputs/`, then from the repo root: `.venv/bin/python facts.py; .venv/bin/python build.py; python3 page.py` (pictures already in `img/` are reused; `--force` redraws; `--nomap` skips drawing). Publishing needs only `index.html` + `img/`, no rerun.

## Individual-look items
G6 cards E1–E10 (large estuaries: Bella Coola, Tzouhalem/Cowichan, Kingcome, Toba, Kemano/Wahoo, Sarita, Wakeman, Kowesas, Tsawwassen shore, Kitlope), G7 cards X1–X7 (Power House Creek, Suter Brook Creek, 360848528, 354091340, Nelson Creek, 356045296, 356349946), and the 20 slough cards S1–S20 (no answer pre-filled).

---

Page: PAGE_URL (not published yet; pictures for every card and 2 to 5 per bulk group). Written 2026-11-12 from the promoted atlas (`data/generated/atlas/full`, the second T678 candidate) and its open list (187 lines; 4 of them were the item-C lines already answered, RULINGS E20, so 183 are here). Nothing is applied: every line stays held at today's result (`data/curated/waters/shore_held.json`) until the owner answers. Scripts and pictures: backup folder above (facts.py, build.py, page.py).

How the owner answers: in chat, by group number ("G1 yes", "G2 yes except line 4", "E3 keep", "S5 tidal").

Source order applied (owner, binding): DFO written boundary, then the mouth line, DFO polygon for shading only. None of these lines has a DFO written boundary of its own.

## Summary

| Group | What | Proposed answer | Lines | Tidal today | After |
|---|---|---|---|---|---|
| G1 | Side channels of small estuaries, wholly inside DFO's polygon | Fresh to the shoreline | 59 | 21.2 km | 0.0 km |
| G2 | Small creeks in those estuaries, cut a short way up by the polygon | Drop the cut (fresh) | 17 | 1.9 km | 0.0 km |
| G3 | Creeks joining a river that is tidal by a written boundary (Fraser arms, Somass, Skeena) | Cut where the creek meets the river | 18 | 11.8 km | 8.2 km |
| G4 | Creeks joining a tidal river where the automatic reading says 'all fresh' | Keep today's cut | 10 | 6.7 km | 6.7 km |
| G5 | Three Burnaby creeks at the Fraser's North Arm | Keep today's cut | 3 | 0.5 km | 0.5 km |
| G6 | Large estuaries where DFO's polygon runs far up the river: one card each | Fresh to the shoreline, look at each | 63 | 36.1 km | 0.0 km |
| G7 | Seven odd lines: one card each | One answer per card | 7 | 0.5 km | 0.6 km |
| S1–S20 | Fraser sloughs and possible ditches, one card each | none filled in (owner's instruction) | 23 | 8.6 km | owner's |

G1–G7: 177 lines, 78.6 km tidal today, 16.0 km if every proposed answer is taken. The six Burnaby slough lines (S12–S14) are the other 6 of the 183.

## G1 — Side channels of small estuaries, wholly inside DFO's polygon

59 lines, 21.2 km tidal today, 0.0 km after.

- Question: These short channels sit in the mouth of a river whose main channel is already fresh water right to the shoreline in the live build (your mouth-line rule). They are still marked tidal only because DFO's polygon covers them. Should they follow the main channel?
- Proposed: Yes: fresh water to the shoreline, like the main channel beside them.
- Why: No DFO written boundary exists for any of them. Your order puts the mouth line next and the polygon last (shading only). Every one lies inside the mapped shoreline and each is under 1 km. To be plain about it: on the satellite pictures several of these run across tidal marsh or mud flat, so the mouth-line rule calls water fresh that the tide does reach. If you would rather keep marsh channels tidal, answer 'keep' for the group or for an estuary by name.
- For an angler: Today these channels are left off the fishing map as tidal water. After: they are drawn and carry the freshwater rules of their river and region.

Pictured on the page: 10 of 59 lines.

| No. | Water | FWA line | Tidal today (m) | Proposed (m) | Where |
|---|---|---|---|---|---|
| 1 | Adam River (whole today) | 354088630 | 125 | 0 | Adam River estuary |
| 2 | (unnamed) (whole today) | 360562880 | 148 | 0 | Ain River estuary |
| 3 | (unnamed) (whole today) | 360882432 | 390 | 0 | Apple River estuary |
| 4 | Apple River (whole today) | 360231832 | 356 | 0 | Apple River estuary |
| 5 | Apple River (whole today) | 360229891 | 353 | 0 | Apple River estuary |
| 6 | Bear River (whole today) | 360219120 | 503 | 0 | Bear River estuary |
| 7 | Bear River (whole today) | 360238946 | 225 | 0 | Bear River estuary |
| 8 | Bonsall Creek (whole today) | 354088030 | 110 | 0 | Bonsall Creek estuary |
| 9 | (unnamed) (whole today) | 354088384 | 78 | 0 | Bonsall Creek estuary |
| 10 | Brem River (whole today) | 360230804 | 230 | 0 | Brem River estuary |
| 11 | Brittain River (whole today) | 360227345 | 145 | 0 | Brittain River estuary |
| 12 | Bulson Creek (whole today) | 354087928 | 326 | 0 | Bulson Creek estuary |
| 13 | Caycuse River (whole today) | 354088977 | 295 | 0 | Caycuse River estuary |
| 14 | Cayeghle Creek (whole today) | 354088760 | 675 | 0 | Cayeghle Creek estuary |
| 15 | Cayeghle Creek (whole today) | 354087755 | 550 | 0 | Cayeghle Creek estuary |
| 16 | Cayeghle Creek (whole today) | 354088761 | 248 | 0 | Cayeghle Creek estuary |
| 17 | Franklin River (whole today) | 354088093 | 412 | 0 | Franklin River estuary |
| 18 | Franklin River (whole today) | 354088094 | 137 | 0 | Franklin River estuary |
| 19 | Gold River (whole today) | 354088092 | 551 | 0 | Gold River estuary |
| 20 | Kainet Creek (whole today) | 360227727 | 266 | 0 | Kainet Creek estuary |
| 21 | Khutze River (whole today) | 360243798 | 577 | 0 | Khutze River estuary |
| 22 | Khutze River (whole today) | 360233582 | 478 | 0 | Khutze River estuary |
| 23 | Kimsquit River (whole today) | 360225521 | 819 | 0 | Kimsquit River estuary |
| 24 | Kitimat River (whole today) | 360243518 | 561 | 0 | Kitimat River estuary |
| 25 | Kitkiata Creek (whole today) | 360222159 | 366 | 0 | Kitkiata Creek estuary |
| 26 | Kitkiata Creek (whole today) | 360227992 | 262 | 0 | Kitkiata Creek estuary |
| 27 | Kitkiata Creek (whole today) | 360227991 | 193 | 0 | Kitkiata Creek estuary |
| 28 | Kitkiata Creek (whole today) | 360227990 | 168 | 0 | Kitkiata Creek estuary |
| 29 | Kokish River (whole today) | 354087920 | 289 | 0 | Kokish River estuary |
| 30 | Kshwan River (whole today) | 360225715 | 519 | 0 | Kshwan River estuary |
| 31 | Ksi Gingolx (whole today) | 360240195 | 364 | 0 | Ksi Gingolx estuary; joins Ksi Gingolx 160 m above its written boundary (the Kincolith River bridge signs) |
| 32 | Ksi X'anmas (whole today) | 360243665 | 787 | 0 | Ksi X'anmas estuary |
| 33 | Kwatna River (whole today) | 360226459 | 758 | 0 | Kwatna River estuary |
| 34 | Lockhart Gordon Creek (whole today) | 360223100 | 157 | 0 | Lockhart Gordon Creek estuary |
| 35 | Mamin River (whole today) | 360222774 | 880 | 0 | Mamin River estuary |
| 36 | Mamin River (whole today) | 360222776 | 82 | 0 | Mamin River estuary |
| 37 | Marmot River (whole today) | 360243809 | 407 | 0 | Marmot River estuary |
| 38 | Moyeha River (whole today) | 354088814 | 414 | 0 | Moyeha River estuary |
| 39 | Moyeha River (whole today) | 360887656 | 390 | 0 | Moyeha River estuary |
| 40 | Mussel River (whole today) | 360241570 | 174 | 0 | Mussel River estuary |
| 41 | Nimpkish River (whole today) | 354088412 | 390 | 0 | Nimpkish River estuary; joins the Nimpkish 965 m above its written tidal boundary |
| 42 | Oona River (whole today) | 360238061 | 254 | 0 | Oona River estuary |
| 43 | Pa-aat River (whole today) | 360232663 | 407 | 0 | Pa-aat River estuary |
| 44 | Quaal River (whole today) | 360233191 | 396 | 0 | Quaal River estuary |
| 45 | Quaal River (whole today) | 360242778 | 394 | 0 | Quaal River estuary |
| 46 | (unnamed) (whole today) | 360458979 | 898 | 0 | Squamish estuary (unnamed channels) |
| 47 | (unnamed) (whole today) | 360752446 | 601 | 0 | Squamish estuary (unnamed channels) |
| 48 | Stagoo Creek (whole today) | 360235038 | 409 | 0 | Stagoo Creek estuary |
| 49 | (unnamed) (whole today) | 354149178 | 102 | 0 | Strandby River estuary |
| 50 | Tom Browne Creek (whole today) | 360227387 | 317 | 0 | Tom Browne Creek estuary |
| 51 | Tom Browne Creek (whole today) | 360227389 | 214 | 0 | Tom Browne Creek estuary |
| 52 | Tom Browne Creek (whole today) | 360241995 | 199 | 0 | Tom Browne Creek estuary |
| 53 | Tom Browne Creek (whole today) | 360224636 | 186 | 0 | Tom Browne Creek estuary |
| 54 | Tranquil Creek (whole today) | 354088196 | 575 | 0 | Tranquil Creek estuary |
| 55 | Tranquil Creek (whole today) | 354088197 | 144 | 0 | Tranquil Creek estuary |
| 56 | Tsowwin River (whole today) | 354088951 | 211 | 0 | Tsowwin River estuary |
| 57 | (unnamed) (whole today) | 360240745 | 247 | 0 | Unnamed creek, Grenville Channel area |
| 58 | Wathl Creek (whole today) | 360218212 | 291 | 0 | Wathl Creek estuary |
| 59 | Zeballos River (whole today) | 354087806 | 151 | 0 | Zeballos River estuary |

## G2 — Small creeks in those estuaries, cut a short way up by the polygon

17 lines, 1.9 km tidal today, 0.0 km after.

- Question: These creeks join one of the estuary channels of G1 (or the shore beside them). Today each is cut where it leaves DFO's polygon, so its lowest stretch is tidal. If the estuary channel it joins becomes fresh (G1), the cut has nothing tidal below it.
- Proposed: Drop the cut: the creek is fresh water for its whole length. (If you say no to G1 for an estuary, its creeks here keep today's cut.)
- Why: Same reasoning as G1, and your rule that a cut which makes nothing tidal is not made. Most of these pieces are under 100 m.
- For an angler: The lowest stretch of each creek (the metres shown) appears on the map as fresh water with the creek's rules.

Pictured on the page: 10 of 17 lines.

| No. | Water | FWA line | Tidal today (m) | Proposed (m) | Where |
|---|---|---|---|---|---|
| 1 | (unnamed) | 354150730 | 355 | 0 | Bonsall Creek estuary |
| 2 | (unnamed) | 354140796 | 58 | 0 | Bulson Creek estuary |
| 3 | (unnamed) | 354137987 | 38 | 0 | Bulson Creek estuary |
| 4 | Kainet Creek | 360219048 | 233 | 0 | Kainet Creek estuary |
| 5 | (unnamed) | 360854599 | 73 | 0 | Kimsquit River estuary |
| 6 | (unnamed) | 360556340 | 62 | 0 | Kimsquit River estuary |
| 7 | (unnamed) | 360857233 | 81 | 0 | Kitkiata Creek estuary |
| 8 | (unnamed) | 360868992 | 48 | 0 | Kshwan River estuary |
| 9 | (unnamed) | 360661248 | 318 | 0 | Kwatna River estuary |
| 10 | (unnamed) | 360755813 | 194 | 0 | Kwatna River estuary |
| 11 | (unnamed) | 360722750 | 155 | 0 | Kwatna River estuary |
| 12 | (unnamed) | 354144154 | 45 | 0 | Nimpkish River estuary; joins the Nimpkish side channel above |
| 13 | (unnamed) | 360703319 | 33 | 0 | Pa-aat River estuary |
| 14 | (unnamed) | 360532676 | 44 | 0 | Quaal River estuary |
| 15 | (unnamed) | 360884541 | 46 | 0 | Stagoo Creek estuary |
| 16 | (unnamed) | 360609299 | 63 | 0 | Unnamed creek, Grenville Channel area |
| 17 | (unnamed) | 360862188 | 32 | 0 | Wathl Creek estuary |

## G3 — Creeks joining a river that is tidal by a written boundary (Fraser arms, Somass, Skeena)

18 lines, 11.8 km tidal today, 8.2 km after.

- Question: Each of these creeks flows into a river that is tidal there for certain (a DFO or owner boundary upstream of the join). Both readings cut the creek, but at different spots: today's cut is where the creek leaves DFO's polygon, the proposed cut is where the creek's line enters the river's own water as the base map draws it.
- Proposed: Cut where the creek meets the river's water (the coast crossing). On most this moves the cut 50 to 100 m toward the river. On five lines that are wholly tidal today (lines marked 'whole today') only the stretch inside the river's water stays tidal.
- Why: Your order: no written boundary on the creek itself, so the mouth line decides, and here the mouth is where the creek enters the tidal river. Two lines go the other way and gain tidal water: the two Skeena side lines by about 60 m each, and line 356070273 from 6 m to 586 m (its line runs 586 m inside a Fraser arm's water).
- For an angler: A short stretch at each creek mouth switches from tidal to fresh (or the reverse on the three lines noted) and takes the creek's freshwater rules.

Pictured on the page: 5 of 18 lines.

| No. | Water | FWA line | Tidal today (m) | Proposed (m) | Where |
|---|---|---|---|---|---|
| 1 | (unnamed) (whole today) | 356035877 | 962 | 210 | unnamed line 355991973 |
| 2 | (unnamed) (whole today) | 356165637 | 885 | 235 | unnamed line 355991968 |
| 3 | (unnamed) (whole today) | 356133616 | 338 | 133 | unnamed line 355994872 |
| 4 | (unnamed) | 356210636 | 295 | 217 | unnamed line 355996907 |
| 5 | (unnamed) | 354135384 | 252 | 33 | unnamed line 354087624 |
| 6 | (unnamed) | 356070273 | 6 | 586 | unnamed line 355991973 |
| 7 | (unnamed) (whole today) | 356232808 | 1501 | 294 | Fraser River |
| 8 | Palmateer Creek | 356230481 | 590 | 523 | Fraser River |
| 9 | (unnamed) | 356214484 | 569 | 517 | Fraser River |
| 10 | (unnamed) | 356258738 | 380 | 294 | Fraser River |
| 11 | (unnamed) | 356189396 | 347 | 249 | Fraser River |
| 12 | (unnamed) | 356085631 | 324 | 267 | Fraser River |
| 13 | Barker Creek | 356274648 | 278 | 209 | Fraser River |
| 14 | (unnamed) | 360567741 | 2076 | 2141 | Skeena River |
| 15 | (unnamed) | 360786731 | 2019 | 2078 | Skeena River |
| 16 | (unnamed) (whole today) | 354147179 | 540 | 134 | Somass River |
| 17 | McCoy Creek | 354130563 | 318 | 42 | Somass River |
| 18 | (unnamed) | 356304006 | 139 | 83 | Williamson Slough |

## G4 — Creeks joining a tidal river where the automatic reading says 'all fresh'

10 lines, 6.7 km tidal today, 6.7 km after.

- Question: These lines flow into a river below that river's written tidal boundary (Nass, Campbell, Nitinat, Chemainus, the San Juan side channel), so the water they enter is tidal. The automatic coast reading still says the whole line is fresh, because it does not count the lowest stretch drawn across the river's own water.
- Proposed: Keep today's cut. The stretch drawn inside the tidal river stays tidal; the creek is fresh above it.
- Why: A creek cannot be fresh below the point where it enters tidal water. On the Nass lines the lowest 1.6 to 1.8 km of each line is drawn across the Nass's own tidal water to reach its centre line; today's cut sits at the river's edge. All pictured.
- For an angler: Nothing changes on the map.

Pictured on the page: 10 of 10 lines.

| No. | Water | FWA line | Tidal today (m) | Proposed (m) | Where |
|---|---|---|---|---|---|
| 1 | (unnamed) | 354101341 | 290 | 290 | unnamed line 354087702 |
| 2 | (unnamed) | 354100939 | 588 | 588 | Campbell River |
| 3 | Nunns Creek | 354149928 | 222 | 222 | Campbell River |
| 4 | (unnamed) | 354148021 | 27 | 27 | Chemainus River |
| 5 | (unnamed) | 360767360 | 1814 | 1814 | Nass River |
| 6 | (unnamed) | 360683185 | 1771 | 1771 | Nass River |
| 7 | (unnamed) | 360649407 | 1643 | 1643 | Nass River |
| 8 | (unnamed) | 354146575 | 147 | 147 | Nitinat River |
| 9 | (unnamed) | 354120779 | 95 | 95 | Nitinat River |
| 10 | (unnamed) | 354105314 | 56 | 56 | Nitinat River |

## G5 — Three Burnaby creeks at the Fraser's North Arm

3 lines, 0.5 km tidal today, 0.5 km after.

- Question: Boundary Creek, Fraser River Trib. 1 and Jr 1-1 come from the City of Burnaby's stream data. Each line has a connecting piece drawn across the Fraser's own water to reach the Fraser's centre line. Today that piece (130 to 200 m) is tidal and the creek above the bank is fresh.
- Proposed: Keep today's cut at the river bank.
- Why: The piece below the cut is the tidal Fraser itself. The automatic reading says 'fresh' only because it does not count a connecting piece.
- For an angler: Nothing changes on the map.

Pictured on the page: 3 of 3 lines.

| No. | Water | FWA line | Tidal today (m) | Proposed (m) | Where |
|---|---|---|---|---|---|
| 1 | Boundary Creek | -2000000122 | 200 | 200 | unnamed line 355994872 |
| 2 | Fraser River Trib. 1 | -2000000218 | 153 | 153 | unnamed line 355994872 |
| 3 | Jr 1-1 | -2000000239 | 134 | 134 | unnamed line 355994872 |

## G6 — Large estuaries where DFO's polygon runs far up the river: one card each

63 lines, 36.1 km tidal today, 0.0 km after.

- Question: Same question as G1 and G2, but each of these has a stretch over 1 km (or over 2 km in total) that is tidal today only because of the polygon. Worth a look each before applying the rule.
- Proposed: Mouth line on each (fresh to the shoreline), unless you say otherwise per card.
- Why: Your source order. They are separated only because the change is large: 10 estuaries, the tidal stretch today reaches up to 2.9 km.
- For an angler: These stretches come back onto the map as fresh water with the river's own rules. On the bigger rivers (Bella Coola, Toba, Kingcome, Kemano) that is where the river's salmon and steelhead rules would start to show.

### E1 — Bella Coola River estuary

- 15 lines, 8.8 km tidal today; the longest single stretch is 1470 m.
- The polygon runs far up the channel here, so the change is large. No DFO written boundary was found for this water. Check on the picture how much of the pink runs over tidal flat.
- Proposed: Mouth line: fresh water to the shoreline (your rule). Say 'keep' to hold today's tidal stretch, or name a spot.
- Lines: E1·1 Bella Coola River 360218496 (1470 m tidal today → 0 m); E1·2 Bella Coola River 360218497 (961 m tidal today → 0 m); E1·3 (unnamed) 360668677 (945 m tidal today → 0 m); E1·4 Bella Coola River 360218494 (900 m tidal today → 0 m); E1·5 Bella Coola River 360218495 (579 m tidal today → 0 m); E1·6 (unnamed) 360778020 (564 m tidal today → 0 m); E1·7 (unnamed) 360465739 (489 m tidal today → 0 m); E1·8 Bella Coola River 360218498 (422 m tidal today → 0 m); E1·9 Bella Coola River 360221089 (417 m tidal today → 0 m); E1·10 (unnamed) 360692196 (401 m tidal today → 0 m); E1·11 Bella Coola River 360231479 (398 m tidal today → 0 m); E1·12 (unnamed) 360814236 (386 m tidal today → 0 m); E1·13 (unnamed) 360625597 (351 m tidal today → 0 m); E1·14 Bella Coola River 360218499 (312 m tidal today → 0 m); E1·15 Bella Coola River 360221088 (186 m tidal today → 0 m)

### E2 — Tzouhalem Creek / Cowichan River estuary

- 11 lines, 5.3 km tidal today; the longest single stretch is 2266 m.
- The polygon runs far up the channel here, so the change is large. No DFO written boundary was found for this water. Check on the picture how much of the pink runs over tidal flat.
- Proposed: Mouth line: fresh water to the shoreline (your rule). Say 'keep' to hold today's tidal stretch, or name a spot.
- Lines: E2·1 Tzouhalem Creek 354153794 (2266 m tidal today → 0 m); E2·2 (unnamed) 354127315 (869 m tidal today → 0 m); E2·3 Tzouhalem Creek 354088647 (486 m tidal today → 0 m); E2·4 Tzouhalem Creek 354087619 (298 m tidal today → 0 m); E2·5 Cowichan River 354087621 (274 m tidal today → 0 m); E2·6 Cowichan River 354088549 (243 m tidal today → 0 m); E2·7 Tzouhalem Creek 354087618 (220 m tidal today → 0 m); E2·8 Koksilah River 354088058 (210 m tidal today → 0 m); E2·9 Tzouhalem Creek 354088493 (182 m tidal today → 0 m); E2·10 Tzouhalem Creek 354087620 (138 m tidal today → 0 m); E2·11 Tzouhalem Creek 354088492 (106 m tidal today → 0 m)

### E3 — Kingcome River estuary

- 5 lines, 4.7 km tidal today; the longest single stretch is 2882 m.
- The polygon runs far up the channel here, so the change is large. No DFO written boundary was found for this water. Check on the picture how much of the pink runs over tidal flat.
- Proposed: Mouth line: fresh water to the shoreline (your rule). Say 'keep' to hold today's tidal stretch, or name a spot.
- Lines: E3·1 Kingcome River 360214183 (2882 m tidal today → 0 m); E3·2 Kingcome River 360214188 (1042 m tidal today → 0 m); E3·3 (unnamed) 360335466 (482 m tidal today → 0 m); E3·4 (unnamed) 360837032 (289 m tidal today → 0 m); E3·5 (unnamed) 360626810 (54 m tidal today → 0 m)

### E4 — Toba River estuary

- 7 lines, 4.7 km tidal today; the longest single stretch is 2753 m.
- The polygon runs far up the channel here, so the change is large. No DFO written boundary was found for this water. Check on the picture how much of the pink runs over tidal flat.
- Proposed: Mouth line: fresh water to the shoreline (your rule). Say 'keep' to hold today's tidal stretch, or name a spot.
- Lines: E4·1 Toba River 360883346 (2753 m tidal today → 0 m); E4·2 Toba River 360242728 (552 m tidal today → 0 m); E4·3 (unnamed) 360590016 (446 m tidal today → 0 m); E4·4 (unnamed) 360703983 (425 m tidal today → 0 m); E4·5 (unnamed) 360557652 (389 m tidal today → 0 m); E4·6 (unnamed) 360829432 (74 m tidal today → 0 m); E4·7 (unnamed) 360746420 (67 m tidal today → 0 m)

### E5 — Kemano River / Wahoo Creek estuary

- 6 lines, 3.0 km tidal today; the longest single stretch is 2364 m.
- The polygon runs far up the channel here, so the change is large. No DFO written boundary was found for this water. Check on the picture how much of the pink runs over tidal flat.
- Proposed: Mouth line: fresh water to the shoreline (your rule). Say 'keep' to hold today's tidal stretch, or name a spot.
- Lines: E5·1 Kemano River 360222049 (2364 m tidal today → 0 m); E5·2 (unnamed) 360387161 (334 m tidal today → 0 m); E5·3 (unnamed) 360570734 (125 m tidal today → 0 m); E5·4 (unnamed) 360592791 (108 m tidal today → 0 m); E5·5 (unnamed) 360643052 (76 m tidal today → 0 m); E5·6 Wahoo Creek 360887120 (40 m tidal today → 0 m)

### E6 — Sarita River estuary

- 7 lines, 2.7 km tidal today; the longest single stretch is 1126 m.
- The polygon runs far up the channel here, so the change is large. No DFO written boundary was found for this water. Check on the picture how much of the pink runs over tidal flat.
- Proposed: Mouth line: fresh water to the shoreline (your rule). Say 'keep' to hold today's tidal stretch, or name a spot.
- Lines: E6·1 Sarita River 354088014 (1126 m tidal today → 0 m); E6·2 Sarita River 354088012 (959 m tidal today → 0 m); E6·3 Sarita River 354088556 (231 m tidal today → 0 m); E6·4 Sarita River 354088013 (197 m tidal today → 0 m); E6·5 Sarita River 354088011 (97 m tidal today → 0 m); E6·6 Sarita River 354088015 (73 m tidal today → 0 m); E6·7 (unnamed) 354133310 (35 m tidal today → 0 m)

### E7 — Wakeman River estuary (unnamed channels)

- 6 lines, 2.3 km tidal today; the longest single stretch is 976 m.
- The polygon runs far up the channel here, so the change is large. No DFO written boundary was found for this water. Check on the picture how much of the pink runs over tidal flat.
- Proposed: Mouth line: fresh water to the shoreline (your rule). Say 'keep' to hold today's tidal stretch, or name a spot.
- Lines: E7·1 (unnamed) 360233887 (976 m tidal today → 0 m); E7·2 (unnamed) 360229557 (458 m tidal today → 0 m); E7·3 (unnamed) 360233888 (369 m tidal today → 0 m); E7·4 (unnamed) 360233886 (335 m tidal today → 0 m); E7·5 (unnamed) 360229558 (114 m tidal today → 0 m); E7·6 (unnamed) 360233884 (71 m tidal today → 0 m)

### E8 — Kowesas River estuary

- 2 lines, 1.7 km tidal today; the longest single stretch is 1276 m.
- The polygon runs far up the channel here, so the change is large. No DFO written boundary was found for this water. Check on the picture how much of the pink runs over tidal flat.
- Proposed: Mouth line: fresh water to the shoreline (your rule). Say 'keep' to hold today's tidal stretch, or name a spot.
- Lines: E8·1 Kowesas River 360223581 (1276 m tidal today → 0 m); E8·2 Kowesas River 360223582 (396 m tidal today → 0 m)

### E9 — Tsawwassen shore (unnamed channels)

- 2 lines, 1.5 km tidal today; the longest single stretch is 1237 m.
- The polygon runs far up the channel here, so the change is large. No DFO written boundary was found for this water. Check on the picture how much of the pink runs over tidal flat.
- Proposed: Mouth line: fresh water to the shoreline (your rule). Say 'keep' to hold today's tidal stretch, or name a spot.
- Lines: E9·1 (unnamed) 356265269 (1237 m tidal today → 0 m); E9·2 (unnamed) 356193418 (271 m tidal today → 0 m)

### E10 — Kitlope River estuary

- 2 lines, 1.3 km tidal today; the longest single stretch is 1101 m.
- The polygon runs far up the channel here, so the change is large. No DFO written boundary was found for this water. Check on the picture how much of the pink runs over tidal flat.
- Proposed: Mouth line: fresh water to the shoreline (your rule). Say 'keep' to hold today's tidal stretch, or name a spot.
- Lines: E10·1 Kitlope River 360227034 (1101 m tidal today → 0 m); E10·2 (unnamed) 360828673 (169 m tidal today → 0 m)

## G7 — Seven odd lines: one card each

7 lines, 0.5 km tidal today, 0.6 km after.

- Question: Lines where the automatic readings disagree with each other or find nothing to measure.
- Proposed: One proposed answer per card.
- Why: Each card says what the data shows.
- For an angler: Small: a stretch under 200 m at each, except Suter Brook Creek (130 m becomes tidal).

### X1 — Power House Creek (Fraser North Arm, Burnaby)

- Line 5220 m long. Today cut at 165 m.
- The two automatic crossings disagree: the base map's own crossing is at 175 m, the shoreline crossing at 111 m (64 m apart, over the 50 m tolerance).
- It joins the Fraser's North Arm, tidal by your boundary.
- Proposed: Keep today's cut at 165 m (it sits between the two readings, 10 m from the base map's crossing).
- Lines: X1·1 Power House Creek 356268746 (165 m tidal today → 165 m)

### X2 — Suter Brook Creek (Port Moody, Burrard Inlet)

- Line from Port Moody's stream data, 1,639 m. Fresh for its whole length today.
- The shoreline crossing finds the lowest 130 m of the line in the sea; the base map has no crossing for an added line. 205 m lies inside DFO's polygon.
- Proposed: Cut at 130 m: the lowest 130 m is drawn over the inlet's tidal flat.
- Lines: X2·1 Suter Brook Creek -2200000388 (0 m tidal today → 130 m)

### X3 — Unnamed river 360848528 (far north-west, Alaska border)

- 52 km line at 59.20 N, 135.29 W. Fresh for its whole length today.
- The shoreline reading calls its lowest 36 km 'sea'. That stretch lies outside B.C., in Alaska, where the base map has no land. The line does end at salt water, but in Alaska; no B.C. tidal water is involved.
- Proposed: Leave fresh. Nothing to cut. (The picture shows only the lowest stretch.)
- Lines: X3·1 (unnamed) 360848528 (0 m tidal today → 0 m)

### X4 — Unnamed creek joining Tzouhalem Creek (Cowichan Bay)

- 487 m line. Today cut at 99 m, where it leaves DFO's polygon.
- It joins Tzouhalem Creek 2 km up, inside the stretch that is tidal today. The automatic reading finds no tidal water at its end because it reads Tzouhalem Creek as fresh there.
- Follows card E2 (Tzouhalem / Cowichan): if E2 becomes fresh, this cut has nothing tidal below it.
- Proposed: Follow E2: drop the cut if E2 goes to the mouth line, keep it if E2 is kept.
- Lines: X4·1 (unnamed) 354091340 (99 m tidal today → 0 m)

### X5 — Nelson Creek (joins the Brunette River, Coquitlam / Burnaby)

- 3207 m line. Today cut at 38 m, where it leaves DFO's polygon.
- It joins the Brunette River 1898 m above the Brunette's mouth on the Fraser. The Brunette is tidal there by your own boundary on it.
- The automatic reading finds no tidal water at the creek's end.
- Proposed: Keep today's cut: the creek enters tidal water.
- Lines: X5·1 Nelson Creek 356013125 (38 m tidal today → 38 m)

### X6 — Unnamed creek joining the Brunette River

- 1296 m line. Today cut at 34 m, where it leaves DFO's polygon.
- It joins the Brunette River 2737 m above the Brunette's mouth on the Fraser. The Brunette is tidal there by your own boundary on it.
- The automatic reading finds no tidal water at the creek's end.
- Proposed: Keep today's cut: the creek enters tidal water.
- Lines: X6·1 (unnamed) 356045296 (34 m tidal today → 34 m)

### X7 — Unnamed line 356349946 (Ladner marsh, one of the 'four new tide cuts')

- 221 m line; today its lowest 195 m is tidal, leaving 26 m fresh at the top.
- It joins an unnamed Fraser side channel (355991964) that was dropped from the network as a braid loop, so the automatic reading has nothing tidal to attach it to and says 'fresh'.
- It lies below the Fraser's tidal boundary, 168 m of it inside the Fraser's own water.
- Proposed: Make the whole line tidal (the 26 m at the top is close to your 25 m stub rule and the rest is in the Fraser's tidal water).
- Lines: X7·1 (unnamed) 356349946 (195 m tidal today → 221 m)

## S1–S20 — Fraser sloughs and possible ditches (one card each, nothing pre-filled)

Owner ruling: sloughs are not covered by the Fraser channels rule; each is decided on its own. Each stays as the live build has it until answered.

### S1 — Cohilukthan Slough (356285962)

- Length on the map: 4,998 m. DFO's polygon covers 884 m of it.
- Today in the live build: tidal for the lowest 27 m (up to where the line leaves the river's water or DFO's polygon), fresh above.
- Only its lower end meets tidal water; the upper end is inland.
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: culvert (FISS record).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

### S2 — Crescent Slough (356326465)

- Length on the map: 4,545 m. DFO's polygon covers 27 m of it.
- Today in the live build: tidal for the lowest 17 m (up to where the line leaves the river's water or DFO's polygon), fresh above.
- Only its lower end meets tidal water; the upper end is inland.
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: dam (FISS record).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

### S3 — Deas Slough (356358883)

- Length on the map: 8,318 m. DFO's polygon covers 2,935 m of it.
- Today in the live build: tidal for the lowest 2,907 m (up to where the line leaves the river's water or DFO's polygon), fresh above.
- Only its lower end meets tidal water; the upper end is inland.
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: none in the provincial obstacle, dam and crossing records (these records miss most floodgates and pump stations, so this does not show it is open).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

### S4 — Gundersen Slough (356286798)

- Length on the map: 2,182 m. DFO's polygon covers 1,489 m of it.
- Today in the live build: tidal for the lowest 221 m (up to where the line leaves the river's water or DFO's polygon), fresh above.
- Only its lower end meets tidal water; the upper end is inland.
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: none in the provincial obstacle, dam and crossing records (these records miss most floodgates and pump stations, so this does not show it is open).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

### S5 — Katzie Slough (off the Pitt River) (356339436)

- Length on the map: 9,972 m. DFO's polygon covers 406 m of it.
- Today in the live build: tidal for the lowest 296 m (up to where the line leaves the river's water or DFO's polygon), fresh above.
- Only its lower end meets tidal water; the upper end is inland.
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: bridge (PSCIS record); concrete box culvert (PSCIS record); round culvert (PSCIS record).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

### S6 — London Slough (356346058)

- Length on the map: 2,598 m. DFO's polygon covers 600 m of it.
- Today in the live build: tidal for the lowest 575 m (up to where the line leaves the river's water or DFO's polygon), fresh above.
- Only its lower end meets tidal water; the upper end is inland.
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: none in the provincial obstacle, dam and crossing records (these records miss most floodgates and pump stations, so this does not show it is open).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

### S7 — Matsqui Slough (356310903)

- Length on the map: 4,161 m. DFO's polygon covers 420 m of it.
- Today in the live build: tidal for the lowest 320 m (up to where the line leaves the river's water or DFO's polygon), fresh above.
- Only its lower end meets tidal water; the upper end is inland.
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: dam (FISS record).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

### S8 — McDonald Slough (356248171)

- Length on the map: 6,984 m. DFO's polygon covers 760 m of it.
- Today in the live build: tidal for the lowest 714 m (up to where the line leaves the river's water or DFO's polygon), fresh above.
- Only its lower end meets tidal water; the upper end is inland.
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: none in the provincial obstacle, dam and crossing records (these records miss most floodgates and pump stations, so this does not show it is open).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

### S9 — Tamboline Slough (356165068)

- Length on the map: 2,373 m. DFO's polygon covers 701 m of it.
- Today in the live build: tidal for the lowest 678 m (up to where the line leaves the river's water or DFO's polygon), fresh above.
- Only its lower end meets tidal water; the upper end is inland.
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: none in the provincial obstacle, dam and crossing records (these records miss most floodgates and pump stations, so this does not show it is open).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

### S10 — Woodward Slough (356303771)

- Length on the map: 3,374 m. DFO's polygon covers 426 m of it.
- Today in the live build: tidal for the lowest 409 m (up to where the line leaves the river's water or DFO's polygon), fresh above.
- Both ends meet tidal water (DFO's polygon).
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: none in the provincial obstacle, dam and crossing records (these records miss most floodgates and pump stations, so this does not show it is open).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

### S11 — Cranberry Slough (off the Pitt River) (356288238)

- Length on the map: 7,122 m. DFO's polygon covers 181 m of it.
- Today in the live build: fresh for its whole length.
- Its mapped lower end does not touch DFO's polygon.
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: bridge (PSCIS record); concrete box culvert (PSCIS record); round culvert (PSCIS record).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

### S12 — Salmon Slough (Burnaby) (-2000000292, -2000000295, -2000000296, -2000000299)

- Length on the map: 1,331 m (4 lines). DFO's polygon covers 432 m of it.
- Today in the live build: tidal for the lowest 432 m (up to where the line leaves the river's water or DFO's polygon), fresh above.
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: none in the provincial obstacle, dam and crossing records (these records miss most floodgates and pump stations, so this does not show it is open).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

### S13 — Sanctuary Slough (Burnaby) (-2000000302)

- Length on the map: 753 m. DFO's polygon covers 112 m of it.
- Today in the live build: tidal for the lowest 112 m (up to where the line leaves the river's water or DFO's polygon), fresh above.
- Only its lower end meets tidal water; the upper end is inland.
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: none in the provincial obstacle, dam and crossing records (these records miss most floodgates and pump stations, so this does not show it is open).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

### S14 — Swing Bridge Slough (Burnaby) (-2000000373)

- Length on the map: 569 m. DFO's polygon covers 111 m of it.
- Today in the live build: tidal for the lowest 111 m (up to where the line leaves the river's water or DFO's polygon), fresh above.
- Only its lower end meets tidal water; the upper end is inland.
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: none in the provincial obstacle, dam and crossing records (these records miss most floodgates and pump stations, so this does not show it is open).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

### S15 — Unnamed line 356152742 (possible ditch) (356152742)

- Length on the map: 1,049 m. DFO's polygon covers 274 m of it.
- Today in the live build: tidal for the lowest 125 m (up to where the line leaves the river's water or DFO's polygon), fresh above.
- Only its lower end meets tidal water; the upper end is inland.
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: none in the provincial obstacle, dam and crossing records (these records miss most floodgates and pump stations, so this does not show it is open).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

### S16 — Unnamed line 356227245 (possible ditch) (356227245)

- Length on the map: 2,016 m. DFO's polygon covers 316 m of it.
- Today in the live build: tidal for the lowest 228 m (up to where the line leaves the river's water or DFO's polygon), fresh above.
- Only its lower end meets tidal water; the upper end is inland.
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: none in the provincial obstacle, dam and crossing records (these records miss most floodgates and pump stations, so this does not show it is open).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

### S17 — Unnamed line 356232974 (possible ditch) (356232974)

- Length on the map: 1,526 m. DFO's polygon covers 346 m of it.
- Today in the live build: tidal for the lowest 214 m (up to where the line leaves the river's water or DFO's polygon), fresh above.
- Only its lower end meets tidal water; the upper end is inland.
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: none in the provincial obstacle, dam and crossing records (these records miss most floodgates and pump stations, so this does not show it is open).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

### S18 — Unnamed line 356249719 (possible ditch) (356249719)

- Length on the map: 1,080 m. DFO's polygon covers 701 m of it.
- Today in the live build: tidal for the lowest 429 m (up to where the line leaves the river's water or DFO's polygon), fresh above.
- Only its lower end meets tidal water; the upper end is inland.
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: none in the provincial obstacle, dam and crossing records (these records miss most floodgates and pump stations, so this does not show it is open).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

### S19 — Unnamed line 356321196 (possible ditch) (356321196)

- Length on the map: 7,927 m. DFO's polygon covers 530 m of it.
- Today in the live build: tidal for the lowest 428 m (up to where the line leaves the river's water or DFO's polygon), fresh above.
- Only its lower end meets tidal water; the upper end is inland.
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: none in the provincial obstacle, dam and crossing records (these records miss most floodgates and pump stations, so this does not show it is open).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

### S20 — Unnamed line 356330529 (possible ditch) (356330529)

- Length on the map: 861 m. DFO's polygon covers 479 m of it.
- Today in the live build: tidal for the lowest 362 m (up to where the line leaves the river's water or DFO's polygon), fresh above.
- Only its lower end meets tidal water; the upper end is inland.
- Its mouth is downstream of Mission, the Fraser's tidal boundary.
- Barriers on record within 60 m of the line: none in the provincial obstacle, dam and crossing records (these records miss most floodgates and pump stations, so this does not show it is open).
- DFO wording: none found in DFO's tidal-area pages.
- Answer: (owner's, not filled in)

Not asked: Gilmour, Tilbury, Williamson sloughs (the owner's own whole-water tidal rows), Ewen Slough (owner A, 2026-11-11). Sloughs the tidal polygon does not touch (up the Pitt; above Mission) are fresh and not asked.

## What this replaces, and what does not reconcile

- **The 187 open lines:** The build's open list had 187. Four were the item-C lines you answered today (drop the cut, ruling E20); the fifth item-C line, the San Juan side channel, already has its written boundary. That leaves 183, all on this page: 177 in G1 to G7 and 6 Burnaby slough lines in S12 to S14.
- **"46 estuaries where the mouth methods disagree":** Replaced by G1, G2 and G6. The 46 was a count from the earlier trial that compared three ways of drawing a mouth line. The build uses a different test (DFO's polygon against the mapped shoreline) and finds 150 lines in 50 estuaries. I cannot match the old 46 to the new 50 one for one; the old list was never written out per estuary.
- **"61 held rivers" and "32 more held (93 in all)":** Replaced. Of the 93: 64 have no tidal water in the live build because the main river is now fresh to its mouth under your mouth-line rule (Kwatna, Quaal, Bella Coola, Khutze, Kemano, Kitimat and the others named in that row); 18 are cut where they meet tidal water (seven of them are sloughs or possible ditches and are S cards here); 8 keep a cut you already ruled on (the four item-C lines and the four creek pieces); 3 are still open and are in G4 (Nunns Creek, 354100939, 354148021). What is left of the old question is the side channels of those estuaries, in G1 and G6. Kainet Creek's two lines are G1 and G2 lines. The old held-rivers page and its cards are retired.
- **"Four new tide cuts, Brem not cut, Ucona 33 m":** 354135384 and 356070273 are lines in G3; Crescent Slough is card S2; 356349946 is card X7. Brem: the main river is fresh to its mouth and its one side line is in G1, so there is nothing separate to look at. Ucona: you answered it on 12 November (fine).
- **"Fraser sloughs one by one, plus 7 unnamed possible ditches and Katzie Slough":** Replaced by the S cards. Six of the seven unnamed lines have a card; the seventh, 356341234, was one of the item-C lines answered today.
- **Pictures:** Every card (E, X, S) has its own picture. The bulk groups show examples only: 3 estuaries each for G1 and G2, 4 pictures for G3; G4 and G5 are pictured in full. X3's picture shows only the lowest stretch of a 52 km line. No line was impossible to draw.
