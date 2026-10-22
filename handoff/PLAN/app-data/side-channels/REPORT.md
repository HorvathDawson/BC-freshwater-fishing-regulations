# Side channels and sloughs in Conditions — investigation (2026-10-21)

THE ANSWER: the owner is right, and there are two separate defects.

DEFECT 1, THE MAIN CAUSE: `mainstem_of` adopts a reach from another river.
- Where: pipeline/atlas/gauges/build_panels.py.
- What it does: a side channel (FWA edge type 1100 or 1350) takes "the LARGEST ordinary reach within 4 hops", whatever river that reach is on. Four hops is enough to leave the channel's own river and reach the river that river flows into.
- Widgeon Slough (sid 60045, watershed code 100-025956, the Pitt's code) walks to the Pitt at hop 1 (1,181 km²) and to the Fraser at hop 2 (64,682 km², code 100). It adopts the Fraser.
- The Klesilkwa braids (sids 317336 and 317795, code 970-106243-844109) adopt the Skagit (code 970-106243).
- Province-wide, 17,946 of the 28,753 side-channel sections (62%) adopted a reach with a different watershed code from their own.
- FWA already gives a secondary channel its river's watershed code. The graph builder's own docstring says "a side channel shares the mainstem's WSC". `mainstem_of` never reads that code.
- The adopted reach's area becomes the target area. That is why the panel says "this spot drains 64,682 km²".

DEFECT 2: on a big river, the river's own far-upstream gauges outvote the gauge standing on this water.
- The lower Fraser's panel gives 08KA004 (Hansard), 08KB001 (Shelley) and 08MF005 (Hope) a third each.
- `error_for` is flat at 11.7 points for any size gap up to 10x.
- The magnitude-to-area model also compresses big rivers. Hansard is 9,246 km² in the model against 17,970 km² from ECCC; Hope is 60,356 km² against 216,600 km².
- So today the Fraser and everything that adopts it reads the 50th percentile, while Hope reads the 39th. Hansard is 8% of Hope's real catchment.

DEFECT 3, MINOR: the downstream walk passes through the Fraser into split-off pieces.
- Example: Nicomen Slough sid 59622 (1.4 km²) is described by four creeks hundreds of kilometres away.
- Rule that detects it: a reach downstream of a gauge cannot drain less than the gauge. Only 33 sections (42 km) have an upstream donor more than twice their own area.

NOT THE CAUSE:
- Tidal influence. The bundle's `tidal` table has 1 row. But Pitt near Port Coquitlam (08MH035) and Fraser at Mission (08MH024) report level only, so the Pitt's own answer today comes entirely from one tributary, 08MH006 North Alouette (26 km², 45x apart).
- Braid splits. None of these cases involve them.

PROOF THE PROTOTYPE IS THE SHIPPING BUILD:
- scripts/prototype.py reruns `build_panels.build` on the live graph.pkl, with donors rebuilt from the live bundle and HYDAT.
- Its baseline matches the live bundle's section panels on all 249,415 sections, by member set and area; 0 differ.
- Defect counts come from the live data. Today's percentiles come from the feed index of 2026-10-21, through a port of trust.ts `estimate()`.

EXAMPLES TODAY: live, then option A.
- Widgeon Slough 60045: 50th from Hansard, Shelley and Hope at 33% each, "drains 64,682 km²". Under A: 17th, the same as the Pitt on both sides.
- Pitt River:
  - 60043, the side piece where the slough leaves: 50th, from the Fraser. Under A: 17th.
  - 308351, the main channel where it rejoins: 17th from North Alouette 100%, in both.
- Klesilkwa braids 317336 and 317795: 21st from 08PA012, "Skagit River ABOVE Klesilkwa", a gauge on another river that no Klesilkwa water passes. Under A: no-record. The Klesilkwa main channel 442952 is no-record in both, because 08PA001 is quiet.
- Chilako braid 60853: 25th from the Nechako (67%, regulated). Under A: no-record, the same as the Chilako main channel 313115.
- Mess Creek braid 766463: 69th from the Stikine. Under A: no-record, the same as Mess Creek.
- Rushton Slough 60081: 36th from the Thompson's Clearwater gauge. Under A: 22nd from the North Thompson gauges.
- Gravel Slough 61198: 50th from the Fraser. Under A: no-record, the same as its main reach 235794.
- True Fraser channels (Nicomen 59618, Herrling, Annacis, Jeperson and others) already adopt the Fraser correctly. They are unchanged under A.

SHOTS (web, 420 px; my own ports 39517 and 39518; data pinned to RELEASE=r-f579fe5eac3e), all in shots/:
- widgeon-map.png: the slough is drawn teal (normal) while the Pitt on both sides is orange (low).
- widgeon.png: the Widgeon Slough panel. "About normal", drains 64,682 km², lead gauge Fraser at Hansard at 350 m³/s.
- pitt-beside-widgeon.png: the Pitt beside it. "Low", 1,181 km².
- klesilkwa-map.png, klesilkwa-braid.png, klesilkwa-main.png: the Klesilkwa map, a braid piece ("Low", drains 586 km², from 08PA012), and the main channel ("NO ESTIMATE").

SYMPTOM COUNT (a piece whose answer today is at least 25 points off its water's longest piece, or answers where that piece is silent, or the reverse):

| | sections | km | waters |
|---|---|---|---|
| live | 7,948 | 13,386 | 2,088 |
| after A | 5,587 | 11,829 | 1,523 |

OPTIONS

A (recommended): pipeline only.
- Change: in `mainstem_of`, a candidate reach must have `o.wsc == node.wsc`. Keep the "largest" rule and the 4-hop budget, so the Herrling braid stubs stay fixed. If no same-code reach exists, the channel keeps its own panel.
- 13,639 sections (7,865 km) change panel. Today:

| effect | sections |
|---|---|
| moves under 10 points | 6,852 |
| moves 10 to 25 points | 394 |
| moves 25 points or more | 193 |
| loses its answer | 3,788 |
| gains an answer | 120 |
| silent before and after | 2,292 |

- The 3,788 that lose their answer are braid pieces of creeks whose own main channel is already silent. They now say the same thing as their creek.
- A′, which gives no panel instead of the channel's own, is the same as A to within about 50 sections.
- Ships with a bundle rebuild, then a release. Tiles and app do not change.

B: A plus a ship-able link from each section to the river it follows.
- A new `section_follows` table, or a field in the paint file, so the app can say "Side channel of the Pitt River — the reading is the river's".
- Moves the same numbers as A.
- Needs a format bump, new words in core, whole-string tests and conformance updates.

C1: relative share among donors on the same river (app arithmetic in trust.ts `weightFor`, mirrored in panel.py `weight_of`).
- A same-river donor's weight is multiplied by its area divided by the largest same-river donor's area in the panel.
- Lower Fraser: 50th becomes 44th (Hope 72%, Shelley 17%, Hansard 11%).
- 925 sections (2,779 km) have this panel shape. Today no section moves 10 points or more; 298 move 5 to 10. Expect bigger moves in freshet.
- C2, keeping one same-river donor per direction, is not recommended: 1,195 more sections go silent whenever that one gauge is quiet.

D: the downstream walk must not record a node under half the gauge's area.
- 33 sections, 42 km. Ships with A.

Excluding tidal sloughs is not proposed: there is no tidal data to drive it.

## Option A — built (2026-10-21, owner decision 2026-10-22 "a is what i want. just follows its own river at that point")

THE CHANGE: one condition in `mainstem_of` (pipeline/atlas/gauges/build_panels.py). A candidate reach is adopted only
when `o.wsc == node.wsc`. A reach on another code is still walked THROUGH (the 4-hop budget and the "largest" rule are
unchanged, so the Herrling braid stubs stay fixed), it is just never adopted. No same-code reach in range → the channel
is not adopted and keeps its own panel (none where its own river is silent). B, C1, C2 and D are NOT built.

TESTS (pipeline/tests/test_build_panels.py, written first and red before the change):
- Widgeon/Pitt/Fraser shape: the slough adopts the Pitt, not the Fraser two hops on.
- Klesilkwa/Skagit shape: a braid with no same-code reach keeps its own panel; with none of its own it is silent.
- Nicomen/Herrling shape: a Fraser-coded channel still adopts the Fraser past a stub and a bigger other-code creek.
- Mutation pin: `mainstem_of`'s own source with the code condition removed adopts the Fraser and the Skagit. Separately
  checked by hand: removing the condition from the file fails 4 tests.

PROOF ON REAL DATA, against the CHANGED code (not the prototype's reimplementation):
- `prototype.py OUT baseline same_wsc` run in the change's worktree, so `baseline` calls the shipped `mainstem_of`.
- Built code vs the prototype's `same_wsc` today: 246,619 sections, 0 differ. Vs the report's earlier `same_wsc`: 0 differ.
- compare.py, old baseline (still identical to the live bundle on all 249,415 sections) against the built panels:

| | report (option A) | built |
|---|---|---|
| sections that change panel | 13,639 (7,865 km) | 13,639 (7,865 km), all side channels |
| moves under 10 points | 6,852 | 6,852 |
| moves 10 to 25 points | 394 | 394 |
| moves 25 points or more | 193 | 193 |
| loses its answer | 3,788 | 3,788 |
| gains an answer | 120 | 120 |
| silent before and after | 2,292 | 2,292 |

Named examples, live → built (feed index of 2026-10-21):
- Widgeon Slough 60045: 50th (Hansard/Shelley/Hope 33% each, 64,682 km²) → 17th, 08MH006 100%, 1,191 km². Pitt side
  piece 60043 the same; Pitt main 308351 17th in both.
- Klesilkwa braids 317336 and 317795: 21st from 08PA012 → no-record (110.6 km², the Klesilkwa main's own panel).
- Chilako braid 60853: 25th from the Nechako → no-record, the same as Chilako main 313115.
- Mess Creek braid 766463: 69th → no-record. Rushton Slough 60081: 36th → 22nd (North Thompson gauges). Gravel Slough
  61198: 50th → no-record.
- True main-river channels unchanged: of the 10,480 side channels that already adopted a same-code reach, 0 change; the
  197 Fraser-coded (wsc 100) ones among them, Nicomen 59618 included, 0 change.

APP: no app test or fixture pins a changed panel. The dev fixture bundle (app/packages/data/dev/bundle.sqlite) is its own
2,822-section set and carries none of the example sids; pipeline/tests/test_release.py builds its own synthetic panels.

SHIPS WITH: a bundle rebuild and a release (heavy, via heavy.sh). Tiles and app do not change.
