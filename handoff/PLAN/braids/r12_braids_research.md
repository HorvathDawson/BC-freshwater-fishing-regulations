# R12 item 5: braids that straddle a cut (research only)

2026-10-16. Owner decision: keep majority-side placement for now (RULINGS E15). Nothing in the pipeline was changed.
Data: atlas/full (2026-10-10), live reach run + bundle `section_straddle` (38 rows / 18 sids), and the R10
subgraph dump `data/generated/scratch_r10/braids_sub.pkl`. A census of the whole atlas ran once through heavy.sh.
Scripts, JSON and sketches are in `handoff/PLAN/r12_braids/`. The recomputed E15 shares match all 38 bundle rows
to within 0.01.

## The 18 braids (21 braid × cut pairs; Adams and Peace span two cuts)

Offset is the braid's distance from the main line. "Snap" is how far the cut would have to move for the braid
(piece) or its whole channel network to lie on one side. The tangent columns give the number of times the
perpendicular hits the braid at ±50/±100/±200 m, and where it cuts, in metres from the braid's lower end.

| # | braid | river | len m | off max/mean | cut | share above (proj.) | snap piece/network | tangent hits | split at (±50; ±100; ±200) | spread m |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 355996658:0 | Adams | 2845 | 360/189 | signs 200 m below platforms | 0.34 | 714/714 | 1/1/1 | 1638; 1743; 1899 | 260 |
| 2 | 〃 | Adams | | | signs 200 m above platforms | 0.14 | 314/314 | 1/1/1 | 2454; 2460; 2466 | 11 |
| 3 | 360237278:0 | Atnarko | 1582 | 350/193 | Goat Creek confluence | 0.05 | 46/46 | 1/1/1 | 1509; 1515; 1515 | 6 |
| 4 | 360225339:0 | Atnarko | 215 | 53/35 | Atnarko Park campsite signs | **0.00** | **0/0** | 1/0/0 | 199; –; – | – |
| 5 | 360239390:0 | Bulkley | 403 | 158/85 | Moricetown Canyon +100 m | 0.17 | 49/50 | 1/1/1 | 299; 312; 335 | 36 |
| 6 | 359002043:0 | Chowade | 1574 | 556/370 | Horseshoe Road Bridge | 0.45 | 759/759 | 1/1/1 | 844; 840; 752 | 91 |
| 7 | 359006635:0 | Chowade | 1016 | 331/199 | Horseshoe Road Bridge | 0.59 | 253/292 | 1/1/1 | 432; 434; 470 | 38 |
| 8 | 360218590:0 | Chuckwalla | 657 | 206/125 | Ten Mile Pool signs (200 m dn) | 0.14 | 29/29 | 1/1/1 | 567; 556; 532 | 35 |
| 9 | 360227736:0 | Dean | 645 | 134/96 | signs 0.5 km above canyon | 0.31 | 177/363 | 1/1/1 | 426; 421; 418 | 8 |
| 10 | 360227738:0 | Dean | 1976 | 489/390 | signs 0.5 km above canyon | 0.91 | 4/363 | **0/0/0** | – (wholly above) | – |
| 11 | 360213366:0 | Morice | 2400 | 626/226 | Lamprey Creek confluence | 0.81 | 318/854 | 1/1/1 | 446; 446; 465 | 19 |
| 12 | 360213368:0 | Morice | 1392 | 699/534 | Lamprey Creek confluence | 0.06 | 43/854 | 1/1/1 | 1314; 1314; 1357 | 43 |
| 13 | 359005110:0 | Peace | 5935 | 1009/655 | 1,200 m signs below Site C | 0.40 | 1563/1563 | 1/1/1 | 3373; 3455; 3551 | 178 |
| 14 | 〃 | Peace | | | Site C dam | 0.11 | 363/363 | 1/1/1 | 5294; 5294; 5286 | 8 |
| 15 | 359013560:0 | Peace | 2885 | 352/268 | 1,200 m signs | 0.94 | 71/72 | 1/1/1 | 183; 166; 146 | 37 |
| 16 | 〃 | Peace | | | Site C dam | 0.60 | 1271/1272 | 1/1/1 | 1156; 1156; 1162 | 5 |
| 17 | 359012853:0 | Peace | 4750 | 851/612 | Halfway River −5,000 m | 0.27 | 1260/1260 | 1/1/1 | 3366; 3358; 3447 | 89 |
| 18 | 360243437:0 | Skeena | 1557 | 431/268 | Cedarvale | 0.31 | 377/378 | 1/1/1 | 1070; 1069; 1060 | 10 |
| 19 | 355995300:0 | Thompson | 481 | 208/146 | signs 1 km below Martel | 0.60 | 184/185 | 1/1/1 | 188; 187; 185 | 3 |
| 20 | 360241575:0 | Zymoetz | 172 | 69/38 | McDonell Lake +3,000 m | 0.74 | 15/15 | 1/1/1 | 51; 33; 25 | 26 |
| 21 | 360245483:0 | Zymoetz | 463 | 157/91 | Zymoetz Canyon upper entrance | **0.86** | **0/0** | 1/1/1 | 0; 0; 0 (its end) | 0 |

**Rows 4 and 21 are false straddles.** The cut sits exactly on the braid's junction, within 1 m. Their 9 % and
14 % "minority" is 5 m samples near the junction that project onto the cut's own vertex. Both sides are already
right; only the mark is wrong.

## A. Build-time simplifications

| option | braids resolved (of 18) | boundary shift | risk |
|---|---|---|---|
| A0 junction-at-cut tolerance: a braid whose junction is within ≤5 m of the cut takes the other side | **2** (Atnarko 225339, Zymoetz 245483) | 0 m | none. Sides are unchanged; two spurious marks go. |
| A1 snap the cut to the network's junction when it is ≤ X m away | X=50: 5; X=100: 6; X=250: 7; X=500: 11 | median **363 m**, max **1,563 m** (network level); piece level median 184 m | Moves a book landmark (Site C dam 363 m; the Halfway −5 km signs 1.26 km) for **every** rule on that split. It silently widens or narrows main-stem closures (AGENTS 12/13, ㉗). The main-line section id `{blk}:{M}` changes. Only defensible within a split's own placement tolerance (≤50 m), which buys 3 beyond A0. |
| A2 collapse a narrow braid (max offset ≤ N) into the main stem | N=100: **2**; N=200: 5; N=400: 11 | 0 on the main line | Collapse means one of two things. **Prune**: the channel leaves sections and tiles (basemap only), which the owner rejected in E15 ("never deleted"); 15 of 18 carry a tributary (the reason the braid prune kept them), so mouths must be re-homed. **Merge into the main section**: the braid's geometry still has to be cut at the cut, so this is A3/B under another name. |
| A2' collapse a short braid (≤ L) | L=250: 2; 500: 5; 1000: 7 | – | Same as A2. The long ones are the dangerous ones (Peace 2.9–5.9 km). |
| A3 split the braid where its nearest-point projection crosses the cut | 18 (sides exact by E15's own measure) | 0 | 1 crossing on 18 of 21 pairs. 0 crossings on the 2 junction-at-cut pairs (no split). **3 crossings on Dean 738**, which would give 4 pieces with slivers. It needs the same sectionizer mechanism as B. |

## B. Tangent splitting (braids only)

The cut line is perpendicular to the main line's chord between c−W and c+W; the sum of unit tangents equals the chord.
At **±100 m**: **19 of 21 pairs are cut once**; none is hit twice. The two misses resolve the braid rather than fail:

- **Dean 738:** the line passes through its partner 736, so 738 lies wholly above (100 % in; E15 says 91 %).
- **Atnarko 225339:** the junction is at the cut, and the braid lies wholly below. At ±50 m the line grazes it 16 m
  from its end, which would leave a sliver.

**Robustness.** The split point moves a median of **26 m** between ±50, ±100 and ±200. It moves most where the main
line bends near the cut and the braid is far from it:

- Adams lower sign: 260 m (braid 280–500 m from the sign)
- Peace 1,200 m signs: 178 m (about 1 km away)
- Chowade bridge meander: 91 m
- Peace Halfway: 89 m

±200 m tracks the meander on the Chowade (see the sketch), and ±50 m catches junctions. **±100 m is the best of the three.**

**Pieces.** Each pair gets one more braid section, about +19 in all: Peace 359013560 and 359005110 get two cuts
each, and Dean 738 gets none.

**Cost.** The sectionizer must cut a blue line that is not the cut's own, at a measure derived from geometry. That
is a new mechanism, but it is still upstream of resolution (AGENTS 10). The new upper pieces get new
`{braidblk}:{M}` ids, and tiles must be rebuilt to the same vintage.

**Interpretive risk.** The line is invented across the floodplain, up to 1 km of land on the Peace. The book draws
none there.

**Determinism.** The split is a pure function of atlas geometry. Verify it with a rule-binding diff, subtracting the
old-vs-old build noise (memory: atlas not deterministic).

## C. Spread per placement (38 bundle rows)

Minority m is the length the E15 majority puts on the wrong side: by projection, and by the ±100 m tangent.
**The tangent and the projection never disagree on the majority side (0 flips of 38).** Their lengths differ by
≤ 30 m on most rows. The largest differences are Adams +140 m, Peace +112 m (entry scope) and Dean 738 −170 m.

| braid | rule(s) (what) | E15 side / share | minority m proj / tangent | danger |
|---|---|---|---|---|
| Peace 359005110 | entry scope: downstream row (in 0.60); Hwy 29 row + r1–r4 (out 0.89) | in/out | 2,368/2,480; 641/640 | **HIGH.** 1,731 m (29 %) lies in the Site C dam **No fishing** zone yet reads under the downstream row, and is **not marked** for the dam row (it touches only pieces outside that 1.2 km window, so topology placed it silently). |
| Peace 359013560 | dam-zone row (out 0.66); Hwy 29 row + r1–r4 (in 0.60); downstream row (out 0.94) | – | 969/990 in the dam zone reads open | **HIGH.** Year-round no-fishing zone reads open. FWA channels predate the dam. |
| Peace 359012853 | r5 No fishing Mar 1–Jun 30 (Halfway ±5 km) | out 0.73 | 1,297/1,390 | **HIGH.** 1.3 km of a closure reads open. |
| Adams 355996658 | r1 No fishing Sep 1–Oct 31 (platform signs, a 400 m window) | out 0.80 | 575/715 | **HIGH.** The braid's in-window length is longer than the main-stem window itself. |
| Chowade 359002043 / 359006635 | r1 No fishing Aug 15–Oct 15 above the bridge | out 0.55 / in 0.59 | 710/735 open; 420/435 closed | **HIGH.** Near-even split; one braid under-closes and one over-closes. |
| Skeena 360243437 | winter closure r1/r3 (Jan–Jun) above Cedarvale | out 0.69 | 486/486 | Medium: 486 m of a closure reads open. |
| Morice 360213366 / 368 | r4 No fishing Jan 1–Aug 31; r5 fly only Sep | in 0.81 / out 0.94 | 444/445; 81/80 | Medium: over-closes 444 m. |
| Dean 360227736 / 738 | r3/r9/r10 above the signs; r4 No fishing (all year) below | out 0.69 / in 0.91 | 200/225; 170/0 | Low–medium: r4 is a year-round closure. |
| Thompson 355995300 | r5 bait ban | in 0.60 | 191/185 | Low. |
| Chuckwalla, Bulkley, Atnarko 237278, Zymoetz 241575 | closures / no angling from a boat / no powered boats | 0.73–0.95 | 46–90 | Low. |
| Atnarko 225339, Zymoetz 245483 | closures | 0.86–0.91 | 20–65 / **0** | None (junction at the cut, see A0). |

**Not in the 38 (census of the whole atlas).** I checked every side channel of every named river against every
cut on its main line.

- **26 pairs / 23 pieces at curated cuts.** Besides the 18 above:
  - **Serpentine River at 168th Street.** Two channels: 2,844 m at 52/48 and 1,435 m at 40/60. r1 is No fishing
    upstream and r2/r3 are seasonal closures downstream. The channels are **bound but unmarked**: they reach the
    river only around the cut, so topology placed them and E15 never ran.
  - **Chowade 359006631:259.** 99 % on one side, so harmless.
  - **Squamish at the Ashlu confluence** (3 pieces), **Skeena at Zymagotitz** and **Pend-d'Oreille at Seven Mile
    Reservoir head** sit on curated cuts that no rule uses yet. They straddle the day a rule does.
- **22 pieces at area, park, MU or border cuts.** Zone and area rules place these by polygon, not E15.
- **402 pieces at auto length or gauge cuts.** No rule binds there today. Any of them becomes a straddler if a split
  is curated at that spot.

## Recommendation

1. **Keep E15 majority.** The tangent model agrees with it on every one of the 38 side choices; majority is coarse,
   not wrong. Snapping (A1) and collapsing (A2) are not worth it: they resolve 2–6 braids at safe thresholds, and
   they move landmarks (median 363 m) or delete water.
2. **Cheap, no boundary moves, worth doing:**
   - (a) A0, the ≤5 m junction tolerance, which drops 2 false marks.
   - (b) Extend the mark to **geometric** straddlers that topology placed silently: the Peace 5110 dam zone and
     Serpentine ×2. `position.share_within` already measures this.
   - (c) Have answers/UI read `section_straddle`, which the R10 log says is still not done, so the danger rows
     above at least say "this channel runs across the boundary".
3. **If exact placement is wanted later:** split by the ±100 m tangent in the sectionizer, gated to braids whose
   minority on a closure is ≥ ~300 m. That is 8 braids: Peace ×3, Adams, Chowade ×2, Skeena, Morice 366. It costs
   about 10 new sections and resolves every HIGH row. Lower the gate to all 16 real straddlers only if the owner
   accepts an invented line across the floodplain.

Sketches (now vs tangent split): `r12_braids/dean_canyon.png`, `r12_braids/peace_site_c.png`,
`r12_braids/chowade_network.png`. The Peace flows east, so the left triangle is the dam and the right triangle is
the 1,200 m signs.
