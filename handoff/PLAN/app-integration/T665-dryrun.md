# T665 dry run: the polygon rule on the live atlas, in memory

2026-11-11. Step one of `T665-design.md`. The new rule (`pipeline/atlas/splits/shore.py` `polygon_inside` /
`polygon_rule` / `owner_limited`) ran on the live atlas (build 27, `data/generated/atlas/full`) and its reach run's
`tidal.jsonl`, in memory, through `scripts/heavy.sh` (lane 2, about 3 min). Nothing was built, written or promoted. It is
not wired into the build yet. The driver script stayed in the scratchpad: it loads `graph.pkl` and `geometries.pkl`, calls the
three functions, and writes the results. Map: `T665-dryrun-map.html` (one file, inline SVG, no network).

Owner rulings applied: D1 (no cut into fresh water), D3 (first exit, outside runs up to 25 m bridged), D4 (the line end
stays the limit on creeks with a `mouth` row), D5/D6 (no change). The cutter is `clean_cut.decide_chain`, exact to the
polygon edge (dips under 5 m ignored), run against the 601,582-vertex polygon in 64-vertex edge chunks.

## Counts

| | dry run | design expected |
|---|---|---|
| lines meeting the polygon | 16,292 runs of line | — |
| **Rule A: lines wholly inside, removed** | **215 lines, 67.7 km** (104 named 44.3 km, 111 unnamed 23.4 km) | 206 + the 5 whose exit is their top |
| named Rule A vs `T660-inside-named.json` | 102 of 103 match (the 103rd, Brae Slough, has no FWA name: counted unnamed); 2 more: Adam River 354088630 (125 m), Ksi Gingolx 360240195 (365 m) | 103 |
| the 177 T660 creeks | 136 cut · 33 D4 (no cut) · 5 Rule A (exit = line top) · 3 mouth outside, never meet the polygon (D6) | 169 cut |
| exit vs the T660 10 m samples | every exit within 5.5 m of T660's | — |
| **shore cuts in total** | **376** (138 tidal below the cut, 25 cut but fresh; 213 into a removed line, D2: 65 tidal, 148 fresh) | about 169 + D2 |
| tidal length below the cuts | 119.7 km | — |
| against build 27's 532 shore cuts | 277 lines cut again (moved), 255 dropped, 99 new | 532 → ~169 |
| not cut: the line's own curated limit governs | 55 lines (new guard, see below) | — |
| not cut: exit = line top (whole line tidal) | 30 | 5 |
| not cut: mouth outside the polygon | 28 (13 to curated tidal water, 15 to a removed line) | 3 |
| not cut: D4 owner `mouth` row | 92 lines (the 33 below + 59 more) | 33 |
| lake on the line before the exit | 1 (360226010); 1 cut has a lake below its exit (360614376) | ~0 |
| exit within 5 m of a standing boundary (alias) | 0 | not measured |
| leaves and re-enters (cut at the first exit) | 35 cuts | 3 |
| added (municipal) streams cut | 9 | 9 |
| mouth inside, joins fresh water (D1) | 15,456 runs of line, never cut | 371 cuts dropped |

## What differs from the design

1. **The rule cut rivers above the owner's own limits.** A line carrying a `split`/`whole` limit is "a creek whose mouth
   is inside and that reaches tidal water", so the bare rule cut the Pitt 3.4 km up (limit at 153 m), the Nitinat 23.9 km,
   the Nimpkish 4.2 km, the Brunette 2.9 km, the Kanaka 1.5 km. Every piece below a shore cut is a crossing, so that
   would have moved tidal water above the limits. **Fixed in the code**: `owner_limited` stops the cut on every line of a
   water (same gnis) with a `split`/`whole` row or a tidal section (`THE_OWNERS_LIMIT_GOVERNS_ITS_OWN_WATER`, 55 lines),
   and D4 also covers the whole water. The design did not cover this case.
2. **D2 is large: 213 cuts into removed lines**, and 148 of them leave nothing tidal: the walk "down while inside the
   polygon" goes through freshwater pieces (braided rivers: Kemano, Kingcome, Kitlope, Kshwan, Zeballos, Bear) before it
   reaches a removed line. Together with 25 more into curated water, **173 cuts make nothing tidal**. Design step 3 says
   "cut, but freshwater", so the code does that. D1's reasoning ("they make nothing tidal") says drop them.
   **Owner question; recommended: cut only where the piece below goes tidal.**
3. **Main rivers with no owner limit go tidal through D2**, below their polygon exit: Bella Coola 360218493 (212 m),
   Mamin (443 m), Apple River ×3 (534–703 m), Kshwan 360225716 (99 m), Sarita (419 m), Bedwell 354088440 (317 m). Each
   needs an owner look; they are not on the map page.
4. **D4 covers 92 lines, not 33.** 59 more creeks with a `mouth` row have their mouth inside the polygon (56 of them flow
   into a removed line, the D2 case). Same default (no cut). 32 of the 33 hold a tidal crossing in build 27, so **under D4
   those 32 crossings go back to freshwater.** The design expected the reverse ("after the cut the piece below the shore
   is tidal").
5. Re-entries: 35 cut lines come back into the polygon more than 25 m above their first exit (design: 3, counted on
   crossing tops). The higher stretch stays freshwater (D3).
6. The ~30 "exit = top" lines are whole lines inside after the 25 m bridge but not after 5 m dips. They go tidal whole.
   The design counted 5.

## The 33 creeks: the limit stays at the end of the mapped line (D4)

"Inside" is how far the mapped line runs inside the DFO polygon, from its end to where it leaves the polygon. Build 27 is
the live state; under D4 its shore cut goes and the line is freshwater to its end.

| # | creek | blue line | inside (m) | line end (lat, lon) | polygon edge (lat, lon) | build 27 |
|---|---|---|---|---|---|---|
| 1 | Baker Creek | 356164168 | 332 | 49.23032, -122.76739 | 49.23140, -122.77144 | tidal crossing (shore cut at 309 m) |
| 2 | Barker Creek | 356274648 | 278 | 49.21843, -122.85370 | 49.21593, -122.85396 | tidal crossing (shore cut at 209 m) |
| 3 | Benson Canal | 356179037 | 134 | 49.16844, -122.46063 | 49.16744, -122.46030 | tidal crossing (shore cut at 98 m) |
| 4 | Boneyard Creek | 360885692 | 2506 | 54.15070, -130.07197 | 54.17250, -130.08115 | tidal crossing (shore cut at 2523 m) |
| 5 | Carlisle Creek | 360764607 | 3214 | 54.14160, -130.09261 | 54.11364, -130.08077 | tidal crossing (shore cut at 3202 m) |
| 6 | Chester Creek | 356237969 | 763 | 49.15186, -122.39957 | 49.15226, -122.39151 | tidal crossing (shore cut at 229 m) |
| 7 | Cohilukthan Slough | 356285962 | 884 | 49.09398, -123.08656 | 49.08778, -123.07921 | tidal crossing (shore cut at 27 m) |
| 8 | Como Creek | 356150276 | 224 | 49.22241, -122.85113 | 49.22438, -122.85057 | tidal crossing (shore cut at 178 m) |
| 9 | Cougar Canyon Creek | 356170069 | 336 | 49.15808, -122.94585 | 49.15617, -122.94243 | tidal crossing (shore cut at 316 m) |
| 10 | Crescent Slough | 356326465 | 27 | 49.10468, -123.07251 | 49.10451, -123.07227 | tidal crossing (shore cut at 17 m) |
| 11 | Deas Slough | 356358883 | 2935 | 49.11496, -123.08401 | 49.12654, -123.05220 | tidal crossing (shore cut at 2907 m) |
| 12 | Ewen Slough | 356355111 | 1224 | 49.10523, -123.15462 | 49.10114, -123.16996 | tidal crossing (shore cut at 1203 m) |
| 13 | Gundersen Slough | 356286798 | 1489 | 49.16970, -122.92743 | 49.17740, -122.91496 | tidal crossing (shore cut at 221 m) |
| 14 | Hanna Creek | 356323547 | 361 | 49.14046, -122.38857 | 49.13895, -122.39286 | tidal crossing (shore cut at 327 m) |
| 15 | Katzie Slough | 356339436 | 406 | 49.24352, -122.73773 | 49.24169, -122.73353 | tidal crossing (shore cut at 296 m) |
| 16 | Kildala Creek | 360864443 | 2838 | 54.14344, -130.08791 | 54.11895, -130.07673 | tidal crossing (shore cut at 2823 m) |
| 17 | Kitsucksus Creek | 354145435 | 400 | 49.25682, -124.81462 | 49.26022, -124.81469 | tidal crossing (shore cut at 174 m) |
| 18 | London Slough | 356346058 | 600 | 49.10286, -123.15125 | 49.10004, -123.15757 | tidal crossing (shore cut at 575 m) |
| 19 | MacMillan Creek | 360874166 | 2556 | 54.14492, -130.08533 | 54.16619, -130.09982 | tidal crossing (shore cut at 2528 m) |
| 20 | Matsqui Slough | 356310903 | 420 | 49.11325, -122.32478 | 49.11034, -122.32121 | tidal crossing (shore cut at 320 m) |
| 21 | McCoy Creek | 354130563 | 318 | 49.26795, -124.85280 | 49.26845, -124.85658 | tidal crossing (shore cut at 42 m) |
| 22 | McDonald Slough | 356248171 | 760 | 49.21524, -123.17662 | 49.21099, -123.18359 | tidal crossing (shore cut at 714 m) |
| 23 | McLennan Creek | 356195562 | 297 | 49.10942, -122.33929 | 49.10754, -122.34095 | tidal crossing (shore cut at 155 m) |
| 24 | Nelson Creek | 356013125 | 38 | 49.22957, -122.87702 | 49.22944, -122.87653 | no cut |
| 25 | Nunns Creek | 354149928 | 1424 | 50.04440, -125.25519 | 50.03480, -125.25528 | tidal crossing (shore cut at 222 m) |
| 26 | Palmateer Creek | 356230481 | 590 | 49.16758, -122.52523 | 49.16243, -122.52575 | tidal crossing (shore cut at 523 m) |
| 27 | Rogers Creek | 354154568 | 1182 | 49.25503, -124.81291 | 49.25786, -124.80397 | tidal crossing (shore cut at 63 m) |
| 28 | Salmon River | 356358833 | 95 | 49.17738, -122.58574 | 49.17689, -122.58681 | tidal crossing (shore cut at 64 m) |
| 29 | Silverdale Creek | 356346138 | 277 | 49.13115, -122.35871 | 49.13361, -122.35827 | tidal crossing (shore cut at 232 m) |
| 30 | Tamboline Slough | 356165068 | 701 | 49.10146, -123.14632 | 49.09542, -123.14887 | tidal crossing (shore cut at 678 m) |
| 31 | Tones Creek | 356327002 | 170 | 49.11919, -122.35988 | 49.11796, -122.36127 | tidal crossing (shore cut at 141 m) |
| 32 | Windebank Creek | 356341732 | 370 | 49.12608, -122.33549 | 49.12909, -122.33351 | tidal crossing (shore cut at 349 m) |
| 33 | Woodward Slough | 356303771 | 426 | 49.10755, -123.12265 | 49.11117, -123.12349 | tidal crossing (shore cut at 409 m) |

The other 59 `mouth`-row creeks with their mouth inside (no cut, same default): Adam River 354154447 (299 m, via removed line), Anderson Creek 360859311 (392 m, via removed line), Apple River 360884532 (1371 m, via removed line), Bedwell River 354154798 (924 m, via removed line), Bella Coola River 360885253 (1320 m, via removed line), Bonsall Creek 354133599 (461 m, via removed line), Brem River 360854323 (102 m, via removed line), Brittain River 360882453 (279 m, via removed line), Brothers Creek 360836484 (46 m), Bulson Creek 354152039 (656 m, via removed line), Cascade Creek 354150623 (39 m, via removed line), Cayeghle Creek 354150572 (1487 m, via removed line), Colonial Creek 354154029 (464 m, via removed line), Cowichan River 354155148 (951 m, via removed line), Deer Creek 354149342 (70 m, via removed line), Draney Creek 360878108 (817 m, via removed line), Franklin River 354150804 (577 m, via removed line), Glacier Creek 360815596 (214 m, via removed line), Gold River 354154308 (4075 m, via removed line), Kainet Creek 360871314 (16 m, via removed line), Kemano River 360885559 (813 m, via removed line), Khutze River 360878779 (1068 m, via removed line), Kimsquit River 360887414 (974 m, via removed line), Kingcome River 360874718 (4556 m, via removed line), Kitimat River 360885824 (1250 m, via removed line), Kitkiata Creek 360852130 (1077 m, via removed line), Kokish River 354154484 (153 m, via removed line), Kowesas River 360883956 (810 m, via removed line), Kshwan River 360860569 (698 m, via removed line), Ksi X'anmas 360882263 (1641 m, via removed line), Kwatna River 360858825 (3125 m, via removed line), Mamin River 360873432 (1573 m, via removed line), Marmot River 360883324 (41 m, via removed line), Monmouth Creek 360877261 (341 m), Moyeha River 354154217 (959 m, via removed line), Mussel River 360884170 (635 m, via removed line), Necleetsconnay River 360884575 (617 m, via removed line), Nieumiamus Creek 360869552 (120 m, via removed line), Oona River 360776500 (922 m, via removed line), Pa-aat River 360871265 (2961 m, via removed line), Quaal River 360886968 (2357 m, via removed line), Sarita River 354155136 (1786 m, via removed line), Scribner Creek 360817979 (593 m, via removed line), Silverado Creek 360833243 (77 m, via removed line), Stagoo Creek 360877620 (399 m, via removed line), Sydney River 354147245 (2042 m, via removed line), Theodosia River 360856269 (441 m, via removed line), Toba River 360883346 (2753 m, via removed line), Tom Browne Creek 360870027 (855 m, via removed line), Tranquil Creek 354154428 (303 m, via removed line), Trent River 354148132 (326 m, via removed line), Tsowwin River 354150904 (574 m, via removed line), Tzouhalem Creek 354153794 (2266 m, via removed line), Ucona River 354153463 (80 m, via removed line), Vinson Creek 360744582 (69 m), Wahoo Creek 360887120 (40 m, via removed line), Wathl Creek 360875560 (928 m, via removed line), Winachee Creek 360808890 (292 m, via removed line), Zeballos River 354151763 (336 m, via removed line).

## Owner choices made by default (listed, not decided)
- Lines of a water that has its own `split`/`whole` limit are never cut by the polygon (new guard, item 1).
- D4 applies to every line of a water with a `mouth` row (92 lines), not only the 33.
- A cut that leaves nothing tidal is made anyway, as the design says (173). Recommended: drop them.
- Exit = line top after the 25 m bridge: the whole line goes tidal (30).
- Lake on the line before the exit: no cut, listed (1).

## What the real build still needs (design steps 4–8)
`build.cut_shores` on the new rule, with real `BlkChain`s instead of `graph_lines`; `inside_tidal.json` and its
`sidecars.py` writer; `reach/outside.py` `inside:` owner and `POLICY_VERSION` 37; `alias_mouths` moving the tidal-boundary
name; `T665-orphans.json` (not produced here); the braid sweep on the new cut points (not run here); the D2 question
above; RULINGS/MAP; a side build together with T664 and the nine DFO cuts, then a binding diff and owner maps. Also an
OWNER-TODO row linking this page (D4 double check), which is not added yet.

## Not verified
- The braid sweep, sliver gate, straddle and unplaced gates were not run at the new cut points. No reach run, no binding diff.
- `graph_lines` rebuilds lines from the cut atlas, so lines are split at lake runs. In the build, `BlkChain`s will
  carry the real lake runs. Results can differ at lake ends (2 cases seen).
- Map panels were checked by eye on a phone-width render (first cards only).
