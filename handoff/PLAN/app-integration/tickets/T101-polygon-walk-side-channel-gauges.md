# T101 — Gauges walk through slough/canal polygons: ship once the side-channel question is answered

Status: PARTIAL — merged on redesign/stream-sections 2026-10-23; deliver + needs_bundle checks running. Polygon walk
74e10c03 ba6dde1d da1ab3e9 20faf224 2ffbae66 1fbc2488 (were 8d57e759 … de39b70c), Q44 rule 92c188b4, review fix a2fae3cf.

Owner 2026-10-22: "if a slough or canal is fully polygon only it should still be able to get gauge info of neighbours
if the propagation means it would have values if it was just a stream itself."

## What the branch does (measured on the real graph)
- The panel walk reads the water's kind: a stream item's polygon is walked, sized (its own stream magnitude: Vedder
  Canal 994 km², Seton Canal 1,410 km²) and is not a lake crossing. Side-channel slough polygons take their lines'
  answer (review fix a389bf59).
- Stream polygons answered 38 → 59 of 145 (Seton Canal, Lewis Slough new; Bowman, Hansen stay grey because the lines
  beside them are unanswered too). No lake/wetland section gains or changes a panel. 0 lost.
- Flow: +65 sections, 30 changed (Vedder/Sumas gain Chilliwack/Vedder gauges; Alouette gains 08MH014; Seton/Cayoosh
  gain 08ME002/08ME018).

## Why it is held
- Level: +40,011 sections and 31,747 changed, almost all from two Nicomen Slough LEVEL gauges (08MH085 Deroche,
  08MH086 Dewdney, 5 years of record) climbing out of the slough into the Fraser and up every tributary within the
  area ratio (e.g. Stein-basin creeks). The slough polygons used to stop that walk by accident (as "lake crossings").
- Bell and Nelson Slough would take nine creek flow gauges 100–400 km away via the Fraser — not hydrologically right.
- The underlying rule already exists: 29,520 live answers today rely on a side-channel gauge speaking for its parent
  river. A "side-channel gauge may not climb into its parent river" stop would make this change +134 / 65 / −6, but
  removes those 29,520 existing answers.

## Owner question (Q44)
May a gauge standing on a side channel or slough speak for the river it branched from?
- Default (Claude): NO for Level (level is local to the backwater), and for Flow only when the side channel carries a
  large share of the river (otherwise no). Measure the effect on the 29,520 live answers before shipping.

## Also found (separate fix)
The lake-crossing flag depends on walk order (pre-existing): 2,074 records at 75 stations are flagged as crossing a
lake although a lake-free path exists. Fixing it changes panels elsewhere → needs its own before/after diff.

## As built (Q44 rule) — 2026-10-23, 92c188b4
ONE decision, ONE function: `build_panels.across_branch(share)` → the quantities a reading keeps when the walk carries
it across a BRANCH POINT: `{discharge}` when `share >= LARGE_SHARE`, else nothing. Level never.
- `LARGE_SHARE = 0.5` (named constant, build_panels.py). Share = the smaller water's drainage (`area_of`, stream
  magnitude) over the larger's, each the largest of its water's sections within `MAINSTEM_HOPS` (4) of the edge — not
  the two end sections, which FWA cuts into stubs (Nicomen leaves the Fraser through a 2.6 km² "Fraser River" piece).
- Branch points (`branch_points`), from the graph, never names: an edge between two STREAM waters on the SAME
  watershed code (FWA gives a side channel its river's code), or the river flowing INTO a code nested under its own
  (Nelson Slough's head). Continued systems (`water_continue`) are one river. Real graph: 107 edges, 36 water pairs —
  every one a channel and its river (Nicomen, Maria, Herrling, Morey … off the Fraser; Hotsprings, Baldy, Back off the
  Columbia; Old Kootenay, East Branch Kootenay; Widgeon off the Pitt; Seton Canal; Bell off Nelson). Six pairs are
  at or above 0.5: Hogg/Mountain Slough 1.0, Hansen Slough/South Hawks Creek 1.0, Lewis Slough/Hanson Creek 1.0, Seton
  Canal/Seton River 0.97, Dallas/Noons Creek 0.89, Bell/Nelson Slough 0.55. Every named Fraser/Thompson/Columbia/
  Kootenay side channel is below 0.04 (Nicomen 0.0032).
- The walk rules on EVERY reading going INTO a side channel (river gauges, and creeks that reached the river), and on
  the readings of the side channel's OWN gauges going OUT of it. A creek that only drains through a slough is a
  tributary of the river as before (Widgeon Creek's 08MH010 keeps the Pitt). A section is walked again when a later
  path carries more (the way down the river beats the way through the slough; the old seen-set let edge order decide).
- A member is a station and the app answers each quantity from every member reporting it — a row cannot say "Flow
  only" — so a gauge measuring a quantity the crossing does not allow stays on its channel (180 records on the real
  graph: Cayoosh 08ME002 across Seton Canal, Kootenay River level+discharge gauges across the Creston sloughs).
- "The side channel still takes its own gauge" (`own_gauges`): an adopted side-channel line that is a water of its own
  keeps the own gauges its walk LED with, ahead of the river's members (river's leads ranked after); 5 sections —
  Nicomen 355991886:0 (08MH086/085 lead), Herrling Island (08MF047), Cannery Channel ×2 (08MH028), East Branch
  Kootenay (08NH026). A piece of the river's own water (Bella Coola at Hagensborg, Taku, McGregor …) is the river.
- Tests: `pipeline/tests/test_side_channel_gauges.py` (whole-value; mutation pins: the share gate, the two walk
  directions, the walk-again rule, own_gauges; no second copy of LARGE_SHARE / across_branch). Fixtures in
  test_polygon_walk.py now carry watershed codes (a tributary's code extends its river's).
- Known limit: a backwater FWA codes like a tributary is a branch only at its HEAD; at its mouth it looks like any
  tributary (08MF064 Fraser near Chilliwack still answers Bell/Nelson Level from their mouth, as today).

## Measured (real graph, heavy.sh, read-only; BASE reconstructs the live bundle exactly: 246,657 of 246,657)
Per quantity: "answered" = sections whose panel holds a member reporting it; changed = that member set changed.
| | Flow answered | +gained / changed / lost | Level answered | +gained / changed / lost |
|---|---|---|---|---|
| live (BASE) | 246,004 | | 127,475 | |
| polygon walk alone (held) | 246,076 | +77 / 13 / −5 | 184,476 | +57,001 / 14,757 / 0 |
| polygon walk + Q44 (ships) | 246,060 | +67 / 8 / −11 | 127,270 | +120 / 35 / −325 |
Sections with any answer: 246,657 → 246,705 (+128, 303 changed, 80 lost).
- NICOMEN: 08MH085/086 answer 0 Fraser River sections (71,983 sections on the held branch; 315 live, 287 now — Nicomen
  Slough, its creeks (Norrish, Siddall, Pye, Barnes …) and their unnamed pieces). The live answers that reached other
  waters through the Fraser go (Zaitscullachan 5, Sumas Lake Canal 2, Vedder 3, Sumas 2, Hatzic …).
- BELL / NELSON SLOUGH: all 11 sections back to Flow none, Level 08MF064 only (as live); the nine creek gauges
  (08KE015, 08LF001 …) 100–400 km up the Fraser are gone.
- THE 29,520 (live answers the old "no climbing out of a side channel" guard removed; 35,384 sections it touched in
  all): 35,327 UNCHANGED, 40 lose their answer, 5 change otherwise, 12 change as the guard said. The guard's victims were
  main-river gauges FWA snapped onto side pieces of their own river (Taku 08BB001 14,453, McGregor 08KB005 6,378,
  Bella Coola 08FB008 5,014, Klinaklini 08GE001 4,991 …) — the same water, so not a branch point here. What Q44 does
  remove from them: Nicomen 08MH085/086 (40), Kootenay at Slough Bridge 08NH026 (17), Sumas 08MH094/131 (1).
- Level −325: 286 unnamed + Widgeon Creek 8 (Pitt 08MH035 entering Widgeon Slough), Kitchener 6 (08NH026), Zaitscullachan
  5 … — slough level gauges that reached other waters through their river, and river level entering a slough.
- Flow −11: 5 unnamed, Nicomen Slough 2 (creek artefacts via the Fraser), Pratt, Hot, Pooley Creek, Gravel Slough 1 each.
- No lake, wetland or unowned polygon gains a panel.

## Owner question
- Q44a — "a large share" = `LARGE_SHARE` 0.5 (the channel is the larger half of the split). DEFAULT 0.5. On today's graph
  (corrected by the adversarial review: four of the six pairs first listed were one water taking another name) only
  three pairs reach it: Hanson Creek/Lewis Slough 1.0, Seton Canal/Seton River 0.97 — both a polygon with a line beside
  it, not a true channel — and Bell/Nelson Slough 0.55. 0.2 adds Alec Chief/Toozaza Creek (0.24), 0.1 Widgeon
  Slough/Pitt River (0.18). No Fraser side channel comes near any of them.

## Ship steps left
Merge the branch, `python -m pipeline.deliver` via scripts/heavy.sh (NOT run here), `pytest -m needs_bundle
pipeline/tests/test_stream_polygon_panels.py pipeline/tests/test_live_panels.py`, diff section_panel against the numbers
above (Flow 246,060 / Level 127,270 answered), fixtures re-cut, strict + smoke.

## Adversarial review — 2026-10-23 (fix a2fae3cf)
Tried to break Q44 on the real graph (read-only, heavy.sh; scratchpad q44rev/audit.py, values4.pkl) and by mutating the code.

DEFECT FOUND AND FIXED — `branch_points` called a NAME CHANGE a branch. "Same code, another stream water" also matches
one water running on under another name, where nothing divides: Hogg Slough → Mountain Slough, South Hawks Creek through
Hansen Slough's ponds, Noons → Dallas Creek, Eagle Trib.5 → Trib.5-3 (8 of the 107 edges). There every reading lost Level
(share ~1.0: Flow only) though there is no side channel at all, and those pairs made up most of Q44a's "six pairs at or
above 0.5". Fix: a branch point is a SPLIT OR A JOIN — the upper end's code goes on another way, or the lower end's code
already flowed in another way — looked for within `MAINSTEM_HOPS` on that code (FWA cuts the split into stubs: the
Fraser enters Nelson Slough through a 1.4 km² Fraser piece whose only way on is the slough; a first, edge-local version
of this fix read that as a name change and let the nine creek gauges back into Bell/Nelson — caught by re-measuring),
never a braid that rejoins above the edge. Now 99 edges, 32 pairs. Tests: `test_one_water_taking_another_name_is_not_a_
branch_point`, `test_a_split_cut_into_a_stub_is_still_a_branch_point`, `test_a_braid_above_a_new_name_…`, two mutation
pins. `_code_child` now asks the tributary walk's own parent/child test (`tributaries._wsc_below`).
- Re-measured (builder's measure.py): IDENTICAL panels to 92c188b4 — 246,705 of 246,705 sections equal; Flow 246,060
  (+67 / 8 / −11), Level 127,270 (+120 / 35 / −325); Nicomen 0 Fraser sections; Bell/Nelson Flow none, Level 08MF064. The
  fix changes no panel today (no gauge reaches those renamed pairs within the area ratio); it corrects the rule and the
  Q44a evidence above.

CHECKED, NOT A DEFECT
- Escape routes for a side-channel gauge to its parent river: on NEW no station on a side-channel water (Nicomen 085/086,
  Herrling 08MF047, Cannery 08MH028, East Branch Kootenay 08NH026, Seton River 08ME003/018) is a member of any section of
  its river, owned or an unowned piece of the river's code at river size. The walk is one-directional per role (up OR
  down), so "out via a creek" cannot happen: a creek that drains into the slough is upstream of it; climbing it and coming
  back down is not a path. Nicomen's 263 unowned answers are all 100-066153-* (Norrish Creek's basin). The only
  channel-gauge crossing is 08ME018 (discharge) onto Seton Canal at 0.97 — Flow at a large share, as ruled.
- Lake crossing: `lake_gauge` holds 173 rows, all lake items; no stream item (slough) carries one, so no slough level
  reaches a river through the item road. Latent: `lake_gauge_links` keys by owner item with no kind check (follow-up).
- Re-walk: compares counts of nested sets ({level,discharge} ⊃ {discharge} ⊃ ∅), so "more" is unambiguous; pinned.
- Braids/floodplain: unowned pieces never form a branch point (both ends must be owned stream waters of different
  systems), so the 28,359 equal-code edges cannot cut a main river's answer. Sections losing a gauge that stands on their
  OWN water, base→new: 1 — Sumas River polygon lake:328997872 (53 km², above the Vedder) drops 08MH094/131 (below the
  Vedder, 1,036 km²) — the polygon walk sizing it as a line would be (T101 part one, not Q44); it keeps four Sumas River
  gauges (08MH029/093/130/133). The Level −325 are 286 unowned + creeks: Pitt 08MH035 entering Widgeon Slough (264),
  Nicomen 085/086 (42), 08NH026 (14), Columbia 08NA002/08NB005 (7), Liard 10BE001 (1) — none a main-river section of the
  gauge's own river. The Flow −11 are creeks and slough pieces reached through a side channel (Pratt, Hot, Pooley Creek;
  Nicomen 22415 and its polygon; Gravel Slough's polygon).
- The 180 dual-quantity records: all at the two ≥0.5 polygon-and-line pairs (Kootenay River stations at Hanson
  Creek/Lewis Slough; Cayoosh 08ME002 at Seton Canal). None is a loss against live (08ME002: 1 section live, 2 now) —
  they are gains the polygon walk would have made. Keeping them as Flow-only members needs a per-member quantity column
  (bundle schema + app) — not worth it for two artefact pairs; follow-up only if the owner wants Flow-only members.
- One copy: `LARGE_SHARE`/`across_branch`/`branch_points` appear only in build_panels (test pins it). Other places a
  gauge crosses waters: `mainstem_of` (an adopted side-channel line takes the river's WHOLE answer, Level included),
  `side_channel_polygons`/`stream_polygons` (a polygon takes its own water's line), `own_distances` (continuation = one
  system). None lets a channel's gauge out; but see the inward note below.
- Mutation sweep (source edited, tests run, restored): level-crosses, any-share, no-outward, no-inward, seen-set,
  no-own-gauges, no-dual-filter, renames-are-branches, end-section share, no code-child, stub-blind, river-is-the-channel
  — each fails at least one whole-value test for its own reason (e.g. no-outward: the Nicomen miniature's r_down gains N).
- T102 (`THE_WALK_NEVER_ENTERS_A_PARENT`) vs `branch_points`: two decisions (the tributary walk never climbs into a parent
  code; a river flowing INTO a nested code is a backwater head) over one predicate — now one predicate
  (`_wsc_below`). They should not share more: equal codes are where T102 says WSC is silent and the order guard decides,
  and where branch_points asks the water.

FOLLOW-UPS (not done here)
- Inward vs adoption: `across_branch` cuts the river's readings going INTO a channel by the walk, while `mainstem_of`
  gives an adopted channel line the river's full answer. Owner Q44 rules only the outward direction; the inward cut is
  Claude's extension (it is what keeps the creek gauges out of Bell/Nelson). Owner to confirm, or the two to be made one.
- Known limit stays: a backwater FWA codes like a tributary is a branch only at its head (Nelson's mouth); no gauge
  stands on such a water today, so nothing leaks outward.
- `lake_gauge_links` should refuse a stream item (latent route for a slough level gauge to the whole item).

Verdict: MERGE (with the review fix). Ship steps unchanged; the numbers to diff against are unchanged.
