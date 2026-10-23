# T101 — Gauges walk through slough/canal polygons: ship once the side-channel question is answered

Status: BUILT, NOT MERGED — Q44 rule built and measured 2026-10-23; ready to merge + deliver. Branch
worktree-wf_3976eddc-bba-1 (.claude/worktrees/wf_3976eddc-bba-1), rebased on redesign/stream-sections: polygon walk
b808d9be 92af842e d148fdc4 8406b74e 5907ca73 709b6033 (were 8d57e759 … de39b70c), Q44 rule b73313d9.

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

## As built (Q44 rule) — 2026-10-23, b73313d9
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
  only six channel/river pairs reach it (above); 0.4 adds Eagle Trib.5 (0.46), 0.2 Alec Chief/Toozaza Creek (0.24),
  0.1 Widgeon Slough/Pitt River (0.18). No Fraser side channel comes near any of them.

## Ship steps left
Merge the branch, `python -m pipeline.deliver` via scripts/heavy.sh (NOT run here), `pytest -m needs_bundle
pipeline/tests/test_stream_polygon_panels.py pipeline/tests/test_live_panels.py`, diff section_panel against the numbers
above (Flow 246,060 / Level 127,270 answered), fixtures re-cut, strict + smoke.
