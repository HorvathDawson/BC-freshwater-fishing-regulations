# T101 — Gauges walk through slough/canal polygons: ship once the side-channel question is answered

Status: OPEN — Q44 answered 2026-10-23 (no Level from a side channel; Flow only when it carries a large share). Built and reviewed, NOT merged; the Q44 rule is the remaining build: branch worktree-wf_3976eddc-bba-1
(.claude/worktrees/wf_3976eddc-bba-1), commits 8d57e759 0a0fa59b d831ed6a 7cc92b90 a389bf59 de39b70c.

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

## Ship steps (after Q44)
Merge the branch (+ the Q44 rule), `deliver` via heavy.sh, `pytest -m needs_bundle test_stream_polygon_panels.py
test_live_panels.py`, diff section_panel against the measured numbers, fixtures, strict + smoke.
