"""OWNER 2026-10-22 — estimates use every gauge that could inform a section, chosen live.

"make sure the section gauge percentage in prediction computes for all gauges we have possible so that even if some
drop/get added in that list it is still able to make a prediction using all available gauges on the current flow at
the location."

Two halves, two places:
  · THE PANEL (here, build time): every station with a record that could describe the section, ranked by the usual
    weight, the best `MEMBERS_PER_QUANTITY` PER QUANTITY — not filtered by the stations roster's `realtime` flag, which
    is a snapshot and drifts (13 of its 439 "live" stations had stopped transmitting by 2026-10-22, 6 "silent" ones
    had started). Own-gauge lead candidates likewise.
  · THE CHOICE (the app, read time): the members reporting the asked quantity in the current feed, re-weighted among
    themselves; the lead to the best-ranked own gauge reporting. app/packages/data/src/livePanel.test.ts; the
    pipeline mirror (`leader_of`, `lead_shares`, `combine`) is pinned below and held equal by app/tools/one-formula.
"""

from __future__ import annotations

import ast
import inspect
import textwrap
from pathlib import Path

import pytest

from pipeline.atlas.gauges import panel as P
from pipeline.atlas.gauges.build_panels import build
from pipeline.atlas.gauges.panel import (
    CORE_MEMBERS, MEMBERS_PER_QUANTITY, OWN_LEADS_PER_QUANTITY, QUANTITIES, combine, lead_shares, leader_of, panel_for, weight_of,
)
from pipeline.tests.test_build_panels import MODEL, E, N, g


def _cand(station, area, years=40, same=True):
    return (station, "up", area, years, False, False, same)


# ------------------------------------------------------------------ the constants

def test_the_reserve_depth_is_the_measured_one():
    """Measured (panel.py `MEMBERS_PER_QUANTITY`; handoff/PLAN/app-data/live-panels/scripts). Changing it moves the
    paint file against its 1 MB gzip gate."""
    assert (MEMBERS_PER_QUANTITY, OWN_LEADS_PER_QUANTITY, QUANTITIES) == (8, 4, ("discharge", "level"))


def test_the_core_is_the_panel_before_the_reserve():
    """`CORE_MEMBERS` is the old cap (the best four overall): the app's `estimate` lets reserve members past it stand
    back rather than silence the core (review 2026-10-22). app/tools/trust-ladder.test.ts holds the app's equal."""
    assert CORE_MEMBERS == 4


# ------------------------------------------------------------------ the panel: a ranked reserve per quantity

def test_each_quantity_keeps_its_own_best_members():
    """Ten flow gauges and three level-only ones, the level ones the heaviest: Flow keeps its best eight, Level keeps
    all three — each quantity ranks only the stations with a record in it."""
    cand = ([_cand(f"Q{i}", 100.0 * (1 + i), years=40) for i in range(10)]
            + [_cand(f"L{i}", 100.0, years=40) for i in range(3)])
    measures = {**{f"Q{i}": {"discharge"} for i in range(10)}, **{f"L{i}": {"level"} for i in range(3)}}
    got = panel_for(cand, 100.0, measures=measures)
    assert sorted(d.station for d in got) == sorted([f"Q{i}" for i in range(8)] + ["L0", "L1", "L2"])
    assert [d.weight for d in got] == sorted((d.weight for d in got), reverse=True)


def test_a_level_only_station_does_not_take_a_flow_seat():
    """THE OLD CAP was the best four OVERALL, so four heavy level-only gauges left a Flow panel with nothing to say."""
    cand = [_cand(f"L{i}", 100.0) for i in range(4)] + [_cand("FLOW", 900.0)]
    measures = {**{f"L{i}": {"level"} for i in range(4)}, "FLOW": {"discharge"}}
    assert [d.station for d in panel_for(cand, 100.0, measures=measures)] == ["L0", "L1", "L2", "L3", "FLOW"]


def test_a_station_measuring_both_counts_in_both_lists_once():
    cand = [_cand(f"B{i}", 100.0 + i) for i in range(MEMBERS_PER_QUANTITY + 2)]
    measures = {f"B{i}": {"discharge", "level"} for i in range(MEMBERS_PER_QUANTITY + 2)}
    got = [d.station for d in panel_for(cand, 100.0, measures=measures)]
    assert got == [f"B{i}" for i in range(MEMBERS_PER_QUANTITY)]


def test_without_measures_every_station_counts_in_both():
    """The synthetic callers that never had `measures`: the best `MEMBERS_PER_QUANTITY`."""
    cand = [_cand(f"S{i}", 100.0 + i) for i in range(12)]
    assert [d.station for d in panel_for(cand, 100.0)] == [f"S{i}" for i in range(MEMBERS_PER_QUANTITY)]


def test_a_station_with_no_record_in_either_quantity_is_not_a_member():
    cand = [_cand("RECORD", 100.0), _cand("NONE", 100.0)]
    assert [d.station for d in panel_for(cand, 100.0, measures={"RECORD": {"discharge"}})] == ["RECORD"]


# ------------------------------------------------------------------ the roster flag no longer decides anything

def _river(n=5, length=5_000.0):
    nodes = [N(f"s{i}", 100 + i, length_m=length) for i in range(n)]
    return g(nodes, [E(f"s{i}", f"s{i+1}") for i in range(n - 1)]), {f"s{i}": "gnis:river" for i in range(n)}


def _leads(panels, sec):
    return {m.station: m.lead for m in panels.members[panels.by_section[sec][0]]}


def test_the_build_takes_no_roster_flag():
    """`live` (the roster's `realtime`) is gone from the build: no caller can make it change a panel."""
    assert "live" not in inspect.signature(build).parameters


def test_the_bundle_passes_the_panel_build_no_roster_flag():
    """pipeline/deliver/bundle/build.py calls `build_panels(...)` with the water map, the continuations (T76), the water kinds (a stream
    water's polygons take its answer — test_stream_polygon_panels.py), the measures and the published areas — and
    nothing derived from `realtime`."""
    src = Path(__file__).resolve().parents[1] / "deliver" / "bundle" / "build.py"
    calls = [c for c in ast.walk(ast.parse(src.read_text(encoding="utf-8")))
             if isinstance(c, ast.Call) and getattr(c.func, "id", None) == "build_panels"]
    assert len(calls) == 1
    assert sorted(k.arg for k in calls[0].keywords) == ["continues", "kind_of", "measures", "published", "water_of"]
    assert "realtime" not in ast.unparse(calls[0])


def test_a_historic_own_gauge_is_a_lead_candidate_ranked_as_ever():
    """Nearest first, whether or not the roster calls it real-time: HISTORIC (2 sections up) leads s3 if it reports;
    if it is quiet the app hands the lead to LIVE, rank 2."""
    graph, water = _river()
    donors = [("HISTORIC", "s2", 40, False), ("LIVE", "s0", 40, False)]
    panels = build(graph, MODEL, donors, water_of=water)
    assert _leads(panels, "s3") == {"HISTORIC": 1, "LIVE": 2}


def test_a_historic_member_keeps_its_seat():
    """A panel member that is not real-time today is still a member: it may transmit again tomorrow."""
    graph, water = _river(n=2)
    panels = build(graph, MODEL, [("HISTORIC", "s0", 40, False), ("LIVE", "s0", 40, False)], water_of=water,
                   measures={"HISTORIC": {"discharge"}, "LIVE": {"discharge"}})
    assert sorted(_leads(panels, "s1")) == ["HISTORIC", "LIVE"]


def test_the_build_keeps_the_reserve_per_quantity():
    """Ten flow gauges on tributaries of s1 and four level gauges: the panel holds the best eight flow and all four
    level, so Flow can lose a few to silence and still answer."""
    graph, water = _river(n=2)
    donors, measures = [], {}
    for i in range(14):
        graph.nodes[f"t{i}"] = N(f"t{i}", 100, blk=f"T{i}", length_m=1_000.0)
        graph.edges.append(E(f"t{i}", "s1"))
        st = f"F{i}" if i < 10 else f"L{i}"
        donors.append((st, f"t{i}", 3 + i, False))     # longer record, heavier: F9 is the best
        measures[st] = {"discharge"} if i < 10 else {"level"}
    panels = build(graph, MODEL, donors, measures=measures)
    got = _leads(panels, "s1")
    assert sorted(got) == sorted([f"F{i}" for i in range(2, 10)] + ["L10", "L11", "L12", "L13"])


# ------------------------------------------------------------------ the read-time choice, pipeline mirror

def test_a_silent_member_re_weights_the_rest():
    """A quiet member is a vote of weight 0 (no reading in the quantity asked for): the rest share the answer."""
    w = [weight_of(1.0, "up", 40), weight_of(0.5, "up", 40), weight_of(0.1, "up", 40)]
    assert lead_shares([w[0], 0.0, w[2]], None) == pytest.approx([w[0] / (w[0] + w[2]), 0.0, w[2] / (w[0] + w[2])])
    assert combine([(0.3, w[0]), (0.5, w[2])]) == combine([(0.3, w[0]), (0.9, 0.0), (0.5, w[2])])


def test_the_own_lead_moves_to_the_next_own_gauge_when_the_first_goes_silent():
    assert leader_of([1, 2, 0], [1.0, 1.0, 1.0]) == 0
    assert leader_of([1, 2, 0], [0.0, 1.0, 1.0]) == 1
    assert lead_shares([0.0, 1.0, 1.0], 1) == pytest.approx([0.0, 0.7, 0.3])


def test_all_silent_says_nothing():
    assert combine([(0.3, 0.0), (0.5, 0.0)]) is None
    assert leader_of([1, 2], [0.0, 0.0]) is None
    assert lead_shares([0.0, 0.0], None) == [0.0, 0.0]


# ------------------------------------------------------------------ mutation pins

def _mutant(fn, old, new, module=P):
    src = textwrap.dedent(inspect.getsource(fn))
    assert src.count(old) == 1, f"{old!r} moved; update this pin"
    ns = dict(vars(module))
    exec(compile(src.replace(old, new), module.__file__, "exec"), ns)
    return ns[fn.__name__]


def test_the_per_quantity_choice_is_what_holds_the_flow_seat():
    """Pinned: rank across both quantities at once (the old overall cap) and the flow gauge loses its seat."""
    mutant = _mutant(panel_for, "if measures is None or q in measures.get(d.station, ())", "if True")
    cand = [_cand(f"L{i}", 100.0) for i in range(MEMBERS_PER_QUANTITY)] + [_cand("FLOW", 900.0)]
    measures = {**{f"L{i}": {"level"} for i in range(MEMBERS_PER_QUANTITY)}, "FLOW": {"discharge"}}
    assert "FLOW" in [d.station for d in panel_for(cand, 100.0, measures=measures)]
    assert "FLOW" not in [d.station for d in mutant(cand, 100.0, measures=measures)]


def test_the_reserve_depth_is_what_holds_the_eight():
    mutant = _mutant(panel_for, "[:MEMBERS_PER_QUANTITY]", "[:4]")
    cand = [_cand(f"S{i}", 100.0 + i) for i in range(12)]
    assert len(panel_for(cand, 100.0)) == MEMBERS_PER_QUANTITY
    assert len(mutant(cand, 100.0)) == 4
