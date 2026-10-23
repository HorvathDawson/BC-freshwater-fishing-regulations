"""The panel walk reads the WATER's kind — OWNER 2026-10-22 (late), "a polygon-only slough or canal gets gauges like a
stream" (handoff/PLAN/app-integration/decisions.md).

"if a slough or canal is fully polygon only it should still be able to get gauge info of neighbours if the propagation
means it would have values if it was just a stream itself."

A slough or canal is a stream item whose pieces may be polygons — `lake:<wbk>` nodes in the graph. The walk used to
read that `NodeKind` as the water's kind: a stream item's polygon was offered no panel, ended every relationship that
crossed it (a "lake"), and a gauge standing on one was a lake gauge. Now a polygon of a STREAM item is walked, sized and
offered a panel exactly like a line in its place; a polygon of a lake item (or of no item) is still a lake.

Fixtures give a polygon the `stream_magnitude` the real graph gives it — the largest magnitude of the network lines
beneath it (`graph.build_stream_graph`), on a canal the through-line at its outlet.
"""

from __future__ import annotations

import inspect
import textwrap

import pipeline.atlas.gauges.build_panels as bp
from pipeline.atlas.gauges.build_panels import build, candidates, own_distances
from pipeline.tests.test_build_panels import AREA, MODEL, E, N, g

CANAL, UPPER, LOWER, LAKE, SLOUGH = "gnis:canal", "gnis:upper", "gnis:lower", "wbk:lake", "gnis:slough"
KINDS = {CANAL: "stream", UPPER: "stream", LOWER: "stream", LAKE: "lake", SLOUGH: "stream"}


def poly(node_id: str, mag: int = 100) -> N:
    """A polygon section: a `lake:<wbk>` node, no blue-line key, sized by the lines under it."""
    return N(node_id, stream_magnitude=mag, kind="lake", blk="", length_m=0.0)


def answers(panels) -> dict[str, tuple[tuple[tuple, ...], float | None]]:
    """`section -> ((station, role, same_river, lead) per member, target area)` — the whole answer by value."""
    return {sec: (tuple(sorted((m.station, m.role, m.same_river, m.lead) for m in panels.members[pid])), area)
            for sec, (pid, area) in panels.by_section.items()}


def _canal():
    """A polygon-only canal between two gauged lines of OTHER waters (Seton Canal between the Seton River's lines):

        upper ─▶ canal ─▶ lower          gauges U on `upper`, D on `lower`
    """
    graph = g([N("upper", 100, blk="U"), poly("canal"), N("lower", 100, blk="L")],
              [E("upper", "canal"), E("canal", "lower")])
    water_of = {"upper": UPPER, "canal": CANAL, "lower": LOWER}
    return graph, water_of, [("U", "upper", 40, False), ("D", "lower", 40, False)]


def test_a_polygon_only_canal_between_two_gauged_lines_gets_both():
    """Whole value. The canal answers like a line in its place: U above it (an upstream donor), D below it; and the
    canal no longer parts the two lines — each now hears the other's gauge, as two lines joined end to end would."""
    graph, water_of, donors = _canal()
    got = answers(build(graph, MODEL, donors, water_of=water_of, kind_of=KINDS))
    assert got == {
        "upper": ((("D", "down", False, 0), ("U", "up", True, 1)), 100.0),
        "canal": ((("D", "down", False, 0), ("U", "up", False, 0)), 100.0),
        "lower": ((("D", "up", True, 1), ("U", "up", False, 0)), 100.0),
    }


def test_before_this_ruling_the_canal_was_a_lake_crossing():
    """The same graph read by GEOMETRY (no water kinds): the canal is a lake, gets nothing, and walls the lines apart."""
    graph, water_of, donors = _canal()
    got = answers(build(graph, MODEL, donors, water_of=water_of))
    assert got == {
        "upper": ((("U", "up", True, 1),), 100.0),
        "lower": ((("D", "up", True, 1),), 100.0),
    }


def test_a_gauge_standing_on_a_stream_polygon_speaks_for_it_and_past_it():
    """A station matched to the canal's polygon is a stream gauge on the canal, not a lake level: its own section first,
    then the lines either side."""
    graph = g([N("upper", 100, blk="U"), poly("canal"), N("lower", 100, blk="L")],
              [E("upper", "canal"), E("canal", "lower")])
    got = answers(build(graph, MODEL, [("C", "canal", 40, False)],
                        water_of={"upper": UPPER, "canal": CANAL, "lower": LOWER}, kind_of=KINDS))
    assert got == {
        "upper": ((("C", "down", False, 0),), 100.0),
        "canal": ((("C", "up", True, 1),), 100.0),
        "lower": ((("C", "up", False, 0),), 100.0),
    }


def test_a_line_boxed_between_two_polygons_of_its_slough_is_reached():
    """Nicomen Slough 59620: a 57 m line between two polygons of the slough, no gauge of its own. Both gauges now walk
    through the polygons to it, on the slough's own water (same river)."""
    nodes = [N("top", 100, blk="S"), poly("p_up"), N("mid", 100, blk="S"), poly("p_low"), N("foot", 100, blk="S")]
    edges = [E("top", "p_up"), E("p_up", "mid"), E("mid", "p_low"), E("p_low", "foot")]
    water_of = {n.node_id: SLOUGH for n in nodes}
    got = answers(build(g(nodes, edges), MODEL, [("TOP", "top", 40, False), ("FOOT", "foot", 40, False)],
                        water_of=water_of, kind_of=KINDS))
    # Lead ranks are along-water distance with a polygon at 0 m: `mid` is 0 m below TOP's section and 1 km above
    # FOOT's; `p_low` is 1 km from both (the tie goes to the station id, as for any two own gauges equally placed).
    nearer_top = ((("FOOT", "down", True, 2), ("TOP", "up", True, 1)), 100.0)
    nearer_foot = ((("FOOT", "down", True, 1), ("TOP", "up", True, 2)), 100.0)
    assert got == {
        "top": nearer_top,
        "p_up": nearer_top,
        "mid": nearer_top,
        "p_low": nearer_foot,
        "foot": ((("FOOT", "up", True, 1), ("TOP", "up", True, 2)), 100.0),
    }


def test_a_true_lake_on_the_same_path_still_crosses_as_a_lake():
    """A lake item's polygon keeps lake treatment wherever it sits: it gets no panel, and everything beyond it is
    crossed (storage integrates the signal). The canal below the lake hears nothing from the gauge above it."""
    graph = g([N("upper", 100, blk="U"), poly("lake"), poly("canal"), N("lower", 100, blk="L")],
              [E("upper", "lake"), E("lake", "canal"), E("canal", "lower")])
    water_of = {"upper": UPPER, "lake": LAKE, "canal": CANAL, "lower": LOWER}
    got = answers(build(graph, MODEL, [("U", "upper", 40, False)], water_of=water_of, kind_of=KINDS))
    assert got == {"upper": ((("U", "up", True, 1),), 100.0)}
    cand = candidates(graph, AREA, [("U", "upper", 40, False)], water_of=water_of, kind_of=KINDS)
    assert {sec: [(c[0], c[5]) for c in cs] for sec, cs in cand.items()} == {
        "upper": [("U", False)], "canal": [("U", True)], "lower": [("U", True)]}


def test_a_polygon_no_water_owns_is_a_lake():
    graph = g([N("upper", 100), poly("nobody"), N("lower", 100)], [E("upper", "nobody"), E("nobody", "lower")])
    got = answers(build(graph, MODEL, [("U", "upper", 40, False)],
                        water_of={"upper": UPPER, "lower": UPPER}, kind_of=KINDS))
    assert got == {"upper": ((("U", "up", True, 1),), 100.0)}


def test_a_polygon_is_sized_by_its_own_magnitude_and_the_ratio_still_bounds_it():
    """No borrowed size: a polygon past the area ratio from the gauge is not reached, exactly as a line would not be."""
    graph = g([N("upper", 100), poly("canal", mag=100_000), N("lower", 100)],
              [E("upper", "canal"), E("canal", "lower")])
    got = answers(build(graph, MODEL, [("U", "upper", 40, False)],
                        water_of={"upper": UPPER, "canal": CANAL, "lower": LOWER}, kind_of=KINDS, max_ratio=10.0))
    assert got == {"upper": ((("U", "up", True, 1),), 100.0)}


def test_the_own_gauge_walk_passes_through_its_own_water_s_polygon():
    """The Vedder Canal does not part the Vedder from its own gauge: the polygon is walked at its graph length (0)."""
    nodes = [N("above", 100, blk="V", length_m=2_000.0), poly("canal"), N("below", 100, blk="V", length_m=3_000.0)]
    graph = g(nodes, [E("above", "canal"), E("canal", "below")])
    water_of = {"above": UPPER, "canal": UPPER, "below": UPPER}
    donors = [("V", "above", 40, False)]
    assert own_distances(graph, water_of, donors, kind_of=KINDS) == {
        "above": {"V": 0.0}, "canal": {"V": 0.0}, "below": {"V": 0.0}}
    # By geometry the polygon is a lake of another kind and the walk stops at it.
    assert own_distances(graph, water_of, donors) == {"above": {"V": 0.0}}


# ------------------------------------------------------------------ mutation pin

def test_the_water_kind_gate_is_what_holds_these_tests():
    """MUTATION PIN on `flowing`: key it on the GEOMETRY again and the canal goes grey and walls its lines apart; open
    it to any owned polygon and the true lake takes a river's flow. Both mutants must fail."""
    src = textwrap.dedent(inspect.getsource(bp.flowing))
    gate = 'kind_of.get(water) == "stream"'
    assert src.count(gate) == 1, "the water-kind gate moved; update this pin to match"

    def with_gate(replacement: str):
        ns = dict(vars(bp))
        exec(compile(src.replace(gate, replacement), bp.__file__, "exec"), ns)
        return ns["flowing"]

    graph, water_of, donors = _canal()
    lake_graph = g([N("upper", 100, blk="U"), poly("lake"), N("lower", 100, blk="L")],
                   [E("upper", "lake"), E("lake", "lower")])
    lake_water = {"upper": UPPER, "lake": LAKE, "lower": LOWER}
    lake_donors = [("U", "upper", 40, False)]

    def run(flowing):
        real = bp.flowing
        bp.flowing = flowing
        try:
            return (answers(build(graph, MODEL, donors, water_of=water_of, kind_of=KINDS)),
                    answers(build(lake_graph, MODEL, lake_donors, water_of=lake_water, kind_of=KINDS)))
        finally:
            bp.flowing = real

    canal_ok, lake_ok = run(bp.flowing)
    assert "canal" in canal_ok and lake_ok == {"upper": ((("U", "up", True, 1),), 100.0)}

    canal_geo, _ = run(with_gate("False"))                  # geometry decides: the canal is a lake again
    assert canal_geo == {"upper": ((("U", "up", True, 1),), 100.0), "lower": ((("D", "up", True, 1),), 100.0)}

    _, lake_open = run(with_gate("True"))                   # any owned polygon flows: the lake answers
    assert "lake" in lake_open and "lower" in lake_open
