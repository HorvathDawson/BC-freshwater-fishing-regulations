"""A cut sitting on a blue line's END (`extent._half`).

The Bella Coola's head is the confluence where the Talchako meets the Atnarko, and a curated split
picked up that head. Every cut before it had a piece of its own line on both sides; this one does
not, and both of `_by_measure`'s halves were wrong in different ways because of it.
"""

from __future__ import annotations

from pipeline.atlas.reach.extent import _by_measure, _half
from pipeline.common.models import NodeKind, StreamGraph, StreamNode
from pipeline.common.models.graph import FlowEdge

MAIN, FORK_A, FORK_B = "B", "A", "T"


def _fork_graph():
    """Two forks meeting to form a mainstem, the shape of the Atnarko + Talchako -> Bella Coola.

        A (0..800)  \
                      >--- B (0..1000), flowing down to 0
        T (0..500)  /

    The junction is B's TOP (measure 1000), which is also where the curated split lands.
    """
    g = StreamGraph()
    def node(nid, blk, lo, hi):
        return StreamNode(node_id=nid, kind=NodeKind.stream, blk=blk,
                          down_m=lo, up_m=hi, length_m=hi - lo)
    g.nodes = {
        "B:0":   node("B:0", MAIN, 0.0, 600.0),
        "B:600": node("B:600", MAIN, 600.0, 1000.0),
        "A:0":   node("A:0", FORK_A, 0.0, 800.0),
        "T:0":   node("T:0", FORK_B, 0.0, 500.0),
    }
    # flow: A -> B, T -> B, and B:600 -> B:0
    g.edges = [FlowEdge(from_node="A:0", to_node="B:600", at_measure=1000.0, x=0.0, y=0.0,
                        kind="continuation"),
               FlowEdge(from_node="T:0", to_node="B:600", at_measure=1000.0, x=0.0, y=0.0,
                        kind="tributary"),
               FlowEdge(from_node="B:600", to_node="B:0", at_measure=600.0, x=0.0, y=0.0,
                        kind="continuation")]
    for i, e in enumerate(g.edges):
        g.down_adj.setdefault(e.from_node, []).append(i)
        g.up_adj.setdefault(e.to_node, []).append(i)
    return g, set(g.nodes)


MAINSTEM = {"B:0", "B:600"}
ABOVE = {"A:0", "T:0"}


def test_the_half_above_an_end_cut_is_not_empty():
    """`_by_measure` seeds from pieces of the cut's OWN line inside the window. A cut on the line's
    top leaves that window empty, so the half came back with ZERO sections — not "nearly none", none
    — and `between(x, this)` intersected with it and bound nothing. A rule that binds nothing reads
    as "no regulation here", which is the one failure this project cannot afford."""
    g, universe = _fork_graph()
    raw, _ = _by_measure(g, universe, MAIN, 1000.0, float("inf"))
    assert raw == set(), "precondition: the measure window alone finds nothing above an end cut"

    up, _ = _half(g, universe, MAIN, 1000.0, upper=True)
    assert up == ABOVE, "the water above the head is on the next lines up, not missing"


def test_the_half_below_an_end_cut_does_not_swallow_the_far_side():
    """The mirror defect, and the one that actually kept the reach unresolved. With `outside` empty
    the braid fixpoint can only ever move a piece INWARD, so it walks across the junction and calls
    the rivers ABOVE the cut "downstream" of it. On the real corpus that put five Atnarko sections
    below the Bella Coola's head, which is upside down."""
    g, universe = _fork_graph()
    down, _ = _half(g, universe, MAIN, 1000.0, upper=False)
    assert down == MAINSTEM
    assert not (down & ABOVE), "nothing above the junction may count as below the cut"


def test_an_end_cut_halves_the_water_exactly():
    """The two halves must partition the scoped water: anything else means a piece is claimed twice
    or lost, and `between` is an intersection of halves."""
    g, universe = _fork_graph()
    up, _ = _half(g, universe, MAIN, 1000.0, upper=True)
    down, _ = _half(g, universe, MAIN, 1000.0, upper=False)
    assert up | down == universe
    assert not (up & down)


def test_an_ordinary_interior_cut_is_untouched():
    """The fallback only applies to a cut ON an end. An interior cut still has pieces of its own
    line on both sides, so it must resolve exactly as it always did."""
    g, universe = _fork_graph()
    for upper in (True, False):
        assert _half(g, universe, MAIN, 600.0, upper=upper)[0] == \
               _by_measure(g, universe, MAIN, *((600.0, float("inf")) if upper else (0.0, 600.0)))[0]


# ------------------------------------------------------------------ T833: only the cut's own point is across
MOUTH = (500.0, 500.0)


def _long_end_piece_graph():
    """The Gitnadoix's shape (T833): ONE long piece L:0 (0..5000) whose mouth at MOUTH flows into a receiver R,
    with a dead-end side channel S leaving it 2 km up and a creek C joining it 3 km up. The cut is the river's
    own mouth (an alias at m 0), so `upstream_of` it is the whole river.

        C (0..400) ─────┐ 3000
        S (0..300) <────┤ 2000   (leaves L, ends)
                        L:0 (0..5000), mouth at MOUTH ──> R:0 (0..9000), joining at R's 4000
    """
    from shapely.geometry import LineString
    g = StreamGraph()

    def node(nid, blk, lo, hi):
        return StreamNode(node_id=nid, kind=NodeKind.stream, blk=blk, down_m=lo, up_m=hi, length_m=hi - lo)
    g.nodes = {"L:0": node("L:0", "L", 0.0, 5000.0), "R:0": node("R:0", "R", 0.0, 9000.0),
               "S:0": node("S:0", "S", 0.0, 300.0), "C:0": node("C:0", "C", 0.0, 400.0)}
    x0, y0 = MOUTH
    g.edges = [FlowEdge(from_node="L:0", to_node="R:0", at_measure=4000.0, x=x0, y=y0),
               FlowEdge(from_node="L:0", to_node="S:0", at_measure=300.0, x=x0, y=y0 + 2000.0),
               FlowEdge(from_node="C:0", to_node="L:0", at_measure=3000.0, x=x0, y=y0 + 3000.0)]
    for i, e in enumerate(g.edges):
        g.down_adj.setdefault(e.from_node, []).append(i)
        g.up_adj.setdefault(e.to_node, []).append(i)
    # the end piece's geometry, mouth first (`position.attach` names the build in a real run)
    g._position_geoms = ({}, {"L:0": LineString([MOUTH, (x0, y0 + 5000.0)])})
    return g, set(g.nodes)


def test_a_cut_on_a_line_end_seeds_only_the_lines_at_its_point():
    """Only the receiver meets L at its mouth; the side channel and the creek meet it kilometres up, on the cut's
    own side. The old seed took every neighbour of the end PIECE, so `upstream_of` the mouth lost both."""
    from pipeline.atlas.reach.extent import _across_the_junction
    g, universe = _long_end_piece_graph()
    assert _across_the_junction(g, universe, "L", 0.0, upper=False) == {"R:0"}
    up, _ = _half(g, universe, "L", 0.0, upper=True)
    assert up == {"L:0", "S:0", "C:0"}
    down, _ = _half(g, universe, "L", 0.0, upper=False)
    assert down == {"R:0"}, "the receiver is still across the mouth, never upstream of it"


def test_a_cut_on_a_line_end_whole_piece_seed_is_the_mutation_pin(monkeypatch):
    """False restores the whole-piece seed: the side channel and the creek go outside (the Gitnadoix's 991)."""
    from pipeline.atlas.reach import extent as X
    monkeypatch.setattr(X, "A_CUT_ON_A_LINE_END_IS_ACROSS_ONLY_AT_ITS_POINT", False)
    g, universe = _long_end_piece_graph()
    up, _ = _half(g, universe, "L", 0.0, upper=True)
    assert up == {"L:0"}


def test_an_outgoing_edge_with_no_known_point_is_not_guessed_across():
    """Without the line's geometry and with nothing joining at the cut's measure, where an outgoing edge leaves the
    line is unknown: it is not seeded across (never a guess)."""
    from pipeline.atlas.reach.extent import _across_the_junction
    g, universe = _long_end_piece_graph()
    del g._position_geoms
    assert _across_the_junction(g, universe, "L", 0.0, upper=False) == frozenset()
