"""The length cap: cut a too-long section at its own confluences, or leave it alone.

Synthetic — a straight mainstem with tributaries at known measures — because the thing under
test is the CHOICE of junction, and a real graph makes that choice unreadable.
"""

import pytest

from pipeline.common.models import (AnchorType, FlowEdge, NodeKind, StreamGraph, StreamNode)
from pipeline.atlas.splits.length_splits import (DEFAULT_CAP_M, MIN_PIECE_M, _cuts_in,
                                                 junction_cuts)


def _graph(length_m: float, tribs, blk="X"):
    """One mainstem 0..length_m, with `tribs` = [(measure, name), ...] joining it."""
    g = StreamGraph()
    main = f"{blk}:0"
    g.nodes[main] = StreamNode(node_id=main, kind=NodeKind.stream, blk=blk, wsc="100",
                               display_name="X River", down_m=0.0, up_m=length_m,
                               length_m=length_m)
    for i, (m, name) in enumerate(tribs):
        tid = f"T{i}:0"
        g.nodes[tid] = StreamNode(node_id=tid, kind=NodeKind.stream, blk=f"T{i}",
                                  wsc=f"100-{i}", display_name=name, up_m=100.0, length_m=100.0)
        g.edges.append(FlowEdge(from_node=tid, to_node=main, at_measure=m))
    return g, main


def test_a_section_inside_the_cap_is_left_alone():
    g, _ = _graph(9_000, [(3_000, "Mid Creek")])
    assert junction_cuts(g, cap_m=10_000) == []


def test_a_long_section_is_cut_at_its_confluence():
    g, _ = _graph(30_000, [(12_000, "Mid Creek")])
    cuts = junction_cuts(g, cap_m=10_000)
    assert [c.route_measure for c in cuts] == [12_000]
    # The boundary is a CONFLUENCE, not a generic split — the map and the location text both
    # read the kind, and "downstream of Mid Creek" is only true because the cut is one.
    assert cuts[0].anchor_type is AnchorType.confluence
    assert cuts[0].label == "Mid Creek"
    assert cuts[0].split_id == "length:X:12000"


def test_a_long_section_with_no_confluence_is_left_long():
    """The out-of-BC case: nothing is known to change along it, so nothing is claimed."""
    g, _ = _graph(300_000, [])
    assert junction_cuts(g, cap_m=25_000) == []


def test_it_takes_the_last_junction_inside_the_cap_not_the_first():
    """The cap is a ceiling, not a target — the fewest cuts that do the job."""
    g, _ = _graph(30_000, [(2_000, ""), (5_000, ""), (9_000, ""), (18_000, "")])
    cuts = [c.route_measure for c in junction_cuts(g, cap_m=10_000)]
    assert cuts == [9_000, 18_000]


def test_a_named_tributary_wins_over_a_nearer_unnamed_one():
    """A cut that can be described is worth more than 800 m of extra length."""
    g, _ = _graph(30_000, [(8_000, "Sloquet Creek"), (8_800, "")])
    cuts = junction_cuts(g, cap_m=10_000)
    assert [(c.route_measure, c.label) for c in cuts][0] == (8_000, "Sloquet Creek")


def test_a_named_tributary_too_close_to_the_start_does_not_win():
    """Preferring a describable cut must not shred the river into short pieces: below half
    the cap the length wins, and the near named junction is passed over."""
    g, _ = _graph(30_000, [(1_000, "Tiny Creek"), (9_500, "")])
    assert [c.route_measure for c in junction_cuts(g, cap_m=10_000)] == [9_500]


def test_it_overshoots_rather_than_leaving_a_long_run_undivided():
    """No junction fits inside the cap, so the first one past it is taken — one 26 km piece
    beats one 60 km piece, and there is no third option."""
    g, _ = _graph(60_000, [(26_000, "Far Creek")])
    assert [c.route_measure for c in junction_cuts(g, cap_m=10_000)] == [26_000]


def test_it_never_mints_a_stub():
    """A junction 40 m from the end would make a section too short to tap or label."""
    g, _ = _graph(30_000, [(29_960, "Nearly There Creek"), (11_000, "Real Creek")])
    cuts = [c.route_measure for c in junction_cuts(g, cap_m=10_000)]
    assert 29_960 not in cuts
    assert 11_000 in cuts


def test_a_junction_outside_the_section_is_not_interior():
    """Only confluences strictly INSIDE the piece count; one at either bound is already a
    boundary and cutting there would be a no-op that mints a zero-length section."""
    g, main = _graph(30_000, [(0.0, "Mouth Creek"), (30_000.0, "Source Creek")])
    assert junction_cuts(g, cap_m=10_000) == []


def test_lake_nodes_are_never_cut():
    g, main = _graph(30_000, [(12_000, "Mid Creek")])
    g.nodes[main] = type(g.nodes[main])(**{**g.nodes[main].__dict__, "kind": NodeKind.lake})
    assert junction_cuts(g, cap_m=10_000) == []


@pytest.mark.parametrize("cap_m", [5_000, 10_000, 25_000, DEFAULT_CAP_M])
def test_every_piece_it_produces_clears_the_floor(cap_m):
    """Whatever the cap, no cut may leave a piece shorter than MIN_PIECE_M."""
    tribs = [(float(m), "" if m % 3 else "Named Creek") for m in range(200, 80_000, 271)]
    node = _graph(80_000, tribs)[0].nodes["X:0"]
    cuts = [m for m, _ in _cuts_in(node, [(m, n) for m, n in tribs], cap_m, MIN_PIECE_M)]
    bounds = [node.down_m, *cuts, node.up_m]
    assert min(b - a for a, b in zip(bounds, bounds[1:])) >= MIN_PIECE_M


def test_the_cap_is_actually_enforced_where_junctions_allow():
    tribs = [(float(m), "") for m in range(1_000, 100_000, 1_000)]
    g, _ = _graph(100_000, tribs)
    cuts = sorted(c.route_measure for c in junction_cuts(g, cap_m=10_000))
    bounds = [0.0, *cuts, 100_000.0]
    assert max(b - a for a, b in zip(bounds, bounds[1:])) <= 10_000


def test_ids_are_stable_across_runs():
    """A section id is an ABI. The same graph must mint the same cut ids every time."""
    g, _ = _graph(60_000, [(9_000, "A Creek"), (19_000, ""), (28_000, "B Creek")])
    assert ([c.split_id for c in junction_cuts(g, cap_m=10_000)]
            == [c.split_id for c in junction_cuts(g, cap_m=10_000)])


def test_the_cuts_survive_the_sectionizer():
    """End to end: resolve, apply, and check the graph really holds the smaller pieces.

    `junction_cuts` returning the right measures is worth nothing if `split_graph_at` cannot
    land them — the two have separate notions of what a valid cut is (interior measure vs.
    findable piece), and this is where a disagreement would show.
    """
    from pipeline.atlas.splits.sectionizer import split_graph_at

    g, main = _graph(30_000, [(9_000, "A Creek"), (19_000, "B Creek")])
    cuts = junction_cuts(g, cap_m=10_000)
    split_graph_at(g, {}, cuts, None)

    pieces = sorted((n for n in g.nodes.values()
                     if n.blk == "X" and n.kind is NodeKind.stream),
                    key=lambda n: n.down_m)
    assert [(p.down_m, p.up_m) for p in pieces] == [(0, 9_000), (9_000, 19_000),
                                                    (19_000, 30_000)]
    # Each new boundary names the tributary that made it, which is what turns into
    # "downstream of A Creek" in the section's location text.
    assert pieces[0].upper_bound.label == "A Creek"
    assert pieces[1].lower_bound.label == "A Creek"
    assert pieces[1].upper_bound.label == "B Creek"
    # Each tributary now hangs off a piece rather than the original whole — and it is the
    # piece ABOVE its own confluence, because the sectionizer moves an edge at `>= M` up.
    # Every cut this stage makes lands exactly on a confluence, so that convention fires
    # every time, which is worth stating rather than discovering: the tributary is still
    # upstream of the lower piece (via the continuation edge), so every walk reaches it, but
    # it is one hop further away than its position on the river suggests. Catchment area is
    # unaffected — it comes from the FWA magnitude repartitioned across member fids, not
    # from this edge.
    into = {e.from_node: e.to_node for e in g.edges if e.from_node in ("T0:0", "T1:0")}
    assert into == {"T0:0": "X:9000", "T1:0": "X:19000"}
    # ...and the lower piece still reaches both by walking up.
    up = {e.from_node for e in g.edges if e.to_node == "X:0"}
    assert "X:9000" in up


def test_two_tributaries_at_one_point_are_one_junction():
    """A confluence where two creeks arrive together is one place to cut, not two."""
    g, _ = _graph(30_000, [(12_000.0, "A Creek"), (12_000.3, "B Creek")])
    assert len(junction_cuts(g, cap_m=10_000)) == 1


def test_it_cuts_at_the_exact_measure_not_a_rounded_one():
    """`_repartition` splits segments on a strict `<`, so a cut a few centimetres off a
    segment boundary leaves that segment in BOTH pieces — and off in the wrong direction,
    the piece above a confluence inherits the drainage from below it."""
    g, _ = _graph(30_000, [(12_345.678, "Mid Creek")])
    assert [c.route_measure for c in junction_cuts(g, cap_m=10_000)] == [12_345.678]


def test_it_snaps_onto_the_segment_boundary_it_is_beside():
    # The junction is 0.4 m below where FWA ends the segment. Unsnapped, the downstream
    # segment straddles the cut and the upper piece takes its magnitude.
    g, main = _graph(30_000, [(11_999.6, "Mid Creek")])
    node = g.nodes[main]
    g.nodes[main] = type(node)(**{**node.__dict__, "member_fids": ("f1", "f2")})
    fx = {"f1": (0.0, 12_000.0, 3, 900), "f2": (12_000.0, 30_000.0, 2, 40)}
    assert [c.route_measure for c in junction_cuts(g, cap_m=10_000, fid_index=fx)] \
        == [12_000.0]


def test_it_does_not_snap_across_to_a_different_junction():
    """Snapping closes a centimetre of slop. A boundary 400 m away is a different place."""
    g, main = _graph(30_000, [(11_600.0, "Mid Creek")])
    node = g.nodes[main]
    g.nodes[main] = type(node)(**{**node.__dict__, "member_fids": ("f1",)})
    fx = {"f1": (0.0, 12_000.0, 3, 900)}
    assert [c.route_measure for c in junction_cuts(g, cap_m=10_000, fid_index=fx)] \
        == [11_600.0]


def test_each_piece_gets_its_own_order_and_magnitude():
    """THE POINT OF CUTTING AT A CONFLUENCE. Above the junction the river drains less
    country, and the new piece has to say so — otherwise the split has made two sections
    that answer identically and the map is no better off."""
    from pipeline.atlas.splits.sectionizer import split_graph_at

    g, main = _graph(30_000, [(12_000.0, "A Creek")])
    node = g.nodes[main]
    g.nodes[main] = type(node)(**{**node.__dict__, "member_fids": ("lo", "hi"),
                                  "stream_order": 5, "stream_magnitude": 900})
    fid_index = {"lo": (0.0, 12_000.0, 5, 900), "hi": (12_000.0, 30_000.0, 4, 40)}
    split_graph_at(g, {}, junction_cuts(g, cap_m=10_000, fid_index=fid_index), fid_index)

    lower, upper = g.nodes["X:0"], g.nodes["X:12000"]
    assert (lower.stream_order, lower.stream_magnitude) == (5, 900)
    assert (upper.stream_order, upper.stream_magnitude) == (4, 40)
    # And each keeps only the segments that are actually in it.
    assert lower.member_fids == ("lo",)
    assert upper.member_fids == ("hi",)
