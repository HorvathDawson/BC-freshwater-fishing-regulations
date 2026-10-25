"""Curated sectionizer + tributary roll-up (03 S5/S6, 04).

Proves the ordering requirement: splits become graph nodes BEFORE the tributary walk, so
"tributaries of X between A and B" is a walk over the A–B node. Synthetic, no gpkg.
"""

from shapely.geometry import LineString

from pipeline.atlas.graph import cutting
from pipeline.atlas.splits.anchors import resolve_split_defs
from pipeline.atlas.graph.blk_chains import FidRow, build_blk_chains
from pipeline.atlas.graph.graph import build_stream_graph
from pipeline.common.models import AnchorType, SplitAnchor, SplitDef, SplitPoint
from pipeline.atlas.splits.sectionizer import split_graph_at
from pipeline.atlas.graph.tributaries import tributaries_between, tributary_node_ids


def _fid(fid, blk, wsc, coords, down_m, up_m, gnis_name=""):
    geom = LineString(coords)
    dn, un = cutting.blk_endpoints(geom)
    return FidRow(fid=fid, blk=blk, wsc=wsc, edge_type="1000", wbk="",
                  gnis_id="9" if gnis_name else "", gnis_name=gnis_name,
                  stream_order=1, stream_magnitude=1, down_m=down_m, up_m=up_m,
                  geometry=geom, down_node=dn, up_node=un)


def _mainstem_with_tribs():
    """Mainstem X (blk X) mouth->source over measures 0..300. X is broken at 50/150/280 so the
    tributaries L (@50), M (@150), U (@280) join at real endpoint nodes; splits A=120, B=250
    fall mid-fid between them."""
    return [
        _fid("X1", "X", "100", [(0, 0), (50, 0)], 0, 50, gnis_name="X River"),
        _fid("X2", "X", "100", [(50, 0), (150, 0)], 50, 150, gnis_name="X River"),
        _fid("X3", "X", "100", [(150, 0), (280, 0)], 150, 280, gnis_name="X River"),
        _fid("X4", "X", "100", [(280, 0), (300, 0)], 280, 300, gnis_name="X River"),
        _fid("L1", "L", "100-1", [(50, 0), (50, 80)], 0, 80, gnis_name="Low Creek"),
        _fid("M1", "M", "100-5", [(150, 0), (150, 80)], 0, 80, gnis_name="Mid Creek"),
        _fid("U1", "U", "100-9", [(280, 0), (280, 80)], 0, 80, gnis_name="Up Creek"),
    ]


def _graph():
    fids = _mainstem_with_tribs()
    return build_stream_graph(build_blk_chains(fids, {}), fids), fids


def test_split_creates_section_nodes_and_labels():
    g, _ = _graph()
    pts = [SplitPoint("A", "X", 120.0, "", "A", AnchorType.point),
           SplitPoint("B", "X", 250.0, "", "B", AnchorType.point)]
    split_graph_at(g, {}, pts)
    xs = {nid for nid, n in g.nodes.items() if n.blk == "X"}
    assert xs == {"X:0", "X:120", "X:250"}                     # below | mid | above
    assert g.nodes["X:0"].location_identifier == "downstream of A"
    assert g.nodes["X:120"].location_identifier == "between A and B"
    assert g.nodes["X:250"].location_identifier == "upstream of B"


def test_tributaries_between_excludes_upstream_mainstem():
    g, _ = _graph()
    split_graph_at(g, {}, [SplitPoint("A", "X", 120.0, "", "A", AnchorType.point),
                           SplitPoint("B", "X", 250.0, "", "B", AnchorType.point)])
    # "tributaries of X between A and B" -> only Mid Creek, NOT the Up Creek / upper mainstem.
    assert tributaries_between(g, "X:120") == {"M:0"}
    # the full guarded closure of the lowest section is the whole upstream network.
    assert tributary_node_ids(g, "X:0") == {"X:120", "X:250", "L:0", "M:0", "U:0"}


def test_tributary_edge_reattaches_by_measure():
    g, _ = _graph()
    split_graph_at(g, {}, [SplitPoint("A", "X", 120.0, "", "A", AnchorType.point)])
    # Low Creek (@50) stays on the lower piece; Mid (@150) and Up (@280) move to the upper.
    assert "L:0" in tributary_node_ids(g, "X:0")
    assert "M:0" not in tributaries_between(g, "X:0")   # M is above the A cut now
    assert {"M:0", "U:0"} <= tributary_node_ids(g, "X:120")


def test_a_minted_boundary_beside_a_curated_split_moves_to_it():
    """A curated split a few metres from a MINTED boundary (the auto border split) is that boundary —
    but an authored cut never moves (owner 2026-10-24): the border cut shifts onto the authored measure,
    the authored id names it and the minted id is an alias (the Kootenay "Idaho border" point)."""
    g, _ = _graph()
    split_graph_at(g, {}, [SplitPoint("border:X:0", "X", 120.0, "", "BC boundary", AnchorType.border,
                                      source="border")])
    before = set(g.nodes)
    applied: list = []
    near = SplitPoint("idaho_border", "X", 123.0, "", "Idaho border", AnchorType.point, source="curated")
    split_graph_at(g, {}, [near], applied=applied)
    assert set(g.nodes) == before                        # no new node: the 120 cut moved
    assert applied[0].picked_up is False and applied[0].route_measure == 123.0
    lo, hi = g.nodes["X:0"], g.nodes["X:120"]
    assert (lo.up_m, hi.down_m) == (123.0, 123.0) and lo.length_m == 123.0
    b = lo.upper_bound
    assert b.boundary_id == "split:idaho_border" and b.label == "Idaho border" and b.route_measure == 123.0
    assert "split:border:X:0" in b.aliases and hi.lower_bound == b
    assert "M:0" in tributaries_between(g, "X:120"), "Mid Creek (@150) stays above"


def test_shifting_a_cut_rehomes_geometry_and_the_edges_it_passes():
    """The minted cut at 500 moves to the authored 497: the 3 m of line and a tributary joining at
    498 go to the upper piece, the continuation edge moves with the cut, each piece keeps its own
    attributes (an out-of-B.C. flag stays on its side)."""
    from pipeline.common.models import BoundaryKind, FlowEdge, SectionBoundary
    border = SectionBoundary("split:border:1:500", BoundaryKind.border, 500.0, "BC boundary",
                             source="border")
    g = _line((0, 500, None, border), (500, 1000, border, None))
    g.nodes["1:500"] = __import__("dataclasses").replace(g.nodes["1:500"], out_of_bc=True)
    g.nodes["T:0"] = __import__("dataclasses").replace(g.nodes["1:0"], node_id="T:0", blk="T",
                                                       lower_bound=None, upper_bound=None)
    g.edges = [FlowEdge("1:500", "1:0", 500.0, 500.0, 0.0, kind="continuation"),
               FlowEdge("T:0", "1:0", 498.0, 498.0, 0.0)]
    geoms = {"1:0": LineString([(0, 0), (500, 0)]), "1:500": LineString([(500, 0), (1000, 0)])}
    split_graph_at(g, geoms, [_p("idaho_border", 497.0)])
    assert _pieces(g) == [(0.0, 497.0), (497.0, 1000.0)]
    assert geoms["1:0"].length == 497.0 and geoms["1:500"].length == 503.0
    assert g.edges[0].at_measure == 497.0 and (g.edges[0].x, g.edges[0].y) == (497.0, 0.0)
    assert g.edges[1].to_node == "1:500", "the tributary at 498 is above the cut now"
    assert g.nodes["1:500"].out_of_bc and not g.nodes["1:0"].out_of_bc
    assert _at(g, 497)[1:] == ("split:idaho_border", "idaho_border", ("split:border:1:500",))


def test_a_far_split_still_cuts():
    """Beyond `AUTHORED_MERGE_M` a curated split cuts normally (no false pickup)."""
    g, _ = _graph()
    split_graph_at(g, {}, [SplitPoint("A", "X", 120.0, "", "A", AnchorType.point)])
    applied: list = []
    far = SplitPoint("C", "X", 250.0, "", "C", AnchorType.point)
    split_graph_at(g, {}, [far], applied=applied)
    assert "X:250" in g.nodes and applied[0].picked_up is False


def test_anchor_resolves_point_to_measure():
    _, fids = _graph()
    chains = build_blk_chains(fids, {})
    sd = SplitDef(id="A", anchor=SplitAnchor(type=AnchorType.point, coord=(120.0, 0.0)), blk="X")
    pts = resolve_split_defs([sd], chains)
    assert len(pts) == 1
    assert pts[0].blk == "X" and abs(pts[0].route_measure - 120.0) < 1e-6


def test_anchor_point_proximity_gate_skips_far_blk():
    _, fids = _graph()
    chains = build_blk_chains(fids, {})
    # target the whole WSC family but place the cut only near X; the far side-creeks are skipped.
    sd = SplitDef(id="A", anchor=SplitAnchor(type=AnchorType.point, coord=(120.0, 0.0)),
                  wsc="100", proximity_m=10.0)
    pts = resolve_split_defs([sd], chains)
    assert [p.blk for p in pts] == ["X"]     # only the blue line within 10 m of the cut


# --------------------------------------------------------------------------- #
# Outgoing edges must re-home on a split, like incoming ones
# --------------------------------------------------------------------------- #

def _split_env():
    """A 1,000 m mainstem piece with a side channel leaving it at 800 m and rejoining at 700 m."""
    from shapely.geometry import LineString
    from pipeline.common.models import AnchorType, FlowEdge, NodeKind, SplitPoint, StreamGraph, StreamNode

    g = StreamGraph()
    g.nodes["M:0"] = StreamNode(node_id="M:0", kind=NodeKind.stream, blk="M",
                                down_m=0.0, up_m=1000.0, length_m=1000.0, display_name="River")
    g.nodes["B:0"] = StreamNode(node_id="B:0", kind=NodeKind.stream, blk="B",
                                down_m=0.0, up_m=120.0, length_m=120.0, display_name="River")
    # mainstem -> braid, leaving at 800 m along M (x=800 on a straight line)
    g.edges.append(FlowEdge(from_node="M:0", to_node="B:0", at_measure=120.0, x=800.0, y=0.0))
    # braid -> mainstem, rejoining at 700 m along M
    g.edges.append(FlowEdge(from_node="B:0", to_node="M:0", at_measure=700.0, x=700.0, y=0.0))
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    geoms = {"M:0": LineString([(0, 0), (1000, 0)]), "B:0": LineString([(800, 0), (750, 40), (700, 0)])}
    sp = SplitPoint(split_id="cut", blk="M", route_measure=500.0, fid="",
                    label="Cut", anchor_type=AnchorType.point)
    return g, geoms, sp


def test_outgoing_edge_moves_to_the_upper_piece():
    """The braid leaves at 800 m, above a cut at 500 m — so the edge must leave the UPPER piece.
    Left on the lower piece, the braid's two ends straddle a cut it is nowhere near."""
    from pipeline.atlas.splits.sectionizer import split_graph_at

    g, geoms, sp = _split_env()
    split_graph_at(g, geoms, [sp])
    out = [e for e in g.edges if e.to_node == "B:0"]
    assert len(out) == 1
    assert out[0].from_node == "M:500", f"outgoing edge stranded on {out[0].from_node}"


def test_incoming_edge_still_moves_by_at_measure():
    from pipeline.atlas.splits.sectionizer import split_graph_at

    g, geoms, sp = _split_env()
    split_graph_at(g, geoms, [sp])
    back = [e for e in g.edges if e.from_node == "B:0"]
    assert len(back) == 1 and back[0].to_node == "M:500"


def test_both_ends_of_the_braid_name_the_same_piece():
    """The whole point: a channel entirely above the cut must not appear to span it."""
    from pipeline.atlas.splits.sectionizer import split_graph_at

    g, geoms, sp = _split_env()
    split_graph_at(g, geoms, [sp])
    ends = {e.from_node for e in g.edges if e.to_node == "B:0"} | \
           {e.to_node for e in g.edges if e.from_node == "B:0"}
    assert ends == {"M:500"}


def test_an_edge_without_a_coordinate_is_left_alone():
    from pipeline.common.models import FlowEdge
    from pipeline.atlas.splits.sectionizer import split_graph_at

    g, geoms, sp = _split_env()
    g.edges[0] = FlowEdge(from_node="M:0", to_node="B:0", at_measure=120.0, x=0.0, y=0.0)
    split_graph_at(g, geoms, [sp])
    assert [e for e in g.edges if e.to_node == "B:0"][0].from_node == "M:0"


def test_split_inside_a_lake_run_is_aliased_not_dropped():
    """A dam or weir "at the outlet" projects a little way INTO the lake, so its measure lands in the
    lake run where there is no stream piece to cut. Dropping it silently left every rule that bound it
    dangling (Duncan Dam, the Mitchell Lake dam, the Babine juvenile weir). It is recorded as another
    name for the boundary that stands at that place instead."""
    from dataclasses import replace

    from pipeline.common.models import BoundaryKind, NodeKind, SectionBoundary, StreamGraph, StreamNode
    from pipeline.common.models.enums import AnchorType
    from pipeline.common.models.splits import SplitPoint
    from pipeline.atlas.splits.sectionizer import split_graph_at

    lake_bnd = SectionBoundary(boundary_id="lake:999", kind=BoundaryKind.lake,
                               route_measure=1000.0, label="Duncan Lake")
    below = StreamNode(node_id="10:0", kind=NodeKind.stream, blk="10", down_m=0.0, up_m=1000.0,
                       length_m=1000.0, upper_bound=lake_bnd)
    above = StreamNode(node_id="10:5000", kind=NodeKind.stream, blk="10", down_m=5000.0, up_m=6000.0,
                       length_m=1000.0, lower_bound=lake_bnd)
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in (below, above)}
    g.edges, g.up_adj, g.down_adj = [], {}, {}

    # the dam sits at 1100 m — inside the 1000..5000 lake run, so there is no piece to split
    sp = SplitPoint("duncan_river__duncan_dam", "10", 1100.0, "", "Duncan Dam", AnchorType.point)
    aliased: list = []
    split_graph_at(g, {}, [sp], aliased=aliased)

    assert aliased == [("duncan_river__duncan_dam", "lake:999", 100.0)]
    assert len(g.nodes) == 2, "no piece was cut"
    # both sides of the shared boundary carry the alias, so the id resolves from either
    assert "split:duncan_river__duncan_dam" in g.nodes["10:0"].upper_bound.aliases
    assert "split:duncan_river__duncan_dam" in g.nodes["10:5000"].lower_bound.aliases
    assert g.nodes["10:0"].upper_bound.boundary_id == "lake:999", "the lake keeps its own identity"


def test_a_split_deep_inside_a_lake_is_not_aliased_to_its_edge():
    """The alias means "this is the lake's edge, just projected past it". A point 49 km into the lake
    is not that, so it stays missing and visible rather than binding to the wrong place."""
    from pipeline.common.models import BoundaryKind, NodeKind, SectionBoundary, StreamGraph, StreamNode
    from pipeline.common.models.enums import AnchorType
    from pipeline.common.models.splits import SplitPoint
    from pipeline.atlas.splits.sectionizer import split_graph_at

    bnd = SectionBoundary(boundary_id="lake:999", kind=BoundaryKind.lake, route_measure=1000.0)
    below = StreamNode(node_id="10:0", kind=NodeKind.stream, blk="10", down_m=0.0, up_m=1000.0,
                       length_m=1000.0, upper_bound=bnd)
    above = StreamNode(node_id="10:90000", kind=NodeKind.stream, blk="10", down_m=90000.0,
                       up_m=91000.0, length_m=1000.0, lower_bound=bnd)
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in (below, above)}
    g.edges, g.up_adj, g.down_adj = [], {}, {}

    sp = SplitPoint("far_away", "10", 50000.0, "", "Far Away", AnchorType.point)
    aliased: list = []
    split_graph_at(g, {}, [sp], aliased=aliased)
    assert aliased == [], "50 km from the nearest boundary is not the same place"


def test_a_split_in_a_non_lake_gap_is_never_aliased():
    """The narrow contract: only a LAKE run yields an alias. A gap between two pieces whose bounds are
    a confluence and a curated split is not the same place at all, and attaching the split to one of
    them would create a confident wrong binding — worse than a visibly missing one."""
    from pipeline.common.models import BoundaryKind, NodeKind, SectionBoundary, StreamGraph, StreamNode
    from pipeline.common.models.enums import AnchorType
    from pipeline.common.models.splits import SplitPoint
    from pipeline.atlas.splits.sectionizer import split_graph_at

    lo = SectionBoundary(boundary_id="split:some_confluence", kind=BoundaryKind.confluence,
                         route_measure=1000.0, label="Some Creek")
    hi = SectionBoundary(boundary_id="split:other_cut", kind=BoundaryKind.split,
                         route_measure=2000.0, label="Other Cut")
    below = StreamNode(node_id="10:0", kind=NodeKind.stream, blk="10", down_m=0.0, up_m=1000.0,
                       length_m=1000.0, upper_bound=lo)
    above = StreamNode(node_id="10:2000", kind=NodeKind.stream, blk="10", down_m=2000.0, up_m=3000.0,
                       length_m=1000.0, lower_bound=hi)
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in (below, above)}
    g.edges, g.up_adj, g.down_adj = [], {}, {}

    sp = SplitPoint("in_the_gap", "10", 1500.0, "", "In The Gap", AnchorType.point)
    aliased: list = []
    split_graph_at(g, {}, [sp], aliased=aliased)
    assert aliased == []
    assert g.nodes["10:0"].upper_bound.aliases == ()
    assert g.nodes["10:2000"].lower_bound.aliases == ()


def test_alias_attaches_to_the_nearer_lake_edge():
    """A lake has two bounds. Aliasing both would give the split two route measures and make every
    reach that binds it ambiguous, so only the edge the point actually sits against gets the name."""
    from pipeline.common.models import BoundaryKind, NodeKind, SectionBoundary, StreamGraph, StreamNode
    from pipeline.common.models.enums import AnchorType
    from pipeline.common.models.splits import SplitPoint
    from pipeline.atlas.splits.sectionizer import split_graph_at

    down_edge = SectionBoundary(boundary_id="lake:7", kind=BoundaryKind.lake, route_measure=1000.0)
    up_edge = SectionBoundary(boundary_id="lake:7", kind=BoundaryKind.lake, route_measure=4000.0)
    below = StreamNode(node_id="10:0", kind=NodeKind.stream, blk="10", down_m=0.0, up_m=1000.0,
                       length_m=1000.0, upper_bound=down_edge)
    above = StreamNode(node_id="10:4000", kind=NodeKind.stream, blk="10", down_m=4000.0, up_m=5000.0,
                       length_m=1000.0, lower_bound=up_edge)
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in (below, above)}
    g.edges, g.up_adj, g.down_adj = [], {}, {}

    # 3800 is nearest the UPSTREAM edge (200 m) rather than the downstream one (2800 m)
    sp = SplitPoint("upper_outlet", "10", 3800.0, "", "Upper Outlet", AnchorType.point)
    aliased: list = []
    split_graph_at(g, {}, [sp], aliased=aliased)
    assert aliased == [("upper_outlet", "lake:7", 200.0)]
    assert g.nodes["10:4000"].lower_bound.aliases == ("split:upper_outlet",)
    assert g.nodes["10:0"].upper_bound.aliases == (), "the far edge must not also claim the split"


def test_a_caller_that_did_not_ask_gets_no_aliases():
    """`split_graph_at` is shared by the CURATED pass and the BORDER pass. Border splits mint
    `border:{blk}:{m}` ids that no rule ever binds, so aliasing them onto lakes is pure noise in the
    registry. Only a caller that passes an `aliased` collector opts in."""
    from pipeline.common.models import BoundaryKind, NodeKind, SectionBoundary, StreamGraph, StreamNode
    from pipeline.common.models.enums import AnchorType
    from pipeline.common.models.splits import SplitPoint
    from pipeline.atlas.splits.sectionizer import split_graph_at

    lake = SectionBoundary(boundary_id="lake:999", kind=BoundaryKind.lake, route_measure=1000.0)
    below = StreamNode(node_id="10:0", kind=NodeKind.stream, blk="10", down_m=0.0, up_m=1000.0,
                       length_m=1000.0, upper_bound=lake)
    above = StreamNode(node_id="10:5000", kind=NodeKind.stream, blk="10", down_m=5000.0, up_m=6000.0,
                       length_m=1000.0, lower_bound=lake)
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in (below, above)}
    g.edges, g.up_adj, g.down_adj = [], {}, {}

    sp = SplitPoint("border:10:0", "10", 1100.0, "", "border", AnchorType.point)
    split_graph_at(g, {}, [sp])                     # no `aliased` -> no opt-in
    assert g.nodes["10:0"].upper_bound.aliases == ()
    assert g.nodes["10:5000"].lower_bound.aliases == ()


# ------------------------------------------------------------------------------------------
# ONE PLACEMENT PASS (T179) — collect every position on a line, then place; order never matters
# ------------------------------------------------------------------------------------------
def _line(*pieces):
    """Blue line "1" made of `(down, up, lower_bound, upper_bound)` stream pieces, no edges."""
    from pipeline.common.models import NodeKind, StreamGraph, StreamNode
    g = StreamGraph()
    g.nodes = {f"1:{int(d)}": StreamNode(node_id=f"1:{int(d)}", kind=NodeKind.stream, blk="1",
                                         down_m=float(d), up_m=float(u), length_m=float(u - d),
                                         lower_bound=lb, upper_bound=ub)
               for d, u, lb, ub in pieces}
    g.edges, g.up_adj, g.down_adj = [], {}, {}
    return g


def _p(sid, m, source="curated", label=None, anchor=AnchorType.point, **kw):
    return SplitPoint(split_id=sid, blk="1", route_measure=float(m), fid="", label=label or sid,
                      anchor_type=anchor, source=source, **kw)


def _lake(m, wbk="329016196", label="Sproat Lake"):
    from pipeline.common.models import BoundaryKind, SectionBoundary
    return SectionBoundary(boundary_id=f"lake:{wbk}", kind=BoundaryKind.lake, route_measure=float(m),
                           label=label)


def _bounds(g):
    """Every boundary on line 1: {(measure, id, label, aliases)}."""
    return {(round(b.route_measure, 3), b.boundary_id, b.label, tuple(b.aliases or ()))
            for n in g.nodes.values() if n.blk == "1"
            for b in (n.lower_bound, n.upper_bound) if b is not None}


def _at(g, m):
    got = {b for b in _bounds(g) if abs(b[0] - m) < 1e-6}
    assert len(got) == 1, got
    return next(iter(got))


def _pieces(g):
    return sorted((n.down_m, n.up_m) for n in g.nodes.values() if n.blk == "1")


def test_sproat_a_gauge_half_a_metre_from_a_lake_edge_keeps_the_lake_name():
    """`gauge__08HB008` sat 0.5 m from the Sproat Lake edge; taking the edge cost
    `sproat_river__sproat_lake` the boundary it resolves through. A minted id never relabels a name:
    the edge keeps id, kind and label, the station joins as an alias — at an end and in the interior."""
    g = _line((0, 1000, None, _lake(1000)))
    split_graph_at(g, {}, [_p("gauge__08HB008", 999.5, source="gauge")])
    assert _pieces(g) == [(0.0, 1000.0)], "no sliver cut"
    assert _at(g, 1000) == (1000.0, "lake:329016196", "Sproat Lake", ("split:gauge__08HB008",))

    kid = _lake(500, "329385415", "Kidprice Lake")
    g = _line((0, 500, None, kid), (800, 1000, _lake(800, "329385415", "Kidprice Lake"), None))
    split_graph_at(g, {}, [_p("gauge__08ED001", 501.0, source="gauge")])
    assert _at(g, 500)[1:3] == ("lake:329385415", "Kidprice Lake")
    assert "split:gauge__08ED001" in _at(g, 500)[3]


def test_spuzzum_an_mu_line_on_a_confluence_keeps_both_names():
    """The Region 2/3 boundary on the Fraser lands exactly on the Spuzzum Creek confluence. Both are
    authored and both bind: one boundary, the one standing keeps the name, the other is an alias —
    and a displaced split carries no `label:` (its ref already is its name)."""
    from pipeline.common.models import BoundaryKind, SectionBoundary
    creek = SectionBoundary("split:fraser_river__spuzzum_creek_into_fraser_river",
                            BoundaryKind.confluence, 500.0, "Spuzzum Creek → Fraser River",
                            source="curated")
    g = _line((0, 500, None, creek), (500, 1000, creek, None))
    split_graph_at(g, {}, [_p("fraser_river__region_2_3_boundary", 500.0, anchor=AnchorType.mu_boundary)])
    b = _at(g, 500)
    assert b[1] == "split:fraser_river__spuzzum_creek_into_fraser_river"
    assert b[3] == ("split:fraser_river__region_2_3_boundary",)

    # both new, in one call, a hair apart (the Thompson mouth authored on both waters): one spot
    g = _line((0, 1000, None, None))
    conflicts: list = []
    split_graph_at(g, {}, [_p("b_region", 500.02), _p("a_creek", 500.0)], conflicts=conflicts)
    assert _pieces(g) == [(0.0, 500.0), (500.0, 1000.0)] and conflicts == []
    assert _at(g, 500)[1:] == ("split:a_creek", "a_creek", ("split:b_region",))


def test_two_curated_cuts_20_m_apart_stay_two():
    """Both are real, however close: distance never merges two authored cuts."""
    g = _line((0, 1000, None, None))
    conflicts: list = []
    split_graph_at(g, {}, [_p("signs_upstream", 520.0), _p("bridge", 500.0)], conflicts=conflicts)
    assert _pieces(g) == [(0.0, 500.0), (500.0, 520.0), (520.0, 1000.0)]
    assert _at(g, 500)[1] == "split:bridge" and _at(g, 520)[1] == "split:signs_upstream"
    assert conflicts == []


def test_curated_cuts_too_close_for_two_stretches_are_both_cut_and_reported():
    """Under a sliver apart but not one spot: both are cut (never silently collapsed), the pair is a
    conflict, and the sliver gate stops the build on the piece between — fix the source."""
    from pipeline.atlas.splits import sliver_gate as SG
    g = _line((0, 1000, None, None))
    before = SG.short_pieces(g)
    conflicts: list = []
    split_graph_at(g, {}, [_p("a", 300.0), _p("b", 302.5)], conflicts=conflicts)
    assert _pieces(g) == [(0.0, 300.0), (300.0, 302.5), (302.5, 1000.0)]
    assert conflicts == [{"blk": "1", "kind": "authored_pair", "ids": ["split:a", "split:b"], "at_m": 300.0,
                          "distance_m": 2.5}]
    assert [s["node"] for s in SG.slivers(g, before)] == ["1:300"]


def test_a_gauge_defers_to_an_authored_cut_on_either_side():
    """The old pickup ran in measure order, so a station ABOVE a curated cut deferred and one BELOW it
    cut a stretch of its own (08MH024, 20 m below the CPR bridge at Mission). Now both join, and the
    authored name stands (the Babine signs, not `08EC013`)."""
    for gm in (480.0, 520.0, 101.0, 899.0):
        g = _line((0, 1000, None, None))
        split_graph_at(g, {}, [_p("gauge__08EC013", gm, source="gauge", label="08EC013 · Babine"),
                               _p("babine_signs", 500.0, label="signs 80 m downstream of the fence")])
        assert _pieces(g) == [(0.0, 500.0), (500.0, 1000.0)], gm
        assert _at(g, 500)[1:] == ("split:babine_signs", "signs 80 m downstream of the fence",
                                   ("split:gauge__08EC013",))


def test_a_gauge_beyond_its_merge_distance_cuts():
    from pipeline.atlas.splits.sectionizer import MERGE_M
    g = _line((0, 2000, None, None))
    split_graph_at(g, {}, [_p("signs", 500.0), _p("gauge__X", 500.0 + MERGE_M["gauge"] + 1, source="gauge")])
    assert len(_pieces(g)) == 3


def test_a_curated_split_relabels_a_standing_gauge_and_the_gauge_moves():
    """One-directional: a curator authoring a name at a minted position names it (`gauge__08EB005`
    became "above Babine River") — at the AUTHORED measure: the minted cut moves, the name never does."""
    from pipeline.common.models import BoundaryKind, SectionBoundary
    gauge = SectionBoundary("split:gauge__08EB005", BoundaryKind.split, 500.0, "08EB005 · Skeena",
                            source="gauge")
    g = _line((0, 500, None, gauge), (500, 1000, gauge, None))
    split_graph_at(g, {}, [_p("skeena_river__above_babine_river", 500.22)])
    assert _pieces(g) == [(0.0, 500.22), (500.22, 1000.0)]
    assert _at(g, 500.22)[1:] == ("split:skeena_river__above_babine_river",
                                  "skeena_river__above_babine_river", ("split:gauge__08EB005",))


def test_an_auto_point_within_a_sliver_joins_the_authored_measure():
    """Area / border / length cuts join only the same point of each other, but an AUTHORED place within
    `AUTHORED_MERGE_M` they join — at the authored measure."""
    for src in ("area", "border", "length"):
        g = _line((0, 1000, None, None))
        split_graph_at(g, {}, [_p(f"{src}:x", 503.0, source=src), _p("signs", 500.0)])
        assert _pieces(g) == [(0.0, 500.0), (500.0, 1000.0)], src
        assert _at(g, 500)[1:] == ("split:signs", "signs", (f"split:{src}:x",)), src


def test_an_auto_split_never_takes_a_bare_end():
    """A bare end has no name to lose, but a station code there would be the end's only name."""
    for src in ("gauge", "area", "border", "length"):
        g = _line((0, 1000, None, None))
        split_graph_at(g, {}, [_p(f"{src}:x", 999.0, source=src)])
        assert _pieces(g) == [(0.0, 999.0), (999.0, 1000.0)], src


def test_a_curated_split_a_sliver_from_an_end_stays_and_is_reported():
    """An authored cut never moves: 3 m from the line's source end it is cut where it stands, the pair
    is a `line_end` conflict, the sliver gate stops the build on the 3 m piece, and the proposal has
    no anchor to offer (a source end has none)."""
    from pipeline.atlas.splits import sliver_gate as SG
    from pipeline.atlas.splits.sectionizer import place_points
    g = _line((0, 1000, None, None))
    before = SG.short_pieces(g)
    conflicts, proposals = [], []
    place_points(g, [_p("talchako", 997.0)], conflicts=conflicts, proposals=proposals)
    split_graph_at(g, {}, [_p("talchako", 997.0)])
    assert _pieces(g) == [(0.0, 997.0), (997.0, 1000.0)]
    assert conflicts == [{"blk": "1", "kind": "line_end", "ids": ["end:source", "split:talchako"],
                          "at_m": 997.0, "distance_m": 3.0}]
    assert proposals == [{"split_id": "talchako", "blk": "1", "kind": "line_end", "edge": "source",
                          "distance_m": 3.0, "split_m": 997.0, "edge_m": 1000.0, "band": "0-5m",
                          "anchor": None}]
    assert [s["node"] for s in SG.slivers(g, before)] == ["1:997"]

    # at the MOUTH end the line's own confluence is the anchor offered
    g, proposals = _line((0, 1000, None, None)), []
    place_points(g, [_p("mouth_sign", 2.0)], proposals=proposals)
    assert proposals[0]["edge"] == "mouth"
    assert proposals[0]["anchor"] == {"type": "confluence", "tributary_blk": "1"}

    g = _line((0, 1000, None, None))
    split_graph_at(g, {}, [_p("talchako", 942.0)])
    assert _pieces(g) == [(0.0, 942.0), (942.0, 1000.0)], "no at_edge: an ordinary cut"

    g = _line((0, 1000, None, None))
    split_graph_at(g, {}, [_p("talchako", 942.0, at_edge=True)])
    assert _pieces(g) == [(0.0, 1000.0)] and _at(g, 1000) == (1000.0, "split:talchako", "talchako", ())


def test_stamp_an_at_edge_dam_takes_the_lake_edge_and_the_lake_still_binds():
    """The dam at Great Central Lake's outlet names the edge; the lake's ref AND its label travel as
    aliases, because a rule binds the readable id the registry spells from the label."""
    g = _line((0, 1000, None, _lake(1000, "329016195", "Great Central Lake")))
    split_graph_at(g, {}, [_p("stamp_dam", 960.0, label="Stamp dam", at_edge=True)])
    assert _pieces(g) == [(0.0, 1000.0)]
    assert _at(g, 1000) == (1000.0, "split:stamp_dam", "Stamp dam",
                            ("label:Great Central Lake", "lake:329016195"))


def test_at_edge_never_takes_another_authored_cut():
    from pipeline.common.models import BoundaryKind, SectionBoundary
    other = SectionBoundary("split:someone_elses_bridge", BoundaryKind.split, 1000.0, "Bridge",
                            source="curated")
    g = _line((0, 1000, None, other))
    split_graph_at(g, {}, [_p("my_dam", 960.0, at_edge=True)])
    assert _pieces(g) == [(0.0, 960.0), (960.0, 1000.0)]
    assert _at(g, 1000)[1] == "split:someone_elses_bridge"


def test_a_curated_split_a_sliver_from_a_lake_edge_stays_reported_and_proposed():
    """Owner 2026-10-24: the authored point stays, the pair is a `lake_edge` conflict, and — since a cut
    that close is usually meant to BE at the lake — a re-anchoring on the lake is PROPOSED (written,
    never applied). The lake keeps its edge; the 1.5 m piece between stops the sliver gate."""
    from pipeline.atlas.splits import sliver_gate as SG
    from pipeline.atlas.splits.sectionizer import place_points
    from pipeline.common.models import NodeKind, StreamNode
    lake = _lake(1000, "329016195", "Great Central Lake")
    g = _line((0, 1000, None, lake), (3000, 4000, _lake(3000, "329016195", "Great Central Lake"), None))
    g.nodes["lake:329016195"] = StreamNode(node_id="lake:329016195", kind=NodeKind.lake,
                                           wbk="329016195", gnis_id="17501")
    before = SG.short_pieces(g)
    conflicts, proposals = [], []
    pts = [_p("dam_below", 998.5), _p("weir_above", 3004.0)]
    placed = place_points(g, pts, conflicts=conflicts, proposals=proposals)
    assert sorted((p.split_id, p.route_measure, p.picked_up) for p in placed) == \
        [("dam_below", 998.5, False), ("weir_above", 3004.0, False)], "neither moves"
    assert [(c["kind"], c["ids"], c["distance_m"]) for c in conflicts] == [
        ("lake_edge", ["lake:329016195", "split:dam_below"], 1.5),
        ("lake_edge", ["lake:329016195", "split:weir_above"], 4.0)]
    assert [(p["split_id"], p["edge"], p["wbk"], p["gnis_id"], p["lake"], p["anchor"]) for p in proposals] == [
        ("dam_below", "outlet", "329016195", "17501", "Great Central Lake",
         {"type": "lake", "wbk": "329016195"}),
        ("weir_above", "inlet", "329016195", "17501", "Great Central Lake",
         {"type": "lake", "wbk": "329016195"})]
    split_graph_at(g, {}, pts)
    assert _at(g, 1000)[1:3] == ("lake:329016195", "Great Central Lake")
    assert sorted(s["node"] for s in SG.slivers(g, before)) == ["1:3000", "1:998"]


def test_authorship_is_the_source_never_the_id_spelling():
    g = _line((0, 1000, None, _lake(1000)))
    split_graph_at(g, {}, [_p("gauge__looks_minted", 999.0, source="curated")])
    assert _at(g, 999)[1] == "split:gauge__looks_minted", "a curated id spelled like a gauge is authored"
    g = _line((0, 1000, None, _lake(1000)))
    split_graph_at(g, {}, [_p("looks_curated", 999.0, source="gauge")])
    assert _at(g, 1000)[1] == "lake:329016196", "a gauge spelled like a curated id still defers"


def test_minted_ids_name_a_place_by_rank_and_join_only_the_same_point():
    """Area and border cuts are exact geometry: they join only the same point (1.5 m is two places —
    the 2026-10-06 ruling), and where they do, the border names it over a region line over a gauge."""
    g = _line((0, 1000, None, None))
    split_graph_at(g, {}, [_p("gauge__G", 400.0, source="gauge"),
                           _p("area:5", 400.0 + 1e-9, source="area", anchor=AnchorType.area_boundary)])
    assert _at(g, 400)[1] == "split:area:5" and _at(g, 400)[3] == ("split:gauge__G",)
    g = _line((0, 1000, None, None))
    split_graph_at(g, {}, [_p("area:5", 400.0, source="area", anchor=AnchorType.area_boundary),
                           _p("length:1:401", 401.5, source="length")])
    assert len(_pieces(g)) == 3


def _mixed():
    """Every family on one line with a lake run at 500..800: who meets whom is the whole matrix."""
    lake = _lake(500, "7", "Mid Lake")
    pts = [_p("a_curated", 300.0), _p("gauge__near_a", 300.4, source="gauge"),
           _p("gauge__below_lake", 499.5, source="gauge"),
           _p("b_curated", 1200.0), _p("c_curated", 1220.0), _p("gauge__near_b", 1100.0, source="gauge"),
           _p("area:5", 1500.0, source="area", anchor=AnchorType.area_boundary),
           _p("length:1:1500", 1500.0 + 1e-9, source="length"),
           _p("head_dam", 1960.0, at_edge=True), _p("d_twice", 1700.0), _p("e_twice", 1700.3)]
    return (lambda: _line((0, 500, None, lake), (800, 2000, _lake(800, "7", "Mid Lake"), None))), pts


def test_the_same_inputs_in_any_order_give_identical_boundaries():
    """The acceptance shuffle: one call, every permutation sampled, the same boundaries, pieces and
    `picked_up` record."""
    import random
    make, pts = _mixed()
    want = None
    rnd = random.Random(179)
    for _ in range(40):
        order = pts[:]
        rnd.shuffle(order)
        g, applied = make(), []
        split_graph_at(g, {}, order, applied=applied)
        got = (_bounds(g), _pieces(g),
               sorted((a.split_id, round(a.route_measure, 3), a.picked_up) for a in applied))
        if want is None:
            want = got
        assert got == want
    bounds, pieces, applied = want
    assert pieces == [(0.0, 300.0), (300.0, 500.0), (800.0, 1200.0), (1200.0, 1220.0), (1220.0, 1500.0),
                      (1500.0, 1700.0), (1700.0, 2000.0)]
    names = {b[1]: b[3] for b in bounds}
    assert names["split:a_curated"] == ("split:gauge__near_a",)
    assert (500.0, "lake:7", "Mid Lake", ("split:gauge__below_lake",)) in bounds
    assert (800.0, "lake:7", "Mid Lake", ()) in bounds
    assert names["split:b_curated"] == ("split:gauge__near_b",)
    assert names["split:area:5"] == ("split:length:1:1500",)
    assert names["split:d_twice"] == ("split:e_twice",)
    assert names["split:head_dam"] == () and "split:c_curated" in names


def test_place_points_says_where_split_graph_at_cuts():
    """`place_points` is the plan the braid sweep judges; cutting the plan cuts the same graph."""
    from pipeline.atlas.splits.sectionizer import place_points
    make, pts = _mixed()
    g1, a1 = make(), []
    split_graph_at(g1, {}, pts, applied=a1)
    g2 = make()
    placed = place_points(g2, pts)
    a2: list = []
    split_graph_at(g2, {}, placed, applied=a2)
    assert _bounds(g1) == _bounds(g2) and _pieces(g1) == _pieces(g2)
    key = lambda a: (a.split_id, round(a.route_measure, 3), a.picked_up)  # noqa: E731
    assert sorted(map(key, placed)) == sorted(map(key, a1)) == sorted(map(key, a2))


def test_across_two_passes_an_authored_point_takes_a_minted_one_within_a_sliver():
    """Passes still exist (border before curated, length after). Across them an authored point takes a
    minted boundary only within `AUTHORED_MERGE_M`, so the names agree either way round; that is why
    the build places the curated and gauge points in ONE call."""
    def run(first, second):
        g = _line((0, 1000, None, None))
        split_graph_at(g, {}, first)
        split_graph_at(g, {}, second)
        return {b[1]: b[3] for b in _bounds(g)}
    a, gz = [_p("signs", 500.0)], [_p("gauge__G", 503.0, source="gauge")]
    assert run(a, gz) == run(gz, a) == {"split:signs": ("split:gauge__G",)}


# ------------------------------------------------------------------------------------------
# WHICH CROSSING — a boundary that meets one river several times
# ------------------------------------------------------------------------------------------
def _crossing_ids(g, blk):
    out = set()
    for n in g.nodes.values():
        if getattr(n, "blk", None) != blk:
            continue
        for b in (n.lower_bound, n.upper_bound):
            if b is not None:
                out.add(b.boundary_id)
                out.update(b.aliases or ())
    return out


def test_a_boundary_crossed_three_times_names_its_first_and_last():
    """Every crossing is minted as the one id `area:5`, so a rule cannot say WHICH — and Region
    3's "Hells Gate upstream to the Region 3 boundary" runs on into Region 5 for want of a name.
    `@down` is the first crossing, `@up` the last: the bottom and top of the straddle."""
    g, _ = _graph()
    pts = [SplitPoint("area:5", "X", m, "", "R5", AnchorType.area_boundary)
           for m in (120.0, 200.0, 250.0)]
    split_graph_at(g, {}, pts)
    got = _crossing_ids(g, "X")
    assert "split:area:5@down" in got and "split:area:5@up" in got

    at = {}
    for n in g.nodes.values():
        for b in (getattr(n, "lower_bound", None), getattr(n, "upper_bound", None)):
            for a in (b.aliases or ()) if b is not None else ():
                if "@" in a:
                    at.setdefault(a, set()).add(b.route_measure)
    assert at["split:area:5@down"] == {120.0}, "the FIRST crossing, nearest the mouth"
    assert at["split:area:5@up"] == {250.0}, "the LAST crossing"
    assert 200.0 not in at["split:area:5@down"] | at["split:area:5@up"], "the middle is not an end"


def test_one_crossing_gets_no_qualified_name():
    """`area:5` is already unambiguous there, and @down/@up would be two names for one place."""
    g, _ = _graph()
    split_graph_at(g, {}, [SplitPoint("area:5", "X", 120.0, "", "R5", AnchorType.area_boundary)])
    assert not [i for i in _crossing_ids(g, "X") if "@" in i]


def test_the_aliases_add_no_cut():
    """Minted as SplitPoints they put a second cut where one already was — the zero-length piece
    that breaks both halves. As aliases the section count is untouched."""
    g, _ = _graph()
    pts = [SplitPoint("area:5", "X", m, "", "R5", AnchorType.area_boundary) for m in (120.0, 250.0)]
    split_graph_at(g, {}, pts)
    assert {nid for nid, n in g.nodes.items() if n.blk == "X"} == {"X:0", "X:120", "X:250"}


# ------------------------------------------------------------------------------------------
# T179 follow-up 2 (owner 2026-10-24): proposals reach 25 m; a merged place keeps every name
# ------------------------------------------------------------------------------------------
def test_a_lake_or_mouth_within_25_m_draws_a_proposal_and_the_cut_never_moves():
    """Within `PROPOSAL_M` of a lake edge or the line's mouth an authored cut is OFFERED the anchor, with
    its distance band; placement is untouched (it stands where it is) and only under 5 m is the pair
    also a sliver conflict. A source end offers no anchor, so past the sliver it draws nothing."""
    from pipeline.atlas.splits.sectionizer import PROPOSAL_M, place_points
    from pipeline.common.models import NodeKind, StreamNode
    assert PROPOSAL_M == 25.0

    def line():
        lake = _lake(1000, "329016195", "Great Central Lake")
        g = _line((0, 1000, None, lake), (3000, 4000, _lake(3000, "329016195", "Great Central Lake"), None))
        g.nodes["lake:329016195"] = StreamNode(node_id="lake:329016195", kind=NodeKind.lake,
                                               wbk="329016195", gnis_id="17501")
        return g
    pts = [_p("bridge_below", 988.0), _p("far_bridge", 960.0), _p("weir_above", 3003.0),
           _p("mouth_bridge", 20.0), _p("near_source", 3988.0)]
    conflicts, proposals = [], []
    placed = place_points(line(), pts, conflicts=conflicts, proposals=proposals)
    assert {p.split_id: p.route_measure for p in placed} == \
        {p.split_id: p.route_measure for p in pts}, "no cut moves"
    got = {p["split_id"]: (p["kind"], p["edge"], p["distance_m"], p["band"], p["anchor"]) for p in proposals}
    assert got == {
        "bridge_below": ("lake_edge", "outlet", 12.0, "5-25m", {"type": "lake", "wbk": "329016195"}),
        "weir_above": ("lake_edge", "inlet", 3.0, "0-5m", {"type": "lake", "wbk": "329016195"}),
        "mouth_bridge": ("line_end", "mouth", 20.0, "5-25m", {"type": "confluence", "tributary_blk": "1"}),
    }, "far_bridge (40 m) and near_source (a source end, 12 m) draw nothing"
    assert [(c["kind"], c["ids"]) for c in conflicts] == [
        ("lake_edge", ["lake:329016195", "split:weir_above"])], "only the 3 m pair is a sliver"
    g = line()
    split_graph_at(g, {}, pts)
    assert sorted({m for m, *_ in _bounds(g)}) == [20.0, 960.0, 988.0, 1000.0, 3000.0, 3003.0, 3988.0]


def test_a_merged_place_carries_every_name_and_every_id():
    """Authored + authored under 1 m + gauge + area at one place, and a cut aliased onto a lake's edge:
    the place keeps the canonical name, every other id is an alias and every other NAME is in
    `names` — whatever order the points arrive in."""
    import random
    pts = [_p("bridge", 500.0, label="Old Bridge"), _p("bridge_again", 500.4, label="Old Bridge (dup)"),
           _p("gauge__08XX001", 650.0, source="gauge", label="Test R near town"),
           _p("area:9", 500.0, source="area", label="Region 9", anchor=AnchorType.area_boundary)]
    want = ("split:bridge", "Old Bridge",
            ("split:area:9", "split:bridge_again", "split:gauge__08XX001"),
            ("Old Bridge (dup)", "Region 9", "Test R near town"))
    rng = random.Random(7)
    for _ in range(12):
        g = _line((0, 1000, None, None))
        split_graph_at(g, {}, rng.sample(pts, len(pts)))
        b = g.nodes["1:0"].upper_bound
        assert (b.boundary_id, b.label, b.aliases, b.names) == want
        assert g.nodes["1:500"].lower_bound == b

    # lake-side: a dam projecting into the lake run is the edge's alias, its name with it
    g = _line((0, 1000, None, _lake(1000)), (3000, 4000, _lake(3000), None))
    split_graph_at(g, {}, [_p("dam", 1010.0, label="Sproat dam")], aliased=[])
    edge = g.nodes["1:0"].upper_bound
    assert (edge.label, edge.aliases, edge.names) == ("Sproat Lake", ("split:dam",), ("Sproat dam",))

    # a split taking a lake's edge carries the lake's name (and the `label:` the registry binds by)
    g = _line((0, 1000, None, _lake(1000)))
    split_graph_at(g, {}, [_p("dam", 990.0, label="Sproat dam", at_edge=True)])
    edge = g.nodes["1:0"].upper_bound
    assert (edge.label, edge.names) == ("Sproat dam", ("Sproat Lake",))
    assert "label:Sproat Lake" in edge.aliases


def test_a_border_coincident_with_a_cut_keeps_its_source_and_the_cuts_name():
    """`_coincident`'s border case built a fresh boundary: it lost `source` (read back as AUTHORED) and the
    standing cut's name."""
    from pipeline.common.models import BoundaryKind, SectionBoundary
    reg = SectionBoundary("split:area:5", BoundaryKind.area, 500.0, "Region 5", source="area")
    g = _line((0, 500, None, reg), (500, 1000, reg, None))
    split_graph_at(g, {}, [_p("border:1:0", 500.0, source="border", label="B.C. border",
                              anchor=AnchorType.border)], exact=True)
    b = g.nodes["1:0"].upper_bound
    assert (b.boundary_id, b.source, b.names) == ("split:border:1:0", "border", ("Region 5",))
    assert "split:area:5" in b.aliases


def test_minted_id_reads_the_family_by_its_first_separator():
    from pipeline.atlas.splits.sectionizer import minted_id
    assert minted_id("gauge__08HB008") and minted_id("split:area:6") and minted_id("area:x__y")
    assert minted_id("border:360512399:0") and minted_id("length:356559402:23614")
    assert not minted_id("stamp_river__great_central_lake") and not minted_id("gauge_road_bridge")
