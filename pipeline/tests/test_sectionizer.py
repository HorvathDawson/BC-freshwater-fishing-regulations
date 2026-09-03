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


def test_proximity_pickup_reuses_nearby_boundary():
    """A curated split within proximity_m of an EXISTING boundary reuses + relabels it instead of
    cutting a near-duplicate (how Kootenay 'Idaho border' snaps onto the auto border split)."""
    g, _ = _graph()
    split_graph_at(g, {}, [SplitPoint("A", "X", 120.0, "", "A", AnchorType.point)])
    before = set(g.nodes)
    applied: list = []
    near = SplitPoint("border_pickup", "X", 123.0, "", "Idaho border", AnchorType.point,
                      proximity_m=10.0)
    split_graph_at(g, {}, [near], proximity_pickup=True, applied=applied)
    assert set(g.nodes) == before                        # no new node — reused the 120 boundary
    assert applied[0].picked_up is True
    assert g.nodes["X:0"].upper_bound.label == "Idaho border"   # boundary relabelled
    assert g.nodes["X:120"].lower_bound.label == "Idaho border"


def test_proximity_pickup_far_split_still_cuts():
    """Outside proximity_m, a curated split cuts normally (no false pickup)."""
    g, _ = _graph()
    split_graph_at(g, {}, [SplitPoint("A", "X", 120.0, "", "A", AnchorType.point)])
    applied: list = []
    far = SplitPoint("C", "X", 250.0, "", "C", AnchorType.point, proximity_m=10.0)
    split_graph_at(g, {}, [far], proximity_pickup=True, applied=applied)
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


def test_pickup_keeps_the_id_of_the_boundary_it_reuses():
    """Two curated splits can land on the SAME measure — the Fraser's Region 2/3 MU boundary sits
    exactly on the Spuzzum Creek confluence, because the region boundary follows the creek. Pickup
    relabels the boundary it reuses, so without carrying the displaced id forward the second split
    silently erases the first, and any rule bound to the loser resolves to nothing."""
    from dataclasses import replace
    from pipeline.common.models import BoundaryKind, NodeKind, SectionBoundary, StreamGraph, StreamNode
    from pipeline.atlas.splits.sectionizer import _pickup
    from pipeline.common.models.splits import SplitPoint

    first = SectionBoundary(boundary_id="split:spuzzum", kind=BoundaryKind.confluence,
                            route_measure=500.0, label="Spuzzum Creek")
    g = StreamGraph()
    g.nodes = {
        "1:0":   StreamNode(node_id="1:0", kind=NodeKind.stream, blk="1", down_m=0.0, up_m=500.0,
                            upper_bound=first),
        "1:500": StreamNode(node_id="1:500", kind=NodeKind.stream, blk="1", down_m=500.0, up_m=900.0,
                            lower_bound=first),
    }
    from pipeline.common.models.splits import AnchorType
    sp = SplitPoint(split_id="region_2_to_3", blk="1", route_measure=500.0, fid="",
                    label="Region 2/3", anchor_type=AnchorType.point)
    assert _pickup(g, "1", sp, {"1": ["1:0", "1:500"]}) is True
    got = g.nodes["1:0"].upper_bound
    assert got.boundary_id == "split:region_2_to_3"
    assert "split:spuzzum" in (got.aliases or ()), "the displaced id must survive as an alias"
