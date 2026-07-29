"""Curated sectionizer + tributary roll-up (03 S5/S6, 04).

Proves the ordering requirement: splits become graph nodes BEFORE the tributary walk, so
"tributaries of X between A and B" is a walk over the A–B node. Synthetic, no gpkg.
"""

from shapely.geometry import LineString

from stream_sections import cutting
from stream_sections.anchors import resolve_split_defs
from stream_sections.blk_chains import FidRow, build_blk_chains
from stream_sections.graph import build_stream_graph
from stream_sections.models import AnchorType, SplitAnchor, SplitDef, SplitPoint
from stream_sections.sectionizer import split_graph_at
from stream_sections.tributaries import tributaries_between, tributary_node_ids


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
