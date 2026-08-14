"""Tributary-scoped area_boundary closure split (Garibaldi Park / Pitt River shape).

Synthetic — a box 'park' outline and streams that end-in / pass-through / sit inside / sit outside.
No gpkg. Mirrors test_border.py: the SAME cut machinery, but the INSIDE pieces are flagged (a
closure gate marker), not the outside ones, and the inside pieces get the "within {park}" identifier.

        park = box(0,0,100,100)
        TH (through):    (50,-50) ▶ (50,50) ▶ (50,150)   enters y=0 (m=50), exits y=100 (m=150)
        EN (ends-in):    (30,-30) ▶ (30,30)              enters y=0 (m=30); headwaters INSIDE
        IN (wholly in):  (70,20)  ▶ (70,80)              never crosses; whole piece inside
        OUT (wholly out):(150,20) ▶ (150,80)             in WSC scope, but entirely outside
        OOS (out-scope): (40,20)  ▶ (40,80)              inside, but a DIFFERENT WSC (not a Pitt trib)
"""

from shapely.geometry import LineString, box

from pipeline.graph import cutting
from pipeline.splits.anchors import _target_blks, resolve_split_defs
from pipeline.graph.blk_chains import FidRow, build_blk_chains
from pipeline.splits.border import mark_inside_area
from pipeline.graph.graph import build_section_geometries, build_stream_graph
from pipeline.models import AnchorType, SplitAnchor, SplitDef
from pipeline.splits.sectionizer import split_graph_at

PARK = box(0, 0, 100, 100)
PARK_WSC = "100-025956"          # "Pitt River" trunk; descendants share the prefix


def _fid(fid, blk, wsc, coords):
    geom = LineString(coords)
    dn, un = cutting.blk_endpoints(geom)
    return FidRow(fid=fid, blk=blk, wsc=wsc, edge_type="1000", wbk="", gnis_id="",
                  gnis_name="", stream_order=1, stream_magnitude=1,
                  down_m=0.0, up_m=geom.length, geometry=geom, down_node=dn, up_node=un)


def _setup():
    fids = [
        _fid("TH1", "TH", PARK_WSC + "-1",   [(50, -50), (50, 50), (50, 150)]),
        _fid("EN1", "EN", PARK_WSC + "-2",   [(30, -30), (30, 30)]),
        _fid("IN1", "IN", PARK_WSC + "-3",   [(70, 20), (70, 80)]),
        _fid("OUT1", "OUT", PARK_WSC + "-4", [(150, 20), (150, 80)]),
        _fid("OOS1", "OOS", "200-999999",    [(40, 20), (40, 80)]),
    ]
    chains = build_blk_chains(fids, {})
    graph = build_stream_graph(chains, fids, {}, {})
    geoms = build_section_geometries(chains, fids, {})
    return chains, graph, geoms


def _split_def():
    return SplitDef(
        id="gari", wsc=PARK_WSC, stream_name="Pitt River", label="Garibaldi Park",
        anchor=SplitAnchor(type=AnchorType.area_boundary, area_layer="parks_bc",
                           area_name_field="PROTECTED_LANDS_NAME", area_name="Garibaldi Park",
                           wsc_descendants=True))


def _apply(chains, graph, geoms):
    sd = _split_def()
    pts = resolve_split_defs([sd], chains, area_polys={"Garibaldi Park": PARK})
    split_graph_at(graph, geoms, pts, proximity_pickup=False)
    blks = set(_target_blks(sd, chains, descendants=True))
    n = mark_inside_area(graph, geoms, PARK, "Garibaldi Park", blks=blks)
    return pts, blks, n


# ----------------------------------------------------------------- targeting / scope

def test_wsc_descendants_prefix_match():
    chains, _, _ = _setup()
    sd = _split_def()
    desc = set(_target_blks(sd, chains, descendants=True))
    assert desc == {"TH", "EN", "IN", "OUT"}          # all Pitt-WSC descendants
    exact = set(_target_blks(sd, chains, descendants=False))
    assert exact == set()                             # none have the EXACT trunk WSC (regression guard)


# ----------------------------------------------------------------- cutting

def test_area_split_points_at_every_crossing():
    chains, _, _ = _setup()
    pts = resolve_split_defs([_split_def()], chains, area_polys={"Garibaldi Park": PARK})
    by_blk = {}
    for p in pts:
        by_blk.setdefault(p.blk, []).append(round(p.route_measure))
        assert p.anchor_type == AnchorType.area_boundary
        assert p.label == "Garibaldi Park"
    assert sorted(by_blk["TH"]) == [50, 150]          # enter + exit
    assert sorted(by_blk["EN"]) == [30]               # single entry
    assert "IN" not in by_blk and "OUT" not in by_blk # no crossings
    assert "OOS" not in by_blk                        # out of WSC scope


def test_through_stream_yields_three_pieces_middle_inside():
    chains, graph, geoms = _setup()
    _apply(chains, graph, geoms)
    assert {"TH:0", "TH:50", "TH:150"} <= set(graph.nodes)
    assert graph.nodes["TH:50"].in_areas == ("Garibaldi Park",)   # the INSIDE middle piece
    assert graph.nodes["TH:0"].in_areas == ()                     # outside ends not flagged
    assert graph.nodes["TH:150"].in_areas == ()
    # geometry KEPT on every piece (unlike under-lake)
    assert all(geoms[n] is not None and not geoms[n].is_empty for n in ("TH:0", "TH:50", "TH:150"))


# ----------------------------------------------------------------- identifiers (the crux)

def test_identifiers_through_stream():
    chains, graph, geoms = _setup()
    _apply(chains, graph, geoms)
    assert graph.nodes["TH:0"].location_identifier == "downstream of Garibaldi Park"
    assert graph.nodes["TH:50"].location_identifier == "within Garibaldi Park"
    assert graph.nodes["TH:150"].location_identifier == "upstream of Garibaldi Park"


def test_identifier_ends_in_park_says_within_not_upstream():
    """The hard case: a piece from the boundary to the headwaters INSIDE the park must read
    'within', not the misleading 'upstream of' the bounds alone would give."""
    chains, graph, geoms = _setup()
    _apply(chains, graph, geoms)
    assert {"EN:0", "EN:30"} <= set(graph.nodes)
    assert graph.nodes["EN:0"].location_identifier == "downstream of Garibaldi Park"
    assert graph.nodes["EN:30"].in_areas == ("Garibaldi Park",)
    assert graph.nodes["EN:30"].location_identifier == "within Garibaldi Park"


def test_wholly_inside_tributary_flagged_uncut():
    chains, graph, geoms = _setup()
    _apply(chains, graph, geoms)
    assert "IN:0" in graph.nodes and "IN:50" not in graph.nodes   # never cut
    assert graph.nodes["IN:0"].in_areas == ("Garibaldi Park",)
    assert graph.nodes["IN:0"].location_identifier == "within Garibaldi Park"


# ----------------------------------------------------------------- scope negatives

def test_outside_tributary_untouched():
    chains, graph, geoms = _setup()
    _apply(chains, graph, geoms)
    assert graph.nodes["OUT:0"].in_areas == ()                    # in WSC scope but outside geometry
    assert graph.nodes["OUT:0"].location_identifier is None


def test_out_of_scope_wsc_not_flagged_even_if_inside():
    chains, graph, geoms = _setup()
    _apply(chains, graph, geoms)
    assert graph.nodes["OOS:0"].in_areas == ()                    # inside the box, wrong WSC -> ignored


# ----------------------------------------------------------------- determinism

def test_deterministic_across_two_builds():
    a = _setup(); _apply(*a)
    b = _setup(); _apply(*b)
    ga, gb = a[1], b[1]
    assert set(ga.nodes) == set(gb.nodes)
    assert {n: ga.nodes[n].in_areas for n in ga.nodes} == {n: gb.nodes[n].in_areas for n in gb.nodes}
