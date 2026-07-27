"""Tributary reachability tests (03 S6, 08): lake inlets/outlets, lake tributaries (dropping the
through-mainstem), and range selection for 'X from lake A to lake C'. Synthetic, no gpkg.

The fixture is a mainstem X threading three lakes A, B, C:

    mouth 0 ── X ──[A]── X ──[B]── X ──[C]── X ── headwaters 300
                 ▲tSA      ▲tSB      ▲tSC          (side creeks into each lake)

Each lake gets one side creek (tSA/tSB/tSC) plus the through-river inflow/outflow.
"""

from shapely.geometry import LineString

from stream_sections import cutting
from stream_sections.blk_chains import FidRow, build_blk_chains
from stream_sections.graph import build_stream_graph
from stream_sections.tributaries import (lake_inlets, lake_outlets, lake_tributaries,
                                         sections_in_reach, tributary_node_ids)


def _fid(fid, blk, wsc, coords, down_m, up_m, wbk="", gnis_name=""):
    geom = LineString(coords)
    dn, un = cutting.blk_endpoints(geom)
    return FidRow(fid=fid, blk=blk, wsc=wsc, edge_type="1000", wbk=wbk,
                  gnis_id="9" if gnis_name else "", gnis_name=gnis_name,
                  stream_order=1, stream_magnitude=1, down_m=down_m, up_m=up_m,
                  geometry=geom, down_node=dn, up_node=un)


def _three_lake_river():
    """X (blk X, mouth 0 -> source 300) threads lakes A[50,100], B[150,200], C[250,300]. Each
    lake is two fids so it has an interior node where a non-lake side creek attaches."""
    fids = [
        _fid("X0", "X", "100", [(0, 0), (50, 0)], 0, 50, gnis_name="X River"),           # below A
        _fid("XA1", "X", "100", [(50, 0), (70, 0)], 50, 70, wbk="A", gnis_name="X River"),
        _fid("XA2", "X", "100", [(70, 0), (100, 0)], 70, 100, wbk="A", gnis_name="X River"),
        _fid("X1", "X", "100", [(100, 0), (150, 0)], 100, 150, gnis_name="X River"),      # A->B
        _fid("XB1", "X", "100", [(150, 0), (170, 0)], 150, 170, wbk="B", gnis_name="X River"),
        _fid("XB2", "X", "100", [(170, 0), (200, 0)], 170, 200, wbk="B", gnis_name="X River"),
        _fid("X2", "X", "100", [(200, 0), (250, 0)], 200, 250, gnis_name="X River"),      # B->C
        _fid("XC1", "X", "100", [(250, 0), (270, 0)], 250, 270, wbk="C", gnis_name="X River"),
        _fid("XC2", "X", "100", [(270, 0), (300, 0)], 270, 300, wbk="C", gnis_name="X River"),
        # a non-lake side creek attached at each lake's interior node (70/170/270)
        _fid("SA", "SA", "100-1", [(70, 0), (70, 40)], 0, 40, gnis_name="Creek A"),
        _fid("SB", "SB", "100-2", [(170, 0), (170, 40)], 0, 40, gnis_name="Creek B"),
        _fid("SC", "SC", "100-3", [(270, 0), (270, 40)], 0, 40, gnis_name="Creek C"),
    ]
    lake_kind = {"A": "lake", "B": "lake", "C": "lake"}
    names = {"A": "Lake A", "B": "Lake B", "C": "Lake C"}
    return build_stream_graph(build_blk_chains(fids, lake_kind), fids, lake_kind=lake_kind,
                              lake_names=names)


def test_lake_inlets_and_outlets():
    g = _three_lake_river()
    # Flow runs source(300) -> mouth(0), so Lake B[150,200] takes inflow from the upstream B->C
    # piece X:200 + Creek B, and drains OUT to the downstream A->B piece X:100.
    assert lake_inlets(g, "lake:B") == {"X:200", "SB:0"}
    assert lake_outlets(g, "lake:B") == {"X:100"}


def test_lake_tributaries_drop_the_mainstem():
    g = _three_lake_river()
    # "tributaries of Lake B" = Creek B only, NOT the through-river (X:200) or anything upstream.
    assert lake_tributaries(g, "lake:B") == {"SB:0"}
    # sanity: the raw inlets include the through-mainstem inflow, which lake_tributaries drops.
    assert "X:200" in lake_inlets(g, "lake:B")


def test_range_reach_from_lake_A_to_lake_C_spans_lake_B():
    g = _three_lake_river()
    # 'X River from Lake A to Lake C': the mainstem pieces between A's upper bound and C's lower
    # bound — spanning Lake B. Bound measures come from the pieces' structured bounds.
    a_upper = g.nodes["X:100"].lower_bound.route_measure     # X:100 starts at Lake A (=100)
    c_lower = g.nodes["X:200"].upper_bound.route_measure     # X:200 ends at Lake C (=250)
    reach = sections_in_reach(g, "X", a_upper, c_lower)
    assert reach == {"X:100", "X:200"}                       # the A->B and B->C mainstem pieces
    # and their location identifiers read naturally:
    assert g.nodes["X:100"].location_identifier == "between Lake A and Lake B"
    assert g.nodes["X:200"].location_identifier == "between Lake B and Lake C"


def test_full_closure_reaches_through_lakes():
    g = _three_lake_river()
    # the lowest mainstem piece's guarded closure includes every lake + side creek + upper piece.
    anc = tributary_node_ids(g, "X:0")
    assert {"lake:A", "lake:B", "lake:C", "SA:0", "SB:0", "SC:0"} <= anc
