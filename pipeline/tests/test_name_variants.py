"""Name-variation application to the graph (docs/13). Synthetic, no gpkg.

Fixture: mainstem X (gnis 'Big River') threading lake W over [100,200], so it splits into a
lower piece X:0 and an upper piece X:200, plus lake:W. A separate wetland wbk 'WET' rides on
the lower piece's fids (a wetland is NOT a node/split — just an overlay via member_wbks).
"""

from shapely.geometry import LineString

from pipeline.graph import cutting
from pipeline.graph.blk_chains import FidRow, build_blk_chains
from pipeline.graph.graph import build_stream_graph
from pipeline.graph.names import _display_case, apply_name_variants


def _fid(fid, blk, wsc, coords, down_m, up_m, wbk="", gnis_name=""):
    geom = LineString(coords)
    dn, un = cutting.blk_endpoints(geom)
    return FidRow(fid=fid, blk=blk, wsc=wsc, edge_type="1000", wbk=wbk,
                  gnis_id="9" if gnis_name else "", gnis_name=gnis_name,
                  stream_order=1, stream_magnitude=1, down_m=down_m, up_m=up_m,
                  geometry=geom, down_node=dn, up_node=un)


def _graph():
    fids = [
        _fid("X0", "X", "100", [(0, 0), (100, 0)], 0, 100, wbk="WET", gnis_name="Big River"),
        _fid("XA", "X", "100", [(100, 0), (200, 0)], 100, 200, wbk="W", gnis_name="Big River"),
        _fid("X3", "X", "100", [(200, 0), (300, 0)], 200, 300, gnis_name="Big River"),
    ]
    lk = {"W": "lake"}                      # W is a lake (node+split); WET is a wetland (overlay)
    return build_stream_graph(build_blk_chains(fids, lk), fids, lake_kind=lk, lake_names={})


def test_display_flag_beats_gazette_on_reach():
    g = _graph()
    apply_name_variants(g, [{"target": {"blks": ["X"]}, "reach": {"from_m": 200},
                             "names": [{"name": "Two Forty-One Creek", "source": "gauge",
                                        "note": "gauged; above the lake", "display": True}]}])
    assert g.nodes["X:200"].display_name == "Two Forty-One Creek"   # gauge+display beats gazette
    assert g.nodes["X:0"].display_name == "Big River"               # lower piece untouched
    assert any(t.note == "gauged; above the lake" for t in g.nodes["X:200"].name_tuples)


def test_no_display_flag_keeps_gazette_searchable_variant():
    g = _graph()
    apply_name_variants(g, [{"target": {"blks": ["X"]},
                             "names": [{"name": "Old Nickname", "source": "regulation"}]}])
    assert g.nodes["X:0"].display_name == "Big River"               # gazette still displays
    assert "Old Nickname" in {t.name for t in g.nodes["X:0"].name_tuples}   # but searchable


def test_lake_wbk_target_and_shouty_casing():
    g = _graph()
    apply_name_variants(g, [{"target": {"wbks": ["W"]},
                             "names": [{"name": "UPPER FOO L.", "source": "stocking"}]}])
    assert g.nodes["lake:W"].display_name == "Upper Foo Lake"       # unnamed lake -> stocking, cased
    assert any(t.name == "UPPER FOO L." for t in g.nodes["lake:W"].name_tuples)


def test_wetland_wbk_overlays_the_through_stream_piece():
    g = _graph()
    # WET is a wetland (not a lake) -> no node/split; its name rides on the piece it flows through.
    assert "WET" in g.nodes["X:0"].member_wbks and "lake:WET" not in g.nodes
    n = apply_name_variants(g, [{"target": {"wbks": ["WET"]},
                                 "names": [{"name": "Cattail Marsh", "source": "regulation"}]}])
    assert n == 1
    assert "Cattail Marsh" in {t.name for t in g.nodes["X:0"].name_tuples}
    assert g.nodes["X:0"].display_name == "Big River"              # overlay is searchable, not display


def test_multi_blk_target_applies_to_all():
    g = _graph()
    apply_name_variants(g, [{"target": {"blks": ["X", "OTHER"]},
                             "names": [{"name": "Shared Alias", "source": "regulation"}]}])
    assert "Shared Alias" in {t.name for t in g.nodes["X:0"].name_tuples}


def test_display_case_rules():
    assert _display_case("McArthur Island Slough") == "McArthur Island Slough"
    assert _display_case("LONG LAKE") == "Long Lake"
    assert _display_case("UPPER ARROW L.") == "Upper Arrow Lake"


def test_reach_proximity_ignores_sub_metre_boundary_touch():
    # A reach ending 0.5 m into the upper piece [200,300] must NOT name it (rounded-measure bleed);
    # the lower piece [0,100] with full overlap IS named. Guards the reservoir-reach 0.01 m leak.
    g = _graph()
    apply_name_variants(g, [{"target": {"blks": ["X"]}, "reach": {"from_m": 0, "to_m": 200.5},
                             "names": [{"name": "Lower Reach", "source": "regulation", "display": True}]}])
    assert g.nodes["X:0"].display_name == "Lower Reach"        # full overlap -> named
    assert g.nodes["X:200"].display_name != "Lower Reach"      # 0.5 m touch -> NOT named
