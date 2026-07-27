"""Name-variation application to the graph (docs/13). Synthetic, no gpkg.

Fixture: mainstem X (gnis 'Big River') threading lake W over [100,200], so it splits into a
lower piece X:0 and an upper piece X:200, plus lake:W.
"""

from shapely.geometry import LineString

from stream_sections import cutting
from stream_sections.blk_chains import FidRow, build_blk_chains
from stream_sections.graph import build_stream_graph
from stream_sections.names import _display_case, apply_name_variants


def _fid(fid, blk, wsc, coords, down_m, up_m, wbk="", gnis_name=""):
    geom = LineString(coords)
    dn, un = cutting.blk_endpoints(geom)
    return FidRow(fid=fid, blk=blk, wsc=wsc, edge_type="1000", wbk=wbk,
                  gnis_id="9" if gnis_name else "", gnis_name=gnis_name,
                  stream_order=1, stream_magnitude=1, down_m=down_m, up_m=up_m,
                  geometry=geom, down_node=dn, up_node=un)


def _graph():
    fids = [
        _fid("X0", "X", "100", [(0, 0), (100, 0)], 0, 100, gnis_name="Big River"),
        _fid("XA", "X", "100", [(100, 0), (200, 0)], 100, 200, wbk="W", gnis_name="Big River"),
        _fid("X3", "X", "100", [(200, 0), (300, 0)], 200, 300, gnis_name="Big River"),
    ]
    lk = {"W": "lake"}
    return build_stream_graph(build_blk_chains(fids, lk), fids, lake_kind=lk, lake_names={})


def test_reach_targets_only_the_upper_piece():
    g = _graph()
    apply_name_variants(g, [{"target": {"blk": "X"}, "reach": {"from_m": 200},
                             "names": [{"name": "Two Forty-One Creek", "source": "override",
                                        "note": "above the lake"}]}])
    assert g.nodes["X:200"].display_name == "Two Forty-One Creek"     # upper piece renamed
    assert g.nodes["X:0"].display_name == "Big River"                 # lower piece untouched
    # the note is preserved on the tuple
    assert any(t.note == "above the lake" for t in g.nodes["X:200"].name_tuples)


def test_alias_is_searchable_but_never_beats_gazette():
    g = _graph()
    apply_name_variants(g, [{"target": {"blk": "X"},
                             "names": [{"name": "Old Nickname", "source": "alias"}]}])
    # display stays the gazette name; the alias is present (searchable) but lower priority.
    assert g.nodes["X:0"].display_name == "Big River"
    names = {t.name for t in g.nodes["X:0"].name_tuples}
    assert "Old Nickname" in names and "Big River" in names


def test_lake_wbk_target_and_shouty_display_casing():
    g = _graph()
    apply_name_variants(g, [{"target": {"wbk": "W"},
                             "names": [{"name": "UPPER FOO L.", "source": "stocking"}]}])
    # the unnamed lake had no gazette name -> the stocking name displays, title-cased + expanded.
    assert g.nodes["lake:W"].display_name == "Upper Foo Lake"
    assert any(t.name == "UPPER FOO L." for t in g.nodes["lake:W"].name_tuples)  # raw kept for search


def test_display_case_leaves_proper_names_alone():
    assert _display_case("McArthur Island Slough") == "McArthur Island Slough"
    assert _display_case("LONG LAKE") == "Long Lake"
    assert _display_case("UPPER ARROW L.") == "Upper Arrow Lake"
