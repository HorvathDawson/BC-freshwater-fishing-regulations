"""Stream graph tests (03 S3) — the INVERTED graph: nodes = streams, edges = flows-into.

Synthetic tests build a tiny FidRow graph (no gpkg). The two real-data regression cases
(Chehalis/Harrison, Kootenay/Columbia) are skipped until the guarded tributary walk + a small
real extract fixture exist — they gate the WSC filter / 2300 barrier (docs/10).
"""

import pytest
from shapely.geometry import LineString

from stream_sections import cutting
from stream_sections.blk_chains import FidRow, build_blk_chains
from stream_sections.graph import ancestors, build_stream_graph


def _fid(fid, blk, wsc, coords, down_m, up_m, gnis_name="", wbk="", edge_type="1000"):
    geom = LineString(coords)
    dn, un = cutting.blk_endpoints(geom)
    return FidRow(fid=fid, blk=blk, wsc=wsc, edge_type=edge_type, wbk=wbk,
                  gnis_id="9" if gnis_name else "", gnis_name=gnis_name,
                  stream_order=1, stream_magnitude=1, down_m=down_m, up_m=up_m,
                  geometry=geom, down_node=dn, up_node=un)


def _confluence_fids():
    # Mainstem A: mouth (0,0) -> (100,0) -> (200,0). Tributary B joins A at (100,0).
    return [
        _fid("A1", "A", "100", [(0, 0), (100, 0)], 0, 100, gnis_name="Main River"),
        _fid("A2", "A", "100", [(100, 0), (200, 0)], 100, 200, gnis_name="Main River"),
        _fid("B1", "B", "100-111", [(100, 0), (100, 100)], 0, 100, gnis_name="Trib Creek"),
    ]


def _graph():
    fids = _confluence_fids()
    return build_stream_graph(build_blk_chains(fids, lake_wbk_kind={}), fids)


def test_mainstem_is_one_node_not_per_confluence():
    g = _graph()
    assert set(g.nodes) == {"A", "B"}          # A stays ONE node (not split at the confluence)
    assert g.nodes["A"].display_name == "Main River"


def test_tributary_flows_into_mainstem_at_measure():
    g = _graph()
    assert len(g.edges) == 1
    e = g.edges[0]
    assert (e.from_node, e.to_node) == ("B", "A")   # B flows INTO A
    assert e.at_measure == 100                        # confluence at measure 100 on A
    assert (e.x, e.y) == (100.0, 0.0)                 # confluence coordinate


def test_ancestors_are_tributaries():
    g = _graph()
    assert ancestors(g, "A") == {"B"}   # B is upstream of A
    assert ancestors(g, "B") == set()   # A is downstream of B -> NOT an ancestor (no leak)


def test_mainstem_is_root():
    g = _graph()
    assert not g.down_adj.get("A")      # A drains out (root within this extent)
    assert g.down_adj.get("B")          # B flows into A


@pytest.mark.skip(reason="needs guarded tributary walk + small real extract fixture (docs/10 S1)")
def test_chehalis_harrison_no_leak_via_wsc_filter():
    """WSC-descendant filter must keep a Chehalis ancestor-walk from including Harrison."""


@pytest.mark.skip(reason="needs guarded tributary walk + small real extract fixture (docs/10 S2)")
def test_kootenay_columbia_no_leak_via_2300_barrier():
    """EDGE_TYPE=2300 barrier must stop the Kootenay/Columbia canal leak (blk 356366076)."""
