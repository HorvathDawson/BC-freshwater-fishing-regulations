"""Topology graph tests (03 S3-4).

Synthetic tests build a tiny FidRow graph (no gpkg). The two real-data regression cases
(Chehalis/Harrison, Kootenay/Columbia) are skipped until the tributary walk + a small
real extract fixture exist — they gate deletion of the WSC filter / 2300 barrier (docs/10).
"""

import pytest
from shapely.geometry import LineString

from stream_sections.blk_chains import FidRow
from stream_sections import cutting
from stream_sections.topology import build_topology


def _fid(fid, blk, wsc, coords, down_m, up_m, wbk="", edge_type="1000"):
    geom = LineString(coords)
    dn, un = cutting.blk_endpoints(geom)
    return FidRow(fid=fid, blk=blk, wsc=wsc, edge_type=edge_type, wbk=wbk,
                  gnis_id="", gnis_name="", stream_order=1, stream_magnitude=1,
                  down_m=down_m, up_m=up_m, geometry=geom, down_node=dn, up_node=un)


def _confluence_graph(trib_wbk=""):
    # Mainstem A: mouth (0,0) -> (100,0) -> (200,0). Tributary B joins at (100,0).
    return [
        _fid("A1", "A", "100", [(0, 0), (100, 0)], 0, 100),
        _fid("A2", "A", "100", [(100, 0), (200, 0)], 100, 200),
        _fid("B1", "B", "100-111", [(100, 0), (100, 100)], 0, 100, wbk=trib_wbk),
    ]


def test_confluence_splits_mainstem_and_covers_all_fids():
    topo = build_topology(_confluence_graph(), lake_wbk_kind={})
    members = set()
    for s in topo.segments.values():
        members |= set(s.member_fids)
    assert members == {"A1", "A2", "B1"}          # full coverage
    assert len(topo.segments) == 3                 # A splits at the confluence; B is one
    assert topo.nodes["100.0_0.0"].kind.value == "confluence"


def test_reverse_adjacency_is_upstream():
    topo = build_topology(_confluence_graph(), lake_wbk_kind={})
    # from the confluence node, up_adj reaches the tributary + upper mainstem, not the mouth.
    conf = "100.0_0.0"
    up_from_conf = {topo.segments[sid].blk for sid in topo.up_adj.get(conf, [])}
    assert up_from_conf == {"A", "B"}              # A2 (upper mainstem) + B1 (tributary)
    # down_adj from the confluence flows toward the mouth (lower mainstem A1).
    down_from_conf = {topo.segments[sid].blk for sid in topo.down_adj.get(conf, [])}
    assert down_from_conf == {"A"}


def test_lake_node_collapse_no_orphans():
    # Mark the tributary as under-lake -> its fid is absorbed into the lake node.
    topo = build_topology(_confluence_graph(trib_wbk="W"), lake_wbk_kind={"W": "lake"})
    members = set()
    for s in topo.segments.values():
        members |= set(s.member_fids)
    assert members == {"A1", "A2"}                 # B1 (under-lake) absorbed, not a segment
    assert "lake:W" in topo.nodes
    assert topo.nodes["lake:W"].is_barrier is True
    # no segment references a missing node
    for s in topo.segments.values():
        assert s.from_node in topo.nodes and s.to_node in topo.nodes


def test_edge_type_transition_splits_segment():
    # Same BLK, straight line, but edge_type changes mid-run -> two homogeneous segments.
    rows = [
        _fid("X1", "X", "200", [(0, 0), (50, 0)], 0, 50, edge_type="1000"),
        _fid("X2", "X", "200", [(50, 0), (100, 0)], 50, 100, edge_type="2300"),
    ]
    topo = build_topology(rows, lake_wbk_kind={})
    ets = sorted(s.edge_type for s in topo.segments.values())
    assert ets == ["1000", "2300"]                 # not merged across the 2300 boundary


@pytest.mark.skip(reason="needs tributary walk + small real extract fixture (docs/10 S1)")
def test_chehalis_harrison_no_leak_via_wsc_filter():
    """WSC-descendant filter must stop a Chehalis-seeded walk from including Harrison."""


@pytest.mark.skip(reason="needs tributary walk + small real extract fixture (docs/10 S2)")
def test_kootenay_columbia_no_leak_via_2300_barrier():
    """EDGE_TYPE=2300 barrier must stop the Kootenay/Columbia canal leak (blk 356366076)."""
