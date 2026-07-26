"""Stream graph tests (03 S3) — the INVERTED graph: nodes = streams, edges = flows-into.

Synthetic tests build a tiny FidRow graph (no gpkg) and pin the logic deterministically,
including the two guards that used to live in the (deferred) tributary step but now run at
graph-build time: the WSC-descendant edge filter (S1) and the EDGE_TYPE=2300 barrier (S2).

The two real-data regression cases (Chehalis/Harrison, Kootenay/Columbia) build a small extract
straight from the gpkg and assert the leak is stopped by those guards. They skip automatically
when the data file is absent (CI without the 4.9 GB gpkg).
"""

import os

import pytest
from shapely.geometry import LineString

from stream_sections import cutting
from stream_sections.blk_chains import FidRow, build_blk_chains
from stream_sections.graph import ancestors, build_stream_graph

_DATA = "data/bc_fisheries_data.gpkg"
_needs_data = pytest.mark.skipif(not os.path.exists(_DATA), reason="needs data/bc_fisheries_data.gpkg")


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


def _graph(fids=None, **kw):
    fids = fids if fids is not None else _confluence_fids()
    return build_stream_graph(build_blk_chains(fids, lake_wbk_kind={}), fids, **kw)


def _out(graph, node_id):
    """Downstream nodes ``node_id`` flows into (the to_node of each outgoing edge)."""
    return [graph.edges[i].to_node for i in graph.down_adj.get(node_id, [])]


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


# --------------------------------------------------------------- WSC-descendant edge filter (S1)

def _cross_watershed_fids():
    """A canal C (wsc 300-625474) whose downstream continuation D is in a DIFFERENT WSC branch
    (300-999971) — the Columbia/Kootenay shape. A real tributary would extend C's code."""
    return [
        _fid("D1", "D", "300-999971", [(-50, 0), (0, 0)], 0, 50, gnis_name="Other River"),
        _fid("C1", "C", "300-625474", [(0, 0), (50, 0)], 0, 50, gnis_name="Canal"),
    ]


def test_wsc_filter_drops_cross_watershed_edge():
    fids = _cross_watershed_fids()
    on = _graph(fids, apply_wsc_filter=True)
    off = _graph(fids, apply_wsc_filter=False)
    assert _out(off, "C") == ["D"]          # without the filter C spuriously flows into D
    assert _out(on, "C") == []              # with it, the cross-watershed edge is dropped -> C root
    assert "C" not in ancestors(on, "D")    # so C is not a (spurious) tributary of D
    assert "C" in ancestors(off, "D")       # ... but it is without the filter


def test_wsc_filter_keeps_real_tributary():
    # B (100-111) is a genuine descendant of A (100): the filter must NOT drop it.
    g = _graph(apply_wsc_filter=True)
    assert ancestors(g, "A") == {"B"}


# ----------------------------------------------------------------------- 2300 barrier (S2)

def _canal_barrier_fids():
    """D <- C(2300 canal) <- U. All in one WSC branch (so the WSC filter keeps every edge);
    only the 2300 barrier should stop the walk at C."""
    return [
        _fid("D1", "D", "100", [(0, 0), (100, 0)], 0, 100, gnis_name="Down River"),
        _fid("C1", "C", "100-1", [(100, 0), (200, 0)], 0, 100, edge_type="2300"),
        _fid("U1", "U", "100-1-1", [(200, 0), (300, 0)], 0, 100, gnis_name="Up River"),
    ]


def test_edge_type_2300_preserved_through_merge():
    g = _graph(_canal_barrier_fids())
    assert g.nodes["C"].edge_types == ("2300",)
    assert g.nodes["C"].is_barrier is True
    assert g.nodes["D"].is_barrier is False


def test_2300_barrier_stops_guarded_walk_only():
    g = _graph(_canal_barrier_fids())
    # All three edges survive the WSC filter (U->C->D all in-branch).
    assert ancestors(g, "D", guarded=False) == {"C", "U"}   # raw closure reaches through canal
    assert ancestors(g, "D", guarded=True) == set()          # barrier C excluded, U beyond it


# ----------------------------------------------------------------- real-data regressions

def _extract_chains(names=None, lake_gnis=None, pad=None):
    """Load + merge a small extract straight from the gpkg for one named area."""
    from data.data_extractor import FWADataAccessor
    from stream_sections.blk_chains import load_stream_fids
    from stream_sections.build import bbox_from_gnis, get_lake_wbk_kind
    from stream_sections.names import resolve_names

    fwa = FWADataAccessor(_DATA)
    if lake_gnis is not None:
        lg = fwa.get_layer("lakes", columns=["WATERBODY_KEY", "GNIS_NAME_1"])
        sel = lg[lg["GNIS_NAME_1"].astype(str).str.contains(lake_gnis, case=False, na=False)]
        minx, miny, maxx, maxy = sel.total_bounds
        p = pad or 8000
        bbox = (minx - p, miny - p, maxx + p, maxy + p)
    else:
        bbox = bbox_from_gnis(fwa, names)
    lake_kind = get_lake_wbk_kind(fwa, bbox)
    fids = load_stream_fids(_DATA, bbox=bbox)
    return resolve_names(build_blk_chains(fids, lake_kind)), fids


def _extract_graph(names=None, lake_gnis=None, pad=None, apply_wsc_filter=True):
    chains, fids = _extract_chains(names=names, lake_gnis=lake_gnis, pad=pad)
    return build_stream_graph(chains, fids, apply_wsc_filter=apply_wsc_filter), fids


def _main(graph, name):
    cands = [n for n in graph.nodes.values() if (n.display_name or "") == name]
    return max(cands, key=lambda n: (n.stream_magnitude or 0, n.length_m)) if cands else None


@_needs_data
def test_chehalis_harrison_no_leak_via_wsc_filter():
    """Chehalis (100-077501-094860) flows INTO the Harrison (100-077501); the braided mouth must
    never make the Harrison a tributary of the Chehalis."""
    g, _ = _extract_graph(names=["Chehalis River", "Harrison River"])
    ch, ha = _main(g, "Chehalis River"), _main(g, "Harrison River")
    assert ch and ha
    assert ha.node_id not in ancestors(g, ch.node_id)   # Harrison is NOT upstream of Chehalis
    assert ch.node_id in ancestors(g, ha.node_id)        # Chehalis IS a tributary of the Harrison


@_needs_data
def test_kootenay_columbia_no_leak_via_2300_barrier():
    """The Baillie-Grohman canal (blk 356366076, EDGE_TYPE 2300) artificially links Columbia Lake
    to the Kootenay. Both guards must sever it: the WSC filter drops its cross-watershed outflow
    edge, and the 2300 barrier stops the guarded walk from traversing it."""
    canal = "356366076"
    chains, fids = _extract_chains(lake_gnis="Columbia Lake")

    # Guard 1 — WSC filter (the real build): the canal drains into a different WSC branch, so
    # its outflow edge is dropped at build time and the canal becomes a severed root.
    g = build_stream_graph(chains, fids, apply_wsc_filter=True)
    assert canal in g.nodes
    assert g.nodes[canal].edge_types == ("1450", "2300")   # edge_type survived the merge
    assert g.nodes[canal].is_barrier is True
    assert _out(g, canal) == []                             # cross-watershed outflow severed
    assert not any(canal in ancestors(g, n.node_id) for n in g.nodes.values())

    # Guard 2 — the 2300 barrier does INDEPENDENT work: even if the WSC filter were absent (a
    # same-watershed canal would slip past it), the barrier still stops the guarded walk.
    graw = build_stream_graph(chains, fids, apply_wsc_filter=False)
    assert _out(graw, canal) == ["356547900"]              # WSC-off: the canal edge exists
    reach_raw = any(canal in ancestors(graw, n.node_id, guarded=False) for n in graw.nodes.values())
    reach_guarded = any(canal in ancestors(graw, n.node_id, guarded=True) for n in graw.nodes.values())
    assert reach_raw and not reach_guarded                 # barrier alone severs the leak
