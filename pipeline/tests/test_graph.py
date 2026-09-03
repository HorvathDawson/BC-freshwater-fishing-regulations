"""Stream graph tests (03 S3) — the INVERTED graph: nodes = stream pieces + lakes, edges =
flows-into.

Synthetic tests build a tiny FidRow graph (no gpkg) and pin the logic deterministically:
mainstem stays one node across a confluence; lakes become their own node and split the BLK; the
WSC-descendant edge filter (S1) and the EDGE_TYPE=2300 barrier (S2), both applied at build time.

The two real-data regression cases (Chehalis/Harrison, Kootenay/Columbia) build a small extract
from the gpkg and assert the leaks are stopped; they skip when the data file is absent.
"""

import os

import pytest
from shapely.geometry import LineString

from pipeline.atlas.graph import cutting
from pipeline.atlas.graph.blk_chains import FidRow, build_blk_chains
from pipeline.atlas.graph.graph import ancestors, build_stream_graph
from pipeline.common.curated import CURATED, SOURCE

_DATA = str(SOURCE / "bc_fisheries_data.gpkg")
_needs_data = pytest.mark.skipif(not os.path.exists(_DATA), reason="needs data/bc_fisheries_data.gpkg")


def _fid(fid, blk, wsc, coords, down_m, up_m, gnis_name="", wbk="", edge_type="1000"):
    geom = LineString(coords)
    dn, un = cutting.blk_endpoints(geom)
    return FidRow(fid=fid, blk=blk, wsc=wsc, edge_type=edge_type, wbk=wbk,
                  gnis_id="9" if gnis_name else "", gnis_name=gnis_name,
                  stream_order=1, stream_magnitude=1, down_m=down_m, up_m=up_m,
                  geometry=geom, down_node=dn, up_node=un)


def _piece(blk, down_m=0):
    return f"{blk}:{int(down_m)}"


def _graph(fids, lake_kind=None, lake_names=None, **kw):
    chains = build_blk_chains(fids, lake_wbk_kind=lake_kind or {})
    return build_stream_graph(chains, fids, lake_kind=lake_kind, lake_names=lake_names, **kw)


def _out(graph, node_id):
    return [graph.edges[i].to_node for i in graph.down_adj.get(node_id, [])]


# ---------------------------------------------------------------------- confluence basics

def _confluence_fids():
    # Mainstem A: mouth (0,0) -> (100,0) -> (200,0). Tributary B joins A at (100,0).
    return [
        _fid("A1", "A", "100", [(0, 0), (100, 0)], 0, 100, gnis_name="Main River"),
        _fid("A2", "A", "100", [(100, 0), (200, 0)], 100, 200, gnis_name="Main River"),
        _fid("B1", "B", "100-111", [(100, 0), (100, 100)], 0, 100, gnis_name="Trib Creek"),
    ]


def test_mainstem_is_one_node_not_per_confluence():
    g = _graph(_confluence_fids())
    assert set(g.nodes) == {_piece("A"), _piece("B")}   # A stays ONE piece across the confluence
    assert g.nodes[_piece("A")].display_name == "Main River"


def test_tributary_flows_into_mainstem_at_measure():
    g = _graph(_confluence_fids())
    assert len(g.edges) == 1
    e = g.edges[0]
    assert (e.from_node, e.to_node) == (_piece("B"), _piece("A"))   # B flows INTO A
    assert e.at_measure == 100 and (e.x, e.y) == (100.0, 0.0)


def test_ancestors_are_tributaries():
    g = _graph(_confluence_fids())
    assert ancestors(g, _piece("A")) == {_piece("B")}   # B upstream of A
    assert ancestors(g, _piece("B")) == set()           # A downstream of B -> not an ancestor


# --------------------------------------------------------------------- lakes as nodes

def _through_lake_fids():
    """One BLK L threading lake wbk=W: below-lake, under-lake run, above-lake. Plus tributary T
    dipping into the lake."""
    return [
        _fid("L1", "L", "100", [(0, 0), (100, 0)], 0, 100, gnis_name="Big River"),          # below
        _fid("L2", "L", "100", [(100, 0), (200, 0)], 100, 200, gnis_name="Big River", wbk="W"),  # under lake
        _fid("L3", "L", "100", [(200, 0), (300, 0)], 200, 300, gnis_name="Big River"),        # above
        _fid("T1", "T", "100-1", [(150, 0), (150, 80)], 0, 80, gnis_name="Side Creek", wbk="W"),  # trib mouth under lake
        _fid("T2", "T", "100-1", [(150, 80), (150, 160)], 80, 160, gnis_name="Side Creek"),   # trib above lake
    ]


def test_lake_becomes_node_and_splits_the_blk():
    g = _graph(_through_lake_fids(), lake_kind={"W": "lake"}, lake_names={"W": "Big Lake"})
    # BLK L -> two stream pieces (below/above) + one lake node; wetlands would NOT do this.
    assert _piece("L", 0) in g.nodes and _piece("L", 200) in g.nodes
    assert "lake:W" in g.nodes
    lake = g.nodes["lake:W"]
    assert lake.kind.value == "lake" and lake.display_name == "Big Lake"
    assert "Big River" in lake.through_names and "Side Creek" in lake.through_names


def test_lake_inlets_and_outlet_from_adjacency():
    g = _graph(_through_lake_fids(), lake_kind={"W": "lake"}, lake_names={"W": "Big Lake"})
    inlets = {g.edges[i].from_node for i in g.up_adj.get("lake:W", [])}
    outlets = {g.edges[i].to_node for i in g.down_adj.get("lake:W", [])}
    # The side creek's mouth fid is UNDER the lake, so its stream piece starts at down_m=80.
    assert inlets == {_piece("L", 200), _piece("T", 80)}  # above-lake river + the side creek
    assert outlets == {_piece("L", 0)}                    # the below-lake river drains it


def test_no_split_on_wetland_wbk():
    # Same fids, but W is NOT in lake_kind (a wetland) -> BLK L stays ONE piece, no lake node.
    g = _graph(_through_lake_fids(), lake_kind={})
    assert _piece("L", 0) in g.nodes and _piece("L", 200) not in g.nodes
    assert not any(n.kind.value == "lake" for n in g.nodes.values())


# --------------------------------------------------------------- WSC-descendant edge filter (S1)

def _cross_watershed_fids():
    """Canal C (wsc 300-625474) whose downstream continuation D is in a DIFFERENT WSC branch."""
    return [
        _fid("D1", "D", "300-999971", [(-50, 0), (0, 0)], 0, 50, gnis_name="Other River"),
        _fid("C1", "C", "300-625474", [(0, 0), (50, 0)], 0, 50, gnis_name="Canal"),
    ]


def test_wsc_filter_drops_cross_watershed_edge():
    on = _graph(_cross_watershed_fids(), apply_wsc_filter=True)
    off = _graph(_cross_watershed_fids(), apply_wsc_filter=False)
    assert _out(off, _piece("C")) == [_piece("D")]        # without the filter C flows into D
    assert _out(on, _piece("C")) == []                    # with it the cross-watershed edge drops
    assert _piece("C") not in ancestors(on, _piece("D"))
    assert _piece("C") in ancestors(off, _piece("D"))


def test_wsc_filter_keeps_real_tributary():
    g = _graph(_confluence_fids(), apply_wsc_filter=True)
    assert ancestors(g, _piece("A")) == {_piece("B")}     # genuine descendant kept


# ----------------------------------------------------------------------- 2300 barrier (S2)

def _canal_barrier_fids():
    """D <- C(2300 canal) <- U, all one WSC branch (so the WSC filter keeps every edge);
    only the 2300 barrier should stop the walk at C."""
    return [
        _fid("D1", "D", "100", [(0, 0), (100, 0)], 0, 100, gnis_name="Down River"),
        _fid("C1", "C", "100-1", [(100, 0), (200, 0)], 0, 100, edge_type="2300"),
        _fid("U1", "U", "100-1-1", [(200, 0), (300, 0)], 0, 100, gnis_name="Up River"),
    ]


def test_edge_type_2300_preserved_through_merge():
    g = _graph(_canal_barrier_fids())
    assert g.nodes[_piece("C")].edge_types == ("2300",)
    assert g.nodes[_piece("C")].is_barrier is True
    assert g.nodes[_piece("D")].is_barrier is False


def test_2300_barrier_stops_guarded_walk_only():
    g = _graph(_canal_barrier_fids())
    assert ancestors(g, _piece("D"), guarded=False) == {_piece("C"), _piece("U")}
    assert ancestors(g, _piece("D"), guarded=True) == set()   # barrier C excluded, U beyond it


# ----------------------------------------------------------------- real-data regressions

def _extract_chains(names=None, lake_gnis=None, pad=None):
    from data.data_extractor import FWADataAccessor
    from pipeline.atlas.graph.blk_chains import load_stream_fids
    from pipeline.atlas.build import bbox_from_gnis, get_lake_names, get_lake_wbk_kind
    from pipeline.atlas.graph.names import resolve_names

    fwa = FWADataAccessor(_DATA)
    if lake_gnis is not None:
        lg = fwa.get_layer("lakes", columns=["WATERBODY_KEY", "GNIS_NAME_1"])
        sel = lg[lg["GNIS_NAME_1"].astype(str).str.contains(lake_gnis, case=False, na=False)]
        minx, miny, maxx, maxy = sel.total_bounds
        p = pad or 8000
        bbox = (minx - p, miny - p, maxx + p, maxy + p)
    else:
        bbox = bbox_from_gnis(fwa, names)
    lake_kind, lake_names = get_lake_wbk_kind(fwa, bbox), get_lake_names(fwa, bbox)
    fids = load_stream_fids(_DATA, bbox=bbox)
    chains = resolve_names(build_blk_chains(fids, lake_kind))
    return chains, fids, lake_kind, lake_names


def _build(chains, fids, lk, ln, **kw):
    return build_stream_graph(chains, fids, lake_kind=lk, lake_names=ln, **kw)


def _named(graph, name):
    return {nid for nid, n in graph.nodes.items() if (n.display_name or "") == name}


def _by_blk(graph, blk):
    return {nid for nid, n in graph.nodes.items() if n.blk == blk}


@_needs_data
def test_chehalis_harrison_no_leak_via_wsc_filter():
    """Chehalis flows INTO the Harrison; the braided mouth must never make the Harrison a
    tributary of the Chehalis (WSC-descendant filter, S1)."""
    g = _build(*_extract_chains(names=["Chehalis River", "Harrison River"]))
    chehalis, harrison = _named(g, "Chehalis River"), _named(g, "Harrison River")
    assert chehalis and harrison
    # No Harrison piece is upstream of any Chehalis piece.
    assert not any(harrison & ancestors(g, c) for c in chehalis)
    # The Chehalis IS a tributary of some Harrison piece.
    assert any(chehalis & ancestors(g, h) for h in harrison)


@_needs_data
def test_kootenay_columbia_no_leak_via_2300_barrier():
    """The Baillie-Grohman canal (blk 356366076, EDGE_TYPE 2300) artificially links the Columbia
    Lake side to the Kootenay. Once lakes are nodes the canal drains into a lake node, so the
    WSC filter (stream->stream only) no longer touches that edge — the 2300 barrier is the
    primary guard: the Columbia-side reach above the canal must not appear as a tributary of
    anything below it."""
    g = _build(*_extract_chains(lake_gnis="Columbia Lake"))
    canal = _by_blk(g, "356366076")
    assert canal and any(g.nodes[c].is_barrier for c in canal)   # 2300 survived the merge
    assert any("2300" in g.nodes[c].edge_types for c in canal)

    # The canal drains somewhere (a lake node, per FWA); the reach at/above the canal is the
    # Columbia side that must not leak downstream.
    downstream = {g.edges[i].to_node for c in canal for i in g.down_adj.get(c, [])}
    assert downstream
    above = set(canal).union(*[ancestors(g, c, guarded=False) for c in canal])
    for d in downstream:
        assert not (above & ancestors(g, d, guarded=True))    # barrier severs the leak
        assert above & ancestors(g, d, guarded=False)          # ... which the barrier is doing
