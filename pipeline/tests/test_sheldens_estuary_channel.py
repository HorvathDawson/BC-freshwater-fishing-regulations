"""Sheldens Creek runs on to the road bridge (owner 2026-11-16 20:01, T806: "Extend to the bridge").

The FWA line (blk 360846083) stops at the head of the salt marsh; the channel from there through the marsh to the
SPUR 14 road bridge and on to the flats of Sheldens Bay is a HAND-DRAWN added stream (`manual` source,
data/source/municipal/manual_streams.geojson). It must CONTINUE the creek: its head is the FWA line's own mouth vertex,
it shares the creek's watershed code, so the graph joins them at one node and the creek flows into it; its own mouth
is the sea (no connector). The low-confidence tidal boundary is a cut on it at the bridge (`waters/tidal_limits.json`).
Builds the Sheldens bbox graph the way `pipeline/atlas/build.py` does. `needs_source`: FAILS without the gpkg."""

import json

import pytest

import pipeline.atlas.build as b
from pipeline.atlas.graph.blk_chains import build_blk_chains, load_stream_fids
from pipeline.atlas.graph.graph import build_section_geometries, build_stream_graph
from pipeline.atlas.graph.names import resolve_names
from pipeline.atlas.graph.tributaries import subtree
from pipeline.atlas.waters.added_streams import fwa_match
from pipeline.atlas.waters.added_streams.build_dataset import to_graph_inputs
from pipeline.atlas.waters.added_streams.ingest import attach_connectors
from pipeline.tests.under_test import curated
from pipeline.tests.conftest import need, GPKG_HINT

SHELDENS, ESTUARY = "360846083", "-2400000001"
BRIDGE = (-131.76003, 53.14746)


def _estuary() -> dict:
    data = json.loads(curated().waters.added_streams.read_text(encoding="utf-8"))
    (s,) = [s for s in data["streams"] if str(s["blk"]) == ESTUARY]
    return s


def test_the_estuary_channel_is_minted_as_its_own_line_of_the_creeks_code():
    s = _estuary()
    assert (s["name"], s["source"], s["klass"], s["wsc"], s["receiver_kind"], s["connector"]) == \
        ("Sheldens Creek", "manual", "novel", "950-016367", "tidal", None)
    assert (s["stream_order"], s["stream_magnitude"]) == (2, 7)   # the creek's own, carried on (not 1)
    seg = s["segments"][-1]["coords3005"][-1]                     # stored mouth first: the last vertex is the head
    assert (round(seg[0], 3), round(seg[1], 3)) == (615733.858, 920150.919)   # = FWA 360846083's mouth vertex


def test_a_tail_past_the_fwa_mouth_is_not_an_upstream_extension():
    """The resolver read the trace as an EXTENSION of the FWA line and minted it at the line's TOP measure (3,692 m)
    with its geometry at the bottom. For a trusted (hand-drawn) source a tail past the mouth is a stream of its own."""
    from shapely.geometry import LineString
    fwa = LineString([(0, 0), (0, 1000)])                          # mouth first, as FWA lines are
    trace = LineString([(0, 0), (0, -300), (200, -400)])           # drawn on from the mouth
    assert fwa_match._tail_past_end(trace, fwa, 40.0, downstream_tail_novel=True) is None
    assert fwa_match._tail_past_end(trace, fwa, 40.0) is not None             # city layers: unchanged
    up = LineString([(0, 1000), (0, 1300), (200, 1400)])           # a tail past the SOURCE stays an extension
    assert fwa_match._tail_past_end(up, fwa, 40.0, downstream_tail_novel=True) is not None


@pytest.fixture(scope="module")
def graph(request):
    need(request, "source", b._DEFAULT_GPKG, GPKG_HINT)
    bbox = (612000.0, 916000.0, 619000.0, 923000.0)
    fwa = b.FWADataAccessor(b._DEFAULT_GPKG)
    lake_kind, lake_names = b.get_lake_wbk_kind(fwa, bbox), b.get_lake_names(fwa, bbox)
    fids = load_stream_fids(b._DEFAULT_GPKG, bbox=bbox)
    add_fids, specs = to_graph_inputs([_estuary()])
    fids += add_fids
    chains = resolve_names(build_blk_chains(fids, lake_kind), fids, curated_blks=())
    g = build_stream_graph(chains, fids, lake_kind, lake_names)
    rep = attach_connectors(g, build_section_geometries(chains, fids, lake_kind), specs)
    assert rep["skipped"] == 0 and not rep["connectors"]
    return g


def _on(g, blk):
    return {n for n, x in g.nodes.items() if x.blk == blk}


@pytest.mark.needs_source
def test_the_creek_flows_into_the_estuary_channel_and_it_into_the_sea(graph):
    g = graph
    (est,) = _on(g, ESTUARY)
    ups = {g.nodes[g.edges[i].from_node].blk for i in g.up_adj.get(est, [])}
    assert ups == {SHELDENS}                                       # the FWA creek joins its head, nothing else
    assert not g.down_adj.get(est)                                 # its mouth is the sea
    assert _on(g, SHELDENS) <= subtree(g, {est})                   # the whole creek is upstream of the bridge


def test_the_tidal_boundary_is_a_low_confidence_cut_at_the_bridge():
    from pipeline.atlas.splits import tidal as T
    rows = {r.blk: r for r in T.load()}
    assert SHELDENS not in rows                                    # the FWA line's mouth is no longer the sea
    r = rows[ESTUARY]
    assert (r.limit, r.mark) == ("cut", None)
    assert tuple(r.at["coord"]) == BRIDGE and "LOW CONFIDENCE" in r.why and "Extend to the bridge" in r.why


def test_the_bridge_cut_lands_on_the_estuary_channel_clear_of_its_end():
    """The atlas makes the tidal `cut` row a point split on its own line; on the estuary channel it lands at the
    bridge, 62 m above the channel's mouth — never on the line's end (`tidal.refuse_end_cuts`)."""
    from shapely.geometry import LineString
    from pipeline.atlas.splits import anchors as A
    from pipeline.atlas.splits import tidal as T
    from pipeline.common.models import BlkChain
    rows = [r for r in T.load() if r.blk == ESTUARY]
    (sd,) = T.cut_defs(rows, T.ids(rows, ()))
    assert sd.blk == ESTUARY and "tidal" in sd.label
    s = _estuary()
    g = LineString([c for seg in s["segments"] for c in seg["coords3005"]])
    chain = BlkChain(blk=ESTUARY, fwa_watershed_code=s["wsc"], fids=(), geometry=g, mouth_measure=0.0,
                     length_m=g.length, name_tuples=())
    (p,) = A.resolve_split_defs([sd], [chain])
    assert T.END_M < p.route_measure == pytest.approx(62.0, abs=1.0)
