"""Northrop Spawning Channel is a SIDE CHANNEL OF the Stave (owner 2026-10-29), wired like an FWA one (Ruskin):
the Stave feeds its head (the intake) and its mouth drains to FWA 356193720. A tributary walk from the Stave reaches
it; a tributary walk from Northrop never climbs into the Stave (the Stave is never its tributary).
Builds the Stave/Ruskin bbox graph the way `pipeline/atlas/build.py` does. `needs_source`: FAILS without the gpkg."""

import json

import pytest

import pipeline.atlas.build as b
from pipeline.atlas.graph.blk_chains import build_blk_chains, load_stream_fids
from pipeline.atlas.graph.graph import build_section_geometries, build_stream_graph
from pipeline.atlas.graph.names import resolve_names
from pipeline.atlas.graph.tributaries import subtree, with_tributaries
from pipeline.atlas.waters.added_streams.build_dataset import to_graph_inputs
from pipeline.atlas.waters.added_streams.ingest import attach_connectors
from pipeline.tests.conftest import need, GPKG_HINT

NORTHROP, STAVE, RECEIVER = "-2300000000", "356362384", "356193720"


@pytest.fixture(scope="module")
def graph(request):
    need(request, "source", b._DEFAULT_GPKG, GPKG_HINT)
    bbox = (1259000.0, 468000.0, 1264000.0, 474000.0)
    fwa = b.FWADataAccessor(b._DEFAULT_GPKG)
    lake_kind, lake_names = b.get_lake_wbk_kind(fwa, bbox), b.get_lake_names(fwa, bbox)
    fids = load_stream_fids(b._DEFAULT_GPKG, bbox=bbox)
    data = json.loads(b._ADDED_STREAMS_JSON.read_text(encoding="utf-8"))
    add = [s for s in data["streams"] if str(s["blk"]) == NORTHROP]
    add_fids, specs = to_graph_inputs(add)
    fids += add_fids
    chains = resolve_names(build_blk_chains(fids, lake_kind), fids, curated_blks=())
    g = build_stream_graph(chains, fids, lake_kind, lake_names)
    rep = attach_connectors(g, build_section_geometries(chains, fids, lake_kind), specs)
    assert rep["skipped"] == 0 and {c["kind"] for c in rep["connectors"]} == {"connector", "intake"}
    return g


def _on(g, blk):
    return {n for n, x in g.nodes.items() if x.blk == blk}


@pytest.mark.needs_source
def test_northrop_is_a_side_channel_with_both_ends_connected(graph):
    g = graph
    (nid,) = _on(g, NORTHROP)
    n = g.nodes[nid]
    assert set(n.edge_types) == {"1350"} and n.wsc.startswith("100-046202-028692")
    downs = {g.edges[i].to_node for i in g.down_adj.get(nid, [])}
    ups = {g.edges[i].from_node for i in g.up_adj.get(nid, [])}
    assert {g.nodes[d].blk for d in downs} == {RECEIVER}           # the mouth joins its receiver
    assert {g.nodes[u].blk for u in ups} == {STAVE}                 # the Stave feeds the intake


@pytest.mark.needs_source
def test_the_stave_walk_reaches_northrop_and_northrop_never_climbs_the_stave(graph):
    g = graph
    stave = _on(g, STAVE)
    assert _on(g, NORTHROP) <= with_tributaries(g, stave)            # the river's walk takes its side channel
    up = with_tributaries(g, _on(g, NORTHROP))
    assert not (up & stave)                                          # the Stave is never Northrop's tributary
    assert not (subtree(g, _on(g, NORTHROP)) & stave)                # nor anything upstream of it, guarded
    # (not vacuous: the Stave IS on Northrop's incoming edges — the intake — test above)
