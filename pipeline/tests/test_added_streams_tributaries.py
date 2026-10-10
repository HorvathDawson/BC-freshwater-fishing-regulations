"""Integration guard: the added (municipal) streams reach the FWA network as real tributaries.

The Burnaby Lake / Deer Lake municipal cluster drains Still Creek -> Burnaby Lake -> the Brunette
River. Once the frozen `added_streams.build.json` is merged into the build (exclude superseded FWA
fids, add the synthetic fids, wire the connectors), those minted streams must show up in
`ancestors(Brunette River)` — i.e. they are genuine tributaries, not orphaned nodes.

This walks the SAME build path `pipeline/atlas/build.py` uses (load fids -> apply added streams ->
build graph -> attach connectors), scoped to a Brunette/Still Creek bbox so it stays a few seconds.
Marked `needs_source`: FAILS without the gpkg. See pipeline/atlas/waters/added_streams.
"""

import json

import pytest

import pipeline.atlas.build as b
from pipeline.atlas.graph.blk_chains import build_blk_chains, load_stream_fids
from pipeline.atlas.graph.graph import ancestors, build_section_geometries, build_stream_graph
from pipeline.atlas.graph.names import resolve_names
from pipeline.atlas.waters.added_streams.build_dataset import to_graph_inputs
from pipeline.atlas.waters.added_streams.ingest import attach_connectors
from pipeline.tests.conftest import need, GPKG_HINT

_DATA = b._DEFAULT_GPKG
_needs_data = pytest.mark.needs_source


@pytest.fixture(autouse=True)
def _the_gpkg(request):
    """A test marked `needs_source` FAILS without the gpkg, naming the command that fetches it."""
    if request.node.get_closest_marker("needs_source"):
        need(request, "source", _DATA, GPKG_HINT)

# Distinctive municipal creeks we minted for the Burnaby/Deer Lake cluster; each must resolve as a
# tributary (transitive ancestor) of the Brunette. Named, non-fragment streams -> stable to assert.
_EXPECT_ADDED_TRIBS = {
    "Deer Lake Brook", "Beaver Creek", "Turtle Creek",
    "Chickadee Creek", "Angelo Creek", "First Beach Creek",
}


def _brunette_graph(added: bool):
    """Build the graph over a Brunette/Still Creek bbox, optionally merging the added streams.
    Returns (graph, {node_id: added-stream name} for minted blks)."""
    fwa = b.FWADataAccessor(_DATA)
    bbox = b.bbox_from_gnis(fwa, ["Brunette River", "Still Creek"], pad=4000.0)
    lake_kind = b.get_lake_wbk_kind(fwa, bbox)
    lake_names = b.get_lake_names(fwa, bbox)
    fids = load_stream_fids(_DATA, bbox=bbox)

    add_specs: list = []
    name_by_blk: dict[str, str] = {}
    if added:
        data = json.loads(b._ADDED_STREAMS_JSON.read_text(encoding="utf-8"))
        add_streams = b._streams_in_bbox(data["streams"], bbox)
        name_by_blk = {str(s["blk"]): s["name"] for s in add_streams if s.get("name")}
        add_fids, add_specs = to_graph_inputs(add_streams)
        fids, _ = b._apply_fwa_exclude(fids, data.get("fwa_exclude", []))
        fids += add_fids

    chains = resolve_names(build_blk_chains(fids, lake_kind))
    graph = build_stream_graph(chains, fids, lake_kind, lake_names)
    if add_specs:
        geoms = build_section_geometries(chains, fids, lake_kind)
        attach_connectors(graph, geoms, add_specs)
    return graph, name_by_blk


@pytest.fixture(scope="module")
def brunette_with_added():
    return _brunette_graph(added=True)


def _added_ancestor_names(graph, name_by_blk) -> set[str]:
    node = b.resolve_node(graph, "Brunette River")
    assert node is not None, "Brunette River not in the graph"
    return {name_by_blk[graph.nodes[a].blk] for a in ancestors(graph, node)
            if graph.nodes[a].blk in name_by_blk}


@_needs_data
def test_brunette_river_resolves(brunette_with_added):
    graph, _ = brunette_with_added
    node = b.resolve_node(graph, "Brunette River")
    assert node is not None
    assert "brunette" in (graph.nodes[node].display_name or "").lower()


@_needs_data
def test_added_streams_are_brunette_tributaries(brunette_with_added):
    graph, name_by_blk = brunette_with_added
    added_tribs = _added_ancestor_names(graph, name_by_blk)
    missing = _EXPECT_ADDED_TRIBS - added_tribs
    assert not missing, f"expected added streams not tributaries of the Brunette: {sorted(missing)}"
    # The whole Deer Lake / Burnaby Lake cluster resolves upstream (dozens of streams), not just a few.
    assert len(added_tribs) >= 50, f"only {len(added_tribs)} added streams reached the Brunette"


@_needs_data
def test_added_tributaries_are_minted_not_fwa(brunette_with_added):
    """The expected tributaries carry MINTED (negative) blks — proof they came from the added
    dataset, not from FWA blue lines that happen to share a name."""
    graph, name_by_blk = brunette_with_added
    node = b.resolve_node(graph, "Brunette River")
    minted = {name_by_blk[graph.nodes[a].blk]: graph.nodes[a].blk
              for a in ancestors(graph, node) if graph.nodes[a].blk in name_by_blk}
    for name in _EXPECT_ADDED_TRIBS:
        assert int(minted[name]) < 0, f"{name} is not a minted added stream (blk {minted[name]})"


@_needs_data
def test_without_added_streams_the_cluster_is_absent():
    """Turning the added dataset off removes the whole municipal cluster: none of the expected
    creek names resolve as Brunette tributaries (they exist only in the added dataset)."""
    graph, _ = _brunette_graph(added=False)
    node = b.resolve_node(graph, "Brunette River")
    names = {(graph.nodes[a].display_name or "") for a in ancestors(graph, node)}
    assert _EXPECT_ADDED_TRIBS.isdisjoint(names), \
        f"unexpected: {sorted(_EXPECT_ADDED_TRIBS & names)} present without added streams"
