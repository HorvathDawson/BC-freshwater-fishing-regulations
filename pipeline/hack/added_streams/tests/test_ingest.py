"""ingest + attach_connectors — synthetic FWA mainstem + a merged added mainstem + a fork + a manual
stream. Verifies the added streams become real, WSC-nested, tributary-walkable graph nodes. Hermetic
(no gpkg, no network): the FWA side is a hand-built BlkChain/FidRow."""

from pyproj import Transformer
from shapely.geometry import LineString
from shapely.ops import transform as shp_transform

from pipeline.hack.added_streams.ingest import attach_connectors, ingest
from pipeline.graph import cutting
from pipeline.graph.blk_chains import FidRow
from pipeline.graph.graph import ancestors, build_section_geometries, build_stream_graph
from pipeline.models import BlkChain, FidSpan

_TO = Transformer.from_crs("EPSG:4326", "EPSG:3005", always_xy=True)


def _albers(coords):
    return shp_transform(lambda xs, ys, z=None: _TO.transform(xs, ys), LineString(coords))


def _fwa(blk, gnis, wsc, coords):
    line = _albers(coords)
    dn, up = cutting.blk_endpoints(line)
    fid = FidRow(fid=f"F{blk}", blk=blk, wsc=wsc, edge_type="1000", wbk="", gnis_id=gnis,
                 gnis_name="Test River", stream_order=3, stream_magnitude=5, down_m=0.0,
                 up_m=line.length, geometry=line, down_node=dn, up_node=up)
    chain = BlkChain(blk=blk, fwa_watershed_code=wsc, fids=(FidSpan(fid.fid, 0.0, line.length),),
                     geometry=line, mouth_measure=0.0, length_m=line.length, name_tuples=(),
                     gnis_id=gnis, gnis_name="Test River", stream_order=3, stream_magnitude=5)
    return fid, chain


def _feat(coords, **props):
    return {"type": "Feature", "properties": props,
            "geometry": {"type": "LineString", "coordinates": coords}}


def _build():
    fwa_fid, fwa_chain = _fwa("1000000", "10070", "100-100000", [(-123.0, 49.20), (-123.0, 49.22)])
    feats = [
        # a same-name 2-way channel -> ONE merged mainstem, joins FWA (gap => connector)
        _feat([[-123.0005, 49.210], [-123.010, 49.210]], source="osm", osm_way_id=111,
              name="Foo Creek", connect_to={"gnis_id": "10070"}),
        _feat([[-123.010, 49.210], [-123.020, 49.211]], source="osm", osm_way_id=112, name="Foo Creek"),
        # a fork (different name) joining the added mainstem (shared vertex => confluence)
        _feat([[-123.010, 49.210], [-123.012, 49.216]], source="manual", blk=-9001,
              name="Bar Creek", connect_to={"blk": -111}),
    ]
    add_fids, add_chains, specs = ingest(feats, [fwa_chain])
    graph = build_stream_graph([fwa_chain] + add_chains, [fwa_fid] + add_fids, {}, {})
    geoms = build_section_geometries([fwa_chain] + add_chains, [fwa_fid] + add_fids, {})
    report = attach_connectors(graph, geoms, specs)
    return graph, geoms, specs, report


def test_added_streams_become_nodes_with_geometry():
    graph, geoms, _, _ = _build()
    assert "-111:0" in graph.nodes and "-9001:0" in graph.nodes    # merged mainstem + fork
    assert graph.nodes["-111:0"].blk == "-111"
    assert geoms.get("-111:0") is not None and geoms["-111:0"].length > 0


def test_merged_mainstem_is_one_node():
    graph, _, _, _ = _build()
    foo_nodes = [nid for nid, n in graph.nodes.items() if str(n.blk) == "-111"]
    assert foo_nodes == ["-111:0"]                                 # two OSM ways => a single node


def test_wsc_hierarchy_nests():
    graph, _, _, _ = _build()
    foo_wsc = graph.nodes["-111:0"].wsc
    bar_wsc = graph.nodes["-9001:0"].wsc
    assert foo_wsc.startswith("100-100000")                        # descends the FWA mainstem
    assert bar_wsc.startswith(foo_wsc)                             # fork descends its receiver


def test_added_streams_are_tributaries_of_fwa():
    graph, _, _, _ = _build()
    fwa_node = [nid for nid, n in graph.nodes.items() if str(n.blk) == "1000000"][0]
    anc = ancestors(graph, fwa_node)
    assert "-111:0" in anc and "-9001:0" in anc                    # real flow edges, walkable


def test_connector_vs_confluence_and_not_a_barrier():
    graph, geoms, specs, report = _build()
    assert report["added"] == 2 and report["skipped"] == 0
    kinds = {s.from_node: s.kind for s in specs}
    assert kinds["-111:0"] == "connector"                         # FWA join has a gap
    assert kinds["-9001:0"] == "confluence"                       # fork shares a vertex
    assert not graph.nodes["-111:0"].is_barrier                   # edge_type 1000, never 2300
    assert any(k.startswith("connector:") for k in geoms)         # bridge geometry emitted


def test_connector_carries_the_mainstem_blk():
    _, geoms, _, report = _build()
    by_from = {c["from_node"]: c for c in report["connectors"]}
    # the connector for the added mainstem is attributed to the RECEIVING (FWA) mainstem blk
    assert by_from["-111:0"]["blk"] == "1000000"
    assert by_from["-111:0"]["geom_id"] == "connector:1000000:-111:0"
    # the fork's connector is attributed to its added-mainstem receiver (Foo Creek, blk -111)
    assert by_from["-9001:0"]["blk"] == "-111"
    assert by_from["-9001:0"]["geom_id"] in geoms


def test_strahler_order_and_shreve_magnitude():
    graph, _, _, _ = _build()
    foo, bar = graph.nodes["-111:0"], graph.nodes["-9001:0"]
    assert (bar.stream_order, bar.stream_magnitude) == (1, 1)     # a leaf
    assert (foo.stream_order, foo.stream_magnitude) == (2, 2)     # one order-1 tributary -> 2 / 2


def test_blk_is_negative_wayid():
    graph, _, _, _ = _build()
    assert graph.nodes["-111:0"].blk == "-111"                    # -min(way_id) of the merged ways
