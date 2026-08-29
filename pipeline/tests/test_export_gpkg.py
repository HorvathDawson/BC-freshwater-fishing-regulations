"""graph.gpkg exporter — minted waterbodies must reach the `lakes` layer.

`mint_waterbody_nodes` gives an isolated lake or an overlaid wetland a node so it becomes a
matchable registry item. Those nodes carry NO sidecar geometry (the client draws the FWA polygon
by wbk), and the exporter used to skip any node without geometry — so all 309 minted items drew
nothing on the review map, which defeats the point of minting them.
"""

import pyogrio
from shapely.geometry import LineString, Polygon

from pipeline.io.export_gpkg import export_graph_gpkg
from pipeline.models import NodeKind, StreamGraph, StreamNode


def _square(x=0.0, y=0.0, s=10.0):
    return Polygon([(x, y), (x + s, y), (x + s, y + s), (x, y + s)])


def _graph():
    g = StreamGraph()
    g.nodes["1:0"] = StreamNode(node_id="1:0", kind=NodeKind.stream, blk="1", display_name="A Creek")
    g.nodes["lake:900"] = StreamNode(node_id="lake:900", kind=NodeKind.lake, wbk="900",
                                     display_name="Isolated Lake")
    g.nodes["lake:901"] = StreamNode(node_id="lake:901", kind=NodeKind.wetland, wbk="901",
                                     display_name="Cheam Marsh")
    return g


def _lakes(path):
    return pyogrio.read_dataframe(path, layer="lakes")


def test_minted_waterbody_uses_its_own_polygon(tmp_path):
    g = _graph()
    geoms = {"1:0": LineString([(0, 0), (5, 5)])}          # only the stream has section geometry
    out = str(tmp_path / "graph.gpkg")
    export_graph_gpkg(g, geoms, out, wbk_polys={"900": _square(), "901": _square(20, 20)})

    df = _lakes(out)
    assert set(df["wbk"]) == {"900", "901"}, "both minted waterbodies must be written"
    assert all(not gg.is_empty for gg in df.geometry), "each must carry its FWA polygon"


def test_wetland_goes_to_lakes_layer_not_streams(tmp_path):
    """A wetland is a waterbody keyed by wbk. The old `== NodeKind.lake` test dropped it into the
    stream branch, which writes blk/wsc/stream_order columns a wetland node does not have."""
    g = _graph()
    out = str(tmp_path / "graph.gpkg")
    export_graph_gpkg(g, {"1:0": LineString([(0, 0), (5, 5)])}, out,
                      wbk_polys={"900": _square(), "901": _square(20, 20)})

    assert set(_lakes(out)["kind"]) == {"lake", "wetland"}
    streams = pyogrio.read_dataframe(out, layer="streams")
    assert set(streams["node_id"]) == {"1:0"}, "no waterbody may land in the streams layer"


def test_without_polygons_minted_nodes_are_skipped_not_crashing(tmp_path):
    """No polygon available -> the node is simply absent. It must not raise, and must not emit an
    empty geometry row that would break the map client."""
    out = str(tmp_path / "graph.gpkg")
    export_graph_gpkg(_graph(), {"1:0": LineString([(0, 0), (5, 5)])}, out, wbk_polys={})
    assert pyogrio.list_layers(out).size and "lakes" not in [l[0] for l in pyogrio.list_layers(out)]


def test_graph_nodes_layer_handles_a_polygon_geometry(tmp_path):
    """`_mouth_point` reads `geom.coords[0]`, which a Polygon does not have — a minted waterbody
    must fall back to a representative point instead of raising."""
    out = str(tmp_path / "graph.gpkg")
    export_graph_gpkg(_graph(), {"1:0": LineString([(0, 0), (5, 5)])}, out,
                      wbk_polys={"900": _square(), "901": _square(20, 20)})
    nodes = pyogrio.read_dataframe(out, layer="graph_nodes")
    assert set(nodes["node_id"]) == {"1:0", "lake:900", "lake:901"}
    for pt, poly in [(nodes.set_index("node_id").geometry["lake:900"], _square())]:
        assert poly.covers(pt), "the representative point must lie inside the polygon"


def test_lakes_layer_accepts_mixed_line_and_polygon_geometry(tmp_path):
    """The production shape: `lakes` holds LineStrings (a real lake node's stitched under-lake
    channel, from the sidecar) AND Polygons (minted waterbodies, from FWA) in ONE layer. A
    GeoPackage layer with a declared single geometry type would reject that."""
    g = _graph()
    g.nodes["lake:800"] = StreamNode(node_id="lake:800", kind=NodeKind.lake, wbk="800",
                                     display_name="Threaded Lake")
    geoms = {"1:0": LineString([(0, 0), (5, 5)]),
             "lake:800": LineString([(1, 1), (2, 2), (3, 1)])}   # a lake WITH sidecar geometry
    out = str(tmp_path / "graph.gpkg")
    export_graph_gpkg(g, geoms, out, wbk_polys={"900": _square(20, 20), "901": _square(40, 40)})

    df = _lakes(out)
    assert set(df["wbk"]) == {"800", "900", "901"}
    assert {gg.geom_type for gg in df.geometry} == {"LineString", "Polygon"}
