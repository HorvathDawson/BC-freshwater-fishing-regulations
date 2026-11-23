"""T1000 — OUTSIDE B.C. IS NOT DRAWN AT ALL, polygon and line alike.

Adskwatim Creek's polygon `lake:329268440` (a stream's own polygon, `gnis:7384`) is stamped `out_of_bc` (T679 #1's one
outside flag: it is in no region). `export_streams` dropped its route, as it drops every outside piece, but
`export_waterbodies` drew the polygon, so the one section was in the `lake` layer (`water: stream`) and not in the
`stream` layer — two live tests red (`test_one_water_kind.py`). The same gap drew 71 unnamed border-sliver lakes the
flag had moved outside: drawn, tappable, ruleless. One rule now: an `out_of_bc` node is in no water layer.

A small invented atlas (no data): a creek line, its polygon in B.C., its polygon outside, a plain lake each side."""

from __future__ import annotations

import json
import pickle

import orjson
from shapely.geometry import LineString, box

from pipeline.common.models import NodeKind, StreamGraph, StreamNode
from pipeline.deliver.tiles.export import export_streams, export_waterbodies

#: section id -> its handle (the line number in section_handles.txt)
_IDS = ["A:0", "lake:1", "lake:2", "lake:3", "lake:4"]


def _atlas(tmp_path):
    mu = ("7-1",)
    nodes = [
        StreamNode(node_id="A:0", kind=NodeKind.stream, blk="A", gnis_id="1", display_name="Test Creek",
                   stream_order=2, stream_magnitude=3, mus=mu),
        StreamNode(node_id="lake:1", kind=NodeKind.lake, wbk="1", mus=mu),                  # the creek's, in B.C.
        StreamNode(node_id="lake:2", kind=NodeKind.lake, wbk="2", mus=mu, out_of_bc=True),  # the creek's, outside
        StreamNode(node_id="lake:3", kind=NodeKind.lake, wbk="3", mus=mu, out_of_bc=True),  # a plain lake, outside
        StreamNode(node_id="lake:4", kind=NodeKind.lake, wbk="4", mus=mu)]                  # a plain lake, in B.C.
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    (tmp_path / "graph.pkl").write_bytes(pickle.dumps(g))
    x0, y0 = 1_000_000, 500_000
    polys = {n.node_id: box(x0 + 1000 * i, y0, x0 + 1000 * i + 500, y0 + 500) for i, n in enumerate(nodes)
             if n.node_id.startswith("lake:")}
    (tmp_path / "waterbody_polys.pkl").write_bytes(pickle.dumps(polys))
    routes = {"A:0": LineString([(x0 - 900, y0 + 250), (x0 - 100, y0 + 250)])}
    routes |= {k: LineString([(p.bounds[0], y0 + 250), (p.bounds[2], y0 + 250)]) for k, p in polys.items()}
    (tmp_path / "geometries.pkl").write_bytes(pickle.dumps(routes))
    (tmp_path / "registry.json").write_text(json.dumps({"items": [
        {"id": "gnis:1", "name": "Test Creek", "kind": "stream", "section_ids": ["A:0", "lake:1", "lake:2"]}]}))
    (tmp_path / "section_handles.txt").write_text("\n".join(_IDS) + "\n")
    out = tmp_path / "layers"
    out.mkdir()
    return out


def _drawn(path) -> dict[str, dict]:
    if not path.exists():
        return {}
    return {_IDS[orjson.loads(x)["properties"]["section_id"] - 1]: orjson.loads(x)["properties"]
            for x in path.read_bytes().splitlines()}


def test_a_section_outside_bc_is_in_no_water_layer(tmp_path):
    out = _atlas(tmp_path)
    export_waterbodies(tmp_path, "", out)
    export_streams(tmp_path, out)
    lake, stream = (_drawn(out / f"{n}.geojsonl") for n in ("lake", "stream"))
    under = (out / "under_lake.geojsonl").read_bytes().splitlines()         # the dotted routes carry no id
    # the creek's polygon in B.C. is in BOTH its layers: the polygon (water: stream) and its route as the creek
    assert lake["lake:1"]["water"] == "stream" and "lake:1" in stream
    assert lake["lake:4"]["water"] == "lake"
    # outside B.C.: drawn nowhere, the creek's own polygon (the Adskwatim shape) and a plain lake alike
    for sec in ("lake:2", "lake:3"):
        assert sec not in lake and sec not in stream, sec
    assert set(lake) == {"lake:1", "lake:4"} and set(stream) == {"A:0", "lake:1"}
    assert len(under) == 1                                     # lake:4's dotted route; lake:3's is outside too
