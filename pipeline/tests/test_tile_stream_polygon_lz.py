"""T1024 — A STREAM'S POLYGON SAYS WHEN ITS LINE IS DRAWN (`lz`), and the written files prove it.

Gravel Slough (staging report 3444007425e9d3bd): a selected slough was ringed round every section's piece of its polygon
over its route's casing. The app now rings a stream's polygon only where its line is not drawn, which it reads from the
polygon's `lz` — the first zoom its route is in the `stream` layer, absent when it has none (the Okanagan River Oxbows,
no sidecar line). These tests run the real exporters over a small invented atlas (no data), and the check
(`tiles/stream_polygon_lines.py`) over hand-written layer files, one per way to break the rule."""

from __future__ import annotations

import json
import pickle

import orjson
import pytest
from shapely.geometry import LineString, box

from pipeline.common.models import NodeKind, StreamGraph, StreamNode
from pipeline.deliver.tiles.export import export_streams, export_waterbodies
from pipeline.deliver.tiles.stream_polygon_lines import check, offenders

#: section id -> its handle (the line number in section_handles.txt)
_IDS = ["A:0", "lake:1", "lake:2", "lake:3"]


def _atlas(tmp_path):
    """A creek line A:0; its polygon lake:1 WITH a route (a sidecar line); its polygon lake:2 with NONE; a plain lake."""
    mu = ("7-1",)
    nodes = [
        StreamNode(node_id="A:0", kind=NodeKind.stream, blk="A", gnis_id="1", display_name="Test Slough",
                   stream_order=2, stream_magnitude=3, mus=mu),
        StreamNode(node_id="lake:1", kind=NodeKind.lake, wbk="1", mus=mu),
        StreamNode(node_id="lake:2", kind=NodeKind.lake, wbk="2", mus=mu),
        StreamNode(node_id="lake:3", kind=NodeKind.lake, wbk="3", mus=mu)]
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    (tmp_path / "graph.pkl").write_bytes(pickle.dumps(g))
    x0, y0 = 1_000_000, 500_000
    polys = {n.node_id: box(x0 + 1000 * i, y0, x0 + 1000 * i + 500, y0 + 500) for i, n in enumerate(nodes)
             if n.node_id.startswith("lake:")}
    (tmp_path / "waterbody_polys.pkl").write_bytes(pickle.dumps(polys))
    routes = {"A:0": LineString([(x0 - 900, y0 + 250), (x0 - 100, y0 + 250)]),
              "lake:1": LineString([(x0 + 1000, y0 + 250), (x0 + 1500, y0 + 250)]),
              "lake:3": LineString([(x0 + 3000, y0 + 250), (x0 + 3500, y0 + 250)])}
    (tmp_path / "geometries.pkl").write_bytes(pickle.dumps(routes))
    (tmp_path / "registry.json").write_text(json.dumps({"items": [
        {"id": "gnis:1", "name": "Test Slough", "kind": "stream", "section_ids": ["A:0", "lake:1", "lake:2"]}]}))
    (tmp_path / "section_handles.txt").write_text("\n".join(_IDS) + "\n")
    out = tmp_path / "layers"
    out.mkdir()
    return out


def _features(path) -> dict[str, dict]:
    return {_IDS[orjson.loads(x)["properties"]["section_id"] - 1]: orjson.loads(x)
            for x in path.read_bytes().splitlines()}


def test_the_exporters_write_lz_from_the_route_as_written(tmp_path):
    out = _atlas(tmp_path)
    route_zooms: dict = {}
    export_streams(tmp_path, out, route_zooms=route_zooms)
    export_waterbodies(tmp_path, "", out, route_zooms=route_zooms)
    lake, stream = (_features(out / f"{n}.geojsonl") for n in ("lake", "stream"))
    assert route_zooms == {"lake:1": stream["lake:1"]["tippecanoe"]["minzoom"], "lake:2": None}
    # the polygon with a route says the zoom its line is first in `stream`; the one with none says nothing; a lake neither
    assert {k: (f["properties"]["water"], f["properties"].get("lz")) for k, f in lake.items()} == {
        "lake:1": ("stream", stream["lake:1"]["tippecanoe"]["minzoom"]), "lake:2": ("stream", None),
        "lake:3": ("lake", None)}
    assert "lake:2" not in stream
    (out / "wetland.geojsonl").touch()        # this atlas has no wetland, so the export opened no wetland file
    assert check(out) == 2                                  # both stream polygons, each telling the truth


def test_a_stream_polygon_the_streams_export_did_not_report_is_refused(tmp_path):
    out = _atlas(tmp_path)
    with pytest.raises(SystemExit, match="lake:2: a stream's own polygon `export_streams` did not report"):
        export_waterbodies(tmp_path, "", out, route_zooms={"lake:1": 9})


def _layers(tmp_path, polys: list[dict], lines: list[tuple[int, int]]):
    """Layer files as `_writer` writes them: `polys` as lake features, `lines` as (section id, minzoom) stream ones."""
    def row(props, z):
        return json.dumps({"type": "Feature", "geometry": {"type": "Point", "coordinates": [0, 0]},
                           "properties": props, "tippecanoe": {"layer": "x", "minzoom": z, "maxzoom": 13}},
                          separators=(",", ":"))
    (tmp_path / "lake.geojsonl").write_text("".join(row(p, 10) + "\n" for p in polys))
    (tmp_path / "wetland.geojsonl").write_text("")
    (tmp_path / "stream.geojsonl").write_text("".join(row({"section_id": s, "ord": 2}, z) + "\n" for s, z in lines))
    (tmp_path / "lake_link.geojsonl").write_text("")
    return tmp_path


def test_the_check_takes_the_two_true_cases(tmp_path):
    d = _layers(tmp_path, [{"section_id": 1, "water": "stream", "lz": 11}, {"section_id": 2, "water": "stream"},
                           {"section_id": 3, "water": "lake"}],
                [(1, 12), (1, 11), (3, 10)])                # a route of two pieces: its FIRST zoom is the line's
    assert offenders(d) == (2, {}) and check(d) == 2


@pytest.mark.parametrize(("polys", "lines", "how"), [
    ([{"section_id": 1, "water": "stream", "lz": 11}], [], "lz_without_line"),
    ([{"section_id": 1, "water": "stream", "lz": 11}], [(1, 12)], "lz_not_the_line_zoom"),
    ([{"section_id": 1, "water": "stream"}], [(1, 12)], "line_without_lz"),
    ([{"section_id": 1, "water": "lake", "lz": 9}], [], "lz_on_standing_water"),
])
def test_the_check_names_each_way_to_break_it(tmp_path, polys, lines, how):
    d = _layers(tmp_path, polys, lines)
    assert offenders(d)[1] == {how: [1]}
    with pytest.raises(SystemExit, match=f"{how} 1 "):
        check(d)
