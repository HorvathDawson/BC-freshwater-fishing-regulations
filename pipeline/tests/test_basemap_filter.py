"""T85 / T202 — the basemap build keeps only the landuse kinds, water lines and label points the style draws
(`tiles.basemap.feature_filter`; the app side is `app/tools/basemap-filter.test.ts`)."""

from __future__ import annotations

import json
from pathlib import Path

from pipeline.deliver.tiles import basemap as B
from pipeline.deliver.tiles import basemap_grid as G
from pipeline.deliver.tiles import basemap_roads as R


def test_the_filter_keeps_drawn_landuse_and_river_lines_and_drops_the_wetland():
    f = B.feature_filter()
    assert f["landuse"][:2] == ["in", "kind"]
    kept = set(f["landuse"][2:])
    assert {"wood", "glacier", "forest"} <= kept
    # T202 (owner 2026-10-27): the undrawn wetland stand-in is dropped; see LANDUSE_STANDIN for how to bring it back
    assert "wetland" not in kept and B.LANDUSE_STANDIN == ()
    assert not kept & {"park", "national_park", "nature_reserve", "military", "residential", "farmland"}
    # every water polygon stays; of its lines only the rivers; of its label points only the sea's
    assert f["water"] == ["any", ["==", "$type", "Polygon"],
                          ["all", ["==", "$type", "LineString"], ["in", "kind", "river"]],
                          ["all", ["==", "$type", "Point"], ["!in", "kind", "water", "lake"]]]
    assert set(B.WATER_LINES_DROPPED) == {"stream"}
    # earth: the land and the island labels; its cliff lines are drawn by nothing
    assert f["earth"] == ["!=", "$type", "LineString"]


def _mock(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(B.shutil, "which", lambda t: f"/bin/{t}")
    monkeypatch.setattr(B, "_boundary_4326", lambda src, out: out)
    monkeypatch.setattr(R, "roads_geojsonl", lambda archive, out, zoom=14: 1)

    def grid(src, out, tmp, workers=4):
        calls.append(["grid", src, out])
        out.write_bytes(b"x")
    monkeypatch.setattr(G, "requantize_archive", grid)

    def run(cmd):
        calls.append(cmd)
        Path(cmd[3] if cmd[0] == "pmtiles" else cmd[cmd.index("-o") + 1]).write_bytes(b"x")
    monkeypatch.setattr(B, "_run", run)
    src = tmp_path / "bc.pmtiles"
    src.write_bytes(b"x" * 10)
    return src, calls


def test_the_build_passes_the_filter_and_the_unread_keys_to_every_tile_join(tmp_path, monkeypatch):
    src, calls = _mock(tmp_path, monkeypatch)
    B.build(src, tmp_path / "basemap.pmtiles", tmp_path / "bc_boundary.geojson")
    filtered = [c for c in calls if c[0] == "tile-join" and "-j" in c]
    assert len(filtered) == 2                                    # z<13 with every layer, z13 without roads
    for c in filtered:
        assert json.loads(c[c.index("-j") + 1]) == B.feature_filter()
        assert c[c.index("-x") + 1] == "sort_rank"
    top = next(c for c in filtered if "-Z13" in c)
    assert "roads" not in [top[i + 1] for i, a in enumerate(top) if a == "-l"]
    low = next(c for c in filtered if "-z12" in c)
    assert "roads" in [low[i + 1] for i, a in enumerate(low) if a == "-l"]


def test_the_z13_cap_rebuilds_the_roads_from_z14_and_joins_three_parts(tmp_path, monkeypatch):
    src, calls = _mock(tmp_path, monkeypatch)
    out = tmp_path / "basemap.pmtiles"
    B.build(src, out, tmp_path / "bc_boundary.geojson")
    extract = next(c for c in calls if c[0] == "pmtiles")
    assert "--maxzoom=14" in extract                            # the roads come from z14
    tippe = next(c for c in calls if c[0] == "tippecanoe")
    assert {"-Z13", "-z13"} <= set(tippe) and tippe[tippe.index("-l") + 1] == "roads"
    final = calls[-2]
    joined = final[final.index("-o") + 1]
    assert final[0] == "tile-join" and Path(joined).name == "joined-z13.pmtiles"
    assert [Path(p).name for p in final[final.index(joined) + 1:]] == ["low-z13.pmtiles", "top-z13.pmtiles",
                                                                      "roads-z13.pmtiles"]
    # cut 5: the joined archive is re-gridded into the output
    assert calls[-1] == ["grid", Path(joined), out]


def test_a_rebuilt_road_drops_urban_kinds_and_unread_keys():
    assert R.props_of({"kind": "path", "kind_detail": "sidewalk"}) is None
    named = R.props_of({"kind": "minor_road", "kind_detail": "unclassified", "name": "Ashlu River FSR",
                        "min_zoom": 13, "sort_rank": 400, "ref": "x", "oneway": "yes"})
    assert named == {"kind": "minor_road", "kind_detail": "unclassified", "name": "Ashlu River FSR", "min_zoom": 13}
    # min_zoom is only the label sort key: an unnamed line has no label
    assert R.props_of({"kind": "path", "kind_detail": "track", "min_zoom": 13}) == {"kind": "path",
                                                                                    "kind_detail": "track"}


def test_the_grid_rounds_drops_collapsed_rings_and_their_holes():
    zz = G._zz
    # a 40x40 exterior with a 4x4 hole, then a 1x1 exterior (collapses at shift 2) with a hole of its own
    def ring(pts, cur):
        (x0, y0), rest = pts[0], pts[1:]
        out = [1 | 8, zz(x0 - cur[0]), zz(y0 - cur[1])]
        cx, cy = x0, y0
        out.append(2 | (len(rest) << 3))
        for x, y in rest:
            out += [zz(x - cx), zz(y - cy)]
            cx, cy = x, y
        return out + [7 | 8], (cx, cy)
    outer = [(0, 0), (40, 0), (40, 40), (0, 40)]
    hole = [(10, 10), (10, 14), (14, 14), (14, 10)]
    tiny = [(100, 100), (101, 100), (101, 101), (100, 101)]
    tiny_hole = [(100, 100), (100, 101), (101, 101)]
    cmds, cur = [], (0, 0)
    for r in (outer, hole, tiny, tiny_hole):
        c, cur = ring(r, cur)
        cmds += c
    parts = G._parts(G.quantize(cmds, 3, 2))
    assert parts == [[(0, 0), (10, 0), (10, 10), (0, 10)], [(3, 3), (3, 4), (4, 4), (4, 3)]]
    # a line that collapses to one point is dropped; one that survives loses its repeated vertices
    line = [1 | 8, zz(0), zz(0), 2 | (2 << 3), zz(1), zz(0), zz(8), zz(0)]
    assert G._parts(G.quantize(line, 2, 2)) == [[(0, 0), (2, 0)]]
    assert G.quantize([1 | 8, zz(0), zz(0), 2 | 8, zz(1), zz(0)], 2, 2) == []


def test_the_grid_rewrites_only_its_layers():
    import mapbox_vector_tile
    tile = mapbox_vector_tile.encode([
        {"name": "landuse", "features": [{"geometry": "POLYGON ((0 0, 400 0, 400 400, 0 400, 0 0))",
                                          "properties": {"kind": "wood"}}]},
        {"name": "roads", "features": [{"geometry": "LINESTRING (1 1, 403 7)", "properties": {"kind": "path"}}]},
    ])
    out = mapbox_vector_tile.decode(G.requantize_tile(tile))
    assert out["landuse"]["extent"] == 1024 and out["landuse"]["features"][0]["properties"] == {"kind": "wood"}
    assert out["roads"]["extent"] == 4096
    assert out["roads"]["features"][0]["geometry"]["coordinates"] == [[1, 1], [403, 7]]
