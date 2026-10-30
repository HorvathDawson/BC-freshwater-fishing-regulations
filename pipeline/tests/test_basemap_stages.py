"""T280 — the basemap in two stages: the SOURCE (`basemap_source`: the upstream extract fetched by range requests,
resumable by zoom band, sha256-pinned by a sidecar) and the SHIP stage (`basemap`: trim, grid and the green fills
simplified per zoom, `basemap_grid.SIMPLIFY`), which refuses a source its sidecar does not describe and stamps the
source into the shipped archive's metadata."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.deliver.tiles import basemap as B
from pipeline.deliver.tiles import basemap_grid as G
from pipeline.deliver.tiles import basemap_source as S


# --- ship stage: simplification ---------------------------------------------------------------------------------------

def _ring_cmds(rings):
    zz, out, cx, cy = G._zz, [], 0, 0
    for pts in rings:
        (x0, y0), rest = pts[0], pts[1:]
        out += [1 | 8, zz(x0 - cx), zz(y0 - cy), 2 | (len(rest) << 3)]
        cx, cy = x0, y0
        for x, y in rest:
            out += [zz(x - cx), zz(y - cy)]
            cx, cy = x, y
        out.append(7 | 8)
    return out


def test_simplify_drops_vertices_within_the_tolerance_and_keeps_corners():
    # a 100x100 square whose bottom edge wobbles by one unit: the wobble goes at tol 1.5, the corners stay
    ring = [(0, 0), (25, 1), (50, 0), (75, 1), (100, 0), (100, 100), (0, 100)]
    assert G.simplify_ring(ring, 1.5) == [(0, 0), (100, 0), (100, 100), (0, 100)]
    assert G.simplify_ring(ring, 0.5) == ring
    assert G.simplify_ring(ring, 0) == ring


def test_simplify_keeps_winding_and_drops_slivers_with_their_holes():
    outer = [(0, 0), (100, 0), (100, 100), (50, 101), (0, 100)]     # exterior (positive area, the MVT spec's)
    hole = [(40, 40), (40, 60), (60, 60), (60, 40)]
    sliver = [(200, 200), (203, 200), (203, 201), (200, 201)]          # 3 units² exterior
    sliver_hole = [(201, 200), (202, 201), (202, 200)]
    for r in (outer, sliver):
        assert G._area2(r) > 0
    parts = G._parts(G.quantize(_ring_cmds([outer, hole, sliver, sliver_hole]), 3, 0, tol=1.5, min_area=4))
    assert parts == [[(0, 0), (100, 0), (100, 100), (0, 100)], hole]
    # a ring thinner than the tolerance collapses; under it and with min_area 0 the sliver survives
    assert len(G._parts(G.quantize(_ring_cmds([outer, hole, sliver]), 3, 0, tol=1.5))) == 2
    assert len(G._parts(G.quantize(_ring_cmds([outer, hole, sliver]), 3, 0, tol=0.5))) == 3


def test_simplify_is_per_layer_and_per_zoom():
    table = {"landuse": {0: (1.5, 4), 13: (1.0, 2)}}
    assert G.simplify_at("landuse", 8, table) == (1.5, 4)
    assert G.simplify_at("landuse", 13, table) == (1.0, 2)
    assert G.simplify_at("roads", 8, table) == (0.0, 0)
    assert set(G.SIMPLIFY) <= {"landuse", "landcover"}      # never roads, water, earth, places or boundaries


def test_the_reencode_simplifies_landuse_at_a_zoom_and_leaves_roads():
    import mapbox_vector_tile
    wobbly = "POLYGON ((0 0, 1000 4, 2000 0, 3000 4, 4000 0, 4000 4000, 0 4000, 0 0))"
    tile = mapbox_vector_tile.encode([
        {"name": "landuse", "features": [{"geometry": wobbly, "properties": {"kind": "wood"}}]},
        {"name": "roads", "features": [{"geometry": "LINESTRING (0 0, 1000 4, 2000 0)", "properties": {}}]},
    ])
    n = lambda t, lyr: sum(len(r) for r in mapbox_vector_tile.decode(t)[lyr]["features"][0]["geometry"]["coordinates"])  # noqa: E731
    table = {"landuse": {0: (1.5, 4)}}
    assert n(G.requantize_tile(tile, zoom=10, simplify=table), "landuse") < n(G.requantize_tile(tile), "landuse")
    out = mapbox_vector_tile.decode(G.requantize_tile(tile, zoom=10, simplify=table))
    assert out["roads"]["features"][0]["geometry"]["coordinates"] == [[0, 0], [1000, 4], [2000, 0]]
    assert out["landuse"]["extent"] == 1024


def test_the_build_stamps_the_source_into_the_shipped_metadata():
    side = {"upstream_url": "https://build.protomaps.com/20260305.pmtiles", "upstream_version": "4.14.1",
            "osm_replication_time": "2026-03-05T04:00:00Z", "sha256": "ab" * 32, "bytes": 1}
    assert B.source_meta(side) == {"source_upstream_url": side["upstream_url"], "source_upstream_version": "4.14.1",
                                   "source_osm_replication_time": side["osm_replication_time"],
                                   "source_sha256": "ab" * 32}
    assert B.source_meta(None) == {}


def test_the_build_refuses_a_source_its_sidecar_does_not_describe(tmp_path, monkeypatch):
    src = tmp_path / "bc.pmtiles"
    src.write_bytes(b"x" * 10)
    with pytest.raises(SystemExit, match="no bc.pmtiles.source.json"):
        S.check(src)
    S.sidecar_of(src).write_text(json.dumps({"bytes": 10, "sha256": "0" * 64}))
    with pytest.raises(SystemExit, match="not the file its sidecar describes"):
        S.check(src)
    from pipeline.deliver.tiles.version import sha256_file
    S.sidecar_of(src).write_text(json.dumps({"bytes": 10, "sha256": sha256_file(src)}))
    assert S.check(src)["bytes"] == 10


# --- source stage: range-request fetch, resumable by zoom band ---------------------------------------------------------

def _fake_tools(monkeypatch, who):
    calls = []

    def run(cmd):
        calls.append(cmd)
        if cmd[1] == "extract":
            assert cmd[2].startswith("https://")                 # read remotely: `pmtiles extract` range-requests
            Path(cmd[3]).write_bytes(b"part")
        elif cmd[1] == "merge":
            Path(cmd[-1]).write_bytes(b"merged")
        return ""
    monkeypatch.setattr(S, "_run", run)
    monkeypatch.setattr(S, "identity", lambda p: dict(who))
    monkeypatch.setattr(S, "describe", lambda out, url, **k: {"bytes": out.stat().st_size, "sha256": "s",
                                                              "upstream_url": url})
    return calls


WHO = {"upstream_version": "4.15.2", "osm_replication_time": "2026-10-29T04:00:00Z", "osm_replication_seq": "1"}


def test_the_fetch_extracts_each_zoom_band_by_range_requests_then_merges(tmp_path, monkeypatch):
    calls = _fake_tools(monkeypatch, WHO)
    out = tmp_path / "bc.pmtiles"
    S.fetch("https://build.protomaps.com/20261029.pmtiles", out)
    bands = [(c[c.index(next(a for a in c if a.startswith("--minzoom")))], c[-2]) for c in calls if c[1] == "extract"]
    assert [b[0] for b in bands] == [f"--minzoom={lo}" for lo, _ in S.ZOOM_PARTS]
    assert all(f"--bbox={','.join(map(str, S.BBOX))}" in c for c in calls if c[1] == "extract")
    assert sum(c[1] == "verify" for c in calls) == len(S.ZOOM_PARTS) + 1   # every band, then the merge
    assert next(c for c in calls if c[1] == "merge")
    assert out.read_bytes() == b"merged" and not (tmp_path / "bc.pmtiles.parts").exists()


def test_a_fetch_resumes_from_its_done_bands_and_refuses_another_build(tmp_path, monkeypatch):
    out = tmp_path / "bc.pmtiles"
    url = "https://build.protomaps.com/20261029.pmtiles"
    calls = _fake_tools(monkeypatch, WHO)
    parts = tmp_path / "bc.pmtiles.parts"
    parts.mkdir()
    (parts / "state.json").write_text(json.dumps({"upstream_url": url, **WHO, "bbox": list(S.BBOX),
                                                  "maxzoom": S.MAXZOOM}))
    for lo, hi in S.ZOOM_PARTS[:3]:                                  # an interrupted fetch: three bands done
        (parts / f"z{lo}-{hi}.pmtiles").write_bytes(b"part")
        (parts / f"z{lo}-{hi}.done").write_text("t")
    S.fetch(url, out)
    assert len([c for c in calls if c[1] == "extract"]) == len(S.ZOOM_PARTS) - 3
    # a half-fetched build cannot be finished from another one
    parts.mkdir()
    (parts / "state.json").write_text(json.dumps({"upstream_url": url, **WHO, "osm_replication_seq": "0",
                                                  "bbox": list(S.BBOX), "maxzoom": S.MAXZOOM}))
    with pytest.raises(SystemExit, match="another build"):
        S.fetch(url, out, replace=True)


def test_a_fetch_never_overwrites_the_source_unasked(tmp_path, monkeypatch):
    _fake_tools(monkeypatch, WHO)
    out = tmp_path / "bc.pmtiles"
    out.write_bytes(b"old")
    with pytest.raises(SystemExit, match="--replace"):
        S.fetch("https://build.protomaps.com/20261029.pmtiles", out)
    S.fetch("https://build.protomaps.com/20261029.pmtiles", out, replace=True)
    assert (tmp_path / "bc.pmtiles.prev").read_bytes() == b"old"


# --- ship stage: cut 7, no names outside British Columbia (T299) -------------------------------------------------------

def test_names_outside_the_province_are_dropped_and_roads_keep_their_ground():
    import mapbox_vector_tile
    from shapely.geometry import box
    # z1 tile (0, 0) spans lon -180..0, lat 0..85; "BC" is its western half (lon < -90)
    inside = box(-180, -10, -90, 86)
    down = {"y_coord_down": True}
    tile = mapbox_vector_tile.encode([
        {"name": "places", "features": [
            {"geometry": "POINT (1000 2000)", "properties": {"kind": "locality", "name": "In"}},
            {"geometry": "POINT (3000 2000)", "properties": {"kind": "locality", "name": "Out"}}]},
        {"name": "roads", "features": [
            {"geometry": "LINESTRING (2500 100, 3500 200)", "properties": {"kind": "highway", "name": "I-5",
                                                                           "name:en": "I-5", "shield_text": "5"}},
            {"geometry": "LINESTRING (1000 100, 3500 200)", "properties": {"kind": "highway", "name": "Hwy 99"}}]},
        {"name": "landuse", "features": [{"geometry": "POLYGON ((3000 3000, 3500 3000, 3500 3500, 3000 3000))",
                                          "properties": {"kind": "wood"}}]},
    ], default_options=down)
    out = mapbox_vector_tile.decode(G.confine_tile(tile, 1, 0, 0, inside), default_options=down)
    assert [f["properties"]["name"] for f in out["places"]["features"]] == ["In"]
    roads = out["roads"]["features"]
    assert roads[0]["properties"] == {"kind": "highway"}                   # drawn, not named or shielded
    assert roads[1]["properties"]["name"] == "Hwy 99"                       # enters BC: keeps its name
    assert len(out["landuse"]["features"]) == 1                             # ground is never cut
    # a tile wholly inside is copied byte for byte; one wholly outside loses every name
    assert G.confine_tile(tile, 1, 0, 0, box(-181, -90, 1, 90)) == tile
    gone = mapbox_vector_tile.decode(G.confine_tile(tile, 1, 0, 0, box(10, 10, 20, 20)), default_options=down)
    assert gone["places"]["features"] == [] and all("name" not in f["properties"] for f in gone["roads"]["features"])
