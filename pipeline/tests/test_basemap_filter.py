"""T85 / T202 — the basemap build keeps only the landuse kinds, water lines and label points the style draws
(`tiles.basemap.feature_filter`; the app side is `app/tools/basemap-filter.test.ts`)."""

from __future__ import annotations

import json
from pathlib import Path

from pipeline.deliver.tiles import basemap as B
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
    final = calls[-1]
    assert final[0] == "tile-join" and final[final.index("-o") + 1] == str(out)
    assert [Path(p).name for p in final[final.index(str(out)) + 1:]] == ["low-z13.pmtiles", "top-z13.pmtiles",
                                                                        "roads-z13.pmtiles"]


def test_a_rebuilt_road_drops_urban_kinds_and_unread_keys():
    assert R.props_of({"kind": "path", "kind_detail": "sidewalk"}) is None
    named = R.props_of({"kind": "minor_road", "kind_detail": "unclassified", "name": "Ashlu River FSR",
                        "min_zoom": 13, "sort_rank": 400, "ref": "x", "oneway": "yes"})
    assert named == {"kind": "minor_road", "kind_detail": "unclassified", "name": "Ashlu River FSR", "min_zoom": 13}
    # min_zoom is only the label sort key: an unnamed line has no label
    assert R.props_of({"kind": "path", "kind_detail": "track", "min_zoom": 13}) == {"kind": "path",
                                                                                    "kind_detail": "track"}
