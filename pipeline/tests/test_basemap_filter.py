"""T85 — the basemap build keeps only the landuse kinds and water lines the style draws
(`tiles.basemap.feature_filter`; the app side is `app/tools/basemap-filter.test.ts`)."""

from __future__ import annotations

import json
from pathlib import Path

from pipeline.deliver.tiles import basemap as B


def test_the_filter_keeps_drawn_landuse_the_wetland_standin_and_river_lines():
    f = B.feature_filter()
    assert f["landuse"][:2] == ["in", "kind"]
    kept = set(f["landuse"][2:])
    assert {"wood", "glacier", "forest", "wetland"} <= kept
    assert not kept & {"park", "national_park", "nature_reserve", "military", "residential", "farmland"}
    # every polygon and label point of `water` stays; of its lines only the rivers
    assert f["water"] == ["any", ["!=", "$type", "LineString"], ["in", "kind", "river"]]
    assert set(B.WATER_LINES_DROPPED) == {"stream"}


def test_the_build_passes_it_to_tile_join(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(B.shutil, "which", lambda t: f"/bin/{t}")
    monkeypatch.setattr(B, "_boundary_4326", lambda src, out: out)

    def run(cmd):
        calls.append(cmd)
        Path(cmd[3] if cmd[0] == "pmtiles" else cmd[cmd.index("-o") + 1]).write_bytes(b"x")
    monkeypatch.setattr(B, "_run", run)
    src = tmp_path / "bc.pmtiles"
    src.write_bytes(b"x" * 10)
    B.build(src, tmp_path / "basemap.pmtiles", tmp_path / "bc_boundary.geojson")
    join = next(c for c in calls if c[0] == "tile-join")
    assert json.loads(join[join.index("-j") + 1]) == B.feature_filter()
