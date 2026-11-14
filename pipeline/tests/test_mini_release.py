"""T667 slice 8: the mini release's INPUTS (an invented FWA-shaped GeoPackage + coast pair, from the committed
`pipeline/tests/fixtures/mini/world.json`) and the atlas build's input flags it needs (`--gpkg` reaching the area
layers and the drainage areas, `--coast-gpkg`). No live data: every gpkg here is written into `tmp_path`."""
from __future__ import annotations

import argparse
import json

import pyogrio
import pytest

from pipeline.tools import mini_release as M


@pytest.fixture(scope="module")
def inputs(tmp_path_factory):
    return M.write_inputs(tmp_path_factory.mktemp("mini-inputs"))


def test_the_world_is_invented_and_its_ids_sit_in_their_own_ranges():
    w = M.load_world()
    assert w["format"] == M.FORMAT
    assert all(s["blk"] >= 900_000_001 for s in w["streams"])
    assert all(b["wbk"] >= 800_000_001 for b in w["waterbodies"])
    gnis = [x["gnis_id"] for x in w["streams"] + w["waterbodies"] if x.get("gnis_id")]
    assert gnis and all(g >= 990_001 for g in gnis)


def test_a_break_outside_its_line_is_refused(tmp_path):
    w = json.loads(M.WORLD.read_text(encoding="utf-8"))
    w["streams"][0]["breaks"] = [5000, 99_000]
    p = tmp_path / "world.json"
    p.write_text(json.dumps(w))
    with pytest.raises(ValueError, match="breaks must rise strictly inside"):
        M.load_world(p)
    w["streams"][0]["breaks"] = [5000, 10000]                      # the lake run's ends are no longer breaks
    p.write_text(json.dumps(w))
    with pytest.raises(ValueError, match="must start and end on a break"):
        M.load_world(p)


def test_codes_are_fwa_width():
    w = M.load_world()
    assert M.full_wsc(w, "").startswith("920-123456-000000") and len(M.full_wsc(w, "").split("-")) == 21
    assert M.full_wsc(w, "222222-500000").split("-")[:4] == ["920", "123456", "222222", "500000"]
    assert M.full_wsc(w, None).startswith("999-999999")


def test_the_gpkg_carries_every_layer_and_column_the_atlas_reads(inputs):
    from pipeline.atlas.graph.blk_chains import _STREAM_COLUMNS
    fwa, coast = inputs
    layers = {n for n, _ in pyogrio.list_layers(fwa)}
    assert {"streams", "lakes", "wetlands", "manmade", "watersheds", "wmu", "tidal_boundary", "pfma_areas"} <= layers
    assert set(_STREAM_COLUMNS) <= set(pyogrio.read_info(fwa, layer="streams")["fields"])
    assert {"WILDLIFE_MGMT_UNIT_ID", "REGION_RESPONSIBLE_ID"} <= set(pyogrio.read_info(fwa, layer="wmu")["fields"])
    assert {n for n, _ in pyogrio.list_layers(coast)} == {"land", "rivers"}
    assert "BLUE_LINE_KEY" in pyogrio.read_info(coast, layer="rivers")["fields"]
    assert fwa.stat().st_size < 1_000_000 and coast.stat().st_size < 1_000_000


def test_the_streams_read_back_as_fwa_features_mouth_first(inputs):
    from pipeline.atlas.graph.blk_chains import load_stream_fids
    rows = load_stream_fids(str(inputs[0]))
    river = sorted((r for r in rows if r.blk == "900000001"), key=lambda r: r.down_m)
    assert [round(r.down_m) for r in river] == [0, 5000, 10000, 15000, 18000, 25000, 30000]
    assert river[-1].up_m == pytest.approx(45000)
    assert [r.wbk for r in river if r.wbk] == ["800000001"]           # the lake run, one feature
    mock = next(r for r in rows if r.blk == "900000002")
    assert mock.down_node == river[2].down_node                        # the confluence is a shared vertex
    assert mock.wsc == "920-123456-222222"


def test_area_polygons_come_from_the_builds_own_gpkg(inputs, tmp_path):
    """`load_area_polys` read the CONFIGURED gpkg whatever the build was given: the mini atlas got the province's
    regions. Now it reads the accessor's file — and clips to THAT file's outline (the mini world sits inside the real
    province, so only the mini outline cuts this watershed)."""
    from pipeline.atlas.fwa import FWADataAccessor
    from pipeline.atlas.splits.area_splits import load_area_polys
    w = json.loads(M.WORLD.read_text(encoding="utf-8"))
    w["watersheds"][0]["box"] = [0, -10000, 60000, 10000]              # 10 km past the mini outline (x = 50 km)
    fwa, _ = M.write_inputs(tmp_path / "wide", w)
    acc = FWADataAccessor(fwa)
    regions = load_area_polys(acc, {"id": "regions", "layer": "wmu", "name_field": "REGION_RESPONSIBLE_ID",
                                    "which": "all"})
    assert sorted(regions) == ["1", "2"]
    ws = load_area_polys(acc, {"id": "ws", "layer": "watersheds", "name_field": "GNIS_NAME", "which": "all"})
    minx, _, maxx, _ = ws["Fixture River"].bounds
    assert (maxx - minx) == pytest.approx(50000)                       # clipped to the mini outline, not the live one


def test_the_drainage_areas_read_the_gpkg_the_registry_is_given(monkeypatch, inputs):
    from pipeline.atlas.registry import build_registry
    from pipeline.atlas.splits import pfma_drainage
    from pipeline.common.models import StreamGraph
    seen = []
    monkeypatch.setattr(pfma_drainage, "basin_areas", lambda areas, gpkg: seen.append(gpkg) or {})
    build_registry(StreamGraph(), pinned={}, gpkg=str(inputs[0]))
    build_registry(StreamGraph(), pinned={})
    assert seen == [str(inputs[0]), pfma_drainage._GPKG]               # the default is the configured file, as before


def test_the_mini_basin_drains_to_its_invented_area(inputs):
    from pipeline.atlas.splits import pfma_drainage
    assert pfma_drainage.basin_areas(pfma_drainage.MINTED_AREAS, str(inputs[0])) == {"920-123456-": 29}


def test_the_atlas_input_flags_default_to_the_configured_files():
    from pipeline.atlas import build
    from project_config import get_config
    a = build._parse_args(["--full"])
    assert a.gpkg == str(get_config().fwa_data_gpkg) and a.coast_gpkg == str(get_config().fwa_coast_gpkg)
    b = build._parse_args(["--full", "--gpkg", "/x/fwa.gpkg", "--coast-gpkg", "/x/coast.gpkg"])
    assert (b.gpkg, b.coast_gpkg) == ("/x/fwa.gpkg", "/x/coast.gpkg")
    assert isinstance(b, argparse.Namespace)


def test_a_digest_ignores_the_clock_and_the_scratch_path(tmp_path):
    a = {"build_ts": "2026-11-14T10:00:00", "input_hashes": {"source:fwa_gpkg": "stamp:1:2"}, "n": 1}
    b = {"build_ts": "2026-11-14T11:00:00", "input_hashes": {"source:fwa_gpkg": "stamp:1:9"}, "n": 1}
    na = M._normalised("atlas/graph.pkl.meta.json", json.dumps(a).encode(), tmp_path)
    nb = M._normalised("atlas/graph.pkl.meta.json", json.dumps(b).encode(), tmp_path)
    assert na == nb
    nc = M._normalised("atlas/graph.pkl.meta.json", json.dumps(dict(b, n=2)).encode(), tmp_path)
    assert nc != nb                                                    # a real difference still shows


def test_the_build_refuses_a_directory_inside_the_fixture_or_the_data_tree():
    with pytest.raises(SystemExit, match="scratch only"):
        M.build(M.FIXTURE / "out")
