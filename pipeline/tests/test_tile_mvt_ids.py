"""Section layers carry `section_id` as their OWN MVT feature id (P18 tile step, from the N1 spike).

MapLibre Native ignores the style's `promoteId`, so on Android every feature's id came back
empty and `setFeatureState` addressed nothing. The fix is in the tile: the section layers
(`stream`, `lake`, `wetland` — `lake_flowing` / `wetland_flowing` are style layers over the
last two) write `section_id` as the feature id.

AND ONLY THERE (T202, owner 2026-10-27): the tile carries no `section_id` PROPERTY. It was an 18 MB
duplicate of the MVT id; the web style stopped promoting it and reads `feature.id` like the phone.
The layer FILES keep the property (the bundle's section access reads them). The id is stamped per
feature into the GeoJSON `id` while the layer files are copied into the build's temp dir, where the
properties are also cut to what the tile carries (`tippe._tile_props`).
"""

from __future__ import annotations

import gzip
import subprocess
from pathlib import Path

import orjson
import pytest

from pipeline.deliver.tiles import layers as L
from pipeline.deliver.tiles import tippe

SECTION_LAYERS = {"stream", "lake", "wetland"}


def test_exactly_the_section_layers_take_section_id_as_their_mvt_id():
    got = {s.name for s in L.ALL if s.mvt_id}
    assert got == SECTION_LAYERS
    for s in L.ALL:
        if s.mvt_id:
            assert s.attrs[0] == "section_id", f"{s.name}: an MVT id must be the section handle"
            assert not s.decorative
        elif not s.decorative:
            # an area layer's id is a string (`area_id`, `basin_id`, ...): MVT ids are
            # unsigned integers, so those keep property-based styling (N1).
            assert s.attrs[0] != "section_id", f"{s.name} carries section_id but no MVT id"


def test_the_contract_declares_the_mvt_id_and_no_section_id_property():
    c = L.contract()["layers"]
    assert {n for n, v in c.items() if v["mvtId"]} == SECTION_LAYERS
    for n in SECTION_LAYERS:
        assert c[n]["featureId"] == "section_id" and "section_id" not in c[n]["attrs"]
        assert L.BY_NAME[n].attrs[0] == "section_id", f"{n}: the layer FILE keeps the handle"
    import json
    on_disk = json.loads(Path(tippe.__file__).with_name("tile-contract.json").read_text())
    assert on_disk["layers"] == c, "tile-contract.json is stale: python -m pipeline.deliver.tiles --write-contract"


def _feature(layer: str, minzoom: int, **props) -> bytes:
    return orjson.dumps({
        "type": "Feature", "geometry": {"type": "Point", "coordinates": [-123.0, 49.0]},
        "properties": props, "tippecanoe": {"layer": layer, "minzoom": minzoom, "maxzoom": 14},
    }) + b"\n"


def _layer_dir(tmp_path: Path) -> Path:
    d = tmp_path / "layers"
    d.mkdir()
    (d / "stream.geojsonl").write_bytes(
        _feature("stream", 4, section_id=11, ord=7) + _feature("stream", 12, section_id=12, ord=1))
    (d / "lake.geojsonl").write_bytes(_feature("lake", 6, section_id=21, water="lake", area_m2=9))
    (d / "under_lake.geojsonl").write_bytes(_feature("under_lake", 10))
    (d / "park.geojsonl").write_bytes(_feature("park", 6, area_id="X PARK", name="X", kind="provincial"))
    return d


def _captured_build(tmp_path, monkeypatch, **kw):
    """Run `tippe.build` with tippecanoe faked out; return each command plus the features of
    every input file AS IT WAS when that command ran (the temp dir is gone afterwards)."""
    calls = []

    def fake_run(cmd):
        files = {}
        for a in cmd:
            if a.endswith(".geojsonl"):
                files[a] = [orjson.loads(ln) for ln in Path(a).read_bytes().splitlines() if ln.strip()]
        calls.append((cmd, files))
        Path(cmd[cmd.index("-o") + 1]).write_bytes(b"")

    monkeypatch.setattr(tippe, "_run", fake_run)
    monkeypatch.setattr(tippe, "check", lambda: None)
    tippe.build(_layer_dir(tmp_path), tmp_path / "atlas.pmtiles", **kw)
    return calls


@pytest.mark.parametrize("bands", [dict(), dict(minzoom=9, maxzoom=14)], ids=["two-band", "single-pass"])
def test_every_tippecanoe_run_feeds_section_features_with_section_id_as_their_id(tmp_path, monkeypatch, bands):
    calls = _captured_build(tmp_path, monkeypatch, **bands)
    runs = [(cmd, files) for cmd, files in calls if cmd[0] == "tippecanoe"]
    assert runs and len(runs) == (2 if not bands else 1)
    for cmd, files in runs:
        # the id is stamped and checked per feature — see the module docstring
        assert not any(a.startswith(("--use-attribute-for-id", "-ai", "--generate-ids")) for a in cmd)
        seen = set()
        for path, feats in files.items():
            for f in feats:
                layer = f["tippecanoe"]["layer"]
                seen.add(layer)
                if layer in SECTION_LAYERS:
                    assert isinstance(f.get("id"), int), (layer, f)
                    assert "section_id" not in f["properties"], f"{layer}: the id ships twice: {f}"
                else:
                    assert "id" not in f, f"{layer} must not get an MVT id: {f}"
        assert {"stream", "park"} <= seen
    if not bands:
        low, high = runs
        low_streams = [f for fs in low[1].values() for f in fs if f["tippecanoe"]["layer"] == "stream"]
        assert [f["id"] for f in low_streams] == [11], "the low band still holds only what it draws"
        high_streams = [f for fs in high[1].values() for f in fs if f["tippecanoe"]["layer"] == "stream"]
        assert [f["id"] for f in high_streams] == [11, 12]


def test_the_tile_carries_only_what_a_reader_needs(tmp_path, monkeypatch):
    """T202: `water` that only says the layer's own name, and an unnamed lake's area where the outline width cannot
    show it (the app reads a missing area as 10,000 m2), are left out; a named lake keeps both name and area."""
    lake = L.BY_NAME["lake"]
    assert tippe._tile_props(lake, {"section_id": 1, "water": "lake", "area_m2": 99_999}) == {}
    assert tippe._tile_props(lake, {"section_id": 1, "water": "lake", "area_m2": 100_000}) == {"area_m2": 100_000}
    assert tippe._tile_props(lake, {"section_id": 1, "water": "stream", "area_m2": 9}) == {"water": "stream"}
    assert tippe._tile_props(lake, {"section_id": 1, "name": "Cultus Lake", "water": "lake", "area_m2": 9}) == \
        {"name": "Cultus Lake", "area_m2": 9}
    assert tippe._tile_props(L.BY_NAME["wetland"], {"section_id": 1, "water": "wetland"}) == {}
    # anything a layer file carries that the spec does not declare never reaches a tile
    assert tippe._tile_props(L.BY_NAME["region"], {"region_id": "7A", "name": "Omineca"}) == {"region_id": "7A"}
    calls = _captured_build(tmp_path, monkeypatch)
    lakes = [f for cmd, files in calls if cmd[0] == "tippecanoe" for fs in files.values() for f in fs
             if f["tippecanoe"]["layer"] == "lake"]
    assert lakes and all(f["properties"] == {} and f["id"] == 21 for f in lakes)


def test_mu_is_its_own_pass_to_z12_and_the_high_band_never_holds_it(tmp_path, monkeypatch):
    """T202: the units stop at z12, built in their own pass so z12 is a TOP zoom (finer edge), then joined."""
    assert tippe.OWN_PASS == ("mu",) and L.BY_NAME["mu"].maxzoom == 12
    d = _layer_dir(tmp_path)
    (d / "mu.geojsonl").write_bytes(_feature("mu", 7, mu_id="2-4"))
    calls = []

    def fake_run(cmd):
        calls.append((cmd, [a for a in cmd if a.endswith(".geojsonl")]))
        Path(cmd[cmd.index("-o") + 1]).write_bytes(b"")

    monkeypatch.setattr(tippe, "_run", fake_run)
    monkeypatch.setattr(tippe, "check", lambda: None)
    tippe.build(d, tmp_path / "atlas.pmtiles")
    runs = [(c, fs) for c, fs in calls if c[0] == "tippecanoe"]
    assert len(runs) == 3
    (low, low_in), (high, high_in), (mu, mu_in) = runs
    assert any(f.endswith("mu.geojsonl") for f in low_in)
    assert not any(f.endswith("mu.geojsonl") for f in high_in)
    assert [Path(f).name for f in mu_in] == ["mu.geojsonl"]
    assert "--maximum-zoom=12" in mu and "--full-detail=12" in mu
    assert f"--simplification-at-maximum-zoom={tippe.OWN_PASS_SIMPLIFICATION_AT_MAX}" in mu
    assert f"--full-detail={tippe.TOP_DETAIL}" in high and "--maximum-zoom=13" in high
    join = [c for c, _ in calls if c[0] == "tile-join"][0]
    assert sum(a.endswith(".mbtiles") for a in join) == 3


def test_tile_join_is_not_told_to_drop_ids(tmp_path, monkeypatch):
    calls = _captured_build(tmp_path, monkeypatch)
    joins = [cmd for cmd, _ in calls if cmd[0] == "tile-join"]
    assert len(joins) == 1
    assert not any(a.startswith(("--no-feature-ids", "-x", "--exclude")) for a in joins[0])


def test_a_section_feature_without_a_section_id_fails_the_build(tmp_path):
    src = tmp_path / "stream.geojsonl"
    src.write_bytes(_feature("stream", 4, ord=3))
    with pytest.raises(SystemExit, match="section_id"):
        tippe._copy_layer(src, tmp_path / "out.geojsonl", max_minzoom=None, id_attr="section_id")


# ----------------------------------------------------------------- the built archive

def _tile(archive: Path, z: int, x: int, y: int) -> bytes:
    r = subprocess.run(["pmtiles", "tile", str(archive), str(z), str(x), str(y)], capture_output=True)
    if r.returncode:
        raise RuntimeError(r.stderr.decode()[-500:])
    data = r.stdout
    return gzip.decompress(data) if data[:2] == b"\x1f\x8b" else data


def _features(data: bytes, layer: str):
    from mapbox_vector_tile.Mapbox import vector_tile_pb2 as PB
    t = PB.tile()
    t.ParseFromString(data)
    for lyr in t.layers:
        if lyr.name != layer:
            continue
        keys = list(lyr.keys)
        for f in lyr.features:
            props = {keys[f.tags[i]]: lyr.values[f.tags[i + 1]].ListFields()[0][1]
                     for i in range(0, len(f.tags), 2)}
            yield (f.id if f.HasField("id") else None), props


@pytest.mark.slow
@pytest.mark.needs_tiles
def test_the_built_archive_carries_section_id_as_the_stream_feature_id(request):
    """Lower Mainland tiles that hold streams: z8 (top of the low band), z9 (bottom of the high
    band) and z13 — so the id is proven on BOTH sides of the tile-join, and is in the tile ONCE."""
    from pipeline.common.curated import GENERATED
    from pipeline.tests.conftest import need
    archive = need(request, "tiles", GENERATED.tiles / "atlas.pmtiles", "python -m pipeline.deliver.tiles --tiles-only")
    for z, x, y in ((8, 40, 87), (9, 81, 175), (13, 1311, 2807)):
        feats = list(_features(_tile(archive, z, x, y), "stream"))
        assert feats, f"no stream features in z{z}/{x}/{y}"
        bad = [(fid, p) for fid, p in feats if fid is None or "section_id" in p]
        assert not bad, f"z{z}/{x}/{y}: {len(bad)} of {len(feats)} streams lack an MVT id or repeat it, e.g. {bad[:3]}"
