"""Section layers carry `section_id` as their OWN MVT feature id (P18 tile step, from the N1 spike).

MapLibre Native ignores the style's `promoteId`, so on Android every feature's id came back
empty and `setFeatureState` addressed nothing. The fix is in the tile: the section layers
(`stream`, `lake`, `wetland` — `lake_flowing` / `wetland_flowing` are style layers over the
last two) write `section_id` as the feature id. `section_id` STAYS a property as well, so the
web style's `promoteId` keeps working unchanged.

Not `tippecanoe --use-attribute-for-id=section_id`: that flag DELETES the attribute it reads
(measured, tippecanoe 2.79), so the property would vanish from the tile. The id is stamped
per feature into the GeoJSON `id` while the layer files are copied into the build's temp dir.
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


def test_the_contract_declares_the_mvt_id_and_keeps_section_id_a_property():
    c = L.contract()["layers"]
    assert {n for n, v in c.items() if v["mvtId"]} == SECTION_LAYERS
    for n in SECTION_LAYERS:
        assert c[n]["featureId"] == "section_id" and "section_id" in c[n]["attrs"]
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
        # the flag would strip section_id from the properties — see the module docstring
        assert not any(a.startswith(("--use-attribute-for-id", "-ai", "--generate-ids")) for a in cmd)
        seen = set()
        for path, feats in files.items():
            for f in feats:
                layer = f["tippecanoe"]["layer"]
                seen.add(layer)
                if layer in SECTION_LAYERS:
                    assert f["id"] == f["properties"]["section_id"], (layer, f)
                else:
                    assert "id" not in f, f"{layer} must not get an MVT id: {f}"
        assert {"stream", "park"} <= seen
    if not bands:
        low, high = runs
        low_streams = [f for fs in low[1].values() for f in fs if f["tippecanoe"]["layer"] == "stream"]
        assert [f["id"] for f in low_streams] == [11], "the low band still holds only what it draws"
        high_streams = [f for fs in high[1].values() for f in fs if f["tippecanoe"]["layer"] == "stream"]
        assert [f["id"] for f in high_streams] == [11, 12]


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
@pytest.mark.xfail(strict=True, reason="atlas.pmtiles predates P18 tile ids; passes after the tiles-only "
                                       "rebuild — then DELETE this marker (strict, so an XPASS fails)")
def test_the_built_archive_carries_section_id_as_the_stream_feature_id(request):
    """Lower Mainland tiles that hold streams: z8 (top of the low band), z9 (bottom of the high
    band) and z14 — so the id is proven on BOTH sides of the tile-join."""
    from pipeline.common.curated import GENERATED
    from pipeline.tests.conftest import need
    archive = need(request, "tiles", GENERATED.tiles / "atlas.pmtiles", "python -m pipeline.deliver.tiles --tiles-only")
    for z, x, y in ((8, 40, 87), (9, 81, 175), (14, 2623, 5614)):
        feats = list(_features(_tile(archive, z, x, y), "stream"))
        assert feats, f"no stream features in z{z}/{x}/{y}"
        bad = [(fid, p.get("section_id")) for fid, p in feats if fid is None or fid != p.get("section_id")]
        assert not bad, f"z{z}/{x}/{y}: {len(bad)} of {len(feats)} streams' id != section_id, e.g. {bad[:3]}"
