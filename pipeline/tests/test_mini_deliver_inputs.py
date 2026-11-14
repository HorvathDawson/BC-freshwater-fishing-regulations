"""T667 slice 8, step 2b: the deliver inputs the mini release names, and their LIVE DEFAULTS, unchanged.

The tile step (`--obstacles`, `--no-archive`, the admin areas from `--gpkg`) and the bundle step (`--source`, `--clim`,
`--dfo-typed`, `--dfo-cache`) each take an input a flag did not move before; with no flag each reads exactly what it
read before. The pins below are the live defaults; each was proved to bite by planting the old or a wrong default
(the ticket's "Slice 8, step 2b/3 built" lists the mutations)."""
from __future__ import annotations

import json

import pytest

from pipeline.tools import mini_release as M


def test_the_tile_step_reads_the_fetched_obstacles_unless_told_otherwise():
    from pipeline.common.curated import SOURCE
    from pipeline.deliver.tiles import __main__ as T
    from pipeline.deliver.tiles import obstacles
    a = T.parser().parse_args([])
    assert a.obstacles == SOURCE / obstacles.GPKG == SOURCE / "fish_obstacles/fish_obstacles.gpkg"
    assert a.no_archive is False
    b = T.parser().parse_args(["--obstacles", "/x/o.gpkg", "--no-archive", "--no-release"])
    assert (str(b.obstacles), b.no_archive, b.no_release) == ("/x/o.gpkg", True, True)


def test_a_tile_set_with_no_archive_is_refused_for_the_live_tiles_and_without_no_release(tmp_path):
    from pipeline.deliver.tiles.__main__ import no_archive_refusals
    from pipeline.tests import under_test
    assert no_archive_refusals(tmp_path / "tiles", True, False) == []
    assert any("live tiles directory" in p for p in no_archive_refusals(under_test.path("tiles"), True, False))
    assert any("--no-release" in p for p in no_archive_refusals(tmp_path / "tiles", False, False))
    assert any("--tiles-only" in p for p in no_archive_refusals(tmp_path / "tiles", True, True))


def test_a_sidecar_with_no_archive_names_no_tile_version(tmp_path, monkeypatch):
    from pipeline.common import section_handles
    from pipeline.deliver.tiles import __main__ as T
    monkeypatch.setattr(section_handles, "digest_for", lambda b: "0123456789abcdef")
    monkeypatch.setattr(section_handles, "registry_digest_for", lambda b: "fedcba9876543210")
    (tmp_path / "t" / "layers").mkdir(parents=True)
    T._write_sidecar(tmp_path / "atlas", tmp_path / "t", None)
    assert json.loads((tmp_path / "t" / "atlas.meta.json").read_text()) == {
        "section_handles": "0123456789abcdef", "build": "atlas", "registry": "fedcba9876543210"}
    T._write_sidecar(tmp_path / "atlas", tmp_path / "t", "0123456789abcdef.0123456789ab")
    assert json.loads((tmp_path / "t" / "atlas.meta.json").read_text())["tile_version"] == \
        "0123456789abcdef.0123456789ab"


def test_the_admin_areas_are_drawn_from_the_export_gpkg(tmp_path, monkeypatch):
    """The tile export drew the CONFIGURED gpkg's areas whatever `--gpkg` said: the mini atlas got the province's nine
    regions over its two. Now `export_admin(gpkg)` reads that file (the default `--gpkg` is the configured one)."""
    from pipeline.deliver.tiles import export
    monkeypatch.setenv("CURATED_ROOT", str(M.CURATED_TREE))
    fwa, _ = M.write_inputs(tmp_path / "in")
    (tmp_path / "layers").mkdir()                                   # the tile step makes it (`__main__`)
    counts = export.export_admin(str(fwa), tmp_path / "layers")
    regions = [json.loads(x)["properties"]["region_id"] for x in
               (tmp_path / "layers" / "region.geojsonl").read_text().splitlines() if x.strip()]
    assert sorted(regions) == ["1", "1", "2", "2"]          # the areas.json def's two, then the wmu dissolve's two
    assert counts["no_access"] == 1 and counts["indigenous_land"] == 1 and counts["parcel"] == 1


def test_the_bundle_step_names_its_inputs_or_reads_the_live_ones(monkeypatch, tmp_path):
    import importlib
    from pipeline.deliver import __main__ as D
    B = importlib.import_module("pipeline.deliver.bundle.build")      # the package re-exports `build`, the function
    seen = []
    monkeypatch.setattr(B, "build", lambda *a, **kw: seen.append(kw))
    D.main(["bundle", "--build", str(tmp_path), "--out", str(tmp_path / "b.sqlite")])
    assert {k: seen[0][k] for k in ("data_dir", "clim", "dfo_typed", "dfo_cache")} == dict.fromkeys(
        ("data_dir", "clim", "dfo_typed", "dfo_cache"))
    D.main(["bundle", "--build", str(tmp_path), "--out", str(tmp_path / "b.sqlite"), "--source", "/s",
            "--clim", "/c.json", "--dfo-typed", "/t", "--dfo-cache", "/k"])
    assert {k: str(seen[1][k]) for k in ("data_dir", "clim", "dfo_typed", "dfo_cache")} == {
        "data_dir": "/s", "clim": "/c.json", "dfo_typed": "/t", "dfo_cache": "/k"}


def test_the_gauge_envelope_is_the_live_feeds_unless_named(tmp_path):
    from pipeline.gauges.consume import bundle_tables as BT
    assert BT.clim_file() == BT.CLIM_PATH and BT.CLIM_PATH.name == "clim.json"
    assert BT.clim_file(tmp_path / "c.json") == tmp_path / "c.json"


def test_a_named_bundle_input_that_is_missing_is_refused(tmp_path):
    from pipeline.deliver.bundle.build import build
    for kw, flag in (({"clim": tmp_path / "nope.json"}, "--clim"), ({"dfo_typed": tmp_path / "nope"}, "--dfo-typed"),
                     ({"dfo_cache": tmp_path / "nope"}, "--dfo-cache")):
        with pytest.raises(FileNotFoundError, match=flag):
            build(tmp_path / "atlas", tmp_path / "b.sqlite", data_dir=tmp_path, **kw)
    assert not (tmp_path / "b.sqlite").exists()
