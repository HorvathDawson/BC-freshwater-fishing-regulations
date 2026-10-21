"""The release names the tile set the promoted tiles ARE, never "the last folder by name".

A tile version is "<handles16>.<tilebuild12>" and the build half is a hash, so sorting folders by name picks an arbitrary
one: after the P18 id rebuild the release went on naming the older set because "ffac…" sorts after "c9f9…".
"""
from __future__ import annotations

import json

import pytest

from pipeline.deliver.release.manifest import tiles_block

H = "d768d6e135e62b99"


def _set(root, v):
    d = root / "tiles" / v
    d.mkdir(parents=True)
    (d / f"atlas-{v}.pmtiles").write_bytes(v.encode())
    (d / "bc_outside.geojson").write_text("{}")


def _meta(tmp_path, v):
    p = tmp_path / "atlas.meta.json"
    p.write_text(json.dumps({"section_handles": H, "tile_version": v}))
    return p


def test_it_names_the_promoted_tiles_version_even_when_an_older_one_sorts_last(tmp_path):
    rel = tmp_path / "release"
    _set(rel, f"{H}.ffac7c1eb56e")          # older, sorts last
    _set(rel, f"{H}.c9f9604f12bf")          # what the promoted tiles are
    got = tiles_block(rel, H, _meta(tmp_path, f"{H}.c9f9604f12bf"))
    assert got["current"]["id"] == f"{H}.c9f9604f12bf"
    assert got["current"]["url"] == f"tiles/{H}.c9f9604f12bf/atlas-{H}.c9f9604f12bf.pmtiles"


def test_promoted_tiles_with_no_released_folder_are_refused(tmp_path):
    rel = tmp_path / "release"
    _set(rel, f"{H}.ffac7c1eb56e")
    with pytest.raises(SystemExit, match="c9f9604f12bf"):
        tiles_block(rel, H, _meta(tmp_path, f"{H}.c9f9604f12bf"))


def test_unstamped_or_absent_tiles_name_none(tmp_path):
    rel = tmp_path / "release"
    _set(rel, f"{H}.ffac7c1eb56e")
    p = tmp_path / "atlas.meta.json"
    p.write_text(json.dumps({"section_handles": H}))
    assert tiles_block(rel, H, p) is None
    assert tiles_block(rel, H, tmp_path / "missing.json") is None


def test_tiles_of_another_sid_space_are_refused(tmp_path):
    rel = tmp_path / "release"
    _set(rel, f"{H}.ffac7c1eb56e")
    with pytest.raises(SystemExit, match="sid space"):
        tiles_block(rel, H, _meta(tmp_path, "0000000000000000.ffac7c1eb56e"))
