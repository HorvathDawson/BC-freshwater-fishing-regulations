"""T75 — the basemap is released like a tile version and named in the manifest (`basemap: {id, url, bytes, sha256}`),
so a native install can fetch the ground with a sha check; publish uploads it before any data file."""
from __future__ import annotations

import hashlib
import json

import pytest

from pipeline.deliver.release import publish
from pipeline.deliver.release.manifest import basemap_block, manifest
from pipeline.deliver.tiles import basemap as B


def _released(tmp_path, body=b"PMTiles basemap bytes"):
    tiles, rel = tmp_path / "tiles", tmp_path / "release"
    tiles.mkdir()
    a = tiles / "basemap.pmtiles"
    a.write_bytes(body)
    return a, tiles, rel, B.to_release(a, rel, tiles)


def test_the_basemap_step_releases_a_content_addressed_copy_and_a_sidecar(tmp_path):
    a, tiles, rel, meta = _released(tmp_path)
    sha = hashlib.sha256(a.read_bytes()).hexdigest()
    vid = f"basemap-{sha[:12]}"
    assert meta == {"basemap_version": vid, "bytes": a.stat().st_size, "sha256": sha}
    assert json.loads((tiles / B.META).read_text()) == meta
    assert (rel / "basemap" / vid / f"{vid}.pmtiles").read_bytes() == a.read_bytes()
    assert B.to_release(a, rel, tiles) == meta                  # the same bytes again: the same id, nothing moves
    (rel / "basemap" / vid / f"{vid}.pmtiles").write_bytes(b"tampered")
    with pytest.raises(SystemExit, match="immutable"):
        B.to_release(a, rel, tiles)


def test_the_manifest_names_the_released_basemap(tmp_path):
    a, tiles, rel, meta = _released(tmp_path)
    got = basemap_block(rel, tiles / B.META)
    vid = meta["basemap_version"]
    assert got == {"id": vid, "url": f"basemap/{vid}/{vid}.pmtiles", "bytes": meta["bytes"], "sha256": meta["sha256"]}
    assert manifest("r-1", {}, "d768d6e135e62b99", basemap=got)["basemap"] == got
    # none released yet: null, and the app keeps the ground it holds
    assert basemap_block(rel, tmp_path / "nowhere.json") is None


def test_a_sidecar_that_does_not_describe_the_released_copy_is_refused(tmp_path):
    a, tiles, rel, meta = _released(tmp_path)
    p = rel / "basemap" / meta["basemap_version"] / f"{meta['basemap_version']}.pmtiles"
    p.write_bytes(b"other")
    with pytest.raises(SystemExit, match="not the basemap"):
        basemap_block(rel, tiles / B.META)
    p.unlink()
    with pytest.raises(SystemExit, match="missing"):
        basemap_block(rel, tiles / B.META)


def test_publish_uploads_the_basemap_before_the_data_and_the_manifest_last(tmp_path):
    a, tiles, rel, meta = _released(tmp_path)
    r = rel / "r-1"
    r.mkdir()
    (r / "core-x.gz").write_bytes(b"x")
    m = manifest("r-1", {"core": {"url": "r-1/core-x.gz"}}, "d768d6e135e62b99",
                 basemap=basemap_block(rel, tiles / B.META))
    (r / "manifest.json").write_text(json.dumps(m))
    keys = [p.key for p in publish.plan(r, rel)]
    assert keys == [m["basemap"]["url"], "r-1/core-x.gz", "manifest.json"]
