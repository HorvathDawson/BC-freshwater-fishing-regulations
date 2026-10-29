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


def _ship(tmp_path, tag, basemap):
    from pipeline.deliver.release.build import FORMATS, ship
    work = tmp_path / f"work-{tag}"
    work.mkdir()
    names = sorted(FORMATS)[:2]
    files = {}
    for n in names:
        files[n] = work / f"{n}.raw"
        files[n].write_bytes(f"{n} bytes".encode())
    pdf = tmp_path / "synopsis.pdf"
    pdf.write_bytes(b"%PDF")
    return ship(work, files, "d768d6e135e62b99", tmp_path / f"out-{tag}", basemap=basemap,
                tiles={"current": {"id": "t1"}, "assets": None, "mask": None}, valid_until="2027-03-31",
                regs_versions={"rows": 1}, sources={"basemap": {"date": "2026-03-05"}},
                bundle={"entries_digest": "e", "reach_digest": "r"}, synopsis=pdf)


def test_a_release_rebased_on_a_new_basemap_has_the_id_the_build_would_give_it(tmp_path):
    # T280: a basemap-only change ships without re-running the data build — and the id is not invented: it is the one
    # `ship` gives the same inputs with the new basemap block
    from pipeline.deliver.release import rebase as RB
    old_bm = {"id": "basemap-aaa", "url": "basemap/basemap-aaa/basemap-aaa.pmtiles", "bytes": 1, "sha256": "a"}
    new_bm = {"id": "basemap-bbb", "url": "basemap/basemap-bbb/basemap-bbb.pmtiles", "bytes": 2, "sha256": "b"}
    old = _ship(tmp_path, "old", old_bm)
    want = _ship(tmp_path, "want", new_bm)
    assert RB.release_id(json.loads((old / "manifest.json").read_text())) == old.name
    m = json.loads((old / "manifest.json").read_text())
    m["published"] = "2026-10-29"                                  # a publish stamp is kept, outside the hash
    (old / "manifest.json").write_text(json.dumps(m))
    got = RB.rebase(old, new_bm)
    assert got.name == want.name != old.name
    gm, wm = json.loads((got / "manifest.json").read_text()), json.loads((want / "manifest.json").read_text())
    assert gm.pop("published") == "2026-10-29" and gm == wm
    assert sorted(p.name for p in got.iterdir()) == sorted(p.name for p in want.iterdir())
    with pytest.raises(SystemExit, match="already names"):
        RB.rebase(got, new_bm)
    # a manifest that no longer recomputes to its id is refused
    m["regulations"]["valid_until"] = "2099-01-01"
    (old / "manifest.json").write_text(json.dumps(m))
    with pytest.raises(SystemExit, match="does not recompute"):
        RB.rebase(old, {**new_bm, "id": "basemap-ccc"})
