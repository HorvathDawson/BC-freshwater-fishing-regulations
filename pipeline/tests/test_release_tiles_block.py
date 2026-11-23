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


def test_a_tile_version_is_the_sid_space_plus_the_tile_build():
    """Moved from test_tiles.py (T667): invented digests belong in a test that builds its own data."""
    from pipeline.deliver.tiles import version as V
    assert V.version_id("d768d6e135e62b99", "5f1c0a9e2b7d") == "d768d6e135e62b99.5f1c0a9e2b7d"
    with pytest.raises(ValueError):
        V.version_id("d768d6e1", "5f1c0a9e2b7d")
    meta = {"name": "atlas", "vector_layers": [{"id": "stream"}]}
    got = V.stamped_metadata(meta, "d768d6e135e62b99.5f1c0a9e2b7d", "d768d6e135e62b99", "8c77ba1cd2dc3929")
    assert got == {**meta, "tile_version": "d768d6e135e62b99.5f1c0a9e2b7d", "section_handles": "d768d6e135e62b99",
                   "labels_registry": "8c77ba1cd2dc3929"}


def test_the_assets_block_names_the_folder_and_its_pack_written_beside_it_when_missing(tmp_path):
    """(T919 c) `tiles.assets.pack` is the folder as ONE file, hashed as it is on disk; a tile version released before
    the pack gains it beside the folder (its other files untouched). The folder's own keys stay: older apps read them."""
    import hashlib

    from pipeline.deliver.tiles.assets import files_sha256
    rel = tmp_path / "release"
    v = f"{H}.c9f9604f12bf"
    _set(rel, v)
    folder = rel / "tiles" / v / "map-assets-1"
    (folder / "sprites/v4").mkdir(parents=True)
    (folder / "sprites/v4/light.json").write_text("{}")
    files = {"sprites/v4/light.json": hashlib.sha256(b"{}").hexdigest()}
    (folder / "assets.json").write_text(json.dumps({"id": "map-assets-1", "pin": "p", "files": files}))
    got = tiles_block(rel, H, _meta(tmp_path, v))["assets"]
    pack = rel / "tiles" / v / "map-assets-1.tar.zlib"
    assert pack.exists()
    assert got == {"id": "map-assets-1", "url": f"tiles/{v}/map-assets-1/", "pin": "p",
                   "files_sha256": files_sha256(folder / "assets.json"),
                   "pack": {"url": f"tiles/{v}/map-assets-1.tar.zlib", "bytes": pack.stat().st_size,
                            "sha256": hashlib.sha256(pack.read_bytes()).hexdigest(),
                            "unpacked": (folder / "assets.json").stat().st_size + 2}}
    assert tiles_block(rel, H, _meta(tmp_path, v))["assets"] == got        # the same pack, not written again


def _released_pack(tmp_path):
    """A release root whose manifest names a tile version's assets folder and its pack, as `tiles_block` writes them."""
    import hashlib
    rel = tmp_path / "release"
    v = f"{H}.c9f9604f12bf"
    _set(rel, v)
    folder = rel / "tiles" / v / "map-assets-1"
    (folder / "sprites/v4").mkdir(parents=True)
    for name, body in (("light.json", b"{}"), ("dark.json", b"[]")):
        (folder / "sprites/v4" / name).write_bytes(body)
    files = {f"sprites/v4/{n}": hashlib.sha256(b).hexdigest() for n, b in (("light.json", b"{}"), ("dark.json", b"[]"))}
    (folder / "assets.json").write_text(json.dumps({"id": "map-assets-1", "pin": "p", "files": files}))
    return rel, folder, {"tiles": tiles_block(rel, H, _meta(tmp_path, v))}


def test_the_release_check_holds_the_assets_pack_to_the_manifest_and_to_its_folder(tmp_path):
    """(T919 c) `manifest_problems` checks the pack the manifest names: on disk, at its bytes + sha256 + unpacked size,
    and member for member its folder's assets.json. Every way it can be wrong is named; the manifest as built passes."""
    import hashlib
    import io
    import tarfile
    import zlib

    from pipeline.deliver.tiles.assets import pack_path
    from pipeline.release import pack_problems_of
    rel, folder, m = _released_pack(tmp_path)
    pk = m["tiles"]["assets"]["pack"]
    assert pack_problems_of(m, rel) == []
    assert pack_problems_of({"tiles": {"assets": {**m["tiles"]["assets"], "pack": None}}}, rel) == []   # before the pack
    pack = pack_path(folder)
    good = pack.read_bytes()
    # the manifest's numbers
    assert pack_problems_of({"tiles": {"assets": {**m["tiles"]["assets"], "pack": {**pk, "bytes": 1}}}}, rel) == [
        f"tiles.assets.pack: {pk['url']} is {len(good):,} B, the manifest says 1"]
    assert pack_problems_of({"tiles": {"assets": {**m["tiles"]["assets"], "pack": {**pk, "sha256": "0" * 64}}}}, rel) == [
        f"tiles.assets.pack: {pk['url']} is not the sha256 the manifest names"]
    assert pack_problems_of({"tiles": {"assets": {**m["tiles"]["assets"], "pack": {**pk, "unpacked": 1}}}}, rel) == [
        f"tiles.assets.pack: unpacked is 1, the folder takes {pk['unpacked']} B"]
    # the folder moved under the pack: its list names other bytes than the pack carries (manifest numbers still the pack's)
    listing = folder / "assets.json"
    keep = listing.read_text()
    lst = json.loads(keep)
    lst["files"]["sprites/v4/light.json"] = "f" * 64
    listing.write_text(json.dumps(lst))
    got = pack_problems_of(m, rel)
    assert f"tiles.assets.pack: {pack}: its assets.json is not the folder's" in got
    assert f"tiles.assets.pack: {pack}: sprites/v4/light.json is not the sha256 the folder's assets.json names" in got
    listing.write_text(keep)
    # a pack missing a member (rewritten, the manifest updated to its new bytes so only the members are wrong)
    raw = io.BytesIO()
    with tarfile.open(fileobj=io.BytesIO(zlib.decompress(good)), mode="r:") as src, \
            tarfile.open(fileobj=raw, mode="w", format=tarfile.USTAR_FORMAT) as dst:
        for mem in src.getmembers():
            if mem.name != "sprites/v4/dark.json":
                dst.addfile(mem, src.extractfile(mem))
    pack.write_bytes(zlib.compress(raw.getvalue()))
    cut = {**pk, "bytes": pack.stat().st_size, "sha256": hashlib.sha256(pack.read_bytes()).hexdigest()}
    assert pack_problems_of({"tiles": {"assets": {**m["tiles"]["assets"], "pack": cut}}}, rel) == [
        f"tiles.assets.pack: {pack} is not its folder's assets.json (extra [], missing ['sprites/v4/dark.json'])"]
    pack.unlink()
    assert pack_problems_of(m, rel) == [f"tiles.assets.pack: {pk['url']} is missing"]


def test_the_release_check_runs_the_pack_check(tmp_path, monkeypatch):
    """(T919 c) `manifest_problems` (the release's preflight) carries what `pack_problems_of` finds: a pack cut on disk is
    a stop there, not only in a helper."""
    from pipeline import release as R
    from pipeline.deliver.release import build as RB
    from pipeline.deliver.release import publish as P
    from pipeline.deliver.release.manifest import FORMAT
    rel, folder, m = _released_pack(tmp_path)
    m = {**m, "format": FORMAT, "data_release": "r-1", "files": {}, "bundle": {"reach_digest": "d"}}
    monkeypatch.setattr(R, "release_root", lambda: rel)
    monkeypatch.setattr(R, "manifest_of", lambda rid: m)
    monkeypatch.setattr(R, "reach_dir", lambda: tmp_path)
    monkeypatch.setattr(R, "reach_digest_of", lambda d: "d")
    monkeypatch.setattr(P, "plan", lambda *a, **k: None)
    monkeypatch.setattr(RB, "gate_problems", lambda d: [])
    assert R.manifest_problems("r-1") == []
    pk = m["tiles"]["assets"]["pack"]
    whole = (rel / pk["url"]).read_bytes()
    (rel / pk["url"]).write_bytes(whole[:20])                                   # cut part way
    assert R.manifest_problems("r-1") == [f"tiles.assets.pack: {pk['url']} is 20 B, the manifest says {pk['bytes']}",
                                          f"tiles.assets.pack: {rel / pk['url']} would not read as a zlib tar "
                                          "(Error -5 while decompressing data: incomplete or truncated stream)"]
