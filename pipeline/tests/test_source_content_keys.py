"""(T1220) The big source files enter the clim, tiles and deliver keys by their BYTES, never by their path or time.

Before T1220 `build.stamp` was [absolute path, size, mtime]: a moved checkout, a fresh clone, a copy or a re-download of
identical bytes reran clim, tiles and deliver. Every test here builds its own two "checkouts" in tmp, holding the same
files, and asks the three keys of each."""
from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import pytest

from pipeline import build as B

#: the files the three keys stamp, by the name `sources()` / `obstacles_source()` give them
FILES = {"gpkg": "fwa.gpkg", "stations": "bc_hydrometric_stations.json", "places": "bc_places.json",
         "hydat": "hydat.sqlite3", "obstacles": "fish_obstacles.gpkg"}
#: a fixed mtime (ns) for every file, so "a touch" and "the same second" are exact
T0 = 1_700_000_000_000_000_000


def _checkout(root: Path) -> Path:
    """One checkout: its source files (same bytes and mtime in every checkout) and a build dir for the tiles key."""
    src = root / "source"
    src.mkdir(parents=True)
    for k, name in FILES.items():
        (src / name).write_bytes(f"{k} bytes v1\n".encode() * 64)
        os.utime(src / name, ns=(T0, T0))
    build = root / "atlas"
    build.mkdir()
    (build / "section_handles.txt").write_text("blk:1:0\nblk:1:100\n")
    (build / "splits.resolved.json").write_text("[]")
    from pipeline.atlas.splits import coast as C
    (build / "shore_cuts.json").write_text('{"rule": "' + C.RULE + '", "runs": []}')
    C.write_record(build, {}, {})
    (root / "generated").mkdir()
    (root / "generated" / "clim.json").write_text('{"release": "2026-01"}')
    return build


@pytest.fixture
def keys_in(tmp_path: Path, monkeypatch):
    """`keys_in(root)`: the clim, tiles and deliver source keys as a checkout at `root` computes them (its own repo
    root, source tree and generated tree, so its own hash cache). Everything not under test is pinned."""
    monkeypatch.setattr(B, "stage_code", lambda *_a, **_k: "code")
    monkeypatch.setattr(B, "tool_versions", lambda: {"tippecanoe": "v"})
    monkeypatch.setattr(B, "dfo_feed_digest", lambda: "dfo")
    monkeypatch.setattr(B, "inseason_ends_digest", lambda: "ends")

    def at(root: Path) -> dict:
        src = root / "source"
        monkeypatch.setattr(B, "REPO_ROOT", root)
        monkeypatch.setattr(B, "SOURCE", src)
        monkeypatch.setattr(B, "MANIFEST", root / "generated" / "build-manifest.json")
        monkeypatch.setattr(B, "CLIM_PATH", root / "generated" / "clim.json")
        monkeypatch.setattr(B, "sources", lambda: {k: src / FILES[k] for k in ("gpkg", "stations", "places", "hydat")})
        monkeypatch.setattr(B, "obstacles_source", lambda: src / FILES["obstacles"])
        build = root / "atlas"
        return {"clim": B.clim_key(), "tiles": B.tiles_key(build), "deliver": B.deliver_sources()}
    return at


def _two(tmp_path: Path) -> tuple[Path, Path]:
    a, b = tmp_path / "checkout-a", tmp_path / "elsewhere" / "checkout-b"
    _checkout(a)
    _checkout(b)
    return a, b


def test_two_checkouts_of_the_same_files_key_alike(tmp_path, keys_in):
    a, b = _two(tmp_path)
    assert keys_in(a) == keys_in(b)


def test_no_key_input_names_the_repo_root(tmp_path, keys_in):
    a, b = _two(tmp_path)
    for root in (a, b):
        text = json.dumps(keys_in(root), sort_keys=True)
        assert str(root) not in text and str(tmp_path) not in text and str(B.SOURCE) not in text


def test_a_copy_with_new_times_keys_alike(tmp_path, keys_in):
    """A copy (or a re-download) of identical bytes: other mtimes, same keys."""
    a, b = _two(tmp_path)
    for name in FILES.values():
        os.utime(b / "source" / name, ns=(T0 + 86_400 * 10**9, T0 + 86_400 * 10**9))
    assert keys_in(a) == keys_in(b)


def test_a_touch_leaves_the_keys_alike_and_costs_one_rehash(tmp_path, keys_in, monkeypatch):
    a, _b = _two(tmp_path)
    before = keys_in(a)
    hashed = []
    real = B.hashlib.file_digest
    monkeypatch.setattr(B.hashlib, "file_digest", lambda f, alg: hashed.append(Path(f.name).name) or real(f, alg))
    assert keys_in(a) == before and hashed == []                    # unchanged: read from the cache, nothing hashed
    gpkg = a / "source" / FILES["gpkg"]
    os.utime(gpkg, ns=(T0 + 10**9, T0 + 10**9))                     # touch
    assert keys_in(a) == before
    assert hashed == [FILES["gpkg"]]                                # one rehash, once (three keys read it)...
    assert keys_in(a) == before and hashed == [FILES["gpkg"]]       # ...and then it matches


@pytest.mark.parametrize("which", ["gpkg", "hydat", "stations", "places", "obstacles"])
def test_a_one_byte_change_moves_the_keys(tmp_path, keys_in, which):
    """Same size, and the same whole second (an mtime-in-seconds stamp cannot see it): the bytes move the key."""
    a, _b = _two(tmp_path)
    before = keys_in(a)
    p = a / "source" / FILES[which]
    data = bytearray(p.read_bytes())
    data[0] ^= 1
    p.write_bytes(bytes(data))
    os.utime(p, ns=(T0 + 1, T0 + 1))
    after = keys_in(a)
    moved = {k for k in after if after[k] != before[k]}
    want = {"gpkg": {"tiles", "deliver"}, "hydat": {"clim", "deliver"}, "stations": {"clim", "deliver"},
            "places": {"deliver"}, "obstacles": {"tiles"}}[which]
    assert moved == want


def test_a_missing_source_file_raises_naming_it(tmp_path, keys_in):
    a, _b = _two(tmp_path)
    (a / "source" / FILES["hydat"]).unlink()
    with pytest.raises(FileNotFoundError, match="hydat.sqlite3"):
        keys_in(a)


def test_a_broken_hash_cache_is_refused_by_name(tmp_path, keys_in):
    a, _b = _two(tmp_path)
    keys_in(a)
    cache = a / "generated" / B.HASHES_NAME
    assert json.loads(cache.read_text())["format"] == B.HASHES_FORMAT
    cache.write_text('{"files": []}')
    with pytest.raises(SystemExit, match=B.HASHES_NAME):
        keys_in(a)


def test_a_moved_checkout_keeps_its_keys_and_its_cache(tmp_path, keys_in, monkeypatch):
    """The cache is the only place a file is named, and it names it by its place in the checkout: a checkout moved with
    its generated tree keys alike and hashes nothing again."""
    a, _b = _two(tmp_path)
    before = keys_in(a)
    moved = tmp_path / "moved"
    shutil.move(str(a), str(moved))
    names = set(json.loads((moved / "generated" / B.HASHES_NAME).read_text())["files"])
    assert names == {f"source/{n}" for n in FILES.values()}
    hashed = []
    real = B.hashlib.file_digest
    monkeypatch.setattr(B.hashlib, "file_digest", lambda f, alg: hashed.append(f.name) or real(f, alg))
    assert keys_in(moved) == before and hashed == []
