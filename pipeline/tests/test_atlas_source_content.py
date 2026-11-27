"""(T1223) The atlas record holds its two big source files by CONTENT (`sha256:<64 hex>`), through the stage keys'
cache (`pipeline.common.source_hash`), which it only reads: the atlas check writes nothing. Each test builds its own
files and its own cache."""
from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from pipeline.atlas import inputs as I
from pipeline.common import source_hash as H

REPO = Path(__file__).resolve().parents[2]
SOURCES = I.SOURCE_KEYS


@pytest.fixture
def cache(tmp_path, monkeypatch) -> Path:
    """The live cache the atlas record reads, in tmp; the process memo emptied."""
    c = tmp_path / "generated" / H.NAME
    monkeypatch.setattr(H, "live_cache", lambda: c)
    monkeypatch.setattr(H, "_MEMO", {})
    return c


def _file(p: Path, body: bytes = b"gpkg bytes v1\n" * 100) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(body)
    return p


def test_a_source_input_is_its_bytes_sha256_wherever_it_is(tmp_path, cache):
    a = _file(tmp_path / "a" / "fwa.gpkg")
    b = tmp_path / "elsewhere" / "fwa.gpkg"
    b.parent.mkdir()
    shutil.copy(a, b)
    os.utime(b, ns=(1, 1))
    for key in SOURCES:
        got = I.digest(key, a)
        assert got == "sha256:" + hashlib.sha256(a.read_bytes()).hexdigest() and len(got) == 7 + 64
        assert I.digest(key, b) == got                              # a copy at another path, other times: the same
    a.write_bytes(b"gpkg bytes v2\n" * 100)
    assert I.digest(SOURCES[0], a) != I.digest(SOURCES[0], b)


def test_the_atlas_check_writes_no_cache_and_reads_a_warm_one(tmp_path, cache, monkeypatch):
    gpkg = _file(tmp_path / "src" / "fwa.gpkg")
    I.digest(SOURCES[0], gpkg)
    assert not cache.exists()                                       # read-only: a miss is hashed in memory
    H.sha256(gpkg, cache=cache, root=I.REPO_ROOT, persist=True)     # the build's stamp warms it (same root)
    monkeypatch.setattr(H, "_MEMO", {})                             # a new process
    hashed = []
    real = hashlib.file_digest
    monkeypatch.setattr(hashlib, "file_digest", lambda f, alg: hashed.append(f.name) or real(f, alg))
    before = cache.read_bytes()
    assert I.digest(SOURCES[0], gpkg).startswith("sha256:") and hashed == []      # a hit: nothing read
    assert cache.read_bytes() == before


def test_one_process_hashes_a_file_once(tmp_path, cache, monkeypatch):
    """The plan keys the gpkg (a dry run: nothing persisted) and then prints the atlas warning: 10 GB read once."""
    gpkg = _file(tmp_path / "src" / "fwa.gpkg")
    hashed = []
    real = hashlib.file_digest
    monkeypatch.setattr(hashlib, "file_digest", lambda f, alg: hashed.append(f.name) or real(f, alg))
    H.sha256(gpkg, cache=tmp_path / "side" / H.NAME, root=tmp_path, persist=False)
    I.digest(SOURCES[0], gpkg)
    assert len(hashed) == 1


def test_behind_names_a_record_made_by_size_and_mtime(tmp_path, cache, monkeypatch):
    gpkg = _file(tmp_path / "src" / "fwa.gpkg")
    monkeypatch.setattr(I, "ATLAS_INPUTS", {k: "a source" for k in SOURCES})
    monkeypatch.setattr(I, "default_path", lambda key: gpkg)
    monkeypatch.setattr(I, "code_behind", lambda _b: [])
    atlas = tmp_path / "atlas"
    atlas.mkdir()
    I.write_meta(atlas, {"input_hashes": {k: "stamp:1400:1795739535" for k in SOURCES}})
    got = I.behind(atlas)
    assert got == [f"fwa.gpkg ({k}): recorded by size and mtime (an atlas built before T1223), not by content — "
                   f"rebuild: {I.REBUILD}" for k in SOURCES]
    I.write_meta(atlas, {"input_hashes": I.digests()})
    assert I.behind(atlas) == []
    assert not cache.exists()


def test_backfill_still_proves_a_missing_source_input(tmp_path, cache, monkeypatch):
    gpkg = _file(tmp_path / "src" / "fwa.gpkg")
    os.utime(gpkg, (0, 0))                                          # before the build
    monkeypatch.setattr(I, "ATLAS_INPUTS", {k: "a source" for k in SOURCES})
    monkeypatch.setattr(I, "default_path", lambda key: gpkg)
    atlas = tmp_path / "atlas"
    atlas.mkdir()
    I.write_meta(atlas, {"input_hashes": {SOURCES[0]: I.digest(SOURCES[0], gpkg)}, "build_ts": 1000.0})
    got, unproven = I.backfill(atlas, log=lambda *_a: None)
    assert unproven == [] and got == {SOURCES[1]: "sha256:" + hashlib.sha256(gpkg.read_bytes()).hexdigest()}


def test_the_atlas_does_not_import_the_build():
    """`pipeline.atlas` reads the cache through `pipeline.common`, never by importing `pipeline.build`."""
    code = ("import sys, pathlib, tempfile; from pipeline.atlas import inputs as I; "
            "p = pathlib.Path(tempfile.mkdtemp()) / 'g'; p.write_bytes(b'x'); I.digest('source:fwa_gpkg', p); "
            "assert 'pipeline.build' not in sys.modules, 'pipeline.build imported'")
    subprocess.run([sys.executable, "-c", code], check=True, cwd=REPO, env={**os.environ, "PYTHONPATH": str(REPO)})
