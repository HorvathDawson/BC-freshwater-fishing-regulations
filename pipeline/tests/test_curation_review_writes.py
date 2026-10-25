"""THE ONE WRITE of the curation-review app — a mark — backed up, all or nothing, never served stale.

Runs the backend (curation-review/backend) against TEMP COPIES of the curated files it reads (the
catalogue region files, splits.json) and the marks store it appends to, with backups going to a
temp dir (`CURATION_BACKUP_DIR`). Never the real files. The editors' writes (entry save, split
save / delete / rename) are retired (T185): `test_curation_review_api.py` pins that no route
writes under data/curated.

  commit        `writes.commit` backs every target up, byte-identical, into a timestamped snapshot
                first, and a failure injected into the SECOND file write leaves every file as it was
  a mark        backs the store up and only appends
  caches        a build-derived cache follows the file on disk (`reuse.ensure_fresh`), and every
                cache in reuse.py is either dropped on a rebuild or declared static
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"
_MODS = ("app", "reuse", "model_api", "answer", "synopsis_pages", "writes",
                "review", "evidence")


@pytest.fixture
def env(tmp_path):
    """A fresh temp copy of the curated files the app reads, the marks store, and the app pointed
    at them."""
    cat, bak = tmp_path / "catalogue", tmp_path / "backups"
    cat.mkdir()
    for p in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json")):
        shutil.copy2(p, cat / p.name)
    splits = tmp_path / "waters" / "splits.json"
    splits.parent.mkdir()
    shutil.copy2(CURATED.waters.splits, splits)
    mp = pytest.MonkeyPatch()
    mp.setenv("CURATION_ENTRIES_DIR", str(cat))
    marks = tmp_path / "review" / "marks.jsonl"          # the store is checked in, empty
    marks.parent.mkdir()
    marks.write_text("")
    mp.setenv("CURATION_MARKS", str(marks))
    mp.setenv("CURATION_SPLITS_JSON", str(splits))
    mp.setenv("CURATION_BACKUP_DIR", str(bak))
    mp.syspath_prepend(str(BACKEND))
    for mod in _MODS:
        sys.modules.pop(mod, None)
    import app as app_mod                                     # noqa: E402
    import reuse                                              # noqa: E402
    import writes                                             # noqa: E402
    assert reuse.ENTRIES_DIR == cat and reuse.SPLITS_JSON_PATH == splits
    assert writes.BACKUP_ROOT == bak
    from fastapi.testclient import TestClient
    yield {"client": TestClient(app_mod.app), "reuse": reuse, "writes": writes, "tmp": tmp_path,
           "cat": cat, "splits": splits, "backups": bak, "mp": mp, "marks": marks}
    mp.undo()
    for mod in _MODS:
        sys.modules.pop(mod, None)


@pytest.fixture
def as_built(env):
    """Every target read as built. These tests are about the store and its backups; whether the
    reach run, bundle and release on disk are the temp catalogue's (T133's gate on a verified mark)
    is `test_curation_review_vintage.py`'s."""
    from pipeline.regs.review import vintage as V
    now = {"state": V.CURRENT, "reasons": [], "splits": [], "splits_checked": "anchors",
           "commands": []}
    env["mp"].setattr(env["reuse"], "entry_vintage", lambda e, on=None: dict(now))
    env["mp"].setattr(env["reuse"], "split_vintage", lambda sid, on=None: dict(now))


def _snapshots(env) -> list[Path]:
    root = env["backups"]
    return sorted(d for d in root.iterdir() if d.is_dir()) if root.exists() else []


def _fail_on_call(env, n: int):
    """Make the n-th per-file write of a commit raise (an injected disk failure)."""
    real, calls = env["writes"]._write, [0]

    def flaky(path, text):
        calls[0] += 1
        if calls[0] == n:
            raise OSError(f"injected failure on write {n} ({Path(path).name})")
        return real(path, text)
    env["mp"].setattr(env["writes"], "_write", flaky)
    return calls


# --------------------------------------------------------------------------- #
# writes.commit — backup first, all or nothing
# --------------------------------------------------------------------------- #

def test_commit_restores_every_file_when_the_second_write_fails(env):
    w, tmp = env["writes"], env["tmp"]
    paths = [tmp / f"f{i}.json" for i in range(3)]
    for i, p in enumerate(paths):
        p.write_text(f"before {i}\n", encoding="utf-8")
    calls = _fail_on_call(env, 2)
    with pytest.raises(OSError, match="injected"):
        w.commit([(p, f"after {i}\n") for i, p in enumerate(paths)], "test")
    assert calls[0] == 2, "the commit must stop at the failed write"
    assert [p.read_text() for p in paths] == [f"before {i}\n" for i in range(3)], \
        "the first file was written and must have been put back"
    snap, = _snapshots(env)
    assert [(snap / tmp.name / p.name).read_bytes() for p in paths] == \
        [f"before {i}\n".encode() for i in range(3)], "every target is backed up before any write"


def test_commit_removes_a_file_it_created_when_a_later_write_fails(env):
    w, tmp = env["writes"], env["tmp"]
    new, old = tmp / "new.json", tmp / "old.json"
    old.write_text("old\n")
    _fail_on_call(env, 2)
    with pytest.raises(OSError):
        w.commit([(new, "created\n"), (old, "changed\n")], "test")
    assert not new.exists() and old.read_text() == "old\n"
    snap, = _snapshots(env)
    assert (snap / "ABSENT").read_text().split() == [f"{tmp.name}/new.json"]


def test_backups_are_bounded(env):
    w, tmp = env["writes"], env["tmp"]
    env["mp"].setattr(w, "KEEP", 3)
    p = tmp / "x.json"
    for i in range(6):
        w.commit([(p, f"{i}\n")], "test")
    snaps = _snapshots(env)
    assert len(snaps) == 3
    assert [next(s.rglob("x.json")).read_text() for s in snaps] == ["2\n", "3\n", "4\n"], \
        "the newest snapshots are kept, each holding the file as it was before that write"


# --------------------------------------------------------------------------- #
# The one write: a mark, appended
# --------------------------------------------------------------------------- #

def _post_mark(c, eid: str, **over):
    body = {"scope": "entry", "target": eid, "verdict": "verified",
            "answers": {"T": "y", "R": "y", "P": "y", "A": "y"}, "reviewer": "tester", **over}
    return c.post("/api/marks", json=body)


@pytest.mark.needs_atlas
def test_a_mark_backs_up_the_store_and_only_appends(env, as_built):
    path = min(env["cat"].glob("region-*.json"), key=lambda p: p.stat().st_size)
    eid = json.loads(path.read_text(encoding="utf-8"))["entries"][0]["entry_id"]
    c, store = env["client"], env["marks"]
    r = _post_mark(c, eid)
    assert r.status_code == 200, r.text
    first, = _snapshots(env)
    assert (first / "review" / "marks.jsonl").read_bytes() == b""
    once = store.read_bytes()
    r = _post_mark(c, eid, verdict="deferred", answers={}, note="later")
    assert r.status_code == 200, r.text
    second = _snapshots(env)[-1]
    assert (second / "review" / "marks.jsonl").read_bytes() == once
    assert store.read_bytes().startswith(once) and len(store.read_text().splitlines()) == 2


# --------------------------------------------------------------------------- #
# Caches follow the files
# --------------------------------------------------------------------------- #

def test_a_build_cache_follows_the_file_on_disk(env):
    reuse, tmp = env["reuse"], env["tmp"]
    resolved = tmp / "splits.resolved.json"
    resolved.write_text(json.dumps([{"split_id": "a", "label": "first"}]))
    env["mp"].setattr(reuse, "SPLITS_RESOLVED_PATH", resolved)
    env["mp"].setattr(reuse, "_BUILD_FILES", (resolved,))
    reuse._splits_meta.cache_clear()
    reuse.ensure_fresh()
    assert reuse._splits_meta()["a"]["label"] == "first"
    resolved.write_text(json.dumps([{"split_id": "a", "label": "rebuilt, longer"}]))
    assert reuse._splits_meta()["a"]["label"] == "first", "cached until the build is seen to change"
    env["client"].get("/api/synopsis/pages")          # any API request checks the build first
    assert reuse._splits_meta()["a"]["label"] == "rebuilt, longer"


def test_every_cache_is_dropped_on_a_rebuild_or_declared_static(env):
    """A new `lru_cache` over a build artifact that is not in `_BUILD_CACHES` would be served
    stale after a promote — the old list missed `_geoms`, `_blk_index` and the place-namer."""
    reuse = env["reuse"]
    static = {"_row_image_index", "_to_lonlat", "_fwa_lake_poly"}  # source data, not the build
    cached = {n for n, f in vars(reuse).items()
              if callable(f) and hasattr(f, "cache_clear") and getattr(f, "__module__", "reuse")
              in ("reuse", None)}
    built = {f.__name__ for f in reuse._BUILD_CACHES}
    assert cached - built - static == set(), "a cache neither dropped on rebuild nor static"
    assert not (built & static)

