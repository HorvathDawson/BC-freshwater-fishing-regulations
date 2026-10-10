"""The curation-review app's WRITES: validated, backed up, all or nothing, and never served stale.

Runs the backend (curation-review/backend) against TEMP COPIES of every curated file it writes —
the catalogue region files, the DFO salmon entry files, splits.json, the verification sidecar —
with backups going to a temp dir (`CURATION_BACKUP_DIR`). Never the real files.

  save_split    validated by the pipeline's own split loader; an invalid split is a 422 naming
                the loader's reason, and nothing is written
  rename_split  rewrites splits.json + every catalogue/DFO extent binding the id, or nothing:
                a failure injected into the SECOND file write leaves every file byte-identical
  backups       every write first copies its target, byte-identical, into a timestamped snapshot
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
_MODS = ("app", "reuse", "model_api", "rebuild", "verification", "answer", "synopsis_pages",
         "writes")


@pytest.fixture
def env(tmp_path):
    """A fresh temp copy of every curated file the app writes, and the app pointed at it."""
    cat, dfo, bak = tmp_path / "catalogue", tmp_path / "dfo_salmon", tmp_path / "backups"
    cat.mkdir(), dfo.mkdir()
    for p in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json")):
        shutil.copy2(p, cat / p.name)
    for p in sorted(CURATED.regulations.entries.dfo_salmon.glob("region-*.json")):
        shutil.copy2(p, dfo / p.name)
    splits = tmp_path / "waters" / "splits.json"
    splits.parent.mkdir()
    shutil.copy2(CURATED.waters.splits, splits)
    mp = pytest.MonkeyPatch()
    mp.setenv("CURATION_ENTRIES_DIR", str(cat))
    mp.setenv("CURATION_VERIFICATION", str(tmp_path / "verification.json"))
    mp.setenv("CURATION_SPLITS_JSON", str(splits))
    mp.setenv("CURATION_DFO_ENTRIES_DIR", str(dfo))
    mp.setenv("CURATION_BACKUP_DIR", str(bak))
    mp.syspath_prepend(str(BACKEND))
    for mod in _MODS:
        sys.modules.pop(mod, None)
    import app as app_mod                                     # noqa: E402
    import reuse                                              # noqa: E402
    import writes                                             # noqa: E402
    assert reuse.ENTRIES_DIR == cat and reuse.SPLITS_JSON_PATH == splits
    assert reuse.DFO_ENTRIES_DIR == dfo and writes.BACKUP_ROOT == bak
    from fastapi.testclient import TestClient
    yield {"client": TestClient(app_mod.app), "reuse": reuse, "writes": writes, "tmp": tmp_path,
           "cat": cat, "dfo": dfo, "splits": splits, "backups": bak, "mp": mp}
    mp.undo()
    for mod in _MODS:
        sys.modules.pop(mod, None)


def _files(env) -> dict[Path, bytes]:
    out = {env["splits"]: env["splits"].read_bytes()}
    for d in (env["cat"], env["dfo"]):
        out.update({p: p.read_bytes() for p in sorted(d.glob("region-*.json"))})
    return out


def _snapshots(env) -> list[Path]:
    root = env["backups"]
    return sorted(d for d in root.iterdir() if d.is_dir()) if root.exists() else []


def _split_ids(splits: Path) -> list[str]:
    d = json.loads(splits.read_text(encoding="utf-8"))
    return [s["id"] for w in d["waterbodies"] for s in w["splits"]]


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
# save_split — the pipeline's split model decides
# --------------------------------------------------------------------------- #

def _a_split(env, anchor_type: str) -> str:
    d = json.loads(env["splits"].read_text(encoding="utf-8"))
    return next(s["id"] for w in d["waterbodies"] for s in w["splits"]
                if s["anchor"]["type"] == anchor_type)


@pytest.mark.parametrize("anchor, why", [
    ({"type": "lake"}, "lake anchor needs wbk"),
    ({"type": "line", "coords": [[1, 2]]}, "line anchor needs >=2 coords"),
    ({"type": "no_such_anchor"}, "no_such_anchor"),
    ({"type": "point", "coord": [1, 2], "offset_m": 50}, "offset_m requires offset_dir"),
])
def test_save_split_refuses_what_the_split_model_refuses(env, anchor, why):
    sid = _a_split(env, "point")
    before = _files(env)
    r = env["client"].put(f"/api/splits/{sid}", json={"patch": {"anchor": anchor}})
    assert r.status_code == 422, r.text
    assert why in r.text, r.text
    assert _files(env) == before, "a refused split must write nothing"
    assert _snapshots(env) == [], "a refused split must not even back up"


def test_save_split_writes_one_split_and_backs_up_first(env):
    sid = _a_split(env, "point")
    before = env["splits"].read_bytes()
    r = env["client"].put(f"/api/splits/{sid}", json={"patch": {"label": "Test label"}})
    assert r.status_code == 200, r.text
    after = env["splits"].read_bytes()
    assert after.endswith(b"}\n"), "the file keeps its trailing newline"
    old = json.loads(before)
    for w in old["waterbodies"]:
        for s in w["splits"]:
            if s["id"] == sid:
                s["label"] = "Test label"
    assert json.loads(after) == old, "only that split's label changed"
    snap, = _snapshots(env)
    assert next(snap.rglob("splits.json")).read_bytes() == before, \
        "the backup is the file exactly as it was before the save"


# --------------------------------------------------------------------------- #
# rename_split — every reference, or nothing
# --------------------------------------------------------------------------- #

def _bound_in(d: Path, sid: str) -> list[Path]:
    return [p for p in sorted(d.glob("region-*.json"))
            if f'"{sid}"' in p.read_text(encoding="utf-8")]


def _renameable(env) -> str:
    """A split bound by BOTH a catalogue file and a DFO file, and named in no pipeline code."""
    reuse = env["reuse"]
    for sid in _split_ids(env["splits"]):
        if len(_bound_in(env["cat"], sid)) and _bound_in(env["dfo"], sid) \
                and not reuse._split_in_code(sid):
            return sid
    raise AssertionError("no split is bound by both the catalogue and the DFO files")


def test_rename_refuses_a_split_named_in_pipeline_code(env):
    before = _files(env)
    r = env["client"].post("/api/splits/skeena_river__cnr_railway_bridge_terrace/rename",
                           json={"new_id": "skeena_river__renamed"})
    assert r.status_code == 422 and "pipeline/regs/dfo_salmon/entries.py" in r.text, r.text
    assert _files(env) == before


@pytest.mark.needs_atlas
def test_rename_is_all_or_nothing_when_the_second_write_fails(env):
    sid = _renameable(env)
    before = _files(env)
    _fail_on_call(env, 2)
    r = env["client"].post(f"/api/splits/{sid}/rename", json={"new_id": "renamed_for_test"})
    assert r.status_code == 422 and "restored" in r.text, r.text
    assert _files(env) == before, \
        "splits.json was written first; a failed region file must put it back, byte for byte"


@pytest.mark.needs_atlas
def test_rename_rewrites_every_reference(env):
    sid, new = _renameable(env), "renamed_for_test"
    before = _files(env)
    touched = {env["splits"], *_bound_in(env["cat"], sid), *_bound_in(env["dfo"], sid)}
    n_refs = sum(p.read_text(encoding="utf-8").count(f'"{sid}"') for p in touched)
    r = env["client"].post(f"/api/splits/{sid}/rename", json={"new_id": new})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["updated_rules"] and body["updated_dfo_files"] and body["failed_rules"] == []
    now = _files(env)
    assert not any(f'"{sid}"'.encode() in b for b in now.values()), "the old id is gone everywhere"
    assert sum(b.count(f'"{new}"'.encode()) for b in now.values()) == n_refs
    assert {p for p in now if now[p] != before[p]} == touched, "no other file was rewritten"
    for p in touched:                       # only the id changed, nothing else in the file
        assert now[p].decode().replace(f'"{new}"', f'"{sid}"') == before[p].decode(), p.name
    snap, = _snapshots(env)
    for p in touched:
        assert (snap / p.parent.name / p.name).read_bytes() == before[p], p


# --------------------------------------------------------------------------- #
# Every other write: backed up first
# --------------------------------------------------------------------------- #

@pytest.mark.needs_atlas
def test_entry_save_backs_up_the_file_first(env):
    path = min(env["cat"].glob("region-*.json"), key=lambda p: p.stat().st_size)
    region = path.stem.split("region-", 1)[1]
    eid = json.loads(path.read_text(encoding="utf-8"))["entries"][0]["entry_id"]
    served = env["client"].get(f"/api/entries/{eid}").json()["entry"]
    before = path.read_bytes()
    r = env["client"].put(f"/api/entries/{eid}", json={"region": region, "entry": served})
    assert r.status_code == 200, r.text
    snap, = _snapshots(env)
    assert (snap / "catalogue" / path.name).read_bytes() == before


@pytest.mark.needs_atlas
def test_a_verification_mark_backs_up_the_sidecar(env):
    path = min(env["cat"].glob("region-*.json"), key=lambda p: p.stat().st_size)
    eid = json.loads(path.read_text(encoding="utf-8"))["entries"][0]["entry_id"]
    side = env["tmp"] / "verification.json"
    c = env["client"]
    assert c.put(f"/api/entries/{eid}/verify", json={"state": "verified"}).status_code == 200
    first, = _snapshots(env)
    assert (first / "ABSENT").read_text().split() == [f"{env['tmp'].name}/verification.json"]
    before = side.read_bytes()
    assert c.put(f"/api/entries/{eid}/verify",
                 json={"state": "flagged", "note": "x"}).status_code == 200
    second = _snapshots(env)[-1]
    assert next(second.rglob("verification.json")).read_bytes() == before


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
