"""T1171 step 1: the holes where a stale stage result passed as fresh.

Every test is a mutation: change ONLY the input a key missed and the key moves (the stage runs); leave it and the key
stays (nothing reruns needlessly). The trees are the tests' own, inside the repo (`.test-scratch/`, git-ignored: the
recorder skips the temp dir by design); the outside tools are fakes on $PATH. No build runs.

  1. a stage's non-.py code (the bundle's schema.sql) was in no stage key;
  2. the tiles and clim keys left out the installed packages, SQLite and the tippecanoe / tile-join versions;
  3. the atlas recorded its curated inputs but no code.
(4, `changed_waters` against the release before, is a design question: the PR says why; no code here.)
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import sys
import textwrap
import uuid
from pathlib import Path

import pytest

from pipeline import build as B
from pipeline.atlas import inputs as I
from pipeline.common import stepcache as SC
from pipeline.common.curated import REPO_ROOT

PY = sys.executable
SCRATCH = REPO_ROOT / ".test-scratch"


@pytest.fixture
def scratch(monkeypatch):
    """A repo-local tree whose files count as CODE (`stepcache.CODE_READ_ROOTS` names it for the test)."""
    d = SCRATCH / f"keyholes-{uuid.uuid4().hex[:8]}"
    (d / "code").mkdir(parents=True)
    monkeypatch.setattr(SC, "CODE_READ_ROOTS", (SC._rel(str(d / "code")) + "/",))
    yield d
    shutil.rmtree(d, ignore_errors=True)


def rel(p: Path) -> str:
    return SC._rel(str(p))


#: imports a module of the tree, reads a code file of it (the schema) and a data file outside the code roots
RUN = textwrap.dedent("""
    import pathlib, sys
    import pipeline                                  # installs the recorder (PIPELINE_TRACE), as every stage does
    import t1171mod                                  # a repo module (M line)
    pathlib.Path(sys.argv[1]).read_text()            # code that is no module: the schema
    pathlib.Path(sys.argv[2]).read_text()            # data: keyed by the stage's own parts, never as code
""")


def _tree(scratch: Path) -> tuple[Path, Path, Path]:
    mod = scratch / "code" / "t1171mod.py"
    mod.write_text("X = 1\n")
    schema = scratch / "code" / "schema.sql"
    schema.write_text("CREATE TABLE t (a);\n")
    data = scratch / "data.json"
    data.write_text("{}")
    return mod, schema, data


def _traced_run(scratch: Path, schema: Path, data: Path, tag: str = "tr") -> Path:
    tr = scratch / tag
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPYCACHEPREFIX"}
    subprocess.run([PY, "-c", RUN, str(schema), str(data)], check=True, cwd=REPO_ROOT,
                   env={**env, "PIPELINE_TRACE": str(tr), "PYTHONPATH": f"{REPO_ROOT}{os.pathsep}{scratch / 'code'}"})
    return tr


# ------------------------------------------------------------------------------- hole 1: non-.py code in the stage key

def test_hole1_a_schema_only_edit_moves_the_traced_stage_key(scratch):
    """The run's key (`_traced_key`) and the next plan's key (`stage_code` on the manifest record) are the same while
    nothing moved; a schema.sql-only edit moves the next one, so deliver runs; a data file is not code."""
    mod, schema, data = _tree(scratch)
    keyd = {"corpus": "c"}
    key, _inputs, code_files, code_reads, note = B._traced_key("deliver", _traced_run(scratch, schema, data), keyd)
    assert key is not None, note
    assert rel(mod) in code_files and rel(schema) in code_reads and rel(data) not in code_reads
    m = {"stages": {"deliver": {"key": key, "code_files": code_files, "code_reads": code_reads}}, "runs": []}
    plan_key = lambda: B._sha(dict(keyd, code=B.stage_code("deliver", m)))      # noqa: E731
    assert plan_key() == key                       # unchanged: up to date
    data.write_text('{"moved": 1}')
    assert plan_key() == key                       # data is not code
    schema.write_text("CREATE TABLE t (a, b);\n")
    assert plan_key() != key                       # the schema alone: deliver runs
    schema.write_text("CREATE TABLE t (a);\n")
    assert plan_key() == key


def test_hole1_a_reused_step_still_names_its_code_files_to_the_stage(scratch):
    """A REUSED bundle step writes its code into the stage's trace: its modules and (T1171) its code reads, so a
    stage whose bundle was reused still keys on schema.sql."""
    mod, schema, data = _tree(scratch)
    SC.write_modules_trace(scratch / "stage", {rel(mod): "ab" * 32}, "bundle", reads=[rel(schema), rel(data)])
    t = SC.Trace(scratch / "stage")
    assert set(t.reads) == {rel(schema)}
    _modules, reads, moved = SC.traced_code(t)
    assert set(reads) == {rel(schema)} and not [x for x in moved if x == rel(schema)]


def test_hole1_a_code_file_edited_while_the_stage_ran_keys_nothing(scratch):
    mod, schema, data = _tree(scratch)
    tr = _traced_run(scratch, schema, data)
    schema.write_text("CREATE TABLE t (a, c);\n")             # after it was read: the run saw the older bytes
    key, *_rest, note = B._traced_key("deliver", tr, {"x": 1})
    assert key is None and rel(schema) in note


def test_hole1_the_untraced_fallback_holds_every_file_under_the_roots(scratch):
    _mod, schema, _data = _tree(scratch)
    roots = (rel(scratch / "code"),)
    before = B.code_digest(roots)
    assert B.code_digest(roots) == before
    schema.write_text("CREATE TABLE t (z);\n")
    assert B.code_digest(roots) != before


# ------------------------------------------------------------- hole 2: tiles + clim: packages, SQLite, outside tools

def test_hole2_every_stage_is_traced_and_carries_the_environment(scratch, monkeypatch):
    assert set(B.TRACED) == set(B.STAGES)
    mod, _schema, _data = _tree(scratch)
    m = {"stages": {"tiles": {"code_files": {rel(mod): "x"}, "code_reads": {}}}, "runs": []}
    for stage, mm in (("tiles", m), ("clim", {"stages": {}, "runs": []})):     # recorded, and before its first record
        k1 = B.stage_code(stage, mm)
        assert B.stage_code(stage, mm) == k1
        real = SC.env_fingerprint()
        monkeypatch.setattr(SC, "env_fingerprint", lambda real=real: dict(real, distributions="shapely bumped"))
        assert B.stage_code(stage, mm) != k1, stage
        monkeypatch.setattr(SC, "env_fingerprint", lambda real=real: real)


def test_hole2_the_environment_names_sqlite_and_no_install_path(monkeypatch):
    SC.env_fingerprint.cache_clear()
    try:
        env = SC.env_fingerprint()
        assert env["sqlite"] == sqlite3.sqlite_version
        assert not any(isinstance(v, str) and (sys.prefix in v or os.path.realpath(PY) in v) for v in env.values())
        assert SC.env_fingerprint() == env
        SC.env_fingerprint.cache_clear()
        monkeypatch.setattr(sqlite3, "sqlite_version", "0.0.1")
        assert SC.env_fingerprint() != env                     # a new SQLite under the same interpreter
    finally:
        monkeypatch.undo()
        SC.env_fingerprint.cache_clear()


def _fake_tools(d: Path, tippecanoe: str, tile_join: str = "tile-join v2.0.0") -> None:
    d.mkdir(exist_ok=True)
    for name, said in (("tippecanoe", tippecanoe), ("tile-join", tile_join)):
        p = d / name
        p.write_text(f"#!/bin/sh\necho '{said}' >&2\n")       # tippecanoe says its version on stderr
        p.chmod(0o755)


def _tiles_build(tmp: Path, monkeypatch) -> Path:
    """The tiles key's other inputs, small and fixed (as test_tiles.py builds them)."""
    build = tmp / "full"
    build.mkdir()
    (build / "section_handles.txt").write_text("blk:1:0\nblk:1:100\n")
    (build / "splits.resolved.json").write_text("[]")
    from pipeline.atlas.splits import coast as C
    (build / "shore_cuts.json").write_text('{"rule": "' + C.RULE + '", "runs": []}')
    C.write_record(build, {}, {})
    gpkg = tmp / "fwa.gpkg"
    gpkg.write_bytes(b"x")
    monkeypatch.setattr(B, "sources", lambda: {"gpkg": gpkg})
    monkeypatch.setattr(B, "obstacles_source", lambda: tmp / "fish_obstacles.gpkg")
    monkeypatch.setattr(B, "MANIFEST", tmp / "build-manifest.json")          # no record: the stage's roots
    return build


def test_hole2_a_tippecanoe_upgrade_moves_the_tiles_key(tmp_path, monkeypatch):
    build = _tiles_build(tmp_path, monkeypatch)
    bin_ = tmp_path / "bin"
    _fake_tools(bin_, "tippecanoe v2.53.0")
    monkeypatch.setenv("PATH", f"{bin_}{os.pathsep}/usr/bin{os.pathsep}/bin")
    before = B.tiles_key(build)
    assert before["tools"] == {"tippecanoe": "tippecanoe v2.53.0", "tile-join": "tile-join v2.0.0"}
    assert B.tiles_key(build) == before                        # the same tools: up to date
    _fake_tools(bin_, "tippecanoe v2.54.0")                    # brew upgrade tippecanoe
    assert B.tiles_key(build) != before
    _fake_tools(bin_, "tippecanoe v2.53.0", tile_join="tile-join v2.1.0")
    assert B.tiles_key(build) != before
    _fake_tools(bin_, "tippecanoe v2.53.0")
    assert B.tiles_key(build) == before


def test_hole2_a_missing_tool_is_said_and_a_broken_one_stops(tmp_path, monkeypatch):
    bin_ = tmp_path / "bin"
    _fake_tools(bin_, "tippecanoe v2.53.0")
    (bin_ / "tile-join").unlink()
    monkeypatch.setenv("PATH", f"{bin_}{os.pathsep}/usr/bin{os.pathsep}/bin")
    assert B.tool_versions()["tile-join"] == "missing"        # the stage runs; the preflight refuses it by name
    (bin_ / "tippecanoe").write_text("#!/bin/sh\nexit 3\n")
    with pytest.raises(SystemExit, match="tippecanoe --version` failed"):
        B.tool_versions()


# ------------------------------------------------------------------------------------- hole 3: the atlas's code stamp

#: the atlas build's first step (`start_code_record`), then the module it imports and the code file it reads, then
#: its meta record (`record_code`), in one process with no outer recorder
ATLAS = textwrap.dedent("""
    import json, pathlib, sys
    from pipeline.atlas import inputs as I
    from pipeline.common import stepcache as SC
    SC.CODE_READ_ROOTS = (sys.argv[3],)
    d, own = I.start_code_record()
    assert own
    import t1171mod
    pathlib.Path(sys.argv[1]).read_text()
    out = pathlib.Path(sys.argv[2])
    (out / I.META).write_text(json.dumps({"input_hashes": {"k": "v"}}))
    I.record_code(out, pathlib.Path(d))
""")


def _atlas(scratch: Path, schema: Path) -> Path:
    out = scratch / "atlas"
    out.mkdir()
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPYCACHEPREFIX", "PIPELINE_TRACE")}
    subprocess.run([PY, "-c", ATLAS, str(schema), str(out), SC.CODE_READ_ROOTS[0]], check=True, cwd=REPO_ROOT,
                   env={**env, "PYTHONPATH": f"{REPO_ROOT}{os.pathsep}{scratch / 'code'}"})
    return out


def test_hole3_an_atlas_code_edit_is_behind(scratch, monkeypatch):
    mod, schema, _data = _tree(scratch)
    atlas = _atlas(scratch, schema)
    code = I.read_meta(atlas)[I.CODE]
    assert rel(mod) in code["modules"] and rel(schema) in code["reads"] and code["env"] == SC.env_fingerprint()
    monkeypatch.setattr(I, "ATLAS_INPUTS", {"k": "a stand-in input"})
    assert I.behind(atlas, {"k": "v"}) == []                  # inputs and code unchanged: current
    mod.write_text("X = 2\n")
    got = I.behind(atlas, {"k": "v"})
    assert len(got) == 1 and got[0].startswith(f"{rel(mod)} (code): changed since the atlas was built")
    assert I.REBUILD in got[0]
    mod.write_text("X = 1\n")
    assert I.behind(atlas, {"k": "v"}) == []
    schema.write_text("-- a code file the atlas read\n")
    assert [g.split(" (")[0] for g in I.behind(atlas, {"k": "v"})] == [rel(schema)]
    schema.write_text("CREATE TABLE t (a);\n")
    real = SC.env_fingerprint()
    monkeypatch.setattr(SC, "env_fingerprint", lambda: dict(real, distributions="shapely bumped"))
    got = I.behind(atlas, {"k": "v"})
    assert len(got) == 1 and "(code: env) changed" in got[0]


def test_hole3_an_atlas_with_no_code_record_is_behind(scratch, monkeypatch):
    monkeypatch.setattr(I, "ATLAS_INPUTS", {"k": "a stand-in input"})
    atlas = scratch / "old"
    atlas.mkdir()
    I.write_meta(atlas, {"input_hashes": {"k": "v"}})          # built before T1171
    got = I.behind(atlas, {"k": "v"})
    assert len(got) == 1 and "records no code" in got[0] and I.REBUILD in got[0]
    I.write_meta(atlas, {"input_hashes": {"k": "v"}, I.CODE: {"unrecorded": "x.py changed while the atlas was built"}})
    got = I.behind(atlas, {"k": "v"})
    assert len(got) == 1 and "its code was not recorded (x.py changed" in got[0]
