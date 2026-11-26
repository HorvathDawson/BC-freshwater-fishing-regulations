"""T1171 step 1: the holes where a stale stage result passed as fresh, and the owner's two picks (2026-11-26).

Every test is a mutation: change ONLY the input a key missed and the key moves (the stage runs); leave it — or change
something the stage never uses — and the key stays (nothing reruns needlessly). The trees are the tests' own, inside
the repo (`.test-scratch/`, git-ignored: the recorder skips the temp dir by design) or a temp git repo; the outside
tools are fakes on $PATH. No build runs.

  1. a stage's non-.py code (the bundle's schema.sql) was in no stage key;
  2. the tiles and clim keys left out the packages, SQLite and the tippecanoe / tile-join identities;
     owner pick A: a key holds only the packages a stage IMPORTS and the config values it READS — and a stage whose
     imports or config reads cannot be traced stops the build, naming it;
  3. the atlas recorded its curated inputs but no code (the recorder now starts before the atlas module's imports, and
     leaves the record machinery out); owner pick B: an atlas built before this is proven once by git (`--backfill-code`).
(4, `changed_waters` against the release before, is a design question: the PR says why; no code here.)
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import textwrap
import uuid
from importlib import metadata
from pathlib import Path

import pytest
import yaml

from pipeline import build as B
from pipeline.atlas import inputs as I
from pipeline.common import stepcache as SC
from pipeline.common.curated import REPO_ROOT

PY = sys.executable
SCRATCH = REPO_ROOT / ".test-scratch"
MACHINERY = ("pipeline/_trace.py", "pipeline/common/stepcache.py", "pipeline/atlas/inputs.py")


@pytest.fixture
def scratch(monkeypatch):
    """A repo-local tree whose files count as CODE (`stepcache.CODE_READ_ROOTS` names it for the test)."""
    d = SCRATCH / f"keyholes-{uuid.uuid4().hex[:8]}"
    (d / "code").mkdir(parents=True)
    monkeypatch.setattr(SC, "CODE_READ_ROOTS", (SC._rel(str(d / "code")) + "/",))
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def config_copy(tmp_path, monkeypatch):
    """config.yaml as the consumer reads it NOW: a copy the test may edit (the C lines were recorded from the real one,
    whose values the copy holds until edited)."""
    p = tmp_path / "config.yaml"
    p.write_bytes((REPO_ROOT / "config.yaml").read_bytes())
    monkeypatch.setattr(SC, "config_path", lambda: p)

    def edit(dotted: str, value) -> None:
        blob = yaml.safe_load(p.read_text(encoding="utf-8"))
        node = blob
        *head, last = dotted.split(".")
        for k in head:
            node = node[k]
        node[last] = value
        p.write_text(yaml.safe_dump(blob), encoding="utf-8")
    return edit


def bump(monkeypatch, dist: str) -> None:
    """`dist` is installed at another version from now on (every other distribution as it is)."""
    real = metadata.version
    monkeypatch.setattr(metadata, "version", lambda d: "999.0.0" if d == dist else real(d))


def rel(p: Path) -> str:
    return SC._rel(str(p))


#: a stage stand-in: imports a module of the tree and one package (PyYAML), reads a code file of it (the schema), a
#: data file outside the code roots and one config value
RUN = textwrap.dedent("""
    import pathlib, sys
    import pipeline                                  # installs the recorder (PIPELINE_TRACE), as every stage does
    import t1171mod                                  # a repo module (M line)
    import yaml                                      # a package: PyYAML (P line)
    pathlib.Path(sys.argv[1]).read_text()            # code that is no module: the schema
    pathlib.Path(sys.argv[2]).read_text()            # data: keyed by the stage's own parts, never as code
    from project_config import get_config
    get_config().get_path("data_accessor", "gpkg_path")      # one config value (C line)
""")


def _tree(scratch: Path) -> tuple[Path, Path, Path]:
    mod = scratch / "code" / "t1171mod.py"
    mod.write_text("X = 1\n")
    schema = scratch / "code" / "schema.sql"
    schema.write_text("CREATE TABLE t (a);\n")
    data = scratch / "data.json"
    data.write_text("{}")
    return mod, schema, data


def _env(scratch: Path, **extra) -> dict:
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPYCACHEPREFIX", "PIPELINE_TRACE")}
    return {**env, "PYTHONPATH": f"{REPO_ROOT}{os.pathsep}{scratch / 'code'}", **extra}


def _traced_run(scratch: Path, schema: Path, data: Path, tag: str = "tr") -> Path:
    tr = scratch / tag
    subprocess.run([PY, "-c", RUN, str(schema), str(data)], check=True, cwd=REPO_ROOT,
                   env=_env(scratch, PIPELINE_TRACE=str(tr)))
    return tr


def _keyed(scratch: Path, stage: str = "deliver"):
    """(the recorded key, the next plan's key function) for one traced run of `RUN`."""
    mod, schema, data = _tree(scratch)
    keyd = {"corpus": "c"}
    key, _inputs, code, note = B._traced_key(stage, _traced_run(scratch, schema, data), keyd)
    assert key is not None, note
    m = {"stages": {stage: {"key": key, "code_files": code["modules"], "code_reads": code["reads"],
                            "code_packages": code["packages"], "code_config": code["config"]}}, "runs": []}
    return key, code, (lambda: B._sha(dict(keyd, code=B.stage_code(stage, m)))), (mod, schema, data)


# ------------------------------------------------------------------------------- hole 1: non-.py code in the stage key

def test_hole1_a_schema_only_edit_moves_the_traced_stage_key(scratch, config_copy):
    """The run's key (`_traced_key`) and the next plan's key (`stage_code` on the manifest record) are the same while
    nothing moved; a schema.sql-only edit moves the next one, so deliver runs; a data file is not code."""
    key, code, plan_key, (mod, schema, data) = _keyed(scratch)
    assert rel(mod) in code["modules"] and rel(schema) in code["reads"] and rel(data) not in code["reads"]
    assert plan_key() == key                       # unchanged: up to date
    data.write_text('{"moved": 1}')
    assert plan_key() == key                       # data is not code
    schema.write_text("CREATE TABLE t (a, b);\n")
    assert plan_key() != key                       # the schema alone: deliver runs
    schema.write_text("CREATE TABLE t (a);\n")
    assert plan_key() == key


def test_hole1_a_reused_step_still_names_its_code_to_the_stage(scratch):
    """A REUSED bundle step writes its code into the stage's trace: its modules and (T1171) its code reads, packages and
    config values, so a stage whose bundle was reused still keys on schema.sql."""
    mod, schema, data = _tree(scratch)
    SC.write_modules_trace(scratch / "stage", {rel(mod): "ab" * 32}, "bundle", reads=[rel(schema), rel(data)],
                           packages=["yaml"], config={"curated": "c0ffee"})
    t = SC.Trace(scratch / "stage")
    assert set(t.reads) == {rel(schema)} and t.packages == {"yaml"} and t.config == {"curated": "c0ffee"}


def test_hole1_a_code_file_edited_while_the_stage_ran_keys_nothing(scratch, config_copy):
    mod, schema, data = _tree(scratch)
    tr = _traced_run(scratch, schema, data)
    schema.write_text("CREATE TABLE t (a, c);\n")             # after it was read: the run saw the older bytes
    key, _i, _c, note = B._traced_key("deliver", tr, {"x": 1})
    assert key is None and rel(schema) in note


def test_hole1_the_untraced_fallback_holds_every_file_under_the_roots(scratch):
    _mod, schema, _data = _tree(scratch)
    roots = (rel(scratch / "code"),)
    before = B.code_digest(roots)
    assert B.code_digest(roots) == before
    schema.write_text("CREATE TABLE t (z);\n")
    assert B.code_digest(roots) != before


# ------------------------------------------------- hole 2 + owner pick A: the packages it imports, the config it reads

def test_hole2_every_stage_is_traced():
    """Every stage a key decides; (T1120, merged from main) the in-season resolution is made only when it is missing or
    stale (`MADE_WHEN_MISSING`), never because its key moved, so no code key of it decides anything."""
    assert set(B.TRACED) == set(B.STAGES) - set(B.MADE_WHEN_MISSING) and B.MADE_WHEN_MISSING == ("inseason",)


def test_pick_a_only_a_package_the_stage_imports_moves_its_key(scratch, config_copy, monkeypatch):
    key, code, plan_key, _files = _keyed(scratch, "tiles")
    assert "pyyaml" in code["packages"] and "pytest" not in code["packages"]
    bump(monkeypatch, "pytest")                    # installed here, never imported by the stage
    assert plan_key() == key
    bump(monkeypatch, "pyyaml")                    # imported by it
    assert plan_key() != key


def test_pick_a_only_a_config_value_the_stage_reads_moves_its_key(scratch, config_copy):
    key, code, plan_key, _files = _keyed(scratch, "clim")
    assert "data_accessor.gpkg_path" in code["config"]
    config_copy("cache.dfo_salmon", "elsewhere/dfo")          # a path this stage never reads
    assert plan_key() == key
    config_copy("data_accessor.gpkg_path", "elsewhere/fwa.gpkg")
    assert plan_key() != key


def test_pick_a_an_import_no_distribution_owns_stops_the_build_naming_the_stage(scratch):
    tr = scratch / "tr"
    tr.mkdir()
    (tr / "1.trace").write_text(f"M\t{REPO_ROOT / 'pipeline' / '__init__.py'}\t"
                                f"{SC.code_now(['pipeline/__init__.py'])['pipeline/__init__.py']}\n"
                                "P\tno_such_package_t1171\t\n")
    with pytest.raises(SystemExit, match=r"build: tiles: tiles imported `no_such_package_t1171`, which no installed"):
        B._traced_key("tiles", tr, {"x": 1})


def test_pick_a_config_read_with_no_value_recorded_stops_the_build(scratch):
    tr = scratch / "tr"
    tr.mkdir()
    (tr / "1.trace").write_text(f"R\t{REPO_ROOT / 'config.yaml'}\t1:1\n")
    with pytest.raises(SystemExit, match=r"build: reach: reach read config.yaml but recorded no value"):
        B._traced_key("reach", tr, {"x": 1})


#: (second review of #286, BLOCKER) the REAL recorder over REAL Cython packages: pandas 3.0.3 registers
#: `pandas._libs._cyutility` under the bare `sys.modules` key `_cyutility`, scipy its `_csparsetools`, `_moduleTNC`,
#: `_ni_label` — no distribution owns those names, and keying the sys.modules KEY stopped every real build after the
#: stage had run. The child also says which bare keys it saw, so the test proves it still exercises the case.
REAL_CYTHON = textwrap.dedent("""
    import json, sys
    import pipeline                                  # installs the recorder (PIPELINE_TRACE), as every stage does
    from pipeline.common import curated              # what every stage imports first: PyYAML and its config values
    curated.GENERATED.bundle                         # (read on first use, as every stage takes its paths)
    import pandas, scipy.sparse, scipy.optimize, scipy.ndimage
    print(json.dumps(sorted(k for k, m in sys.modules.items()
                            if "." not in k and getattr(getattr(m, "__spec__", None), "name", k) != k)))
""")


def test_finding1_real_pandas_and_scipy_imports_key_under_the_real_recorder(scratch):
    tr = scratch / "tr"
    got = subprocess.run([PY, "-c", REAL_CYTHON], check=True, cwd=REPO_ROOT, capture_output=True, text=True,
                         env=_env(scratch, PIPELINE_TRACE=str(tr)))
    bare = set(json.loads(got.stdout.strip().splitlines()[-1]))
    assert bare & {"_cyutility", "_csparsetools", "_moduleTNC", "_ni_label"}, \
        f"pandas / scipy no longer register a module under a bare key ({sorted(bare)}): this test tests nothing"
    t = SC.Trace(tr)
    assert not t.packages & bare                     # the module's own name (its spec), never the bare key
    key, _inputs, code, note = B._traced_key("reach", tr, {"x": 1})       # raised SystemExit before the fix
    assert key is not None, note
    assert {"pandas", "scipy", "numpy", "pyyaml"} <= set(code["packages"]) and code["config"]


def test_finding2_an_empty_package_or_config_set_is_refused_never_none(scratch, monkeypatch):
    """A run of repo code that recorded no package or no config value is a recorder miss: the build stops, naming the
    stage; a manifest record or an atlas record holding the empty set is refused (the stage runs; the atlas is behind)
    — never read as "this stage imports nothing" and kept "up to date"."""
    init = REPO_ROOT / "pipeline" / "__init__.py"
    m_line = f"M\t{init}\t{SC.code_now(['pipeline/__init__.py'])['pipeline/__init__.py']}\n"
    for name, lines, says in (("nopkg", m_line + "C\tcurated\tc0ffee\n", "no package"),
                              ("nocfg", m_line + "P\tyaml\t\n", "no config value")):
        tr = scratch / name
        tr.mkdir()
        (tr / "1.trace").write_text(lines)
        with pytest.raises(SystemExit, match=rf"build: deliver: deliver recorded {says}: .*recorder miss"):
            B._traced_key("deliver", tr, {"x": 1})
    # the manifest record: refused, keyed as never recorded, and the plan says why
    rec = {"key": "k", "code_files": {"pipeline/__init__.py": "x"}, "code_reads": {}, "code_packages": [],
           "code_config": {"curated": "c0ffee"}}
    m = {"stages": {"reach": rec}, "runs": []}
    fallback = {"roots": B.code_digest(B.CODE["reach"]), "env": SC.env_fingerprint(), "config": SC.config_now(["*"])}
    assert B.stage_code("reach", m) == fallback
    assert "names no package (a recorder miss): refused" in B.refused_record("reach", rec)
    assert "no config value" in B.refused_record("reach", dict(rec, code_packages=["pyyaml"], code_config={}))
    assert B.refused_record("reach", dict(rec, code_packages=["pyyaml"])) is None
    # the atlas record: behind, so `pipeline release` refuses it
    monkeypatch.setattr(I, "ATLAS_INPUTS", {"k": "a stand-in input"})
    atlas = scratch / "atlas"
    atlas.mkdir()
    code = {"modules": {"pipeline/__init__.py": SC.code_now(["pipeline/__init__.py"])["pipeline/__init__.py"]},
            "reads": {}, "packages": {}, "config": SC.config_now(["curated"]), **SC.stage_env()}   # all else current
    I.write_meta(atlas, {"input_hashes": {"k": "v"}, I.CODE: code})
    got = I.behind(atlas, {"k": "v"})
    assert len(got) == 1 and "its code record names no package" in got[0] and I.REBUILD in got[0]


def test_pick_a_config_yaml_is_loaded_only_by_the_two_recording_loaders():
    """The C lines are written by the loaders; a module that parsed config.yaml itself would read values no key holds.
    (The run-time guard above catches one that opens it; this catches it before it runs.)"""
    allowed = {"pipeline/common/curated.py", "project_config.py"}
    offenders = []          # the keying side (the record machinery) reads it to compare values NOW: not a stage
    for p in sorted([*REPO_ROOT.joinpath("pipeline").rglob("*.py"), REPO_ROOT / "project_config.py"]):
        r = str(p.relative_to(REPO_ROOT))
        if "/tests/" in r or r in allowed or SC.is_machinery(r):
            continue
        text = p.read_text(encoding="utf-8")
        if "yaml.safe_load" in text and ("config.yaml" in text or "curated.CONFIG" in text or "import CONFIG" in text):
            offenders.append(r)
    assert not offenders, f"config.yaml parsed outside the recording loaders: {offenders}"
    for r in sorted(allowed):
        assert "config_read(" in (REPO_ROOT / r).read_text(encoding="utf-8")


def test_hole2_the_stage_env_names_sqlite_and_no_install_path(monkeypatch):
    env = SC.stage_env()
    assert env == {"python": sys.version, "sqlite": sqlite3.sqlite_version}
    assert not any(sys.prefix in v or os.path.realpath(PY) in v for v in env.values())
    monkeypatch.setattr(sqlite3, "sqlite_version", "0.0.1")
    assert SC.stage_env() != env                               # a new SQLite under the same interpreter


def test_hole2_the_step_cache_env_names_sqlite_and_no_install_path(monkeypatch):
    SC.env_fingerprint.cache_clear()
    try:
        env = SC.env_fingerprint()
        assert env["sqlite"] == sqlite3.sqlite_version and "executable" not in env
        SC.env_fingerprint.cache_clear()
        monkeypatch.setattr(sqlite3, "sqlite_version", "0.0.1")
        assert SC.env_fingerprint() != env
    finally:
        monkeypatch.undo()
        SC.env_fingerprint.cache_clear()


def _fake_tools(d: Path, tippecanoe: str, tile_join_build: str = "build 1") -> None:
    """tippecanoe says its version on stderr; tile-join, like 2.79.0's, REJECTS --version (rc 101) — its identity is
    its binary's bytes (`tile_join_build` stands for another build of it)."""
    d.mkdir(exist_ok=True)
    (d / "tippecanoe").write_text(f"#!/bin/sh\necho '{tippecanoe}' >&2\n")
    (d / "tile-join").write_text(f"#!/bin/sh\n# {tile_join_build}\n"
                                 "echo \"tile-join: unrecognized option '$1'\" >&2\nexit 101\n")
    for n in ("tippecanoe", "tile-join"):
        (d / n).chmod(0o755)


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


def test_hole2_a_tippecanoe_or_tile_join_change_moves_the_tiles_key(tmp_path, monkeypatch):
    build = _tiles_build(tmp_path, monkeypatch)
    bin_ = tmp_path / "bin"
    _fake_tools(bin_, "tippecanoe v2.79.0")
    monkeypatch.setenv("PATH", f"{bin_}{os.pathsep}/usr/bin{os.pathsep}/bin")
    before = B.tiles_key(build)
    assert before["tools"]["tippecanoe"] == "tippecanoe v2.79.0"
    assert before["tools"]["tile-join"].startswith("sha256:")            # no --version: its bytes
    assert B.tiles_key(build) == before                        # the same tools: up to date
    _fake_tools(bin_, "tippecanoe v2.80.0")                    # brew upgrade tippecanoe
    assert B.tiles_key(build) != before
    _fake_tools(bin_, "tippecanoe v2.79.0", tile_join_build="build 2")      # another tile-join binary
    assert B.tiles_key(build) != before
    _fake_tools(bin_, "tippecanoe v2.79.0")
    assert B.tiles_key(build) == before


def test_hole2_a_missing_tool_is_said_and_a_broken_one_stops(tmp_path, monkeypatch):
    bin_ = tmp_path / "bin"
    _fake_tools(bin_, "tippecanoe v2.79.0")
    (bin_ / "tile-join").unlink()
    monkeypatch.setenv("PATH", f"{bin_}{os.pathsep}/usr/bin{os.pathsep}/bin")
    assert B.tool_versions()["tile-join"] == "missing"        # the stage runs; the preflight refuses it by name
    (bin_ / "tippecanoe").write_text("#!/bin/sh\nexit 3\n")
    with pytest.raises(SystemExit, match="tippecanoe --version` failed"):
        B.tool_versions()


# ------------------------------------------------------------------------------------- hole 3: the atlas's code stamp

#: the atlas build's shape: the recorder first (`_trace.start_own`), then its imports (a repo module, a package, the
#: machinery), a code file, a config value, then its meta record (`record_code`); one process, no outer recorder
ATLAS = textwrap.dedent("""
    import json, pathlib, sys
    from pipeline import _trace
    d, own = _trace.start_own("t1171-atlas-", __file__ if "__file__" in dir() else "-")
    assert own
    import t1171mod
    import yaml
    from project_config import get_config
    get_config().get_path("data_accessor", "gpkg_path")
    from pipeline.atlas import inputs as I
    from pipeline.common import stepcache as SC
    SC.CODE_READ_ROOTS = (sys.argv[3],)
    pathlib.Path(sys.argv[1]).read_text()
    out = pathlib.Path(sys.argv[2])
    (out / I.META).write_text(json.dumps({"input_hashes": {"k": "v"}}))
    I.record_code(out, pathlib.Path(d))
""")


def _atlas(scratch: Path, schema: Path) -> Path:
    out = scratch / "atlas"
    out.mkdir()
    subprocess.run([PY, "-c", ATLAS, str(schema), str(out), SC.CODE_READ_ROOTS[0]], check=True, cwd=REPO_ROOT,
                   env=_env(scratch))
    return out


def test_hole3_an_atlas_code_package_or_config_change_is_behind(scratch, config_copy, monkeypatch):
    mod, schema, _data = _tree(scratch)
    atlas = _atlas(scratch, schema)
    code = I.read_meta(atlas)[I.CODE]
    assert rel(mod) in code["modules"] and rel(schema) in code["reads"]
    assert "pyyaml" in code["packages"] and "data_accessor.gpkg_path" in code["config"]
    assert {code["python"], code["sqlite"]} == set(SC.stage_env().values())
    monkeypatch.setattr(I, "ATLAS_INPUTS", {"k": "a stand-in input"})
    current = lambda: I.behind(atlas, {"k": "v"})              # noqa: E731
    assert current() == []                                     # inputs and code unchanged: current
    mod.write_text("X = 2\n")
    got = current()
    assert len(got) == 1 and got[0].startswith(f"{rel(mod)} (code): changed since the atlas was built")
    assert I.REBUILD in got[0]
    mod.write_text("X = 1\n")
    schema.write_text("-- a code file the atlas read\n")
    assert [g.split(" (")[0] for g in current()] == [rel(schema)]
    schema.write_text("CREATE TABLE t (a);\n")
    config_copy("cache.dfo_salmon", "elsewhere/dfo")          # a value it never read
    bump(monkeypatch, "pytest")                                # a package it never imported
    assert current() == []
    config_copy("data_accessor.gpkg_path", "elsewhere/fwa.gpkg")
    assert [g.split(" (")[1].split(")")[0] for g in current()] == ["code: config"]
    config_copy("data_accessor.gpkg_path", yaml.safe_load((REPO_ROOT / "config.yaml").read_text())
                ["data_accessor"]["gpkg_path"])
    bump(monkeypatch, "pyyaml")
    got = current()
    assert len(got) == 1 and got[0].startswith("pyyaml ") and "(code: packages)" in got[0]


def test_finding2_the_record_machinery_is_not_the_atlas_s_code(scratch):
    """The recorder, the step cache and the atlas-record module are imported by every atlas build (and by the record
    itself), but decide what is recorded, not what the atlas holds: none is in the record, so a cache change never makes
    the atlas behind. The rule is the module's own `RECORD_MACHINERY = True`."""
    _mod, schema, _data = _tree(scratch)
    code = I.read_meta(_atlas(scratch, schema))[I.CODE]
    assert not set(MACHINERY) & set(code["modules"]) and code["modules"]
    assert all(SC.is_machinery(m) for m in MACHINERY)
    declared = sorted(str(p.relative_to(REPO_ROOT)) for p in REPO_ROOT.joinpath("pipeline").rglob("*.py")
                      if "/tests/" not in str(p) and SC.is_machinery(str(p.relative_to(REPO_ROOT))))
    assert declared == sorted(MACHINERY)


def test_finding3_the_atlas_module_records_what_its_own_imports_read(scratch):
    """`python -m pipeline.atlas.build` starts the recorder before the module's imports: the config values they read
    (curated paths, the generated tree) and `project_config` are in its trace."""
    probe = textwrap.dedent("""
        import json, runpy, sys
        sys.argv = ["pipeline.atlas.build", "--help"]
        try:
            runpy.run_module("pipeline.atlas.build", run_name="__main__")
        except SystemExit:
            pass
        from pipeline import _trace
        from pipeline.common import stepcache as SC
        t = SC.Trace(_trace.directory())
        print(json.dumps({"config": sorted(t.config), "modules": sorted(t.modules)}))
    """)
    got = subprocess.run([PY, "-c", probe], check=True, cwd=REPO_ROOT, env=_env(scratch), capture_output=True,
                         text=True)
    seen = json.loads(got.stdout.strip().splitlines()[-1])
    assert "project_config.py" in seen["modules"] and "pipeline/atlas/build.py" in seen["modules"]
    assert seen["config"], "the atlas module's imports read config.yaml before any value was recorded"


def test_hole3_an_atlas_with_no_code_record_names_the_backfill(scratch, monkeypatch):
    monkeypatch.setattr(I, "ATLAS_INPUTS", {"k": "a stand-in input"})
    atlas = scratch / "old"
    atlas.mkdir()
    I.write_meta(atlas, {"input_hashes": {"k": "v"}})          # built before T1171
    got = I.behind(atlas, {"k": "v"})
    assert len(got) == 1 and "records no code" in got[0] and I.BACKFILL_CODE in got[0] and I.REBUILD in got[0]
    I.write_meta(atlas, {"input_hashes": {"k": "v"}, I.CODE: {"unrecorded": "x.py changed while the atlas was built"}})
    got = I.behind(atlas, {"k": "v"})
    assert len(got) == 1 and "its code was not recorded (x.py changed" in got[0]


# ----------------------------------------------------------------- owner pick B: the one-time backfill, proven by git

def _git(root: Path, *args: str, when: str | None = None) -> str:
    env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t",
           "GIT_COMMITTER_EMAIL": "t@t", **({"GIT_AUTHOR_DATE": when, "GIT_COMMITTER_DATE": when} if when else {})}
    return subprocess.run(["git", *args], cwd=root, env=env, check=True, capture_output=True, text=True).stdout


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """A temp git repo shaped like this one: an atlas entry importing a helper, a machinery module and PyYAML; a code
    file beside them; config.yaml. Committed 2026-01-01; the atlas below was built 2026-01-02."""
    root = tmp_path.resolve() / "repo"
    (root / "pipeline" / "atlas").mkdir(parents=True)
    (root / "pipeline" / "__init__.py").write_text("")
    (root / "pipeline" / "atlas" / "__init__.py").write_text("")
    (root / "pipeline" / "atlas" / "build.py").write_text(
        "import yaml\nfrom pipeline.atlas import helper\nfrom . import record\n\n\ndef main():\n"
        "    import optional_missing_t1171  # noqa: F401 (an optional import nobody installed)\n")
    (root / "pipeline" / "atlas" / "helper.py").write_text("X = 1\n")
    (root / "pipeline" / "atlas" / "record.py").write_text("RECORD_MACHINERY = True\n")
    (root / "pipeline" / "atlas" / "contract.json").write_text("{}\n")
    (root / "pipeline" / "atlas" / "README.md").write_text("words\n")
    (root / "config.yaml").write_text("a: 1\n")
    _git(root, "init", "-q")
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "base", when="2026-01-01T00:00:00Z")
    for mod in (I, SC):
        monkeypatch.setattr(mod, "REPO_ROOT", root)
    monkeypatch.delenv("CURATED_ROOT", raising=False)          # git proves the live tree only (`git_proof`)
    atlas = tmp_path / "atlas"
    atlas.mkdir()
    import datetime as dt
    built = dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc).timestamp()
    I.write_meta(atlas, {"input_hashes": {"k": "v"}, "build_ts": built})
    monkeypatch.setattr(I, "ATLAS_INPUTS", {"k": "a stand-in input"})
    return root, atlas


def test_pick_b_backfill_records_the_proven_code_and_adopts_the_packages(repo):
    root, atlas = repo
    said: list[str] = []
    code = I.backfill_code(atlas, log=said.append)
    assert sorted(code["modules"]) == ["pipeline/__init__.py", "pipeline/atlas/__init__.py",
                                       "pipeline/atlas/build.py", "pipeline/atlas/helper.py"]   # machinery out
    assert list(code["reads"]) == ["pipeline/atlas/contract.json"]                              # no README
    assert list(code["packages"]) == ["pyyaml"] and list(code["config"]) == ["*"]
    head = _git(root, "rev-parse", "HEAD").strip()
    assert code["backfilled"]["commit"] == head and code["backfilled"]["adopted"] == ["packages", "python", "sqlite"]
    assert any(s.startswith("inputs: ADOPTED, NOT PROVEN") and "pyyaml==" in s for s in said)
    assert I.behind(atlas, {"k": "v"}) == []                  # no refusal for the backfilled atlas
    (root / "pipeline" / "atlas" / "helper.py").write_text("X = 2\n")          # a later real edit still trips it
    got = I.behind(atlas, {"k": "v"})
    assert len(got) == 1 and got[0].startswith("pipeline/atlas/helper.py (code): changed since the atlas was built")
    with pytest.raises(SystemExit, match="already records its code"):
        I.backfill_code(atlas, log=lambda *_: None)


def test_pick_b_backfill_refuses_a_file_changed_since_the_build(repo):
    root, atlas = repo
    (root / "pipeline" / "atlas" / "helper.py").write_text("X = 2\n")
    _git(root, "commit", "-qam", "after the atlas", when="2026-01-03T00:00:00Z")
    with pytest.raises(SystemExit, match=r"refused: git cannot prove 1 file\(s\)[\s\S]*pipeline/atlas/helper.py"):
        I.backfill_code(atlas, log=lambda *_: None)
    assert I.CODE not in I.read_meta(atlas)                   # nothing written
    assert "records no code" in I.behind(atlas, {"k": "v"})[0]


def test_pick_b_backfill_refuses_an_uncommitted_edit(repo):
    root, atlas = repo
    (root / "pipeline" / "atlas" / "contract.json").write_text('{"x": 1}\n')
    with pytest.raises(SystemExit, match=r"pipeline/atlas/contract.json"):
        I.backfill_code(atlas, log=lambda *_: None)
