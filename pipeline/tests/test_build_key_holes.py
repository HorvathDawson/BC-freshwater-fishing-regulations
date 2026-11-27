"""T1171 step 1: the holes where a stale stage result passed as fresh, and the owner's two picks (2026-11-26).

Every test is a mutation: change ONLY the input a key missed and the key moves (the stage runs); leave it — or change
something the stage never uses — and the key stays (nothing reruns needlessly). The trees are the tests' own, inside
the repo (`.test-scratch/`, git-ignored: the recorder skips the temp dir by design) or a temp git repo; the outside
tools are fakes on $PATH. No build runs.

  1. a stage's non-.py code (the bundle's schema.sql) was in no stage key;
  2. the tiles and clim keys left out the packages, SQLite and the tippecanoe / tile-join / pmtiles identities and the
     map style; owner pick A: a key holds only the packages a stage IMPORTS and the config values it READS — a module
     no distribution owns is keyed by its file (third review), and config reads that cannot be traced stop the build;
  3. the atlas recorded its curated inputs but no code (the recorder now starts before the atlas module's imports, and
     leaves the record machinery out); owner 2026-11-26: an atlas built before this is REBUILT (no backfill), and one
     command says whether the atlas needs rebuilding and why (`python -m pipeline.atlas.inputs`, rc 1).
(4, `changed_waters` against the release before, is a design question: the PR says why; no code here.)
"""
from __future__ import annotations

import json
import os
import re
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
    from pipeline.common.curated import SOURCE
    SOURCE.fwa_gpkg                                  # one named source file: two config values (C lines)
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
    config values, so a stage whose bundle was reused still keys on schema.sql; and (M1) where a package no
    distribution owns was loaded from."""
    mod, schema, data = _tree(scratch)
    SC.write_modules_trace(scratch / "stage", {rel(mod): "ab" * 32}, "bundle", reads=[rel(schema), rel(data)],
                           packages=["yaml", "_virtualenv"], config={"curated": "c0ffee"},
                           package_files={"_virtualenv": "/venv/site/_virtualenv.py"})
    t = SC.Trace(scratch / "stage")
    assert set(t.reads) == {rel(schema)} and t.packages == {"yaml", "_virtualenv"} and t.config == {"curated": "c0ffee"}
    assert t.package_files == {"_virtualenv": "/venv/site/_virtualenv.py"}


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
    assert {"source.fwa_gpkg", "roots.source"} <= set(code["config"])
    config_copy("cache.dfo_salmon", "elsewhere/dfo")          # a path this stage never reads
    assert plan_key() == key
    config_copy("source.fwa_gpkg", "elsewhere.gpkg")
    assert plan_key() != key


def test_a_named_source_file_keys_its_own_name_and_the_source_root_only(scratch, config_copy):
    """(T1192) `SOURCE.fwa_gpkg` is the source root joined with the gpkg's name, and records exactly those two values:
    a stage that reads only the gpkg is not keyed on the coast file, the other names or the other roots, and is keyed
    on the root its gpkg sits under."""
    key, code, plan_key, _files = _keyed(scratch, "tiles")
    assert {k for k in code["config"] if k.startswith(("source", "roots"))} == {"source.fwa_gpkg", "roots.source"}
    for other in ("source.coast_gpkg", "source.synopsis_pdf", "source.fetch_temp", "roots.generated", "roots.release",
                  "roots.curated", "roots.cache"):
        config_copy(other, "elsewhere")
        assert plan_key() == key, other
    config_copy("roots.source", "elsewhere")
    assert plan_key() != key


def test_m1_an_import_no_distribution_owns_is_keyed_by_its_file_never_a_stop(scratch, tmp_path):
    """(Third review of #286, M1) a top-level name no distribution owns STOPPED the build after the stage ran, before
    the manifest was saved — so every build repeated it. It is keyed by its file's bytes now, and the run says so."""
    lone = tmp_path / "no_such_package_t1171.py"
    lone.write_text("X = 1\n")
    tr = scratch / "tr"
    tr.mkdir()
    (tr / "1.trace").write_text(f"M\t{REPO_ROOT / 'pipeline' / '__init__.py'}\t"
                                f"{SC.code_now(['pipeline/__init__.py'])['pipeline/__init__.py']}\n"
                                f"P\tno_such_package_t1171\t{lone}\nP\tyaml\t\n"
                                f"C\tcurated\t{SC.config_now(['curated'])['curated']}\n")
    key, _i, code, note = B._traced_key("tiles", tr, {"x": 1})          # raised SystemExit before the fix
    assert key is not None and SC.unowned_entry("no_such_package_t1171", str(lone)) in code["packages"]
    assert "keyed by their file's bytes instead of a version: no_such_package_t1171" in note
    k = SC.code_key({}, {}, code["packages"], {})
    assert str(lone) not in json.dumps(k)                       # the key holds the bytes, never this machine's path
    lone.write_text("X = 2\n")
    assert SC.code_key({}, {}, code["packages"], {}) != k       # its bytes moved: the stage runs
    m = {"stages": {"tiles": {"code_packages": code["packages"]}}}
    assert "no_such_package_t1171" in B._unowned_of(m, "tiles")      # the plan names it too


#: (M1) a stage stand-in run the way a uv / virtualenv venv runs everything: its `_virtualenv.py` (installed by the venv
#: tool into site-packages, owned by no distribution) is imported before any stage code
VENV_RUN = textwrap.dedent("""
    import _virtualenv                               # what a virtualenv's _virtualenv.pth imports at startup
    import pipeline
    from pipeline.common import curated
    curated.GENERATED.bundle
    import yaml
""")


def test_m1_a_virtualenv_s_own_module_keys_the_stage_under_the_real_recorder(scratch, tmp_path):
    site = tmp_path / "site"
    site.mkdir()
    (site / "_virtualenv.py").write_text("PATCHED = True\n")
    tr = scratch / "tr"
    subprocess.run([PY, "-c", VENV_RUN], check=True, cwd=REPO_ROOT,
                   env={**_env(scratch, PIPELINE_TRACE=str(tr)),
                        "PYTHONPATH": f"{site}{os.pathsep}{REPO_ROOT}"})
    assert "_virtualenv" not in metadata.packages_distributions()     # owned by nothing, as in a real venv
    key, _i, code, note = B._traced_key("reach", tr, {"x": 1})        # raised SystemExit before the fix
    assert key is not None, note
    assert SC.unowned_entry("_virtualenv", str((site / "_virtualenv.py").resolve())) in code["packages"] \
        or SC.unowned_entry("_virtualenv", str(site / "_virtualenv.py")) in code["packages"]
    assert "_virtualenv" in note and "pyyaml" in code["packages"]


#: (M1) an entry script OUTSIDE the repo with a spawn pool: the parent's `__main__` and every worker's `__mp_main__`
#: are that script, owned by no distribution
SPAWN_SCRIPT = textwrap.dedent("""
    import multiprocessing as mp
    import pipeline
    from pipeline.common import curated
    import yaml


    def work(x):
        curated.GENERATED.bundle
        return x


    if __name__ == "__main__":
        curated.GENERATED.bundle
        with mp.get_context("spawn").Pool(1) as pool:
            assert pool.map(work, [1]) == [1]
""")


def test_m1_an_entry_script_outside_the_repo_and_its_spawn_workers_key_by_its_bytes(scratch, tmp_path):
    script = tmp_path / "entry_t1171.py"
    script.write_text(SPAWN_SCRIPT)
    tr = scratch / "tr"
    subprocess.run([PY, str(script)], check=True, cwd=REPO_ROOT, env=_env(scratch, PIPELINE_TRACE=str(tr)))
    t = SC.Trace(tr)
    assert {"__main__", "__mp_main__"} <= t.packages, sorted(t.packages)       # the case is really exercised
    key, _i, code, note = B._traced_key("deliver", tr, {"x": 1})      # raised SystemExit before the fix
    assert key is not None, note
    tops = {SC.unowned_parts(d)[0] for d in code["packages"] if d.startswith(SC.UNOWNED)}
    assert {"__main__", "__mp_main__"} <= tops
    k = SC.code_key({}, {}, code["packages"], {})
    script.write_text(SPAWN_SCRIPT + "\n# edited\n")
    assert SC.code_key({}, {}, code["packages"], {}) != k


def test_m1_an_install_with_no_ownership_record_keys_its_package_directory(tmp_path, monkeypatch):
    """A PEP 660 editable install with no `top_level.txt`, or a conda package with no RECORD: the package is found but
    no distribution claims it. Its directory is keyed, every file in it."""
    pkg = tmp_path / "lonepkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    (pkg / "core.py").write_text("A = 1\n")
    monkeypatch.setattr(metadata, "packages_distributions", lambda: {})
    got = SC.distributions_of({"lonepkg"}, "tiles", {"lonepkg": str(pkg)})
    assert got == [SC.unowned_entry("lonepkg", str(pkg))]
    before = SC.package_versions(got)
    (pkg / "core.py").write_text("A = 2\n")                    # a submodule, not the __init__
    assert SC.package_versions(got) != before
    assert SC.package_versions([SC.unowned_entry("gone", "")]) == {SC.unowned_entry("gone", ""): "no file"}


def test_l1_a_config_value_edited_while_the_stage_ran_keys_nothing(scratch, config_copy):
    """(Third review of #286, L1) the config-moved guard (`traced_code`: a value read differs NOW): the run was made from
    the value read, so it cannot be keyed on the value there now."""
    _mod, schema, data = _tree(scratch)
    tr = _traced_run(scratch, schema, data)
    config_copy("source.fwa_gpkg", "elsewhere.gpkg")           # after the stage read it
    key, _i, _c, note = B._traced_key("deliver", tr, {"x": 1})
    assert key is None and "config source.fwa_gpkg" in note


def test_l1_a_config_value_read_before_the_recorder_is_still_written(tmp_path):
    """(L1) a config loader that ran at import time, before `install`, is written out by it (`_config_seen` replayed)."""
    probe = textwrap.dedent("""
        import sys
        from pipeline import _trace
        _trace.config_read("early.key", {"a": 1})        # before any recorder
        _trace.install(sys.argv[1])
    """)
    d = tmp_path / "tr"
    subprocess.run([PY, "-c", probe, str(d)], check=True, cwd=REPO_ROOT,
                   env={k: v for k, v in os.environ.items() if k != "PIPELINE_TRACE"})
    lines = "".join(f.read_text() for f in d.glob("*.trace")).splitlines()
    from pipeline import _trace
    assert f"C\tearly.key\t{_trace.config_digest({'a': 1})}" in lines


def test_l1_the_exit_sweep_names_a_package_loaded_past_the_finders(scratch, tmp_path):
    """(L1) a module loaded straight from its file (`spec_from_file_location`: no finder on `sys.meta_path` is asked, as
    a plugin loader does) is named only by the sweep at exit."""
    plug = tmp_path / "plugin_t1171.py"
    plug.write_text("X = 1\n")
    probe = textwrap.dedent("""
        import importlib.util, sys
        import pipeline
        spec = importlib.util.spec_from_file_location("plugin_t1171", sys.argv[1])
        mod = importlib.util.module_from_spec(spec)
        sys.modules["plugin_t1171"] = mod
        spec.loader.exec_module(mod)
    """)
    tr = scratch / "tr"
    subprocess.run([PY, "-c", probe, str(plug)], check=True, cwd=REPO_ROOT, env=_env(scratch, PIPELINE_TRACE=str(tr)))
    t = SC.Trace(tr)
    assert "plugin_t1171" in t.packages and t.package_files["plugin_t1171"].endswith("plugin_t1171.py")


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
    assert len(got) == 1 and "its code record names no package" in got[0]


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
        # (third review of #286, L4) any YAML loader, not only `yaml.safe_load`, in a file that names the config file
        loads = re.search(r"\byaml\.(safe_|full_|unsafe_)?load(_all)?\b|\bYAML\(", text)
        names = re.search(r"config\.yaml|\bCONFIG\b|config_path\(", text)
        if loads and names:
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


def _fake_tools(d: Path, tippecanoe: str, tile_join_build: str = "build 1", pmtiles_build: str = "build 1") -> None:
    """tippecanoe says its version on stderr; tile-join, like 2.79.0's, REJECTS --version (rc 101) — its identity is
    its binary's bytes (`tile_join_build` stands for another build of it); pmtiles, like Homebrew's 1.31.2, has no
    `--version` ("unknown flag", rc 1) and a `version` that says "dev": its bytes too."""
    d.mkdir(exist_ok=True)
    (d / "tippecanoe").write_text(f"#!/bin/sh\necho '{tippecanoe}' >&2\n")
    (d / "tile-join").write_text(f"#!/bin/sh\n# {tile_join_build}\n"
                                 "echo \"tile-join: unrecognized option '$1'\" >&2\nexit 101\n")
    (d / "pmtiles").write_text(f"#!/bin/sh\n# {pmtiles_build}\n"
                               "echo \"pmtiles: error: unknown flag $1\" >&2\nexit 1\n")
    for n in ("tippecanoe", "tile-join", "pmtiles"):
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
    # (third review of #286, M2b) pmtiles stamps the archive's metadata (`tiles.version.stamp`): its binary is keyed
    assert before["tools"]["pmtiles"].startswith("sha256:")
    _fake_tools(bin_, "tippecanoe v2.79.0", pmtiles_build="build 2")
    assert B.tiles_key(build) != before


def test_hole2_a_missing_tool_is_said_and_a_broken_one_stops(tmp_path, monkeypatch):
    bin_ = tmp_path / "bin"
    _fake_tools(bin_, "tippecanoe v2.79.0")
    (bin_ / "tile-join").unlink()
    (bin_ / "pmtiles").unlink()
    # ONLY the fakes on $PATH: a runner with tippecanoe installed has a real tile-join in /usr/bin (CI on #286); the
    # fakes are `#!/bin/sh` scripts using builtins only, so they need nothing else on it
    monkeypatch.setenv("PATH", str(bin_))
    got = B.tool_versions()
    assert got["tile-join"] == "missing" and got["pmtiles"] == "missing"     # the stage runs; the preflight says
    (bin_ / "tippecanoe").write_text("#!/bin/sh\nexit 3\n")
    with pytest.raises(SystemExit, match="tippecanoe --version` failed"):
        B.tool_versions()


STYLE = "app/packages/map/style/style.json"
#: (M2a) the tiles stage's read of the map style, as `tiles.assets.wanted` makes it, under the real recorder
STYLE_RUN = textwrap.dedent("""
    import pipeline
    from pipeline.common import curated
    curated.GENERATED.bundle
    from pipeline.deliver.tiles import assets
    assets.wanted()
""")


def test_m2a_a_font_added_to_the_map_style_moves_the_tiles_key(tmp_path, monkeypatch):
    """(Third review of #286, M2a) the tiles stage reads the map style (`assets.wanted` -> `style_fonts`): it was left
    out of the code key (`CODE_READ_ROOTS` held only `pipeline/`), so a font added to the style kept the tiles
    "up to date" and the shipped assets lacked its glyphs."""
    assert SC.is_code_read(STYLE)
    tr = tmp_path / "tr"
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPYCACHEPREFIX", "PIPELINE_TRACE")}
    subprocess.run([PY, "-c", STYLE_RUN], check=True, cwd=REPO_ROOT,
                   env={**env, "PYTHONPATH": str(REPO_ROOT), "PIPELINE_TRACE": str(tr)})
    key, _i, code, note = B._traced_key("tiles", tr, {"x": 1})
    assert key is not None and STYLE in code["reads"], note
    copy = tmp_path / "style.json"
    copy.write_bytes((REPO_ROOT / STYLE).read_bytes())
    real_abs = SC._abs
    monkeypatch.setattr(SC, "_abs", lambda r: copy if r == STYLE else real_abs(r))   # the style as the test edits it
    m = {"stages": {"tiles": {"key": key, "code_files": code["modules"], "code_reads": code["reads"],
                              "code_packages": code["packages"], "code_config": code["config"]}}, "runs": []}
    plan_key = lambda: B._sha(dict({"x": 1}, code=B.stage_code("tiles", m)))       # noqa: E731
    assert plan_key() == key
    style = json.loads(copy.read_text())
    style["layers"].append({"id": "t1171", "type": "symbol", "source": "x",
                            "layout": {"text-font": ["Noto Sans Bold T1171"]}})
    copy.write_text(json.dumps(style))
    assert plan_key() != key


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
    from pipeline.common.curated import SOURCE
    SOURCE.fwa_gpkg
    from pipeline.atlas import inputs as I
    from pipeline.common import stepcache as SC
    SC.CODE_READ_ROOTS = (sys.argv[3],)
    pathlib.Path(sys.argv[1]).read_text()
    if sys.argv[4] == "edit":                        # (L2) a code file and a module edited WHILE the atlas is built
        pathlib.Path(sys.argv[1]).write_text("-- edited during the build")
        pathlib.Path(t1171mod.__file__).write_text("X = 3")
    out = pathlib.Path(sys.argv[2])
    (out / I.META).write_text(json.dumps({"input_hashes": {"k": "v"}}))
    I.record_code(out, pathlib.Path(d))
""")


def _atlas(scratch: Path, schema: Path, edit: bool = False) -> Path:
    out = scratch / "atlas"
    out.mkdir()
    subprocess.run([PY, "-c", ATLAS, str(schema), str(out), SC.CODE_READ_ROOTS[0], "edit" if edit else "-"],
                   check=True, cwd=REPO_ROOT, env=_env(scratch))
    return out


def test_hole3_an_atlas_code_package_or_config_change_is_behind(scratch, config_copy, monkeypatch):
    mod, schema, _data = _tree(scratch)
    atlas = _atlas(scratch, schema)
    code = I.read_meta(atlas)[I.CODE]
    assert rel(mod) in code["modules"] and rel(schema) in code["reads"]
    assert "pyyaml" in code["packages"] and "source.fwa_gpkg" in code["config"]
    assert {code["python"], code["sqlite"]} == set(SC.stage_env().values())
    monkeypatch.setattr(I, "ATLAS_INPUTS", {"k": "a stand-in input"})
    current = lambda: I.behind(atlas, {"k": "v"})              # noqa: E731
    assert current() == []                                     # inputs and code unchanged: current
    mod.write_text("X = 2\n")
    got = current()
    assert len(got) == 1 and got[0].startswith(f"{rel(mod)} (code): changed since the atlas was built")
    schema.write_text("-- both moved\n")
    assert [g.split(" (")[0] for g in current()] == sorted([rel(mod), rel(schema)])     # every file named, no count
    schema.write_text("CREATE TABLE t (a);\n")
    mod.write_text("X = 1\n")
    schema.write_text("-- a code file the atlas read\n")
    assert [g.split(" (")[0] for g in current()] == [rel(schema)]
    schema.write_text("CREATE TABLE t (a);\n")
    config_copy("cache.dfo_salmon", "elsewhere/dfo")          # a value it never read
    bump(monkeypatch, "pytest")                                # a package it never imported
    assert current() == []
    config_copy("source.fwa_gpkg", "elsewhere.gpkg")
    assert [g.split(" (")[1].split(")")[0] for g in current()] == ["code: config"]
    config_copy("source.fwa_gpkg", yaml.safe_load((REPO_ROOT / "config.yaml").read_text())["source"]["fwa_gpkg"])
    bump(monkeypatch, "pyyaml")
    got = current()
    # (L5, owner pick A) a routine bump of a package it imports is a reason, naming the package and both versions
    assert len(got) == 1 and got[0].startswith(f"package pyyaml {code['packages']['pyyaml']} -> 999.0.0 (code: packages)")
    monkeypatch.undo()
    monkeypatch.setattr(I, "ATLAS_INPUTS", {"k": "a stand-in input"})
    monkeypatch.setattr(sqlite3, "sqlite_version", "0.0.1")   # (L1) a new SQLite under the same interpreter
    assert current() == [f"sqlite {code['sqlite']} -> 0.0.1 (code: env): the atlas was built under another sqlite"]


def test_l2_an_edit_made_while_the_atlas_was_built_keeps_the_record_and_is_behind(scratch, monkeypatch):
    """(Third review of #286, L2) an edit to a module or code file DURING the 32-minute build marked the whole atlas
    `unrecorded`: the record is kept — the module as imported, the moved code file as `MOVED` — and each is a reason."""
    mod, schema, _data = _tree(scratch)
    atlas = _atlas(scratch, schema, edit=True)
    code = I.read_meta(atlas)[I.CODE]
    assert "unrecorded" not in code, code
    import hashlib
    assert code["reads"][rel(schema)] == I.MOVED                               # hashed at the end: not what was read
    assert code["modules"][rel(mod)] == hashlib.sha256(b"X = 1\n").hexdigest()  # as imported, not "X = 3"
    monkeypatch.setattr(I, "ATLAS_INPUTS", {"k": "a stand-in input"})
    got = I.behind(atlas, {"k": "v"})
    assert f"{rel(schema)} (code): {I.MOVED}" in got
    assert f"{rel(mod)} (code): changed since the atlas was built" in got        # imported as X = 1, X = 3 now


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
    (curated paths, the generated tree, the gpkg's `source.fwa_gpkg`) and the config loader module are in its trace."""
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
    assert "pipeline/common/curated.py" in seen["modules"] and "pipeline/atlas/build.py" in seen["modules"]
    assert "source.fwa_gpkg" in seen["config"]
    assert seen["config"], "the atlas module's imports read config.yaml before any value was recorded"


def test_hole3_an_atlas_with_no_code_record_is_rebuilt(scratch, monkeypatch):
    """Owner 2026-11-26 ("yes to rebuild"): an atlas built before T1171 is rebuilt at the next cut; no backfill."""
    monkeypatch.setattr(I, "ATLAS_INPUTS", {"k": "a stand-in input"})
    atlas = scratch / "old"
    atlas.mkdir()
    I.write_meta(atlas, {"input_hashes": {"k": "v"}})          # built before T1171
    got = I.behind(atlas, {"k": "v"})
    assert got == ["old/graph.pkl.meta.json (code): records no code (an atlas built before T1171): rebuild it"]
    assert not hasattr(I, "backfill_code") and not hasattr(I, "BACKFILL_CODE")
    I.write_meta(atlas, {"input_hashes": {"k": "v"}, I.CODE: {"unrecorded": "x.py changed while the atlas was built"}})
    got = I.behind(atlas, {"k": "v"})
    assert len(got) == 1 and "its code was not recorded (x.py changed" in got[0]


# ------------------------------- owner 2026-11-26: the atlas's commit, and "does the atlas need rebuilding" as a command

def _git(root: Path, *args: str, when: str | None = None) -> str:
    env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t",
           "GIT_COMMITTER_EMAIL": "t@t", **({"GIT_AUTHOR_DATE": when, "GIT_COMMITTER_DATE": when} if when else {})}
    return subprocess.run(["git", *args], cwd=root, env=env, check=True, capture_output=True, text=True).stdout


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """A temp git repo shaped like this one (a module, a code file, config.yaml), committed; a trace of an atlas build
    that imported the module and PyYAML, read the code file and one config value; an atlas meta beside it."""
    root = tmp_path.resolve() / "repo"
    (root / "pipeline" / "atlas").mkdir(parents=True)
    (root / "pipeline" / "atlas" / "helper.py").write_text("X = 1\n")
    (root / "pipeline" / "atlas" / "contract.json").write_text("{}\n")
    (root / "config.yaml").write_text("a: 1\n")
    _git(root, "init", "-q")
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "base")
    for mod in (I, SC):
        monkeypatch.setattr(mod, "REPO_ROOT", root)
    monkeypatch.setattr(SC, "config_path", lambda: root / "config.yaml")
    monkeypatch.setattr(I, "ATLAS_INPUTS", {"k": "a stand-in input"})
    from pipeline import _trace
    tr = tmp_path / "trace"
    tr.mkdir()
    helper, contract = root / "pipeline" / "atlas" / "helper.py", root / "pipeline" / "atlas" / "contract.json"
    (tr / "1.trace").write_text(f"M\t{helper}\t{SC.sha_file(helper)}\nR\t{contract}\t{_trace.stamp(str(contract))}\n"
                                f"P\tyaml\t\nC\ta\t{_trace.config_digest(1)}\n")
    atlas = tmp_path / "atlas"
    atlas.mkdir()
    I.write_meta(atlas, {"input_hashes": {"k": "v"}})
    return root, tr, atlas


def test_m4_the_atlas_records_its_commit_and_the_check_says_committed_or_uncommitted(repo):
    """(Owner 2026-11-26, M4) `record_code` stores the commit the build started from and whether a recorded file
    differed from it; the check then says, per moved file, whether it was committed since or is an uncommitted edit
    (`git diff <commit> HEAD -- <file>`)."""
    root, tr, atlas = repo
    head = _git(root, "rev-parse", "HEAD").strip()
    code = I.record_code(atlas, tr, I.git_state())
    assert code["git"] == {"commit": head, "dirty": False}
    assert I.behind(atlas, {"k": "v"}) == []
    (root / "pipeline" / "atlas" / "helper.py").write_text("X = 2\n")
    assert I.behind(atlas, {"k": "v"}) == [
        f"pipeline/atlas/helper.py (code): changed since the atlas was built; git: an uncommitted edit since {head[:12]}"]
    _git(root, "commit", "-qam", "later")
    assert I.behind(atlas, {"k": "v"}) == [
        f"pipeline/atlas/helper.py (code): changed since the atlas was built; git: committed since {head[:12]}"]


def test_m4_an_atlas_built_on_uncommitted_code_is_dirty_and_names_no_git_reason(repo):
    root, tr, atlas = repo
    (root / "pipeline" / "atlas" / "contract.json").write_text('{"x": 1}\n')     # before the record, after the read
    code = I.record_code(atlas, tr, I.git_state())
    assert code["git"]["dirty"] is True
    assert code["reads"]["pipeline/atlas/contract.json"] == I.MOVED              # (L2) kept, not `unrecorded`
    assert I.behind(atlas, {"k": "v"}) == [f"pipeline/atlas/contract.json (code): {I.MOVED}"]


def test_owner_the_check_command_prints_every_reason_and_exits_by_the_answer(repo, capsys, monkeypatch):
    """(Owner 2026-11-26: "a way to know if the atlas needs rebuilding programatically") `python -m
    pipeline.atlas.inputs`: rc 0 current, 1 needs rebuilding with every reason on its own line, 2 cannot tell; and
    `pipeline build --status` / `--dry-run` say the same."""
    root, tr, atlas = repo
    monkeypatch.setattr(I, "digests", lambda paths=None: {"k": "v"})
    I.record_code(atlas, tr, I.git_state())
    assert I.main(["--build", str(atlas)]) == 0
    assert "is current: its 1 inputs, 1 modules, 1 code files, 1 packages, 1 config values" in capsys.readouterr().out
    (root / "pipeline" / "atlas" / "helper.py").write_text("X = 2\n")
    (root / "pipeline" / "atlas" / "contract.json").write_text('{"x": 1}\n')
    bump(monkeypatch, "pyyaml")
    assert I.main(["--build", str(atlas)]) == 1
    out = capsys.readouterr().out.splitlines()
    assert out[0] == f"inputs: the atlas {atlas} NEEDS REBUILDING — 3 reason(s):"
    assert [o.split(" (")[0].strip() for o in out[1:3]] == ["pipeline/atlas/contract.json", "pipeline/atlas/helper.py"]
    assert out[3].strip().startswith("package pyyaml ") and "-> 999.0.0" in out[3]
    assert out[-1] == f"inputs: rebuild: {I.REBUILD}"
    assert I.main(["--build", str(atlas.parent / "nowhere")]) == 2
    status = B.atlas_status_line(atlas)
    assert "NEEDS REBUILDING — 3 reason(s)" in status and I.CHECK in status
    warn = B.atlas_inputs_warning(atlas)
    assert "pipeline/atlas/contract.json" in warn and "pipeline/atlas/helper.py" in warn and "package pyyaml" in warn


def test_owner_the_check_writes_nothing(repo, monkeypatch):
    root, tr, atlas = repo
    monkeypatch.setattr(I, "digests", lambda paths=None: {"k": "v"})
    I.record_code(atlas, tr, I.git_state())
    before = {p: p.read_bytes() for p in atlas.iterdir()}
    status = _git(root, "status", "--porcelain")
    I.check(atlas, log=lambda *_: None)
    assert {p: p.read_bytes() for p in atlas.iterdir()} == before and _git(root, "status", "--porcelain") == status


# ------------------------------------------------------------- re-check of #286: "cannot tell" is rc 2, never rc 1

def test_recheck_a_check_that_cannot_answer_is_rc_2_never_needs_rebuilding(repo, capsys, monkeypatch):
    """A broken config.yaml (`yaml.YAMLError`) or a malformed meta (KeyError) escaped as a traceback, whose exit 1 read
    as "needs rebuilding": each is rc 2 with its reason; the build's warning and `--status` line and the release say
    "cannot tell" too (the release still refuses)."""
    root, tr, atlas = repo
    monkeypatch.setattr(I, "digests", lambda paths=None: {"k": "v"})
    I.record_code(atlas, tr, I.git_state())
    (root / "config.yaml").write_text("a: [1\n")                       # an unclosed `[`
    assert I.main(["--build", str(atlas)]) == 2
    out = capsys.readouterr().out
    assert "cannot tell whether the atlas" in out and ("ScannerError" in out or "ParserError" in out), out
    assert "cannot tell" in B.atlas_inputs_warning(atlas) and "cannot tell" in B.atlas_status_line(atlas)
    (root / "config.yaml").write_text("a: 1\n")
    meta = I.read_meta(atlas)
    del meta[I.CODE]["reads"]                                          # a malformed record
    I.write_meta(atlas, meta)
    assert I.main(["--build", str(atlas)]) == 2
    assert "KeyError" in capsys.readouterr().out
    assert I.main(["--build", str(atlas.parent / "nowhere")]) == 2   # no atlas: still rc 2
    from types import SimpleNamespace
    from pipeline import release as R
    monkeypatch.setattr(R, "atlas_behind", lambda: I.behind(atlas))
    got = R.check_atlas(SimpleNamespace(stale_atlas=None))
    assert got.ok is False and "cannot tell whether the atlas needs rebuilding: KeyError" in got.said


def test_recheck_a_module_imported_with_two_contents_is_moved_in_the_atlas_record(repo, monkeypatch):
    """(Re-check of #286) two processes of the atlas build imported one module with two contents: the record cannot say
    which it was built from, so it is `MOVED` and a reason."""
    root, tr, atlas = repo
    helper = root / "pipeline" / "atlas" / "helper.py"
    (tr / "2.trace").write_text(f"M\t{helper}\t{'0' * 64}\n")
    code = I.record_code(atlas, tr, I.git_state())
    assert code["modules"]["pipeline/atlas/helper.py"] == I.MOVED
    assert I.behind(atlas, {"k": "v"}) == [f"pipeline/atlas/helper.py (code): {I.MOVED}"]


def test_recheck_an_unowned_package_directory_keys_by_content_wherever_it_is(tmp_path):
    """(Re-check of #286) the key holds an unowned package's bytes, not its path: the same package at two paths keys
    the same; an edit to a submodule (not its `__init__`) moves it."""
    a, b = tmp_path / "one" / "lonepkg", tmp_path / "two" / "lonepkg"
    for d in (a, b):
        (d / "sub").mkdir(parents=True)
        (d / "__init__.py").write_text("")
        (d / "sub" / "core.py").write_text("A = 1\n")
    ka = SC.code_key({}, {}, [SC.unowned_entry("lonepkg", str(a))], {})
    kb = SC.code_key({}, {}, [SC.unowned_entry("lonepkg", str(b))], {})
    assert ka == kb and str(a) not in json.dumps(ka)
    (b / "sub" / "core.py").write_text("A = 2\n")
    assert SC.code_key({}, {}, [SC.unowned_entry("lonepkg", str(b))], {}) != ka


def test_recheck_the_tiles_preflight_refuses_a_missing_pmtiles(tmp_path, monkeypatch):
    """(Re-check of #286) the tiles stage runs pmtiles (`tiles.version.stamp`): the preflight refuses it missing before
    the 15-minute stage, not when the stamp step fails."""
    from pipeline.deliver.tiles import tippe
    bin_ = tmp_path / "bin"
    _fake_tools(bin_, "tippecanoe v2.79.0")
    monkeypatch.setenv("PATH", str(bin_))
    tippe.check()                                                       # all three there
    (bin_ / "pmtiles").unlink()
    with pytest.raises(FileNotFoundError, match="pmtiles is not on"):
        tippe.check()
