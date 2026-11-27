"""T1192: ONE generated root, never a per-file override — the twin of `test_curated_root.py`.

`GENERATED_ROOT` moves the whole generated tree (`pipeline.common.curated`), honoured only by a process that opted in
(the review backend, the tests' helper); every other process refuses it, and a traced build stage refuses it even
then. The review tool's per-file `CURATION_*` path overrides it replaced (`RETIRED_GENERATED_ENV`,
`RETIRED_SOURCE_ENV`) are refused when set, never ignored, and no code names one.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from pipeline.common import curated as C
from pipeline.tests.conftest import curated_root, generated_root, no_served_build
from pipeline.tests.test_curated_root import _code_files

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "tools" / "curation-review" / "backend"
#: where the retired names may be spelled: the refusal list and this test
ALLOWED = {REPO / "pipeline/common/curated.py", Path(__file__).resolve()}


def test_no_code_names_a_retired_generated_override():
    pat = re.compile(r"\b(" + "|".join(map(re.escape, {**C.RETIRED_GENERATED_ENV, **C.RETIRED_SOURCE_ENV})) + r")\b")
    hits = [f"{p.relative_to(REPO)}:{i}: {line.strip()}"
            for p in _code_files() if p.resolve() not in ALLOWED
            for i, line in enumerate(p.read_text(encoding="utf-8", errors="replace").splitlines(), 1)
            if pat.search(line)]
    assert not hits, "a retired per-file generated override is still named (T1192):\n" + "\n".join(hits)


@pytest.mark.parametrize("name", sorted(C.RETIRED_GENERATED_ENV))
def test_a_retired_override_is_refused_loudly(name, monkeypatch):
    monkeypatch.setenv(name, "/anywhere")
    with pytest.raises(RuntimeError, match=rf"{name} \(was .*\).* point GENERATED_ROOT at a whole copy"):
        C.generated()
    with pytest.raises(RuntimeError, match=name):
        C.GENERATED.review                                   # noqa: B018 — the access is the check
    with pytest.raises(RuntimeError, match=name):
        C.CURATED.waters                                     # noqa: B018 — and every curated read refuses it too


def test_a_retired_source_override_says_a_source_file_has_none(monkeypatch):
    monkeypatch.setenv("CURATION_TIDAL_GPKG", "/anywhere")
    with pytest.raises(RuntimeError, match=r"CURATION_TIDAL_GPKG \(was SOURCE\.fwa_gpkg\).* a source file has no "
                                           r"override$"):
        C.generated()


def test_a_process_that_did_not_opt_in_refuses_the_root(tmp_path, monkeypatch):
    root = generated_root(monkeypatch, tmp_path / "generated")
    monkeypatch.setattr(C, "_GENERATED_ROOT_ALLOWED", False)
    with pytest.raises(RuntimeError, match="only the review tool and tests may move the generated tree"):
        C.GENERATED.review                                   # noqa: B018 — the access is the check
    # a build, a release, a deliver step: each refuses at its first generated read, here at import
    env = {**os.environ, C.GENERATED_ROOT_ENV: str(root), "PYTHONPATH": str(REPO)}
    for mod in ("pipeline.build", "pipeline.release"):
        got = subprocess.run([sys.executable, "-c", f"import {mod}"], cwd=REPO, env=env, capture_output=True, text=True)
        assert got.returncode != 0 and "only the review tool and tests may move the generated tree" in got.stderr, mod


def test_nothing_set_is_the_tree_config_yaml_names(monkeypatch):
    monkeypatch.delenv(C.GENERATED_ROOT_ENV, raising=False)
    assert C.generated() is C.load_generated()               # the same cached tree, so nothing moves for the owner
    assert C.generated_root() is None


def test_the_root_moves_every_generated_path(tmp_path, monkeypatch):
    live = C.load_generated()
    root = generated_root(monkeypatch, tmp_path / "generated")
    got = C.generated()
    names = C.parse_config()[0]["generated"]

    def leaves(tree, block):
        for k, v in block.items():
            yield from leaves(getattr(tree, k), v) if isinstance(v, dict) else [(getattr(tree, k), v)]
    # it moves ONE root: each path is the copy joined with the very name config.yaml states
    pairs = list(leaves(got, names))
    assert len(pairs) > 20 and all(p == root / name for p, name in pairs)
    assert got.build() == root / "atlas" / C.parse_config()[0]["atlas_build"]
    assert Path(C.GENERATED.review) == root / "review" and Path(C.ROOTS.generated) == root
    assert C.load_generated() == live                        # the live tree is still named, as `load_repo`
    monkeypatch.delenv(C.GENERATED_ROOT_ENV)
    assert C.generated() is live


def test_a_traced_build_stage_refuses_a_moved_root(tmp_path, monkeypatch):
    """A stage's key records `roots.generated` from config.yaml: a moved root would key it on one tree while it reads
    and writes another, so it is refused, named, both under `pipeline build` (`PIPELINE_TRACE`) and in a process that
    records itself (the atlas build, `_trace.start_own`)."""
    from pipeline import _trace
    root = generated_root(monkeypatch, tmp_path / "generated")
    monkeypatch.setenv("PIPELINE_TRACE", str(tmp_path / "trace"))
    with pytest.raises(RuntimeError, match=rf"GENERATED_ROOT={re.escape(str(root))} inside a traced build stage"):
        C.GENERATED.review                                   # noqa: B018 — the access is the check
    monkeypatch.delenv("PIPELINE_TRACE")
    monkeypatch.setattr(_trace, "directory", lambda: str(tmp_path / "own"))
    with pytest.raises(RuntimeError, match="inside a traced build stage"):
        C.ROOTS.generated                                    # noqa: B018
    monkeypatch.delenv(C.GENERATED_ROOT_ENV)
    assert Path(C.GENERATED.review).name == "review"         # the live tree is no refusal


def test_a_bad_root_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(C, "_GENERATED_ROOT_ALLOWED", True)
    monkeypatch.setenv(C.GENERATED_ROOT_ENV, "")
    with pytest.raises(RuntimeError, match="empty"):
        C.generated()
    monkeypatch.setenv(C.GENERATED_ROOT_ENV, str(tmp_path / "absent"))
    with pytest.raises(FileNotFoundError, match="make_generated_root"):
        C.generated()


def test_a_copy_holds_only_what_is_named_and_never_a_build(tmp_path):
    live = C.load_roots().generated
    root = C.make_generated_root(tmp_path / "g", ("review/q154/sample.json",))      # a committed file: CI has it
    assert [p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()] == ["review/q154/sample.json"]
    assert (root / "review/q154/sample.json").read_bytes() == (live / "review/q154/sample.json").read_bytes()
    for bad in ("atlas", "reaches/full", "bundle", "tiles", "side", "runtiming", "release", "deliver-cache",
                "build-logs", "regs", "../curated", "/etc", "review/no-such-file"):
        with pytest.raises((ValueError, FileNotFoundError)):
            C.make_generated_root(tmp_path / "h", (bad,))
    for dst in (live, live / "review", REPO):
        with pytest.raises(ValueError, match="overlaps"):
            C.make_generated_root(dst)


def test_the_review_backend_reads_every_generated_input_from_the_copy(tmp_path, monkeypatch):
    """The whole tool moves with the one root: each machine-written input, its logs and its served build's default,
    with no per-file variable left to point one of them elsewhere."""
    no_served_build(monkeypatch, tmp_path)
    curated_root(monkeypatch, tmp_path / "curated")
    root = generated_root(monkeypatch, tmp_path / "generated")
    monkeypatch.delenv("CURATION_BUILD")                     # the served build's default follows the root too
    monkeypatch.setattr(C, "_GENERATED_ROOT_ALLOWED", False)  # the backend opts itself in (app.py), as at a real start
    mods = ("app", "reuse", "dfo", "preverify", "awaiting_water", "evidence", "inseason", "write_through",
            "inputs_sync", "tidal", "writes", "review", "model_api", "answer", "synopsis_pages", "checks")
    monkeypatch.syspath_prepend(str(BACKEND))
    for m in mods:
        sys.modules.pop(m, None)
    monkeypatch.delenv("CURATION_REFUSED_LOG")               # conftest's guard; its default is what is under test
    try:
        import app
        import awaiting_water
        import dfo
        import evidence
        import inseason
        import preverify
        import reuse
        import writes
        from pipeline.regs.review import names, proposals, q154, slough, tidal
        got = {"reuse._BUILD": reuse._BUILD, "reuse.REACH_DIR": reuse.REACH_DIR, "reuse.TILES_DIR": reuse.TILES_DIR,
               "reuse.ROW_IMAGES_DIR": reuse.ROW_IMAGES_DIR, "reuse._proposals_dir": reuse._proposals_dir(),
               "dfo.SUGGESTIONS_PATH": dfo.SUGGESTIONS_PATH, "dfo._fixes_path": dfo._fixes_path(),
               "dfo._closures_path": dfo._closures_path(), "dfo._closed_geometry_path": dfo._closed_geometry_path(),
               "awaiting_water.PATH": awaiting_water.PATH, "evidence.ROW_BOXES_PATH": evidence.ROW_BOXES_PATH,
               "evidence.RAW_DATA_PATH": evidence.RAW_DATA_PATH, "inseason.feed_path": inseason.feed_path(),
               "app.refused_log": app.refused_log(), "app.WRITE_THROUGH.log_path": app.WRITE_THROUGH.log_path,
               "app.INPUTS_SYNC.log_path": app.INPUTS_SYNC.log_path, "names._path": names._path(),
               "q154._path": q154._path(), "tidal.path": tidal.path(), "tidal.pass2_path": tidal.pass2_path(),
               "proposals.out_dir": proposals.out_dir(), "writes.BACKUP_ROOT": writes.BACKUP_ROOT,
               **{f"preverify.FILES[{k}]": v for k, v in preverify.FILES.items()},
               **{f"slough._paths[{i}]": v for i, v in enumerate(slough._paths())}}
        assert {k: v for k, v in got.items() if root not in Path(v).parents} == {}
    finally:
        for m in mods:
            sys.modules.pop(m, None)


def test_a_link_is_never_copied(tmp_path, monkeypatch):
    """A copied link would point back at a real file: refused, never followed or copied (T1192 review)."""
    root = generated_root(monkeypatch, tmp_path / "a")
    (root / "review").mkdir()
    (root / "review" / "x.json").symlink_to(tmp_path / "elsewhere.json")
    for name in ("review", "review/x.json"):
        with pytest.raises((ValueError, FileNotFoundError), match="link"):
            C.make_generated_root(tmp_path / "b", (name,))


def test_the_mini_release_watches_the_live_tree_under_a_moved_root(tmp_path, monkeypatch):
    from pipeline.tools import mini_release as M
    live = M.watched_dirs()
    root = generated_root(monkeypatch, tmp_path / "generated")
    assert M.watched_dirs() == live and not [d for d in live if root in Path(d).parents]


def test_a_rehearsal_under_a_moved_root_is_never_canonical(tmp_path, monkeypatch):
    from pipeline.regs.review import vintage as V
    live = V.Target.canonical()
    root = generated_root(monkeypatch, tmp_path / "generated")
    assert V.Target.canonical() == live
    b = C.GENERATED.build()                                  # what the tool serves by default under the root
    moved = V.Target(entries=live.entries, splits=live.splits, build=b, reach=C.GENERATED.reaches / b.name,
                     bundle=C.GENERATED.bundle / "bundle.sqlite", release=root / "release", tiles=C.GENERATED.tiles)
    assert moved.kind() == "scratch" and root in Path(moved.build).parents
    assert not any("pipeline build --promote" in c or c.startswith("scripts/heavy.sh build")
                   for c in V.rebuild_commands(["no reach run"], moved))


def test_the_backend_refuses_a_generated_copy_beside_the_live_curated_tree(tmp_path, monkeypatch):
    """Write-through and the input sync act on the curated tree: from copied inputs they would write the live files."""
    generated_root(monkeypatch, tmp_path / "generated")
    monkeypatch.delenv(C.ROOT_ENV, raising=False)
    monkeypatch.syspath_prepend(str(BACKEND))
    sys.modules.pop("app", None)
    try:
        with pytest.raises(RuntimeError, match="GENERATED_ROOT is set without CURATED_ROOT"):
            import app  # noqa: F401
    finally:
        sys.modules.pop("app", None)


def test_a_real_backend_start_lands_every_import_time_path_in_the_copy(tmp_path, monkeypatch):
    """The import-time constants (the backups, the bundle the tool reads) are frozen when a process starts: right for a
    real start, so one is made (a fresh process, the variables set before it imports anything)."""
    curated = curated_root(monkeypatch, tmp_path / "curated")
    root = C.make_generated_root(tmp_path / "generated")
    env = {k: v for k, v in os.environ.items() if not k.startswith("CURATION_")}
    env.update({C.GENERATED_ROOT_ENV: str(root), C.ROOT_ENV: str(curated), "PYTHONPATH": str(REPO)})
    probe = ("import json, app, writes, preverify, awaiting_water, evidence, dfo\n"
             "from pipeline.deliver.bundle import read\n"
             "from pipeline.deliver.bundle.read import records\n"
             "print(json.dumps({'writes.BACKUP_ROOT': str(writes.BACKUP_ROOT), 'records.BUNDLE': records.BUNDLE,\n"
             "  'read.BUNDLE': read.BUNDLE, 'preverify': [str(v) for v in preverify.FILES.values()],\n"
             "  'awaiting_water.PATH': str(awaiting_water.PATH), 'evidence.ROW_BOXES_PATH': str(evidence.ROW_BOXES_PATH),\n"
             "  'dfo.SUGGESTIONS_PATH': str(dfo.SUGGESTIONS_PATH), 'refused': str(app.refused_log())}))\n")
    got = subprocess.run([sys.executable, "-c", probe], cwd=BACKEND, env=env, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr[-2000:]
    paths = json.loads(got.stdout.strip().splitlines()[-1])
    flat = {k: v for k, vs in paths.items() for v in (vs if isinstance(vs, list) else [vs])}
    assert {k: v for k, v in flat.items() if root.resolve() not in Path(v).resolve().parents} == {}
