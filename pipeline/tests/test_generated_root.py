"""T1192: ONE generated root, never a per-file override — the twin of `test_curated_root.py`.

`GENERATED_ROOT` moves the whole generated tree (`pipeline.common.curated`); the review tool's per-file
`CURATION_*` path overrides it replaced (`RETIRED_GENERATED_ENV`) are refused when set, never ignored, and no code
names one. A traced build stage refuses a moved root: its key records `roots.generated` as config.yaml states it.
"""
from __future__ import annotations

import re
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
    pat = re.compile(r"\b(" + "|".join(map(re.escape, C.RETIRED_GENERATED_ENV)) + r")\b")
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
    for bad in ("atlas", "reaches/full", "bundle", "tiles", "side", "../curated", "/etc", "review/no-such-file"):
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
               "proposals.out_dir": proposals.out_dir(),
               **{f"preverify.FILES[{k}]": v for k, v in preverify.FILES.items()},
               **{f"slough._paths[{i}]": v for i, v in enumerate(slough._paths())}}
        assert {k: v for k, v in got.items() if root not in Path(v).parents} == {}
    finally:
        for m in mods:
            sys.modules.pop(m, None)
