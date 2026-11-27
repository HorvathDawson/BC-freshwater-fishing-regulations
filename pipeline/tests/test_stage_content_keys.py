"""T1229 THE STAGE-KEY GUARD: every curated file a build stage reads has its CONTENT in that stage's key, or a reason it
needs none. A stage's config record (T1192) says WHERE a file is; only a content part of its key says what is IN it, so
a read with neither ships a stale output (the sign-zone tile layer, the atlas's override-named items: the two holes).

NET 1, what a stage RECORDS reading: the `curated.<a>.<b>` config reads (C lines) of each stage's processes in the mini
chain (`mini_chain.built`, every stage under the recorder) and in its flag parse.
NET 2, what a stage's CODE names: every `CURATED.<a>.<b>` in every module those runs imported (their M lines, run or
not), and every site reaching a leaf by a computed name (`DYNAMIC`, each with its reason).
COVERED: the curated files `build.curated_parts(stage)` OPENS, traced (the key's own reads: no second list); the atlas's
`ATLAS_INPUTS`; and what a stage it depends on covers (`build.DEPENDS`: that stage's run reruns this one).
EXEMPT: `build.NOT_CONTENT_KEYED[stage]`, the atlas's `NOT_ATLAS_INPUTS`, each with its reason.

WHAT IT DOES NOT SEE: a read in a module no mini stage and no flag parse imports; a leaf reached by a computed name at a
site that is not `CURATED` / `load_repo()` (a path passed in from elsewhere); `generated.*` and `source.*` reads; the
`inseason` stage (made when missing, never by its key); clim beyond its flag parse (the mini has no HYDAT); a module
that opens a curated file when `curated_parts` first imports it would count as keyed.
"""
from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
import textwrap
import uuid
from pathlib import Path

import pytest

from pipeline import build as B
from pipeline.atlas import inputs as I
from pipeline.atlas.splits.area_splits import area_files, load_area_split_defs
from pipeline.common import stepcache as SC
from pipeline.common.curated import REPO_ROOT, Curated, make_root
from pipeline.tests import mini_chain
from pipeline.tests.conftest import curated_root

PY = sys.executable
SCRATCH = REPO_ROOT / ".test-scratch"

#: build stage -> its production module and sub-command (`build.commands`; the atlas is started by hand, rule 17)
STAGE_MODULES = {"atlas": ("pipeline.atlas.build",), "clim": ("pipeline.gauges.feed.climatology",),
                 "reach": ("pipeline.atlas.reach.cli",), "tiles": ("pipeline.deliver.tiles",),
                 "deliver": ("pipeline.deliver", "all")}
#: stages the guard leaves out, and why
NOT_GUARDED = {"inseason": "made only when missing (`build.MADE_WHEN_MISSING`), never rerun by its key"}
#: the mini chain's stages (`mini_release.stage_commands`) that are no build stage
NOT_A_STAGE = {"check_curated": "the preflight's audit (`build.preflight`), in the `pipeline build` process"}

#: SITES THAT REACH A CURATED LEAF BY A COMPUTED NAME: {(module, function): what that leaf is and what keys it}
DYNAMIC = {
    ("pipeline/common/curated.py", "<module>"): "the tree's own definition (`CURATED = _Lazy(load)`)",
    ("pipeline/atlas/inputs.py", "curated_path"): "resolves `ATLAS_INPUTS` / `RECORD_NAME_OF` keys: the atlas record",
    ("pipeline/atlas/splits/area_splits.py", "_resolve_area_file"):
        "an areas def's `file`: held by the tiles and atlas keys (`test_every_area_file_is_in_the_tiles_and_atlas_keys`)",
    # imported by the production stages (their manifest records), not by the mini chain's
    ("pipeline/build.py", "not_build_inputs"): "resolves the exclusion lists `NOT_BUILD_INPUTS` / `NOT_DELIVER_INPUTS`",
    ("pipeline/build.py", "_bytes_digest"): "the leaves of `REACH_FILES` and the areas file: the key parts themselves",
    ("pipeline/build.py", "curated_digest"): "the whole tree (`model_dump`): the deliver key itself",
    ("pipeline/regs/review/proposals.py", "side_commands"): "a side copy's paths (`proposals side`), run by no stage",
    ("pipeline/regs/review/proposals.py", "write_side"): "a side copy's root (`proposals side`), run by no stage",
}

#: a stage stand-in: the real stage module's flag parse (`--help`)
FLAG_PARSE = textwrap.dedent("""
    import runpy, sys
    import pipeline                                  # installs the recorder (PIPELINE_TRACE), as every stage does
    sys.argv = [sys.argv[1], *sys.argv[2:], "--help"]
    try:
        runpy.run_module(sys.argv[0], run_name="__main__")
    except SystemExit:
        pass
""")

#: {curated leaf: its file} on stdout, for the tree `CURATED_ROOT` names
LEAVES = textwrap.dedent("""
    import json
    from pipeline.common import curated as C
    tree = C.load()
    print(json.dumps({f"{b}.{k}": str(getattr(getattr(tree, b), k))
                      for b in type(tree).model_fields for k in type(getattr(tree, b)).model_fields}))
""")
#: one stage's curated key parts under the recorder, then the leaves
COVER = "import sys\nimport pipeline\nfrom pipeline import build as B\nB.curated_parts(sys.argv[1])\n" + LEAVES


def _env(**extra) -> dict:
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPYCACHEPREFIX", "PIPELINE_TRACE")}
    return {**env, "PYTHONPATH": str(REPO_ROOT), **extra}


def _leaves(trace: SC.Trace) -> set[str]:
    return {k.split(".", 1)[1] for k in trace.config if k.startswith("curated.") and k.count(".") >= 2}


def _mini_stages() -> dict[str, tuple[str, ...]]:
    from pipeline.deliver.__main__ import CACHED_STEPS
    return {"atlas": ("atlas",), "reach": ("reach",), "tiles": ("tiles",), "deliver": (*CACHED_STEPS, "release")}


@pytest.fixture(scope="module")
def runs(tmp_path_factory) -> dict[str, list[SC.Trace]]:
    """{stage: the traces of its runs}: its mini chain stages, then its flag parse."""
    traces = mini_chain.traces(tmp_path_factory)
    out = {st: [SC.Trace(traces / name) for name in names] for st, names in _mini_stages().items()}
    tmp = tmp_path_factory.mktemp("flag-parse")
    for st, mod in STAGE_MODULES.items():
        subprocess.run([PY, "-c", FLAG_PARSE, *mod], check=True, cwd=REPO_ROOT, capture_output=True,
                       env=_env(PIPELINE_TRACE=str(tmp / st)))
        out.setdefault(st, []).append(SC.Trace(tmp / st))
    return out


def _cover_tree() -> Path:
    """This process's curated tree copied INSIDE the repo (the recorder skips the temp dir), every file a leaf names
    that is absent written empty (`{}`): a key part that reads a file only when it exists (the closed areas) is seen."""
    root = make_root(SCRATCH / f"t1229-{uuid.uuid4().hex[:8]}")
    got = subprocess.run([PY, "-c", LEAVES], check=True,
                         cwd=REPO_ROOT, capture_output=True, text=True, env=_env(CURATED_ROOT=str(root)))
    for p in map(Path, json.loads(got.stdout).values()):
        if p.suffix in (".json", ".jsonl") and not p.exists():
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("" if p.suffix == ".jsonl" else "{}\n", encoding="utf-8")
    return root


@pytest.fixture(scope="module")
def covered() -> dict[str, set[str]]:
    """{stage: the curated leaves its key holds by content}."""
    import shutil
    root = _cover_tree()
    try:
        own: dict[str, set[str]] = {}
        for st in ("clim", "reach", "tiles", "deliver"):
            tr = root / f"trace-{st}"
            got = subprocess.run([PY, "-c", COVER, st], check=True, cwd=REPO_ROOT, capture_output=True, text=True,
                                 env=_env(CURATED_ROOT=str(root), PIPELINE_TRACE=str(tr)))
            t = SC.Trace(tr)
            opened = set(t.reads) | set(t.lists)
            own[st] = set()
            for leaf, p in json.loads(got.stdout).items():
                r = SC._rel(p)
                if r in opened or any(x.startswith(r + "/") for x in opened):
                    own[st].add(leaf)
    finally:
        shutil.rmtree(root, ignore_errors=True)
    own["atlas"] = {I.RECORD_NAME_OF.get(k, k.split(":", 1)[1]) for k in I.ATLAS_INPUTS if k.startswith("curated:")}
    out = {}
    for st, leaves in own.items():                       # a dependency's run reruns this stage: its cover is this one's
        todo, seen = list(B.DEPENDS.get(st, ())), set(leaves)
        while todo:
            d = todo.pop()
            seen |= own[d]
            todo += list(B.DEPENDS.get(d, ()))
        out[st] = seen
    return out


def _exempt(stage: str) -> dict[str, str]:
    if stage == "atlas":
        return {k.split(":", 1)[1]: v for k, v in I.NOT_ATLAS_INPUTS.items()}
    return B.NOT_CONTENT_KEYED[stage]


def _named(path: Path) -> tuple[set[str], set[tuple[str, str]]]:
    """(every `CURATED.<a>.<b>` / `load_repo().<a>.<b>` leaf the module names, every site that reaches the tree without
    naming a leaf: (module, function))."""
    rel = str(path.relative_to(REPO_ROOT))
    tree = ast.parse(path.read_text(encoding="utf-8"))
    parent, func = {}, {}
    for n in ast.walk(tree):
        for c in ast.iter_child_nodes(n):
            parent[c] = n

    def where(n) -> str:
        while n in parent:
            n = parent[n]
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                return n.name
        return "<module>"
    leaves, sites = set(), set()
    for n in ast.walk(tree):
        is_tree = (isinstance(n, ast.Name) and n.id == "CURATED") or (
            isinstance(n, ast.Call) and getattr(n.func, "id", getattr(n.func, "attr", "")) == "load_repo")
        if not is_tree or isinstance(parent.get(n), (ast.alias, ast.ImportFrom)):
            continue
        chain, x = [], n
        while isinstance(parent.get(x), ast.Attribute) and parent[x].value is x:
            x = parent[x]
            chain.append(x.attr)
        if chain[:1] == ["base"]:
            continue                                     # the root: a directory, never a leaf's content
        if len(chain) >= 2 and chain[0] in Curated.model_fields:
            leaves.add(".".join(chain[:2]))
        else:
            sites.add((rel, where(n)))
    return leaves, sites


def _modules(runs, stage) -> set[str]:
    return {m for t in runs[stage] for m in t.modules if m.endswith(".py")}


# ------------------------------------------------------------------------------------------------------- the guard

def test_every_stage_is_guarded():
    """A new build stage, or a new mini chain stage, is placed here or the guard says so."""
    assert set(STAGE_MODULES) | set(NOT_GUARDED) == set(B.STAGES) | {"atlas"}
    from pipeline.tools import mini_release as M
    names = [n for n, _ in M.stage_commands(Path("o"), Path("f"), Path("c"))]
    placed = [n for ns in _mini_stages().values() for n in ns] + list(NOT_A_STAGE)
    assert sorted(names) == sorted(placed)
    assert set(B.NOT_CONTENT_KEYED) == set(STAGE_MODULES) - {"atlas"}


@pytest.mark.parametrize("stage", sorted(STAGE_MODULES))
def test_net1_every_curated_file_a_stage_records_reading_is_content_keyed(runs, covered, stage):
    read = set().union(*(_leaves(t) for t in runs[stage]))
    assert read, f"{stage} recorded no curated read: the trace is broken"
    loose = sorted(read - covered[stage] - set(_exempt(stage)))
    assert not loose, (f"{stage} reads curated file(s) whose CONTENT is in no part of its key: {loose} — add each to "
                       "its key (`build.curated_parts`; the atlas: `ATLAS_INPUTS`), or to `build.NOT_CONTENT_KEYED` "
                       "(the atlas: `NOT_ATLAS_INPUTS`) with the reason it needs none")


@pytest.mark.parametrize("stage", sorted(STAGE_MODULES))
def test_net2_every_curated_leaf_a_stage_s_modules_name_is_content_keyed(runs, covered, stage):
    named, sites = {}, set()
    for m in sorted(_modules(runs, stage)):
        leaves, s = _named(REPO_ROOT / m)
        for leaf in leaves:
            named.setdefault(leaf, m)
        sites |= s
    loose = {leaf: m for leaf, m in sorted(named.items()) if leaf not in covered[stage] | set(_exempt(stage))}
    assert not loose, (f"{stage} imports code naming curated file(s) whose CONTENT is in no part of its key: {loose} — "
                       "key it (`build.curated_parts`; the atlas: `ATLAS_INPUTS`) or exempt it with its reason")
    assert not sites - set(DYNAMIC), (f"{stage}: a curated leaf reached by a computed name: {sorted(sites - set(DYNAMIC))}"
                                      " — name the leaf, or declare the site in DYNAMIC with what keys it")


def test_every_declared_site_and_exemption_is_live(runs, covered):
    """A DYNAMIC site that is gone from its module, an exemption naming no leaf, or one the key holds anyway, is refused
    (no rot). A site is checked in its module's source: some are imported only on production paths."""
    sites = set().union(*(_named(REPO_ROOT / m)[1] for m in {m for m, _ in DYNAMIC}))
    assert set(DYNAMIC) <= sites, f"DYNAMIC names sites that are gone: {sorted(set(DYNAMIC) - sites)}"
    leaves = {f"{b}.{k}" for b, f in Curated.model_fields.items() for k in f.annotation.model_fields}
    for st in STAGE_MODULES:
        ex = set(_exempt(st))
        assert ex <= leaves, f"{st}: exemptions name no curated leaf: {sorted(ex - leaves)}"
        assert not ex & covered[st], f"{st}: exempt yet content-keyed: {sorted(ex & covered[st])}"


def test_every_area_file_is_in_the_tiles_and_atlas_keys(covered):
    """THE DYNAMIC SITE THE MINI NEVER TAKES (it has no `file` def): the atlas cuts and the tiles draw every areas def's
    polygons (`load_area_polys`), and a def naming a file by `file` reads that file. Its content must be in both keys,
    or a moved vertex ships a stale `sign_zone` layer (hole 1) and leaves the atlas's cut where it was."""
    named = {f"waters.{ad['file']}" for ad in load_area_split_defs() if ad.get("file")}
    assert named, "no areas def names a file: this pin has nothing to hold"
    for st in ("atlas", "tiles"):
        assert named <= covered[st], f"areas defs name file(s) the {st} key does not hold: {sorted(named - covered[st])}"


# ------------------------------------------------------------------------------------------------- the two holes

def test_hole1_the_tiles_key_holds_every_file_an_areas_def_names(tmp_path, monkeypatch):
    """A one-byte change of the sign zones' file moves the tiles key; a def naming a second file is in it too."""
    from pipeline.tests.test_build_key_holes import _fake_tools, _tiles_build
    curated_root(monkeypatch, tmp_path / "curated")
    build = _tiles_build(tmp_path, monkeypatch)
    _fake_tools(tmp_path / "bin", "tippecanoe v2.79.0")
    monkeypatch.setenv("PATH", f"{tmp_path / 'bin'}{os.pathsep}/usr/bin{os.pathsep}/bin")
    files = area_files()
    assert set(files) == {"added_areas"}
    before = B.tiles_key(build)
    assert set(before["area_files"]) == {"added_areas"}
    sign = files["added_areas"]
    sign.write_bytes(sign.read_bytes() + b" ")
    after = B.tiles_key(build)
    assert after != before and after["areas"] == before["areas"]
    second = tmp_path / "more_areas.geojson"
    second.write_text('{"type": "FeatureCollection", "features": []}')
    defs_file = Path(B.CURATED.waters.area_closures)
    doc = json.loads(defs_file.read_text(encoding="utf-8"))
    doc["areas"].append({"id": "t1229", "file": str(second), "name_field": "name", "not_drawn": "a test"})
    defs_file.write_text(json.dumps(doc), encoding="utf-8")
    with_second = B.tiles_key(build)
    assert set(with_second["area_files"]) == {"added_areas", str(second)}
    second.write_text('{"type": "FeatureCollection", "features": [] }')
    assert B.tiles_key(build)["area_files"] != with_second["area_files"]
    second.unlink()
    with pytest.raises(FileNotFoundError):
        B.tiles_key(build)


def test_hole2_the_atlas_records_the_book_name_links():
    """`pinned_by_override` makes and names items from the overrides: their content is an atlas input."""
    assert "curated:regulations.book_name_links" in I.ATLAS_INPUTS
    assert I.LISTED_AFTER.keys() <= I.ATLAS_INPUTS.keys()


def test_hole3_the_reach_key_holds_the_cuts_a_closed_area_lands_in(covered):
    """Found by net 2 over the production reach run's modules (the mini has no confirmed closed area): a confirmed
    closed area's new end binds once it has landed as a cut in the curated cuts file (`dfo_salmon.landed_resolver` ->
    `review.proposals.landed_cut`, which reads that file itself)."""
    from pipeline.atlas.reach import dfo_salmon
    assert callable(dfo_salmon.landed_resolver)
    assert "waters.cuts" in covered["reach"]
