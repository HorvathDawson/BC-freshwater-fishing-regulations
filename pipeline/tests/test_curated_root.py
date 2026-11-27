"""T422: ONE curated root, never a per-file override.

`CURATED_ROOT` moves the whole curated tree (`pipeline.common.curated`); the per-file `CURATION_MARKS`,
`CURATION_SPLITS_JSON`, `CURATION_ENTRIES_DIR`, `CURATION_DFO_DIR` (and the earlier retired two) are refused when
set, and no code reads a `CURATION_*` variable in place of a curated path (the grep gate below).
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from pipeline.common import curated as C

REPO = Path(__file__).resolve().parents[2]
#: the code a curated-path override could hide in (docs and archived plans are history, not code)
CODE_DIRS = ("pipeline", "tools/curation-review", "scripts", "deploy")
CODE_EXT = {".py", ".sh", ".ts", ".tsx", ".js", ".mjs", ".yml", ".yaml"}
SKIP = ("node_modules", "pipeline/docs/archive", ".venv")
#: where the retired names may be spelled: the refusal list and the tests that pin it
ALLOWED = {REPO / "pipeline/common/curated.py", Path(__file__).resolve()}


def _code_files():
    for d in CODE_DIRS:
        for p in (REPO / d).rglob("*"):
            rel = p.relative_to(REPO).as_posix()
            if p.suffix in CODE_EXT and p.is_file() and not any(s in rel for s in SKIP):
                yield p


def test_no_code_names_a_retired_override():
    pat = re.compile(r"\b(" + "|".join(map(re.escape, C.RETIRED_ENV)) + r")\b")
    hits = [f"{p.relative_to(REPO)}:{i}: {line.strip()}"
            for p in _code_files() if p.resolve() not in ALLOWED
            for i, line in enumerate(p.read_text(encoding="utf-8", errors="replace").splitlines(), 1)
            if pat.search(line)]
    assert not hits, "a retired per-file curated override is still named (T422):\n" + "\n".join(hits)


def _env_name(node: ast.AST) -> str | None:
    """The literal name of an `os.environ[...]` / `os.environ.get(...)` / `os.getenv(...)` read, else None."""
    if isinstance(node, ast.Subscript) and ast.unparse(node.value).endswith("environ"):
        key = node.slice
    elif isinstance(node, ast.Call) and ast.unparse(node.func).endswith(("environ.get", "getenv")) and node.args:
        key = node.args[0]
    else:
        return None
    return key.value if isinstance(key, ast.Constant) and isinstance(key.value, str) else None


def test_no_curation_variable_stands_in_for_a_curated_path():
    """A statement that reads a `CURATION_*` variable must not also name `CURATED` — that shape
    (`Path(env) if env else CURATED.x`) is a per-file override of a curated path."""
    hits = []
    for p in _code_files():
        if p.suffix != ".py" or p.resolve() in ALLOWED:
            continue
        src = p.read_text(encoding="utf-8")
        tree = ast.parse(src)
        for stmt in ast.walk(tree):
            if not isinstance(stmt, ast.stmt) or isinstance(stmt, (ast.FunctionDef, ast.ClassDef, ast.If,
                                                                   ast.For, ast.While, ast.With, ast.Try,
                                                                   ast.AsyncFunctionDef)):
                continue
            names = {n for n in map(_env_name, ast.walk(stmt)) if n and n.startswith("CURATION_")}
            if names and re.search(r"\bCURATED\b", ast.get_source_segment(src, stmt) or ""):
                hits.append(f"{p.relative_to(REPO)}:{stmt.lineno}: {sorted(names)}")
    assert not hits, "a CURATION_* variable repoints a curated path (use CURATED_ROOT, T422):\n" + "\n".join(hits)


@pytest.mark.parametrize("name", sorted(C.RETIRED_ENV))
def test_a_retired_override_is_refused_loudly(name, monkeypatch):
    monkeypatch.setenv(name, "/anywhere")
    with pytest.raises(RuntimeError, match=name):
        C.load()
    with pytest.raises(RuntimeError, match=name):
        C.CURATED.waters                                     # noqa: B018 — the access is the check


def test_the_root_moves_every_curated_path(tmp_path, monkeypatch):
    live = C.load_repo()
    root = C.make_root(tmp_path / "curated")
    monkeypatch.setenv(C.ROOT_ENV, str(root))
    got = C.load()
    assert Path(got.base) == root.resolve()
    pairs = [(Path(getattr(getattr(live, g), k)), Path(getattr(getattr(got, g), k)))
             for g in type(live).model_fields
             for k in type(getattr(live, g)).model_fields]
    assert pairs and all(b == root.resolve() / a.relative_to(live.base) for a, b in pairs)
    assert Path(C.CURATED.review.marks) == root.resolve() / "review" / "marks.jsonl"
    # (T1192) it moves ONE root: each path is the copy joined with the very name config.yaml states
    names = C.parse_config()[0]["curated"]
    assert all(Path(getattr(getattr(got, g), k)) == root.resolve() / v for g, b in names.items() for k, v in b.items())
    assert C.load_repo() == live                             # the guards still see the live tree
    monkeypatch.delenv(C.ROOT_ENV)
    assert C.load() == live


def test_under_the_sandbox_every_door_to_a_curated_path_lands_in_the_copy(tmp_path, monkeypatch):
    # (T1192 review) the live root is reachable only through the guards' `load_repo`: `ROOTS.curated` and
    # `get_path("curated", …)` / `get_path("roots", "curated")` would hand it out under CURATED_ROOT, so they refuse
    from project_config import ProjectConfig
    root = C.make_root(tmp_path / "curated").resolve()
    monkeypatch.setenv(C.ROOT_ENV, str(root))
    names = C.parse_config()[0]["curated"]
    got = [Path(C.CURATED.base), Path(C.load().base)] + [
        Path(getattr(getattr(C.CURATED, g), k)) for g, b in names.items() for k in b]
    assert got and all(p == root or root in p.parents for p in got), [p for p in got if root not in (p, *p.parents)]
    with pytest.raises(AttributeError, match="CURATED.base"):
        C.ROOTS.curated
    for keys in (("curated", "waters", "cuts"), ("roots", "curated")):
        with pytest.raises(KeyError, match="honours CURATED_ROOT"):
            ProjectConfig().get_path(*keys)


def test_a_bad_root_is_refused(tmp_path, monkeypatch):
    monkeypatch.setenv(C.ROOT_ENV, "")
    with pytest.raises(RuntimeError, match="empty"):
        C.load()
    monkeypatch.setenv(C.ROOT_ENV, str(tmp_path / "absent"))
    with pytest.raises(FileNotFoundError):
        C.load()
    (tmp_path / "half").mkdir()                              # a root without the files: pydantic names the key
    monkeypatch.setenv(C.ROOT_ENV, str(tmp_path / "half"))
    with pytest.raises(Exception, match="waters|cuts"):
        C.load()


def test_a_root_is_never_made_over_the_live_tree():
    live = Path(C.load_repo().base)
    for dst in (live, live / "side", REPO):
        with pytest.raises(ValueError, match="overlaps"):
            C.make_root(dst)
