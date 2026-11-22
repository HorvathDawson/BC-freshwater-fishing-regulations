"""(T994 #21) The review backend installs from the repo's ONE locked set, and every third-party module it imports is in it.

Its requirements.txt held floors (`fastapi>=0.110`, `uvicorn[standard]>=0.29`) and left pydantic and shapely out
("the repo .venv has them"); the versions installed (fastapi/starlette) are what refuse a text/plain mark post, and a
floor would not have held them. Mutation: put the floors back, or drop uvicorn from requirements.txt -> fails.
"""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"
#: import name -> distribution name, where they differ
DIST = {"PIL": "pillow", "yaml": "pyyaml", "dotenv": "python-dotenv"}
#: the repo's own modules the backend imports (not distributions)
OWN = {"pipeline", "project_config"}


def _names(path: Path) -> set[str]:
    """distribution names a requirements / constraints file lists (extras and pins stripped, lower-cased)"""
    out = set()
    for ln in path.read_text(encoding="utf-8").splitlines():
        ln = ln.split("#", 1)[0].strip()
        if ln and not ln.startswith("-"):
            out.add(re.split(r"[\[=<>!~; ]", ln, maxsplit=1)[0].lower().replace("_", "-"))
    return out


def _pinned(path: Path) -> set[str]:
    return {ln.split("==", 1)[0].lower().replace("_", "-")
            for ln in path.read_text(encoding="utf-8").splitlines() if "==" in ln and not ln.startswith("#")}


def _imports() -> set[str]:
    mods = set()
    for p in BACKEND.glob("*.py"):
        for n in ast.walk(ast.parse(p.read_text(encoding="utf-8"))):
            if isinstance(n, ast.Import):
                mods |= {a.name.split(".")[0] for a in n.names}
            elif isinstance(n, ast.ImportFrom) and n.level == 0 and n.module:
                mods.add(n.module.split(".")[0])
    local = {p.stem for p in BACKEND.glob("*.py")}
    return {m for m in mods if m not in sys.stdlib_module_names and m not in local and m not in OWN}


def test_the_backend_requirements_are_the_repo_lock():
    lines = [ln.strip() for ln in (BACKEND / "requirements.txt").read_text(encoding="utf-8").splitlines()
             if ln.strip() and not ln.strip().startswith("#")]
    assert lines == ["-r ../../requirements.txt", "-c ../../constraints.txt"]


def test_every_backend_import_and_its_server_are_listed_and_pinned():
    listed, pinned = _names(REPO / "requirements.txt"), _pinned(REPO / "constraints.txt")
    want = {DIST.get(m, m).lower() for m in _imports()} | {"uvicorn", "fastapi", "starlette"}
    assert {"fastapi", "pydantic", "shapely"} <= want                       # the scan sees what the finding named
    assert sorted(w for w in want - {"starlette"} if w not in listed) == []  # starlette comes with fastapi
    assert sorted(w for w in want if w not in pinned) == []
