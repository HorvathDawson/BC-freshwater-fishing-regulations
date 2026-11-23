#!/usr/bin/env python3
"""THE TESTS A CHANGE REACHES (T1041), for the pre-push hook and the mutation report. Standard library only.

    python3 scripts/related_tests.py pytest PATH...   # the pipeline test files the changed paths reach, one per line
    python3 scripts/related_tests.py app PATH...      # the app files to hand `vitest related`, relative to app/

Why: two red CI runs on 2026-11-23 were checks the pre-push hook skipped. #63: a change in app/packages/core broke
ui-native tests (WaterScreen.tidal.test.tsx: fixtures with no `byKey`) — the hook ran only each changed file's sibling
test. #67: pipeline/deliver/answers/display/decided.py changed and test_local_closures.py, which imports it, failed in
the no-data tier the hook leaves to the PR.

THE APP: `vitest related <files>` follows vitest's own import graph, so a change in one package runs the tests of every
package that imports it. That is most of the suite for a core file (core's index re-exports it: ~550 of ~570 files,
~2 min here); it is the honest answer, not a cap. The feeds worker (its own vitest root) is the hook's "feeds worker
tests" step. A changed non-code file (a fixture, a .gz) has no import graph: listed, its readers left to the PR.

THE PIPELINE: the test files that import a changed module DIRECTLY (`import a.b` / `from a.b import c` / relative), and
changed test files themselves. Not the transitive closure: through the package `__init__`s it is ~400 of ~550 files
for one leaf module (the whole tier, minutes). A changed non-Python file under pipeline/ (a pinned JSON beside its
module) counts as a change to each module whose source names its file name. A changed conftest.py is said, not
followed (it reaches every test under it: the tier's job). The list is capped at `PY_CAP` files (direct-import counts
here: decided.py 5, salmon.py 27, common/curated.py 50 at 111 s); past it, the rest are NAMED as left to
`scripts/before_pr.sh` and the PR's tier — never dropped quietly.
"""
from __future__ import annotations

import ast
import configparser
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
#: the pipeline test files the hook runs at most (salmon.py's 27 direct importers: 71 s here)
PY_CAP = 25
#: what `vitest related` can follow (its import graph)
APP_CODE = re.compile(r"\.(ts|tsx|mts|cts|js|jsx|mjs|cjs)$")


def testpaths(root: Path = ROOT) -> list[str]:
    """pytest.ini's testpaths (the folders the tier collects)."""
    c = configparser.ConfigParser()
    if not c.read(root / "pytest.ini"):
        raise SystemExit(f"related-tests: no pytest.ini at {root}")
    return c["pytest"]["testpaths"].split()


def _module(rel: str) -> str:
    parts = list(Path(rel).with_suffix("").parts)
    return ".".join(parts[:-1] if parts[-1] == "__init__" else parts)


def _is_test(rel: str, tps: list[str]) -> bool:
    return Path(rel).name.startswith("test_") and any(rel.startswith(t.rstrip("/") + "/") for t in tps)


def import_graph(root: Path = ROOT) -> tuple[dict[str, str], dict[str, set[str]]]:
    """(module -> repo-relative path, module -> the modules that import it directly) over pipeline/**/*.py. A name
    imported is resolved to the deepest module it names (`from a.b import c`: a.b.c when that is a module, else a.b)."""
    mods = {_module(str(p.relative_to(root))): str(p.relative_to(root)) for p in (root / "pipeline").rglob("*.py")}
    rev: dict[str, set[str]] = {}
    for name, rel in mods.items():
        try:
            tree = ast.parse((root / rel).read_text(encoding="utf-8"))
        except SyntaxError as e:
            raise SystemExit(f"related-tests: {rel} does not parse ({e}): fix it first") from e
        pkg = name if rel.endswith("__init__.py") else name.rpartition(".")[0]
        named: set[str] = set()
        for n in ast.walk(tree):
            if isinstance(n, ast.Import):
                named |= {a.name for a in n.names}
            elif isinstance(n, ast.ImportFrom):
                base = n.module or ""
                if n.level:
                    up = pkg.split(".")
                    up = up[:len(up) - (n.level - 1)] if n.level > 1 else up
                    base = ".".join([*up, base] if base else up)
                named |= {base, *(f"{base}.{a.name}" for a in n.names)}
        for g in named:
            while g and g not in mods:
                g = g.rpartition(".")[0]
            if g and g != name:
                rev.setdefault(g, set()).add(name)
    return mods, rev


def pytest_related(paths: list[str], root: Path = ROOT, cap: int = PY_CAP) -> tuple[list[str], list[str]]:
    """(the test files to run, what was chosen and why, one line each) for these changed paths."""
    tps = testpaths(root)
    mods, rev = import_graph(root)
    by_path = {p: m for m, p in mods.items()}
    said: list[str] = []
    changed_tests = sorted(p for p in paths if _is_test(p, tps) and (root / p).is_file())
    sources: set[str] = set()
    for p in paths:
        if Path(p).name == "conftest.py" and (p == "conftest.py" or p.startswith("pipeline/")):
            said.append(f"{p}: a conftest reaches every test under it — left to the tier (scripts/before_pr.sh, CI)")
        elif not p.startswith("pipeline/") or _is_test(p, tps):
            continue
        elif p.endswith(".py"):
            if p in by_path:
                sources.add(by_path[p])
            else:
                said.append(f"{p}: deleted — the tests that imported it fail to import (the tier, scripts/before_pr.sh)")
        elif (root / p).is_file():
            readers = sorted(m for m, rel in mods.items() if Path(p).name in (root / rel).read_text(encoding="utf-8"))
            sources |= set(readers)
            said.append(f"{p}: read by {', '.join(readers) or 'no pipeline module that names it'}")
        else:
            said.append(f"{p}: deleted — its readers' tests run in the tier (scripts/before_pr.sh, CI)")
    importers = sorted({mods[i] for s in sources for i in rev.get(s, ()) if _is_test(mods[i], tps)}
                       - set(changed_tests))
    picked = changed_tests + importers
    if sources:
        said.append(f"{len(sources)} changed pipeline module(s): {len(importers)} test file(s) import one directly")
    if changed_tests:
        said.append(f"{len(changed_tests)} changed test file(s)")
    if len(picked) > cap:
        left = picked[cap:]
        said.append(f"CAPPED at {cap} of {len(picked)} test files: {len(left)} left to scripts/before_pr.sh and the "
                    f"PR's tier: {', '.join(Path(x).name for x in left[:8])}{' …' if len(left) > 8 else ''}")
        picked = picked[:cap]
    return picked, said


def app_related(paths: list[str], root: Path = ROOT) -> tuple[list[str], list[str]]:
    """(the changed app code files, relative to app/, for `vitest related`; what was left out and why)."""
    files, other, gone, said = [], [], [], []
    for p in paths:
        if not p.startswith("app/") or "/node_modules/" in p or p.startswith("app/workers/"):
            continue
        if not APP_CODE.search(p):
            other.append(p)
        elif (root / p).is_file():
            files.append(p[len("app/"):])
        else:
            gone.append(p)
    if gone:
        said.append(f"{len(gone)} app code file(s) deleted (no graph to follow; their importers changed too, and the "
                    f"typecheck in check:fast finds a stale import): {', '.join(gone[:6])}{' …' if len(gone) > 6 else ''}")
    if other:
        said.append(f"{len(other)} changed app file(s) that are not code (no import graph; their readers run on the "
                    f"PR): {', '.join(other[:6])}{' …' if len(other) > 6 else ''}")
    return sorted(files), said


def only_regex(files: list[str]) -> str:
    """scripts/pytest-chunked.sh `--only`: exactly these paths (grep -E over `find`'s output)."""
    return "^(" + "|".join(re.escape(f) for f in files) + ")$"


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) >= 1 and argv[0] in ("pytest", "app"):
        got, said = (pytest_related if argv[0] == "pytest" else app_related)(argv[1:])
        for ln in said:
            print(f"related-tests: {ln}", file=sys.stderr)
        print("\n".join(got))
        return 0
    print(__doc__.split("\n\n")[1], file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
