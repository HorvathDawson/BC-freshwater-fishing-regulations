"""SMOKE COVERAGE (T432, 2026-11-05). `pytest -m smoke` (Tier 1, `smoke.txt`) is one fast data-free test per test file,
but nothing made a new file join it: on 11-05 the set covered 205 of 385 files and the root conftest refused only stale
ids. Now every test file under the pytest.ini testpaths must be

  - in `smoke.txt` (at least one id), or
  - made only of data / slow tests (every test, or the module's `pytestmark`, carries a `needs_*` or `slow` marker), or
  - in `smoke_pending.txt` (fast tests exist, the fastest was never picked) — a ratchet: the list may only shrink.

A file in both `smoke.txt` and `smoke_pending.txt`, or a pending line naming no file, fails (stale).
"""
from __future__ import annotations

import ast
import configparser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SMOKE = ROOT / "pipeline" / "tests" / "smoke.txt"
PENDING = ROOT / "pipeline" / "tests" / "smoke_pending.txt"
#: smoke_pending.txt after T496 (2026-11-08); it may only shrink (a new file goes into smoke.txt)
PENDING_MAX = 3


def _lines(p: Path) -> list[str]:
    return [l.strip() for l in p.read_text(encoding="utf-8").splitlines() if l.strip() and not l.lstrip().startswith("#")]


def _test_files() -> list[str]:
    ini = configparser.ConfigParser()
    ini.read(ROOT / "pytest.ini")
    trees = ini["pytest"]["testpaths"].split()
    return sorted(p.relative_to(ROOT).as_posix() for t in trees for p in (ROOT / t).rglob("test_*.py")
                  if "__pycache__" not in p.parts)




def _out_mark(node: ast.AST) -> bool:
    s = ast.unparse(node)
    return "needs_" in s or "mark.slow" in s


def only_data_or_slow(source: str) -> bool:
    """Every test in the file carries a needs_* or slow marker (its own, its class's, or the module's `pytestmark`)."""
    tree = ast.parse(source)
    for n in tree.body:
        if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "pytestmark" for t in n.targets) \
                and _out_mark(n.value):
            return True
    marked: list[bool] = []
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name.startswith("test"):
            marked.append(any(_out_mark(d) for d in n.decorator_list))
        elif isinstance(n, ast.ClassDef) and n.name.startswith("Test"):
            cls = any(_out_mark(d) for d in n.decorator_list)
            for f in n.body:
                if isinstance(f, (ast.FunctionDef, ast.AsyncFunctionDef)) and f.name.startswith("test"):
                    marked.append(cls or any(_out_mark(d) for d in f.decorator_list))
    return bool(marked) and all(marked)


def uncovered(files: list[str], smoke: set[str], pending: set[str], read) -> list[str]:
    return [f for f in files if f not in smoke and f not in pending and not only_data_or_slow(read(f))]


def _read(f: str) -> str:
    return (ROOT / f).read_text(encoding="utf-8")


def test_every_test_file_is_smoke_listed_data_only_or_pending():
    smoke = {i.split("::")[0] for i in _lines(SMOKE)}
    bad = uncovered(_test_files(), smoke, set(_lines(PENDING)), _read)
    assert not bad, ("a test file with no smoke id: put its fastest data-free test in pipeline/tests/smoke.txt "
                     f"(one per file): {bad}")


def test_pending_is_not_stale_and_only_shrinks():
    smoke = {i.split("::")[0] for i in _lines(SMOKE)}
    pending = _lines(PENDING)
    files = set(_test_files())
    assert len(pending) == len(set(pending)), "smoke_pending.txt repeats a line"
    assert not [p for p in pending if p in smoke], "in smoke.txt now: delete its smoke_pending.txt line"
    assert not [p for p in pending if p not in files], "smoke_pending.txt names a file that is gone"
    assert len(pending) <= PENDING_MAX, f"smoke_pending.txt may only shrink (≤ {PENDING_MAX}): a new file goes into smoke.txt"


def test_the_gate_names_a_planted_file():
    """Mutation pin: a new file with a fast test and no smoke id is named; marked needs_*/slow, or listed, it is not."""
    src = {"pipeline/tests/test_planted.py": "def test_fast():\n    assert 1\n",
           "pipeline/tests/test_planted_data.py": "import pytest\npytestmark = pytest.mark.needs_bundle\ndef test_a():\n    pass\n",
           "pipeline/tests/test_planted_slow.py": "import pytest\n@pytest.mark.slow\ndef test_a():\n    pass\n"
                                                  "class TestB:\n    @pytest.mark.needs_atlas\n    def test_b(self):\n        pass\n",
           "pipeline/tests/test_planted_mixed.py": "import pytest\n@pytest.mark.slow\ndef test_a():\n    pass\ndef test_b():\n    pass\n"}
    files = sorted(src)
    assert uncovered(files, set(), set(), src.__getitem__) == ["pipeline/tests/test_planted.py", "pipeline/tests/test_planted_mixed.py"]
    assert uncovered(files, {"pipeline/tests/test_planted.py"}, {"pipeline/tests/test_planted_mixed.py"}, src.__getitem__) == []
