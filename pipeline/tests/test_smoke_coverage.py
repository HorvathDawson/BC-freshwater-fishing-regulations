"""SMOKE COVERAGE (T432 2026-11-05; derived by rule since T667 D6, 2026-11-14). The smoke set is no longer a hand list:
`pipeline/tests/smoke.py` picks, per test file, the tests it marks `smoke` itself, else its first no-data test. Every
test file with a no-data test is therefore in the set by construction; these tests pin the rule by mutation, and that
nothing names the retired hand lists.
"""
from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from pipeline.tests import smoke

ROOT = Path(__file__).resolve().parents[2]


@dataclass
class _M:
    name: str


@dataclass
class _Item:
    nodeid: str
    marks: list[str] = field(default_factory=list)

    def iter_markers(self, name=None):
        return iter([_M(m) for m in self.marks if name is None or m == name])


def _ids(items):
    return [i.nodeid for i in smoke.pick(items)]


def test_a_new_file_joins_by_its_first_no_data_test():
    items = [_Item("t/test_a.py::test_data", ["needs_bundle"]), _Item("t/test_a.py::test_slow", ["slow"]),
             _Item("t/test_a.py::test_fast"), _Item("t/test_a.py::test_fast_2")]
    assert _ids(items) == ["t/test_a.py::test_fast"]


def test_a_file_that_marks_its_own_has_exactly_those():
    items = [_Item("t/test_g.py::test_first"), _Item("t/test_g.py::test_guard", ["smoke"]),
             _Item("t/test_g.py::test_other"), _Item("t/test_g.py::test_guard_2", ["smoke"])]
    assert _ids(items) == ["t/test_g.py::test_guard", "t/test_g.py::test_guard_2"]


def test_a_data_or_slow_only_file_has_none_and_files_do_not_mix():
    items = [_Item("t/test_d.py::test_a", ["needs_atlas"]), _Item("t/test_d.py::test_b", ["slow", "needs_tiles"]),
             _Item("t/test_e.py::test_a", ["needs_source"]), _Item("t/test_e.py::test_b"),
             _Item("t/test_f.py::test_a", ["smoke"]), _Item("t/test_h.py::test_a")]
    assert _ids(items) == ["t/test_e.py::test_b", "t/test_f.py::test_a", "t/test_h.py::test_a"]


def test_a_smoke_mark_on_a_data_or_slow_test_is_refused():
    items = [_Item("t/test_a.py::test_ok", ["smoke"]), _Item("t/test_a.py::test_bad", ["smoke", "needs_bundle"]),
             _Item("t/test_b.py::test_bad", ["slow", "smoke"]), _Item("t/test_b.py::test_data", ["needs_bundle"])]
    assert smoke.refused(items) == ["t/test_a.py::test_bad", "t/test_b.py::test_bad"]


@pytest.mark.smoke
def test_no_hand_list_is_back():
    """The hand lists are gone (D6): nothing may recreate them, or a script read them."""
    for name in ("smoke.txt", "smoke_pending.txt"):
        assert not (ROOT / "pipeline" / "tests" / name).exists(), f"pipeline/tests/{name} is retired (T667 D6)"
    for script in ("scripts/test-smoke.sh", "scripts/fast-fail.sh", "conftest.py", "pytest.ini"):
        text = (ROOT / script).read_text(encoding="utf-8")
        code = "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("#"))
        assert "smoke.txt" not in code and "smoke_pending" not in code, f"{script} still reads a retired smoke list"


@pytest.mark.smoke
def test_the_root_conftest_applies_the_rule():
    """Real collection, not the rule alone: `-m smoke` on this file selects exactly its two marked tests (the first test
    of the file, unmarked, is not picked because the file marks its own)."""
    me = Path(__file__).relative_to(ROOT).as_posix()
    out = subprocess.run([sys.executable, "-m", "pytest", "--collect-only", "-q", "-p", "no:cacheprovider", "-m", "smoke",
                          me], cwd=ROOT, capture_output=True, text=True)
    assert out.returncode == 0, out.stdout + out.stderr
    got = sorted(l for l in out.stdout.splitlines() if "::" in l)
    assert got == [f"{me}::test_no_hand_list_is_back", f"{me}::test_the_root_conftest_applies_the_rule"], out.stdout
