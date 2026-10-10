"""NO SILENT SKIPS (P2, 2026-10-09; user decision: markers, and missing data ALWAYS fails).

A skipped test proves nothing and reads as green. Data a test needs is declared with a
`needs_<kind>` marker and fetched through `conftest.need`, which FAILS naming the make-command;
the no-data tier (CI) deselects by marker. The only skips left are genuine optional tools and
deliberate opt-ins, each listed below with its reason and its exact count — a new skip anywhere,
or one more in a listed file, fails here; so does a listed skip that is gone (stale allowlist).
"""
from __future__ import annotations

import ast
import configparser
import re
from pathlib import Path

import pytest

from pipeline.tests import conftest as C

ROOT = Path(__file__).resolve().parents[2]
TREES = (ROOT / "pipeline" / "tests", ROOT / "pipeline" / "atlas" / "waters" / "added_streams" / "tests")

#: file (repo-relative) -> (exact number of skip sites, why each is a genuine opt-in / optional tool)
ALLOWLIST: dict[str, tuple[int, str]] = {
    "pipeline/tests/conftest.py": (
        1, "predates(): UI_EXPORT_ALLOW_PREDATING=1, the user's deliberate switch for a run "
           "against an old side build (fails by default)"),
    "pipeline/tests/test_stocking_match.py": (1, "optional tool: geopandas (importorskip)"),
    "pipeline/tests/test_mu_regions.py": (1, "optional tool: geopandas (importorskip)"),
    "pipeline/tests/test_no_slivers.py": (
        1, "opt-in: ATLAS_BUILD_TWIN names a second build to compare for determinism"),
    "pipeline/tests/test_one_water_kind.py": (
        3, "opt-in: measuring the merge needs a second, pre-merge shipped bundle of the same atlas"),
    "pipeline/tests/test_answers_reference.py": (
        3, "opt-in: ANSWERS_GOLDEN (+ ANSWERS_PORT_BUNDLE/EXPORT) golden outputs of the page"),
    "pipeline/tests/test_export_codec.py": (
        1, "opt-in: UI_EXPORT_BUNDLE points at a side bundle, whose export is not the shipped one"),
}

_SKIP_CALLS = {"skip", "importorskip", "xfail", "skipTest"}
_SKIP_MARKS = {"skip", "skipif", "xfail"}
_SKIP_NAMES = {"skip", "skipif", "importorskip", "xfail", "SkipTest", "skipIf", "skipUnless",
               "expectedFailure"}


def skip_sites(source: str) -> list[tuple[int, str]]:
    """Every way this repo's tests could skip: `pytest.skip(`, `pytest.importorskip(`,
    `pytest.xfail(`, `pytest.mark.skip/skipif/xfail`, `self.skipTest(`, `unittest.skip*`,
    `SkipTest`, and those names imported from pytest/unittest. `pytest.skip.Exception` (what a
    test asserts is raised) is not a site."""
    tree = ast.parse(source)
    out: list[tuple[int, str]] = []
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr in _SKIP_CALLS:
            out.append((n.lineno, ast.unparse(n.func)))
        elif isinstance(n, ast.Attribute) and n.attr in _SKIP_MARKS and isinstance(n.value, ast.Attribute) \
                and n.value.attr == "mark":
            out.append((n.lineno, ast.unparse(n)))
        elif isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) and n.value.id == "unittest" \
                and n.attr in _SKIP_NAMES:
            out.append((n.lineno, ast.unparse(n)))
        elif isinstance(n, ast.Name) and n.id == "SkipTest":
            out.append((n.lineno, n.id))
        elif isinstance(n, ast.ImportFrom) and (n.module or "").split(".")[0] in ("pytest", "unittest", "_pytest"):
            for a in n.names:
                if a.name in _SKIP_NAMES or a.name == "*":
                    out.append((n.lineno, f"from {n.module} import {a.name}"))
    return sorted(out)


def _files() -> list[Path]:
    return sorted(p for t in TREES for p in t.rglob("*.py") if "__pycache__" not in p.parts)


def test_no_skip_outside_the_allowlist():
    found: dict[str, list[tuple[int, str]]] = {}
    for p in _files():
        sites = skip_sites(p.read_text(encoding="utf-8"))
        if sites:
            found[p.relative_to(ROOT).as_posix()] = sites
    bad = [f"{f}:{ln} {what}" for f, sites in found.items() if f not in ALLOWLIST for ln, what in sites]
    assert not bad, ("a test may not skip — declare the data with a needs_* marker and fetch it "
                     "with conftest.need (it FAILS naming the make-command):\n  " + "\n  ".join(bad))
    wrong = {f: (len(found.get(f, [])), n) for f, (n, _) in ALLOWLIST.items() if len(found.get(f, [])) != n}
    assert not wrong, f"allowlisted skip counts changed (found, allowed): {wrong}"


def test_every_allowlisted_skip_says_why():
    assert all(len(why) > 20 for _, why in ALLOWLIST.values())


@pytest.mark.parametrize("planted", [
    'import pytest\n\ndef test_x():\n    pytest.skip("no data")\n',
    'import pytest\n\n@pytest.mark.skipif(True, reason="no data")\ndef test_x():\n    pass\n',
    'import pytest\n\npytestmark = pytest.mark.skip(reason="no data")\n',
    'import pytest\n\ngp = pytest.importorskip("geopandas")\n',
    'import pytest\n\ndef test_x():\n    pytest.xfail("no data")\n',
    'import unittest\n\n@unittest.skip("no data")\nclass T(unittest.TestCase):\n    pass\n',
    'import unittest\n\nclass T(unittest.TestCase):\n    def test_x(self):\n        self.skipTest("no data")\n',
    'from pytest import skip\n\ndef test_x():\n    skip("no data")\n',
    'from unittest import SkipTest\n\ndef test_x():\n    raise SkipTest("no data")\n',
])
def test_mutation_the_scanner_catches_a_planted_skip(tmp_path, planted):
    """MUTATION: plant each form of skip in a file and the scanner must see it."""
    p = tmp_path / "test_planted.py"
    p.write_text(planted, encoding="utf-8")
    assert skip_sites(p.read_text(encoding="utf-8"))


def test_the_scanner_ignores_what_is_not_a_skip():
    src = ('import pytest\n\ndef test_x():\n    """we never skip"""\n'
           '    with pytest.raises(pytest.skip.Exception):\n        pass\n    pytest.fail("x")\n')
    assert skip_sites(src) == []


# --------------------------------------------------------------------------- need() and the markers
class _Node:
    """A pytest node carrying exactly the markers given."""
    nodeid = "planted::test"

    def __init__(self, *markers):
        self.markers = set(markers)

    def get_closest_marker(self, name):
        return name if name in self.markers else None


def test_need_fails_when_the_marker_is_absent(tmp_path):
    """MUTATION: the data is there, but the test does not declare it — the marker would drift, so
    `need` fails rather than reading silently."""
    with pytest.raises(pytest.fail.Exception, match="without the `needs_bundle` marker"):
        C.need(_Node(), "bundle", tmp_path, "make it")
    with pytest.raises(pytest.fail.Exception, match="without the `needs_atlas` marker"):
        C.need(_Node("needs_bundle"), "atlas", tmp_path, "make it")


def test_need_fails_on_this_tests_own_request_without_the_marker(request, tmp_path):
    """The same through a real `request`: this test carries no needs_* marker."""
    with pytest.raises(pytest.fail.Exception, match="without the `needs_bundle` marker"):
        C.need(request, "bundle", tmp_path, "make it")
    with pytest.raises(pytest.fail.Exception, match="without the `needs_bundle` marker"):
        C.need(None, "bundle", tmp_path, "make it")


def test_need_fails_on_missing_data_naming_the_command(tmp_path):
    missing = tmp_path / "bundle.sqlite"
    with pytest.raises(pytest.fail.Exception,
                       match=r"missing bundle: .*bundle\.sqlite — make it with: python -m pipeline\.deliver"):
        C.need(_Node("needs_bundle"), "bundle", missing, "python -m pipeline.deliver")


def test_need_passes_and_returns_the_path_when_declared_and_present(tmp_path):
    assert C.need(_Node("needs_cache"), "cache", tmp_path, "x") == tmp_path


def test_need_refuses_an_unknown_kind(tmp_path):
    with pytest.raises(ValueError):
        C.need(_Node("needs_bundel"), "bundel", tmp_path, "x")


def test_predates_fails_unless_the_opt_in_is_set(monkeypatch):
    monkeypatch.delenv("UI_EXPORT_ALLOW_PREDATING", raising=False)
    with pytest.raises(pytest.fail.Exception):
        C.predates("old bundle")
    monkeypatch.setenv("UI_EXPORT_ALLOW_PREDATING", "1")
    with pytest.raises(pytest.skip.Exception):
        C.predates("old bundle")


# --------------------------------------------------------------------------- the tiers agree
def _ini_markers() -> set[str]:
    cp = configparser.ConfigParser()
    cp.read(ROOT / "pytest.ini")
    return {ln.split(":")[0].strip() for ln in cp["pytest"]["markers"].splitlines() if ln.strip()}


def test_every_kind_is_a_registered_marker_and_strict():
    assert {f"needs_{k}" for k in C.KINDS} <= _ini_markers()
    cp = configparser.ConfigParser()
    cp.read(ROOT / "pytest.ini")
    assert "--strict-markers" in cp["pytest"]["addopts"]


def test_ci_deselects_slow_and_every_data_kind():
    """The no-data tier must leave out every marker that needs data, and `slow` (the command line
    `-m` REPLACES the ini's `-m "not slow"`, so CI must say it again)."""
    wf = (ROOT / ".github" / "workflows" / "pipeline-ci.yml").read_text(encoding="utf-8")
    m = re.search(r"python -m pytest[^\n]*-m \"([^\"]+)\"", wf)
    assert m, "the CI Tests step does not select a tier with -m"
    sel = m.group(1)
    for name in ["slow"] + [f"needs_{k}" for k in C.KINDS]:
        assert f"not {name}" in sel, f"CI runs the {name} tests, which need what a runner lacks"
