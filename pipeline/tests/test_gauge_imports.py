"""THE MATCHER IS REACHED ONLY BY HAND (audit F05, `pipeline/gauges/__init__.py`).

`pipeline.gauges.generate` needs the atlas's pickled geometry and runs a few times a year; every
build-time consumer reads the frozen `matches.json` through `pipeline.gauges.matches` and places it
with `pipeline.gauges.consume.place`. The bundle used to import `nodes_for` / `summarise` from the
matcher, which made "the build must not re-run matching" a convention rather than a fact about the
import graph. This pins the fact: no module outside `generate/` imports it.

No data: an AST walk over the repo's own Python.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GENERATE = "pipeline.gauges.generate"


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    out: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out |= {a.name for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            out.add(node.module)
            out |= {f"{node.module}.{a.name}" for a in node.names}
    return out


def _offenders() -> list[str]:
    bad = []
    for root in ("pipeline", "curation-review", "scripts", "data"):
        for p in sorted((ROOT / root).rglob("*.py")):
            rel = p.relative_to(ROOT)
            if "tests" in rel.parts or "__pycache__" in rel.parts or ".venv" in rel.parts:
                continue
            if rel.parts[:3] == ("pipeline", "gauges", "generate"):
                continue
            if any(i == GENERATE or i.startswith(GENERATE + ".") for i in _imports(p)):
                bad.append(str(rel))
    return bad


def test_nothing_outside_the_matcher_imports_it():
    assert _offenders() == []


def test_the_walk_would_see_an_import_of_the_matcher(tmp_path, monkeypatch):
    """Mutation pin: the walk is not vacuous — a consumer importing the matcher is caught."""
    import pipeline.tests.test_gauge_imports as me
    fake = tmp_path / "pipeline" / "deliver"
    fake.mkdir(parents=True)
    (fake / "x.py").write_text("from pipeline.gauges.generate.match import nodes_for\n")
    monkeypatch.setattr(me, "ROOT", tmp_path)
    assert me._offenders() == ["pipeline/deliver/x.py"]


def test_the_placement_lives_with_the_consumers():
    from pipeline.gauges.consume import place
    assert callable(place.nodes_for) and callable(place.summarise)
