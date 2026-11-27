"""THE TEST-ENVIRONMENT CONTRACT (T667 slice 7, 2026-11-14): the world a test sees follows from its TIER, never from the
runner that started it.

A tier is the run's marker expression: `-m` when given; with no `-m`, CI's no-data expression (T1193: a plain `pytest`
on a fresh clone is green, and its last line says how many data tests it left out); `--with-data` instead of `-m`
is `-m "not slow"` (the default before T1193). The root `conftest.py` applies the choice.

  * NO-DATA tier: the expression deselects every `slow` and every `needs_*` test (CI's "Tests" step, `-m smoke`,
    fast-fail's "as CI" step, the fresh-copy run). Such a run ALWAYS hides this machine's builds
    (`pipeline.tests.ci_no_build`, loaded by the repo-root `conftest.py`), so an unmarked test that reads a build
    fails on the laptop exactly as on CI or on a fresh clone. Before this, only fast-fail passed
    `-p pipeline.tests.ci_no_build`; `pytest -m smoke`, the chunked runner and a bare `pytest -m "<CI markers>"` saw the
    builds, which is how two tests failed only under the plugin on build 27 (a8a1fbc12, 9f81817c8).
    A plain `pytest` (no `-m`) is this tier too (T1193).
  * DATA tier: anything else (`--with-data`, `-m slow`, `-m needs_bundle`): the builds are visible and a marked test
    reaches them through `need()` / `under_test`.

The pytest header says which (`test tier: no-data, this machine's builds hidden` / `test tier: data ...`).
The shell half of the contract (interpreter, bytecode prefix, CI's marker expression) is `scripts/test-env.sh`.
"""
from __future__ import annotations

import configparser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
#: the workflow whose "Tests" step holds the no-data tier's marker expression: its ONE home (T535)
WORKFLOW = ROOT / ".github" / "workflows" / "pipeline-ci.yml"
#: (T1193) what `--with-data` selects: every test but the slow ones, the default before plain `pytest` became no-data
WITH_DATA = "not slow"


def data_markers(ini: Path = ROOT / "pytest.ini") -> list[str]:
    """`slow` and every registered `needs_*` marker: the ones a no-data run must leave out."""
    cp = configparser.ConfigParser()
    cp.read(ini)
    names = [ln.split(":")[0].strip() for ln in cp["pytest"]["markers"].splitlines() if ln.strip()]
    return ["slow"] + sorted(n for n in names if n.startswith("needs_"))


def ci_markers(workflow: Path = WORKFLOW) -> str:
    """The no-data tier's marker expression, read from CI's "Tests" step (`-m "<expr>"`), never typed twice: a plain
    `pytest` runs it (root `conftest.py`) and `scripts/test-env.sh` `no_data_markers` prints it for the runners."""
    import re

    import yaml
    steps = [s for j in yaml.safe_load(workflow.read_text(encoding="utf-8"))["jobs"].values() for s in j["steps"]
             if s.get("name") == "Tests"]
    if len(steps) != 1:
        raise RuntimeError(f"{workflow}: {len(steps)} steps named \"Tests\", want exactly one (the no-data tier)")
    m = re.search(r'-m "([^"]+)"', steps[0]["run"])
    if not m:
        raise RuntimeError(f"{workflow}: the \"Tests\" step selects no tier with -m \"...\"")
    return m.group(1)


def is_no_data(markexpr: str, markers: list[str]) -> bool:
    """True when `markexpr` deselects a test carrying any ONE of `markers` (and nothing else)."""
    if not markexpr.strip():
        return False
    from _pytest.mark.expression import Expression
    expr = Expression.compile(markexpr)
    return not any(expr.evaluate(lambda name, /, **kw: name == m) for m in markers)


HIDDEN = "test tier: no-data, this machine's builds hidden (pipeline/tests/tiers.py)"
VISIBLE = "test tier: data, this machine's builds visible (pipeline/tests/tiers.py)"
