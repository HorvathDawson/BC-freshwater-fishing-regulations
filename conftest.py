"""TEST TIERS (P00, 2026-10-20). `pytest -m smoke` is Tier 1: per test file, the tests it marks `@pytest.mark.smoke`
itself, else its first test with no `needs_*` / `slow` marker. The rule is `pipeline/tests/smoke.py` (T667 D6,
2026-11-14: it replaced a hand-kept list of node ids, which needed an edit for every new or renamed test); this hook gives
the picked items the `smoke` marker (registered in `pytest.ini`), so a new test file joins the set with no edit elsewhere.

A test marked `smoke` in source that also carries `needs_*` or `slow` stops the run, naming it: the smoke tier runs
without data.

THE TIER DECIDES THE WORLD (T667 slice 7, `pipeline/tests/tiers.py`): a run whose marker expression leaves out every
`slow` and `needs_*` test (CI's tier, `-m smoke`) hides this machine's builds (`pipeline.tests.ci_no_build`), whatever
started it; the header says which tier the run is.

A PLAIN `pytest` IS THE NO-DATA TIER, LOUDLY (T1193, owner 2026-11-26 "yes with loud default"): with no `-m`, the run
deselects what CI's "Tests" step deselects (`tiers.ci_markers`, read from the workflow: its one home), so a fresh clone
is green, and its last line names how many data tests it left out and the flag that adds them: `--with-data`
(`-m "not slow"`, the default before T1193). They are deselected and counted, never skipped (test_no_silent_skips). An
explicit `-m` (every script and CI pass one) is left exactly as given; `-m` with `--with-data` is a usage error.

TESTS NEVER READ MACHINE STATE (T777, `pipeline/tests/machine_state.py`, registered below for every run): the curated
tree a run sees is a session sandbox (the tracked files, review empty), a no-data test that touches the real curated
tree or an untracked generated path fails naming the marker to add, and the run's temp files go in a folder of its own.
"""
from __future__ import annotations

import pytest

from pipeline.tests import smoke, tiers

pytest_plugins = ("pipeline.tests.machine_state",)

_NO_DATA: list[bool] = []
#: (T1193) the no-data expression this run applies itself (no `-m` given), else "" (the run's `-m` decides)
_DEFAULT: list[str] = []
#: (T1193) what the default left out: [tests with a needs_* marker, slow-only tests]
_LEFT_OUT: list[int] = [0, 0]


def pytest_addoption(parser):
    parser.addoption("--with-data", action="store_true",
                     help="(T1193) also run the tests that need built data (needs_*): -m \"not slow\". Without it and "
                          "with no -m, a plain run is CI's no-data tier and says how many it left out")


def pytest_configure(config):
    given = (config.option.markexpr or "").strip()
    with_data = config.getoption("--with-data")
    if given and with_data:
        raise pytest.UsageError(f"--with-data and -m {given!r} together: -m picks the tests by itself, drop one")
    if with_data:
        config.option.markexpr = tiers.WITH_DATA
    _DEFAULT[:] = ["" if given or with_data else tiers.ci_markers()]
    _LEFT_OUT[:] = [0, 0]
    no_data = tiers.is_no_data(_DEFAULT[0] or config.option.markexpr or "", tiers.data_markers())
    _NO_DATA[:] = [no_data]
    if no_data:
        import pipeline.tests.ci_no_build  # noqa: F401  (hides the builds on import, before any test module loads)


def pytest_report_header(config):
    return tiers.HIDDEN if _NO_DATA and _NO_DATA[0] else tiers.VISIBLE


def pytest_collection_modifyitems(session, config, items):
    bad = smoke.refused(items)
    if bad:
        pytest.exit(f"{len(bad)} test(s) marked smoke also need data or are slow (the smoke tier runs without data): "
                    + "; ".join(bad[:10]), returncode=pytest.ExitCode.USAGE_ERROR)
    for item in smoke.pick(items):
        if not smoke.marked_smoke(item):
            item.add_marker(pytest.mark.smoke)


@pytest.hookimpl(trylast=True, specname="pytest_collection_modifyitems")
def pytest_collection_modifyitems_no_data_default(session, config, items):
    """(T1193) no `-m` given: deselect by CI's no-data expression here (after `-k`, so only what the tier left out is
    counted), through pytest's own `pytest_deselected` (the summary's "N deselected": never a skip)."""
    if not _DEFAULT or not _DEFAULT[0]:
        return
    from _pytest.mark.expression import Expression
    expr, data = Expression.compile(_DEFAULT[0]), set(tiers.data_markers()) - {"slow"}
    keep, out = [], []
    for item in items:
        names = {m.name for m in item.iter_markers()}
        (keep if expr.evaluate(lambda name, /, **kw: name in names) else out).append(item)
    if out:
        n_data = sum(1 for i in out if data & {m.name for m in i.iter_markers()})
        _LEFT_OUT[0] += n_data
        _LEFT_OUT[1] += len(out) - n_data
        config.hook.pytest_deselected(items=out)
        items[:] = keep


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """(T1193) the ONE loud line of a plain run: what the no-data default left out and the flag that adds it."""
    if not _DEFAULT or not _DEFAULT[0]:
        return
    if config.pluginmanager.has_plugin("dsession"):          # xdist: the workers collect, so the count is theirs
        terminalreporter.write_sep("!", "data tests NOT RUN (the no-data tier; not counted under -n): add --with-data",
                                   red=True, bold=True)
        return
    data, slow = _LEFT_OUT
    if data or slow:
        tests = "test" if data == 1 else "tests"
        terminalreporter.write_sep("!", f"{data} data {tests} NOT RUN (they need built data): add --with-data"
                                   + (f" (and {slow} slow: -m slow)" if slow else ""), red=True, bold=True)
