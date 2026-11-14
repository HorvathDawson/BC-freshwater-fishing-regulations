"""TEST TIERS (P00, 2026-10-20). `pytest -m smoke` is Tier 1: per test file, the tests it marks `@pytest.mark.smoke`
itself, else its first test with no `needs_*` / `slow` marker. The rule is `pipeline/tests/smoke.py` (T667 D6,
2026-11-14: it replaced a hand-kept list of node ids, which needed an edit for every new or renamed test); this hook gives
the picked items the `smoke` marker (registered in `pytest.ini`), so a new test file joins the set with no edit elsewhere.

A test marked `smoke` in source that also carries `needs_*` or `slow` stops the run, naming it: the smoke tier runs
without data.

THE TIER DECIDES THE WORLD (T667 slice 7, `pipeline/tests/tiers.py`): a run whose marker expression leaves out every
`slow` and `needs_*` test (CI's tier, `-m smoke`) hides this machine's builds (`pipeline.tests.ci_no_build`), whatever
started it; the header says which tier the run is.
"""
from __future__ import annotations

import pytest

from pipeline.tests import smoke, tiers

_NO_DATA: list[bool] = []


def pytest_configure(config):
    no_data = tiers.is_no_data(config.option.markexpr or "", tiers.data_markers())
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
