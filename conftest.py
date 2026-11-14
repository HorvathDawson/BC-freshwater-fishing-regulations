"""TEST TIERS (P00, 2026-10-20). `pytest -m smoke` is Tier 1: per test file, the tests it marks `@pytest.mark.smoke`
itself, else its first test with no `needs_*` / `slow` marker. The rule is `pipeline/tests/smoke.py` (T667 D6,
2026-11-14: it replaced a hand-kept list of node ids, which needed an edit for every new or renamed test); this hook gives
the picked items the `smoke` marker (registered in `pytest.ini`), so a new test file joins the set with no edit elsewhere.

A test marked `smoke` in source that also carries `needs_*` or `slow` stops the run, naming it: the smoke tier runs
without data.
"""
from __future__ import annotations

import pytest

from pipeline.tests import smoke


def pytest_collection_modifyitems(session, config, items):
    bad = smoke.refused(items)
    if bad:
        pytest.exit(f"{len(bad)} test(s) marked smoke also need data or are slow (the smoke tier runs without data): "
                    + "; ".join(bad[:10]), returncode=pytest.ExitCode.USAGE_ERROR)
    for item in smoke.pick(items):
        if not smoke.marked_smoke(item):
            item.add_marker(pytest.mark.smoke)
