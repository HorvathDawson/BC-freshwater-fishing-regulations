"""TEST TIERS (P00, 2026-10-20). `pytest -m smoke` is Tier 1: the ids listed in `pipeline/tests/smoke.txt`, one fast
no-data test per test file. This hook gives them the `smoke` marker (registered in `pytest.ini`), so the list lives in one
place rather than as decorators spread across 185 files.

A whole-tree smoke run (no path arguments) REFUSES a listed id that was not collected, naming it. Without that check, a
renamed test would quietly shrink the smoke set.
"""
from __future__ import annotations

from pathlib import Path

import pytest

SMOKE_LIST = Path(__file__).parent / "pipeline" / "tests" / "smoke.txt"


def smoke_ids() -> list[str]:
    lines = SMOKE_LIST.read_text().splitlines()
    return [ln.strip() for ln in lines if ln.strip() and not ln.lstrip().startswith("#")]


def pytest_collection_modifyitems(session, config, items):
    wanted = set(smoke_ids())
    seen = set()
    for item in items:
        if item.nodeid in wanted:
            item.add_marker(pytest.mark.smoke)
            seen.add(item.nodeid)
    whole_tree = not config.args or [Path(a).resolve() for a in config.args] == [
        (config.rootpath / p).resolve() for p in config.getini("testpaths")]
    if config.option.markexpr == "smoke" and whole_tree and wanted - seen:
        stale = sorted(wanted - seen)
        pytest.exit(f"{len(stale)} smoke id(s) in {SMOKE_LIST.name} were not collected (renamed or removed?): "
                    + "; ".join(stale[:10]), returncode=pytest.ExitCode.USAGE_ERROR)
