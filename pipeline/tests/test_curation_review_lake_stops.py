"""T1177 review round 2: the review app's lake stops (`reuse._lake_stops`) are kept per what decides them
(`build.lake_row_stop_inputs`: the rows' rules with carve-outs AND their `matched`), and dropped with the build
(`invalidate_caches`), never keyed on object ids CPython may reuse after a promote."""
from __future__ import annotations

import copy
import sys
from pathlib import Path

import pytest

from pipeline.tests.test_t1177_tributary_walk import UPPER, chain  # noqa: F401  (the fixture)

BACKEND = Path(__file__).resolve().parents[2] / "tools" / "curation-review" / "backend"


@pytest.fixture()
def reuse(monkeypatch):
    monkeypatch.syspath_prepend(str(BACKEND))
    sys.modules.pop("reuse", None)
    import reuse as R
    R._lake_stops_for.cache_clear()
    yield R
    R._lake_stops_for.cache_clear()
    sys.modules.pop("reuse", None)


def test_an_edited_matched_changes_the_stops_without_a_restart(reuse, monkeypatch, chain):  # noqa: F811
    g, reg = chain
    row = copy.deepcopy(UPPER)
    monkeypatch.setattr(reuse, "_registry", lambda: reg)
    monkeypatch.setattr(reuse, "_graph", lambda: g)
    monkeypatch.setattr(reuse, "_all_entries", lambda: [("4", row)])
    [st] = reuse._lake_stops()
    assert st.lakes == {"lake:UL"}
    assert reuse._lake_stops() is reuse._lake_stops(), "kept while nothing that decides it changed"
    row["matched"] = ["wbk:LL"]                       # the row re-matched to the lower lake: its stop moves with it
    [st] = reuse._lake_stops()
    assert st.lakes == {"lake:LL"}
    row["rules"][0]["tributary_excludes"] = [{"op": "whole", "item_id": "gnis:D"}]   # an edited carve-out
    [st] = reuse._lake_stops()
    assert st.excluded == {"D:0"}


def test_the_stops_go_with_the_build(reuse):
    assert reuse._lake_stops_for in reuse._BUILD_CACHES
