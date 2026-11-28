"""(T1258) The In-season tab's row, without data: the backend's call of `matches.span` against small rows.

T1167 made `span` give `(from, to, dated)`; the backend still unpacked two, and every queue raised `ValueError: too many
values to unpack` — on main for days, because the only tests of that call are the data tier's
(`test_curation_review_inseason.py`, CI never runs them). This file runs in the plain tier: the row view on a fixture
row, no atlas, no feed file. `dated` is not dropped: it is the row's `dates.dated`, and `banner` is the app's own word
for today (`matches.on`, core's `inSeasonOn`: "upcoming" in the 14 days before a dated row).
Mutation: unpack two values again, or drop `dated` / `banner` from `_row_view` -> fails.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "tools" / "curation-review" / "backend"
_MODS = ("reuse", "writes", "inseason")


@pytest.fixture
def ins(monkeypatch):
    monkeypatch.syspath_prepend(str(BACKEND))
    for mod in _MODS:
        sys.modules.pop(mod, None)
    import inseason
    import reuse
    monkeypatch.setattr(reuse, "_registry", lambda: {})       # a curated match's water: no atlas here
    yield inseason
    for mod in _MODS:
        sys.modules.pop(mod, None)


def _row(frm, to):
    return {"id": "is-aaaaaaaaaaa1", "region": "1", "water": "Cowichan River", "effective": f"{frm} to {to}",
            "from": frm, "to": to, "change": "No fishing", "parsed": {"kinds": ["closure"]}, "status": "new"}


def _view(ins, row, today, m=None):
    return ins._row_view("is-aaaaaaaaaaa1", row, m, [], today, {})


def test_a_dated_row_is_upcoming_in_the_lead_days(ins):
    v = _view(ins, _row("2026-12-10", "2026-12-31"), "2026-12-01")
    assert v["dates"] == {"from": "2026-12-10", "to": "2026-12-31", "source": "feed", "dated": True}
    assert (v["in_effect"], v["banner"]) == (False, "upcoming")
    assert _view(ins, _row("2026-12-10", "2026-12-31"), "2026-12-15")["banner"] == "in force"
    assert _view(ins, _row("2026-12-10", "2026-12-31"), "2027-01-01")["banner"] is None


def test_an_undated_row_is_never_upcoming(ins):
    v = _view(ins, _row("2026-12-10", None), "2026-12-01")
    assert v["dates"]["dated"] is False and (v["in_effect"], v["banner"]) == (False, None)
    assert _view(ins, _row("2026-12-10", None), "2026-12-11")["banner"] == "in force"


def test_the_curated_dates_win_and_are_read_the_same_way(ins):
    from pipeline.regs.inseason import matches as M
    m = M.Match(region="1", water="Cowichan River", extents=[{"op": "whole", "item_id": "gnis:12227"}],
                effective=M.Effective.model_validate({"to": "2099-12-31"}))
    v = _view(ins, _row("2025-07-04", "2025-08-31"), "2026-01-01", m)
    assert v["dates"] == {"from": "2025-07-04", "to": "2099-12-31", "source": "curated", "dated": True}
    assert (v["state"], v["in_effect"], v["banner"]) == ("matched", True, "in force")


def test_a_stale_match_with_no_row_has_no_dates(ins):
    from pipeline.regs.inseason import matches as M
    m = M.Match(region="1", water="Gone Creek", extents=[{"op": "whole", "item_id": "gnis:12227"}])
    v = ins._row_view("is-aaaaaaaaaaa9", None, m, [], "2026-01-01", {})
    assert v["state"] == "stale" and v["dates"]["dated"] is False and v["in_effect"] is None
    assert v["banner"] is None                                  # the app's feed no longer holds it: no banner
    gone = {**_row("2026-12-10", "2026-12-31"), "removed": True, "status": "removed"}
    assert _view(ins, gone, "2026-12-15")["banner"] is None
