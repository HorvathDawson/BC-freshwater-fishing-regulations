"""A RANGE PRINTED TO FEB 28 RUNS THROUGH FEB 29 (ruling 2026-10-06).

The 2025-2027 synopsis prints its dates for years without a Feb 29; "Jan 1-Feb 28" means through
the end of February. One source: `catalogue.range_days`, which `_days`, `complement` and
`read.in_force` read. A non-leap year never asks day 60, so it is unchanged; a range starting Mar 1
is untouched.
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.bundle import read as R
from pipeline.regs.parsing import catalogue as C
from pipeline.tests.conftest import need, BUNDLE_HINT

FEB29 = C._day_index(2, 29)


def _r(fm, fd, tm, td):
    return C.DateRange(from_month=fm, from_day=fd, to_month=tm, to_day=td)


def test_a_range_to_feb_28_holds_feb_29():
    assert FEB29 in C.range_days(_r(1, 1, 2, 28))
    assert FEB29 in C.range_days(_r(11, 1, 2, 28))                    # across New Year
    when = {"dates": [{"from_month": 1, "from_day": 1, "to_month": 2, "to_day": 28}]}
    assert R.in_force(when, (2, 29)) == "yes"
    # mutation: the day before and after read as they always did
    assert R.in_force(when, (2, 28)) == "yes" and R.in_force(when, (3, 1)) == "no"


def test_a_range_from_mar_1_and_other_ends_are_untouched():
    assert FEB29 not in C.range_days(_r(3, 1, 4, 30))
    assert C.range_days(_r(3, 1, 4, 30))[0] == C._day_index(3, 1)
    assert FEB29 not in C.range_days(_r(2, 1, 2, 27))
    when = {"dates": [{"from_month": 3, "from_day": 1, "to_month": 9, "to_day": 30}]}
    assert R.in_force(when, (2, 29)) == "no"


def test_a_non_leap_year_is_unchanged():
    """Every day but Feb 29 reads exactly the printed range."""
    for r in (_r(1, 1, 2, 28), _r(11, 1, 2, 28), _r(3, 1, 4, 30), _r(12, 15, 1, 31)):
        a, b = C._day_index(r.from_month, r.from_day), C._day_index(r.to_month, r.to_day)
        printed = set(range(a, b + 1)) if a <= b else set(range(a, 367)) | set(range(1, b + 1))
        assert set(C.range_days(r)) - {FEB29} == printed - {FEB29}


def test_complement_of_a_range_to_feb_28_starts_mar_1():
    got = C.complement([_r(1, 1, 2, 28)])
    assert [(d.from_month, d.from_day, d.to_month, d.to_day) for d in got] == [(3, 1, 12, 31)]


BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE)


@pytest.mark.needs_bundle
def test_the_nicola_below_the_lake_is_catch_and_release_on_feb_29(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    try:
        sid = db.execute("SELECT MIN(s.sid) FROM section_ruleset s JOIN ruleset r ON "
                         "r.set_id = s.set_id WHERE r.entry_id = 'r3:nicola_river@3-13' AND "
                         "r.rule_id = 'nicola_river.r3'").fetchone()[0]
    finally:
        db.close()
    if sid is None:
        pytest.fail("pinned rule r3:nicola_river@3-13::nicola_river.r3 binds nothing in this bundle")
    speaks = lambda md: {R.rid(x) for x in R.effective_rules(sid, md, "RB", str(BUNDLE))  # noqa
                         if x["state"] == "speaks"}
    feb29 = speaks((2, 29))
    assert "r3:nicola_river@3-13::nicola_river.r3" in feb29
    assert "z3:spring_stream_closure::spring_stream_closure.r1" not in feb29
    assert feb29 == speaks((2, 28))
    assert "z3:spring_stream_closure::spring_stream_closure.r1" in speaks((3, 1))
