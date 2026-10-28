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


# ---- P06 (owner 2026-10-20, D31): Feb 29 READS Feb 28's regulations, on its OWN real weekday ------------------
# One rule, `calendar_spec.reading_day` + `weekday_of`, generated for the app (calendar.generated.ts) and the reference
# page (app/fixtures/reference/calendar.generated.js); the Python readers (status index, tap) ask through `calendar.reading_day_of`.

import datetime as _dt
import json as _json

from pipeline.common import calendar_spec as SPEC
from pipeline.deliver import calendar as CAL

ZYMOETZ, ZYMOETZ_R10 = "gnis:3137", "r6:zymoetz_copper_river@6-9::zymoetz_river.r10"


def test_feb_29_reads_feb_28_on_its_own_weekday():
    assert SPEC.reading_day(2, 29) == SPEC.reading_day(2, 28) == 59
    assert all(SPEC.reading_day(*CAL.month_day(d)) == d for d in range(1, 367) if d != 60)
    assert SPEC.weekday_of(2036, 2, 29) == "Friday" and SPEC.weekday_of(2036, 2, 28) == "Thursday"
    assert {c["date"]: (c["reading_day"], c["weekday"]) for c in SPEC.cases()}["2036-02-29"] == (59, "Friday")
    assert CAL.reading_day_of(_dt.date(2036, 2, 29)) == 59 and CAL.day_of(_dt.date(2036, 2, 29)) == 60


@pytest.mark.needs_bundle
def test_no_stored_year_reads_differently_on_day_60(request):
    """The data invariant that keeps a day-60 reader honest: no answers segment starts at day 60, and every status
    profile holds the same code on day 60 as on day 59."""
    from pipeline.common.curated import GENERATED
    from pipeline.deliver import status_index as SI
    from pipeline.deliver.export.contract import OUT
    from pipeline.tests.conftest import EXPORT_HINT
    answers = need(request, "bundle", OUT.parent / "ui-rules-answers.json", EXPORT_HINT)
    index = need(request, "bundle", GENERATED.bundle / "status_index.bin", BUNDLE_HINT)
    wire = _json.loads(answers.read_bytes())
    assert not [i for i, seg in enumerate(wire["segments"]) if 60 in seg]
    ix = SI.Index(index.read_bytes())
    assert not [i for i, p in enumerate(ix.profiles) if p[59] != p[58]]


@pytest.mark.needs_bundle
def test_the_zymoetz_on_friday_2036_02_29_reads_feb_28_and_the_friday_rule(request):
    """Zymoetz River B (`zymoetz_river.r10`, "Fridays, Saturdays, and Sundays Jul 24 – May 31"): on Fri 2036-02-29 the
    tap reads Feb 28's day on a Friday, so the rule speaks; on Feb 28's own Thursday it does not, and a Thursday Feb 29
    reads exactly Feb 28."""
    from pipeline.deliver.answers import common as AC, encode as E
    from pipeline.deliver.export.contract import OUT
    from pipeline.tests.conftest import EXPORT_HINT
    exp = need(request, "bundle", OUT.parent / "ui-rules-answers.json", EXPORT_HINT).parent
    wire = _json.loads((exp / "ui-rules-answers.json").read_bytes())
    data, _guide = AC.load_export(exp)
    wd = SPEC.weekday_of(2036, 2, 29)
    fri = E.tap(wire, data, ZYMOETZ, 0, 2, 29, weekday=wd)["ladder"]
    thu = E.tap(wire, data, ZYMOETZ, 0, 2, 28, weekday=SPEC.weekday_of(2036, 2, 28))["ladder"]
    assert wd == "Friday"
    speaks = lambda lad: {r for f in lad for o in lad[f] for r, v in lad[f][o].items() if v[0] == "speaks"}  # noqa
    assert ZYMOETZ_R10 in speaks(fri) and ZYMOETZ_R10 not in speaks(thu)
    assert E.tap(wire, data, ZYMOETZ, 0, 2, 29, weekday="Thursday")["ladder"] == thu


def test_held_days_is_the_one_no_dates_means_the_year():
    """T125 item 8 (answers 11): a `when`'s dates as days, every day when it prints none — `catalogue.held_days`; the
    bundle's dated lift (`rules._when_term`) and the licensing overlap check each spelled the year themselves. MUTATION: either re-inlined
    (a Jan 1-Dec 31 `DateRange`, a `range(1, 367)`) fails the source pin."""
    import inspect
    from pipeline.deliver.bundle import licensing as BL, rules as BR
    w = C.When.model_validate({"dates": [{"from_month": 2, "from_day": 28, "to_month": 3, "to_day": 1}]})
    assert sorted(C.held_days(w)) == [59, 60, 61]                    # Feb 28, Feb 29, Mar 1
    assert C.held_days(None) == C.held_days(C.When.model_validate({"weekdays": ["Saturday"]})) == C.YEAR_DAYS
    assert len(C.YEAR_DAYS) == 366
    for src in (inspect.getsource(BR._when_term),
                inspect.getsource(BL)):
        assert "range(1, 367)" not in src and "to_month=12, to_day=31" not in src
