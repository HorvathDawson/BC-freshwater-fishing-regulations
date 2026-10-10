"""THE CALENDAR SPEC (`pipeline/common/calendar_spec.py`) and its generated copies.

The spec is the one source; `python -m pipeline.tools.emit_calendar` writes the app's TypeScript /
JSON and the reference page's JS from it, and `--check` (CI) fails when a copy is stale or was
edited by hand. Gate 4 in `test_dataflow_gates.py` refuses a hand-written table anywhere else.
"""
from __future__ import annotations

import datetime as dt
import json

import pytest

from pipeline.common import calendar_spec as S
from pipeline.deliver import calendar as CAL
from pipeline.regs.parsing import catalogue as C
from pipeline.tools import emit_calendar as EMIT


def test_the_leap_calendar():
    assert S.DAYS == 366 and S.LEAP_DAY == 60 == S.day_index(2, 29)
    assert S.day_index(1, 1) == 1 and S.day_index(3, 1) == 61 and S.day_index(12, 31) == 366
    assert S.DAYS_BEFORE == (0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335)
    assert sum(S.COMMON_YEAR_LAST_DAY) == 365 and S.COMMON_YEAR_LAST_DAY[1] == 28
    assert [S.month_day(S.day_index(m, d)) for m in range(1, 13)
            for d in range(1, S.LAST_DAY[m - 1] + 1)] == \
        [(m, d) for m in range(1, 13) for d in range(1, S.LAST_DAY[m - 1] + 1)]
    for bad in (0, 367):
        with pytest.raises(ValueError):
            S.month_day(bad)
    with pytest.raises(ValueError):
        S.day_index(13, 1)


def test_every_python_owner_reads_the_spec():
    """The catalogue's and the delivery's calendars are the spec's, day for day."""
    days = [(m, d) for m in range(1, 13) for d in range(1, S.LAST_DAY[m - 1] + 1)]
    assert [C._day_index(m, d) for m, d in days] == [S.day_index(m, d) for m, d in days] \
        == [CAL.day_of((m, d)) for m, d in days] == list(range(1, 367))
    assert [C._md(i) for i in range(1, 367)] == [CAL.month_day(i) for i in range(1, 367)] == days
    assert CAL.day_of(dt.date(2025, 3, 1)) == CAL.day_of(dt.date(2024, 3, 1)) == 61
    assert CAL.WEEKDAYS == S.WEEKDAYS and CAL.MONTHS == S.MONTHS and CAL.LEAP == S.LEAP_DAY
    assert C.DateRange(from_month=11, from_day=1, to_month=4, to_day=30).words() == "Nov 1-Apr 30"


def test_the_generated_copies_are_current():
    """What `emit_calendar --check` runs in CI."""
    assert EMIT.stale() == []
    assert EMIT.main(["--check"]) == 0
    got = json.loads(EMIT.OUT_JSON.read_text())
    assert {k: v for k, v in got.items() if not k.startswith("_")} == S.as_json()


@pytest.mark.parametrize("which,edit", [
    ("OUT_TS", lambda t: t.replace("[0, 31, 60, 91,", "[0, 31, 59, 90,")),
    ("OUT_JSON", lambda t: t.replace('"leap_day": 60', '"leap_day": 59')),
    ("OUT_PAGE_JS", lambda t: t.replace('"Monday"', '"Mon"')),
    ("OUT_TS", lambda t: t + "\n// a hand note\n"),
])
def test_a_hand_edit_to_a_generated_copy_fails_the_check(tmp_path, monkeypatch, which, edit):
    for name in ("OUT_TS", "OUT_JSON", "OUT_PAGE_JS"):
        src = getattr(EMIT, name)
        dst = tmp_path / src.name
        dst.write_text(src.read_text())
        monkeypatch.setattr(EMIT, name, dst)
    monkeypatch.setattr(EMIT, "ROOT", tmp_path)
    assert EMIT.main(["--check"]) == 0
    p = getattr(EMIT, which)
    edited = edit(p.read_text())
    assert edited != p.read_text()
    p.write_text(edited)
    assert [q for q, _ in EMIT.stale()] == [p]
    assert EMIT.main(["--check"]) == 1
    assert p.read_text() == edited                    # --check never rewrites
    assert EMIT.main([]) == 0 and EMIT.main(["--check"]) == 0
