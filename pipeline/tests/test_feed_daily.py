"""The feed's own daily record: this calendar year, plus last December while it is January (S1 lead-up)."""

from __future__ import annotations

from pipeline.gauges.feed.publish import merge_daily

PREV = [["2026-12-27", 1.0, 10.0], ["2026-11-30", 1.1, 11.0], ["2027-01-01", 1.2, 12.0]]


def test_january_keeps_last_december_and_nothing_older():
    got = merge_daily(PREV, {"2026-12-31": (1.3, 13.0), "2027-01-02": (1.4, 14.0)}, 2027, 1)
    assert [r[0] for r in got] == ["2026-12-27", "2026-12-31", "2027-01-01", "2027-01-02"]


def test_february_drops_last_year_entirely():
    got = merge_daily(PREV, {"2026-12-31": (1.3, 13.0)}, 2027, 2)
    assert [r[0] for r in got] == ["2027-01-01"]


def test_the_freshest_reading_of_a_day_wins():
    got = merge_daily([["2027-03-01", 1.0, 10.0]], {"2027-03-01": (2.0, 20.0)}, 2027, 3)
    assert got == [["2027-03-01", 2.0, 20.0]]
