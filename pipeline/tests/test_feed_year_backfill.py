"""This year's record back to 1 January, from ECCC's Water Office (ticket 2026-10-21).

The fixtures are real responses recorded 2026-10-21 (pipeline/tests/fixtures/gauge_feed):
HYDAT daily means for 08MH001 + 08LF051, 1-10 January 2026, and 08MH001's 5-minute
real-time readings for 1 August 2026, Pacific standard time.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from pipeline.gauges.feed import year_backfill as yb

FIX = Path(__file__).parent / "fixtures" / "gauge_feed"
HYDAT_CSV = (FIX / "wo_daily_data_2026-01-01_10.csv").read_text(encoding="utf-8-sig")
RT_CSV = (FIX / "wo_realtime_08MH001_2026-08-01.csv").read_text(encoding="utf-8-sig")
EMPTY_RT = RT_CSV.splitlines()[0] + "\n"
EMPTY_HYDAT = HYDAT_CSV.splitlines()[0] + "\n"
PST = timedelta(hours=-8)


def _days(a: str, b: str) -> list[str]:
    d, end, out = date.fromisoformat(a), date.fromisoformat(b), []
    while d <= end:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


# ---------------------------------------------------------------- parsing

def test_hydat_daily_reads_both_quantities_per_day():
    got = yb.hydat_daily(HYDAT_CSV)
    assert sorted(got) == ["08LF051", "08MH001"]
    assert got["08MH001"]["2026-01-01"] == (1.933, 61.3)
    assert got["08LF051"] == {
        "2026-01-01": (0.945, 245.0), "2026-01-02": (0.961, 248.0), "2026-01-03": (0.993, 255.0),
        "2026-01-04": (1.022, 261.0), "2026-01-05": (1.035, 264.0), "2026-01-06": (1.064, 270.0),
        "2026-01-07": (1.078, 273.0)}


def test_realtime_daily_is_the_mean_of_the_local_standard_day():
    """288 level and 288 discharge readings, 08:00Z on the 1st to 07:55Z on the 2nd: one
    whole Pacific-standard day, every reading provisional."""
    assert yb.realtime_daily(RT_CSV, PST) == {
        "08MH001": {"2026-08-01": (1.6429, 18.2767, "provisional")}}


def test_read_in_utc_the_same_readings_straddle_two_days():
    """Pins the offset: a UTC day boundary would split the river's day in two."""
    assert sorted(yb.realtime_daily(RT_CSV, timedelta(0))["08MH001"]) == ["2026-08-01", "2026-08-02"]


def test_a_day_is_final_only_if_every_reading_is():
    head = RT_CSV.splitlines()[0]
    text = "\n".join([head,
                      "X,2026-03-01T09:00:00Z,47,10,,,Final/Finales,,",
                      "X,2026-03-01T10:00:00Z,47,20,,,Final/Finales,,",
                      "X,2026-03-02T09:00:00Z,47,10,,,Final/Finales,,",
                      "X,2026-03-02T10:00:00Z,47,30,,,Provisional/Provisoire,,",
                      "X,2026-03-03T10:00:00Z,47,99999,,,Provisional/Provisoire,,"])
    assert yb.realtime_daily(text, PST) == {"X": {
        "2026-03-01": (None, 15.0, "final"), "2026-03-02": (None, 20.0, "provisional")}}


def test_chunks_cut_each_run_to_a_month():
    days = _days("2026-01-01", "2026-03-05") + ["2026-04-01"]
    assert yb.chunks(days) == [("2026-01-01", "2026-01-31"), ("2026-02-01", "2026-03-03"),
                               ("2026-03-04", "2026-03-05"), ("2026-04-01", "2026-04-01")]


def test_the_realtime_request_asks_for_whole_local_days():
    url = yb._realtime_url("08MH001", "2026-08-01", "2026-08-01", PST)
    assert url == ("https://wateroffice.ec.gc.ca/services/real_time_data/csv/inline?"
                   "stations%5B%5D=08MH001&parameters%5B%5D=46&parameters%5B%5D=47"
                   "&start_date=2026-08-01+08%3A00%3A00&end_date=2026-08-02+07%3A59%3A59")


# ---------------------------------------------------------------- politeness

def test_a_bare_422_is_retried_with_backoff_and_requests_are_spaced():
    answers = iter([(422, None), (200, "ok"), (200, "two")])
    slept: list[float] = []
    net = yb.Polite(fetch=lambda url: next(answers), sleep=slept.append)
    assert (net.get("a"), net.get("b")) == ("ok", "two")
    assert slept == [10.0, 2.0]
    assert (net.requests, net.failed) == (3, [])


def test_a_request_that_never_answers_is_given_up_and_counted():
    slept: list[float] = []
    net = yb.Polite(fetch=lambda url: (422, None), sleep=slept.append)
    assert net.get("a") is None
    assert slept == [10.0, 20.0, 40.0]
    assert net.failed == ["a"]


# ---------------------------------------------------------------- end to end

class FakeNet(yb.Polite):
    def __init__(self, hydat: str = HYDAT_CSV, rt: str = RT_CSV):
        self.asked: list[str] = []

        def fetch(url: str):
            self.asked.append(url)
            return 200, (hydat if url.startswith(yb.HYDAT_DAILY) else rt)
        super().__init__(fetch=fetch, sleep=lambda s: None)


def _station(tmp: Path, days: list[str], extra: list[list] | None = None) -> None:
    rows = [[d, 1.0, 1.0] for d in days] + (extra or [])
    (tmp / "08MH001.json").write_text(json.dumps({
        "station": "08MH001", "now": {"at": "2026-10-21T08:55:00-08:00"},
        "daily": sorted(rows), "priorYears": {"discharge": {}}}))
    (tmp / "index.json").write_text("{}")
    (tmp / "temp_history.json").write_text("{}")


def test_backfill_fills_from_hydat_then_realtime_and_labels_each(tmp_path):
    _station(tmp_path, _days("2026-09-22", "2026-10-20"))
    net = FakeNet()
    s = yb.backfill(tmp_path, today=date(2026, 10, 21), net=net)
    doc = json.loads((tmp_path / "08MH001.json").read_text())
    rows = {r[0]: r for r in doc["daily"]}
    assert rows["2026-01-01"] == ["2026-01-01", 1.933, 61.3, "final"]
    assert rows["2026-01-10"] == ["2026-01-10", 1.862, 46.7, "final"]
    assert rows["2026-08-01"] == ["2026-08-01", 1.6429, 18.2767, "provisional"]
    assert rows["2026-09-22"] == ["2026-09-22", 1.0, 1.0]          # the feed's own, untouched
    assert doc["priorYears"] == {"discharge": {}}                    # the rest of the file kept
    assert s == {"stations": 1, "requests": 1 + 9, "failed": 0, "filled": 11,
                 "fromHydat": 10, "fromRealtime": 1, "provisional": 1,
                 "stillMissing": {"08MH001": [["2026-01-11", "2026-07-31"],
                                              ["2026-08-02", "2026-09-21"]]}}
    # One HYDAT request for the whole span, then a month at a time from 11 January.
    assert net.asked[0] == yb._hydat_url(["08MH001"], "2026-01-01", "2026-09-21")
    assert net.asked[1] == yb._realtime_url("08MH001", "2026-01-11", "2026-02-10", PST)


def test_hydat_wins_where_both_hold_the_day(tmp_path):
    """HYDAT is the approved record; the real-time pull only asks for what is still missing."""
    _station(tmp_path, [d for d in _days("2026-01-01", "2026-10-20")
                        if d not in ("2026-01-05",)])
    net = FakeNet()
    yb.backfill(tmp_path, today=date(2026, 10, 21), net=net)
    assert [u for u in net.asked if u.startswith(yb.REALTIME)] == []


def test_a_complete_year_makes_no_request(tmp_path):
    _station(tmp_path, _days("2026-01-01", "2026-10-20"))
    net = FakeNet()
    s = yb.backfill(tmp_path, today=date(2026, 10, 21), net=net)
    assert (net.asked, s["stations"], s["filled"]) == ([], 0, 0)


def test_a_day_no_source_holds_stays_absent(tmp_path):
    _station(tmp_path, [d for d in _days("2026-01-01", "2026-10-20") if d != "2026-06-15"])
    s = yb.backfill(tmp_path, today=date(2026, 10, 21), net=FakeNet(EMPTY_HYDAT, EMPTY_RT))
    doc = json.loads((tmp_path / "08MH001.json").read_text())
    assert "2026-06-15" not in [r[0] for r in doc["daily"]]
    assert s["stillMissing"] == {"08MH001": [["2026-06-15", "2026-06-15"]]}


def test_an_empty_row_is_replaced_but_a_held_one_never(tmp_path):
    _station(tmp_path, [d for d in _days("2026-01-01", "2026-10-20")
                        if d not in ("2026-01-02", "2026-01-03")],
             extra=[["2026-01-02", None, None]])
    yb.backfill(tmp_path, today=date(2026, 10, 21), net=FakeNet())
    rows = {r[0]: r for r in json.loads((tmp_path / "08MH001.json").read_text())["daily"]}
    assert (rows["2026-01-01"], rows["2026-01-02"], rows["2026-01-03"]) == (
        ["2026-01-01", 1.0, 1.0], ["2026-01-02", 1.917, 58.3, "final"],
        ["2026-01-03", 1.933, 60.6, "final"])


def test_the_publisher_keeps_the_label_on_its_next_run():
    """`merge_daily` carries prior rows whole, so a backfilled day keeps its tag."""
    from pipeline.gauges.feed.publish import merge_daily
    got = merge_daily([["2026-01-01", 1.9, 61.3, "final"]], {"2026-10-20": (1.4, 13.0)}, 2026, 10)
    assert got == [["2026-01-01", 1.9, 61.3, "final"], ["2026-10-20", 1.4, 13.0]]
