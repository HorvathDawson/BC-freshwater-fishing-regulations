"""Every publish refills any missing day inside ECCC's 30-day archive (ticket 2026-10-21).

Before this, the archive was read only for a station with NO daily record at all, so a
station that had ever been published never refilled a gap: 08MH001 lost 26 Sep-12 Oct and
16-18 Oct to nights the publisher did not run, and kept losing them.

The network is a fake: `publish._get` is replaced by a dict of URL -> CSV text, and every
URL asked for is recorded, so "made no archive request" is a fact the test can check.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest

from pipeline.gauges.feed import publish as pub

STATION = "08MH001"
HOURLY = f"{pub.BASE}/hourly/BC_{STATION}_hourly_hydrometric.csv"
ARCHIVE = pub._DAILY.format(station=STATION)
HEAD = f"ID,Date,{pub._LEVEL},{pub._FLOW}"
# 17:00 UTC on 21 October is 09:00 Pacific standard: the station's own day is the 21st.
NOW = datetime(2026, 10, 21, 17, 0, tzinfo=timezone.utc)


def _days(a: str, b: str) -> list[str]:
    d, end, out = date.fromisoformat(a), date.fromisoformat(b), []
    while d <= end:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _csv(rows: list[tuple[str, float, float]]) -> str:
    return "\n".join([HEAD] + [f"{STATION},{t},{lv},{q}" for t, lv, q in rows]) + "\n"


# The 2-day hourly file: yesterday and this morning, local standard time. Seven rows, because
# the publisher keeps every sixth (5-minute data thinned to the half hour): rows 0 and 6.
HOURLY_CSV = _csv([(f"2026-10-20T{hh:02d}:00:00-08:00", 1.4, 13.0) for hh in range(0, 24, 4)]
                  + [("2026-10-21T08:55:00-08:00", 1.5, 14.0)])
# The 30-day archive: 21 Sep .. 20 Oct, two readings a day, discharge = day of month.
ARCHIVE_CSV = _csv([(f"{d}T{hh}:00:00-08:00", 2.0, float(int(d[-2:])))
                    for d in _days("2026-09-21", "2026-10-20") for hh in ("06", "18")])


@pytest.fixture
def net(monkeypatch):
    asked: list[str] = []
    served = {HOURLY: HOURLY_CSV, ARCHIVE: ARCHIVE_CSV}

    def fake(url: str):
        asked.append(url)
        return served.get(url)              # the temperature service answers nothing

    monkeypatch.setattr(pub, "_get", fake)
    monkeypatch.setattr(pub, "_sleep", lambda s: None)   # the temperature retries wait for nobody
    return asked


def _seed(out, days: list[str]) -> None:
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{STATION}.json").write_text(json.dumps(
        {"station": STATION, "daily": [[d, 1.0, 1.0] for d in days]}))


def _daily(out) -> list[list]:
    return json.loads((out / f"{STATION}.json").read_text())["daily"]


def test_a_mid_window_gap_is_filled_from_the_archive(tmp_path, net):
    have = [d for d in _days("2026-01-01", "2026-10-19")
            if d not in ("2026-10-05", "2026-10-06", "2026-10-07")]
    _seed(tmp_path, have)
    summary = pub.publish(tmp_path, [STATION], now=NOW)
    assert net.count(ARCHIVE) == 1
    rows = {r[0]: r for r in _daily(tmp_path)}
    assert [rows[d] for d in ("2026-10-05", "2026-10-06", "2026-10-07")] == [
        ["2026-10-05", 2.0, 5.0], ["2026-10-06", 2.0, 6.0], ["2026-10-07", 2.0, 7.0]]
    assert [r[0] for r in _daily(tmp_path)] == _days("2026-01-01", "2026-10-21")
    assert summary["archive"] == {"asked": 1, "filled": 1}
    assert summary["gapsBeyondWindow"] == {}


def test_the_archive_rewrites_its_window_so_a_cut_off_day_is_completed(tmp_path, net):
    """The day before a gap was written by a run that stopped mid-day; the archive's mean
    of the whole day replaces it, which is the freshest-reading-wins rule applied again."""
    have = [d for d in _days("2026-01-01", "2026-10-19") if d != "2026-10-10"]
    _seed(tmp_path, have)
    pub.publish(tmp_path, [STATION], now=NOW)
    rows = {r[0]: r for r in _daily(tmp_path)}
    assert rows["2026-10-09"] == ["2026-10-09", 2.0, 9.0]
    # Outside the window the record is the record: nothing the archive cannot see moves.
    assert rows["2026-09-01"] == ["2026-09-01", 1.0, 1.0]


def test_a_complete_record_makes_no_archive_request(tmp_path, net):
    _seed(tmp_path, _days("2026-01-01", "2026-10-20"))
    summary = pub.publish(tmp_path, [STATION], now=NOW)
    assert ARCHIVE not in net
    assert summary["archive"] == {"asked": 0, "filled": 0}


def test_yesterday_missing_but_in_the_hourly_file_is_not_a_gap(tmp_path, net):
    """The 2-day hourly file already covers yesterday; asking the archive for it would
    be one request per station per run for nothing."""
    _seed(tmp_path, _days("2026-01-01", "2026-10-19"))
    pub.publish(tmp_path, [STATION], now=NOW)
    assert ARCHIVE not in net


def test_a_gap_older_than_the_window_is_reported_not_invented(tmp_path, net):
    have = [d for d in _days("2026-01-01", "2026-10-20")
            if d not in ("2026-08-01", "2026-08-02", "2026-08-03", "2026-08-09")]
    _seed(tmp_path, have)
    summary = pub.publish(tmp_path, [STATION], now=NOW)
    assert ARCHIVE not in net
    assert summary["gapsBeyondWindow"] == {
        STATION: [["2026-08-01", "2026-08-03"], ["2026-08-09", "2026-08-09"]]}
    days = [r[0] for r in _daily(tmp_path)]
    assert "2026-08-02" not in days and "2026-08-09" not in days


def test_a_cold_station_still_starts_with_a_month(tmp_path, net):
    summary = pub.publish(tmp_path, [STATION], now=NOW)
    assert net.count(ARCHIVE) == 1
    assert [r[0] for r in _daily(tmp_path)] == _days("2026-09-21", "2026-10-21")
    assert summary["gapsBeyondWindow"] == {STATION: [["2026-01-01", "2026-09-20"]]}


# ---------------------------------------------------------------- the pure part

def test_gaps_split_at_the_window_edge():
    have = [[d, 1.0, 1.0] for d in _days("2026-01-01", "2026-10-20")
            if d not in ("2026-09-20", "2026-09-21", "2026-09-22", "2026-10-15")]
    inside, beyond = pub.daily_gaps(have, date(2026, 10, 21))
    assert inside == ["2026-09-22", "2026-10-15"]
    assert beyond == ["2026-09-20", "2026-09-21"]


def test_a_row_with_no_value_is_a_gap():
    have = [[d, 1.0, 1.0] for d in _days("2026-01-01", "2026-10-20")]
    have[-3] = ["2026-10-18", None, None]
    assert pub.daily_gaps(have, date(2026, 10, 21)) == (["2026-10-18"], [])


def test_in_january_the_window_reaches_back_into_december():
    """S1: last December rides along in January, so a December gap is a window gap and the
    archive refills it; a gap in November is nobody's, since the record drops it."""
    have = [[d, 1.0, 1.0] for d in _days("2026-12-01", "2027-01-09") if d != "2026-12-20"]
    assert pub.daily_gaps(have, date(2027, 1, 10)) == (["2026-12-20"], [])


def test_in_february_last_december_is_not_a_gap():
    have = [[d, 1.0, 1.0] for d in _days("2027-01-01", "2027-02-09")]
    assert pub.daily_gaps(have, date(2027, 2, 10)) == ([], [])


def test_runs_compresses_days_into_ranges():
    assert pub.day_runs(["2026-01-01", "2026-01-02", "2026-01-04"]) == [
        ["2026-01-01", "2026-01-02"], ["2026-01-04", "2026-01-04"]]
