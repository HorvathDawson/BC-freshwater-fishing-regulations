"""T126 — the BC River Forecast Centre's clock, and CLEVER's summary, on recorded data.

Found by T52 (2026-10-22): CLEVER's CSV stamps carry no zone and are PACIFIC STANDARD TIME, but the publisher read them
as UTC, so every +1/+3/+5/+10 sampled the step eight hours off; and CLEVER's summary layer reports the maximum in FIVE
days on a ten-day run, so the headline missed a later peak.

Fixtures (`fixtures/gauge_feed/clever_clock/`):

    08MH090.CSV                the Centre's CSV for 08MH090, fetched 2026-10-23 (the run issued 10:35 AM 2026-10-22,
                               the same run the feed carried), verbatim
    observed_vs_forecast.json  every CLEVER station in the live feed of 2026-10-23T03:50Z: the first 40 hourly steps
                               of its series (first stamp verbatim, naive as the feed wrote it) and the gauge's
                               observed discharge on the hour (first stamp verbatim, zoned as ECCC writes it)
"""

from __future__ import annotations

import json
import re
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipeline.gauges.feed import change as C
from pipeline.gauges.feed import forecast as F
from pipeline.gauges.feed import publish as pub

FIX = Path(__file__).parent / "fixtures" / "gauge_feed" / "clever_clock"
FEED = Path(__file__).resolve().parents[1] / "gauges" / "feed"


def recorded_csv() -> str:
    return (FIX / "08MH090.CSV").read_text(encoding="utf-8")


# ----------------------------------------------------------------------------------------- the measured fit ----

def _fit(read) -> float:
    """Median over stations of each station's median |forecast / observed - 1|, with the series' stamps read by
    `read(naive stamp) -> aware instant`."""
    rec = json.loads((FIX / "observed_vs_forecast.json").read_text(encoding="utf-8"))
    per: list[float] = []
    for st in rec["stations"].values():
        t0 = datetime.fromisoformat(st["observedStart"])
        obs = {(t0 + timedelta(hours=i)).astimezone(timezone.utc): q
               for i, q in enumerate(st["observed"]) if q is not None and q > 0}
        s0 = datetime.fromisoformat(st["start"])
        errs = []
        for i, v in enumerate(st["mid"]):
            if v is None:
                continue
            q = obs.get(read((s0 + timedelta(hours=i)).isoformat()).astimezone(timezone.utc))
            if q:
                errs.append(abs(v / q - 1))
        if errs:
            per.append(statistics.median(errs))
    return round(100 * statistics.median(per), 2)


def _as_zone(hours: int):
    return lambda at: datetime.fromisoformat(at).replace(tzinfo=timezone(timedelta(hours=hours)))


def test_the_centres_clock_is_pst_measured_against_the_gauges():
    """THE MEASUREMENT, pinned: read in every whole-hour zone, the recorded forecasts sit closest to the gauges at
    UTC-8 — and `forecast.instant`, the one reading of the clock, lands exactly there.
    Mutation: CENTRE_UTC_OFFSET_H = 0 (the old UTC reading) -> the last assertion fails (1.78 != 0.88)."""
    fits = {h: _fit(_as_zone(h)) for h in range(-12, 13)}
    assert min(fits, key=fits.get) == -8 == F.CENTRE_UTC_OFFSET_H
    assert (fits[-8], fits[-7], fits[0]) == (0.88, 0.99, 1.78)
    assert _fit(F.instant) == fits[-8]


# ----------------------------------------------------------------------------------------- the recorded CSV ----

def test_the_recorded_csv_is_written_with_the_centres_zone():
    """Whole stamps at the head and tail of the run: a Centre wall clock, written as the instant it is, so the app's
    `Date.parse` reads the same moment on any device. Mutation: drop `zoned(...)` in `parse_series` -> fails."""
    s = F.parse_series(recorded_csv(), "CLEVER")
    assert s is not None
    assert (len(s["at"]), s["at"][:2], s["at"][-1], s["step"]) == (
        240, ["2026-10-22T00:00:00-08:00", "2026-10-22T01:00:00-08:00"], "2026-10-31T23:00:00-08:00", "1h")
    assert (s["mid"][:2], s["mid"][-1]) == ([0.205, 0.207], 0.226)


def test_a_horizon_reads_the_step_at_that_instant_not_eight_hours_off():
    """19:00 UTC on 10-22 is 11:00 at the Centre: the 11:00 step answers, not the 19:00 one.
    Mutation: `instant` reading a naive stamp as UTC -> returns the 19:00 step (row 19) instead of row 11."""
    s = F.parse_series(recorded_csv(), "CLEVER")
    rows = [ln.split(",") for ln in recorded_csv().splitlines() if ln.startswith("2026-")]
    at11 = next(float(r[2]) for r in rows if (r[0], r[1]) == ("2026-10-22", "11"))
    at19 = next(float(r[2]) for r in rows if (r[0], r[1]) == ("2026-10-22", "19"))
    assert at11 != at19                                             # the fixture can tell the two apart
    assert pub.forecast_at(s, datetime(2026, 10, 22, 19, tzinfo=timezone.utc), "discharge") == at11
    # and a stamp written naive (a station file from before T126) is read the same way
    naive = {**s, "at": [a.removesuffix("-08:00") for a in s["at"]]}
    assert pub.forecast_at(naive, datetime(2026, 10, 22, 19, tzinfo=timezone.utc), "discharge") == at11


def test_a_daily_step_answers_for_its_whole_centre_day():
    """ELF's steps are DAYS. 15:30 at the Centre on 10-22 is 10-22's value — not 10-23's, whose midnight is nearer.
    Mutation: drop the `is_day` branch in `forecast_at` -> 23:30Z reads 10-23 (11.0)."""
    s = {"at": ["2026-10-21", "2026-10-22", "2026-10-23"], "mid": [10.0, 20.0, 11.0], "level_mid": [None] * 3}
    got = [pub.forecast_at(s, datetime(2026, 10, d, h, m, tzinfo=timezone.utc), "discharge")
           for d, h, m in ((22, 8, 0), (22, 23, 30), (23, 7, 59), (23, 8, 0), (24, 8, 0))]
    assert got == [20.0, 20.0, 20.0, 11.0, None]


def test_a_daily_run_stamped_at_midnight_is_a_day_too():
    """COFFEE writes its daily steps as midnights ("2026-10-22T00:00:00", `step: "1d"`). The change measure always read
    them as whole days; the horizons read them as midnight instants, so 15:30 at the Centre took tomorrow's midnight.
    One answer now (`forecast.step_day`). Mutation: `step_day` ignoring `step` (bare days only) -> 23:30Z reads 11.0."""
    s = {"at": ["2026-10-21T00:00:00-08:00", "2026-10-22T00:00:00-08:00", "2026-10-23T00:00:00-08:00"],
         "mid": [10.0, 20.0, 11.0], "level_mid": [None] * 3, "step": "1d"}
    got = [pub.forecast_at(s, datetime(2026, 10, d, h, m, tzinfo=timezone.utc), "discharge")
           for d, h, m in ((22, 8, 0), (22, 23, 30), (23, 7, 59), (23, 8, 0), (24, 8, 0))]
    assert got == [20.0, 20.0, 20.0, 11.0, None]
    assert [F.step_day(s, a) for a in s["at"]] == ["2026-10-21", "2026-10-22", "2026-10-23"]
    assert [F.step_day({"step": "1h"}, "2026-10-22T05:00:00-08:00"), F.step_day({"step": "1h"}, "2026-10-22"),
            F.step_day(None, "2026-10-22T13:00:00Z")] == [None, "2026-10-22", None]


# ------------------------------------------------------------------------------------- the summary, from the series ----

SUMMARY_08MH090 = {"model": "CLEVER", "issuedAt": "2026-10-22T10:35:00", "horizonDays": 10, "value": 0.263,
                   "extreme": "max", "min": None, "ave": None, "max": 0.263, "observed": 0.205, "unit": "m3/s"}


def test_clevers_summary_is_taken_from_its_ten_day_series():
    """08MH090: the layer said 0.263 m3/s (its "maximum in 5 days"); the run peaks at 5.431 at 10:00 on 10-28, day
    six. Whole run (min/ave/max over the steps from the 10:35 issue on, read on the Centre's clock).
    Mutation: drop `summary_from_series` from CLEVER's config -> value/max stay 0.263 and min/ave None."""
    run = F.settle({**SUMMARY_08MH090, "series": F.parse_series(recorded_csv(), "CLEVER")})
    assert {k: v for k, v in run.items() if k != "series"} == {
        "model": "CLEVER", "issuedAt": "2026-10-22T10:35:00", "horizonDays": 10, "value": 5.431, "extreme": "max",
        "min": 0.207, "ave": 0.46, "max": 5.431, "observed": 0.205, "unit": "m3/s"}


def test_the_hours_before_the_issue_are_not_in_the_summary():
    """The CSV opens at 00:00 on the issue day; the run was made at 10:35. 08MH090's 00:00 step (0.205) is the lowest
    in the file and is not ahead of anyone. Mutation: drop the `t < issued` filter in `ahead_of_issue` -> min 0.205,
    and 240 steps instead of 229."""
    run = {**SUMMARY_08MH090, "series": F.parse_series(recorded_csv(), "CLEVER")}
    ahead = F.ahead_of_issue(run)
    assert (len(ahead), min(ahead), max(ahead)) == (229, 0.207, 5.431)
    assert min(run["series"]["mid"]) == 0.205


def test_other_models_keep_their_layers_summary():
    """ELF's and COFFEE's columns cover their own horizons (30 and 5 days); only CLEVER's does not."""
    assert [m for m, c in F.MODELS.items() if c.get("summary_from_series")] == ["CLEVER"]
    elf = {"model": "ELF", "issuedAt": "2026-10-22T12:53:00", "value": 0.0675, "extreme": "min", "min": 0.0675,
           "ave": 0.1, "max": 0.56, "series": {"at": ["2026-10-22", "2026-10-23"], "mid": [0.3, 0.2]}}
    assert F.settle(elf) == elf


def test_the_publisher_writes_the_series_summary_and_zoned_stamps(monkeypatch):
    """`with_series`, the publisher's one entry: a fetched CLEVER run comes out settled."""
    monkeypatch.setattr(F, "series", lambda st, m, timeout=25.0: F.parse_series(recorded_csv(), m))
    now = datetime(2026, 10, 22, 22, 0, tzinfo=timezone.utc)
    out = F.with_series({"08MH090": {"CLEVER": dict(SUMMARY_08MH090)}}, stations={"08MH090"}, now=now)
    run = out["08MH090"]["CLEVER"]
    assert (run["value"], run["max"], run["series"]["at"][0]) == (5.431, 5.431, "2026-10-22T00:00:00-08:00")


# ------------------------------------------------------------------------------------------- no second copy ----

def test_the_clock_is_read_in_one_place():
    """ONE decision, one function: `forecast.instant`. The publisher and the change measure each kept their own zone
    (UTC and PST) — that disagreement WAS the bug. Neither may parse a stamp or build a zone of its own again, nor decide
    on its own which steps are whole days (`forecast.step_day`; they decided it two ways until the T126 review)."""
    for name in ("publish.py", "change.py"):
        src = (FEED / name).read_text(encoding="utf-8")
        assert re.findall(r"datetime\.fromisoformat|timezone\(timedelta|def _parse\(|hours=-8|\"1d\"|is_day\(", src) == [], name
    assert C._local("2026-10-22T05:00:00") == C._local("2026-10-22T05:00:00-08:00") == datetime(2026, 10, 22, 5)


@pytest.mark.parametrize("at,want", [
    ("2026-10-22T05:00:00", "2026-10-22T05:00:00-08:00"),          # a Centre wall clock
    ("2026-10-22T13:00:00Z", "2026-10-22T13:00:00+00:00"),         # a stated zone is kept
    ("2026-10-22T05:00:00-08:00", "2026-10-22T05:00:00-08:00"),    # idempotent
    ("2026-10-22", "2026-10-22"),                                  # a day stays a day
    ("sometime", "sometime"),                                      # unreadable is kept, never guessed
])
def test_zoned(at, want):
    assert F.zoned(at) == want
