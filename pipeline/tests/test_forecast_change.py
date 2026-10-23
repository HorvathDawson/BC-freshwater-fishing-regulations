"""T52 — the expected change over the next ten days, read DAY BY DAY, on recorded runs from the live feed.

Owner 2026-10-22: "even if it is a quick spike then drop we want to show this river will have a big change within the
CLEVER forecast (it could also use multiple forecasts) … look at the daily forecast, not just the 1, 3, 5, 10".

The fixtures under `fixtures/gauge_feed/forecast_change/` are real station files from the feed fetched
2026-10-22T22:28:51Z (series trimmed to `at`/`mid`/`step`, with the station's discharge envelope from `clim.json`), the
afternoon a south-coast storm was forecast for the weekend:

    08HA003   CLEVER spikes 23x on Mon 10-26 — day four, between +3 and +5 — and is back down by Tue
    08MH029   CLEVER rises 2.8x on Wed 10-28 and holds to the end of the run (a step)
    08EE020   ELF falls to 0.59x by 11-01 (a quarter of the record down for the date); CLEVER steady
    08HB025   CLEVER 47x and COFFEE 544x on Sun 10-25: two models, both kept, each against its own today
    08CG001   every run issued 10-14/15 — stale; out of the change and out of the horizons
    08LE077   no model running: out of season
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipeline.gauges.feed import change as C
from pipeline.gauges.feed import forecast as F
from pipeline.gauges.feed import publish as pub

FIX = Path(__file__).parent / "fixtures" / "gauge_feed" / "forecast_change"


def load(station: str) -> dict:
    return json.loads((FIX / f"{station}.json").read_text(encoding="utf-8"))


def at_station(station: str):
    """`station_change` on a recorded station: its runs, its reading, its envelope, at the feed's own clock."""
    r = load(station)
    now = datetime.fromisoformat(r["fetchedAt"].replace("Z", "+00:00"))
    env = r["envelope"]["discharge"]
    rank = (lambda v, d: pub.percentile_of(v, pub.band_for(env, datetime(d.year, d.month, d.day, 12,
                                                                          tzinfo=timezone.utc)))) if env else None
    return C.station_change(r["forecasts"], now, r["now"]["discharge"], rank), r, now


# ------------------------------------------------------------------------------------------- recorded runs ----

def test_a_one_day_spike_between_plus_3_and_plus_5_is_detected_and_dated():
    (per, _, summary), r, now = at_station("08HA003")
    # the map reads CLEVER and COFFEE together (T144): COFFEE peaks a day sooner (44x on 10-25), so the joined peak
    # is 10-25 — sqrt(22.73 x 44.17). COFFEE stops on 10-26 with the joined line still above half the rise, so it is
    # OPEN there (T144 review): 10-27 is CLEVER alone, and the old "back by 10-27" was the average losing COFFEE
    assert summary == {"discharge": 0.9937, "models": ["CLEVER", "COFFEE"], "dir": "rise", "ratio": 31.6876,
                       "day": "2026-10-25", "shape": "open"}
    assert per["CLEVER"]["peak"] == {"day": "2026-10-26", "ratio": 22.7944, "value": 10.645, "percentile": 0.7717,
                                     "points": 0.6717, "back": "2026-10-27"}
    # THE POINT: +5 (10-27) samples the day after it and reads the bottom of the record; the day itself is no horizon
    ahead = pub.forecast_percentiles(r["forecasts"], r["envelope"], now)
    assert ahead["5"] == {"discharge": 0.05, "model": "CLEVER"}
    assert {(now + timedelta(days=d)).date().isoformat() for d in pub.HORIZONS}.isdisjoint({"2026-10-26"})


def test_a_rise_that_holds_is_a_step():
    (per, _, summary), _, _ = at_station("08MH029")
    # COFFEE stops on 10-26 (flat); the rise on 10-28 is CLEVER's alone, past COFFEE's reach
    assert summary == {"discharge": 0.7688, "models": ["CLEVER", "COFFEE"], "dir": "rise", "ratio": 2.7695,
                       "day": "2026-10-28", "shape": "step"}
    assert per["CLEVER"]["peak"]["back"] is None


def test_a_fall_is_dated_and_named_by_its_own_model():
    (per, _, summary), _, _ = at_station("08EE020")
    assert per["ELF"]["dir"] == "fall"
    assert per["ELF"]["trough"] == {"day": "2026-11-01", "ratio": 0.5884, "value": 6.5968, "percentile": 0.1502,
                                    "points": -0.2551, "back": None}
    # ELF's own fall is kept on its run, but the station's change is the flood models' (T144: never ELF beside them):
    # they expect little either way, so the station is steady
    assert summary == {"discharge": 0.493, "models": ["CLEVER", "COFFEE"], "dir": "steady", "ratio": 0.976,
                       "day": "2026-10-30", "shape": "step"}


def test_two_models_are_both_kept_each_against_its_own_today():
    (per, _, summary), r, _ = at_station("08HB025")
    assert sorted(per) == ["CLEVER", "COFFEE", "ELF"]
    assert (per["CLEVER"]["base"], per["CLEVER"]["peak"]["ratio"]) == (0.12, 47.45)
    # COFFEE's today is 0.2 against a gauge reading 0.122: the ratio is COFFEE against COFFEE (108.8 / 0.2), never
    # against the gauge, or its day-0 bias would be a rise nobody forecast
    assert (per["COFFEE"]["base"], per["COFFEE"]["peak"]["value"], per["COFFEE"]["peak"]["ratio"]) == (0.2, 108.8, 544.0)
    assert r["now"]["discharge"] == 0.122
    # together: sqrt(47.45 x 544) on 10-25, each against its own today
    assert summary == {"discharge": 0.995, "models": ["CLEVER", "COFFEE"], "dir": "rise", "ratio": 160.6636,
                       "day": "2026-10-25", "shape": "spike", "back": "2026-10-26"}


def test_out_of_season_says_nothing():
    (per, _, summary), r, _ = at_station("08LE077")
    assert r["forecasts"] == {}
    assert (per, summary) == ({}, None)


def test_stale_runs_answer_neither_the_change_nor_the_horizons():
    (per, _, summary), r, now = at_station("08CG001")
    assert sorted(run["issuedAt"] for run in r["forecasts"].values()) == [
        "2026-10-14T09:18:00", "2026-10-15T09:04:00", "2026-10-15T09:52:00"]
    assert (per, summary) == ({}, None)
    assert pub.forecast_percentiles(r["forecasts"], r["envelope"], now) == {}


# -------------------------------------------------------------------------------------- the rules, pinned ----

NOW = datetime(2026, 10, 22, 22, 0, tzinfo=timezone.utc)          # 14:00 PST
ISSUED = "2026-10-22T09:00:00"


def daily_run(values: list[float], model: str = "ELF", issued: str = ISSUED) -> dict:
    """A daily model's run from today (PST), one value a day."""
    at = [(datetime(2026, 10, 22) + timedelta(days=i)).date().isoformat() for i in range(len(values))]
    return {"model": model, "issuedAt": issued, "series": {"at": at, "mid": values, "step": "1d"}}


def change_of(values: list[float], **kw) -> dict:
    got = C.run_change(daily_run(values, **kw), NOW, 10.0, None)
    assert got is not None
    return got


@pytest.mark.parametrize("peak,want", [(20.0, "rise"), (19.9, "steady")])
def test_a_rise_is_big_at_twice_today(peak, want):
    assert change_of([10.0, peak, 10.0])["dir"] == want


@pytest.mark.parametrize("trough,want", [(5.0, "fall"), (5.1, "steady")])
def test_a_fall_is_big_at_half_today(trough, want):
    assert change_of([10.0, trough, 10.0])["dir"] == want


@pytest.mark.parametrize("points,want", [(0.25, "rise"), (0.24, "steady")])
def test_a_rise_is_big_at_a_quarter_of_the_record(points, want):
    """1.2x is no doubling, but a quarter of the record for the date is big on its own."""
    got = C.run_change(daily_run([10.0, 12.0, 10.0]), NOW, 10.0, lambda v, d: 0.5 + (points if v > 11 else 0.0))
    assert got["dir"] == want


def test_spike_or_step_is_whether_it_comes_back_by_half():
    # 10 -> 30: half the rise is 20. Back to 20 on day three is a spike; 21 to the end is a step.
    assert change_of([10.0, 30.0, 25.0, 20.0])["peak"]["back"] == "2026-10-25"
    assert change_of([10.0, 30.0, 25.0, 21.0])["peak"]["back"] is None


def test_a_move_still_going_on_the_last_day_read_is_open_not_a_step():
    """A peak on the run's LAST day says nothing about whether it holds: the forecast stops while it is still rising.
    On the 2026-10-22 feed, 30 of the 57 big moves with no return were on that day (18 of them COFFEE, a five-day run)."""
    _, _, s = C.station_change({"COFFEE": daily_run([10.0, 12.0, 20.0, 30.0], model="COFFEE")}, NOW, 10.0, None)
    assert s == {"discharge": round(C.coded(3.0), 4), "models": ["COFFEE"], "dir": "rise", "ratio": 3.0,
                 "day": "2026-10-25", "shape": "open"}
    # one day held after it is a step
    _, _, s = C.station_change({"COFFEE": daily_run([10.0, 30.0, 30.0], model="COFFEE")}, NOW, 10.0, None)
    assert (s["shape"], s["day"]) == ("step", "2026-10-23")
    # a fall to the last day, likewise
    _, _, s = C.station_change({"COFFEE": daily_run([10.0, 8.0, 4.0], model="COFFEE")}, NOW, 10.0, None)
    assert (s["dir"], s["shape"], s["day"]) == ("fall", "open", "2026-10-24")


def test_the_window_is_ten_days():
    """ELF publishes thirty; a peak on day eleven is not about to happen."""
    got = change_of([10.0] * 11 + [80.0])
    assert (got["dir"], got["peak"]["ratio"], got["days"][-1][0]) == ("steady", 1.0, "2026-11-01")


def test_a_run_whose_today_is_zero_carries_no_ratio():
    """Six COFFEE runs on 2026-10-22 started at 0.0: a ratio against zero is not a forecast."""
    assert C.run_change(daily_run([0.0, 5.0], model="COFFEE"), NOW, 10.0, None) is None


def test_an_hourly_day_is_its_highest_and_lowest_hour_on_the_pst_clock():
    """CLEVER stamps are PST (measured: best fit +8 h against 305 gauges). 22:00 UTC is 14:00 PST: the base is the
    14:00 step, and the day's hours already past are not ahead."""
    at = [(datetime(2026, 10, 22) + timedelta(hours=h)).isoformat() for h in range(48)]
    mid = [float(h) for h in range(48)]
    run = {"model": "CLEVER", "issuedAt": ISSUED, "series": {"at": at, "mid": mid, "step": "1h"}}
    got = C.run_change(run, NOW, 14.0, None)
    assert (got["base"], got["days"]) == (14.0, [["2026-10-22", 14.0, 23.0], ["2026-10-23", 24.0, 47.0]])


def test_a_run_past_36_hours_is_stale():
    assert C.stale({"issuedAt": "2026-10-21T01:59:00"}, NOW) is True     # 36 h 1 min before 14:00 PST
    assert C.stale({"issuedAt": "2026-10-21T02:00:00"}, NOW) is False
    assert C.stale({"issuedAt": None}, NOW) is False


def test_the_coded_value_is_the_ratio_in_probit_units():
    assert [C.coded(r) for r in (1.0, 2.0, 0.5, 4.0)] == pytest.approx([0.5, 0.691462, 0.308538, 0.841345], abs=1e-6)
    assert (C.coded(1e9), C.coded(1e-9)) == (0.995, 0.005)


# ------------------------------------------------------------------------------- T144: which models speak ----
#
# `change_source` is the one decision. Each case below is the whole station change (`station_change`'s source) or the
# whole index summary; the mutations that fail them are listed in the ticket (T144 "Tests").

def hourly_run(values_by_day: list[float], model: str = "CLEVER") -> dict:
    """An hourly run from 00:00 PST today, each day's 24 steps at that day's value."""
    at, mid = [], []
    for d, v in enumerate(values_by_day):
        for h in range(24):
            at.append((datetime(2026, 10, 22) + timedelta(days=d, hours=h)).isoformat())
            mid.append(v)
    return {"model": model, "issuedAt": ISSUED, "series": {"at": at, "mid": mid, "step": "1h"}}


def test_clever_and_coffee_together_are_their_geometric_mean_day_by_day():
    """CLEVER 4x and COFFEE 9x (each against its own today, COFFEE's today 1.5x CLEVER's as it runs high) on the same
    day: together sqrt(4 x 9) = 6x. Past COFFEE's last day, CLEVER alone; `reach` says where each stops."""
    runs = {"CLEVER": hourly_run([10.0, 40.0, 10.0, 10.0]),
            "COFFEE": daily_run([15.0, 135.0, 15.0], model="COFFEE"),
            "ELF": daily_run([10.0, 9.0, 8.0, 7.0])}
    per, src, s = C.station_change(runs, NOW, 10.0, None)
    assert sorted(per) == ["CLEVER", "COFFEE", "ELF"]           # every run keeps its own change, ELF too
    assert src == {"models": ["CLEVER", "COFFEE"], "lowWater": False,
                   "reach": {"CLEVER": "2026-10-25", "COFFEE": "2026-10-24"}, "base": 1.0,
                   "days": [["2026-10-22", 1.0, 1.0], ["2026-10-23", 6.0, 6.0], ["2026-10-24", 1.0, 1.0],
                            ["2026-10-25", 1.0, 1.0]],
                   "peak": {"day": "2026-10-23", "ratio": 6.0, "value": 6.0, "back": "2026-10-24"},
                   "trough": {"day": "2026-10-22", "ratio": 1.0, "value": 1.0, "back": None},
                   "dir": "rise"}
    assert s == {"discharge": round(C.coded(6.0), 4), "models": ["CLEVER", "COFFEE"], "dir": "rise", "ratio": 6.0,
                 "day": "2026-10-23", "shape": "spike", "back": "2026-10-24"}


def test_a_handover_is_not_a_return():
    """T144 review (08HD027 on the 2026-10-23 feed: COFFEE 11x on its last day, CLEVER 1.06x, joined 3.5x, then CLEVER
    alone — called a spike "back down by Tue" though neither model said so). COFFEE 9x on its last day, CLEVER flat:
    joined 3x there, CLEVER's 1x the day after. Not back while both speak, so no `back`: the end is `until` COFFEE's last
    day and the shape is `open`. A real return inside the joined stretch is still a spike (the geometric-mean test)."""
    runs = {"CLEVER": hourly_run([10.0, 10.0, 10.0, 10.0]),
            "COFFEE": daily_run([15.0, 15.0, 135.0], model="COFFEE")}
    _, src, s = C.station_change(runs, NOW, 10.0, None)
    assert src["peak"] == {"day": "2026-10-24", "ratio": 3.0, "value": 3.0, "back": None, "until": "2026-10-24"}
    assert src["trough"] == {"day": "2026-10-22", "ratio": 1.0, "value": 1.0, "back": None}
    assert s == {"discharge": round(C.coded(3.0), 4), "models": ["CLEVER", "COFFEE"], "dir": "rise", "ratio": 3.0,
                 "day": "2026-10-24", "shape": "open"}
    # one model alone has no handover: the same rise in COFFEE's own run, then CLEVER-free days, is a spike as before
    alone = C.station_change({"COFFEE": daily_run([10.0, 10.0, 30.0, 10.0], model="COFFEE")}, NOW, 10.0, None)[2]
    assert (alone["shape"], alone.get("back")) == ("spike", "2026-10-25")


def test_one_flood_model_alone_is_its_own_change():
    """COFFEE alone (CLEVER out of season): the source is COFFEE's own ratios, and ELF stays out though it speaks."""
    runs = {"COFFEE": daily_run([10.0, 30.0, 10.0], model="COFFEE"), "ELF": daily_run([10.0, 2.0, 2.0])}
    per, src, s = C.station_change(runs, NOW, 10.0, None)
    assert per["ELF"]["dir"] == "fall"
    assert s == {"discharge": round(C.coded(3.0), 4), "models": ["COFFEE"], "dir": "rise", "ratio": 3.0,
                 "day": "2026-10-23", "shape": "spike", "back": "2026-10-24"}
    assert (src["days"], src["reach"]) == ([["2026-10-22", 1.0, 1.0], ["2026-10-23", 3.0, 3.0],
                                            ["2026-10-24", 1.0, 1.0]], {"COFFEE": "2026-10-24"})


def test_clever_alone_is_its_own_change_and_ranks_like_its_run():
    """CLEVER alone: the station's change IS its run's — same ratio, day, shape and percentile move."""
    runs = {"CLEVER": hourly_run([8.0, 20.0, 8.0])}
    per, src, s = C.station_change(runs, NOW, 10.0, lambda v, d: min(0.95, v / 100.0))
    assert s == {"discharge": round(C.coded(2.5), 4), "models": ["CLEVER"], "dir": "rise", "ratio": 2.5,
                 "day": "2026-10-23", "shape": "spike", "back": "2026-10-24"}
    assert {k: v for k, v in src["peak"].items() if k != "value"} == \
        {k: v for k, v in per["CLEVER"]["peak"].items() if k != "value"} == \
        {"day": "2026-10-23", "ratio": 2.5, "percentile": 0.25, "points": 0.15, "back": "2026-10-24"}


def test_elf_speaks_only_when_no_flood_model_does_and_says_it_is_a_low_water_model():
    runs = {"ELF": daily_run([10.0, 8.0, 6.0, 4.0])}
    _, src, s = C.station_change(runs, NOW, 10.0, None)
    assert s == {"discharge": round(C.coded(0.4), 4), "models": ["ELF"], "dir": "fall", "ratio": 0.4,
                 "day": "2026-10-25", "shape": "open", "lowWater": True}
    assert (src["models"], src["lowWater"]) == (["ELF"], True)


def test_a_stale_flood_run_hands_the_change_to_elf_not_beside_it():
    """A CLEVER run past 36 h is no forecast: ELF is then the only voice (with its caveat), never mixed with CLEVER."""
    runs = {"CLEVER": {**hourly_run([10.0, 50.0]), "issuedAt": "2026-10-20T09:00:00"},
            "ELF": daily_run([10.0, 9.0])}
    per, _, s = C.station_change(runs, NOW, 10.0, None)
    assert sorted(per) == ["ELF"]
    assert (s["models"], s.get("lowWater"), s["ratio"]) == (["ELF"], True, 0.9)


def test_a_model_the_policy_does_not_name_never_speaks():
    runs = {"NEWMODEL": daily_run([10.0, 50.0], model="NEWMODEL")}
    per, src, s = C.station_change(runs, NOW, 10.0, None)
    assert sorted(per) == ["NEWMODEL"] and (src, s) == (None, None)


def test_the_policy_names_two_flood_models_equally_weighted_and_one_low_water_model():
    """The decision's inputs, whole: changing a weight or the low-water model is a decision, and this says which."""
    assert (C.FLOOD_MODELS, C.FLOOD_WEIGHTS, C.LOW_WATER_MODEL, C.CHANGE_ORDER) == (
        ("CLEVER", "COFFEE"), {"CLEVER": 1.0, "COFFEE": 1.0}, "ELF", ("CLEVER", "COFFEE", "ELF"))


def test_the_source_is_decided_once():
    """No second copy of the model choice: only `change_source` reads the policy's model names."""
    src = (Path(C.__file__)).read_text(encoding="utf-8")
    body = src.split("def change_source", 1)[1].split("\ndef ", 1)[0]
    rest = src.replace(body, "")
    assert "FLOOD_MODELS" in body and "LOW_WATER_MODEL" in body
    assert [n for n in ("FLOOD_MODELS if", "LOW_WATER_MODEL in", "FLOOD_WEIGHTS.get") if n in rest] == []
    pub_src = Path(pub.__file__).read_text(encoding="utf-8")
    assert [n for n in ('"CLEVER", "COFFEE"', "FLOOD_MODELS", "LOW_WATER_MODEL", "lowWater") if n in pub_src] == []


# ------------------------------------------------------------------------------------------- politeness ----

def test_series_requests_are_only_for_moved_fresh_runs_and_are_logged(monkeypatch, capsys):
    asked: list[tuple[str, str]] = []
    monkeypatch.setattr(F, "series", lambda st, m, timeout=25.0: asked.append((st, m)) or {"at": [], "mid": []})
    summary = {
        "08A": {"CLEVER": {"issuedAt": "2026-10-22T09:43:00"},           # moved: asked
                "ELF": {"issuedAt": "2026-10-22T12:53:00"}},             # same as on disk: carried
        "08B": {"ELF": {"issuedAt": "2026-09-24T09:27:00"}},             # stale: not asked
        "08C": {"COFFEE": {"issuedAt": "2026-10-22T09:05:00"}},          # not a published station: not asked
    }
    keep = {"08A": {"CLEVER": {"issuedAt": "2026-10-21T09:40:00", "series": {"at": ["x"]}},
                    "ELF": {"issuedAt": "2026-10-22T12:53:00", "series": {"at": ["y"]}}}}
    F.with_series(summary, stations={"08A", "08B"}, keep=keep, now=NOW)
    assert asked == [("08A", "CLEVER")]
    assert capsys.readouterr().out == ("  series   1 requests, 1 fetched, 1 carried forward, 1 stale skipped "
                                       "(4 runs)\n")


def test_series_requests_are_capped(monkeypatch):
    summary = {f"08{i:04d}": {"CLEVER": {"issuedAt": ISSUED}} for i in range(F.SERIES_REQUEST_CAP + 7)}
    want, carried, old = F.plan_series(summary, None, None, NOW)
    assert (len(want), carried, old) == (F.SERIES_REQUEST_CAP, 0, 0)


# ------------------------------------------------------------------------------------------- the feed ----

def test_the_feed_carries_the_change_the_runs_and_the_attribution(tmp_path, monkeypatch):
    """One recorded station through `publish`: the index row's `change`, each run's own `change` in the station file,
    and the Province's attribution and newest issue time at the top of the index."""
    r = load("08HB025")
    now = datetime.fromisoformat(r["fetchedAt"].replace("Z", "+00:00"))
    monkeypatch.setattr(pub, "fetch_station", lambda st: {
        "station": st, "at": "2026-10-22T13:50:00-08:00", "level": None, "discharge": r["now"]["discharge"],
        "recent": [["2026-10-22T13:50:00-08:00", None, r["now"]["discharge"]]]})
    monkeypatch.setattr(pub, "fetch_temperatures", lambda stations, hours=24: {})
    monkeypatch.setattr(pub, "_get", lambda url: None)
    clim = {"release": "test", "stations": {"08HB025": {"discharge": r["envelope"]["discharge"]}}}
    pub.publish(tmp_path, ["08HB025"], clim=clim, forecast={"08HB025": r["forecasts"]}, now=now)
    index = json.loads((tmp_path / "index.json").read_text())
    assert index["forecastAttribution"] == F.ATTRIBUTION
    assert index["forecastIssuedAt"] == "2026-10-22T12:53:00"
    row = index["stations"]["08HB025"]
    assert row["change"] == {"discharge": 0.995, "models": ["CLEVER", "COFFEE"], "dir": "rise", "ratio": 160.6636,
                             "day": "2026-10-25", "shape": "spike", "back": "2026-10-26"}
    assert "change" not in row.get("ahead", {})
    station = json.loads((tmp_path / "08HB025.json").read_text())
    assert {m: run["change"]["peak"]["ratio"] for m, run in station["forecasts"].items()} == {
        "CLEVER": 47.45, "COFFEE": 544.0, "ELF": 1.2127}
    # T144: the station's own change — CLEVER and COFFEE day by day, ELF not in it — is what the panel's first line reads
    assert station["change"] == {
        "models": ["CLEVER", "COFFEE"], "lowWater": False, "reach": {"CLEVER": "2026-10-31", "COFFEE": "2026-10-26"},
        "base": 1.0,
        "days": [["2026-10-22", 1.0, 1.0878], ["2026-10-23", 0.9958, 1.0916], ["2026-10-24", 16.173, 94.8432],
                 ["2026-10-25", 94.7657, 160.6636], ["2026-10-26", 12.2515, 41.8312], ["2026-10-27", 0.9833, 1.2083],
                 ["2026-10-28", 0.9833, 0.9833], ["2026-10-29", 0.9833, 1.025], ["2026-10-30", 1.0333, 1.275],
                 ["2026-10-31", 1.15, 1.25]],
        "peak": {"day": "2026-10-25", "ratio": 160.6636, "value": 160.6636, "percentile": 0.95, "points": 0.9,
                 "back": "2026-10-26"},
        "trough": {"day": "2026-10-27", "ratio": 0.9833, "value": 0.9833, "percentile": 0.05, "points": 0.0,
                   "back": "2026-10-30"},
        "dir": "rise"}


def test_no_run_no_change_and_no_attribution(tmp_path, monkeypatch):
    monkeypatch.setattr(pub, "fetch_station", lambda st: {
        "station": st, "at": "2026-10-22T13:50:00-08:00", "level": None, "discharge": 0.016,
        "recent": [["2026-10-22T13:50:00-08:00", None, 0.016]]})
    monkeypatch.setattr(pub, "fetch_temperatures", lambda stations, hours=24: {})
    monkeypatch.setattr(pub, "_get", lambda url: None)
    pub.publish(tmp_path, ["08LE077"], clim=None, forecast={}, now=NOW)
    index = json.loads((tmp_path / "index.json").read_text())
    assert "forecastAttribution" not in index and "change" not in index["stations"]["08LE077"]
