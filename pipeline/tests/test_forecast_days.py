"""EVERY FORECAST DAY RANKED (T171): `change.forecast_days`, and the one rule for which model answers a day.

owner 2026-10-23: "in the how similar is a spot to when the spot was added section sometimes it has gaps and says weather
only despite gauge forecast on both sides". The strip read the four sampled horizons, so +2 and +4 had no flow. The feed
now ranks each of the next ten days' daily mean, the models per day joined by `change.combine_day` (T177; T171's
first-model `day_model` is gone) — the one combination the horizons and the Change ask too. The combination itself, its
no-second-copy pin and the skill archive are in `test_forecast_combine.py`.

The fixtures under `fixtures/gauge_feed/forecast_days/` are real station files from the feed of 2026-10-23T20:41Z (runs
without their `change`, the reading, the clim envelopes, and the index's `ahead` as it was published): 08MH001 Chilliwack
River at Vedder Crossing (CLEVER + COFFEE + ELF), 08DB013 (COFFEE + ELF, no CLEVER), 08MG013 Harrison River (ELF only).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipeline.gauges.feed import change as C
from pipeline.gauges.feed import publish as pub

FIX = Path(__file__).parent / "fixtures" / "gauge_feed" / "forecast_days"


def load(station: str) -> dict:
    return json.loads((FIX / f"{station}.json").read_text(encoding="utf-8"))


def ranker(env):
    """The publisher's own ranking of a flow on a day (the `rank_day` it hands the Change and `forecast_days`)."""
    return (lambda v, d: pub.percentile_of(v, pub.band_for(env, datetime(d.year, d.month, d.day, 12,
                                                                          tzinfo=timezone.utc)))) if env else None


def recorded(station: str):
    r = load(station)
    now = datetime.fromisoformat(r["fetchedAt"].replace("Z", "+00:00"))
    days = C.forecast_days(r["forecasts"], now, r["now"]["discharge"], ranker(r["envelope"].get("discharge")))
    ahead = pub.forecast_percentiles(r["forecasts"], r["envelope"], now,
                                     {"discharge": r["now"]["discharge"], "level": r["now"]["level"]})
    return days, ahead, r


# ------------------------------------------------------------------------------------------ the gauge scale ----

def test_a_peak_model_is_put_on_the_gauges_scale_and_the_others_are_not():
    """COFFEE's numbers are instantaneous peaks (its day 0 here is 2.7 against a reading of 0.691 m3/s): ranked as
    published they read high. Scaled by observed / its own today; without a reading it cannot be ranked at all."""
    r = load("08DB013")
    now = datetime.fromisoformat(r["fetchedAt"].replace("Z", "+00:00"))
    assert C.PEAK_MODELS == ("COFFEE",)
    assert C.gauge_scale("COFFEE", r["forecasts"]["COFFEE"], now, 0.691) == pytest.approx(0.691 / 2.7)
    assert C.gauge_scale("COFFEE", r["forecasts"]["COFFEE"], now, None) is None
    assert C.gauge_scale("COFFEE", r["forecasts"]["COFFEE"], now, 0.0) is None
    assert C.gauge_scale("ELF", r["forecasts"]["ELF"], now, 0.691) == 1.0
    assert C.gauge_scale("CLEVER", {"series": {}}, now, None) == 1.0


# ------------------------------------------------------------------------------------------- recorded days ----

def test_chilliwack_every_day_carries_flow_and_the_tenth_is_elfs():
    """+2 and +4 (the strip's gaps) carry flow: CLEVER and COFFEE joined while COFFEE reaches (24-27 Oct, T177), CLEVER
    alone to 1 Nov, and 2 Nov is ELF's, marked low-water."""
    days, _, _ = recorded("08MH001")
    assert days == {"from": "2026-10-24", "discharge": [0.05] * 10,
                    "model": ["CLEVER+COFFEE"] * 4 + ["CLEVER"] * 5 + ["ELF"], "lowWater": [9]}


def test_a_station_with_coffee_and_elf_takes_coffee_for_its_five_days_then_elf_with_the_caveat():
    days, _, _ = recorded("08DB013")
    assert days == {"from": "2026-10-24",
                    "discharge": [0.145, 0.1118, 0.1155, 0.1182, 0.322, 0.311, 0.3027, 0.2994, 0.301, 0.3048],
                    "model": ["COFFEE"] * 4 + ["ELF"] * 6, "lowWater": [4, 5, 6, 7, 8, 9]}
    # by hand, +1: COFFEE's 1.8 x (0.691 / 2.7) = 0.4607 m3/s, between p10 0.3998 and p25 0.6024 of 24 Oct's envelope
    assert 0.10 + (1.8 * 0.691 / 2.7 - 0.39984) / (0.60245 - 0.39984) * 0.15 == pytest.approx(0.145, abs=5e-5)


def test_a_station_with_only_elf_is_low_water_every_day():
    days, _, _ = recorded("08MG013")
    assert days == {"from": "2026-10-24", "discharge": [0.05] * 10, "model": ["ELF"] * 10,
                    "lowWater": list(range(10))}


def test_a_ranked_day_agrees_with_the_horizon_on_the_same_day():
    """THE SAME RANKING: where the horizon's model steps once a day (COFFEE, ELF), the day's mean IS that step, so the
    day's percentile is the horizon's to the feed's rounding — same model, same envelope, same day of year."""
    days, ahead, _ = recorded("08DB013")
    for h in (1, 3, 5, 10):
        assert (days["model"][h - 1], days["discharge"][h - 1]) == (ahead[str(h)]["model"],
                                                                    round(ahead[str(h)]["discharge"], 4))


def test_the_horizons_now_take_coffee_before_elf():
    """T171's one rule (T177's combination since) moved the horizons too: at 08DB013 +1 and +3 were ELF's (no CLEVER), now COFFEE's (a flood
    model reaches them); the level still ELF's (COFFEE forecasts no stage); +5 and +10 unchanged."""
    _, ahead, r = recorded("08DB013")
    live = r["liveAhead"]
    assert [ahead[h]["model"] for h in ("1", "3", "5", "10")] == ["COFFEE", "COFFEE", "ELF", "ELF"]
    assert [live[h]["model"] for h in ("1", "3", "5", "10")] == ["ELF"] * 4
    assert ahead["1"]["levelModel"] == "ELF" and ahead["1"]["level"] == live["1"]["level"]
    assert (ahead["5"], ahead["10"]) == (live["5"], live["10"])
    # where CLEVER runs, T177 joins COFFEE in on the days both reach (+1, +3); the values here sit in the bottom tenth
    # either way (0.05); +5 and +10 unchanged
    _, ahead_c, rc = recorded("08MH001")
    live_c = rc["liveAhead"]
    assert [ahead_c[h]["model"] for h in ("1", "3", "5", "10")] == ["CLEVER+COFFEE", "CLEVER+COFFEE", "CLEVER", "ELF"]
    assert [live_c[h]["model"] for h in ("1", "3", "5", "10")] == ["CLEVER", "CLEVER", "CLEVER", "ELF"]
    assert [ahead_c[h]["discharge"] for h in ("1", "3")] == [live_c[h]["discharge"] for h in ("1", "3")] == [0.05, 0.05]
    assert (ahead_c["5"], ahead_c["10"]) == (live_c["5"], live_c["10"])


# ------------------------------------------------------------------------------------------- built days ----

NOW = datetime(2026, 9, 4, 20, 0, tzinfo=timezone.utc)          # 12:00 on the Centre's clock
FLAT = {str(p): [5.0, 8.0, 11.0, 14.0, 18.0] for p in range(73)}
RANK = ranker(FLAT)


def hourly(hours: int, value, start: datetime = datetime(2026, 9, 4, 0, 0, tzinfo=C.CENTRE_ZONE)):
    at = [(start + timedelta(hours=h)).isoformat() for h in range(hours)]
    return {"at": at, "mid": [value(h) for h in range(hours)], "step": "1h"}


def test_a_day_past_every_model_is_null_never_the_last_value():
    """CLEVER for three days and nothing after: days four to ten are null in both lists (the app says weather only)."""
    runs = {"CLEVER": {"series": hourly(24 * 4, lambda h: 11.0)}}
    assert C.forecast_days(runs, NOW, 11.0, RANK) == {
        "from": "2026-09-05", "discharge": [0.5, 0.5, 0.5] + [None] * 7, "model": ["CLEVER"] * 3 + [None] * 7}


def test_each_day_is_the_mean_of_its_own_hours():
    """Day k is the mean of that Centre day's 24 steps: 8 (p25) all morning and 14 (p75) all afternoon is 11, p50."""
    runs = {"CLEVER": {"series": hourly(24 * 11, lambda h: 8.0 if h % 24 < 12 else 14.0)}}
    got = C.forecast_days(runs, NOW, 11.0, RANK)
    assert got["discharge"] == [0.5] * 10 and got["model"] == ["CLEVER"] * 10 and "lowWater" not in got


def test_a_day_the_run_covers_only_partly_is_not_a_day():
    """A run that stops at 08:00 has a morning of its last day, not the day; DAY_COVER is three quarters."""
    assert C.DAY_COVER == 0.75
    assert list(C.day_means(hourly(24 + 17, lambda h: 11.0))) == ["2026-09-04"]
    assert list(C.day_means(hourly(24 + 18, lambda h: 11.0))) == ["2026-09-04", "2026-09-05"]


def test_a_stale_run_never_ranks_a_day():
    stale = (NOW - timedelta(hours=C.STALE_HOURS + 1)).astimezone(C.CENTRE_ZONE).replace(tzinfo=None).isoformat()
    runs = {"CLEVER": {"issuedAt": stale, "series": hourly(24 * 11, lambda h: 11.0)}}
    assert C.forecast_days(runs, NOW, 11.0, RANK) is None


def test_no_envelope_no_days():
    runs = {"CLEVER": {"series": hourly(24 * 11, lambda h: 11.0)}}
    assert C.forecast_days(runs, NOW, 11.0, None) is None
    assert C.forecast_days(None, NOW, 11.0, RANK) is None


def test_elf_fills_only_the_days_no_flood_model_reaches():
    """CLEVER days one to three, ELF all thirty: ELF answers four to ten, each marked."""
    elf = {"at": [(datetime(2026, 9, 4) + timedelta(days=d)).date().isoformat() for d in range(31)],
           "mid": [5.0] * 31, "step": "1d"}
    runs = {"ELF": {"series": elf}, "CLEVER": {"series": hourly(24 * 4, lambda h: 18.0)}}
    assert C.forecast_days(runs, NOW, 11.0, RANK) == {
        "from": "2026-09-05", "discharge": [0.95] * 3 + [0.05] * 7, "model": ["CLEVER"] * 3 + ["ELF"] * 7,
        "lowWater": [3, 4, 5, 6, 7, 8, 9]}


def test_the_index_carries_every_day(tmp_path, monkeypatch):
    """Through `publish`: the index row's `aheadDays` is `forecast_days` on the station's runs and reading."""
    r = load("08DB013")
    now = datetime.fromisoformat(r["fetchedAt"].replace("Z", "+00:00"))
    monkeypatch.setattr(pub, "fetch_station", lambda st: {
        "station": st, "at": r["now"]["at"], "level": r["now"]["level"], "discharge": r["now"]["discharge"],
        "recent": [[r["now"]["at"], r["now"]["level"], r["now"]["discharge"]]]})
    monkeypatch.setattr(pub, "fetch_temperatures", lambda stations, hours=24: {})
    monkeypatch.setattr(pub, "_get", lambda url: None)
    clim = {"release": "test", "stations": {"08DB013": r["envelope"]}}
    pub.publish(tmp_path, ["08DB013"], clim=clim, forecast={"08DB013": r["forecasts"]}, now=now)
    row = json.loads((tmp_path / "index.json").read_text())["stations"]["08DB013"]
    assert row["aheadDays"] == {
        "from": "2026-10-24",
        "discharge": [0.145, 0.1118, 0.1155, 0.1182, 0.322, 0.311, 0.3027, 0.2994, 0.301, 0.3048],
        "model": ["COFFEE"] * 4 + ["ELF"] * 6, "lowWater": [4, 5, 6, 7, 8, 9]}
    assert row["ahead"]["1"]["model"] == "COFFEE"
