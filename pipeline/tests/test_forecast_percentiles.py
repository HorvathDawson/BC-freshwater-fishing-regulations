"""Where the river is HEADING, ranked against the day it will be heading there.

The failure this guards: ranking a forecast three days out against TODAY's envelope. The
number looks perfectly reasonable either way, which is exactly why it needs a test — low
in September is not low in June, and that is the whole reason the app reports a percentile
for the date rather than a discharge.
"""

from datetime import datetime, timedelta, timezone

import pytest

from pipeline.gauges.feed.publish import (HORIZONS, MODEL_ORDER, forecast_at,
                                          forecast_percentiles)

NOW = datetime(2026, 9, 4, 12, 0, tzinfo=timezone.utc)


def hourly(start: datetime, hours: int, value):
    """A CLEVER-shaped run: hourly steps, discharge only."""
    at = [(start + timedelta(hours=h)).isoformat() for h in range(0, hours + 1, 6)]
    return {"at": at, "mid": [value(h) for h in range(0, hours + 1, 6)],
            "level_mid": [None] * len(at)}


def daily(start: datetime, days: int, q, level=None):
    """An ELF-shaped run: daily steps, discharge and level."""
    at = [(start + timedelta(days=d)).date().isoformat() for d in range(days + 1)]
    return {"at": at, "mid": [q] * len(at),
            "level_mid": [level] * len(at) if level is not None else [None] * len(at)}


# p10..p90, flat across the year unless a test says otherwise.
FLAT = {str(p): [5.0, 8.0, 11.0, 14.0, 18.0] for p in range(73)}


def test_no_runs_says_nothing():
    assert forecast_percentiles(None, {"discharge": FLAT}, NOW) == {}
    assert forecast_percentiles({}, {"discharge": FLAT}, NOW) == {}


def test_no_envelope_says_nothing():
    """A forecast with nothing to rank it against is a discharge, not a standing."""
    runs = {"CLEVER": {"series": hourly(NOW, 240, lambda h: 11.0)}}
    assert forecast_percentiles(runs, None, NOW) == {}


def test_every_horizon_is_answered_when_the_run_reaches():
    runs = {"CLEVER": {"series": hourly(NOW, 240, lambda h: 11.0)}}
    got = forecast_percentiles(runs, {"discharge": FLAT}, NOW)
    assert sorted(int(k) for k in got) == list(HORIZONS)
    for h in got.values():
        assert h["discharge"] == pytest.approx(0.5)     # 11.0 is the median
        assert h["model"] == "CLEVER"


def test_a_horizon_past_the_run_is_absent_not_flat():
    """Returning the last value would report day thirty's forecast as day five's."""
    runs = {"CLEVER": {"series": hourly(NOW, 48, lambda h: 11.0)}}   # two days only
    got = forecast_percentiles(runs, {"discharge": FLAT}, NOW)
    assert sorted(int(k) for k in got) == [1]


def test_it_ranks_against_the_FUTURE_day_not_today():
    """THE POINT. Same forecast value, a season that is drying: five days out must rank
    HIGHER than one day out, because the bar it is measured against has fallen."""
    yday = NOW.timetuple().tm_yday
    env = {}
    for p in range(73):
        # Each pentad drier than the last around the current date.
        drop = max(0, p - (yday - 1) // 5) * 1.5
        env[str(p)] = [5.0 - drop, 8.0 - drop, 11.0 - drop, 14.0 - drop, 18.0 - drop]
    runs = {"CLEVER": {"series": hourly(NOW, 240, lambda h: 11.0)}}
    got = forecast_percentiles(runs, {"discharge": env}, NOW)
    assert got["5"]["discharge"] > got["1"]["discharge"]


def test_it_prefers_the_short_range_model():
    """CLEVER is hourly over ten days and calibrated against the observed hydrograph. ELF
    is a thirty-day seasonal-volume model answering a different question."""
    assert MODEL_ORDER[0] == "CLEVER"
    runs = {"ELF": {"series": daily(NOW, 30, 18.0)},
            "CLEVER": {"series": hourly(NOW, 240, lambda h: 5.0)}}
    got = forecast_percentiles(runs, {"discharge": FLAT}, NOW)
    assert got["1"]["model"] == "CLEVER"
    assert got["1"]["discharge"] == pytest.approx(0.05)     # 5.0 is at or below p10


def test_it_falls_back_when_the_preferred_model_is_not_running():
    """Out of season CLEVER 404s. ELF answering is better than nothing answering."""
    runs = {"ELF": {"series": daily(NOW, 30, 11.0)}}
    got = forecast_percentiles(runs, {"discharge": FLAT}, NOW)
    assert got["3"]["model"] == "ELF"


def test_level_and_discharge_are_ranked_separately():
    """A level forecast against a discharge envelope is arithmetic across two units."""
    runs = {"ELF": {"series": daily(NOW, 30, 11.0, level=2.0)}}
    env = {"discharge": FLAT, "level": {str(p): [1.0, 1.5, 2.0, 2.5, 3.0] for p in range(73)}}
    got = forecast_percentiles(runs, env, NOW)
    assert got["1"]["discharge"] == pytest.approx(0.5)
    assert got["1"]["level"] == pytest.approx(0.5)


def test_a_quantity_the_model_does_not_publish_is_absent():
    """CLEVER publishes discharge only. An absent level must not borrow the discharge."""
    runs = {"CLEVER": {"series": hourly(NOW, 240, lambda h: 11.0)}}
    env = {"discharge": FLAT, "level": {str(p): [1.0, 1.5, 2.0, 2.5, 3.0] for p in range(73)}}
    got = forecast_percentiles(runs, env, NOW)
    assert "level" not in got["1"]


def test_forecast_at_takes_the_nearest_step_and_not_an_interpolation():
    """A value between two steps is a number no model produced — and it would then be
    ranked and shown as a percentile, traceable to no run."""
    s = hourly(NOW, 240, lambda h: float(h))
    # +1 day = hour 24, which is a published step.
    assert forecast_at(s, NOW + timedelta(days=1), "discharge") == pytest.approx(24.0)
    # Between steps it takes the nearer one, never the average.
    got = forecast_at(s, NOW + timedelta(hours=27), "discharge")
    assert got in (24.0, 30.0)


def test_forecast_at_refuses_a_step_that_is_about_a_different_day():
    s = hourly(NOW, 24, lambda h: 11.0)
    assert forecast_at(s, NOW + timedelta(days=5), "discharge") is None


def test_a_run_of_all_nulls_answers_nothing():
    runs = {"CLEVER": {"series": {"at": [NOW.isoformat()], "mid": [None],
                                  "level_mid": [None]}}}
    assert forecast_percentiles(runs, {"discharge": FLAT}, NOW) == {}
