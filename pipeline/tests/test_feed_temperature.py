"""Water temperature in the feed: degrees first, a ranking only once one is earned."""

from __future__ import annotations

import json

import pytest

from pipeline.gauges.feed.publish import (
    TEMP_SAMPLES_PER_PENTAD, accumulate_temperatures, temperature_band,
    temperature_envelope,
)


# ---------------------------------------------------------------- the band

@pytest.mark.parametrize("c,band", [
    (6.4, "cool"), (14.2, "cool"), (17.9, "cool"),
    (18.0, "warm"), (19.9, "warm"),
    (20.0, "critical"), (24.0, "critical"),
])
def test_the_band_is_absolute_not_seasonal(c, band):
    """A fish released into 20 C water often dies, in September as in July. The threshold
    is biological and does not move with the calendar, which is exactly why temperature is
    NOT published as a percentile."""
    assert temperature_band(c) == band


# ---------------------------------------------------------------- accumulation

def test_it_starts_a_record_where_none_exists(tmp_path):
    """HYDAT has no water temperature at all — level, flow and sediment and nothing else.
    So there is no archive to rank against, and the only way to ever have one is to keep
    one. This is that."""
    hist = accumulate_temperatures(tmp_path, {"08MH001": ("t", 16.3)}, pent=49)
    assert hist == {"08MH001": {"49": [16.3]}}
    assert json.loads((tmp_path / "temp_history.json").read_text()) == hist


def test_it_appends_across_runs_and_keeps_pentads_apart(tmp_path):
    accumulate_temperatures(tmp_path, {"A": ("t", 10.0)}, pent=1)
    accumulate_temperatures(tmp_path, {"A": ("t", 11.0)}, pent=1)
    hist = accumulate_temperatures(tmp_path, {"A": ("t", 20.0)}, pent=2)
    assert hist["A"] == {"1": [10.0, 11.0], "2": [20.0]}


def test_one_warm_afternoon_cannot_fill_a_bucket(tmp_path):
    """Half-hourly runs would otherwise put 48 readings from one day into one pentad and
    a percentile would describe that afternoon rather than the season."""
    for i in range(TEMP_SAMPLES_PER_PENTAD + 50):
        accumulate_temperatures(tmp_path, {"A": ("t", float(i))}, pent=3)
    vals = json.loads((tmp_path / "temp_history.json").read_text())["A"]["3"]
    assert len(vals) == TEMP_SAMPLES_PER_PENTAD
    assert vals[-1] == float(TEMP_SAMPLES_PER_PENTAD + 49)      # oldest dropped, not newest


def test_a_station_with_no_sensor_is_absent_not_zero(tmp_path):
    """165 of the 439 have no temperature sensor. That is the normal case, never an error,
    and never a reason to write a zero that would read as ice."""
    hist = accumulate_temperatures(tmp_path, {}, pent=5)
    assert hist == {}


def test_a_corrupt_history_file_is_started_over_rather_than_crashing(tmp_path):
    (tmp_path / "temp_history.json").write_text("{not json")
    hist = accumulate_temperatures(tmp_path, {"A": ("t", 9.0)}, pent=0)
    assert hist == {"A": {"0": [9.0]}}


# ---------------------------------------------------------------- the envelope

def test_a_thin_pentad_publishes_nothing():
    """Same 10-observation gate the HYDAT envelopes use. Below it there is no shape, and a
    percentile from four readings is a number pretending to authority."""
    assert temperature_envelope({"A": {"1": [5.0, 6.0, 7.0]}}) == {}


def test_a_full_pentad_publishes_the_same_five_numbers_as_every_other_envelope():
    """One shape for all envelopes, so a client that can read a flow band can read this."""
    env = temperature_envelope({"A": {"1": [float(i) for i in range(20)]}})
    band = env["A"]["1"]
    assert len(band) == 5
    assert band == sorted(band)
    assert band[2] == pytest.approx(9.0, abs=1.0)               # the median of 0..19


def test_it_is_empty_until_a_season_has_passed_and_that_is_not_a_failure():
    assert temperature_envelope({}) == {}
