"""The feed index says when the newest forecast run was issued (T68: the More tab's "Forecasts" line).

The per-station files carry every BC River Forecast Centre run with its `issuedAt`, but the More tab reads only the
index (one small fetch, already made for the sync state). So the index carries ONE feed-wide stamp,
`forecastIssuedAt`: the newest run's `issuedAt` across every station, written exactly as the station files carry it
(the app parses both the same way, so the line and a station's chart can never disagree about the hour).

    a string   the newest run's issue time
    null       this run holds no forecast at all (out of season, or the Centre's pull failed): known, none running
    absent     an index from before T68: no word, the app says so
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from pipeline.gauges.feed import publish as pub

NOW = datetime(2026, 10, 22, 21, 0, tzinfo=timezone.utc)


@pytest.fixture
def net(monkeypatch):
    monkeypatch.setattr(pub, "_get", lambda url: None)
    monkeypatch.setattr(pub, "_sleep", lambda s: None)


def _index(out) -> dict:
    return json.loads((out / "index.json").read_text())


def test_newest_run_across_every_station_and_model(tmp_path, net):
    forecast = {
        # the stamps as the Centre's layers give them (naive) and one zoned form; one form nobody can read
        "08MF005": {"CLEVER": {"issuedAt": "2026-10-22T09:22:00"}, "ELF": {"issuedAt": "2026-10-22T12:53:00"}},
        "08MH001": {"CLEVER": {"issuedAt": "2026-10-15T09:52:00"}, "COFFEE": {"issuedAt": "Updated at: soon"}},
        "08NM139": {"ELF": {"issuedAt": "2026-10-22T10:35:00Z"}, "CLEVER": {}},
    }
    pub.publish(tmp_path, [], forecast=forecast, now=NOW)
    assert _index(tmp_path)["forecastIssuedAt"] == "2026-10-22T12:53:00"


def test_no_forecast_is_null_not_absent(tmp_path, net):
    pub.publish(tmp_path, [], forecast={}, now=NOW)
    idx = _index(tmp_path)
    assert "forecastIssuedAt" in idx
    assert idx["forecastIssuedAt"] is None


def test_runs_with_no_readable_stamp_are_none(tmp_path, net):
    pub.publish(tmp_path, [], forecast={"08MF005": {"CLEVER": {"issuedAt": None}, "ELF": {"issuedAt": "x"}}},
                now=NOW)
    assert _index(tmp_path)["forecastIssuedAt"] is None


def test_the_function_whole_value():
    assert pub.newest_forecast_issue({
        "a": {"CLEVER": {"issuedAt": "2026-10-21T23:59:00"}},
        "b": {"ELF": {"issuedAt": "2026-10-22T00:01:00"}, "CLEVER": {"issuedAt": "2026-09-25T09:48:00"}},
    }) == "2026-10-22T00:01:00"
    assert pub.newest_forecast_issue(None) is None
