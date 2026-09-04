"""A forecast series that is already on disk must survive the next publish.

THE BUG THIS PINS. `with_series` skipped re-fetching a run whose issue time had not moved —
correct, and free — but it was given only the issue TIME, so it had nothing to carry into
the new summary. The publisher then wrote the station file from that summary, erasing the
series it had just decided not to re-download. Measured on the live feed: 897 of 925 runs
reported "already current" while exactly ONE station file still had a series in it.

Everything downstream of a series was therefore empty almost everywhere — the forecast
ribbon on a chart, and the forecast percentiles that colour the map for +1/+3/+5 days.
"""

import pytest

from pipeline.gauges.feed import forecast as _f
from pipeline.gauges.feed.forecast import with_series


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """The fetch is never the thing under test here, and a test that reaches the Province
    is a test that fails on a train."""
    monkeypatch.setattr(_f, "series", lambda station, model, timeout=25.0: None)

SERIES = {"at": ["2026-09-04T00:00:00"], "mid": [11.0], "level_mid": [None]}


def test_a_still_current_run_keeps_its_series():
    summary = {"08MH001": {"CLEVER": {"issuedAt": "2026-09-04T09:51:00"}}}
    keep = {"08MH001": {"CLEVER": {"issuedAt": "2026-09-04T09:51:00", "series": SERIES}}}
    out = with_series(summary, stations={"08MH001"}, keep=keep)
    assert out["08MH001"]["CLEVER"]["series"] == SERIES


def test_a_reissued_run_is_not_carried():
    """A new issue time is a new forecast. Carrying the old series across would show
    yesterday's model run under today's timestamp."""
    summary = {"08MH001": {"CLEVER": {"issuedAt": "2026-09-05T09:00:00"}}}
    keep = {"08MH001": {"CLEVER": {"issuedAt": "2026-09-04T09:51:00", "series": SERIES}}}
    out = with_series(summary, stations={"08MH001"}, keep=keep)
    assert "series" not in out["08MH001"]["CLEVER"]


def test_a_previous_run_with_no_series_is_not_carried():
    summary = {"08MH001": {"CLEVER": {"issuedAt": "2026-09-04T09:51:00"}}}
    keep = {"08MH001": {"CLEVER": {"issuedAt": "2026-09-04T09:51:00"}}}
    out = with_series(summary, stations={"08MH001"}, keep=keep)
    assert "series" not in out["08MH001"]["CLEVER"]


def test_it_carries_each_model_separately():
    """Three models publish on their own clocks; one moving must not drop the other two."""
    summary = {"08MH001": {"CLEVER": {"issuedAt": "T1"}, "ELF": {"issuedAt": "T2"},
                           "COFFEE": {"issuedAt": "T9"}}}
    keep = {"08MH001": {"CLEVER": {"issuedAt": "T1", "series": SERIES},
                        "ELF": {"issuedAt": "T2", "series": SERIES},
                        "COFFEE": {"issuedAt": "T3", "series": SERIES}}}
    out = with_series(summary, stations={"08MH001"}, keep=keep)
    assert out["08MH001"]["CLEVER"]["series"] == SERIES
    assert out["08MH001"]["ELF"]["series"] == SERIES
    assert "series" not in out["08MH001"]["COFFEE"]      # reissued


def test_a_station_absent_from_keep_is_untouched():
    summary = {"08MH001": {"CLEVER": {"issuedAt": "T1"}}}
    out = with_series(summary, stations={"08MH001"}, keep={})
    assert "series" not in out["08MH001"]["CLEVER"]


def test_carrying_survives_a_round_trip_through_the_publisher_shape():
    """The publisher hands `keep` the runs it read back off disk, so the shape this takes
    has to be the shape a station file stores — `{model: run}`, series and all."""
    on_disk = {"CLEVER": {"model": "CLEVER", "issuedAt": "T1", "horizonDays": 10,
                          "series": SERIES}}
    summary = {"08MH001": {"CLEVER": {"model": "CLEVER", "issuedAt": "T1",
                                      "horizonDays": 10}}}
    out = with_series(summary, stations={"08MH001"}, keep={"08MH001": on_disk})
    assert out["08MH001"]["CLEVER"]["series"] == SERIES
