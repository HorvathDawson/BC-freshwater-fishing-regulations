"""T830: the index says which quantities a station reported live (`reports`), ranked or not.

Capilano at Canyon (08GA031) reports a level on every EC row; its HYDAT level record is two years (2024-2025) against
`record.MIN_RECORD_YEARS` = 3, so clim.json has a discharge band only. The index then carried no level at all, and the
app, reading "no level rank" as "no level", said "Reports flow only today, not water level" (16 stations, 2026-11-17).
A level off the record's rating (T550: another datum, 5 stations) stays out on purpose.

Mutation pins: dropping the `level_off_datum` test lets 08HD006's other-datum level in
(`test_a_level_on_another_datum_is_not_reported`); writing `reports` from `per` (the ranks) instead of the reading drops
Capilano's level (`test_the_publisher_says_capilano_reports_its_level`).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from pipeline.gauges.feed import publish as pub
from pipeline.gauges.feed.climatology import RATING_PAD_M

NOW = datetime(2026, 11, 17, 16, 0, tzinfo=timezone.utc)
AT = "2026-11-17T07:55:00-08:00"
BAND = [[1.0, 2.0, 3.0, 4.0, 5.0]] * 73                               # one band per pentad
# 08HD006 since 2016 (test_level_datum): a live 2.338 m at 118 m3/s is off it
RATING = [[2.36, 6.296, 6.409], [6.16, 6.388, 6.561], [14.7, 6.533, 6.686], [23.1, 6.631, 6.783],
          [32.04, 6.729, 6.896], [42.4, 6.816, 7.0], [54.46, 6.933, 7.143], [72.92, 7.053, 7.292],
          [97.06, 7.231, 7.562], [141.4, 7.502, 9.435], [958.0, 7.502, 9.435]]


def test_both_quantities_when_both_are_read():
    assert pub.live_quantities(25.7, 1.187, level_off_datum=False) == ["discharge", "level"]


def test_one_quantity_when_only_one_is_read():
    assert pub.live_quantities(None, 0.308, level_off_datum=False) == ["level"]           # Nazko, level only
    assert pub.live_quantities(12.0, None, level_off_datum=False) == ["discharge"]
    assert pub.live_quantities(None, None, level_off_datum=False) == []


def test_a_level_on_another_datum_is_not_reported():
    assert pub.live_quantities(118.0, 2.338, level_off_datum=True) == ["discharge"]


def _publish(tmp_path, monkeypatch, readings: dict, clim: dict) -> dict:
    monkeypatch.setattr(pub, "fetch_station", lambda st: {
        "station": st, "at": AT, "discharge": readings[st][0], "level": readings[st][1],
        "recent": [[AT, readings[st][1], readings[st][0]]]})
    monkeypatch.setattr(pub, "fetch_temperatures", lambda stations, hours=24: {})
    monkeypatch.setattr(pub, "_get", lambda url: None)
    monkeypatch.setattr(pub, "_sleep", lambda s: None)
    out = tmp_path / "feeds"
    pub.publish(out, sorted(readings), clim={"release": "test", **clim}, now=NOW)
    return json.loads((out / "index.json").read_text())["stations"]


def _bands(*qs: str) -> dict:
    return {q: {str(p): BAND[0] for p in range(73)} for q in qs}


def test_the_publisher_says_capilano_reports_its_level(tmp_path, monkeypatch):
    rows = _publish(tmp_path, monkeypatch, {"08GA031": (25.7, 1.187)}, {"stations": {"08GA031": _bands("discharge")}})
    cap = rows["08GA031"]
    assert cap["reports"] == ["discharge", "level"]
    assert "level" not in cap and cap["parameter"] == "discharge" and cap["discharge"] is not None  # no level RANK: no band
    assert list(cap)[:3] == ["percentile", "parameter", "reports"]           # the worker writes the same order


def test_the_publisher_leaves_an_other_datum_level_out(tmp_path, monkeypatch):
    rows = _publish(tmp_path, monkeypatch, {"08HD006": (118.0, 2.338)},
                    {"stations": {"08HD006": _bands("discharge", "level")}, "ratings": {"08HD006": RATING},
                     "ratingPadM": RATING_PAD_M})
    assert rows["08HD006"]["reports"] == ["discharge"] and "level" not in rows["08HD006"]


def test_an_old_reading_keeps_what_it_reported(tmp_path, monkeypatch):
    # age_gate drops the RANKS of a reading past the line, never what the reading was
    entry = {"percentile": 0.4, "parameter": "discharge", "reports": ["discharge", "level"], "discharge": 0.4,
             "observedAt": "2026-11-14T07:55:00-08:00"}
    assert pub.age_gate(entry, NOW)["reports"] == ["discharge", "level"]
