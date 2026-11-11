"""T550 item 2: a level record is cut at its last datum shift, and a live level off the record's rating is not ranked.

Measured on HYDAT 20261019 over the BC roster: 25 level records shift (16 of them live), 08HD006 from 2016 (3.3 m -> 6.9 m
at mid flows) and 08LB024 from 2018 (0.3 m -> 1.9 m); on the 2026-11-10 staging feed 5 of 349 live (flow, level) pairs
are off their rating (08HD006, 07FD001, 08EF001, 08KH019, 08MH153), each ranking its level at an end of the band.
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest

from pipeline.gauges.feed.climatology import (RATING_PAD_M, build, level_record, level_shifts, rating_of)
from pipeline.gauges.feed.publish import level_on_rating

# 08HD006's mid-flow medians by year (HYDAT 20261019)
HD006 = [(2011, 3.34), (2012, 3.32), (2013, 3.34), (2014, 3.32), (2015, 3.42), (2016, 6.87), (2017, 6.82),
         (2018, 6.83), (2019, 6.88), (2020, 6.88), (2021, 6.88), (2022, 6.92), (2023, 6.89), (2024, 6.86)]


def test_the_salmon_near_sayward_shifts_in_2016():
    assert level_shifts(HD006) == [2016]


def test_a_steady_record_and_a_wet_year_are_no_shift():
    steady = [(y, 1.0 + 0.05 * (y % 3)) for y in range(1990, 2020)]
    assert level_shifts(steady) == []
    wet = steady[:15] + [(2005, 1.6)] + steady[16:]
    assert level_shifts(wet) == []


def test_the_last_of_two_shifts_is_found():
    two = [(y, 1.0) for y in range(1990, 2000)] + [(y, 3.0) for y in range(2000, 2010)] + [(y, 5.0) for y in range(2010, 2016)]
    assert level_shifts(two) == [2000, 2010]


def test_the_rating_bins_level_by_flow_decile():
    pairs = [(q, 2.0 + q / 100) for q in range(1, 401)]
    r = rating_of(pairs)
    assert r is not None and len(r) == 11
    assert r[0][0] == 1 and r[-1][0] == 400
    assert rating_of(pairs[:50]) is None


def test_a_live_pair_off_the_rating_is_refused_and_one_on_it_or_untestable_is_kept():
    rating = [[2.36, 6.296, 6.409], [6.16, 6.388, 6.561], [14.7, 6.533, 6.686], [23.1, 6.631, 6.783],
              [32.04, 6.729, 6.896], [42.4, 6.816, 7.0], [54.46, 6.933, 7.143], [72.92, 7.053, 7.292],
              [97.06, 7.231, 7.562], [141.4, 7.502, 9.435], [958.0, 7.502, 9.435]]  # 08HD006 since 2016
    assert not level_on_rating(rating, 118, 2.338, RATING_PAD_M)            # staging 2026-11-10
    assert level_on_rating(rating, 118, 7.4, RATING_PAD_M)
    assert level_on_rating(rating, None, 2.338, RATING_PAD_M)                # a level-only reading: nothing to pair
    assert level_on_rating(None, 118, 2.338, None)                           # no rating
    with pytest.raises(ValueError):
        level_on_rating(rating, 118, 2.338, None)


def _hydat(path: Path) -> None:
    db = sqlite3.connect(path)
    cols = ", ".join(f"{{c}}{d} REAL" for d in range(1, 32))
    for table, c in (("DLY_LEVELS", "LEVEL"), ("DLY_FLOWS", "FLOW")):
        db.execute(f"CREATE TABLE {table} (STATION_NUMBER TEXT, YEAR INT, MONTH INT, {cols.format(c=c)})")
    by_month: dict[tuple, dict] = {}
    d = date(2010, 1, 1)
    while d <= date(2021, 12, 31):
        q = 10.0 + (d.timetuple().tm_yday % 50)
        h = (1.0 if d.year < 2016 else 4.0) + q / 100
        by_month.setdefault((d.year, d.month), {})[d.day] = (h, q)
        d += timedelta(days=1)
    for (y, m), days in by_month.items():
        for table, k in (("DLY_LEVELS", 0), ("DLY_FLOWS", 1)):
            vals = [days.get(i, (None, None))[k] for i in range(1, 32)]
            db.execute(f"INSERT INTO {table} VALUES (?, ?, ?, {', '.join('?' * 31)})", ["X1", y, m, *vals])
    db.commit()
    db.close()


def test_the_envelope_pools_only_the_years_since_the_shift(tmp_path):
    p = tmp_path / "hydat.sqlite3"
    _hydat(p)
    db = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    rec = level_record(db, "X1")
    db.close()
    assert rec["shifts"] == [2016] and rec["since"] == 2016
    out = build(p, ["X1"], "test")
    level = out["stations"]["X1"]["level"]
    assert min(b[0] for b in level.values()) >= 4.0                          # nothing from the old datum
    assert out["stats"]["X1"]["level"]["from_year"] == 2016
    assert out["stats"]["X1"]["level"]["level_shifts"] == [2016]
    assert out["levelShifts"]["X1"] == {"since": 2016, "shifts": [2016], "ranked": True}
    assert out["ratings"]["X1"][0][1] >= 4.0
    assert out["stats"]["X1"]["discharge"]["from_year"] == 2010              # flow is untouched
