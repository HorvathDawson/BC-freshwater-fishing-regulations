"""Unit tests for hydro.jobs._compact_db — the persisted-DB slimming step.

Guards the invariants the nightly job relies on: sub-daily unit values (46/47)
are down-sampled to the 30-min marks recent/ emits and aged out beyond the recent
window, while daily means (3/6) that back history/ survive.
"""

import sqlite3
from datetime import datetime, timedelta, timezone

from pipeline.recurring.hydro import jobs


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_db(path):
    conn = sqlite3.connect(path)
    conn.execute(
        """CREATE TABLE readings (
            station_id TEXT NOT NULL, ts TEXT NOT NULL, parameter TEXT NOT NULL,
            value REAL, grade TEXT, symbol TEXT, approval TEXT, qualifier TEXT,
            source TEXT, attribution TEXT,
            PRIMARY KEY (station_id, ts, parameter))"""
    )
    return conn


def _insert(conn, ts, param, value=1.0):
    conn.execute(
        "INSERT OR REPLACE INTO readings (station_id, ts, parameter, value) "
        "VALUES ('08AA001', ?, ?, ?)",
        (ts, param, value),
    )


def _rows(conn, param):
    return {
        r[0]
        for r in conn.execute(
            "SELECT ts FROM readings WHERE parameter=?", (param,)
        ).fetchall()
    }


def test_compact_decimates_and_ages_unit_values(tmp_path):
    db = tmp_path / "hydro.db"
    conn = _make_db(db)
    now = datetime.now(timezone.utc)

    # Fresh (1 day old) unit values at 5-min cadence — only :00/:30 should survive.
    base = now - timedelta(days=1)
    on_mark, off_mark = [], []
    for m in (0, 5, 10, 15, 20, 25, 30, 35):
        ts = _iso(base.replace(minute=m, second=0, microsecond=0))
        _insert(conn, ts, "47")
        (on_mark if m % jobs.UNIT_STEP_MIN == 0 else off_mark).append(ts)

    # Unit value older than the retention window — pruned even though on-mark.
    aged_unit = _iso((now - timedelta(days=jobs.UNIT_RETAIN_DAYS + 2)).replace(
        minute=0, second=0, microsecond=0))
    _insert(conn, aged_unit, "46")

    # Daily means: recent one kept, one beyond the daily window pruned.
    recent_daily = _iso(now - timedelta(days=10))
    aged_daily = _iso(now - timedelta(days=jobs.DAILY_RETAIN_DAYS + 5))
    _insert(conn, recent_daily, "6")
    _insert(conn, aged_daily, "3")
    conn.commit()
    conn.close()

    jobs._compact_db(db)

    conn = sqlite3.connect(db)
    try:
        unit = _rows(conn, "47") | _rows(conn, "46")
        assert set(on_mark) <= unit, "on-30-min-mark unit values must survive"
        assert unit.isdisjoint(off_mark), "off-mark 5-min rows must be dropped"
        assert aged_unit not in unit, "aged unit value must be pruned"

        daily = _rows(conn, "6") | _rows(conn, "3")
        assert recent_daily in daily, "in-window daily mean must survive"
        assert aged_daily not in daily, "aged daily mean must be pruned"
    finally:
        conn.close()


def test_compact_no_readings_table_is_noop(tmp_path):
    db = tmp_path / "empty.db"
    sqlite3.connect(db).close()
    jobs._compact_db(db)  # must not raise when the table is absent
