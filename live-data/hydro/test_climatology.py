#!/usr/bin/env python3
"""
Unit tests for the HYDAT day-of-year percentile climatology (fetch_hydat.py).

Focus is data integrity: percentile math, day-of-year slotting (incl. the
Feb-29 edge and year-boundary window wrap), the min-years / min-obs publish
gates, and an end-to-end compute against a synthetic HYDAT DLY_FLOWS table.

    python test_climatology.py            # or: python -m unittest test_climatology
"""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

import fetch_hydat as fh
import hydro_poc


class TestPercentiles(unittest.TestCase):
    def test_linear_interpolation_matches_r7(self):
        vals = list(range(1, 101))  # 1..100, already sorted
        p0, p25, p50, p75, p100 = fh._percentiles(vals, (0, 25, 50, 75, 100))
        self.assertEqual(p0, 1)
        self.assertEqual(p100, 100)
        self.assertAlmostEqual(p50, 50.5)
        self.assertAlmostEqual(p25, 25.75)
        self.assertAlmostEqual(p75, 75.25)

    def test_single_value(self):
        self.assertEqual(fh._percentiles([7.0], (0, 50, 100)), [7.0, 7.0, 7.0])


class TestDoy(unittest.TestCase):
    def test_known_slots(self):
        self.assertEqual(fh._doy(1, 1), 1)
        self.assertEqual(fh._doy(2, 29), 60)   # leap-year calendar keeps a slot
        self.assertEqual(fh._doy(3, 1), 61)
        self.assertEqual(fh._doy(12, 31), 366)


class TestPooling(unittest.TestCase):
    def test_window_wraps_year_boundary(self):
        by_doy = {365: [10.0], 366: [11.0], 1: [1.0], 2: [2.0], 3: [3.0]}
        pooled = fh._pooled_windows(by_doy, 2)
        # doy 1 pools 364..3 -> slots 365,366,1,2,3 present here.
        self.assertEqual(pooled[1], [1.0, 2.0, 3.0, 10.0, 11.0])
        # returned sorted for percentile use.
        self.assertEqual(pooled[1], sorted(pooled[1]))

    def test_empty_slots_absent(self):
        pooled = fh._pooled_windows({100: [5.0]}, 0)
        self.assertIn(100, pooled)
        self.assertNotIn(200, pooled)


def _make_hydat(path: Path, station: str, years, month, flows_per_day):
    """Write a minimal HYDAT-shaped DLY_FLOWS: one row per (year, month) with
    FLOW1..FLOW31. `flows_per_day` maps day-of-month -> value (others NULL)."""
    conn = sqlite3.connect(path)
    cols = ", ".join(f"FLOW{d} REAL" for d in range(1, 32))
    conn.execute(f"CREATE TABLE DLY_FLOWS (STATION_NUMBER TEXT, YEAR INT, MONTH INT, {cols})")
    for y in years:
        row = [flows_per_day.get(d) for d in range(1, 32)]
        conn.execute(
            "INSERT INTO DLY_FLOWS VALUES (?,?,?," + ",".join("?" * 31) + ")",
            [station, y, month, *row])
    conn.commit()
    conn.close()


class TestComputeClimatology(unittest.TestCase):
    def test_end_to_end_and_gates(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            hydat = tmp / "hydat.sqlite3"
            # Two stations: one with 12 years (publishes), one with 3 (gated out).
            _make_hydat(hydat, "08AA001", range(2000, 2012), 7,
                        {15: 5.0, 16: 5.0, 17: 5.0})
            # Append the short-record station into the same DLY_FLOWS table.
            c = sqlite3.connect(hydat)
            for y in range(2000, 2003):
                row = [None] * 31
                row[14] = 9.0  # day 15
                c.execute("INSERT INTO DLY_FLOWS VALUES (?,?,?," + ",".join("?" * 31) + ")",
                          ["08BB002", y, 7, *row])
            c.commit(); c.close()

            db = tmp / "hydro.db"
            conn = hydro_poc.connect(db)
            conn.execute("INSERT INTO stations (station_id, province) VALUES ('08AA001','BC')")
            conn.execute("INSERT INTO stations (station_id, province) VALUES ('08BB002','BC')")
            conn.commit()

            summary = fh.compute_climatology(conn, hydat, "20260715")

            # Only the long-record station is published.
            self.assertEqual(summary["discharge_stations"], 1)
            meta = conn.execute(
                "SELECT start_year, end_year, n_years FROM flow_climatology_meta "
                "WHERE station_id='08AA001' AND parameter='6'").fetchone()
            self.assertEqual(meta, (2000, 2011, 12))
            self.assertIsNone(conn.execute(
                "SELECT 1 FROM flow_climatology_meta WHERE station_id='08BB002'").fetchone())

            # Day-of-year 197 (Jul 15) pools 3 days x 12 years = 36 obs, all 5.0.
            row = conn.execute(
                "SELECT p0, p50, p100, n_obs FROM flow_climatology "
                "WHERE station_id='08AA001' AND parameter='6' AND doy=?",
                (fh._doy(7, 16),)).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], 5.0)
            self.assertEqual(row[1], 5.0)
            self.assertEqual(row[2], 5.0)
            self.assertEqual(row[3], 36)

            # A day-of-year far from July has no pooled sample -> no row.
            self.assertIsNone(conn.execute(
                "SELECT 1 FROM flow_climatology WHERE station_id='08AA001' AND doy=?",
                (fh._doy(1, 15),)).fetchone())
            conn.close()

    def test_rerun_replaces_prior_climatology(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            hydat = tmp / "hydat.sqlite3"
            _make_hydat(hydat, "08AA001", range(2000, 2012), 7, {15: 5.0, 16: 5.0, 17: 5.0})
            db = tmp / "hydro.db"
            conn = hydro_poc.connect(db)
            conn.execute("INSERT INTO stations (station_id, province) VALUES ('08AA001','BC')")
            conn.commit()
            fh.compute_climatology(conn, hydat, "20260701")
            n1 = conn.execute("SELECT COUNT(*) FROM flow_climatology").fetchone()[0]
            fh.compute_climatology(conn, hydat, "20260715")  # rerun, same data
            n2 = conn.execute("SELECT COUNT(*) FROM flow_climatology").fetchone()[0]
            self.assertEqual(n1, n2)  # replaced wholesale, not duplicated
            rel = conn.execute(
                "SELECT DISTINCT hydat_release_date FROM flow_climatology_meta").fetchall()
            self.assertEqual(rel, [("20260715",)])
            conn.close()


if __name__ == "__main__":
    unittest.main()
