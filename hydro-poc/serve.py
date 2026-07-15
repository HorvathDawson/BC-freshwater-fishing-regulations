#!/usr/bin/env python3
"""
Tiny viewer for the hydrometric POC.

Serves index.html plus a couple of JSON endpoints backed by hydro.db:
  GET /api/stations                       -> stations that have readings
  GET /api/readings?station=..&parameter=.. -> time series for one param
  GET /api/attribution                    -> latest attribution strings

Run:
  python serve.py         # then open http://localhost:8765
"""

from __future__ import annotations

import json
import sqlite3
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).parent
DB_PATH = HERE / "hydro.db"
PORT = 8765


def query(sql: str, args: tuple = ()) -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql, args)]
    finally:
        conn.close()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # quieter console
        pass

    def _json(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # Client disconnected before response could be sent
            pass

    def _file(self, path: Path, content_type: str):
        if not path.exists():
            self.send_error(404)
            return
        body = path.read_bytes()
        try:
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # Client disconnected before response could be sent
            pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)

        if parsed.path in ("/", "/index.html"):
            self._file(HERE / "index.html", "text/html; charset=utf-8")
            return

        if parsed.path == "/api/stations":
            rows = query(
                """SELECT s.station_id, s.name, s.province,
                          COUNT(r.ts) AS n,
                          GROUP_CONCAT(DISTINCT r.parameter) AS parameters
                   FROM readings r
                   LEFT JOIN stations s ON s.station_id = r.station_id
                   GROUP BY r.station_id
                   ORDER BY r.station_id"""
            )
            self._json(rows)
            return

        if parsed.path == "/api/readings":
            station = (qs.get("station") or [""])[0]
            param = (qs.get("parameter") or [""])[0]
            if not station or not param:
                self._json({"error": "station and parameter required"}, 400)
                return
            rows = query(
                """SELECT ts, value, grade, approval, source
                   FROM readings
                   WHERE station_id = ? AND parameter = ? AND value IS NOT NULL
                   ORDER BY ts""",
                (station, param),
            )
            self._json(rows)
            return

        if parsed.path == "/api/attribution":
            rows = query(
                """SELECT source, attribution FROM readings
                   WHERE attribution IS NOT NULL
                   GROUP BY source"""
            )
            self._json(rows)
            return

        if parsed.path == "/api/forecast_models":
            # Available forecast models + how many of their stations also have
            # observed readings (so the extend-from-observed overlay works).
            rows = query(
                """SELECT f.model,
                          f.horizon_days,
                          COUNT(DISTINCT f.station_id) AS total,
                          COUNT(DISTINCT r.station_id) AS with_readings
                   FROM forecasts f
                   LEFT JOIN readings r ON r.station_id = f.station_id
                   GROUP BY f.model
                   ORDER BY f.model"""
            )
            self._json(rows)
            return

        if parsed.path == "/api/forecast_stations":
            model = (qs.get("model") or [""])[0]
            if not model:
                self._json({"error": "model required"}, 400)
                return
            # Stations that have this forecast AND observed readings.
            rows = query(
                """SELECT DISTINCT f.station_id, f.station_name AS name,
                          COUNT(r.ts) AS n
                   FROM forecasts f
                   JOIN readings r ON r.station_id = f.station_id
                   WHERE f.model = ?
                   GROUP BY f.station_id
                   ORDER BY f.station_id""",
                (model,),
            )
            self._json(rows)
            return

        if parsed.path == "/api/forecast":
            station = (qs.get("station") or [""])[0]
            model = (qs.get("model") or [""])[0]
            if not station:
                self._json({"error": "station required"}, 400)
                return
            sql = """SELECT model, station_id, station_name, issued_at,
                          horizon_days, obs_value, obs_rp, forecast_value,
                          forecast_min, forecast_ave, forecast_max, forecast_rp,
                          hydrograph_url
                     FROM forecasts WHERE station_id = ?"""
            params = [station]
            if model:
                sql += " AND model = ?"
                params.append(model)
            sql += " ORDER BY model"
            rows = query(sql, tuple(params))
            # Back-compat: single object when a model is specified.
            self._json((rows[0] if rows else {}) if model else rows)
            return

        if parsed.path == "/api/forecast_series":
            station = (qs.get("station") or [""])[0]
            model = (qs.get("model") or [""])[0]
            if not station:
                self._json({"error": "station required"}, 400)
                return
            sql = """SELECT model, date, qobs, qfor_min, qfor_ave, qfor_max,
                          hobs, hfor_min, hfor_ave, hfor_max
                     FROM forecast_series WHERE station_id = ?"""
            params = [station]
            if model:
                sql += " AND model = ?"
                params.append(model)
            sql += " ORDER BY model, date"
            rows = query(sql, tuple(params))
            self._json(rows)
            return

        if parsed.path == "/api/attributes":
            station = (qs.get("station") or [""])[0]
            if not station:
                self._json({"error": "station required"}, 400)
                return
            # Full station metadata row (all columns) — these are the fields
            # available for linking a gauge to a river/waterbody.
            meta = query("SELECT * FROM stations WHERE station_id = ?", (station,))
            station_meta = meta[0] if meta else {}
            # Reading columns + distinct values / examples so the user can see
            # every attribute the time-series carries.
            reading_cols = [r["name"] for r in query("PRAGMA table_info(readings)")]
            fields = []
            for col in reading_cols:
                info = query(
                    f"""SELECT
                          (SELECT COUNT(DISTINCT {col}) FROM readings
                             WHERE station_id = ? AND {col} IS NOT NULL) AS distinct_count,
                          (SELECT GROUP_CONCAT(v, ' | ') FROM
                             (SELECT DISTINCT {col} AS v FROM readings
                                WHERE station_id = ? AND {col} IS NOT NULL
                                LIMIT 8)) AS examples""",
                    (station, station),
                )
                fields.append({
                    "field": col,
                    "distinct": info[0]["distinct_count"] if info else 0,
                    "examples": info[0]["examples"] if info else "",
                })
            # Forecasts available for this station (with the referenced PDF).
            forecasts = query(
                """SELECT model, issued_at, horizon_days, forecast_min,
                          forecast_ave, forecast_max, forecast_value,
                          forecast_rp, hydrograph_url
                   FROM forecasts WHERE station_id = ? ORDER BY model""",
                (station,),
            )
            self._json({"station": station_meta, "reading_fields": fields,
                        "forecasts": forecasts})
            return

        self.send_error(404)


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(f"Hydro POC viewer -> http://localhost:{PORT}  (Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()