#!/usr/bin/env python3
"""
Tiny viewer for the hydrometric POC.

Serves index.html plus a couple of JSON endpoints backed by hydro.db:
  GET /api/stations                       -> stations that have readings
  GET /api/readings?station=..&parameter=.. -> time series for one param
  GET /api/current_conditions?station=..  -> live percentile classification
  GET /api/climatology?station=..&parameter=.. -> day-of-year percentile envelope
  GET /api/attribution                    -> latest attribution strings
  GET /api/cron                           -> background cron-loop status
  GET /hydro/<file>                       -> exported output/hydro artifacts

Run:
  python serve.py                    # viewer only, no background work
  python serve.py --cron 300         # every 5 min: fetch update + export realtime
  python serve.py --cron 300 --match-on-start   # also refresh gauge→fid once
  python serve.py --cron 900 --match-every 4    # re-run match_fwa every 4th cycle

The ``--cron`` loop is a **local stand-in for the production cron**: each tick
runs the same steps the scheduled job would — ``hydro_poc update`` (fetch head)
then ``export_hydro --scope realtime`` (which re-resolves each gauge's
fid→reach_id against the current deploy shards). The heavy gauge→fid
``match_fwa`` pass only needs re-running when the atlas changes, so it's opt-in
(``--match-on-start`` / ``--match-every N``), mirroring how the real pipeline
splits the frequent job from the atlas-bound one.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).parent
DB_PATH = HERE / "hydro.db"
OUTPUT_DIR = HERE / "output" / "hydro"

# Unit-value params (46/47) are stored at 5-min in hydro.db, but the viewer
# should show what production ships, not the raw feed — so downsample them to
# the SAME cadence as export_hydro's recent tier: 30-min for the last 3 days,
# hourly for days 3-14 (older unit values are dropped). Keep these in sync with
# export_hydro.FINE_DAYS / FINE_STEP_MIN / COARSE_DAYS.
UNIT_PARAMS = ("46", "47")
FINE_DAYS, FINE_STEP_MIN, COARSE_DAYS = 3, 30, 14


def _on_step(ts: str, step_min: int) -> bool:
    """True when an ISO timestamp's minute falls on a step boundary (:00/:30 for
    30) — keeps on-the-mark readings rather than blindly decimating."""
    try:
        return int(ts[14:16]) % step_min == 0
    except (ValueError, IndexError):
        return False


def _downsample_unit(rows: list[dict], now: datetime) -> list[dict]:
    """Mirror export_hydro's recent tier: 30-min over the last FINE_DAYS, hourly
    for days FINE..COARSE, nothing older. `rows` are ordered by ts ascending."""
    fine_cut = (now - timedelta(days=FINE_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    coarse_cut = (now - timedelta(days=COARSE_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    out = []
    for r in rows:
        ts = r["ts"]
        if ts >= fine_cut:
            if _on_step(ts, FINE_STEP_MIN):
                out.append(r)
        elif ts >= coarse_cut and _on_step(ts, 60):
            out.append(r)
    return out
PORT = 8765

# Background cron state, published at /api/cron (guarded by _CRON_LOCK).
_CRON_LOCK = threading.Lock()
_CRON_STATE: dict = {"enabled": False, "cycles": 0, "running": None, "steps": {}}


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class CronRunner(threading.Thread):
    """Runs the fetch/export (and optional match) cycle on an interval.

    Each step is a subprocess of the sibling scripts using this interpreter —
    the same commands the real cron invokes, so a green loop here is real
    evidence the scheduled job works end-to-end (fetch → reach-resolved export).
    """

    def __init__(self, interval: float, match_every: int, match_on_start: bool):
        super().__init__(daemon=True)
        self.interval = interval
        self.match_every = match_every
        self.match_on_start = match_on_start

    def _run(self, name: str, argv: list[str], timeout: float) -> None:
        with _CRON_LOCK:
            _CRON_STATE["running"] = name
        start = time.time()
        ok, msg = False, ""
        try:
            proc = subprocess.run(
                [sys.executable, *argv], cwd=str(HERE),
                capture_output=True, text=True, timeout=timeout,
            )
            ok = proc.returncode == 0
            out = (proc.stdout or "").strip().splitlines()
            err = (proc.stderr or "").strip().splitlines()
            msg = (out[-1] if ok and out else
                   (err[-1] if err else out[-1] if out else "")) [:300]
        except subprocess.TimeoutExpired:
            msg = f"timed out after {timeout:.0f}s"
        except Exception as exc:  # noqa: BLE001 — surface any launch failure
            msg = f"{type(exc).__name__}: {exc}"
        dur = round(time.time() - start, 1)
        with _CRON_LOCK:
            _CRON_STATE["steps"][name] = {
                "ok": ok, "duration_s": dur, "message": msg, "at": _now_iso(),
            }
            _CRON_STATE["running"] = None
        print(f"[cron] {name}: {'ok' if ok else 'FAIL'} ({dur}s) {msg}")

    def _match(self) -> None:
        self._run("match_fwa", [str(HERE / "match_fwa.py")], timeout=600)

    def _cycle(self) -> None:
        self._run("update", [str(HERE / "hydro_poc.py"), "update", "--bc"], timeout=1200)
        self._run("export", [str(HERE / "export_hydro.py"), "--scope", "realtime"], timeout=300)

    def run(self) -> None:
        with _CRON_LOCK:
            _CRON_STATE.update(enabled=True, interval_s=self.interval,
                               match_every=self.match_every)
        if self.match_on_start:
            self._match()
        while True:
            with _CRON_LOCK:
                _CRON_STATE["cycles"] += 1
                cycle = _CRON_STATE["cycles"]
            self._cycle()
            if self.match_every and cycle % self.match_every == 0:
                self._match()
            with _CRON_LOCK:
                _CRON_STATE["last_cycle_at"] = _now_iso()
            time.sleep(self.interval)


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

        if parsed.path == "/api/cron":
            with _CRON_LOCK:
                self._json(json.loads(json.dumps(_CRON_STATE)))
            return

        if parsed.path.startswith("/hydro/"):
            # Serve the exported artifacts (stations.json, recent/<id>.json, …)
            # so the cron output is inspectable in the same viewer.
            rel = parsed.path[len("/hydro/"):]
            target = (OUTPUT_DIR / rel).resolve()
            if OUTPUT_DIR.resolve() not in target.parents or not target.is_file():
                self.send_error(404)
                return
            self._file(target, "application/json")
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
            # Serve unit values at the shipped cadence (30-min/hourly), not the
            # raw 5-min feed, so the viewer previews production faithfully.
            if param in UNIT_PARAMS:
                rows = _downsample_unit(rows, datetime.now(timezone.utc))
            self._json(rows)
            return

        if parsed.path == "/api/climatology":
            station = (qs.get("station") or [""])[0]
            param = (qs.get("parameter") or [""])[0]
            if not station or not param:
                self._json({"error": "station and parameter required"}, 400)
                return
            # Daily-mean params carry the climatology (46/47 unit -> 6/3 daily).
            param = {"47": "6", "46": "3"}.get(param, param)
            meta = query(
                """SELECT start_year, end_year, n_years, window_days,
                          hydat_release_date, attribution
                   FROM flow_climatology_meta
                   WHERE station_id = ? AND parameter = ?""",
                (station, param),
            )
            if not meta:
                self._json({})  # no envelope for this station/parameter
                return
            rows = query(
                """SELECT doy, p0, p10, p25, p50, p75, p90, p100, n_obs
                   FROM flow_climatology
                   WHERE station_id = ? AND parameter = ?
                   ORDER BY doy""",
                (station, param),
            )
            self._json({"station_id": station, "parameter": param,
                        "record": meta[0], "bands": rows})
            return

        if parsed.path == "/api/attribution":
            # ECCC observed/HYDAT attribution (from readings) + the separate
            # BC River Forecast Centre (Province of BC) forecast attribution.
            rows = query(
                """SELECT source, attribution FROM readings
                   WHERE attribution IS NOT NULL
                   GROUP BY source"""
            )
            fc = query(
                """SELECT attribution FROM forecasts
                   WHERE attribution IS NOT NULL LIMIT 1"""
            )
            if fc:
                rows.append({"source": "BCRFC forecast", "attribution": fc[0]["attribution"]})
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

        if parsed.path == "/api/current_conditions":
            station = (qs.get("station") or [""])[0]
            if not station:
                self._json({"error": "station required"}, 400)
                return
            rows = query(
                "SELECT * FROM current_conditions WHERE station_id = ?", (station,)
            )
            self._json(rows[0] if rows else {})
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
            cc = query("SELECT * FROM current_conditions WHERE station_id = ?", (station,))
            current_condition = cc[0] if cc else {}
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
                        "forecasts": forecasts,
                        "current_condition": current_condition})
            return

        self.send_error(404)


def main() -> None:
    ap = argparse.ArgumentParser(description="Hydro POC viewer + local cron test")
    ap.add_argument("--cron", type=float, default=0, metavar="SECONDS",
                    help="run the fetch+export cycle every N seconds (0 = off)")
    ap.add_argument("--match-every", type=int, default=0, metavar="N",
                    help="re-run the heavy gauge→fid match_fwa every Nth cycle (0 = never)")
    ap.add_argument("--match-on-start", action="store_true",
                    help="run match_fwa once before the first cron cycle")
    ap.add_argument("--port", type=int, default=PORT)
    args = ap.parse_args()

    if args.cron > 0:
        print(f"[cron] loop enabled: every {args.cron:.0f}s "
              f"(match_on_start={args.match_on_start}, match_every={args.match_every})")
        CronRunner(args.cron, args.match_every, args.match_on_start).start()

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"Hydro POC viewer -> http://localhost:{args.port}  (Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()