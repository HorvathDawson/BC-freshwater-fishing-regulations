#!/usr/bin/env python3
"""
export_hydro.py — shape hydro.db into tiered, R2-ready JSON artifacts.

The data changes at three different rates, so it's split into three tiers,
each written as its own object(s) so a frequent cron only rewrites the small
fast-changing ones and never regenerates the slow historical record:

  output/hydro/
    stations.json          roster + metadata + latest value + percentile per
                           station — the map index / headline. One small file.
    recent/<id>.json       per station: discharge & level at 30-min for the last
                           3 days, hourly for days 3-14, plus forecast series.
                           ~10-30 KB each.
    history/<id>.json      per station: daily-mean discharge & level, long record.
    climatology/<id>.json  per station: day-of-year percentile envelope
                           (P0/P10/P25/P50/P75/P90/P100) from HYDAT's full
                           record — the "seasonal" chart's historical bands.
                           Only present for stations with enough record.

Scopes (which tiers to (re)write):
    --scope realtime   stations.json + recent/       (frequent cron)
    --scope history    history/ + climatology/       (daily cron)
    --scope all        everything                    (bootstrap / manual; default)

climatology/ rides the history (daily) scope, not realtime: the underlying
flow_climatology table only changes when a new HYDAT release is synced
(~biweekly, bootstrap-only), so a daily re-serialize is cheap and idempotent
and picks up a fresh envelope the day after a sync — it never belongs in the
frequent realtime cron.

This writes local files only — a separate step uploads output/hydro/ to R2
(same pattern as the in-season pipeline). Read the artifacts directly (static,
R2-style) or via serve.py for local inspection.

CLI
---
    python export_hydro.py                 # --scope all
    python export_hydro.py --scope realtime
    python export_hydro.py --out some/dir
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .hydro_poc import DB_PATH


def _default_out_dir() -> Path:
    """Where the R2-ready hydro artifacts are written.

    Precedence: ``HYDRO_OUT_DIR`` env override → config ``output.pipeline.cron``
    + ``hydro`` (the unified cron subtree, deploy/cron/hydro) → repo-relative
    fallback. Env override keeps this portable to a container with no repo.
    """
    env = os.environ.get("HYDRO_OUT_DIR")
    if env:
        return Path(env)
    try:
        from project_config import ProjectConfig

        return ProjectConfig().get_path(
            "output", "pipeline", "cron", default="output/pipeline/deploy/cron"
        ) / "hydro"
    except Exception:
        return (
            Path(__file__).resolve().parents[3]
            / "output"
            / "pipeline"
            / "deploy"
            / "cron"
            / "hydro"
        )


HERE = Path(__file__).parent
DEFAULT_OUT = _default_out_dir()

# Parameter ids (see hydro_poc.PARAMETERS)
P_LEVEL_DAILY, P_DISCHARGE_DAILY = "3", "6"
P_LEVEL_UNIT, P_DISCHARGE_UNIT = "46", "47"

# Climatology parameter -> (json key, unit) for the seasonal envelope export.
_CLIMATOLOGY_PARAMS = {P_DISCHARGE_DAILY: ("discharge", "m³/s"),
                       P_LEVEL_DAILY: ("level", "m")}
_PCTL_COLS = ("p0", "p10", "p25", "p50", "p75", "p90", "p100")

# Recent-tier windows / resolutions
FINE_DAYS = 3        # last 3 days at 30-min
FINE_STEP_MIN = 30
COARSE_DAYS = 14     # days 3-14 at hourly
HISTORY_DAYS = 550   # ~18 months of daily means


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _on_step(ts: str, step_min: int) -> bool:
    """True if an ISO timestamp's minute falls on a step boundary (e.g. :00,
    :15, :30, :45 for step 15) — keeps the on-the-mark readings when
    downsampling 5-min data, rather than blind decimation."""
    try:
        minute = int(ts[14:16])
    except (ValueError, IndexError):
        return False
    return minute % step_min == 0


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------

def _sig(x, figs: int = 4):
    """Round to `figs` significant figures, stripping HYDAT's float-storage
    noise (0.03799999877 -> 0.038) so the exported envelope is both clean and
    compact. None passes through; integers stay integers."""
    if x is None or x == 0:
        return x
    from math import floor, log10
    d = figs - 1 - floor(log10(abs(x)))
    r = round(x, d)
    return int(r) if d <= 0 else r


def _table_exists(conn, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _series(conn, sid, param, since, until=None):
    q = ("SELECT ts, value FROM readings WHERE station_id=? AND parameter=? "
         "AND value IS NOT NULL AND ts>=?")
    a = [sid, param, since]
    if until:
        q += " AND ts<?"
        a.append(until)
    return conn.execute(q + " ORDER BY ts", a).fetchall()


def _recent_param(conn, sid, unit_param, now):
    """30-min for the last FINE_DAYS + hourly for days FINE..COARSE, from the
    5-min unit-value series."""
    t_fine = _iso(now - timedelta(days=FINE_DAYS))
    t_coarse = _iso(now - timedelta(days=COARSE_DAYS))
    fine = [[ts, v] for ts, v in _series(conn, sid, unit_param, t_fine)
            if _on_step(ts, FINE_STEP_MIN)]
    coarse = [[ts, v] for ts, v in _series(conn, sid, unit_param, t_coarse, t_fine)
              if _on_step(ts, 60)]
    return fine, coarse


def _latest(conn, sid, unit_param, daily_param):
    """Latest reading for a station — prefer the 5-min unit value, fall back to
    the daily mean."""
    for param in (unit_param, daily_param):
        row = conn.execute(
            "SELECT ts, value FROM readings WHERE station_id=? AND parameter=? "
            "AND value IS NOT NULL ORDER BY ts DESC LIMIT 1", (sid, param)).fetchone()
        if row:
            return {"value": row[1], "ts": row[0]}
    return None


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

# NOTE: the gauge→reach/waterbody link is NO LONGER resolved here. It is a static
# artifact (gauge_matches.json) produced once during the full pipeline run (see
# match_fwa.py); the frontend joins reach_id → station_id at lookup time. This
# cron only fetches + shapes per-station data keyed by station_id, so it needs no
# atlas, no fid shards, and no gauge_fwa_match table.


def build_stations_index(conn, clim_station_ids: set | None = None) -> dict:
    """Roster + metadata + latest value + percentile — the map/headline file.

    ``clim_station_ids`` overrides the has_climatology flag source. The unified
    cron runs on an ephemeral scratch DB that only has the climatology table on
    HYDAT-refresh runs, so it passes the flag set preserved from the prior
    stations.json; when None we derive it from the DB (full/pipeline runs).
    """
    now = _iso(datetime.now(timezone.utc))
    attribution = conn.execute(
        "SELECT attribution FROM readings WHERE attribution IS NOT NULL LIMIT 1"
    ).fetchone()
    # BC River Forecast Centre forecasts carry a distinct Province-of-BC
    # attribution + disclaimer (see hydro_poc.attribution('bcrfc')).
    forecast_attribution = conn.execute(
        "SELECT attribution FROM forecasts WHERE attribution IS NOT NULL LIMIT 1"
    ).fetchone()
    clim_stations = _climatology_stations(conn) if clim_station_ids is None else clim_station_ids
    stations = []
    for s in conn.execute(
        """SELECT station_id, name, lat, lon, hyd_status,
                  drainage_area_gross_km2, coord_source
           FROM stations ORDER BY station_id"""
    ):
        sid = s[0]
        cc = conn.execute(
            """SELECT condition_class, percentile_low, percentile_high,
                      percentile_text, latest_discharge, latest_stage
               FROM current_conditions WHERE station_id=?""", (sid,)).fetchone()
        stations.append({
            "id": sid, "name": s[1], "lat": s[2], "lon": s[3],
            "hyd_status": s[4], "drainage_area_km2": s[5], "coord_source": s[6],
            "has_climatology": sid in clim_stations,
            "latest": {
                "discharge": _latest(conn, sid, P_DISCHARGE_UNIT, P_DISCHARGE_DAILY),
                "level": _latest(conn, sid, P_LEVEL_UNIT, P_LEVEL_DAILY),
            },
            "condition": None if not cc else {
                "class": cc[0], "pct_low": cc[1], "pct_high": cc[2],
                "text": cc[3], "latest_discharge": cc[4], "latest_stage": cc[5],
            },
        })
    from pipeline.recurring.provenance import provenance

    base = provenance(
        generator="export_hydro",
        source="Environment and Climate Change Canada (Wateroffice) + BC River Forecast Centre",
        source_url="https://wateroffice.ec.gc.ca/",
        attribution=attribution[0] if attribution else None,
    )
    return {**base,
            "generated_at": now,  # keep the DB-derived timestamp as the canonical one
            "forecast_attribution": forecast_attribution[0] if forecast_attribution else None,
            "count": len(stations), "stations": stations}


def build_geojson(index: dict) -> dict:
    """A GeoJSON FeatureCollection of gauge points for the map layer, derived
    from the already-built stations index (so latest value + condition + the
    has_climatology flag ride along without a second DB pass). Stations with no
    coordinate are omitted (they can't be placed on the map)."""
    features = []
    for st in index.get("stations", []):
        lon, lat = st.get("lon"), st.get("lat")
        if lon is None or lat is None:
            continue
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "id": st["id"],
                "name": st.get("name"),
                "hyd_status": st.get("hyd_status"),
                "has_climatology": st.get("has_climatology", False),
                "latest": st.get("latest"),
                "condition": st.get("condition"),
            },
        })
    return {"type": "FeatureCollection", "features": features}


def build_recent(conn, sid, now) -> dict:
    d_fine, d_coarse = _recent_param(conn, sid, P_DISCHARGE_UNIT, now)
    l_fine, l_coarse = _recent_param(conn, sid, P_LEVEL_UNIT, now)
    forecast = {}
    for model, date, mn, av, mx in conn.execute(
        """SELECT model, date, qfor_min, qfor_ave, qfor_max
           FROM forecast_series WHERE station_id=? ORDER BY model, date""", (sid,)):
        forecast.setdefault(model, []).append([date, mn, av, mx])
    return {
        "id": sid, "generated_at": _iso(now),
        "discharge": {"q30m_3d": d_fine, "q1h_3to14d": d_coarse},
        "level": {"q30m_3d": l_fine, "q1h_3to14d": l_coarse},
        "forecast": forecast,
    }


def build_history(conn, sid) -> dict:
    since = _iso(datetime.now(timezone.utc) - timedelta(days=HISTORY_DAYS))
    return {
        "id": sid,
        "discharge_daily": [[ts[:10], v] for ts, v in _series(conn, sid, P_DISCHARGE_DAILY, since)],
        "level_daily": [[ts[:10], v] for ts, v in _series(conn, sid, P_LEVEL_DAILY, since)],
    }


def _climatology_stations(conn) -> set:
    """station_ids that have a published climatology (for the index flag)."""
    if not _table_exists(conn, "flow_climatology_meta"):
        return set()
    return {r[0] for r in conn.execute(
        "SELECT DISTINCT station_id FROM flow_climatology_meta")}


def build_climatology(conn, sid) -> dict | None:
    """Per-station seasonal envelope: for each parameter with a published
    climatology, dense 366-length percentile arrays (index 0 = doy 1; NULL
    where a day-of-year didn't clear the min-sample gate) + record provenance.
    Returns None if the station has no climatology at all."""
    out = {"id": sid}
    have_any = False
    for param, (key, unit) in _CLIMATOLOGY_PARAMS.items():
        meta = conn.execute(
            "SELECT start_year, end_year, n_years, window_days, "
            "       hydat_release_date, attribution "
            "FROM flow_climatology_meta WHERE station_id=? AND parameter=?",
            (sid, param)).fetchone()
        if not meta:
            out[key] = None
            continue
        have_any = True
        cols = ", ".join(_PCTL_COLS)
        series = {c: [None] * 366 for c in _PCTL_COLS}
        n = [None] * 366
        for row in conn.execute(
                f"SELECT doy, {cols}, n_obs FROM flow_climatology "
                "WHERE station_id=? AND parameter=?", (sid, param)):
            doy = row[0]
            if not (1 <= doy <= 366):
                continue
            for i, c in enumerate(_PCTL_COLS, start=1):
                series[c][doy - 1] = _sig(row[i])
            n[doy - 1] = row[len(_PCTL_COLS) + 1]
        out[key] = {
            "unit": unit,
            "record": {"start_year": meta[0], "end_year": meta[1],
                       "n_years": meta[2], "window_days": meta[3]},
            "hydat_release_date": meta[4],
            "attribution": meta[5],
            **series,
            "n": n,
        }
    return out if have_any else None


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

def _write(path: Path, payload: dict) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    blob = json.dumps(payload, separators=(",", ":"))
    path.write_text(blob)
    return len(blob)


def export(conn, out_dir: Path, scope: str, clim_station_ids: set | None = None) -> None:
    now = datetime.now(timezone.utc)
    station_ids = [r[0] for r in conn.execute("SELECT station_id FROM stations ORDER BY station_id")]

    if scope in ("realtime", "all"):
        index = build_stations_index(conn, clim_station_ids=clim_station_ids)
        size = _write(out_dir / "stations.json", index)
        print(f"stations.json: {len(station_ids)} stations, {size/1024:.1f} KB")
        gsize = _write(out_dir / "gauges.geojson", build_geojson(index))
        print(f"gauges.geojson: {gsize/1024:.1f} KB")
        total = 0
        for sid in station_ids:
            total += _write(out_dir / "recent" / f"{sid}.json", build_recent(conn, sid, now))
        print(f"recent/: {len(station_ids)} files, {total/1024/1024:.2f} MB total")

    if scope in ("history", "all"):
        total = 0
        for sid in station_ids:
            total += _write(out_dir / "history" / f"{sid}.json", build_history(conn, sid))
        print(f"history/: {len(station_ids)} files, {total/1024/1024:.2f} MB total")

        clim_total = clim_n = 0
        for sid in station_ids:
            payload = build_climatology(conn, sid)
            if payload is None:
                continue  # no envelope for this station — omit the file entirely
            clim_total += _write(out_dir / "climatology" / f"{sid}.json", payload)
            clim_n += 1
        print(f"climatology/: {clim_n} files, {clim_total/1024/1024:.2f} MB total")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1] if __doc__ else "")
    parser.add_argument("--scope", choices=("realtime", "history", "all"), default="all",
                        help="which tiers to (re)write (default: all)")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT,
                        help=f"output directory (default: {DEFAULT_OUT})")
    parser.add_argument("--db", type=Path, default=DB_PATH, help="hydro.db path")
    args = parser.parse_args(argv)

    if not args.db.exists():
        raise FileNotFoundError(f"{args.db} not found — run hydro_poc.py bootstrap first.")

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    try:
        export(conn, args.out, args.scope)
    finally:
        conn.close()
    print(f"Exported ({args.scope}) -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
