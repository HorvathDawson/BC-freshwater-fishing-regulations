#!/usr/bin/env python3
"""
fetch_hydat.py — bulk station metadata sync from ECCC's HYDAT database.

hydro_poc.py discovers stations via the Real-time Station Search (HTML
scrape) and backfills coordinates via the Current Conditions KML feed.
That KML feed only carries stations with a discharge rating (it exists to
show a percentile classification, which needs a stage-discharge curve) —
so it silently excludes every level-only station (lakes, harbours, the
tidally-influenced lower Fraser), leaving a real chunk of active stations
with no coordinate at all.

HYDAT is ECCC's bulk single-file SQLite export of every hydrometric
station Canada has ever operated (Active + Discontinued), refreshed
roughly every two weeks, at:
    https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/Hydat_sqlite3_<date>.zip
Its STATIONS table carries real LATITUDE/LONGITUDE (plain decimals, no
DMS parsing needed), HYD_STATUS ('A'/'D'), drainage area, and a REAL_TIME
flag — everything the KML feed is missing, for every station at once.

This module downloads the current release (skipping the ~266MB pull if
we're already synced to it), extracts just the BC rows, and UPDATEs
hydro.db's own `stations` table — never INSERTs. HYDAT enriches stations
`fetch_stations()` already discovered; it doesn't drive discovery itself,
since a brand-new station can lag days behind the latest HYDAT release.

CLI
---
    python fetch_hydat.py            # sync if a newer release is available
    python fetch_hydat.py --force    # re-sync even if already up to date
"""

from __future__ import annotations

import argparse
import re
import shutil
import sqlite3
import sys
import tempfile
import time
import urllib.request
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

from hydro_poc import DB_PATH, USER_AGENT, _SSL_CTX, attribution, connect

HYDAT_INDEX_URL = "https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/"
_RELEASE_RE = re.compile(r'href="(Hydat_sqlite3_(\d{8})\.zip)"')

# STATIONS columns we care about, alongside the hydro.db column each fills.
_COLUMN_MAP = {
    "HYD_STATUS": "hyd_status",
    "DRAINAGE_AREA_GROSS": "drainage_area_gross_km2",
    "DRAINAGE_AREA_EFFECT": "drainage_area_effective_km2",
    "RHBN": "rhbn",
    "REAL_TIME": "real_time_flag",
}

# --- Percentile climatology (from HYDAT DLY_FLOWS / DLY_LEVELS) ------------
#
# For each tracked station we pool the entire daily record by day-of-year and
# compute a percentile envelope (P0/P10/P25/P50/P75/P90/P100). That envelope is
# what the "seasonal" chart shades today's flow against.
#
# Data-integrity gates (a sparse envelope is worse than none — it invites a
# false "record low" read from three years of noise):
#   * MIN_YEARS       — a (station, parameter) needs at least this many distinct
#                       years of record to publish a climatology at all.
#   * MIN_OBS_PER_DOY — a single day-of-year percentile is emitted only when its
#                       pooled sample (this day ± WINDOW_DAYS, across all years)
#                       reaches this many observations; sparser days stay NULL.
#   * WINDOW_DAYS     — centered half-width (wrapping the year boundary) the
#                       day-of-year pool draws from, to smooth the envelope so a
#                       single freak day doesn't spike one calendar day.
MIN_YEARS = 10
MIN_OBS_PER_DOY = 10
WINDOW_DAYS = 2

# HYDAT daily tables: (table, per-day value column stem, our parameter id).
_DAILY_TABLES = (
    ("DLY_FLOWS", "FLOW", "6"),    # discharge (m³/s)
    ("DLY_LEVELS", "LEVEL", "3"),  # water level (m)
)
_PCTLS = (0, 10, 25, 50, 75, 90, 100)
# Day-of-year slot for a (month, day), on a fixed leap-year calendar so every
# calendar date always maps to the same 1..366 slot (Feb 29 -> 60).
_LEAP_JAN1 = date(2000, 1, 1)


def _doy(month: int, day: int) -> int:
    return (date(2000, month, day) - _LEAP_JAN1).days + 1


def _percentiles(sorted_vals: list[float], pcts: tuple[int, ...]) -> list[float]:
    """Linear-interpolation percentiles (same method as numpy's default
    'linear' / R-7), computed for many percentiles over one pre-sorted list."""
    n = len(sorted_vals)
    out = []
    for p in pcts:
        if n == 1:
            out.append(sorted_vals[0])
            continue
        rank = (p / 100) * (n - 1)
        lo = int(rank)
        frac = rank - lo
        if lo + 1 < n:
            out.append(sorted_vals[lo] + frac * (sorted_vals[lo + 1] - sorted_vals[lo]))
        else:
            out.append(sorted_vals[lo])
    return out


def _pooled_windows(by_doy: dict[int, list[float]], window: int) -> dict[int, list[float]]:
    """For each of the 366 slots, gather values from slots within `window`
    (circular over the 366-day leap-year calendar) and return them sorted."""
    pooled: dict[int, list[float]] = {}
    for target in range(1, 367):
        bucket: list[float] = []
        for off in range(-window, window + 1):
            slot = (target - 1 + off) % 366 + 1
            vals = by_doy.get(slot)
            if vals:
                bucket.extend(vals)
        if bucket:
            bucket.sort()
            pooled[target] = bucket
    return pooled


def _read_daily(hydat_conn, table, stem, station_ids):
    """Yield (station_id, doy, year, value) for every non-NULL day in `table`
    for the given stations. HYDAT stores one row per station-month with the
    day values in FLOW1..FLOW31 / LEVEL1..LEVEL31 (NULL for absent days)."""
    day_cols = [f"{stem}{d}" for d in range(1, 32)]
    id_list = sorted(station_ids)
    for i in range(0, len(id_list), 500):  # keep under SQLite's variable cap
        chunk = id_list[i:i + 500]
        placeholders = ",".join("?" * len(chunk))
        sql = (f"SELECT STATION_NUMBER, YEAR, MONTH, {', '.join(day_cols)} "
               f"FROM {table} WHERE STATION_NUMBER IN ({placeholders})")
        for row in hydat_conn.execute(sql, chunk):
            sid, year, month = row[0], row[1], row[2]
            for day, val in enumerate(row[3:], start=1):
                if val is None:
                    continue
                try:
                    doy = _doy(month, day)
                except ValueError:
                    continue  # e.g. day 31 in a 30-day month (unused HYDAT slot)
                yield sid, doy, year, val


def compute_climatology(
    conn: sqlite3.Connection, hydat_path: Path, release_date: str
) -> dict:
    """(Re)build flow_climatology + flow_climatology_meta from the extracted
    HYDAT release, for every station hydro.db tracks. Fully replaces any prior
    climatology (a new release supersedes it wholesale). Returns a small summary.

    Runs inside sync() while the ~266MB HYDAT file is already on disk, so it
    costs no extra download — hence it rides the same slow cadence as the
    metadata merge and never touches the frequent `update` path.
    """
    station_ids = {r[0] for r in conn.execute("SELECT station_id FROM stations")}
    now = datetime.now(timezone.utc).isoformat()
    attr = attribution("hydat", release_date=release_date)

    hydat_conn = sqlite3.connect(f"file:{hydat_path}?mode=ro", uri=True)
    conn.execute("DELETE FROM flow_climatology")
    conn.execute("DELETE FROM flow_climatology_meta")

    published = {"6": 0, "3": 0}
    try:
        for table, stem, param in _DAILY_TABLES:
            if not _table_exists(hydat_conn, table):
                print(f"  {table} not present in this HYDAT release — skipping "
                      f"parameter {param} climatology")
                continue
            # station_id -> {doy: [values]} and -> set(years)
            by_station: dict[str, dict[int, list[float]]] = {}
            years: dict[str, set[int]] = {}
            for sid, doy, year, val in _read_daily(hydat_conn, table, stem, station_ids):
                by_station.setdefault(sid, {}).setdefault(doy, []).append(float(val))
                years.setdefault(sid, set()).add(year)

            for sid, by_doy in by_station.items():
                yr = years.get(sid, set())
                if len(yr) < MIN_YEARS:
                    continue
                pooled = _pooled_windows(by_doy, WINDOW_DAYS)
                rows = []
                for doy, vals in pooled.items():
                    if len(vals) < MIN_OBS_PER_DOY:
                        continue
                    pv = _percentiles(vals, _PCTLS)
                    rows.append((sid, param, doy, *pv, len(vals)))
                if not rows:
                    continue
                conn.executemany(
                    "INSERT INTO flow_climatology "
                    "(station_id, parameter, doy, p0, p10, p25, p50, p75, p90, p100, n_obs) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
                conn.execute(
                    "INSERT INTO flow_climatology_meta "
                    "(station_id, parameter, start_year, end_year, n_years, "
                    " window_days, hydat_release_date, computed_at, attribution) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (sid, param, min(yr), max(yr), len(yr), WINDOW_DAYS,
                     release_date, now, attr))
                published[param] += 1
    finally:
        hydat_conn.close()
    conn.commit()

    print(f"  climatology: {published['6']} discharge + {published['3']} level "
          f"station series published (>= {MIN_YEARS} yr record)")
    return {"discharge_stations": published["6"], "level_stations": published["3"]}


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def find_latest_release() -> tuple[str, str]:
    """Return (release_date 'YYYYMMDD', full zip URL) for the newest release
    listed at HYDAT_INDEX_URL."""
    req = urllib.request.Request(HYDAT_INDEX_URL, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60, context=_SSL_CTX) as resp:
        html = resp.read().decode("utf-8", "replace")

    matches = _RELEASE_RE.findall(html)
    if not matches:
        raise RuntimeError(f"No Hydat_sqlite3_*.zip links found at {HYDAT_INDEX_URL}")
    filename, date_str = max(matches, key=lambda m: m[1])
    return date_str, HYDAT_INDEX_URL + filename


def already_synced(conn: sqlite3.Connection, release_date: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM hydat_sync WHERE release_date = ?", (release_date,)
    ).fetchone()
    return row is not None


def _download(url: str, zip_path: Path, attempts: int = 4) -> None:
    """Stream a large file to disk, retrying on transient connection drops.

    A ~266MB pull over a minute-plus regularly hits a mid-stream
    ConnectionResetError / incomplete read; each attempt restarts the whole
    download (the server doesn't reliably support range resumption), and the
    result is validated against the advertised Content-Length before accepting.
    """
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(req, timeout=600, context=_SSL_CTX) as resp:
                total = int(resp.headers.get("Content-Length", 0))
                with open(zip_path, "wb") as f:
                    shutil.copyfileobj(resp, f, length=1024 * 1024)
            got = zip_path.stat().st_size
            if total and got < total:
                raise IOError(f"incomplete download: {got} of {total} bytes")
            print(f"  downloaded {got / (1024*1024):.0f}MB"
                  + (f" of {total / (1024*1024):.0f}MB" if total else ""))
            return
        except (OSError, urllib.error.URLError) as exc:
            if attempt == attempts:
                raise
            wait = 2 ** attempt
            print(f"  download attempt {attempt} failed ({exc}); retrying in {wait}s ...",
                  file=sys.stderr)
            time.sleep(wait)


def download_and_extract(url: str, dest_dir: Path) -> Path:
    """Stream the zip to disk (too large to read into memory at once),
    extract Hydat.sqlite3, return its path."""
    zip_path = dest_dir / "hydat.zip"
    print(f"Downloading {url} ...")
    _download(url, zip_path)

    print("Extracting Hydat.sqlite3 ...")
    with zipfile.ZipFile(zip_path) as zf:
        sqlite_names = [n for n in zf.namelist() if n.lower().endswith(".sqlite3")]
        if not sqlite_names:
            raise RuntimeError(f"No .sqlite3 file found inside {url}")
        zf.extract(sqlite_names[0], dest_dir)
        extracted = dest_dir / sqlite_names[0]
    return extracted


def sync_station_metadata(
    conn: sqlite3.Connection, hydat_path: Path, release_date: str
) -> dict:
    """Merge HYDAT's BC rows into hydro.db's stations table.

    Two roles:
      * UPDATE every station fetch_stations() already discovered, enriching it
        with authoritative lat/lon, HYD_STATUS, drainage area, etc.
      * INSERT any BC station HYDAT flags **real-time + active**
        (REAL_TIME=1 AND HYD_STATUS='A') that the Real-time Search scrape
        missed — the completeness backstop. The two rosters each drop a few
        the other has, so their union is the full set of active real-time BC
        stations (see README "Discovery"). Discontinued / non-real-time HYDAT
        rows are never inserted (HYDAT lags for brand-new stations and carries
        the entire historical roster, so it must not drive discovery on its
        own — only its real-time-active rows are eligible to add).
    """
    known_ids = {r[0] for r in conn.execute("SELECT station_id FROM stations")}

    hydat_conn = sqlite3.connect(f"file:{hydat_path}?mode=ro", uri=True)
    try:
        cols = ["STATION_NUMBER", "STATION_NAME", "LATITUDE", "LONGITUDE", *_COLUMN_MAP]
        rows = hydat_conn.execute(
            f"SELECT {', '.join(cols)} FROM STATIONS WHERE PROV_TERR_STATE_LOC = 'BC'"
        ).fetchall()
    finally:
        hydat_conn.close()

    now = datetime.now(timezone.utc).isoformat()
    matched = 0
    inserted: list[str] = []
    discontinued_but_tracked: list[str] = []
    for station_id, name, lat, lon, *rest in rows:
        values = dict(zip(_COLUMN_MAP.values(), rest))

        if station_id not in known_ids:
            # Completeness backstop: add only real-time + active stations.
            if not (values.get("real_time_flag") == 1 and values.get("hyd_status") == "A"):
                continue
            conn.execute(
                """INSERT INTO stations
                     (station_id, name, province, lat, lon, hyd_status,
                      drainage_area_gross_km2, drainage_area_effective_km2,
                      rhbn, real_time_flag, coord_source, hydat_release_date,
                      updated_at)
                   VALUES (?, ?, 'BC', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (station_id, name, lat, lon, values.get("hyd_status"),
                 values.get("drainage_area_gross_km2"),
                 values.get("drainage_area_effective_km2"),
                 values.get("rhbn"), values.get("real_time_flag"),
                 "hydat" if lat is not None else None, release_date, now),
            )
            inserted.append(station_id)
            known_ids.add(station_id)
            continue

        matched += 1
        if values.get("hyd_status") == "D":
            discontinued_but_tracked.append(station_id)

        set_clauses = [f"{col} = ?" for col in values]
        params: list = list(values.values())
        if lat is not None and lon is not None:
            set_clauses += ["lat = ?", "lon = ?", "coord_source = 'hydat'"]
            params += [lat, lon]
        set_clauses.append("hydat_release_date = ?")
        params.append(release_date)
        params.append(station_id)

        conn.execute(
            f"UPDATE stations SET {', '.join(set_clauses)} WHERE station_id = ?",
            params,
        )

    hydat_ids = {r[0] for r in rows}
    unmatched = sorted(known_ids - hydat_ids)
    conn.execute(
        "INSERT OR REPLACE INTO hydat_sync (release_date, synced_at, rows_matched) "
        "VALUES (?, ?, ?)",
        (release_date, now, matched),
    )
    conn.commit()

    return {
        "release_date": release_date,
        "matched": matched,
        "inserted": inserted,
        "tracked_total": len(known_ids),
        "unmatched_station_ids": unmatched,
        "discontinued_but_tracked": discontinued_but_tracked,
    }


def _print_summary(summary: dict) -> None:
    print(f"\nHYDAT release {summary['release_date']}: "
          f"{summary['matched']}/{summary['tracked_total']} tracked stations matched.")
    if summary.get("inserted"):
        print(f"  Added {len(summary['inserted'])} real-time+active station(s) the "
              f"search scrape missed: {', '.join(summary['inserted'])}")
    if summary["unmatched_station_ids"]:
        print(f"  Not found in this HYDAT release ({len(summary['unmatched_station_ids'])}): "
              f"{', '.join(summary['unmatched_station_ids'])}")
    if summary["discontinued_but_tracked"]:
        print(f"  WARNING: HYD_STATUS='D' for stations we still actively track "
              f"({len(summary['discontinued_but_tracked'])}): "
              f"{', '.join(summary['discontinued_but_tracked'])}")


def sync(conn: sqlite3.Connection, force: bool = False) -> dict | None:
    """Sync hydro.db's stations table from the latest HYDAT release.
    Returns the summary dict, or None if already up to date and skipped."""
    release_date, url = find_latest_release()
    if not force and already_synced(conn, release_date):
        # Self-heal the migration case: a DB synced before climatology existed
        # is "up to date" on metadata but has an empty climatology. Re-run (the
        # download is the only cost) so the envelope gets built without needing
        # an explicit --force.
        have_clim = conn.execute(
            "SELECT 1 FROM flow_climatology_meta LIMIT 1").fetchone()
        if have_clim:
            print(f"Already synced to HYDAT release {release_date} — skipping "
                  "(pass --force to re-sync).")
            return None
        print(f"Metadata already at HYDAT release {release_date}, but no "
              "climatology yet — rebuilding the percentile envelope.")

    with tempfile.TemporaryDirectory(prefix="hydat_") as tmp:
        hydat_path = download_and_extract(url, Path(tmp))
        summary = sync_station_metadata(conn, hydat_path, release_date)
        # Same extracted file, no extra download: derive the day-of-year
        # percentile climatology while it's on disk (slow cadence — a new
        # release supersedes it wholesale).
        print("\n--- Computing day-of-year percentile climatology (HYDAT daily record) ---")
        summary["climatology"] = compute_climatology(conn, hydat_path, release_date)
    _print_summary(summary)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Sync hydro.db station metadata (lat/lon, status, drainage area) "
                    "from ECCC's bulk HYDAT release."
    )
    parser.add_argument("--force", action="store_true",
                         help="re-sync even if already at the latest release")
    args = parser.parse_args(argv)

    conn = connect(DB_PATH)
    try:
        sync(conn, force=args.force)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
