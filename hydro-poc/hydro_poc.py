#!/usr/bin/env python3
"""
Hydrometric gauge data — PROOF OF CONCEPT
=========================================

Fetches water level / discharge data from Environment and Climate Change
Canada's Wateroffice real-time web services, stores it in a local SQLite
database, and (via serve.py) renders raw plots in the browser.

Two stages:
  1. `stations`  — discover stations from the Real-time Station Search
                   (province-tagged; official name + province + schedule).
  2. `bulk`      — one-time historical pull (default: daily means, last 18 months).
  3. `realtime`  — recurring pull of the latest real-time (unit) values.

--------------------------------------------------------------------------
DATA ATTRIBUTION  (REQUIRED — this data MUST be referenced like this)
--------------------------------------------------------------------------
When this POC graduates to production and the data is displayed on the
fishing website / stored in Cloudflare, every view MUST carry the correct
ECCC attribution. Per https://wateroffice.ec.gc.ca/contactus/faq_e.html :

  * Real-time data retrieved from the Wateroffice web site:
      "Extracted from the Environment and Climate Change Canada Real-time
       Hydrometric Data web site
       (https://wateroffice.ec.gc.ca/mainmenu/real_time_data_index_e.html)
       on [DATE]"

  * Historical data retrieved from the Wateroffice web site:
      "Extracted from the Environment and Climate Change Canada Historical
       Hydrometric Data web site
       (https://wateroffice.ec.gc.ca/mainmenu/historical_data_index_e.html)
       on [DATE]"

  * Historical data retrieved from the MDB (HYDAT) file:
      "Extracted from Environment and Climate Change Canada's HYDAT.mdb,
       released on [DATE]"

The exact attribution string (with today's date substituted) is emitted by
`attribution()` below and stored alongside every fetch batch so the website
can render it verbatim.

--------------------------------------------------------------------------
APPROVAL & GRADE reference  (from the Wateroffice FAQ)
--------------------------------------------------------------------------
Approval:
  PROVISIONAL  Data provided is best available and is subject to change.
  FINAL        Data confirmed to capture all observations and meeting all
               quality standards.

Grade:
  10  ICE          Ice conditions/processes may have caused backwater,
                   affecting the stage-discharge relationship.
  20  ESTIMATED    Discharge estimated using an alternative to the station's
                   stage-discharge model.
  30  PARTIAL DAY  Daily mean values that include missing periods larger than
                   120 consecutive minutes in the same day.
  40  DRY          Water level dropped below the lowest limit observable by the
                   sensor (may or may not indicate a dry gauging pool).
  50  REVISED      Previously approved data subsequently reviewed and edited.

--------------------------------------------------------------------------
WATEROFFICE WEB SERVICE ENDPOINTS  (GET requests)
--------------------------------------------------------------------------
Real-time Station Search — used here to discover province-tagged stations:
  https://wateroffice.ec.gc.ca/search/real_time_results_e.html?search_type=province&province=BC
    * Returns an HTML table: Station Name, Province, Station Number,
      Data Available (past 2 hours), Operation Schedule.

Current Conditions (KML) — optional lat/lon backfill for known stations:
  https://wateroffice.ec.gc.ca/services/current_conditions/xml/inline?stations[]=&lang=en
    * stations[]  — omit to return ALL stations
    * lang        — en | fr
    * Format: KML

Recent Real-Time Data (CSV) — latest 5 minutes:
  https://wateroffice.ec.gc.ca/services/recent_real_time_data/csv/inline?stations[]=&parameters[]=
    * stations[]   — omit to return ALL stations
    * parameters[] — only 46 and 47 available
    * Format: CSV

Real-Time Data (CSV) — historical window:
  https://wateroffice.ec.gc.ca/services/real_time_data/csv/inline?stations[]=&parameters[]=&start_date=&end_date=
    * stations[]   — required
    * parameters[] — see below
    * start_date / end_date — "YYYY-MM-DD HH24:MI:SS" (UTC)
    * Format: CSV
    * Notes: split large requests (fewer stations or shorter windows).
             Daily-mean params (3, 6) are available for the past 60 months;
             other params for the last 18 months.

Parameters:
    3   Water level (daily mean values)
    6   Discharge   (daily mean values)
   46   Water level (unit values)
   47   Discharge   (unit values)

All service times are in UTC.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sqlite3
import ssl
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

# --- Constants ------------------------------------------------------------

BASE = "https://wateroffice.ec.gc.ca/services"
CURRENT_CONDITIONS_URL = f"{BASE}/current_conditions/xml/inline"
RECENT_REALTIME_URL = f"{BASE}/recent_real_time_data/csv/inline"
REALTIME_URL = f"{BASE}/real_time_data/csv/inline"

# Authoritative station list (province-tagged) — the Real-time Station Search
# results page. Filtering by province returns a table with the station's
# official name, province, number, data-availability, and operation schedule.
STATION_SEARCH_URL = "https://wateroffice.ec.gc.ca/search/real_time_results_e.html"

# Province / territory codes accepted by the search's `province` field.
PROVINCES = ["AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC",
             "SK", "YT"]

# --- BC River Forecast Centre model forecasts (ArcGIS Feature Services) ----
# The BCRFC forecast maps (CLEVER / COFFEE / ELF) are ArcGIS web apps backed by
# public Feature Services in one org. Each record is a per-station forecast
# summary (latest observed value + forecast min/ave/max over a horizon + return
# period), plus a Hydrograph_url to the per-station PDF.
ARCGIS_BASE = ("https://services6.arcgis.com/ubm4tcTYICKBpist/"
               "arcgis/rest/services")

# model -> config. Fields differ per service; `fmax`/`fmin`/`fave` are the
# forecast discharge (m3/s) columns, `rep` picks the representative value used
# for the extend-from-observed overlay (max for high-flow models, min for the
# low-flow ELF model).
FORECAST_MODELS = {
    "CLEVER": {
        "service": "CLM_MapHub_forecast", "horizon_days": 10,
        "obs": "Latest_Reading", "obs_rp": "Return_Period_OBS",
        "fmax": "Forecast_maximum_in_5_days", "for_rp": "Return_Period_FOR",
        "issued": "Issued_at", "rep": "max",
        # CLEVER publishes an HOURLY 10-day forecast CSV (forecast + bounds).
        "series_csv": "https://bcrfc.env.gov.bc.ca/freshet/clever/{sid}.CSV",
        "series_kind": "fdlu_hourly",
    },
    "COFFEE": {
        "service": "coffee_MapHub_forecast", "horizon_days": 5,
        "obs": "Latest_Reading", "obs_rp": "Return_Period_OBS",
        "fmin": "Forecast_minimum_in_5_days",
        "fave": "Forecast_average_in_5_days",
        "fmax": "Forecast_maximum_in_5_days", "for_rp": "Return_Period_FOR",
        "issued": "Issued_at", "rep": "max",
        # COFFEE publishes a DAILY 5-day forecast CSV (forecast + bounds).
        "series_csv": "https://bcrfc.env.gov.bc.ca/fallfloods/coffee/COFFEE_{sid}.CSV",
        "series_kind": "fdlu_daily",
    },
    "ELF": {
        "service": "MapHub_ELF_Forecast", "horizon_days": 30,
        "obs": "Qobs_m3_s_",
        "fmin": "Qfor_MIN_30_Days_m3_s_",
        "fave": "Qfor_AVE_30_Days_m3_s_",
        "fmax": "Qfor_MAX_30_Days_m3_s_",
        "issued": "Issued_at", "rep": "min",
        # ELF publishes a DAILY 30-day forecast CSV (obs + min/ave/max).
        "series_csv": "https://bcrfc.env.gov.bc.ca/lowflow/elf/csv/{sid}_elf_forecast.csv",
        "series_kind": "elf",
    },
}

# parameter id -> human label
PARAMETERS = {
    "3": "Water level (daily mean)",
    "6": "Discharge (daily mean)",
    "46": "Water level (unit)",
    "47": "Discharge (unit)",
}

DB_PATH = Path(__file__).parent / "hydro.db"

USER_AGENT = "BC-fishing-regs-hydro-poc/0.1 (proof of concept)"

# Polite pause between requests so we don't hammer the service.
REQUEST_DELAY_S = 0.5


# TLS context. Some gov.bc.ca hosts serve an incomplete certificate chain that
# Python's default trust store (esp. under conda) can't verify, even though it's
# a legitimate public CA. Prefer certifi's bundle; fall back to the system
# default. As a last resort for these public, read-only data endpoints, disable
# verification with a visible warning.
def _make_ssl_context() -> ssl.SSLContext:
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except Exception:  # noqa: BLE001
        return ssl.create_default_context()


_SSL_CTX = _make_ssl_context()
_SSL_INSECURE: ssl.SSLContext | None = None


# --- Attribution ----------------------------------------------------------

def attribution(kind: str) -> str:
    """Return the exact ECCC attribution string (see module docstring).

    `kind` is 'realtime' or 'historical'. The website MUST display this
    verbatim wherever the data appears.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    if kind == "historical":
        return (
            "Extracted from the Environment and Climate Change Canada Historical "
            "Hydrometric Data web site "
            "(https://wateroffice.ec.gc.ca/mainmenu/historical_data_index_e.html) "
            f"on {today}"
        )
    return (
        "Extracted from the Environment and Climate Change Canada Real-time "
        "Hydrometric Data web site "
        "(https://wateroffice.ec.gc.ca/mainmenu/real_time_data_index_e.html) "
        f"on {today}"
    )


# --- HTTP -----------------------------------------------------------------

def _get(url: str, params: list[tuple[str, str]]) -> bytes:
    """GET with repeated query keys (stations[]=a&stations[]=b) preserved."""
    global _SSL_INSECURE
    query = urllib.parse.urlencode(params)
    full = f"{url}?{query}" if query else url
    req = urllib.request.Request(full, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=120, context=_SSL_CTX) as resp:
            return resp.read()
    except urllib.error.URLError as exc:
        # Retry once with verification disabled for TLS chain issues on these
        # public, read-only gov endpoints.
        if isinstance(getattr(exc, "reason", None), ssl.SSLCertVerificationError):
            if _SSL_INSECURE is None:
                print("WARNING: TLS verification failed; retrying without "
                      f"verification for {urllib.parse.urlsplit(url).netloc}.",
                      file=sys.stderr)
                _SSL_INSECURE = ssl._create_unverified_context()
            with urllib.request.urlopen(req, timeout=120,
                                        context=_SSL_INSECURE) as resp:
                return resp.read()
        raise


# --- Database -------------------------------------------------------------

def connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS stations (
            station_id TEXT PRIMARY KEY,
            name       TEXT,
            province   TEXT,
            lat        REAL,
            lon        REAL,
            data_available     TEXT,
            operation_schedule TEXT,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS readings (
            station_id TEXT NOT NULL,
            ts         TEXT NOT NULL,   -- ISO UTC timestamp
            parameter  TEXT NOT NULL,   -- parameter id ('3','6','46','47')
            value      REAL,
            grade      TEXT,
            symbol     TEXT,
            approval   TEXT,
            qualifier  TEXT,
            source     TEXT,            -- 'bulk' | 'realtime'
            attribution TEXT,
            PRIMARY KEY (station_id, ts, parameter)
        );

        CREATE INDEX IF NOT EXISTS idx_readings_station
            ON readings (station_id, parameter, ts);

        CREATE TABLE IF NOT EXISTS forecasts (
            model        TEXT NOT NULL,   -- 'CLEVER' | 'COFFEE' | 'ELF'
            station_id   TEXT NOT NULL,
            station_name TEXT,
            issued_at    TEXT,
            horizon_days INTEGER,
            obs_value    REAL,            -- latest observed discharge (m3/s)
            obs_rp       TEXT,            -- observed return period
            forecast_value REAL,          -- representative forecast (m3/s)
            forecast_min REAL,
            forecast_ave REAL,
            forecast_max REAL,
            forecast_rp  TEXT,            -- forecast return period
            hydrograph_url TEXT,
            lat REAL,
            lon REAL,
            raw  TEXT,                    -- full attribute JSON
            attribution TEXT,
            fetched_at TEXT,
            PRIMARY KEY (model, station_id)
        );

        CREATE INDEX IF NOT EXISTS idx_forecasts_station
            ON forecasts (station_id);

        CREATE TABLE IF NOT EXISTS forecast_series (
            model      TEXT NOT NULL,
            station_id TEXT NOT NULL,
            date       TEXT NOT NULL,   -- YYYY-MM-DD (daily)
            qobs REAL, qfor_min REAL, qfor_ave REAL, qfor_max REAL,
            hobs REAL, hfor_min REAL, hfor_ave REAL, hfor_max REAL,
            fetched_at TEXT,
            PRIMARY KEY (model, station_id, date)
        );
        """
    )
    # Idempotent migration for pre-existing databases.
    existing = {r[1] for r in conn.execute("PRAGMA table_info(stations)")}
    for col in ("data_available", "operation_schedule"):
        if col not in existing:
            conn.execute(f"ALTER TABLE stations ADD COLUMN {col} TEXT")
    conn.commit()
    return conn


# --- Stage 1: station discovery ------------------------------------------

def _localname(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


# One results row looks like:
#   <td><input ... value="08LD003,1,,,"/></td>
#   <td><label for="check1">ADAMS LAKE NEAR SQUILAX</label></td>
#   <td>BC</td>
#   <td>08LD003</td>
#   <td class="icon-green image-center">Yes</td>
#   <td>Continuous</td>
_ROW_RE = re.compile(
    r"<label[^>]*>([^<]+)</label>\s*</td>\s*"   # 1: station name
    r"<td>\s*([A-Z]{2})\s*</td>\s*"             # 2: province
    r"<td>\s*([0-9A-Z]+)\s*</td>\s*"            # 3: station number
    r"<td[^>]*>\s*([^<]*?)\s*</td>\s*"          # 4: data available (Yes/No)
    r"<td[^>]*>\s*([^<]*?)\s*</td>",            # 5: operation schedule
    re.IGNORECASE,
)


def _parse_station_rows(html: str) -> list[tuple[str, str, str, str, str]]:
    """Return (station_id, name, province, data_available, schedule) tuples."""
    out = []
    for name, prov, num, avail, sched in _ROW_RE.findall(html):
        out.append((num.strip(), name.strip(), prov.strip(),
                    avail.strip(), sched.strip()))
    return out


def fetch_stations(conn: sqlite3.Connection,
                   provinces: list[str] | None = None) -> int:
    """Discover stations from the authoritative Real-time Station Search.

    Filtering the search by province yields a table with each station's
    official name, PROVINCE, number, data-availability, and operation
    schedule — no bounding box needed. We iterate the requested provinces
    (default: all) and upsert every row.
    """
    provinces = provinces or PROVINCES
    now = datetime.now(timezone.utc).isoformat()
    total = 0

    for prov in provinces:
        print(f"Fetching station list for province {prov} ...")
        try:
            html = _get(STATION_SEARCH_URL, [
                ("search_type", "province"),
                ("province", prov),
                ("station_name", ""),
                ("station_number", ""),
            ]).decode("utf-8", "replace")
        except Exception as exc:  # noqa: BLE001 — surface, then continue
            print(f"  ERROR fetching {prov}: {exc}", file=sys.stderr)
            continue

        rows = _parse_station_rows(html)
        if not rows:
            print(f"  WARNING: no rows parsed for {prov} "
                  "(results markup may have changed).", file=sys.stderr)
            continue

        conn.executemany(
            """INSERT INTO stations
                 (station_id, name, province, data_available,
                  operation_schedule, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(station_id) DO UPDATE SET
                 name=excluded.name,
                 province=excluded.province,
                 data_available=excluded.data_available,
                 operation_schedule=excluded.operation_schedule,
                 updated_at=excluded.updated_at""",
            [(sid, name, prov_, avail, sched, now)
             for (sid, name, prov_, avail, sched) in rows],
        )
        conn.commit()
        print(f"  {len(rows)} stations.")
        total += len(rows)
        time.sleep(REQUEST_DELAY_S)

    print(f"Stored/updated {total} stations across {len(provinces)} province(s).")
    return total


def enrich_coordinates(conn: sqlite3.Connection) -> int:
    """Optionally add lat/lon to known stations from the Current Conditions KML.

    The province search table has no coordinates; this backfills them so the
    data can later be placed on a map. Omitting stations[] returns all.
    """
    print("Enriching coordinates from Current Conditions KML feed ...")
    raw = _get(CURRENT_CONDITIONS_URL, [("lang", "en")])
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        print(f"ERROR: could not parse KML: {exc}", file=sys.stderr)
        raise

    updates: list[tuple] = []
    for elem in root.iter():
        if _localname(elem.tag) != "Placemark":
            continue
        station_id = None
        lat = lon = None
        for child in elem.iter():
            ln = _localname(child.tag)
            if ln == "name" and station_id is None and child.text:
                station_id = child.text.strip()
            elif ln == "coordinates" and child.text:
                parts = child.text.strip().split(",")
                if len(parts) >= 2:
                    try:
                        lon, lat = float(parts[0]), float(parts[1])
                    except ValueError:
                        pass
        if station_id and lat is not None:
            updates.append((lat, lon, station_id))

    conn.executemany(
        "UPDATE stations SET lat = ?, lon = ? WHERE station_id = ?", updates)
    conn.commit()
    print(f"Updated coordinates for {len(updates)} stations.")
    return len(updates)


# --- CSV parsing ----------------------------------------------------------

def _match_col(headers: list[str], *keywords: str) -> str | None:
    for h in headers:
        hl = h.lower()
        if any(k in hl for k in keywords):
            return h
    return None


def parse_readings_csv(text: str) -> list[dict]:
    """Defensively parse a Wateroffice CSV into normalized reading dicts.

    Column names vary (bilingual headers), so we match by keyword rather than
    exact string.
    """
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        return []
    headers = [h for h in reader.fieldnames if h is not None]

    col_id = _match_col(headers, "id")
    col_date = _match_col(headers, "date")
    col_param = _match_col(headers, "parameter", "paramètre", "parametre")
    col_value = _match_col(headers, "value", "valeur")
    col_grade = _match_col(headers, "grade")
    col_symbol = _match_col(headers, "symbol", "symbole")
    col_approval = _match_col(headers, "approval", "approbation")
    col_qual = _match_col(headers, "qualifier", "qualificatif")

    out: list[dict] = []
    for row in reader:
        station = (row.get(col_id) or "").strip() if col_id else ""
        ts = (row.get(col_date) or "").strip() if col_date else ""
        if not station or not ts:
            continue
        raw_val = (row.get(col_value) or "").strip() if col_value else ""
        try:
            value = float(raw_val) if raw_val not in ("", "None") else None
        except ValueError:
            value = None
        out.append({
            "station_id": station,
            "ts": ts,
            "parameter": (row.get(col_param) or "").strip() if col_param else "",
            "value": value,
            "grade": (row.get(col_grade) or "").strip() if col_grade else "",
            "symbol": (row.get(col_symbol) or "").strip() if col_symbol else "",
            "approval": (row.get(col_approval) or "").strip() if col_approval else "",
            "qualifier": (row.get(col_qual) or "").strip() if col_qual else "",
        })
    return out


def _normalize_parameter(raw: str, requested: list[str]) -> str:
    """Map a CSV parameter cell to a parameter id where possible."""
    r = raw.strip()
    if r in PARAMETERS:  # already an id
        return r
    rl = r.lower()
    if "discharge" in rl or "débit" in rl or "debit" in rl:
        return "6" if "daily" in rl or "mean" in rl else "47"
    if "level" in rl or "niveau" in rl:
        return "3" if "daily" in rl or "mean" in rl else "46"
    # Fall back to the single requested parameter if unambiguous.
    return requested[0] if len(requested) == 1 else r


def _store_readings(conn: sqlite3.Connection, readings: list[dict],
                    requested: list[str], source: str, attribution_str: str) -> int:
    rows = [
        (
            r["station_id"], r["ts"],
            _normalize_parameter(r["parameter"], requested),
            r["value"], r["grade"], r["symbol"], r["approval"], r["qualifier"],
            source, attribution_str,
        )
        for r in readings
    ]
    conn.executemany(
        """INSERT OR REPLACE INTO readings
           (station_id, ts, parameter, value, grade, symbol, approval,
            qualifier, source, attribution)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()
    return len(rows)


# --- Station selection ----------------------------------------------------

# A small default sample so the POC stays fast. Use --all for everything.
DEFAULT_SAMPLE = [
    "08MF005",  # Fraser River at Hope
    "08HB048",  # Englishman River near Parksville
    "08NM116",  # Okanagan River at Penticton
    "08GA010",  # Capilano River
    "07EA004",  # (example from the docs)
]


def select_stations(conn: sqlite3.Connection, args) -> list[str]:
    if args.stations:
        return args.stations
    if getattr(args, "bc", False):
        # Province comes straight from the authoritative station-search table.
        return [
            r[0] for r in conn.execute(
                "SELECT station_id FROM stations WHERE province = 'BC' "
                "ORDER BY station_id"
            )
        ]
    if args.all:
        return [r[0] for r in conn.execute("SELECT station_id FROM stations")]
    if args.province:
        return [
            r[0] for r in conn.execute(
                "SELECT station_id FROM stations WHERE province = ? LIMIT ?",
                (args.province.upper(), args.limit),
            )
        ]
    # Default: known sample, but only those that exist in the stations table
    # (fall back to the raw sample if discovery hasn't run yet).
    known = {r[0] for r in conn.execute("SELECT station_id FROM stations")}
    sample = [s for s in DEFAULT_SAMPLE if s in known] or DEFAULT_SAMPLE
    return sample[: args.limit]


def _chunk(seq: list, n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


# --- Stage 2: bulk historical --------------------------------------------

def fetch_bulk(conn: sqlite3.Connection, args) -> int:
    stations = select_stations(conn, args)
    params = args.parameters
    end = datetime.now(timezone.utc)
    # --days takes precedence over --months when provided.
    if getattr(args, "days", None):
        start = end - timedelta(days=args.days)
    else:
        start = end - timedelta(days=int(args.months * 30.44))
    fmt = "%Y-%m-%d %H:%M:%S"
    attribution_str = attribution("historical")

    print(f"Bulk pull: {len(stations)} station(s), params={params}, "
          f"{start:%Y-%m-%d}..{end:%Y-%m-%d}")
    total = 0
    # One station per request keeps each response small (per the API notes).
    for i, station in enumerate(stations, 1):
        query = [("stations[]", station)]
        for p in params:
            query.append(("parameters[]", p))
        query.append(("start_date", start.strftime(fmt)))
        query.append(("end_date", end.strftime(fmt)))
        try:
            raw = _get(REALTIME_URL, query).decode("utf-8", "replace")
        except Exception as exc:  # noqa: BLE001 — surface, then continue
            print(f"  [{i}/{len(stations)}] {station}: ERROR {exc}",
                  file=sys.stderr)
            continue
        readings = parse_readings_csv(raw)
        n = _store_readings(conn, readings, params, "bulk", attribution_str)
        total += n
        print(f"  [{i}/{len(stations)}] {station}: {n} readings")
        time.sleep(REQUEST_DELAY_S)

    print(f"Bulk pull complete: {total} readings stored.")
    return total


# --- Stage 3: realtime update --------------------------------------------

def fetch_realtime(conn: sqlite3.Connection, args) -> int:
    """Latest 5-minute values (recent real-time). Only params 46 & 47 exist."""
    stations = select_stations(conn, args)
    params = [p for p in args.parameters if p in ("46", "47")] or ["46", "47"]
    attribution_str = attribution("realtime")

    print(f"Realtime pull: {len(stations)} station(s), params={params}")
    total = 0
    # Recent endpoint accepts many stations at once; chunk to stay polite.
    for group in _chunk(stations, 20):
        query = [("stations[]", s) for s in group]
        for p in params:
            query.append(("parameters[]", p))
        try:
            raw = _get(RECENT_REALTIME_URL, query).decode("utf-8", "replace")
        except Exception as exc:  # noqa: BLE001
            print(f"  chunk {group[:2]}...: ERROR {exc}", file=sys.stderr)
            continue
        readings = parse_readings_csv(raw)
        n = _store_readings(conn, readings, params, "realtime", attribution_str)
        total += n
        print(f"  {len(group)} stations -> {n} readings")
        time.sleep(REQUEST_DELAY_S)

    print(f"Realtime pull complete: {total} readings stored.")
    return total


# --- Stage 4: BCRFC model forecasts --------------------------------------

def _num(s) -> float | None:
    """Extract the first number from strings like '=0.001(m3/s)' or '>3000'."""
    if s is None:
        return None
    m = re.search(r"-?\d+(?:\.\d+)?", str(s))
    return float(m.group()) if m else None


def _arcgis_query_all(service: str) -> list[dict]:
    """Fetch every feature's attributes from an ArcGIS FeatureServer layer."""
    url = f"{ARCGIS_BASE}/{service}/FeatureServer/0/query"
    out: list[dict] = []
    offset = 0
    page = 1000
    while True:
        params = [
            ("where", "1=1"), ("outFields", "*"), ("returnGeometry", "false"),
            ("f", "json"), ("resultOffset", str(offset)),
            ("resultRecordCount", str(page)),
        ]
        data = json.loads(_get(url, params).decode("utf-8", "replace"))
        feats = data.get("features", [])
        out.extend(f.get("attributes", {}) for f in feats)
        if len(feats) < page or not data.get("exceededTransferLimit"):
            break
        offset += page
        time.sleep(REQUEST_DELAY_S)
    return out


def fetch_forecasts(conn: sqlite3.Connection,
                    models: list[str] | None = None,
                    series: bool = True) -> int:
    """Pull BCRFC model forecasts (CLEVER/COFFEE/ELF) into the forecasts table.

    Each record is joined to gauges by Station_ID (the WSC station number), so
    the frontend can extend a forecast from the observed hydrograph. For models
    that publish a full daily forecast time series (ELF), the per-station CSV is
    also downloaded into forecast_series when `series` is True.
    """
    models = models or list(FORECAST_MODELS)
    attribution_str = attribution("realtime")
    now = datetime.now(timezone.utc).isoformat()
    total = 0

    for model in models:
        cfg = FORECAST_MODELS[model]
        print(f"Fetching {model} forecasts ({cfg['service']}) ...")
        try:
            attrs = _arcgis_query_all(cfg["service"])
        except Exception as exc:  # noqa: BLE001 — surface, then continue
            print(f"  ERROR: {exc}", file=sys.stderr)
            continue

        rows = []
        for a in attrs:
            fmin = _num(a.get(cfg.get("fmin"))) if cfg.get("fmin") else None
            fave = _num(a.get(cfg.get("fave"))) if cfg.get("fave") else None
            fmax = _num(a.get(cfg.get("fmax"))) if cfg.get("fmax") else None
            rep = {"min": fmin, "ave": fave, "max": fmax}.get(cfg["rep"])
            rep = rep if rep is not None else (fmax or fave or fmin)
            rows.append((
                model,
                (a.get("Station_ID") or "").strip(),
                a.get("Station_Name"),
                a.get(cfg.get("issued", "")),
                cfg["horizon_days"],
                _num(a.get(cfg.get("obs"))),
                a.get(cfg.get("obs_rp")) if cfg.get("obs_rp") else None,
                rep, fmin, fave, fmax,
                a.get(cfg.get("for_rp")) if cfg.get("for_rp") else None,
                a.get("Hydrograph_url"),
                a.get("LATITUDE"), a.get("LONGITUDE"),
                json.dumps(a), attribution_str, now,
            ))

        rows = [r for r in rows if r[1]]  # require a station id
        conn.executemany(
            """INSERT OR REPLACE INTO forecasts
               (model, station_id, station_name, issued_at, horizon_days,
                obs_value, obs_rp, forecast_value, forecast_min, forecast_ave,
                forecast_max, forecast_rp, hydrograph_url, lat, lon, raw,
                attribution, fetched_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            rows,
        )
        conn.commit()
        matched = conn.execute(
            """SELECT COUNT(*) FROM forecasts f
               WHERE f.model = ? AND EXISTS
                 (SELECT 1 FROM stations s WHERE s.station_id = f.station_id)""",
            (model,),
        ).fetchone()[0]
        print(f"  {len(rows)} forecasts ({matched} match known stations).")
        total += len(rows)

        # Daily/hourly forecast time series (CLEVER hourly, COFFEE & ELF daily).
        # Limited to stations that have observed readings so the overlay is useful.
        if series and cfg.get("series_csv"):
            fetch_forecast_series(conn, model, cfg)
        time.sleep(REQUEST_DELAY_S)

    print(f"Forecast pull complete: {total} records stored.")
    return total


def _parse_elf_series_csv(text: str) -> list[dict]:
    """Parse the ELF forecast CSV (skips the disclaimer preamble)."""
    lines = text.splitlines()
    start = next((i for i, ln in enumerate(lines)
                  if ln.upper().startswith("DATE,")), None)
    if start is None:
        return []
    reader = csv.DictReader(lines[start:])
    out = []
    for row in reader:
        date = (row.get("DATE") or "").strip()
        if not date:
            continue
        out.append({
            "date": date,
            "qobs": _num(row.get("QOBS")), "qfor_min": _num(row.get("QFOR_MIN")),
            "qfor_ave": _num(row.get("QFOR_AVE")), "qfor_max": _num(row.get("QFOR_MAX")),
            "hobs": _num(row.get("HOBS")), "hfor_min": _num(row.get("HFOR_MIN")),
            "hfor_ave": _num(row.get("HFOR_AVE")), "hfor_max": _num(row.get("HFOR_MAX")),
        })
    return out


def _parse_fdlu_series_csv(text: str, hourly: bool) -> list[dict]:
    """Parse the CLEVER/COFFEE forecast CSV format:
       DATE,[HOUR,]FORECAST_DISCHARGE,LOWER_BOUND,UPPER_BOUND.

    Mapped onto the shared series columns as forecast average (=FORECAST_DISCHARGE)
    with min/max = LOWER/UPPER bound. CLEVER is hourly (DATE+HOUR → timestamp).
    """
    lines = text.splitlines()
    start = next((i for i, ln in enumerate(lines)
                  if ln.upper().startswith("DATE,")), None)
    if start is None:
        return []
    reader = csv.DictReader(lines[start:])
    out = []
    for row in reader:
        date = (row.get("DATE") or "").strip()
        if not date:
            continue
        if hourly:
            hour = (row.get("HOUR") or "00").strip().zfill(2)
            ts = f"{date}T{hour}:00:00"
        else:
            ts = date
        out.append({
            "date": ts,
            "qobs": None,
            "qfor_min": _num(row.get("LOWER_BOUND")),
            "qfor_ave": _num(row.get("FORECAST_DISCHARGE")),
            "qfor_max": _num(row.get("UPPER_BOUND")),
            "hobs": None, "hfor_min": None, "hfor_ave": None, "hfor_max": None,
        })
    return out


def _parse_series_csv(text: str, kind: str) -> list[dict]:
    if kind == "elf":
        return _parse_elf_series_csv(text)
    if kind.startswith("fdlu"):
        return _parse_fdlu_series_csv(text, hourly=kind.endswith("hourly"))
    return []


def fetch_forecast_series(conn: sqlite3.Connection, model: str,
                          cfg: dict) -> int:
    """Download the per-station daily/hourly forecast CSV for stations that
    also have observed readings (so the overlay is useful)."""
    url_tmpl = cfg["series_csv"]
    kind = cfg.get("series_kind", "elf")
    stations = [r[0] for r in conn.execute(
        """SELECT DISTINCT f.station_id FROM forecasts f
           JOIN readings r ON r.station_id = f.station_id
           WHERE f.model = ? ORDER BY f.station_id""", (model,))]
    if not stations:
        return 0
    print(f"  Fetching {model} forecast series for {len(stations)} station(s) ...")
    now = datetime.now(timezone.utc).isoformat()
    total = 0
    for i, sid in enumerate(stations, 1):
        try:
            text = _get(url_tmpl.format(sid=sid), []).decode("utf-8", "replace")
        except Exception as exc:  # noqa: BLE001 — surface, then continue
            # 404s are common (not every station publishes a CSV) — skip quietly.
            if "404" not in str(exc):
                print(f"    [{i}/{len(stations)}] {sid}: ERROR {exc}",
                      file=sys.stderr)
            continue
        rows = _parse_series_csv(text, kind)
        conn.executemany(
            """INSERT OR REPLACE INTO forecast_series
               (model, station_id, date, qobs, qfor_min, qfor_ave, qfor_max,
                hobs, hfor_min, hfor_ave, hfor_max, fetched_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            [(model, sid, r["date"], r["qobs"], r["qfor_min"], r["qfor_ave"],
              r["qfor_max"], r["hobs"], r["hfor_min"], r["hfor_ave"],
              r["hfor_max"], now) for r in rows],
        )
        conn.commit()
        total += len(rows)
        time.sleep(REQUEST_DELAY_S / 2)
    print(f"  {model} series: {total} rows stored.")
    return total


# --- CLI ------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    sub = parser.add_subparsers(dest="command", required=True)

    def add_selection(p):
        p.add_argument("--stations", nargs="*", help="explicit station ids")
        p.add_argument("--all", action="store_true", help="all known stations")
        p.add_argument("--bc", action="store_true",
                       help="all BC stations (province = BC)")
        p.add_argument("--province", help="filter by province code, e.g. BC")
        p.add_argument("--limit", type=int, default=5,
                       help="cap station count for sample/province modes")

    p_st = sub.add_parser("stations", help="discover stations (stage 1)")
    p_st.add_argument("--provinces", nargs="+", default=None,
                      help="province codes to fetch (default: all)")
    p_st.add_argument("--coords", action="store_true",
                      help="also backfill lat/lon from the KML feed")

    p_bulk = sub.add_parser("bulk", help="one-time historical pull (stage 2)")
    add_selection(p_bulk)
    p_bulk.add_argument("--months", type=float, default=18,
                        help="history window in months (default 18)")
    p_bulk.add_argument("--days", type=float, default=None,
                        help="history window in days (overrides --months); "
                             "use with --parameters 46 47 for high-frequency data")
    p_bulk.add_argument("--parameters", nargs="+", default=["3", "6"],
                        help="parameter ids (default daily means 3 & 6; "
                             "use 46 47 for unit/high-frequency values)")

    p_rt = sub.add_parser("realtime", help="latest real-time values (stage 3)")
    add_selection(p_rt)
    p_rt.add_argument("--parameters", nargs="+", default=["46", "47"],
                      help="parameter ids (only 46 & 47 supported)")

    p_fc = sub.add_parser("forecast",
                          help="BCRFC model forecasts (CLEVER/COFFEE/ELF)")
    p_fc.add_argument("--models", nargs="+", default=None,
                      choices=list(FORECAST_MODELS),
                      help="which models to fetch (default: all)")
    p_fc.add_argument("--no-series", action="store_true",
                      help="skip downloading ELF daily forecast time series")

    args = parser.parse_args(argv)
    conn = connect()
    try:
        if args.command == "stations":
            fetch_stations(conn, args.provinces)
            if args.coords:
                enrich_coordinates(conn)
        elif args.command == "bulk":
            fetch_bulk(conn, args)
        elif args.command == "realtime":
            fetch_realtime(conn, args)
        elif args.command == "forecast":
            fetch_forecasts(conn, args.models, series=not args.no_series)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
