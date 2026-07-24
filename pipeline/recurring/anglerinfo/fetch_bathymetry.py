#!/usr/bin/env python3
"""
fetch_bathymetry.py — BC WSA lake bathymetric survey maps, live.

Replicated copy: this is `../bathymetry/fetch_bathymetry.py`, unchanged
except for DB_PATH — it writes `bathy_surveys` into this directory's shared
`anglerinfo.db` instead of `bathymetry/`'s own `bathymetry.db`, so this
folder's testbed (see this dir's README) has bathymetry data alongside
stocking/markers and WDIC in one file. `bathy_survey_polygons` still goes
into its own local GPKG (sqlite has no geometry type without spatialite —
same reasoning as the original). `../bathymetry/` stays the standalone
original; this copy isn't imported from it, so the two can drift — resync
manually if `../bathymetry/`'s fetch logic changes and this testbed still
needs it.

This is the live-data/ POC counterpart to data/fetch_data.py's
`bathymetry_maps` + `bathymetry_polygons` datasets (which feed the
production pipeline/matching/bathymetry_matcher.py). Same two upstream BC
Data Catalogue sources, fetched directly here rather than through the
data/fetch_data.py batch pipeline, so this whole feed (fetch + match) can be
iterated on independently — see live-data/README.md's "POC -> production"
framing.

Two sources, kept as separate tables rather than merged (a sheet can live in
one source and not the other — confirmed live: HARRISON LAKE / 00081HARR is
in source 2 but missing from source 1's CSV export):

  1. `bathy_surveys` — the WSA "Bathymetry Open Reference Table and Maps"
     CSV: one row per survey map sheet (a single WSA identifier can span
     several sheets), keyed by WATERBODY_IDENTIFIER_WSA_50K — the exact same
     zfill(WATERBODY_KEY_50K, 5) + WATERSHED_GROUP_CODE_50K scheme FIDQ's
     wbid uses (see live-data/common/waterbody_matcher.py). Primary source
     for match.py's identifier lookup.

  2. `bathy_survey_polygons` (GPKG geometry) + `bathy_surveys_wfs` (this
     layer's own tabular attributes, same identifier/name/pdf_filename
     shape as bathy_surveys) — both from WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW,
     fetched live via WFS (public, no auth — same openmaps.gov.bc.ca pattern
     as WDIC, confirmed live). The GPKG holds this layer's own physical
     footprint on the ground (kept for whenever polygon-overlap matching
     gets revisited — no current reader); `bathy_surveys_wfs` is the
     gap-fill fallback for identifiers `bathy_surveys` is missing.

CLI
---
    python -m pipeline.recurring.anglerinfo.fetch_bathymetry              # fetch both sources
    python -m pipeline.recurring.anglerinfo.fetch_bathymetry --csv-only    # skip the (slower) polygon WFS fetch
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import geopandas as gpd
import pandas as pd

logger = logging.getLogger(__name__)

from .paths import DB_PATH  # centralized output/pipeline/anglerinfo/ location
GPKG_PATH = Path(__file__).parent / "bathymetry_polygons.gpkg"
POLYGON_LAYER = "bathymetry_polygons"

USER_AGENT = "BC-fishing-regs-bathymetry-poc/0.1 (proof of concept)"

# BC Data Catalogue — "Bathymetry Open Reference Table and Maps" (one row per
# bathymetric survey map, with the PDF map URL). Same URL
# data/fetch_data.py's `bathymetry_maps` dataset already uses.
BATHY_CSV_URL = (
    "https://catalogue.data.gov.bc.ca/dataset/1427d389-cd21-4fe2-8ed9-282d9bdcb7e2/"
    "resource/d1d89c2e-1994-4f7d-a269-55b40e26067d/download/"
    "bathyopenreferencetableandmapsaug22_2023final.csv"
)

# WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW — the survey sheets' own polygon
# footprints. Same WFS pattern live-data/common/waterbody_matcher.py's
# fetch_wdic_geometries() uses for WDIC, and data/fetch_data.py's
# fetch_wfs_paginated() uses for every other WFS layer in this project.
BATHY_POLYGON_WFS_URL = "https://openmaps.gov.bc.ca/geo/pub/WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW/ows"

# CSV column names — identical to pipeline/matching/bathymetry_matcher.py's
# own COL_* constants, kept in lockstep since both read the same CSV schema.
COL_ID = "WATERBODY_IDENTIFIER_WSA_50K"
COL_NAME = "GAZETTED_NAME"
COL_WSC = "WATERSHED_CODE_WSA_50K"
COL_MAPTITLE = "MAP_TITLE"
COL_PDF = "PDF_FILE_URL"
COL_PDF_FILENAME = "MAP_IMAGE_FILENAME_PDF"

# WFS layer's own column names for the same fields — different schema
# (it's a spatial layer, not the flat CSV), and it has no equivalent of
# MAP_IMAGE_FILENAME_PDF, so pdf_filename is derived from its PDF_FILE_URL
# instead (see _pdf_filename_from_url()).
WFS_COL_ID = "WATERBODY_IDENTIFIER"
WFS_COL_NAME = "GAZETTED_NAME"
WFS_COL_WSC = "NEW_WATERSHED_CODE"
WFS_COL_MAPTITLE = "MAP_TITLE"
WFS_COL_PDF = "PDF_FILE_URL"


def connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS bathy_surveys (
            identifier      TEXT,
            gazetted_name   TEXT,
            watershed_code  TEXT,
            map_title       TEXT,
            pdf_url         TEXT,
            pdf_filename    TEXT PRIMARY KEY,
            fetched_at      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_bathy_surveys_identifier
            ON bathy_surveys (identifier);
        CREATE INDEX IF NOT EXISTS idx_bathy_surveys_name
            ON bathy_surveys (gazetted_name);

        -- Same identifier/name/pdf-filename schema as bathy_surveys, but
        -- sourced from the WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW WFS layer
        -- instead of the CSV reference table. A sheet can live in one
        -- source and not the other (confirmed live: HARRISON LAKE /
        -- 00081HARR is in this WFS layer but missing from the CSV) — kept
        -- as its own parallel table rather than merged into bathy_surveys
        -- so downstream matching can treat the CSV as primary and this as
        -- an explicit gap-fill fallback rather than silently overwriting.
        CREATE TABLE IF NOT EXISTS bathy_surveys_wfs (
            identifier      TEXT,
            gazetted_name   TEXT,
            watershed_code  TEXT,
            map_title       TEXT,
            pdf_url         TEXT,
            pdf_filename    TEXT PRIMARY KEY,
            fetched_at      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_bathy_surveys_wfs_identifier
            ON bathy_surveys_wfs (identifier);
        CREATE INDEX IF NOT EXISTS idx_bathy_surveys_wfs_name
            ON bathy_surveys_wfs (gazetted_name);
        """
    )
    return conn


def attribution() -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    return (
        "Extracted from the BC Data Catalogue's 'Bathymetry Open Reference "
        f"Table and Maps' dataset on {today}"
    )


def fetch_bathy_csv(url: str = BATHY_CSV_URL) -> pd.DataFrame:
    logger.info("Downloading WSA bathymetry reference CSV ...")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        df = pd.read_csv(resp, dtype=str)
    logger.info("Downloaded %d bathymetry survey row(s).", len(df))
    return df


def _cell(value: object) -> str:
    """Blank-safe cell -> str. Even with pd.read_csv(dtype=str), a genuinely
    empty CSV cell comes back as float NaN, not "" — str(value or "") still
    turns that into the literal string "nan" (NaN is truthy), so this
    checks pd.isna() first rather than relying on truthiness."""
    return "" if pd.isna(value) else str(value).strip()


def store_surveys(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    missing = {COL_ID, COL_NAME, COL_WSC, COL_MAPTITLE, COL_PDF, COL_PDF_FILENAME} - set(df.columns)
    if missing:
        raise ValueError(f"Bathymetry CSV missing expected column(s): {sorted(missing)}")

    now = datetime.now(timezone.utc).isoformat()
    rows = [
        (
            _cell(row[COL_ID]),
            _cell(row[COL_NAME]),
            _cell(row[COL_WSC]),
            _cell(row[COL_MAPTITLE]),
            _cell(row[COL_PDF]),
            _cell(row[COL_PDF_FILENAME]).lower(),
            now,
        )
        for _, row in df.iterrows()
        if not pd.isna(row[COL_PDF_FILENAME]) and str(row[COL_PDF_FILENAME]).strip()
    ]
    # Full replace, not an upsert — same reasoning as stocking's write_matches:
    # a stale sheet left over from a prior fetch would misreport the catalogue.
    conn.execute("DELETE FROM bathy_surveys")
    conn.executemany(
        """INSERT OR REPLACE INTO bathy_surveys
           (identifier, gazetted_name, watershed_code, map_title, pdf_url, pdf_filename, fetched_at)
           VALUES (?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()
    logger.info("Stored %d survey sheet(s) -> bathy_surveys.", len(rows))
    return len(rows)


def _pdf_filename_from_url(pdf_url: str) -> str:
    """Pull the ?filename=... query param off a WSA download URL, lowercased
    — the WFS layer has no MAP_IMAGE_FILENAME_PDF column of its own, but its
    PDF_FILE_URL points at the same downloadBathymetricMap.do endpoint the
    CSV's pdf_filename is keyed on, so this derives an equivalent value."""
    if not pdf_url:
        return ""
    query = urllib.parse.urlparse(pdf_url).query
    values = urllib.parse.parse_qs(query).get("filename") or []
    return values[0].strip().lower() if values else ""


def store_wfs_survey_sheets(conn: sqlite3.Connection, gdf: gpd.GeoDataFrame) -> int:
    """Store the survey-sheet WFS layer's own tabular attributes into
    bathy_surveys_wfs — see that table's schema comment in connect() for why
    this stays separate from bathy_surveys rather than merging in."""
    missing = {WFS_COL_ID, WFS_COL_NAME, WFS_COL_WSC, WFS_COL_MAPTITLE, WFS_COL_PDF} - set(gdf.columns)
    if missing:
        raise ValueError(f"Bathymetry WFS layer missing expected column(s): {sorted(missing)}")

    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for _, row in gdf.iterrows():
        pdf_url = _cell(row[WFS_COL_PDF])
        pdf_filename = _pdf_filename_from_url(pdf_url)
        if not pdf_filename:
            continue
        rows.append(
            (
                _cell(row[WFS_COL_ID]),
                _cell(row[WFS_COL_NAME]),
                _cell(row[WFS_COL_WSC]),
                _cell(row[WFS_COL_MAPTITLE]),
                pdf_url,
                pdf_filename,
                now,
            )
        )
    # Full replace, not an upsert — same reasoning as store_surveys().
    conn.execute("DELETE FROM bathy_surveys_wfs")
    conn.executemany(
        """INSERT OR REPLACE INTO bathy_surveys_wfs
           (identifier, gazetted_name, watershed_code, map_title, pdf_url, pdf_filename, fetched_at)
           VALUES (?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()
    logger.info("Stored %d survey sheet(s) -> bathy_surveys_wfs.", len(rows))
    return len(rows)


def fetch_survey_polygons(conn: sqlite3.Connection, gpkg_path: Path = GPKG_PATH) -> int:
    """Paginated WFS fetch of WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW into
    ``gpkg_path`` (layer POLYGON_LAYER) — mirrors data/fetch_data.py's own
    fetch_wfs_paginated() (same pagination shape, same reprojection-to-
    EPSG:3005-if-needed step), reimplemented locally so this feed stays a
    standalone script per live-data/README.md's POC convention rather than
    depending on the batch data/ pipeline. Returns the feature count
    written. Network/service failure is loud (raises), unlike the WDIC
    lookup in waterbody_matcher.py — this is the primary fetch for the
    layer's geometry (kept for whenever polygon-overlap matching gets
    revisited) as well as this layer's own tabular attributes, stored
    alongside via store_wfs_survey_sheets() as a gap-fill fallback for
    identifiers the CSV reference table is missing.
    """
    logger.info("Fetching survey-sheet polygons (WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW) ...")
    start_index = 0
    max_features = 10000
    chunks: List[gpd.GeoDataFrame] = []
    while True:
        params = {
            "SERVICE": "WFS", "VERSION": "2.0.0", "REQUEST": "GetFeature",
            "typeNames": "pub:WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW",
            "outputFormat": "json",
            "SRSNAME": "EPSG:3005",
            "count": max_features,
            "startIndex": start_index,
            "sortBy": "OBJECTID",
        }
        url = f"{BATHY_POLYGON_WFS_URL}?{urllib.parse.urlencode(params)}"
        chunk = gpd.read_file(url)
        if chunk.empty:
            break
        chunks.append(chunk)
        logger.info("  ... %d feature(s) so far", start_index + len(chunk))
        if len(chunk) < max_features:
            break
        start_index += max_features

    if not chunks:
        logger.warning("No survey-sheet polygons returned — layer left unwritten.")
        return 0

    gdf = pd.concat(chunks, ignore_index=True)
    if gdf.crs and gdf.crs.to_epsg() != 3005:
        gdf = gdf.to_crs(epsg=3005)

    store_wfs_survey_sheets(conn, gdf)

    gdf.to_file(gpkg_path, layer=POLYGON_LAYER, driver="GPKG", engine="pyogrio")
    logger.info("Wrote %d survey-sheet polygon(s) -> %s [%s]", len(gdf), gpkg_path, POLYGON_LAYER)
    return len(gdf)


def main(argv: List[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(description="Fetch BC WSA bathymetric survey maps.")
    parser.add_argument(
        "--csv-only", action="store_true",
        help="Skip the survey-sheet polygon WFS fetch (faster, but "
             "bathy_surveys_wfs won't get refreshed and any CSV coverage "
             "gaps it fills in — e.g. HARRISON LAKE — go stale).",
    )
    args = parser.parse_args(argv)

    conn = connect()
    try:
        df = fetch_bathy_csv()
        store_surveys(conn, df)

        if not args.csv_only:
            fetch_survey_polygons(conn)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
