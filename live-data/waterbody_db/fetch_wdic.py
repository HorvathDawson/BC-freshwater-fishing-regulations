#!/usr/bin/env python3
"""
fetch_wdic.py — fetches the *entire* WHSE_FISH.WDIC_WATERBODY_POLY_SVW
layer ("WSA - Water Polygon Features (50,000)", BC Data Catalogue
414be2d6-f4d9-4f32-b960-caa074c6d36b) into `wdic_cache`, one table in this
directory's shared `waterbody_db.db` (see this dir's README) — self-
contained: it doesn't need `fetch_stocking.py`/`fetch_bathymetry.py` (this
folder's own replicated copies, or the originals in `../stocking/`,
`../bathymetry/`) to have run first. Every one of BC's ~322k WDIC waterbody
records that carries a WATERBODY_IDENTIFIER (the ~26k that don't are
river/generic polygons this identifier scheme can never match anything to,
so they're filtered out server-side rather than paying to download and then
discard them) — not just the ~3,452 identifiers `stocking`/`bathymetry`
happen to reference today, so this cache stays useful standalone and
doesn't need re-fetching every time those feeds' own scope changes.

Why WFS, not WMS
-----------------
The BC Data Catalogue page for this dataset (both on catalogue.data.gov.bc.ca
and its open.canada.ca mirror) only *lists* WMS and a KML ground-overlay
loader as resources — no WFS entry. But WMS is a map-*image* service
(GetMap tiles, or GetFeatureInfo for a single queried point/pixel); it has
no bulk mechanism for pulling attribute data like WATERBODY_IDENTIFIER or
GAZETTED_NAME out of ~300k records. WFS GetFeature — same server
(openmaps.gov.bc.ca), same `pub:WHSE_FISH.WDIC_WATERBODY_POLY_SVW` layer,
just SERVICE=WFS instead of SERVICE=WMS — is the actual vector-data service
underneath, confirmed live: it returns full GeoJSON features with every
attribute and geometry, paginated (see _PAGE_SIZE). This is the same
approach fetch_bathymetry.py's fetch_survey_polygons() already uses for a
different WHSE_FISH layer on the same server. No bulk CSV/shapefile
download of any kind exists for this dataset (confirmed live: the
catalogue's own "Custom Download" resource is indirect-access-only with an
empty URL — the same authenticated order-and-email workflow
stocking/README.md documents for FIDQ) — paginated WFS is the only way to
get this data at all, bulk or otherwise.

What's cached
--------------
Per record: WATERBODY_IDENTIFIER, WATERBODY_POLY_ID, WBODY_ID,
GAZETTED_NAME, and an *approximate* lat/lng (the polygon's own
representative_point(), reprojected from WDIC's native EPSG:3005 to WGS84
right here at fetch time) — not the full polygon geometry, since nothing
downstream (match.py) needs more than a representative coordinate.
PROPERTYNAME on the WFS request limits each page to just the columns this
module actually uses, keeping the transient per-page download small even
though the full layer's geometries alone run into the hundreds of MB
uncompressed.

CLI
---
    cd live-data/waterbody_db
    python fetch_wdic.py             # fetch the full WDIC layer into waterbody_db.db's wdic_cache
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import geopandas as gpd
from pyproj import Transformer

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "waterbody_db.db"

WDIC_WFS_URL = "https://openmaps.gov.bc.ca/geo/pub/WHSE_FISH.WDIC_WATERBODY_POLY_SVW/ows"
WDIC_TYPE_NAME = "pub:WHSE_FISH.WDIC_WATERBODY_POLY_SVW"

# Reprojects WDIC's native EPSG:3005 polygon centroid/representative_point
# to WGS84 lat/lng for storage — a portable coordinate any downstream
# consumer can use without needing BC Albers.
_TO_WGS84 = Transformer.from_crs("EPSG:3005", "EPSG:4326", always_xy=True)

# WFS GetFeature page size — same pagination shape fetch_bathymetry.py's
# fetch_survey_polygons() already uses for a different layer on this server.
_PAGE_SIZE = 10000

_PROPERTIES = "WATERBODY_IDENTIFIER,WATERBODY_POLY_ID,WBODY_ID,GAZETTED_NAME,GEOMETRY"


def fetch_all_wdic_records() -> Dict[str, Dict[str, Any]]:
    """identifier -> {poly_id, wbody_id, gazetted_name, approx_lat, approx_lng}
    for every WDIC record that carries a WATERBODY_IDENTIFIER, paginated over
    the entire layer (see _PAGE_SIZE)."""
    out: Dict[str, Dict[str, Any]] = {}
    start_index = 0
    page = 0
    while True:
        page += 1
        params = {
            "SERVICE": "WFS", "VERSION": "2.0.0", "REQUEST": "GetFeature",
            "typeNames": WDIC_TYPE_NAME,
            "outputFormat": "json",
            "CQL_FILTER": "WATERBODY_IDENTIFIER IS NOT NULL",
            "PROPERTYNAME": _PROPERTIES,
            "count": _PAGE_SIZE,
            "startIndex": start_index,
            "sortBy": "OBJECTID",
        }
        url = f"{WDIC_WFS_URL}?{urllib.parse.urlencode(params)}"
        gdf = gpd.read_file(url)
        if gdf.empty:
            break
        if gdf.crs and gdf.crs.to_epsg() != 3005:
            gdf = gdf.to_crs(epsg=3005)
        for _, row in gdf.iterrows():
            if row.geometry is None or row.geometry.is_empty:
                continue
            ident = str(row.get("WATERBODY_IDENTIFIER", "") or "").strip()
            if not ident:
                continue
            pt = row.geometry.representative_point()
            lng, lat = _TO_WGS84.transform(pt.x, pt.y)
            out[ident] = {
                "poly_id": str(row.get("WATERBODY_POLY_ID", "") or ""),
                "wbody_id": str(row.get("WBODY_ID", "") or ""),
                "gazetted_name": str(row.get("GAZETTED_NAME", "") or ""),
                "approx_lat": lat,
                "approx_lng": lng,
            }
        logger.info("  page %d: %d feature(s) (%d unique identifier(s) so far)",
                    page, len(gdf), len(out))
        if len(gdf) < _PAGE_SIZE:
            break
        start_index += _PAGE_SIZE
    return out


def connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def store_cache(conn: sqlite3.Connection, wdic: Dict[str, Dict[str, Any]]) -> int:
    conn.execute("DROP TABLE IF EXISTS wdic_cache")
    conn.execute(
        """CREATE TABLE wdic_cache (
            identifier      TEXT PRIMARY KEY,
            poly_id         TEXT,
            wbody_id        TEXT,
            gazetted_name   TEXT,
            approx_lat      REAL,
            approx_lng      REAL,
            fetched_at      TEXT
        )"""
    )
    now = datetime.now(timezone.utc).isoformat()
    conn.executemany(
        """INSERT INTO wdic_cache
           (identifier, poly_id, wbody_id, gazetted_name, approx_lat, approx_lng, fetched_at)
           VALUES (?,?,?,?,?,?,?)""",
        [
            (ident, hit["poly_id"], hit["wbody_id"], hit["gazetted_name"],
             hit["approx_lat"], hit["approx_lng"], now)
            for ident, hit in wdic.items()
        ],
    )
    conn.commit()
    logger.info("Stored %d WDIC record(s) -> wdic_cache.", len(wdic))
    return len(wdic)


def main(argv: List[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(
        description="Fetch the entire WDIC 1:50K waterbody reference layer into a local cache.",
    )
    parser.parse_args(argv)

    logger.info("Fetching WHSE_FISH.WDIC_WATERBODY_POLY_SVW (paginated, %d/page) ...", _PAGE_SIZE)
    wdic = fetch_all_wdic_records()

    conn = connect()
    try:
        store_cache(conn, wdic)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
