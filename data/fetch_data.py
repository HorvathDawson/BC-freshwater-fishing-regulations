import os
import argparse
import logging
import urllib.request
import urllib.parse
import zipfile
import shutil
import fiona
import geopandas as gpd
import pandas as pd
import osmnx as ox
from pathlib import Path
from tqdm import tqdm

from project_config import get_config

logger = logging.getLogger(__name__)

# ==========================================
# 1. CORE FUNCTIONS
# ==========================================


def fetch_wfs_paginated(
    short_name, type_name, gpkg_path, temp_dir, sort_field="OBJECTID"
):
    print(f"\n[WFS] Fetching: {short_name} ({type_name})")
    start_index = 0
    max_features = 10000
    all_chunks = []
    while True:
        params = {
            "SERVICE": "WFS",
            "VERSION": "2.0.0",
            "REQUEST": "GetFeature",
            "typeNames": type_name,
            "outputFormat": "json",
            "SRSNAME": "EPSG:4326",
            "count": max_features,
            "startIndex": start_index,
            "sortBy": sort_field,
        }
        url = f"https://openmaps.gov.bc.ca/geo/pub/ows?{urllib.parse.urlencode(params)}"
        try:
            chunk = gpd.read_file(url)
            if chunk.empty:
                break
            all_chunks.append(chunk)
            if len(chunk) < max_features:
                break
            start_index += max_features
        except Exception as e:
            raise RuntimeError(f"WFS fetch failed for '{type_name}': {e}") from e
    if all_chunks:
        final_gdf = pd.concat(all_chunks, ignore_index=True)
        if final_gdf.crs and final_gdf.crs.to_epsg() != 3005:
            final_gdf = final_gdf.to_crs(epsg=3005)
        final_gdf.to_file(gpkg_path, layer=short_name, driver="GPKG", engine="pyogrio")


def fetch_osm_roi_boundaries(short_name, queries, gpkg_path):
    """Fetches multiple OSM polygons and merges them into a single ROI layer."""
    print(f"\n[OSM] Building ROI Layer: {short_name}")
    is_first = True

    for query in queries:
        print(f"  -> Fetching query: {query}")
        try:
            gdf = ox.geocoder.geocode_to_gdf(query)
            if gdf.empty:
                print(f"     ⚠️ No results for {query}")
                continue

            if gdf.crs and gdf.crs.to_epsg() != 3005:
                gdf = gdf.to_crs(epsg=3005)

            # Clean list-type columns for GPKG compatibility
            for col in gdf.columns:
                if gdf[col].apply(lambda x: isinstance(x, list)).any():
                    gdf[col] = gdf[col].apply(str)

            # Determine write mode: overwrite on first success, append thereafter
            write_mode = "w" if is_first else "a"
            gdf.to_file(
                gpkg_path,
                layer=short_name,
                driver="GPKG",
                engine="pyogrio",
                mode=write_mode,
            )
            is_first = False
            print(f"     ✅ Added {query}")

        except Exception as e:
            logger.warning("OSM fetch failed for '%s': %s", query, e)


def ensure_ftp_extracted(ftp_url, temp_dir):
    zip_path = temp_dir / os.path.basename(ftp_url)
    gdb_path = temp_dir / zip_path.name.replace(".zip", ".gdb")
    if not zip_path.exists():
        print(f"\n[FTP] Downloading: {zip_path.name}")
        urllib.request.urlretrieve(ftp_url, zip_path)
    if not gdb_path.exists():
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)
    return gdb_path


def extract_gdb_layer(short_name, ftp_url, gdb_layer, gpkg_path, temp_dir):
    print(f"\n[GDB] Extracting: {short_name}")
    gdb_path = ensure_ftp_extracted(ftp_url, temp_dir)
    gdf = gpd.read_file(gdb_path, layer=gdb_layer, engine="pyogrio")
    if gdf.crs and gdf.crs.to_epsg() != 3005:
        gdf = gdf.to_crs(epsg=3005)
    gdf.to_file(gpkg_path, layer=short_name, driver="GPKG", engine="pyogrio")


def combine_streams(short_name, ftp_url, gpkg_path, temp_dir):
    print(f"\n[STREAMS] Merging watershed blocks...")
    gdb_path = ensure_ftp_extracted(ftp_url, temp_dir)
    layers = [lyr for lyr in fiona.listlayers(str(gdb_path)) if isinstance(lyr, str)]
    is_first = True
    for lyr in tqdm(layers):
        gdf = gpd.read_file(gdb_path, layer=lyr, engine="pyogrio")
        if gdf.empty:
            continue
        if gdf.crs and gdf.crs.to_epsg() != 3005:
            gdf = gdf.to_crs(epsg=3005)
        mode = "w" if is_first else "a"
        gdf.to_file(
            gpkg_path, layer=short_name, driver="GPKG", engine="pyogrio", mode=mode
        )
        is_first = False


# ==========================================
# 2. MAIN EXECUTION
# ==========================================


def main():
    config = get_config()
    gpkg_out = Path(config.fetch_output_gpkg_path)
    temp_dir = Path(config.fetch_temp_dir)

    FTP_FWA = "ftp://ftp.geobc.gov.bc.ca/sections/outgoing/bmgs/FWA_Public/FWA_BC.zip"
    FTP_STR = "ftp://ftp.geobc.gov.bc.ca/sections/outgoing/bmgs/FWA_Public/FWA_STREAM_NETWORKS_SP.zip"

    DATASETS = {
        # OSM Admin Boundaries Layer - Add as many names as you like to this list
        "osm_admin_boundaries": {
            "type": "OSM_ADMIN",
            "queries": [
                "Malcolm Knapp Research Forest",
            ],
        },
        "wma": {"type": "WFS", "source": "WHSE_TANTALIS.TA_WILDLIFE_MGMT_AREAS_SVW"},
        "wmu": {
            "type": "WFS",
            "source": "WHSE_WILDLIFE_MANAGEMENT.WAA_WILDLIFE_MGMT_UNITS_SVW",
        },
        "parks_bc": {"type": "WFS", "source": "WHSE_TANTALIS.TA_PARK_ECORES_PA_SVW"},
        "lakes": {"type": "FWA_GDB", "ftp": FTP_FWA, "layer": "FWA_LAKES_POLY"},
        "wetlands": {"type": "FWA_GDB", "ftp": FTP_FWA, "layer": "FWA_WETLANDS_POLY"},
        "streams": {"type": "FWA_STREAMS", "ftp": FTP_STR},
    }

    parser = argparse.ArgumentParser(description="BC Fresh Water Data Fetcher")
    parser.add_argument("--layers", nargs="+", help="Explicitly list layers to fetch")
    parser.add_argument(
        "--skip-streams", action="store_true", help="Skip heavy stream network merge"
    )
    parser.add_argument(
        "--skip-ftp", action="store_true", help="Skip all heavy FTP downloads"
    )
    args = parser.parse_args()

    if not temp_dir.exists():
        temp_dir.mkdir(parents=True)

    # Filtering Logic
    if args.layers:
        to_fetch = {k: v for k, v in DATASETS.items() if k in args.layers}
    else:
        to_fetch = DATASETS.copy()
        if args.skip_ftp:
            to_fetch = {
                k: v
                for k, v in to_fetch.items()
                if "ftp" not in v and v["type"] != "FWA_STREAMS"
            }
        elif args.skip_streams:
            to_fetch.pop("streams", None)

    for name, cfg in to_fetch.items():
        try:
            if cfg["type"] == "WFS":
                fetch_wfs_paginated(name, cfg["source"], gpkg_out, temp_dir)
            elif cfg["type"] in ("OSM_ROI", "OSM_ADMIN"):
                fetch_osm_roi_boundaries(name, cfg["queries"], gpkg_out)
            elif cfg["type"] == "FWA_GDB":
                extract_gdb_layer(name, cfg["ftp"], cfg["layer"], gpkg_out, temp_dir)
            elif cfg["type"] == "FWA_STREAMS":
                combine_streams(name, cfg["ftp"], gpkg_out, temp_dir)
        except Exception as e:
            print(f"❌ Error on {name}: {e}")

    print("\n✅ Data fetch complete!")


if __name__ == "__main__":
    main()
