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
from pathlib import Path
from typing import List, Optional
from tqdm import tqdm

from project_config import get_config

logger = logging.getLogger(__name__)


# ==========================================
# 1. CORE FUNCTIONS
# ==========================================


def fetch_wfs_paginated(
    short_name: str, type_name: str, gpkg_path: Path, temp_dir: Path, sort_field: str = "OBJECTID"
) -> None:
    """Downloads WFS data in 10,000 feature chunks."""
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
            raise RuntimeError(
                f"WFS fetch failed for '{type_name}' at index {start_index}: {e}"
            ) from e

    if all_chunks:
        final_gdf = pd.concat(all_chunks, ignore_index=True)

        # Reproject to BC Albers (EPSG:3005) for consistency with FWA layers
        if final_gdf.crs and final_gdf.crs.to_epsg() != 3005:
            print(f"  -> Reprojecting from {final_gdf.crs} to EPSG:3005...")
            final_gdf = final_gdf.to_crs(epsg=3005)

        final_gdf.to_file(gpkg_path, layer=short_name, driver="GPKG", engine="pyogrio")
        print(f"  -> Saved {len(final_gdf)} features to {short_name}")


def ensure_ftp_extracted(ftp_url: str, temp_dir: Path) -> Path:
    """Downloads and extracts a zip from an FTP URL, returning the .gdb path."""
    zip_path = temp_dir / os.path.basename(ftp_url)
    gdb_name = zip_path.name.replace(".zip", ".gdb")
    gdb_path = temp_dir / gdb_name

    if not zip_path.exists():
        print(f"\n[FTP] Downloading: {zip_path.name}")

        # Download with progress bar
        with tqdm(
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            miniters=1,
            desc="  Progress",
            dynamic_ncols=True,
        ) as pbar:

            def reporthook(block_num, block_size, total_size):
                if total_size > 0 and pbar.total is None:
                    pbar.total = total_size
                downloaded = block_num * block_size
                if block_num > 0:
                    pbar.update(block_size)

            urllib.request.urlretrieve(ftp_url, zip_path, reporthook=reporthook)

    if not gdb_path.exists():
        print(f"  -> Extracting {zip_path.name}...")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            members = zip_ref.namelist()
            for member in tqdm(members, desc="  Progress", unit="file"):
                zip_ref.extract(member, temp_dir)

    return gdb_path


def extract_gdb_layer(short_name: str, ftp_url: str, gdb_layer: str, gpkg_path: Path, temp_dir: Path) -> None:
    """Extracts a specific layer from a local GDB and saves it to the GPKG."""
    print(f"\n[GDB] Extracting: {short_name} from {gdb_layer}")
    gdb_path = ensure_ftp_extracted(ftp_url, temp_dir)

    try:
        gdf = gpd.read_file(gdb_path, layer=gdb_layer, engine="pyogrio", use_arrow=True)

        # Reproject to BC Albers (EPSG:3005) for consistency
        if gdf.crs and gdf.crs.to_epsg() != 3005:
            print(f"  -> Reprojecting from {gdf.crs} to EPSG:3005...")
            gdf = gdf.to_crs(epsg=3005)

        gdf.to_file(gpkg_path, layer=short_name, driver="GPKG", engine="pyogrio")
        print(f"  -> Saved {len(gdf)} features to {short_name}")
    except Exception as e:
        logger.warning("Error processing %s: %s", short_name, e)
        raise


def combine_streams(short_name: str, ftp_url: str, gpkg_path: Path, temp_dir: Path) -> None:
    """Iterates through all watershed blocks in the GDB and combines them into one GPKG layer."""
    print(f"\n[STREAMS] Building master network: {short_name}")
    gdb_path = ensure_ftp_extracted(ftp_url, temp_dir)

    layers = [lyr for lyr in fiona.listlayers(str(gdb_path)) if isinstance(lyr, str)]
    is_first = True

    for lyr in tqdm(layers, desc=f"Merging watersheds into '{short_name}'"):
        try:
            gdf = gpd.read_file(gdb_path, layer=lyr, engine="pyogrio", use_arrow=True)
            if gdf.empty:
                continue

            # Reproject to BC Albers (EPSG:3005) for consistency
            if gdf.crs and gdf.crs.to_epsg() != 3005:
                gdf = gdf.to_crs(epsg=3005)

            # mode="w" (overwrite) for the first chunk, mode="a" (append) for the rest
            write_mode = "w" if is_first else "a"
            gdf.to_file(
                gpkg_path,
                layer=short_name,
                driver="GPKG",
                engine="pyogrio",
                mode=write_mode,
            )
            is_first = False

        except Exception as e:
            logger.warning("Failed to process block for '%s': %s", short_name, e)
            raise


# ==========================================
# 2. MAIN EXECUTION
# ==========================================


def main() -> int:
    """CLI entry point for BC GIS data fetching."""
    # Load config for default paths
    config = get_config()
    default_output = str(config.fetch_output_gpkg_path)
    default_temp = str(config.fetch_temp_dir)

    # Define FTP Sources (constants)
    FTP_FWA_BC = (
        "ftp://ftp.geobc.gov.bc.ca/sections/outgoing/bmgs/FWA_Public/FWA_BC.zip"
    )
    FTP_STREAMS = "ftp://ftp.geobc.gov.bc.ca/sections/outgoing/bmgs/FWA_Public/FWA_STREAM_NETWORKS_SP.zip"

    # Dataset manifest - defines all available layers
    DATASETS = {
        # --- WFS Layers (Admin Boundaries) ---
        "wma": {"type": "WFS", "source": "WHSE_TANTALIS.TA_WILDLIFE_MGMT_AREAS_SVW"},
        "wmu": {
            "type": "WFS",
            "source": "WHSE_WILDLIFE_MANAGEMENT.WAA_WILDLIFE_MGMT_UNITS_SVW",
        },
        "parks_bc": {"type": "WFS", "source": "WHSE_TANTALIS.TA_PARK_ECORES_PA_SVW"},
        "parks_nat": {
            "type": "WFS",
            "source": "WHSE_ADMIN_BOUNDARIES.CLAB_NATIONAL_PARKS",
            "sort_field": "NATIONAL_PARK_ID",
        },
        "historic_sites": {
            "type": "WFS",
            "source": "WHSE_HUMAN_CULTURAL_ECONOMIC.HIST_HISTORIC_ENVIRONMNT_PA_SV",
        },  # will use 4533329 as id
        # --- FTP Geodatabase Layers (FWA Polygons) ---
        "lakes": {"type": "FWA_GDB", "ftp": FTP_FWA_BC, "layer": "FWA_LAKES_POLY"},
        "wetlands": {
            "type": "FWA_GDB",
            "ftp": FTP_FWA_BC,
            "layer": "FWA_WETLANDS_POLY",
        },
        "watersheds": {
            "type": "FWA_GDB",
            "ftp": FTP_FWA_BC,
            "layer": "FWA_NAMED_WATERSHEDS_POLY",
        },
        "manmade_water": {
            "type": "FWA_GDB",
            "ftp": FTP_FWA_BC,
            "layer": "FWA_MANMADE_WATERBODIES_POLY",
        },
        # --- FTP Streams (Requires combining multiple GDB layers) ---
        "streams": {"type": "FWA_STREAMS", "ftp": FTP_STREAMS},
    }

    parser = argparse.ArgumentParser(
        description="BC Freshwater Fishing Regulations - Data Fetch Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download all datasets with defaults from config
  python -m data.fetch_data

  # Specify custom output location
  python -m data.fetch_data --output custom_data.gpkg
  
  # Download specific layers only
  python -m data.fetch_data --layers lakes streams
  
  # Reload a specific layer
  python -m data.fetch_data --reload-layer lakes
  
  # Reload all WFS layers (admin boundaries)
  python -m data.fetch_data --reload-wfs
        """,
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=default_output,
        help=f"Output GeoPackage path (default: {default_output})",
    )

    parser.add_argument(
        "--temp-dir",
        type=Path,
        default=default_temp,
        help=f"Temporary download directory (default: {default_temp})",
    )

    parser.add_argument(
        "--layers",
        type=str,
        nargs="+",
        help="Only download specific layers (e.g., lakes streams)",
    )

    parser.add_argument(
        "--reload-layer",
        type=str,
        nargs="+",
        help="Reload specific layer(s) by name (e.g., lakes streams). Alias for --layers.",
    )

    parser.add_argument(
        "--reload-wfs",
        action="store_true",
        help="Reload all WFS layers (admin boundaries: wma, wmu, parks_bc, parks_nat, historic_sites)",
    )

    args = parser.parse_args()

    # Create temp directory (clean slate for each run)
    temp_dir = Path(args.temp_dir)
    if temp_dir.exists():
        print(f"🗑️  Cleaning temp directory: {temp_dir}")
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)

    gpkg_out = Path(args.output)

    # Filter datasets if specific layers requested
    datasets_to_fetch = DATASETS
    selected_layers = args.layers or args.reload_layer

    # Handle --reload-wfs flag (select all WFS layers)
    if args.reload_wfs:
        wfs_layers = [k for k, v in DATASETS.items() if v["type"] == "WFS"]
        if selected_layers:
            # Merge with other selected layers
            selected_layers = list(set(selected_layers + wfs_layers))
        else:
            selected_layers = wfs_layers

    if selected_layers:
        datasets_to_fetch = {k: v for k, v in DATASETS.items() if k in selected_layers}
        if not datasets_to_fetch:
            print(
                f"❌ Error: No matching layers found. Available layers: {', '.join(DATASETS.keys())}"
            )
            return 1

    # Print configuration
    print("=" * 80)
    print("BC FRESHWATER FISHING REGULATIONS - DATA FETCH")
    print("=" * 80)

    print("\n📁 Output:")
    print(f"  GeoPackage: {gpkg_out}")
    print(f"  Temp directory: {temp_dir}")

    print("\n⚙️  Configuration:")
    print(f"  Total datasets: {len(DATASETS)}")
    print(f"  Fetching: {len(datasets_to_fetch)} layer(s)")
    if selected_layers:
        print(f"  Selected layers: {', '.join(datasets_to_fetch.keys())}")

    print("\n📥 Datasets:")
    for layer_name in datasets_to_fetch.keys():
        layer_type = datasets_to_fetch[layer_name]["type"]
        print(f"  • {layer_name:<15} [{layer_type}]")
    print()

    print(f"Starting download pipeline...")
    print()

    # Fetch each dataset
    for short_name, config_dict in datasets_to_fetch.items():
        try:
            if config_dict["type"] == "WFS":
                sort_field = config_dict.get("sort_field", "OBJECTID")
                fetch_wfs_paginated(
                    short_name, config_dict["source"], gpkg_out, temp_dir, sort_field
                )

            elif config_dict["type"] == "FWA_GDB":
                extract_gdb_layer(
                    short_name,
                    config_dict["ftp"],
                    config_dict["layer"],
                    gpkg_out,
                    temp_dir,
                )

            elif config_dict["type"] == "FWA_STREAMS":
                combine_streams(short_name, config_dict["ftp"], gpkg_out, temp_dir)

        except Exception as e:
            print(f"❌ Critical failure on {short_name}: {e}")
            # Continue with other datasets even if one fails

    print("\n✅ Data fetch complete!")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    exit(main())
