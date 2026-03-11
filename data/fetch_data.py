import os
import json
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
from shapely.geometry import Polygon, LineString
from shapely.ops import polygonize, linemerge
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


def fetch_overpass_aboriginal_lands(short_name: str, gpkg_path: Path) -> None:
    """Fetch all boundary=aboriginal_lands polygons in BC from the Overpass API.

    Queries for relations tagged ``boundary=aboriginal_lands`` within the
    bounding box of British Columbia.  Each polygon is saved with its OSM
    tags (name, name:en, indigenous name, url, wikidata, wikipedia).
    """
    print(f"\n[OVERPASS] Fetching aboriginal lands for BC...")

    # BC bounding box (lat/lon): south, west, north, east
    bbox = "48.2,-139.1,60.0,-114.0"

    query = f"""
[out:json][timeout:120];
(
  relation["boundary"="aboriginal_lands"]({bbox});
  way["boundary"="aboriginal_lands"]({bbox});
);
out body;
>;
out skel qt;
"""

    overpass_url = "https://overpass-api.de/api/interpreter"
    encoded = urllib.parse.urlencode({"data": query})
    req = urllib.request.Request(
        overpass_url,
        data=encoded.encode("utf-8"),
        headers={"User-Agent": "BC-FishRegs-DataFetch/1.0"},
    )
    print("  -> Querying Overpass API (this may take a minute)...")
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        raise RuntimeError(f"Overpass fetch failed for aboriginal lands: {e}") from e

    elements = raw.get("elements", [])
    nodes = {e["id"]: e for e in elements if e["type"] == "node"}
    ways = {e["id"]: e for e in elements if e["type"] == "way"}
    relations = [e for e in elements if e["type"] == "relation"]
    standalone_ways = [
        e
        for e in elements
        if e["type"] == "way"
        and e.get("tags", {}).get("boundary") == "aboriginal_lands"
    ]

    def _way_coords(way_element):
        """Return list of (lon, lat) for a way's node refs."""
        return [
            (nodes[nid]["lon"], nodes[nid]["lat"])
            for nid in way_element.get("nodes", [])
            if nid in nodes
        ]

    def _build_polygon_from_relation(rel):
        """Assemble a polygon from a relation's outer/inner way members."""
        outers, inners = [], []
        for member in rel.get("members", []):
            if member["type"] != "way" or member["ref"] not in ways:
                continue
            coords = _way_coords(ways[member["ref"]])
            if len(coords) < 2:
                continue
            role = member.get("role", "outer")
            if role == "inner":
                inners.append(coords)
            else:
                outers.append(coords)

        if not outers:
            return None

        # Merge outer way segments into closed rings
        outer_lines = [LineString(c) for c in outers if len(c) >= 2]
        if not outer_lines:
            return None
        merged = linemerge(outer_lines)
        outer_polys = list(polygonize(merged))

        inner_polys = []
        if inners:
            inner_lines = [LineString(c) for c in inners if len(c) >= 2]
            if inner_lines:
                inner_merged = linemerge(inner_lines)
                inner_polys = list(polygonize(inner_merged))

        if not outer_polys:
            return None

        # Subtract inner rings from outer polygons
        result = outer_polys[0]
        for op in outer_polys[1:]:
            result = result.union(op)
        for ip in inner_polys:
            result = result.difference(ip)

        return result if not result.is_empty else None

    def _build_polygon_from_way(way_el):
        """Build a polygon from a standalone closed way."""
        coords = _way_coords(way_el)
        if len(coords) >= 4:
            return Polygon(coords)
        return None

    def _tags_to_record(osm_id, tags, geom):
        """Build a GeoDataFrame-ready dict from OSM tags."""
        return {
            "osm_id": str(osm_id),
            "name": tags.get("name", ""),
            "name_en": tags.get("name:en", tags.get("name", "")),
            # Best-effort indigenous name: first name:* tag that isn't en/fr.
            # OSM tag order is non-deterministic so this is approximate.
            "name_indigenous": next(
                (
                    v
                    for k, v in tags.items()
                    if k.startswith("name:") and k != "name:en" and k != "name:fr"
                ),
                "",
            ),
            "boundary": tags.get("boundary", ""),
            "type": "aboriginal_lands",
            "url": tags.get("url", tags.get("website", "")),
            "wikidata": tags.get("wikidata", ""),
            "wikipedia": tags.get("wikipedia", ""),
            "geometry": geom,
        }

    records = []
    # Process relations
    for rel in relations:
        tags = rel.get("tags", {})
        geom = _build_polygon_from_relation(rel)
        if geom is None:
            continue
        records.append(_tags_to_record(rel["id"], tags, geom))

    # Process standalone ways (not part of a relation)
    relation_way_ids = set()
    for rel in relations:
        for m in rel.get("members", []):
            if m["type"] == "way":
                relation_way_ids.add(m["ref"])
    for way_el in standalone_ways:
        if way_el["id"] in relation_way_ids:
            continue
        tags = way_el.get("tags", {})
        geom = _build_polygon_from_way(way_el)
        if geom is None:
            continue
        records.append(_tags_to_record(way_el["id"], tags, geom))

    if not records:
        print("  ⚠️  No aboriginal lands polygons found.")
        return

    gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")
    gdf = gdf.to_crs(epsg=3005)

    # Clean any list-type columns
    for col in gdf.columns:
        if col != "geometry" and gdf[col].apply(lambda x: isinstance(x, list)).any():
            gdf[col] = gdf[col].apply(str)

    gdf.to_file(gpkg_path, layer=short_name, driver="GPKG", engine="pyogrio")
    print(f"  ✅ '{short_name}' written ({len(gdf)} aboriginal lands polygons)")


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


def fetch_r2_gpkg_layer(short_name, r2_url, source_layer, gpkg_path):
    """Download a specific layer from an R2-hosted GPKG and write it into the local GPKG.

    Downloads the remote GPKG to a temp file, reads the named layer, reprojects
    to EPSG:3005, and writes it as ``short_name`` in the output GPKG.
    """
    import tempfile

    print(f"\n[R2] Fetching layer '{source_layer}' from {r2_url}")
    with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        urllib.request.urlretrieve(r2_url, tmp_path)
        gdf = gpd.read_file(tmp_path, layer=source_layer, engine="pyogrio")
        if gdf.crs and gdf.crs.to_epsg() != 3005:
            gdf = gdf.to_crs(epsg=3005)
        gdf.to_file(gpkg_path, layer=short_name, driver="GPKG", engine="pyogrio")
        print(f"  ✅ '{short_name}' written ({len(gdf)} row(s))")
    finally:
        tmp_path.unlink(missing_ok=True)


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
        # Aboriginal / Indigenous lands from OpenStreetMap (Overpass API)
        "aboriginal_lands": {
            "type": "OVERPASS_ABORIGINAL",
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
        "tidal_boundary": {
            "type": "R2_GPKG",
            "url": "https://bc-fishing-r2.horvath-dawson.workers.dev/DFO_TIDAL_BOUNDARY.gpkg",
            "layer": "tidal_boundary",
        },
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
            elif cfg["type"] == "R2_GPKG":
                fetch_r2_gpkg_layer(name, cfg["url"], cfg["layer"], gpkg_out)
            elif cfg["type"] == "OVERPASS_ABORIGINAL":
                fetch_overpass_aboriginal_lands(name, gpkg_out)
        except Exception as e:
            print(f"❌ Error on {name}: {e}")

    print("\n✅ Data fetch complete!")


if __name__ == "__main__":
    main()
