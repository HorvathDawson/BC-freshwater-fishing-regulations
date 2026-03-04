#!/usr/bin/env python3
"""
Metadata Builder - FWA Metadata Extraction & Zone Assignment

Consumes the optimized graph output and FWA polygon layers to create
a lightweight metadata lookup table.

OPTIMIZATION NOTE:
- Zone boundaries are buffered ONCE at startup.
- Point-in-Polygon checks are performed against these buffered zones.
- This avoids buffering 9+ million stream endpoints individually.
- Both buffered (500m) and unbuffered (exact) zone assignments are stored.
"""

import sys
import logging
import pickle
import argparse
from data.data_extractor import FWADataAccessor
import pandas as pd
from pathlib import Path
from shapely.geometry import Point
from shapely.strtree import STRtree
from enum import Enum
from project_config import get_config
from tqdm import tqdm


# --- Shared Enum ---
class FeatureType(Enum):
    """Enum for FWA feature types (including admin boundary layers)."""

    # FWA hydrological features
    STREAM = "streams"
    LAKE = "lakes"
    WETLAND = "wetlands"
    MANMADE = "manmade"
    UNGAZETTED = "ungazetted"
    POINT = "point"

    # Administrative boundary layers
    PARK_NATIONAL = "parks_nat"
    PARK_BC = "parks_bc"
    WMA = "wma"
    WATERSHED = "watersheds"
    HISTORIC_SITE = "historic_sites"
    OSM_ADMIN = "osm_admin_boundaries"

    UNKNOWN = "unknown"


# Admin feature types (subset used for iteration)
ADMIN_FEATURE_TYPES = frozenset(
    {
        FeatureType.PARK_NATIONAL,
        FeatureType.PARK_BC,
        FeatureType.WMA,
        FeatureType.WATERSHED,
        FeatureType.HISTORIC_SITE,
        FeatureType.OSM_ADMIN,
    }
)


# --- Admin Layer Configuration ---
# Field mappings for each administrative layer in the GPKG.
# Shared by the metadata builder (extraction) and the gazetteer (lookup).
#
# Keys:
#   id_field    – primary key column in the GPKG layer
#   name_field  – human-readable name column
#   code_field  – (optional) column that categorises features within the layer
#   code_map    – (optional) {code_value: human_label} for demultiplexing sub-types
ADMIN_LAYER_CONFIG: dict = {
    "parks_nat": {
        "feature_type": FeatureType.PARK_NATIONAL,
        "id_field": "NATIONAL_PARK_ID",
        "name_field": "ENGLISH_NAME",
    },
    "parks_bc": {
        "feature_type": FeatureType.PARK_BC,
        "id_field": "ADMIN_AREA_SID",
        "name_field": "PROTECTED_LANDS_NAME",
        "code_field": "PROTECTED_LANDS_CODE",
        "code_map": {
            "OI": "ECOLOGICAL_RESERVE",
            "PA": "PROTECTED_AREA",
            "PP": "PROVINCIAL_PARK",
            "RC": "RECREATION_AREA",
        },
    },
    "wma": {
        "feature_type": FeatureType.WMA,
        "id_field": "ADMIN_AREA_SID",
        "name_field": "WILDLIFE_MANAGEMENT_AREA_NAME",
    },
    "watersheds": {
        "feature_type": FeatureType.WATERSHED,
        "id_field": "NAMED_WATERSHED_ID",
        "name_field": "GNIS_NAME",
    },
    "historic_sites": {
        "feature_type": FeatureType.HISTORIC_SITE,
        "id_field": "SITE_ID",
        "name_field": "COMMON_SITE_NAME",
    },
    "osm_admin_boundaries": {
        "feature_type": FeatureType.OSM_ADMIN,
        "id_field": "osm_id",
        "name_field": "name",
        "code_field": "type",
        "code_map": {
            "protected_area": "PROTECTED_AREA",
            "national_park": "NATIONAL_PARK",
            "nature_reserve": "NATURE_RESERVE",
            "forest": "FOREST",
            "military": "MILITARY",
            "aboriginal_lands": "ABORIGINAL_LANDS",
        },
    },
}


# --- Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

# Buffer: Features within 500m of a border get assigned to BOTH zones
ZONE_BOUNDARY_BUFFER_M = 500.0

# --- Helper Functions (Static for Multiprocessing) ---


def find_zones_spatial(geometry, zones_gdf, zone_index, geom_col="geometry_buffered"):
    """
    Finds zones intersecting the geometry.

    Args:
        geometry: Shapely Point or Polygon (Stream endpoint or Lake)
        zones_gdf: GeoDataFrame containing zone geometry columns
        zone_index: STRtree index built on the geometry column specified by *geom_col*
        geom_col: Column to use for intersection checks.
                  ``"geometry_buffered"`` (default) uses pre-buffered zones;
                  ``"geometry"`` uses the original unbuffered boundaries.

    Returns:
        dict with keys:
            zones:        sorted list of REGION_RESPONSIBLE_ID values (e.g. ["7A", "7B"])
            mgmt_units:   sorted list of WILDLIFE_MGMT_UNIT_ID values (e.g. ["7-55"])
            region_names: list of REGION_RESPONSIBLE_NAME values, **positionally paired
                          with zones** (e.g. ["Omineca", "Peace"] when zones=["7A","7B"])
    """
    # 1. Broad Search (Spatial Index)
    candidate_indices = zone_index.query(geometry)

    zone_to_name = {}  # zone_id → region_name (maintains pairing)
    mgmt_units = set()

    # 2. Precise Check
    for idx in candidate_indices:
        zone_row = zones_gdf.iloc[idx]
        check_geom = zone_row[geom_col]

        if check_geom.intersects(geometry):
            zone_id = zone_row["zone"]
            zone_to_name[zone_id] = zone_row.get("REGION_RESPONSIBLE_NAME", "") or ""
            mgmt_units.add(zone_row["WILDLIFE_MGMT_UNIT_ID"])

    sorted_zones = sorted(zone_to_name.keys())
    return {
        "zones": sorted_zones,
        "mgmt_units": sorted(list(mgmt_units)),
        "region_names": [zone_to_name[z] for z in sorted_zones],
    }


def _merge_zone_lookups(*lookups):
    """Merge multiple zone lookup dicts into one combined result."""
    zone_to_name = {}
    mgmt_units = set()
    for lookup in lookups:
        for z, n in zip(lookup["zones"], lookup["region_names"]):
            zone_to_name[z] = n
        mgmt_units.update(lookup["mgmt_units"])
    sorted_zones = sorted(zone_to_name.keys())
    return {
        "zones": sorted_zones,
        "mgmt_units": sorted(mgmt_units),
        "region_names": [zone_to_name[z] for z in sorted_zones],
    }


# --- Main Class ---


class MetadataBuilder:
    def __init__(self, graph_path, gpkg_path, output_path):
        self.paths = {
            "graph": Path(graph_path),
            "gpkg": Path(gpkg_path),
            "output": Path(output_path),
        }
        self.data = {
            "zones_gdf": None,
            "zone_index": None,
            "edge_attrs": None,
            "node_coords": None,
        }
        self.metadata = {
            "zone_metadata": {},
            FeatureType.STREAM: {},
            FeatureType.LAKE: {},
            FeatureType.WETLAND: {},
            FeatureType.MANMADE: {},
        }
        # Admin feature types start empty; populated by process_admin_layers()
        for ftype in ADMIN_FEATURE_TYPES:
            self.metadata[ftype] = {}
        self.data_accessor = FWADataAccessor(self.paths["gpkg"])

    def load_zones(self):
        logger.info("Loading wildlife management units layer using FWADataAccessor...")
        gdf = self.data_accessor.get_layer("wmu")

        # Reproject to BC Albers (EPSG:3005) to match FWA feature layers and graph coordinates
        if gdf.crs and gdf.crs.to_epsg() != 3005:
            logger.info(f"  Reprojecting WMU zones from {gdf.crs} to EPSG:3005...")
            gdf = gdf.to_crs(epsg=3005)

        gdf["zone"] = gdf["REGION_RESPONSIBLE_ID"]

        logger.info(
            f"Pre-buffering zones by {ZONE_BOUNDARY_BUFFER_M}m and clipping to provincial boundary..."
        )
        provincial_boundary = gdf.geometry.union_all()
        gdf["geometry_buffered"] = gdf.geometry.buffer(
            ZONE_BOUNDARY_BUFFER_M
        ).intersection(provincial_boundary)

        self.data["zones_gdf"] = gdf
        self.data["zone_index"] = STRtree(gdf["geometry_buffered"])
        # Unbuffered index for exact zone boundary matching
        self.data["zone_index_unbuffered"] = STRtree(gdf["geometry"])

        logger.info("Building zone definitions...")
        for _, row in gdf.iterrows():
            zid = row["zone"]
            mu = row["WILDLIFE_MGMT_UNIT_ID"]
            region_name = row.get("REGION_RESPONSIBLE_NAME", "")
            if zid not in self.metadata["zone_metadata"]:
                self.metadata["zone_metadata"][zid] = {
                    "zone_number": zid,
                    "region_name": region_name,  # e.g. "Omineca", "Peace"
                    "mgmt_units": [],
                    "mgmt_unit_details": {},
                }
            entry = self.metadata["zone_metadata"][zid]
            # Ensure region_name is set (all MUs in a zone share the same name)
            if not entry.get("region_name") and region_name:
                entry["region_name"] = region_name
            entry["mgmt_units"].append(mu)
            entry["mgmt_unit_details"][mu] = {
                "full_id": mu,
                "region_name": region_name,
                "bounds": list(row.geometry.bounds),
            }
        for z in self.metadata["zone_metadata"].values():
            z["mgmt_units"].sort()

    def load_graph(self):
        logger.info(f"Loading graph pickle from {self.paths['graph']}...")
        with open(self.paths["graph"], "rb") as f:
            dump = pickle.load(f)
            self.data["edge_attrs"] = dump["edge_attrs"]
            self.data["node_coords"] = dump["node_coords"]
        logger.info(f"Loaded {len(self.data['edge_attrs']):,} edges.")

    def extract_streams(self):
        logger.info("Processing streams...")

        edge_list = []
        skipped_edges = 0
        for key, attrs in self.data["edge_attrs"].items():
            try:
                u_id = attrs["source"]
                v_id = attrs["target"]
                u_xy = self.data["node_coords"][u_id]
                v_xy = self.data["node_coords"][v_id]
                edge_list.append((key, u_xy[0], u_xy[1], v_xy[0], v_xy[1], attrs))
            except KeyError as e:
                skipped_edges += 1
                if skipped_edges <= 5:
                    logger.warning(f"Skipping edge {key}: missing key {e}")
        if skipped_edges > 0:
            logger.warning(
                f"Skipped {skipped_edges} edges with missing node coordinates"
            )

        zones_gdf = self.data["zones_gdf"]
        zone_index = self.data["zone_index"]
        zone_index_ub = self.data["zone_index_unbuffered"]

        results = {}
        border_crossings = 0

        for key, u_x, u_y, v_x, v_y, attrs in tqdm(
            edge_list, desc="Streams", unit="edge"
        ):
            u_pt = Point(u_x, u_y)
            v_pt = Point(v_x, v_y)

            # Buffered spatial lookup for both ends
            u_z = find_zones_spatial(u_pt, zones_gdf, zone_index, "geometry_buffered")
            v_z = find_zones_spatial(v_pt, zones_gdf, zone_index, "geometry_buffered")
            merged = _merge_zone_lookups(u_z, v_z)

            is_cross_boundary = len(merged["zones"]) > 1
            if is_cross_boundary:
                border_crossings += 1

            # Unbuffered spatial lookup for both ends
            u_z_ub = find_zones_spatial(u_pt, zones_gdf, zone_index_ub, "geometry")
            v_z_ub = find_zones_spatial(v_pt, zones_gdf, zone_index_ub, "geometry")
            merged_ub = _merge_zone_lookups(u_z_ub, v_z_ub)

            results[str(key)] = {
                "linear_feature_id": str(key),
                "gnis_name": attrs.get("gnis_name", ""),
                "gnis_id": attrs.get("gnis_id", ""),
                "fwa_watershed_code": attrs.get("fwa_watershed_code", ""),
                "fwa_watershed_code_clean": attrs.get("fwa_watershed_code_clean", ""),
                "stream_order": attrs.get("stream_order"),
                "stream_magnitude": attrs.get("stream_magnitude"),
                "length": attrs.get("length", 0),
                "waterbody_key": attrs.get("waterbody_key", ""),
                "feature_code": attrs.get("feature_code", ""),
                "blue_line_key": attrs.get("blue_line_key", ""),
                "edge_type": attrs.get("edge_type", ""),
                "zones": merged["zones"],
                "mgmt_units": merged["mgmt_units"],
                "region_names": merged["region_names"],
                "cross_boundary": is_cross_boundary,
                "unnamed_depth_distance_corrected": attrs.get(
                    "unnamed_depth_distance_corrected"
                ),
                "zones_unbuffered": merged_ub["zones"],
                "mgmt_units_unbuffered": merged_ub["mgmt_units"],
                "region_names_unbuffered": merged_ub["region_names"],
            }

        self.metadata[FeatureType.STREAM].update(results)
        logger.info(
            f"  Processed {len(results):,} streams "
            f"({border_crossings:,} cross-boundary)"
        )

    def process_polygons(self):
        """Iterates over lakes, wetlands, and manmade waterbodies using available layer names from FWADataAccessor."""
        # Map FeatureType to actual layer names in the GeoPackage
        layer_map = {
            FeatureType.LAKE: "lakes",
            FeatureType.WETLAND: "wetlands",
            FeatureType.MANMADE: "manmade_water",
        }
        available_layers = self.data_accessor.list_layers()
        for ftype_enum, layer_name in layer_map.items():
            logger.info(f"Processing {ftype_enum.name} ({layer_name})...")
            if layer_name not in available_layers:
                logger.error(
                    f"Layer '{layer_name}' not found. Available: {available_layers}"
                )
                continue
            try:
                gdf = self.data_accessor.get_layer(layer_name)
                results = {}
                for idx, row in tqdm(
                    gdf.iterrows(),
                    total=len(gdf),
                    desc=f"{ftype_enum.name}",
                    unit="feat",
                ):
                    # WATERBODY_POLY_ID already cleaned by FWADataAccessor (0/None → "")
                    pid = (
                        row.get("WATERBODY_POLY_ID", row.get("WATERBODY_KEY", "")) or ""
                    )
                    if not pid:  # Skip if no valid ID
                        continue
                    z_data = find_zones_spatial(
                        row.geometry,
                        self.data["zones_gdf"],
                        self.data["zone_index"],
                        "geometry_buffered",
                    )
                    z_data_ub = find_zones_spatial(
                        row.geometry,
                        self.data["zones_gdf"],
                        self.data["zone_index_unbuffered"],
                        "geometry",
                    )
                    name = row.get("GNIS_NAME_1", row.get("GNIS_NAME", "")) or ""
                    gid = row.get("GNIS_ID_1", row.get("GNIS_ID", "")) or ""
                    name_2 = row.get("GNIS_NAME_2", "") or ""
                    gid_2 = row.get("GNIS_ID_2", "") or ""
                    blue_line_key = row.get("BLUE_LINE_KEY", "") or ""
                    results[pid] = {
                        "waterbody_poly_id": pid,
                        "waterbody_key": row.get("WATERBODY_KEY", ""),
                        "gnis_name": name,
                        "gnis_id": gid,
                        "gnis_name_2": name_2,
                        "gnis_id_2": gid_2,
                        "fwa_watershed_code": row.get("FWA_WATERSHED_CODE", ""),
                        "blue_line_key": blue_line_key,
                        "area_sqm": row.geometry.area,
                        "zones": z_data["zones"],
                        "mgmt_units": z_data["mgmt_units"],
                        "region_names": z_data["region_names"],
                        "zones_unbuffered": z_data_ub["zones"],
                        "mgmt_units_unbuffered": z_data_ub["mgmt_units"],
                        "region_names_unbuffered": z_data_ub["region_names"],
                    }
                self.metadata[ftype_enum] = results
                logger.info(f"  Extracted {len(results):,} {ftype_enum.name}.")
            except Exception as e:
                logger.error(f"Failed to process {layer_name}: {e}")

    def process_admin_layers(self):
        """
        Extract metadata from administrative boundary layers.

        Reads each admin layer from the GPKG, performs zone assignment
        (same as lakes/wetlands), and stores structured metadata under
        the corresponding FeatureType key.

        Each admin feature record follows the same dict pattern as FWA
        polygons so the gazetteer can build FWAFeature objects uniformly.
        """
        available_layers = self.data_accessor.list_layers()

        for layer_key, cfg in ADMIN_LAYER_CONFIG.items():
            ftype = cfg["feature_type"]
            id_field = cfg["id_field"]
            name_field = cfg["name_field"]
            code_field = cfg.get("code_field")

            logger.info(f"Processing admin layer {ftype.name} ({layer_key})...")

            if layer_key not in available_layers:
                logger.warning(f"  Admin layer '{layer_key}' not in GPKG, skipping")
                continue

            try:
                gdf = self.data_accessor.get_layer(layer_key)
                results = {}

                for _, row in tqdm(
                    gdf.iterrows(),
                    total=len(gdf),
                    desc=f"Admin {layer_key}",
                    unit="feat",
                ):
                    fid = row.get(id_field, "")
                    if pd.isnull(fid) or fid == "" or fid == 0:
                        continue
                    # FWADataAccessor already normalized IDs to strings
                    fid = str(fid)

                    name = row.get(name_field, "") or ""
                    code = str(row.get(code_field, "")) if code_field else None

                    # Zone assignment via spatial lookup (same as polygons)
                    z_data = find_zones_spatial(
                        row.geometry,
                        self.data["zones_gdf"],
                        self.data["zone_index"],
                        "geometry_buffered",
                    )
                    z_data_ub = find_zones_spatial(
                        row.geometry,
                        self.data["zones_gdf"],
                        self.data["zone_index_unbuffered"],
                        "geometry",
                    )

                    results[fid] = {
                        "admin_id": fid,
                        "gnis_name": name,
                        "admin_code": code,
                        "admin_layer": layer_key,
                        "area_sqm": row.geometry.area if row.geometry else 0,
                        "zones": z_data["zones"],
                        "mgmt_units": z_data["mgmt_units"],
                        "region_names": z_data["region_names"],
                        "zones_unbuffered": z_data_ub["zones"],
                        "mgmt_units_unbuffered": z_data_ub["mgmt_units"],
                        "region_names_unbuffered": z_data_ub["region_names"],
                    }

                self.metadata[ftype] = results
                logger.info(f"  Extracted {len(results):,} {ftype.name} features.")

            except Exception as e:
                logger.error(f"Failed to process admin layer '{layer_key}': {e}")

    def save(self):
        self.paths["output"].parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Saving to {self.paths['output']}...")
        with open(self.paths["output"], "wb") as f:
            pickle.dump(self.metadata, f, protocol=5)
        logger.info("Done.")


def main():
    config = get_config()
    # Use FWADataAccessor for default paths
    data_accessor = FWADataAccessor(config.fetch_output_gpkg_path)
    default_graph = getattr(config, "fwa_graph_path", None) or getattr(
        data_accessor, "graph_path", None
    )
    default_gpkg = getattr(data_accessor, "gpkg_path", None) or getattr(
        config, "fetch_output_gpkg_path", None
    )
    default_output = (
        getattr(config, "fwa_metadata_output_path", None)
        or "output/fwa/fwa_metadata.pickle"
    )

    parser = argparse.ArgumentParser(
        description="Extract FWA metadata and assign zones."
    )
    parser.add_argument(
        "--graph-path",
        type=Path,
        default=default_graph,
        help=f"Path to graph pickle (default: {default_graph})",
    )
    parser.add_argument(
        "--gpkg-path",
        type=Path,
        default=default_gpkg,
        help=f"Path to FWA GeoPackage (default: {default_gpkg})",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=default_output,
        help=f"Path to output pickle file (default: {default_output})",
    )
    args = parser.parse_args()

    print("=" * 80)
    print("BC FRESHWATER FISHING REGULATIONS - FWA METADATA BUILDER")
    print("=" * 80)
    print("\n📁 Input:")
    print(f"  Graph pickle: {args.graph_path}")
    print(f"  GeoPackage (zones + polygons): {args.gpkg_path}")
    print("\n📁 Output:")
    print(f"  Metadata pickle: {args.output_path}")
    print("\n⚙️  Configuration:")
    print()

    if not args.graph_path or not args.graph_path.exists():
        logger.error(f"Graph file not found: {args.graph_path}")
        logger.error("Please run graph_builder.py first.")
        sys.exit(1)
    if not args.gpkg_path or not args.gpkg_path.exists():
        logger.error(f"GeoPackage not found: {args.gpkg_path}")
        sys.exit(1)

    builder = MetadataBuilder(args.graph_path, args.gpkg_path, args.output_path)
    builder.load_zones()
    builder.load_graph()
    builder.extract_streams()
    builder.process_polygons()
    builder.process_admin_layers()
    builder.save()


if __name__ == "__main__":
    import multiprocessing

    multiprocessing.freeze_support()
    sys.exit(main())
