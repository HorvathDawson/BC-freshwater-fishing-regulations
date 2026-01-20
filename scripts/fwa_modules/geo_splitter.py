#!/usr/bin/env python3
"""
Geo Feature Splitter - Split Features by Wildlife Management Zones

Splits all waterbody features into zone-based layers using metadata:
- Streams: Creates LineString geometries from graph endpoints
- Lakes/Wetlands/Manmade: Loads from source GDB and assigns to zones
- Zone boundaries: Adds zone outline layers

Outputs a single GeoPackage with layers organized by zone.
"""

import os
import sys
import logging
import pickle
import json
import gc
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict, Optional
import fiona
import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point, shape
import networkx as nx

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


def _write_layer_worker(
    gdf: gpd.GeoDataFrame, output_gpkg: Path, layer_name: str, mode: str
) -> tuple:
    """
    Worker function to write a layer to GeoPackage (for parallel processing).

    Args:
        gdf: GeoDataFrame to write
        output_gpkg: Path to output GeoPackage
        layer_name: Name of layer
        mode: Write mode ('w' or 'a')

    Returns:
        tuple: (layer_name, feature_count, success)
    """
    try:
        if gdf is None or len(gdf) == 0:
            return (layer_name, 0, True)

        gdf.to_file(
            output_gpkg,
            layer=layer_name,
            driver="GPKG",
            mode=mode,
        )
        return (layer_name, len(gdf), True)
    except Exception as e:
        logger.error(f"Failed to write layer {layer_name}: {e}")
        return (layer_name, 0, False)


class GeoSplitter:
    """Splits waterbody features by zone and creates organized GeoPackage."""

    def __init__(
        self,
        metadata_path: Path,
        streams_gdb_path: Path,
        polygons_gdb_path: Path,
        zones_path: Path,
        output_gpkg: Path,
        num_workers: int = 8,
    ):
        """
        Initialize the geo splitter.

        Args:
            metadata_path: Path to stream_metadata.pickle
            streams_gdb_path: Path to FWA_STREAM_NETWORKS_SP.gdb (for streams)
            polygons_gdb_path: Path to FWA_BC.gdb (for lakes/wetlands/manmade polygons)
            zones_path: Path to wildlife management units GeoPackage
            output_gpkg: Path to output GeoPackage file
            num_workers: Number of parallel workers for writing (default: 8)
        """
        self.metadata_path = metadata_path
        self.streams_gdb_path = streams_gdb_path
        self.polygons_gdb_path = polygons_gdb_path
        self.zones_path = zones_path
        self.output_gpkg = output_gpkg
        self.num_workers = num_workers

        self.metadata = None
        self.zones_gdf = None

        # Zone color scheme (consistent colors for each zone)
        self.zone_colors = {
            "1": "#FF6B6B",  # Red
            "2": "#4ECDC4",  # Teal
            "3": "#45B7D1",  # Blue
            "4": "#FFA07A",  # Light Salmon
            "5": "#98D8C8",  # Mint
            "6": "#F7DC6F",  # Yellow
            "7": "#BB8FCE",  # Purple
            "8": "#85C1E2",  # Sky Blue
        }

    def load_metadata(self):
        """Load metadata from pickle file."""
        logger.info(f"Loading metadata from: {self.metadata_path}")

        with open(self.metadata_path, "rb") as f:
            self.metadata = pickle.load(f)

        logger.info(f"  Loaded metadata:")
        logger.info(f"    Streams: {len(self.metadata.get('streams', {})):,}")
        logger.info(f"    Lakes: {len(self.metadata.get('lakes', {})):,}")
        logger.info(f"    Wetlands: {len(self.metadata.get('wetlands', {})):,}")
        logger.info(f"    Manmade: {len(self.metadata.get('manmade', {})):,}")
        logger.info(f"    Zones: {len(self.metadata.get('zone_metadata', {}))}")

    def load_zones(self):
        """Load zone boundaries."""
        logger.info(f"Loading zones from: {self.zones_path}")

        self.zones_gdf = gpd.read_file(self.zones_path)

        # Convert to BC Albers (EPSG:3005) if needed
        if self.zones_gdf.crs != "EPSG:3005":
            logger.info(f"  Converting zones from {self.zones_gdf.crs} to EPSG:3005")
            self.zones_gdf = self.zones_gdf.to_crs("EPSG:3005")

        self.zones_gdf["zone"] = (
            self.zones_gdf["WILDLIFE_MGMT_UNIT_ID"].str.split("-").str[0]
        )

        logger.info(f"  Loaded {len(self.zones_gdf)} management units")

    def create_all_stream_geometries(self) -> Dict[str, gpd.GeoDataFrame]:
        """Create stream geometries for ALL zones in a single pass by streaming from GDB.

        Returns:
            Dictionary mapping zone number to GeoDataFrame with stream geometries
        """
        logger.info("Creating stream geometries for all zones (streaming from GDB)...")

        # Initialize data structures for each zone
        streams_by_zone = defaultdict(list)
        stream_metadata = self.metadata.get("streams", {})

        # Get all stream layer names from GDB
        logger.info(f"  Discovering stream layers in GDB...")
        available_layers = fiona.listlayers(str(self.streams_gdb_path))

        # Filter to stream network layers (4-letter watershed codes, exclude special layers starting with _)
        stream_layers = [
            layer
            for layer in available_layers
            if len(layer) == 4 and layer.isupper() and not layer.startswith("_")
        ]

        logger.info(
            f"  Found {len(stream_layers)} stream layers (4-letter watershed codes)"
        )

        # Stream all layers
        total_processed = 0
        not_in_metadata_count = 0
        total_layers = len(stream_layers)

        for idx, layer_name in enumerate(stream_layers, 1):
            try:
                with fiona.open(str(self.streams_gdb_path), layer=layer_name) as src:
                    layer_count = len(src)
                    crs = src.crs
                    processed = 0

                    for feature in src:
                        processed += 1
                        total_processed += 1

                        if total_processed % 250000 == 0:
                            logger.info(
                                f"  Progress: {idx}/{total_layers} layers, {total_processed:,} streams processed"
                            )

                        # Get LINEAR_FEATURE_ID
                        props = feature["properties"]
                        linear_feature_id = props.get("LINEAR_FEATURE_ID")

                        if linear_feature_id is None:
                            continue

                        linear_feature_id_str = str(int(linear_feature_id))

                        # Check if in metadata
                        metadata = stream_metadata.get(linear_feature_id_str)
                        if not metadata:
                            not_in_metadata_count += 1
                            continue

                        # Get zones for this stream
                        zones = metadata.get("zones", [])
                        if not zones:
                            continue

                        # Get geometry
                        geom = shape(feature["geometry"])

                        # Determine minzoom based on stream_order for tippecanoe
                        stream_order = metadata.get("stream_order")
                        if stream_order is not None:
                            # Very aggressive filtering - only show major rivers when zoomed out
                            if stream_order >= 7:
                                minzoom = (
                                    4  # Absolute largest rivers visible from zoom 4
                                )
                            elif stream_order >= 6:
                                minzoom = 6  # Very large rivers from zoom 6
                            elif stream_order >= 5:
                                minzoom = 7  # Large rivers from zoom 7
                            elif stream_order >= 4:
                                minzoom = 8  # Major rivers from zoom 8
                            elif stream_order == 3:
                                minzoom = (
                                    9  # Medium rivers from zoom 9 (rounded from 8.5)
                                )
                            elif stream_order == 2:
                                minzoom = 10  # Smaller streams from zoom 10
                            else:  # stream_order == 1
                                minzoom = 11  # Smallest streams from zoom 11
                        else:
                            minzoom = 11  # Default for streams without order

                        # Build feature attributes
                        feature_data = {
                            "linear_feature_id": linear_feature_id_str,
                            "gnis_name": metadata.get("gnis_name", ""),
                            "stream_tributary_of": metadata.get(
                                "stream_tributary_of", ""
                            ),
                            "lake_tributary_of": metadata.get("lake_tributary_of", ""),
                            "stream_order": stream_order,
                            "length": metadata.get("length", 0),
                            "waterbody_key": metadata.get("waterbody_key", ""),
                            "lake_name": metadata.get("lake_name", ""),
                            "fwa_watershed_code": metadata.get(
                                "fwa_watershed_code", ""
                            ),
                            "unnamed_depth_distance_raw": metadata.get(
                                "unnamed_depth_distance_raw"
                            ),
                            "unnamed_depth_distance_corrected": metadata.get(
                                "unnamed_depth_distance_corrected"
                            ),
                            "zones": ",".join(zones),
                            "mgmt_units": ",".join(metadata.get("mgmt_units", [])),
                            "cross_boundary": metadata.get("cross_boundary", False),
                            "stroke_color": self.zone_colors.get(
                                zones[0], "#4169E1"
                            ),  # Use first zone color
                            "stroke_width": 1.5,
                            "tippecanoe:minzoom": minzoom,  # Control visibility in tiles
                            "geometry": geom,
                        }

                        # Add to all zones this stream belongs to
                        for zone in zones:
                            streams_by_zone[zone].append(feature_data)

            except Exception as e:
                logger.error(f"  Failed to process layer {layer_name}: {e}")
                continue

        logger.info(f"  Processed {total_processed:,} streams total")
        if not_in_metadata_count > 0:
            logger.info(f"  Skipped {not_in_metadata_count:,} streams not in metadata")

        # Convert to GeoDataFrames
        result = {}
        for zone, features in streams_by_zone.items():
            if features:
                gdf = gpd.GeoDataFrame(features, crs="EPSG:3005")  # BC Albers
                result[zone] = gdf
                logger.info(f"  Zone {zone}: {len(gdf):,} stream features")
            else:
                result[zone] = None

        return result

    def create_all_polygon_geometries(
        self, feature_type: str, layer_name: str
    ) -> Dict[str, gpd.GeoDataFrame]:
        """Create polygon geometries for ALL zones in a single pass by streaming from GDB.

        Args:
            feature_type: Type of feature ('lakes', 'wetlands', 'manmade')
            layer_name: Layer name in GDB

        Returns:
            Dictionary mapping zone number to GeoDataFrame with polygon geometries
        """
        logger.info(
            f"Creating {feature_type} geometries for all zones (streaming from GDB)..."
        )

        # Initialize data structures for each zone
        polygons_by_zone = defaultdict(list)
        feature_metadata = self.metadata.get(feature_type, {})

        # Build lookup set of waterbody keys by zone
        zone_lookup = defaultdict(set)
        for waterbody_key, metadata in feature_metadata.items():
            for zone in metadata.get("zones", []):
                zone_lookup[zone].add(waterbody_key)

        logger.info(
            f"  Prepared zone lookup for {len(feature_metadata):,} {feature_type}"
        )

        # Stream layer from GDB
        logger.info(f"  Streaming {layer_name} from GDB...")
        try:
            total_processed = 0
            not_in_metadata_count = 0

            with fiona.open(str(self.polygons_gdb_path), layer=layer_name) as src:
                total = len(src)
                crs = src.crs

                for feature in src:
                    total_processed += 1
                    if total_processed % 50000 == 0:
                        logger.info(
                            f"    Streamed {total_processed:,}/{total:,} features ({total_processed/total*100:.1f}%)"
                        )

                    # Check if feature has waterbody key
                    props = feature["properties"]
                    waterbody_key = props.get("WATERBODY_KEY")

                    if waterbody_key is None:
                        continue

                    waterbody_key_str = str(int(waterbody_key))

                    # Get metadata
                    meta = feature_metadata.get(waterbody_key_str)
                    if not meta:
                        not_in_metadata_count += 1
                        continue

                    # Get zones for this feature
                    zones = meta.get("zones", [])
                    if not zones:
                        continue

                    # Build simplified properties
                    gnis_name = props.get("GNIS_NAME_1") or props.get("GNIS_NAME", "")

                    # Create geometry
                    geom = shape(feature["geometry"])

                    # Calculate area in square meters for size-based minzoom
                    area_sqm = geom.area  # BC Albers is in meters

                    # Assign minzoom based on size - larger features visible at lower zooms
                    if area_sqm >= 10_000_000:  # >= 10 km²
                        minzoom = 4  # Very large waterbodies
                    elif area_sqm >= 1_000_000:  # >= 1 km²
                        minzoom = 6  # Large waterbodies
                    elif area_sqm >= 100_000:  # >= 0.1 km²
                        minzoom = 8  # Medium waterbodies
                    elif area_sqm >= 10_000:  # >= 0.01 km²
                        minzoom = 10  # Small waterbodies
                    else:
                        minzoom = 12  # Very small waterbodies

                    feature_data = {
                        "WATERBODY_KEY": waterbody_key_str,
                        "gnis_name": gnis_name,
                        "zones": ",".join(zones),
                        "mgmt_units": ",".join(meta.get("mgmt_units", [])),
                        "fill_color": self.zone_colors.get(
                            zones[0], "#4169E1"
                        ),  # Use first zone color
                        "fill_opacity": 0.4,
                        "stroke_color": self.zone_colors.get(zones[0], "#1E3A8A"),
                        "stroke_width": 1.0,
                        "area_sqm": area_sqm,  # Store for reference
                        "tippecanoe:minzoom": minzoom,  # Size-based visibility
                        "geometry": geom,
                    }

                    # Add to all zones this feature belongs to
                    for zone in zones:
                        polygons_by_zone[zone].append(feature_data)

            logger.info(f"  Processed {total_processed:,} {feature_type} total")
            if not_in_metadata_count > 0:
                logger.info(
                    f"  Skipped {not_in_metadata_count:,} {feature_type} not in metadata"
                )

            # Convert to GeoDataFrames
            result = {}
            for zone, features in polygons_by_zone.items():
                if features:
                    gdf = gpd.GeoDataFrame(features, crs=crs)
                    # Convert to BC Albers (EPSG:3005) if needed
                    if gdf.crs != "EPSG:3005":
                        gdf = gdf.to_crs("EPSG:3005")
                    result[zone] = gdf
                    logger.info(f"  Zone {zone}: {len(gdf):,} {feature_type} features")
                else:
                    result[zone] = None

            return result

        except Exception as e:
            logger.error(f"  Failed to stream {layer_name}: {e}")
            return {}

    def create_zone_boundary_layer(self, zone: str) -> gpd.GeoDataFrame:
        """Create zone boundary layer with styling attributes.

        Args:
            zone: Zone number (e.g., '1')

        Returns:
            GeoDataFrame with zone boundaries and color attributes
        """
        logger.info(f"Creating zone boundary layer for zone {zone}...")

        # Filter to this zone
        zone_gdf = self.zones_gdf[self.zones_gdf["zone"] == zone].copy()

        # Add color and opacity attributes for styling
        zone_gdf["fill_color"] = self.zone_colors.get(zone, "#CCCCCC")
        zone_gdf["fill_opacity"] = 0.25  # 25% opacity for boundaries
        zone_gdf["stroke_color"] = self.zone_colors.get(zone, "#888888")
        zone_gdf["stroke_width"] = 2.0
        zone_gdf["stroke_opacity"] = 0.8
        zone_gdf["tippecanoe:minzoom"] = 0  # Zone boundaries visible from zoom 0

        logger.info(f"  Zone {zone} has {len(zone_gdf)} management units")

        return zone_gdf

    def run(self):
        """Execute the full splitting pipeline with parallel writing."""
        logger.info("=== Starting Geo Feature Splitting ===")
        logger.info(f"Using {self.num_workers} parallel workers for writing")

        # Step 1: Load all required data
        self.load_metadata()
        self.load_zones()

        # Get all zones
        zones = sorted(self.metadata.get("zone_metadata", {}).keys())
        logger.info(f"\nProcessing {len(zones)} zones: {', '.join(zones)}")

        # Remove existing output file if it exists
        if self.output_gpkg.exists():
            logger.info(f"Removing existing output file: {self.output_gpkg}")
            self.output_gpkg.unlink()

        # Ensure output directory exists
        self.output_gpkg.parent.mkdir(parents=True, exist_ok=True)

        # Step 2: Process and prepare all layers
        logger.info("\n=== Preparing Layers ===")
        layers_to_write = []
        first_layer = True

        # Create all stream geometries in a single pass
        stream_gdfs = self.create_all_stream_geometries()

        # Create all polygon geometries in single passes
        logger.info("\n=== Processing Polygon Features ===")
        lakes_gdfs = self.create_all_polygon_geometries("lakes", "FWA_LAKES_POLY")
        wetlands_gdfs = self.create_all_polygon_geometries(
            "wetlands", "FWA_WETLANDS_POLY"
        )
        manmade_gdfs = self.create_all_polygon_geometries(
            "manmade", "FWA_MANMADE_WATERBODIES_POLY"
        )

        for zone in zones:
            logger.info(f"\n=== Preparing Zone {zone} Layers ===")

            # Create zone boundary layer
            zone_boundary = self.create_zone_boundary_layer(zone)
            if zone_boundary is not None and len(zone_boundary) > 0:
                layers_to_write.append(
                    (
                        zone_boundary,
                        f"zone_{zone}_boundaries",
                        "w" if first_layer else "a",
                    )
                )
                first_layer = False

            # Get streams layer for this zone (already created)
            streams_gdf = stream_gdfs.get(zone)
            if streams_gdf is not None and len(streams_gdf) > 0:
                layers_to_write.append((streams_gdf, f"zone_{zone}_streams", "a"))

            # Get lakes layer for this zone (already created)
            lakes_gdf = lakes_gdfs.get(zone)
            if lakes_gdf is not None and len(lakes_gdf) > 0:
                layers_to_write.append((lakes_gdf, f"zone_{zone}_lakes", "a"))

            # Get wetlands layer for this zone (already created)
            wetlands_gdf = wetlands_gdfs.get(zone)
            if wetlands_gdf is not None and len(wetlands_gdf) > 0:
                layers_to_write.append((wetlands_gdf, f"zone_{zone}_wetlands", "a"))

            # Get manmade layer for this zone (already created)
            manmade_gdf = manmade_gdfs.get(zone)
            if manmade_gdf is not None and len(manmade_gdf) > 0:
                layers_to_write.append((manmade_gdf, f"zone_{zone}_manmade", "a"))

        # Clear all data
        del stream_gdfs, lakes_gdfs, wetlands_gdfs, manmade_gdfs
        # Step 3: Write all layers in parallel
        logger.info(f"\n=== Writing {len(layers_to_write)} layers to GeoPackage ===")

        # Note: GeoPackage doesn't support true concurrent writes, so we write sequentially
        # but use workers to prepare the data
        total_features = 0
        for idx, (gdf, layer_name, mode) in enumerate(layers_to_write):
            logger.info(
                f"  Writing layer {idx+1}/{len(layers_to_write)}: {layer_name} ({len(gdf):,} features)"
            )

            gdf.to_file(
                self.output_gpkg,
                layer=layer_name,
                driver="GPKG",
                mode=mode,
            )
            total_features += len(gdf)

            # Clear memory
            del gdf
            gc.collect()

        logger.info("\n=== Geo Feature Splitting Complete ===")
        logger.info(f"Output saved to: {self.output_gpkg}")
        logger.info(f"Total features written: {total_features:,}")

        # Log file size
        file_size_mb = self.output_gpkg.stat().st_size / (1024 * 1024)
        logger.info(f"Output file size: {file_size_mb:.1f} MB")

        # Log styling info
        logger.info("\n=== Layer Styling Information ===")
        logger.info("Layers are organized by zone (zone_1_*, zone_2_*, etc.)")
        logger.info(
            "Layer order: boundaries (bottom), streams, lakes, wetlands, manmade (top)"
        )
        logger.info("Style attributes included:")
        logger.info("  - Zone boundaries: 25% fill opacity, colored borders")
        logger.info("  - Streams: Zone-colored strokes, 1.5px width")
        logger.info(
            "  - Polygons (lakes/wetlands/manmade): 40% fill opacity, zone-colored"
        )
        logger.info("\nZone color scheme:")
        for zone, color in sorted(self.zone_colors.items()):
            logger.info(f"  Zone {zone}: {color}")

        return self.output_gpkg


def main():
    """Main entry point for script execution."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Split waterbody features by wildlife management zones"
    )
    parser.add_argument(
        "--metadata-path",
        type=Path,
        help="Path to stream_metadata.pickle",
    )
    parser.add_argument(
        "--streams-gdb-path",
        type=Path,
        help="Path to FWA_STREAM_NETWORKS_SP.gdb",
    )
    parser.add_argument(
        "--polygons-gdb-path",
        type=Path,
        help="Path to FWA_BC.gdb",
    )
    parser.add_argument(
        "--zones-path",
        type=Path,
        help="Path to wildlife management units GeoPackage",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        help="Path to output GeoPackage file",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of parallel workers (default: 8)",
    )

    args = parser.parse_args()

    # Setup default paths
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent

    metadata_path = args.metadata_path or (
        script_dir.parent / "output" / "fwa_modules" / "stream_metadata.pickle"
    )
    streams_gdb_path = args.streams_gdb_path or (
        project_root
        / "data"
        / "ftp.geobc.gov.bc.ca"
        / "sections"
        / "outgoing"
        / "bmgs"
        / "FWA_Public"
        / "FWA_STREAM_NETWORKS_SP"
        / "FWA_STREAM_NETWORKS_SP.gdb"
    )
    polygons_gdb_path = args.polygons_gdb_path or (
        project_root
        / "data"
        / "ftp.geobc.gov.bc.ca"
        / "sections"
        / "outgoing"
        / "bmgs"
        / "FWA_Public"
        / "FWA_BC"
        / "FWA_BC.gdb"
    )
    zones_path = args.zones_path or (
        project_root
        / "data"
        / "ftp.geobc.gov.bc.ca"
        / "sections"
        / "outgoing"
        / "bmgs"
        / "FWA_Public"
        / "WAA_WILDLIFE_MGMT_UNITS_SVW.gpkg"
    )
    output_path = args.output_path or (
        script_dir.parent / "output" / "fwa_modules" / "waterbodies_by_zone.gpkg"
    )

    # Validate input files
    if not metadata_path.exists():
        logger.error(f"Metadata file not found: {metadata_path}")
        logger.info("Please run metadata_builder.py first.")
        return 1

    if not streams_gdb_path.exists():
        logger.error(f"Streams GDB not found: {streams_gdb_path}")
        return 1

    if not polygons_gdb_path.exists():
        logger.error(f"Polygons GDB not found: {polygons_gdb_path}")
        return 1

    if not zones_path.exists():
        logger.error(f"Zones file not found: {zones_path}")
        return 1

    # Run splitting
    splitter = GeoSplitter(
        metadata_path,
        streams_gdb_path,
        polygons_gdb_path,
        zones_path,
        output_path,
        args.workers,
    )
    splitter.run()

    return 0


if __name__ == "__main__":
    sys.exit(main())
