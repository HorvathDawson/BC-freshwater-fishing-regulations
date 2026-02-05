#!/usr/bin/env python3
"""
Metadata Builder - Stream and Waterbody Metadata Extraction with Zone Assignment

Extracts metadata from the graph and polygon layers, enriching with zone assignments:
- Streams: Extracts all graph edge attributes + adds zone assignments
- Lakes/Wetlands/Manmade: Extracts polygon attributes + adds zone assignments

Creates a lightweight metadata lookup table (stream_metadata.pickle) containing:
- All graph-enriched attributes (tributary relationships, names, etc.)
- Zone assignments (zones, management units, cross-boundary status)
- Polygon feature metadata

This enables fast feature splitting without loading the full graph.
"""

import os
import sys
import logging
import pickle
import argparse
import gc
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from shapely.strtree import STRtree
import networkx as nx

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


def _find_zones_for_point_static(
    point: Point, zones_gdf: gpd.GeoDataFrame, zone_index: STRtree
) -> dict:
    """
    Static function to find zones for a point (for use in multiprocessing).

    Args:
        point: Shapely Point geometry
        zones_gdf: GeoDataFrame with zone geometries
        zone_index: STRtree spatial index

    Returns:
        dict with 'zones' and 'mgmt_units' lists
    """
    # Query spatial index
    possible_matches_idx = zone_index.query(point)

    zones = set()
    mgmt_units = set()

    # Check actual containment
    for idx in possible_matches_idx:
        zone_geom = zones_gdf.iloc[idx].geometry
        if zone_geom.contains(point):
            zone_num = zones_gdf.iloc[idx]["zone"]
            mgmt_unit_id = zones_gdf.iloc[idx]["WILDLIFE_MGMT_UNIT_ID"]
            zones.add(zone_num)
            mgmt_units.add(mgmt_unit_id)

    return {"zones": sorted(list(zones)), "mgmt_units": sorted(list(mgmt_units))}


def _process_stream_chunk(chunk, zones_gdf, zone_index):
    """
    Process a chunk of stream edges in parallel.

    Args:
        chunk: List of (key, u_x, u_y, v_x, v_y, data) tuples
        zones_gdf: GeoDataFrame with zone polygons
        zone_index: STRtree spatial index

    Returns:
        tuple: (chunk_metadata dict, cross_boundary_count int)
    """
    chunk_metadata = {}
    cross_boundary_count = 0

    for key, u_x, u_y, v_x, v_y, data in chunk:
        # Create points
        u_point = Point(u_x, u_y)
        v_point = Point(v_x, v_y)

        # Find zones for each endpoint
        u_zones = _find_zones_for_point_static(u_point, zones_gdf, zone_index)
        v_zones = _find_zones_for_point_static(v_point, zones_gdf, zone_index)

        # Combine zones from both endpoints
        all_zones = sorted(set(u_zones["zones"] + v_zones["zones"]))
        all_mgmt_units = sorted(set(u_zones["mgmt_units"] + v_zones["mgmt_units"]))

        # Track cross-boundary streams
        cross_boundary = len(all_zones) > 1
        if cross_boundary:
            cross_boundary_count += 1

        # Extract all graph attributes
        linear_feature_id = str(key)

        # Build complete metadata entry
        chunk_metadata[linear_feature_id] = {
            # Graph attributes
            "linear_feature_id": linear_feature_id,
            "gnis_name": data.get("gnis_name", ""),
            "fwa_watershed_code": data.get("fwa_watershed_code", ""),
            "fwa_watershed_code_clean": data.get("fwa_watershed_code_clean", ""),
            "stream_order": data.get("stream_order"),
            "length": data.get("length", 0),
            "waterbody_key": data.get("waterbody_key", ""),
            "lake_name": data.get("lake_name", ""),
            "stream_tributary_of": data.get("stream_tributary_of", ""),
            "lake_tributary_of": data.get("lake_tributary_of", ""),
            "unnamed_depth_distance_raw": data.get("unnamed_depth_distance_raw"),
            "unnamed_depth_distance_corrected": data.get(
                "unnamed_depth_distance_corrected"
            ),
            # Zone assignments
            "zones": all_zones,
            "mgmt_units": all_mgmt_units,
            "cross_boundary": cross_boundary,
        }

    return chunk_metadata, cross_boundary_count


class MetadataBuilder:
    """Builds comprehensive metadata lookup table from graph and polygon layers."""

    def __init__(
        self,
        graph_path: Path,
        zones_path: Path,
        lakes_gdb_path: Path,
        output_path: Path,
    ):
        """
        Initialize the metadata builder.

        Args:
            graph_path: Path to graph pickle file (with tributary enrichment)
            zones_path: Path to wildlife management units GeoPackage
            lakes_gdb_path: Path to FWA_BC.gdb with polygon layers
            output_path: Path to output stream_metadata.pickle file
        """
        self.graph_path = graph_path
        self.zones_path = zones_path
        self.lakes_gdb_path = lakes_gdb_path
        self.output_path = output_path

        self.G = None
        self.node_coords = None  # O(1) coordinate lookups
        self.edge_attrs = None  # O(1) edge attribute lookups
        self.zones_gdf = None
        self.zone_index = None
        self.zone_metadata = {}

        # Results
        self.stream_metadata = {}
        self.lake_metadata = {}
        self.wetland_metadata = {}
        self.manmade_metadata = {}

    def load_graph(self):
        """Load the stream network graph and lookup dictionaries (igraph format)."""
        logger.info(f"Loading graph from: {self.graph_path}")

        with open(self.graph_path, "rb") as f:
            data = pickle.load(f)

        self.G = data["graph"]  # igraph format
        self.node_coords = data["node_coords"]  # O(1) coordinate lookups
        self.edge_attrs = data["edge_attrs"]  # O(1) edge attribute lookups

        logger.info(
            f"  Loaded graph: {self.G.vcount():,} nodes, {self.G.ecount():,} edges"
        )
        logger.info(
            f"  Loaded lookups: {len(self.node_coords):,} node coords, {len(self.edge_attrs):,} edge attrs"
        )

    def load_zones(self):
        """Load wildlife management units and build spatial index."""
        logger.info(f"Loading zones from: {self.zones_path}")

        self.zones_gdf = gpd.read_file(self.zones_path)
        logger.info(f"  Loaded {len(self.zones_gdf)} management units")

        # Extract zone number from WILDLIFE_MGMT_UNIT_ID (e.g., "1-15" -> "1")
        self.zones_gdf["zone"] = (
            self.zones_gdf["WILDLIFE_MGMT_UNIT_ID"].str.split("-").str[0]
        )

        # Build spatial index for fast lookups
        logger.info("  Building spatial index...")
        self.zone_index = STRtree(self.zones_gdf.geometry)

        # Build zone metadata
        logger.info("  Building zone metadata...")
        self._build_zone_metadata()

    def _build_zone_metadata(self):
        """Build comprehensive zone metadata from zones GeoDataFrame."""
        zones_by_number = defaultdict(list)

        # Group management units by zone number
        for idx, row in self.zones_gdf.iterrows():
            zone_num = row["zone"]
            mgmt_unit_id = row["WILDLIFE_MGMT_UNIT_ID"]
            zones_by_number[zone_num].append(mgmt_unit_id)

        # Build metadata structure
        for zone_num, mgmt_units in sorted(zones_by_number.items()):
            # Get details for each management unit
            mgmt_unit_details = {}
            for mgmt_unit_id in sorted(mgmt_units):
                unit_row = self.zones_gdf[
                    self.zones_gdf["WILDLIFE_MGMT_UNIT_ID"] == mgmt_unit_id
                ].iloc[0]

                # Get bounding box
                bounds = unit_row.geometry.bounds  # (minx, miny, maxx, maxy)

                mgmt_unit_details[mgmt_unit_id] = {
                    "full_id": mgmt_unit_id,
                    "region_name": unit_row.get("REGION_RESPONSIBLE_NAME", ""),
                    "game_zone_id": unit_row.get("GAME_MANAGEMENT_ZONE_ID", ""),
                    "game_zone_name": unit_row.get("GAME_MANAGEMENT_ZONE_NAME", ""),
                    "bounds": list(bounds),
                }

            self.zone_metadata[zone_num] = {
                "zone_number": zone_num,
                "mgmt_units": sorted(mgmt_units),
                "mgmt_unit_details": mgmt_unit_details,
                "total_mgmt_units": len(mgmt_units),
            }

        logger.info(f"  Built metadata for {len(self.zone_metadata)} zones")

    def find_zones_for_point(self, point: Point) -> dict:
        """
        Find all zones and management units that contain a point.

        Args:
            point: Shapely Point geometry

        Returns:
            dict with 'zones' and 'mgmt_units' lists
        """
        # Query spatial index
        possible_matches_idx = self.zone_index.query(point)

        zones = set()
        mgmt_units = set()

        # Check actual containment
        for idx in possible_matches_idx:
            zone_geom = self.zones_gdf.iloc[idx].geometry
            if zone_geom.contains(point):
                zone_num = self.zones_gdf.iloc[idx]["zone"]
                mgmt_unit_id = self.zones_gdf.iloc[idx]["WILDLIFE_MGMT_UNIT_ID"]
                zones.add(zone_num)
                mgmt_units.add(mgmt_unit_id)

        return {"zones": sorted(list(zones)), "mgmt_units": sorted(list(mgmt_units))}

    def find_zones_for_polygon(self, polygon) -> dict:
        """
        Find all zones and management units that intersect with a polygon.

        This is more accurate than centroid-based assignment for large features
        that may span multiple management units.

        Args:
            polygon: Shapely Polygon or MultiPolygon geometry

        Returns:
            dict with 'zones' and 'mgmt_units' lists
        """
        # Query spatial index using polygon bounds
        possible_matches_idx = self.zone_index.query(polygon)

        zones = set()
        mgmt_units = set()

        # Check actual intersection
        for idx in possible_matches_idx:
            zone_geom = self.zones_gdf.iloc[idx].geometry
            if zone_geom.intersects(polygon):
                zone_num = self.zones_gdf.iloc[idx]["zone"]
                mgmt_unit_id = self.zones_gdf.iloc[idx]["WILDLIFE_MGMT_UNIT_ID"]
                zones.add(zone_num)
                mgmt_units.add(mgmt_unit_id)

        return {"zones": sorted(list(zones)), "mgmt_units": sorted(list(mgmt_units))}

    def extract_stream_metadata(self, num_workers: int = 8):
        """Extract all edge attributes from graph and add zone assignments using parallel processing.

        Args:
            num_workers: Number of parallel workers (default: 8)
        """
        logger.info(
            f"Extracting stream metadata from graph (using {num_workers} workers)..."
        )

        if self.G is None:
            logger.error("Graph not loaded. Call load_graph() first.")
            return

        # Build edge data list using constant-time lookups
        total_edges = len(self.edge_attrs)
        logger.info(
            f"  Processing {total_edges:,} stream edges (using O(1) lookups)..."
        )

        edges_data = []
        for key, attrs in self.edge_attrs.items():
            u_x, u_y = attrs["source_coords"]
            v_x, v_y = attrs["target_coords"]
            edges_data.append((key, u_x, u_y, v_x, v_y, attrs))

        # Split into chunks for parallel processing
        chunk_size = max(1000, total_edges // (num_workers * 4))
        chunks = [
            edges_data[i : i + chunk_size]
            for i in range(0, len(edges_data), chunk_size)
        ]

        logger.info(f"  Split into {len(chunks)} chunks of ~{chunk_size:,} edges each")

        # Process chunks in parallel
        self.stream_metadata = {}
        processed = 0
        cross_boundary_count = 0

        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            # Submit all chunks
            future_to_chunk = {
                executor.submit(
                    _process_stream_chunk, chunk, self.zones_gdf, self.zone_index
                ): i
                for i, chunk in enumerate(chunks)
            }

            # Collect results as they complete
            for future in as_completed(future_to_chunk):
                chunk_idx = future_to_chunk[future]
                try:
                    chunk_metadata, chunk_cross_boundary = future.result()

                    # Merge results
                    self.stream_metadata.update(chunk_metadata)
                    cross_boundary_count += chunk_cross_boundary
                    processed += len(chunk_metadata)

                    logger.info(
                        f"    Processed {processed:,}/{total_edges:,} edges ({processed/total_edges*100:.1f}%) - chunk {chunk_idx + 1}/{len(chunks)}"
                    )
                    gc.collect()

                except Exception as e:
                    logger.error(f"    Chunk {chunk_idx} failed: {e}")

        logger.info(f"  Extracted metadata for {len(self.stream_metadata):,} streams")
        logger.info(
            f"  Cross-boundary streams: {cross_boundary_count:,} ({cross_boundary_count/total_edges*100:.2f}%)"
        )

        # Count streams per zone
        zone_counts = defaultdict(int)
        for metadata in self.stream_metadata.values():
            for zone in metadata["zones"]:
                zone_counts[zone] += 1

        logger.info("  Streams per zone:")
        for zone in sorted(zone_counts.keys()):
            logger.info(f"    Zone {zone}: {zone_counts[zone]:,} streams")

    def process_polygon_layer(self, layer_name: str, feature_type: str) -> dict:
        """
        Process a polygon layer and assign zones based on centroids.

        Args:
            layer_name: Name of the layer in the GDB
            feature_type: Type of feature ('lakes', 'wetlands', 'manmade')

        Returns:
            Dictionary mapping waterbody_key to metadata
        """
        logger.info(f"Processing {feature_type} from layer: {layer_name}")

        try:
            # Load layer
            gdf = gpd.read_file(str(self.lakes_gdb_path), layer=layer_name)
            logger.info(f"  Loaded {len(gdf):,} features")

            # Filter to features with valid WATERBODY_KEY
            if "WATERBODY_KEY" not in gdf.columns:
                logger.warning(f"  No WATERBODY_KEY column found in {layer_name}")
                return {}

            initial_count = len(gdf)
            gdf = gdf[gdf["WATERBODY_KEY"].notna()].copy()
            logger.info(
                f"  Features with valid WATERBODY_KEY: {len(gdf):,} (filtered from {initial_count:,})"
            )

            # Process features
            feature_metadata = {}
            processed = 0
            total = len(gdf)

            for idx, row in gdf.iterrows():
                processed += 1

                if processed % 10000 == 0:
                    logger.info(
                        f"    Processed {processed:,}/{total:,} {feature_type} ({processed/total*100:.1f}%)"
                    )
                    gc.collect()

                # Get waterbody key
                waterbody_key = str(int(row["WATERBODY_KEY"]))

                # Find zones using polygon intersection (not just centroid)
                # This ensures large lakes spanning multiple MUs get all their MUs
                zone_result = self.find_zones_for_polygon(row.geometry)

                # Extract name (different columns for different layers)
                name = ""
                if "GNIS_NAME_1" in row.index:
                    name = row.get("GNIS_NAME_1", "")
                elif "GNIS_NAME" in row.index:
                    name = row.get("GNIS_NAME", "")

                # Extract GNIS ID (for deduplication of same lake with different watershed codes)
                gnis_id = ""
                if "GNIS_ID_1" in row.index:
                    gid = row.get("GNIS_ID_1", "")
                    gnis_id = str(int(gid)) if pd.notna(gid) and gid else ""

                # Extract alternate name and ID (GNIS_NAME_2 / GNIS_ID_2)
                name_2 = ""
                if "GNIS_NAME_2" in row.index:
                    name_2 = row.get("GNIS_NAME_2", "")

                gnis_id_2 = ""
                if "GNIS_ID_2" in row.index:
                    gid_2 = row.get("GNIS_ID_2", "")
                    gnis_id_2 = str(int(gid_2)) if pd.notna(gid_2) and gid_2 else ""

                # Extract watershed code
                watershed_code = ""
                if "FWA_WATERSHED_CODE" in row.index:
                    wsc = row.get("FWA_WATERSHED_CODE", "")
                    watershed_code = wsc if pd.notna(wsc) else ""

                # Store metadata
                feature_metadata[waterbody_key] = {
                    "waterbody_key": waterbody_key,
                    "gnis_name": name if pd.notna(name) else "",
                    "gnis_id": gnis_id,
                    "gnis_name_2": name_2 if pd.notna(name_2) else "",
                    "gnis_id_2": gnis_id_2,
                    "fwa_watershed_code": watershed_code,
                    "feature_type": feature_type,
                    "zones": zone_result["zones"],
                    "mgmt_units": zone_result["mgmt_units"],
                }

            logger.info(f"  Processed {processed:,} {feature_type}")

            # Count features per zone
            zone_counts = defaultdict(int)
            for metadata in feature_metadata.values():
                for zone in metadata["zones"]:
                    zone_counts[zone] += 1

            logger.info(f"  {feature_type.capitalize()} per zone:")
            for zone in sorted(zone_counts.keys()):
                logger.info(f"    Zone {zone}: {zone_counts[zone]:,} {feature_type}")

            return feature_metadata

        except Exception as e:
            logger.error(f"  Failed to process {layer_name}: {e}")
            return {}

    def process_all_polygons(self):
        """Process all polygon layers (lakes, wetlands, manmade)."""
        logger.info("Processing polygon features...")

        # Define layers to process
        polygon_layers = {
            "lakes": "FWA_LAKES_POLY",
            "wetlands": "FWA_WETLANDS_POLY",
            "manmade": "FWA_MANMADE_WATERBODIES_POLY",
        }

        # Process each layer
        self.lake_metadata = self.process_polygon_layer(
            polygon_layers["lakes"], "lakes"
        )
        gc.collect()

        self.wetland_metadata = self.process_polygon_layer(
            polygon_layers["wetlands"], "wetlands"
        )
        gc.collect()

        self.manmade_metadata = self.process_polygon_layer(
            polygon_layers["manmade"], "manmade"
        )
        gc.collect()

    def build_metadata_table(self) -> dict:
        """Build the complete metadata lookup table."""
        logger.info("Building metadata lookup table...")

        metadata_table = {
            "zone_metadata": self.zone_metadata,
            "streams": self.stream_metadata,
            "lakes": self.lake_metadata,
            "wetlands": self.wetland_metadata,
            "manmade": self.manmade_metadata,
        }

        # Log summary statistics
        logger.info("Metadata table summary:")
        logger.info(f"  Zones: {len(self.zone_metadata)}")
        logger.info(
            f"  Total management units: {sum(z['total_mgmt_units'] for z in self.zone_metadata.values())}"
        )
        logger.info(f"  Streams: {len(self.stream_metadata):,}")
        logger.info(f"  Lakes: {len(self.lake_metadata):,}")
        logger.info(f"  Wetlands: {len(self.wetland_metadata):,}")
        logger.info(f"  Manmade: {len(self.manmade_metadata):,}")
        logger.info(
            f"  Total features: {len(self.stream_metadata) + len(self.lake_metadata) + len(self.wetland_metadata) + len(self.manmade_metadata):,}"
        )

        return metadata_table

    def save_metadata_table(self, metadata_table: dict):
        """Save metadata table to pickle file."""
        logger.info(f"Saving metadata table to: {self.output_path}")

        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.output_path, "wb") as f:
            pickle.dump(metadata_table, f, protocol=5)

        # Log file size
        file_size_mb = self.output_path.stat().st_size / (1024 * 1024)
        logger.info(f"  Saved metadata table: {file_size_mb:.1f} MB")

    def run(self):
        """Execute the full metadata extraction pipeline."""
        logger.info("=== Starting Metadata Extraction ===")

        # Step 1: Load zones
        self.load_zones()

        # Step 2: Load graph
        self.load_graph()

        # Step 3: Extract stream metadata
        self.extract_stream_metadata()

        # Step 4: Process polygon features
        self.process_all_polygons()

        # Step 5: Build metadata table
        metadata_table = self.build_metadata_table()

        # Step 6: Save metadata table
        self.save_metadata_table(metadata_table)

        logger.info("=== Metadata Extraction Complete ===")
        return True


def main():
    """Main entry point for script execution."""
    parser = argparse.ArgumentParser(
        description="Extract metadata from graph and add zone assignments"
    )
    parser.add_argument("--graph-path", type=Path, help="Path to graph pickle file")
    parser.add_argument(
        "--zones-path", type=Path, help="Path to wildlife management units GeoPackage"
    )
    parser.add_argument("--lakes-gdb-path", type=Path, help="Path to FWA_BC.gdb")
    parser.add_argument(
        "--output-path", type=Path, help="Path to output stream_metadata.pickle file"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of parallel workers for stream processing (default: 8)",
    )

    args = parser.parse_args()

    # Setup default paths
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent

    graph_path = args.graph_path or (
        script_dir.parent / "output" / "fwa_modules" / "fwa_bc_primal_full.gpickle"
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
    lakes_gdb_path = args.lakes_gdb_path or (
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
    output_path = args.output_path or (
        script_dir.parent / "output" / "fwa_modules" / "stream_metadata.pickle"
    )

    # Validate input files
    if not graph_path.exists():
        logger.error(f"Graph file not found: {graph_path}")
        logger.info("Please run graph_builder.py first to create the graph.")
        return 1

    if not zones_path.exists():
        logger.error(f"Zones file not found: {zones_path}")
        return 1

    if not lakes_gdb_path.exists():
        logger.error(f"Lakes GDB not found: {lakes_gdb_path}")
        return 1

    # Run metadata extraction
    builder = MetadataBuilder(graph_path, zones_path, lakes_gdb_path, output_path)

    # Override extract_stream_metadata to use specified number of workers
    original_extract = builder.extract_stream_metadata
    builder.extract_stream_metadata = lambda: original_extract(num_workers=args.workers)

    success = builder.run()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
