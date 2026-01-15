"""
Phase 4: Memory-Efficient Zone Splitting

Goals:
1. Split all features (streams, lakes, wetlands, manmade, points) by wildlife management zones
2. Never load all features of one type into memory simultaneously

Memory Strategy:
- Process streams layer-by-layer, append to zone files incrementally
- Load polygons fully (manageable size) but write zone-by-zone
- Use spatial index for efficient zone lookup
- Write directly to output, no accumulation
"""

import gc
import time
import fiona
import geopandas as gpd
import pandas as pd
from pathlib import Path
from typing import Dict, Optional
from .utils import setup_logging

logger = setup_logging(__name__)


class ZoneSplitter:
    """Splits geospatial data by wildlife management zones with minimal memory usage."""

    def __init__(
        self,
        streams_gdb: Path,
        lakes_gdb: Path,
        enriched_points: Path,
        wildlife_zones: Path,
        tributary_map_path: Path,
        lake_segments_path: Path,
        output_gdb: Path,
    ):
        """Initialize zone splitter.

        Args:
            streams_gdb: Path to preprocessed streams GeoPackage (.gpkg)
            lakes_gdb: Path to FWA_BC.gdb (lakes, wetlands, manmade)
            enriched_points: Path to enriched KML points
            wildlife_zones: Path to wildlife management units GPKG
            tributary_map_path: Path to tributary relationships JSON
            lake_segments_path: Path to lake segments JSON
            output_gdb: Path to output zone-split GDB
        """
        self.streams_gdb = streams_gdb
        self.lakes_gdb = lakes_gdb
        self.enriched_points = enriched_points
        self.wildlife_zones = wildlife_zones
        self.tributary_map_path = tributary_map_path
        self.lake_segments_path = lake_segments_path
        self.output_gdb = output_gdb

        self.stats = {
            "streams_processed": 0,
            "lakes_processed": 0,
            "wetlands_processed": 0,
            "manmade_processed": 0,
            "points_processed": 0,
        }

    def load_zones(self) -> gpd.GeoDataFrame:
        """Load wildlife management zones.

        Returns:
            GeoDataFrame of zones in BC Albers projection
        """
        logger.info("Loading wildlife management zones...")

        zones = gpd.read_file(str(self.wildlife_zones))

        # Find zone field
        zone_field = next(
            col
            for col in zones.columns
            if "ZONE" in col.upper() or "UNIT" in col.upper()
        )

        # Create zone groups (first part before hyphen)
        zones["ZONE_GROUP"] = zones[zone_field].astype(str).str.split("-").str[0]

        # Reproject to BC Albers
        target_crs = "EPSG:3005"
        if zones.crs != target_crs:
            logger.info(f"Reprojecting zones from {zones.crs} to {target_crs}")
            zones = zones.to_crs(target_crs)
        else:
            logger.info(f"Zones already in {target_crs}")

        logger.info(
            f"Loaded {len(zones)} management units in {len(zones['ZONE_GROUP'].unique())} zone groups"
        )
        logger.info(f"Zone bounds: {zones.total_bounds}")

        return zones

    def load_tributary_data(self) -> Dict:
        """Load tributary and lake segment data.

        Returns:
            Dict with 'tributary_of' and 'lake_segment' mappings
        """
        import json

        data = {"tributary_of": {}, "lake_segment": {}}

        # Load tributary map
        if self.tributary_map_path.exists():
            with open(self.tributary_map_path, "r") as f:
                data["tributary_of"] = json.load(f)
            logger.info(f"Loaded {len(data['tributary_of'])} tributary relationships")

        # Load lake segments
        if self.lake_segments_path.exists():
            with open(self.lake_segments_path, "r") as f:
                data["lake_segment"] = json.load(f)
            logger.info(f"Loaded {len(data['lake_segment'])} lake segments")

        return data

    def clip_features_to_zones(
        self, features: gpd.GeoDataFrame, zones: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """Assign features to zones using vectorized spatial join.

        For features that span multiple zones, assigns to all intersecting zones.

        Args:
            features: GeoDataFrame to assign
            zones: Zone boundaries

        Returns:
            Features with ZONE_GROUP and IS_CLIPPED columns
        """
        if features.crs != zones.crs:
            features = features.to_crs(zones.crs)

        # Use spatial join to find all zone intersections
        joined = gpd.sjoin(
            features,
            zones[["geometry", "ZONE_GROUP"]],
            how="left",
            predicate="intersects",
        )

        # Group by original feature index to find multi-zone features
        # Filter out NaN values and convert to strings
        try:
            zone_counts = (
                joined.groupby(level=0)["ZONE_GROUP"]
                .apply(lambda x: [str(z) for z in x if pd.notna(z) and str(z) != "nan"])
                .to_dict()
            )
            logger.debug(f"Grouped {len(zone_counts)} feature-zone assignments")
        except Exception as e:
            logger.error(f"Zone grouping failed: {e}")
            logger.error(f"Joined ZONE_GROUP dtype: {joined['ZONE_GROUP'].dtype}")
            logger.error(
                f"Joined ZONE_GROUP sample: {joined['ZONE_GROUP'].head(10).tolist()}"
            )
            logger.error(
                f"Joined ZONE_GROUP unique values: {joined['ZONE_GROUP'].unique()[:20]}"
            )
            raise

        # Create result dataframe
        result = features.copy()

        def create_zone_string(idx):
            try:
                zones_list = zone_counts.get(idx, [])
                if not zones_list:
                    return None
                # Ensure all are strings and filter out 'nan' strings
                clean_zones = [str(z) for z in zones_list if str(z) != "nan"]
                if not clean_zones:
                    return None
                return ";".join(sorted(set(clean_zones)))
            except Exception as e:
                logger.error(f"Error creating zone string for idx {idx}: {e}")
                logger.error(
                    f"zones_list type: {type(zones_list)}, value: {zones_list}"
                )
                raise

        try:
            result["ZONE_GROUP"] = result.index.map(create_zone_string)
            logger.debug(
                f"Zone strings created for {result['ZONE_GROUP'].notna().sum()} features"
            )
        except Exception as e:
            logger.error(f"Failed to create ZONE_GROUP column: {e}")
            raise
        result["IS_CLIPPED"] = result.index.map(
            lambda idx: (
                len(set(zone_counts.get(idx, []))) > 1
                if zone_counts.get(idx)
                else False
            )
        )

        # Filter out features with no zone assignment
        result = result[result["ZONE_GROUP"].notna()]

        return result

    def write_features_to_zone_layers(
        self,
        features: gpd.GeoDataFrame,
        layer_prefix: str,
        dedup_column: Optional[str] = None,
    ):
        """Write features to zone-specific layers in output GDB.

        For multi-zone features, they appear in all relevant zone layers.

        Args:
            features: GeoDataFrame with ZONE_GROUP column (semicolon-separated list)
            layer_prefix: Prefix for layer names (e.g., "STREAMS", "LAKES")
            dedup_column: Column to deduplicate on (if any)
        """
        # Explode multi-zone features
        exploded_features = []

        for idx, feature in features.iterrows():
            zone_str = feature.get("ZONE_GROUP", "")

            if pd.isna(zone_str) or zone_str == "":
                continue

            # Split zone list
            zones = str(zone_str).split(";")

            for zone in zones:
                zone = zone.strip()
                if zone:
                    feature_copy = feature.copy()
                    feature_copy["_ZONE_SINGLE"] = zone
                    exploded_features.append(feature_copy)

        if len(exploded_features) == 0:
            return

        exploded_gdf = gpd.GeoDataFrame(exploded_features, crs=features.crs)
        unique_zones = exploded_gdf["_ZONE_SINGLE"].unique()

        for zone in sorted(unique_zones):
            zone_features = exploded_gdf[exploded_gdf["_ZONE_SINGLE"] == zone].copy()

            if zone_features.empty:
                continue

            # Deduplicate if requested
            if dedup_column and dedup_column in zone_features.columns:
                zone_features = zone_features.drop_duplicates(subset=[dedup_column])

            # Remove temporary column before saving
            zone_features = zone_features.drop(
                columns=["_ZONE_SINGLE"], errors="ignore"
            )

            # Write to GDB
            layer_name = f"{layer_prefix}_ZONE_{zone}"

            try:
                zone_features.to_file(
                    str(self.output_gdb),
                    layer=layer_name,
                    driver="OpenFileGDB",
                    mode=(
                        "a"
                        if (self.output_gdb / f"{layer_name}.gdbtable").exists()
                        else "w"
                    ),
                )
                time.sleep(0.05)  # Small delay to avoid file lock issues
            except Exception as e:
                logger.error(f"Failed to write {layer_name}: {e}")

    def split_streams(self, zones: gpd.GeoDataFrame, tributary_data: Dict):
        """Split streams by zone with memory-efficient layer-by-layer processing.

        Args:
            zones: Zone boundaries
            tributary_data: Tributary and lake segment mappings
        """
        logger.info("Splitting streams by zone...")

        try:
            layers = fiona.listlayers(str(self.streams_gdb))
            total_layers = len(layers)

            for layer_idx, layer_name in enumerate(layers, 1):
                if layer_idx % 25 == 0:
                    logger.info(f"  Processing layer {layer_idx}/{total_layers}...")

                try:
                    # Load layer in BC Albers
                    streams = gpd.read_file(str(self.streams_gdb), layer=layer_name)
                    if streams.crs != "EPSG:3005":
                        streams = streams.to_crs("EPSG:3005")

                    if streams.empty:
                        continue

                    # Add tributary data
                    streams["TRIBUTARY_OF"] = streams["LINEAR_FEATURE_ID"].map(
                        lambda x: (
                            tributary_data["tributary_of"]
                            .get(str(x), {})
                            .get("tributary_of")
                            if str(x) in tributary_data["tributary_of"]
                            else None
                        )
                    )

                    streams["LAKE_POLY_ID"] = streams["LINEAR_FEATURE_ID"].map(
                        lambda x: (
                            tributary_data["tributary_of"]
                            .get(str(x), {})
                            .get("lake_poly_id")
                            if str(x) in tributary_data["tributary_of"]
                            else tributary_data["lake_segment"].get(str(x))
                        )
                    )

                    # Convert to nullable integer
                    if "LAKE_POLY_ID" in streams.columns:
                        streams["LAKE_POLY_ID"] = streams["LAKE_POLY_ID"].astype(
                            "Int64"
                        )

                    # Assign zones with clipping
                    try:
                        streams = self.clip_features_to_zones(streams, zones)
                    except Exception as e:
                        logger.error(
                            f"Failed to assign zones for layer {layer_name}: {e}"
                        )
                        logger.error(
                            f"Streams info: shape={streams.shape}, crs={streams.crs}"
                        )
                        raise

                    # Write to zone layers
                    self.write_features_to_zone_layers(
                        streams, "STREAMS", dedup_column="LINEAR_FEATURE_ID"
                    )

                    self.stats["streams_processed"] += len(streams)

                    del streams
                    gc.collect()

                except Exception as e:
                    logger.warning(f"Failed to process stream layer {layer_name}: {e}")

            logger.info(
                f"  Streams complete: {self.stats['streams_processed']:,} features"
            )

        except Exception as e:
            logger.error(f"Failed to split streams: {e}")

    def split_polygons(
        self,
        zones: gpd.GeoDataFrame,
        layer_name: str,
        output_prefix: str,
        dedup_column: str = "WATERBODY_POLY_ID",
    ):
        """Split polygon layer by zone.

        Args:
            zones: Zone boundaries
            layer_name: Name of layer in lakes_gdb
            output_prefix: Prefix for output layers (e.g., "LAKES", "WETLANDS")
            dedup_column: Column to deduplicate on
        """
        logger.info(f"Splitting {output_prefix.lower()} by zone...")

        try:
            # Load polygons in BC Albers
            polygons = gpd.read_file(str(self.lakes_gdb), layer=layer_name)
            if polygons.crs != "EPSG:3005":
                polygons = polygons.to_crs("EPSG:3005")

            if polygons.empty:
                logger.warning(f"  No {output_prefix.lower()} found")
                return

            logger.info(f"  Loaded {len(polygons)} {output_prefix.lower()}")
            logger.info(f"  Polygon CRS: {polygons.crs}")
            logger.info(f"  Polygon columns: {polygons.columns.tolist()}")

            # Assign zones with clipping
            try:
                polygons = self.clip_features_to_zones(polygons, zones)
                logger.info(
                    f"  Zone assignment complete: {len(polygons)} features with zones"
                )
            except Exception as e:
                logger.error(f"Zone clipping failed for {output_prefix}: {e}")
                logger.error(f"Polygon info: {polygons.info()}")
                raise
            # Write to zone layers
            self.write_features_to_zone_layers(
                polygons, output_prefix, dedup_column=dedup_column
            )

            # Update stats
            if output_prefix == "LAKES":
                self.stats["lakes_processed"] = len(polygons)
            elif output_prefix == "WETLANDS":
                self.stats["wetlands_processed"] = len(polygons)
            elif output_prefix == "MANMADE":
                self.stats["manmade_processed"] = len(polygons)

            logger.info(f"  {output_prefix} complete: {len(polygons):,} features")

            del polygons
            gc.collect()

        except Exception as e:
            logger.warning(f"Failed to split {output_prefix.lower()}: {e}")

    def split_points(self, zones: gpd.GeoDataFrame):
        """Split enriched KML points by zone.

        Args:
            zones: Zone boundaries
        """
        logger.info("Splitting points by zone...")

        if not self.enriched_points.exists():
            logger.warning("  No enriched points found")
            return

        try:
            # Load points in BC Albers
            points = gpd.read_file(str(self.enriched_points))
            if points.crs != "EPSG:3005":
                points = points.to_crs("EPSG:3005")

            if points.empty:
                logger.warning("  No points found")
                return

            logger.info(f"  Loaded {len(points)} points")

            # Assign zones with clipping
            points = self.clip_features_to_zones(points, zones)

            # Write to zone layers
            self.write_features_to_zone_layers(points, "LABELED_POINTS")

            self.stats["points_processed"] = len(points)

            logger.info(f"  Points complete: {len(points):,} features")

            del points
            gc.collect()

        except Exception as e:
            logger.error(f"Failed to split points: {e}")

    def save_zone_outlines(self, zones: gpd.GeoDataFrame):
        """Save zone outline layers and management units.

        Args:
            zones: Zone boundaries
        """
        logger.info("Saving zone outlines...")

        # Save full management units layer
        zones.to_file(
            str(self.output_gdb), layer="WILDLIFE_MGMT_UNITS", driver="OpenFileGDB"
        )

        # Save dissolved zone outlines
        zone_outlines = zones.dissolve(by="ZONE_GROUP")

        for zone in sorted(zones["ZONE_GROUP"].unique()):
            if zone in zone_outlines.index:
                outline = zone_outlines.loc[[zone]]
                outline.to_file(
                    str(self.output_gdb),
                    layer=f"ZONE_OUTLINE_{zone}",
                    driver="OpenFileGDB",
                )
                time.sleep(0.05)

        logger.info(f"  Saved {len(zone_outlines)} zone outlines")

    def run(self):
        """Execute zone splitting.

        Returns:
            Path to output GDB
        """
        logger.info("=== Phase 4: Zone Splitting ===")

        # Prepare output
        self.output_gdb.parent.mkdir(parents=True, exist_ok=True)

        if self.output_gdb.exists():
            import shutil

            try:
                shutil.rmtree(self.output_gdb)
                logger.info(f"Removed old output: {self.output_gdb}")
            except PermissionError as e:
                logger.warning(
                    f"Could not remove old GDB (may be open in another program): {e}"
                )
                logger.warning(
                    "Continuing with existing GDB - layers will be overwritten"
                )
            except Exception as e:
                logger.warning(f"Error removing old GDB: {e}")
                logger.warning(
                    "Continuing with existing GDB - layers will be overwritten"
                )

        # Load zones
        zones = self.load_zones()

        # Load tributary data
        tributary_data = self.load_tributary_data()

        # Split each feature type
        self.split_streams(zones, tributary_data)
        self.split_polygons(zones, "FWA_LAKES_POLY", "LAKES")
        self.split_polygons(zones, "FWA_WETLANDS_POLY", "WETLANDS")
        self.split_polygons(zones, "FWA_MANMADE_WATERBODIES_POLY", "MANMADE")
        self.split_points(zones)

        # Save zone outlines
        self.save_zone_outlines(zones)

        # Final stats
        logger.info("=== Zone Splitting Complete ===")
        logger.info(f"  Streams: {self.stats['streams_processed']:,}")
        logger.info(f"  Lakes: {self.stats['lakes_processed']:,}")
        logger.info(f"  Wetlands: {self.stats['wetlands_processed']:,}")
        logger.info(f"  Manmade: {self.stats['manmade_processed']:,}")
        logger.info(f"  Points: {self.stats['points_processed']:,}")
        logger.info(f"  Output: {self.output_gdb}")

        return self.output_gdb
