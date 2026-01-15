"""
Phase 2: KML Point Enrichment

Goals:
1. Link KML points to containing polygons (lakes/wetlands/manmade)
2. Generate warning log for unmatched points

Memory Strategy:
- Polygon datasets are manageable size (~100K-500K total)
- Use spatial indexes instead of full spatial joins
- Process points iteratively
"""

import geopandas as gpd
import pandas as pd
from pathlib import Path
from typing import Optional
from .utils import setup_logging

logger = setup_logging(__name__)


class KMLEnricher:
    """Enriches KML points with waterbody polygon references."""

    def __init__(
        self,
        kml_path: Path,
        lakes_gdb: Path,
        output_points_path: Path,
        output_log_path: Path,
    ):
        """Initialize KML enricher.

        Args:
            kml_path: Path to KML file with labeled points
            lakes_gdb: Path to FWA_BC.gdb (contains lakes, wetlands, manmade)
            output_points_path: Path to save enriched points
            output_log_path: Path to save warning log
        """
        self.kml_path = kml_path
        self.lakes_gdb = lakes_gdb
        self.output_points_path = output_points_path
        self.output_log_path = output_log_path

        self.stats = {
            "total_points": 0,
            "matched_lakes": 0,
            "matched_wetlands": 0,
            "matched_manmade": 0,
            "unmatched": 0,
        }

    def load_kml_points(self) -> Optional[gpd.GeoDataFrame]:
        """Load user-labeled points from KML file.

        Returns:
            GeoDataFrame of points in WGS84, or None if failed
        """
        if not self.kml_path.exists():
            logger.warning(f"KML file not found: {self.kml_path}")
            return None

        try:
            # Enable KML driver
            import fiona

            fiona.drvsupport.supported_drivers["KML"] = "rw"
            fiona.drvsupport.supported_drivers["LIBKML"] = "rw"

            points = gpd.read_file(str(self.kml_path))

            # KML files are always WGS84
            if points.crs is None:
                points.set_crs(epsg=4326, inplace=True)

            self.stats["total_points"] = len(points)
            logger.info(f"Loaded {len(points)} KML points")
            return points

        except Exception as e:
            logger.error(f"Failed to load KML: {e}")
            return None

    def load_polygons(
        self, layer_name: str, polygon_type: str
    ) -> Optional[gpd.GeoDataFrame]:
        """Load polygon layer from FWA_BC.gdb.

        Args:
            layer_name: Name of layer in GDB
            polygon_type: Type label for logging (lakes/wetlands/manmade)

        Returns:
            GeoDataFrame of polygons in EPSG:3005, or None if failed
        """
        try:
            polygons = gpd.read_file(str(self.lakes_gdb), layer=layer_name)
            if polygons.crs != "EPSG:3005":
                polygons = polygons.to_crs("EPSG:3005")
            logger.info(f"Loaded {len(polygons)} {polygon_type} polygons")
            return polygons
        except Exception as e:
            logger.warning(f"Failed to load {polygon_type}: {e}")
            return None

    def enrich_with_polygon_ids(
        self,
        points: gpd.GeoDataFrame,
        polygons: gpd.GeoDataFrame,
        id_column: str,
        polygon_type: str,
    ) -> gpd.GeoDataFrame:
        """Enrich points with polygon IDs using spatial index.

        Args:
            points: Point GeoDataFrame to enrich
            polygons: Polygon GeoDataFrame to match against
            id_column: Column name to store matched polygon IDs
            polygon_type: Type label for logging

        Returns:
            Points with new ID column added
        """
        if polygons is None or polygons.empty:
            points[id_column] = None
            return points

        # Ensure CRS match
        if points.crs != polygons.crs:
            points = points.to_crs(polygons.crs)

        # Check for WATERBODY_POLY_ID column
        if "WATERBODY_POLY_ID" not in polygons.columns:
            logger.warning(f"{polygon_type} missing WATERBODY_POLY_ID column")
            points[id_column] = None
            return points

        # Initialize ID column
        points[id_column] = None

        # Use spatial index for efficient lookup
        logger.info(f"Matching points to {polygon_type} using spatial index...")

        # Build spatial index
        spatial_index = polygons.sindex

        matched_count = 0
        named_polygon_warnings = []

        for idx, point in points.iterrows():
            # Find candidate polygons using spatial index
            possible_matches_idx = list(
                spatial_index.intersection(point.geometry.bounds)
            )

            if not possible_matches_idx:
                continue

            # Check which polygon actually contains the point
            possible_matches = polygons.iloc[possible_matches_idx]

            for poly_idx, poly in possible_matches.iterrows():
                if poly.geometry.contains(point.geometry):
                    # Found containing polygon
                    poly_id = poly["WATERBODY_POLY_ID"]

                    if pd.notna(poly_id):
                        points.at[idx, id_column] = int(poly_id)
                        matched_count += 1

                        # Quality check: warn if polygon is named (should mostly be unnamed)
                        poly_name = poly.get("GNIS_NAME_1") or poly.get("GNIS_NAME")
                        if pd.notna(poly_name) and str(poly_name).strip():
                            point_name = point.get("Name") or point.get("name")
                            named_polygon_warnings.append(
                                {
                                    "point_idx": idx,
                                    "point_name": (
                                        str(point_name)
                                        if pd.notna(point_name)
                                        else "Unknown"
                                    ),
                                    "polygon_name": str(poly_name),
                                    "polygon_id": int(poly_id),
                                    "polygon_type": polygon_type,
                                }
                            )

                    break  # Take first match

        # Log quality warnings
        if named_polygon_warnings:
            logger.warning(
                f"!! {len(named_polygon_warnings)} points matched NAMED {polygon_type}. "
                f"Expected mostly unnamed polygons."
            )
            # Log first few examples
            for warning in named_polygon_warnings[:5]:
                logger.warning(
                    f"  Point '{warning['point_name']}' → "
                    f"{polygon_type} '{warning['polygon_name']}' (ID: {warning['polygon_id']})"
                )
            if len(named_polygon_warnings) > 5:
                logger.warning(f"  ... and {len(named_polygon_warnings) - 5} more")

        logger.info(f"  Matched {matched_count} points to {polygon_type}")

        # Update stats
        if polygon_type == "lakes":
            self.stats["matched_lakes"] = matched_count
        elif polygon_type == "wetlands":
            self.stats["matched_wetlands"] = matched_count
        elif polygon_type == "manmade":
            self.stats["matched_manmade"] = matched_count

        # Store warnings for log file
        if not hasattr(self, "_named_polygon_warnings"):
            self._named_polygon_warnings = []
        self._named_polygon_warnings.extend(named_polygon_warnings)

        return points

    def write_warning_log(self, points: gpd.GeoDataFrame):
        """Write warning log for unmatched points and named polygon matches.

        Args:
            points: Enriched points GeoDataFrame
        """
        # Find unmatched points
        unmatched = points[
            points["LAKE_POLY_ID"].isna()
            & points["WETLAND_POLY_ID"].isna()
            & points["MANMADE_POLY_ID"].isna()
        ]

        self.stats["unmatched"] = len(unmatched)

        # Create log file
        self.output_log_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.output_log_path, "w", encoding="utf-8") as f:
            f.write("KML Point Enrichment Warning Log\n")
            f.write("=" * 80 + "\n\n")

            # Section 1: Unmatched points
            f.write("SECTION 1: UNMATCHED POINTS\n")
            f.write("-" * 80 + "\n\n")

            if unmatched.empty:
                f.write("✓ All points successfully matched to waterbody polygons!\n")
            else:
                f.write(
                    f"!! WARNING: {len(unmatched)} points did not match any waterbody polygon !!\n\n"
                )
                f.write("These points may indicate:\n")
                f.write("  - GPS coordinate errors\n")
                f.write("  - Waterbodies missing from FWA database\n")
                f.write("  - Points placed outside waterbody boundaries\n\n")

                for idx, point in unmatched.iterrows():
                    point_name = point.get("Name") or point.get("name") or "Unknown"
                    coords = point.geometry

                    f.write(f"Point: {point_name}\n")
                    f.write(f"  Coordinates: ({coords.y:.6f}, {coords.x:.6f})\n")
                    f.write(f"  Index: {idx}\n")
                    f.write("\n")

            # Section 2: Named polygon matches
            f.write("\n" + "=" * 80 + "\n")
            f.write("SECTION 2: POINTS MATCHING NAMED POLYGONS\n")
            f.write("-" * 80 + "\n\n")

            if (
                hasattr(self, "_named_polygon_warnings")
                and self._named_polygon_warnings
            ):
                warnings = self._named_polygon_warnings
                f.write(
                    f"!! QUALITY ALERT: {len(warnings)} points matched NAMED waterbodies !!\n\n"
                )
                f.write("These should typically match UNNAMED waterbodies.\n")
                f.write("Review these matches for accuracy:\n\n")

                for warning in warnings:
                    f.write(f"Point: {warning['point_name']}\n")
                    f.write(
                        f"  Matched {warning['polygon_type']}: {warning['polygon_name']}\n"
                    )
                    f.write(f"  Polygon ID: {warning['polygon_id']}\n")
                    f.write(f"  Point Index: {warning['point_idx']}\n")
                    f.write("\n")
            else:
                f.write("✓ No points matched named polygons (expected behavior)\n")

            f.write("\n" + "=" * 80 + "\n")
            f.write(f"Log file: {self.output_log_path}\n")

        logger.info(f"Warning log written to: {self.output_log_path}")

        if not unmatched.empty:
            logger.warning(
                f"!! {len(unmatched)} points did not match any waterbody polygon !!"
            )

        if hasattr(self, "_named_polygon_warnings") and self._named_polygon_warnings:
            logger.warning(
                f"!! {len(self._named_polygon_warnings)} points matched NAMED polygons !!"
            )

    def run(self) -> Optional[Path]:
        """Execute KML enrichment.

        Returns:
            Path to enriched points file, or None if failed
        """
        logger.info("=== Phase 2: KML Point Enrichment ===")

        # Load KML points
        points = self.load_kml_points()
        if points is None or points.empty:
            logger.error("No KML points to process")
            return None

        # Load polygons
        lakes = self.load_polygons("FWA_LAKES_POLY", "lakes")
        wetlands = self.load_polygons("FWA_WETLANDS_POLY", "wetlands")
        manmade = self.load_polygons("FWA_MANMADE_WATERBODIES_POLY", "manmade")

        # Enrich points with polygon IDs
        points = self.enrich_with_polygon_ids(points, lakes, "LAKE_POLY_ID", "lakes")
        points = self.enrich_with_polygon_ids(
            points, wetlands, "WETLAND_POLY_ID", "wetlands"
        )
        points = self.enrich_with_polygon_ids(
            points, manmade, "MANMADE_POLY_ID", "manmade"
        )

        # Convert to nullable integer type
        for col in ["LAKE_POLY_ID", "WETLAND_POLY_ID", "MANMADE_POLY_ID"]:
            points[col] = points[col].astype("Int64")

        # Write warning log
        self.write_warning_log(points)

        # Save enriched points
        self.output_points_path.parent.mkdir(parents=True, exist_ok=True)
        points.to_file(str(self.output_points_path), driver="GPKG")
        logger.info(f"Enriched points saved to: {self.output_points_path}")

        # Final stats
        logger.info("=== KML Enrichment Complete ===")
        logger.info(f"  Total points: {self.stats['total_points']}")
        logger.info(f"  Matched to lakes: {self.stats['matched_lakes']}")
        logger.info(f"  Matched to wetlands: {self.stats['matched_wetlands']}")
        logger.info(f"  Matched to manmade: {self.stats['matched_manmade']}")
        logger.info(f"  Unmatched: {self.stats['unmatched']}")

        return self.output_points_path
