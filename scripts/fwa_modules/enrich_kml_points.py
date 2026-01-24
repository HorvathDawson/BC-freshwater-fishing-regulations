#!/usr/bin/env python3
"""
KML Point Enrichment for Waterbody Polygon Processing

Enriches KML labeled points with waterbody keys by spatial matching:
- Loads KML points from data/labelled/unnamed_lakes.kml
- Matches points to polygon waterbodies (lakes, marshes/wetlands, manmade)
- Extracts WATERBODY_KEY from intersecting polygons
- Outputs enriched points as JSON for downstream matching

Output JSON structure:
{
    "points": [
        {
            "name": "unnamed lake c - map b",
            "description": "...",
            "longitude": -125.477748387775,
            "latitude": 50.13407308649698,
            "lake_waterbody_key": "12345",
            "marsh_waterbody_key": null,
            "manmade_waterbody_key": null
        },
        ...
    ]
}
"""

import os
import sys
import logging
import json
from pathlib import Path
from xml.etree import ElementTree as ET
import geopandas as gpd
from shapely.geometry import Point
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


class KMLPointEnricher:
    """Enriches KML points with waterbody polygon keys."""

    def __init__(self, kml_path: Path, gdb_path: Path, output_path: Path):
        """
        Initialize the KML point enricher.

        Args:
            kml_path: Path to input KML file with labeled points
            gdb_path: Path to FWA_BC.gdb with polygon layers
            output_path: Path to output JSON file
        """
        self.kml_path = kml_path
        self.gdb_path = gdb_path
        self.output_path = output_path

        # Polygon layers to process
        self.polygon_layers = {
            "lake": "FWA_LAKES_POLY",
            "marsh": "FWA_WETLANDS_POLY",
            "manmade": "FWA_MANMADE_WATERBODIES_POLY",
        }

        self.polygon_gdfs = {}
        self.kml_points = []

    def parse_kml(self):
        """Parse KML file and extract point features with names and coordinates."""
        logger.info(f"Parsing KML file: {self.kml_path}")

        # Define KML namespace
        ns = {"kml": "http://www.opengis.net/kml/2.2"}

        tree = ET.parse(self.kml_path)
        root = tree.getroot()

        # Find all Placemarks
        placemarks = root.findall(".//kml:Placemark", ns)
        logger.info(f"Found {len(placemarks)} placemarks in KML")

        for placemark in placemarks:
            # Extract name
            name_elem = placemark.find("kml:name", ns)
            name = name_elem.text if name_elem is not None else None

            # Extract description (optional)
            desc_elem = placemark.find("kml:description", ns)
            description = desc_elem.text if desc_elem is not None else None

            # Extract coordinates from Point
            point_elem = placemark.find(".//kml:Point", ns)
            if point_elem is None:
                continue

            coords_elem = point_elem.find("kml:coordinates", ns)
            if coords_elem is None:
                continue

            # Parse coordinates (format: "lon,lat,altitude")
            coords_text = coords_elem.text.strip()
            coords_parts = coords_text.split(",")

            if len(coords_parts) >= 2:
                try:
                    lon = float(coords_parts[0])
                    lat = float(coords_parts[1])

                    self.kml_points.append(
                        {
                            "name": name,
                            "description": description,
                            "longitude": lon,
                            "latitude": lat,
                            "geometry": Point(lon, lat),
                        }
                    )
                except ValueError:
                    logger.warning(f"Could not parse coordinates for {name}: {coords_text}")

        logger.info(f"Parsed {len(self.kml_points)} valid points from KML")

    def load_polygon_layers(self):
        """Load polygon layers from GDB into GeoDataFrames."""
        logger.info(f"Loading polygon layers from: {self.gdb_path}")

        for key, layer_name in self.polygon_layers.items():
            try:
                logger.info(f"  Loading {layer_name}...")
                gdf = gpd.read_file(str(self.gdb_path), layer=layer_name)

                # Filter to only features with valid WATERBODY_KEY
                if "WATERBODY_KEY" in gdf.columns:
                    initial_count = len(gdf)
                    gdf = gdf[gdf["WATERBODY_KEY"].notna()].copy()
                    logger.info(
                        f"    Loaded {len(gdf):,} features (filtered from {initial_count:,})"
                    )
                else:
                    logger.warning(f"    No WATERBODY_KEY column found in {layer_name}")

                # Store only necessary columns
                columns_to_keep = ["WATERBODY_KEY", "geometry"]
                if "GNIS_NAME_1" in gdf.columns:
                    columns_to_keep.append("GNIS_NAME_1")
                elif "GNIS_NAME" in gdf.columns:
                    columns_to_keep.append("GNIS_NAME")

                # Filter columns
                available_cols = [col for col in columns_to_keep if col in gdf.columns]
                gdf = gdf[available_cols].copy()

                self.polygon_gdfs[key] = gdf

            except Exception as e:
                logger.error(f"    Failed to load {layer_name}: {e}")
                self.polygon_gdfs[key] = None

    def enrich_points(self):
        """Match KML points to polygon waterbodies and extract WATERBODY_KEY."""
        logger.info("Enriching KML points with waterbody keys...")

        enriched_points = []
        total_points = len(self.kml_points)

        for idx, point_data in enumerate(self.kml_points):
            if (idx + 1) % 50 == 0:
                logger.info(f"  Processing point {idx + 1}/{total_points}...")

            # Initialize waterbody keys as None
            enriched = {
                "name": point_data["name"],
                "description": point_data["description"],
                "longitude": point_data["longitude"],
                "latitude": point_data["latitude"],
                "lake_waterbody_key": None,
                "marsh_waterbody_key": None,
                "manmade_waterbody_key": None,
            }

            point_geom = point_data["geometry"]

            # Check each polygon type
            for key, gdf in self.polygon_gdfs.items():
                if gdf is None or gdf.empty:
                    continue

                # Find polygons that contain this point
                containing = gdf[gdf.contains(point_geom)]

                if not containing.empty:
                    # Take the first match (could be multiple overlapping polygons)
                    waterbody_key = containing.iloc[0]["WATERBODY_KEY"]

                    # Convert to string, handle different numeric types
                    if pd.notna(waterbody_key):
                        try:
                            # Convert to int first to remove decimals, then to string
                            waterbody_key_str = str(int(waterbody_key))
                        except (ValueError, TypeError):
                            waterbody_key_str = str(waterbody_key)

                        # Store in appropriate field
                        enriched[f"{key}_waterbody_key"] = waterbody_key_str

                        logger.debug(
                            f"    Matched {point_data['name']} to {key} waterbody {waterbody_key_str}"
                        )

            enriched_points.append(enriched)

        logger.info(f"Enriched {len(enriched_points)} points")

        # Count matches
        lake_matches = sum(1 for p in enriched_points if p["lake_waterbody_key"])
        marsh_matches = sum(1 for p in enriched_points if p["marsh_waterbody_key"])
        manmade_matches = sum(1 for p in enriched_points if p["manmade_waterbody_key"])

        logger.info(f"  Lake matches: {lake_matches}")
        logger.info(f"  Marsh/wetland matches: {marsh_matches}")
        logger.info(f"  Manmade matches: {manmade_matches}")

        unmatched = sum(
            1
            for p in enriched_points
            if not (
                p["lake_waterbody_key"]
                or p["marsh_waterbody_key"]
                or p["manmade_waterbody_key"]
            )
        )
        logger.info(f"  Unmatched points: {unmatched}")

        return enriched_points

    def save_enriched_points(self, enriched_points):
        """Save enriched points to JSON file."""
        logger.info(f"Saving enriched points to: {self.output_path}")

        output_data = {
            "metadata": {
                "source_kml": str(self.kml_path),
                "source_gdb": str(self.gdb_path),
                "total_points": len(enriched_points),
                "lake_matches": sum(1 for p in enriched_points if p["lake_waterbody_key"]),
                "marsh_matches": sum(1 for p in enriched_points if p["marsh_waterbody_key"]),
                "manmade_matches": sum(
                    1 for p in enriched_points if p["manmade_waterbody_key"]
                ),
            },
            "points": enriched_points,
        }

        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Successfully saved {len(enriched_points)} enriched points")

    def run(self):
        """Execute the full enrichment pipeline."""
        logger.info("=== Starting KML Point Enrichment ===")

        # Step 1: Parse KML
        self.parse_kml()

        if not self.kml_points:
            logger.error("No points found in KML file. Exiting.")
            return False

        # Step 2: Load polygon layers
        self.load_polygon_layers()

        # Step 3: Enrich points
        enriched_points = self.enrich_points()

        # Step 4: Save results
        self.save_enriched_points(enriched_points)

        logger.info("=== KML Point Enrichment Complete ===")
        return True


def main():
    """Main entry point for script execution."""
    # Setup paths
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent

    kml_path = project_root / "data" / "labelled" / "unnamed_lakes.kml"
    gdb_path = (
        project_root
        / "data/ftp.geobc.gov.bc.ca/sections/outgoing/bmgs/FWA_Public/FWA_BC/FWA_BC.gdb"
    )
    output_path = (
        project_root / "scripts" / "output" / "fwa_modules" / "enriched_kml_points.json"
    )

    # Validate input files
    if not kml_path.exists():
        logger.error(f"KML file not found: {kml_path}")
        return 1

    if not gdb_path.exists():
        logger.error(f"GDB file not found: {gdb_path}")
        return 1

    # Run enrichment
    enricher = KMLPointEnricher(kml_path, gdb_path, output_path)
    success = enricher.run()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
