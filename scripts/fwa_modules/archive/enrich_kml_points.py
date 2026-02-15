#!/usr/bin/env python3
"""
KML Point Enrichment for Waterbody Polygon Processing

Enriches KML labeled points with waterbody keys and zone/MU information:
- Loads KML points from data/labelled/unnamed_lakes.kml
- Matches points to polygon waterbodies (lakes, marshes/wetlands, manmade)
- Extracts WATERBODY_KEY from intersecting polygons
- Queries wildlife management units to get zones, regions, and MUs
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
            "manmade_waterbody_key": null,
            "zones": ["1"],
            "region": "Region 1",
            "mgmt_units": ["1-10"],
            "cross_boundary": false
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
    """Enriches KML points with waterbody polygon keys and zone/MU information."""

    def __init__(
        self, kml_path: Path, gdb_path: Path, zones_path: Path, output_path: Path
    ):
        """
        Initialize the KML point enricher.

        Args:
            kml_path: Path to input KML file with labeled points
            gdb_path: Path to FWA_BC.gdb with polygon layers
            zones_path: Path to wildlife management units GeoPackage
            output_path: Path to output JSON file
        """
        self.kml_path = kml_path
        self.gdb_path = gdb_path
        self.zones_path = zones_path
        self.output_path = output_path

        # Polygon layers to process
        self.polygon_layers = {
            "lake": "FWA_LAKES_POLY",
            "marsh": "FWA_WETLANDS_POLY",
            "manmade": "FWA_MANMADE_WATERBODIES_POLY",
        }

        self.polygon_gdfs = {}
        self.zones_gdf = None
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
                    logger.warning(
                        f"Could not parse coordinates for {name}: {coords_text}"
                    )

        logger.info(f"Parsed {len(self.kml_points)} valid points from KML")

    def load_zones(self):
        """Load wildlife management units for zone assignment."""
        logger.info(f"Loading wildlife management units from: {self.zones_path}")

        self.zones_gdf = gpd.read_file(str(self.zones_path))

        # Extract zone number from WILDLIFE_MGMT_UNIT_ID (e.g., "1-10" -> "1")
        self.zones_gdf["zone"] = (
            self.zones_gdf["WILDLIFE_MGMT_UNIT_ID"].str.split("-").str[0]
        )

        logger.info(f"  Loaded {len(self.zones_gdf):,} wildlife management units")
        logger.info(f"  Covering {len(self.zones_gdf['zone'].unique())} unique zones")
        logger.info(f"  Zones CRS: {self.zones_gdf.crs}")

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

            # Initialize enriched data
            enriched = {
                "name": point_data["name"],
                "description": point_data["description"],
                "longitude": point_data["longitude"],
                "latitude": point_data["latitude"],
                "lake_waterbody_key": None,
                "marsh_waterbody_key": None,
                "manmade_waterbody_key": None,
                "fwa_watershed_code": None,
                "zones": [],
                "region": None,
                "mgmt_units": [],
                "cross_boundary": False,
            }

            point_geom = point_data["geometry"]

            # Find zones containing this point
            if self.zones_gdf is not None:
                # Create a GeoDataFrame from the point to handle CRS transformation
                point_gdf = gpd.GeoDataFrame(
                    [{"geometry": point_geom}], crs="EPSG:4326"
                )

                # Transform to zones CRS if different
                if point_gdf.crs != self.zones_gdf.crs:
                    point_gdf = point_gdf.to_crs(self.zones_gdf.crs)

                transformed_point = point_gdf.geometry.iloc[0]

                # Find zones containing this point
                containing_zones = self.zones_gdf[
                    self.zones_gdf.contains(transformed_point)
                ]

                if not containing_zones.empty:
                    zones = sorted(containing_zones["zone"].unique().tolist())
                    mgmt_units = sorted(
                        containing_zones["WILDLIFE_MGMT_UNIT_ID"].unique().tolist()
                    )

                    enriched["zones"] = zones
                    enriched["mgmt_units"] = mgmt_units
                    enriched["cross_boundary"] = len(zones) > 1

                    # Set region based on first zone
                    if zones:
                        enriched["region"] = f"Region {zones[0]}"
                else:
                    logger.debug(
                        f"    No zones found for {point_data['name']} at ({point_data['longitude']}, {point_data['latitude']})"
                    )

            # Check each polygon type
            for key, gdf in self.polygon_gdfs.items():
                if gdf is None or gdf.empty:
                    continue

                # Transform point to polygon's CRS for accurate spatial query
                point_gdf_for_poly = gpd.GeoDataFrame(
                    [{"geometry": point_geom}], crs="EPSG:4326"
                )
                if point_gdf_for_poly.crs != gdf.crs:
                    point_gdf_for_poly = point_gdf_for_poly.to_crs(gdf.crs)

                transformed_point_for_poly = point_gdf_for_poly.geometry.iloc[0]

                # Find polygons that contain this point
                containing = gdf[gdf.contains(transformed_point_for_poly)]

                if not containing.empty:
                    # Take the first match (could be multiple overlapping polygons)
                    waterbody_key = containing.iloc[0]["WATERBODY_KEY"]
                    watershed_code = containing.iloc[0].get("FWA_WATERSHED_CODE", "")

                    # Convert waterbody_key to string, handle different numeric types
                    if pd.notna(waterbody_key):
                        try:
                            # Convert to int first to remove decimals, then to string
                            waterbody_key_str = str(int(waterbody_key))
                        except (ValueError, TypeError):
                            waterbody_key_str = str(waterbody_key)

                        # Store in appropriate field
                        enriched[f"{key}_waterbody_key"] = waterbody_key_str

                        # Store watershed code if available and not already set
                        if (
                            pd.notna(watershed_code)
                            and watershed_code
                            and not enriched["fwa_watershed_code"]
                        ):
                            enriched["fwa_watershed_code"] = str(watershed_code)

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

        # Count zone matches
        zone_matches = sum(1 for p in enriched_points if p["zones"])
        logger.info(f"  Points with zone info: {zone_matches}")

        return enriched_points

    def save_enriched_points(self, enriched_points):
        """Save enriched points to JSON file."""
        logger.info(f"Saving enriched points to: {self.output_path}")

        output_data = {
            "metadata": {
                "source_kml": str(self.kml_path),
                "source_gdb": str(self.gdb_path),
                "source_zones": str(self.zones_path),
                "total_points": len(enriched_points),
                "lake_matches": sum(
                    1 for p in enriched_points if p["lake_waterbody_key"]
                ),
                "marsh_matches": sum(
                    1 for p in enriched_points if p["marsh_waterbody_key"]
                ),
                "manmade_matches": sum(
                    1 for p in enriched_points if p["manmade_waterbody_key"]
                ),
                "zone_matches": sum(1 for p in enriched_points if p["zones"]),
                "cross_boundary_points": sum(
                    1 for p in enriched_points if p["cross_boundary"]
                ),
            },
            "points": enriched_points,
        }

        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Successfully saved {len(enriched_points)} enriched points")

    def generate_name_variations(self, enriched_points):
        """
        Generate name variation entries for KML points that have polygon matches.

        This outputs Python code that can be pasted into name_variations.py to
        convert KML point matches into explicit name variations.

        Groups points with duplicate names to use a single entry with multiple feature_ids.

        Args:
            enriched_points: List of enriched point data
        """
        logger.info("=== Generating Name Variations ===")

        # Group by region and then by name (to detect duplicates)
        by_region = {}
        for point in enriched_points:
            # Only include points that have a polygon match
            waterbody_key = (
                point.get("lake_waterbody_key")
                or point.get("marsh_waterbody_key")
                or point.get("manmade_waterbody_key")
            )

            if not waterbody_key:
                continue

            region = point.get("region", "Unknown")
            name = point["name"]

            if region not in by_region:
                by_region[region] = {}

            # Group by name to detect duplicates
            if name not in by_region[region]:
                by_region[region][name] = {
                    "waterbody_keys": [],
                    "mgmt_units": set(),
                    "types": set(),
                }

            by_region[region][name]["waterbody_keys"].append(waterbody_key)
            by_region[region][name]["mgmt_units"].update(point.get("mgmt_units", []))
            by_region[region][name]["types"].add(
                "lake"
                if point.get("lake_waterbody_key")
                else "marsh" if point.get("marsh_waterbody_key") else "manmade"
            )

        # Generate output file
        output_path = self.output_path.parent / "kml_name_variations.py"

        with open(output_path, "w", encoding="utf-8") as f:
            f.write('"""\n')
            f.write("Generated Name Variations from KML Points\n\n")
            f.write("These entries can be copied into name_variations.py to convert\n")
            f.write("KML point matches into explicit name variations.\n\n")
            f.write("This allows:\n")
            f.write("- Better documentation with URLs/notes\n")
            f.write("- Explicit waterbody_key matching via feature_ids list\n")
            f.write("- Handling of MU boundary issues\n")
            f.write("- Multiple polygons with same name (feature_ids list)\n")
            f.write('"""\n\n')

            total_entries = 0
            total_polygons = 0
            for region in sorted(by_region.keys()):
                names = by_region[region]
                total_entries += len(names)

                # Count total polygons
                region_polygons = sum(
                    len(data["waterbody_keys"]) for data in names.values()
                )
                total_polygons += region_polygons

                f.write(
                    f"    # {region} - {len(names)} unique names, {region_polygons} polygon(s)\n"
                )
                f.write(f'    "{region}": {{\n')

                for name, data in sorted(names.items()):
                    # Format the name for use as dictionary key
                    key_name = name.upper()

                    # Escape quotes and backslashes properly
                    key_name_escaped = key_name.replace("\\", "\\\\").replace(
                        '"', '\\"'
                    )
                    target_name_escaped = name.replace("\\", "\\\\").replace('"', '\\"')

                    # Build type description
                    types_str = "/".join(sorted(data["types"]))
                    mus_str = ", ".join(sorted(data["mgmt_units"]))

                    # Indicate if multiple polygons
                    count_note = (
                        f" ({len(data['waterbody_keys'])} polygons)"
                        if len(data["waterbody_keys"]) > 1
                        else ""
                    )

                    f.write(f'        "{key_name_escaped}": NameVariation(\n')
                    f.write(f'            target_names=["{target_name_escaped}"],\n')
                    f.write(
                        f'            note="{types_str.capitalize()} polygon match from KML point{count_note} (MUs: {mus_str})",\n'
                    )

                    # Use waterbody_key for single polygon, feature_ids for multiple
                    if len(data["waterbody_keys"]) == 1:
                        f.write(
                            f'            waterbody_key="{data["waterbody_keys"][0]}",\n'
                        )
                    else:
                        feature_ids_str = ", ".join(
                            f'"{wk}"' for wk in sorted(data["waterbody_keys"])
                        )
                        f.write(f"            feature_ids=[{feature_ids_str}],\n")
                    f.write(f"        ),\n")

                f.write("    },\n\n")

            f.write(
                f"# Summary: {total_entries} unique names, {total_polygons} total polygons\n"
            )

        logger.info(
            f"Generated name variations for {total_entries} unique names ({total_polygons} total polygons)"
        )
        logger.info(f"Output saved to: {output_path}")
        logger.info("Review the file and copy relevant entries to name_variations.py")

    def run(self):
        """Execute the full enrichment pipeline."""
        logger.info("=== Starting KML Point Enrichment ===")

        # Step 1: Parse KML
        self.parse_kml()

        if not self.kml_points:
            logger.error("No points found in KML file. Exiting.")
            return False

        # Step 2: Load zones
        self.load_zones()

        # Step 3: Load polygon layers
        self.load_polygon_layers()

        # Step 4: Enrich points
        enriched_points = self.enrich_points()

        # Step 4: Save results
        self.save_enriched_points(enriched_points)

        # Step 5: Generate name variations for KML points with polygon matches
        self.generate_name_variations(enriched_points)

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
    zones_path = (
        project_root
        / "data/ftp.geobc.gov.bc.ca/sections/outgoing/bmgs/FWA_Public"
        / "WAA_WILDLIFE_MGMT_UNITS_SVW.gpkg"
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

    if not zones_path.exists():
        logger.error(f"Zones file not found: {zones_path}")
        return 1

    # Run enrichment
    enricher = KMLPointEnricher(kml_path, gdb_path, zones_path, output_path)
    success = enricher.run()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
