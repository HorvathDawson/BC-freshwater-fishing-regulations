#!/usr/bin/env python3
"""
GPKG to PMTiles Converter

Converts the waterbodies_by_zone.gpkg GeoPackage from geo_splitter.py
into a PMTiles vector tile archive with all layers properly included.

The script:
1. Reads all layers from the input GPKG
2. Generates vector tiles using tippecanoe
3. Converts to PMTiles format for efficient web serving

Requirements:
    - tippecanoe (https://github.com/felt/tippecanoe)
    - pmtiles Python package: pip install pmtiles
    - fiona, geopandas
"""

import os
import sys
import logging
import subprocess
import json
import tempfile
import shutil
from pathlib import Path
from typing import List, Dict, Optional
import fiona
import geopandas as gpd
from pmtiles.convert import mbtiles_to_pmtiles

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


class GpkgToPmtilesConverter:
    """Converts GeoPackage to PMTiles vector tiles."""

    def __init__(
        self,
        input_gpkg: Path,
        output_pmtiles: Path,
        max_zoom: int = 14,
        min_zoom: int = 4,
        base_zoom: int = 12,
        simplification: int = 10,
    ):
        """
        Initialize the converter.

        Args:
            input_gpkg: Path to input GeoPackage
            output_pmtiles: Path to output PMTiles file
            max_zoom: Maximum zoom level (default: 14)
            min_zoom: Minimum zoom level (default: 4)
            base_zoom: Base zoom for detail level (default: 12)
            simplification: Simplification factor for tippecanoe (default: 10)
        """
        self.input_gpkg = input_gpkg
        self.output_pmtiles = output_pmtiles
        self.max_zoom = max_zoom
        self.min_zoom = min_zoom
        self.base_zoom = base_zoom
        self.simplification = simplification

    def check_dependencies(self) -> bool:
        """
        Check if required tools are installed.

        Returns:
            bool: True if all dependencies are available
        """
        # Check tippecanoe
        try:
            result = subprocess.run(
                ["tippecanoe", "--version"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                logger.info("✓ tippecanoe found")
            else:
                logger.error("tippecanoe not found")
                logger.info("Install: https://github.com/felt/tippecanoe")
                return False
        except (subprocess.TimeoutExpired, FileNotFoundError):
            logger.error("tippecanoe not found")
            logger.info("Install: https://github.com/felt/tippecanoe")
            return False

        # Check pmtiles Python library
        try:
            from pmtiles.convert import mbtiles_to_pmtiles

            logger.info("✓ pmtiles Python library found")
        except ImportError:
            logger.error("pmtiles Python library not found")
            logger.info("Install: pip install pmtiles")
            return False

        return True

    def get_layers(self) -> List[str]:
        """
        Get list of all layers in the GeoPackage.

        Returns:
            List of layer names
        """
        layers = fiona.listlayers(str(self.input_gpkg))
        logger.info(f"Found {len(layers)} layers in {self.input_gpkg.name}")
        return layers

    def get_layer_info(self, layer_name: str) -> Dict:
        """
        Get information about a layer.

        Args:
            layer_name: Name of the layer

        Returns:
            Dict with layer metadata
        """
        with fiona.open(str(self.input_gpkg), layer=layer_name) as src:
            return {
                "name": layer_name,
                "count": len(src),
                "crs": src.crs,
                "schema": src.schema,
                "bounds": src.bounds,
            }

    def convert_to_geojsonseq(self, layers: List[str], output_dir: Path) -> List[Path]:
        """
        Convert GPKG layers to GeoJSON sequence files for tippecanoe.

        Args:
            layers: List of layer names to convert
            output_dir: Directory to write GeoJSON files

        Returns:
            List of paths to generated GeoJSON files
        """
        logger.info(f"\n=== Converting {len(layers)} layers to GeoJSON ===")
        geojson_files = []

        for idx, layer_name in enumerate(layers, 1):
            try:
                logger.info(f"  [{idx}/{len(layers)}] Converting {layer_name}...")

                # Read layer
                gdf = gpd.read_file(self.input_gpkg, layer=layer_name)

                if len(gdf) == 0:
                    logger.warning(f"    Skipping empty layer: {layer_name}")
                    continue

                # Ensure proper CRS (WGS84 for web tiles)
                if gdf.crs != "EPSG:4326":
                    gdf = gdf.to_crs("EPSG:4326")

                # Add layer name as attribute for tippecanoe
                gdf["layer"] = layer_name

                # Write as GeoJSON with explicit flush
                output_file = output_dir / f"{layer_name}.geojson"

                # Use fiona engine for more reliable writing
                import fiona

                gdf.to_file(output_file, driver="GeoJSON", engine="fiona")

                # Verify the file was written completely
                if not output_file.exists():
                    logger.error(f"    Failed to write {output_file.name}")
                    continue

                geojson_files.append(output_file)

                logger.info(f"    ✓ Wrote {len(gdf):,} features to {output_file.name}")

            except Exception as e:
                logger.error(f"    Failed to convert {layer_name}: {e}")
                continue

        return geojson_files

    def run_tippecanoe(self, geojson_files: List[Path], temp_mbtiles: Path) -> bool:
        """
        Run tippecanoe to create MBTiles from GeoJSON files.

        Args:
            geojson_files: List of GeoJSON files to process
            temp_mbtiles: Output MBTiles path

        Returns:
            bool: True if successful
        """
        logger.info(f"\n=== Running tippecanoe ===")

        # Build tippecanoe command
        cmd = [
            "tippecanoe",
            "-o",
            str(temp_mbtiles),
            "--force",  # Overwrite existing
            f"--minimum-zoom={self.min_zoom}",
            f"--maximum-zoom={self.max_zoom}",
            f"--base-zoom={self.base_zoom}",
            f"--simplification={self.simplification}",
            "--no-tiny-polygon-reduction",  # Prevent small polygons from disappearing
            "--drop-densest-as-needed",  # Drop features if tile is too large
            "--extend-zooms-if-still-dropping",  # Extend zoom if needed
            "--coalesce-densest-as-needed",  # Combine close features
            "--detect-shared-borders",  # Preserve shared polygon borders (critical for zone boundaries)
            "--preserve-input-order",  # Keep layer ordering
            "--read-parallel",  # Faster processing
            "--maximum-tile-bytes=5000000",  # Allow up to 5MB tiles (default is 500KB)
            "--maximum-tile-features=500000",  # Allow more features per tile
            "--layer=waterbodies",  # Single layer name for all features
            "--attribution=FWA BC, Province of British Columbia",
            "--name=BC Freshwater Atlas",
            "--description=BC Freshwater fishing regulations waterbodies by zone",
        ]

        # Add all GeoJSON files
        for geojson_file in geojson_files:
            cmd.append(str(geojson_file))

        logger.info(f"Command: {' '.join(cmd[:10])}... ({len(geojson_files)} files)")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout
            )

            if result.returncode == 0:
                logger.info("✓ Tippecanoe completed successfully")
                if result.stdout:
                    logger.debug(f"Output: {result.stdout}")
                return True
            else:
                logger.error(f"Tippecanoe failed with code {result.returncode}")
                if result.stderr:
                    logger.error(f"Error: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            logger.error("Tippecanoe timed out after 1 hour")
            return False
        except Exception as e:
            logger.error(f"Failed to run tippecanoe: {e}")
            return False

    def convert_to_pmtiles(self, mbtiles_path: Path) -> bool:
        """
        Convert MBTiles to PMTiles format using Python library.

        Args:
            mbtiles_path: Path to input MBTiles file

        Returns:
            bool: True if successful
        """
        logger.info(f"\n=== Converting to PMTiles ===")

        try:
            # Use the Python pmtiles library for conversion
            mbtiles_to_pmtiles(
                str(mbtiles_path), str(self.output_pmtiles), self.max_zoom
            )
            logger.info("✓ PMTiles conversion completed successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to convert to PMTiles: {e}")
            return False

    def run(self) -> bool:
        """
        Execute the full conversion pipeline.

        Returns:
            bool: True if successful
        """
        logger.info("=== GPKG to PMTiles Conversion ===")
        logger.info(f"Input:  {self.input_gpkg}")
        logger.info(f"Output: {self.output_pmtiles}")

        # Step 1: Check dependencies
        if not self.check_dependencies():
            return False

        # Step 2: Validate input
        if not self.input_gpkg.exists():
            logger.error(f"Input file not found: {self.input_gpkg}")
            return False

        # Step 3: Get layers
        try:
            layers = self.get_layers()
            if not layers:
                logger.error("No layers found in GeoPackage")
                return False
        except Exception as e:
            logger.error(f"Failed to read GeoPackage: {e}")
            return False

        # Step 4: Create temp directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            logger.info(f"\nUsing temp directory: {temp_path}")

            # Step 5: Convert to GeoJSON
            geojson_files = self.convert_to_geojsonseq(layers, temp_path)
            if not geojson_files:
                logger.error("No GeoJSON files created")
                return False

            # Step 6: Run tippecanoe
            temp_mbtiles = temp_path / "temp.mbtiles"
            if not self.run_tippecanoe(geojson_files, temp_mbtiles):
                return False

            # Check MBTiles was created
            if not temp_mbtiles.exists():
                logger.error("MBTiles file was not created")
                return False

            mbtiles_size_mb = temp_mbtiles.stat().st_size / (1024 * 1024)
            logger.info(f"MBTiles size: {mbtiles_size_mb:.1f} MB")

            # Step 7: Convert to PMTiles
            if not self.convert_to_pmtiles(temp_mbtiles):
                return False

        # Step 8: Verify output
        if not self.output_pmtiles.exists():
            logger.error("PMTiles file was not created")
            return False

        output_size_mb = self.output_pmtiles.stat().st_size / (1024 * 1024)
        logger.info(f"\n=== Conversion Complete ===")
        logger.info(f"Output: {self.output_pmtiles}")
        logger.info(f"Size: {output_size_mb:.1f} MB")
        logger.info(f"Zoom levels: {self.min_zoom} - {self.max_zoom}")

        return True


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Convert GeoPackage to PMTiles vector tiles"
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="Input GeoPackage file",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output PMTiles file",
    )
    parser.add_argument(
        "--max-zoom",
        type=int,
        default=14,
        help="Maximum zoom level (default: 14)",
    )
    parser.add_argument(
        "--min-zoom",
        type=int,
        default=4,
        help="Minimum zoom level (default: 4)",
    )
    parser.add_argument(
        "--base-zoom",
        type=int,
        default=12,
        help="Base zoom for detail level (default: 12)",
    )
    parser.add_argument(
        "--simplification",
        type=int,
        default=10,
        help="Simplification factor for tippecanoe (default: 10)",
    )

    args = parser.parse_args()

    # Set default paths
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent

    input_path = args.input or (
        script_dir.parent / "output" / "fwa_modules" / "waterbodies_by_zone.gpkg"
    )
    output_path = args.output or (
        script_dir.parent / "output" / "fwa_modules" / "waterbodies_bc.pmtiles"
    )

    # Create output directory if needed
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Run conversion
    converter = GpkgToPmtilesConverter(
        input_path,
        output_path,
        max_zoom=args.max_zoom,
        min_zoom=args.min_zoom,
        base_zoom=args.base_zoom,
        simplification=args.simplification,
    )

    success = converter.run()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
