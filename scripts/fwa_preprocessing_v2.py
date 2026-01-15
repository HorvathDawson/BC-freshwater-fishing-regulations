#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BC FWA Processing - Memory-Efficient Implementation

Complete rewrite focused on:
1. Memory efficiency - Never load all data at once
2. Correctness - Accurate tributary relationships via network analysis
3. Performance - Vectorized operations and spatial indexes

Processing Pipeline:
  Phase 1: Stream Preprocessing
    - Remove unnamed streams >1 level from named
    - Merge braided streams with same watershed code

  Phase 2: KML Point Enrichment
    - Link points to containing waterbody polygons
    - Generate warning log for unmatched points

  Phase 3: Network Analysis
    - Build tributary relationships via watershed hierarchy
    - Correct for lake tributaries
    - Mark lake segments (streams 50%+ in lakes)

  Phase 4: Zone Splitting
    - Split all features by wildlife management zones
    - Add tributary data to streams
    - Write incrementally to avoid memory crashes
"""
import os
import sys
import gc
import time
import argparse
import warnings
from pathlib import Path

# Fix for GDAL header.dxf warning
os.environ["GDAL_SKIP"] = "DXF"

# Set GDAL_DATA if needed
if "GDAL_DATA" not in os.environ:
    candidates = [
        os.path.join(sys.prefix, "share", "gdal"),
        os.path.join(sys.prefix, "Library", "share", "gdal"),
    ]
    for c in candidates:
        if os.path.exists(c):
            os.environ["GDAL_DATA"] = c
            break

# Enable KML driver
import fiona

fiona.drvsupport.supported_drivers["KML"] = "rw"
fiona.drvsupport.supported_drivers["LIBKML"] = "rw"

warnings.filterwarnings("ignore")

# Import processing modules
from fwa_modules.stream_preprocessing import StreamPreprocessor
from fwa_modules.kml_enrichment import KMLEnricher
from fwa_modules.network_analysis import NetworkAnalyzer
from fwa_modules.zone_splitting import ZoneSplitter
from fwa_modules.index_builder import IndexBuilder
from fwa_modules.utils import setup_logging

logger = setup_logging("fwa_preprocessing")


class FWAProcessor:
    """Main orchestrator for BC FWA processing pipeline."""

    def __init__(
        self,
        streams_gdb: Path,
        lakes_gdb: Path,
        wildlife_gpkg: Path,
        kml_path: Path,
        output_dir: Path,
        test_mode: bool = False,
    ):
        """Initialize processor with data paths.

        Args:
            streams_gdb: Path to FWA_STREAM_NETWORKS_SP.gdb
            lakes_gdb: Path to FWA_BC.gdb
            wildlife_gpkg: Path to wildlife management units
            kml_path: Path to user-labeled KML points
            output_dir: Directory for all outputs
            test_mode: If True, only process 5 stream layers
        """
        self.streams_gdb = streams_gdb
        self.lakes_gdb = lakes_gdb
        self.wildlife_gpkg = wildlife_gpkg
        self.kml_path = kml_path
        self.output_dir = output_dir
        self.test_mode = test_mode

        # Define output paths
        self.temp_dir = output_dir / "temp"
        self.cleaned_streams_gdb = self.temp_dir / "cleaned_streams.gdb"
        self.enriched_points_gpkg = self.temp_dir / "enriched_points.gpkg"
        self.kml_warning_log = self.temp_dir / "kml_warning.log"
        self.tributary_map_json = self.temp_dir / "tributary_map.json"
        self.lake_segments_json = self.temp_dir / "lake_segments.json"
        self.network_graph_graphml = self.temp_dir / "stream_network.graphml"
        self.final_gdb = output_dir / "FWA_Zone_Grouped.gdb"
        self.waterbody_index_json = output_dir / "waterbody_index.json"

    def run_phase1_stream_preprocessing(self) -> bool:
        """Phase 1: Preprocess streams.

        Returns:
            True if successful, False otherwise
        """
        logger.info("\n" + "=" * 80)
        logger.info("PHASE 1: STREAM PREPROCESSING")
        logger.info("=" * 80)

        try:
            preprocessor = StreamPreprocessor(
                self.streams_gdb, self.cleaned_streams_gdb, test_mode=self.test_mode
            )

            preprocessor.run()

            # Force garbage collection
            gc.collect()

            return True

        except Exception as e:
            logger.error(f"Phase 1 failed: {e}", exc_info=True)
            return False

    def run_phase2_kml_enrichment(self) -> bool:
        """Phase 2: Enrich KML points.

        Returns:
            True if successful, False otherwise
        """
        logger.info("\n" + "=" * 80)
        logger.info("PHASE 2: KML POINT ENRICHMENT")
        logger.info("=" * 80)

        try:
            enricher = KMLEnricher(
                self.kml_path,
                self.lakes_gdb,
                self.enriched_points_gpkg,
                self.kml_warning_log,
            )

            enricher.run()

            gc.collect()

            return True

        except Exception as e:
            logger.error(f"Phase 2 failed: {e}", exc_info=True)
            return False

    def run_phase3_network_analysis(self) -> bool:
        """Phase 3: Analyze network for tributary relationships.

        Returns:
            True if successful, False otherwise
        """
        logger.info("\n" + "=" * 80)
        logger.info("PHASE 3: NETWORK ANALYSIS")
        logger.info("=" * 80)

        try:
            analyzer = NetworkAnalyzer(
                self.cleaned_streams_gdb,
                self.lakes_gdb,
                self.tributary_map_json,
                self.lake_segments_json,
                self.network_graph_graphml,
            )

            analyzer.run()

            gc.collect()

            return True

        except Exception as e:
            logger.error(f"Phase 3 failed: {e}", exc_info=True)
            return False

    def run_phase4_zone_splitting(self) -> bool:
        """Phase 4: Split all features by zone.

        Returns:
            True if successful, False otherwise
        """
        logger.info("\n" + "=" * 80)
        logger.info("PHASE 4: ZONE SPLITTING")
        logger.info("=" * 80)

        try:
            splitter = ZoneSplitter(
                self.cleaned_streams_gdb,
                self.lakes_gdb,
                self.enriched_points_gpkg,
                self.wildlife_gpkg,
                self.tributary_map_json,
                self.lake_segments_json,
                self.final_gdb,
            )

            splitter.run()

            gc.collect()

            return True

        except Exception as e:
            logger.error(f"Phase 4 failed: {e}", exc_info=True)
            return False

    def run_phase5_index_building(self, n_cores: int = 4) -> bool:
        """Phase 5: Build waterbody index.

        Args:
            n_cores: Number of CPU cores for parallel processing

        Returns:
            True if successful, False otherwise
        """
        logger.info("\n" + "=" * 80)
        logger.info("PHASE 5: INDEX BUILDING")
        logger.info("=" * 80)

        try:
            builder = IndexBuilder(
                self.final_gdb, self.waterbody_index_json, n_cores=n_cores
            )

            builder.run()

            gc.collect()

            return True

        except Exception as e:
            logger.error(f"Phase 5 failed: {e}", exc_info=True)
            return False

    def run(self, skip_phases: set = None, build_index: bool = False, n_cores: int = 4):
        """Execute full processing pipeline.

        Args:
            skip_phases: Set of phase numbers to skip (1-4)
        """
        skip_phases = skip_phases or set()
        start_time = time.time()

        logger.info("\n" + "=" * 80)
        logger.info("BC FWA PROCESSING - MEMORY-EFFICIENT IMPLEMENTATION")
        logger.info("=" * 80)
        logger.info(f"Test Mode: {self.test_mode}")
        logger.info(f"Output Directory: {self.output_dir}")
        logger.info(
            f"Skipping Phases: {sorted(skip_phases) if skip_phases else 'None'}"
        )
        logger.info("")

        # Create output directories
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        # Phase 1: Stream Preprocessing
        if 1 not in skip_phases:
            if not self.run_phase1_stream_preprocessing():
                logger.error("Pipeline aborted due to Phase 1 failure")
                return False
        else:
            logger.info("Skipping Phase 1: Stream Preprocessing")

        # Phase 2: KML Enrichment
        if 2 not in skip_phases:
            if not self.run_phase2_kml_enrichment():
                logger.error("Pipeline aborted due to Phase 2 failure")
                return False
        else:
            logger.info("Skipping Phase 2: KML Enrichment")

        # Phase 3: Network Analysis
        if 3 not in skip_phases:
            if not self.run_phase3_network_analysis():
                logger.error("Pipeline aborted due to Phase 3 failure")
                return False
        else:
            logger.info("Skipping Phase 3: Network Analysis")

        # Phase 4: Zone Splitting
        if 4 not in skip_phases:
            if not self.run_phase4_zone_splitting():
                logger.error("Pipeline aborted due to Phase 4 failure")
                return False
        else:
            logger.info("Skipping Phase 4: Zone Splitting")

        # Phase 5: Index Building (optional)
        if build_index:
            if not self.run_phase5_index_building(n_cores=n_cores):
                logger.error("Pipeline aborted due to Phase 5 failure")
                return False
        else:
            logger.info(
                "Skipping Phase 5: Index Building (use --build-index to enable)"
            )

        # Final summary
        elapsed = time.time() - start_time
        logger.info("\n" + "=" * 80)
        logger.info("PROCESSING COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Total Time: {elapsed / 60:.2f} minutes")
        logger.info(f"Output GDB: {self.final_gdb}")
        if build_index:
            logger.info(f"Index JSON: {self.waterbody_index_json}")
        logger.info(f"Temp Files: {self.temp_dir}")
        logger.info("=" * 80)

        return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="BC FWA Processing - Memory-Efficient Implementation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full processing with index building
  python fwa_preprocessing_v2.py --build-index
  
  # Test mode (5 layers only)
  python fwa_preprocessing_v2.py --test-mode
  
  # Build index only from existing GDB
  python fwa_preprocessing_v2.py --build-index --skip-phase 1 --skip-phase 2 --skip-phase 3 --skip-phase 4
  
  # Skip Phase 1 (use existing cleaned streams)
  python fwa_preprocessing_v2.py --skip-phase 1
  
  # Only run Phase 3 and 4
  python fwa_preprocessing_v2.py --skip-phase 1 --skip-phase 2
  
  # Resume from Phase 4 (zone splitting)
  python fwa_preprocessing_v2.py --skip-phase 1 --skip-phase 2 --skip-phase 3
        """,
    )

    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Test mode: process only 5 stream layers",
    )

    parser.add_argument(
        "--skip-phase",
        type=int,
        action="append",
        choices=[1, 2, 3, 4],
        help="Skip specific phases (can be used multiple times)",
    )

    parser.add_argument(
        "--build-index",
        action="store_true",
        help="Build waterbody index for web application (Phase 5)",
    )

    parser.add_argument(
        "--cores",
        type=int,
        default=4,
        help="Number of CPU cores for index building (default: 4)",
    )

    args = parser.parse_args()

    # Determine paths
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    base_data = (
        project_root
        / "data"
        / "ftp.geobc.gov.bc.ca"
        / "sections"
        / "outgoing"
        / "bmgs"
        / "FWA_Public"
    )

    streams_gdb = base_data / "FWA_STREAM_NETWORKS_SP" / "FWA_STREAM_NETWORKS_SP.gdb"
    lakes_gdb = base_data / "FWA_BC" / "FWA_BC.gdb"
    wildlife_gpkg = base_data / "WAA_WILDLIFE_MGMT_UNITS_SVW.gpkg"
    kml_path = project_root / "data" / "labelled" / "unnamed_lakes.kml"
    output_dir = script_dir / "output" / "fwa_preprocessing"

    # Validate inputs
    if not streams_gdb.exists() and (
        args.skip_phase is None or 1 not in args.skip_phase
    ):
        logger.error(f"Streams GDB not found: {streams_gdb}")
        return 1

    if not lakes_gdb.exists():
        logger.error(f"Lakes GDB not found: {lakes_gdb}")
        return 1

    if not wildlife_gpkg.exists():
        logger.error(f"Wildlife zones not found: {wildlife_gpkg}")
        return 1

    # Create processor
    processor = FWAProcessor(
        streams_gdb,
        lakes_gdb,
        wildlife_gpkg,
        kml_path,
        output_dir,
        test_mode=args.test_mode,
    )

    # Run pipeline
    skip_phases = set(args.skip_phase) if args.skip_phase else set()
    success = processor.run(
        skip_phases=skip_phases, build_index=args.build_index, n_cores=args.cores
    )

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
