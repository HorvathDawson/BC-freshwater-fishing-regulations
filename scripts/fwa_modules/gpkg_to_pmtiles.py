#!/usr/bin/env python3
"""
Stream Network to PMTiles Converter (Grouped/Whole-River Ranking)
"""

import argparse
import logging
import subprocess
import json
import pickle
import sys
from pathlib import Path
from typing import Dict, List
from datetime import datetime
import geopandas as gpd
import pandas as pd
import fiona

# ==========================================
# 1. SCORING CONFIGURATION
# ==========================================
WEIGHTS = {
    "order": 20.0,  # Applied to the MAX order in the group
    "magnitude": 1.0,  # Applied to the MAX magnitude in the group
    "length_km": 0.5,  # Applied to the TOTAL length of the group
    "has_name": 100.0,  # Applied if ANY segment in group has a name
}

# ==========================================
# 2. RANKING CONFIGURATION
# ==========================================
# Keys = The PMTiles Zoom Level (minzoom)
# Values = How many rivers (groups) are allowed to appear at this level or lower
STREAM_ZOOM_CAPS = {
    5: 60,  # Top 60 Major Rivers visible at z5
    6: 300,  # Top 300 visible at z6
    7: 1_500,
    8: 6_000,
    9: 25_000,
    10: 100_000,
    11: 300_000,
    # Everything else defaults to z12
}

LAKE_ZOOM_THRESHOLDS = {
    4: 100_000_000,
    5: 25_000_000,
    6: 5_000_000,
    7: 1_000_000,
    8: 250_000,
    9: 50_000,
    10: 10_000,
    11: 0,
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


class StreamConverter:
    def __init__(
        self,
        input_gpkg: Path,
        output_pmtiles: Path,
        metadata_path: Path,
        work_dir: Path,
    ):
        self.input_gpkg = input_gpkg
        self.output_pmtiles = output_pmtiles
        self.metadata_path = metadata_path
        self.work_dir = work_dir
        self.geojson_path = work_dir / "prepared_data.geojsonseq"

        self.work_dir.mkdir(parents=True, exist_ok=True)
        # Maps 'linear_feature_id' (str) -> minzoom (int)
        self.watershed_minzoom_map: Dict[str, int] = {}

    def _get_lake_minzoom(self, value: float) -> int:
        for zoom in sorted(LAKE_ZOOM_THRESHOLDS.keys()):
            if value >= LAKE_ZOOM_THRESHOLDS[zoom]:
                return zoom
        return 12

    def load_scores_from_metadata(self):
        if not self.metadata_path.exists():
            logger.error(f"Metadata file not found: {self.metadata_path}")
            sys.exit(1)

        logger.info(f"Loading metadata from {self.metadata_path.name}...")

        try:
            with open(self.metadata_path, "rb") as f:
                data = pickle.load(f)
                streams_data = data.get("streams", data)

            # ---------------------------------------------------------
            # GROUPING STAGE: Aggregate segments by Watershed Code
            # ---------------------------------------------------------
            logger.info("Grouping segments by FWA Watershed Code...")

            # Dictionary to hold aggregated stats for each unique watershed code
            # Key: watershed_code, Value: { stats... }
            grouped_watersheds = {}

            for lin_id, props in streams_data.items():
                # Try to find the grouping key.
                # Priority: fwa_watershed_code -> watershed_code -> fallback to ID
                w_code = str(
                    props.get("fwa_watershed_code")
                    or props.get("watershed_code")
                    or lin_id
                )

                if w_code not in grouped_watersheds:
                    grouped_watersheds[w_code] = {
                        "ids": [],  # List of linear_feature_ids in this river
                        "max_order": 0,
                        "max_mag": 0,
                        "total_len": 0.0,
                        "has_name": 0,
                    }

                group = grouped_watersheds[w_code]

                # Append the linear_feature_id to this group
                group["ids"].append(str(lin_id))

                # Aggregate Stats
                s_order = props.get("stream_order", 0) or 0
                s_mag = props.get("stream_magnitude", 0) or 0
                s_len = (props.get("length_metre", 0) or 0) / 1000.0
                s_name = 1 if props.get("gnis_name") else 0

                group["max_order"] = max(group["max_order"], s_order)
                group["max_mag"] = max(group["max_mag"], s_mag)
                group["total_len"] += s_len
                # If ANY segment in the river has a name, the whole river "has a name"
                if s_name > 0:
                    group["has_name"] = 1

            logger.info(
                f"Aggregated {len(streams_data)} segments into {len(grouped_watersheds)} unique rivers/groups."
            )

            # ---------------------------------------------------------
            # SCORING STAGE: Score the Groups
            # ---------------------------------------------------------
            scored_groups = []
            for w_code, stats in grouped_watersheds.items():
                score = (
                    (stats["max_order"] * WEIGHTS["order"])
                    + (stats["max_mag"] * WEIGHTS["magnitude"])
                    + (stats["total_len"] * WEIGHTS["length_km"])
                    + (stats["has_name"] * WEIGHTS["has_name"])
                )
                scored_groups.append(
                    {
                        "code": w_code,
                        "score": score,
                        "ids": stats[
                            "ids"
                        ],  # We need the list of IDs to apply the zoom later
                    }
                )

            logger.info("Sorting groups by importance...")
            scored_groups.sort(key=lambda x: x["score"], reverse=True)

            # ---------------------------------------------------------
            # ASSIGNMENT STAGE: Map IDs to Zoom
            # ---------------------------------------------------------
            self.watershed_minzoom_map = {}
            caps_sorted = sorted(STREAM_ZOOM_CAPS.items())

            for rank, group_item in enumerate(scored_groups):
                assigned_zoom = 12

                # Determine zoom based on the Rank of the Group
                for zoom_level, cap in caps_sorted:
                    if rank < cap:
                        assigned_zoom = zoom_level
                        break

                # Apply this zoom to ALL segments in this group
                for segment_id in group_item["ids"]:
                    self.watershed_minzoom_map[segment_id] = assigned_zoom

            logger.info("✓ Group-based zoom assignment complete.")

        except Exception as e:
            logger.error(f"Failed to process metadata: {e}")
            sys.exit(1)

    def print_stats(self):
        """Prints the zoom distribution histogram based on the MAP (ideal state)."""
        logger.info("\n" + "=" * 70)
        logger.info(" STREAM DISTRIBUTION ANALYSIS (Based on Grouped Ranks)")
        logger.info("=" * 70)

        if not self.watershed_minzoom_map:
            return

        # Count segments per zoom
        zoom_segment_counts = {z: 0 for z in range(4, 13)}
        for z in self.watershed_minzoom_map.values():
            if z in zoom_segment_counts:
                zoom_segment_counts[z] += 1

        # Count unique watersheds per zoom by grouping segment IDs
        # We need to reverse the logic: for each watershed code, find its assigned zoom
        zoom_watershed_sets = {z: set() for z in range(4, 13)}

        # Build a reverse map: segment_id -> watershed_code
        # We'll need to track which watershed each segment belongs to
        segment_to_watershed = {}

        # Re-process the metadata to get watershed groupings
        try:
            with open(self.metadata_path, "rb") as f:
                data = pickle.load(f)
                streams_data = data.get("streams", data)

            # Group segments by watershed code
            for lin_id, props in streams_data.items():
                w_code = str(
                    props.get("fwa_watershed_code")
                    or props.get("watershed_code")
                    or lin_id
                )
                segment_to_watershed[str(lin_id)] = w_code

            # Now count unique watersheds per zoom
            for segment_id, zoom in self.watershed_minzoom_map.items():
                if segment_id in segment_to_watershed:
                    watershed_code = segment_to_watershed[segment_id]
                    zoom_watershed_sets[zoom].add(watershed_code)

        except Exception as e:
            logger.warning(f"Could not calculate unique watersheds: {e}")

        # Print Unique Watersheds
        logger.info("\n📍 UNIQUE WATERSHED CODES PER ZOOM LEVEL")
        logger.info("-" * 70)

        total_watersheds = sum(len(s) for s in zoom_watershed_sets.values())
        watershed_cumulative = 0

        logger.info(
            f"{'Zoom':<5} | {'Count':<10} | {'%':<6} | {'Cum%':<6} | {'Distribution'}"
        )
        logger.info("-" * 70)

        for z in sorted(zoom_watershed_sets.keys()):
            count = len(zoom_watershed_sets[z])
            watershed_cumulative += count
            pct = (count / total_watersheds) * 100 if total_watersheds > 0 else 0
            cum_pct = (
                (watershed_cumulative / total_watersheds) * 100
                if total_watersheds > 0
                else 0
            )
            bar = "█" * int(pct / 2)
            logger.info(
                f"z{z:<4} | {count:<10,} | {pct:>5.1f}% | {cum_pct:>5.1f}% | {bar}"
            )

        logger.info("-" * 70)
        logger.info(
            f"{'TOTAL':<5} | {total_watersheds:<10,} | {'100.0%':<6} | {'100.0%':<6} |"
        )

        # Print Total Segments
        logger.info("\n📊 TOTAL SEGMENTS PER ZOOM LEVEL")
        logger.info("-" * 70)

        segment_cumulative = 0
        total_segments = sum(zoom_segment_counts.values())

        logger.info(
            f"{'Zoom':<5} | {'Count':<10} | {'%':<6} | {'Cum%':<6} | {'Distribution'}"
        )
        logger.info("-" * 70)

        for z in sorted(zoom_segment_counts.keys()):
            count = zoom_segment_counts[z]
            segment_cumulative += count
            pct = (count / total_segments) * 100 if total_segments > 0 else 0
            cum_pct = (
                (segment_cumulative / total_segments) * 100 if total_segments > 0 else 0
            )
            bar = "█" * int(pct / 2)
            logger.info(
                f"z{z:<4} | {count:<10,} | {pct:>5.1f}% | {cum_pct:>5.1f}% | {bar}"
            )

        logger.info("-" * 70)
        logger.info(
            f"{'TOTAL':<5} | {total_segments:<10,} | {'100.0%':<6} | {'100.0%':<6} |"
        )
        logger.info("=" * 70 + "\n")

    def prepare_geojson(self):
        """Writes GeoJSONSeq using linear_feature_id lookup."""
        logger.info("=== STAGE 1: Generating GeoJSONSeq ===")
        if self.geojson_path.exists():
            self.geojson_path.unlink()

        try:
            layers = fiona.listlayers(self.input_gpkg)
        except Exception as e:
            logger.error(f"Cannot open GPKG: {e}")
            sys.exit(1)

        count = 0
        # Initialize stats for written distribution
        written_stats = {z: 0 for z in range(0, 13)}

        for layer_name in layers:
            logger.info(f"Processing {layer_name}...")
            gdf = gpd.read_file(self.input_gpkg, layer=layer_name)
            if gdf.empty:
                continue

            if gdf.crs != "EPSG:4326":
                gdf = gdf.to_crs("EPSG:4326")

            # Column normalization (lowercase)
            gdf.columns = [c.lower() for c in gdf.columns]

            # --- STREAMS ---
            if "stream" in layer_name.lower():
                # CORRECTED LOOKUP: Use 'linear_feature_id' to match pickle keys
                if "linear_feature_id" in gdf.columns:
                    # Map strictly on string ID
                    gdf["tippecanoe:minzoom"] = (
                        gdf["linear_feature_id"]
                        .astype(str)
                        .map(self.watershed_minzoom_map)
                    )

                    # Log orphan count
                    missing = gdf["tippecanoe:minzoom"].isna().sum()
                    if missing > 0:
                        logger.warning(
                            f"  - {missing} streams had no rank. Defaulting to z12."
                        )

                    # Fill orphans with deepest zoom (z12)
                    gdf["tippecanoe:minzoom"] = (
                        gdf["tippecanoe:minzoom"].fillna(12).astype(int)
                    )

                else:
                    logger.error(
                        f"  ! 'linear_feature_id' column missing in {layer_name}. Defaulting to z12."
                    )
                    gdf["tippecanoe:minzoom"] = 12

            # --- LAKES / WETLANDS ---
            elif any(x in layer_name.lower() for x in ["lake", "wetland", "poly"]):
                if "area_sqm" in gdf.columns:
                    areas = gdf["area_sqm"]
                elif "area_ha" in gdf.columns:
                    areas = gdf["area_ha"] * 10000
                else:
                    areas = gdf.geometry.area * 1.2e10

                gdf["tippecanoe:minzoom"] = areas.apply(self._get_lake_minzoom).astype(
                    int
                )

            # --- OTHERS (Boundaries, etc) ---
            else:
                gdf["tippecanoe:minzoom"] = 0

            # Update verification stats
            if "tippecanoe:minzoom" in gdf.columns:
                vals = gdf["tippecanoe:minzoom"].value_counts()
                for z, c in vals.items():
                    written_stats[z] = written_stats.get(z, 0) + c

            # --- WRITE ---
            with open(self.geojson_path, "a") as f:
                for _, row in gdf.iterrows():
                    properties = {
                        k: v for k, v in row.drop("geometry").items() if pd.notnull(v)
                    }
                    # Use actual layer name so frontend can filter by it
                    properties["layer"] = layer_name

                    feat = {
                        "type": "Feature",
                        "properties": properties,
                        "geometry": row["geometry"].__geo_interface__,
                        "tippecanoe": {"minzoom": int(row["tippecanoe:minzoom"])},
                    }
                    f.write(json.dumps(feat) + "\n")
            count += len(gdf)

        # --- FINAL ACTUAL DISTRIBUTION LOG ---
        logger.info(f"Written {count} features.")
        logger.info("\n" + "=" * 60)
        logger.info(" FINAL WRITTEN DISTRIBUTION (Actual Output)")
        logger.info("=" * 60)

        total_written = sum(written_stats.values())
        for z in sorted(written_stats.keys()):
            count = written_stats[z]
            pct = (count / total_written) * 100 if total_written > 0 else 0
            bar = "█" * int(pct / 2)
            logger.info(f"z{z:<4} | {count:<10,} | {pct:>5.1f}% | {bar}")
        logger.info("=" * 60 + "\n")

    def generate_tiles(self):
        logger.info("=== STAGE 2: Tippecanoe ===")

        cmd = [
            "tippecanoe",
            "-o",
            str(self.output_pmtiles),
            "--force",
            "--hilbert",  # Put features in Hilbert Curve order instead of the usual Z-Order. This improves the odds that spatially adjacent features will be sequentially adjacent, and should improve density calculations and spatial coalescing. It should be the default eventually
            "--minimum-zoom=4",
            "--maximum-zoom=12",
            # "--read-parallel",
            # "--drop-smallest-as-needed",
            # "--coalesce-smallest-as-needed",
            # "-zg", "--extend-zooms-if-still-dropping",
            # "--detect-shared-borders", # DEPRECATED. In the manner of TopoJSON, detect borders that are shared between multiple polygons and simplify them identically in each polygon. This takes more time and memory than considering each polygon individually. Use no-simplification-of-shared-nodes instead, which is faster and more correct.
            "--no-simplification-of-shared-nodes",
            "--no-tiny-polygon-reduction",
            "--simplification=15",
            # "--low-detail=6",
            # "--full-detail=11",
            "--simplification-at-maximum-zoom=1",
            "--read-parallel",
            "--layer=waterbodies",
            str(self.geojson_path),
        ]

        logger.info(f"Running: {' '.join(cmd)}")

        # Stream output with progress on same line
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
        )

        last_line = ""
        for line in process.stdout:
            line = line.rstrip()
            # Progress lines and "Read X million features" - overwrite same line
            if (
                "%" in line
                or line.strip().startswith(
                    ("0.", "1.", "2.", "3.", "4.", "5.", "6.", "7.", "8.", "9.")
                )
                or line.startswith("Read ")
            ):
                print(f"\r{line}", end="", flush=True)
                last_line = line
            else:
                # Non-progress lines: print normally
                if last_line:
                    print()  # New line after progress
                    last_line = ""
                print(line)

        process.wait()

        if last_line:
            print()  # Final newline after progress

        if process.returncode == 0:
            logger.info(f"✓ SUCCESS! PMTiles created at: {self.output_pmtiles}")
        else:
            logger.error("Tippecanoe conversion failed.")
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, help="Input GeoPackage")
    parser.add_argument("--output", type=Path, help="Output PMTiles")
    parser.add_argument("--metadata", type=Path, help="Metadata pickle path")
    parser.add_argument("--dry-run", action="store_true", help="Stop after stats")
    parser.add_argument("--work-dir", type=Path, default=Path("./temp_work"))

    args = parser.parse_args()

    # Defaults
    script_dir = Path(__file__).parent
    default_metadata = (
        script_dir.parent / "output" / "fwa_modules" / "stream_metadata.pickle"
    )
    default_output = (
        script_dir.parent.parent
        / "map-webapp"
        / "public"
        / "data"
        / "waterbodies_bc.pmtiles"
    )
    default_input = (
        script_dir.parent / "output" / "fwa_modules" / "waterbodies_by_zone.gpkg"
    )

    metadata_path = args.metadata or default_metadata
    output_path = args.output or default_output
    input_path = args.input or default_input

    # Setup logging to both console and file
    log_dir = script_dir.parent / "output" / "fwa_modules"
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"gpkg_to_pmtiles_{timestamp}.log"

    # Add file handler to existing logger
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Log file: {log_file}")

    converter = StreamConverter(input_path, output_path, metadata_path, args.work_dir)
    converter.load_scores_from_metadata()
    converter.print_stats()

    if args.dry_run:
        return

    converter.prepare_geojson()
    converter.generate_tiles()


if __name__ == "__main__":
    main()
