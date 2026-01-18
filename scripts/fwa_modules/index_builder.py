"""
Index Builder - Searchable Waterbody Index from Metadata

Builds searchable JSON index from preprocessed metadata files (no GDB loading):
- stream_metadata.pickle - All stream/polygon metadata with zone assignments
- enriched_kml_points.json - KML labeled points matched to waterbodies

Index Structure:
    index[zone][normalized_name] = [list of features]

Features include:
- Streams (by GNIS_NAME and TRIBUTARY_OF)
- Lakes, wetlands, manmade waterbodies (by GNIS_NAME)
- Labeled points (linked to containing waterbodies)
"""

import json
import pickle
import logging
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def normalize_name(name: str) -> Optional[str]:
    """Normalize waterbody name for indexing.

    Args:
        name: Raw name string

    Returns:
        Normalized lowercase name, or None if invalid
    """
    if not name or str(name).lower() in ("none", "nan", ""):
        return None

    normalized = str(name).strip().lower()
    return normalized if normalized else None


class IndexBuilder:
    """Builds searchable waterbody index from metadata pickle files."""

    def __init__(
        self,
        metadata_path: Path,
        kml_points_path: Optional[Path],
        output_json: Path,
    ):
        """Initialize index builder.

        Args:
            metadata_path: Path to stream_metadata.pickle
            kml_points_path: Path to enriched_kml_points.json (optional)
            output_json: Path for output waterbody_index.json
        """
        self.metadata_path = metadata_path
        self.kml_points_path = kml_points_path
        self.output_json = output_json

        self.stats = {
            "total_features": 0,
            "unique_names": 0,
            "zones": 0,
        }

    def load_metadata(self) -> dict:
        """Load metadata from pickle file."""
        logger.info(f"Loading metadata from: {self.metadata_path}")

        with open(self.metadata_path, "rb") as f:
            metadata = pickle.load(f)

        logger.info(f"  Loaded metadata:")
        logger.info(f"    Streams: {len(metadata.get('streams', {})):,}")
        logger.info(f"    Lakes: {len(metadata.get('lakes', {})):,}")
        logger.info(f"    Wetlands: {len(metadata.get('wetlands', {})):,}")
        logger.info(f"    Manmade: {len(metadata.get('manmade', {})):,}")

        return metadata

    def load_kml_points(self) -> List[dict]:
        """Load KML enriched points from JSON file."""
        if not self.kml_points_path or not self.kml_points_path.exists():
            logger.info("No KML points file provided or file doesn't exist, skipping")
            return []

        logger.info(f"Loading KML points from: {self.kml_points_path}")

        with open(self.kml_points_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        points = data.get("points", [])
        logger.info(f"  Loaded {len(points)} KML points")

        return points

    def index_streams(self, streams_metadata: dict, index: dict) -> int:
        """Index stream features by zone and name.

        Args:
            streams_metadata: Dictionary of stream metadata
            index: Main index dictionary to populate

        Returns:
            Number of features indexed
        """
        logger.info("Indexing streams...")

        feature_count = 0

        for stream_id, metadata in streams_metadata.items():
            gnis_name = metadata.get("gnis_name", "")
            normalized = normalize_name(gnis_name)

            if not normalized:
                continue

            # Get zones for this stream
            zones = metadata.get("zones", [])

            for zone in zones:
                feature_data = {
                    "type": "stream",
                    "linear_feature_id": stream_id,
                    "gnis_name": gnis_name,
                    "stream_tributary_of": metadata.get("stream_tributary_of", ""),
                    "lake_tributary_of": metadata.get("lake_tributary_of", ""),
                    "stream_order": metadata.get("stream_order"),
                    "length": metadata.get("length", 0),
                    "waterbody_key": metadata.get("waterbody_key", ""),
                    "lake_name": metadata.get("lake_name", ""),
                    "zones": zones,
                    "mgmt_units": metadata.get("mgmt_units", []),
                    "cross_boundary": metadata.get("cross_boundary", False),
                }

                # Index by GNIS_NAME
                if normalized not in index[zone]:
                    index[zone][normalized] = []
                index[zone][normalized].append(feature_data)
                feature_count += 1

                # Also index by stream_tributary_of for tributary searches
                tributary_of = metadata.get("stream_tributary_of", "")
                if tributary_of:
                    tributary_normalized = normalize_name(tributary_of + " tributary")
                    if tributary_normalized:
                        if tributary_normalized not in index[zone]:
                            index[zone][tributary_normalized] = []
                        index[zone][tributary_normalized].append(feature_data)

        logger.info(f"  Indexed {feature_count:,} stream entries")
        return feature_count

    def index_polygons(
        self, polygon_metadata: dict, feature_type: str, index: dict
    ) -> int:
        """Index polygon features (lakes, wetlands, manmade) by zone and name.

        Args:
            polygon_metadata: Dictionary of polygon metadata
            feature_type: Type of feature ('lakes', 'wetlands', 'manmade')
            index: Main index dictionary to populate

        Returns:
            Number of features indexed
        """
        logger.info(f"Indexing {feature_type}...")

        feature_count = 0

        for waterbody_key, metadata in polygon_metadata.items():
            gnis_name = metadata.get("gnis_name", "")
            normalized = normalize_name(gnis_name)

            if not normalized:
                continue

            # Get zones for this feature
            zones = metadata.get("zones", [])

            for zone in zones:
                feature_data = {
                    "type": feature_type.rstrip("s"),  # 'lakes' -> 'lake'
                    "waterbody_key": waterbody_key,
                    "gnis_name": gnis_name,
                    "zones": zones,
                    "mgmt_units": metadata.get("mgmt_units", []),
                }

                if normalized not in index[zone]:
                    index[zone][normalized] = []
                index[zone][normalized].append(feature_data)
                feature_count += 1

        logger.info(f"  Indexed {feature_count:,} {feature_type} entries")
        return feature_count

    def index_kml_points(self, kml_points: List[dict], index: dict) -> int:
        """Index KML labeled points.

        Args:
            kml_points: List of KML point dictionaries
            index: Main index dictionary to populate

        Returns:
            Number of features indexed
        """
        logger.info("Indexing KML labeled points...")

        feature_count = 0

        for point in kml_points:
            name = point.get("name", "")
            normalized = normalize_name(name)

            if not normalized:
                continue

            # Extract waterbody keys
            lake_key = point.get("lake_waterbody_key")
            marsh_key = point.get("marsh_waterbody_key")
            manmade_key = point.get("manmade_waterbody_key")

            # Determine which zones this point belongs to
            # (We'll need to look up the waterbody in metadata to get zones)
            # For now, index in all zones - can be refined later

            point_data = {
                "type": "labeled_point",
                "name": name,
                "description": point.get("description", ""),
                "latitude": point.get("latitude"),
                "longitude": point.get("longitude"),
                "lake_waterbody_key": lake_key,
                "marsh_waterbody_key": marsh_key,
                "manmade_waterbody_key": manmade_key,
            }

            # Add to all zones (could be improved by looking up waterbody zones)
            for zone in index.keys():
                if normalized not in index[zone]:
                    index[zone][normalized] = []
                # Check if not already added
                if not any(
                    p.get("type") == "labeled_point" and p.get("name") == name
                    for p in index[zone][normalized]
                ):
                    index[zone][normalized].append(point_data)
                    feature_count += 1

        logger.info(f"  Indexed {feature_count:,} KML point entries")
        return feature_count

    def run(self) -> Path:
        """Build waterbody index from metadata.

        Returns:
            Path to output JSON file
        """
        logger.info("=== Building Waterbody Index from Metadata ===")

        if not self.metadata_path.exists():
            raise FileNotFoundError(f"Metadata file not found: {self.metadata_path}")

        # Load metadata
        metadata = self.load_metadata()
        kml_points = self.load_kml_points()

        # Structure: index[zone][normalized_name] = [list of features]
        index = defaultdict(lambda: defaultdict(list))

        # Initialize zones from metadata
        zone_metadata = metadata.get("zone_metadata", {})
        for zone_num in zone_metadata.keys():
            index[zone_num] = {}

        total_features = 0

        # Index streams
        total_features += self.index_streams(metadata.get("streams", {}), index)

        # Index polygons
        total_features += self.index_polygons(metadata.get("lakes", {}), "lakes", index)
        total_features += self.index_polygons(
            metadata.get("wetlands", {}), "wetlands", index
        )
        total_features += self.index_polygons(
            metadata.get("manmade", {}), "manmade", index
        )

        # Index KML points
        if kml_points:
            total_features += self.index_kml_points(kml_points, index)

        # Convert to regular dict
        output_index = {}
        for zone, names in index.items():
            output_index[zone] = dict(names)

        total_unique_names = sum(len(names) for names in index.values())

        self.stats["total_features"] = total_features
        self.stats["unique_names"] = total_unique_names
        self.stats["zones"] = len(output_index)

        logger.info(f"\nIndex Statistics:")
        logger.info(f"  Total feature entries indexed: {total_features:,}")
        logger.info(f"  Unique names: {total_unique_names:,}")
        logger.info(f"  Zones covered: {len(output_index)}")

        logger.info(f"\nWriting index to: {self.output_json}")
        self.output_json.parent.mkdir(parents=True, exist_ok=True)

        with open(self.output_json, "w", encoding="utf-8") as f:
            json.dump(output_index, f, ensure_ascii=False, indent=2)

        file_size_mb = self.output_json.stat().st_size / 1024 / 1024
        logger.info(f"Index saved successfully ({file_size_mb:.1f} MB)")

        return self.output_json


def main():
    """Main entry point for script execution."""
    import argparse
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

    parser = argparse.ArgumentParser(
        description="Build searchable waterbody index from metadata"
    )
    parser.add_argument(
        "--metadata-path", type=Path, help="Path to stream_metadata.pickle"
    )
    parser.add_argument(
        "--kml-points-path",
        type=Path,
        help="Path to enriched_kml_points.json (optional)",
    )
    parser.add_argument(
        "--output-path", type=Path, help="Path to output waterbody_index.json"
    )

    args = parser.parse_args()

    # Setup default paths
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent

    metadata_path = args.metadata_path or (
        script_dir.parent / "output" / "fwa_modules" / "stream_metadata.pickle"
    )
    kml_points_path = args.kml_points_path or (
        script_dir.parent / "output" / "fwa_modules" / "enriched_kml_points.json"
    )
    output_path = args.output_path or (
        script_dir.parent / "output" / "fwa_modules" / "waterbody_index.json"
    )

    # Validate input files
    if not metadata_path.exists():
        logger.error(f"Metadata file not found: {metadata_path}")
        logger.info("Please run metadata_builder.py first to create the metadata file.")
        return 1

    # Build index
    builder = IndexBuilder(metadata_path, kml_points_path, output_path)
    builder.run()

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
