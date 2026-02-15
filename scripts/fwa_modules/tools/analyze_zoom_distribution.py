"""
Analyze stream zoom level distribution and scoring components.

Groups streams by Blue Line Key (matching geo_exporter merged mode) to show:
- How many Blue Line Key groups fall into each zoom level
- Total stream segments represented by each zoom level
- Component contribution breakdown (order, magnitude, length, has_name, side_channel)
- Statistics for each zoom bucket

Uses percentile-based zoom assignment:
- Adjust PERCENTILES dict to control distribution
- Each percentile defines the minimum rank for that zoom level
- Scores are calculated per Blue Line Key group (aggregated stats)
"""

import json
import pickle
import numpy as np
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Tuple

# Match the weights from geo_exporter.py
# Setting all to 0 except magnitude to see distribution based purely on magnitude
WEIGHTS = {
    "order": 0.0,
    "magnitude": 1.0,  # Only magnitude drives scoring
    "length_km": 0.0,
    "has_name": 0.0,
    "side_channel_penalty": 0.0,
}

# Define target percentiles for each zoom level
# Higher percentile = fewer streams (only the most important)
# Adjust these values to control how many streams appear at each zoom
# Matching geo_exporter.py percentiles
PERCENTILES = {
    5: 100.0,  # Top 1
    6: 99.999,  # Top 0.001% of streams start at zoom 6
    7: 99.0,  # Top 1% at zoom 7
    8: 95.0,  # Top 5% at zoom 8
    10: 0.0,  # All remaining streams at zoom 10
}

MAIN_FLOW_CODES = {1000, 1050, 1200, 1250, 1410, 1450}


def calculate_score(
    max_order: int = 0,
    magnitude: int = 0,
    total_length_km: float = 0.0,
    has_name: bool = False,
    is_side_channel: bool = False,
) -> Tuple[float, Dict[str, float]]:
    """Calculate score and return score + component breakdown."""

    # Ensure all values are numeric
    max_order = max_order or 0
    magnitude = magnitude or 0
    total_length_km = total_length_km or 0.0

    # Calculate component contributions
    components = {
        "order": max_order * WEIGHTS["order"],
        "magnitude": magnitude * WEIGHTS["magnitude"],
        "length_km": total_length_km * WEIGHTS["length_km"],
        "has_name": int(has_name) * WEIGHTS["has_name"],
        "side_channel": int(is_side_channel) * WEIGHTS["side_channel_penalty"],
    }

    total_score = sum(components.values())
    return total_score, components


def calculate_percentile_thresholds(all_scores: List[float]) -> List[Tuple[float, int]]:
    """Calculate score thresholds based on percentiles."""
    scores_array = np.array(all_scores)

    thresholds = []
    for zoom in sorted(PERCENTILES.keys()):
        threshold_score = np.percentile(scores_array, PERCENTILES[zoom])
        thresholds.append((threshold_score, zoom))

    return thresholds


def assign_zoom(score: float, thresholds: List[Tuple[float, int]]) -> int:
    """Assign zoom level based on score and percentile thresholds."""
    for threshold_score, zoom in thresholds:
        if score >= threshold_score:
            return zoom
    return 12


def analyze_streams(metadata_path: Path):
    """Analyze stream metadata grouped by Blue Line Key and show zoom distribution."""

    print("=" * 80)
    print("STREAM ZOOM LEVEL DISTRIBUTION ANALYSIS")
    print("Grouped by Blue Line Key (matches geo_exporter merged mode)")
    print("=" * 80)
    print()

    # Load metadata from either JSON or pickle format
    if metadata_path.suffix == ".pickle":
        print(f"Loading pickle file: {metadata_path}")
        with open(metadata_path, "rb") as f:
            metadata = pickle.load(f)
    else:
        print(f"Loading JSON file: {metadata_path}")
        with open(metadata_path, "r") as f:
            metadata = json.load(f)

    stream_metadata = metadata.get("streams", {})
    print(f"Total streams in metadata: {len(stream_metadata):,}")
    print()

    # Show sample metadata to verify available fields
    print("Sample metadata (first stream):")
    print("-" * 80)
    if stream_metadata:
        first_id = next(iter(stream_metadata))
        first_meta = stream_metadata[first_id]
        print(f"Stream ID: {first_id}")
        print(f"Available fields: {list(first_meta.keys())}")
        print(f"  stream_order: {first_meta.get('stream_order')}")
        print(f"  stream_magnitude: {first_meta.get('stream_magnitude')}")
        print(f"  length: {first_meta.get('length')}")
        print(f"  gnis_name: {first_meta.get('gnis_name')}")
        print(f"  edge_type: {first_meta.get('edge_type')}")
    print()

    # Check magnitude distribution
    magnitude_counts = Counter()
    sample_size = min(100000, len(stream_metadata))
    print(f"Checking magnitude values (sampling {sample_size:,} streams)...")
    for i, meta in enumerate(stream_metadata.values()):
        if i >= sample_size:
            break
        mag = meta.get("stream_magnitude", 0) or 0
        magnitude_counts[mag] += 1

    print(f"Magnitude value distribution (top 10):")
    for mag, count in magnitude_counts.most_common(10):
        print(f"  Magnitude {mag}: {count:,} streams")
    print()

    # STEP 1: Group streams by Blue Line Key and aggregate stats (matches geo_exporter)
    print("Step 1: Grouping streams by Blue Line Key and aggregating stats...")
    blk_stats = defaultdict(
        lambda: {
            "len": 0,
            "max_order": 0,
            "max_magnitude": 0,
            "has_name": False,
            "is_side_channel": False,
            "segment_count": 0,
        }
    )

    # Aggregate all segments by Blue Line Key
    for linear_id, meta in stream_metadata.items():
        blk = meta.get("blue_line_key")
        if not blk:
            continue

        s = blk_stats[blk]
        s["len"] += meta.get("length", 0) or 0
        s["max_order"] = max(s["max_order"], meta.get("stream_order", 0) or 0)
        s["max_magnitude"] = max(
            s["max_magnitude"], meta.get("stream_magnitude", 0) or 0
        )
        s["segment_count"] += 1

        if meta.get("gnis_name"):
            s["has_name"] = True

        edge_type = meta.get("edge_type")
        if edge_type is not None and edge_type not in MAIN_FLOW_CODES:
            s["is_side_channel"] = True

    print(
        f"  Aggregated {len(stream_metadata):,} stream segments into {len(blk_stats):,} Blue Line Key groups"
    )
    print()

    # STEP 2: Calculate scores for each Blue Line Key group
    print("Step 2: Calculating scores for Blue Line Key groups...")
    all_scores = []
    blk_data = []  # Store (blk, stats, score, components) for later

    for blk, stats in blk_stats.items():
        score, components = calculate_score(
            max_order=stats["max_order"],
            magnitude=stats["max_magnitude"],
            total_length_km=stats["len"] / 1000.0,
            has_name=stats["has_name"],
            is_side_channel=stats["is_side_channel"],
        )
        all_scores.append(score)
        blk_data.append((blk, stats, score, components))

    # STEP 3: Calculate percentile-based thresholds
    print("Step 3: Calculating percentile thresholds...")
    thresholds = calculate_percentile_thresholds(all_scores)
    print()

    # STEP 4: Assign zoom levels and analyze distribution
    print("Step 4: Assigning zoom levels to Blue Line Key groups...")
    zoom_distribution = Counter()
    zoom_details = defaultdict(
        lambda: {
            "count": 0,
            "total_segments": 0,
            "avg_score": 0,
            "avg_order": 0,
            "avg_magnitude": 0,
            "avg_length_km": 0,
            "named_count": 0,
            "side_channel_count": 0,
            "scores": [],
            "component_sums": {
                "order": 0,
                "magnitude": 0,
                "length_km": 0,
                "has_name": 0,
                "side_channel": 0,
            },
        }
    )

    for blk, stats, score, components in blk_data:
        zoom = assign_zoom(score, thresholds)

        zoom_distribution[zoom] += 1
        details = zoom_details[zoom]
        details["count"] += 1
        details["total_segments"] += stats["segment_count"]
        details["scores"].append(score)
        details["avg_order"] += stats["max_order"]
        details["avg_magnitude"] += stats["max_magnitude"]
        details["avg_length_km"] += stats["len"] / 1000.0
        if stats["has_name"]:
            details["named_count"] += 1
        if stats["is_side_channel"]:
            details["side_channel_count"] += 1

        for component, value in components.items():
            details["component_sums"][component] += value

    # Calculate averages
    for zoom, details in zoom_details.items():
        count = details["count"]
        if count > 0:
            details["avg_score"] = sum(details["scores"]) / count
            details["avg_order"] /= count
            details["avg_magnitude"] /= count
            details["avg_length_km"] /= count
            for component in details["component_sums"]:
                details["component_sums"][component] /= count

    # Print threshold reference
    print()
    print("PERCENTILE-BASED THRESHOLDS:")
    print("-" * 80)
    for threshold_score, zoom in thresholds:
        percentile = PERCENTILES[zoom]
        coverage = 100 - percentile
        print(
            f"  Zoom {zoom:2d}: Score >= {threshold_score:7.2f} (top {coverage:5.2f}% at percentile {percentile})"
        )
    print(f"  Zoom 12: Below minimum threshold (fallback)")
    print()

    # Print distribution
    print("ZOOM LEVEL DISTRIBUTION (Blue Line Key Groups):")
    print("-" * 80)
    total_blk_groups = sum(zoom_distribution.values())
    total_segments = sum(details["total_segments"] for details in zoom_details.values())

    for zoom in sorted(zoom_distribution.keys()):
        count = zoom_distribution[zoom]
        segment_count = zoom_details[zoom]["total_segments"]
        percentage = (count / total_blk_groups * 100) if total_blk_groups > 0 else 0
        bar_length = int(percentage / 2)  # Scale to 50 chars max
        bar = "█" * bar_length

        print(
            f"Zoom {zoom:2d}: {count:8,} BLK groups ({percentage:5.1f}%), {segment_count:,} segments  {bar}"
        )

    print()
    print(
        f"Total: {total_blk_groups:,} Blue Line Key groups representing {total_segments:,} stream segments"
    )
    print()

    # Print detailed breakdown
    print("DETAILED BREAKDOWN BY ZOOM LEVEL:")
    print("=" * 80)

    for zoom in sorted(zoom_details.keys()):
        details = zoom_details[zoom]
        count = details["count"]
        segment_count = details["total_segments"]

        print(f"\n{'─' * 80}")
        print(
            f"ZOOM {zoom} - {count:,} Blue Line Key groups ({segment_count:,} segments)"
        )
        print(f"{'─' * 80}")
        print(f"  Avg Score:      {details['avg_score']:8.2f}")
        print(f"  Avg Order:      {details['avg_order']:8.2f}")
        print(f"  Avg Magnitude:  {details['avg_magnitude']:8.2f}")
        print(f"  Avg Length (km):{details['avg_length_km']:8.2f}")
        print(
            f"  Named BLK Groups: {details['named_count']:8,} ({details['named_count']/count*100:5.1f}%)"
        )
        print(
            f"  Side Channel BLKs: {details['side_channel_count']:8,} ({details['side_channel_count']/count*100:5.1f}%)"
        )
        print()
        print("  Component Contributions (average per BLK group):")
        for component, avg_value in details["component_sums"].items():
            print(f"    {component:18s}: {avg_value:8.2f}")

        # Show score range
        scores = details["scores"]
        print(f"\n  Score Range: {min(scores):.2f} - {max(scores):.2f}")

    print("\n" + "=" * 80)
    print("CONFIGURATION:")
    print("-" * 80)
    print("\nWeights (for scoring):")
    for key, value in WEIGHTS.items():
        print(f"  {key:20s}: {value:8.2f}")
    print("\nPercentiles (for zoom assignment):")
    for zoom in sorted(PERCENTILES.keys()):
        coverage = 100 - PERCENTILES[zoom]
        print(
            f"  Zoom {zoom:2d}: {PERCENTILES[zoom]:6.2f}th percentile (top {coverage:5.2f}%)"
        )
    print("=" * 80)
    print("\nNOTE: This analysis groups streams by Blue Line Key before scoring,")
    print("matching the behavior of geo_exporter with merge_geometries=True.")
    print("\nTo adjust distribution, edit PERCENTILES dict in this file.")
    print("Higher percentile = fewer Blue Line Key groups at that zoom level.")


if __name__ == "__main__":
    import sys

    # Updated paths for tools/ directory location
    possible_paths = [
        Path(__file__).parent.parent.parent
        / "output"
        / "fwa_modules"
        / "stream_metadata.pickle",
        Path(__file__).parent.parent.parent
        / "output"
        / "fwa_modules"
        / "metadata_gazetteer.json",
    ]

    # Check if path provided as argument
    if len(sys.argv) > 1:
        metadata_path = Path(sys.argv[1])
    else:
        # Try to find the file automatically
        metadata_path = None
        for path in possible_paths:
            if path.exists():
                metadata_path = path
                break

    if not metadata_path or not metadata_path.exists():
        print("=" * 80)
        print("ERROR: Metadata file not found!")
        print("=" * 80)
        print("\nSearched in:")
        for path in possible_paths:
            print(f"  - {path}")
        print("\nUsage:")
        print(f"  python -m fwa_modules.tools.analyze_zoom_distribution")
        print(
            f"  python -m fwa_modules.tools.analyze_zoom_distribution <path_to_metadata_file>"
        )
        print("\nThe metadata file is generated by the regulation pipeline.")
        print("Look for either:")
        print("  - stream_metadata.pickle (recommended)")
        print("  - metadata_gazetteer.json")
        print("\nOr generate it by running the full regulation linking pipeline.")
        exit(1)

    print(f"Loading metadata from: {metadata_path}")
    print()

    analyze_streams(metadata_path)
