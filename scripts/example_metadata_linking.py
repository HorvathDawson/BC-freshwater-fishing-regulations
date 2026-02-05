#!/usr/bin/env python3
"""
Example: Using MetadataGazetteer for Waterbody Linking

Demonstrates how to link regulation waterbody names to FWA features
using the pre-built stream_metadata.pickle file.
"""

from pathlib import Path
import sys

# Add scripts directory to path
scripts_dir = Path(__file__).parent
sys.path.insert(0, str(scripts_dir))

from fwa_modules.linking import MetadataGazetteer, WaterbodyLinker


def main():
    """Example usage of metadata gazetteer."""

    # Path to metadata file
    metadata_path = scripts_dir / "output" / "fwa_modules" / "stream_metadata.pickle"

    if not metadata_path.exists():
        print(f"ERROR: Metadata file not found: {metadata_path}")
        print("\nGenerate it first with:")
        print("  python fwa_modules/metadata_builder.py")
        return

    print("=" * 80)
    print("FWA Waterbody Linking Example")
    print("=" * 80)

    # Load gazetteer
    print(f"\n1. Loading metadata from: {metadata_path.name}")
    gazetteer = MetadataGazetteer(metadata_path)

    # Create linker
    linker = WaterbodyLinker(gazetteer)

    # Example 1: Link unique waterbody
    print("\n2. Example 1: Link 'Elk River' in Region 4")
    print("-" * 80)

    result = linker.link_waterbody("Elk River", region="Region 4")

    print(f"   Status: {result.status.value}")
    if result.is_success:
        print(f"   Matched: {result.matched_feature.name}")
        print(f"   FWA ID: {result.matched_feature.fwa_id}")
        print(f"   Type: {result.matched_feature.geometry_type}")
        print(f"   Region: {result.matched_feature.region}")

        # Get full metadata
        metadata = gazetteer.get_stream_metadata(result.matched_feature.fwa_id)
        if metadata:
            print(f"\n   Full Metadata:")
            print(f"     Zones: {metadata.get('zones', [])}")
            print(f"     Management Units: {metadata.get('mgmt_units', [])}")
            print(f"     Watershed Code: {metadata.get('fwa_watershed_code', 'N/A')}")
            print(f"     Stream Order: {metadata.get('stream_order', 'N/A')}")
            print(f"     Cross-boundary: {metadata.get('cross_boundary', False)}")

    # Example 2: Handle ambiguous waterbody
    print("\n3. Example 2: Link 'Mill Creek' (ambiguous)")
    print("-" * 80)

    result = linker.link_waterbody("Mill Creek")

    print(f"   Status: {result.status.value}")
    if result.status.value == "ambiguous":
        print(f"   Found {len(result.candidate_features)} matches:")
        for i, feature in enumerate(result.candidate_features, 1):
            print(f"     {i}. {feature.name} - {feature.region} (ID: {feature.fwa_id})")

    # Example 3: Resolve with region filter
    print("\n4. Example 3: Resolve ambiguity with region filter")
    print("-" * 80)

    result = linker.link_waterbody("Mill Creek", region="Region 4")

    print(f"   Status: {result.status.value}")
    if result.is_success:
        print(
            f"   Matched: {result.matched_feature.name} in {result.matched_feature.region}"
        )

    # Example 4: Not found
    print("\n5. Example 4: Waterbody not found")
    print("-" * 80)

    result = linker.link_waterbody("Nonexistent Creek", region="Region 1")

    print(f"   Status: {result.status.value}")
    if result.needs_manual_review:
        print(f"   → Requires manual review")

    # Example 5: Zone metadata
    print("\n6. Example 5: Get zone metadata")
    print("-" * 80)

    zone_meta = gazetteer.get_zone_metadata("4")
    if zone_meta:
        print(f"   Zone: {zone_meta['zone_number']}")
        print(f"   Management Units: {zone_meta['mgmt_units']}")
        print(f"   Total Units: {zone_meta['total_mgmt_units']}")

    # Example 6: Get all features in a zone
    print("\n7. Example 6: Get features in Zone 4")
    print("-" * 80)

    streams_in_zone = gazetteer.get_features_in_zone("4", feature_type="streams")

    # Get unique names (many segments may have same name)
    unique_names = set()
    for stream in streams_in_zone[:100]:  # Sample first 100
        name = stream.get("gnis_name", "")
        if name:
            unique_names.add(name)

    print(f"   Total streams in Zone 4: {len(streams_in_zone):,}")
    print(f"   Sample unique stream names (first 10):")
    for name in sorted(list(unique_names))[:10]:
        print(f"     - {name}")

    print("\n" + "=" * 80)
    print("Examples complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
