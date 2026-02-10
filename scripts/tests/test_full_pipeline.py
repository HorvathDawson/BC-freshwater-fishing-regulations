#!/usr/bin/env python
"""
Test the full RegulationMapper pipeline with merge and export functionality.

Tests the complete flow: Link → Scope → Enrich → Map → Merge → Export
"""

import sys
import json
from pathlib import Path

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from fwa_modules.linking import (
    WaterbodyLinker,
    RegulationMapper,
    ScopeFilter,
    TributaryEnricher,
    MetadataGazetteer,
)


def main():
    print("=" * 80)
    print("REGULATION MAPPER - FULL PIPELINE TEST")
    print("=" * 80)

    # Setup paths
    script_dir = Path(__file__).parent
    output_dir = script_dir / "output"
    gazetteer_path = output_dir / "fwa_modules" / "enriched_kml_points.json"
    parsed_regs_path = output_dir / "parse_synopsis" / "parsed_results.json"
    graph_path = output_dir / "fwa_modules" / "fwa_bc_primal_full.gpickle"

    # Output directory for indices
    index_output_dir = output_dir / "regulation_indices"

    print(f"\nLoading gazetteer from: {gazetteer_path}")
    gazetteer = MetadataGazetteer(gazetteer_path)
    print(
        f"Loaded: {len(gazetteer.streams)} streams, {len(gazetteer.lakes)} lakes, "
        f"{len(gazetteer.wetlands)} wetlands, {len(gazetteer.manmade)} manmade"
    )

    print(f"\nLoading parsed regulations from: {parsed_regs_path}")
    with open(parsed_regs_path, "r") as f:
        parsed_data = json.load(f)
    print(f"Loaded {len(parsed_data)} waterbodies")

    print("\nInitializing pipeline components...")
    linker = WaterbodyLinker(gazetteer)
    scope_filter = ScopeFilter()
    tributary_enricher = TributaryEnricher(graph_path)

    mapper = RegulationMapper(linker, scope_filter, tributary_enricher)

    print("\n" + "=" * 80)
    print("RUNNING FULL PIPELINE")
    print("=" * 80)

    # Run full pipeline with export
    result = mapper.process_and_export(parsed_data, output_dir=index_output_dir)

    # Display results
    stats = result.stats
    feature_to_regs = result.feature_to_regs
    merged_groups = result.merged_groups
    files = result.exported_files or {}

    print("\n" + "=" * 80)
    print("STATISTICS")
    print("=" * 80)

    print(f"\nRegulations:")
    print(f"  Total                : {stats.total_regulations:,}")
    print(
        f"  Linked               : {stats.linked_regulations:,} ({stats.linked_regulations/stats.total_regulations*100:.1f}%)"
    )
    print(
        f"  Failed to link       : {stats.failed_to_link_regulations:,} ({stats.failed_to_link_regulations/stats.total_regulations*100:.1f}%)"
    )

    print(f"\nRules:")
    print(f"  Total rules processed: {stats.total_rules_processed:,}")

    print(f"\nFeature Mapping:")
    print(f"  Unique features      : {stats.unique_features_with_rules:,}")
    print(f"  Rule→Feature mappings: {stats.total_rule_to_feature_mappings:,}")

    print(f"\nMerging:")
    print(f"  Merged groups        : {len(merged_groups):,}")
    print(
        f"  Reduction ratio      : {stats.unique_features_with_rules/len(merged_groups):.1f}x"
    )

    # Show top merged groups
    print(f"\nTop 5 Largest Groups:")
    sorted_groups = sorted(
        merged_groups.items(), key=lambda x: x[1].feature_count, reverse=True
    )
    for group_id, group in sorted_groups[:5]:
        print(
            f"  {group_id}: {group.feature_count:,} features, "
            f"{len(group.regulation_ids)} regulations, "
            f"{group.gnis_name or group.watershed_code or 'unnamed'}"
        )

    # Show link status breakdown
    print(f"\nLink Status Breakdown:")
    for status, count in sorted(
        stats.link_status_counts.items(), key=lambda x: x[1], reverse=True
    ):
        pct = count / stats.total_regulations * 100
        print(f"  {status:<20} : {count:5,} ({pct:5.1f}%)")

    # Show exported files
    if files:
        print(f"\nExported Files:")
        for name, path in files.items():
            size_mb = path.stat().st_size / (1024 * 1024)
            print(f"  {name:<20} : {path}")
            print(f"    Size: {size_mb:.2f} MB")

    print("\n" + "=" * 80)
    print("TEST COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
