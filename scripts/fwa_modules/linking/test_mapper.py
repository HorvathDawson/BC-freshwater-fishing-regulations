#!/usr/bin/env python3
"""
Test Regulation Mapper

Tests the regulation mapping pipeline against parsed regulations.
Shows statistics about linking, scope filtering, and feature mapping.
"""

import json
import argparse
import shutil
import sys
from pathlib import Path
from collections import Counter
from typing import Dict, List

from .linker import WaterbodyLinker, LinkStatus
from .metadata_gazetteer import MetadataGazetteer
from .name_variations import (
    NAME_VARIATIONS,
    DIRECT_MATCHES,
    SKIP_ENTRIES,
    UNMARKED_WATERBODIES,
    ManualCorrections,
)
from .regulation_mapper import RegulationMapper
from .scope_filter import ScopeFilter
from .tributary_enricher import TributaryEnricher


# --- Dynamic Formatting Helpers ---


def get_terminal_width(default=80):
    """Get current terminal width."""
    try:
        return shutil.get_terminal_size((default, 20)).columns
    except Exception:
        return default


def print_divider(char="="):
    print(char * get_terminal_width())


def print_header(text):
    print()
    print_divider("=")
    print(text)
    print_divider("=")
    print()


def print_sub_header(text):
    print(f"\n{text}")
    print("-" * get_terminal_width())


# ANSI Colors
RED = "\033[91m"
YELLOW = "\033[93m"
GREEN = "\033[92m"
BLUE = "\033[94m"
RESET = "\033[0m"


def format_percentage(count, total):
    """Format a percentage with color coding."""
    if total == 0:
        return "N/A"
    pct = (count / total) * 100
    if pct >= 90:
        color = GREEN
    elif pct >= 70:
        color = YELLOW
    else:
        color = RED
    return f"{color}{pct:.1f}%{RESET}"


def main():
    parser = argparse.ArgumentParser(description="Test regulation mapping pipeline")
    parser.add_argument(
        "--parsed-regs",
        type=Path,
        default=Path("output/parse_synopsis/parsed_results.json"),
        help="Path to parsed regulations JSON file",
    )
    parser.add_argument(
        "--gazetteer",
        type=Path,
        default=Path("output/fwa_modules/stream_metadata.pickle"),
        help="Path to stream metadata pickle file",
    )
    parser.add_argument(
        "--graph",
        type=Path,
        default=Path("output/fwa_modules/fwa_bc_primal_full.gpickle"),
        help="Path to FWA graph pickle file (for tributary enrichment)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of regulations to process (for testing)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show verbose output",
    )

    args = parser.parse_args()

    # Load parsed regulations
    print_header("REGULATION MAPPER TEST")
    print(f"Loading parsed regulations from: {args.parsed_regs}")

    if not args.parsed_regs.exists():
        print(
            f"{RED}ERROR: Parsed regulations file not found: {args.parsed_regs}{RESET}"
        )
        print(f"\nPlease run the synopsis parsing pipeline first:")
        print(f"  cd scripts")
        print(f"  python -m synopsis_pipeline.parse_synopsis")
        sys.exit(1)

    with open(args.parsed_regs) as f:
        parsed_data = json.load(f)

    regulations = (
        parsed_data
        if isinstance(parsed_data, list)
        else parsed_data.get("regulations", [])
    )

    if args.limit:
        regulations = regulations[: args.limit]
        print(f"Limiting to first {args.limit} regulations")

    print(f"Loaded {len(regulations)} regulations")

    # Load gazetteer
    print(f"\nLoading gazetteer from: {args.gazetteer}")

    if not args.gazetteer.exists():
        print(f"{YELLOW}WARNING: Gazetteer file not found: {args.gazetteer}{RESET}")
        print(f"Creating empty gazetteer (all links will fail)")

        # Create a temporary empty metadata file
        import tempfile
        import pickle

        temp_metadata = tempfile.NamedTemporaryFile(
            mode="wb", delete=False, suffix=".pickle"
        )
        pickle.dump(
            {
                "zone_metadata": {},
                "streams": {},
                "lakes": {},
                "wetlands": {},
                "manmade": {},
            },
            temp_metadata,
        )
        temp_metadata.close()

        gazetteer = MetadataGazetteer(Path(temp_metadata.name))
        print(f"Created empty gazetteer")
    else:
        gazetteer = MetadataGazetteer(args.gazetteer)
        print(f"Loaded gazetteer with {len(gazetteer.name_index)} unique names")

    # Initialize components
    print("\nInitializing pipeline components...")

    manual_corrections = ManualCorrections(
        name_variations=NAME_VARIATIONS,
        direct_matches=DIRECT_MATCHES,
        skip_entries=SKIP_ENTRIES,
        unmarked_waterbodies=UNMARKED_WATERBODIES,
    )

    linker = WaterbodyLinker(
        gazetteer=gazetteer,
        manual_corrections=manual_corrections,
    )

    scope_filter = ScopeFilter()

    # Initialize tributary enricher with graph (if available)
    if args.graph.exists():
        print(f"Loading graph for tributary enrichment: {args.graph}")
        tributary_enricher = TributaryEnricher(
            graph_source=args.graph, metadata_gazetteer=gazetteer
        )
    else:
        print(f"{YELLOW}WARNING: Graph file not found: {args.graph}{RESET}")
        print(f"Tributary enrichment will be disabled")
        tributary_enricher = TributaryEnricher(metadata_gazetteer=gazetteer)

    mapper = RegulationMapper(
        linker=linker,
        scope_filter=scope_filter,
        tributary_enricher=tributary_enricher,
    )

    print(f"{GREEN}✓ Pipeline initialized{RESET}")

    # Process regulations using full pipeline (link → scope → enrich → merge → export)
    print_sub_header("Processing Regulations")

    output_dir = Path("output/fwa_modules/regulation_index")

    print(f"Running full pipeline: Link → Scope → Enrich → Merge → Export")
    result = mapper.process_and_export(regulations, output_dir=output_dir)

    # Get statistics
    mapper_stats = mapper.get_stats()
    scope_stats = scope_filter.get_stats()
    tributary_stats = tributary_enricher.get_stats()

    # Unpack results
    feature_to_regs = result.feature_to_regs
    merged_groups = result.merged_groups

    # Print results
    print_header("REGULATION MAPPING RESULTS")

    print_sub_header("Regulation Linking Statistics")
    print(f"Total regulations:          {mapper_stats.total_regulations}")
    print(
        f"Successfully linked:        {mapper_stats.linked_regulations} "
        f"({format_percentage(mapper_stats.linked_regulations, mapper_stats.total_regulations)})"
    )
    print(f"Failed to link:             {mapper_stats.failed_to_link_regulations}")

    print("\nLink Status Breakdown:")
    for status, count in mapper_stats.link_status_counts.most_common():
        pct = (count / mapper_stats.total_regulations) * 100
        print(f"  {status:20s}: {count:4d} ({pct:5.1f}%)")

    print_sub_header("Rule Processing Statistics")
    print(
        f"Total rules processed:      {mapper_stats.total_rules_processed} (from {mapper_stats.linked_regulations} regulations)"
    )
    print(f"Rule → feature mappings:    {mapper_stats.total_rule_to_feature_mappings}")
    print(f"Unique features with rules: {mapper_stats.unique_features_with_rules}")

    if mapper_stats.total_rules_processed > 0:
        avg_mappings = (
            mapper_stats.total_rule_to_feature_mappings
            / mapper_stats.total_rules_processed
        )
        print(f"Avg features per rule:      {avg_mappings:.1f}")
        avg_rules = (
            mapper_stats.total_rules_processed / mapper_stats.linked_regulations
            if mapper_stats.linked_regulations > 0
            else 0
        )
        print(f"Avg rules per regulation:   {avg_rules:.1f}")

    print_sub_header("Scope Filter Statistics")
    print(
        f"Scope types encountered:    {', '.join(scope_stats['scope_types_seen']) or 'None'}"
    )
    print(f"Fallback to WHOLE_SYSTEM:   {scope_stats['fallback_count']}")

    print_sub_header("Tributary Enricher Statistics")
    print(f"Enrichment requests:        {tributary_stats['enrichment_requests']}")
    print(f"Cache hits:                 {tributary_stats['cache_hits']}")
    print(f"Cache size:                 {tributary_stats['cache_size']}")
    print(
        f"Total tributaries found:    {tributary_stats.get('total_tributaries_found', 0)}"
    )
    print(
        f"Total base features:        {tributary_stats.get('total_base_features', 0)}"
    )
    print(f"Stream seeds used:          {tributary_stats.get('total_stream_seeds', 0)}")
    print(f"Lake seeds used:            {tributary_stats.get('total_lake_seeds', 0)}")
    if tributary_stats["enrichment_requests"] > 0:
        avg_tributaries = (
            tributary_stats.get("total_tributaries_found", 0)
            / tributary_stats["enrichment_requests"]
        )
        print(f"Avg tributaries/request:    {avg_tributaries:.1f}")

    # Merging statistics
    print_sub_header("Feature Merging Statistics")
    print(f"Total features:             {len(feature_to_regs)}")
    print(f"Merged groups:              {len(merged_groups)}")
    if len(feature_to_regs) > 0:
        reduction_pct = (1 - len(merged_groups) / len(feature_to_regs)) * 100
        print(
            f"Reduction:                  {reduction_pct:.1f}% ({len(feature_to_regs)} → {len(merged_groups)})"
        )

    # Analyze group sizes
    group_sizes = Counter()
    for group in merged_groups.values():
        group_sizes[group.feature_count] += 1

    print("\nGroups by size:")
    for size, count in sorted(group_sizes.items())[:10]:  # Show first 10
        print(f"  {size:3d} feature(s): {count:5d} groups")
    if len(group_sizes) > 10:
        print(f"  ... and {len(group_sizes) - 10} more size categories")

    # Show largest groups
    print("\nTop 5 largest merged groups:")
    top_groups = sorted(
        merged_groups.values(), key=lambda g: g.feature_count, reverse=True
    )[:5]
    for group in top_groups:
        print(
            f"  {group.group_id}: {group.feature_count} features, {len(group.regulation_ids)} regulations"
        )

    # Feature distribution analysis
    if feature_to_regs:
        print_sub_header("Feature Mapping Distribution")

        rule_counts = Counter()
        for feature_id, rule_list in feature_to_regs.items():
            rule_counts[len(rule_list)] += 1

        print("\nFeatures by number of rules:")
        for num_rules, count in sorted(rule_counts.items()):
            print(f"  {num_rules:3d} rule(s): {count:5d} features")

        # Always show top 5 features with most rules for debugging
        print("\nTop 5 features with most rules:")
        top_features = sorted(
            feature_to_regs.items(), key=lambda x: len(x[1]), reverse=True
        )[:5]

        for feature_id, rule_ids in top_features:
            # Try to get feature name from gazetteer
            feature_name = "unknown"
            try:
                # Look up feature in gazetteer to get name
                for category in ["streams", "lakes", "wetlands", "manmade"]:
                    features = getattr(gazetteer.metadata, category, {})
                    if feature_id in features:
                        feature_info = features[feature_id]
                        feature_name = feature_info.get("gnis_name", "unnamed")
                        break
            except:
                pass

            print(f"  {feature_id:30s}: {len(rule_ids):5d} rules - {feature_name}")

            # Show sample rule IDs if this is suspicious
            if len(rule_ids) > 1000:
                sample_rules = list(rule_ids)[:5]
                print(f"    Sample rules: {', '.join(sample_rules)}")

        # Additional verbose output
        if args.verbose:
            print("\nTop 10 features with most rules (detailed):")
            for i, (feature_id, rule_ids) in enumerate(top_features[:10], 1):
                print(f"\n  {i}. Feature: {feature_id}")
                print(f"     Rules: {len(rule_ids)}")
                print(f"     Rule IDs: {', '.join(list(rule_ids)[:10])}")
                if len(rule_ids) > 10:
                    print(f"     ... and {len(rule_ids) - 10} more")

    print_header("SUMMARY")

    # Overall success metrics
    if mapper_stats.total_regulations > 0:
        link_success_rate = (
            mapper_stats.linked_regulations / mapper_stats.total_regulations
        ) * 100
        print(
            f"Regulation link rate:  {format_percentage(mapper_stats.linked_regulations, mapper_stats.total_regulations)} ({link_success_rate:.1f}%)"
        )

    if mapper_stats.total_rules_processed > 0:
        print(
            f"Rules processed:       {mapper_stats.total_rules_processed} (from {mapper_stats.linked_regulations} regulations)"
        )
        print(f"Rule→feature mappings: {mapper_stats.total_rule_to_feature_mappings}")

    if len(feature_to_regs) > 0:
        reduction_pct = (1 - len(merged_groups) / len(feature_to_regs)) * 100
        print(
            f"\nFeature merging:       {len(feature_to_regs)} → {len(merged_groups)} ({reduction_pct:.1f}% reduction)"
        )
    else:
        print(f"\nFeature merging:       No features to merge (0 regulations linked)")

    print(f"\n{GREEN}✓ Full regulation mapping pipeline complete{RESET}")


if __name__ == "__main__":
    main()
