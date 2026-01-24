#!/usr/bin/env python3
"""
Analyze segment-level regulations in LLM parsed data.

Finds waterbodies with multiple geographic locations (segment-specific regulations)
and exports them for review.

ASSUMPTIONS:
1. Vague locations like "parts", "parts of lake", "on parts" are treated as
   whole-waterbody rules since they don't specify which segments they apply to.
2. These vague entries are filtered from multi-segment analysis but documented separately.
3. Location strings are case-insensitive when checking for vague patterns.
"""

import json
import csv
from collections import defaultdict
from pathlib import Path

# Vague location patterns that cannot be mapped to specific segments
# These will be treated as whole-waterbody rules
VAGUE_LOCATION_PATTERNS = [
    "parts",
    "parts of lake",
    "parts of river",
    "parts of stream",
    "parts of the lake",
    "parts of the river",
    "on parts",
    "on parts of the lake",
    "where open",
    "entire river",
    "entire lake",
    "entire stream",
    "various locations",
]

# Tributary-related locations that should be excluded from segment analysis
# These are whole-waterbody inheritance rules, not specific segments
TRIBUTARY_PATTERNS = [
    "includes tributaries",
    "including tributaries",
    "tributaries",
]


def is_vague_location(location):
    """
    Check if a location string is too vague to map to specific segments.

    Args:
        location: Location string from geographic_groups

    Returns:
        True if location is vague, False otherwise
    """
    if not location:
        return False

    location_lower = location.lower().strip()

    # Check for exact matches
    if location_lower in VAGUE_LOCATION_PATTERNS:
        return True

    # Check if it contains tributary patterns
    for pattern in TRIBUTARY_PATTERNS:
        if pattern in location_lower:
            return True

    # Check for partial matches for swimming/various locations
    if "swimming area" in location_lower or "various location" in location_lower:
        return True

    return False


def analyze_segment_regulations(input_file, output_csv):
    """
    Analyze LLM parsed results for segment-level regulations.

    Filters out vague location patterns (e.g., "parts", "parts of lake") that
    cannot be mapped to specific segments. These are treated as whole-waterbody rules.

    Args:
        input_file: Path to llm_parsed_results.json
        output_csv: Path to output CSV file
    """

    print(f"Loading data from {input_file}...")
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"Total entries: {len(data)}")

    # Statistics
    total_entries = len(data)
    entries_with_multiple_locations = 0
    entries_with_multiple_specific_locations = 0  # Excluding vague
    entries_with_single_location = 0
    entries_with_no_location = 0
    vague_location_entries = 0

    # Collect segment data
    segment_data = []
    vague_segment_data = []
    location_counts = defaultdict(int)
    vague_location_counts = defaultdict(int)

    for entry in data:
        waterbody_name = entry.get("waterbody_name", "UNKNOWN")
        geographic_groups = entry.get("geographic_groups", [])

        num_locations = len(geographic_groups)

        # Filter out vague locations for multi-segment counting
        specific_locations = []
        has_vague_location = False

        for group in geographic_groups:
            location = group.get("location", "").strip()
            if location and is_vague_location(location):
                has_vague_location = True
            elif location:
                specific_locations.append(location)
            else:
                specific_locations.append("[WHOLE WATERBODY]")

        # Count statistics
        if len(geographic_groups) > 1:
            entries_with_multiple_locations += 1
            # Only count as multi-segment if has >1 specific (non-vague) locations
            if len(specific_locations) > 1:
                entries_with_multiple_specific_locations += 1
        elif len(geographic_groups) == 1:
            entries_with_single_location += 1
        else:
            entries_with_no_location += 1

        if has_vague_location:
            vague_location_entries += 1

        # Store data for each location
        for group in geographic_groups:
            location = group.get("location", "").strip()
            if not location:
                location = "[WHOLE WATERBODY]"

            is_vague = is_vague_location(location)

            # Get rule summary
            rules = group.get("rules", [])
            rule_summary = "; ".join(
                [r.get("rule", "") for r in rules[:3]]
            )  # First 3 rules
            if len(rules) > 3:
                rule_summary += "..."

            row_data = {
                "waterbody_name": waterbody_name,
                "location": location,
                "num_locations_total": len(geographic_groups),
                "num_rules": len(rules),
                "rule_summary": rule_summary,
                "zone": ", ".join(entry.get("mu", [])),
                "includes_tribs": (
                    "YES" if "Incl. Tribs" in entry.get("symbols", []) else "NO"
                ),
                "region": entry.get("region", ""),
                "page": entry.get("page", ""),
                "is_vague": "YES" if is_vague else "NO",
            }

            segment_data.append(row_data)

            if is_vague:
                vague_segment_data.append(row_data)
                vague_location_counts[location] += 1
            else:
                location_counts[location] += 1

    # Print statistics
    print("\n" + "=" * 70)
    print("SEGMENT-LEVEL REGULATION ANALYSIS")
    print("=" * 70)
    print(f"\nTotal waterbody entries: {total_entries}")
    print(
        f"  - Entries with multiple locations: {entries_with_multiple_locations} ({entries_with_multiple_locations/total_entries*100:.1f}%)"
    )
    print(
        f"      - With specific segments (excluding vague): {entries_with_multiple_specific_locations} ({entries_with_multiple_specific_locations/total_entries*100:.1f}%)"
    )
    print(
        f"  - Entries with single location: {entries_with_single_location} ({entries_with_single_location/total_entries*100:.1f}%)"
    )
    print(
        f"  - Entries with no locations: {entries_with_no_location} ({entries_with_no_location/total_entries*100:.1f}%)"
    )
    print(
        f"  - Entries with vague locations (treated as whole-waterbody): {vague_location_entries} ({vague_location_entries/total_entries*100:.1f}%)"
    )

    print(f"\nTotal location records (including whole waterbody): {len(segment_data)}")
    print(f"  - Specific locations: {len(location_counts)}")
    print(f"  - Vague locations: {len(vague_segment_data)}")

    if vague_segment_data:
        print(f"\nVague location patterns (will be treated as whole-waterbody rules):")
        for pattern, count in sorted(
            vague_location_counts.items(), key=lambda x: x[1], reverse=True
        ):
            print(f"  * {pattern}: {count} occurrence(s)")

    # Top location patterns
    print("\n" + "-" * 70)
    print("TOP 20 MOST COMMON SPECIFIC LOCATION PATTERNS:")
    print("-" * 70)
    sorted_locations = sorted(location_counts.items(), key=lambda x: x[1], reverse=True)
    for i, (location, count) in enumerate(sorted_locations[:20], 1):
        print(f"{i:2d}. [{count:4d}] {location[:60]}...")

    # Export to CSV
    print(f"\n" + "-" * 70)
    print(f"Exporting to {output_csv}...")

    fieldnames = [
        "waterbody_name",
        "location",
        "num_locations_total",
        "num_rules",
        "rule_summary",
        "zone",
        "includes_tribs",
        "region",
        "page",
        "is_vague",
    ]

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(segment_data)

    print(f"✓ Exported {len(segment_data)} records")

    # Export multi-segment entries separately (EXCLUDING vague locations and whole waterbody)
    multi_segment_file = str(output_csv).replace(".csv", "_multi_segment_only.csv")
    multi_segment_data = [
        row
        for row in segment_data
        if row["num_locations_total"] > 1
        and row["is_vague"] == "NO"
        and row["location"] != "[WHOLE WATERBODY]"
    ]

    with open(multi_segment_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(multi_segment_data)

    print(
        f"✓ Exported {len(multi_segment_data)} specific multi-segment records to {multi_segment_file}"
    )

    # Export vague location entries separately for documentation
    vague_file = str(output_csv).replace(".csv", "_vague_locations.csv")
    with open(vague_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(vague_segment_data)

    print(
        f"✓ Exported {len(vague_segment_data)} vague location records to {vague_file}"
    )

    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)

    return {
        "total_entries": total_entries,
        "multi_location": entries_with_multiple_locations,
        "multi_specific_location": entries_with_multiple_specific_locations,
        "single_location": entries_with_single_location,
        "no_location": entries_with_no_location,
        "vague_location_entries": vague_location_entries,
        "segment_records": len(segment_data),
        "vague_records": len(vague_segment_data),
    }


if __name__ == "__main__":
    # File paths
    script_dir = Path(__file__).parent.parent
    input_file = script_dir / "output" / "llm_parser" / "llm_parsed_results.json"
    output_csv = script_dir / "output" / "llm_parser" / "segment_analysis.csv"

    # Run analysis
    stats = analyze_segment_regulations(input_file, output_csv)
