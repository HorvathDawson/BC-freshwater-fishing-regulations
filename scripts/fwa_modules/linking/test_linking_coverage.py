#!/usr/bin/env python3
"""
Test Linking Coverage

Tests the waterbody linking system against all parsed regulations
to see how well we can link waterbodies to FWA features.

This script:
1. Loads parsed_results.json
2. Initializes MetadataGazetteer with stream_metadata.pickle
3. Tries to link each waterbody using WaterbodyLinker
4. Reports statistics on success/failure rates

Usage:
    python test_linking_coverage.py
    python test_linking_coverage.py --export-not-found output.json
"""

import json
import argparse
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Any

from .linker import WaterbodyLinker, LinkStatus
from .metadata_gazetteer import MetadataGazetteer
from .name_variations import NAME_VARIATIONS, DIRECT_MATCHES, ManualCorrections


def export_not_found_to_file(
    not_found_items: List[Dict], output_path: Path, parsed_data_lookup: Dict[str, Dict]
):
    """
    Export NOT_FOUND items to a JSON file for manual review.

    Args:
        not_found_items: List of NOT_FOUND items from linking
        output_path: Path to write the output file
        parsed_data_lookup: Dictionary mapping (region, name_verbatim) to full parsed data
    """
    export_data = {
        "_instructions": {
            "description": "NOT_FOUND waterbodies - need to add name variations to name_variations.py",
            "how_to_fix": [
                "1. For each entry, determine why it wasn't found:",
                "   - Spelling difference? Add to NAME_VARIATIONS with target_names=[correct spelling]",
                "   - Name order difference? Add to NAME_VARIATIONS (e.g., 'Maxwell Lake' -> 'Lake Maxwell')",
                "   - Plural/singular? Add to NAME_VARIATIONS with singular/plural variant",
                "   - Renamed? Add to NAME_VARIATIONS with new name",
                "   - Not in gazetteer? Search online (AllTrails, Google Maps), add to DIRECT_MATCHES if found",
                "2. Check 'search_terms_used' to see what the linker searched for",
                "3. Check 'location_descriptor' for hints about location",
                "4. Use management_units to narrow down region/MU",
                "5. Add entry to name_variations.py in the appropriate region section",
            ],
            "example_name_variation": {
                "TOQUART LAKE": {
                    "target_names": ["toquaht lake"],
                    "note": "Spelling mismatch",
                }
            },
            "example_direct_match": {
                "LONG LAKE (Nanaimo)": {
                    "gnis_id": "17501",
                    "note": "Disambiguate using GNIS ID",
                }
            },
        },
        "count": len(not_found_items),
        "entries": [],
    }

    for item in not_found_items:
        region = item["region"]
        name_verbatim = item["name_verbatim"]
        waterbody_key = item["waterbody_key"]

        # Get full regulation data
        full_data = parsed_data_lookup.get((region, name_verbatim), {})

        # Check if name variation exists
        existing_variation = None
        if region and region in NAME_VARIATIONS:
            existing_variation = NAME_VARIATIONS[region].get(name_verbatim)

        # Check if direct match exists
        existing_direct_match = None
        if region and region in DIRECT_MATCHES:
            existing_direct_match = DIRECT_MATCHES[region].get(name_verbatim)

        export_item = {
            "name_verbatim": name_verbatim,
            "waterbody_key": waterbody_key,
            "region": region,
            "management_units": item.get("mu", []),
            "identity_type": item.get("identity_type"),
            "location_descriptor": item.get("location_descriptor"),
            "alternate_names": item.get("alternate_names", []),
            "page": full_data.get("page"),
            "regulations_summary": full_data.get("regs_verbatim"),
            "search_terms_used": item.get("search_terms", [waterbody_key.lower()]),
            "existing_variation": (
                {
                    "target_names": existing_variation.target_names,
                    "note": existing_variation.note,
                    "ignored": existing_variation.ignored,
                }
                if existing_variation
                else None
            ),
            "existing_direct_match": (
                {
                    "gnis_id": existing_direct_match.gnis_id,
                    "fwa_watershed_code": existing_direct_match.fwa_watershed_code,
                    "note": existing_direct_match.note,
                }
                if existing_direct_match
                else None
            ),
            "suggested_action": (
                "Add to NAME_VARIATIONS if spelling/formatting issue, or DIRECT_MATCHES if specific feature needed"
            ),
            "full_identity": full_data.get("identity"),
        }

        export_data["entries"].append(export_item)

    # Write to file
    with open(output_path, "w") as f:
        json.dump(export_data, f, indent=2)

    print(
        f"\nExported {len(export_data['entries'])} NOT_FOUND entries to {output_path}"
    )


def export_ambiguous_to_file(
    ambiguous_items: List[Dict], output_path: Path, parsed_data_lookup: Dict[str, Dict]
):
    """
    Export AMBIGUOUS items to a JSON file for manual review.

    Args:
        ambiguous_items: List of AMBIGUOUS items from linking
        output_path: Path to write the output file
        parsed_data_lookup: Dictionary mapping (region, name_verbatim) to full parsed data
    """
    export_data = {
        "_instructions": {
            "description": "AMBIGUOUS waterbodies - multiple candidates found, need disambiguation",
            "how_to_fix": [
                "1. Review candidate_waterbodies to see all matches found",
                "2. Compare regulation MUs with FWA MUs for each candidate",
                "3. Use location_descriptor to identify correct waterbody",
                "4. Choose the correct candidate based on:",
                "   - MU match (regulation MU should match FWA MU)",
                "   - Location hints in location_descriptor or regulations_summary",
                "   - Proximity to other named features",
                "5. Add DIRECT_MATCH entry with the correct GNIS ID or watershed code",
                "6. For lakes: use gnis_id from the correct candidate",
                "7. For streams: use fwa_watershed_code from the correct candidate",
            ],
            "example_direct_match_lake": {
                "RAINBOW LAKE": {
                    "gnis_id": "28692",
                    "note": "Disambiguate: Rainbow Lake in MU 5-12 (not the ones in 8-6, 7-40, etc.)",
                }
            },
            "example_direct_match_stream": {
                "BEAR RIVER": {
                    "fwa_watershed_code": "910-998407-000000-000000-...",
                    "note": "Disambiguate: Bear River in Region 1 MU 1-10 (not Region 6)",
                }
            },
        },
        "count": len(ambiguous_items),
        "entries": [],
    }

    for item in ambiguous_items:
        region = item["region"]
        name_verbatim = item["name_verbatim"]
        reg_mus = item.get("mu", [])

        # Get full regulation data
        full_data = parsed_data_lookup.get((region, name_verbatim), {})

        # Check if name variation exists
        existing_variation = None
        if region and region in NAME_VARIATIONS:
            existing_variation = NAME_VARIATIONS[region].get(name_verbatim)

        # Check if direct match exists
        existing_direct_match = None
        if region and region in DIRECT_MATCHES:
            existing_direct_match = DIRECT_MATCHES[region].get(name_verbatim)

        # Process candidates to add helpful info
        candidates = item.get("candidates", [])
        processed_candidates = []
        for candidate in candidates:
            fwa_mus = candidate.get("management_units", [])
            mu_match = any(mu in fwa_mus for mu in reg_mus)

            processed_candidate = {
                "fwa_name": candidate.get("name"),
                "gnis_id": candidate.get("gnis_id"),
                "fwa_watershed_code": candidate.get("fwa_watershed_code"),
                "waterbody_key": candidate.get("waterbody_key"),
                "feature_type": candidate.get("feature_type"),
                "fwa_region": candidate.get("region"),
                "fwa_management_units": fwa_mus,
                "mu_match": mu_match,
                "mu_comparison": {
                    "regulation_mus": reg_mus,
                    "fwa_mus": fwa_mus,
                    "matches": mu_match,
                },
                "count": candidate.get("count", 1),
            }
            processed_candidates.append(processed_candidate)

        export_item = {
            "name_verbatim": name_verbatim,
            "waterbody_key": item["waterbody_key"],
            "region": region,
            "regulation_management_units": reg_mus,
            "identity_type": item.get("identity_type"),
            "location_descriptor": item.get("location_descriptor"),
            "alternate_names": item.get("alternate_names", []),
            "page": full_data.get("page"),
            "regulations_summary": full_data.get("regs_verbatim"),
            "candidate_count": len(candidates),
            "candidate_waterbodies": processed_candidates,
            "suggested_action": (
                "Add to DIRECT_MATCHES with gnis_id (lakes) or fwa_watershed_code (streams) of correct candidate"
            ),
            "existing_variation": (
                {
                    "target_names": existing_variation.target_names,
                    "note": existing_variation.note,
                    "ignored": existing_variation.ignored,
                }
                if existing_variation
                else None
            ),
            "existing_direct_match": (
                {
                    "gnis_id": existing_direct_match.gnis_id,
                    "fwa_watershed_code": existing_direct_match.fwa_watershed_code,
                    "waterbody_key": existing_direct_match.waterbody_key,
                    "waterbody_keys": existing_direct_match.waterbody_keys,
                    "linear_feature_ids": existing_direct_match.linear_feature_ids,
                    "note": existing_direct_match.note,
                }
                if existing_direct_match
                else None
            ),
            "full_identity": full_data.get("identity"),
        }

        export_data["entries"].append(export_item)

    # Write to file
    with open(output_path, "w") as f:
        json.dump(export_data, f, indent=2)

    print(
        f"\nExported {len(export_data['entries'])} AMBIGUOUS entries to {output_path}"
    )


def test_linking_coverage(
    export_not_found_path: str = None, export_ambiguous_path: str = None
):
    """Test linking coverage on parsed regulations.

    Args:
        export_not_found_path: Optional path to export NOT_FOUND items for manual review
        export_ambiguous_path: Optional path to export AMBIGUOUS items for manual review
    """

    # Paths (relative to scripts directory)
    scripts_dir = Path(__file__).parent.parent.parent
    parsed_file = scripts_dir / "output/parse_synopsis/parsed_results.json"
    metadata_file = scripts_dir / "output/fwa_modules/stream_metadata.pickle"
    enriched_kml_file = scripts_dir / "output/fwa_modules/enriched_kml_points.json"
    kml_points_file = scripts_dir / "data/labelled/unnamed_lakes.kml"

    print("=" * 80)
    print("WATERBODY LINKING COVERAGE TEST")
    print("=" * 80)
    print()

    # Load parsed regulations
    print(f"Loading parsed regulations from {parsed_file}...")
    with open(parsed_file) as f:
        parsed_data = json.load(f)
    print(f"Loaded {len(parsed_data)} waterbodies")
    print()

    # Initialize gazetteer
    print(f"Loading FWA metadata from {metadata_file}...")
    print(f"Loading enriched KML points from {enriched_kml_file}...")
    gazetteer = MetadataGazetteer(metadata_file, enriched_kml_file)

    # Get counts from metadata
    streams_count = len(gazetteer.metadata.get("streams", {}))
    lakes_count = len(gazetteer.metadata.get("lakes", {}))
    kml_points_count = (
        len(gazetteer.enriched_kml_data.get("points", []))
        if gazetteer.enriched_kml_data
        else 0
    )
    print(
        f"Loaded {streams_count:,} streams, {lakes_count:,} lakes, {kml_points_count} KML points"
    )
    print()

    # Initialize linker
    print("Initializing waterbody linker with name variations...")
    manual_corrections = ManualCorrections(NAME_VARIATIONS, DIRECT_MATCHES)
    linker = WaterbodyLinker(gazetteer, manual_corrections)
    total_variations = sum(len(v) for v in NAME_VARIATIONS.values())
    total_direct = sum(len(v) for v in DIRECT_MATCHES.values())
    print(
        f"Loaded {total_variations} name variations and {total_direct} direct matches across {len(NAME_VARIATIONS)} regions"
    )
    print()

    # Test linking
    print("Testing linking...")
    print("-" * 80)
    print()

    results_by_status = defaultdict(list)
    results_by_region = defaultdict(lambda: Counter())
    failed_parse_entries = []
    no_region_entries = []
    tributaries_non_stream_matches = []  # Track TRIBUTARIES matched to non-streams
    used_name_variations = set()  # Track which name variations were used
    matched_kml_points = set()  # Track which KML points were matched
    parsed_data_lookup = {}  # For export: (region, name_verbatim) -> full data
    feature_to_waterbodies = defaultdict(
        list
    )  # Track FWA feature -> waterbodies mapping

    for i, waterbody in enumerate(parsed_data):
        identity = waterbody["identity"]
        waterbody_key = identity["waterbody_key"]
        name_verbatim = identity["name_verbatim"]
        location_descriptor = identity.get("location_descriptor")
        alternate_names = identity.get("alternate_names", [])
        identity_type = identity.get("identity_type")
        mu_list = waterbody.get("mu", [])

        # Skip entries that failed parsing
        if waterbody_key == "FAILED":
            failed_parse_entries.append(
                {
                    "name_verbatim": name_verbatim,
                    "error": waterbody.get("error", "Unknown error"),
                    "mu": mu_list,
                    "region": waterbody.get("region", "Unknown"),
                    "page": waterbody.get("page"),
                }
            )
            continue

        # Extract region from the region field
        # Format: "REGION 4 - Kootenay" -> "Region 4" or "REGION 7A" -> "Region 7"
        region = None
        region_str = waterbody.get("region", "")
        if region_str and region_str.startswith("REGION "):
            region_num = region_str.split(" ")[1].split("-")[0].strip()
            # Strip any letter suffix (e.g., "7A" -> "7")
            region_num = "".join(c for c in region_num if c.isdigit())
            region = f"Region {region_num}"

        # Skip entries with no region
        if not region:
            no_region_entries.append(
                {
                    "waterbody_key": waterbody_key,
                    "name_verbatim": name_verbatim,
                    "mu": mu_list,
                    "page": waterbody.get("page"),
                }
            )
            continue

        # Store full data for potential export
        if region:
            parsed_data_lookup[(region, name_verbatim)] = waterbody

        # Track name variation usage
        if region and region in NAME_VARIATIONS:
            if name_verbatim in NAME_VARIATIONS[region]:
                used_name_variations.add((region, name_verbatim))

        # Try linking (will search region first, then fall back to all regions if no match)
        result = linker.link_waterbody(
            waterbody_key,
            region=region,
            mgmt_units=mu_list,
            name_verbatim=name_verbatim,
        )

        # Track KML point matches
        # Mark KML point as matched if ANY feature with the same waterbody_key was successfully linked
        # (even if the matched feature is a polygon, not the point itself)
        # OR if the waterbody name directly matches a KML point name
        if result.status == LinkStatus.SUCCESS:
            # Handle both single and multi-waterbody matches
            if result.matched_features:
                matched_features = result.matched_features
            elif result.matched_feature:
                matched_features = [result.matched_feature]
            else:
                matched_features = []

            # First, try matching by waterbody_key
            for feature in matched_features:
                if feature.waterbody_key:
                    # Check if there's a KML point with this waterbody_key
                    if (
                        gazetteer.enriched_kml_data
                        and "points" in gazetteer.enriched_kml_data
                    ):
                        for point in gazetteer.enriched_kml_data["points"]:
                            point_wbk = (
                                point.get("lake_waterbody_key")
                                or point.get("marsh_waterbody_key")
                                or point.get("manmade_waterbody_key")
                            )
                            if point_wbk == feature.waterbody_key:
                                point_name = point.get("name", "").strip().lower()
                                if point_name:
                                    matched_kml_points.add(point_name)
                                break

            # Also check if the waterbody_key or matched_name directly matches a KML point name
            # This handles cases where KML points have no waterbody_key assigned
            if gazetteer.enriched_kml_data and "points" in gazetteer.enriched_kml_data:
                for point in gazetteer.enriched_kml_data["points"]:
                    point_name = point.get("name", "").strip().lower()
                    if point_name:
                        # Check if waterbody_key matches KML point name
                        if waterbody_key.lower() == point_name:
                            matched_kml_points.add(point_name)
                        # Check if matched_name matches KML point name
                        elif (
                            result.matched_name
                            and result.matched_name.lower() == point_name
                        ):
                            matched_kml_points.add(point_name)

        # Check if this is a TRIBUTARIES entry matched to non-stream features
        is_tributary_non_stream = False
        if identity_type == "TRIBUTARIES" and result.status == LinkStatus.SUCCESS:
            # Check if any matched features are NOT multilinestring (streams)
            matched_features = result.matched_features or (
                [result.matched_feature] if result.matched_feature else []
            )
            if matched_features and all(
                f.geometry_type != "multilinestring" for f in matched_features
            ):
                is_tributary_non_stream = True
                tributaries_non_stream_matches.append(
                    {
                        "waterbody_key": waterbody_key,
                        "name_verbatim": name_verbatim,
                        "location_descriptor": location_descriptor,
                        "alternate_names": alternate_names,
                        "identity_type": identity_type,
                        "region": region,
                        "mu": mu_list,
                        "result": result,
                    }
                )

        # Collect statistics
        results_by_status[result.status].append(
            {
                "waterbody_key": waterbody_key,
                "name_verbatim": name_verbatim,
                "location_descriptor": location_descriptor,
                "alternate_names": alternate_names,
                "identity_type": identity_type,
                "region": region,
                "mu": mu_list,
                "result": result,
                "is_tributary_non_stream": is_tributary_non_stream,
            }
        )

        results_by_region[region][result.status] += 1

        # Track which FWA features are matched by which waterbodies (for cross-region detection)
        if result.status == LinkStatus.SUCCESS:
            matched_features = result.matched_features or (
                [result.matched_feature] if result.matched_feature else []
            )
            for feature in matched_features:
                if feature:
                    feature_to_waterbodies[feature.fwa_id].append(
                        {
                            "waterbody_key": waterbody_key,
                            "region": region,
                            "mu": mu_list,
                            "feature_region": feature.region,
                        }
                    )

        # Progress indicator
        if (i + 1) % 100 == 0:
            print(f"Processed {i + 1}/{len(parsed_data)} waterbodies...", end="\r")

    print(f"Processed {len(parsed_data)}/{len(parsed_data)} waterbodies    ")
    print()

    # Print summary statistics
    print("=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)
    print()

    # Report excluded entries separately
    if failed_parse_entries:
        print(
            f"⚠️  FAILED PARSE ENTRIES: {len(failed_parse_entries)} (excluded from linking)"
        )
    if no_region_entries:
        print(f"⚠️  NO REGION ENTRIES: {len(no_region_entries)} (excluded from linking)")
    if failed_parse_entries or no_region_entries:
        print()

    # Calculate totals excluding failed and no-region entries
    total_linkable = (
        len(parsed_data) - len(failed_parse_entries) - len(no_region_entries)
    )
    for status in LinkStatus:
        count = len(results_by_status[status])
        percentage = (count / total_linkable) * 100 if total_linkable > 0 else 0
        print(f"{status.value.upper():20s}: {count:5d} ({percentage:5.1f}%)")

    # Show TRIBUTARIES matched to non-streams as a special case
    if tributaries_non_stream_matches:
        trib_count = len(tributaries_non_stream_matches)
        trib_percentage = (
            (trib_count / total_linkable) * 100 if total_linkable > 0 else 0
        )
        print(
            f"  ⚠️  of which TRIBUTARIES→non-stream: {trib_count:5d} ({trib_percentage:5.1f}%) [essentially unmatched]"
        )

    print()
    print("=" * 80)
    print("CROSS-REGION MATCHES")
    print("=" * 80)
    print()

    # Find FWA features matched by waterbodies from different regions
    cross_region_matches = []
    for fwa_id, waterbodies in feature_to_waterbodies.items():
        if len(waterbodies) > 1:
            # Check if waterbodies are from different regions
            regions = set(wb["region"] for wb in waterbodies)
            if len(regions) > 1:
                cross_region_matches.append(
                    {"fwa_id": fwa_id, "waterbodies": waterbodies, "regions": regions}
                )

    if cross_region_matches:
        print(
            f"\nFound {len(cross_region_matches)} FWA features matched by waterbodies from different regions:"
        )
        print("This could indicate boundary waterbodies or potential matching issues.")
        print("-" * 80)
        for match in cross_region_matches[:30]:  # Show first 30
            regions_str = ", ".join(sorted(match["regions"]))
            print(f"\n  FWA Feature: {match['fwa_id']}")
            print(f"  Matched from regions: {regions_str}")
            print(f"  Waterbodies ({len(match['waterbodies'])}):")
            for wb in match["waterbodies"]:
                mus = ", ".join(wb["mu"]) if wb["mu"] else "No MUs"
                print(
                    f"    • {wb['waterbody_key']:40s} | {wb['region']:10s} | MUs: {mus} | Feature in: {wb['feature_region']}"
                )
        if len(cross_region_matches) > 30:
            print(f"\n  ... and {len(cross_region_matches) - 30} more")
    else:
        print(
            "\nNo cross-region matches found. All FWA features matched by waterbodies from a single region."
        )

    print()
    print("=" * 80)
    print("RESULTS BY REGION")
    print("=" * 80)
    print()

    for region in sorted(results_by_region.keys()):
        print(f"\n{region}:")
        region_total = sum(results_by_region[region].values())
        for status in LinkStatus:
            count = results_by_region[region][status]
            percentage = (count / region_total) * 100 if region_total > 0 else 0
            if count > 0:
                print(f"  {status.value:15s}: {count:4d} ({percentage:5.1f}%)")

    print()
    print("=" * 80)
    print("NEEDS REVIEW (Sample)")
    print("=" * 80)

    # Show samples of items needing review, grouped by identity type for NOT_FOUND
    for status in [LinkStatus.NOT_FOUND, LinkStatus.AMBIGUOUS, LinkStatus.ERROR]:
        items = results_by_status[status]
        if items:
            if status == LinkStatus.NOT_FOUND:
                # Group NOT_FOUND by identity_type
                by_type = {}
                for item in items:
                    identity_type = item.get("identity_type", "UNKNOWN")
                    if identity_type not in by_type:
                        by_type[identity_type] = []
                    by_type[identity_type].append(item)

                print(
                    f"\n{status.value.upper()} (grouped by type, showing 5 per type):"
                )
                print("-" * 80)
                for identity_type in sorted(by_type.keys()):
                    type_items = by_type[identity_type]
                    print(f"\n  {identity_type} ({len(type_items)} total):")
                    for item in type_items[:5]:  # Show first 5 of each type
                        print(
                            f"    {item['waterbody_key']:38s} | {item['region'] or 'NO REGION':10s}"
                        )
                        print(f"      Name: {item['name_verbatim']}")
                        if item.get("location_descriptor"):
                            print(f"      Location: {item['location_descriptor']}")
                        if item["alternate_names"]:
                            print(
                                f"      Alternate Names: {', '.join(item['alternate_names'])}"
                            )
                        print(
                            f"      Regulation MUs: {', '.join(item['mu']) if item['mu'] else 'None'}"
                        )
                        print()
            elif status == LinkStatus.ERROR:
                # For ERROR, show compact single-line format
                print(f"\n{status.value.upper()} (showing first 20):")
                print("-" * 80)
                for item in items[:20]:
                    mus = ", ".join(item["mu"]) if item["mu"] else "None"
                    error_msg = (
                        item["result"].error_message[:60]
                        if item["result"].error_message
                        else "Unknown error"
                    )
                    print(
                        f"  {item['waterbody_key']:40s} | {item['region'] or 'NO REGION':10s} | MUs: {mus:20s} | Error: {error_msg}"
                    )
            else:
                # For AMBIGUOUS, group by identity_type like NOT_FOUND
                by_type = {}
                for item in items:
                    identity_type = item.get("identity_type", "UNKNOWN")
                    if identity_type not in by_type:
                        by_type[identity_type] = []
                    by_type[identity_type].append(item)

                print(
                    f"\n{status.value.upper()} (grouped by type, showing 10 per type):"
                )
                print("-" * 80)
                for identity_type in sorted(by_type.keys()):
                    type_items = by_type[identity_type]
                    print(f"\n  {identity_type} ({len(type_items)} total):")
                    for item in type_items[:10]:  # Show first 10 of each type
                        print(
                            f"    {item['waterbody_key']:40s} | {item['region'] or 'NO REGION':30s}"
                        )
                        print(f"      Name: {item['name_verbatim']}")
                        if item["location_descriptor"]:
                            print(f"      Location: {item['location_descriptor']}")
                        if item["alternate_names"]:
                            print(
                                f"      Alternate Names: {', '.join(item['alternate_names'])}"
                            )
                        print(
                            f"      Regulation MUs: {', '.join(item['mu']) if item['mu'] else 'None'}"
                        )

                        # Show which name matched (if applicable)
                        if item["result"].matched_name:
                            if item["result"].matched_name != item["waterbody_key"]:
                                print(
                                    f"      ✓ Matched using alternate name: '{item['result'].matched_name}'"
                                )
                            else:
                                print(f"      ✓ Matched using primary name")

                        candidates = item["result"].candidate_features
                        # Group candidates by identity (same logic as linker deduplication)
                        # This shows distinct waterbodies more clearly than grouping by WSC alone
                        identity_groups = {}
                        for f in candidates:
                            # Use same identity logic as linker
                            if f.geometry_type == "multilinestring" and f.fwa_watershed_code:
                                identity = ("stream", f.fwa_watershed_code, f.region or "Unknown")
                            elif f.gnis_id:
                                identity = ("gnis", f.gnis_id, f.region or "Unknown")
                            elif f.waterbody_key:
                                identity = ("waterbody_key", f.waterbody_key, f.region or "Unknown")
                            else:
                                identity = ("fwa_id", f.fwa_id, f.region or "Unknown")
                            
                            if identity not in identity_groups:
                                identity_groups[identity] = []
                            identity_groups[identity].append(f)

                        print(
                            f"      Candidate waterbodies ({len(identity_groups)} distinct):"
                        )
                        for identity, features in list(identity_groups.items())[:10]:
                            identity_type, identity_value, region = identity
                            
                            # Collect all unique mgmt units across all features in this group
                            all_mgmt_units = set()
                            for f in features:
                                if f.mgmt_units:
                                    all_mgmt_units.update(f.mgmt_units)
                            mgmt_str = (
                                ", ".join(sorted(all_mgmt_units))
                                if all_mgmt_units
                                else "No MUs"
                            )

                            # Collect unique GNIS IDs for polygons
                            gnis_ids = set()
                            for f in features:
                                if f.gnis_id:
                                    gnis_ids.add(f.gnis_id)
                            gnis_str = (
                                f", GNIS_ID: {', '.join(sorted(gnis_ids))}"
                                if gnis_ids
                                else ""
                            )

                            # Build name display with GNIS_NAME_2 annotation if needed
                            first_feature = features[0]
                            name_display = first_feature.name

                            # Show GNIS_NAME_2 annotation when:
                            # 1. Multiple distinct waterbodies exist (len(identity_groups) > 1), AND
                            # 2. This feature matched via GNIS_NAME_2
                            if (
                                len(identity_groups) > 1
                                and first_feature.matched_via == "gnis_name_2"
                            ):
                                # Show primary name with alternate name annotation
                                if (
                                    first_feature.gnis_name
                                    and first_feature.gnis_name_2
                                ):
                                    name_display = f"{first_feature.gnis_name} (GNIS_NAME_2: {first_feature.gnis_name_2})"

                            # Check geometry type to determine how to display
                            first_geom_type = first_feature.geometry_type
                            is_stream = first_geom_type == "multilinestring"

                            if identity_type == "stream":
                                # Stream - show watershed code
                                wsc_short = identity_value[:50] + "..." if len(identity_value) > 50 else identity_value
                                print(
                                    f"      • Stream WSC {wsc_short} ({region}): {len(features)} segment(s) - {name_display}"
                                )
                                print(f"        FWA MUs: {mgmt_str}")
                            elif identity_type == "gnis":
                                # Polygon with GNIS ID
                                print(
                                    f"      • Polygon GNIS {identity_value} ({region}): {len(features)} polygon(s) - {name_display} ({first_geom_type})"
                                )
                                print(f"        FWA MUs: {mgmt_str}")
                            elif identity_type == "waterbody_key":
                                # Polygon/point with waterbody key
                                print(
                                    f"      • Waterbody Key {identity_value} ({region}): {len(features)} feature(s) - {name_display} ({first_geom_type}){gnis_str}"
                                )
                                print(f"        FWA MUs: {mgmt_str}")
                            else:
                                # FWA ID fallback
                                print(
                                    f"      • FWA ID {identity_value} ({region}): {len(features)} feature(s) - {name_display} ({first_geom_type}){gnis_str}"
                                )
                                print(f"        FWA MUs: {mgmt_str}")
                    print()  # Add blank line between entries

    print()
    print("=" * 80)
    print("NO REGION ENTRIES")
    print("=" * 80)

    if no_region_entries:
        print(
            f"\nFound {len(no_region_entries)} entries with no region (showing first 20):"
        )
        print("-" * 80)
        for item in no_region_entries[:20]:
            print(f"  Name: {item['name_verbatim']}")
            print(f"    Waterbody Key: {item['waterbody_key']}")
            print(f"    MU: {', '.join(item['mu']) if item['mu'] else 'None'}")
            print(f"    Page: {item['page']}")
            print()
    else:
        print("\nNo entries without region found.")

    print()
    print("=" * 80)
    print("TRIBUTARIES MATCHED TO NON-STREAMS (Essentially Unmatched)")
    print("=" * 80)

    if tributaries_non_stream_matches:
        print(
            f"\nFound {len(tributaries_non_stream_matches)} TRIBUTARIES entries matched to lakes/polygons instead of streams (showing first 30):"
        )
        print("-" * 80)
        for item in tributaries_non_stream_matches[:30]:
            matched_features = item["result"].matched_features or (
                [item["result"].matched_feature]
                if item["result"].matched_feature
                else []
            )
            matched_to = matched_features[0].name if matched_features else "Unknown"
            matched_type = (
                matched_features[0].geometry_type if matched_features else "Unknown"
            )
            print(
                f"  {item['waterbody_key']:24s} | {item['region']:8s} | Matched to: {matched_to} ({matched_type})"
            )
    else:
        print("\nNo TRIBUTARIES entries matched to non-streams found.")

    print()
    print("=" * 80)
    print("UNUSED NAME VARIATIONS")
    print("=" * 80)

    # Find unused name variations
    unused_variations = []
    for region, variations in NAME_VARIATIONS.items():
        for waterbody_key, variation in variations.items():
            if (
                not variation.ignored
                and (region, waterbody_key) not in used_name_variations
            ):
                unused_variations.append(
                    {
                        "region": region,
                        "waterbody_key": waterbody_key,
                        "target_names": variation.target_names,
                        "note": variation.note,
                    }
                )

    if unused_variations:
        print(
            f"\nFound {len(unused_variations)} unused name variations (not matched in this test, showing first 50):"
        )
        print("-" * 80)
        for item in unused_variations[:50]:
            targets = (
                ", ".join(item["target_names"])[:40] + "..."
                if len(", ".join(item["target_names"])) > 40
                else ", ".join(item["target_names"])
            )
            note = item["note"][:35] + "..." if len(item["note"]) > 35 else item["note"]
            print(
                f"  {item['waterbody_key']:40s} | {item['region']:12s} | → {targets:40s} | {note}"
            )
        if len(unused_variations) > 50:
            print(f"  ... and {len(unused_variations) - 50} more")
    else:
        print("\nAll name variations were used successfully.")

    print()
    print("=" * 80)
    print("UNMATCHED KML POINTS")
    print("=" * 80)

    # Find unmatched KML points
    all_kml_points = set()
    if gazetteer.enriched_kml_data and "points" in gazetteer.enriched_kml_data:
        for point in gazetteer.enriched_kml_data["points"]:
            name = point.get("name", "").strip()
            if name:
                all_kml_points.add(name.lower())

    unmatched_kml = []
    if all_kml_points:
        for kml_name in all_kml_points:
            if kml_name not in matched_kml_points:
                # Find the original point for details
                for point in gazetteer.enriched_kml_data["points"]:
                    if point.get("name", "").strip().lower() == kml_name:
                        waterbody_key = (
                            point.get("lake_waterbody_key")
                            or point.get("marsh_waterbody_key")
                            or point.get("manmade_waterbody_key")
                        )
                        unmatched_kml.append(
                            {
                                "name": point.get("name"),
                                "region": point.get("region"),
                            }
                        )
                        break

    if unmatched_kml:
        print(
            f"\nFound {len(unmatched_kml)} KML points that were not matched in this test (showing first 50):"
        )
        print("-" * 80)
        for item in unmatched_kml[:50]:
            print(f"  {item['region']:12s} | {item['name']}")
        if len(unmatched_kml) > 50:
            print(f"  ... and {len(unmatched_kml) - 50} more")
    else:
        if all_kml_points:
            print("\nAll KML points were matched successfully.")
        else:
            print("\nNo KML points loaded.")

    print()
    print("=" * 80)
    print("NOT FOUND IN DATA (Searched but no FWA data)")
    print("=" * 80)

    # Find entries marked as not_found
    not_found_in_data = []
    for region, variations in NAME_VARIATIONS.items():
        for waterbody_key, variation in variations.items():
            if variation.not_found:
                not_found_in_data.append(
                    {
                        "region": region,
                        "waterbody_key": waterbody_key,
                        "note": variation.note,
                    }
                )

    for region, matches in DIRECT_MATCHES.items():
        for waterbody_key, match in matches.items():
            if match.not_found:
                not_found_in_data.append(
                    {
                        "region": region,
                        "waterbody_key": waterbody_key,
                        "note": match.note,
                    }
                )

    if not_found_in_data:
        print(
            f"\n{len(not_found_in_data)} waterbodies were searched for but could not be found in FWA data:"
        )
        print("-" * 80)
        for item in not_found_in_data:
            note = item["note"][:50] + "..." if len(item["note"]) > 50 else item["note"]
            print(f"  {item['waterbody_key']:40s} | {item['region']:12s} | {note}")
    else:
        print("\nNo waterbodies marked as not found in data.")

    print()
    print("=" * 80)
    print("IGNORED WATERBODIES")
    print("=" * 80)

    ignored = results_by_status[LinkStatus.IGNORED]
    if ignored:
        print(
            f"\n{len(ignored)} waterbodies are marked as ignored in name_variations.py"
        )
    else:
        print("\nNo ignored waterbodies found.")

    print()
    print("=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)
    print()

    not_found = len(results_by_status[LinkStatus.NOT_FOUND])
    not_in_data = len(results_by_status[LinkStatus.NOT_IN_DATA])
    ambiguous = len(results_by_status[LinkStatus.AMBIGUOUS])
    success = len(results_by_status[LinkStatus.SUCCESS])
    failed_parse = len(failed_parse_entries)
    no_region = len(no_region_entries)
    trib_non_stream = len(tributaries_non_stream_matches)

    print(f"✅ Successfully linked: {success} waterbodies")
    if trib_non_stream > 0:
        print(
            f"   ⚠️  But {trib_non_stream} are TRIBUTARIES→non-stream (essentially unmatched)"
        )
        print(f"   → Effective success: {success - trib_non_stream} waterbodies")
    print(f"⚠️  Need name variations: {not_found} waterbodies (NOT_FOUND)")
    print(f"📝 Documented as not in data: {not_in_data} waterbodies (NOT_IN_DATA)")
    print(f"⚠️  Need disambiguation: {ambiguous} waterbodies (AMBIGUOUS)")
    if failed_parse > 0:
        print(f"❌ Failed parsing: {failed_parse} entries (need to fix parser)")
    if no_region > 0:
        print(f"⚠️  No region: {no_region} entries (need MU region mapping)")
    print()

    # Export NOT_FOUND items if requested
    if export_not_found_path:
        export_not_found_to_file(
            results_by_status[LinkStatus.NOT_FOUND],
            Path(export_not_found_path),
            parsed_data_lookup,
        )

    # Export AMBIGUOUS items if requested
    if export_ambiguous_path:
        export_ambiguous_to_file(
            results_by_status[LinkStatus.AMBIGUOUS],
            Path(export_ambiguous_path),
            parsed_data_lookup,
        )

    print("Next steps:")
    if failed_parse > 0:
        print("1. Fix parsing issues for FAILED entries (see parse_synopsis.py)")
        print("2. Review NOT_FOUND items and add name variations to name_variations.py")
        print(
            "3. Review AMBIGUOUS items and add region filtering or specific variations"
        )
    else:
        print("1. Review NOT_FOUND items and add name variations to name_variations.py")
        print(
            "2. Review AMBIGUOUS items and add region filtering or specific variations"
        )
        print("3. Run this script again to verify improvements")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Test waterbody linking coverage against parsed regulations"
    )
    parser.add_argument(
        "--export-not-found",
        type=str,
        help="Export NOT_FOUND entries to JSON file for manual review",
        metavar="PATH",
    )
    parser.add_argument(
        "--export-ambiguous",
        type=str,
        help="Export AMBIGUOUS entries to JSON file for manual review",
        metavar="PATH",
    )

    args = parser.parse_args()
    test_linking_coverage(
        export_not_found_path=args.export_not_found,
        export_ambiguous_path=args.export_ambiguous,
    )
