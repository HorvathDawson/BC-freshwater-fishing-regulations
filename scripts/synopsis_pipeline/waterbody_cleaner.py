"""
Waterbody name cleaning and grouping utilities.

This module provides functionality to clean waterbody names by removing quotes
and parenthetical content, then group waterbodies by region and cleaned name.
"""

import re
from typing import List, Dict, Any
from collections import defaultdict


def clean_waterbody_name(name: str) -> str:
    """
    Clean a waterbody name by removing quotes and parenthetical content.

    Also strips part suffixes like ", ALL PARTS", " - MAIN BODY", " - UPPER WEST ARM",
    etc. to group waterbody sections together.

    Examples:
        "ANDERSON" LAKE -> ANDERSON LAKE
        "(Lower) CAMPBELL LAKE'S TRIBUTARIES" -> CAMPBELL LAKE'S TRIBUTARIES
        "CLEAR LAKE (Quadra Island)" -> CLEAR LAKE
        "KOOTENAY LAKE, ALL PARTS" -> KOOTENAY LAKE
        "KOOTENAY LAKE - MAIN BODY" -> KOOTENAY LAKE
        "KOOTENAY LAKE - UPPER WEST ARM" -> KOOTENAY LAKE
        "WIGWAM RIVER (upstream of... (Ram) Forest Service Road)" -> WIGWAM RIVER

    Args:
        name: The original waterbody name

    Returns:
        Cleaned waterbody name with quotes, parenthetical content, and part suffixes removed
    """
    if not name:
        return name

    # Remove quotes around words
    # This handles cases like "ANDERSON" LAKE or "name"
    cleaned = re.sub(r'"([^"]+)"', r"\1", name)

    # Remove parenthetical content (including nested parentheses)
    # Use a loop to handle nested parentheses by repeatedly removing the innermost ones
    while "(" in cleaned:
        prev = cleaned
        # Remove innermost parentheses first (non-greedy match)
        cleaned = re.sub(r"\([^()]*\)", "", cleaned)
        # Break if nothing changed (safety check to avoid infinite loop)
        if cleaned == prev:
            break

    # Remove part suffixes to group waterbody sections together
    # Matches patterns like:
    # - ", ALL PARTS"
    # - " - MAIN BODY", " - UPPER WEST ARM", " - LOWER WEST ARM"
    # - " - NORTH ARM", " - SOUTH ARM", " - EAST ARM", " - WEST ARM"
    # - " - UPPER", " - LOWER", " - NORTH", " - SOUTH", " - EAST", " - WEST"
    # Pattern handles compound directions (UPPER WEST, LOWER WEST, etc.) and optional type suffix
    part_suffix_pattern = r"(, ALL PARTS| - (MAIN|UPPER|LOWER|NORTH|SOUTH|EAST|WEST)( (NORTH|SOUTH|EAST|WEST))?( (BODY|ARM|SECTION|PART|PORTION))?)"
    cleaned = re.sub(part_suffix_pattern, "", cleaned, flags=re.IGNORECASE)

    # Clean up extra whitespace that may result from removals
    cleaned = " ".join(cleaned.split())

    return cleaned.strip()


def add_cleaned_names(parsed_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Add cleaned_waterbody_name field to each parsed result.

    Args:
        parsed_results: List of parsed waterbody dictionaries

    Returns:
        Same list with cleaned_waterbody_name field added to each item
    """
    for result in parsed_results:
        if "waterbody_name" in result:
            result["cleaned_waterbody_name"] = clean_waterbody_name(
                result["waterbody_name"]
            )

    return parsed_results


def group_by_region_and_cleaned_name(
    parsed_results: List[Dict[str, Any]],
) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """
    Group parsed results first by region, then by cleaned waterbody name.

    Multiple waterbodies with different original names but the same cleaned name
    will be grouped together within each region.

    Args:
        parsed_results: List of parsed waterbody dictionaries with cleaned_waterbody_name field

    Returns:
        Nested dictionary structure:
        {
            "Region 1": {
                "CLEANED NAME 1": [waterbody1, waterbody2, ...],
                "CLEANED NAME 2": [waterbody3, ...],
                ...
            },
            "Region 2": {
                ...
            },
            ...
        }
    """
    # First level: group by region
    regional_groups = defaultdict(lambda: defaultdict(list))

    for result in parsed_results:
        region = result.get("region", "Unknown Region")
        cleaned_name = result.get(
            "cleaned_waterbody_name", result.get("waterbody_name", "Unknown")
        )

        # Add to the appropriate region and cleaned name group
        regional_groups[region][cleaned_name].append(result)

    # Convert defaultdicts to regular dicts for cleaner output
    return {
        region: dict(name_groups) for region, name_groups in regional_groups.items()
    }


def process_and_group_results(parsed_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Main processing function: add cleaned names and group by region and cleaned name.

    Args:
        parsed_results: List of parsed waterbody dictionaries

    Returns:
        Dictionary containing:
        - regions: Nested dict grouped by region then cleaned name
        - stats: Statistics about the grouping
    """
    # Add cleaned names
    results_with_cleaned = add_cleaned_names(parsed_results)

    # Group by region and cleaned name
    grouped = group_by_region_and_cleaned_name(results_with_cleaned)

    # Calculate statistics
    stats = {
        "total_waterbodies": len(parsed_results),
        "regions": len(grouped),
        "cleaned_names_by_region": {
            region: len(names) for region, names in grouped.items()
        },
        "total_unique_cleaned_names": sum(len(names) for names in grouped.values()),
    }

    return {"regions": grouped, "stats": stats}
