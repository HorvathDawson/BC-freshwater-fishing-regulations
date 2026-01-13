#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Waterbody Linking Script

Links regulation data from grouped_results.json to GIS waterbody features.

Features:
- Uses cleaned waterbody names from LLM parser output
- Region-specific name corrections (1-to-many or 1-to-1)
- No normalization - exact matching only with name corrections
- Dataclass-based type safety with attrs
- Modular design - run entire pipeline or individual steps
- GDB export of matched features with regulations
- Reverse index for feature_id -> regulations lookup

Input:
- scripts/output/llm_parser/grouped_results.json
- scripts/output/fwa_preprocessing/waterbody_index.json

Output:
- scripts/output/waterbody_linking/matched_waterbodies.json
- scripts/output/waterbody_linking/unmatched_waterbodies.csv
- scripts/output/waterbody_linking/matched_waterbodies.gdb (optional)
- scripts/output/waterbody_linking/feature_regulation_index.json (optional)

Usage:
    # Run full pipeline
    python waterbody_linking.py
    
    # Run only matching step
    python waterbody_linking.py --step match
    
    # Export to GDB only (requires matched_waterbodies.json)
    python waterbody_linking.py --step export-gdb
    
    # Create reverse index only
    python waterbody_linking.py --step reverse-index
"""

import json
import re
import signal
import sys
import csv
import argparse
from pathlib import Path
from typing import List, Dict, Optional, Set, Tuple
from difflib import SequenceMatcher
import attrs

from synopsis_pipeline.utils import normalize_name

# ANSI color codes
C_GREEN = "\033[92m"
C_RED = "\033[91m"
C_CYAN = "\033[96m"
C_RESET = "\033[0m"


@attrs.frozen
class NameCorrection:
    """Correction mapping for a waterbody name."""

    target_names: List[str]
    note: str
    ignored: bool = False  # If True, waterbody won't appear in unmatched results


@attrs.frozen
class WaterbodyMatch:
    """Result of matching a waterbody to GIS features."""

    region: str
    waterbody_key: str
    original_waterbody_names: List[str]  # Raw names from regulations before cleaning
    match_status: str  # "matched" or "unmatched"
    gis_matches: List[Dict]
    regulations: List[Dict]
    pipeline_log: List[str]
    warnings: Optional[List[str]] = None
    debug_zones: Optional[List[str]] = None
    debug_mus: Optional[List[str]] = None
    debug_suggestions: Optional[List[str]] = None
    unmatched_correction_targets: Optional[List[str]] = (
        None  # Target names from corrections that didn't match
    )


# Region-specific name corrections
# Format: {"REGION NAME": {"WATERBODY NAME": NameCorrection(target_names=[...], note="...")}}
# Use "ALL REGIONS" for wildcard patterns that apply everywhere
# Wildcard patterns use * as placeholder, e.g., "* LAKE'S TRIBUTARIES" -> ["* lake tributary"]
NAME_CORRECTIONS: Dict[str, Dict[str, NameCorrection]] = {
    "ALL REGIONS": {
        "* LAKE'S TRIBUTARIES": NameCorrection(
            target_names=["* lake tributary"],
            note="Standardize possessive lake tributaries",
        ),
        "* RIVER'S TRIBUTARIES": NameCorrection(
            target_names=["* river tributary"],
            note="Standardize possessive river tributaries",
        ),
        "* CREEK'S TRIBUTARIES": NameCorrection(
            target_names=["* creek tributary"],
            note="Standardize possessive creek tributaries",
        ),
    },
    "REGION 1 - Vancouver Island": {
        "TOQUART LAKE": NameCorrection(
            target_names=["toquaht lake"], note="Spelling mismatch"
        ),
        "TOQUART RIVER": NameCorrection(
            target_names=["toquaht river"], note="Spelling mismatch"
        ),
        "MAGGIE LAKE": NameCorrection(
            target_names=["makii lake"],
            note="renamed in gazette (https://apps.gov.bc.ca/pub/bcgnws/names/62541.html)",
        ),
        "MAHATTA RIVER": NameCorrection(
            target_names=["mahatta creek"], note="gazetteer lists as creek"
        ),
        "BIG QUALICUM RIVER": NameCorrection(
            target_names=["qualicum river"], note="i think this is just qualicum river"
        ),
        "MAXWELL LAKE": NameCorrection(
            target_names=["lake maxwell"], note="it is 'lake maxwell'"
        ),
        "LINK RIVER": NameCorrection(
            target_names=[],
            note="Listed as 'Marble (Link) River' in gazzetteer",
            ignored=True,
        ),
        "STOWELL LAKE": NameCorrection(
            target_names=["lake stowell"], note="it is 'lake stowell' in gazetteer"
        ),
        "WESTON LAKE": NameCorrection(
            target_names=["lake weston"], note="Name order correction"
        ),
    },
    "REGION 2 - Lower Mainland": {
        "CHILLIWACK / VEDDER RIVERS": NameCorrection(
            target_names=["chilliwack river", "vedder river"],
            note="Split combined entry into distinct rivers",
        ),
        "LILLOOET LAKE, LILLOOET RIVER": NameCorrection(
            target_names=["lillooet lake", "lillooet river"],
            note="Split combined entry into distinct waterbodies.",
        ),
        "CAP SHEAF LAKES": NameCorrection(
            target_names=["cap sheaf lake"],
            note="Plural to singular for KML point match (https://www.alltrails.com/explore/recording/afternoon-hike-at-placer-mountain-0d770c4?p=-1&sh=li9ufv)",
        ),
        "ERROCK LAKE": NameCorrection(
            target_names=["lake errock"], note="Name order correction"
        ),
        "LUCILLE LAKE": NameCorrection(
            target_names=["lake lucille"], note="Name order correction"
        ),
        "MCKAY CREEK": NameCorrection(
            target_names=["mackay creek"], note="Spelling correction"
        ),
        "SARDIS PARK POND": NameCorrection(
            target_names=["sardis pond"], note="Name simplification"
        ),
        "HATZIC LAKE AND SLOUGH": NameCorrection(
            target_names=["hatzic lake", "hatzic slough"],
            note="Split combined entry into distinct waterbodies",
        ),
        "JONES LAKE": NameCorrection(
            target_names=["wahleach lake"],
            note="Labelled as Wahleach Lake in GIS (https://www.bchydro.com/community/recreation_areas/jones_lake.html)",
        ),
        "WEAVER LAKE and WEAVER CREEK": NameCorrection(
            target_names=["weaver lake", "weaver creek"],
            note="Split combined entry into distinct waterbodies",
        ),
        "PAQ LAKE": NameCorrection(
            target_names=["lily lake"],
            note="Known locally as Lily Lake",
        ),
        "SWELTZER CREEK": NameCorrection(
            target_names=["sweltzer river"],
            note="Labelled as Sweltzer River in GIS",
        ),
    },
    "REGION 3 - Thompson-Nicola": {
        "LITTLE DUM LAKE": NameCorrection(
            target_names=["little dum lake", "little dum lake 2"],
            note="Split numbered lakes for KML point matches",
        ),
    },
    "REGION 4 - Kootenay": {
        "CARIBOU LAKES": NameCorrection(
            target_names=["north caribou lake", "south caribou lake"],
            note="Split into North Caribou Lake and South Caribou Lake",
        ),
        "CHAMPION LAKES NO. 1 & 2": NameCorrection(
            target_names=["champion lake no. 1", "champion lake no. 2"],
            note="Split numbered lakes for KML point matches",
        ),
        "ARROW PARK CREEK": NameCorrection(
            target_names=["mosquito creek"], note="gazetteer lists as 'mosquito creek'"
        ),
        "LAKE REVELSTOKE": NameCorrection(
            target_names=["revelstoke lake"],
            note="Name order correction to match gazetteer",
        ),
        "LAKE REVELSTOKE'S TRIBUTARIES": NameCorrection(
            target_names=["revelstoke lake tributary"],
            note="Name order correction to match gazetteer",
        ),
        "ARROW LAKES": NameCorrection(
            target_names=[],
            note="Regulation refers to Upper/Lower Arrow Lake details",
            ignored=True,
        ),
        "ARROW LAKES' TRIBUTARIES": NameCorrection(
            target_names=[],
            note="Likely covered by Upper/Lower tributaries",
            ignored=True,
        ),
        "CONNOR LAKE": NameCorrection(
            target_names=["connor lakes"], note="Plural variation"
        ),
        "CONNOR LAKE'S TRIBUTARIES": NameCorrection(
            target_names=["connor lakes tributary"], note="Plural variation"
        ),
        "ECHOES LAKE": NameCorrection(
            target_names=["echoes lakes"], note="Plural variation"
        ),
        "EDWARDS LAKE": NameCorrection(
            target_names=["edwards lakes"], note="Plural variation"
        ),
        "PEND D'OREILLE RIVER": NameCorrection(
            target_names=["pend-d'oreille river"], note="Hyphenation correction"
        ),
        "PEND D'OREILLE RIVER'S TRIBUTARIES": NameCorrection(
            target_names=["pend-d'oreille river tributary"],
            note="Hyphenation correction",
        ),
        "SEVEN MILE RESERVOIR": NameCorrection(
            target_names=[],
            note="Dammed portion of Pend d'Oreille River - uses same regulations as Pend d'Oreille River. This may change in future. Polygons are in unnamed manmade lakes.",
            ignored=True,
        ),
        "SEVEN MILE RESERVOIR'S TRIBUTARIES": NameCorrection(
            target_names=[],
            note="Covered by Pend d'Oreille River tributary regulations",
            ignored=True,
        ),
        "WANETA RESERVOIR": NameCorrection(
            target_names=[],
            note="Dammed portion of Pend d'Oreille River - uses same regulations as Pend d'Oreille River. This may change in future. Polygons are in unnamed manmade lakes.",
            ignored=True,
        ),
        "WANETA RESERVOIR'S TRIBUTARIES": NameCorrection(
            target_names=[],
            note="Covered by Pend d'Oreille River tributary regulations",
            ignored=True,
        ),
        "ROCK ISLAND LAKE": NameCorrection(
            target_names=["rock isle lake"],
            note="Name variation; rock isle lake (in 4-25, rock island lake in 3-39)",
        ),
        "GARBUTT LAKE": NameCorrection(
            target_names=["norbury lake"],
            note="Official name is Norbury (Garbutt) Lake",
        ),
        "KOOCANUSA RESERVOIR": NameCorrection(
            target_names=["lake koocanusa"], note="Name variation"
        ),
        "LITTLE SLOCAN LAKE'S TRIBUTARIES": NameCorrection(
            target_names=[
                "upper little slocan lake tributary",
                "lower little slocan lake tributary",
            ],
            note="Split into upper and lower tributaries",
        ),
        "MCNAUGHTON LAKE": NameCorrection(
            target_names=["kinbasket lake"],
            note="McNaughton Lake is part of Kinbasket Lake reservoir",
        ),
        "WHITETAIL LAKE'S INLET & OUTLET STREAMS": NameCorrection(
            target_names=[
                "whitetail lake tributary",
                "whitetail creek tributary",
                "whitetail creek",
            ],
            note="Standardize inlet/outlet to tributary",
        ),
    },
    "REGION 5 - Cariboo": {
        "BALLON LAKE": NameCorrection(
            target_names=["baillon lake"], note="Spelling correction"
        ),
        "GRASSY LAKE": NameCorrection(
            target_names=["grassy lake", "grassy lake 2"],
            note="Split numbered lakes for KML point matches",
        ),
        "HIGH LAKE": NameCorrection(
            target_names=["high lake", "high lake 2", "high lake 3"],
            note="Split numbered lakes for KML point matches",
        ),
        "TANYA LAKE'S TRIBUTARIES": NameCorrection(
            target_names=["tanya lakes tributary"], note="Plural variation"
        ),
    },
    "REGION 6 - Skeena": {
        "SUSTUT LAKES": NameCorrection(
            target_names=["sustut lake"], note="Singular variation"
        ),
        "KSI HLGINX RIVER": NameCorrection(
            target_names=["ksi hlginx"], note="Drops 'river' suffix"
        ),
        "KSI SGASGINIST CREEK": NameCorrection(
            target_names=["ksi sgasginist"], note="Drops 'creek' suffix"
        ),
        "KSI SII AKS RIVER": NameCorrection(
            target_names=["ksi sii aks"], note="Drops 'river' suffix"
        ),
        "KSI X'ANMAS RIVER": NameCorrection(
            target_names=["ksi x'anmas"], note="Drops 'river' suffix"
        ),
        "MCDONNEL LAKE": NameCorrection(
            target_names=["mcdonell lake"], note="Spelling correction"
        ),
    },
    "REGION 7A - Omineca": {
        "MORFEE LAKE": NameCorrection(
            target_names=["morfee lakes"], note="Plural variation"
        ),
        "HAUTETE LAKE": NameCorrection(
            target_names=["hautête lake"], note="Accent correction"
        ),
        "EAST HAUTETE LAKE": NameCorrection(
            target_names=["east hautête lake"], note="Accent correction"
        ),
        "JOHN'S LAKE": NameCorrection(
            target_names=["johns lake"], note="Possessive apostrophe removal"
        ),
        "KLWALI LAKE": NameCorrection(
            target_names=["klawli lake"], note="Spelling correction"
        ),
        "THORN CREEK": NameCorrection(
            target_names=["thorne creek"], note="Spelling correction"
        ),
    },
    "REGION 7B - Peace": {
        "SUNDANCE LAKE": NameCorrection(
            target_names=["sundance lakes"], note="Plural variation"
        ),
    },
    "REGION 8 - Okanagan": {
        "TUC-EL-NUIT LAKE": NameCorrection(
            target_names=["tugulnuit lake"], note="spelling mismatch"
        ),
        "BIGHORN RESERVOIR": NameCorrection(
            target_names=["big horn reservoir"], note="Spacing correction"
        ),
        "TEE PEE LAKES": NameCorrection(
            target_names=["tepee lakes"], note="Spelling correction"
        ),
    },
}


def extract_zones_from_regulations(regulations: List[Dict]) -> List[str]:
    """Extract management unit zones from regulation blocks."""
    zones = set()
    for reg in regulations:
        for mu in reg.get("mu", []):
            # Extract zone number (e.g., "2-7" -> "2")
            match = re.match(r"(\d+)-", mu)
            if match:
                zones.add(match.group(1))
    return sorted(zones)


def extract_mus_from_regulations(regulations: List[Dict]) -> List[str]:
    """Extract full management units from regulation blocks."""
    mus = set()
    for reg in regulations:
        for mu in reg.get("mu", []):
            mus.add(mu)
    return sorted(mus)


def validate_management_units(
    regulations: List[Dict], gis_matches: List[Dict], waterbody_key: str
) -> List[str]:
    """Validate that GIS feature MUs match regulation MUs.

    Returns list of warning messages if mismatches are found.
    """
    warnings = []

    # Extract all MUs from regulations
    reg_mus = set()
    for reg in regulations:
        for mu in reg.get("mu", []):
            reg_mus.add(mu)

    if not reg_mus:
        warnings.append(f"No management units found in regulations")
        return warnings

    # Track mismatches to group by pattern
    # Key: (type, gis_mu, mu_field_name, reg_mus_tuple)
    # Value: list of feature_ids
    mismatch_groups = {}
    no_mu_field_by_type = {}

    # Check each GIS match for MU field
    debug_shown = False
    for match in gis_matches:
        attrs = match.get("attributes", {})

        # DEBUG: Show what fields are actually available (only for first feature)
        if not debug_shown:
            available_fields = list(attrs.keys())
            zone_fields = [
                f
                for f in available_fields
                if "ZONE" in f.upper() or "UNIT" in f.upper()
            ]
            if not zone_fields:
                warnings.append(
                    f"DEBUG: No ZONE/UNIT fields found in GIS data. Available fields: {available_fields[:10]}..."
                )
            debug_shown = True

        # Look for MU field (dynamically search for fields containing ZONE or UNIT)
        gis_mu = None
        mu_field_name = None
        for field_name, field_value in attrs.items():
            if (
                "ZONE" in field_name.upper() or "UNIT" in field_name.upper()
            ) and field_name != "ZONE_GROUP":
                gis_mu = field_value
                mu_field_name = field_name
                break

        feature_type = match.get("type", "unknown")
        feature_id = match.get("feature_id")

        if not gis_mu:
            # Group by type
            if feature_type not in no_mu_field_by_type:
                no_mu_field_by_type[feature_type] = []
            no_mu_field_by_type[feature_type].append(feature_id)
            continue

        # Check if GIS MU is in regulation MUs
        if str(gis_mu) not in reg_mus:
            # Group by pattern (type, gis_mu, field_name, reg_mus)
            key = (feature_type, str(gis_mu), mu_field_name, tuple(sorted(reg_mus)))
            if key not in mismatch_groups:
                mismatch_groups[key] = []
            mismatch_groups[key].append(feature_id)

    # Build warnings from grouped data
    for feature_type, feature_ids in no_mu_field_by_type.items():
        if len(feature_ids) == 1:
            warnings.append(
                f"GIS feature {feature_ids[0]} ({feature_type}) has no MU field"
            )
        else:
            warnings.append(
                f"GIS features {feature_ids} ({feature_type}) have no MU field"
            )

    for (
        feature_type,
        gis_mu,
        mu_field_name,
        reg_mus_tuple,
    ), feature_ids in mismatch_groups.items():
        if len(feature_ids) == 1:
            warnings.append(
                f"MU mismatch: GIS feature {feature_ids[0]} ({feature_type}) "
                f"has MU '{gis_mu}' (field: {mu_field_name}) but regulations specify {list(reg_mus_tuple)}"
            )
        else:
            warnings.append(
                f"MU mismatch: GIS features {feature_ids} ({feature_type}) "
                f"have MU '{gis_mu}' (field: {mu_field_name}) but regulations specify {list(reg_mus_tuple)}"
            )

    return warnings


def expand_point_to_polygons(point_match: Dict, zone: str, index: Dict) -> List[Dict]:
    """If a KML point has polygon IDs, fetch and return those polygons.

    Returns list of linked polygon features.
    """
    linked_polygons = []
    attrs = point_match.get("attributes", {})

    # Check for polygon IDs in the point
    poly_id_fields = [
        ("LAKE_POLY_ID", "lake"),
        ("WETLAND_POLY_ID", "wetland"),
        ("MANMADE_POLY_ID", "manmade"),
    ]

    for field_name, poly_type in poly_id_fields:
        poly_id = attrs.get(field_name)
        if poly_id and str(poly_id) != "nan":
            # Search through the zone's index for this polygon
            if zone in index:
                for name_key, features in index[zone].items():
                    for feature in features:
                        if feature.get("type") == poly_type:
                            feature_poly_id = feature.get("attributes", {}).get(
                                "WATERBODY_POLY_ID"
                            )
                            if str(feature_poly_id) == str(poly_id):
                                poly_copy = feature.copy()
                                poly_copy["_source_zone"] = zone
                                poly_copy["_linked_from_point"] = True
                                linked_polygons.append(poly_copy)
                                break

    return linked_polygons


def apply_wildcard_correction(
    waterbody_key: str, pattern: str, correction: NameCorrection
) -> Optional[NameCorrection]:
    """
    Apply a wildcard pattern correction to a waterbody name.

    Args:
        waterbody_key: The waterbody name to match against
        pattern: The wildcard pattern (e.g., "* LAKE'S TRIBUTARIES")
        correction: The NameCorrection with wildcard target_names

    Returns:
        A new NameCorrection with * replaced by the matched prefix, or None if no match
    """
    # Convert pattern to regex (escape special chars except *)
    import re as regex_module

    pattern_lower = pattern.lower()
    key_lower = waterbody_key.lower()

    # Replace * with a capture group
    regex_pattern = pattern_lower.replace("*", "(.+?)")
    regex_pattern = "^" + regex_pattern + "$"

    match = regex_module.match(regex_pattern, key_lower)
    if not match:
        return None

    # Extract the wildcard match (the prefix)
    prefix = match.group(1).strip()

    # Substitute prefix into target_names
    substituted_names = []
    for target in correction.target_names:
        if "*" in target:
            substituted_names.append(target.replace("*", prefix))
        else:
            substituted_names.append(target)

    # Return new correction with substituted names
    return NameCorrection(
        target_names=substituted_names, note=correction.note, ignored=correction.ignored
    )


def search_gis_index(
    search_name: str, zones: List[str], index: Dict, matched_kml_points: Set[str] = None
) -> List[Dict]:
    """
    Search GIS index for a waterbody name in specified zones.
    Returns list of ALL matched features including:
    - Direct name matches (streams, polygons, points)
    - Polygons linked from KML points

    Args:
        search_name: Normalized waterbody name to search for
        zones: List of management zones to search in
        index: GIS waterbody index
        matched_kml_points: Set to track which KML points have been matched (modified in place)
    """
    matches = []

    for zone in zones:
        if zone in index and search_name in index[zone]:
            found = index[zone][search_name]
            for f in found:
                match_copy = f.copy()
                match_copy["_source_zone"] = zone
                match_copy["_matched_on"] = search_name
                matches.append(match_copy)

                # Track if this is a KML point match
                if matched_kml_points is not None and f.get("type") == "point":
                    # Get all possible names from the point and add them all
                    names_to_add = []
                    if f.get("gnis_name"):
                        names_to_add.append(f.get("gnis_name"))
                    attrs = f.get("attributes", {})
                    if attrs.get("Name"):
                        names_to_add.append(attrs.get("Name"))
                    if attrs.get("name"):
                        names_to_add.append(attrs.get("name"))

                    # Add all unique names found
                    for name in names_to_add:
                        if name:
                            matched_kml_points.add(name)

                # If this is a KML point with polygon links, fetch those too
                if f.get("type") == "point":
                    linked_polys = expand_point_to_polygons(f, zone, index)
                    matches.extend(linked_polys)

    return matches


def process_waterbody(
    region: str,
    waterbody_key: str,
    regulations: List[Dict],
    index: Dict,
    stats: Dict,
    matched_kml_points: Set[str] = None,
) -> WaterbodyMatch:
    """Process a single waterbody and attempt to match it to GIS features."""

    pipeline_log = []
    zones = extract_zones_from_regulations(regulations)

    if not zones:
        pipeline_log.append(f"{C_RED}No management units found in regulations{C_RESET}")

    # Check for name correction
    target_names = [normalize_name(waterbody_key)]  # Default: use normalized key
    applied_correction = None

    # First check region-specific exact match
    if region in NAME_CORRECTIONS and waterbody_key in NAME_CORRECTIONS[region]:
        applied_correction = NAME_CORRECTIONS[region][waterbody_key]

    # If no exact match, check wildcard patterns in ALL REGIONS
    if not applied_correction and "ALL REGIONS" in NAME_CORRECTIONS:
        for pattern, correction in NAME_CORRECTIONS["ALL REGIONS"].items():
            if "*" in pattern:
                wildcard_result = apply_wildcard_correction(
                    waterbody_key, pattern, correction
                )
                if wildcard_result:
                    applied_correction = wildcard_result
                    pipeline_log.append(f"Applied wildcard pattern: '{pattern}'")
                    break

    # Apply the correction if found
    if applied_correction:
        target_names = [
            normalize_name(name) for name in applied_correction.target_names
        ]
        pipeline_log.append(f"Correction: '{waterbody_key}' -> {target_names}")
        pipeline_log.append(f"  Note: {applied_correction.note}")

    # Search for each target name
    all_matches = []
    unmatched_targets = []  # Track unmatched targets from corrections
    for target_name in target_names:
        matches = search_gis_index(target_name, zones, index, matched_kml_points)

        if matches:
            all_matches.extend(matches)
            # Count feature types for detailed logging
            type_counts = {}
            for m in matches:
                ftype = m.get("type", "unknown")
                type_counts[ftype] = type_counts.get(ftype, 0) + 1

            type_summary = ", ".join(
                [f"{count} {ftype}" for ftype, count in type_counts.items()]
            )
            pipeline_log.append(
                f"Found {len(matches)} GIS feature(s) for '{target_name}': {type_summary}"
            )
        else:
            pipeline_log.append(f"{C_RED}No GIS match for '{target_name}'{C_RESET}")
            # Track unmatched targets only if a correction was applied
            if applied_correction:
                unmatched_targets.append(target_name)

    # Deduplicate by feature_id
    unique_matches = {m["feature_id"]: m for m in all_matches}.values()
    final_matches = list(unique_matches)

    if final_matches:
        stats["matched"] += 1

        # Validate management units
        warnings = validate_management_units(regulations, final_matches, waterbody_key)

        # Extract original waterbody names from regulations
        original_names = list(
            set(
                reg.get("waterbody_name", waterbody_key)
                for reg in regulations
                if reg.get("waterbody_name")
            )
        )
        if not original_names:
            original_names = [waterbody_key]

        if warnings:
            stats.setdefault("warnings", 0)
            stats["warnings"] += 1
            print(
                f"  {C_GREEN}[MATCH]{C_RESET} '{waterbody_key}' -> {len(final_matches)} features "
                f"{C_RED}(with {len(warnings)} warnings){C_RESET}"
            )
        else:
            print(
                f"  {C_GREEN}[MATCH]{C_RESET} '{waterbody_key}' -> {len(final_matches)} features"
            )

        return WaterbodyMatch(
            region=region,
            waterbody_key=waterbody_key,
            original_waterbody_names=original_names,
            match_status="matched",
            gis_matches=final_matches,
            regulations=regulations,
            pipeline_log=pipeline_log,
            warnings=warnings if warnings else None,
            unmatched_correction_targets=(
                unmatched_targets if unmatched_targets else None
            ),
        )
    else:
        # Find similar names for debugging with improved matching
        suggestions = []
        search_term = normalize_name(waterbody_key)
        search_words = set(search_term.split())

        # Collect candidates with scores
        candidates = []
        for zone in zones:
            if zone in index:
                for gis_name in index[zone].keys():
                    gis_words = set(gis_name.split())

                    # Calculate different similarity metrics
                    # 1. Sequence similarity (overall string match)
                    seq_ratio = SequenceMatcher(None, search_term, gis_name).ratio()

                    # 2. Word overlap (Jaccard similarity)
                    common_words = search_words & gis_words
                    all_words = search_words | gis_words
                    word_ratio = len(common_words) / len(all_words) if all_words else 0

                    # 3. Check for word order swap (e.g., "stowell lake" vs "lake stowell")
                    is_word_swap = (
                        len(search_words) == len(gis_words)
                        and search_words == gis_words
                        and search_term != gis_name
                    )

                    # 4. Check if all search words are in GIS name (subset match)
                    is_subset = search_words.issubset(gis_words)

                    # Combine scores with weighted priorities
                    if is_word_swap:
                        score = 0.95  # Very high for word swaps
                    elif is_subset and len(common_words) >= 2:
                        score = 0.85  # High for subset with multiple words
                    elif word_ratio >= 0.7:
                        score = 0.8  # High for significant word overlap
                    elif seq_ratio >= 0.6:
                        score = seq_ratio
                    else:
                        score = 0

                    if score >= 0.6:
                        candidates.append((score, gis_name, zone))

        # Sort by score (descending) and take top suggestions
        candidates.sort(reverse=True, key=lambda x: x[0])
        for score, gis_name, zone in candidates[:5]:  # Limit to top 5
            suggestions.append(f"{gis_name} (zone {zone})")

        # Extract original waterbody names from regulations
        original_names = list(
            set(
                reg.get("waterbody_name", waterbody_key)
                for reg in regulations
                if reg.get("waterbody_name")
            )
        )
        if not original_names:
            original_names = [waterbody_key]

        print(f"  {C_RED}[NO MATCH]{C_RESET} '{waterbody_key}'")
        mus = extract_mus_from_regulations(regulations)
        return WaterbodyMatch(
            region=region,
            waterbody_key=waterbody_key,
            original_waterbody_names=original_names,
            match_status="unmatched",
            gis_matches=[],
            regulations=regulations,
            pipeline_log=pipeline_log,
            warnings=None,
            debug_zones=zones,
            debug_mus=mus,
            debug_suggestions=suggestions[:3],
            unmatched_correction_targets=(
                unmatched_targets if unmatched_targets else None
            ),
        )


def save_matched_results(results: Dict, output_dir: Path) -> None:
    """Save matched waterbodies to JSON file."""
    output_path = output_dir / "matched_waterbodies.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"Output: {output_path}")


def save_unmatched_csv(
    unmatched_rows: List, output_dir: Path, ignored_count: int = 0
) -> None:
    """Save unmatched waterbodies to CSV file."""
    output_path = output_dir / "unmatched_waterbodies.csv"
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "Region",
                "Waterbody Key (Raw)",
                "Cleaned Name",
                "Name Variations (from corrections)",
                "Note",
                "MUs",
                "Similar Names",
            ]
        )
        writer.writerows(unmatched_rows)
    ignored_msg = f" ({ignored_count} ignored)" if ignored_count > 0 else ""
    print(f"Unmatched: {output_path}{ignored_msg}")


def save_warnings_log(results: Dict, output_dir: Path) -> None:
    """Save all warnings to a log file, separated by waterbody type."""
    output_path = output_dir / "matching_warnings.log"

    # Separate waterbodies by whether they contain stream warnings
    lakes_and_polygons = {}  # Non-stream waterbodies
    streams = {}  # Stream waterbodies

    for region, waterbodies in results.items():
        for wb_name, wb_data in waterbodies.items():
            if wb_data.get("warnings"):
                # Check if waterbody name suggests it's a stream/river
                name_lower = wb_name.lower()
                is_stream = any(
                    keyword in name_lower
                    for keyword in [
                        "river",
                        "creek",
                        "stream",
                        "tributary",
                        "tributaries",
                    ]
                )

                if is_stream:
                    if region not in streams:
                        streams[region] = {}
                    streams[region][wb_name] = wb_data
                else:
                    if region not in lakes_and_polygons:
                        lakes_and_polygons[region] = {}
                    lakes_and_polygons[region][wb_name] = wb_data

    warning_count = 0
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("WATERBODY MATCHING WARNINGS LOG\n")
        f.write("=" * 80 + "\n\n")

        # Write lakes and polygon waterbodies first
        if lakes_and_polygons:
            f.write("LAKES, WETLANDS, AND OTHER WATERBODIES\n")
            f.write("=" * 80 + "\n")

            for region, waterbodies in lakes_and_polygons.items():
                f.write(f"\n{region}\n")
                f.write("-" * 80 + "\n")

                for wb_name, wb_data in waterbodies.items():
                    f.write(f"\n{wb_name}:\n")
                    for warning in wb_data["warnings"]:
                        f.write(f"  ⚠ {warning}\n")
                        warning_count += 1

        # Write streams and rivers separately
        if streams:
            f.write("\n\n")
            f.write("STREAMS, RIVERS, AND TRIBUTARIES\n")
            f.write("=" * 80 + "\n")

            for region, waterbodies in streams.items():
                f.write(f"\n{region}\n")
                f.write("-" * 80 + "\n")

                for wb_name, wb_data in waterbodies.items():
                    f.write(f"\n{wb_name}:\n")
                    for warning in wb_data["warnings"]:
                        f.write(f"  ⚠ {warning}\n")
                        warning_count += 1

    if warning_count > 0:
        print(f"Warnings: {output_path} ({warning_count} warnings)")
    else:
        print("No warnings found")


def extract_kml_points_from_index(index: Dict) -> Set[str]:
    """Extract all KML point names from the waterbody index.

    Args:
        index: The waterbody index dictionary

    Returns:
        Set of all KML point names (original names, not normalized)
    """
    kml_point_names = set()

    for zone, zone_data in index.items():
        for normalized_name, features in zone_data.items():
            for feature in features:
                if feature.get("type") == "point":
                    # Get all possible names from the point
                    names_to_add = []
                    if feature.get("gnis_name"):
                        names_to_add.append(feature.get("gnis_name"))
                    attrs = feature.get("attributes", {})
                    if attrs.get("Name"):
                        names_to_add.append(attrs.get("Name"))
                    if attrs.get("name"):
                        names_to_add.append(attrs.get("name"))

                    # Add all unique names found
                    for name in names_to_add:
                        if name:
                            kml_point_names.add(name)

    return kml_point_names


def save_linking_warnings(
    all_kml_points: Set[str],
    matched_kml_points: Set[str],
    results: Dict,
    output_dir: Path,
) -> None:
    """Save combined warnings for unmatched KML points and correction targets.

    Args:
        all_kml_points: Set of all KML point names from the file
        matched_kml_points: Set of KML points that were matched to regulations
        results: Dictionary of all waterbody matching results
        output_dir: Directory to save the warning log
    """
    # Check for unmatched KML points
    unmatched_points = all_kml_points - matched_kml_points

    # Collect all unmatched correction targets
    unmatched_by_region = {}
    correction_count = 0

    for region, waterbodies in results.items():
        for wb_name, wb_data in waterbodies.items():
            unmatched_targets = wb_data.get("unmatched_correction_targets")
            if unmatched_targets:
                if region not in unmatched_by_region:
                    unmatched_by_region[region] = []
                unmatched_by_region[region].append(
                    {"waterbody": wb_name, "targets": unmatched_targets}
                )
                correction_count += len(unmatched_targets)

    # If nothing to report, print success message and return
    if not unmatched_points and not unmatched_by_region:
        success_msgs = []
        if all_kml_points:
            success_msgs.append(
                f"All {len(all_kml_points)} KML points matched to regulations"
            )
        success_msgs.append("All correction targets matched successfully")
        print(f"Linking Warnings: {' & '.join(success_msgs)} ✓")
        return

    # Create combined log file
    output_path = output_dir / "linking_warnings.log"

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("WATERBODY LINKING WARNINGS LOG\n")
        f.write("=" * 80 + "\n\n")
        f.write(
            "This file contains warnings about potential issues in the linking process.\n"
        )
        f.write("\n")

        # Section 1: Unmatched KML Points
        if unmatched_points:
            f.write("\n" + "=" * 80 + "\n")
            f.write("SECTION 1: UNMATCHED KML POINTS\n")
            f.write("=" * 80 + "\n\n")
            f.write(
                f"The following {len(unmatched_points)} KML points were not linked to any waterbody regulation.\n"
            )
            f.write("This could indicate:\n")
            f.write("  - The waterbody has no specific regulations in the synopsis\n")
            f.write(
                "  - The point was not matched to a waterbody polygon in preprocessing\n"
            )
            f.write("  - The name doesn't match any regulation entries\n")
            f.write("\n" + "-" * 80 + "\n\n")

            for point_name in sorted(unmatched_points):
                f.write(f"  ⚠ {point_name}\n")
        else:
            f.write("\n" + "=" * 80 + "\n")
            f.write("SECTION 1: UNMATCHED KML POINTS\n")
            f.write("=" * 80 + "\n\n")
            if all_kml_points:
                f.write(
                    f"✓ All {len(all_kml_points)} KML points matched to regulations\n"
                )
            else:
                f.write("No KML points found in index\n")

        # Section 2: Unmatched Correction Targets
        f.write("\n\n" + "=" * 80 + "\n")
        f.write("SECTION 2: UNMATCHED NAME CORRECTION TARGETS\n")
        f.write("=" * 80 + "\n\n")

        if unmatched_by_region:
            f.write(
                f"The following {correction_count} target names from NAME_CORRECTIONS were not matched in GIS.\n"
            )
            f.write("This could indicate:\n")
            f.write("  - The target name has a typo or incorrect spelling\n")
            f.write("  - The GIS feature is missing from the index\n")
            f.write("  - The feature is in a different management zone\n")
            f.write("  - The correction should be updated or removed\n")
            f.write("\n" + "-" * 80 + "\n")

            for region in sorted(unmatched_by_region.keys()):
                f.write(f"\n{region}\n")
                f.write("-" * 80 + "\n")

                for entry in unmatched_by_region[region]:
                    f.write(f"\n{entry['waterbody']}:\n")
                    for target in entry["targets"]:
                        f.write(f"  ⚠ {target}\n")
        else:
            f.write("✓ All correction targets matched successfully\n")

    # Print summary
    warnings = []
    if unmatched_points:
        warnings.append(f"{len(unmatched_points)} unmatched KML points")
    if correction_count > 0:
        warnings.append(f"{correction_count} unmatched correction targets")

    print(f"Linking Warnings: {output_path} ({', '.join(warnings)})")


def create_reverse_index(matched_results: Dict, output_dir: Path) -> None:
    """Create a reverse index: feature_id -> regulation data.
    
    This allows looking up regulations by GIS feature ID.
    
    Args:
        matched_results: Dictionary of matched waterbody results
        output_dir: Directory to save the reverse index
    """
    print(f"\n{'-'*60}\nCREATING REVERSE INDEX\n{'-'*60}")
    
    reverse_index = {}
    feature_count = 0
    
    for region, waterbodies in matched_results.items():
        for waterbody_key, wb_data in waterbodies.items():
            if wb_data["match_status"] != "matched":
                continue
                
            # Extract regulation summary (remove internal fields)
            regulation_summary = {
                "region": region,
                "waterbody_key": waterbody_key,
                "original_names": wb_data["original_waterbody_names"],
                "regulations": wb_data["regulations"],
            }
            
            # Add each GIS feature to the reverse index
            for gis_match in wb_data["gis_matches"]:
                feature_id = gis_match["feature_id"]
                
                # Features can be linked to multiple waterbodies (e.g., tributaries)
                # Store as list to handle multi-regulation features
                if feature_id not in reverse_index:
                    reverse_index[feature_id] = []
                    
                reverse_index[feature_id].append({
                    **regulation_summary,
                    "feature_type": gis_match["type"],
                    "feature_zone": gis_match.get("_source_zone"),
                    "matched_on_name": gis_match.get("_matched_on"),
                })
                feature_count += 1
    
    output_path = output_dir / "feature_regulation_index.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(reverse_index, f, indent=2, ensure_ascii=False)
    
    unique_features = len(reverse_index)
    print(f"Created reverse index: {unique_features} unique features, {feature_count} total feature-regulation links")
    print(f"Output: {output_path}")


def export_to_gdb(matched_results: Dict, waterbody_index: Dict, output_dir: Path) -> None:
    """Export matched waterbodies to a geodatabase.
    
    Creates a GDB with separate feature classes for streams, points, and polygons.
    Each feature includes regulation data as attributes.
    
    NOTE: Geometries are read from the source GDB, not from waterbody_index.json
    (which doesn't contain geometry to keep file size manageable).
    
    Args:
        matched_results: Dictionary of matched waterbody results
        waterbody_index: GIS waterbody index (used to find layer names)
        output_dir: Directory to create the GDB
    """
    print(f"\n{'-'*60}\nEXPORTING TO GEODATABASE\n{'-'*60}")
    
    try:
        import fiona
        from fiona.crs import CRS
        import shapely.geometry
        import geopandas as gpd
    except ImportError:
        print("Error: fiona, shapely, and geopandas are required for GDB export")
        print("Install with: conda install -c conda-forge fiona shapely geopandas")
        return
    
    # Find source GDB path (sibling to waterbody_index.json)
    script_dir = Path(__file__).parent
    source_gdb = script_dir / "output" / "fwa_preprocessing" / "FWA_Zone_Grouped.gdb"
    
    if not source_gdb.exists():
        print(f"Error: Source GDB not found: {source_gdb}")
        print("Run fwa_preprocessing.py first to create the GDB")
        return
    
    gdb_path = output_dir / "matched_waterbodies.gdb"
    
    # Remove existing GDB if present
    if gdb_path.exists():
        import shutil
        shutil.rmtree(gdb_path)
    
    # Build lookup: unique_id -> regulation data
    # For streams: LINEAR_FEATURE_ID
    # For polygons: WATERBODY_POLY_ID
    # For points: use layer + FID (points don't have unique IDs across layers)
    print("Building feature ID to regulation lookup...")
    feature_reg_lookup = {}
    matched_layer_set = set()  # Track which layers we need
    
    for region, waterbodies in matched_results.items():
        for waterbody_key, wb_data in waterbodies.items():
            if wb_data["match_status"] != "matched":
                continue
            
            # Serialize regulations to JSON string
            regulations_json = json.dumps(wb_data["regulations"], ensure_ascii=False)
            
            for gis_match in wb_data["gis_matches"]:
                feature_type = gis_match["type"]
                layer_name = gis_match.get("layer", "")
                
                if not layer_name:
                    continue
                
                matched_layer_set.add(layer_name)
                
                # Build attributes for this feature
                attributes = {
                    "FEATURE_ID": gis_match["feature_id"][:254],
                    "REGION": region[:100],
                    "WATERBODY": waterbody_key[:254],
                    "ORIG_NAMES": ", ".join(wb_data["original_waterbody_names"])[:254],
                    "FEATURE_TYP": feature_type[:50],
                    "SOURCE_ZON": str(gis_match.get("_source_zone", ""))[:10],
                    "MATCHED_ON": str(gis_match.get("_matched_on", ""))[:254],
                    "REGS_JSON": regulations_json[:32000],
                }
                
                # Add GIS attributes
                gis_attrs = gis_match.get("attributes", {})
                if "GNIS_NAME" in gis_attrs:
                    attributes["GNIS_NAME"] = str(gis_attrs.get("GNIS_NAME", ""))[:254]
                if "GNIS_NAME_1" in gis_attrs:
                    attributes["GNIS_NAME"] = str(gis_attrs.get("GNIS_NAME_1", ""))[:254]
                if "WATERBODY_POLY_ID" in gis_attrs:
                    attributes["WB_POLY_ID"] = str(gis_attrs["WATERBODY_POLY_ID"])[:50]
                
                # Determine unique key based on feature type
                if feature_type == "stream":
                    # Use LINEAR_FEATURE_ID for streams
                    unique_id = gis_attrs.get("LINEAR_FEATURE_ID")
                    if unique_id:
                        key = ("stream", str(unique_id))
                        feature_reg_lookup[key] = attributes
                elif feature_type in ["lake", "wetland", "manmade"]:
                    # Use WATERBODY_POLY_ID for polygons
                    unique_id = gis_attrs.get("WATERBODY_POLY_ID")
                    if unique_id:
                        key = ("polygon", str(unique_id))
                        feature_reg_lookup[key] = attributes
                elif feature_type == "point":
                    # Points: use layer + feature_id (no unique ID field)
                    key = ("point", layer_name, gis_match["feature_id"])
                    feature_reg_lookup[key] = attributes
    
    print(f"Found {len(feature_reg_lookup)} feature-regulation links across {len(matched_layer_set)} layers")
    
    # Read features from source GDB and add regulation attributes
    print("Reading features from source GDB and adding regulations...")
    
    features_by_type = {
        "streams": [],
        "points": [],
        "polygons": []
    }
    
    feature_count = 0
    skipped_count = 0
    
    # Track KML points with polygon links: (zone, poly_id, poly_type) -> reg_attrs
    point_polygon_links = {}
    
    # Process each matched layer
    for layer_name in sorted(matched_layer_set):
        try:
            with fiona.open(str(source_gdb), layer=layer_name) as src:
                # Map layer type
                if layer_name.startswith("STREAMS_"):
                    output_type = "streams"
                    lookup_type = "stream"
                    unique_field = "LINEAR_FEATURE_ID"
                elif layer_name.startswith("LABELED_POINTS_"):
                    output_type = "points"
                    lookup_type = "point"
                    unique_field = None  # Points use layer + FID
                elif layer_name.startswith(("LAKES_", "WETLANDS_", "MANMADE_")):
                    output_type = "polygons"
                    lookup_type = "polygon"
                    unique_field = "WATERBODY_POLY_ID"
                else:
                    continue
                
                # Read features
                for feat in src:
                    # Build lookup key based on feature type
                    if lookup_type == "point":
                        # Points: use layer + FID
                        fid = str(feat['id'])
                        key = (lookup_type, layer_name, fid)
                    else:
                        # Streams and polygons: use unique ID field
                        unique_id = feat['properties'].get(unique_field)
                        if not unique_id:
                            continue
                        key = (lookup_type, str(unique_id))
                    
                    # Check if this feature has regulations
                    if key not in feature_reg_lookup:
                        continue
                    
                    # Get regulation attributes
                    reg_attrs = feature_reg_lookup[key]
                    
                    # Merge with GDB properties (regulation attrs take precedence)
                    properties = {**feat['properties'], **reg_attrs}
                    
                    features_by_type[output_type].append({
                        "geometry": feat['geometry'],
                        "properties": properties
                    })
                    feature_count += 1
                    
                    # If this is a KML point with polygon links, track them
                    if output_type == "points":
                        # Extract zone from layer name (e.g., "LABELED_POINTS_ZONE_2" -> "2")
                        zone = layer_name.split("_")[-1]
                        
                        # Check for polygon ID fields
                        poly_links = [
                            ("LAKE_POLY_ID", "lake", f"LAKES_ZONE_{zone}"),
                            ("WETLAND_POLY_ID", "wetland", f"WETLANDS_ZONE_{zone}"),
                            ("MANMADE_POLY_ID", "manmade", f"MANMADE_ZONE_{zone}"),
                        ]
                        
                        for poly_field, poly_type, poly_layer in poly_links:
                            poly_id = feat['properties'].get(poly_field)
                            if poly_id and str(poly_id) != "nan" and str(poly_id) != "None":
                                # Store link with regulation attrs
                                link_key = (poly_layer, str(poly_id), poly_type)
                                point_polygon_links[link_key] = reg_attrs.copy()
                                # Mark as linked from point
                                point_polygon_links[link_key]["LINKED_PT"] = "Yes"
                    
        except Exception as e:
            print(f"Warning: Error reading layer {layer_name}: {e}")
            skipped_count += 1
    
    print(f"Collected {feature_count} features with regulations")
    
    # Add linked polygons from KML points
    if point_polygon_links:
        print(f"Processing {len(point_polygon_links)} polygon links from KML points...")
        linked_poly_count = 0
        
        for (poly_layer, poly_id, poly_type), reg_attrs in point_polygon_links.items():
            try:
                with fiona.open(str(source_gdb), layer=poly_layer) as src:
                    # Search for polygon with matching WATERBODY_POLY_ID
                    for feat in src:
                        feat_poly_id = str(feat['properties'].get('WATERBODY_POLY_ID', ''))
                        if feat_poly_id == poly_id:
                            # Found the linked polygon - add it with regulations
                            properties = {**feat['properties'], **reg_attrs}
                            
                            features_by_type["polygons"].append({
                                "geometry": feat['geometry'],
                                "properties": properties
                            })
                            linked_poly_count += 1
                            feature_count += 1
                            break  # Found the polygon, move to next link
                            
            except Exception as e:
                # Layer might not exist (e.g., no lakes in this zone)
                pass
        
        print(f"Added {linked_poly_count} linked polygons from KML points")
    
    # Define schema for regulation fields (will be added to existing GDB properties)
    regulation_fields = {
        "REGION": "str:100",
        "WATERBODY": "str:254",
        "ORIG_NAMES": "str:254",
        "MATCHED_ON": "str:254",
        "REGS_JSON": "str:32000",
        "LINKED_PT": "str:10",  # Indicates polygon was linked from KML point
    }
    
    # Auto-detect schema from first feature of each type
    schemas = {}
    for layer_name, features in features_by_type.items():
        if not features:
            continue
        
        # Get sample feature to determine geometry type and properties
        sample_feat = features[0]
        geom_type = sample_feat['geometry']['type']
        
        # Promote to Multi* types
        if geom_type == "LineString":
            geom_type = "MultiLineString"
        elif geom_type == "Polygon":
            geom_type = "MultiPolygon"
        
        # Collect all unique property keys from all features
        all_props = {}
        for feat in features:
            for key, value in feat['properties'].items():
                if key not in all_props:
                    # Determine type
                    if isinstance(value, int):
                        all_props[key] = "int"
                    elif isinstance(value, float):
                        all_props[key] = "float"
                    else:
                        all_props[key] = "str:254"
        
        # Override with regulation field types
        all_props.update(regulation_fields)
        
        schemas[layer_name] = {
            "geometry": geom_type,
            "properties": all_props
        }
    
    print(f"Created schemas for {len(schemas)} output layers")
    
    # Write each layer
    for layer_name, features in features_by_type.items():
        if not features:
            print(f"Skipping {layer_name}: no features")
            continue
        
        schema = schemas[layer_name]
        
        with fiona.open(
            str(gdb_path),  # GDB path only
            "w",
            driver="OpenFileGDB",
            layer=layer_name,  # Layer name separate
            crs=CRS.from_epsg(3005),  # BC Albers
            schema=schema,
        ) as dst:
            for feature in features:
                # Ensure all schema properties are present
                props = {}
                for key in schema["properties"].keys():
                    value = feature["properties"].get(key, "")
                    # Handle None values
                    if value is None:
                        value = "" if schema["properties"][key].startswith("str") else 0
                    props[key] = value
                
                geom = feature["geometry"]
                geom_type = geom['type']
                
                # Convert single geometries to Multi* if needed for schema
                if schema["geometry"] == "MultiLineString" and geom_type == "LineString":
                    geom = {"type": "MultiLineString", "coordinates": [geom["coordinates"]]}
                elif schema["geometry"] == "MultiPolygon" and geom_type == "Polygon":
                    geom = {"type": "MultiPolygon", "coordinates": [geom["coordinates"]]}
                
                dst.write({
                    "geometry": geom,
                    "properties": props
                })
        
        print(f"Wrote {len(features)} features to {layer_name} layer")
    
    print(f"GDB created: {gdb_path} ({feature_count} total features)")


def load_input_data(script_dir: Path, grouped_results_path: Optional[str] = None) -> Tuple[Dict, Dict, Path, Path]:
    """Load input data files.
    
    Args:
        script_dir: Script directory path
        grouped_results_path: Optional custom path to grouped_results.json
    
    Returns:
        Tuple of (grouped_dict, index, grouped_path, index_path)
    """
    # Determine grouped_results path
    if grouped_results_path:
        grouped_path = Path(grouped_results_path)
        if not grouped_path.is_absolute():
            grouped_path = script_dir / grouped_results_path
    else:
        grouped_path = script_dir / "output" / "llm_parser" / "grouped_results.json"

    index_path = script_dir / "output" / "fwa_preprocessing" / "waterbody_index.json"

    if not grouped_path.exists():
        raise FileNotFoundError(f"Input file not found: {grouped_path}")

    if not index_path.exists():
        raise FileNotFoundError(f"Input file not found: {index_path}")

    print("Loading data...")
    with open(grouped_path, "r", encoding="utf-8") as f:
        grouped_data = json.load(f)

    with open(index_path, "r", encoding="utf-8") as f:
        index = json.load(f)

    # Support both 'grouped' and 'regions' keys
    if "regions" in grouped_data:
        grouped_dict = grouped_data["regions"]
        print(f"Found {len(grouped_dict)} regions in data (using 'regions' key)")
    elif "grouped" in grouped_data:
        grouped_dict = grouped_data["grouped"]
        print(f"Found {len(grouped_dict)} regions in data (using 'grouped' key)")
    else:
        raise ValueError(f"Could not find 'regions' or 'grouped' key in {grouped_path}")
    
    return grouped_dict, index, grouped_path, index_path


def run_matching(grouped_dict: Dict, index: Dict, output_dir: Path) -> Tuple[Dict, List, Dict, Set, Set]:
    """Run the waterbody matching process.
    
    Args:
        grouped_dict: Dictionary of grouped regulations by region
        index: GIS waterbody index
        output_dir: Output directory
    
    Returns:
        Tuple of (results, unmatched_rows, stats, all_kml_points, matched_kml_points)
    """
    print(f"\n{'-'*60}\nSTARTING WATERBODY MATCHING\n{'-'*60}")

    # Extract KML points from index
    all_kml_points = extract_kml_points_from_index(index)
    matched_kml_points = set()

    if all_kml_points:
        print(f"Found {len(all_kml_points)} KML points in index for tracking")

    results = {}
    unmatched_rows = []
    stats = {"matched": 0, "total": 0, "ignored": 0}

    # Process each region
    for region, waterbodies in grouped_dict.items():
        if region == "null" or not waterbodies:
            continue

        print(f"\n{C_CYAN}REGION:{C_RESET} {region}")

        if region not in results:
            results[region] = {}

        for waterbody_key, regulations in waterbodies.items():
            if not regulations:
                continue

            # Check if ignored
            is_ignored = False
            if region in NAME_CORRECTIONS and waterbody_key in NAME_CORRECTIONS[region]:
                correction = NAME_CORRECTIONS[region][waterbody_key]
                if correction.ignored:
                    is_ignored = True
                    stats["ignored"] += 1
                    stats["total"] += 1
                    print(
                        f"  {C_CYAN}[IGNORED]{C_RESET} '{waterbody_key}' - {correction.note}"
                    )
                    continue

            stats["total"] += 1

            result = process_waterbody(
                region, waterbody_key, regulations, index, stats, matched_kml_points
            )

            results[region][waterbody_key] = attrs.asdict(result)

            if result.match_status == "unmatched":
                # Get name variations and note
                name_variations = ""
                note = ""
                if (
                    region in NAME_CORRECTIONS
                    and waterbody_key in NAME_CORRECTIONS[region]
                ):
                    correction = NAME_CORRECTIONS[region][waterbody_key]
                    name_variations = ", ".join(correction.target_names)
                    note = correction.note

                # Extract similar names
                similar_names = []
                for suggestion in result.debug_suggestions or []:
                    name_only = suggestion.rsplit(" (zone ", 1)[0]
                    if name_only not in similar_names:
                        similar_names.append(name_only)

                unmatched_rows.append(
                    [
                        region,
                        waterbody_key,
                        waterbody_key.lower(),
                        name_variations,
                        note,
                        result.debug_mus or [],
                        ", ".join(similar_names),
                    ]
                )

    return results, unmatched_rows, stats, all_kml_points, matched_kml_points


def save_all_results(results: Dict, unmatched_rows: List, stats: Dict, 
                     all_kml_points: Set, matched_kml_points: Set, output_dir: Path) -> None:
    """Save all matching results to files.
    
    Args:
        results: Matched waterbody results
        unmatched_rows: Unmatched waterbody data
        stats: Matching statistics
        all_kml_points: All KML points from index
        matched_kml_points: KML points that were matched
        output_dir: Output directory
    """
    print(f"\n{'-'*60}")
    print(f"COMPLETE: Matched {stats['matched']}/{stats['total']} waterbodies")
    if stats.get("ignored", 0) > 0:
        print(f"  {stats['ignored']} waterbodies ignored")
    if "warnings" in stats:
        print(f"  {stats['warnings']} matches have warnings")
    
    save_matched_results(results, output_dir)
    save_unmatched_csv(
        unmatched_rows, output_dir, ignored_count=stats.get("ignored", 0)
    )
    save_warnings_log(results, output_dir)
    save_linking_warnings(all_kml_points, matched_kml_points, results, output_dir)
    
    print(f"{'-'*60}")


def main():
    """Main entry point with argument parsing for modular execution."""
    parser = argparse.ArgumentParser(
        description="Link fishing regulations to GIS waterbody features",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline (matching + reverse index + GDB export)
  python waterbody_linking.py
  
  # Run only matching step
  python waterbody_linking.py --step match
  
  # Export to GDB (requires matched_waterbodies.json)
  python waterbody_linking.py --step export-gdb
  
  # Create reverse index only
  python waterbody_linking.py --step reverse-index
  
  # Custom input file
  python waterbody_linking.py --input output/llm_parser/grouped_results.json
  
  # Run matching and reverse index, skip GDB export
  python waterbody_linking.py --skip-gdb
        """
    )
    parser.add_argument(
        "--input",
        help="Path to grouped_results.json file (relative to scripts/ or absolute)",
        metavar="PATH"
    )
    parser.add_argument(
        "--step",
        choices=["match", "export-gdb", "reverse-index", "all"],
        default="all",
        help="Which step to run (default: all)"
    )
    parser.add_argument(
        "--skip-gdb",
        action="store_true",
        help="Skip GDB export in full pipeline"
    )
    parser.add_argument(
        "--skip-reverse-index",
        action="store_true",
        help="Skip reverse index creation in full pipeline"
    )
    
    args = parser.parse_args()
    script_dir = Path(__file__).parent
    output_dir = script_dir / "output" / "waterbody_linking"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # STEP 1: Load data (always needed)
        if args.step in ["match", "all"]:
            grouped_dict, index, grouped_path, index_path = load_input_data(
                script_dir, args.input
            )
        elif args.step in ["export-gdb", "reverse-index"]:
            # Load only what's needed for export/indexing
            matched_path = output_dir / "matched_waterbodies.json"
            if not matched_path.exists():
                print(f"Error: {matched_path} not found")
                print("Run matching step first: python waterbody_linking.py --step match")
                return
            
            with open(matched_path, "r", encoding="utf-8") as f:
                results = json.load(f)
            
            # Load index if needed for GDB export
            if args.step == "export-gdb":
                index_path = script_dir / "output" / "fwa_preprocessing" / "waterbody_index.json"
                if not index_path.exists():
                    print(f"Error: {index_path} not found")
                    return
                with open(index_path, "r", encoding="utf-8") as f:
                    index = json.load(f)
        
        # STEP 2: Run matching
        if args.step in ["match", "all"]:
            results, unmatched_rows, stats, all_kml_points, matched_kml_points = run_matching(
                grouped_dict, index, output_dir
            )
            save_all_results(
                results, unmatched_rows, stats, all_kml_points, matched_kml_points, output_dir
            )
        
        # STEP 3: Create reverse index
        if args.step == "reverse-index" or (args.step == "all" and not args.skip_reverse_index):
            create_reverse_index(results, output_dir)
        
        # STEP 4: Export to GDB
        if args.step == "export-gdb" or (args.step == "all" and not args.skip_gdb):
            export_to_gdb(results, index, output_dir)
        
        print(f"\n{C_GREEN}✓ Pipeline complete{C_RESET}")
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
    main()
