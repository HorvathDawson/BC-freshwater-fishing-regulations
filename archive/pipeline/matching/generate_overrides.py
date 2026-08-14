"""
One-time migration: v1 linking_corrections.py → v2 overrides.json.

Reads ALL manual corrections from regulation_mapping/linking_corrections.py,
transforms them into OverrideEntry objects,  validates every identifier
against the match_table.json (base entries), and writes overrides.json.

Also transforms FeatureNameVariation entries into FeatureDisplayName format.

Usage
-----
    python -m pipeline.matching.generate_overrides
    python -m pipeline.matching.generate_overrides --dry-run
    python -m pipeline.matching.generate_overrides \
        --match-table path/to/match_table.json \
        --out path/to/overrides.json

The output file (overrides.json) becomes the v2 source of truth for manual
corrections.  After migration, linking_corrections.py data dicts are
considered frozen — all new corrections go into overrides.json.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from .match_table import BaseEntry, FeatureDisplayName, OverrideEntry
from .reg_models import MatchCriteria

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Region mapping: v1 short keys → v2 full region strings
# ---------------------------------------------------------------------------

_REGION_MAP: Dict[str, str] = {
    "Region 1": "REGION 1 - Vancouver Island",
    "Region 2": "REGION 2 - Lower Mainland",
    "Region 3": "REGION 3 - Thompson-Nicola",
    "Region 4": "REGION 4 - Kootenay",
    "Region 5": "REGION 5 - Cariboo",
    "Region 6": "REGION 6 - Skeena",
    "Region 7A": "REGION 7A - Omineca",
    "Region 7B": "REGION 7B - Peace",
    "Region 8": "REGION 8 - Okanagan",
}

_REVERSE_REGION_MAP: Dict[str, str] = {v: k for k, v in _REGION_MAP.items()}


def _v1_region(full_region: Optional[str]) -> Optional[str]:
    """Convert v2 full region string → v1 short key."""
    if full_region is None:
        return None
    return _REVERSE_REGION_MAP.get(full_region, full_region)


def _v2_region(short_region: str) -> str:
    """Convert v1 short region key → v2 full region string."""
    return _REGION_MAP.get(short_region, short_region)


# ---------------------------------------------------------------------------
# Build a criteria lookup from the base match table
# ---------------------------------------------------------------------------


def _load_base_criteria(match_table_path: Path) -> Dict[Tuple[Optional[str], str], List[str]]:
    """Load match_table.json and return {(region, name_verbatim): mus} lookup.

    This provides the mus values for each synopsis row so overrides can be
    built with the correct criteria.
    """
    with open(match_table_path, encoding="utf-8") as f:
        entries = json.load(f)
    lookup: Dict[Tuple[Optional[str], str], List[str]] = {}
    for entry in entries:
        c = entry.get("criteria", {})
        key = (c.get("region"), c["name_verbatim"])
        lookup[key] = c.get("mus", [])
    return lookup


# ---------------------------------------------------------------------------
# Transform v1 corrections → v2 OverrideEntry
# ---------------------------------------------------------------------------


def _transform_direct_matches(
    direct_matches: Dict[str, Dict[str, Any]],
    criteria_lookup: Dict[Tuple[Optional[str], str], List[str]],
) -> List[OverrideEntry]:
    """Transform DIRECT_MATCHES → OverrideEntry list."""
    overrides: List[OverrideEntry] = []

    for v1_region, entries in direct_matches.items():
        v2_region = _v2_region(v1_region)
        for name, dm in entries.items():
            mus = criteria_lookup.get((v2_region, name), [])
            if not mus:
                logger.warning(
                    f"DirectMatch for '{name}' in {v1_region} has no matching "
                    f"synopsis row in match_table.json — using empty mus."
                )

            override = OverrideEntry(
                criteria=MatchCriteria(
                    name_verbatim=name,
                    region=v2_region,
                    mus=mus,
                ),
                note=dm.note,
                gnis_ids=dm.gnis_ids or [],
                waterbody_keys=dm.waterbody_keys or [],
                fwa_watershed_codes=dm.fwa_watershed_codes or [],
                blue_line_keys=dm.blue_line_keys or [],
                linear_feature_ids=dm.linear_feature_ids or [],
                waterbody_poly_ids=dm.waterbody_poly_ids or [],
                ungazetted_waterbody_id=dm.ungazetted_waterbody_id,
                additional_info=dm.additional_info,
            )
            overrides.append(override)

    return overrides


def _transform_skip_entries(
    skip_entries: Dict[str, Dict[str, Any]],
    criteria_lookup: Dict[Tuple[Optional[str], str], List[str]],
) -> List[OverrideEntry]:
    """Transform SKIP_ENTRIES → OverrideEntry list with skip=True."""
    overrides: List[OverrideEntry] = []

    for v1_region, entries in skip_entries.items():
        v2_region = _v2_region(v1_region)
        for name, se in entries.items():
            mus = criteria_lookup.get((v2_region, name), [])
            reason_parts = []
            if se.not_found:
                reason_parts.append("not_found")
            if se.ignored:
                reason_parts.append("ignored")
            reason = ", ".join(reason_parts) if reason_parts else "skipped"

            override = OverrideEntry(
                criteria=MatchCriteria(
                    name_verbatim=name,
                    region=v2_region,
                    mus=mus,
                ),
                note=se.note,
                skip=True,
                skip_reason=reason,
            )
            overrides.append(override)

    return overrides


def _transform_admin_matches(
    admin_matches: Dict[str, Dict[str, Any]],
    criteria_lookup: Dict[Tuple[Optional[str], str], List[str]],
) -> List[OverrideEntry]:
    """Transform ADMIN_DIRECT_MATCHES → OverrideEntry list with admin_targets."""
    overrides: List[OverrideEntry] = []

    for v1_region, entries in admin_matches.items():
        v2_region = _v2_region(v1_region)
        for name, adm in entries.items():
            mus = criteria_lookup.get((v2_region, name), [])

            admin_targets = []
            for at in adm.admin_targets:
                target: Dict[str, str] = {
                    "layer": at.layer,
                    "feature_id": at.feature_id,
                }
                if at.code_filter:
                    target["code_filter"] = at.code_filter
                admin_targets.append(target)

            # Convert FeatureType enums → string names if present
            admin_feature_types = None
            if adm.feature_types:
                admin_feature_types = [ft.value for ft in adm.feature_types]

            override = OverrideEntry(
                criteria=MatchCriteria(
                    name_verbatim=name,
                    region=v2_region,
                    mus=mus,
                ),
                note=adm.note,
                admin_targets=admin_targets,
                admin_feature_types=admin_feature_types,
                additional_info=adm.additional_info,
            )
            overrides.append(override)

    return overrides


def _transform_name_variation_links(
    name_variation_links: Dict[str, Dict[str, Any]],
    criteria_lookup: Dict[Tuple[Optional[str], str], List[str]],
    overrides: List[OverrideEntry],
) -> List[OverrideEntry]:
    """Transform NAME_VARIATION_LINKS → skip entries + inject name_variants.

    For each NameVariationLink:
    1. Create a skip OverrideEntry for the variant name
    2. Find the primary entry (in overrides or base table) and add the
       variant as a name_variant so search still finds it

    Returns the new skip entries. Modifies existing overrides in-place
    to add name_variants.
    """
    new_overrides: List[OverrideEntry] = []

    # Build lookup of existing overrides by (region, name) for injecting variants
    override_idx: Dict[Tuple[str, str], OverrideEntry] = {}
    for entry in overrides:
        key = (entry.criteria.region or "", entry.criteria.name_verbatim)
        override_idx[key] = entry

    for v1_region, entries in name_variation_links.items():
        v2_region = _v2_region(v1_region)
        for variant_name, nvl in entries.items():
            mus = criteria_lookup.get((v2_region, variant_name), [])

            # Create skip entry for the variant
            skip_entry = OverrideEntry(
                criteria=MatchCriteria(
                    name_verbatim=variant_name,
                    region=v2_region,
                    mus=mus,
                ),
                note=nvl.note,
                skip=True,
                skip_reason=f"Name variant of: {nvl.primary_name}",
            )
            new_overrides.append(skip_entry)

            # Inject variant name onto the primary entry if it exists
            primary_key = (v2_region, nvl.primary_name)
            primary = override_idx.get(primary_key)
            if primary:
                if variant_name not in primary.name_variants:
                    primary.name_variants.append(variant_name)
            else:
                logger.warning(
                    f"NameVariationLink '{variant_name}' → '{nvl.primary_name}' "
                    f"in {v1_region}: primary entry not found in overrides. "
                    f"The variant name will be a skip entry but won't be "
                    f"injected as a name_variant on the primary."
                )

    return new_overrides


def _transform_feature_name_variations(
    feature_name_variations: Dict[str, list],
) -> List[Dict[str, Any]]:
    """Transform FEATURE_NAME_VARIATIONS → FeatureDisplayName dicts."""
    display_names: List[Dict[str, Any]] = []

    for _region, variations in feature_name_variations.items():
        for fnv in variations:
            fdn = FeatureDisplayName(
                display_name=fnv.name,
                note=fnv.note,
                blue_line_keys=fnv.blue_line_keys or [],
                waterbody_keys=fnv.waterbody_keys or [],
            )
            display_names.append(fdn.to_dict())

    return display_names


def _inject_base_name_variants(
    overrides: List[OverrideEntry],
    criteria_lookup: Dict[Tuple[Optional[str], str], List[str]],
) -> int:
    """Add base_name variants to overrides whose name contains parenthetical content.

    The base_entry_builder emits a second entry with parenthetical content
    stripped (e.g., "ECHOES LAKE (near Kimberley)" → "ECHOES LAKE").  If an
    override covers the full name, the stripped name must be a name_variant
    so MatchTable.lookup() resolves both forms.

    Modifies overrides in-place.  Returns count of variants added.
    """
    import re

    RE_BRACKETS = re.compile(r"\s*\([^)]*\)\s*")
    RE_SPACES = re.compile(r"\s+")

    # Build set of all synopsis names per region for quick existence check
    names_by_region: Dict[Optional[str], Set[str]] = {}
    for (region, name) in criteria_lookup:
        names_by_region.setdefault(region, set()).add(name)

    count = 0
    for entry in overrides:
        name = entry.criteria.name_verbatim
        if "(" not in name:
            continue
        stripped = RE_BRACKETS.sub(" ", name).strip()
        stripped = RE_SPACES.sub(" ", stripped)
        if not stripped or stripped == name:
            continue

        region = entry.criteria.region
        # Only add if the stripped name actually exists as a synopsis row
        if region in names_by_region and stripped in names_by_region[region]:
            if stripped not in entry.name_variants:
                entry.name_variants.append(stripped)
                count += 1

    return count


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _validate_overrides(
    overrides: List[OverrideEntry],
    criteria_lookup: Dict[Tuple[Optional[str], str], List[str]],
) -> List[str]:
    """Validate override entries. Returns list of error messages.

    Checks:
    - Non-skip entries must have at least one identifier
    - Entries should have a matching synopsis row (warns, not error)
    """
    errors: List[str] = []

    for entry in overrides:
        label = f"[{entry.criteria.region}] {entry.criteria.name_verbatim}"

        # Non-skip entries must reference something
        if not entry.skip and not entry.has_match:
            errors.append(
                f"ERROR: {label} — not skipped but has no identifiers "
                f"(gnis_ids, waterbody_keys, fwa_watershed_codes, "
                f"blue_line_keys, linear_feature_ids, waterbody_poly_ids, "
                f"ungazetted_waterbody_id, or admin_targets)."
            )

        # Check synopsis row exists
        key = (entry.criteria.region, entry.criteria.name_verbatim)
        if key not in criteria_lookup:
            logger.info(
                f"INFO: {label} — no matching synopsis row in match_table.json. "
                f"This override may be for a future or removed entry."
            )

    return errors


# ---------------------------------------------------------------------------
# Main migration
# ---------------------------------------------------------------------------


def generate(
    match_table_path: Path,
    out_path: Path,
    feature_display_path: Optional[Path] = None,
    dry_run: bool = False,
) -> None:
    """Run the full v1 → v2 migration."""
    # Import v1 corrections (uses lazy import to avoid geopandas at module level)
    from regulation_mapping.linking_corrections import (
        ADMIN_DIRECT_MATCHES,
        DIRECT_MATCHES,
        FEATURE_NAME_VARIATIONS,
        NAME_VARIATION_LINKS,
        SKIP_ENTRIES,
    )

    print(f"Loading base criteria from: {match_table_path}")
    criteria_lookup = _load_base_criteria(match_table_path)
    print(f"  {len(criteria_lookup)} synopsis entries loaded.")

    # --- Pass 1: Transform all correction types ---
    print("\nTransforming corrections...")

    overrides: List[OverrideEntry] = []

    dm_entries = _transform_direct_matches(DIRECT_MATCHES, criteria_lookup)
    print(f"  DirectMatch:        {len(dm_entries)} entries")
    overrides.extend(dm_entries)

    skip_entries = _transform_skip_entries(SKIP_ENTRIES, criteria_lookup)
    print(f"  SkipEntry:          {len(skip_entries)} entries")
    overrides.extend(skip_entries)

    admin_entries = _transform_admin_matches(ADMIN_DIRECT_MATCHES, criteria_lookup)
    print(f"  AdminDirectMatch:   {len(admin_entries)} entries")
    overrides.extend(admin_entries)

    # --- Pass 2: Name variation links (modifies existing overrides) ---
    nvl_entries = _transform_name_variation_links(
        NAME_VARIATION_LINKS, criteria_lookup, overrides
    )
    print(f"  NameVariationLink:  {len(nvl_entries)} entries (skip)")
    overrides.extend(nvl_entries)

    # --- Pass 3: Inject base_name variants ---
    # The match_table has base_name entries where parenthetical content was
    # stripped (e.g., 'ECHOES LAKE (near Kimberley)' → 'ECHOES LAKE').
    # If an override covers the full name, the stripped version must be
    # added as a name_variant so MatchTable lookup finds it.
    base_name_variants = _inject_base_name_variants(overrides, criteria_lookup)
    print(f"  Base name variants: {base_name_variants} injected")

    print(f"\n  TOTAL overrides: {len(overrides)}")

    # --- Feature display names (separate output) ---
    feature_display = _transform_feature_name_variations(FEATURE_NAME_VARIATIONS)
    print(f"  FeatureDisplayName: {len(feature_display)} entries")

    # --- Validate ---
    print("\nValidating...")
    errors = _validate_overrides(overrides, criteria_lookup)

    if errors:
        print(f"\n{'=' * 60}")
        print(f"  VALIDATION ERRORS: {len(errors)}")
        print(f"{'=' * 60}")
        for err in errors:
            print(f"  {err}")
        print(f"{'=' * 60}\n")
        sys.exit(1)
    else:
        print("  All overrides valid.")

    # --- Summary ---
    _print_summary(overrides)

    if dry_run:
        print("\n[DRY RUN] No files written.")
        return

    # --- Write overrides.json ---
    out_path.parent.mkdir(parents=True, exist_ok=True)
    data = [e.to_dict() for e in overrides]
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"\nWrote {len(overrides)} overrides → {out_path}")

    # --- Write feature_display_names.json ---
    if feature_display:
        fd_path = feature_display_path or out_path.parent / "feature_display_names.json"
        with open(fd_path, "w", encoding="utf-8") as f:
            json.dump(feature_display, f, indent=2, ensure_ascii=False)
        print(f"Wrote {len(feature_display)} feature display names → {fd_path}")


def _print_summary(overrides: List[OverrideEntry]) -> None:
    """Print a breakdown of override types."""
    from collections import Counter

    skip_count = sum(1 for e in overrides if e.skip)
    matched_count = sum(1 for e in overrides if e.has_match)
    admin_count = sum(1 for e in overrides if e.admin_targets)

    id_type_counts: Counter = Counter()
    for e in overrides:
        if e.gnis_ids:
            id_type_counts["gnis_ids"] += 1
        if e.waterbody_keys:
            id_type_counts["waterbody_keys"] += 1
        if e.fwa_watershed_codes:
            id_type_counts["fwa_watershed_codes"] += 1
        if e.blue_line_keys:
            id_type_counts["blue_line_keys"] += 1
        if e.linear_feature_ids:
            id_type_counts["linear_feature_ids"] += 1
        if e.waterbody_poly_ids:
            id_type_counts["waterbody_poly_ids"] += 1
        if e.ungazetted_waterbody_id:
            id_type_counts["ungazetted_waterbody_id"] += 1
        if e.admin_targets:
            id_type_counts["admin_targets"] += 1

    region_counts: Counter = Counter(
        e.criteria.region or "NO REGION" for e in overrides
    )

    print(f"\n{'=' * 60}")
    print(f"  OVERRIDE SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Total:     {len(overrides)}")
    print(f"  Matched:   {matched_count}")
    print(f"  Skipped:   {skip_count}")
    print(f"  Admin:     {admin_count}")
    print()
    print(f"  By identifier type:")
    for id_type, count in sorted(id_type_counts.items(), key=lambda x: -x[1]):
        print(f"    {id_type:<25} {count:>4}")
    print()
    print(f"  By region:")
    for region, count in sorted(region_counts.items()):
        print(f"    {region:<35} {count:>4}")
    print(f"{'=' * 60}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    from project_config import get_config
    import yaml

    parser = argparse.ArgumentParser(
        description="Migrate v1 linking_corrections → v2 overrides.json"
    )
    parser.add_argument("--match-table", help="Path to match_table.json")
    parser.add_argument("--out", help="Output overrides.json path")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print summary without writing files",
    )
    args = parser.parse_args()

    config = get_config()
    with open(config.project_root / "config.yaml") as f:
        cfg = yaml.safe_load(f)

    match_table_path = (
        Path(args.match_table)
        if args.match_table
        else config.project_root
        / cfg["output"]["pipeline"]["match_table"]
    )
    # Overrides live next to the code (manually curated), not in output/
    out_path = (
        Path(args.out)
        if args.out
        else Path(__file__).resolve().parent / "overrides.json"
    )

    generate(
        match_table_path=match_table_path,
        out_path=out_path,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
