"""One-time conversion: v1 ZoneRegulation Python objects → JSON entries.

Run:
    python -m regulation_mapping_v2.enrichment.scripts.convert_zone_regs

Reads zone_base_regulations.py (Python dataclasses) and writes all entries
(including disabled) to base_regulations.json alongside the existing
provincial entries.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from fwa_pipeline.metadata_builder import FeatureType
from regulation_mapping.admin_target import AdminTarget
from regulation_mapping.zone_base_regulations import ZONE_BASE_REGULATIONS

# v1 FeatureType enum → v2 string mapping
FEATURE_TYPE_MAP = {
    FeatureType.STREAM: "stream",
    FeatureType.LAKE: "lake",
    FeatureType.WETLAND: "wetland",
    FeatureType.MANMADE: "manmade",
}

OUTPUT_PATH = Path(__file__).resolve().parent.parent / "base_regulations.json"

# Existing provincial regulations (preserve as-is)
PROVINCIAL_REGS = [
    {
        "reg_id": "PROV_NAT_PARKS_CLOSED",
        "source": "provincial",
        "zone_ids": [],
        "rule_text": "Freshwater fishing is prohibited in National Parks unless opened under the National Parks Fishing Regulations. Where open, anglers require a National Park Fishing Permit to fish in park waters.",
        "restriction": {
            "type": "Closed",
            "details": "Fishing prohibited unless opened under National Parks Regulations. National Park Fishing Permit required where open.",
        },
        "admin_targets": [{"layer": "parks_nat"}],
        "buffer_m": 500.0,
        "notes": "Applies to all 7 National Parks in BC. Source: Provincial Regulations, 2025-2027 Synopsis, pages 9-10.",
    },
    {
        "reg_id": "PROV_ECO_RESERVES_CLOSED",
        "source": "provincial",
        "zone_ids": [],
        "rule_text": "Fishing is prohibited in Ecological Reserves in B.C.",
        "restriction": {
            "type": "Closed",
            "details": "Fishing prohibited in all Ecological Reserves.",
        },
        "admin_targets": [{"layer": "eco_reserves"}],
        "buffer_m": 500.0,
        "notes": "Applies to all Ecological Reserves province-wide. Source: Provincial Regulations, 2025-2027 Synopsis, page 9.",
    },
]


def _convert_feature_types(ft_list):
    """Convert v1 FeatureType enums to v2 strings."""
    if ft_list is None:
        return None
    return [FEATURE_TYPE_MAP[ft] for ft in ft_list if ft in FEATURE_TYPE_MAP]


def _convert_admin_targets(targets):
    """Convert v1 AdminTarget namedtuples to v2 dicts."""
    if targets is None:
        return None
    result = []
    for t in targets:
        d = {"layer": t.layer}
        if t.feature_id is not None:
            d["feature_id"] = t.feature_id
        if t.code_filter is not None:
            d["code_filter"] = t.code_filter
        result.append(d)
    return result


def convert_one(zr) -> dict:
    """Convert a single v1 ZoneRegulation to v2 JSON dict."""
    entry = {
        "reg_id": zr.regulation_id,
        "source": "zone",
        "zone_ids": zr.zone_ids or [],
        "rule_text": zr.rule_text,
        "restriction": zr.restriction,
        "notes": zr.notes,
    }

    # Feature types
    ft = _convert_feature_types(zr.feature_types)
    if ft is not None:
        entry["feature_types"] = ft

    # MU filters
    if zr.mu_ids is not None:
        entry["mu_ids"] = zr.mu_ids
    if zr.exclude_mu_ids is not None:
        entry["exclude_mu_ids"] = zr.exclude_mu_ids
    if zr.include_mu_ids is not None:
        entry["include_mu_ids"] = zr.include_mu_ids

    # Dates
    if zr.dates is not None:
        entry["dates"] = zr.dates

    # Scope location
    if zr.scope_location is not None:
        entry["scope_location"] = zr.scope_location

    # Admin targets
    admin = _convert_admin_targets(zr.admin_targets)
    if admin is not None:
        entry["admin_targets"] = admin

    # Direct-match fields
    if zr.gnis_ids is not None:
        entry["gnis_ids"] = zr.gnis_ids
    if zr.blue_line_keys is not None:
        entry["blue_line_keys"] = zr.blue_line_keys
    if getattr(zr, "fwa_watershed_codes", None) is not None:
        entry["fwa_watershed_codes"] = zr.fwa_watershed_codes
    if getattr(zr, "waterbody_keys", None) is not None:
        entry["waterbody_keys"] = zr.waterbody_keys
    if getattr(zr, "linear_feature_ids", None) is not None:
        entry["linear_feature_ids"] = zr.linear_feature_ids

    # Disabled
    if getattr(zr, "_disabled", False):
        entry["disabled"] = True
        if "TODO" not in entry.get("notes", ""):
            entry["notes"] = (
                entry.get("notes", "") + " TODO: Needs direct-match IDs to enable."
            ).strip()

    return entry


def main():
    print(f"Converting {len(ZONE_BASE_REGULATIONS)} v1 zone regulations...")

    zone_entries = []
    disabled_count = 0
    for zr in ZONE_BASE_REGULATIONS:
        entry = convert_one(zr)
        zone_entries.append(entry)
        if entry.get("disabled"):
            disabled_count += 1

    # Combine: provincial first, then zone
    all_entries = PROVINCIAL_REGS + zone_entries

    # Write
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(all_entries, f, indent=2, ensure_ascii=False)

    print(f"Wrote {len(all_entries)} entries to {OUTPUT_PATH}")
    print(f"  Provincial: {len(PROVINCIAL_REGS)}")
    print(f"  Zone: {len(zone_entries)} ({disabled_count} disabled)")

    # Verify round-trip
    from regulation_mapping_v2.enrichment.models import BaseRegulationDef

    errors = 0
    for entry in all_entries:
        try:
            BaseRegulationDef.from_dict(entry)
        except Exception as e:
            print(f"  ERROR: {entry['reg_id']}: {e}")
            errors += 1

    if errors:
        print(f"\n{errors} entries failed validation!")
        sys.exit(1)
    else:
        print("All entries validated successfully via BaseRegulationDef.from_dict()")


if __name__ == "__main__":
    main()
