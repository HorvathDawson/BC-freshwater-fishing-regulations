"""
Quick utility to query regulation details and their geographic mappings.

Usage:
    python -m fwa_modules.tools.query_regulation <reg_id> [<reg_id2> ...]
    python -m fwa_modules.tools.query_regulation --feature <feature_id>

Examples:
    python -m fwa_modules.tools.query_regulation R001
    python -m fwa_modules.tools.query_regulation R001 R002 R003
    python -m fwa_modules.tools.query_regulation --feature LAKE_123456
"""

import sys
import json
from pathlib import Path
from typing import List, Dict, Set


def load_data():
    """Load parsed regulations and feature mappings."""
    scripts_dir = Path(__file__).parent.parent
    output_dir = scripts_dir / "output" / "fwa_modules"

    # Load parsed regulations (raw format with 'identity', 'rules', etc.)
    parsed_regs_path = scripts_dir / "output" / "parse_synopsis" / "parsed_results.json"
    if not parsed_regs_path.exists():
        print(f"❌ Regulations file not found: {parsed_regs_path}")
        return None, None

    with open(parsed_regs_path) as f:
        parsed_data = json.load(f)

    regulations = (
        parsed_data
        if isinstance(parsed_data, list)
        else parsed_data.get("regulations", [])
    )

    # Build lookup by index (regulations are accessed by index in mapping)
    reg_by_index = {i: reg for i, reg in enumerate(regulations)}

    # Load feature mappings
    feature_to_regs_path = output_dir / "feature_to_regs.json"
    if not feature_to_regs_path.exists():
        print(f"❌ Feature mappings not found: {feature_to_regs_path}")
        return reg_by_index, None

    with open(feature_to_regs_path) as f:
        feature_to_regs = json.load(f)

    return reg_by_index, feature_to_regs


def build_reverse_index(feature_to_regs: Dict[str, List[str]]) -> Dict[str, Set[str]]:
    """Build regulation -> features mapping."""
    reg_to_features = {}
    for feature_id, reg_ids in feature_to_regs.items():
        for reg_id in reg_ids:
            if reg_id not in reg_to_features:
                reg_to_features[reg_id] = set()
            reg_to_features[reg_id].add(feature_id)
    return reg_to_features


def parse_regulation_id(reg_id: str) -> tuple:
    """Parse regulation ID like 'reg_0376_rule0' into (reg_index, rule_index)."""
    try:
        parts = reg_id.split("_")
        if len(parts) >= 3 and parts[0] == "reg" and "rule" in parts[2]:
            reg_index = int(parts[1])
            rule_index = int(parts[2].replace("rule", ""))
            return (reg_index, rule_index)
    except (ValueError, IndexError):
        pass
    return None


def format_regulation(reg: dict, reg_id: str, rule_index: int = None) -> str:
    """Format regulation details for display."""
    lines = []
    lines.append(f"\n{'='*80}")
    lines.append(f"Regulation ID: {reg_id}")

    # Show waterbody name prominently (check multiple possible field names)
    identity = reg.get("identity", {})
    wb_name = (
        identity.get("name_verbatim")
        or identity.get("waterbody_name")
        or identity.get("waterbody_key")
    )

    if wb_name:
        lines.append(f"Waterbody: {wb_name}")
    else:
        lines.append(f"Waterbody: (not identified)")

    lines.append(f"{'='*80}")

    # Waterbody identity details (only if we have useful info to show)
    if identity:
        detail_lines = []

        if identity_type := identity.get("identity_type"):
            detail_lines.append(f"  Type: {identity_type}")

        if alt_names := identity.get("alternate_names"):
            if alt_names:
                detail_lines.append(f"  Alternate names: {', '.join(alt_names)}")

        if location := identity.get("location_descriptor"):
            detail_lines.append(f"  Location: {location}")

        if global_scope := identity.get("global_scope"):
            scope_type = global_scope.get("type")
            includes_tribs = global_scope.get("includes_tributaries")
            if scope_type:
                scope_str = f"  Scope: {scope_type}"
                if includes_tribs:
                    scope_str += " (includes tributaries)"
                detail_lines.append(scope_str)

        if exclusions := identity.get("exclusions"):
            if exclusions:
                detail_lines.append(f"  Exclusions: {len(exclusions)}")
                for exc in exclusions[:3]:
                    exc_name = (
                        exc
                        if isinstance(exc, str)
                        else exc.get("name_verbatim", "unnamed")
                    )
                    detail_lines.append(f"    - {exc_name}")
                if len(exclusions) > 3:
                    detail_lines.append(f"    ... and {len(exclusions) - 3} more")

        if inclusions := identity.get("inclusions"):
            if inclusions:
                detail_lines.append(f"  Inclusions: {len(inclusions)}")
                for inc in inclusions[:3]:
                    inc_name = (
                        inc
                        if isinstance(inc, str)
                        else inc.get("name_verbatim", "unnamed")
                    )
                    detail_lines.append(f"    - {inc_name}")
                if len(inclusions) > 3:
                    detail_lines.append(f"    ... and {len(inclusions) - 3} more")

        if detail_lines:
            lines.append(f"\n📍 Waterbody Identity:")
            lines.extend(detail_lines)

    # Verbatim text
    if verbatim := reg.get("regs_verbatim"):
        lines.append(f"\n📝 Original Text:")
        lines.append(
            f"  {verbatim[:200]}..." if len(verbatim) > 200 else f"  {verbatim}"
        )

    # Rules
    if rules := reg.get("rules"):
        if rule_index is not None:
            # Show only the specific rule
            if 0 <= rule_index < len(rules):
                lines.append(f"\n📋 Rule {rule_index}:")
                rule = rules[rule_index]
                format_rule(rule, lines, indent="  ")
            else:
                lines.append(
                    f"\n⚠️  Rule index {rule_index} out of range (0-{len(rules)-1})"
                )
        else:
            # Show all rules
            lines.append(f"\n📋 Rules ({len(rules)} total):")
            for i, rule in enumerate(rules[:5]):
                lines.append(f"\n  Rule {i}:")
                format_rule(rule, lines, indent="    ")
            if len(rules) > 5:
                lines.append(f"\n  ... and {len(rules) - 5} more rules")

    return "\n".join(lines)


def format_rule(rule: dict, lines: list, indent: str = ""):
    """Format a single rule for display."""
    if species := rule.get("species"):
        if isinstance(species, list):
            lines.append(f"{indent}Species: {', '.join(species)}")
        else:
            lines.append(f"{indent}Species: {species}")

    if daily_quota := rule.get("daily_quota"):
        lines.append(f"{indent}Daily Quota: {daily_quota}")

    if possession_limit := rule.get("possession_limit"):
        lines.append(f"{indent}Possession Limit: {possession_limit}")

    if size_limits := rule.get("size_limits"):
        lines.append(f"{indent}Size Limits: {size_limits}")

    if bait_ban := rule.get("bait_ban"):
        lines.append(f"{indent}Bait Ban: {bait_ban}")

    if other := rule.get("other_restrictions"):
        lines.append(f"{indent}Other: {other}")


def query_by_regulation_ids(
    reg_ids: List[str], reg_by_index: dict, reg_to_features: dict
):
    """Display info for specific regulation IDs."""
    for reg_id in reg_ids:
        parsed = parse_regulation_id(reg_id)
        if not parsed:
            print(f"\n❌ Invalid regulation ID format: {reg_id}")
            print(f"   Expected format: reg_NNNN_ruleN (e.g., reg_0376_rule0)")
            continue

        reg_index, rule_index = parsed

        if reg_index not in reg_by_index:
            print(f"\n❌ Regulation index not found: {reg_index}")
            print(f"   Available range: 0-{len(reg_by_index)-1}")
            continue

        reg = reg_by_index[reg_index]
        print(format_regulation(reg, reg_id, rule_index))

        # Show mapped features
        if reg_to_features and reg_id in reg_to_features:
            features = sorted(reg_to_features[reg_id])
            print(f"\n🗺️  Mapped to {len(features)} geographic features:")
            for feat in features[:10]:
                print(f"  - {feat}")
            if len(features) > 10:
                print(f"  ... and {len(features) - 10} more")
        else:
            print(f"\n⚠️  Not mapped to any geographic features")


def query_by_feature_id(feature_id: str, feature_to_regs: dict, reg_by_index: dict):
    """Display regulations for a specific feature."""
    if feature_id not in feature_to_regs:
        print(f"\n❌ Feature ID not found: {feature_id}")
        print(f"   Sample features: {', '.join(list(feature_to_regs.keys())[:10])}...")
        return

    print(f"\n{'='*80}")
    print(f"Feature: {feature_id}")
    print(f"{'='*80}")

    reg_ids = feature_to_regs[feature_id]
    print(f"\n📋 Has {len(reg_ids)} regulations:")

    for reg_id in reg_ids:
        parsed = parse_regulation_id(reg_id)
        if parsed:
            reg_index, rule_index = parsed
            if reg_index in reg_by_index:
                reg = reg_by_index[reg_index]
                identity = reg.get("identity", {})
                name = (
                    identity.get("name_verbatim")
                    or identity.get("waterbody_name")
                    or identity.get("waterbody_key")
                    or "N/A"
                )
                print(f"  - {reg_id}: {name}")
            else:
                print(f"  - {reg_id}: (source not found)")
        else:
            print(f"  - {reg_id}: (invalid format)")

    # Optionally show full details
    if len(reg_ids) <= 3:
        print(f"\n{'─'*80}")
        print("Full regulation details:")
        for reg_id in reg_ids:
            parsed = parse_regulation_id(reg_id)
            if parsed:
                reg_index, rule_index = parsed
                if reg_index in reg_by_index:
                    print(
                        format_regulation(reg_by_index[reg_index], reg_id, rule_index)
                    )


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        print(
            "\n❌ Please provide at least one regulation ID or use --feature <feature_id>"
        )
        sys.exit(1)

    # Load data
    print("Loading data...")
    reg_by_index, feature_to_regs = load_data()

    if not reg_by_index:
        sys.exit(1)

    # Build reverse index if available
    reg_to_features = None
    if feature_to_regs:
        reg_to_features = build_reverse_index(feature_to_regs)

    # Parse arguments
    if sys.argv[1] == "--feature":
        if len(sys.argv) < 3:
            print("❌ Please provide a feature ID after --feature")
            sys.exit(1)

        if not feature_to_regs:
            print("❌ Feature mappings not available")
            sys.exit(1)

        query_by_feature_id(sys.argv[2], feature_to_regs, reg_by_index)

    else:
        # Query by regulation IDs
        reg_ids = sys.argv[1:]
        query_by_regulation_ids(reg_ids, reg_by_index, reg_to_features)


if __name__ == "__main__":
    main()
