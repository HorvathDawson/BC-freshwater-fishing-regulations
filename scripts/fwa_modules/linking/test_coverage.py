#!/usr/bin/env python3
"""
Test Linking Coverage (Refactored)

Tests the waterbody linking system against parsed regulations.
Optimized for code readability while preserving EXACT output fidelity and JSON schemas.
Features dynamic terminal sizing and detailed statistical breakdowns.
"""

import json
import argparse
import shutil
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Any

from .linker import WaterbodyLinker, LinkStatus
from .metadata_gazetteer import MetadataGazetteer
from .name_variations import (
    NAME_VARIATIONS,
    DIRECT_MATCHES,
    SKIP_ENTRIES,
    UNMARKED_WATERBODIES,
    ManualCorrections,
)

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


def extract_region(region_str: str) -> str:
    """Standardizes 'REGION 4 - Kootenay' -> 'Region 4'."""
    if not region_str or not region_str.startswith("REGION"):
        return None
    num = "".join(c for c in region_str.split("-")[0] if c.isdigit())
    return f"Region {num}" if num else None


class CoverageStats:
    """Holder for running statistics."""

    def __init__(self):
        self.results = defaultdict(list)
        self.by_region = defaultdict(Counter)
        self.success_methods = Counter()
        self.failed_parse = []
        self.trib_non_stream = []
        self.mu_mismatches = []
        self.used_vars = set()
        self.used_direct = set()
        self.used_skips = set()
        self.reg_names_seen = set()


# --- Export Logic ---


def get_instructions_block(mode: str):
    if mode == "NOT_FOUND":
        return {
            "description": "NOT_FOUND waterbodies - need to add name variations",
            "how_to_fix": [
                "1. Check spelling/formatting/renaming -> Add to NAME_VARIATIONS",
                "2. Not in gazetteer? -> Add to DIRECT_MATCHES",
                "3. Check 'search_terms_used' and 'location_descriptor'",
                "4. Use management_units to narrow down",
            ],
            "example_name_variation": {
                "TOQUART LAKE": {"target_names": ["toquaht lake"], "note": "Spelling"}
            },
            "example_direct_match": {
                "LONG LAKE (Nanaimo)": {"gnis_id": "17501", "note": "Disambiguate"}
            },
        }
    else:
        return {
            "description": "AMBIGUOUS waterbodies - multiple candidates found",
            "how_to_fix": [
                "1. Review candidates",
                "2. Compare MUs",
                "3. Add DIRECT_MATCH with specific ID",
            ],
            "example_direct_match": {
                "RAINBOW LAKE": {"gnis_id": "28692", "note": "Disambiguate MU"}
            },
        }


def export_data(items: List[Dict], path: Path, lookup: Dict, mode: str):
    export_entries = []
    for item in items:
        reg, name = item["region"], item["name_verbatim"]
        full = lookup.get((reg, name), {})
        ex_var = NAME_VARIATIONS.get(reg, {}).get(name)
        ex_match = DIRECT_MATCHES.get(reg, {}).get(name)

        entry = {
            "name_verbatim": name,
            "waterbody_key": item["waterbody_key"],
            "region": reg,
            "management_units": (
                item["mu"]
                if mode == "NOT_FOUND"
                else item.get("regulation_management_units")
            ),
            "identity_type": item.get("identity_type"),
            "location_descriptor": item.get("location_descriptor"),
            "alternate_names": item.get("alternate_names", []),
            "page": full.get("page"),
            "regulations_summary": full.get("regs_verbatim"),
            "full_identity": full.get("identity"),
            "existing_variation": (
                {"target_names": ex_var.target_names, "note": ex_var.note}
                if ex_var
                else None
            ),
            "existing_direct_match": (
                {"gnis_id": ex_match.gnis_id, "note": ex_match.note}
                if ex_match
                else None
            ),
        }

        if mode == "NOT_FOUND":
            entry["search_terms_used"] = item.get(
                "search_terms", [item["waterbody_key"].lower()]
            )
            entry["suggested_action"] = "Add to NAME_VARIATIONS or DIRECT_MATCHES"
        else:
            entry["candidate_count"] = len(item.get("candidates", []))
            entry["candidate_waterbodies"] = item.get("candidate_details", [])
            entry["suggested_action"] = "Add to DIRECT_MATCHES with correct ID"

        export_entries.append(entry)

    out = {
        "_instructions": get_instructions_block(mode),
        "count": len(export_entries),
        "entries": export_entries,
    }
    with open(path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nExported {len(export_entries)} {mode} entries to {path}")


# --- Main Logic ---


def process_ambiguous_candidates(
    candidates: List[Any], reg_mus: List[str]
) -> List[Dict]:
    processed = []
    for c in candidates:
        fwa_mus = c.get("management_units", [])
        match = any(mu in fwa_mus for mu in reg_mus)
        processed.append(
            {
                "fwa_name": c.get("name"),
                "gnis_id": c.get("gnis_id"),
                "fwa_watershed_code": c.get("fwa_watershed_code"),
                "feature_type": c.get("feature_type"),
                "region": c.get("region"),
                "fwa_mus": fwa_mus,
                "mu_match": match,
            }
        )
    return processed


def test_linking_coverage(export_not_found: str = None, export_ambiguous: str = None):
    root = Path(__file__).parent.parent.parent
    print_header("WATERBODY LINKING COVERAGE TEST")

    # Load Data
    print("Loading parsed regulations...")
    with open(root / "output/parse_synopsis/parsed_results.json") as f:
        parsed_data = json.load(f)
    print(f"Loaded {len(parsed_data)} waterbodies")

    print("Loading FWA metadata...")
    gazetteer = MetadataGazetteer(root / "output/fwa_modules/stream_metadata.pickle")
    print(
        f"Loaded {len(gazetteer.metadata.get('streams', {})):,} streams, {len(gazetteer.metadata.get('lakes', {})):,} lakes"
    )

    print("Initializing linker...")
    linker = WaterbodyLinker(
        gazetteer,
        ManualCorrections(
            NAME_VARIATIONS, DIRECT_MATCHES, SKIP_ENTRIES, UNMARKED_WATERBODIES
        ),
    )
    print(f"Loaded configuration across {len(NAME_VARIATIONS)} regions")

    print_header("TESTING LINKING")
    stats = CoverageStats()
    parsed_lookup = {}

    for i, wb in enumerate(parsed_data):
        ident = wb["identity"]
        key, name = ident["waterbody_key"], ident["name_verbatim"]
        region = extract_region(wb.get("region", ""))
        mus = wb.get("mu", [])

        if key == "FAILED":
            stats.failed_parse.append(wb)
            continue
        if not region:
            continue

        parsed_lookup[(region, name)] = wb
        if name:
            stats.reg_names_seen.add((region, name))
        stats.reg_names_seen.add((region, key))

        if region in SKIP_ENTRIES and name in SKIP_ENTRIES[region]:
            stats.used_skips.add((region, name))

        # --- Link ---
        res = linker.link_waterbody(
            key, region=region, mgmt_units=mus, name_verbatim=name
        )

        # --- Statistics & Validation ---
        if res.status == LinkStatus.SUCCESS:
            stats.success_methods[res.link_method] += 1

            # Track Usage
            if res.link_method == "name_variation":
                if name in NAME_VARIATIONS.get(region, {}):
                    stats.used_vars.add((region, name))
                elif key in NAME_VARIATIONS.get(region, {}):
                    stats.used_vars.add((region, key))
            elif res.link_method == "direct_match":
                matched = name if name in DIRECT_MATCHES.get(region, {}) else key
                if matched:
                    stats.used_direct.add((region, matched))

            # Validation: MU Mismatch
            if res.link_method == "direct_match":
                feats = res.matched_features or (
                    [res.matched_feature] if res.matched_feature else []
                )
                fwa_mus = set().union(*(f.mgmt_units for f in feats if f.mgmt_units))
                reg_set = set(mus)

                # Check mismatch logic
                mismatch = False
                if reg_set and fwa_mus and not reg_set.issubset(fwa_mus):
                    mismatch = True
                elif reg_set and not fwa_mus:
                    mismatch = True

                if mismatch:
                    reg_num = region.split()[-1]
                    is_cross = bool(fwa_mus) and not any(
                        m.startswith(f"{reg_num}-") for m in fwa_mus
                    )
                    stats.mu_mismatches.append(
                        {
                            "name_verbatim": name,
                            "region": region,
                            "reg_mus": sorted(reg_set),
                            "fwa_mus": sorted(fwa_mus),
                            "is_cross": is_cross,
                            "page": wb.get("page"),
                        }
                    )

            # Validation: Tributaries
            if ident.get("identity_type") == "TRIBUTARIES":
                feats = res.matched_features or (
                    [res.matched_feature] if res.matched_feature else []
                )
                if feats and all(f.geometry_type != "multilinestring" for f in feats):
                    stats.trib_non_stream.append(
                        {"key": key, "region": region, "matched_to": feats[0].name}
                    )

        # Store Results
        cands_list = []
        if res.candidate_features:
            for f in res.candidate_features:
                cands_list.append(
                    {
                        "name": f.name,
                        "gnis_id": f.gnis_id,
                        "fwa_watershed_code": f.fwa_watershed_code,
                        "feature_type": getattr(f, "geometry_type", "Unknown"),
                        "zones": f.zones,
                        "management_units": f.mgmt_units,
                    }
                )

        item_data = {
            "waterbody_key": key,
            "name_verbatim": name,
            "location_descriptor": ident.get("location_descriptor"),
            "alternate_names": ident.get("alternate_names", []),
            "identity_type": ident.get("identity_type"),
            "region": region,
            "mu": mus,
            "result": res,
            "regulation_management_units": mus,
            "candidates": cands_list,
            "candidate_details": (
                process_ambiguous_candidates(cands_list, mus)
                if res.status == LinkStatus.AMBIGUOUS
                else []
            ),
        }
        stats.results[res.status].append(item_data)
        stats.by_region[region][res.status] += 1

        if (i + 1) % 100 == 0:
            print(f"Processed {i+1}/{len(parsed_data)}...", end="\r")

    print(f"Processed {len(parsed_data)}/{len(parsed_data)} waterbodies    ")

    # --- REPORTING ---

    print_header("SUMMARY STATISTICS")
    if stats.failed_parse:
        print(f"⚠️  FAILED PARSE ENTRIES: {len(stats.failed_parse)} (excluded)")

    total = len(parsed_data) - len(stats.failed_parse)

    # 1. Main Status Table
    for st in LinkStatus:
        count = len(stats.results[st])
        pct = (count / total * 100) if total else 0
        color = (
            GREEN
            if st == LinkStatus.SUCCESS
            else (RED if st == LinkStatus.NOT_FOUND else RESET)
        )
        print(f"{color}{st.value.upper():<20} : {count:5d} ({pct:5.1f}%){RESET}")

    # 2. Success Breakdown
    print("\n   --- Success Breakdown ---")
    method_map = {
        "direct_match": "Direct Match (Config)",
        "name_variation": "Name Variation (Config)",
        "natural_search": "Natural Search (Fuzzy)",
        "exact_match": "Exact Name Match",
    }
    for method, label in method_map.items():
        count = stats.success_methods[method]
        if count > 0:
            print(f"   {label:<25} : {count:5d}")

    # 3. Special Cases
    if stats.trib_non_stream:
        print(
            f"\n   {YELLOW}⚠️  Tributaries -> Non-Stream : {len(stats.trib_non_stream):5d} (Check these){RESET}"
        )

    not_in_data = len(stats.results[LinkStatus.NOT_IN_DATA])
    ignored = len(stats.results[LinkStatus.IGNORED])
    if not_in_data + ignored > 0:
        print("\n   --- Excluded/Known Missing ---")
        if not_in_data:
            print(f"   Not In FWA Data           : {not_in_data:5d}")
        if ignored:
            print(f"   Manually Ignored          : {ignored:5d}")

    # Region Breakdown
    print_header("RESULTS BY REGION")
    width = get_terminal_width()
    col_w = max(10, int((width - 20) / 4))

    header = f"{'Region':<15} | {'SUCCESS':<{col_w}} | {'AMBIG':<{col_w}} | {'NOT_FOUND':<{col_w}}"
    print(header)
    print("-" * len(header))

    for reg in sorted(stats.by_region.keys()):
        s = stats.by_region[reg]
        row = f"{reg:<15} | {s[LinkStatus.SUCCESS]:<{col_w}} | {s[LinkStatus.AMBIGUOUS]:<{col_w}} | {s[LinkStatus.NOT_FOUND]:<{col_w}}"
        print(row)

    # Validations
    print_header("DIRECT MATCH MU VALIDATION")
    if stats.mu_mismatches:
        print(f"Found {len(stats.mu_mismatches)} mismatches:")
        for m in stats.mu_mismatches:
            tag = (
                f" {RED}[CROSS-REGION]{RESET}"
                if m["is_cross"]
                else f" {YELLOW}[MU MISMATCH]{RESET}"
            )
            print(f"\n{tag} {m['name_verbatim']}")
            print(f"   Regulation Region: {m['region']}")
            print(f"   Regulation MUs:    {', '.join(m['reg_mus']) or '(none)'}")
            print(f"   FWA MUs:           {', '.join(m['fwa_mus']) or '(none)'}")
            if m.get("page"):
                print(f"   Page:              {m['page']}")
    else:
        print("✅ All direct matches verified.")

    # Duplicate Mappings
    print_header("DUPLICATE REGULATION MAPPINGS")
    dupes = linker.get_duplicate_mappings()
    if dupes:
        print(
            f"Found {len(dupes)} FWA waterbodies with multiple regulation names mapped to them:"
        )
        sorted_dupes = sorted(
            dupes.items(), key=lambda x: len(x[1]["regulations"]), reverse=True
        )

        for identity_key, data in sorted_dupes[:50]:
            feat = data["feature"]
            print(f"\n  {identity_key}")
            if feat:
                info = f"{feat.name} ({feat.geometry_type})"
                if feat.gnis_id:
                    info += f" [GNIS {feat.gnis_id}]"
                print(f"  Feature: {info}")
                if feat.mgmt_units:
                    print(f"  FWA MUs: {', '.join(sorted(feat.mgmt_units))}")

            print(f"  Mapped by {len(data['regulations'])} regulation(s):")
            for r, k, n in data["regulations"]:
                print(f"    • {r:10s} | {n if n else k}")

        if len(dupes) > 50:
            print(f"\n  ... and {len(dupes) - 50} more")
    else:
        print("No duplicate mappings found.")

    # Samples
    def print_sample(status, limit=5):
        items = stats.results[status]
        if not items:
            return
        print_header(
            f"{status.value.upper()} SAMPLES (Showing {min(limit, len(items))} of {len(items)})"
        )

        # Group by type
        by_type = defaultdict(list)
        for i in items:
            by_type[i.get("identity_type", "UNKNOWN")].append(i)

        for itype, type_items in sorted(by_type.items()):
            print_sub_header(f"{itype}")
            for item in type_items[:limit]:
                mus = ",".join(item["mu"]) or "-"
                # Use name_verbatim instead of key
                print(f" • {item['name_verbatim']} ({item['region']}) | MUs: {mus}")

                if item.get("location_descriptor"):
                    print(f"   Loc: {item['location_descriptor']}")

                if status == LinkStatus.AMBIGUOUS:
                    # Detailed candidate listing
                    cand_features = item["candidates"]
                    unique_candidates = defaultdict(list)
                    for c in cand_features:
                        # Dedupe by ID
                        cid = c["fwa_watershed_code"] or c["gnis_id"]
                        unique_candidates[cid].append(c)

                    print(f"   Candidates ({len(unique_candidates)}):")
                    for cid, feats in list(unique_candidates.items())[:5]:
                        f = feats[0]
                        f_mus = (
                            ", ".join(
                                sorted(
                                    set().union(
                                        *(
                                            x["management_units"]
                                            for x in feats
                                            if x["management_units"]
                                        )
                                    )
                                )
                            )
                            or "None"
                        )
                        zones_str = (
                            ",".join(f["zones"]) if f.get("zones") else "Unknown"
                        )
                        print(
                            f"     - {f['name']} [{cid}] (Zones: {zones_str}) | MUs: {f_mus}"
                        )

    print_sample(LinkStatus.NOT_FOUND, limit=5)
    print_sample(LinkStatus.AMBIGUOUS, limit=5)

    # Unused Configs
    print_header("UNUSED CONFIGURATION")

    unused_vars = []
    for r, v in NAME_VARIATIONS.items():
        for k in v:
            if not SKIP_ENTRIES.get(r, {}).get(k) and (r, k) not in stats.used_vars:
                unused_vars.append(f"{r} | {k}")

    unused_direct = []
    for r, m in DIRECT_MATCHES.items():
        for k in m:
            if (r, k) in stats.reg_names_seen and (r, k) not in stats.used_direct:
                unused_direct.append(f"{r} | {k}")

    if unused_vars:
        print(f"⚠️  {len(unused_vars)} Unused Name Variations (first 10):")
        for u in unused_vars[:10]:
            print(f"   - {u}")
    else:
        print("✅ Name variations clean.")

    if unused_direct:
        print(f"\n⚠️  {len(unused_direct)} Unused Direct Matches (first 10):")
        for u in unused_direct[:10]:
            print(f"   - {u}")
    else:
        print("\n✅ Direct matches clean.")

    # Final detailed block
    print_header("FINAL TALLY")

    print(f"Total Processed:    {total}")
    print(f"{GREEN}Linked (Total):     {len(stats.results[LinkStatus.SUCCESS])}{RESET}")
    print(f"  - Natural Search: {stats.success_methods['natural_search']}")
    print(f"  - Direct Match:   {stats.success_methods['direct_match']}")
    print(f"  - Name Variation: {stats.success_methods['name_variation']}")
    print(f"  - Exact Match:    {stats.success_methods['exact_match']}")
    print(f"{RED}Not Found:          {len(stats.results[LinkStatus.NOT_FOUND])}{RESET}")
    print(
        f"{YELLOW}Ambiguous:          {len(stats.results[LinkStatus.AMBIGUOUS])}{RESET}"
    )
    print(
        f"{BLUE}Not In Data:        {len(stats.results[LinkStatus.NOT_IN_DATA])}{RESET}"
    )
    print(f"Ignored:            {len(stats.results[LinkStatus.IGNORED])}")
    print()

    # Exports
    if export_not_found:
        export_data(
            stats.results[LinkStatus.NOT_FOUND],
            Path(export_not_found),
            parsed_lookup,
            "NOT_FOUND",
        )
    if export_ambiguous:
        export_data(
            stats.results[LinkStatus.AMBIGUOUS],
            Path(export_ambiguous),
            parsed_lookup,
            "AMBIGUOUS",
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--export-not-found")
    parser.add_argument("--export-ambiguous")
    args = parser.parse_args()
    test_linking_coverage(args.export_not_found, args.export_ambiguous)
