#!/usr/bin/env python3
"""
Compare multiple completed parsing sessions to analyze differences and consistency.

Updated to work with the current data model including:
- rule_text_verbatim (list of strings) - supports interleaved rule text
- landmark_end_verbatim (for SEGMENT scopes)
- waterbody_key in scopes
- identity.global_scope and identity.exclusions
- audit_log tracking
- dates field in restrictions

This tool performs comprehensive analysis across multiple dimensions:

1. IDENTITY ANALYSIS
   - Waterbody key changes
   - Global scope changes (type, location)
   - Exclusion changes (added/removed geographic exclusions)

2. SCOPE ANALYSIS
   - Meaningful scope changes (different scope types or locations)
   - Minor text changes (same semantic scope, different wording)
   - Scope type transitions
   - Location verbatim changes

3. RESTRICTION ANALYSIS
   - Restriction type changes (added/removed)
   - Date changes
   - Restriction detail modifications

4. RULE TEXT ANALYSIS
   - Rule splitting (single text → multiple interleaved texts)
   - Rule merging (multiple texts → single text)
   - Text content modifications

5. AUDIT LOG ANALYSIS
   - Items with more/fewer issues
   - New issues introduced
   - Common audit messages

6. CONSISTENCY ANALYSIS
   - Items that always succeed
   - Items that always fail
   - Items with inconsistent parsing across sessions

Usage:
  # Compare two sessions
  python compare_sessions.py session1.json session2.json

  # Generate summary only
  python compare_sessions.py session1.json session2.json --summary-only

  # Save full report to file
  python compare_sessions.py session1.json session2.json -o report.txt

  # Compare specific waterbody across sessions
  python compare_sessions.py session1.json session2.json --item "ALOUETTE LAKE"

  # Single-session analysis (no comparison):
  python compare_sessions.py session1.json --get-audits           # Items with audit_log entries
  python compare_sessions.py session1.json --get-failed           # Items with no rules
  python compare_sessions.py session1.json --get-complex          # Items with >5 rules
  python compare_sessions.py session1.json --get-exclusions       # Items with exclusions in identity
  python compare_sessions.py session1.json --get-vague-scopes     # Items with VAGUE scope types
  python compare_sessions.py session1.json --get-brackets         # Items with parentheses in title

  # Use completed sessions from default output directory
  python compare_sessions.py completed_sessions/2026-01-27_100145/parsed_results.json session.json
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Any, Set, Tuple
from collections import defaultdict, Counter
import argparse
from datetime import datetime


def load_session_results(session_files: List[Path]) -> Dict[str, Dict]:
    """Load results from multiple session files."""
    sessions = {}

    for session_file in session_files:
        if not session_file.exists():
            print(f"Warning: Session file not found: {session_file}")
            continue

        try:
            with open(session_file, "r", encoding="utf-8") as f:
                data = json.load(f)

                # Create unique session name - use parent directory if filename is generic
                session_name = session_file.stem
                if session_name in ["session", "results", "output"]:
                    # Use parent directory + filename for generic names
                    session_name = f"{session_file.parent.name}_{session_file.stem}"

                sessions[session_name] = data

                # Count successful items for partial session info
                if isinstance(data, list):
                    successful = sum(
                        1
                        for item in data
                        if item
                        and isinstance(item, dict)
                        and "rules" in item
                        and len(item.get("rules", [])) > 0
                    )
                    print(
                        f"Loaded session: {session_name} ({len(data)} items, {successful} successful)"
                    )
                elif isinstance(data, dict) and "results" in data:
                    results = data["results"]
                    if results and isinstance(results, list):
                        successful = sum(
                            1
                            for item in results
                            if item
                            and isinstance(item, dict)
                            and "rules" in item
                            and len(item.get("rules", [])) > 0
                        )
                        print(
                            f"Loaded session: {session_name} ({len(results)} items, {successful} successful)"
                        )
                    else:
                        print(
                            f"Loaded session: {session_name} (empty or invalid results)"
                        )
                else:
                    # Debug what we actually got
                    if isinstance(data, dict):
                        keys = list(data.keys())[:5]  # Show first 5 keys
                        print(
                            f"Loaded session: {session_name} (dict with keys: {keys})"
                        )
                    else:
                        print(
                            f"Loaded session: {session_name} (type: {type(data).__name__})"
                        )
        except Exception as e:
            print(f"Error loading {session_file}: {e}")

    return sessions


def extract_session_metadata(sessions: Dict[str, Dict]) -> Dict[str, Dict]:
    """Extract metadata from each session."""
    metadata = {}

    for session_name, data in sessions.items():
        # Handle different session file structures
        results_data = None
        if isinstance(data, list):
            # Already a results array
            results_data = data
        elif isinstance(data, dict) and "results" in data:
            # Session file with results section
            results_data = data["results"]
        elif isinstance(data, dict) and len(data) > 0:
            # Other dict format - check if values look like result items
            first_value = next(iter(data.values()))
            if isinstance(first_value, dict) and (
                "rules" in first_value or "identity" in first_value
            ):
                results_data = list(data.values())

        if results_data and isinstance(results_data, list) and len(results_data) > 0:
            # It's a results array
            total_items = len(results_data)
            successful_items = sum(
                1
                for item in results_data
                if item
                and isinstance(item, dict)
                and "rules" in item
                and len(item.get("rules", [])) > 0
            )
            metadata[session_name] = {
                "total_items": total_items,
                "successful_items": successful_items,
                "success_rate": (
                    successful_items / total_items if total_items > 0 else 0
                ),
                "type": "results_array",
            }
        else:
            # Could be other format
            metadata[session_name] = {
                "total_items": 0,
                "successful_items": 0,
                "success_rate": 0,
                "type": "unknown",
            }

    return metadata


def compare_item_success(sessions: Dict[str, Dict]) -> Dict[str, Dict]:
    """Compare success/failure for each waterbody across sessions, matching by name."""
    item_comparison = defaultdict(dict)

    for session_name, data in sessions.items():
        # Handle different session file structures
        results_data = None
        if isinstance(data, list):
            # Already a results array
            results_data = data
        elif isinstance(data, dict) and "results" in data:
            # Session file with results section
            results_data = data["results"]
        elif isinstance(data, dict) and len(data) > 0:
            # Other dict format - check if values look like result items
            first_value = next(iter(data.values()))
            if isinstance(first_value, dict) and (
                "rules" in first_value or "identity" in first_value
            ):
                results_data = list(data.values())

        if results_data and isinstance(results_data, list):
            for i, item in enumerate(results_data):
                if not item or not isinstance(item, dict):
                    continue
                waterbody = item.get("identity", {}).get("name_verbatim", f"Item_{i}")
                has_rules = "rules" in item and len(item.get("rules", [])) > 0

                item_comparison[waterbody][session_name] = {
                    "success": has_rules,
                    "rule_count": len(item.get("rules", [])),
                    "index": i,
                    "item_data": item,  # Store full item for scope comparison
                }

    return dict(item_comparison)


def analyze_consistency(item_comparison: Dict[str, Dict]) -> Dict:
    """Analyze consistency patterns across sessions."""
    analysis = {
        "always_succeed": [],
        "always_fail": [],
        "inconsistent": [],
        "session_counts": Counter(),
    }

    for waterbody, session_results in item_comparison.items():
        success_count = sum(
            1 for result in session_results.values() if result["success"]
        )
        total_sessions = len(session_results)

        analysis["session_counts"][total_sessions] += 1

        if success_count == total_sessions and total_sessions > 0:
            analysis["always_succeed"].append(waterbody)
        elif success_count == 0:
            analysis["always_fail"].append(waterbody)
        else:
            analysis["inconsistent"].append(
                {
                    "waterbody": waterbody,
                    "success_rate": success_count / total_sessions,
                    "successes": success_count,
                    "total": total_sessions,
                }
            )

    return analysis


def compare_rule_structures(sessions: Dict[str, Dict], waterbody: str) -> Dict:
    """Compare the actual rule structures for a specific waterbody across sessions."""
    comparisons = {}

    for session_name, data in sessions.items():
        # Handle different session file structures
        results_data = None
        if isinstance(data, list):
            # Already a results array
            results_data = data
        elif isinstance(data, dict) and "results" in data:
            # Session file with results section
            results_data = data["results"]
        elif isinstance(data, dict) and len(data) > 0:
            # Other dict format - check if values look like result items
            first_value = next(iter(data.values()))
            if isinstance(first_value, dict) and (
                "rules" in first_value or "identity" in first_value
            ):
                results_data = list(data.values())

        if results_data and isinstance(results_data, list):
            for item in results_data:
                if not item or not isinstance(item, dict):
                    continue
                if item.get("identity", {}).get("name_verbatim") == waterbody:
                    rules = item.get("rules", [])
                    comparisons[session_name] = {
                        "rule_count": len(rules),
                        "scope_types": [
                            rule.get("scope", {}).get("type") for rule in rules
                        ],
                        "restriction_types": [
                            [
                                restr.get("type")
                                for restr in rule.get("restrictions", [])
                            ]
                            for rule in rules
                        ],
                        "scope_details": [
                            {
                                "type": rule.get("scope", {}).get("type"),
                                "location_verbatim": rule.get("scope", {}).get(
                                    "location_verbatim"
                                ),
                                "landmark_verbatim": rule.get("scope", {}).get(
                                    "landmark_verbatim"
                                ),
                                "landmark_end_verbatim": rule.get("scope", {}).get(
                                    "landmark_end_verbatim"
                                ),
                                "direction": rule.get("scope", {}).get("direction"),
                                "includes_tributaries": rule.get("scope", {}).get(
                                    "includes_tributaries"
                                ),
                                "waterbody_key": rule.get("scope", {}).get(
                                    "waterbody_key"
                                ),
                            }
                            for rule in rules
                        ],
                        "rule_text_verbatim": [
                            rule.get("rule_text_verbatim", []) for rule in rules
                        ],
                    }
                    break

    return comparisons


def normalize_scope_for_comparison(scope_type, location_text):
    """Normalize scope information for more meaningful comparisons."""
    if not location_text:
        return (scope_type, None)

    # Normalize common variations
    normalized = location_text.lower().strip()

    # Remove common prefixes that don't change meaning
    common_prefixes = ["on ", "in ", "at ", "of "]
    for prefix in common_prefixes:
        if normalized.startswith(prefix) and len(normalized) > len(prefix):
            # Check if removing prefix still leaves meaningful text
            remaining = normalized[len(prefix) :]
            if remaining and not remaining[0].isspace():
                # Temporarily remove to check if it's just a preposition
                test_normalized = remaining
                break
    else:
        test_normalized = normalized

    # Remove common suffixes/markers that don't change meaning
    test_normalized = test_normalized.replace("on parts", "parts")
    test_normalized = test_normalized.replace("in parts", "parts")
    test_normalized = test_normalized.replace("all parts", "parts")
    test_normalized = test_normalized.replace("in all parts", "parts")
    test_normalized = test_normalized.replace("[includes tributaries]", "")
    test_normalized = test_normalized.strip()

    # Remove extra whitespace
    test_normalized = " ".join(test_normalized.split())

    return (scope_type, test_normalized if test_normalized else None)


def scopes_are_meaningfully_different(scopes1, scopes2):
    """Check if two scope lists are meaningfully different (not just minor text variations)."""
    if len(scopes1) != len(scopes2):
        return True

    # Normalize both scope lists
    norm_scopes1 = [normalize_scope_for_comparison(s[0], s[1]) for s in scopes1]
    norm_scopes2 = [normalize_scope_for_comparison(s[0], s[1]) for s in scopes2]

    # Sort for comparison (order might not matter) - handle None values
    def sort_key(scope_tuple):
        scope_type, location = scope_tuple
        return (scope_type or "", location or "")

    norm_scopes1.sort(key=sort_key)
    norm_scopes2.sort(key=sort_key)

    return norm_scopes1 != norm_scopes2


def analyze_identity_changes(item_comparison: Dict[str, Dict]) -> Dict:
    """Analyze how identity fields (global_scope, waterbody_key, exclusions) have changed."""
    identity_changes = {
        "waterbody_key_changes": [],
        "global_scope_changes": [],
        "exclusion_changes": [],
        "summary": {},
    }

    multi_session_items = {k: v for k, v in item_comparison.items() if len(v) > 1}

    for waterbody, session_results in multi_session_items.items():
        session_names = list(session_results.keys())

        for i in range(len(session_names)):
            for j in range(i + 1, len(session_names)):
                session1, session2 = session_names[i], session_names[j]

                item1 = session_results[session1]["item_data"]
                item2 = session_results[session2]["item_data"]

                identity1 = item1.get("identity", {})
                identity2 = item2.get("identity", {})

                # Check waterbody_key changes
                key1 = identity1.get("waterbody_key")
                key2 = identity2.get("waterbody_key")
                if key1 != key2:
                    identity_changes["waterbody_key_changes"].append(
                        {
                            "waterbody": waterbody,
                            "session_pair": f"{session1} -> {session2}",
                            "before": key1,
                            "after": key2,
                        }
                    )

                # Check global_scope changes
                gs1 = identity1.get("global_scope", {})
                gs2 = identity2.get("global_scope", {})
                if gs1 != gs2:
                    identity_changes["global_scope_changes"].append(
                        {
                            "waterbody": waterbody,
                            "session_pair": f"{session1} -> {session2}",
                            "before": gs1,
                            "after": gs2,
                        }
                    )

                # Check exclusions changes
                excl1 = identity1.get("exclusions", [])
                excl2 = identity2.get("exclusions", [])
                if excl1 != excl2:
                    identity_changes["exclusion_changes"].append(
                        {
                            "waterbody": waterbody,
                            "session_pair": f"{session1} -> {session2}",
                            "before_count": len(excl1),
                            "after_count": len(excl2),
                            "before": excl1,
                            "after": excl2,
                        }
                    )

    identity_changes["summary"] = {
        "total_items_compared": len(multi_session_items),
        "waterbody_key_changes": len(identity_changes["waterbody_key_changes"]),
        "global_scope_changes": len(identity_changes["global_scope_changes"]),
        "exclusion_changes": len(identity_changes["exclusion_changes"]),
    }

    return identity_changes


def analyze_restriction_changes(item_comparison: Dict[str, Dict]) -> Dict:
    """Analyze how restrictions have changed between sessions."""
    restriction_changes = {
        "items_with_restriction_changes": [],
        "restriction_type_changes": Counter(),
        "date_changes": [],
        "summary": {},
    }

    multi_session_items = {k: v for k, v in item_comparison.items() if len(v) > 1}

    for waterbody, session_results in multi_session_items.items():
        session_names = list(session_results.keys())

        for i in range(len(session_names)):
            for j in range(i + 1, len(session_names)):
                session1, session2 = session_names[i], session_names[j]

                item1 = session_results[session1]["item_data"]
                item2 = session_results[session2]["item_data"]

                rules1 = item1.get("rules", [])
                rules2 = item2.get("rules", [])

                # Extract all restrictions from both sessions
                all_restrictions1 = []
                for rule in rules1:
                    all_restrictions1.extend(rule.get("restrictions", []))

                all_restrictions2 = []
                for rule in rules2:
                    all_restrictions2.extend(rule.get("restrictions", []))

                # Compare restriction types
                types1 = [r.get("type") for r in all_restrictions1]
                types2 = [r.get("type") for r in all_restrictions2]

                if sorted(types1) != sorted(types2):
                    restriction_changes["items_with_restriction_changes"].append(
                        {
                            "waterbody": waterbody,
                            "session_pair": f"{session1} -> {session2}",
                            "before": types1,
                            "after": types2,
                        }
                    )

                    # Track type changes
                    added_types = set(types2) - set(types1)
                    removed_types = set(types1) - set(types2)
                    for t in added_types:
                        restriction_changes["restriction_type_changes"][
                            f"Added: {t}"
                        ] += 1
                    for t in removed_types:
                        restriction_changes["restriction_type_changes"][
                            f"Removed: {t}"
                        ] += 1

                # Compare dates
                dates1 = [
                    r.get("dates")
                    for r in all_restrictions1
                    if r.get("dates") and r.get("dates") != "null"
                ]
                dates2 = [
                    r.get("dates")
                    for r in all_restrictions2
                    if r.get("dates") and r.get("dates") != "null"
                ]

                if dates1 != dates2:
                    restriction_changes["date_changes"].append(
                        {
                            "waterbody": waterbody,
                            "session_pair": f"{session1} -> {session2}",
                            "before_count": len(dates1),
                            "after_count": len(dates2),
                        }
                    )

    restriction_changes["summary"] = {
        "total_items_compared": len(multi_session_items),
        "items_with_restriction_changes": len(
            restriction_changes["items_with_restriction_changes"]
        ),
        "items_with_date_changes": len(restriction_changes["date_changes"]),
    }

    return restriction_changes


def analyze_audit_log_changes(item_comparison: Dict[str, Dict]) -> Dict:
    """Analyze how audit logs have changed between sessions."""
    audit_changes = {
        "items_with_more_issues": [],
        "items_with_fewer_issues": [],
        "items_with_new_issues": [],
        "common_audit_messages": Counter(),
        "summary": {},
    }

    multi_session_items = {k: v for k, v in item_comparison.items() if len(v) > 1}

    for waterbody, session_results in multi_session_items.items():
        session_names = list(session_results.keys())

        for i in range(len(session_names)):
            for j in range(i + 1, len(session_names)):
                session1, session2 = session_names[i], session_names[j]

                item1 = session_results[session1]["item_data"]
                item2 = session_results[session2]["item_data"]

                audit1 = item1.get("audit_log", [])
                audit2 = item2.get("audit_log", [])

                if len(audit1) != len(audit2):
                    change_info = {
                        "waterbody": waterbody,
                        "session_pair": f"{session1} -> {session2}",
                        "before_count": len(audit1),
                        "after_count": len(audit2),
                        "before": audit1,
                        "after": audit2,
                    }

                    if len(audit2) > len(audit1):
                        audit_changes["items_with_more_issues"].append(change_info)
                    else:
                        audit_changes["items_with_fewer_issues"].append(change_info)

                # Track new issues that appeared
                new_issues = set(audit2) - set(audit1)
                if new_issues:
                    audit_changes["items_with_new_issues"].append(
                        {
                            "waterbody": waterbody,
                            "session_pair": f"{session1} -> {session2}",
                            "new_issues": list(new_issues),
                        }
                    )

                # Count all audit messages
                for msg in audit2:
                    audit_changes["common_audit_messages"][msg] += 1

    audit_changes["summary"] = {
        "total_items_compared": len(multi_session_items),
        "items_with_more_issues": len(audit_changes["items_with_more_issues"]),
        "items_with_fewer_issues": len(audit_changes["items_with_fewer_issues"]),
        "items_with_new_issues": len(audit_changes["items_with_new_issues"]),
    }

    return audit_changes


def analyze_rule_text_changes(item_comparison: Dict[str, Dict]) -> Dict:
    """Analyze changes in rule_text_verbatim arrays (interleaved rule splitting)."""
    text_changes = {
        "items_with_text_changes": [],
        "single_to_multi": [],  # Rules split from single string to multiple
        "multi_to_single": [],  # Rules merged from multiple to single
        "text_modifications": [],  # Text content changed
        "summary": {},
    }

    multi_session_items = {k: v for k, v in item_comparison.items() if len(v) > 1}

    for waterbody, session_results in multi_session_items.items():
        session_names = list(session_results.keys())

        for i in range(len(session_names)):
            for j in range(i + 1, len(session_names)):
                session1, session2 = session_names[i], session_names[j]

                item1 = session_results[session1]["item_data"]
                item2 = session_results[session2]["item_data"]

                rules1 = item1.get("rules", [])
                rules2 = item2.get("rules", [])

                # Check each rule's text verbatim array
                for rule_idx in range(min(len(rules1), len(rules2))):
                    text1 = rules1[rule_idx].get("rule_text_verbatim", [])
                    text2 = rules2[rule_idx].get("rule_text_verbatim", [])

                    # Ensure both are lists
                    if not isinstance(text1, list):
                        text1 = [text1] if text1 else []
                    if not isinstance(text2, list):
                        text2 = [text2] if text2 else []

                    if text1 != text2:
                        change_info = {
                            "waterbody": waterbody,
                            "session_pair": f"{session1} -> {session2}",
                            "rule_index": rule_idx,
                            "before_count": len(text1),
                            "after_count": len(text2),
                            "before": text1,
                            "after": text2,
                        }

                        if len(text1) == 1 and len(text2) > 1:
                            text_changes["single_to_multi"].append(change_info)
                        elif len(text1) > 1 and len(text2) == 1:
                            text_changes["multi_to_single"].append(change_info)
                        else:
                            text_changes["text_modifications"].append(change_info)

                        text_changes["items_with_text_changes"].append(change_info)

    text_changes["summary"] = {
        "total_items_compared": len(multi_session_items),
        "items_with_text_changes": len(text_changes["items_with_text_changes"]),
        "single_to_multi_splits": len(text_changes["single_to_multi"]),
        "multi_to_single_merges": len(text_changes["multi_to_single"]),
        "text_modifications": len(text_changes["text_modifications"]),
    }

    return text_changes


def analyze_scope_changes(item_comparison: Dict[str, Dict]) -> Dict:
    """Analyze how scopes have changed between sessions."""
    scope_changes = {
        "items_with_scope_changes": [],
        "meaningful_scope_changes": [],
        "minor_text_changes": [],
        "common_scope_type_changes": Counter(),
        "scope_addition_removal": [],
        "summary": {},
    }

    # Only compare items present in multiple sessions
    multi_session_items = {k: v for k, v in item_comparison.items() if len(v) > 1}

    for waterbody, session_results in multi_session_items.items():
        session_names = list(session_results.keys())

        # Compare scopes between all pairs of sessions for this item
        for i in range(len(session_names)):
            for j in range(i + 1, len(session_names)):
                session1, session2 = session_names[i], session_names[j]

                item1 = session_results[session1]["item_data"]
                item2 = session_results[session2]["item_data"]

                rules1 = item1.get("rules", [])
                rules2 = item2.get("rules", [])

                # Extract scope information
                scopes1 = [
                    (
                        rule.get("scope", {}).get("type"),
                        rule.get("scope", {}).get("location_verbatim"),
                    )
                    for rule in rules1
                ]
                scopes2 = [
                    (
                        rule.get("scope", {}).get("type"),
                        rule.get("scope", {}).get("location_verbatim"),
                    )
                    for rule in rules2
                ]

                if scopes1 != scopes2:
                    change_info = {
                        "waterbody": waterbody,
                        "session_pair": f"{session1} -> {session2}",
                        "scopes_before": scopes1,
                        "scopes_after": scopes2,
                        "rule_count_change": len(rules2) - len(rules1),
                    }

                    # Check if this is a meaningful change or just minor text variation
                    if scopes_are_meaningfully_different(scopes1, scopes2):
                        scope_changes["meaningful_scope_changes"].append(change_info)
                        scope_changes["items_with_scope_changes"].append(change_info)

                        # Track meaningful scope type changes only
                        types1 = [s[0] for s in scopes1 if s[0]]
                        types2 = [s[0] for s in scopes2 if s[0]]

                        for t1 in types1:
                            for t2 in types2:
                                if t1 != t2:
                                    scope_changes["common_scope_type_changes"][
                                        f"{t1} -> {t2}"
                                    ] += 1
                    else:
                        scope_changes["minor_text_changes"].append(change_info)

    scope_changes["summary"] = {
        "total_items_compared": len(multi_session_items),
        "items_with_any_changes": len(scope_changes["items_with_scope_changes"]),
        "items_with_meaningful_changes": len(scope_changes["meaningful_scope_changes"]),
        "items_with_minor_text_changes": len(scope_changes["minor_text_changes"]),
        "meaningful_change_rate": (
            len(scope_changes["meaningful_scope_changes"]) / len(multi_session_items)
            if multi_session_items
            else 0
        ),
    }

    return scope_changes


def generate_report(sessions: Dict[str, Dict], output_file: Path = None) -> str:
    """Generate a comprehensive comparison report focusing on scope changes."""
    report_lines = []

    # Header
    report_lines.append("=" * 80)
    report_lines.append("PARSING SESSION COMPARISON REPORT")
    report_lines.append(
        "Enhanced Analysis: Identity, Scopes, Restrictions, Audit Logs, Rule Text"
    )
    report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("=" * 80)
    report_lines.append("")

    # Session overview
    metadata = extract_session_metadata(sessions)
    report_lines.append("SESSION OVERVIEW:")
    report_lines.append("-" * 40)
    total_sessions = len(metadata)
    for i, (session_name, meta) in enumerate(metadata.items()):
        report_lines.append(f"{session_name}:")
        report_lines.append(f"  Total items: {meta['total_items']}")
        report_lines.append(f"  Successful: {meta['successful_items']}")
        report_lines.append(
            f"  Failed: {meta['total_items'] - meta['successful_items']}"
        )
        report_lines.append(f"  Success rate: {meta['success_rate']:.1%}")
        if i < total_sessions - 1:
            report_lines.append("")
    report_lines.append("")

    # Item-by-item comparison
    item_comparison = compare_item_success(sessions)
    consistency = analyze_consistency(item_comparison)

    # Filter to items present in multiple sessions
    multi_session_items = {k: v for k, v in item_comparison.items() if len(v) > 1}
    report_lines.append(
        f"ITEMS PRESENT IN MULTIPLE SESSIONS: {len(multi_session_items)}"
    )
    report_lines.append(
        f"Total unique items across all sessions: {len(item_comparison)}"
    )
    report_lines.append("")

    # Identity change analysis
    identity_changes = analyze_identity_changes(item_comparison)
    if (
        identity_changes["summary"]["waterbody_key_changes"] > 0
        or identity_changes["summary"]["global_scope_changes"] > 0
        or identity_changes["summary"]["exclusion_changes"] > 0
    ):
        report_lines.append("IDENTITY CHANGE ANALYSIS:")
        report_lines.append("-" * 40)
        report_lines.append(
            f"Waterbody key changes: {identity_changes['summary']['waterbody_key_changes']}"
        )
        report_lines.append(
            f"Global scope changes: {identity_changes['summary']['global_scope_changes']}"
        )
        report_lines.append(
            f"Exclusion changes: {identity_changes['summary']['exclusion_changes']}"
        )
        report_lines.append("")

        if identity_changes["waterbody_key_changes"]:
            report_lines.append("WATERBODY KEY CHANGES:")
            report_lines.append("-" * 50)
            for change in identity_changes["waterbody_key_changes"]:
                wb_display = (
                    change["waterbody"][:60] + "..."
                    if len(change["waterbody"]) > 60
                    else change["waterbody"]
                )
                report_lines.append(f"{wb_display} ({change['session_pair']}):")
                report_lines.append(f"  Before: {change['before']}")
                report_lines.append(f"  After:  {change['after']}")
            report_lines.append("")

        if identity_changes["global_scope_changes"]:
            report_lines.append(
                f"GLOBAL SCOPE CHANGES ({len(identity_changes['global_scope_changes'])} items):"
            )
            report_lines.append("-" * 50)
            for change in identity_changes["global_scope_changes"]:
                wb_display = (
                    change["waterbody"][:60] + "..."
                    if len(change["waterbody"]) > 60
                    else change["waterbody"]
                )
                report_lines.append(f"{wb_display} ({change['session_pair']}):")
                report_lines.append(
                    f"  Before: {change['before'].get('type')} - {change['before'].get('location_verbatim', 'None')}"
                )
                report_lines.append(
                    f"  After:  {change['after'].get('type')} - {change['after'].get('location_verbatim', 'None')}"
                )
            report_lines.append("")

        if identity_changes["exclusion_changes"]:
            report_lines.append(
                f"EXCLUSION CHANGES ({len(identity_changes['exclusion_changes'])} items):"
            )
            report_lines.append("-" * 50)
            for change in identity_changes["exclusion_changes"]:
                wb_display = (
                    change["waterbody"][:60] + "..."
                    if len(change["waterbody"]) > 60
                    else change["waterbody"]
                )
                report_lines.append(f"{wb_display} ({change['session_pair']}):")
                report_lines.append(
                    f"  Exclusion count: {change['before_count']} -> {change['after_count']}"
                )
                if change["before"]:
                    report_lines.append("  Before:")
                    for i, excl in enumerate(change["before"]):
                        report_lines.append(
                            f"    {i+1}. {excl.get('type')} - {excl.get('location_verbatim', 'None')}"
                        )
                if change["after"]:
                    report_lines.append("  After:")
                    for i, excl in enumerate(change["after"]):
                        report_lines.append(
                            f"    {i+1}. {excl.get('type')} - {excl.get('location_verbatim', 'None')}"
                        )
            report_lines.append("")

    # Scope change analysis
    scope_changes = analyze_scope_changes(item_comparison)

    # Restriction change analysis
    restriction_changes = analyze_restriction_changes(item_comparison)
    if (
        restriction_changes["summary"]["items_with_restriction_changes"] > 0
        or restriction_changes["summary"]["items_with_date_changes"] > 0
    ):
        report_lines.append("RESTRICTION CHANGE ANALYSIS:")
        report_lines.append("-" * 40)
        report_lines.append(
            f"Items with restriction type changes: {restriction_changes['summary']['items_with_restriction_changes']}"
        )
        report_lines.append(
            f"Items with date changes: {restriction_changes['summary']['items_with_date_changes']}"
        )
        report_lines.append("")

        if restriction_changes["restriction_type_changes"]:
            report_lines.append("MOST COMMON RESTRICTION TYPE CHANGES:")
            report_lines.append("-" * 50)
            for change, count in restriction_changes[
                "restriction_type_changes"
            ].most_common(10):
                report_lines.append(f"{change}: {count} times")
            report_lines.append("")

    # Audit log analysis
    audit_changes = analyze_audit_log_changes(item_comparison)
    if (
        audit_changes["summary"]["items_with_more_issues"] > 0
        or audit_changes["summary"]["items_with_fewer_issues"] > 0
    ):
        report_lines.append("AUDIT LOG CHANGE ANALYSIS:")
        report_lines.append("-" * 40)
        report_lines.append(
            f"Items with MORE audit issues: {audit_changes['summary']['items_with_more_issues']}"
        )
        report_lines.append(
            f"Items with FEWER audit issues: {audit_changes['summary']['items_with_fewer_issues']}"
        )
        report_lines.append(
            f"Items with new issues: {audit_changes['summary']['items_with_new_issues']}"
        )
        report_lines.append("")

        if audit_changes["common_audit_messages"]:
            report_lines.append("MOST COMMON AUDIT MESSAGES:")
            report_lines.append("-" * 50)
            for msg, count in audit_changes["common_audit_messages"].most_common(10):
                msg_display = msg[:80] + "..." if len(msg) > 80 else msg
                report_lines.append(f"{msg_display}: {count} times")
            report_lines.append("")

        if audit_changes["items_with_more_issues"][:5]:  # Show first 5
            report_lines.append("EXAMPLES OF ITEMS WITH MORE AUDIT ISSUES:")
            report_lines.append("-" * 50)
            for change in audit_changes["items_with_more_issues"][:5]:
                wb_display = (
                    change["waterbody"][:60] + "..."
                    if len(change["waterbody"]) > 60
                    else change["waterbody"]
                )
                report_lines.append(f"{wb_display} ({change['session_pair']}):")
                report_lines.append(
                    f"  Issue count: {change['before_count']} -> {change['after_count']}"
                )
                if change["after"]:
                    report_lines.append("  New issues:")
                    for issue in change["after"][:3]:  # Show first 3
                        issue_display = (
                            issue[:100] + "..." if len(issue) > 100 else issue
                        )
                        report_lines.append(f"    • {issue_display}")
            report_lines.append("")

    # Rule text verbatim analysis
    text_changes = analyze_rule_text_changes(item_comparison)
    if text_changes["summary"]["items_with_text_changes"] > 0:
        report_lines.append("RULE TEXT VERBATIM CHANGE ANALYSIS:")
        report_lines.append("-" * 40)
        report_lines.append(
            f"Total rule text changes: {text_changes['summary']['items_with_text_changes']}"
        )
        report_lines.append(
            f"Single → Multiple (rule splitting): {text_changes['summary']['single_to_multi_splits']}"
        )
        report_lines.append(
            f"Multiple → Single (rule merging): {text_changes['summary']['multi_to_single_merges']}"
        )
        report_lines.append(
            f"Text modifications: {text_changes['summary']['text_modifications']}"
        )
        report_lines.append("")

        if text_changes["single_to_multi"]:
            report_lines.append(
                f"RULE SPLITTING (Single → Multiple) - {len(text_changes['single_to_multi'])} cases:"
            )
            report_lines.append("-" * 50)
            for change in text_changes["single_to_multi"][:5]:  # Show first 5
                wb_display = (
                    change["waterbody"][:60] + "..."
                    if len(change["waterbody"]) > 60
                    else change["waterbody"]
                )
                report_lines.append(
                    f"{wb_display} ({change['session_pair']}) - Rule {change['rule_index'] + 1}:"
                )
                report_lines.append(f"  Before: 1 text element")
                if change["before"]:
                    text_display = (
                        change["before"][0][:100] + "..."
                        if len(change["before"][0]) > 100
                        else change["before"][0]
                    )
                    report_lines.append(f"    • {text_display}")
                report_lines.append(f"  After: {change['after_count']} text elements")
                for idx, text in enumerate(change["after"][:3]):  # Show first 3
                    text_display = text[:100] + "..." if len(text) > 100 else text
                    report_lines.append(f"    {idx + 1}. {text_display}")
            report_lines.append("")

    report_lines.append("SCOPE CHANGE ANALYSIS:")
    report_lines.append("-" * 40)
    report_lines.append(
        f"Items compared: {scope_changes['summary']['total_items_compared']}"
    )
    report_lines.append(
        f"Items with meaningful scope changes: {scope_changes['summary']['items_with_meaningful_changes']}"
    )
    report_lines.append(
        f"Items with minor text changes: {scope_changes['summary']['items_with_minor_text_changes']}"
    )
    report_lines.append(
        f"Items unchanged: {scope_changes['summary']['total_items_compared'] - scope_changes['summary']['items_with_meaningful_changes'] - scope_changes['summary']['items_with_minor_text_changes']}"
    )
    report_lines.append(
        f"Meaningful change rate: {scope_changes['summary']['meaningful_change_rate']:.1%}"
    )
    minor_rate = (
        scope_changes["summary"]["items_with_minor_text_changes"]
        / scope_changes["summary"]["total_items_compared"]
        if scope_changes["summary"]["total_items_compared"] > 0
        else 0
    )
    report_lines.append(f"Minor change rate: {minor_rate:.1%}")
    report_lines.append("")

    # Common scope type changes
    if scope_changes["common_scope_type_changes"]:
        report_lines.append("MOST COMMON MEANINGFUL SCOPE TYPE CHANGES:")
        report_lines.append("-" * 50)
        for change, count in scope_changes["common_scope_type_changes"].most_common(10):
            report_lines.append(f"{change}: {count} times")
        report_lines.append("")

    # Show detailed meaningful scope changes
    if scope_changes["meaningful_scope_changes"]:
        report_lines.append(
            f"ALL MEANINGFUL SCOPE CHANGES ({len(scope_changes['meaningful_scope_changes'])} items):"
        )
        report_lines.append("-" * 50)

        # We need access to the session results for detailed rule analysis
        session_results = {}
        for waterbody, results in item_comparison.items():
            session_results[waterbody] = results

        for change in scope_changes["meaningful_scope_changes"]:
            waterbody_full = change["waterbody"]  # Keep original full name
            waterbody_display = (
                change["waterbody"][:60] + "..."
                if len(change["waterbody"]) > 60
                else change["waterbody"]
            )
            report_lines.append(f"\n{waterbody_display} ({change['session_pair']}):")
            report_lines.append(f"  Rule count change: {change['rule_count_change']}")

            if change["scopes_before"] != change["scopes_after"]:
                report_lines.append(f"  Before: {len(change['scopes_before'])} scopes")
                for i, (scope_type, location) in enumerate(change["scopes_before"]):
                    # Show full location text (up to 150 chars)
                    loc_display = (
                        (location[:150] + "...")
                        if location and len(location) > 150
                        else (location or "None")
                    )
                    report_lines.append(f"    {i+1}. {scope_type} - {loc_display}")

                report_lines.append(f"  After: {len(change['scopes_after'])} scopes")
                for i, (scope_type, location) in enumerate(change["scopes_after"]):
                    # Show full location text (up to 150 chars)
                    loc_display = (
                        (location[:150] + "...")
                        if location and len(location) > 150
                        else (location or "None")
                    )
                    report_lines.append(f"    {i+1}. {scope_type} - {loc_display}")

                # Add specific difference analysis
                report_lines.append("  Key Differences:")

                # Show full rule details for meaningful analysis - use full waterbody name
                session1, session2 = change["session_pair"].split(" -> ")
                if waterbody_full in session_results:
                    item1 = session_results[waterbody_full][session1]["item_data"]
                    item2 = session_results[waterbody_full][session2]["item_data"]
                    rules1 = item1.get("rules", [])
                    rules2 = item2.get("rules", [])

                    # Show what each rule actually contains
                    report_lines.append("    BEFORE:")
                    for i, rule in enumerate(rules1):
                        restrictions = [
                            r.get("type", "Unknown")
                            for r in rule.get("restrictions", [])
                        ]
                        scope_info = rule.get("scope", {})
                        scope_type = scope_info.get("type", "Unknown")
                        location = scope_info.get("location_verbatim")
                        # Don't truncate here - we'll show full text in comparison
                        loc_display = f" → Location: {location}" if location else ""
                        report_lines.append(
                            f"      Rule {i+1}: {scope_type} | Restrictions: {restrictions}{loc_display}"
                        )

                    report_lines.append("    AFTER:")
                    for i, rule in enumerate(rules2):
                        restrictions = [
                            r.get("type", "Unknown")
                            for r in rule.get("restrictions", [])
                        ]
                        scope_info = rule.get("scope", {})
                        scope_type = scope_info.get("type", "Unknown")
                        location = scope_info.get("location_verbatim")
                        # Don't truncate here - we'll show full text in comparison
                        loc_display = f" → Location: {location}" if location else ""
                        report_lines.append(
                            f"      Rule {i+1}: {scope_type} | Restrictions: {restrictions}{loc_display}"
                        )

                    # Show audit logs for both sessions
                    audit_log1 = item1.get("audit_log", [])
                    audit_log2 = item2.get("audit_log", [])

                    if audit_log1 or audit_log2:
                        report_lines.append("    AUDIT LOGS:")
                        if audit_log1:
                            report_lines.append("      BEFORE:")
                            for log_entry in audit_log1:
                                # Truncate very long entries
                                entry_display = (
                                    (log_entry[:120] + "...")
                                    if len(log_entry) > 120
                                    else log_entry
                                )
                                report_lines.append(f"        • {entry_display}")
                        else:
                            report_lines.append("      BEFORE: (no audit entries)")

                        if audit_log2:
                            report_lines.append("      AFTER:")
                            for log_entry in audit_log2:
                                # Truncate very long entries
                                entry_display = (
                                    (log_entry[:120] + "...")
                                    if len(log_entry) > 120
                                    else log_entry
                                )
                                report_lines.append(f"        • {entry_display}")
                        else:
                            report_lines.append("      AFTER: (no audit entries)")

                    # Analyze what actually happened
                    report_lines.append("    WHAT CHANGED:")

                    if len(rules1) == 1 and len(rules2) > 1:
                        # Rule was split
                        old_restrictions = [
                            r.get("type") for r in rules1[0].get("restrictions", [])
                        ]
                        all_new_restrictions = []
                        for rule in rules2:
                            all_new_restrictions.extend(
                                [r.get("type") for r in rule.get("restrictions", [])]
                            )

                        # Check if scopes are identical
                        old_scope = rules1[0].get("scope", {})
                        new_scopes = [rule.get("scope", {}) for rule in rules2]
                        scopes_identical = all(
                            s.get("type") == old_scope.get("type")
                            and s.get("location_verbatim")
                            == old_scope.get("location_verbatim")
                            for s in new_scopes
                        )

                        if set(old_restrictions) == set(all_new_restrictions):
                            if scopes_identical:
                                report_lines.append(
                                    "      ✓ One rule was SPLIT into separate rules (same restrictions, IDENTICAL scopes)"
                                )
                            else:
                                report_lines.append(
                                    "      ✓ One rule was SPLIT into separate rules (same restrictions, DIFFERENT scopes)"
                                )
                                # Show scope differences
                                report_lines.append(
                                    f"        Original scope: {old_scope.get('type')} - {old_scope.get('location_verbatim') or 'None'}"
                                )
                                for i, scope in enumerate(new_scopes):
                                    if scope.get("type") != old_scope.get(
                                        "type"
                                    ) or scope.get(
                                        "location_verbatim"
                                    ) != old_scope.get(
                                        "location_verbatim"
                                    ):
                                        report_lines.append(
                                            f"        New rule {i+1} scope: {scope.get('type')} - {scope.get('location_verbatim') or 'None'}"
                                        )
                        else:
                            report_lines.append(
                                "      ⚠ Rule was split AND restrictions were modified"
                            )
                            added = set(all_new_restrictions) - set(old_restrictions)
                            removed = set(old_restrictions) - set(all_new_restrictions)
                            if added:
                                report_lines.append(
                                    f"        + Added restrictions: {list(added)}"
                                )
                            if removed:
                                report_lines.append(
                                    f"        - Removed restrictions: {list(removed)}"
                                )
                            if not scopes_identical:
                                report_lines.append(f"        ⚠ Scopes also changed")
                                for i, scope in enumerate(new_scopes):
                                    if scope.get("type") != old_scope.get(
                                        "type"
                                    ) or scope.get(
                                        "location_verbatim"
                                    ) != old_scope.get(
                                        "location_verbatim"
                                    ):
                                        report_lines.append(
                                            f"          Rule {i+1}: {old_scope.get('type')} → {scope.get('type')}"
                                        )
                                        if scope.get(
                                            "location_verbatim"
                                        ) != old_scope.get("location_verbatim"):
                                            report_lines.append(
                                                f"            Location: '{old_scope.get('location_verbatim') or 'None'}' → '{scope.get('location_verbatim') or 'None'}'"
                                            )

                    elif len(rules1) > 1 and len(rules2) == 1:
                        # Rules were merged
                        all_old_restrictions = []
                        for rule in rules1:
                            all_old_restrictions.extend(
                                [r.get("type") for r in rule.get("restrictions", [])]
                            )
                        new_restrictions = [
                            r.get("type") for r in rules2[0].get("restrictions", [])
                        ]

                        # Check if original scopes were identical
                        old_scopes = [rule.get("scope", {}) for rule in rules1]
                        new_scope = rules2[0].get("scope", {})
                        old_scopes_identical = all(
                            s.get("type") == old_scopes[0].get("type")
                            and s.get("location_verbatim")
                            == old_scopes[0].get("location_verbatim")
                            for s in old_scopes
                        )
                        scopes_match_merged = new_scope.get("type") == old_scopes[
                            0
                        ].get("type") and new_scope.get(
                            "location_verbatim"
                        ) == old_scopes[
                            0
                        ].get(
                            "location_verbatim"
                        )

                        if set(all_old_restrictions) == set(new_restrictions):
                            if old_scopes_identical and scopes_match_merged:
                                report_lines.append(
                                    "      ✓ Multiple rules were MERGED into one rule (same restrictions, IDENTICAL scopes)"
                                )
                            elif old_scopes_identical and not scopes_match_merged:
                                report_lines.append(
                                    "      ✓ Multiple rules were MERGED into one rule (same restrictions, scope changed)"
                                )
                                report_lines.append(
                                    f"        Original scopes (all identical): {old_scopes[0].get('type')} - {old_scopes[0].get('location_verbatim') or 'None'}"
                                )
                                report_lines.append(
                                    f"        Merged scope: {new_scope.get('type')} - {new_scope.get('location_verbatim') or 'None'}"
                                )
                            else:
                                report_lines.append(
                                    "      ✓ Multiple rules were MERGED into one rule (same restrictions, original scopes were DIFFERENT)"
                                )
                                report_lines.append(f"        Original scopes:")
                                for i, scope in enumerate(old_scopes):
                                    report_lines.append(
                                        f"          Rule {i+1}: {scope.get('type')} - {scope.get('location_verbatim') or 'None'}"
                                    )
                                report_lines.append(
                                    f"        Merged into: {new_scope.get('type')} - {new_scope.get('location_verbatim') or 'None'}"
                                )
                        else:
                            report_lines.append(
                                "      ⚠ Rules were merged AND restrictions were modified"
                            )
                            if not old_scopes_identical:
                                report_lines.append(
                                    f"        ⚠ Original scopes were also different"
                                )

                    elif len(rules1) == len(rules2):
                        # Same number of rules, check what changed
                        for i in range(len(rules1)):
                            restr1 = [
                                r.get("type") for r in rules1[i].get("restrictions", [])
                            ]
                            restr2 = [
                                r.get("type") for r in rules2[i].get("restrictions", [])
                            ]

                            if restr1 != restr2:
                                added = set(restr2) - set(restr1)
                                removed = set(restr1) - set(restr2)
                                if added or removed:
                                    report_lines.append(
                                        f"      ⚠ Rule {i+1} restrictions changed:"
                                    )
                                    if added:
                                        report_lines.append(
                                            f"        + Added: {list(added)}"
                                        )
                                    if removed:
                                        report_lines.append(
                                            f"        - Removed: {list(removed)}"
                                        )

                            # Check scope changes
                            scope1 = rules1[i].get("scope", {})
                            scope2 = rules2[i].get("scope", {})

                            if scope1.get("type") != scope2.get("type"):
                                report_lines.append(
                                    f"      ⚠ Rule {i+1} scope type: {scope1.get('type')} → {scope2.get('type')}"
                                )

                            if scope1.get("location_verbatim") != scope2.get(
                                "location_verbatim"
                            ):
                                loc1 = scope1.get("location_verbatim") or "None"
                                loc2 = scope2.get("location_verbatim") or "None"

                                # Smart location difference display
                                if loc1 in loc2:
                                    # loc2 has additional text
                                    added_text = loc2.replace(loc1, "", 1).strip()
                                    report_lines.append(
                                        f"      ⚠ Rule {i+1} location text ADDED: '{added_text}'"
                                    )
                                    report_lines.append(f"        Before: '{loc1}'")
                                    report_lines.append(f"        After:  '{loc2}'")
                                elif loc2 in loc1:
                                    # loc1 had text that was removed
                                    removed_text = loc1.replace(loc2, "", 1).strip()
                                    report_lines.append(
                                        f"      ⚠ Rule {i+1} location text REMOVED: '{removed_text}'"
                                    )
                                    report_lines.append(f"        Before: '{loc1}'")
                                    report_lines.append(f"        After:  '{loc2}'")
                                else:
                                    # Completely different
                                    report_lines.append(
                                        f"      ⚠ Rule {i+1} location CHANGED:"
                                    )
                                    report_lines.append(f"        Before: '{loc1}'")
                                    report_lines.append(f"        After:  '{loc2}'")

                    else:
                        # Different number of rules, more complex change
                        report_lines.append(
                            f"      ⚠ Complex change: {len(rules1)} → {len(rules2)} rules"
                        )
                        if len(rules2) > len(rules1):
                            report_lines.append(
                                f"        + {len(rules2) - len(rules1)} rules added"
                            )
                        else:
                            report_lines.append(
                                f"        - {len(rules1) - len(rules2)} rules removed"
                            )
                else:
                    report_lines.append(
                        f"    • Could not find detailed rule data for comparison"
                    )
        report_lines.append("")

    # Show all minor text changes if there are any
    if scope_changes["minor_text_changes"]:
        report_lines.append(
            f"ALL MINOR TEXT CHANGES ({len(scope_changes['minor_text_changes'])} items):"
        )
        report_lines.append("-" * 50)
        for change in scope_changes["minor_text_changes"]:
            waterbody = (
                change["waterbody"][:60] + "..."
                if len(change["waterbody"]) > 60
                else change["waterbody"]
            )
            report_lines.append(f"\n{waterbody} ({change['session_pair']}):")
            report_lines.append(f"  Rule count change: {change['rule_count_change']}")

            # Show the minor differences in detail
            report_lines.append(f"  Before: {len(change['scopes_before'])} scopes")
            for i, (scope_type, location) in enumerate(change["scopes_before"]):
                loc_display = (
                    (location[:150] + "...")
                    if location and len(location) > 150
                    else (location or "None")
                )
                report_lines.append(f"    {i+1}. {scope_type} - {loc_display}")

            report_lines.append(f"  After: {len(change['scopes_after'])} scopes")
            for i, (scope_type, location) in enumerate(change["scopes_after"]):
                loc_display = (
                    (location[:150] + "...")
                    if location and len(location) > 150
                    else (location or "None")
                )
                report_lines.append(f"    {i+1}. {scope_type} - {loc_display}")

            # Show specific minor differences
            report_lines.append("  Minor Text Differences:")
            for i in range(len(change["scopes_before"])):
                if i < len(change["scopes_after"]):
                    before_loc = change["scopes_before"][i][1] or ""
                    after_loc = change["scopes_after"][i][1] or ""

                    if before_loc != after_loc:
                        before_display = (
                            (before_loc[:100] + "...")
                            if len(before_loc) > 100
                            else before_loc
                        )
                        after_display = (
                            (after_loc[:100] + "...")
                            if len(after_loc) > 100
                            else after_loc
                        )
                        report_lines.append(
                            f"    • Rule {i+1}: '{before_display}' → '{after_display}'"
                        )
        report_lines.append("")

    # Standard consistency analysis for multi-session items only
    multi_consistency = analyze_consistency(multi_session_items)
    report_lines.append("CONSISTENCY ANALYSIS (MULTI-SESSION ITEMS ONLY):")
    report_lines.append("-" * 40)
    report_lines.append(
        f"Always succeed: {len(multi_consistency['always_succeed'])} items"
    )
    report_lines.append(f"Always fail: {len(multi_consistency['always_fail'])} items")
    report_lines.append(f"Inconsistent: {len(multi_consistency['inconsistent'])} items")
    report_lines.append("")

    report = "\n".join(report_lines)

    if output_file:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"Report saved to: {output_file}")

    return report


def generate_summary_report(sessions: Dict[str, Dict]) -> str:
    """Generate a high-level summary report without detailed listings."""
    report_lines = []

    # Header
    report_lines.append("=" * 80)
    report_lines.append("PARSING SESSION COMPARISON - SUMMARY REPORT")
    report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("=" * 80)
    report_lines.append("")

    # Session overview
    metadata = extract_session_metadata(sessions)
    report_lines.append("SESSION OVERVIEW:")
    report_lines.append("-" * 40)
    for session_name, meta in metadata.items():
        report_lines.append(f"{session_name}:")
        report_lines.append(
            f"  Total: {meta['total_items']}, Success: {meta['successful_items']} ({meta['success_rate']:.1%})"
        )
    report_lines.append("")

    # Get all analyses
    item_comparison = compare_item_success(sessions)
    multi_session_items = {k: v for k, v in item_comparison.items() if len(v) > 1}

    identity_changes = analyze_identity_changes(item_comparison)
    restriction_changes = analyze_restriction_changes(item_comparison)
    audit_changes = analyze_audit_log_changes(item_comparison)
    text_changes = analyze_rule_text_changes(item_comparison)
    scope_changes = analyze_scope_changes(item_comparison)
    consistency = analyze_consistency(multi_session_items)

    report_lines.append("SUMMARY STATISTICS:")
    report_lines.append("-" * 40)
    report_lines.append(f"Total unique items: {len(item_comparison)}")
    report_lines.append(f"Items in multiple sessions: {len(multi_session_items)}")
    report_lines.append("")

    report_lines.append("CONSISTENCY:")
    report_lines.append(f"  Always succeed: {len(consistency['always_succeed'])} items")
    report_lines.append(f"  Always fail: {len(consistency['always_fail'])} items")
    report_lines.append(f"  Inconsistent: {len(consistency['inconsistent'])} items")
    report_lines.append("")

    report_lines.append("CHANGES DETECTED:")
    report_lines.append(f"  Identity changes:")
    report_lines.append(
        f"    - Waterbody key: {identity_changes['summary']['waterbody_key_changes']}"
    )
    report_lines.append(
        f"    - Global scope: {identity_changes['summary']['global_scope_changes']}"
    )
    report_lines.append(
        f"    - Exclusions: {identity_changes['summary']['exclusion_changes']}"
    )
    report_lines.append(
        f"  Restriction changes: {restriction_changes['summary']['items_with_restriction_changes']}"
    )
    report_lines.append(f"  Scope changes:")
    report_lines.append(
        f"    - Meaningful: {scope_changes['summary']['items_with_meaningful_changes']}"
    )
    report_lines.append(
        f"    - Minor text: {scope_changes['summary']['items_with_minor_text_changes']}"
    )
    report_lines.append(
        f"  Rule text changes: {text_changes['summary']['items_with_text_changes']}"
    )
    report_lines.append(
        f"    - Rule splitting (1→N): {text_changes['summary']['single_to_multi_splits']}"
    )
    report_lines.append(
        f"    - Rule merging (N→1): {text_changes['summary']['multi_to_single_merges']}"
    )
    report_lines.append(f"  Audit log changes:")
    report_lines.append(
        f"    - More issues: {audit_changes['summary']['items_with_more_issues']}"
    )
    report_lines.append(
        f"    - Fewer issues: {audit_changes['summary']['items_with_fewer_issues']}"
    )
    report_lines.append("")

    report_lines.append("RATES:")
    total = len(multi_session_items) if multi_session_items else 1
    report_lines.append(
        f"  Meaningful scope change rate: {scope_changes['summary']['meaningful_change_rate']:.1%}"
    )
    report_lines.append(
        f"  Restriction change rate: {restriction_changes['summary']['items_with_restriction_changes']/total:.1%}"
    )
    report_lines.append(
        f"  Rule text change rate: {text_changes['summary']['items_with_text_changes']/total:.1%}"
    )
    report_lines.append("")

    return "\n".join(report_lines)


def detailed_item_comparison(sessions: Dict[str, Dict], waterbody_name: str) -> str:
    """Generate detailed comparison for a specific waterbody, focusing on scopes."""
    report_lines = []

    report_lines.append(f"DETAILED COMPARISON: {waterbody_name}")
    report_lines.append("=" * 80)

    rule_comparisons = compare_rule_structures(sessions, waterbody_name)

    if not rule_comparisons:
        report_lines.append(f"No data found for '{waterbody_name}' in any session.")
        return "\n".join(report_lines)

    for session_name, details in rule_comparisons.items():
        report_lines.append(f"\n{session_name}:")
        report_lines.append("-" * 40)
        report_lines.append(f"Rule count: {details['rule_count']}")

        if details["scope_details"]:
            report_lines.append("\nDetailed Scopes:")
            for i, scope in enumerate(details["scope_details"]):
                report_lines.append(f"  Rule {i+1}:")
                report_lines.append(f"    Type: {scope['type']}")
                if scope.get("waterbody_key"):
                    report_lines.append(f"    Waterbody Key: {scope['waterbody_key']}")
                if scope["location_verbatim"]:
                    location = (
                        scope["location_verbatim"][:100] + "..."
                        if len(scope["location_verbatim"]) > 100
                        else scope["location_verbatim"]
                    )
                    report_lines.append(f"    Location: {location}")
                if scope["landmark_verbatim"]:
                    landmark = (
                        scope["landmark_verbatim"][:80] + "..."
                        if len(scope["landmark_verbatim"]) > 80
                        else scope["landmark_verbatim"]
                    )
                    report_lines.append(f"    Landmark: {landmark}")
                if scope.get("landmark_end_verbatim"):
                    landmark_end = (
                        scope["landmark_end_verbatim"][:80] + "..."
                        if len(scope["landmark_end_verbatim"]) > 80
                        else scope["landmark_end_verbatim"]
                    )
                    report_lines.append(f"    Landmark End: {landmark_end}")
                if scope["direction"]:
                    report_lines.append(f"    Direction: {scope['direction']}")
                report_lines.append(
                    f"    Includes Tributaries: {scope['includes_tributaries']}"
                )

        report_lines.append(f"\nScope types: {details['scope_types']}")

    return "\n".join(report_lines)


def get_items_with_audits(session_data: Dict) -> List[Dict]:
    """Extract all items that have non-empty audit_log entries."""
    items_with_audits = []

    # Handle different session file structures
    results_data = None
    if isinstance(session_data, list):
        results_data = session_data
    elif isinstance(session_data, dict) and "results" in session_data:
        results_data = session_data["results"]
    elif isinstance(session_data, dict) and len(session_data) > 0:
        first_value = next(iter(session_data.values()))
        if isinstance(first_value, dict) and (
            "rules" in first_value or "identity" in first_value
        ):
            results_data = list(session_data.values())

    if results_data and isinstance(results_data, list):
        for i, item in enumerate(results_data):
            if not item or not isinstance(item, dict):
                continue

            audit_log = item.get("audit_log", [])
            if audit_log and len(audit_log) > 0:
                items_with_audits.append(
                    {
                        "index": i,
                        "waterbody": item.get("identity", {}).get(
                            "name_verbatim", f"Item_{i}"
                        ),
                        "audit_count": len(audit_log),
                        "audit_log": audit_log,
                        "full_item": item,
                    }
                )

    return items_with_audits


def get_failed_items(session_data: Dict) -> List[Dict]:
    """Extract all items that have no rules (parsing failed)."""
    failed_items = []

    results_data = None
    if isinstance(session_data, list):
        results_data = session_data
    elif isinstance(session_data, dict) and "results" in session_data:
        results_data = session_data["results"]
    elif isinstance(session_data, dict) and len(session_data) > 0:
        first_value = next(iter(session_data.values()))
        if isinstance(first_value, dict) and (
            "rules" in first_value or "identity" in first_value
        ):
            results_data = list(session_data.values())

    if results_data and isinstance(results_data, list):
        for i, item in enumerate(results_data):
            if not item or not isinstance(item, dict):
                failed_items.append(
                    {
                        "index": i,
                        "waterbody": f"Item_{i}",
                        "reason": "Null or invalid item",
                        "full_item": item,
                    }
                )
                continue

            rules = item.get("rules", [])
            if not rules or len(rules) == 0:
                failed_items.append(
                    {
                        "index": i,
                        "waterbody": item.get("identity", {}).get(
                            "name_verbatim", f"Item_{i}"
                        ),
                        "reason": "No rules parsed",
                        "identity": item.get("identity"),
                        "audit_log": item.get("audit_log", []),
                        "full_item": item,
                    }
                )

    return failed_items


def get_complex_items(session_data: Dict, min_rules: int = 5) -> List[Dict]:
    """Extract items with many rules (complex regulations)."""
    complex_items = []

    results_data = None
    if isinstance(session_data, list):
        results_data = session_data
    elif isinstance(session_data, dict) and "results" in session_data:
        results_data = session_data["results"]
    elif isinstance(session_data, dict) and len(session_data) > 0:
        first_value = next(iter(session_data.values()))
        if isinstance(first_value, dict) and (
            "rules" in first_value or "identity" in first_value
        ):
            results_data = list(session_data.values())

    if results_data and isinstance(results_data, list):
        for i, item in enumerate(results_data):
            if not item or not isinstance(item, dict):
                continue

            rules = item.get("rules", [])
            if len(rules) >= min_rules:
                scope_types = [rule.get("scope", {}).get("type") for rule in rules]
                restriction_types = []
                for rule in rules:
                    restriction_types.extend(
                        [r.get("type") for r in rule.get("restrictions", [])]
                    )

                complex_items.append(
                    {
                        "index": i,
                        "waterbody": item.get("identity", {}).get(
                            "name_verbatim", f"Item_{i}"
                        ),
                        "rule_count": len(rules),
                        "scope_types": scope_types,
                        "restriction_types": restriction_types,
                        "audit_log": item.get("audit_log", []),
                        "full_item": item,
                    }
                )

    # Sort by rule count descending
    complex_items.sort(key=lambda x: x["rule_count"], reverse=True)
    return complex_items


def get_items_with_exclusions(session_data: Dict) -> List[Dict]:
    """Extract items that have exclusions in their identity."""
    items_with_exclusions = []

    results_data = None
    if isinstance(session_data, list):
        results_data = session_data
    elif isinstance(session_data, dict) and "results" in session_data:
        results_data = session_data["results"]
    elif isinstance(session_data, dict) and len(session_data) > 0:
        first_value = next(iter(session_data.values()))
        if isinstance(first_value, dict) and (
            "rules" in first_value or "identity" in first_value
        ):
            results_data = list(session_data.values())

    if results_data and isinstance(results_data, list):
        for i, item in enumerate(results_data):
            if not item or not isinstance(item, dict):
                continue

            exclusions = item.get("identity", {}).get("exclusions", [])
            if exclusions and len(exclusions) > 0:
                items_with_exclusions.append(
                    {
                        "index": i,
                        "waterbody": item.get("identity", {}).get(
                            "name_verbatim", f"Item_{i}"
                        ),
                        "exclusion_count": len(exclusions),
                        "exclusions": exclusions,
                        "full_item": item,
                    }
                )

    return items_with_exclusions


def get_items_with_vague_scopes(session_data: Dict) -> List[Dict]:
    """Extract items that have VAGUE scope types (potentially problematic)."""
    items_with_vague = []

    results_data = None
    if isinstance(session_data, list):
        results_data = session_data
    elif isinstance(session_data, dict) and "results" in session_data:
        results_data = session_data["results"]
    elif isinstance(session_data, dict) and len(session_data) > 0:
        first_value = next(iter(session_data.values()))
        if isinstance(first_value, dict) and (
            "rules" in first_value or "identity" in first_value
        ):
            results_data = list(session_data.values())

    if results_data and isinstance(results_data, list):
        for i, item in enumerate(results_data):
            if not item or not isinstance(item, dict):
                continue

            rules = item.get("rules", [])
            vague_rules = []
            for rule_idx, rule in enumerate(rules):
                scope = rule.get("scope", {})
                if scope.get("type") == "VAGUE":
                    vague_rules.append(
                        {
                            "rule_index": rule_idx,
                            "scope": scope,
                            "rule_text_verbatim": rule.get("rule_text_verbatim", []),
                        }
                    )

            if vague_rules:
                items_with_vague.append(
                    {
                        "index": i,
                        "waterbody": item.get("identity", {}).get(
                            "name_verbatim", f"Item_{i}"
                        ),
                        "vague_rule_count": len(vague_rules),
                        "vague_rules": vague_rules,
                        "full_item": item,
                    }
                )

    return items_with_vague


def get_items_with_brackets_in_title(session_data: Dict) -> List[Dict]:
    """Extract items that have parentheses/brackets in their waterbody name.

    These often indicate inclusions, exclusions, or scope definitions in the title itself.
    Examples:
    - "COWICHAN LAKE (including Bear Lake)"
    - "ADAMS RIVER (upstream of Adams Lake)"
    - "ELK RIVER'S TRIBUTARIES (see exceptions)"
    """
    items_with_brackets = []

    results_data = None
    if isinstance(session_data, list):
        results_data = session_data
    elif isinstance(session_data, dict) and "results" in session_data:
        results_data = session_data["results"]
    elif isinstance(session_data, dict) and len(session_data) > 0:
        first_value = next(iter(session_data.values()))
        if isinstance(first_value, dict) and (
            "rules" in first_value or "identity" in first_value
        ):
            results_data = list(session_data.values())

    if results_data and isinstance(results_data, list):
        for i, item in enumerate(results_data):
            if not item or not isinstance(item, dict):
                continue

            waterbody = item.get("identity", {}).get("name_verbatim", "")
            if "(" in waterbody and ")" in waterbody:
                # Extract what's in the brackets for display
                bracket_contents = []
                start_idx = 0
                while True:
                    start = waterbody.find("(", start_idx)
                    if start == -1:
                        break
                    end = waterbody.find(")", start)
                    if end == -1:
                        break
                    bracket_contents.append(waterbody[start + 1 : end])
                    start_idx = end + 1

                items_with_brackets.append(
                    {
                        "index": i,
                        "waterbody": waterbody,
                        "bracket_count": len(bracket_contents),
                        "bracket_contents": bracket_contents,
                        "waterbody_key": item.get("identity", {}).get(
                            "waterbody_key", ""
                        ),
                        "has_exclusions": len(
                            item.get("identity", {}).get("exclusions", [])
                        )
                        > 0,
                        "exclusion_count": len(
                            item.get("identity", {}).get("exclusions", [])
                        ),
                        "rule_count": len(item.get("rules", [])),
                        "full_item": item,
                    }
                )

    return items_with_brackets


def format_single_session_output(
    items: List[Dict], title: str, output_file: Path = None
) -> str:
    """Format output for single-session query results."""
    lines = []

    lines.append("=" * 80)
    lines.append(title.upper())
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Total items found: {len(items)}")
    lines.append("=" * 80)
    lines.append("")

    if not items:
        lines.append("No items found matching criteria.")
        return "\n".join(lines)

    for item in items:
        lines.append(f"[{item['index']}] {item['waterbody']}")
        lines.append("-" * 80)

        # Add specific details based on query type
        if "audit_count" in item:
            lines.append(f"Audit log entries: {item['audit_count']}")
            for i, audit in enumerate(item["audit_log"], 1):
                lines.append(f"  {i}. {audit}")

        if "reason" in item:
            lines.append(f"Failure reason: {item['reason']}")
            if item.get("audit_log"):
                lines.append(f"Audit log:")
                for i, audit in enumerate(item["audit_log"], 1):
                    lines.append(f"  {i}. {audit}")

        if "rule_count" in item and "scope_types" in item:
            lines.append(f"Rules: {item['rule_count']}")
            lines.append(f"Scope types: {item['scope_types']}")
            if item.get("audit_log"):
                lines.append(
                    f"Audit log ({len(item['audit_log'])} entries): {item['audit_log']}"
                )

        if "exclusion_count" in item and "exclusions" in item:
            lines.append(f"Exclusions: {item['exclusion_count']}")
            for i, excl in enumerate(item["exclusions"], 1):
                tribs_status = (
                    "includes tributaries"
                    if excl.get("includes_tributaries", False)
                    else "excludes tributaries"
                )
                lines.append(
                    f"  {i}. {excl.get('type')} - {excl.get('location_verbatim', 'None')} ({tribs_status})"
                )

        if "vague_rule_count" in item and "vague_rules" in item:
            lines.append(f"Vague scopes: {item['vague_rule_count']}")
            for vague_rule in item["vague_rules"]:
                lines.append(f"  Rule {vague_rule['rule_index']}:")
                lines.append(
                    f"    Location: {vague_rule['scope'].get('location_verbatim', 'None')}"
                )
                if vague_rule["rule_text_verbatim"]:
                    text_preview = vague_rule["rule_text_verbatim"][0][:100]
                    lines.append(f"    Text: {text_preview}...")

        if "bracket_count" in item and "bracket_contents" in item:
            lines.append(f"Bracket contents ({item['bracket_count']}):")
            for i, content in enumerate(item["bracket_contents"], 1):
                lines.append(f"  {i}. ({content})")
            lines.append(f"Waterbody key: {item.get('waterbody_key', 'Unknown')}")
            lines.append(
                f"Has exclusions: {item.get('has_exclusions', False)} (count: {item.get('exclusion_count', 0)})"
            )
            lines.append(f"Rules: {item.get('rule_count', 0)}")

        lines.append("")

    output = "\n".join(lines)

    if output_file:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"Results saved to: {output_file}")

    return output


def main():
    parser = argparse.ArgumentParser(
        description="Compare multiple parsing sessions or analyze single session for BC fishing regulations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  
  Multi-session comparison:
    %(prog)s session1.json session2.json
    %(prog)s session1.json session2.json --summary-only
    %(prog)s session1.json session2.json -o report.txt
    %(prog)s session1.json session2.json --item "ALOUETTE LAKE"
  
  Single-session analysis:
    %(prog)s session.json --get-audits              # Items with audit log entries
    %(prog)s session.json --get-failed              # Items that failed parsing
    %(prog)s session.json --get-complex             # Complex items (>=5 rules)
    %(prog)s session.json --get-exclusions          # Items with exclusions
    %(prog)s session.json --get-vague-scopes        # Items with VAGUE scopes
    %(prog)s session.json --get-brackets            # Items with parentheses in title
    
  Save output:
    %(prog)s session.json --get-audits -o audits.txt
        """,
    )

    parser.add_argument(
        "sessions",
        nargs="+",
        help="One or more session JSON files to analyze. For multi-session comparison, provide 2+ files. "
        "For single-session queries (--get-* options), provide exactly 1 file.",
    )

    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Output file path for the report. If not specified, prints to stdout.",
    )

    parser.add_argument(
        "--item",
        metavar="WATERBODY_NAME",
        help="Show detailed comparison for a specific waterbody across all sessions. "
        "Use the exact name_verbatim from the parsed data.",
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("../output/parse_synopsis"),
        help="Base directory for resolving relative session file paths. Default: %(default)s",
    )

    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Show only high-level summary statistics without detailed change listings. "
        "Useful for quick overview of session differences.",
    )

    # Single-session analysis options
    analysis_group = parser.add_argument_group(
        "Single-Session Analysis Options",
        "These options analyze one session file and require exactly one session argument.",
    )

    analysis_group.add_argument(
        "--get-audits",
        action="store_true",
        help="Get all items with non-empty audit_log entries. These are items the LLM flagged "
        "as ambiguous, problematic, or requiring human review.",
    )

    analysis_group.add_argument(
        "--get-failed",
        action="store_true",
        help="Get all items that failed to parse (no rules generated). Useful for identifying "
        "parsing errors or problematic regulation text.",
    )

    analysis_group.add_argument(
        "--get-complex",
        action="store_true",
        help="Get items with many rules (default: >=5 rules, configurable with --min-rules). "
        "Helps identify complex regulations that may need special attention.",
    )

    analysis_group.add_argument(
        "--min-rules",
        type=int,
        default=5,
        metavar="N",
        help="Minimum number of rules for --get-complex filter. Default: %(default)s",
    )

    analysis_group.add_argument(
        "--get-exclusions",
        action="store_true",
        help="Get items with exclusions in their identity (e.g., 'RIVER EXCEPT: Creek X'). "
        "Shows how many exclusions and their details.",
    )

    analysis_group.add_argument(
        "--get-vague-scopes",
        action="store_true",
        help="Get items with VAGUE scope types. VAGUE scopes indicate the LLM couldn't classify "
        "the location into standard types (SEGMENT, DIRECTIONAL, etc.), suggesting ambiguous text.",
    )

    analysis_group.add_argument(
        "--get-brackets",
        action="store_true",
        help="Get items with parentheses/brackets in waterbody name. These often indicate "
        "inclusions ('including X'), scope definitions ('upstream of Y'), or exceptions. "
        "Useful for finding candidates for the inclusions field.",
    )

    args = parser.parse_args()

    # Check if any single-session option is specified
    single_session_mode = any(
        [
            args.get_audits,
            args.get_failed,
            args.get_complex,
            args.get_exclusions,
            args.get_vague_scopes,
            args.get_brackets,
        ]
    )

    # Validate: single-session mode requires exactly one session
    if single_session_mode and len(args.sessions) > 1:
        print(
            "Error: Single-session analysis options (--get-*) require exactly one session file."
        )
        return 1

    # Resolve session file paths
    session_files = []
    for session in args.sessions:
        session_path = Path(session)

        # If it's already absolute, use as-is
        if session_path.is_absolute():
            session_files.append(session_path)
            continue

        # Try the path as-is first (relative to current directory)
        if session_path.exists():
            session_files.append(session_path)
            continue

        # Try with .json extension if no extension
        if not session_path.suffix:
            json_path = session_path.with_suffix(".json")
            if json_path.exists():
                session_files.append(json_path)
                continue

        # Finally try relative to output_dir (original behavior)
        output_relative_path = args.output_dir / session
        if output_relative_path.exists():
            session_files.append(output_relative_path)
            continue

        # Try output_dir with .json extension
        if not Path(session).suffix:
            output_json_path = args.output_dir / (session + ".json")
            if output_json_path.exists():
                session_files.append(output_json_path)
                continue

        # If nothing worked, just add the original path (will show error later)
        session_files.append(session_path)

    # Load sessions
    sessions = load_session_results(session_files)

    if not sessions:
        print("No valid sessions found!")
        return 1

    # Single-session analysis mode
    if single_session_mode:
        if len(sessions) != 1:
            print("Error: Single-session analysis requires exactly one session file.")
            return 1

        session_name, session_data = next(iter(sessions.items()))
        print(f"Analyzing session: {session_name}\n")

        if args.get_audits:
            items = get_items_with_audits(session_data)
            output = format_single_session_output(
                items, f"Items with Audit Log Entries - {session_name}", args.output
            )
            if not args.output:
                print(output)

        elif args.get_failed:
            items = get_failed_items(session_data)
            output = format_single_session_output(
                items, f"Failed Items (No Rules) - {session_name}", args.output
            )
            if not args.output:
                print(output)

        elif args.get_complex:
            items = get_complex_items(session_data, args.min_rules)
            output = format_single_session_output(
                items,
                f"Complex Items (>={args.min_rules} rules) - {session_name}",
                args.output,
            )
            if not args.output:
                print(output)

        elif args.get_exclusions:
            items = get_items_with_exclusions(session_data)
            output = format_single_session_output(
                items, f"Items with Exclusions - {session_name}", args.output
            )
            if not args.output:
                print(output)

        elif args.get_vague_scopes:
            items = get_items_with_vague_scopes(session_data)
            output = format_single_session_output(
                items, f"Items with VAGUE Scopes - {session_name}", args.output
            )
            if not args.output:
                print(output)

        elif args.get_brackets:
            items = get_items_with_brackets_in_title(session_data)
            output = format_single_session_output(
                items, f"Items with Brackets in Title - {session_name}", args.output
            )
            if not args.output:
                print(output)

        return 0

    # Multi-session comparison mode (original functionality)
    if args.item:
        # Detailed item comparison
        comparison = detailed_item_comparison(sessions, args.item)
        print(comparison)
    elif args.summary_only:
        # Generate summary report
        report = generate_summary_report(sessions)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(report)
            print(f"Summary report saved to: {args.output}")
        else:
            print(report)
    else:
        # Generate full report
        report = generate_report(sessions, args.output)
        if not args.output:
            print(report)

    return 0


if __name__ == "__main__":
    exit(main())
