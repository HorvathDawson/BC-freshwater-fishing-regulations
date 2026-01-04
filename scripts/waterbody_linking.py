#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fast Waterbody Linking Using Pre-built Index

This script matches waterbody names from the fishing synopsis JSON
to actual geographic features using a pre-built index.

Features:
- O(1) Exact Matching
- Heuristic Matching (Possessives, Plurals, Singular <-> Plural)
- Manual Lookup Table with Notes
- SPLIT Support (Mapping 1 input name to multiple geographic features)
- Detailed CSV/JSON reporting

Input:
- scripts/output/fishing_data.json
- scripts/output/waterbody_index.json

Output:
- scripts/output/matched_waterbodies.json
- scripts/output/unmatched_waterbodies.csv

Author: BC Freshwater Fishing Regulations Project
Date: January 4, 2026
"""

import json
import re
import signal
import sys
import os
import time
import csv
from pathlib import Path
from difflib import SequenceMatcher

# ANSI color codes for terminal output
COLOR_GREEN = '\033[92m'  # Success/Match
COLOR_YELLOW = '\033[93m'  # Warning/Multiple matches or partials
COLOR_RED = '\033[91m'  # Error/No match
COLOR_CYAN = '\033[96m'  # Info/Similar names
COLOR_RESET = '\033[0m'  # Reset to default

# --- MANUAL NAME CORRECTIONS (LOOKUP TABLE) ---
NAME_CORRECTIONS = {
    # 1. SPLIT EXAMPLE
    "chilliwack / vedder rivers (does not include sumas river) (see map on page 24)": {
        "name": ["chilliwack river", "vedder river"],
        "note": "Split combined entry into distinct rivers"
    },
    
    # 2. SPELLING / ALIAS CORRECTIONS
    "toquart lake": {"name": "toquaht lake", "note": "Spelling mismatch"},
    "toquart river": {"name": "toquaht river", "note": "Spelling mismatch"},
    # "tee pee lakes": {"name": "tepee lakes", "note": "Spelling mismatch"},
    # "trepanier river": {"name": "trepanier creek", "note": "Gazetteer lists as Creek"},
    "tuc-el-nuit lake": {"name": "tugulnuit lake", "note": "Spelling mismatch"},

    # 3. IGNORABLE ENTRIES
    "arrow lakes": {
        "name": "arrow lakes",
        "note": "IGNORE: Regulation refers to Upper/Lower Arrow Lake details"
    },
    "arrow lakes’ tributaries": {
        "name": "arrow lakes tributaries",
        "note": "IGNORE: Likely covered by Upper/Lower tributaries"
    },
}

def normalize_name(name):
    """Normalize a waterbody name for comparison."""
    if not name: return ""
    clean = name.replace('"', '').replace("'", '').replace('\u201c', '').replace('\u201d', '').replace('\u2018', '').replace('\u2019', '').replace('`', '')
    clean = re.sub(r'\([^)]*\)', '', clean)
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean.lower()

def extract_quoted_text(name):
    """Extract text within quotes."""
    if not name: return None
    match = re.search(r'\u201c([^\u201d]+)\u201d', name)
    if match: return normalize_name(match.group(1))
    match = re.search(r'"([^"]+)"', name)
    if match: return normalize_name(match.group(1))
    return None

def generate_variations(normalized_name):
    """Generate heuristic variations (plurals, possessives)."""
    variations = []
    
    # 1. Singular <-> Plural Logic
    if normalized_name.endswith(" lakes"):
        variations.append(normalized_name[:-1]) 
    elif normalized_name.endswith(" lake"):
        variations.append(normalized_name + "s")

    # 2. Possessives & Tributaries
    if " lakes tributaries" in normalized_name:
        variations.append(normalized_name.replace(" lakes tributaries", " lake tributary"))
    if " rivers tributaries" in normalized_name:
        variations.append(normalized_name.replace(" rivers tributaries", " river tributary"))
    elif normalized_name.endswith(" tributaries"):
        variations.append(normalized_name.replace(" tributaries", " tributary"))
    
    # 3. Outlet Streams
    if " lakes outlet stream" in normalized_name:
        variations.append(normalized_name.replace(" lakes outlet stream", " lake tributary"))
        variations.append(normalized_name.replace(" lakes outlet stream", " lake outlet"))

    # 4. Generic Plurals
    if " lakes " in normalized_name:
        variations.append(normalized_name.replace(" lakes ", " lake "))
    if " rivers " in normalized_name:
        variations.append(normalized_name.replace(" rivers ", " river "))
        
    return variations

def extract_zone_from_mu(management_units):
    """Extract zone number from management unit string."""
    if not management_units or len(management_units) == 0: return None
    match = re.match(r'(\d+)-', management_units[0])
    if match: return match.group(1)
    return None

def get_unique_id(feature):
    """Safely extract a unique ID from a feature (handles Lakes vs Rivers)."""
    # Lakes have 'waterbody_poly_id', Rivers have 'linear_feature_id'
    return feature.get('waterbody_poly_id', feature.get('linear_feature_id', 'unknown_id'))

def find_similar_names(waterbody_name, zone, index, top_n=5):
    """Find similar waterbody names using fuzzy matching."""
    if not zone or zone not in index: return []
    similar_names = []
    zone_data = index[zone]
    for indexed_name, features in zone_data.items():
        ratio = SequenceMatcher(None, waterbody_name, indexed_name).ratio()
        if ratio > 0.6:
            feature_type = features[0]['type'] if features else 'unknown'
            original_name = features[0]['gnis_name'] if features else indexed_name
            similar_names.append((ratio, original_name, feature_type))
    similar_names.sort(reverse=True, key=lambda x: x[0])
    return similar_names[:top_n]

def find_waterbody_matches(waterbody_name, zone, index):
    """O(1) lookup in index."""
    if not zone or zone not in index: return []
    return index[zone].get(waterbody_name, [])

def main():
    script_dir = Path(__file__).parent
    json_path = script_dir / "output" / "fishing_data.json"
    index_path = script_dir / "output" / "waterbody_index.json"
    
    if not json_path.exists():
        print(f"ERROR: Fishing data JSON not found: {json_path}")
        return
    if not index_path.exists():
        print(f"{COLOR_YELLOW}Index not found. Building...{COLOR_RESET}")
        try:
            from build_waterbody_index import main as build_index
            build_index()
        except Exception as e:
            print(f"{COLOR_RED}ERROR: {e}{COLOR_RESET}")
            return
    
    print(f"Loading data...")
    with open(json_path, 'r', encoding='utf-8') as f: fishing_data = json.load(f)
    with open(index_path, 'r', encoding='utf-8') as f: waterbody_index = json.load(f)
    
    print("\n" + "="*80 + "\nFAST WATERBODY LINKING REPORT\n" + "="*80 + "\n")
    
    start_time = time.time()
    total_waterbodies = 0
    total_matched = 0
    total_unmatched = 0
    
    matched_list = []
    unmatched_list = []
    
    regions_data = fishing_data.get('regionsData', {})
    estimated_total = sum(len(wb) for wb in regions_data.values())
    
    for region_name, waterbodies in regions_data.items():
        print(f"\n{'-'*80}\nREGION: {region_name}\n{'-'*80}")
        
        for waterbody_key, entries in waterbodies.items():
            if not entries: continue
            total_waterbodies += 1
            
            if total_waterbodies % 100 == 0:
                elapsed = time.time() - start_time
                rate = total_waterbodies / elapsed if elapsed > 0 else 0
                remaining = estimated_total - total_waterbodies
                eta = int((remaining / rate) / 60) if rate > 0 else 0
                print(f"{COLOR_CYAN}[PROGRESS]{COLOR_RESET} {total_waterbodies}/{estimated_total} | ETA: {eta}m")
            
            entry = entries[0]
            original_unprocessed = entry.get('unprocessed_name', waterbody_key)
            management_units = entry.get('management_units', [])
            zone = extract_zone_from_mu(management_units)
            
            names_to_check = [original_unprocessed]
            match_note = None
            is_manual = False
            
            lower_orig = original_unprocessed.lower().strip()
            
            if lower_orig in NAME_CORRECTIONS:
                correction = NAME_CORRECTIONS[lower_orig]
                is_manual = True
                
                if isinstance(correction, dict):
                    val = correction.get("name")
                    match_note = correction.get("note")
                else:
                    val = correction
                    match_note = "Manual correction"
                
                if isinstance(val, list):
                    names_to_check = val
                    print(f"  {COLOR_CYAN}[SPLIT]{COLOR_RESET} '{original_unprocessed}' -> {val}")
                else:
                    names_to_check = [val]
                    print(f"  {COLOR_CYAN}[CORRECT]{COLOR_RESET} '{original_unprocessed}' -> '{val}'")

            all_found_matches = []
            
            for name_variant in names_to_check:
                normalized_name = normalize_name(name_variant)
                current_matches = find_waterbody_matches(normalized_name, zone, waterbody_index)
                method = "exact"
                
                if not current_matches:
                    quoted = extract_quoted_text(name_variant)
                    if quoted and quoted != normalized_name:
                        current_matches = find_waterbody_matches(quoted, zone, waterbody_index)
                        if current_matches: method = "quoted"
                
                if not current_matches:
                    variations = generate_variations(normalized_name)
                    for v in variations:
                        current_matches = find_waterbody_matches(v, zone, waterbody_index)
                        if current_matches: 
                            method = "heuristic"
                            if not is_manual: 
                                print(f"  {COLOR_CYAN}[INFO]{COLOR_RESET} Heuristic: '{normalized_name}' -> '{v}'")
                            break
                
                if current_matches:
                    for m in current_matches:
                        m['_match_method'] = method
                        m['_matched_on_name'] = name_variant
                    all_found_matches.extend(current_matches)
                else:
                    if len(names_to_check) > 1:
                         print(f"  {COLOR_RED}[PARTIAL FAIL]{COLOR_RESET} Split part '{name_variant}' not found")

            # --- REPORTING LOGIC ---
            similar = []
            if not all_found_matches:
                sim_base = normalize_name(names_to_check[0])
                similar = find_similar_names(sim_base, zone, waterbody_index, top_n=3)

            if all_found_matches:
                total_matched += 1
                unique_names = list(set(m['gnis_name'] for m in all_found_matches))
                # FIX: Use get_unique_id helper to avoid KeyError
                unique_ids = list(set(get_unique_id(m) for m in all_found_matches))
                
                if len(unique_names) > 1:
                    match_type = "SPLIT/MULTIPLE" if len(names_to_check) > 1 else "AMBIGUOUS"
                    color = COLOR_GREEN if len(names_to_check) > 1 else COLOR_YELLOW
                    print(f"  {color}[OK] {match_type}:{COLOR_RESET} '{original_unprocessed}' -> {unique_names}")
                elif len(unique_ids) > 1:
                    print(f"  {COLOR_YELLOW}[WARN] DUPLICATE NAMES ({len(unique_ids)}):{COLOR_RESET} '{original_unprocessed}' -> {unique_names[0]}")
                else:
                    print(f"  {COLOR_GREEN}[OK]{COLOR_RESET} '{original_unprocessed}' -> {unique_names[0]}")
                
                matched_list.append({
                    'region': region_name,
                    'waterbody_key': waterbody_key,
                    'original_name': original_unprocessed,
                    'zone': zone,
                    'management_units': management_units,
                    'matches': all_found_matches,
                    'note': match_note,
                    'is_manual_correction': is_manual,
                    'is_split': len(names_to_check) > 1
                })
            else:
                total_unmatched += 1
                sim_str = ", ".join([f"{n} ({r:.2f})" for r, n, _ in similar])
                print(f"  {COLOR_RED}[NO MATCH]{COLOR_RESET} '{original_unprocessed}' | Sim: {sim_str}")
                
                unmatched_list.append({
                    'region': region_name,
                    'waterbody_key': waterbody_key,
                    'original_name': original_unprocessed,
                    'zone': zone,
                    'management_units': management_units,
                    'similar_names': similar,
                    'note': match_note
                })

    print("\n" + "="*80 + f"\nSUMMARY: {total_matched}/{total_waterbodies} matched ({total_matched/total_waterbodies*100:.1f}%) | Time: {time.time()-start_time:.2f}s\n" + "="*80)
    
    matched_path = script_dir / "output" / "matched_waterbodies.json"
    with open(matched_path, 'w', encoding='utf-8') as f:
        json.dump({'matched': matched_list}, f, indent=2, ensure_ascii=False)
        
    unmatched_path = script_dir / "output" / "unmatched_waterbodies.csv"
    with open(unmatched_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        header = ['Region', 'Waterbody Key', 'Original Name', 'Zone', 'Management Units', 'Note', 'Similar 1', 'Score 1', 'Type 1', 'Similar 2', 'Score 2', 'Type 2']
        writer.writerow(header)
        for item in unmatched_list:
            mus = ", ".join(item.get('management_units', [])) if item.get('management_units') else ""
            row = [item['region'], item['waterbody_key'], item['original_name'], item['zone'], mus, item.get('note', '')]
            sims = item.get('similar_names', [])
            for i in range(2):
                if i < len(sims):
                    row.extend([sims[i][1], f"{sims[i][0]:.2f}", sims[i][2]])
                else:
                    row.extend(['', '', ''])
            writer.writerow(row)
            
    print(f"{COLOR_GREEN}Done! Files saved to output/ folder.{COLOR_RESET}\n")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
    main()