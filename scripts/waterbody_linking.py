#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fast Waterbody Linking (Deep Scan Version)

Features:
- Uses your EXACT Name Corrections table.
- Scans `waterbody_key` AND `unprocessed_name` from every regulation block.
- Normalizes smart quotes (matched “ ” to " ") to ensure dictionary hits.
- Returns "Sibling" structure: [GIS Matches] + [Regulation Blocks].

Input:
- scripts/output/fishing_data.json
- scripts/output/waterbody_index.json

Output:
- scripts/output/matched_waterbodies.json
- scripts/output/unmatched_waterbodies.csv
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

# ANSI color codes
C_GREEN = '\033[92m'
C_RED = '\033[91m'
C_CYAN = '\033[96m'
C_RESET = '\033[0m'


NAME_CORRECTIONS = {
    # 1. SPLIT EXAMPLE
    "chilliwack / vedder rivers (does not include sumas river)": {
        "name": ["chilliwack river", "vedder river"],
        "note": "Split combined entry into distinct rivers"
    },
    "lillooet lake, lillooet river": {
        "name": ["lillooet lake", "lillooet river"],
        "note": "Split combined entry into distinct waterbodies."
    },
    "caribou lakes": {
        "name": ["north caribou lake", "south caribou lake"],
        "note": "Split into North Caribou Lake and South Caribou Lake"
    },
    
    # 2. SPELLING / ALIAS CORRECTIONS
    "toquart lake": {"name": "toquaht lake", "note": "Spelling mismatch"},
    "toquart river": {"name": "toquaht river", "note": "Spelling mismatch"},
    # "tee pee lakes": {"name": "tepee lakes", "note": "Spelling mismatch"},
    # "trepanier river": {"name": "trepanier creek", "note": "gazetteer lists as creek"},
    "tuc-el-nuit lake": {"name": "tugulnuit lake", "note": "spelling mismatch"},
    "maggie lake": {"name": "makii lake", "note": "renamed in gazette (https://apps.gov.bc.ca/pub/bcgnws/names/62541.html)"},
    "mahatta river": {"name": "mahatta creek", "note": "gazetteer lists as creek"},
    '"big qualicum" river': {"name": "qualicum river", "note": "i think this is just qualicum river"},
    '"maxwell lake" (lake maxwell)': {"name": "lake maxwell", "note": "it is 'lake maxwell'"},
    'arrow park (mosquito) creek': {"name": "mosquito creek", "note": "gazetteer lists as 'mosquito creek'"},
    "lake revelstoke": {"name": "revelstoke lake", "note": "Name order correction to match gazetteer"},
    "lake revelstoke's tributaries": {"name": "revelstoke lake tributaries", "note": "Name order correction to match gazetteer"},
    
    
    # 3. IGNORABLE ENTRIES
    "arrow lakes": {
        "name": "arrow lakes",
        "note": "IGNORE: Regulation refers to Upper/Lower Arrow Lake details"
    },
    "arrow lakes' tributaries": {
        "name": "arrow lakes tributaries",
        "note": "IGNORE: Likely covered by Upper/Lower tributaries"
    },
    '"link" river': {
        "name": "link river",
        "note": "IGNORE: Listed as 'Marble (Link) River' in gazzetteer"
    },
}
# --- PIPELINE FUNCTIONS ---
def sanitize_name(name):
    """
    STEP 1: SANITIZE
    Converts raw dirty text into a 'Clean Key' for dictionary lookup.
    """
    if not name: return ""
    # Smart quotes -> Standard quotes
    clean = name.replace('\u201c', '"').replace('\u201d', '"')
    clean = clean.replace('\u2018', "'").replace('\u2019', "'").replace('`', "'")
    # Collapse whitespace
    clean = re.sub(r'\s+', ' ', clean)
    return clean.strip().lower()

def prepare_for_gis(name):
    """
    STEP 4 helper: PREPARE
    Strips noise (quotes, parens) solely for the GIS string match.
    """
    if not name: return ""
    # Remove quotes
    clean = name.replace('"', '').replace("'", '')
    # Remove parens content
    clean = re.sub(r'\([^)]*\)', '', clean)
    return clean.strip()

def search_gis_index(clean_name, zones, index):
    """
    STEP 4: SEARCH
    Searches the GIS index using 3 strategies.
    """
    gis_name = prepare_for_gis(clean_name)
    matches = []
    
    # Strategy A: Exact
    for zone in zones:
        if zone in index and gis_name in index[zone]:
            found = index[zone][gis_name]
            for f in found: f['_source_zone'] = zone
            matches.extend(found)
    
    if matches: return matches, "exact"

    # Strategy B: Quoted Text (e.g., 'river "x"')
    quoted_match = re.search(r'"([^"]+)"', clean_name)
    if quoted_match:
        quoted_text = quoted_match.group(1).strip()
        if quoted_text != gis_name:
            for zone in zones:
                if zone in index and quoted_text in index[zone]:
                    found = index[zone][quoted_text]
                    for f in found: f['_source_zone'] = zone
                    matches.extend(found)
            if matches: return matches, "quoted"

    # Strategy C: Heuristics
    variations = []
    if gis_name.endswith(" lakes"): variations.append(gis_name[:-1])
    elif gis_name.endswith(" lake"): variations.append(gis_name + "s")
    if " lakes tributaries" in gis_name: variations.append(gis_name.replace(" lakes tributaries", " lake tributary"))
    elif gis_name.endswith(" tributaries"): variations.append(gis_name.replace(" tributaries", " tributary"))

    for v in variations:
        for zone in zones:
            if zone in index and v in index[zone]:
                found = index[zone][v]
                for f in found: f['_source_zone'] = zone
                matches.extend(found)
        if matches: return matches, "heuristic"

    return [], None

# --- MAIN LOGIC ---

def process_waterbody(key, regs, index, all_stats):
    """Runs the pipeline for a single waterbody group."""
    
    # 0. Context
    zones = set()
    unprocessed_names = {key} # Start with key
    for r in regs:
        # Collect Names
        if r.get('unprocessed_name'): unprocessed_names.add(r['unprocessed_name'])
        # Collect Zones
        for mu in r.get('management_units', []):
            m = re.match(r'(\d+)-', mu)
            if m: zones.add(m.group(1))
    
    target_zones = list(zones)
    
    # PIPELINE EXECUTION
    final_gis_matches = []
    pipeline_log = []
    
    # Iterate over every potential name found in the raw data
    for raw_name in unprocessed_names:
        
        # 1. SANITIZE
        clean_key = sanitize_name(raw_name)
        
        # 2. CORRECT (Lookup)
        targets = [clean_key] # Default: search for yourself
        is_correction = False
        
        if clean_key in NAME_CORRECTIONS:
            entry = NAME_CORRECTIONS[clean_key]
            
            # --- FIX: Handle Dictionary Structure Correctly ---
            val = entry.get('name')
            if isinstance(val, list):
                targets = val
            else:
                targets = [val]
            # --------------------------------------------------

            pipeline_log.append(f"Correction: '{clean_key}' -> {targets}")
            is_correction = True
            
        # 3. SEARCH (GIS)
        for target in targets:
            matches, method = search_gis_index(target, target_zones, index)
            
            if matches:
                for m in matches:
                    m['_match_method'] = method
                    m['_matched_on'] = target
                final_gis_matches.extend(matches)
            elif is_correction:
                pipeline_log.append(f"{C_RED}FAILED TARGET:{C_RESET} '{target}'")

    # Result grouping
    unique_gis = {m['feature_id']: m for m in final_gis_matches}.values() # Deduplicate by ID
    final_gis_matches = list(unique_gis)
    
    if final_gis_matches:
        all_stats['matched'] += 1
        print(f"  {C_GREEN}[MATCH]{C_RESET} '{key}' -> {len(final_gis_matches)} features")
        return {
            "match_status": "matched",
            "normalized_name": sanitize_name(key),
            "gis_matches": final_gis_matches,
            "regulations": regs,
            "pipeline_log": pipeline_log
        }
    else:
        print(f"  {C_RED}[NO MATCH]{C_RESET} '{key}'")
        # Find suggestions for debugging
        sim = []
        clean_main = prepare_for_gis(sanitize_name(key))
        for z in target_zones:
            if z in index:
                for k in index[z]:
                    if SequenceMatcher(None, clean_main, k).ratio() > 0.6:
                        sim.append(f"{k} ({z})")
        
        return {
            "match_status": "unmatched",
            "normalized_name": sanitize_name(key),
            "gis_matches": [],
            "regulations": regs,
            "pipeline_log": pipeline_log,
            "debug_sim": sim[:3],
            "debug_zones": target_zones
        }

def main():
    script_dir = Path(__file__).parent
    json_path = script_dir / "output" / "fishing_data.json"
    index_path = script_dir / "output" / "waterbody_index.json"
    
    if not json_path.exists() or not index_path.exists():
        print("Missing input files.")
        return

    print("Loading data...")
    # Explicit utf-8 to prevent charmap errors on Windows
    with open(json_path, 'r', encoding='utf-8') as f: fishing_data = json.load(f)
    with open(index_path, 'r', encoding='utf-8') as f: index = json.load(f)

    print(f"\n{'-'*60}\nSTARTING MATCHING PIPELINE\n{'-'*60}")
    
    results = {}
    unmatched_rows = []
    stats = {'matched': 0, 'total': 0}
    
    for region, waterbodies in fishing_data.get('regionsData', {}).items():
        print(f"\nREGION: {region}")
        for key, regs in waterbodies.items():
            if not regs: continue
            stats['total'] += 1
            
            res = process_waterbody(key, regs, index, stats)
            res['region'] = region
            results[key] = res
            
            if res['match_status'] == "unmatched":
                unmatched_rows.append([
                    region, key, 
                    res['normalized_name'], 
                    res['debug_zones'], 
                    ", ".join(res['debug_sim'])
                ])

    # Save
    with open(script_dir / "output" / "matched_waterbodies.json", 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
        
    with open(script_dir / "output" / "unmatched_waterbodies.csv", 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(["Region", "Key", "Sanitized Key", "Zones", "Suggestions"])
        w.writerows(unmatched_rows)

    print(f"\nDONE. Matched: {stats['matched']}/{stats['total']}")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
    main()