#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fast Waterbody Linking Using Pre-built Index

This script matches waterbody names from the fishing synopsis JSON
to actual geographic features using a pre-built index for O(1) lookups.

MUST RUN build_waterbody_index.py FIRST to create the index file.

Input:
- scripts/output/fishing_data.json (from get_synopsis_data.py)
- scripts/output/waterbody_index.json (from build_waterbody_index.py)

Output:
- Console output showing matches and mismatches
- scripts/output/linked_waterbodies.json
- scripts/output/matching_report.json

Author: BC Freshwater Fishing Regulations Project
Date: January 3, 2026
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
COLOR_YELLOW = '\033[93m'  # Warning/Multiple matches
COLOR_RED = '\033[91m'  # Error/No match
COLOR_CYAN = '\033[96m'  # Info/Similar names
COLOR_RESET = '\033[0m'  # Reset to default


def normalize_name(name):
    """
    Normalize a waterbody name for comparison.
    
    Removes quotes, parentheses (and their contents), extra whitespace.
    Converts to lowercase for case-insensitive matching.
    """
    if not name:
        return ""
    
    # Remove all types of quotes (regular and smart/curly quotes)
    clean = name.replace('"', '').replace("'", '')
    clean = clean.replace('\u201c', '').replace('\u201d', '')
    clean = clean.replace('\u2018', '').replace('\u2019', '')
    clean = clean.replace('`', '')
    
    # Remove parentheses and their contents
    clean = re.sub(r'\([^)]*\)', '', clean)
    
    # Remove extra whitespace
    clean = re.sub(r'\s+', ' ', clean).strip()
    
    # Lowercase for comparison
    clean = clean.lower()
    
    return clean


def extract_quoted_text(name):
    """
    Extract text within quotes from a name.
    
    For names like '"BIG QUALICUM" RIVER', extracts 'BIG QUALICUM'
    """
    if not name:
        return None
    
    # Try to find text within smart/curly quotes
    match = re.search(r'\u201c([^\u201d]+)\u201d', name)
    if match:
        return normalize_name(match.group(1))
    
    # Try regular quotes
    match = re.search(r'"([^"]+)"', name)
    if match:
        return normalize_name(match.group(1))
    
    return None


def extract_zone_from_mu(management_units):
    """
    Extract zone number from management unit string.
    """
    if not management_units or len(management_units) == 0:
        return None
    
    # Get first MU and extract zone number (before the dash)
    mu = management_units[0]
    match = re.match(r'(\d+)-', mu)
    if match:
        return match.group(1)
    
    return None


def find_similar_names(waterbody_name, zone, index, top_n=5):
    """
    Find similar waterbody names using fuzzy matching from the index.
    
    Args:
        waterbody_name: Name to search for (already normalized)
        zone: Zone number as string
        index: Pre-built waterbody index
        top_n: Number of top similar names to return
        
    Returns:
        List of tuples (similarity_ratio, name, type) sorted by similarity
    """
    if not zone or zone not in index:
        return []
    
    similar_names = []
    zone_data = index[zone]
    
    # Compare against all names in this zone
    for indexed_name, features in zone_data.items():
        ratio = SequenceMatcher(None, waterbody_name, indexed_name).ratio()
        if ratio > 0.6:  # Only include if somewhat similar
            # Get type from first feature
            feature_type = features[0]['type'] if features else 'unknown'
            original_name = features[0]['gnis_name'] if features else indexed_name
            similar_names.append((ratio, original_name, feature_type))
    
    # Sort by similarity and return top N
    similar_names.sort(reverse=True, key=lambda x: x[0])
    return similar_names[:top_n]


def find_waterbody_matches(waterbody_name, zone, index):
    """
    Search for matching waterbody in the pre-built index (O(1) lookup).
    
    Args:
        waterbody_name: Name to search for (already normalized)
        zone: Zone number as string
        index: Pre-built waterbody index
        
    Returns:
        List of matching features (empty if no matches)
    """
    if not zone or zone not in index:
        return []
    
    zone_data = index[zone]
    
    # O(1) lookup!
    if waterbody_name in zone_data:
        return zone_data[waterbody_name]
    
    return []


def main():
    """
    Main function to link waterbodies using pre-built index.
    """
    # Set up paths
    script_dir = Path(__file__).parent
    json_path = script_dir / "output" / "fishing_data.json"
    index_path = script_dir / "output" / "waterbody_index.json"
    output_json = script_dir / "output" / "linked_waterbodies.json"
    gdb_path = script_dir / "output" / "FWA_named_waterbodies.gdb"
    
    # Validate inputs exist
    if not json_path.exists():
        print(f"ERROR: Fishing data JSON not found: {json_path}")
        return
    
    if not gdb_path.exists():
        print(f"ERROR: FWA geodatabase not found: {gdb_path}")
        return
    
    # Check if index exists, if not build it
    if not index_path.exists():
        print(f"{COLOR_YELLOW}Waterbody index not found. Building index first...{COLOR_RESET}\n")
        
        try:
            from build_waterbody_index import main as build_index
            build_index()
            print(f"\n{COLOR_GREEN}Index built successfully!{COLOR_RESET}\n")
        except Exception as e:
            print(f"{COLOR_RED}ERROR: Failed to build index: {e}{COLOR_RESET}")
            return
    
    print(f"Loading fishing data from: {json_path}")
    with open(json_path, 'r', encoding='utf-8') as f:
        fishing_data = json.load(f)
    
    print(f"Loading waterbody index from: {index_path}")
    with open(index_path, 'r', encoding='utf-8') as f:
        waterbody_index = json.load(f)
    
    print("\n" + "="*80)
    print("FAST WATERBODY LINKING REPORT")
    print("="*80 + "\n")
    
    # Start time tracking
    start_time = time.time()
    last_update_time = start_time
    
    # Statistics
    total_waterbodies = 0
    total_matched = 0
    total_unmatched = 0
    total_multiple_matches = 0
    
    # Results structure
    linked_data = {}
    unmatched_list = []
    matched_list = []
    
    # Process each region
    regions_data = fishing_data.get('regionsData', {})
    
    # Estimate total for progress
    estimated_total = sum(len(wb) for wb in regions_data.values())
    
    for region_name, waterbodies in regions_data.items():
        print(f"\n{'-'*80}")
        print(f"REGION: {region_name}")
        print(f"{'-'*80}")
        
        region_results = {}
        
        for waterbody_key, entries in waterbodies.items():
            if not entries or len(entries) == 0:
                continue
            
            total_waterbodies += 1
            
            # Progress estimation every 100 waterbodies
            current_time = time.time()
            if total_waterbodies % 100 == 0 and total_waterbodies > 0:
                elapsed = current_time - start_time
                rate = total_waterbodies / elapsed
                remaining = estimated_total - total_waterbodies
                eta_seconds = remaining / rate if rate > 0 else 0
                eta_mins = int(eta_seconds / 60)
                eta_secs = int(eta_seconds % 60)
                elapsed_mins = int(elapsed / 60)
                elapsed_secs = int(elapsed % 60)
                print(f"\n{COLOR_CYAN}[PROGRESS]{COLOR_RESET} Processed: {total_waterbodies}/{estimated_total} | "
                      f"Elapsed: {elapsed_mins}m {elapsed_secs}s | "
                      f"ETA: {eta_mins}m {eta_secs}s | Rate: {rate:.1f}/sec\n")
            
            # Get first entry to extract zone and name
            entry = entries[0]
            unprocessed_name = entry.get('unprocessed_name', waterbody_key)
            management_units = entry.get('management_units', [])
            
            # Extract zone
            zone = extract_zone_from_mu(management_units)
            
            # Normalize name
            normalized_name = normalize_name(unprocessed_name)
            
            # Fast O(1) lookup in index
            matches = find_waterbody_matches(normalized_name, zone, waterbody_index)
            
            # If no match and name has quotes, try matching just the quoted part
            if len(matches) == 0:
                quoted_text = extract_quoted_text(unprocessed_name)
                if quoted_text and quoted_text != normalized_name:
                    matches = find_waterbody_matches(quoted_text, zone, waterbody_index)
                    if len(matches) > 0:
                        normalized_name = quoted_text
            
            # Report results
            if len(matches) == 0:
                total_unmatched += 1
                
                # Find similar names for debugging
                similar = find_similar_names(normalized_name, zone, waterbody_index, top_n=3)
                
                if similar:
                    similar_str = ", ".join([f"{name} ({type_}, {ratio:.2f})" for ratio, name, type_ in similar])
                    print(f"  {COLOR_RED}[NO MATCH]{COLOR_RESET} '{unprocessed_name}' (normalized: '{normalized_name}') | Zone: {zone}")
                    print(f"     {COLOR_CYAN}Similar:{COLOR_RESET} {similar_str}")
                else:
                    print(f"  {COLOR_RED}[NO MATCH]{COLOR_RESET} '{unprocessed_name}' (normalized: '{normalized_name}') | Zone: {zone}")
                
                unmatched_list.append({
                    'region': region_name,
                    'waterbody_key': waterbody_key,
                    'unprocessed_name': unprocessed_name,
                    'normalized_name': normalized_name,
                    'zone': zone,
                    'management_units': management_units,
                    'similar_names': similar
                })
                region_results[waterbody_key] = {
                    'unprocessed_name': unprocessed_name,
                    'normalized_name': normalized_name,
                    'zone': zone,
                    'matches': [],
                    'similar_names': similar
                }
            elif len(matches) == 1:
                total_matched += 1
                match = matches[0]
                print(f"  {COLOR_GREEN}[OK] MATCHED:{COLOR_RESET} '{unprocessed_name}' -> {match['type'].upper()}: '{match['gnis_name']}' | Zone: {zone}")
                
                # Add to matched list
                matched_list.append({
                    'region': region_name,
                    'waterbody_key': waterbody_key,
                    'unprocessed_name': unprocessed_name,
                    'normalized_name': normalized_name,
                    'zone': zone,
                    'management_units': management_units,
                    'match': match
                })
                
                region_results[waterbody_key] = {
                    'unprocessed_name': unprocessed_name,
                    'normalized_name': normalized_name,
                    'zone': zone,
                    'matches': matches
                }
            else:
                total_matched += 1
                total_multiple_matches += 1
                print(f"  {COLOR_YELLOW}[WARN] MULTIPLE MATCHES ({len(matches)}):{COLOR_RESET} '{unprocessed_name}' | Zone: {zone}")
                
                # Add all matches
                matched_list.append({
                    'region': region_name,
                    'waterbody_key': waterbody_key,
                    'unprocessed_name': unprocessed_name,
                    'normalized_name': normalized_name,
                    'zone': zone,
                    'management_units': management_units,
                    'matches': matches,
                    'multiple_matches': True
                })
                
                region_results[waterbody_key] = {
                    'unprocessed_name': unprocessed_name,
                    'normalized_name': normalized_name,
                    'zone': zone,
                    'matches': matches
                }
        
        linked_data[region_name] = region_results
    
    # Calculate total time
    total_time = time.time() - start_time
    total_mins = int(total_time / 60)
    total_secs = int(total_time % 60)
    rate = total_waterbodies / total_time if total_time > 0 else 0
    
    # Print summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total waterbodies processed: {total_waterbodies}")
    print(f"Matched: {total_matched} ({total_matched/total_waterbodies*100:.1f}%)")
    print(f"  - Single matches: {total_matched - total_multiple_matches}")
    print(f"  - Multiple matches: {total_multiple_matches}")
    print(f"Unmatched: {total_unmatched} ({total_unmatched/total_waterbodies*100:.1f}%)")
    print(f"\n{COLOR_CYAN}Processing time:{COLOR_RESET} {total_mins}m {total_secs}s ({rate:.1f} waterbodies/sec)")
    print("="*80 + "\n")
    
    # Save matched waterbodies
    matched_path = script_dir / "output" / "matched_waterbodies.json"
    matched_data = {
        'summary': {
            'total_matched': total_matched,
            'single_matches': total_matched - total_multiple_matches,
            'multiple_matches': total_multiple_matches,
            'match_rate': f"{total_matched/total_waterbodies*100:.1f}%" if total_waterbodies > 0 else "0%",
            'processing_time': f"{total_mins}m {total_secs}s"
        },
        'matched_waterbodies': matched_list
    }
    
    print(f"Saving matched waterbodies to: {matched_path}")
    with open(matched_path, 'w', encoding='utf-8') as f:
        json.dump(matched_data, f, indent=2, ensure_ascii=False)
    
    # Save unmatched waterbodies as CSV
    unmatched_path = script_dir / "output" / "unmatched_waterbodies.csv"
    
    print(f"Saving unmatched waterbodies to: {unmatched_path}")
    with open(unmatched_path, 'w', encoding='utf-8', newline='') as f:
        if unmatched_list:
            fieldnames = ['region', 'waterbody_key', 'unprocessed_name', 'normalized_name', 
                         'zone', 'management_units', 'similar_1', 'similarity_1', 'type_1',
                         'similar_2', 'similarity_2', 'type_2', 'similar_3', 'similarity_3', 'type_3']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for item in unmatched_list:
                row = {
                    'region': item['region'],
                    'waterbody_key': item['waterbody_key'],
                    'unprocessed_name': item['unprocessed_name'],
                    'normalized_name': item['normalized_name'],
                    'zone': item['zone'],
                    'management_units': ', '.join(item['management_units']) if item['management_units'] else ''
                }
                
                # Add similar names (up to 3)
                similar_names = item.get('similar_names', [])
                for i in range(3):
                    if i < len(similar_names):
                        ratio, name, type_ = similar_names[i]
                        row[f'similar_{i+1}'] = name
                        row[f'similarity_{i+1}'] = f"{ratio:.2f}"
                        row[f'type_{i+1}'] = type_
                    else:
                        row[f'similar_{i+1}'] = ''
                        row[f'similarity_{i+1}'] = ''
                        row[f'type_{i+1}'] = ''
                
                writer.writerow(row)
    
    print(f"{COLOR_GREEN}Done!{COLOR_RESET}\n")


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print(f"\n\n{COLOR_YELLOW}[INTERRUPTED]{COLOR_RESET} Script stopped by user (Ctrl+C)")
    print("Exiting...")
    os._exit(0)


if __name__ == "__main__":
    # Register signal handler for Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] Script stopped by user (Ctrl+C)")
        print("Exiting...")
        sys.exit(0)
