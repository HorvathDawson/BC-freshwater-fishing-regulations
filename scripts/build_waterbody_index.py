#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build Waterbody Index from FWA Geodatabase

This script preprocesses the FWA geodatabase by creating an indexed lookup
structure grouped by GNIS_NAME and zone. This dramatically speeds up matching
by avoiding repeated geodatabase reads.

Input:
- scripts/output/FWA_named_waterbodies.gdb

Output:
- scripts/output/waterbody_index.json (indexed lookup by zone and normalized name)

Author: BC Freshwater Fishing Regulations Project
Date: January 3, 2026
"""

import json
import re
import time
from pathlib import Path
from collections import defaultdict
import geopandas as gpd
import fiona

# ANSI color codes
COLOR_GREEN = '\033[92m'
COLOR_YELLOW = '\033[93m'
COLOR_CYAN = '\033[96m'
COLOR_RESET = '\033[0m'


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


def main():
    """Build indexed lookup structure from FWA geodatabase."""
    
    # Set up paths
    script_dir = Path(__file__).parent
    gdb_path = script_dir / "output" / "FWA_Zone_Grouped.gdb"
    output_path = script_dir / "output" / "waterbody_index.json"
    
    if not gdb_path.exists():
        print(f"ERROR: FWA geodatabase not found: {gdb_path}")
        return
    
    print(f"Building waterbody index from: {gdb_path}")
    print("="*80)
    
    start_time = time.time()
    
    # Structure: index[zone][normalized_name] = [list of features]
    index = defaultdict(lambda: defaultdict(list))
    
    # Get all layers in the geodatabase
    layers = fiona.listlayers(str(gdb_path))
    stream_layers = [l for l in layers if l.startswith('STREAMS_ZONE_')]
    lake_layers = [l for l in layers if l.startswith('LAKES_ZONE_')]
    
    total_features = 0
    total_unique_names = 0
    
    # Process stream layers
    print(f"\n{COLOR_CYAN}Processing stream layers...{COLOR_RESET}")
    for layer_name in stream_layers:
        # Extract zone number from layer name
        zone_match = re.search(r'ZONE_(\d+)', layer_name)
        if not zone_match:
            continue
        zone = zone_match.group(1)
        
        try:
            streams = gpd.read_file(str(gdb_path), layer=layer_name)
            layer_count = 0
            
            for idx, row in streams.iterrows():
                gnis_name = row.get('GNIS_NAME')
                if not gnis_name or str(gnis_name) == 'nan':
                    continue
                
                normalized = normalize_name(gnis_name)
                if not normalized:
                    continue
                
                # Convert row to dict, excluding geometry
                feature_dict = row.drop('geometry').to_dict() if 'geometry' in row else row.to_dict()
                # Convert non-serializable types
                feature_dict = {k: (str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v) 
                               for k, v in feature_dict.items()}
                
                feature_data = {
                    'type': 'stream',
                    'gnis_name': gnis_name,
                    'layer': layer_name,
                    'feature_id': str(idx),
                    'attributes': feature_dict
                }
                
                index[zone][normalized].append(feature_data)
                layer_count += 1
                total_features += 1
            
            print(f"  {COLOR_GREEN}✓{COLOR_RESET} {layer_name}: {layer_count} features")
            
        except Exception as e:
            print(f"  {COLOR_YELLOW}⚠{COLOR_RESET} {layer_name}: Error - {e}")
    
    # Process lake layers
    print(f"\n{COLOR_CYAN}Processing lake layers...{COLOR_RESET}")
    for layer_name in lake_layers:
        # Extract zone number from layer name
        zone_match = re.search(r'ZONE_(\d+)', layer_name)
        if not zone_match:
            continue
        zone = zone_match.group(1)
        
        try:
            lakes = gpd.read_file(str(gdb_path), layer=layer_name)
            layer_count = 0
            
            for idx, row in lakes.iterrows():
                # Check both GNIS_NAME_1 and GNIS_NAME_2
                for name_field in ['GNIS_NAME_1', 'GNIS_NAME_2']:
                    if name_field not in lakes.columns:
                        continue
                    
                    gnis_name = row.get(name_field)
                    if not gnis_name or str(gnis_name) == 'nan':
                        continue
                    
                    normalized = normalize_name(gnis_name)
                    if not normalized:
                        continue
                    
                    # Convert row to dict, excluding geometry
                    feature_dict = row.drop('geometry').to_dict() if 'geometry' in row else row.to_dict()
                    feature_dict = {k: (str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v) 
                                   for k, v in feature_dict.items()}
                    
                    feature_data = {
                        'type': 'lake',
                        'gnis_name': gnis_name,
                        'layer': layer_name,
                        'feature_id': str(idx),
                        'matched_field': name_field,
                        'attributes': feature_dict
                    }
                    
                    # Check for duplicates (same feature, different name field)
                    existing = index[zone][normalized]
                    if not any(f['feature_id'] == str(idx) and f['layer'] == layer_name for f in existing):
                        index[zone][normalized].append(feature_data)
                        layer_count += 1
                        total_features += 1
            
            print(f"  {COLOR_GREEN}✓{COLOR_RESET} {layer_name}: {layer_count} features")
            
        except Exception as e:
            print(f"  {COLOR_YELLOW}⚠{COLOR_RESET} {layer_name}: Error - {e}")
    
    # Count unique names
    for zone, names in index.items():
        total_unique_names += len(names)
    
    # Convert defaultdict to regular dict for JSON serialization
    output_index = {}
    for zone, names in index.items():
        output_index[zone] = dict(names)
    
    # Save index
    elapsed = time.time() - start_time
    mins = int(elapsed / 60)
    secs = int(elapsed % 60)
    
    print("\n" + "="*80)
    print("INDEX STATISTICS")
    print("="*80)
    print(f"Total features indexed: {total_features}")
    print(f"Unique names (normalized): {total_unique_names}")
    print(f"Zones covered: {len(output_index)}")
    print(f"Processing time: {mins}m {secs}s")
    print("="*80 + "\n")
    
    print(f"Saving index to: {output_path}")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_index, f, indent=2, ensure_ascii=False)
    
    print(f"{COLOR_GREEN}Done!{COLOR_RESET} Index ready for fast lookups.\n")
    
    # Show sample of index structure
    print("Sample index structure:")
    for zone in list(output_index.keys())[:2]:
        print(f"\n  Zone {zone}: {len(output_index[zone])} unique names")
        sample_names = list(output_index[zone].keys())[:3]
        for name in sample_names:
            count = len(output_index[zone][name])
            print(f"    '{name}': {count} feature(s)")


if __name__ == "__main__":
    main()
