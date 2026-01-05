#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build Waterbody Index from FWA Geodatabase (Streams, Polygons, and Labelled Points)

This script preprocesses the FWA geodatabase by creating an indexed lookup
structure grouped by GNIS_NAME (or KML Name) and zone. 

Features:
1. Indexes Streams, Lakes, Wetlands, and Manmade waterbodies.
2. Caches Polygon IDs during processing.
3. Indexes Labelled Points (KML).
4. **Enrichment**: If a KML point is linked to a polygon (via ID), 
   that Polygon feature is ALSO added to the index under the KML point's name.

Input:
- scripts/output/FWA_Zone_Grouped.gdb

Output:
- scripts/output/waterbody_index.json

Author: BC Freshwater Fishing Regulations Project
Date: January 4, 2026
"""

import json
import re
import time
from pathlib import Path
from collections import defaultdict
import geopandas as gpd
import fiona
import warnings
import pandas as pd

# Suppress specific warnings if needed
warnings.filterwarnings('ignore', category=UserWarning)

# ANSI color codes
COLOR_GREEN = '\033[92m'
COLOR_YELLOW = '\033[93m'
COLOR_CYAN = '\033[96m'
COLOR_RESET = '\033[0m'


def normalize_name(name):
    """
    Normalize a waterbody name for comparison.
    """
    if not name or str(name) == 'nan':
        return ""
    
    # Remove all types of quotes (regular and smart/curly quotes)
    clean = str(name).replace('"', '').replace("'", '')
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
    
    # Cache for Polygons: poly_cache[zone][type_label][poly_id] = feature_data
    # This allows us to attach the full polygon feature to a point's name later.
    poly_cache = defaultdict(lambda: defaultdict(dict))
    
    # Get all layers in the geodatabase
    try:
        layers = fiona.listlayers(str(gdb_path))
    except Exception as e:
        print(f"Error reading GDB layers: {e}")
        return

    # Categorize layers
    stream_layers = [l for l in layers if l.startswith('STREAMS_ZONE_')]
    point_layers = [l for l in layers if l.startswith('LABELED_POINTS_ZONE_')]
    
    polygon_groups = [
        ('lake', [l for l in layers if l.startswith('LAKES_ZONE_')]),
        ('wetland', [l for l in layers if l.startswith('WETLANDS_ZONE_')]),
        ('manmade', [l for l in layers if l.startswith('MANMADE_ZONE_')])
    ]
    
    total_features = 0
    total_unique_names = 0
    
    # ---------------------------------------------------------
    # 1. PROCESS POLYGONS (Lakes, Wetlands, Manmade)
    #    *Must be done first to populate poly_cache for points*
    # ---------------------------------------------------------
    
    for type_label, layer_list in polygon_groups:
        if not layer_list:
            continue
            
        print(f"\n{COLOR_CYAN}Processing {type_label} layers...{COLOR_RESET}")
        
        for layer_name in layer_list:
            zone_match = re.search(r'ZONE_(\d+)', layer_name)
            if not zone_match: continue
            zone = zone_match.group(1)
            
            try:
                gdf = gpd.read_file(str(gdb_path), layer=layer_name)
                layer_count = 0
                
                for idx, row in gdf.iterrows():
                    # 1. Prepare Feature Data
                    feature_dict = row.drop('geometry').to_dict() if 'geometry' in row else row.to_dict()
                    # Clean for JSON
                    feature_dict = {k: (str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v) 
                                   for k, v in feature_dict.items()}
                    
                    # 2. Extract POLY ID for Cache
                    poly_id = row.get('WATERBODY_POLY_ID')
                    if poly_id and str(poly_id) != 'nan':
                        # Store base feature data in cache (keyed by ID)
                        # We create a generic feature object here for re-use
                        cached_feature = {
                            'type': type_label,
                            'gnis_name': row.get('GNIS_NAME_1') or row.get('GNIS_NAME'),
                            'layer': layer_name,
                            'feature_id': str(idx),
                            'attributes': feature_dict,
                            'is_primary_polygon': True # Flag to distinguish from point matches later
                        }
                        # Cast poly_id to int for consistent lookup keys
                        poly_cache[zone][type_label][int(poly_id)] = cached_feature

                    # 3. Index by GNIS Name (if exists)
                    check_fields = ['GNIS_NAME_1', 'GNIS_NAME_2', 'GNIS_NAME']
                    for name_field in check_fields:
                        if name_field not in gdf.columns: continue
                        
                        gnis_name = row.get(name_field)
                        normalized = normalize_name(gnis_name)
                        
                        if normalized:
                            feature_data = {
                                'type': type_label,
                                'gnis_name': gnis_name,
                                'layer': layer_name,
                                'feature_id': str(idx),
                                'matched_field': name_field,
                                'attributes': feature_dict
                            }
                            
                            # Deduplicate in index
                            existing = index[zone][normalized]
                            if not any(f['feature_id'] == str(idx) and f['layer'] == layer_name for f in existing):
                                index[zone][normalized].append(feature_data)
                                layer_count += 1
                                total_features += 1
                
                print(f"  {COLOR_GREEN}✓{COLOR_RESET} {layer_name}: {layer_count} named features")
                
            except Exception as e:
                print(f"  {COLOR_YELLOW}⚠{COLOR_RESET} {layer_name}: Error - {e}")

    # ---------------------------------------------------------
    # 2. PROCESS STREAMS
    # ---------------------------------------------------------
    print(f"\n{COLOR_CYAN}Processing stream layers...{COLOR_RESET}")
    for layer_name in stream_layers:
        zone_match = re.search(r'ZONE_(\d+)', layer_name)
        if not zone_match: continue
        zone = zone_match.group(1)
        
        try:
            streams = gpd.read_file(str(gdb_path), layer=layer_name)
            layer_count = 0
            
            for idx, row in streams.iterrows():
                gnis_name = row.get('GNIS_NAME')
                normalized = normalize_name(gnis_name)
                
                if normalized:
                    feature_dict = row.drop('geometry').to_dict() if 'geometry' in row else row.to_dict()
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

    # ---------------------------------------------------------
    # 3. PROCESS LABELLED POINTS (KML)
    # ---------------------------------------------------------
    if point_layers:
        print(f"\n{COLOR_CYAN}Processing labelled point layers...{COLOR_RESET}")
        
        for layer_name in point_layers:
            zone_match = re.search(r'ZONE_(\d+)', layer_name)
            if not zone_match: continue
            zone = zone_match.group(1)
            
            try:
                points = gpd.read_file(str(gdb_path), layer=layer_name)
                layer_count = 0
                
                for idx, row in points.iterrows():
                    # 1. Determine Name
                    # KMLs usually use 'Name' or 'name'. Sometimes we promoted 'label'.
                    name_candidates = ['Name', 'name', 'label', 'GNIS_NAME']
                    point_name = None
                    for col in name_candidates:
                        if col in row and pd.notna(row[col]):
                            point_name = row[col]
                            break
                    
                    normalized = normalize_name(point_name)
                    if not normalized:
                        continue

                    # 2. Add Point Feature
                    feature_dict = row.drop('geometry').to_dict() if 'geometry' in row else row.to_dict()
                    feature_dict = {k: (str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v) 
                                   for k, v in feature_dict.items()}
                    
                    point_feature = {
                        'type': 'point',
                        'gnis_name': point_name,
                        'layer': layer_name,
                        'feature_id': str(idx),
                        'attributes': feature_dict
                    }
                    
                    index[zone][normalized].append(point_feature)
                    layer_count += 1
                    total_features += 1
                    
                    # 3. LINK MATCHED POLYGONS (Enrichment)
                    # If this point sits on a known lake/wetland, we want that polygon 
                    # to appear in the search results for this point's name.
                    
                    # Map column name -> cache key
                    link_map = [
                        ('LAKE_POLY_ID', 'lake'),
                        ('WETLAND_POLY_ID', 'wetland'),
                        ('MANMADE_POLY_ID', 'manmade')
                    ]
                    
                    for col_id, type_key in link_map:
                        poly_id_val = row.get(col_id)
                        
                        # Check if ID is valid (not None, not NaN)
                        if pd.notna(poly_id_val):
                            try:
                                pid = int(poly_id_val)
                                # Check cache
                                if pid in poly_cache[zone][type_key]:
                                    linked_poly = poly_cache[zone][type_key][pid].copy()
                                    
                                    # Mark it as a linked feature so UI knows it came via the point
                                    linked_poly['linked_via_point'] = True
                                    linked_poly['point_name_used'] = point_name
                                    
                                    # Add to index under the POINT'S normalized name
                                    index[zone][normalized].append(linked_poly)
                                    # (We don't increment total_features here to avoid double counting the polygon object itself, 
                                    # but it effectively adds a search result)
                            except (ValueError, TypeError):
                                pass

                print(f"  {COLOR_GREEN}✓{COLOR_RESET} {layer_name}: {layer_count} points processed")
                
            except Exception as e:
                print(f"  {COLOR_YELLOW}⚠{COLOR_RESET} {layer_name}: Error - {e}")

    # ---------------------------------------------------------
    # 4. SAVE OUTPUT
    # ---------------------------------------------------------
    for zone, names in index.items():
        total_unique_names += len(names)
    
    # Convert defaultdict to regular dict
    output_index = {}
    for zone, names in index.items():
        output_index[zone] = dict(names)
    
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
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_index, f, indent=2, ensure_ascii=False)
    
    print(f"{COLOR_GREEN}Done!{COLOR_RESET} Index ready for fast lookups.\n")

if __name__ == "__main__":
    main()