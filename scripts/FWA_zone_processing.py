#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BC FWA Processing: Named Streams, River Tributaries, Lake Tributaries, Wetlands, Manmade Waterbodies, Zones, and Labelled Points.

Logic Flow:
1. Load all Streams, Lakes, Wetlands, Manmade Waterbodies, and KML Points.
2. SAFETY LOCK: Identify exactly which streams are unnamed at the start.
3. RIVER ENRICHMENT: Rename locked unnamed streams based on parent codes.
4. LAKE ENRICHMENT: 
   - Check streams from step 3.
   - If they touch a lake, rename them based on the lake name.
5. POINT ENRICHMENT:
   - Check KML points against Lakes, Wetlands, and Manmade polygons.
   - Assign POLY_ID for each type if the point falls inside.
   - ERROR LOGGING: If a point falls inside NOTHING, log it to a CSV.
6. Output: Split EVERYTHING by Zone (Streams, Lakes, Wetlands, Manmade, Points).
7. OPTIONAL: Build searchable waterbody index from processed data.
"""

import os
import sys
import argparse

# --- FIX FOR "Cannot find header.dxf" WARNING ---
os.environ["GDAL_SKIP"] = "DXF" 

if 'GDAL_DATA' not in os.environ:
    candidates = [
        os.path.join(sys.prefix, 'share', 'gdal'),
        os.path.join(sys.prefix, 'Library', 'share', 'gdal'),
    ]
    for c in candidates:
        if os.path.exists(c):
            os.environ['GDAL_DATA'] = c
            break

import fiona
import geopandas as gpd
import pandas as pd
from pathlib import Path
import logging
import time
import shutil
import gc
import warnings
import numpy as np
from concurrent.futures import ProcessPoolExecutor
import json
import re
from collections import defaultdict

# Enable KML Driver
fiona.drvsupport.supported_drivers['KML'] = 'rw'
fiona.drvsupport.supported_drivers['LIBKML'] = 'rw'

warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- WORKER FUNCTIONS ---
def spatial_join_worker(args):
    left_chunk, right_gdf = args
    return gpd.sjoin(left_chunk, right_gdf, predicate='intersects', how='inner')

def normalize_name(name):
    """Normalize a waterbody name for comparison."""
    if not name or str(name) == 'nan':
        return ""
    
    # Remove all types of quotes
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

class FWAProcessor:
    def __init__(self, streams_gdb: str, lakes_gdb: str, wildlife_gpkg: str, kml_path: str, output_gdb: str,
                 process_streams=True, process_lakes=True, process_wetlands=True, 
                 process_manmade=True, process_points=True):
        self.streams_gdb = Path(streams_gdb)
        self.lakes_gdb = Path(lakes_gdb)
        self.wildlife_gpkg = Path(wildlife_gpkg)
        self.kml_path = Path(kml_path)
        self.output_gdb = Path(output_gdb)
        self.n_cores = max(1, os.cpu_count() - 1)
        
        # Feature flags
        self.process_streams = process_streams
        self.process_lakes = process_lakes
        self.process_wetlands = process_wetlands
        self.process_manmade = process_manmade
        self.process_points = process_points
        
        self.stats = {
            'total_streams_read': 0,
            'original_named_streams': 0,
            'river_tributaries_found': 0,
            'lake_tributaries_corrected': 0,
            'final_streams_count': 0,
            'total_lakes': 0,
            'total_wetlands': 0,
            'total_manmade': 0,
            'total_kml_points': 0
        }

    def cleanup_output(self):
        self.output_gdb.parent.mkdir(parents=True, exist_ok=True)
        gc.collect()
        
        # Remove old error logs if they exist
        error_log = self.output_gdb.parent / "unmatched_points_error_log.csv"
        if error_log.exists():
            try:
                os.remove(error_log)
            except PermissionError:
                pass

        if self.output_gdb.exists():
            for i in range(3):
                try:
                    shutil.rmtree(self.output_gdb)
                    break
                except PermissionError:
                    time.sleep(2)

    def get_stream_layers(self) -> list:
        try:
            layers = fiona.listlayers(str(self.streams_gdb))
            return [l for l in layers if not l.startswith('_') and len(l) <= 4]
        except Exception:
            return []

    # --- CODE PARSING ---
    def clean_watershed_code(self, code):
        if not isinstance(code, str): return None
        parts = code.split('-')
        valid_parts = [p for p in parts if p != '000000']
        return "-".join(valid_parts)

    def get_parent_code(self, clean_code):
        if not clean_code or '-' not in clean_code: return None
        return clean_code.rsplit('-', 1)[0]
    
    def get_code_depth(self, code):
        if not isinstance(code, str): return 0
        return len([x for x in code.split('-') if x != '000000'])

    # --- LOADERS ---
    def load_streams_raw(self, test_mode=False):
        logger.info("=== STEP 1: Loading Streams ===")
        layers = self.get_stream_layers()
        if test_mode:
            layers = layers[:5]
            logger.info("TEST MODE: Loading 5 layers.")

        gdf_list = []
        for i, layer in enumerate(layers):
            try:
                gdf = gpd.read_file(str(self.streams_gdb), layer=layer)
                cols = ['FWA_WATERSHED_CODE', 'GNIS_NAME', 'geometry', 'LINEAR_FEATURE_ID']
                existing = [c for c in cols if c in gdf.columns]
                gdf_list.append(gdf[existing])
            except Exception as e:
                logger.warning(f"Skipping {layer}: {e}")
            if (i+1) % 50 == 0: logger.info(f"Loaded {i+1} layers...")

        full_gdf = pd.concat(gdf_list, ignore_index=True)
        full_gdf = gpd.GeoDataFrame(full_gdf, geometry='geometry', crs=gdf_list[0].crs)
        self.stats['total_streams_read'] = len(full_gdf)
        return full_gdf

    def load_lakes(self):
        logger.info("=== STEP 2a: Loading Lakes ===")
        if not self.process_lakes:
            logger.info("Skipping lakes (disabled)")
            return gpd.GeoDataFrame()
        try:
            lakes = gpd.read_file(str(self.lakes_gdb), layer='FWA_LAKES_POLY')
            self.stats['total_lakes'] = len(lakes)
            return lakes
        except Exception as e:
            logger.error(f"Failed to load Lakes: {e}")
            return gpd.GeoDataFrame()

    def load_wetlands(self):
        logger.info("=== STEP 2b: Loading Wetlands ===")
        if not self.process_wetlands:
            logger.info("Skipping wetlands (disabled)")
            return gpd.GeoDataFrame()
        try:
            wetlands = gpd.read_file(str(self.lakes_gdb), layer='FWA_WETLANDS_POLY')
            self.stats['total_wetlands'] = len(wetlands)
            return wetlands
        except Exception as e:
            logger.warning(f"Failed to load Wetlands (Layer might be missing): {e}")
            return gpd.GeoDataFrame()

    def load_manmade(self):
        logger.info("=== STEP 2c: Loading Manmade Waterbodies ===")
        if not self.process_manmade:
            logger.info("Skipping manmade waterbodies (disabled)")
            return gpd.GeoDataFrame()
        try:
            manmade = gpd.read_file(str(self.lakes_gdb), layer='FWA_MANMADE_WATERBODIES_POLY')
            self.stats['total_manmade'] = len(manmade)
            return manmade
        except Exception as e:
            logger.warning(f"Failed to load Manmade Waterbodies (Layer might be missing): {e}")
            return gpd.GeoDataFrame()

    def load_kml_points(self):
        logger.info("=== STEP 2d: Loading KML Points ===")
        if not self.process_points:
            logger.info("Skipping KML points (disabled)")
            return gpd.GeoDataFrame()
        if not self.kml_path.exists():
            logger.warning(f"KML file not found: {self.kml_path}")
            return gpd.GeoDataFrame()
        
        try:
            points = gpd.read_file(str(self.kml_path))
            if points.crs is None:
                points.set_crs(epsg=4326, inplace=True)
            
            self.stats['total_kml_points'] = len(points)
            logger.info(f"Loaded {len(points)} KML points.")
            return points
        except Exception as e:
            logger.error(f"Failed to load KML: {e}")
            return gpd.GeoDataFrame()

    # --- ENRICHMENT LOGIC ---
    def enrich_kml_points(self, points_gdf, lakes_gdf, wetlands_gdf, manmade_gdf):
        """
        Intersects points with waterbody polygons to assign WATERBODY_POLY_ID.
        Ensures IDs are stored as Integers (Int64).
        Logs any points that do not intersect with ANY polygon.
        """
        logger.info("=== STEP 3a: Enriching KML Points with Waterbody IDs ===")
        
        if points_gdf.empty:
            return points_gdf

        target_crs = lakes_gdf.crs
        if points_gdf.crs != target_crs:
            points_gdf = points_gdf.to_crs(target_crs)
        
        # Initialize as object to allow None temporarily
        points_gdf['LAKE_POLY_ID'] = None
        points_gdf['WETLAND_POLY_ID'] = None
        points_gdf['MANMADE_POLY_ID'] = None
        
        def attach_id(points, polys, id_col_name, poly_type_name):
            if polys.empty: 
                return points
            
            if 'WATERBODY_POLY_ID' not in polys.columns:
                logger.warning(f"{poly_type_name} missing WATERBODY_POLY_ID column.")
                return points

            joined = gpd.sjoin(points, polys[['geometry', 'WATERBODY_POLY_ID']], how='left', predicate='intersects')
            
            joined.index.name = 'idx_temp'
            joined = joined.reset_index()
            
            id_map = joined.groupby('idx_temp')['WATERBODY_POLY_ID'].first()
            points.loc[id_map.index, id_col_name] = id_map
            return points

        if not lakes_gdf.empty:
            logger.info("Checking points in Lakes...")
            points_gdf = attach_id(points_gdf, lakes_gdf, 'LAKE_POLY_ID', 'Lakes')

        if not wetlands_gdf.empty:
            logger.info("Checking points in Wetlands...")
            points_gdf = attach_id(points_gdf, wetlands_gdf, 'WETLAND_POLY_ID', 'Wetlands')

        if not manmade_gdf.empty:
            logger.info("Checking points in Manmade Waterbodies...")
            points_gdf = attach_id(points_gdf, manmade_gdf, 'MANMADE_POLY_ID', 'Manmade')

        # --- FORCE INTEGER TYPES ---
        # Using 'Int64' (capital I) allows for integers with NaNs (nullable integers)
        for col in ['LAKE_POLY_ID', 'WETLAND_POLY_ID', 'MANMADE_POLY_ID']:
            points_gdf[col] = points_gdf[col].astype('Int64')

        # --- ERROR LOGGING FOR UNMATCHED POINTS ---
        unmatched = points_gdf[
            points_gdf['LAKE_POLY_ID'].isna() & 
            points_gdf['WETLAND_POLY_ID'].isna() & 
            points_gdf['MANMADE_POLY_ID'].isna()
        ]

        if not unmatched.empty:
            error_log_path = self.output_gdb.parent / "unmatched_points_error_log.csv"
            logger.warning(f"!! ALERT !! {len(unmatched)} KML points did not fall inside any waterbody polygon.")
            logger.warning(f"Saving list of unmatched points to: {error_log_path}")
            
            # Try to grab relevant columns for the log
            log_cols = ['geometry']
            for col in ['Name', 'name', 'Description', 'description', 'label']:
                if col in unmatched.columns:
                    log_cols.insert(0, col)
            
            try:
                unmatched[log_cols].to_csv(error_log_path, index=True)
            except Exception as e:
                logger.error(f"Could not write error log: {e}")

        return points_gdf

    def enrich_streams(self, streams_gdf, lakes_gdf):
        logger.info("=== STEP 3b: Enriching Stream Names ===")
        
        logger.info("Calculating hierarchy depths...")
        streams_gdf['clean_code'] = streams_gdf['FWA_WATERSHED_CODE'].apply(self.clean_watershed_code)
        streams_gdf['parent_code'] = streams_gdf['clean_code'].apply(self.get_parent_code)
        streams_gdf['depth'] = streams_gdf['FWA_WATERSHED_CODE'].apply(self.get_code_depth)
        
        originally_unnamed_mask = (streams_gdf['GNIS_NAME'].isna()) | (streams_gdf['GNIS_NAME'].str.strip() == '')
        
        named_mask = ~originally_unnamed_mask
        self.stats['original_named_streams'] = named_mask.sum()
        logger.info(f"Protected {self.stats['original_named_streams']:,} originally named streams.")
        
        logger.info("Assigning River Tributary names...")
        
        name_map = pd.Series(
            streams_gdf.loc[named_mask, 'GNIS_NAME'].values, 
            index=streams_gdf.loc[named_mask, 'clean_code']
        ).to_dict()

        parents = streams_gdf.loc[originally_unnamed_mask, 'parent_code'].map(name_map)
        matched_indices = parents[parents.notna()].index
        
        streams_gdf.loc[matched_indices, 'GNIS_NAME'] = parents[matched_indices] + " Tributary"
        self.stats['river_tributaries_found'] = len(matched_indices)
        logger.info(f" -> Initial river matches: {len(matched_indices):,}")

        if not lakes_gdf.empty:
            logger.info("Verifying Lake Tributaries...")
            
            candidate_mask = (originally_unnamed_mask) & (streams_gdf['GNIS_NAME'].str.endswith(' Tributary', na=False))
            candidate_streams = streams_gdf[candidate_mask].copy()
            completed_codes = set()

            named_lakes = lakes_gdf[
                (lakes_gdf['GNIS_NAME_1'].notna()) & 
                (lakes_gdf['GNIS_NAME_1'].str.strip() != '')
            ].copy()
            
            named_lakes['depth'] = named_lakes['FWA_WATERSHED_CODE'].apply(self.get_code_depth)
            named_lakes = named_lakes.sort_values('depth', ascending=False)
            unique_depths = sorted(named_lakes['depth'].unique(), reverse=True)
            
            total_corrected = 0
            
            if candidate_streams.crs != named_lakes.crs:
                named_lakes = named_lakes.to_crs(candidate_streams.crs)

            for lake_depth in unique_depths:
                lakes_at_depth = named_lakes[named_lakes['depth'] == lake_depth][['geometry', 'GNIS_NAME_1', 'depth']]
                
                current_candidates = candidate_streams[
                    (~candidate_streams['clean_code'].isin(completed_codes)) & 
                    (candidate_streams['depth'] > lake_depth)
                ]
                
                if current_candidates.empty:
                    continue

                join_result = self.parallel_spatial_join(
                    current_candidates[['geometry', 'FWA_WATERSHED_CODE']], 
                    lakes_at_depth
                )
                
                if not join_result.empty:
                    code_to_lake = join_result.groupby('FWA_WATERSHED_CODE')['GNIS_NAME_1'].first().to_dict()
                    
                    mask_codes = streams_gdf['FWA_WATERSHED_CODE'].isin(code_to_lake.keys())
                    mask_safe_update = mask_codes & originally_unnamed_mask
                    
                    lake_names = streams_gdf.loc[mask_safe_update, 'FWA_WATERSHED_CODE'].map(code_to_lake)
                    streams_gdf.loc[mask_safe_update, 'GNIS_NAME'] = lake_names + " Tributary"
                    
                    completed_codes.update(code_to_lake.keys())
                    total_corrected += len(code_to_lake)

            self.stats['lake_tributaries_corrected'] = total_corrected
            logger.info(f" -> Corrected {total_corrected:,} tributary systems.")
        
        final_streams = streams_gdf[
            (streams_gdf['GNIS_NAME'].notna()) & 
            (streams_gdf['GNIS_NAME'].str.strip() != '')
        ].copy()
        
        final_streams = final_streams.drop(columns=['clean_code', 'parent_code', 'depth'])
        self.stats['final_streams_count'] = len(final_streams)
        return final_streams

    def parallel_spatial_join(self, target_gdf, zone_gdf):
        if len(target_gdf) == 0: return gpd.GeoDataFrame()
        chunks = np.array_split(target_gdf, self.n_cores)
        args = [(chunk, zone_gdf) for chunk in chunks]
        results = []
        with ProcessPoolExecutor(max_workers=self.n_cores) as executor:
            for res in executor.map(spatial_join_worker, args):
                results.append(res)
        if results:
            res_df = pd.concat(results, ignore_index=True)
            return gpd.GeoDataFrame(res_df, geometry='geometry', crs=target_gdf.crs)
        return gpd.GeoDataFrame()

    def split_and_save(self, streams_gdf, lakes_gdf, wetlands_gdf, manmade_gdf, points_gdf):
        logger.info("=== STEP 4: Spatial Processing by Zone ===")
        self.cleanup_output()
        
        wildlife = gpd.read_file(str(self.wildlife_gpkg))
        zone_field = next(col for col in wildlife.columns if 'ZONE' in col.upper() or 'UNIT' in col.upper())
        wildlife['ZONE_GROUP'] = wildlife[zone_field].astype(str).str.split('-').str[0]
        target_crs = wildlife.crs
        
        wildlife.to_file(str(self.output_gdb), layer="WILDLIFE_MGMT_UNITS", driver="OpenFileGDB")
        
        zone_outlines = wildlife.dissolve(by='ZONE_GROUP')
        unique_zones = sorted(wildlife['ZONE_GROUP'].unique())
        
        logger.info(f"Processing {len(unique_zones)} zones...")

        joined_streams = gpd.GeoDataFrame()
        if not streams_gdf.empty:
            logger.info("Joining Streams to Zones...")
            if streams_gdf.crs != target_crs: streams_gdf = streams_gdf.to_crs(target_crs)
            joined_streams = self.parallel_spatial_join(streams_gdf, wildlife[['geometry', 'ZONE_GROUP']])
        
        joined_lakes = gpd.GeoDataFrame()
        if not lakes_gdf.empty:
            logger.info("Joining Lakes to Zones...")
            if lakes_gdf.crs != target_crs: lakes_gdf = lakes_gdf.to_crs(target_crs)
            joined_lakes = self.parallel_spatial_join(lakes_gdf, wildlife[['geometry', 'ZONE_GROUP']])

        joined_wetlands = gpd.GeoDataFrame()
        if not wetlands_gdf.empty:
            logger.info("Joining Wetlands to Zones...")
            if wetlands_gdf.crs != target_crs: wetlands_gdf = wetlands_gdf.to_crs(target_crs)
            joined_wetlands = self.parallel_spatial_join(wetlands_gdf, wildlife[['geometry', 'ZONE_GROUP']])

        joined_manmade = gpd.GeoDataFrame()
        if not manmade_gdf.empty:
            logger.info("Joining Manmade Waterbodies to Zones...")
            if manmade_gdf.crs != target_crs: manmade_gdf = manmade_gdf.to_crs(target_crs)
            joined_manmade = self.parallel_spatial_join(manmade_gdf, wildlife[['geometry', 'ZONE_GROUP']])
            
        joined_points = gpd.GeoDataFrame()
        if not points_gdf.empty:
            logger.info("Joining KML Points to Zones...")
            if points_gdf.crs != target_crs: points_gdf = points_gdf.to_crs(target_crs)
            joined_points = self.parallel_spatial_join(points_gdf, wildlife[['geometry', 'ZONE_GROUP']])

        for zone in unique_zones:
            logger.info(f"Saving Zone {zone}...")
            
            if zone in zone_outlines.index:
                outline = zone_outlines.loc[[zone]]
                outline.to_file(str(self.output_gdb), layer=f"ZONE_OUTLINE_{zone}", driver="OpenFileGDB")
                time.sleep(0.1)

            if not joined_streams.empty:
                z_streams = joined_streams[joined_streams['ZONE_GROUP'] == zone]
                if not z_streams.empty:
                    z_streams = z_streams.drop_duplicates(subset=['LINEAR_FEATURE_ID'])
                    keep_cols = [c for c in streams_gdf.columns if c in z_streams.columns]
                    if 'geometry' not in keep_cols: keep_cols.append('geometry')
                    z_streams = gpd.GeoDataFrame(z_streams[keep_cols], geometry='geometry', crs=target_crs)
                    z_streams.to_file(str(self.output_gdb), layer=f"STREAMS_ZONE_{zone}", driver="OpenFileGDB")

            if not joined_lakes.empty:
                z_lakes = joined_lakes[joined_lakes['ZONE_GROUP'] == zone]
                if not z_lakes.empty:
                    dedup = 'WATERBODY_KEY' if 'WATERBODY_KEY' in z_lakes.columns else None
                    if dedup: z_lakes = z_lakes.drop_duplicates(subset=[dedup])
                    else: z_lakes = z_lakes.drop_duplicates()
                    
                    keep_cols = [c for c in lakes_gdf.columns if c in z_lakes.columns]
                    if 'geometry' not in keep_cols: keep_cols.append('geometry')
                    z_lakes = gpd.GeoDataFrame(z_lakes[keep_cols], geometry='geometry', crs=target_crs)
                    z_lakes.to_file(str(self.output_gdb), layer=f"LAKES_ZONE_{zone}", driver="OpenFileGDB")

            if not joined_wetlands.empty:
                z_wet = joined_wetlands[joined_wetlands['ZONE_GROUP'] == zone]
                if not z_wet.empty:
                    dedup = 'WATERBODY_KEY' if 'WATERBODY_KEY' in z_wet.columns else None
                    if dedup: z_wet = z_wet.drop_duplicates(subset=[dedup])
                    else: z_wet = z_wet.drop_duplicates()
                    
                    keep_cols = [c for c in wetlands_gdf.columns if c in z_wet.columns]
                    if 'geometry' not in keep_cols: keep_cols.append('geometry')
                    z_wet = gpd.GeoDataFrame(z_wet[keep_cols], geometry='geometry', crs=target_crs)
                    z_wet.to_file(str(self.output_gdb), layer=f"WETLANDS_ZONE_{zone}", driver="OpenFileGDB")

            if not joined_manmade.empty:
                z_man = joined_manmade[joined_manmade['ZONE_GROUP'] == zone]
                if not z_man.empty:
                    dedup = 'WATERBODY_KEY' if 'WATERBODY_KEY' in z_man.columns else None
                    if dedup: z_man = z_man.drop_duplicates(subset=[dedup])
                    else: z_man = z_man.drop_duplicates()
                    
                    keep_cols = [c for c in manmade_gdf.columns if c in z_man.columns]
                    if 'geometry' not in keep_cols: keep_cols.append('geometry')
                    z_man = gpd.GeoDataFrame(z_man[keep_cols], geometry='geometry', crs=target_crs)
                    z_man.to_file(str(self.output_gdb), layer=f"MANMADE_ZONE_{zone}", driver="OpenFileGDB")
            
            if not joined_points.empty:
                z_pts = joined_points[joined_points['ZONE_GROUP'] == zone]
                if not z_pts.empty:
                    z_pts = z_pts.drop_duplicates(subset=['geometry']) 
                    keep_cols = [c for c in points_gdf.columns if c in z_pts.columns]
                    if 'geometry' not in keep_cols: keep_cols.append('geometry')
                    z_pts = gpd.GeoDataFrame(z_pts[keep_cols], geometry='geometry', crs=target_crs)
                    z_pts.to_file(str(self.output_gdb), layer=f"LABELED_POINTS_ZONE_{zone}", driver="OpenFileGDB")

            time.sleep(0.1)

    def build_waterbody_index(self):
        """Build indexed lookup structure from processed geodatabase."""
        logger.info("=== STEP 5: Building Waterbody Index ===")
        
        output_path = self.output_gdb.parent / "waterbody_index.json"
        
        if not self.output_gdb.exists():
            logger.error(f"GDB not found: {self.output_gdb}")
            return
        
        # Structure: index[zone][normalized_name] = [list of features]
        index = defaultdict(lambda: defaultdict(list))
        
        # Cache for Polygons
        poly_cache = defaultdict(lambda: defaultdict(dict))
        
        try:
            layers = fiona.listlayers(str(self.output_gdb))
        except Exception as e:
            logger.error(f"Error reading GDB layers: {e}")
            return

        # Categorize layers
        stream_layers = [l for l in layers if l.startswith('STREAMS_ZONE_')] if self.process_streams else []
        point_layers = [l for l in layers if l.startswith('LABELED_POINTS_ZONE_')] if self.process_points else []
        
        polygon_groups = []
        if self.process_lakes:
            polygon_groups.append(('lake', [l for l in layers if l.startswith('LAKES_ZONE_')]))
        if self.process_wetlands:
            polygon_groups.append(('wetland', [l for l in layers if l.startswith('WETLANDS_ZONE_')]))
        if self.process_manmade:
            polygon_groups.append(('manmade', [l for l in layers if l.startswith('MANMADE_ZONE_')]))
        
        total_features = 0
        
        # Process Polygons First (for cache)
        for type_label, layer_list in polygon_groups:
            if not layer_list:
                continue
                
            logger.info(f"Indexing {type_label} layers...")
            
            for layer_name in layer_list:
                zone_match = re.search(r'ZONE_(\d+)', layer_name)
                if not zone_match:
                    continue
                zone = zone_match.group(1)
                
                try:
                    gdf = gpd.read_file(str(self.output_gdb), layer=layer_name)
                    layer_count = 0
                    
                    for idx, row in gdf.iterrows():
                        feature_dict = row.drop('geometry').to_dict() if 'geometry' in row else row.to_dict()
                        feature_dict = {k: (str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v) 
                                       for k, v in feature_dict.items()}
                        
                        # Cache polygon by ID
                        poly_id = row.get('WATERBODY_POLY_ID')
                        if poly_id and str(poly_id) != 'nan':
                            cached_feature = {
                                'type': type_label,
                                'gnis_name': row.get('GNIS_NAME_1') or row.get('GNIS_NAME'),
                                'layer': layer_name,
                                'feature_id': str(idx),
                                'attributes': feature_dict,
                                'is_primary_polygon': True
                            }
                            poly_cache[zone][type_label][int(poly_id)] = cached_feature

                        # Index by GNIS Name
                        check_fields = ['GNIS_NAME_1', 'GNIS_NAME_2', 'GNIS_NAME']
                        for name_field in check_fields:
                            if name_field not in gdf.columns:
                                continue
                            
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
                                
                                existing = index[zone][normalized]
                                if not any(f['feature_id'] == str(idx) and f['layer'] == layer_name for f in existing):
                                    index[zone][normalized].append(feature_data)
                                    layer_count += 1
                                    total_features += 1
                    
                    logger.info(f"  ✓ {layer_name}: {layer_count} features")
                    
                except Exception as e:
                    logger.warning(f"  ⚠ {layer_name}: Error - {e}")

        # Process Streams
        if stream_layers:
            logger.info("Indexing stream layers...")
            for layer_name in stream_layers:
                zone_match = re.search(r'ZONE_(\d+)', layer_name)
                if not zone_match:
                    continue
                zone = zone_match.group(1)
                
                try:
                    streams = gpd.read_file(str(self.output_gdb), layer=layer_name)
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
                    
                    logger.info(f"  ✓ {layer_name}: {layer_count} features")
                    
                except Exception as e:
                    logger.warning(f"  ⚠ {layer_name}: Error - {e}")

        # Process Labelled Points
        if point_layers:
            logger.info("Indexing labelled point layers...")
            
            for layer_name in point_layers:
                zone_match = re.search(r'ZONE_(\d+)', layer_name)
                if not zone_match:
                    continue
                zone = zone_match.group(1)
                
                try:
                    points = gpd.read_file(str(self.output_gdb), layer=layer_name)
                    layer_count = 0
                    
                    for idx, row in points.iterrows():
                        # Determine point name
                        name_candidates = ['Name', 'name', 'label', 'GNIS_NAME']
                        point_name = None
                        for col in name_candidates:
                            if col in row and pd.notna(row[col]):
                                point_name = row[col]
                                break
                        
                        normalized = normalize_name(point_name)
                        if not normalized:
                            continue

                        # Add point feature
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
                        
                        # Link matched polygons
                        link_map = [
                            ('LAKE_POLY_ID', 'lake'),
                            ('WETLAND_POLY_ID', 'wetland'),
                            ('MANMADE_POLY_ID', 'manmade')
                        ]
                        
                        for col_id, type_key in link_map:
                            poly_id_val = row.get(col_id)
                            
                            if pd.notna(poly_id_val):
                                try:
                                    pid = int(poly_id_val)
                                    if pid in poly_cache[zone][type_key]:
                                        linked_poly = poly_cache[zone][type_key][pid].copy()
                                        linked_poly['linked_via_point'] = True
                                        linked_poly['point_name_used'] = point_name
                                        index[zone][normalized].append(linked_poly)
                                except (ValueError, TypeError):
                                    pass

                    logger.info(f"  ✓ {layer_name}: {layer_count} points")
                    
                except Exception as e:
                    logger.warning(f"  ⚠ {layer_name}: Error - {e}")

        # Convert to regular dict and save
        output_index = {}
        for zone, names in index.items():
            output_index[zone] = dict(names)
        
        total_unique_names = sum(len(names) for names in index.values())
        
        logger.info(f"Index Statistics:")
        logger.info(f"  Total features indexed: {total_features}")
        logger.info(f"  Unique names: {total_unique_names}")
        logger.info(f"  Zones covered: {len(output_index)}")
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_index, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Index saved to: {output_path}")

    def run(self, test_mode=False, build_index=False):
        start = time.time()
        
        raw_streams = self.load_streams_raw(test_mode) if self.process_streams else gpd.GeoDataFrame()
        lakes = self.load_lakes()
        wetlands = self.load_wetlands()
        manmade = self.load_manmade()
        points = self.load_kml_points()

        enriched_points = self.enrich_kml_points(points, lakes, wetlands, manmade) if self.process_points else gpd.GeoDataFrame()
        enriched_streams = self.enrich_streams(raw_streams, lakes) if self.process_streams else gpd.GeoDataFrame()

        self.split_and_save(enriched_streams, lakes, wetlands, manmade, enriched_points)
        
        if build_index:
            self.build_waterbody_index()
        
        end = time.time()
        logger.info("="*50)
        logger.info("PROCESSING COMPLETE")
        logger.info(f"Total Time: {(end-start)/60:.2f} mins")
        if self.process_streams:
            logger.info(f"Original Named Streams: {self.stats['original_named_streams']:,}")
            logger.info(f"River Tributaries Found: {self.stats['river_tributaries_found']:,}")
            logger.info(f"Lake Tributaries Corrected: {self.stats['lake_tributaries_corrected']:,}")
            logger.info(f"Final Streams: {self.stats['final_streams_count']:,}")
        if self.process_lakes:
            logger.info(f"Lakes: {self.stats['total_lakes']:,}")
        if self.process_wetlands:
            logger.info(f"Wetlands: {self.stats['total_wetlands']:,}")
        if self.process_manmade:
            logger.info(f"Manmade: {self.stats['total_manmade']:,}")
        if self.process_points:
            logger.info(f"KML Points: {self.stats['total_kml_points']:,}")
        logger.info(f"Output: {self.output_gdb}")
        logger.info("="*50)

def main():
    parser = argparse.ArgumentParser(
        description='Process BC FWA data and split by wildlife management zones',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full processing with index building
  python FWA_zone_processing.py --build-index
  
  # Build index only from existing GDB
  python FWA_zone_processing.py --build-index-only
  
  # Test mode (5 layers only)
  python FWA_zone_processing.py --test-mode
  
  # Skip streams processing
  python FWA_zone_processing.py --skip-streams
  
  # Process only lakes and build index
  python FWA_zone_processing.py --skip-streams --skip-wetlands --skip-manmade --skip-points --build-index
  
  # Rebuild index for only streams and lakes
  python FWA_zone_processing.py --build-index-only --skip-wetlands --skip-manmade --skip-points
        """
    )
    
    parser.add_argument('--test-mode', action='store_true',
                       help='Test mode: process only 5 stream layers')
    parser.add_argument('--build-index', action='store_true',
                       help='Build waterbody index after processing')
    parser.add_argument('--build-index-only', action='store_true',
                       help='Only build the index from existing GDB (skip all processing)')
    parser.add_argument('--skip-streams', action='store_true',
                       help='Skip stream processing')
    parser.add_argument('--skip-lakes', action='store_true',
                       help='Skip lake processing')
    parser.add_argument('--skip-wetlands', action='store_true',
                       help='Skip wetlands processing')
    parser.add_argument('--skip-manmade', action='store_true',
                       help='Skip manmade waterbodies processing')
    parser.add_argument('--skip-points', action='store_true',
                       help='Skip KML points processing')
    
    args = parser.parse_args()
    
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    base_data = project_root / "data" / "ftp.geobc.gov.bc.ca" / "sections" / "outgoing" / "bmgs" / "FWA_Public"
    
    streams_gdb = base_data / "FWA_STREAM_NETWORKS_SP" / "FWA_STREAM_NETWORKS_SP.gdb"
    lakes_gdb = base_data / "FWA_BC" / "FWA_BC.gdb"
    wildlife_gpkg = base_data / "WAA_WILDLIFE_MGMT_UNITS_SVW.gpkg"
    kml_path = project_root / "data" / "labelled" / "unnamed_lakes.kml"
    
    output_gdb = script_dir / "output" / "FWA_zone_processing" / "FWA_Zone_Grouped.gdb"
    
    # Handle build-index-only mode
    if args.build_index_only:
        if not output_gdb.exists():
            print(f"Error: Output GDB not found at {output_gdb}")
            print("Please run the full processing first before building index.")
            return
        
        logger.info("Building index from existing GDB...")
        processor = FWAProcessor(
            str(streams_gdb), 
            str(lakes_gdb), 
            str(wildlife_gpkg), 
            str(kml_path), 
            str(output_gdb),
            process_streams=not args.skip_streams,
            process_lakes=not args.skip_lakes,
            process_wetlands=not args.skip_wetlands,
            process_manmade=not args.skip_manmade,
            process_points=not args.skip_points
        )
        processor.build_waterbody_index()
        return
    
    if not streams_gdb.exists() and not args.skip_streams:
        print(f"Error: Streams GDB not found at {streams_gdb}")
        return

    processor = FWAProcessor(
        str(streams_gdb), 
        str(lakes_gdb), 
        str(wildlife_gpkg), 
        str(kml_path), 
        str(output_gdb),
        process_streams=not args.skip_streams,
        process_lakes=not args.skip_lakes,
        process_wetlands=not args.skip_wetlands,
        process_manmade=not args.skip_manmade,
        process_points=not args.skip_points
    )
    processor.run(test_mode=args.test_mode, build_index=args.build_index)

if __name__ == "__main__":
    main()