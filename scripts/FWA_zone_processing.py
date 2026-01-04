#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BC FWA Processing: Named Streams, River Tributaries, Lake Tributaries, and Zones

Logic Flow:
1. Load all Streams and Lakes.
2. SAFETY LOCK: Identify exactly which streams are unnamed at the start.
3. RIVER ENRICHMENT: Rename locked unnamed streams based on parent codes.
4. LAKE ENRICHMENT: 
   - Check streams from step 3 (and ONLY those streams).
   - If they touch a lake, rename them.
   - Propagate to related segments.
5. Output: Split by Zone.

Reference:
- [cite_start]Hierarchy logic [cite: 22-24].
"""

import fiona
import geopandas as gpd
import pandas as pd
from pathlib import Path
import logging
import time
import shutil
import gc
import warnings
import os
import numpy as np
from concurrent.futures import ProcessPoolExecutor

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

class FWAProcessor:
    def __init__(self, streams_gdb: str, lakes_gdb: str, wildlife_gpkg: str, output_gdb: str):
        self.streams_gdb = Path(streams_gdb)
        self.lakes_gdb = Path(lakes_gdb)
        self.wildlife_gpkg = Path(wildlife_gpkg)
        self.output_gdb = Path(output_gdb)
        self.n_cores = max(1, os.cpu_count() - 1)
        
        self.stats = {
            'total_streams_read': 0,
            'original_named_streams': 0,
            'river_tributaries_found': 0,
            'lake_tributaries_corrected': 0,
            'final_streams_count': 0,
            'total_lakes': 0
        }

    def cleanup_output(self):
        self.output_gdb.parent.mkdir(parents=True, exist_ok=True)
        gc.collect()
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
        logger.info("=== STEP 2: Loading Lakes ===")
        lakes = gpd.read_file(str(self.lakes_gdb), layer='FWA_LAKES_POLY')
        self.stats['total_lakes'] = len(lakes)
        return lakes

    # --- ENRICHMENT LOGIC ---
    def enrich_streams(self, streams_gdf, lakes_gdf):
        logger.info("=== STEP 3: Enriching Stream Names ===")
        
        # 1. PARSE CODES & DEPTH
        logger.info("Calculating hierarchy depths...")
        streams_gdf['clean_code'] = streams_gdf['FWA_WATERSHED_CODE'].apply(self.clean_watershed_code)
        streams_gdf['parent_code'] = streams_gdf['clean_code'].apply(self.get_parent_code)
        streams_gdf['depth'] = streams_gdf['FWA_WATERSHED_CODE'].apply(self.get_code_depth)
        
        # 2. IDENTIFY ORIGINALLY UNNAMED STREAMS (Safety Lock)
        # We define this mask ONCE here. We will ONLY ever update rows that are True in this mask.
        originally_unnamed_mask = (streams_gdf['GNIS_NAME'].isna()) | (streams_gdf['GNIS_NAME'].str.strip() == '')
        
        # Stats for named streams
        named_mask = ~originally_unnamed_mask
        self.stats['original_named_streams'] = named_mask.sum()
        logger.info(f"Protected {self.stats['original_named_streams']:,} originally named streams.")
        
        # 3. RIVER HIERARCHY MATCH
        logger.info("Assigning River Tributary names...")
        
        # Create map from the PROTECTED named streams only
        name_map = pd.Series(
            streams_gdf.loc[named_mask, 'GNIS_NAME'].values, 
            index=streams_gdf.loc[named_mask, 'clean_code']
        ).to_dict()

        # Only look up parents for UNNAMED streams
        parents = streams_gdf.loc[originally_unnamed_mask, 'parent_code'].map(name_map)
        matched_indices = parents[parents.notna()].index
        
        # Apply River Names
        streams_gdf.loc[matched_indices, 'GNIS_NAME'] = parents[matched_indices] + " Tributary"
        self.stats['river_tributaries_found'] = len(matched_indices)
        logger.info(f" -> Initial river matches: {len(matched_indices):,}")

        # 4. LAKE SPATIAL MATCH (Deepest First)
        logger.info("Verifying Lake Tributaries...")

        # A. Filter Candidates:
        # Strict Rule: Must be originally unnamed AND currently have a "Tributary" name we just gave it.
        # This prevents touching anything that had a name in the raw data.
        candidate_mask = (originally_unnamed_mask) & (streams_gdf['GNIS_NAME'].str.endswith(' Tributary', na=False))
        candidate_streams = streams_gdf[candidate_mask].copy()
        
        # Track completed codes to handle hierarchy correctly
        completed_codes = set()

        # B. Prepare Lakes
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
            
            # Filter candidates: 
            # 1. Not processed yet
            # 2. Stream Depth > Lake Depth (Child check)
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
                
                # Update GLOBAL dataframe
                # STRICT SAFETY: We intersect the watershed match mask with 'originally_unnamed_mask'
                # This guarantees we never accidentally rename a named stream even during propagation
                mask_codes = streams_gdf['FWA_WATERSHED_CODE'].isin(code_to_lake.keys())
                mask_safe_update = mask_codes & originally_unnamed_mask
                
                lake_names = streams_gdf.loc[mask_safe_update, 'FWA_WATERSHED_CODE'].map(code_to_lake)
                streams_gdf.loc[mask_safe_update, 'GNIS_NAME'] = lake_names + " Tributary"
                
                completed_codes.update(code_to_lake.keys())
                total_corrected += len(code_to_lake)

        self.stats['lake_tributaries_corrected'] = total_corrected
        logger.info(f" -> Corrected {total_corrected:,} tributary systems.")
        
        # 5. FINAL FILTER
        final_streams = streams_gdf[
            (streams_gdf['GNIS_NAME'].notna()) & 
            (streams_gdf['GNIS_NAME'].str.strip() != '')
        ].copy()
        
        final_streams = final_streams.drop(columns=['clean_code', 'parent_code', 'depth'])
        return final_streams

    # --- PARALLEL PROCESSING UTILS ---
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

    def split_and_save(self, streams_gdf, lakes_gdf):
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

        if streams_gdf.crs != target_crs: streams_gdf = streams_gdf.to_crs(target_crs)
        joined_streams = self.parallel_spatial_join(streams_gdf, wildlife[['geometry', 'ZONE_GROUP']])

        if lakes_gdf.crs != target_crs: lakes_gdf = lakes_gdf.to_crs(target_crs)
        joined_lakes = self.parallel_spatial_join(lakes_gdf, wildlife[['geometry', 'ZONE_GROUP']])

        for zone in unique_zones:
            logger.info(f"Saving Zone {zone}...")
            
            if zone in zone_outlines.index:
                outline = zone_outlines.loc[[zone]]
                outline.to_file(str(self.output_gdb), layer=f"ZONE_OUTLINE_{zone}", driver="OpenFileGDB")
                time.sleep(0.5)

            z_streams = joined_streams[joined_streams['ZONE_GROUP'] == zone]
            if not z_streams.empty:
                z_streams = z_streams.drop_duplicates(subset=['LINEAR_FEATURE_ID'])
                keep_cols = [c for c in streams_gdf.columns if c in z_streams.columns]
                if 'geometry' not in keep_cols: keep_cols.append('geometry')
                z_streams = z_streams[keep_cols]
                z_streams = gpd.GeoDataFrame(z_streams, geometry='geometry', crs=target_crs)
                z_streams.to_file(str(self.output_gdb), layer=f"STREAMS_ZONE_{zone}", driver="OpenFileGDB")
                time.sleep(0.5)

            z_lakes = joined_lakes[joined_lakes['ZONE_GROUP'] == zone]
            if not z_lakes.empty:
                dedup = 'WATERBODY_KEY' if 'WATERBODY_KEY' in z_lakes.columns else None
                if dedup: z_lakes = z_lakes.drop_duplicates(subset=[dedup])
                else: z_lakes = z_lakes.drop_duplicates()
                
                keep_cols_l = [c for c in lakes_gdf.columns if c in z_lakes.columns]
                if 'geometry' not in keep_cols_l: keep_cols_l.append('geometry')
                z_lakes = z_lakes[keep_cols_l]
                z_lakes = gpd.GeoDataFrame(z_lakes, geometry='geometry', crs=target_crs)
                z_lakes.to_file(str(self.output_gdb), layer=f"LAKES_ZONE_{zone}", driver="OpenFileGDB")
                time.sleep(0.5)

    def run(self, test_mode=False):
        start = time.time()
        raw_streams = self.load_streams_raw(test_mode)
        lakes = self.load_lakes()
        enriched_streams = self.enrich_streams(raw_streams, lakes)
        self.split_and_save(enriched_streams, lakes)
        
        end = time.time()
        logger.info("="*50)
        logger.info("PROCESSING COMPLETE")
        logger.info(f"Total Time: {(end-start)/60:.2f} mins")
        logger.info(f"Original Named Streams: {self.stats['original_named_streams']:,}")
        logger.info(f"River Tributaries Found: {self.stats['river_tributaries_found']:,}")
        logger.info(f"Lake Tributaries Corrected: {self.stats['lake_tributaries_corrected']:,}")
        logger.info(f"Final Streams Saved: {self.stats['final_streams_count']:,}")
        logger.info(f"Output: {self.output_gdb}")
        logger.info("="*50)

def main():
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    base_data = project_root / "data" / "ftp.geobc.gov.bc.ca" / "sections" / "outgoing" / "bmgs" / "FWA_Public"
    
    streams_gdb = base_data / "FWA_STREAM_NETWORKS_SP" / "FWA_STREAM_NETWORKS_SP.gdb"
    lakes_gdb = base_data / "FWA_BC" / "FWA_BC.gdb"
    wildlife_gpkg = base_data / "WAA_WILDLIFE_MGMT_UNITS_SVW.gpkg"
    
    output_gdb = script_dir / "output" / "FWA_Zone_Grouped.gdb"
    
    if not streams_gdb.exists():
        print(f"Error: Streams GDB not found at {streams_gdb}")
        return

    processor = FWAProcessor(str(streams_gdb), str(lakes_gdb), str(wildlife_gpkg), str(output_gdb))
    processor.run(test_mode=False)

if __name__ == "__main__":
    main()