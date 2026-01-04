#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BC FWA Processing: MULTI-CORE OPTIMIZED VERSION

Key Features:
1. Parallel Spatial Joins: Uses all CPU cores to link streams/lakes to zones.
2. Global Processing: Performs intersection once, then groups by result.
3. Robustness: Auto-retries file deletion to prevent Windows locking errors.

Date: January 3, 2026
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

# Suppress warnings
warnings.filterwarnings('ignore')

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- WORKER FUNCTION FOR MULTIPROCESSING ---
# Must be defined at module level (outside class) for Windows pickling
def spatial_join_worker(args):
    """
    Worker function to perform spatial join on a chunk of data.
    args: (left_gdf_chunk, right_gdf_zones)
    """
    left_chunk, right_gdf = args
    # Perform inner join
    return gpd.sjoin(left_chunk, right_gdf, predicate='intersects', how='inner')

class FWAProcessor:
    def __init__(self, streams_gdb: str, lakes_gdb: str, wildlife_gpkg: str, output_gdb: str):
        self.streams_gdb = Path(streams_gdb)
        self.lakes_gdb = Path(lakes_gdb)
        self.wildlife_gpkg = Path(wildlife_gpkg)
        self.output_gdb = Path(output_gdb)
        
        # Detect CPU cores (leave 1 free for system if possible, else use all)
        self.n_cores = max(1, os.cpu_count() - 1)
        self.n_cores = min(self.n_cores, 10)  # Cap at 4 cores to avoid overload
        
        self.stats = {
            'total_streams_read': 0,
            'original_named_streams': 0,
            'tributaries_identified': 0,
            'total_lakes': 0
        }

    def cleanup_output(self):
        """Robustly remove existing output GDB."""
        gc.collect()
        if self.output_gdb.exists():
            logger.info(f"Removing existing output: {self.output_gdb}")
            for i in range(3):
                try:
                    shutil.rmtree(self.output_gdb)
                    break
                except PermissionError:
                    logger.warning(f"File locked. Retrying delete ({i+1}/3)...")
                    time.sleep(2)
            
            if self.output_gdb.exists():
                raise PermissionError(f"Could not delete {self.output_gdb}. Close QGIS/ArcGIS.")
        
        self.output_gdb.parent.mkdir(parents=True, exist_ok=True)

    def get_stream_layers(self) -> list:
        try:
            layers = fiona.listlayers(str(self.streams_gdb))
            return [l for l in layers if not l.startswith('_') and len(l) <= 4]
        except Exception as e:
            logger.error(f"Error listing layers: {e}")
            return []

    def clean_watershed_code(self, code):
        if not isinstance(code, str): return None
        parts = code.split('-')
        valid_parts = [p for p in parts if p != '000000']
        return "-".join(valid_parts)

    def get_parent_code(self, clean_code):
        if not clean_code or '-' not in clean_code: return None
        return clean_code.rsplit('-', 1)[0]

    def load_and_enrich_streams(self, test_mode=False):
        logger.info("=== STEP 1: Loading and Enriching Streams ===")
        layers = self.get_stream_layers()
        
        if test_mode:
            layers = layers[:5]
            logger.info(f"TEST MODE: Processing limited layers: {layers}")

        gdf_list = []
        logger.info(f"Reading {len(layers)} stream layers...")
        
        for i, layer in enumerate(layers):
            try:
                gdf = gpd.read_file(str(self.streams_gdb), layer=layer)
                cols = ['FWA_WATERSHED_CODE', 'GNIS_NAME', 'geometry', 'LINEAR_FEATURE_ID']
                existing_cols = [c for c in cols if c in gdf.columns]
                gdf = gdf[existing_cols]
                gdf_list.append(gdf)
            except Exception as e:
                logger.warning(f"Skipping layer {layer}: {e}")
            
            if (i+1) % 50 == 0:
                logger.info(f"Loaded {i+1} layers...")

        if not gdf_list:
            raise ValueError("No stream data loaded.")

        full_df = pd.concat(gdf_list, ignore_index=True)
        full_gdf = gpd.GeoDataFrame(full_df, geometry='geometry', crs=gdf_list[0].crs)
        
        self.stats['total_streams_read'] = len(full_gdf)
        logger.info(f"Total streams loaded: {len(full_gdf):,}")

        # Hierarchy Parsing
        logger.info("Parsing watershed hierarchy...")
        full_gdf['clean_code'] = full_gdf['FWA_WATERSHED_CODE'].apply(self.clean_watershed_code)
        full_gdf['parent_code'] = full_gdf['clean_code'].apply(self.get_parent_code)

        # Build Name Map
        named_subset = full_gdf[
            (full_gdf['GNIS_NAME'].notna()) & 
            (full_gdf['GNIS_NAME'].str.strip() != '')
        ]
        self.stats['original_named_streams'] = len(named_subset)
        
        name_map = pd.Series(
            named_subset['GNIS_NAME'].values, 
            index=named_subset['clean_code']
        ).to_dict()
        
        logger.info(f"Mapped {len(name_map):,} unique named river codes.")

        # Identify Tributaries
        logger.info("Identifying first order tributaries...")
        unnamed_mask = (full_gdf['GNIS_NAME'].isna()) | (full_gdf['GNIS_NAME'].str.strip() == '')
        
        parents_names = full_gdf.loc[unnamed_mask, 'parent_code'].map(name_map)
        matched_indices = parents_names[parents_names.notna()].index
        
        new_names = parents_names[matched_indices] + " Tributary"
        full_gdf.loc[matched_indices, 'GNIS_NAME'] = new_names
        
        self.stats['tributaries_identified'] = len(matched_indices)
        logger.info(f"Renamed {len(matched_indices):,} tributaries.")

        # Final Filter
        final_streams = full_gdf[
            (full_gdf['GNIS_NAME'].notna()) & 
            (full_gdf['GNIS_NAME'].str.strip() != '')
        ].copy()

        return final_streams.drop(columns=['clean_code', 'parent_code'])

    def load_lakes(self):
        logger.info("=== STEP 2: Loading Lakes ===")
        lakes = gpd.read_file(str(self.lakes_gdb), layer='FWA_LAKES_POLY')
        self.stats['total_lakes'] = len(lakes)
        logger.info(f"Loaded {len(lakes):,} lakes.")
        return lakes

    def parallel_spatial_join(self, target_gdf, zone_gdf):
        """
        Splits the target GeoDataFrame into chunks and runs spatial join in parallel.
        """
        if len(target_gdf) == 0:
            return gpd.GeoDataFrame(columns=list(target_gdf.columns) + ['ZONE_GROUP'], geometry='geometry')

        logger.info(f"Parallelizing Spatial Join on {self.n_cores} cores...")
        
        # Split data into chunks
        chunks = np.array_split(target_gdf, self.n_cores)
        
        # Prepare arguments for each worker
        # We pass the full zone_gdf to each worker (it's small enough)
        args = [(chunk, zone_gdf[['geometry', 'ZONE_GROUP']]) for chunk in chunks]
        
        results = []
        with ProcessPoolExecutor(max_workers=self.n_cores) as executor:
            # Map returns results in order
            for res in executor.map(spatial_join_worker, args):
                results.append(res)
        
        # Combine results
        if results:
            logger.info("Merging parallel results...")
            return pd.concat(results, ignore_index=True)
        else:
            return gpd.GeoDataFrame()

    def split_and_save_optimized(self, streams_gdf, lakes_gdf):
        logger.info("=== STEP 3: Optimized Spatial Processing ===")
        self.cleanup_output()
        
        # 1. Prepare Wildlife Units
        wildlife = gpd.read_file(str(self.wildlife_gpkg))
        zone_field = next(col for col in wildlife.columns if 'ZONE' in col.upper() or 'UNIT' in col.upper())
        wildlife['ZONE_GROUP'] = wildlife[zone_field].astype(str).str.split('-').str[0]
        target_crs = wildlife.crs
        
        # Save Wildlife Units Reference
        wildlife.to_file(str(self.output_gdb), layer="WILDLIFE_MGMT_UNITS", driver="OpenFileGDB")
        
        # 2. Process Zone Outlines
        logger.info("Creating Zone Outlines...")
        zone_outlines = wildlife.dissolve(by='ZONE_GROUP')
        for zone_id, row in zone_outlines.iterrows():
            outline_gdf = gpd.GeoDataFrame([row], columns=zone_outlines.columns, crs=target_crs)
            outline_gdf.to_file(str(self.output_gdb), layer=f"ZONE_OUTLINE_{zone_id}", driver="OpenFileGDB")
            time.sleep(0.1)

        # 3. Parallel Stream Join
        logger.info(f"Processing Streams ({len(streams_gdf):,} features)...")
        if streams_gdf.crs != target_crs:
            streams_gdf = streams_gdf.to_crs(target_crs)
            
        # Run Parallel Join
        joined_streams = self.parallel_spatial_join(streams_gdf, wildlife)
        
        logger.info(f"Splitting {len(joined_streams):,} streams into zones...")
        # Convert back to GeoDataFrame (concat usually returns simple DataFrame/GeoDataFrame)
        # Ensure it has geometry set correctly
        if not isinstance(joined_streams, gpd.GeoDataFrame):
            joined_streams = gpd.GeoDataFrame(joined_streams, geometry='geometry', crs=target_crs)
        else:
            joined_streams.set_crs(target_crs, allow_override=True, inplace=True)

        for zone_id, zone_data in joined_streams.groupby('ZONE_GROUP'):
            clean_data = zone_data.drop_duplicates(subset=['LINEAR_FEATURE_ID'])
            
            # Clean columns safely
            keep_cols = [c for c in streams_gdf.columns if c in clean_data.columns]
            if 'geometry' not in keep_cols: keep_cols.append('geometry')
            
            clean_data = clean_data[keep_cols]
            
            if not clean_data.empty:
                clean_data.to_file(str(self.output_gdb), layer=f"STREAMS_ZONE_{zone_id}", driver="OpenFileGDB")
                logger.info(f"  -> Zone {zone_id}: Saved {len(clean_data)} streams")
                time.sleep(0.5)

        # 4. Parallel Lake Join
        logger.info(f"Processing Lakes ({len(lakes_gdf):,} features)...")
        if lakes_gdf.crs != target_crs:
            lakes_gdf = lakes_gdf.to_crs(target_crs)
            
        # Run Parallel Join
        joined_lakes = self.parallel_spatial_join(lakes_gdf, wildlife)
        
        # Ensure GeoDataFrame
        if not isinstance(joined_lakes, gpd.GeoDataFrame):
            joined_lakes = gpd.GeoDataFrame(joined_lakes, geometry='geometry', crs=target_crs)
        else:
            joined_lakes.set_crs(target_crs, allow_override=True, inplace=True)

        for zone_id, zone_data in joined_lakes.groupby('ZONE_GROUP'):
            dedup_col = 'WATERBODY_KEY' if 'WATERBODY_KEY' in zone_data.columns else None
            if dedup_col:
                clean_lakes = zone_data.drop_duplicates(subset=[dedup_col])
            else:
                clean_lakes = zone_data.drop_duplicates()
                
            keep_cols = [c for c in lakes_gdf.columns if c in clean_lakes.columns]
            if 'geometry' not in keep_cols: keep_cols.append('geometry')
            
            clean_lakes = clean_lakes[keep_cols]
            
            if not clean_lakes.empty:
                clean_lakes.to_file(str(self.output_gdb), layer=f"LAKES_ZONE_{zone_id}", driver="OpenFileGDB")
                logger.info(f"  -> Zone {zone_id}: Saved {len(clean_lakes)} lakes")
                time.sleep(0.5)

    def run(self, test_mode=False):
        logger.info(f"Starting processing on {self.n_cores} cores...")
        start = time.time()
        
        enriched_streams = self.load_and_enrich_streams(test_mode=test_mode)
        all_lakes = self.load_lakes()
        
        self.split_and_save_optimized(enriched_streams, all_lakes)
        
        end = time.time()
        logger.info("="*50)
        logger.info("PROCESSING COMPLETE")
        logger.info(f"Total Time: {(end-start)/60:.2f} minutes")
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
    
    # Set to False for production
    processor.run(test_mode=False)

if __name__ == "__main__":
    main()