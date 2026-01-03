#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Filter Unnamed Waterbodies and Split by Wildlife Management Zones

This script:
1. Combines all stream network layers from BC FWA geodatabase
2. Filters streams and lakes to keep only those with GNIS (Geographic Names) 
3. Loads BC Wildlife Management Units
4. Spatially intersects waterbodies with management zones
5. Creates zone-based layers (streams, lakes, and zone outline for each zone)

Input data:
- FWA_STREAM_NETWORKS_SP.gdb: Stream networks organized by watershed
- FWA_BC.gdb: Contains lakes layer (FWA_LAKES_POLY)
- WAA_WILDLIFE_MGMT_UNITS_SVW.gpkg: Wildlife management units/zones

Output:
- FWA_named_waterbodies.gdb containing:
  - STREAMS_ZONE_1, STREAMS_ZONE_2, etc. (named streams per zone)
  - LAKES_ZONE_1, LAKES_ZONE_2, etc. (named lakes per zone)
  - ZONE_OUTLINE_1, ZONE_OUTLINE_2, etc. (zone boundaries)
  - WILDLIFE_MGMT_UNITS (original management unit polygons)

Author: Generated for BC Freshwater Fishing Regulations Project
Date: December 31, 2025 - January 1, 2026
"""

# Import required libraries
import os
import fiona  # For reading GIS file formats
import geopandas as gpd  # For spatial data manipulation
import pandas as pd  # For data manipulation
from pathlib import Path  # For cross-platform file path handling
import logging  # For progress logging
from typing import List, Dict, Any  # For type hints
import time  # For timing operations

# Set up logging to track progress
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StreamFilterProcessor:
    """
    Class to handle filtering of unnamed streams from FWA geodatabase.
    
    This class processes watershed layers from the FWA Stream Networks geodatabase,
    filtering out streams without GNIS names and optionally combining all layers.
    """
    
    def __init__(self, input_gdb_path: str, output_gdb_path: str = None):
        """
        Initialize the processor.
        
        Args:
            input_gdb_path: Path to input geodatabase
            output_gdb_path: Path to output geodatabase (optional, auto-generated if None)
        """
        self.input_gdb_path = Path(input_gdb_path)
        
        # Create default output path if not provided
        if output_gdb_path is None:
            parent_dir = self.input_gdb_path.parent
            stem = self.input_gdb_path.stem
            self.output_gdb_path = parent_dir / f"{stem}_named_streams_only.gdb"
        else:
            self.output_gdb_path = Path(output_gdb_path)
        
        # Initialize empty layer list and statistics tracking
        self.layers = []
        self.stats = {
            'total_layers': 0,
            'processed_layers': 0,
            'total_features': 0,
            'named_features': 0,
            'unnamed_features': 0,
            'skipped_layers': []
        }
    
    def get_layers(self) -> List[str]:
        """
        Get list of all stream layers in the geodatabase.
        
        Filters out system layers (those starting with '_') and keeps only
        watershed layers (4 characters or less, like 'PORI', 'GUIC', etc.)
        
        Returns:
            List of layer names to process
        """
        try:
            # Get all layers from the geodatabase
            layers = fiona.listlayers(str(self.input_gdb_path))
            
            # Filter to get only stream layers (exclude system tables starting with _)
            stream_layers = [layer for layer in layers 
                           if not layer.startswith('_') and len(layer) <= 4]
            
            self.layers = stream_layers
            self.stats['total_layers'] = len(stream_layers)
            logger.info(f"Found {len(stream_layers)} stream layers to process")
            return stream_layers
        except Exception as e:
            logger.error(f"Error reading layers from geodatabase: {e}")
            raise
    
    def analyze_layer(self, layer_name: str) -> Dict[str, Any]:
        """
        Analyze a single layer to get statistics about named vs unnamed streams.
        
        Args:
            layer_name: Name of the layer to analyze
            
        Returns:
            Dictionary with statistics
        """
        try:
            with fiona.open(str(self.input_gdb_path), layer=layer_name) as src:
                total_count = 0
                named_count = 0
                unnamed_count = 0
                
                for feature in src:
                    total_count += 1
                    gnis_name = feature['properties'].get('GNIS_NAME')
                    
                    if gnis_name is not None and gnis_name.strip():
                        named_count += 1
                    else:
                        unnamed_count += 1
                
                return {
                    'layer': layer_name,
                    'total': total_count,
                    'named': named_count,
                    'unnamed': unnamed_count,
                    'named_percent': (named_count / total_count * 100) if total_count > 0 else 0
                }
        
        except Exception as e:
            logger.error(f"Error analyzing layer {layer_name}: {e}")
            return {'layer': layer_name, 'error': str(e)}
    
    def filter_layer(self, layer_name: str, gnis_field: str = 'GNIS_NAME') -> bool:
        """
        Filter a single layer to keep only named features (streams or lakes).
        
        This method:
        1. Reads the layer from the input geodatabase
        2. Filters to keep only features where GNIS name field is not null/empty
        3. Saves filtered features to output geodatabase
        4. Updates statistics
        
        Args:
            layer_name: Name of the layer to filter
            gnis_field: Name of the GNIS field to filter on (default: 'GNIS_NAME')
                       For lakes, use 'GNIS_NAME_1'
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Processing layer: {layer_name}")
            
            # Read the entire layer into a GeoDataFrame
            gdf = gpd.read_file(str(self.input_gdb_path), layer=layer_name)
            
            # Skip empty layers
            if gdf.empty:
                logger.warning(f"Layer {layer_name} is empty, skipping")
                self.stats['skipped_layers'].append(layer_name)
                return True
            
            original_count = len(gdf)
            
            # Filter out unnamed features (keep only features with GNIS name)
            # Check that GNIS field is not null AND not an empty string
            named_features = gdf[
                (gdf[gnis_field].notna()) & 
                (gdf[gnis_field].str.strip() != '')
            ].copy()
            
            named_count = len(named_features)
            unnamed_count = original_count - named_count
            
            # Update running statistics
            self.stats['total_features'] += original_count
            self.stats['named_features'] += named_count
            self.stats['unnamed_features'] += unnamed_count
            
            logger.info(f"  {layer_name}: {original_count} total -> {named_count} named "
                       f"({named_count/original_count*100:.1f}% retained)")
            
            # Only save if we have named features
            if named_count > 0:
                # Write the filtered layer to output geodatabase
                named_features.to_file(
                    str(self.output_gdb_path),
                    layer=layer_name,
                    driver='OpenFileGDB'
                )
            else:
                logger.warning(f"Layer {layer_name} has no named features, skipping output")
                self.stats['skipped_layers'].append(layer_name)
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing layer {layer_name}: {e}")
            self.stats['skipped_layers'].append(layer_name)
            return False
    
    def process_all_layers(self, max_layers: int = None, combine: bool = False) -> gpd.GeoDataFrame:
        """
        Process all layers in the geodatabase.
        
        This is the main processing method that either:
        - Saves each layer separately (combine=False), OR
        - Combines all layers into one GeoDataFrame (combine=True) for zone-based splitting
        
        Args:
            max_layers: Maximum number of layers to process (for testing). 
                       None = process all layers
            combine: If True, combine all layers into one GeoDataFrame instead of 
                    saving separately. Used for zone-based splitting.
            
        Returns:
            Combined GeoDataFrame if combine=True, otherwise None
        """
        start_time = time.time()
        
        # Get list of stream layers to process
        layers = self.get_layers()
        
        # Limit layers if testing
        if max_layers:
            layers = layers[:max_layers]
            logger.info(f"Processing first {max_layers} layers for testing")
        
        # Create output directory if it doesn't exist (only if not combining)
        if not combine:
            self.output_gdb_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Remove existing output geodatabase if it exists
            if self.output_gdb_path.exists():
                logger.info(f"Removing existing output geodatabase: {self.output_gdb_path}")
                import shutil
                import gc
                
                # Force garbage collection to release any file handles
                # (Windows sometimes locks GDB files)
                gc.collect()
                
                # Try to remove with retries (Windows file locking issues)
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        shutil.rmtree(self.output_gdb_path)
                        logger.info("Successfully removed existing geodatabase")
                        break
                    except PermissionError as e:
                        if attempt < max_retries - 1:
                            logger.warning(f"Failed to remove geodatabase (attempt {attempt + 1}/{max_retries}), retrying in 2 seconds...")
                            time.sleep(2)
                        else:
                            logger.warning(f"Could not remove existing geodatabase after {max_retries} attempts. Will attempt to overwrite layers.")
                            logger.warning(f"If you encounter errors, please manually delete: {self.output_gdb_path}")
                    except Exception as e:
                        logger.error(f"Error removing geodatabase: {e}")
                        logger.warning("Will attempt to overwrite layers.")
                        break
        
        logger.info(f"Starting processing of {len(layers)} layers...")
        if not combine:
            logger.info(f"Output will be saved to: {self.output_gdb_path}")
        else:
            logger.info("Layers will be combined into a single GeoDataFrame")
        
        # List to hold GeoDataFrames if combining
        combined_gdf = []
        
        # Process each layer
        for i, layer_name in enumerate(layers, 1):
            try:
                if combine:
                    # COMBINE MODE: Collect named features from all layers into one list
                    # Read the layer
                    gdf = gpd.read_file(str(self.input_gdb_path), layer=layer_name)
                    
                    if not gdf.empty:
                        original_count = len(gdf)
                        
                        # Filter for named features (streams with GNIS names)
                        named_features = gdf[
                            (gdf['GNIS_NAME'].notna()) & 
                            (gdf['GNIS_NAME'].str.strip() != '')
                        ].copy()
                        
                        named_count = len(named_features)
                        
                        # Update statistics
                        self.stats['total_features'] += original_count
                        self.stats['named_features'] += named_count
                        self.stats['unnamed_features'] += original_count - named_count
                        
                        # Add to combined list if we have named features
                        if named_count > 0:
                            combined_gdf.append(named_features)
                            logger.info(f"  {layer_name}: {original_count} total -> {named_count} named ({named_count/original_count*100:.1f}%)")
                        
                    self.stats['processed_layers'] += 1
                else:
                    # SEPARATE MODE: Save each layer individually
                    success = self.filter_layer(layer_name)
                    if success:
                        self.stats['processed_layers'] += 1
                
                # Progress update every 10 layers
                if i % 10 == 0:
                    elapsed = time.time() - start_time
                    avg_time = elapsed / i
                    remaining = (len(layers) - i) * avg_time
                    logger.info(f"Progress: {i}/{len(layers)} layers processed "
                               f"({i/len(layers)*100:.1f}%) - "
                               f"ETA: {remaining/60:.1f} minutes")
                
            except KeyboardInterrupt:
                logger.info("Processing interrupted by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error processing layer {layer_name}: {e}")
                continue
        
        # Return results based on mode
        elapsed_time = time.time() - start_time
        
        if combine and combined_gdf:
            # Combine all GeoDataFrames into one
            logger.info(f"Combining {len(combined_gdf)} layer(s) into single GeoDataFrame...")
            result = gpd.GeoDataFrame(pd.concat(combined_gdf, ignore_index=True))
            logger.info(f"Combined GeoDataFrame: {len(result):,} features")
            logger.info(f"Processing time: {elapsed_time/60:.2f} minutes")
            return result
        else:
            # Print summary and return None
            self.print_summary(elapsed_time)
            return None
    
    def print_summary(self, elapsed_time: float) -> None:
        """Print processing summary."""
        logger.info("\n" + "="*60)
        logger.info("PROCESSING SUMMARY")
        logger.info("="*60)
        logger.info(f"Input geodatabase: {self.input_gdb_path}")
        logger.info(f"Output geodatabase: {self.output_gdb_path}")
        logger.info(f"Total processing time: {elapsed_time/60:.2f} minutes")
        logger.info("")
        logger.info(f"Layers found: {self.stats['total_layers']}")
        logger.info(f"Layers processed: {self.stats['processed_layers']}")
        logger.info(f"Layers skipped: {len(self.stats['skipped_layers'])}")
        logger.info("")
        logger.info(f"Total stream features: {self.stats['total_features']:,}")
        logger.info(f"Named stream features: {self.stats['named_features']:,}")
        logger.info(f"Unnamed stream features: {self.stats['unnamed_features']:,}")
        
        if self.stats['total_features'] > 0:
            named_percent = self.stats['named_features'] / self.stats['total_features'] * 100
            logger.info(f"Percentage with names: {named_percent:.1f}%")
        
        if self.stats['skipped_layers']:
            logger.info(f"\nSkipped layers: {', '.join(self.stats['skipped_layers'])}")
        
        logger.info("="*60)


def main():
    """
    Main function to run the waterbody filtering and zone-based splitting process.
    
    This function orchestrates the entire workflow:
    1. Load and combine all stream network layers (246 watershed layers)
    2. Load and filter lakes layer
    3. Load wildlife management units
    4. Extract zone information from management units
    5. Spatially intersect waterbodies with zones
    6. Create zone-based output layers
    """
    
    # ===== STEP 1: Define all input/output paths =====
    # Use relative paths so script works from any location
    script_dir = Path(__file__).parent
    
    # Input: Stream networks geodatabase (246 watershed layers)
    streams_gdb = (script_dir.parent / "data" / "ftp.geobc.gov.bc.ca" / "sections" / 
                   "outgoing" / "bmgs" / "FWA_Public" / "FWA_STREAM_NETWORKS_SP" / 
                   "FWA_STREAM_NETWORKS_SP.gdb")
    
    # Input: BC geodatabase containing lakes layer
    lakes_gdb = (script_dir.parent / "data" / "ftp.geobc.gov.bc.ca" / "sections" / 
                 "outgoing" / "bmgs" / "FWA_Public" / "FWA_BC" / "FWA_BC.gdb")
    
    # Input: Wildlife management units GeoPackage
    wildlife_gpkg = (script_dir.parent / "data" / "ftp.geobc.gov.bc.ca" / "sections" / 
                     "outgoing" / "bmgs" / "FWA_Public" / "WAA_WILDLIFE_MGMT_UNITS_SVW.gpkg")
    
    # Output: Combined geodatabase with zone-based layers
    output_gdb = (script_dir / "output" / "FWA_named_waterbodies.gdb")
    
    # ===== STEP 2: Initialize processor and validate inputs =====
    processor = StreamFilterProcessor(str(streams_gdb), str(output_gdb))
    
    # Check that all input files exist
    if not processor.input_gdb_path.exists():
        logger.error(f"Input streams geodatabase not found: {streams_gdb}")
        return
    
    if not lakes_gdb.exists():
        logger.error(f"Input lakes geodatabase not found: {lakes_gdb}")
        return
    
    if not wildlife_gpkg.exists():
        logger.error(f"Input wildlife management units GeoPackage not found: {wildlife_gpkg}")
        return
    
    # Log the processing plan
    logger.info(f"Starting BC FWA waterbodies filtering and zone-based splitting process")
    logger.info(f"Streams input: {processor.input_gdb_path}")
    logger.info(f"Lakes input: {lakes_gdb}")
    logger.info(f"Wildlife units input: {wildlife_gpkg}")
    logger.info(f"Output: {processor.output_gdb_path}")
    
    try:
        # ===== STEP 3: Process all stream layers and combine them =====
        # TEST_MODE: Set to True to process only 20 layers for faster testing
        # Set to False to process all 246 layers (~3-4 minutes)
        # TEST MODE: Process only first 20 layers for faster testing
        TEST_MODE = False
        max_test_layers = 20 if TEST_MODE else None
        
        logger.info("\n=== Processing and Combining Stream Layers ===")
        if TEST_MODE:
            logger.info(f"*** TEST MODE: Processing only {max_test_layers} layers ***")
        
        # Combine all stream layers into one GeoDataFrame
        combined_streams = processor.process_all_layers(combine=True, max_layers=max_test_layers)
        
        # ===== STEP 4: Load and filter lakes =====
        logger.info("\n=== Processing Lakes Layer ===")
        
        # Read the lakes polygon layer
        lakes_gdf = gpd.read_file(str(lakes_gdb), layer='FWA_LAKES_POLY')
        logger.info(f"Total lakes: {len(lakes_gdf):,}")
        
        # Filter to keep only lakes with GNIS names
        # Note: Lakes use 'GNIS_NAME_1' field instead of 'GNIS_NAME'
        named_lakes = lakes_gdf[
            (lakes_gdf['GNIS_NAME_1'].notna()) & 
            (lakes_gdf['GNIS_NAME_1'].str.strip() != '')
        ].copy()
        logger.info(f"Named lakes: {len(named_lakes):,} ({len(named_lakes)/len(lakes_gdf)*100:.1f}%)")
        
        # ===== STEP 5: Load wildlife management units and extract zones =====
        logger.info("\n=== Loading Wildlife Management Units ===")
        
        # Check what layers are in the GeoPackage
        wildlife_layers = fiona.listlayers(str(wildlife_gpkg))
        logger.info(f"Found layers in wildlife GeoPackage: {wildlife_layers}")
        
        # Read the wildlife management units
        wildlife_gdf = gpd.read_file(str(wildlife_gpkg))
        logger.info(f"Wildlife management units: {len(wildlife_gdf):,} features")
        
        # Inspect available columns
        logger.info(f"Wildlife GDF columns: {wildlife_gdf.columns.tolist()}")
        
        # Automatically find the zone/management unit field
        # Look for columns containing 'ZONE', 'UNIT', or 'MGMT'
        zone_field = None
        for col in wildlife_gdf.columns:
            if 'ZONE' in col.upper() or 'UNIT' in col.upper() or 'MGMT' in col.upper():
                logger.info(f"Found potential zone field: {col}")
                logger.info(f"Sample values: {wildlife_gdf[col].head(10).tolist()}")
                # Use the first matching field
                if zone_field is None:
                    zone_field = col
        
        # Validate that we found a zone field
        if zone_field is None:
            logger.error("Could not identify zone field in wildlife management units")
            logger.info("Available columns: " + ", ".join(wildlife_gdf.columns.tolist()))
            return
        
        logger.info(f"Using zone field: {zone_field}")
        
        # Extract zone number from management unit IDs
        # Management units are formatted like "1-15", "2-3", etc.
        # We want just the zone number (first part before the dash)
        wildlife_gdf['ZONE_NUM'] = wildlife_gdf[zone_field].astype(str).str.split('-').str[0]
        unique_zones = sorted(wildlife_gdf['ZONE_NUM'].unique())
        logger.info(f"Found {len(unique_zones)} unique zones: {unique_zones}")
        
        # ===== STEP 6: Ensure all layers use the same coordinate reference system (CRS) =====
        logger.info(f"\nCRS - Streams: {combined_streams.crs}, Lakes: {named_lakes.crs}, Wildlife: {wildlife_gdf.crs}")
        
        # Reproject streams if needed
        if combined_streams.crs != wildlife_gdf.crs:
            logger.info(f"Reprojecting streams to match wildlife units CRS")
            combined_streams = combined_streams.to_crs(wildlife_gdf.crs)
        
        # Reproject lakes if needed
        if named_lakes.crs != wildlife_gdf.crs:
            logger.info(f"Reprojecting lakes to match wildlife units CRS")
            named_lakes = named_lakes.to_crs(wildlife_gdf.crs)
        
        # ===== STEP 7: Prepare output geodatabase =====
        # Create output directory if it doesn't exist
        output_gdb.parent.mkdir(parents=True, exist_ok=True)
        
        # Remove existing output geodatabase to start fresh
        if output_gdb.exists():
            import shutil
            import gc
            gc.collect()  # Force garbage collection to release file handles
            logger.info(f"Removing existing output geodatabase...")
            try:
                shutil.rmtree(output_gdb)
            except Exception as e:
                logger.warning(f"Could not remove existing geodatabase: {e}")
        
        # ===== STEP 8: Split streams by zone using spatial intersection =====
        logger.info("\n=== Splitting Streams by Zone ===")
        for zone in unique_zones:
            logger.info(f"Processing Zone {zone}...")
            
            # Get all wildlife management units in this zone
            zone_units = wildlife_gdf[wildlife_gdf['ZONE_NUM'] == zone]
            logger.info(f"  Zone {zone} has {len(zone_units)} management unit(s)")
            
            # Create zone outline by merging (dissolving) all management units in this zone
            # This gives us a single polygon representing the entire zone boundary
            zone_outline = zone_units.dissolve().reset_index(drop=True)
            zone_outline['ZONE_ID'] = zone
            zone_outline['ZONE_NAME'] = f"Zone {zone}"
            zone_outline['NUM_UNITS'] = len(zone_units)
            
            # Save the zone outline layer
            outline_layer_name = f"ZONE_OUTLINE_{zone}"
            zone_outline[['ZONE_ID', 'ZONE_NAME', 'NUM_UNITS', 'geometry']].to_file(
                str(output_gdb),
                layer=outline_layer_name,
                driver='OpenFileGDB'
            )
            logger.info(f"  Created zone outline layer: {outline_layer_name}")
            
            # Spatial intersection - find all streams that intersect this zone
            # This uses a spatial join to match streams with wildlife units
            streams_in_zone = gpd.sjoin(combined_streams, zone_units, predicate='intersects', how='inner')
            
            # Remove duplicate streams (a stream might intersect multiple units in same zone)
            # We only want each unique stream once per zone
            streams_in_zone = streams_in_zone.drop_duplicates(subset=['LINEAR_FEATURE_ID'])
            
            # Drop columns that might cause feature ID conflicts when writing to geodatabase
            # These include index columns and existing object IDs
            columns_to_drop = [col for col in streams_in_zone.columns 
                             if col in ['index_right', 'index_left', 'OBJECTID', 'FID']]
            if columns_to_drop:
                streams_in_zone = streams_in_zone.drop(columns=columns_to_drop)
            
            # Reset the index to ensure clean sequential numbering from 0
            streams_in_zone = streams_in_zone.reset_index(drop=True)
            
            # Create a completely fresh GeoDataFrame to avoid any hidden index issues
            # This prevents feature ID conflicts when writing to the geodatabase
            streams_in_zone = gpd.GeoDataFrame(
                streams_in_zone.copy(),
                geometry='geometry',
                crs=streams_in_zone.crs
            )
            
            logger.info(f"  Zone {zone}: {len(streams_in_zone):,} named streams")
            
            # Save the streams layer for this zone
            if len(streams_in_zone) > 0:
                layer_name = f"STREAMS_ZONE_{zone}"
                streams_in_zone.to_file(
                    str(output_gdb), 
                    layer=layer_name, 
                    driver='OpenFileGDB'
                )
                logger.info(f"  Saved as layer: {layer_name}")
        
        # ===== STEP 9: Split lakes by zone using spatial intersection =====
        logger.info("\n=== Splitting Lakes by Zone ===")
        for zone in unique_zones:
            logger.info(f"Processing Zone {zone}...")
            
            # Get all wildlife management units in this zone
            zone_units = wildlife_gdf[wildlife_gdf['ZONE_NUM'] == zone]
            
            # Spatial intersection - find all lakes that intersect this zone
            lakes_in_zone = gpd.sjoin(named_lakes, zone_units, predicate='intersects', how='inner')
            
            # Remove duplicate lakes (a lake might intersect multiple units in same zone)
            # If WATERBODY_KEY exists, use it as the unique identifier, otherwise drop all duplicates
            if 'WATERBODY_KEY' in lakes_in_zone.columns:
                lakes_in_zone = lakes_in_zone.drop_duplicates(subset=['WATERBODY_KEY'])
            else:
                lakes_in_zone = lakes_in_zone.drop_duplicates()
            
            # Drop columns that might cause feature ID conflicts when writing to geodatabase
            columns_to_drop = [col for col in lakes_in_zone.columns 
                             if col in ['index_right', 'index_left', 'OBJECTID', 'FID']]
            if columns_to_drop:
                lakes_in_zone = lakes_in_zone.drop(columns=columns_to_drop)
            
            # Reset the index to ensure clean sequential numbering from 0
            lakes_in_zone = lakes_in_zone.reset_index(drop=True)
            
            # Create a completely fresh GeoDataFrame to avoid any hidden index issues
            lakes_in_zone = gpd.GeoDataFrame(
                lakes_in_zone.copy(),
                geometry='geometry',
                crs=lakes_in_zone.crs
            )
            
            logger.info(f"  Zone {zone}: {len(lakes_in_zone):,} named lakes")
            
            # Save the lakes layer for this zone
            if len(lakes_in_zone) > 0:
                layer_name = f"LAKES_ZONE_{zone}"
                lakes_in_zone.to_file(
                    str(output_gdb), 
                    layer=layer_name, 
                    driver='OpenFileGDB'
                )
                logger.info(f"  Saved as layer: {layer_name}")
        
        # ===== STEP 10: Add wildlife management units as a reference layer =====
        logger.info("\n=== Adding Wildlife Management Units ===")
        wildlife_gdf.to_file(str(output_gdb), layer='WILDLIFE_MGMT_UNITS', driver='OpenFileGDB')
        logger.info("Added WILDLIFE_MGMT_UNITS layer")
        
        # ===== STEP 11: Print final summary statistics =====
        logger.info("\n" + "="*60)
        logger.info("PROCESSING SUMMARY")
        logger.info("="*60)
        logger.info(f"Output geodatabase: {output_gdb}")
        logger.info("")
        logger.info(f"Total named streams processed: {len(combined_streams):,}")
        logger.info(f"Total named lakes processed: {len(named_lakes):,}")
        logger.info(f"Zones created: {len(unique_zones)}")
        logger.info(f"Layers per zone: 3 (streams + lakes + zone outline)")
        logger.info(f"Total waterbody layers: {len(unique_zones) * 2}")
        logger.info(f"Total zone outline layers: {len(unique_zones)}")
        logger.info("="*60)
        
        logger.info("\nProcessing completed successfully!")
        logger.info(f"Filtered geodatabase saved to: {output_gdb}")
        
    except Exception as e:
        # Log any errors that occur during processing
        logger.error(f"Processing failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


def analyze_sample_layers(n_layers: int = 5):
    """
    Analyze a sample of layers to get statistics before processing.
    
    This helper function examines a few layers to understand the data structure
    and get preliminary counts of named vs unnamed features.
    
    Args:
        n_layers: Number of layers to analyze (default: 5)
    """
    script_dir = Path(__file__).parent
    input_gdb = (script_dir.parent / "data" / "ftp.geobc.gov.bc.ca" / "sections" / 
                "outgoing" / "bmgs" / "FWA_Public" / "FWA_STREAM_NETWORKS_SP" / 
                "FWA_STREAM_NETWORKS_SP.gdb")
    
    # Create processor instance
    processor = StreamFilterProcessor(str(input_gdb))
    
    # Get list of all watershed layers
    layers = processor.get_layers()
    
    logger.info(f"Analyzing first {n_layers} layers...")
    
    # Analyze each sample layer
    for layer_name in layers[:n_layers]:
        stats = processor.analyze_layer(layer_name)
        if 'error' not in stats:
            logger.info(f"{stats['layer']}: {stats['total']:,} total, "
                       f"{stats['named']:,} named ({stats['named_percent']:.1f}%), "
                       f"{stats['unnamed']:,} unnamed")
        else:
            logger.error(f"{layer_name}: {stats['error']}")


if __name__ == "__main__":
    # Uncomment the line below to analyze a sample of layers before processing all data
    # This is useful for understanding the data structure and getting preliminary statistics
    # analyze_sample_layers(10)
    
    # Run the main processing workflow
    # This will filter all named streams and lakes and organize them by wildlife management zone
    main()

