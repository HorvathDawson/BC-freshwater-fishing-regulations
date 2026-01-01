#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Filter Unnamed Streams from BC FWA Stream Networks GeoDB

This script processes the FWA Stream Networks geodatabase and creates a new
geodatabase containing only streams with valid GNIS names, filtering out
all unnamed streams.

Author: Generated for BC Freshwater Fishing Regulations Project
Date: December 31, 2025
"""

import os
import fiona
import geopandas as gpd
from pathlib import Path
import logging
from typing import List, Dict, Any
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StreamFilterProcessor:
    """Class to handle filtering of unnamed streams from FWA geodatabase."""
    
    def __init__(self, input_gdb_path: str, output_gdb_path: str = None):
        """
        Initialize the processor.
        
        Args:
            input_gdb_path: Path to input geodatabase
            output_gdb_path: Path to output geodatabase (optional)
        """
        self.input_gdb_path = Path(input_gdb_path)
        
        if output_gdb_path is None:
            # Create output path based on input path
            parent_dir = self.input_gdb_path.parent
            stem = self.input_gdb_path.stem
            self.output_gdb_path = parent_dir / f"{stem}_named_streams_only.gdb"
        else:
            self.output_gdb_path = Path(output_gdb_path)
        
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
        """Get list of all layers in the geodatabase."""
        try:
            layers = fiona.listlayers(str(self.input_gdb_path))
            # Filter out system layers
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
        Filter a single layer to keep only named streams.
        
        Args:
            layer_name: Name of the layer to filter
            gnis_field: Name of the GNIS field to filter on (default: 'GNIS_NAME')
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Processing layer: {layer_name}")
            
            # Read the layer
            gdf = gpd.read_file(str(self.input_gdb_path), layer=layer_name)
            
            if gdf.empty:
                logger.warning(f"Layer {layer_name} is empty, skipping")
                self.stats['skipped_layers'].append(layer_name)
                return True
            
            original_count = len(gdf)
            
            # Filter out unnamed features (keep only features with GNIS name)
            named_features = gdf[
                (gdf[gnis_field].notna()) & 
                (gdf[gnis_field].str.strip() != '')
            ].copy()
            
            named_count = len(named_features)
            unnamed_count = original_count - named_count
            
            # Update statistics
            self.stats['total_features'] += original_count
            self.stats['named_features'] += named_count
            self.stats['unnamed_features'] += unnamed_count
            
            logger.info(f"  {layer_name}: {original_count} total -> {named_count} named "
                       f"({named_count/original_count*100:.1f}% retained)")
            
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
    
    def process_all_layers(self, max_layers: int = None) -> None:
        """
        Process all layers in the geodatabase.
        
        Args:
            max_layers: Maximum number of layers to process (for testing)
        """
        start_time = time.time()
        
        layers = self.get_layers()
        
        if max_layers:
            layers = layers[:max_layers]
            logger.info(f"Processing first {max_layers} layers for testing")
        
        # Create output directory if it doesn't exist
        self.output_gdb_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Remove existing output geodatabase if it exists
        if self.output_gdb_path.exists():
            logger.info(f"Removing existing output geodatabase: {self.output_gdb_path}")
            import shutil
            import gc
            
            # Force garbage collection to release any file handles
            gc.collect()
            
            # Try to remove with retries
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
        logger.info(f"Output will be saved to: {self.output_gdb_path}")
        
        for i, layer_name in enumerate(layers, 1):
            try:
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
        
        # Final statistics
        elapsed_time = time.time() - start_time
        self.print_summary(elapsed_time)
    
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
    """Main function to run the stream filtering process."""
    # Define paths relative to script location
    script_dir = Path(__file__).parent
    streams_gdb = (script_dir.parent / "data" / "ftp.geobc.gov.bc.ca" / "sections" / 
                   "outgoing" / "bmgs" / "FWA_Public" / "FWA_STREAM_NETWORKS_SP" / 
                   "FWA_STREAM_NETWORKS_SP.gdb")
    
    lakes_gdb = (script_dir.parent / "data" / "ftp.geobc.gov.bc.ca" / "sections" / 
                 "outgoing" / "bmgs" / "FWA_Public" / "FWA_BC" / "FWA_BC.gdb")
    
    wildlife_gpkg = (script_dir.parent / "data" / "ftp.geobc.gov.bc.ca" / "sections" / 
                     "outgoing" / "bmgs" / "FWA_Public" / "WAA_WILDLIFE_MGMT_UNITS_SVW.gpkg")
    
    # Define output path
    output_gdb = (script_dir.parent / "data" / "ftp.geobc.gov.bc.ca" / "sections" / 
                  "outgoing" / "bmgs" / "FWA_Public" / "FWA_named_waterbodies.gdb")
    
    # Initialize processor for streams
    processor = StreamFilterProcessor(str(streams_gdb), str(output_gdb))
    
    # Check if inputs exist
    if not processor.input_gdb_path.exists():
        logger.error(f"Input streams geodatabase not found: {streams_gdb}")
        return
    
    if not lakes_gdb.exists():
        logger.error(f"Input lakes geodatabase not found: {lakes_gdb}")
        return
    
    if not wildlife_gpkg.exists():
        logger.error(f"Input wildlife management units GeoPackage not found: {wildlife_gpkg}")
        return
    
    logger.info(f"Starting BC FWA Stream Networks and Lakes filtering process")
    logger.info(f"Streams input: {processor.input_gdb_path}")
    logger.info(f"Lakes input: {lakes_gdb}")
    logger.info(f"Wildlife units input: {wildlife_gpkg}")
    logger.info(f"Output: {processor.output_gdb_path}")
    
    try:
        # Process all stream layers
        logger.info("\n=== Processing Stream Layers ===")
        processor.process_all_layers()
        
        # Process lakes layer
        logger.info("\n=== Processing Lakes Layer ===")
        lakes_processor = StreamFilterProcessor(str(lakes_gdb), str(output_gdb))
        
        # Filter the FWA_LAKES_POLY layer using GNIS_NAME_1 field
        lakes_processor.filter_layer('FWA_LAKES_POLY', gnis_field='GNIS_NAME_1')
        
        # Add wildlife management units
        logger.info("\n=== Adding Wildlife Management Units ===")
        import fiona
        wildlife_layers = fiona.listlayers(str(wildlife_gpkg))
        logger.info(f"Found {len(wildlife_layers)} layer(s) in wildlife GeoPackage: {wildlife_layers}")
        
        for layer_name in wildlife_layers:
            logger.info(f"Adding layer: {layer_name}")
            wildlife_gdf = gpd.read_file(str(wildlife_gpkg), layer=layer_name)
            logger.info(f"  {layer_name}: {len(wildlife_gdf):,} features")
            
            wildlife_gdf.to_file(
                str(output_gdb),
                layer=layer_name,
                driver='OpenFileGDB'
            )
            logger.info(f"  Successfully added {layer_name}")
        
        # Update combined statistics
        total_features = processor.stats['total_features'] + lakes_processor.stats['total_features']
        named_features = processor.stats['named_features'] + lakes_processor.stats['named_features']
        unnamed_features = processor.stats['unnamed_features'] + lakes_processor.stats['unnamed_features']
        
        logger.info("\n" + "="*60)
        logger.info("COMBINED PROCESSING SUMMARY")
        logger.info("="*60)
        logger.info(f"Streams geodatabase: {streams_gdb}")
        logger.info(f"Lakes geodatabase: {lakes_gdb}")
        logger.info(f"Wildlife units GeoPackage: {wildlife_gpkg}")
        logger.info(f"Output geodatabase: {output_gdb}")
        logger.info("")
        logger.info(f"Stream layers processed: {processor.stats['processed_layers']}")
        logger.info(f"Lakes layer processed: 1")
        logger.info(f"Wildlife layers added: {len(wildlife_layers)}")
        logger.info("")
        logger.info(f"Total waterbody features: {total_features:,}")
        logger.info(f"Named waterbody features: {named_features:,}")
        logger.info(f"Unnamed waterbody features: {unnamed_features:,}")
        
        if total_features > 0:
            named_percent = named_features / total_features * 100
            logger.info(f"Percentage with names: {named_percent:.1f}%")
        
        logger.info("="*60)
        
        logger.info("\nProcessing completed successfully!")
        logger.info(f"Filtered geodatabase saved to: {output_gdb}")
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise


def analyze_sample_layers(n_layers: int = 5):
    """
    Analyze a sample of layers to get statistics before processing.
    
    Args:
        n_layers: Number of layers to analyze
    """
    script_dir = Path(__file__).parent
    input_gdb = (script_dir.parent / "data" / "ftp.geobc.gov.bc.ca" / "sections" / 
                "outgoing" / "bmgs" / "FWA_Public" / "FWA_STREAM_NETWORKS_SP" / 
                "FWA_STREAM_NETWORKS_SP.gdb")
    
    processor = StreamFilterProcessor(str(input_gdb))
    layers = processor.get_layers()
    
    logger.info(f"Analyzing first {n_layers} layers...")
    
    for layer_name in layers[:n_layers]:
        stats = processor.analyze_layer(layer_name)
        if 'error' not in stats:
            logger.info(f"{stats['layer']}: {stats['total']:,} total, "
                       f"{stats['named']:,} named ({stats['named_percent']:.1f}%), "
                       f"{stats['unnamed']:,} unnamed")
        else:
            logger.error(f"{layer_name}: {stats['error']}")


if __name__ == "__main__":
    # Uncomment the line below to analyze sample layers first
    # analyze_sample_layers(10)
    
    # Run the main processing
    main()
