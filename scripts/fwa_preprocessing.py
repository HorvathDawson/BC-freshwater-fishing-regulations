#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BC FWA Processing: Simplified Watershed Hierarchy Approach

Logic Flow:
1. Load Streams, Lakes, Wetlands, Manmade Waterbodies, and KML Points
2. Watershed Code Enrichment:
   - Unnamed streams with SAME code as named → Inherit name (they ARE the river)
   - ALL streams get TRIBUTARY_OF from parent watershed code
   - Only UNNAMED streams get renamed to "X Tributary"
3. Lake Tributary Detection (via WATERBODY_KEY):
   - Streams with same WATERBODY_KEY as lake → Lake tributary
   - Update TRIBUTARY_OF and rename if originally unnamed
4. Braid Name Fixing:
   - Braids (EDGE_TYPE 1100) with same BLUE_LINE_KEY inherit names
5. KML Point Enrichment:
   - Link points to containing waterbody polygons
6. Zone Splitting:
   - Split everything by wildlife management zones
7. Index Building (Optional):
   - Build searchable index from processed data
"""

import os
import sys
import argparse

# --- FIX FOR "Cannot find header.dxf" WARNING ---
os.environ["GDAL_SKIP"] = "DXF"

if "GDAL_DATA" not in os.environ:
    candidates = [
        os.path.join(sys.prefix, "share", "gdal"),
        os.path.join(sys.prefix, "Library", "share", "gdal"),
    ]
    for c in candidates:
        if os.path.exists(c):
            os.environ["GDAL_DATA"] = c
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

# Enable KML Driver
fiona.drvsupport.supported_drivers["KML"] = "rw"
fiona.drvsupport.supported_drivers["LIBKML"] = "rw"

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

from fwa_modules.models import ProcessingStats
from fwa_modules.index_builder import IndexBuilder


# --- WORKER FUNCTIONS FOR PARALLEL PROCESSING ---


def load_stream_layer_worker(args):
    """Worker function to load a single stream layer in parallel."""
    gdb_path, layer_name = args
    try:
        gdf = gpd.read_file(str(gdb_path), layer=layer_name)
        # Only keep columns we need
        essential_cols = [
            "LINEAR_FEATURE_ID",
            "FWA_WATERSHED_CODE",
            "GNIS_NAME",
            "WATERBODY_KEY",
            "EDGE_TYPE",
            "BLUE_LINE_KEY",
            "geometry",
        ]
        existing = [c for c in essential_cols if c in gdf.columns]
        return gdf[existing]
    except Exception as e:
        logger.warning(f"Skipping {layer_name}: {e}")
        return None


def spatial_join_worker(args):
    """Worker function for parallel spatial joins."""
    left_chunk, right_gdf = args
    return gpd.sjoin(left_chunk, right_gdf, predicate="intersects", how="inner")


class FWAProcessor:
    """Main processor for BC Freshwater Atlas (FWA) data."""

    def __init__(
        self,
        streams_gdb: str,
        lakes_gdb: str,
        wildlife_gpkg: str,
        kml_path: str,
        output_gpkg: str,
    ):
        self.streams_gdb = Path(streams_gdb)
        self.lakes_gdb = Path(lakes_gdb)
        self.wildlife_gpkg = Path(wildlife_gpkg)
        self.kml_path = Path(kml_path)
        self.output_gpkg = Path(output_gpkg)

        self.n_cores = max(1, os.cpu_count() - 1)
        self.stats = ProcessingStats()

    def cleanup_output(self):
        """Remove old output files."""
        gc.collect()
        if self.output_gpkg.exists():
            try:
                os.remove(self.output_gpkg)
            except PermissionError:
                pass

    def get_stream_layers(self) -> list:
        """Get list of stream layer names from GDB."""
        try:
            layers = fiona.listlayers(str(self.streams_gdb))
            return [l for l in layers if not l.startswith("_") and len(l) <= 4]
        except Exception:
            return []

    # --- WATERSHED CODE PARSING ---

    def clean_watershed_code(self, code):
        """Remove 000000 segments from watershed codes."""
        if not isinstance(code, str):
            return None
        parts = code.split("-")
        valid_parts = [p for p in parts if p != "000000"]
        return "-".join(valid_parts)

    def get_parent_code(self, clean_code):
        """Get parent watershed code by removing last segment."""
        if not clean_code or "-" not in clean_code:
            return None
        return clean_code.rsplit("-", 1)[0]

    # --- DATA LOADERS ---

    def load_streams_raw(self, test_mode=False):
        """Load all stream features in parallel."""
        logger.info("=" * 80)
        logger.info("PHASE 1: Loading Streams")
        logger.info("=" * 80)

        layers = self.get_stream_layers()
        if test_mode:
            layers = layers[:5]
            logger.info(f"TEST MODE: Loading first 5 layers only")

        total_layers = len(layers)
        logger.info(
            f"Loading {total_layers} stream layers using {self.n_cores} cores..."
        )

        args = [(self.streams_gdb, layer) for layer in layers]
        batch_size = max(self.n_cores, 5)
        total_features = 0
        loaded_layers = 0
        full_gdf = None
        first_crs = None

        with ProcessPoolExecutor(max_workers=self.n_cores) as executor:
            for batch_start in range(0, len(args), batch_size):
                batch_end = min(batch_start + batch_size, len(args))
                batch_args = args[batch_start:batch_end]

                batch_results = []
                for result in executor.map(load_stream_layer_worker, batch_args):
                    if result is not None:
                        batch_results.append(result)
                        total_features += len(result)
                        loaded_layers += 1

                if batch_results:
                    if first_crs is None:
                        first_crs = batch_results[0].crs

                    batch_gdf = pd.concat(batch_results, ignore_index=True)
                    batch_gdf = gpd.GeoDataFrame(
                        batch_gdf, geometry="geometry", crs=first_crs
                    )

                    if full_gdf is None:
                        full_gdf = batch_gdf
                    else:
                        full_gdf = pd.concat([full_gdf, batch_gdf], ignore_index=True)
                        full_gdf = gpd.GeoDataFrame(
                            full_gdf, geometry="geometry", crs=first_crs
                        )

                    del batch_results, batch_gdf
                    gc.collect()

                percent = (batch_end / total_layers) * 100
                logger.info(
                    f"  Progress: {batch_end}/{total_layers} ({percent:.1f}%) - {total_features:,} features"
                )

        if full_gdf is None:
            full_gdf = gpd.GeoDataFrame()

        # Deduplicate by LINEAR_FEATURE_ID
        if "LINEAR_FEATURE_ID" in full_gdf.columns:
            before = len(full_gdf)
            full_gdf = full_gdf.drop_duplicates(subset=["LINEAR_FEATURE_ID"])
            after = len(full_gdf)
            if before != after:
                logger.info(f"  Removed {before - after:,} duplicate features")

        self.stats.total_streams = len(full_gdf)
        logger.info(f"✓ Loaded {len(full_gdf):,} unique stream features")
        return full_gdf

    def load_lakes(self):
        """Load lake polygons from FWA_BC.gdb."""
        logger.info("Loading Lakes...")
        try:
            lakes = gpd.read_file(str(self.lakes_gdb), layer="FWA_LAKES_POLY")
            self.stats.total_lakes = len(lakes)
            logger.info(f"✓ Loaded {len(lakes):,} lakes")
            return lakes
        except Exception as e:
            logger.error(f"Failed to load lakes: {e}")
            return gpd.GeoDataFrame()

    def load_wetlands(self):
        """Load wetland polygons from FWA_BC.gdb."""
        logger.info("Loading Wetlands...")
        try:
            wetlands = gpd.read_file(str(self.lakes_gdb), layer="FWA_WETLANDS_POLY")
            self.stats.total_wetlands = len(wetlands)
            logger.info(f"✓ Loaded {len(wetlands):,} wetlands")
            return wetlands
        except Exception as e:
            logger.warning(f"Failed to load wetlands: {e}")
            return gpd.GeoDataFrame()

    def load_manmade(self):
        """Load manmade waterbody polygons from FWA_BC.gdb."""
        logger.info("Loading Manmade Waterbodies...")
        try:
            manmade = gpd.read_file(
                str(self.lakes_gdb), layer="FWA_MANMADE_WATERBODIES_POLY"
            )
            self.stats.total_manmade = len(manmade)
            logger.info(f"✓ Loaded {len(manmade):,} manmade waterbodies")
            return manmade
        except Exception as e:
            logger.warning(f"Failed to load manmade waterbodies: {e}")
            return gpd.GeoDataFrame()

    def load_kml_points(self):
        """Load user-labeled points from KML."""
        logger.info("Loading KML Points...")
        if not self.kml_path.exists():
            logger.warning(f"KML file not found: {self.kml_path}")
            return gpd.GeoDataFrame()

        try:
            points = gpd.read_file(str(self.kml_path))
            if points.crs is None:
                points.set_crs(epsg=4326, inplace=True)

            self.stats.total_kml_points = len(points)
            logger.info(f"✓ Loaded {len(points):,} KML points")
            return points
        except Exception as e:
            logger.error(f"Failed to load KML: {e}")
            return gpd.GeoDataFrame()

    # --- ENRICHMENT LOGIC ---

    def enrich_kml_points(self, points_gdf, lakes_gdf, wetlands_gdf, manmade_gdf):
        """Link user-labeled points to their containing waterbody polygons."""
        if points_gdf.empty:
            return gpd.GeoDataFrame()

        logger.info("=" * 80)
        logger.info("PHASE 2A: KML Point Enrichment")
        logger.info("=" * 80)

        # Ensure CRS match (convert points to BC Albers)
        target_crs = "EPSG:3005"
        if points_gdf.crs != target_crs:
            points_gdf = points_gdf.to_crs(target_crs)

        # Initialize ID columns
        points_gdf["LAKE_POLY_ID"] = None
        points_gdf["WETLAND_POLY_ID"] = None
        points_gdf["MANMADE_POLY_ID"] = None

        def attach_id(points, polys, id_col_name, poly_type_name):
            """Perform spatial join and assign waterbody IDs."""
            if polys.empty or "WATERBODY_POLY_ID" not in polys.columns:
                return points

            # Ensure CRS match
            if points.crs != polys.crs:
                points = points.to_crs(polys.crs)

            # Spatial join
            joined = gpd.sjoin(
                points,
                polys[["geometry", "WATERBODY_POLY_ID"]],
                how="left",
                predicate="intersects",
            )

            # Handle duplicates (point in multiple polygons - take first)
            id_map = joined.groupby(joined.index)["WATERBODY_POLY_ID"].first()
            matched_count = id_map.notna().sum()
            points.loc[id_map.index, id_col_name] = id_map

            logger.info(f"  {poly_type_name}: {matched_count} points matched")
            return points

        # Match to each polygon type
        if not lakes_gdf.empty:
            points_gdf = attach_id(points_gdf, lakes_gdf, "LAKE_POLY_ID", "Lakes")
        if not wetlands_gdf.empty:
            points_gdf = attach_id(
                points_gdf, wetlands_gdf, "WETLAND_POLY_ID", "Wetlands"
            )
        if not manmade_gdf.empty:
            points_gdf = attach_id(
                points_gdf, manmade_gdf, "MANMADE_POLY_ID", "Manmade"
            )

        # Convert to nullable integer type
        for col in ["LAKE_POLY_ID", "WETLAND_POLY_ID", "MANMADE_POLY_ID"]:
            points_gdf[col] = points_gdf[col].astype("Int64")

        # Log unmatched points
        unmatched = points_gdf[
            points_gdf["LAKE_POLY_ID"].isna()
            & points_gdf["WETLAND_POLY_ID"].isna()
            & points_gdf["MANMADE_POLY_ID"].isna()
        ]

        if not unmatched.empty:
            error_log_path = self.output_gpkg.parent / "unmatched_points_error_log.csv"
            logger.warning(
                f"⚠️  {len(unmatched)} points did not match any waterbody polygon"
            )
            logger.warning(f"  Saving list to: {error_log_path}")

            error_log_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                log_cols = ["geometry"]
                for col in ["Name", "name", "Description", "description"]:
                    if col in unmatched.columns:
                        log_cols.insert(0, col)
                unmatched[log_cols].to_csv(error_log_path, index=True)
            except Exception as e:
                logger.error(f"Could not write error log: {e}")

        logger.info("✓ KML point enrichment complete")
        return points_gdf

    def enrich_streams(self, streams_gdf, lakes_gdf):
        """Enrich stream names using watershed hierarchy and lake associations."""
        logger.info("=" * 80)
        logger.info("PHASE 2B: Stream Enrichment")
        logger.info("=" * 80)

        # Parse watershed codes
        logger.info("Parsing watershed codes...")
        streams_gdf["clean_code"] = streams_gdf["FWA_WATERSHED_CODE"].apply(
            self.clean_watershed_code
        )
        streams_gdf["parent_code"] = streams_gdf["clean_code"].apply(
            self.get_parent_code
        )

        # Initialize new fields
        streams_gdf["TRIBUTARY_OF"] = None
        streams_gdf["LAKE_POLY_ID"] = None

        # Track which streams were originally unnamed
        originally_unnamed_mask = (streams_gdf["GNIS_NAME"].isna()) | (
            streams_gdf["GNIS_NAME"].str.strip() == ""
        )

        named_mask = ~originally_unnamed_mask
        self.stats.originally_named = named_mask.sum()
        logger.info(f"  Originally named streams: {self.stats.originally_named:,}")

        # STEP 1: Unnamed streams with SAME watershed code as named streams inherit name
        logger.info(
            "Step 1: Inheriting names for unnamed streams with same watershed code..."
        )
        name_by_code = streams_gdf.loc[
            named_mask, ["clean_code", "GNIS_NAME"]
        ].drop_duplicates(subset="clean_code")
        code_to_name = pd.Series(
            name_by_code["GNIS_NAME"].values, index=name_by_code["clean_code"]
        )

        inherited_names = streams_gdf.loc[originally_unnamed_mask, "clean_code"].map(
            code_to_name
        )
        inherited_mask = originally_unnamed_mask & inherited_names.notna()
        streams_gdf.loc[inherited_mask, "GNIS_NAME"] = inherited_names[inherited_mask]
        self.stats.inherited_same_code = inherited_mask.sum()
        logger.info(
            f"  ✓ Inherited names for {self.stats.inherited_same_code:,} streams"
        )

        # Update masks after inheritance
        originally_unnamed_mask = (streams_gdf["GNIS_NAME"].isna()) | (
            streams_gdf["GNIS_NAME"].str.strip() == ""
        )
        named_mask = ~originally_unnamed_mask

        # STEP 2: Set TRIBUTARY_OF for ALL streams based on parent watershed code
        logger.info("Step 2: Setting TRIBUTARY_OF for all streams...")
        name_df = streams_gdf.loc[
            named_mask, ["clean_code", "GNIS_NAME"]
        ].drop_duplicates(subset="clean_code")
        name_map = pd.Series(
            name_df["GNIS_NAME"].values,
            index=name_df["clean_code"],
        )

        # Map parent_code → parent name for ALL streams
        all_parent_names = streams_gdf["parent_code"].map(name_map)
        streams_with_parent = all_parent_names.notna()

        # Set TRIBUTARY_OF for all streams with a parent
        streams_gdf.loc[streams_with_parent, "TRIBUTARY_OF"] = all_parent_names[
            streams_with_parent
        ]

        # Only rename UNNAMED streams
        unnamed_with_parent = originally_unnamed_mask & streams_with_parent
        streams_gdf.loc[unnamed_with_parent, "GNIS_NAME"] = (
            all_parent_names[unnamed_with_parent] + " Tributary"
        )

        self.stats.river_tributaries = unnamed_with_parent.sum()
        logger.info(
            f"  ✓ Set TRIBUTARY_OF for {streams_with_parent.sum():,} streams (all)"
        )
        logger.info(
            f"  ✓ Renamed {self.stats.river_tributaries:,} unnamed river tributaries"
        )

        # STEP 3: Lake tributary detection
        if not lakes_gdf.empty:
            logger.info("Step 3: Detecting lake tributaries...")

            # Build lake name lookup by WATERBODY_KEY
            named_lakes = lakes_gdf[
                (lakes_gdf["GNIS_NAME_1"].notna())
                & (lakes_gdf["GNIS_NAME_1"].str.strip() != "")
            ].copy()

            total_lake_tribs = 0
            lake_trib_indices = set()

            # 3a. Streams INSIDE lakes (via WATERBODY_KEY)
            logger.info("  Step 3a: Detecting streams inside lakes (WATERBODY_KEY)...")
            if (
                "WATERBODY_KEY" in named_lakes.columns
                and "WATERBODY_KEY" in streams_gdf.columns
            ):
                # Get first lake name for each WATERBODY_KEY
                lake_key_to_name = named_lakes.groupby("WATERBODY_KEY")[
                    "GNIS_NAME_1"
                ].first()
                lake_key_to_poly_id = named_lakes.groupby("WATERBODY_KEY")[
                    "WATERBODY_POLY_ID"
                ].first()

                # Find streams with matching WATERBODY_KEY
                streams_with_key = streams_gdf["WATERBODY_KEY"].notna()
                matching_lake_names = streams_gdf.loc[
                    streams_with_key, "WATERBODY_KEY"
                ].map(lake_key_to_name)
                has_lake_match = matching_lake_names.notna()

                if has_lake_match.any():
                    matching_indices = streams_gdf.index[streams_with_key][
                        has_lake_match
                    ]

                    # Set TRIBUTARY_OF and LAKE_POLY_ID for all matches
                    streams_gdf.loc[matching_indices, "TRIBUTARY_OF"] = (
                        matching_lake_names[has_lake_match]
                    )
                    matching_poly_ids = streams_gdf.loc[
                        matching_indices, "WATERBODY_KEY"
                    ].map(lake_key_to_poly_id)
                    streams_gdf.loc[matching_indices, "LAKE_POLY_ID"] = (
                        matching_poly_ids
                    )

                    # Track these as lake tributaries
                    lake_trib_indices.update(matching_indices.tolist())

                    # Only rename if originally unnamed
                    unnamed_lake_trib_mask = matching_indices.isin(
                        streams_gdf.index[originally_unnamed_mask]
                    )
                    unnamed_indices = matching_indices[unnamed_lake_trib_mask]

                    if len(unnamed_indices) > 0:
                        streams_gdf.loc[unnamed_indices, "GNIS_NAME"] = (
                            matching_lake_names[has_lake_match][unnamed_lake_trib_mask]
                            + " Tributary"
                        )

                    total_lake_tribs += len(unnamed_indices)
                    logger.info(
                        f"    ✓ Found {len(matching_indices):,} streams inside lakes"
                    )
                    logger.info(
                        f"    ✓ Renamed {len(unnamed_indices):,} unnamed streams"
                    )

            # 3b. Propagate lake tributary status upstream
            logger.info("  Step 3b: Propagating lake tributary status upstream...")

            # Build watershed code to lake mapping from streams we already marked
            code_to_lake = {}
            for idx in lake_trib_indices:
                clean_code = streams_gdf.loc[idx, "clean_code"]
                lake_name = streams_gdf.loc[idx, "TRIBUTARY_OF"]
                if clean_code and lake_name:
                    code_to_lake[clean_code] = lake_name

            # Now check all streams - if their parent code points to a lake tributary,
            # they should ALSO be lake tributaries
            changed = True
            iterations = 0
            max_iterations = 10  # Prevent infinite loops

            while changed and iterations < max_iterations:
                changed = False
                iterations += 1

                for idx, row in streams_gdf.iterrows():
                    parent_code = row["parent_code"]
                    if parent_code and parent_code in code_to_lake:
                        lake_name = code_to_lake[parent_code]
                        current_trib = row["TRIBUTARY_OF"]
                        current_name = row["GNIS_NAME"]

                        # Don't propagate if:
                        # 1. Stream has a TRIBUTARY_OF that's not null and not a lake name
                        #    (preserve tributary relationships to named rivers/creeks)
                        # 2. Stream was originally named in FWA data
                        if pd.notna(current_trib) and not current_trib.endswith(" Lake"):
                            # Stream is tributary of a named river/creek, don't override
                            continue

                        if idx not in streams_gdf.index[originally_unnamed_mask]:
                            # Stream was originally named in FWA, don't override
                            continue

                        # Update if not already this lake tributary
                        if current_trib != lake_name:
                            streams_gdf.loc[idx, "TRIBUTARY_OF"] = lake_name

                            # Also update the code mapping
                            clean_code = row["clean_code"]
                            if clean_code:
                                code_to_lake[clean_code] = lake_name

                            # Rename unnamed stream
                            streams_gdf.loc[idx, "GNIS_NAME"] = (
                                f"{lake_name} Tributary"
                            )
                            total_lake_tribs += 1

                            changed = True

            logger.info(f"    ✓ Propagated in {iterations} iteration(s)")

            self.stats.lake_tributaries = total_lake_tribs
            logger.info(f"  ✓ Total lake tributaries detected: {total_lake_tribs:,}")
        else:
            logger.info("Step 3: Skipping lake tributary detection (no lakes loaded)")

        # STEP 4: Fix braided stream names
        logger.info("Step 4: Fixing braided stream names...")
        if (
            "EDGE_TYPE" in streams_gdf.columns
            and "BLUE_LINE_KEY" in streams_gdf.columns
        ):
            # Braids are EDGE_TYPE 1100
            braid_mask = streams_gdf["EDGE_TYPE"] == 1100
            braids = streams_gdf[braid_mask]

            if len(braids) > 0:
                # Group by BLUE_LINE_KEY and find main channel name
                for blue_line_key in braids["BLUE_LINE_KEY"].unique():
                    if pd.isna(blue_line_key):
                        continue

                    # Get all segments with this BLUE_LINE_KEY
                    same_blue_line = streams_gdf["BLUE_LINE_KEY"] == blue_line_key

                    # Find main channel (non-braid) name and tributary
                    main_channels = streams_gdf[same_blue_line & ~braid_mask]
                    if len(main_channels) > 0:
                        main_name = (
                            main_channels["GNIS_NAME"].dropna().iloc[0]
                            if len(main_channels["GNIS_NAME"].dropna()) > 0
                            else None
                        )
                        main_trib = (
                            main_channels["TRIBUTARY_OF"].dropna().iloc[0]
                            if len(main_channels["TRIBUTARY_OF"].dropna()) > 0
                            else None
                        )

                        # Apply to braids
                        braid_indices = streams_gdf.index[same_blue_line & braid_mask]
                        if main_name:
                            streams_gdf.loc[braid_indices, "GNIS_NAME"] = main_name
                        if main_trib:
                            streams_gdf.loc[braid_indices, "TRIBUTARY_OF"] = main_trib

                self.stats.braids_fixed = len(braids)
                logger.info(
                    f"  ✓ Fixed {self.stats.braids_fixed:,} braided stream names"
                )
        else:
            logger.info(
                "  Skipping braid fixing (EDGE_TYPE or BLUE_LINE_KEY not found)"
            )

        # Filter to named streams only
        final_streams = streams_gdf[
            (streams_gdf["GNIS_NAME"].notna())
            & (streams_gdf["GNIS_NAME"].str.strip() != "")
        ].copy()

        # Drop temporary columns
        temp_cols = ["clean_code", "parent_code"]
        cols_to_drop = [c for c in temp_cols if c in final_streams.columns]
        if cols_to_drop:
            final_streams = final_streams.drop(columns=cols_to_drop)

        self.stats.final_named_count = len(final_streams)
        logger.info(f"✓ Final named streams: {self.stats.final_named_count:,}")

        gc.collect()
        return final_streams

    def parallel_spatial_join(self, target_gdf, zone_gdf):
        """Perform spatial join in parallel."""
        if len(target_gdf) == 0:
            return gpd.GeoDataFrame()

        chunks = np.array_split(target_gdf, self.n_cores)
        args = [(chunk, zone_gdf) for chunk in chunks]
        results = []

        with ProcessPoolExecutor(max_workers=self.n_cores) as executor:
            for res in executor.map(spatial_join_worker, args):
                results.append(res)

        if results:
            res_df = pd.concat(results, ignore_index=True)
            final_gdf = gpd.GeoDataFrame(
                res_df, geometry="geometry", crs=target_gdf.crs
            )
            del results, res_df
            gc.collect()
            return final_gdf
        return gpd.GeoDataFrame()

    def save_to_gpkg(
        self,
        streams_gdf,
        lakes_gdf,
        wetlands_gdf,
        manmade_gdf,
        points_gdf,
        test_mode=False,
    ):
        """Save all datasets to a single GPKG file (test mode) or split by zone."""
        logger.info("=" * 80)
        logger.info("PHASE 3: Saving Output")
        logger.info("=" * 80)

        self.cleanup_output()
        self.output_gpkg.parent.mkdir(parents=True, exist_ok=True)

        target_crs = "EPSG:3005"  # BC Albers

        # CRS transformations
        logger.info("Performing CRS transformations...")
        if not streams_gdf.empty and streams_gdf.crs != target_crs:
            streams_gdf = streams_gdf.to_crs(target_crs)
        if not lakes_gdf.empty and lakes_gdf.crs != target_crs:
            lakes_gdf = lakes_gdf.to_crs(target_crs)
        if not wetlands_gdf.empty and wetlands_gdf.crs != target_crs:
            wetlands_gdf = wetlands_gdf.to_crs(target_crs)
        if not manmade_gdf.empty and manmade_gdf.crs != target_crs:
            manmade_gdf = manmade_gdf.to_crs(target_crs)
        if not points_gdf.empty and points_gdf.crs != target_crs:
            points_gdf = points_gdf.to_crs(target_crs)

        if test_mode:
            # Simple save without zone splitting
            logger.info("TEST MODE: Saving to single GPKG without zone splitting...")

            if not streams_gdf.empty:
                logger.info(f"  Saving {len(streams_gdf):,} streams...")
                streams_gdf.to_file(
                    str(self.output_gpkg), layer="STREAMS", driver="GPKG"
                )

            if not lakes_gdf.empty:
                logger.info(f"  Saving {len(lakes_gdf):,} lakes...")
                lakes_gdf.to_file(str(self.output_gpkg), layer="LAKES", driver="GPKG")

            if not wetlands_gdf.empty:
                logger.info(f"  Saving {len(wetlands_gdf):,} wetlands...")
                wetlands_gdf.to_file(
                    str(self.output_gpkg), layer="WETLANDS", driver="GPKG"
                )

            if not manmade_gdf.empty:
                logger.info(f"  Saving {len(manmade_gdf):,} manmade waterbodies...")
                manmade_gdf.to_file(
                    str(self.output_gpkg), layer="MANMADE", driver="GPKG"
                )

            if not points_gdf.empty:
                logger.info(f"  Saving {len(points_gdf):,} labeled points...")
                points_gdf.to_file(str(self.output_gpkg), layer="POINTS", driver="GPKG")

            logger.info(f"✓ Output saved: {self.output_gpkg}")

        else:
            # Full mode: split by zones
            logger.info("Loading wildlife zones...")
            wildlife = gpd.read_file(str(self.wildlife_gpkg))

            # TODO: Implement zone splitting (for now, just save unsplit)
            logger.info("Zone splitting not yet implemented, saving unsplit data...")

            if not streams_gdf.empty:
                streams_gdf.to_file(
                    str(self.output_gpkg), layer="STREAMS", driver="GPKG"
                )
            if not lakes_gdf.empty:
                lakes_gdf.to_file(str(self.output_gpkg), layer="LAKES", driver="GPKG")

            logger.info(f"✓ Output saved: {self.output_gpkg}")

    def run(self, test_mode=False, build_index=False):
        """Run the complete processing pipeline."""
        start = time.time()

        # Load data
        raw_streams = self.load_streams_raw(test_mode)
        lakes = self.load_lakes()
        wetlands = self.load_wetlands()
        manmade = self.load_manmade()
        points = self.load_kml_points()

        # Enrich KML points (inline logic)
        enriched_points = self.enrich_kml_points(points, lakes, wetlands, manmade)

        del points
        gc.collect()

        # Enrich streams
        enriched_streams = self.enrich_streams(raw_streams, lakes)

        del raw_streams
        gc.collect()

        # Save output
        self.save_to_gpkg(
            enriched_streams, lakes, wetlands, manmade, enriched_points, test_mode
        )

        # Build index if requested
        if build_index:
            logger.info("=" * 80)
            logger.info("PHASE 4: Building Index")
            logger.info("=" * 80)
            builder = IndexBuilder(self.output_gpkg)
            builder.build_index()

        end = time.time()

        # Print summary
        logger.info("=" * 80)
        logger.info("PROCESSING COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Total Time: {(end - start) / 60:.2f} minutes")
        logger.info(f"")
        logger.info(f"Stream Statistics:")
        logger.info(f"  Total streams loaded: {self.stats.total_streams:,}")
        logger.info(f"  Originally named: {self.stats.originally_named:,}")
        logger.info(f"  Inherited same code: {self.stats.inherited_same_code:,}")
        logger.info(f"  River tributaries: {self.stats.river_tributaries:,}")
        logger.info(f"  Lake tributaries: {self.stats.lake_tributaries:,}")
        logger.info(f"  Braids fixed: {self.stats.braids_fixed:,}")
        logger.info(f"  Final named count: {self.stats.final_named_count:,}")
        logger.info(f"")
        logger.info(f"Other Features:")
        logger.info(f"  Lakes: {self.stats.total_lakes:,}")
        logger.info(f"  Wetlands: {self.stats.total_wetlands:,}")
        logger.info(f"  Manmade: {self.stats.total_manmade:,}")
        logger.info(f"  KML Points: {self.stats.total_kml_points:,}")
        logger.info(f"")
        logger.info(f"Output: {self.output_gpkg}")
        logger.info("=" * 80)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Process BC FWA data using simplified watershed hierarchy approach",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode: process only 5 stream layers",
    )
    parser.add_argument(
        "--build-index",
        action="store_true",
        help="Build waterbody index after processing",
    )

    args = parser.parse_args(argv)

    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    base_data = (
        project_root
        / "data"
        / "ftp.geobc.gov.bc.ca"
        / "sections"
        / "outgoing"
        / "bmgs"
        / "FWA_Public"
    )

    streams_gdb = base_data / "FWA_STREAM_NETWORKS_SP" / "FWA_STREAM_NETWORKS_SP.gdb"
    lakes_gdb = base_data / "FWA_BC" / "FWA_BC.gdb"
    wildlife_gpkg = base_data / "WAA_WILDLIFE_MGMT_UNITS_SVW.gpkg"
    kml_path = project_root / "data" / "labelled" / "unnamed_lakes.kml"

    if args.test:
        output_gpkg = script_dir / "output" / "fwa_preprocessing" / "guichon_test.gpkg"
    else:
        output_gpkg = (
            script_dir / "output" / "fwa_preprocessing" / "FWA_Zone_Grouped.gpkg"
        )

    if not streams_gdb.exists():
        print(f"Error: Streams GDB not found at {streams_gdb}")
        return

    processor = FWAProcessor(
        str(streams_gdb),
        str(lakes_gdb),
        str(wildlife_gpkg),
        str(kml_path),
        str(output_gpkg),
    )
    processor.run(test_mode=args.test, build_index=args.build_index)


if __name__ == "__main__":
    main()
