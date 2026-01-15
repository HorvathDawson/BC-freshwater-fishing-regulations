"""
Phase 1: Stream Preprocessing

Goals:
1. Remove unnamed streams >1 level away from named streams
2. Merge braided streams (same watershed code) with named stream names
3. Output cleaned stream GeoPackage

Two-Pass Strategy:
- Pass 1: Load ALL layers metadata-only (no geometries) to build complete network graph
- Pass 2: Load layers with geometries one-at-a-time, filter based on graph, write output

Memory Strategy:
- Pass 1: Metadata only (~500MB for all streams)
- Pass 2: One layer at a time with geometries (~20K features)
- Keep graph in memory throughout (lightweight - codes and names only)
"""

import gc
import fiona
import geopandas as gpd
import pandas as pd
from pathlib import Path
from collections import defaultdict, deque
from typing import Dict, Set, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed
from .utils import clean_watershed_code, get_parent_code, setup_logging
from .models import StreamMetadata

logger = setup_logging(__name__)


def _process_and_write_layer(args):
    """Worker function to process a layer and write to individual GPKG file.

    Args:
        args: Tuple of (streams_gdb, layer_name, graph, distances, code_to_name, output_path)

    Returns:
        Dict with statistics
    """
    streams_gdb, layer_name, graph, distances, code_to_name, output_path = args

    try:
        # Load layer WITH geometries
        streams = gpd.read_file(str(streams_gdb), layer=layer_name)

        if streams.empty:
            return {
                "layer_name": layer_name,
                "count": 0,
                "braids_merged": 0,
                "unnamed_filtered": 0,
                "success": True,
            }

        # Add processing columns
        streams["clean_code"] = streams["FWA_WATERSHED_CODE"].apply(
            clean_watershed_code
        )

        # Track originally named streams
        originally_named = (streams["GNIS_NAME"].notna()) & (
            streams["GNIS_NAME"].str.strip() != ""
        )

        # Map distances from graph
        streams["distance_to_named"] = streams["clean_code"].map(distances)

        # RULE 1: Merge braided streams
        braid_groups = streams.groupby("clean_code")
        braids_merged = 0

        result_rows = []
        for clean_code, group in braid_groups:
            if len(group) == 1:
                result_rows.append(group.iloc[0])
            else:
                # Multiple segments with same code - keep longest or first named
                named_in_group = group[originally_named]
                if not named_in_group.empty:
                    representative = named_in_group.iloc[0]
                else:
                    representative = group.iloc[group.geometry.length.argmax()]

                # Apply braid name if available
                if clean_code in code_to_name:
                    representative = representative.copy()
                    representative["GNIS_NAME"] = code_to_name[clean_code]

                result_rows.append(representative)
                braids_merged += len(group) - 1

        streams = gpd.GeoDataFrame(result_rows, crs=streams.crs)

        # RULE 2: Filter unnamed streams >1 level from named
        unnamed_filtered = 0
        keep_mask = originally_named | (streams["distance_to_named"] <= 1)
        unnamed_filtered = (~keep_mask).sum()
        streams = streams[keep_mask]

        # Drop processing columns
        streams = streams.drop(
            columns=["clean_code", "distance_to_named"], errors="ignore"
        )

        # Write to individual GPKG file (parallel write - no locking!)
        streams.to_file(output_path, driver="GPKG")

        return {
            "layer_name": layer_name,
            "count": len(streams),
            "braids_merged": braids_merged,
            "unnamed_filtered": unnamed_filtered,
            "success": True,
        }

    except Exception as e:
        return {
            "layer_name": layer_name,
            "error": str(e),
            "success": False,
        }


def _process_layer_worker(args):
    """Worker function to process a single layer (read + filter).

    Args:
        args: Tuple of (streams_gdb, layer_name, graph, distances, code_to_name)

    Returns:
        Dict with layer_name, processed GeoDataFrame, and statistics
    """
    streams_gdb, layer_name, graph, distances, code_to_name = args

    try:
        # Load layer WITH geometries
        streams = gpd.read_file(str(streams_gdb), layer=layer_name)

        if streams.empty:
            return {
                "layer_name": layer_name,
                "data": None,
                "count": 0,
                "braids_merged": 0,
                "unnamed_filtered": 0,
                "success": True,
            }

        # Add processing columns
        streams["clean_code"] = streams["FWA_WATERSHED_CODE"].apply(
            clean_watershed_code
        )

        # Track originally named streams
        originally_named = (streams["GNIS_NAME"].notna()) & (
            streams["GNIS_NAME"].str.strip() != ""
        )

        # Map distances from graph
        streams["distance_to_named"] = streams["clean_code"].map(distances)

        # RULE 1: Merge braided streams
        braid_groups = streams.groupby("clean_code")
        braids_merged = 0

        result_rows = []
        for clean_code, group in braid_groups:
            if len(group) == 1:
                result_rows.append(group.iloc[0])
            else:
                # Multiple segments with same code - keep longest or first named
                named_in_group = group[originally_named]
                if not named_in_group.empty:
                    representative = named_in_group.iloc[0]
                else:
                    representative = group.iloc[group.geometry.length.argmax()]

                # Apply braid name if available
                if clean_code in code_to_name:
                    representative = representative.copy()
                    representative["GNIS_NAME"] = code_to_name[clean_code]

                result_rows.append(representative)
                braids_merged += len(group) - 1

        streams = gpd.GeoDataFrame(result_rows, crs=streams.crs)

        # RULE 2: Filter unnamed streams >1 level from named
        unnamed_filtered = 0
        keep_mask = originally_named | (streams["distance_to_named"] <= 1)
        unnamed_filtered = (~keep_mask).sum()
        streams = streams[keep_mask]

        # Drop processing columns
        streams = streams.drop(
            columns=["clean_code", "distance_to_named"], errors="ignore"
        )

        return {
            "layer_name": layer_name,
            "data": streams,
            "count": len(streams),
            "braids_merged": braids_merged,
            "unnamed_filtered": unnamed_filtered,
            "success": True,
        }

    except Exception as e:
        return {
            "layer_name": layer_name,
            "error": str(e),
            "success": False,
        }


def _load_layer_metadata(args):
    """Worker function to load metadata from a single layer.

    Args:
        args: Tuple of (streams_gdb, layer_name)

    Returns:
        Dict with layer statistics and watershed graph data
    """
    streams_gdb, layer_name = args

    try:
        # Load without geometries for speed
        streams = gpd.read_file(
            str(streams_gdb), layer=layer_name, ignore_geometry=True
        )

        layer_graph = {}
        total_streams = 0
        named_streams = 0

        for idx, row in streams.iterrows():
            watershed_code = row.get("FWA_WATERSHED_CODE")
            gnis_name = row.get("GNIS_NAME")
            feature_id = row.get("LINEAR_FEATURE_ID")

            if not isinstance(watershed_code, str) or not feature_id:
                continue

            clean_code = clean_watershed_code(watershed_code)
            if not clean_code:
                continue

            parent_code = get_parent_code(clean_code)
            is_named = pd.notna(gnis_name) and str(gnis_name).strip() != ""

            # Build graph entry
            if clean_code not in layer_graph:
                layer_graph[clean_code] = {
                    "parent_code": parent_code,
                    "has_named": False,
                    "gnis_name": None,
                    "feature_ids": set(),
                    "layer_names": set(),
                }

            layer_graph[clean_code]["feature_ids"].add(feature_id)
            layer_graph[clean_code]["layer_names"].add(layer_name)

            if is_named:
                layer_graph[clean_code]["has_named"] = True
                layer_graph[clean_code]["gnis_name"] = str(gnis_name)

            total_streams += 1
            if is_named:
                named_streams += 1

        del streams

        return {
            "layer_name": layer_name,
            "graph": layer_graph,
            "total_streams": total_streams,
            "named_streams": named_streams,
            "success": True,
        }

    except Exception as e:
        return {
            "layer_name": layer_name,
            "error": str(e),
            "success": False,
        }


class StreamPreprocessor:
    """Preprocesses stream data with memory-efficient batch processing."""

    def __init__(self, streams_gdb: Path, output_gdb: Path, test_mode: bool = False):
        """Initialize preprocessor.

        Args:
            streams_gdb: Path to FWA_STREAM_NETWORKS_SP.gdb
            output_gdb: Path to output GeoPackage for cleaned streams (will use .gpkg extension)
            test_mode: If True, only process first 5 layers
        """
        self.streams_gdb = streams_gdb
        # Change extension to .gpkg
        self.output_gdb = (
            output_gdb.with_suffix(".gpkg")
            if output_gdb.suffix == ".gdb"
            else output_gdb
        )
        self.test_mode = test_mode

        self.stats = {
            "total_streams_read": 0,
            "originally_named": 0,
            "braids_merged": 0,
            "unnamed_filtered": 0,
            "final_count": 0,
            "layers_processed": 0,
        }

    def get_stream_layers(self) -> list:
        """Get list of valid stream layer names from the GDB.

        Returns:
            List of layer names to process
        """
        try:
            layers = fiona.listlayers(str(self.streams_gdb))
            # Keep only layers that look like watershed IDs
            valid_layers = [l for l in layers if not l.startswith("_") and len(l) <= 4]

            if self.test_mode:
                valid_layers = valid_layers[:5]
                logger.info(f"TEST MODE: Processing {len(valid_layers)} layers")

            return valid_layers
        except Exception as e:
            logger.error(f"Failed to list layers: {e}")
            return []

    def build_complete_network_graph(self) -> Dict:
        """PASS 1: Build complete watershed graph from ALL layers (metadata only).

        This loads stream metadata without geometries to build the full network.
        Enables detection of braids and distances across layer boundaries.

        Returns:
            Dict mapping clean_code -> {
                'parent_code': str,
                'has_named': bool,
                'gnis_name': str | None,
                'feature_ids': set,
                'layer_names': set
            }
        """
        logger.info("=== PASS 1: Building Complete Network Graph (Metadata Only) ===")

        layers = self.get_stream_layers()
        total_layers = len(layers)

        graph = defaultdict(
            lambda: {
                "parent_code": None,
                "has_named": False,
                "gnis_name": None,
                "feature_ids": set(),
                "layer_names": set(),
            }
        )

        # Parallelize metadata loading with 8 workers
        num_workers = 8
        logger.info(f"Loading {total_layers} layers using {num_workers} workers...")

        args_list = [(self.streams_gdb, layer_name) for layer_name in layers]
        completed = 0

        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            futures = {
                executor.submit(_load_layer_metadata, args): args[1]
                for args in args_list
            }

            for future in as_completed(futures):
                result = future.result()
                completed += 1

                if completed % 25 == 0 or completed == total_layers:
                    logger.info(
                        f"  Loading metadata: {completed}/{total_layers} layers..."
                    )

                if result["success"]:
                    # Merge layer graph into main graph
                    for clean_code, data in result["graph"].items():
                        graph[clean_code]["parent_code"] = data["parent_code"]
                        graph[clean_code]["feature_ids"].update(data["feature_ids"])
                        graph[clean_code]["layer_names"].update(data["layer_names"])

                        if data["has_named"]:
                            graph[clean_code]["has_named"] = True
                            graph[clean_code]["gnis_name"] = data["gnis_name"]

                    self.stats["total_streams_read"] += result["total_streams"]
                    self.stats["originally_named"] += result["named_streams"]
                else:
                    logger.warning(
                        f"Failed to load metadata from {result['layer_name']}: {result.get('error', 'Unknown error')}"
                    )

        logger.info(f"Graph built: {len(graph)} unique watershed codes")
        logger.info(f"  Total streams: {self.stats['total_streams_read']:,}")
        logger.info(f"  Originally named: {self.stats['originally_named']:,}")

        return dict(graph)

    def calculate_distances_from_named(self, graph: Dict) -> Dict[str, int]:
        """Calculate minimum distance from each code to nearest named stream.

        Uses BFS to traverse the watershed hierarchy with optimized parent->children lookup.

        Args:
            graph: Watershed code graph

        Returns:
            Dict mapping clean_code -> distance_to_named (0 = named, 1 = direct child, etc.)
        """
        distances = {}

        # Build reverse lookup: parent_code -> list of children codes
        # This is much faster than scanning entire graph for each code
        logger.info("  Building parent->children lookup...")
        parent_to_children = defaultdict(list)
        for code, data in graph.items():
            if data["parent_code"]:
                parent_to_children[data["parent_code"]].append(code)

        logger.info(f"  Found {len(parent_to_children)} parent codes with children")

        # Initialize: all named codes have distance 0
        queue = deque()
        for code, data in graph.items():
            if data["has_named"]:
                distances[code] = 0
                queue.append(code)

        logger.info(f"  Starting BFS from {len(queue)} named codes...")

        # BFS upstream from named codes using pre-built lookup
        processed = 0
        while queue:
            current_code = queue.popleft()
            current_distance = distances[current_code]

            processed += 1
            if processed % 100000 == 0:
                logger.info(f"    Processed {processed:,}/{len(graph):,} codes...")

            # Find all children using reverse lookup (much faster!)
            for child_code in parent_to_children.get(current_code, []):
                if child_code not in distances:
                    distances[child_code] = current_distance + 1
                    queue.append(child_code)

        return distances

    def process_layer(
        self, layer_name: str, graph: Dict, distances: Dict, code_to_name: Dict
    ) -> Optional[gpd.GeoDataFrame]:
        """PASS 2: Process a single stream layer with geometries.

        Uses pre-computed graph to filter and apply braid names.

        Args:
            layer_name: Name of layer in GDB
            graph: Complete watershed graph from pass 1
            distances: Pre-computed distances to named streams
            code_to_name: Mapping of clean_code -> braid_name

        Returns:
            Processed GeoDataFrame, or None if failed
        """
        try:
            # Load layer WITH geometries
            streams = gpd.read_file(str(self.streams_gdb), layer=layer_name)

            if streams.empty:
                return None

            # Add processing columns
            streams["clean_code"] = streams["FWA_WATERSHED_CODE"].apply(
                clean_watershed_code
            )

            # Track originally named streams
            originally_named = (streams["GNIS_NAME"].notna()) & (
                streams["GNIS_NAME"].str.strip() != ""
            )

            # Map distances from graph
            streams["distance_to_named"] = streams["clean_code"].map(distances)

            # RULE 1: Merge braided streams
            unnamed_mask = ~originally_named
            inherited_names = streams.loc[unnamed_mask, "clean_code"].map(code_to_name)
            braids_mask = unnamed_mask & inherited_names.notna()

            if braids_mask.any():
                streams.loc[braids_mask, "GNIS_NAME"] = inherited_names[braids_mask]
                self.stats["braids_merged"] += braids_mask.sum()

            # RULE 2: Filter unnamed streams >1 level from named
            keep_mask = (
                originally_named  # Originally named
                | braids_mask  # Got name from braid merging
                | (streams["distance_to_named"] <= 1)  # Within 1 level
            )

            filtered = streams[keep_mask].copy()
            filtered_count = len(streams) - len(filtered)

            if filtered_count > 0:
                self.stats["unnamed_filtered"] += filtered_count

            # Clean up temp columns
            filtered = filtered.drop(
                columns=["clean_code", "distance_to_named"], errors="ignore"
            )

            return filtered

        except Exception as e:
            logger.error(f"Error processing layer {layer_name}: {e}")
            return None

    def run(self) -> Path:
        """Execute two-pass stream preprocessing.

        Returns:
            Path to output GDB
        """
        logger.info("=== Phase 1: Stream Preprocessing (Two-Pass) ===")

        # Create output directory
        self.output_gdb.parent.mkdir(parents=True, exist_ok=True)

        # Remove old output if exists
        if self.output_gdb.exists():
            import os

            os.remove(self.output_gdb)
            logger.info(f"Removed old output: {self.output_gdb}")

        # PASS 1: Build complete network graph (metadata only, ALL layers)
        graph = self.build_complete_network_graph()

        # Calculate distances for all codes
        logger.info("Calculating distances to named streams...")
        distances = self.calculate_distances_from_named(graph)
        logger.info(f"  Calculated distances for {len(distances)} codes")

        # Build braid name mapping
        logger.info("Building braid name mapping...")
        code_to_name = {}
        for code, data in graph.items():
            if data["has_named"] and data["gnis_name"]:
                code_to_name[code] = data["gnis_name"]
        logger.info(f"  Found {len(code_to_name)} named watershed codes")

        # PASS 2: Process layers with geometries in parallel batches
        logger.info("\n=== PASS 2: Filtering and Writing Streams ===")
        layers = self.get_stream_layers()
        total_layers = len(layers)
        num_workers = 4  # Reduced from 16 to avoid memory exhaustion
        logger.info(
            f"Processing and writing {total_layers} stream layers with {num_workers} workers (parallel writes)..."
        )

        # Process AND write in parallel - each layer to its own temp GPKG
        # This avoids SQLite locking issues

        # Create temp directory for individual layer files
        temp_dir = self.output_gdb.parent / "temp_layers"
        temp_dir.mkdir(exist_ok=True)

        # Process all layers in parallel batches
        args_list = [
            (
                self.streams_gdb,
                layer_name,
                graph,
                distances,
                code_to_name,
                str(temp_dir / f"{layer_name}.gpkg"),
            )
            for layer_name in layers
        ]

        completed = 0
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            futures = {
                executor.submit(_process_and_write_layer, args): args[1]
                for args in args_list
            }

            for future in as_completed(futures):
                result = future.result()
                completed += 1

                if result["success"]:
                    self.stats["final_count"] += result["count"]
                    self.stats["braids_merged"] += result["braids_merged"]
                    self.stats["unnamed_filtered"] += result["unnamed_filtered"]
                    self.stats["layers_processed"] += 1
                else:
                    logger.warning(
                        f"Failed to process {result['layer_name']}: {result.get('error', 'Unknown error')}"
                    )

                # Progress logging every 40 layers
                if completed % 40 == 0 or completed == total_layers:
                    percent = (completed / total_layers) * 100
                    logger.info(
                        f"  [{completed}/{total_layers}] ({percent:.1f}%) - {self.stats['final_count']:,} streams written"
                    )

        # Combine all temp GPKG files into final output
        logger.info("Combining layers into final GeoPackage...")
        for layer_name in layers:
            temp_file = temp_dir / f"{layer_name}.gpkg"
            if temp_file.exists():
                try:
                    layer_data = gpd.read_file(str(temp_file))
                    if not layer_data.empty:
                        layer_data.to_file(
                            str(self.output_gdb), layer=layer_name, driver="GPKG"
                        )
                except Exception as e:
                    logger.warning(f"Failed to combine {layer_name}: {e}")

        # Clean up temp directory
        import shutil

        shutil.rmtree(temp_dir)
        logger.info("Cleaned up temporary files")

        # Final stats
        logger.info("=== Stream Preprocessing Complete ===")
        logger.info(f"  Total streams read: {self.stats['total_streams_read']:,}")
        logger.info(f"  Originally named: {self.stats['originally_named']:,}")
        logger.info(f"  Braids merged: {self.stats['braids_merged']:,}")
        logger.info(f"  Unnamed filtered: {self.stats['unnamed_filtered']:,}")
        logger.info(f"  Final count: {self.stats['final_count']:,}")
        logger.info(f"  Layers processed: {self.stats['layers_processed']}")
        logger.info(f"  Output: {self.output_gdb}")

        return self.output_gdb
