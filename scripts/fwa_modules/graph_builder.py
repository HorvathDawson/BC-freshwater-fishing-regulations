#!/usr/bin/env python3
"""
FWA Graph Builder - igraph Implementation
Stream Network Graph Construction with Tributary Enrichment

This is an igraph port of graph_builder.py for improved performance.
Builds a primal graph representation of BC Freshwater Atlas (FWA) stream network.
"""
import os, sys, logging, warnings, argparse, gc, json


# Suppress GDAL warnings about missing DXF driver
class GDALFilter(logging.Filter):
    def filter(self, record):
        return "header.dxf" not in record.getMessage()


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
for handler in logging.root.handlers:
    handler.addFilter(GDALFilter())

os.environ["GDAL_SKIP"] = "DXF"

from multiprocessing import Pool
from collections import deque
import geopandas as gpd
import pandas as pd
import igraph as ig
import fiona
from pathlib import Path
from shapely.geometry import LineString, MultiLineString, shape

warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)

# Feature Code Lookup Table
FEATURE_CODE_DESCRIPTIONS = {
    "AP09200000": "Dump",
    "AP90300100": "Mine - Tailing Pond",
    "EA26700110": "Settling Basin - Sewage",
    "FA02650000": "Boundary (International)",
    "GA03950000": "Canal",
    "GA08800110": "Ditch",
    "GA24850000": "River/Stream - Definite",
    "GA24850140": "River/Stream - Indefinite",
    "GA24850150": "River/Stream - Intermittent",
    "GB11350110": "Flooded Land - Inundated",
    "GB15300000": "Lake - Definite",
    "GB15300130": "Lake - Indefinite",
    "GB15300140": "Lake - Intermittent",
    "GB24300000": "Reservoir - Definite",
    "GB90100000": "Reservoir - Indefinite",
    "GB90100110": "Reservoir - Intermittent",
    "GC17100000": "Marsh",
    "GC30050000": "Swamp",
    "GE14850000": "Island - Definite",
    "GG05800000": "Coastline - Definite",
    "WA11410000": "Flow Connectors - Inferred",
    "WA17100000": "Frequently Flooded Land",
    "WA21100111": "Construction Line - Coastline",
    "WA23111110": "Construction Line - Lakeshore",
    "WA24111110": "Construction Line - Main Flow",
    "WA24111111": "Construction Line - Lake Arm",
    "WA24111120": "Construction Line - Main Connector",
    "WA24111130": "Construction Line - Secondary Flow",
    "WA24111140": "Construction Line - Segment Delimiter",
    "WA24111150": "Construction Line - Secondary",
    "WA24111160": "Construction Line - River Delimiter",
    "WA24111170": "Construction Line - Flow Inferred",
    "WA24111180": "Construction Line - Subsurface Flow",
    "WA24111190": "Construction Line - Flow Connector",
    "WA24200110": "Double-Line Blueline - Right Bank",
    "WA24200120": "Double-Line Blueline - Right",
    "WA24200130": "Double-Line Blueline - Left Bank",
    "WA24200140": "Double-Line Blueline - Left",
    "WA24220110": "Island in River - Right Bank",
    "WA24220120": "Island in River - Right Bank Shared with Wetland",
    "WA24220130": "Island in River - Left Bank",
    "WA24220140": "Island in River - Left Bank Shared with Wetland",
    "WA25100110": "Watershed Boundary - Major",
    "WA25100120": "Watershed Boundary - Minor",
    "WA25100140": "Watershed Boundary - (att) operator modified or added HOL",
    "GA08450110": "Dam - Beaver",
    "GA23500110": "Rapids",
    "GA90002110": "Falls",
    "GA98450000": "Dam",
    "HB27550000": "Sinkhole",
    "GA10450200": "Artificial Waterfall",
    "GA10450300": "Flattened Waterfall",
}


def get_endpoints_helper(geom):
    """Helper function for extracting endpoints from geometry."""
    if isinstance(geom, LineString):
        coords = geom.coords
    elif isinstance(geom, MultiLineString) and len(geom.geoms) > 0:
        coords = [geom.geoms[0].coords[0], geom.geoms[-1].coords[-1]]
    else:
        return None, None, None, None
    x1, y1 = round(coords[0][0], 3), round(coords[0][1], 3)
    x2, y2 = round(coords[-1][0], 3), round(coords[-1][1], 3)
    u = f"{x1}_{y1}"
    v = f"{x2}_{y2}"
    return u, v, (x1, y1), (x2, y2)


def clean_code_helper(fwa_code):
    """Helper function for cleaning FWA codes."""
    if not fwa_code:
        return ""
    return "-".join([p for p in fwa_code.split("-") if p != "000000"])


def process_layer_worker(args):
    """
    Worker function to process a single layer in parallel.
    Returns nodes and edges data to be merged by main thread.
    """
    layer, streams_gdb_path, lake_lookup = args
    nodes_data = []
    edges_data = []
    try:
        # Use fiona to stream features from this layer
        with fiona.open(streams_gdb_path, layer=layer) as src:
            for feature in src:
                props = feature["properties"]
                geom_data = feature["geometry"]
                if not geom_data:
                    continue

                # Filter bad watershed codes
                code = props.get("FWA_WATERSHED_CODE")
                if code and code.startswith("999-999999"):
                    continue

                # Extract GNIS name and ID as-is (propagation happens after preprocessing)
                name = props.get("GNIS_NAME")
                gnis_id = props.get("GNIS_ID")
                # Parse geometry
                try:
                    geom = shape(geom_data)
                except:
                    continue
                if geom.is_empty:
                    continue
                # Extract attributes
                lf_id = props.get("LINEAR_FEATURE_ID")
                wb = props.get("WATERBODY_KEY")
                stream_order = props.get("STREAM_ORDER")
                feature_code = props.get("FEATURE_CODE", "")
                # Get endpoints
                u, v, u_coord, v_coord = get_endpoints_helper(geom)
                if not u:
                    continue
                # Prepare node data
                nodes_data.append((u, u_coord[0], u_coord[1]))
                nodes_data.append((v, v_coord[0], v_coord[1]))
                # Prepare edge attributes
                sid = str(int(lf_id)) if lf_id else f"UNK-{layer}"
                s_code = str(code) if code else ""
                waterbody_key = ""
                lake_name_val = ""
                if wb and int(wb) != 0:
                    waterbody_key = str(int(wb))
                    k = int(wb)
                    if k in lake_lookup:
                        lake_name_val = lake_lookup[k]

                gnis_name = name if name else ""
                gnis_id_str = str(int(gnis_id)) if gnis_id else ""
                # Store edge data
                edge_data = {
                    "v": v,
                    "u": u,
                    "sid": sid,
                    "fwa_watershed_code": s_code,
                    "gnis_name": gnis_name,
                    "gnis_id": gnis_id_str,
                    "waterbody_key": waterbody_key,
                    "lake_name": lake_name_val,
                    "stream_order": int(stream_order) if stream_order else None,
                    "length": geom.length,
                    "feature_code": feature_code,
                }
                edges_data.append(edge_data)
    except Exception as e:
        # Silently skip problematic layers
        pass
    return nodes_data, edges_data


class FWAPrimalGraphIGraph:
    def __init__(self):
        self.script_dir = Path(__file__).resolve().parent
        self.project_root = self.script_dir.parent.parent
        self.base_data = (
            self.project_root
            / "data/ftp.geobc.gov.bc.ca/sections/outgoing/bmgs/FWA_Public"
        )
        self.streams_gdb = (
            self.base_data / "FWA_STREAM_NETWORKS_SP/FWA_STREAM_NETWORKS_SP.gdb"
        )
        self.lakes_gdb = self.base_data / "FWA_BC/FWA_BC.gdb"
        # Output directory for graph exports
        self.output_dir = self.script_dir.parent / "output" / "fwa_modules"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # igraph uses integer vertex IDs, so we need mapping
        self.G = ig.Graph(directed=True)
        self.node_id_to_index = {}  # Maps string node IDs to integer indices
        self.index_to_node_id = {}  # Maps integer indices to string node IDs
        self.lake_lookup = {}
        self.edge_counts = {}  # Track parallel edges for visual offset
        self.layers_used = None  # Track which layers were processed

    def validate_paths(self):
        if not self.streams_gdb.exists():
            logger.error(f"Streams GDB not found: {self.streams_gdb}")
            return False
        return True

    def load_from_pickle(self, pickle_path):
        """Load graph from existing pickle file."""
        import pickle

        logger.info(f"Loading graph from pickle: {pickle_path}...")
        with open(pickle_path, "rb") as f:
            data = pickle.load(f)
            self.G = data["graph"]
            self.node_id_to_index = data["node_id_to_index"]
            self.index_to_node_id = data["index_to_node_id"]
        logger.info(
            f"Loaded graph: {self.G.vcount():,} Nodes, {self.G.ecount():,} Edges"
        )
        return True

    def load_lakes(self):
        if not self.lakes_gdb.exists():
            return
        try:
            df = gpd.read_file(
                str(self.lakes_gdb), layer="FWA_LAKES_POLY", ignore_geometry=True
            )
            valid = df.dropna(subset=["WATERBODY_KEY", "GNIS_NAME_1"])
            self.lake_lookup = pd.Series(
                valid.GNIS_NAME_1.values, index=valid.WATERBODY_KEY.astype(int)
            ).to_dict()
            logger.info(f"Loaded {len(self.lake_lookup)} lake keys.")
        except Exception:
            pass

    def _get_or_create_vertex(self, node_id, x, y):
        """Get vertex index for node_id, creating if it doesn't exist."""
        if node_id in self.node_id_to_index:
            return self.node_id_to_index[node_id]

        # Create new vertex
        idx = self.G.vcount()
        self.G.add_vertex()
        self.node_id_to_index[node_id] = idx
        self.index_to_node_id[idx] = node_id

        # Set attributes
        self.G.vs[idx]["name"] = node_id
        self.G.vs[idx]["x"] = x
        self.G.vs[idx]["y"] = y
        self.G.vs[idx]["size"] = 0.1
        self.G.vs[idx]["label"] = ""

        return idx

    def build(self, limit=None, layers=None):
        import time

        start_time = time.time()

        logger.info("=" * 80)
        logger.info("STEP 1: BUILDING GRAPH FROM FWA STREAM NETWORK")
        logger.info("=" * 80)

        try:
            if layers is None:
                logger.info("Discovering layers in GDB...")
                layers = fiona.listlayers(str(self.streams_gdb))
                layers = [l for l in layers if not l.startswith("_") and len(l) <= 4]
                logger.info(f"  Found {len(layers)} valid layers")
                if limit:
                    layers = layers[:limit]
                    logger.info(f"  Limited to first {limit} layers")
            elif isinstance(layers, str):
                layers = [layers]
            # Store layers for filename generation
            self.layers_used = layers
        except Exception:
            return

        logger.info(f"\nProcessing {len(layers)} layers: {', '.join(layers)}")
        logger.info(f"Parallel processing with 14 workers...\n")
        # Prepare arguments for workers
        worker_args = [
            (layer, str(self.streams_gdb), self.lake_lookup) for layer in layers
        ]

        # Process layers in parallel
        all_nodes_data = []
        all_edges_data = []
        logger.info("Phase 1.1: Extracting features from layers in parallel...")
        with Pool(processes=14) as pool:
            results = pool.map(process_layer_worker, worker_args)
            # Collect results and clean memory periodically
            for i, (nodes_data, edges_data) in enumerate(results):
                all_nodes_data.extend(nodes_data)
                all_edges_data.extend(edges_data)

                # Progress update every 10 layers
                if (i + 1) % 10 == 0 or (i + 1) == len(results):
                    progress = ((i + 1) / len(results)) * 100
                    logger.info(
                        f"  Processed {i+1}/{len(results)} layers ({progress:.1f}%) - {len(all_edges_data):,} edges collected"
                    )

                # Periodic cleanup
                if i % 50 == 0 and i > 0:
                    gc.collect()

        logger.info(
            f"\nPhase 1.2: Merging {len(all_nodes_data):,} node records and {len(all_edges_data):,} edge records into graph..."
        )
        # Add all unique nodes
        logger.info("  Deduplicating nodes...")
        unique_nodes = {}
        for node_id, x, y in all_nodes_data:
            if node_id not in unique_nodes:
                unique_nodes[node_id] = (x, y)

        logger.info(f"  Creating {len(unique_nodes):,} unique vertices...")
        for node_id, (x, y) in unique_nodes.items():
            self._get_or_create_vertex(node_id, x, y)

        del unique_nodes
        del all_nodes_data
        gc.collect()

        # Add all edges with parallel edge counting
        edge_list = []  # (source_idx, target_idx)
        edge_attrs = {
            "linear_feature_id": [],
            "fwa_watershed_code": [],
            "fwa_watershed_code_clean": [],
            "gnis_name": [],
            "gnis_id": [],
            "waterbody_key": [],
            "lake_name": [],
            "stream_order": [],
            "length": [],
            "feature_code": [],
            # Tributary fields removed - use graph traversal instead
            # "stream_tributary_of": [],
            # "lake_tributary_of": [],
            "weight": [],
            "edge_index": [],
        }

        for edge_data in all_edges_data:
            v_id = edge_data["v"]
            u_id = edge_data["u"]

            # Get vertex indices (nodes should already exist)
            v_idx = self.node_id_to_index[v_id]
            u_idx = self.node_id_to_index[u_id]

            # Count parallel edges for visual offset
            edge_key_pair = (v_id, u_id)
            parallel_count = self.edge_counts.get(edge_key_pair, 0)
            self.edge_counts[edge_key_pair] = parallel_count + 1
            visual_offset = parallel_count * 0.02

            # Add edge to list
            edge_list.append((v_idx, u_idx))

            # Store attributes
            edge_attrs["linear_feature_id"].append(edge_data["sid"])
            edge_attrs["fwa_watershed_code"].append(edge_data["fwa_watershed_code"])
            edge_attrs["fwa_watershed_code_clean"].append(
                clean_code_helper(edge_data["fwa_watershed_code"])
            )
            edge_attrs["gnis_name"].append(edge_data["gnis_name"])
            edge_attrs["gnis_id"].append(edge_data["gnis_id"])
            edge_attrs["waterbody_key"].append(edge_data["waterbody_key"])
            edge_attrs["lake_name"].append(edge_data["lake_name"])
            edge_attrs["stream_order"].append(edge_data["stream_order"])
            edge_attrs["length"].append(edge_data["length"])
            edge_attrs["feature_code"].append(edge_data["feature_code"])
            # Tributary fields removed - use graph traversal instead
            # edge_attrs["stream_tributary_of"].append("")
            # edge_attrs["lake_tributary_of"].append("")
            edge_attrs["weight"].append(1.0 + visual_offset)
            edge_attrs["edge_index"].append(parallel_count)

        # Add all edges at once (more efficient)
        logger.info(f"  Adding {len(edge_list):,} edges to graph...")
        self.G.add_edges(edge_list)

        # Set edge attributes
        logger.info(f"  Setting edge attributes ({len(edge_attrs)} attribute types)...")
        for attr_name, attr_values in edge_attrs.items():
            self.G.es[attr_name] = attr_values

        del all_edges_data
        del edge_list
        del edge_attrs
        gc.collect()

        elapsed = time.time() - start_time
        logger.info(f"\n{'='*80}")
        logger.info(
            f"GRAPH BUILD COMPLETE: {self.G.vcount():,} nodes, {self.G.ecount():,} edges"
        )
        logger.info(f"Build time: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
        logger.info(f"{'='*80}\n")

    def preprocess_graph(self):
        """
        Clean up spurious edges before enrichment.
        Remove edges with STREAM_ORDER == 1 that lead to roots (apparent tailwaters).
        Iterates until no more such edges are found (handles cascading effects).
        """
        import time

        start_time = time.time()

        logger.info("=" * 80)
        logger.info("STEP 1.5: PREPROCESSING - REMOVING SPURIOUS EDGES")
        logger.info("=" * 80)
        logger.info("Removing stream_order=1 edges leading to tailwater roots...\n")
        logger.info(
            "NOTE: Preserving coastal streams (watershed codes starting with 9)\n"
        )

        total_edges_removed = 0
        iteration = 0
        # Track statistics about removed edges (first iteration only)
        removed_stats = {
            "watershed_prefixes": {},
            "named_count": 0,
            "unnamed_count": 0,
            "lake_edges": 0,
            "total_length": 0.0,
            "feature_codes": {},
            "removed_edges": [],  # Store details of all removed spurious edges
        }

        while True:
            iteration += 1
            # Find all roots (vertices with out_degree == 0, appearing as tailwater)
            roots = [v.index for v in self.G.vs if v.outdegree() == 0]
            logger.info(
                f"Iteration {iteration}: Found {len(roots)} root nodes (out-degree 0)."
            )

            edges_to_remove = []
            order_counts = {}
            coastal_preserved = 0
            coastal_preserved_stats = {
                "named": 0,
                "unnamed": 0,
                "watershed_prefixes": {},
            }

            # Check edges leading to these roots
            for root in roots:
                for edge in self.G.es.select(_target=root):
                    stream_order = edge["stream_order"]
                    # Track stream order distribution
                    order_counts[stream_order] = order_counts.get(stream_order, 0) + 1
                    # If this edge has stream order 1, it's likely spurious
                    if stream_order == 1:
                        watershed_code = edge["fwa_watershed_code"]
                        gnis_name = edge["gnis_name"]

                        # Preserve coastal streams (watershed codes starting with 9)
                        if watershed_code and watershed_code.startswith("9"):
                            coastal_preserved += 1
                            # Track coastal preservation stats (first iteration only)
                            if iteration == 1:
                                if gnis_name:
                                    coastal_preserved_stats["named"] += 1
                                else:
                                    coastal_preserved_stats["unnamed"] += 1
                                # Track watershed prefix (first 3 digits)
                                prefix = (
                                    watershed_code.split("-")[0]
                                    if watershed_code
                                    else "unknown"
                                )
                                coastal_preserved_stats["watershed_prefixes"][
                                    prefix
                                ] = (
                                    coastal_preserved_stats["watershed_prefixes"].get(
                                        prefix, 0
                                    )
                                    + 1
                                )
                            continue

                        # Preserve named streams (have GNIS name from source data)
                        if gnis_name:
                            continue

                        edges_to_remove.append(edge.index)

                        # Collect statistics (first iteration only)
                        if iteration == 1:
                            # Track watershed code prefix
                            prefix = (
                                watershed_code.split("-")[0]
                                if watershed_code
                                else "unknown"
                            )
                            removed_stats["watershed_prefixes"][prefix] = (
                                removed_stats["watershed_prefixes"].get(prefix, 0) + 1
                            )

                            # Track named vs unnamed
                            if gnis_name:
                                removed_stats["named_count"] += 1
                            else:
                                removed_stats["unnamed_count"] += 1

                            # Store details for all removed spurious edges
                            removed_stats["removed_edges"].append(
                                {
                                    "linear_feature_id": edge["linear_feature_id"],
                                    "gnis_name": gnis_name if gnis_name else "",
                                    "watershed_code": edge["fwa_watershed_code"],
                                    "feature_code": edge["feature_code"],
                                    "stream_order": edge["stream_order"],
                                    "length": edge["length"],
                                }
                            )

                            # Track lake edges
                            if edge["lake_name"]:
                                removed_stats["lake_edges"] += 1

                            # Track total length
                            removed_stats["total_length"] += edge["length"]

                            # Track feature codes
                            fc = (
                                edge["feature_code"]
                                if edge["feature_code"]
                                else "unknown"
                            )
                            removed_stats["feature_codes"][fc] = (
                                removed_stats["feature_codes"].get(fc, 0) + 1
                            )

            if iteration == 1:
                logger.info(
                    f"Stream order distribution of edges to roots: {dict(sorted(order_counts.items()))}"
                )
                if coastal_preserved > 0:
                    logger.info(
                        f"\nPreserved {coastal_preserved} coastal stream edges (9xx watershed codes):"
                    )
                    logger.info(f"  Named: {coastal_preserved_stats['named']}")
                    logger.info(f"  Unnamed: {coastal_preserved_stats['unnamed']}")
                    logger.info(
                        f"  Watershed prefixes: {dict(sorted(coastal_preserved_stats['watershed_prefixes'].items()))}"
                    )

            del order_counts

            # If no edges to remove, we're done
            if not edges_to_remove:
                logger.info(
                    f"No more order-1 edges to remove after {iteration} iteration(s)."
                )
                break

            # Remove the spurious edges
            num_edges_removed = len(edges_to_remove)
            self.G.delete_edges(edges_to_remove)

            total_edges_removed += num_edges_removed
            logger.info(
                f"Iteration {iteration}: Removed {num_edges_removed} order-1 edges."
            )

            del edges_to_remove
            gc.collect()

        # Remove isolated vertices
        isolated = [v.index for v in self.G.vs if v.degree() == 0]
        num_isolated = len(isolated)
        if isolated:
            # Update mappings before deletion
            for idx in sorted(isolated, reverse=True):
                node_id = self.index_to_node_id[idx]
                del self.node_id_to_index[node_id]
                del self.index_to_node_id[idx]

            self.G.delete_vertices(isolated)

            # Rebuild mappings after vertex deletion (indices shift)
            self.node_id_to_index = {}
            self.index_to_node_id = {}
            for v in self.G.vs:
                node_id = v["name"]
                self.node_id_to_index[node_id] = v.index
                self.index_to_node_id[v.index] = node_id

        del isolated
        gc.collect()

        elapsed = time.time() - start_time
        logger.info(f"\n{'='*80}")
        logger.info("PREPROCESSING COMPLETE")
        logger.info(f"  Removed: {total_edges_removed:,} spurious edges")
        logger.info(f"  Removed: {num_isolated:,} isolated nodes")
        logger.info(f"  Result: {self.G.vcount():,} nodes, {self.G.ecount():,} edges")
        logger.info(f"  Time: {elapsed:.1f}s")

        # Log detailed statistics about removed edges
        if total_edges_removed > 0:
            logger.info(f"\n  Removed Edge Statistics:")
            logger.info(f"    Named streams: {removed_stats['named_count']:,}")
            logger.info(f"    Unnamed streams: {removed_stats['unnamed_count']:,}")
            logger.info(f"    Lake edges: {removed_stats['lake_edges']:,}")
            logger.info(f"    Total length: {removed_stats['total_length']:,.1f}m")
            logger.info(
                f"    Watershed code prefixes: {dict(sorted(removed_stats['watershed_prefixes'].items()))}"
            )

            # Display feature code breakdown with descriptions
            logger.info(f"\n  Feature Type Breakdown:")
            for fc, count in sorted(
                removed_stats["feature_codes"].items(), key=lambda x: x[1], reverse=True
            ):
                desc = FEATURE_CODE_DESCRIPTIONS.get(fc, "Unknown")
                logger.info(f"    {fc}: {count:,} ({desc})")

            # Save all removed spurious edges to file
            if removed_stats["removed_edges"]:
                import json

                output_file = os.path.join(
                    os.path.dirname(__file__),
                    "..",
                    "output",
                    "fwa_modules",
                    "removed_spurious_edges.json",
                )
                with open(output_file, "w") as f:
                    json.dump(removed_stats["removed_edges"], f, indent=2)
                logger.info(
                    f"\n  Saved {len(removed_stats['removed_edges'])} removed spurious edges to: removed_spurious_edges.json"
                )

        logger.info(f"{'='*80}\n")
        gc.collect()

    def propagate_names_by_watershed(self):
        """
        Propagate GNIS names to all edges with the same watershed code.
        After spurious edge removal, ensures all segments of the same watershed
        share the same name if any segment has a name.

        NOTE: This approach of renaming all segments with the same watershed code
        may cause issues with regulation mapping. When stream segments have the same
        watershed code but different names, it becomes unclear whether they should be
        treated as tributaries or the mainstem for regulation purposes.

        A better approach might be to preserve the different names, allowing segments
        to be recognized as BOTH tributaries AND the mainstem simultaneously. This would
        provide more flexibility for regulation matching.

        Example: Squamish Powerhouse Channel has segments with the same watershed code
        as Squamish River but with a distinct name. Renaming them to "Squamish River"
        loses the ability to match regulations specific to the powerhouse channel.
        """
        import time

        start_time = time.time()

        logger.info("=" * 80)
        logger.info("STEP 1.7: PROPAGATING NAMES BY WATERSHED CODE")
        logger.info("=" * 80)
        logger.info("Applying names to all segments with same watershed code...\n")

        # Build mapping of watershed_code -> name
        code_to_name = {}
        for edge in self.G.es:
            watershed_code = edge["fwa_watershed_code"]
            gnis_name = edge["gnis_name"]
            if watershed_code and gnis_name:
                if watershed_code not in code_to_name:
                    code_to_name[watershed_code] = gnis_name

        logger.info(f"Found {len(code_to_name):,} unique named watersheds")

        # Apply names to edges with same watershed code
        updated_count = 0
        for edge in self.G.es:
            watershed_code = edge["fwa_watershed_code"]
            if watershed_code and not edge["gnis_name"]:
                if watershed_code in code_to_name:
                    edge["gnis_name"] = code_to_name[watershed_code]
                    updated_count += 1

        elapsed = time.time() - start_time
        logger.info(f"\n{'='*80}")
        logger.info("NAME PROPAGATION COMPLETE")
        logger.info(f"  Updated: {updated_count:,} edges with propagated names")
        logger.info(f"  Time: {elapsed:.1f}s")
        logger.info(f"{'='*80}\n")
        gc.collect()

    def filter_unnamed_depth(self, threshold=2):
        """
        Remove unnamed stream segments that are N or more stream systems away
        from the nearest named waterbody. Also removes all upstream segments.
        Stores distance values in edge attributes 'unnamed_depth_distance_raw' and 'unnamed_depth_distance_corrected'.
        """
        import time

        start_time = time.time()

        logger.info("=" * 80)
        logger.info(f"STEP 1.6: FILTERING UNNAMED STREAMS (threshold={threshold})")
        logger.info("=" * 80)
        logger.info(
            f"Initial graph: {self.G.vcount():,} nodes, {self.G.ecount():,} edges\n"
        )

        # Phase 1: Collect named stream starting points
        logger.info("[PHASE 1/6] Collecting named stream anchors...")
        named_edges = []
        for edge in self.G.es:
            if edge["gnis_name"]:
                named_edges.append(edge.index)
        logger.info(f"Found {len(named_edges):,} named stream edges as anchors.")

        if not named_edges:
            logger.warning("No named streams found. Skipping unnamed depth filtering.")
            return

        # Phase 2: BFS upstream from named streams
        phase2_start = time.time()
        logger.info(
            "\n[PHASE 2/6] Measuring distances from named streams (BFS upstream)..."
        )

        # Pre-compute edge data cache for O(1) lookups
        logger.info("  Building edge data cache...")
        edge_data_cache = {}
        for edge in self.G.es:
            edge_data_cache[edge.index] = {
                "fwa": edge["fwa_watershed_code"],
                "name": edge["gnis_name"],
                "source": edge.source,
                "target": edge.target,
            }

        # Pre-compute predecessor cache for O(1) lookups
        logger.info("Pre-computing predecessor cache...")
        pred_cache = {}
        for v in self.G.vs:
            pred_cache[v.index] = [e.source for e in self.G.es.select(_target=v.index)]

        edge_distance = {}  # edge_index -> min_distance
        queue = deque()

        # Initialize: Add all named stream edges with distance=0
        for edge_idx in named_edges:
            edge_distance[edge_idx] = 0
            edge_info = edge_data_cache[edge_idx]
            queue.append((edge_info["source"], edge_info["fwa"], 0))

        del named_edges
        gc.collect()

        # Track visited nodes to avoid reprocessing
        visited_nodes = set()
        iteration = 0

        # BFS upstream - only visit each node once
        while queue:
            current_node, prev_fwa_code, prev_distance = queue.popleft()

            # Skip if already processed this node
            if current_node in visited_nodes:
                continue
            visited_nodes.add(current_node)
            iteration += 1

            # Look at all edges flowing INTO current_node (predecessors)
            for pred_u in pred_cache.get(current_node, []):
                # Find edges from pred_u to current_node
                for edge in self.G.es.select(_source=pred_u, _target=current_node):
                    edge_idx = edge.index
                    edge_info = edge_data_cache[edge_idx]
                    edge_fwa = edge_info["fwa"]
                    edge_name = edge_info["name"]

                    # Calculate distance for this edge
                    if edge_name:  # Named stream
                        new_distance = 0
                    elif edge_fwa == prev_fwa_code:  # Same stream
                        new_distance = prev_distance
                    else:  # Different unnamed stream
                        new_distance = prev_distance + 1

                    # Only process if better distance or first visit
                    if (
                        edge_idx not in edge_distance
                        or new_distance < edge_distance[edge_idx]
                    ):
                        edge_distance[edge_idx] = new_distance
                        # Only add to queue if not already visited
                        if pred_u not in visited_nodes:
                            queue.append((pred_u, edge_fwa, new_distance))

            # Progress logging and memory cleanup
            if iteration % 50000 == 0 and iteration > 0:
                logger.info(
                    f"    BFS progress: {iteration:,} traversals, {len(edge_distance):,} edges measured"
                )
                gc.collect()

        phase2_elapsed = time.time() - phase2_start
        logger.info(
            f"  ✓ BFS complete: {iteration:,} traversals, {len(edge_distance):,} edges measured ({phase2_elapsed:.1f}s)"
        )

        # Phase 2.5: Correct distances to ensure network consistency
        phase25_start = time.time()
        logger.info(
            "\n[PHASE 2.5/6] Correcting distances to ensure network consistency..."
        )
        corrected_distance = {}

        # Initialize all edges with their BFS distance
        for edge_idx, dist in edge_distance.items():
            corrected_distance[edge_idx] = dist

        # Single-pass correction: BFS from source nodes, propagating minimum distances
        sources = [v.index for v in self.G.vs if v.indegree() == 0]
        logger.info(
            f"  Starting downstream propagation from {len(sources):,} headwaters..."
        )

        # Track how many upstream edges have been processed for each node
        upstream_processed = {v.index: 0 for v in self.G.vs}
        queue = deque(sources)
        processed_nodes = set(sources)
        total_corrections = 0
        edges_processed = 0

        while queue:
            node = queue.popleft()

            # Process all edges flowing out of this node
            for edge in self.G.es.select(_source=node):
                edge_idx = edge.index
                next_node = edge.target
                edges_processed += 1

                if edge_idx not in corrected_distance:
                    continue

                # Find minimum distance among all upstream edges feeding into this edge's start node (node)
                min_upstream = corrected_distance[edge_idx]

                # Look at all edges flowing INTO 'node' (the source of current edge)
                for pred_edge in self.G.es.select(_target=node):
                    pred_idx = pred_edge.index
                    if pred_idx in corrected_distance:
                        min_upstream = min(min_upstream, corrected_distance[pred_idx])

                # Update if we found a lower distance
                if min_upstream < corrected_distance[edge_idx]:
                    corrected_distance[edge_idx] = min_upstream
                    total_corrections += 1

                # Track that we've processed one more upstream edge for next_node
                upstream_processed[next_node] += 1

                # Add next_node to queue when all its upstream edges are processed
                if (
                    upstream_processed[next_node] == self.G.vs[next_node].indegree()
                    and next_node not in processed_nodes
                ):
                    queue.append(next_node)
                    processed_nodes.add(next_node)

        phase25_elapsed = time.time() - phase25_start
        logger.info(
            f"  ✓ Processed {edges_processed:,} edges, made {total_corrections:,} corrections ({phase25_elapsed:.1f}s)"
        )

        # Verify distances are reasonable (upstream edges should have >= distance of downstream)
        logger.info("  Verifying distance consistency...")
        violations = []
        for edge_idx, dist in corrected_distance.items():
            edge = self.G.es[edge_idx]
            source_node = edge.source

            # Check all edges flowing into this edge's source
            for upstream_edge in self.G.es.select(_target=source_node):
                upstream_idx = upstream_edge.index
                if upstream_idx in corrected_distance:
                    upstream_dist = corrected_distance[upstream_idx]
                    # Upstream should be >= downstream (further from named streams)
                    if upstream_dist < dist:
                        violations.append(
                            {
                                "edge": edge_idx,
                                "edge_dist": dist,
                                "upstream_edge": upstream_idx,
                                "upstream_dist": upstream_dist,
                                "difference": dist - upstream_dist,
                            }
                        )

        if violations:
            logger.warning(
                f"Found {len(violations)} edges where upstream has lower distance (may indicate correction needed)"
            )
            # Show first few violations
            for v in violations[:5]:
                logger.warning(
                    f"  Edge {v['edge']} (dist={v['edge_dist']}) > Upstream {v['upstream_edge']} (dist={v['upstream_dist']})"
                )
        else:
            logger.info(
                f"✓ Distance consistency verified: All upstream edges have >= distance of downstream"
            )

        # Phase 3: Mark edges for removal based on corrected threshold
        phase3_start = time.time()
        logger.info(
            f"\n[PHASE 3/6] Marking unnamed edges at distance >= {threshold}..."
        )
        edges_marked = set()
        raw_distance_stats = {}
        corrected_distance_stats = {}

        for edge_idx in edge_distance.keys():
            edge = self.G.es[edge_idx]
            edge_name = edge["gnis_name"]

            raw_dist = edge_distance[edge_idx]
            corrected_dist = corrected_distance.get(edge_idx, raw_dist)

            # Track statistics
            raw_distance_stats[raw_dist] = raw_distance_stats.get(raw_dist, 0) + 1
            corrected_distance_stats[corrected_dist] = (
                corrected_distance_stats.get(corrected_dist, 0) + 1
            )

            # Mark unnamed edges at or above threshold (using corrected distance)
            if not edge_name and corrected_dist >= threshold:
                edges_marked.add(edge_idx)

        phase3_elapsed = time.time() - phase3_start
        logger.info(
            f"  Raw distance distribution: {dict(sorted(raw_distance_stats.items()))}"
        )
        logger.info(
            f"  Corrected distance distribution: {dict(sorted(corrected_distance_stats.items()))}"
        )
        logger.info(
            f"  ✓ Marked {len(edges_marked):,} edges for removal ({phase3_elapsed:.1f}s)"
        )

        # Store distances in edge attributes
        logger.info(
            "Storing unnamed_depth_distance_raw and unnamed_depth_distance_corrected..."
        )
        # Initialize all edges with None
        self.G.es["unnamed_depth_distance_raw"] = [None] * self.G.ecount()
        self.G.es["unnamed_depth_distance_corrected"] = [None] * self.G.ecount()

        for edge_idx in edge_distance.keys():
            raw_dist = edge_distance[edge_idx]
            corrected_dist = corrected_distance.get(edge_idx, raw_dist)
            self.G.es[edge_idx]["unnamed_depth_distance_raw"] = raw_dist
            self.G.es[edge_idx]["unnamed_depth_distance_corrected"] = corrected_dist

        # Clear distance dicts to free memory (but keep pred_cache for Phase 4)
        del edge_distance
        del corrected_distance
        del raw_distance_stats
        del corrected_distance_stats
        gc.collect()

        if not edges_marked:
            logger.info("No edges to remove. Skipping.")
            # Clean up pred_cache before returning
            del pred_cache
            return

        # Phase 4: Expand to all upstream segments
        phase4_start = time.time()
        logger.info(f"\n[PHASE 4/6] Expanding to all upstream segments...")
        all_edges_to_remove = set(edges_marked)
        queue = deque(edges_marked)
        iteration = 0

        while queue:
            edge_idx = queue.popleft()
            edge = self.G.es[edge_idx]
            u = edge.source
            iteration += 1

            # Find all edges flowing into u (upstream)
            for pred_u in pred_cache.get(u, []):
                for pred_edge in self.G.es.select(_source=pred_u, _target=u):
                    pred_idx = pred_edge.index
                    if pred_idx not in all_edges_to_remove:
                        all_edges_to_remove.add(pred_idx)
                        queue.append(pred_idx)

            # Memory cleanup every 5000 iterations
            if iteration % 5000 == 0:
                gc.collect()

        # Clean up pred_cache now that we're done with Phase 4
        del pred_cache
        del edge_data_cache

        phase4_elapsed = time.time() - phase4_start
        expanded_count = len(all_edges_to_remove) - len(edges_marked)
        logger.info(
            f"  ✓ Expanded from {len(edges_marked):,} to {len(all_edges_to_remove):,} edges (+{expanded_count:,} upstream, {phase4_elapsed:.1f}s)"
        )

        # Verify no network breaks: check that if we're removing an edge,
        # we're also removing ALL its upstream edges
        logger.info(
            "Verifying Phase 4 expansion (all upstream edges will be removed)..."
        )
        expansion_violations = []
        for edge_idx in all_edges_to_remove:
            edge = self.G.es[edge_idx]
            source_node = edge.source

            # Check all edges flowing into this edge's source
            for upstream_edge in self.G.es.select(_target=source_node):
                upstream_idx = upstream_edge.index
                if upstream_idx not in all_edges_to_remove:
                    # Found an upstream edge that's NOT marked for removal!
                    expansion_violations.append(
                        {
                            "edge": edge_idx,
                            "upstream_edge": upstream_idx,
                            "upstream_dist": edge_distance.get(upstream_idx, "N/A"),
                            "upstream_name": upstream_edge["gnis_name"],
                        }
                    )

        if expansion_violations:
            logger.error(
                f"NETWORK BREAK: Found {len(expansion_violations)} upstream edges NOT marked for removal!"
            )
            # Show first few violations
            for v in expansion_violations[:10]:
                logger.error(
                    f"  Removing edge {v['edge']}, but keeping upstream {v['upstream_edge']} (dist={v['upstream_dist']}, name='{v['upstream_name']}')"
                )
        else:
            logger.info(
                f"✓ Expansion verified: All upstream edges of {len(all_edges_to_remove)} removed edges are also marked"
            )

        # Phase 5: Export removed data
        phase5_start = time.time()
        logger.info(f"\n[PHASE 5/6] Exporting removed edge data...")
        removed_edge_data = []

        for edge_idx in all_edges_to_remove:
            edge = self.G.es[edge_idx]
            source_name = self.index_to_node_id[edge.source]
            target_name = self.index_to_node_id[edge.target]

            removed_edge_data.append(
                {
                    "linear_feature_id": edge["linear_feature_id"],
                    "fwa_watershed_code": edge["fwa_watershed_code"],
                    "fwa_watershed_code_clean": edge["fwa_watershed_code_clean"],
                    "gnis_name": edge["gnis_name"],
                    "waterbody_key": edge["waterbody_key"],
                    "lake_name": edge["lake_name"],
                    "stream_order": edge["stream_order"],
                    "length": edge["length"],
                    "from_node": source_name,
                    "to_node": target_name,
                }
            )

        self.output_dir.mkdir(parents=True, exist_ok=True)
        removed_edges_file = self.output_dir / "removed_unnamed_depth_edges.json"
        with open(removed_edges_file, "w") as f:
            json.dump(removed_edge_data, f, indent=2)

        phase5_elapsed = time.time() - phase5_start
        logger.info(
            f"  ✓ Exported {len(removed_edge_data):,} edges to {removed_edges_file.name} ({phase5_elapsed:.1f}s)"
        )

        del removed_edge_data
        gc.collect()

        # Phase 6: Remove from graph
        phase6_start = time.time()
        logger.info(
            f"\n[PHASE 6/6] Removing {len(all_edges_to_remove):,} edges from graph..."
        )

        # Sort edge indices in descending order for safe deletion
        edges_to_delete = sorted(all_edges_to_remove, reverse=True)
        self.G.delete_edges(edges_to_delete)

        # Remove isolated vertices
        isolated = [v.index for v in self.G.vs if v.degree() == 0]
        num_isolated = len(isolated)

        if isolated:
            # Sort in descending order for safe deletion
            isolated = sorted(isolated, reverse=True)

            # Delete isolated vertices
            self.G.delete_vertices(isolated)

            # Rebuild mappings after vertex deletion (indices shift)
            self.node_id_to_index = {}
            self.index_to_node_id = {}
            for v in self.G.vs:
                node_id = v["name"]
                self.node_id_to_index[node_id] = v.index
                self.index_to_node_id[v.index] = node_id

        del isolated
        gc.collect()

        phase6_elapsed = time.time() - phase6_start
        total_elapsed = time.time() - start_time

        logger.info(
            f"  ✓ Removed {len(edges_to_delete):,} edges and {num_isolated:,} nodes ({phase6_elapsed:.1f}s)"
        )
        logger.info(f"\n{'='*80}")
        logger.info("UNNAMED STREAM FILTERING COMPLETE")
        logger.info(
            f"  Edges removed: {len(edges_to_delete):,} ({len(edges_to_delete)/self.G.ecount()*100:.1f}% of remaining)"
        )
        logger.info(f"  Nodes removed: {num_isolated:,}")
        logger.info(
            f"  Final graph: {self.G.vcount():,} nodes, {self.G.ecount():,} edges"
        )
        logger.info(
            f"  Total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)"
        )
        logger.info(f"{'='*80}\n")
        gc.collect()

    # TRIBUTARY ENRICHMENT DISABLED - Use graph traversal to find tributaries dynamically
    # Keeping this code commented for future reference
    # def enrich_tributaries(self):
    #     """
    #     DFS Traversal from Roots to Leaves.
    #     """
    #     import time

    #     start_time = time.time()

    #     logger.info("=" * 80)
    #     logger.info("STEP 2: ENRICHING TRIBUTARY RELATIONSHIPS")
    #     logger.info("=" * 80)

    #     roots = [v.index for v in self.G.vs if v.outdegree() == 0]
    #     logger.info(f"Found {len(roots):,} root nodes (tailwaters)")
    #     logger.info(f"Starting DFS traversal upstream...\n")

    #     stack = []
    #     visited_edges = set()

    #     # Process first edges attached to roots
    #     for r in roots:
    #         for edge in self.G.es.select(_target=r):
    #             edge_idx = edge.index
    #             if edge_idx in visited_edges:
    #                 continue
    #             visited_edges.add(edge_idx)

    #             v = edge.source
    #             name = edge["gnis_name"]
    #             lake = edge["lake_name"]

    #             # Initialize both tributary fields
    #             edge["stream_tributary_of"] = "Tailwater"
    #             edge["lake_tributary_of"] = lake if lake else ""

    #             # Track what to inherit upstream (separate for lakes and streams)
    #             upstream_lake_tributary = lake if lake else ""
    #             upstream_stream_tributary = "Tailwater"

    #             stack.append(
    #                 (
    #                     v,
    #                     upstream_stream_tributary,
    #                     upstream_lake_tributary,
    #                     edge,
    #                 )
    #             )

    #     while stack:
    #         (
    #             u,
    #             inherited_stream_tributary,
    #             inherited_lake_tributary,
    #             downstream_edge,
    #         ) = stack.pop()

    #         for edge in self.G.es.select(_target=u):
    #             edge_idx = edge.index
    #             if edge_idx in visited_edges:
    #                 continue
    #             visited_edges.add(edge_idx)

    #             v = edge.source
    #             current_name = edge["gnis_name"]
    #             current_lake = edge["lake_name"]
    #             downstream_name = downstream_edge["gnis_name"]
    #             downstream_stream_tributary_of = downstream_edge["stream_tributary_of"]

    #             # ==== LAKE TRIBUTARY LOGIC (separate field) ====
    #             # If current edge is in/through a lake, set that lake as lake_tributary_of
    #             if current_lake:
    #                 edge["lake_tributary_of"] = current_lake
    #                 upstream_lake_tributary = current_lake
    #             # Otherwise, inherit lake tributary from downstream (until we hit a new lake)
    #             elif inherited_lake_tributary:
    #                 edge["lake_tributary_of"] = inherited_lake_tributary
    #                 upstream_lake_tributary = inherited_lake_tributary
    #             else:
    #                 edge["lake_tributary_of"] = ""
    #                 upstream_lake_tributary = ""

    #             # ==== STREAM TRIBUTARY LOGIC (ignores lakes) ====
    #             # If current stream name matches downstream stream name, they're the same stream
    #             if current_name and current_name == downstream_name:
    #                 edge["stream_tributary_of"] = downstream_stream_tributary_of
    #                 upstream_stream_tributary = inherited_stream_tributary
    #             # If we have an inherited stream tributary, use it
    #             elif inherited_stream_tributary:
    #                 edge["stream_tributary_of"] = inherited_stream_tributary
    #                 upstream_stream_tributary = inherited_stream_tributary
    #             # If downstream is a different named stream, current is tributary of it
    #             elif downstream_name and downstream_name != current_name:
    #                 edge["stream_tributary_of"] = downstream_name
    #                 upstream_stream_tributary = downstream_name
    #             else:
    #                 edge["stream_tributary_of"] = ""
    #                 upstream_stream_tributary = ""

    #             # If current edge has a name and it's not the same as what we're inheriting,
    #             # update upstream_stream_tributary to the current name
    #             if current_name and current_name != upstream_stream_tributary:
    #                 upstream_stream_tributary = current_name

    #             stack.append(
    #                 (
    #                     v,
    #                     upstream_stream_tributary,
    #                     upstream_lake_tributary,
    #                     edge,
    #                 )
    #             )

    #     elapsed = time.time() - start_time
    #     logger.info(f"{'='*80}")
    #     logger.info("TRIBUTARY ENRICHMENT COMPLETE")
    #     logger.info(f"  Processed: {len(visited_edges):,} edges")
    #     logger.info(f"  Time: {elapsed:.1f}s")
    #     logger.info(f"{'='*80}\n")

    def export(self, filename="fwa_primal.graphml"):
        if self.G.vcount() == 0:
            return

        import time

        start_time = time.time()

        logger.info("=" * 80)
        logger.info("EXPORTING GRAPH")
        logger.info("=" * 80)

        # Determine base filename without extension
        base_filename = filename.replace(".graphml", "")

        # Build constant-time lookup dictionaries for fast access without graph
        logger.info("Building constant-time lookup dictionaries...")

        # Node coordinates lookup: node_id -> (x, y)
        node_coords = {}
        for v in self.G.vs:
            node_id = v["name"]
            node_coords[node_id] = (v["x"], v["y"])

        # Edge attributes lookup: edge_key -> {all attributes}
        edge_attrs = {}
        for edge in self.G.es:
            edge_key = edge["linear_feature_id"]
            source_id = self.index_to_node_id[edge.source]
            target_id = self.index_to_node_id[edge.target]

            edge_attrs[edge_key] = {
                "source": source_id,
                "target": target_id,
                "source_coords": node_coords[source_id],
                "target_coords": node_coords[target_id],
                "fwa_watershed_code": edge["fwa_watershed_code"],
                "fwa_watershed_code_clean": edge["fwa_watershed_code_clean"],
                "gnis_name": edge["gnis_name"],
                "gnis_id": edge["gnis_id"],
                "waterbody_key": edge["waterbody_key"],
                "lake_name": edge["lake_name"],
                "stream_order": edge["stream_order"],
                "length": edge["length"],
                "feature_code": edge["feature_code"],
                # Tributary fields removed - use graph traversal instead
                # "stream_tributary_of": edge["stream_tributary_of"],
                # "lake_tributary_of": edge["lake_tributary_of"],
                "weight": edge["weight"],
                "edge_index": edge["edge_index"],
            }

            # Add debug attributes if they exist
            if "unnamed_depth_distance_raw" in self.G.es.attributes():
                edge_attrs[edge_key]["unnamed_depth_distance_raw"] = edge[
                    "unnamed_depth_distance_raw"
                ]
                edge_attrs[edge_key]["unnamed_depth_distance_corrected"] = edge[
                    "unnamed_depth_distance_corrected"
                ]

        logger.info(
            f"Built lookups for {len(node_coords):,} nodes and {len(edge_attrs):,} edges"
        )

        # Export to pickle first (saves igraph object + mappings + lookups)
        pickle_path = self.output_dir / f"{base_filename}.gpickle"
        logger.info(f"Exporting to pickle format: {pickle_path}...")
        import pickle

        with open(pickle_path, "wb") as f:
            pickle.dump(
                {
                    "graph": self.G,
                    "node_id_to_index": self.node_id_to_index,
                    "index_to_node_id": self.index_to_node_id,
                    "node_coords": node_coords,  # O(1) coordinate lookups
                    "edge_attrs": edge_attrs,  # O(1) edge attribute lookups
                },
                f,
                protocol=pickle.HIGHEST_PROTOCOL,
            )
        logger.info(f"Pickle export complete: {pickle_path}")

        # Export to GraphML
        graphml_path = self.output_dir / filename
        logger.info(f"Exporting to GraphML format: {graphml_path}...")

        # Clean None values for GraphML export
        for attr in self.G.vs.attributes():
            values = self.G.vs[attr]
            cleaned = [v if v is not None else "" for v in values]
            self.G.vs[attr] = cleaned

        for attr in self.G.es.attributes():
            values = self.G.es[attr]
            cleaned = [v if v is not None else "" for v in values]
            self.G.es[attr] = cleaned

        try:
            self.G.write_graphml(str(graphml_path))
            logger.info(f"  ✓ GraphML: {graphml_path.name}")
        except Exception as e:
            logger.error(f"  ✗ GraphML export failed: {e}")
            logger.info(f"    Use pickle file instead: {pickle_path.name}")

        elapsed = time.time() - start_time
        logger.info(f"\n{'='*80}")
        logger.info("EXPORT COMPLETE")
        logger.info(f"  Files: {pickle_path.parent}")
        logger.info(f"  Time: {elapsed:.1f}s")
        logger.info(f"{'='*80}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "target", nargs="?", help="Stream name to filter watershed/subtree"
    )
    parser.add_argument(
        "-p", "--pickle", type=str, help="Load existing graph from pickle file"
    )
    parser.add_argument("-l", "--limit", type=int, default=None, help="Limit layers")
    parser.add_argument(
        "-L",
        "--layers",
        type=str,
        nargs="+",
        help="Specific layer names to process (e.g., GUIC LNIC)",
    )
    parser.add_argument(
        "-u",
        "--filter-unnamed",
        action="store_true",
        help="Filter unnamed streams by depth from named waterbodies",
    )
    parser.add_argument(
        "-t",
        "--threshold",
        type=int,
        default=2,
        help="Distance threshold for unnamed stream filtering (default: 2)",
    )
    args = parser.parse_args()

    builder = FWAPrimalGraphIGraph()

    # Load from pickle or build from scratch
    if args.pickle:
        from pathlib import Path

        pickle_path = Path(args.pickle)
        if not pickle_path.exists():
            logger.error(f"Pickle file not found: {pickle_path}")
            sys.exit(1)
        builder.load_from_pickle(pickle_path)
    else:
        if builder.validate_paths():
            builder.load_lakes()
            builder.build(limit=args.limit, layers=args.layers)
            builder.preprocess_graph()
            builder.propagate_names_by_watershed()
            # Optional: Filter unnamed streams by depth
            if args.filter_unnamed:
                builder.filter_unnamed_depth(threshold=args.threshold)
            # Tributary enrichment disabled - tributaries will be found via graph traversal
            # builder.enrich_tributaries()

    # Export
    if args.target:
        # TODO: Implement filter_watershed
        logger.error("Watershed filtering not yet implemented in igraph version")
        sys.exit(1)
    else:
        # Include layer names in filename if specific layers were used
        if args.layers and builder.layers_used:
            layers_str = "_".join(sorted(builder.layers_used))
            builder.export(f"fwa_primal_{layers_str}.graphml")
        else:
            builder.export("fwa_bc_primal_full.graphml")
