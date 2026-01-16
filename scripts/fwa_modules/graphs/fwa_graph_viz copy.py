#!/usr/bin/env python3
"""
FWA Graph Processor (Primal Graph Approach)
Memory Optimized Version with Parallel Processing
"""

import os, sys, logging, warnings, argparse, gc, json, pickle, gzip
import geopandas as gpd
import pandas as pd
import networkx as nx
import fiona
from pathlib import Path
from shapely.geometry import shape, LineString, MultiLineString
from multiprocessing import Pool, cpu_count
from functools import partial
from collections import deque

warnings.filterwarnings("ignore")
os.environ["GDAL_SKIP"] = "DXF"
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


def process_layer_worker(args):
    """
    Worker function to process a single layer in parallel.
    Returns nodes and edges as dictionaries to be merged later.
    """
    layer, streams_gdb, known_names_dict, lake_lookup_dict = args

    nodes = {}
    edges = []
    local_known_names = known_names_dict.copy()

    try:
        with fiona.open(str(streams_gdb), layer=layer) as src:
            for feature in src:
                props = feature["properties"]
                geom_data = feature["geometry"]

                if not geom_data:
                    continue

                # Filter out ditches
                f_source = props.get("FEATURE_SOURCE")
                if f_source and "ditch" in f_source.lower():
                    continue

                # Filter out bad watershed codes
                code = props.get("FWA_WATERSHED_CODE")
                if code and code.startswith("999-999999"):
                    continue

                # Propagate names
                name = props.get("GNIS_NAME")
                if code:
                    if name:
                        local_known_names[code] = name
                    elif code in local_known_names:
                        name = local_known_names[code]

                # Parse Geometry
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

                # Get endpoints
                if isinstance(geom, LineString):
                    coords = geom.coords
                elif isinstance(geom, MultiLineString) and len(geom.geoms) > 0:
                    coords = [geom.geoms[0].coords[0], geom.geoms[-1].coords[-1]]
                else:
                    continue

                x1, y1 = round(coords[0][0], 3), round(coords[0][1], 3)
                x2, y2 = round(coords[-1][0], 3), round(coords[-1][1], 3)
                u = f"{x1}_{y1}"
                v = f"{x2}_{y2}"

                # Store nodes
                if u not in nodes:
                    nodes[u] = {"x": x1, "y": y1, "size": 0.1, "label": ""}
                if v not in nodes:
                    nodes[v] = {"x": x2, "y": y2, "size": 0.1, "label": ""}

                # Prepare edge attributes
                sid = str(int(lf_id)) if lf_id else f"UNK-{layer}"
                s_code = str(code) if code else ""

                waterbody_key = ""
                lake_name_val = ""

                if wb and int(wb) != 0:
                    waterbody_key = str(int(wb))
                    k = int(wb)
                    if k in lake_lookup_dict:
                        lake_name_val = lake_lookup_dict[k]
                        if not name:
                            name = lake_name_val

                edge_name = name if name else ""

                # Clean FWA code
                fwa_code_clean = (
                    "-".join([p for p in s_code.split("-") if p != "000000"])
                    if s_code
                    else ""
                )

                # Store edge (v -> u, mouth to source)
                edge_data = {
                    "u": v,
                    "v": u,
                    "key": sid,
                    "id": sid,
                    "linear_feature_id": sid,
                    "fwa_code": s_code,
                    "fwa_code_clean": fwa_code_clean,
                    "name": edge_name,
                    "waterbody_key": waterbody_key,
                    "lake_name": lake_name_val,
                    "stream_order": int(stream_order) if stream_order else None,
                    "length": geom.length,
                    "thickness": 10.0,
                    "tributary_of": "",
                    "weight": 1.0,
                    "edge_index": 0,
                }
                edges.append(edge_data)

    except Exception as e:
        logger.error(f"Error processing layer {layer}: {e}")
        return None

    # Clean up
    del geom_data, props
    gc.collect()

    return {"nodes": nodes, "edges": edges, "known_names": local_known_names}


class FWAPrimalGraph:
    def __init__(self):
        self.script_dir = Path(__file__).resolve().parent.parent
        self.project_root = self.script_dir.parent.parent
        self.base_data = (
            self.project_root
            / "data/ftp.geobc.gov.bc.ca/sections/outgoing/bmgs/FWA_Public"
        )
        self.streams_gdb = (
            self.base_data / "FWA_STREAM_NETWORKS_SP/FWA_STREAM_NETWORKS_SP.gdb"
        )
        self.lakes_gdb = self.base_data / "FWA_BC/FWA_BC.gdb"

        # MultiDiGraph allows parallel edges
        self.G = nx.MultiDiGraph()
        self.lake_lookup = {}
        self.edge_counts = {}  # Track parallel edges for visual offset

        # NEW: Store known names for codes to replicate preprocess_layer logic across streaming
        self.known_names = {}

    def validate_paths(self):
        if not self.streams_gdb.exists():
            logger.error(f"Streams GDB not found: {self.streams_gdb}")
            return False
        return True

    def load_lakes(self):
        if not self.lakes_gdb.exists():
            return
        try:
            # GPD is fine here as lakes are usually smaller than streams
            df = gpd.read_file(
                str(self.lakes_gdb), layer="FWA_LAKES_POLY", ignore_geometry=True
            )
            valid = df.dropna(subset=["WATERBODY_KEY", "GNIS_NAME_1"])

            # OPTIMIZATION: sys.intern reusable strings
            self.lake_lookup = {}
            for k, n in zip(valid.WATERBODY_KEY, valid.GNIS_NAME_1):
                self.lake_lookup[int(k)] = sys.intern(n)

            logger.info(f"Loaded {len(self.lake_lookup)} lake keys.")
            del df, valid
            gc.collect()
        except Exception:
            pass

    def get_endpoints(self, geom):
        """Returns rounded string IDs for start/end nodes to ensure snapping."""
        if isinstance(geom, LineString):
            coords = geom.coords
        elif isinstance(geom, MultiLineString) and len(geom.geoms) > 0:
            coords = [geom.geoms[0].coords[0], geom.geoms[-1].coords[-1]]
        else:
            return None, None, None, None

        x1, y1 = round(coords[0][0], 3), round(coords[0][1], 3)
        x2, y2 = round(coords[-1][0], 3), round(coords[-1][1], 3)

        # u = Start (Source), v = End (Mouth) in typical GIS digitization
        # OPTIMIZATION: Intern these strings as they are repeated often
        u = sys.intern(f"{x1}_{y1}")
        v = sys.intern(f"{x2}_{y2}")

        return u, v, (x1, y1), (x2, y2)

    def clean_code(self, fwa_code):
        if not fwa_code:
            return ""
        return "-".join([p for p in fwa_code.split("-") if p != "000000"])

    def build(self, limit=None, num_workers=14):
        logger.info(f"Step 1: Loading Segments with {num_workers} parallel workers...")

        try:
            layers = fiona.listlayers(str(self.streams_gdb))
            layers = [l for l in layers if not l.startswith("_") and len(l) <= 4]
            if limit:
                layers = layers[:limit]
        except Exception:
            return

        total_layers = len(layers)
        logger.info(f"Processing {total_layers} layers in parallel...")

        # Prepare arguments for parallel processing
        worker_args = [
            (layer, self.streams_gdb, self.known_names, self.lake_lookup)
            for layer in layers
        ]

        # Process layers in parallel with controlled batch size
        batch_size = num_workers * 2  # Process 2 batches per worker at a time
        all_nodes = {}
        all_edges = []

        with Pool(processes=num_workers) as pool:
            for batch_start in range(0, len(worker_args), batch_size):
                batch_end = min(batch_start + batch_size, len(worker_args))
                batch_args = worker_args[batch_start:batch_end]

                logger.info(
                    f"Processing layers {batch_start+1}-{batch_end} of {total_layers}..."
                )

                # Process batch
                results = pool.map(process_layer_worker, batch_args)

                # Merge results
                for result in results:
                    if result is None:
                        continue

                    # Merge nodes
                    all_nodes.update(result["nodes"])

                    # Collect edges
                    all_edges.extend(result["edges"])

                    # Update known names
                    self.known_names.update(result["known_names"])

                # Clear batch results and force garbage collection
                del results, batch_args
                gc.collect()

                logger.info(
                    f"Progress: {batch_end}/{total_layers} layers processed. "
                    f"Nodes: {len(all_nodes):,}, Edges: {len(all_edges):,}"
                )

        logger.info("Building graph from collected data...")

        # Add all nodes to graph
        for node_id, node_attrs in all_nodes.items():
            self.G.add_node(node_id, **node_attrs)

        # Clean up nodes dict
        del all_nodes
        gc.collect()

        # Calculate parallel edge offsets and add edges
        logger.info("Adding edges with parallel edge handling...")
        for edge_data in all_edges:
            u = edge_data.pop("u")
            v = edge_data.pop("v")

            # Track parallel edges for visual offset
            edge_key_pair = (u, v)
            parallel_count = self.edge_counts.get(edge_key_pair, 0)
            self.edge_counts[edge_key_pair] = parallel_count + 1

            edge_data["weight"] = 1.0 + (parallel_count * 0.02)
            edge_data["edge_index"] = parallel_count

            self.G.add_edge(u, v, **edge_data)

        # Clean up edges list
        del all_edges
        gc.collect()

        logger.info(
            f"Graph Built: {self.G.number_of_nodes():,} Nodes, {self.G.number_of_edges():,} Edges"
        )

    def preprocess_graph(self):
        """
        Clean up spurious edges before enrichment.
        Remove edges with STREAM_ORDER == 1 that lead to roots (apparent tailwaters).
        Exports removed edges to a JSON file for later reference.
        """
        logger.info(
            "Step 1.5: Preprocessing Graph - Removing spurious order-1 tailwater edges..."
        )

        # Find all roots (nodes with out_degree == 0, appearing as tailwater)
        roots = [n for n, d in self.G.out_degree() if d == 0]
        logger.info(f"Found {len(roots)} root nodes (out-degree 0).")

        edges_to_remove = []
        removed_edge_data = []

        # Check edges leading to these roots
        for root in roots:
            for predecessor in list(self.G.predecessors(root)):
                for edge_key in list(self.G[predecessor][root].keys()):
                    edge_data = self.G.edges[predecessor, root, edge_key]
                    stream_order = edge_data.get("stream_order")

                    # If this edge has stream order 1, it's likely spurious
                    if stream_order == 1:
                        edges_to_remove.append((predecessor, root, edge_key))

                        # Store the edge data for export
                        removed_edge_data.append(
                            {
                                "linear_feature_id": edge_data.get(
                                    "linear_feature_id", ""
                                ),
                                "fwa_code": edge_data.get("fwa_code", ""),
                                "fwa_code_clean": edge_data.get("fwa_code_clean", ""),
                                "name": edge_data.get("name", ""),
                                "waterbody_key": edge_data.get("waterbody_key", ""),
                                "lake_name": edge_data.get("lake_name", ""),
                                "stream_order": stream_order,
                                "length": edge_data.get("length", 0),
                                "from_node": predecessor,
                                "to_node": root,
                            }
                        )

        # Export removed edges before deletion
        output_dir = self.script_dir / "output"
        output_dir.mkdir(exist_ok=True)

        removed_edges_file = output_dir / "removed_spurious_edges.json"

        with open(removed_edges_file, "w") as f:
            json.dump(removed_edge_data, f, indent=2)

        logger.info(f"Exported removed edge data to {removed_edges_file}")

        # Remove the spurious edges
        num_edges_removed = len(edges_to_remove)
        for u, v, key in edges_to_remove:
            self.G.remove_edge(u, v, key)

        # Remove isolated nodes that may have been created
        isolated_nodes = list(nx.isolates(self.G))
        self.G.remove_nodes_from(isolated_nodes)

        num_isolated = len(isolated_nodes)

        logger.info(f"Removed {num_edges_removed} order-1 edges leading to roots.")
        logger.info(f"Removed {num_isolated} isolated nodes.")
        logger.info(
            f"Graph after cleanup: {self.G.number_of_nodes():,} Nodes, {self.G.number_of_edges():,} Edges"
        )

        # Force garbage collection
        gc.collect()

    def filter_unnamed_depth(self, threshold=2):
        """
        Remove unnamed stream segments that are N or more stream systems away
        from the nearest named waterbody. Also removes all upstream segments.
        """
        logger.info(
            f"Step 1.6: Filtering unnamed streams by depth (threshold={threshold})..."
        )

        # Phase 1: Collect named stream starting points
        logger.info("Phase 1: Collecting named stream anchors...")
        named_edges = []
        for u, v, key, data in self.G.edges(keys=True, data=True):
            if data.get("name", ""):
                named_edges.append((u, v, key))

        logger.info(f"Found {len(named_edges):,} named stream edges as anchors.")

        if not named_edges:
            logger.warning("No named streams found. Skipping unnamed depth filtering.")
            return

        # Phase 2: BFS upstream from named streams
        logger.info("Phase 2: Measuring distances from named streams...")
        edge_distance = {}  # (u, v, key) -> min_distance
        queue = deque()

        # Initialize: Add all named stream edges with distance=0
        for u, v, key in named_edges:
            edge_id = (u, v, key)
            edge_distance[edge_id] = 0
            edge_data = self.G.edges[u, v, key]
            queue.append((u, edge_data.get("fwa_code", ""), 0))

        # Clear named_edges to free memory
        del named_edges
        gc.collect()

        iteration = 0
        # BFS upstream
        while queue:
            current_node, prev_fwa_code, prev_distance = queue.popleft()
            iteration += 1

            # Look at all edges flowing INTO current_node (predecessors)
            for pred_u in self.G.predecessors(current_node):
                for key in self.G[pred_u][current_node]:
                    edge_id = (pred_u, current_node, key)
                    edge_data = self.G.edges[pred_u, current_node, key]
                    edge_fwa = edge_data.get("fwa_code", "")
                    edge_name = edge_data.get("name", "")

                    # Calculate distance for this edge
                    if edge_name:  # Named stream
                        new_distance = 0
                    elif edge_fwa == prev_fwa_code:  # Same stream
                        new_distance = prev_distance
                    else:  # Different unnamed stream
                        new_distance = prev_distance + 1

                    # Only process if better distance or first visit
                    if (
                        edge_id not in edge_distance
                        or new_distance < edge_distance[edge_id]
                    ):
                        edge_distance[edge_id] = new_distance
                        queue.append((pred_u, edge_fwa, new_distance))

            # Memory cleanup every 10000 iterations
            if iteration % 10000 == 0:
                gc.collect()

        logger.info(
            f"Processed {iteration:,} edge traversals. Computed {len(edge_distance):,} edge distances."
        )

        # Phase 3: Mark edges for removal based on threshold
        logger.info(f"Phase 3: Marking unnamed edges at distance >= {threshold}...")
        edges_marked = set()
        distance_stats = {}

        for edge_id, distance in edge_distance.items():
            u, v, key = edge_id
            edge_data = self.G.edges[u, v, key]
            edge_name = edge_data.get("name", "")

            # Track statistics
            distance_stats[distance] = distance_stats.get(distance, 0) + 1

            # Mark unnamed edges at or above threshold
            if not edge_name and distance >= threshold:
                edges_marked.add(edge_id)

        logger.info(f"Distance statistics: {dict(sorted(distance_stats.items()))}")
        logger.info(
            f"Marked {len(edges_marked):,} edges for removal at threshold {threshold}."
        )

        # Clear edge_distance to free memory
        del edge_distance
        gc.collect()

        if not edges_marked:
            logger.info("No edges to remove. Skipping.")
            return

        # Phase 4: Expand to all upstream segments
        logger.info("Phase 4: Expanding to include all upstream segments...")
        all_edges_to_remove = set(edges_marked)
        queue = deque(edges_marked)

        iteration = 0
        while queue:
            u, v, key = queue.popleft()
            iteration += 1

            # Find all edges flowing into u (upstream)
            for pred_u in self.G.predecessors(u):
                for pred_key in self.G[pred_u][u]:
                    edge_id = (pred_u, u, pred_key)
                    if edge_id not in all_edges_to_remove:
                        all_edges_to_remove.add(edge_id)
                        queue.append(edge_id)

            # Memory cleanup every 5000 iterations
            if iteration % 5000 == 0:
                gc.collect()

        logger.info(
            f"Total edges to remove (including upstream): {len(all_edges_to_remove):,}"
        )

        # Phase 5: Export removed data
        logger.info("Phase 5: Exporting removed edge data...")
        removed_edge_data = []

        for edge_id in all_edges_to_remove:
            u, v, key = edge_id
            if self.G.has_edge(u, v, key):  # Check if edge still exists
                edge_data = self.G.edges[u, v, key]
                removed_edge_data.append(
                    {
                        "linear_feature_id": edge_data.get("linear_feature_id", ""),
                        "fwa_code": edge_data.get("fwa_code", ""),
                        "fwa_code_clean": edge_data.get("fwa_code_clean", ""),
                        "name": edge_data.get("name", ""),
                        "waterbody_key": edge_data.get("waterbody_key", ""),
                        "lake_name": edge_data.get("lake_name", ""),
                        "stream_order": edge_data.get("stream_order"),
                        "length": edge_data.get("length", 0),
                        "from_node": u,
                        "to_node": v,
                    }
                )

        output_dir = self.script_dir / "output"
        output_dir.mkdir(exist_ok=True)
        removed_edges_file = output_dir / "removed_unnamed_depth_edges.json"

        with open(removed_edges_file, "w") as f:
            json.dump(removed_edge_data, f, indent=2)

        logger.info(
            f"Exported {len(removed_edge_data):,} removed edges to {removed_edges_file}"
        )

        # Clear removed_edge_data
        del removed_edge_data
        gc.collect()

        # Phase 6: Remove from graph
        logger.info("Phase 6: Removing edges from graph...")
        for edge_id in all_edges_to_remove:
            u, v, key = edge_id
            if self.G.has_edge(u, v, key):
                self.G.remove_edge(u, v, key)

        # Remove isolated nodes
        isolated_nodes = list(nx.isolates(self.G))
        self.G.remove_nodes_from(isolated_nodes)

        logger.info(
            f"Removed {len(all_edges_to_remove):,} edges and {len(isolated_nodes):,} isolated nodes."
        )
        logger.info(
            f"Graph after filtering: {self.G.number_of_nodes():,} Nodes, {self.G.number_of_edges():,} Edges"
        )

        # Force garbage collection
        gc.collect()

    def enrich_tributaries(self, debug=False):
        """
        DFS Traversal from Roots to Leaves.
        """
        logger.info(
            "Step 2: Enriching Tributary Data (DFS Backwards from Out-Degree Roots)..."
        )

        roots = [n for n, d in self.G.out_degree() if d == 0]

        if debug:
            logger.info("Debug Mode: Initializing debug flags...")
            nx.set_node_attributes(self.G, False, "is_search_root")
            for r in roots:
                self.G.nodes[r]["is_search_root"] = True

        stack = []
        visited_edges = set()

        # Process first edges attached to roots
        for r in roots:
            for v in self.G.predecessors(r):
                # Now iterating explicit string keys (SIDs)
                for edge_key in self.G[v][r]:
                    edge_id = (v, r, edge_key)
                    if edge_id in visited_edges:
                        continue
                    visited_edges.add(edge_id)

                    data = self.G.edges[v, r, edge_key]
                    lake = data.get("lake_name", "")

                    data["tributary_of"] = "Tailwater"

                    upstream_tributary = "Tailwater"
                    if lake:
                        upstream_tributary = lake

                    # MEMORY OPTIMIZATION:
                    # Pass tuple of essential strings instead of full dicts where possible
                    # to keep stack footprint lower.
                    stack.append((v, upstream_tributary, data.get("name", ""), lake))

        while stack:
            u, inherited_tributary, downstream_name, downstream_lake = stack.pop()

            for v in self.G.predecessors(u):
                for edge_key in self.G[v][u]:
                    edge_id = (v, u, edge_key)
                    if edge_id in visited_edges:
                        continue
                    visited_edges.add(edge_id)

                    data = self.G.edges[v, u, edge_key]
                    current_name = data.get("name", "")
                    current_lake = data.get("lake_name", "")

                    # NOTE: Logic below is preserved exactly from original script
                    # just using the variables passed in stack

                    downstream_tributary_of = (
                        ""  # Derived from logic if needed, but here we used inherited
                    )

                    # Track if we're in/near a lake
                    in_lake_context = False

                    # Logic matches your requested priority
                    if current_lake or downstream_lake:
                        lake_name = current_lake if current_lake else downstream_lake
                        data["tributary_of"] = lake_name
                        upstream_tributary = lake_name
                        in_lake_context = True
                    elif current_name and current_name == downstream_name:
                        data["tributary_of"] = inherited_tributary
                        upstream_tributary = inherited_tributary
                    elif inherited_tributary:
                        data["tributary_of"] = inherited_tributary
                        upstream_tributary = inherited_tributary
                    elif (
                        downstream_name and downstream_name != current_name
                    ):  # Prevent self-tributary
                        data["tributary_of"] = downstream_name
                        upstream_tributary = downstream_name
                    else:
                        data["tributary_of"] = ""
                        upstream_tributary = ""

                    # Only update upstream_tributary to current_name if we're NOT in a lake context
                    if (
                        current_name
                        and current_name != upstream_tributary
                        and not in_lake_context
                    ):
                        upstream_tributary = current_name

                    # MEMORY FIX: Intern the result
                    if data["tributary_of"]:
                        data["tributary_of"] = sys.intern(data["tributary_of"])

                    stack.append((v, upstream_tributary, current_name, current_lake))

        logger.info("Tributary enrichment complete.")
        gc.collect()

    def filter_watershed(self, target_name):
        logger.info(f"Filtering for river system: '{target_name}'...")
        search_lower = target_name.lower()

        target_edges = []
        # MultiDiGraph iteration includes keys
        for u, v, key, data in self.G.edges(keys=True, data=True):
            if search_lower in data.get("name", "").lower():
                target_edges.append((u, v, key))

        if not target_edges:
            logger.error("Target river name not found.")
            return

        logger.info("Extracting connected component...")

        # MEMORY FIX: Replaced to_undirected() (which copies graph) with manual BFS
        # This preserves the exact functionality of "finding connected component"
        # without doubling RAM usage.

        seed_nodes = set()
        for u, v, key in target_edges:
            seed_nodes.add(u)
            seed_nodes.add(v)

        full_watershed_nodes = set(seed_nodes)
        queue = list(seed_nodes)

        # We need to traverse both upstream (predecessors) and downstream (successors)
        # to emulate an undirected traversal.
        while queue:
            node = queue.pop(0)

            # Neighbors (Upstream)
            for neighbor in self.G.predecessors(node):
                if neighbor not in full_watershed_nodes:
                    full_watershed_nodes.add(neighbor)
                    queue.append(neighbor)

            # Neighbors (Downstream)
            for neighbor in self.G.successors(node):
                if neighbor not in full_watershed_nodes:
                    full_watershed_nodes.add(neighbor)
                    queue.append(neighbor)

        self.G = self.G.subgraph(list(full_watershed_nodes)).copy()
        gc.collect()

        logger.info(
            f"Filtered Watershed: {self.G.number_of_nodes():,} Nodes, {self.G.number_of_edges():,} Edges."
        )

    def export(self, filename="fwa_primal.graphml"):
        if self.G.number_of_nodes() == 0:
            return

        # Determine base filename without extension
        base_filename = str(filename).replace(".graphml", "")

        logger.info(f"Exporting graph to multiple formats...")

        # Clean attributes for export
        for u, v, key, data in self.G.edges(keys=True, data=True):
            for k, val in list(data.items()):
                if val is None:
                    data[k] = ""

        # Manual GC before heavy export
        gc.collect()

        # Export 1: Pickle format (memory efficient, always works)
        pickle_file = f"{base_filename}.gpickle.gz"
        try:
            logger.info(f"Exporting to compressed pickle format: {pickle_file}")
            with gzip.open(pickle_file, "wb") as f:
                pickle.dump(self.G, f, protocol=pickle.HIGHEST_PROTOCOL)
            logger.info(f"Successfully exported to {pickle_file}")
        except Exception as e:
            logger.error(f"Failed to write pickle format: {e}")

        gc.collect()

        # Export 2: GraphML format (human-readable, may fail on large graphs)
        graphml_file = f"{base_filename}.graphml"
        try:
            logger.info(f"Exporting to GraphML format: {graphml_file}")
            nx.write_graphml(self.G, graphml_file)
            logger.info(f"Successfully exported to {graphml_file}")
        except MemoryError:
            logger.warning(
                f"Failed to write GraphML due to MemoryError. Pickle format available at {pickle_file}"
            )
        except Exception as e:
            logger.error(
                f"Failed to write GraphML: {e}. Pickle format available at {pickle_file}"
            )

        logger.info("Export complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="FWA Graph Processor - Build and analyze BC freshwater stream networks"
    )
    parser.add_argument("target", nargs="?", help="Name of stream to filter")
    parser.add_argument("--limit", type=int, default=None, help="Limit layers")
    parser.add_argument(
        "--debug", action="store_true", help="Mark search roots in graph"
    )
    parser.add_argument(
        "--filter-unnamed-depth",
        action="store_true",
        help="Filter unnamed streams by depth from named waterbodies",
    )
    parser.add_argument(
        "--unnamed-depth-threshold",
        type=int,
        default=2,
        help="Distance threshold for unnamed stream filtering (default: 2)",
    )
    args = parser.parse_args()

    builder = FWAPrimalGraph()

    if builder.validate_paths():
        builder.load_lakes()
        builder.build(limit=args.limit)
        builder.preprocess_graph()

        # Optional: Filter unnamed streams by depth
        if args.filter_unnamed_depth:
            builder.filter_unnamed_depth(threshold=args.unnamed_depth_threshold)

        builder.enrich_tributaries(debug=args.debug)

        if args.target:
            builder.filter_watershed(args.target)
            clean_name = args.target.replace(" ", "_")
            builder.export(f"fwa_primal_{clean_name}")
        else:
            builder.export("fwa_bc_primal_full")
