#!/usr/bin/env python3
"""
FWA Graph Processor (Primal Graph Approach)
"""
import os, sys, logging, warnings, argparse, gc, json
from multiprocessing import Pool
from collections import deque
import geopandas as gpd
import pandas as pd
import networkx as nx
import fiona
from pathlib import Path
from shapely.geometry import LineString, MultiLineString, shape

warnings.filterwarnings("ignore")
os.environ["GDAL_SKIP"] = "DXF"
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


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
            # First pass: collect features and build name propagation mapping
            features_list = []
            code_to_name = {}
            for feature in src:
                props = feature["properties"]
                geom_data = feature["geometry"]
                if not geom_data:
                    continue
                # Filter ditches
                f_source = props.get("FEATURE_SOURCE")
                if f_source and "ditch" in f_source.lower():
                    continue
                # Filter bad watershed codes
                code = props.get("FWA_WATERSHED_CODE")
                if code and code.startswith("999-999999"):
                    continue
                # Build name propagation mapping
                name = props.get("GNIS_NAME")
                if code and name:
                    if code not in code_to_name:
                        code_to_name[code] = name
                features_list.append((feature, code, name))
            # Second pass: process features with name propagation
            for feature, code, name in features_list:
                props = feature["properties"]
                geom_data = feature["geometry"]
                # Apply name propagation
                if code and (not name) and code in code_to_name:
                    name = code_to_name[code]
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
                # Store edge data (will compute parallel counts in main thread)
                edge_data = {
                    "v": v,
                    "u": u,
                    "sid": sid,
                    "fwa_watershed_code": s_code,
                    "gnis_name": gnis_name,
                    "waterbody_key": waterbody_key,
                    "lake_name": lake_name_val,
                    "stream_order": int(stream_order) if stream_order else None,
                    "length": geom.length,
                }
                edges_data.append(edge_data)
            # Clean up
            del features_list
            del code_to_name
            gc.collect()
    except Exception as e:
        # Silently skip problematic layers
        pass
    return nodes_data, edges_data


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

    def validate_paths(self):
        if not self.streams_gdb.exists():
            logger.error(f"Streams GDB not found: {self.streams_gdb}")
            return False
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
        u = f"{x1}_{y1}"
        v = f"{x2}_{y2}"
        return u, v, (x1, y1), (x2, y2)

    def clean_code(self, fwa_code):
        if not fwa_code:
            return ""
        return "-".join([p for p in fwa_code.split("-") if p != "000000"])

    def preprocess_layer(self, gdf):
        """Preprocess layer: filter out ditches and bad codes, propagate names by watershed code."""
        # Filter out ditches
        if "FEATURE_SOURCE" in gdf.columns:
            gdf = gdf[
                ~(
                    gdf["FEATURE_SOURCE"].notna()
                    & (gdf["FEATURE_SOURCE"].str.lower() == "ditch")
                )
            ].copy()
        # Filter out the specific long 999 code
        if "FWA_WATERSHED_CODE" in gdf.columns:
            gdf = gdf[
                ~(
                    gdf["FWA_WATERSHED_CODE"].notna()
                    & (
                        gdf["FWA_WATERSHED_CODE"]
                        == "999-999999-999999-999999-999999-999999-999999-999999-999999-999999-999999-999999-999999-999999-999999-999999-999999-999999-999999-999999-999999"
                    )
                )
            ].copy()
        # Propagate names based on watershed code
        if "FWA_WATERSHED_CODE" in gdf.columns and "GNIS_NAME" in gdf.columns:
            # Build a mapping of watershed code to name (only for named streams)
            code_to_name = {}
            for idx, row in gdf.iterrows():
                code = row.get("FWA_WATERSHED_CODE")
                name = row.get("GNIS_NAME")
                if pd.notna(code) and pd.notna(name) and name:
                    # Use the first named stream we encounter for each code
                    if code not in code_to_name:
                        code_to_name[code] = name

            # Apply names to unnamed streams with matching watershed codes
            def fill_name(row):
                if pd.isna(row["GNIS_NAME"]) or not row["GNIS_NAME"]:
                    code = row.get("FWA_WATERSHED_CODE")
                    if pd.notna(code) and code in code_to_name:
                        return code_to_name[code]
                return row["GNIS_NAME"]

            gdf["GNIS_NAME"] = gdf.apply(fill_name, axis=1)
        return gdf

    def build(self, limit=None, layers=None):
        logger.info("Step 1: Loading Segments...")
        try:
            if layers is None:
                layers = fiona.listlayers(str(self.streams_gdb))
                layers = [l for l in layers if not l.startswith("_") and len(l) <= 4]
                if limit:
                    layers = layers[:limit]
            elif isinstance(layers, str):
                layers = [layers]
        except Exception:
            return
        print(f"Processing {len(layers)} layers in parallel with 14 workers...")
        # Prepare arguments for workers
        worker_args = [
            (layer, str(self.streams_gdb), self.lake_lookup) for layer in layers
        ]
        # Process layers in parallel
        all_nodes_data = []
        all_edges_data = []
        with Pool(processes=14) as pool:
            results = pool.map(process_layer_worker, worker_args)
            # Collect results and clean memory periodically
            for i, (nodes_data, edges_data) in enumerate(results):
                all_nodes_data.extend(nodes_data)
                all_edges_data.extend(edges_data)
                if i % 10 == 0:
                    print(".", end="", flush=True)
                # Periodic cleanup
                if i % 50 == 0 and i > 0:
                    gc.collect()
        print("\nMerging results into graph...")
        # Add all unique nodes
        unique_nodes = {}
        for node_id, x, y in all_nodes_data:
            if node_id not in unique_nodes:
                unique_nodes[node_id] = (x, y)
        for node_id, (x, y) in unique_nodes.items():
            self.G.add_node(node_id, x=x, y=y, size=0.1, label="")
        del unique_nodes
        del all_nodes_data
        gc.collect()
        # Add all edges with parallel edge counting
        for edge_data in all_edges_data:
            v = edge_data["v"]
            u = edge_data["u"]
            sid = edge_data["sid"]
            fwa_watershed_code = edge_data["fwa_watershed_code"]
            gnis_name = edge_data["gnis_name"]
            waterbody_key = edge_data["waterbody_key"]
            lake_name = edge_data["lake_name"]
            stream_order = edge_data["stream_order"]
            length = edge_data["length"]
            # Count parallel edges for visual offset
            edge_key_pair = (v, u)
            parallel_count = self.edge_counts.get(edge_key_pair, 0)
            self.edge_counts[edge_key_pair] = parallel_count + 1
            visual_offset = parallel_count * 0.02
            # Add edge to graph
            self.G.add_edge(
                v,
                u,
                key=sid,
                linear_feature_id=sid,
                fwa_watershed_code=fwa_watershed_code,
                fwa_watershed_code_clean=clean_code_helper(fwa_watershed_code),
                gnis_name=gnis_name,
                waterbody_key=waterbody_key,
                lake_name=lake_name,
                stream_order=stream_order,
                length=length,
                tributary_of="",
                lake_tributary="",
                weight=1.0 + visual_offset,
                edge_index=parallel_count,
            )
        del all_edges_data
        gc.collect()
        print(
            f"\nGraph Built: {self.G.number_of_nodes():,} Nodes, {self.G.number_of_edges():,} Edges"
        )

    def preprocess_graph(self):
        """
        Clean up spurious edges before enrichment.
        Remove edges with STREAM_ORDER == 1 that lead to roots (apparent tailwaters).
        """
        logger.info(
            "Step 1.5: Preprocessing Graph - Removing spurious order-1 tailwater edges..."
        )
        # Find all roots (nodes with out_degree == 0, appearing as tailwater)
        roots = [n for n, d in self.G.out_degree() if d == 0]
        logger.info(f"Found {len(roots)} root nodes (out-degree 0).")
        edges_to_remove = []
        order_counts = {}
        # Check edges leading to these roots
        for root in roots:
            for predecessor in list(self.G.predecessors(root)):
                for edge_key in list(self.G[predecessor][root].keys()):
                    edge_data = self.G.edges[predecessor, root, edge_key]
                    stream_order = edge_data.get("stream_order")
                    # Track stream order distribution
                    order_counts[stream_order] = order_counts.get(stream_order, 0) + 1
                    # If this edge has stream order 1, it's likely spurious
                    if stream_order == 1:
                        edges_to_remove.append((predecessor, root, edge_key))
        logger.info(
            f"Stream order distribution of edges to roots: {dict(sorted(order_counts.items()))}"
        )
        # Clear order_counts dict as it's no longer needed
        del order_counts
        # Remove the spurious edges
        num_edges_removed = len(edges_to_remove)
        for u, v, key in edges_to_remove:
            self.G.remove_edge(u, v, key)
        # Clear edges_to_remove list
        del edges_to_remove
        # Remove isolated nodes that may have been created
        # An isolated node has no edges connected to it (degree 0)
        isolated_nodes = list(nx.isolates(self.G))
        self.G.remove_nodes_from(isolated_nodes)
        num_isolated = len(isolated_nodes)
        del isolated_nodes
        logger.info(f"Removed {num_edges_removed} order-1 edges leading to roots.")
        logger.info(
            f"Removed {num_isolated} isolated nodes (nodes with no connections after edge removal)."
        )
        logger.info(
            f"Graph after cleanup: {self.G.number_of_nodes():,} Nodes, {self.G.number_of_edges():,} Edges"
        )
        # Force garbage collection after cleanup
        import gc

        gc.collect()
        logger.info(
            f"Graph after cleanup: {self.G.number_of_nodes():,} Nodes, {self.G.number_of_edges():,} Edges"
        )

    def filter_unnamed_depth(self, threshold=2, debug=False):
        """
        Remove unnamed stream segments that are N or more stream systems away
        from the nearest named waterbody. Also removes all upstream segments.
        If debug=True, stores the distance value in edge attribute 'unnamed_depth_distance'.
        """
        logger.info(
            f"Step 1.6: Filtering unnamed streams by depth (threshold={threshold})..."
        )
        # Phase 1: Collect named stream starting points
        logger.info("Phase 1: Collecting named stream anchors...")
        named_edges = []
        for u, v, key, data in self.G.edges(keys=True, data=True):
            if data.get("gnis_name", ""):
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
            queue.append((u, edge_data.get("fwa_watershed_code", ""), 0))
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
                    edge_fwa = edge_data.get("fwa_watershed_code", "")
                    edge_name = edge_data.get("gnis_name", "")
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

        # Phase 2.5: Correct distances to ensure network consistency (single-pass BFS)
        logger.info("Phase 2.5: Correcting distances to prevent network breaks...")
        corrected_distance = {}

        # Initialize all edges with their BFS distance
        for edge_id, dist in edge_distance.items():
            corrected_distance[edge_id] = dist

        # Single-pass correction: BFS from source nodes, propagating minimum distances
        # Find all source nodes (in-degree 0) - headwaters
        sources = [n for n, d in self.G.in_degree() if d == 0]

        # Track how many upstream edges have been processed for each node
        upstream_processed = {n: 0 for n in self.G.nodes()}
        queue = deque(sources)
        processed_nodes = set(sources)
        total_corrections = 0

        while queue:
            node = queue.popleft()

            # Process all edges flowing out of this node
            for next_node in self.G.successors(node):
                for edge_key in self.G[node][next_node]:
                    edge_id = (node, next_node, edge_key)

                    if edge_id not in corrected_distance:
                        continue

                    # Find minimum distance among all upstream edges feeding into this edge's start node
                    min_upstream = corrected_distance[edge_id]

                    for pred_node in self.G.predecessors(node):
                        for pred_key in self.G[pred_node][node]:
                            pred_edge_id = (pred_node, node, pred_key)
                            if pred_edge_id in corrected_distance:
                                min_upstream = min(
                                    min_upstream, corrected_distance[pred_edge_id]
                                )

                    # Update if we found a lower distance
                    if min_upstream < corrected_distance[edge_id]:
                        corrected_distance[edge_id] = min_upstream
                        total_corrections += 1

                # Track that we've processed one more upstream edge for next_node
                upstream_processed[next_node] += 1

                # Add next_node to queue when all its upstream edges are processed
                if (
                    upstream_processed[next_node] == self.G.in_degree(next_node)
                    and next_node not in processed_nodes
                ):
                    queue.append(next_node)
                    processed_nodes.add(next_node)

        logger.info(f"Corrected {total_corrections:,} edge distances.")

        # Phase 3: Mark edges for removal based on corrected threshold
        logger.info(
            f"Phase 3: Marking unnamed edges at corrected distance >= {threshold}..."
        )
        edges_marked = set()
        raw_distance_stats = {}
        corrected_distance_stats = {}

        for edge_id in edge_distance.keys():
            u, v, key = edge_id
            edge_data = self.G.edges[u, v, key]
            edge_name = edge_data.get("gnis_name", "")

            raw_dist = edge_distance[edge_id]
            corrected_dist = corrected_distance.get(edge_id, raw_dist)

            # Track statistics
            raw_distance_stats[raw_dist] = raw_distance_stats.get(raw_dist, 0) + 1
            corrected_distance_stats[corrected_dist] = (
                corrected_distance_stats.get(corrected_dist, 0) + 1
            )

            # Mark unnamed edges at or above threshold (using corrected distance)
            if not edge_name and corrected_dist >= threshold:
                edges_marked.add(edge_id)

        logger.info(
            f"Raw distance statistics: {dict(sorted(raw_distance_stats.items()))}"
        )
        logger.info(
            f"Corrected distance statistics: {dict(sorted(corrected_distance_stats.items()))}"
        )
        logger.info(
            f"Marked {len(edges_marked):,} edges for removal at threshold {threshold}."
        )

        # Store distances in edge attributes if debug mode
        if debug:
            logger.info(
                "Debug mode: Storing unnamed_depth_distance_raw and unnamed_depth_distance_corrected..."
            )
            for edge_id in edge_distance.keys():
                u, v, key = edge_id
                if self.G.has_edge(u, v, key):
                    raw_dist = edge_distance[edge_id]
                    corrected_dist = corrected_distance.get(edge_id, raw_dist)
                    self.G.edges[u, v, key]["unnamed_depth_distance_raw"] = raw_dist
                    self.G.edges[u, v, key][
                        "unnamed_depth_distance_corrected"
                    ] = corrected_dist

        # Clear distance dicts to free memory
        del edge_distance
        del corrected_distance
        del raw_distance_stats
        del corrected_distance_stats
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
                        "fwa_watershed_code": edge_data.get("fwa_watershed_code", ""),
                        "fwa_watershed_code_clean": edge_data.get(
                            "fwa_watershed_code_clean", ""
                        ),
                        "gnis_name": edge_data.get("gnis_name", ""),
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
            logger.info(
                f"Debug Mode: Marking {len(roots)} search roots out of {self.G.number_of_nodes()} nodes."
            )
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
                    name = data.get("gnis_name", "")
                    lake = data.get("lake_name", "")

                    # Initialize both tributary fields
                    data["tributary_of"] = "Tailwater"
                    data["lake_tributary"] = lake if lake else ""

                    # Track what to inherit upstream (separate for lakes and streams)
                    upstream_lake_tributary = lake if lake else ""
                    upstream_stream_tributary = "Tailwater"

                    stack.append(
                        (
                            v,
                            upstream_stream_tributary,
                            upstream_lake_tributary,
                            data,
                            edge_key,
                        )
                    )
        while stack:
            (
                u,
                inherited_stream_tributary,
                inherited_lake_tributary,
                downstream_data,
                downstream_key,
            ) = stack.pop()
            for v in self.G.predecessors(u):
                for edge_key in self.G[v][u]:
                    edge_id = (v, u, edge_key)
                    if edge_id in visited_edges:
                        continue
                    visited_edges.add(edge_id)
                    data = self.G.edges[v, u, edge_key]
                    current_name = data.get("gnis_name", "")
                    current_lake = data.get("lake_name", "")
                    downstream_name = downstream_data.get("gnis_name", "")
                    downstream_tributary_of = downstream_data.get("tributary_of", "")

                    # ==== LAKE TRIBUTARY LOGIC (separate field) ====
                    # If current edge is in/through a lake, set that lake as lake_tributary
                    if current_lake:
                        data["lake_tributary"] = current_lake
                        upstream_lake_tributary = current_lake
                    # Otherwise, inherit lake tributary from downstream (until we hit a new lake)
                    elif inherited_lake_tributary:
                        data["lake_tributary"] = inherited_lake_tributary
                        upstream_lake_tributary = inherited_lake_tributary
                    else:
                        data["lake_tributary"] = ""
                        upstream_lake_tributary = ""

                    # ==== STREAM TRIBUTARY LOGIC (ignores lakes) ====
                    # If current stream name matches downstream stream name, they're the same stream
                    if current_name and current_name == downstream_name:
                        data["tributary_of"] = downstream_tributary_of
                        upstream_stream_tributary = inherited_stream_tributary
                    # If we have an inherited stream tributary, use it
                    elif inherited_stream_tributary:
                        data["tributary_of"] = inherited_stream_tributary
                        upstream_stream_tributary = inherited_stream_tributary
                    # If downstream is a different named stream, current is tributary of it
                    elif downstream_name and downstream_name != current_name:
                        data["tributary_of"] = downstream_name
                        upstream_stream_tributary = downstream_name
                    else:
                        data["tributary_of"] = ""
                        upstream_stream_tributary = ""

                    # If current edge has a name and it's not the same as what we're inheriting,
                    # update upstream_stream_tributary to the current name
                    if current_name and current_name != upstream_stream_tributary:
                        upstream_stream_tributary = current_name

                    stack.append(
                        (
                            v,
                            upstream_stream_tributary,
                            upstream_lake_tributary,
                            data,
                            edge_key,
                        )
                    )
        logger.info("Tributary enrichment complete.")

    def filter_watershed(self, target_name):
        logger.info(f"Filtering for river system: '{target_name}'...")
        search_lower = target_name.lower()
        target_edges = []
        # MultiDiGraph iteration includes keys
        for u, v, key, data in self.G.edges(keys=True, data=True):
            if search_lower in data.get("gnis_name", "").lower():
                target_edges.append((u, v, key))
        if not target_edges:
            logger.error("Target river name not found.")
            return
        logger.info("Extracting connected component...")
        undirected = self.G.to_undirected()
        seed_nodes = set()
        for u, v, key in target_edges:
            seed_nodes.add(u)
            seed_nodes.add(v)
        full_watershed_nodes = set()
        for seed in seed_nodes:
            if seed not in full_watershed_nodes:
                comp = nx.node_connected_component(undirected, seed)
                full_watershed_nodes.update(comp)
        self.G = self.G.subgraph(list(full_watershed_nodes)).copy()
        logger.info(
            f"Filtered Watershed: {self.G.number_of_nodes():,} Nodes, {self.G.number_of_edges():,} Edges."
        )

    def export(self, filename="fwa_primal.graphml"):
        if self.G.number_of_nodes() == 0:
            return
        # Determine base filename without extension
        base_filename = filename.replace(".graphml", "")
        # Export to pickle first (more reliable, doesn't run out of memory)
        pickle_filename = f"{base_filename}.gpickle"
        logger.info(f"Exporting to pickle format: {pickle_filename}...")
        import pickle

        with open(pickle_filename, "wb") as f:
            pickle.dump(self.G, f, protocol=pickle.HIGHEST_PROTOCOL)
        logger.info(f"Pickle export complete: {pickle_filename}")
        # Clean attributes for GraphML export
        logger.info(f"Exporting to GraphML format: {filename}...")
        for u, v, key, data in self.G.edges(keys=True, data=True):
            for k, val in list(data.items()):
                if val is None:
                    data[k] = ""
        for n, data in self.G.nodes(data=True):
            for k, val in list(data.items()):
                if val is None:
                    data[k] = ""
                if isinstance(val, bool):
                    data[k] = str(val)
        # Try to export to GraphML (may fail on large graphs due to memory)
        try:
            nx.write_graphml(self.G, filename)
            logger.info(f"GraphML export complete: {filename}")
        except MemoryError:
            logger.warning(
                f"GraphML export failed due to MemoryError. Use pickle file: {pickle_filename}"
            )
        except Exception as e:
            logger.error(
                f"GraphML export failed: {e}. Use pickle file: {pickle_filename}"
            )
        logger.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
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
            builder.filter_unnamed_depth(
                threshold=args.unnamed_depth_threshold, debug=args.debug
            )
        builder.enrich_tributaries(debug=args.debug)
        if args.target:
            builder.filter_watershed(args.target)
            clean_name = args.target.replace(" ", "_")
            builder.export(f"fwa_primal_{clean_name}.graphml")
        else:
            builder.export("fwa_bc_primal_full.graphml")
