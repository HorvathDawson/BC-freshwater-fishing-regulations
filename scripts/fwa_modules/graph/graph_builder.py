#!/usr/bin/env python3
"""
FWA Graph Builder - Strict Consistency Version
Builds a primal graph representation of BC Freshwater Atlas (FWA) stream network.

- Paths match original structure (outputs to scripts/output/).
- Restores 'Downstream Correction' phase to preserve lake outlets.
- Includes detailed debug summary and Progress Bars.
- Features Optimized Name Propagation (Bulk Assign + Fallback).
"""
import os
import sys
import logging
import warnings
import argparse
import gc
import pickle
import fiona
import igraph as ig
from multiprocessing import Pool
from collections import deque, Counter
from pathlib import Path
from shapely.geometry import shape, LineString, MultiLineString

# Try importing tqdm for progress bars, fallback if not available
try:
    from tqdm import tqdm
except ImportError:
    print("tqdm not installed. Install with: pip install tqdm")

    # Dummy wrapper if tqdm missing
    def tqdm(iterable, **kwargs):
        return iterable


# --- Configuration & Setup ---

os.environ["GDAL_SKIP"] = "DXF"
warnings.filterwarnings("ignore")


class GDALFilter(logging.Filter):
    def filter(self, record):
        return "header.dxf" not in record.getMessage()


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
for handler in logging.root.handlers:
    handler.addFilter(GDALFilter())

logger = logging.getLogger(__name__)

# --- Helper Functions ---


def get_endpoints(geom):
    """Extracts start (u) and end (v) points from Shapely geometry."""
    if isinstance(geom, LineString):
        coords = geom.coords
    elif isinstance(geom, MultiLineString) and len(geom.geoms) > 0:
        coords = [geom.geoms[0].coords[0], geom.geoms[-1].coords[-1]]
    else:
        return None, None, None, None

    x1, y1 = round(coords[0][0], 3), round(coords[0][1], 3)
    x2, y2 = round(coords[-1][0], 3), round(coords[-1][1], 3)

    u_id = f"{x1}_{y1}"
    v_id = f"{x2}_{y2}"
    return u_id, v_id, (x1, y1), (x2, y2)


def clean_watershed_code(code):
    if not code:
        return ""
    return "-".join([p for p in code.split("-") if p != "000000"])


def format_id_attribute(val):
    if val is None or val == "" or val == 0:
        return ""
    try:
        return str(int(float(val)))
    except (ValueError, TypeError):
        return str(val)


# --- Worker Function (Parallel) ---


def process_layer_worker(args):
    """
    Reads a GDB layer. Logic matches original script exactly to ensure consistency.
    """
    layer_name, gdb_path = args
    nodes = []
    edges = []

    stats = Counter()
    stats["total_features"] = 0

    try:
        with fiona.open(gdb_path, layer=layer_name) as src:
            for feature in src:
                stats["total_features"] += 1

                props = feature["properties"]
                geom_data = feature["geometry"]

                if not geom_data:
                    stats["skipped_no_geom_data"] += 1
                    continue

                code = props.get("FWA_WATERSHED_CODE")
                if code and code.startswith("999-999999"):
                    stats["skipped_invalid_watershed"] += 1
                    continue

                lf_id = props.get("LINEAR_FEATURE_ID")
                if not lf_id:
                    stats["skipped_missing_id"] += 1
                    continue

                try:
                    geom = shape(geom_data)
                    if geom.is_empty:
                        stats["skipped_empty_geom"] += 1
                        continue
                except Exception:
                    stats["skipped_geom_parse_error"] += 1
                    continue

                u, v, u_coord, v_coord = get_endpoints(geom)
                if not u:
                    stats["skipped_invalid_endpoints"] += 1
                    continue

                # 4. Attribute Extraction (Strict Match to Original)
                try:
                    name = props.get("GNIS_NAME")
                    gnis_name = name if name else ""

                    gnis_id_raw = props.get("GNIS_ID")
                    gnis_id = str(int(gnis_id_raw)) if gnis_id_raw else ""

                    wb = props.get("WATERBODY_KEY")
                    waterbody_key = ""
                    if wb and int(wb) != 0:
                        waterbody_key = str(int(wb))

                    sid = str(int(lf_id))
                    s_code = str(code) if code else ""
                    stream_order = props.get("STREAM_ORDER")
                    stream_magnitude = props.get("STREAM_MAGNITUDE")
                    feature_code = props.get("FEATURE_CODE", "")
                    blue_line_key_raw = props.get("BLUE_LINE_KEY")
                    blue_line_key = (
                        str(int(blue_line_key_raw)) if blue_line_key_raw else ""
                    )
                    edge_type = props.get("EDGE_TYPE", "")

                    edge_attr = {
                        "u": u,
                        "v": v,
                        "linear_feature_id": sid,
                        "fwa_watershed_code": s_code,
                        "gnis_name": gnis_name,
                        "gnis_id": gnis_id,
                        "waterbody_key": waterbody_key,
                        "stream_order": int(stream_order) if stream_order else None,
                        "stream_magnitude": (
                            int(stream_magnitude) if stream_magnitude else None
                        ),
                        "feature_code": feature_code,
                        "blue_line_key": blue_line_key,
                        "edge_type": edge_type,
                        "length": geom.length,
                    }

                    nodes.append((u, u_coord[0], u_coord[1]))
                    nodes.append((v, v_coord[0], v_coord[1]))
                    edges.append(edge_attr)
                    stats["processed_ok"] += 1

                except Exception:
                    stats["skipped_attribute_error"] += 1
                    continue

    except Exception as e:
        logger.error(f"Layer failure {layer_name}: {e}")
        stats["layer_fatal_error"] += 1

    return nodes, edges, stats, layer_name


# --- Main Graph Class ---


class FWAPrimalGraphIGraph:
    def __init__(self):
        self.script_dir = Path(__file__).resolve().parent
        self.project_root = self.script_dir.parents[2]

        self.base_data = (
            self.project_root
            / "data/ftp.geobc.gov.bc.ca/sections/outgoing/bmgs/FWA_Public"
        )
        self.streams_gdb = (
            self.base_data / "FWA_STREAM_NETWORKS_SP/FWA_STREAM_NETWORKS_SP.gdb"
        )
        self.output_dir = self.project_root / "scripts" / "output" / "fwa_modules"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.G = ig.Graph(directed=True)
        self.node_id_to_index = {}
        self.index_to_node_id = {}
        self.edge_counts = {}
        self.layers_used = []

    def validate_paths(self):
        if not self.streams_gdb.exists():
            logger.error(f"Streams GDB not found: {self.streams_gdb}")
            return False
        return True

    def _get_or_create_vertex(self, node_id, x, y):
        if node_id in self.node_id_to_index:
            return self.node_id_to_index[node_id]

        idx = self.G.vcount()
        self.G.add_vertex()
        self.node_id_to_index[node_id] = idx
        self.index_to_node_id[idx] = node_id

        self.G.vs[idx]["name"] = node_id
        self.G.vs[idx]["x"] = x
        self.G.vs[idx]["y"] = y
        return idx

    def load_from_pickle(self, pickle_path):
        logger.info(f"Loading from {pickle_path}...")
        with open(pickle_path, "rb") as f:
            data = pickle.load(f)
            self.G = data["graph"]
            self.node_id_to_index = data["node_id_to_index"]
            self.index_to_node_id = data["index_to_node_id"]
        logger.info(f"Loaded: {self.G.vcount():,} Nodes, {self.G.ecount():,} Edges")

    def build(self, limit=None, specific_layers=None):
        import time

        start = time.time()
        logger.info("STEP 1: BUILDING GRAPH")

        if specific_layers:
            layers = (
                specific_layers
                if isinstance(specific_layers, list)
                else [specific_layers]
            )
        else:
            layers = fiona.listlayers(str(self.streams_gdb))
            layers = [l for l in layers if not l.startswith("_") and len(l) <= 4]
            if limit:
                layers = layers[:limit]

        self.layers_used = layers
        logger.info(f"Processing {len(layers)} layers with 14 workers.")

        worker_args = [(l, str(self.streams_gdb)) for l in layers]

        all_nodes = []
        all_edges = []
        total_stats = Counter()

        with Pool(processes=14) as pool:
            # We use imap here if we want a progress bar for layers, but map is fine for now
            results = pool.map(process_layer_worker, worker_args)

            for n, e, stats, layer_name in results:
                all_nodes.extend(n)
                all_edges.extend(e)
                total_stats.update(stats)

            del results

        # --- DEBUG SUMMARY TABLE ---
        logger.info("\n" + "=" * 50)
        logger.info(
            f"BUILD STATISTICS (Scanned {total_stats['total_features']:,} features)"
        )
        logger.info("=" * 50)
        logger.info(f"{'IMPORTED OK':<25} | {total_stats['processed_ok']:,}")
        logger.info("-" * 50)
        logger.info(
            f"{'Skipped (Missing ID)':<25} | {total_stats['skipped_missing_id']:,}"
        )
        logger.info(
            f"{'Skipped (Bad Watershed)':<25} | {total_stats['skipped_invalid_watershed']:,}"
        )
        logger.info(
            f"{'Skipped (Empty Geom)':<25} | {total_stats['skipped_empty_geom']:,}"
        )
        logger.info(
            f"{'Skipped (Geom Error)':<25} | {total_stats['skipped_geom_parse_error']:,}"
        )
        logger.info(
            f"{'Skipped (Bad Endpoints)':<25} | {total_stats['skipped_invalid_endpoints']:,}"
        )
        logger.info(
            f"{'Skipped (Attr Error)':<25} | {total_stats['skipped_attribute_error']:,}"
        )
        logger.info("=" * 50 + "\n")

        logger.info(f"Merging {len(all_nodes):,} nodes and {len(all_edges):,} edges...")

        unique_nodes = {nid: (x, y) for nid, x, y in all_nodes}
        for nid, (x, y) in unique_nodes.items():
            self._get_or_create_vertex(nid, x, y)

        del all_nodes, unique_nodes
        gc.collect()

        edge_list = []
        attrs = {
            k: []
            for k in [
                "linear_feature_id",
                "waterbody_key",
                "fwa_watershed_code",
                "fwa_watershed_code_clean",
                "gnis_name",
                "gnis_id",
                "stream_order",
                "stream_magnitude",
                "length",
                "feature_code",
                "blue_line_key",
                "edge_type",
                "weight",
                "edge_index",
            ]
        }

        for e in all_edges:
            v_idx = self.node_id_to_index[e["v"]]
            u_idx = self.node_id_to_index[e["u"]]

            key = (e["v"], e["u"])
            p_count = self.edge_counts.get(key, 0)
            self.edge_counts[key] = p_count + 1

            edge_list.append((v_idx, u_idx))

            attrs["linear_feature_id"].append(e["linear_feature_id"])
            attrs["waterbody_key"].append(e["waterbody_key"])
            attrs["fwa_watershed_code"].append(e["fwa_watershed_code"])
            attrs["fwa_watershed_code_clean"].append(
                clean_watershed_code(e["fwa_watershed_code"])
            )
            attrs["gnis_name"].append(e["gnis_name"])
            attrs["gnis_id"].append(e["gnis_id"])
            attrs["feature_code"].append(e["feature_code"])
            attrs["blue_line_key"].append(e["blue_line_key"])
            attrs["edge_type"].append(e["edge_type"])
            attrs["length"].append(e["length"])
            attrs["stream_order"].append(e["stream_order"])
            attrs["stream_magnitude"].append(e["stream_magnitude"])
            attrs["weight"].append(1.0 + (p_count * 0.02))
            attrs["edge_index"].append(p_count)

        self.G.add_edges(edge_list)
        for k, v in attrs.items():
            self.G.es[k] = v

        logger.info(
            f"Built Graph: {self.G.vcount():,} nodes, {self.G.ecount():,} edges. Time: {time.time()-start:.1f}s"
        )
        gc.collect()

    def preprocess_graph(self):
        """Removes spurious Order 1 edges flowing into root nodes."""
        logger.info("STEP 2: CLEANING SPURIOUS EDGES")
        iteration = 0
        total_removed = 0

        while True:
            iteration += 1
            roots = [v.index for v in self.G.vs if v.outdegree() == 0]
            if not roots:
                break

            to_remove = []
            for root in roots:
                for edge in self.G.es.select(_target=root):
                    if edge["stream_order"] == 1:
                        if edge["fwa_watershed_code"].startswith("9"):
                            continue
                        if edge["gnis_name"]:
                            continue
                        to_remove.append(edge.index)

            if not to_remove:
                break
            self.G.delete_edges(to_remove)
            total_removed += len(to_remove)
            logger.info(f"  Iter {iteration}: Removed {len(to_remove)} spurious edges.")

        isolated = [v.index for v in self.G.vs if v.degree() == 0]
        self.G.delete_vertices(isolated)

        # Re-index
        self.node_id_to_index = {v["name"]: v.index for v in self.G.vs}
        self.index_to_node_id = {v.index: v["name"] for v in self.G.vs}

        logger.info(f"Cleanup Complete. Removed {total_removed} edges.")

    def _find_nearest_name_bfs(self, start_nodes, watershed_code, max_hops=15):
        queue = deque([(n, 0) for n in start_nodes])
        visited = set(start_nodes)
        candidates = []

        while queue and len(candidates) < 5:
            node, dist = queue.popleft()
            if dist > max_hops:
                continue

            try:
                incident = self.G.incident(node)
            except ValueError:
                continue

            for edge in self.G.es[incident]:
                if edge["fwa_watershed_code"] != watershed_code:
                    continue

                if edge["gnis_name"]:
                    candidates.append(
                        (dist, edge["gnis_name"], edge["gnis_id"], edge["stream_order"])
                    )
                    continue

                neighbors = set(self.G.neighbors(node))
                for n in neighbors:
                    if n not in visited:
                        visited.add(n)
                        queue.append((n, dist + 1))

        if not candidates:
            return None, None, False

        # Warn on bad data
        if any(c[3] is None for c in candidates):
            seg_str = ", ".join(
                [f"'{c[1]}' (Order: {c[3]}, Dist: {c[0]})" for c in candidates]
            )
            logger.warning(
                f"Watershed {watershed_code} candidates containing None values: {seg_str}"
            )

        # Sorts by:
        # 1. Distance (Ascending: 0, 1, 2...) -> Closest first
        # 2. Stream Order (Ascending: 1, 2, 3...) -> Lowest order first (e.g. Order 1 wins over Order 5)
        candidates.sort(
            key=lambda x: (x[0], x[3] if x[3] is not None else float("inf"))
        )

        min_dist = candidates[0][0]
        top_matches = [c for c in candidates if c[0] == min_dist]
        is_ambiguous = len(set(c[1] for c in top_matches)) > 1

        return candidates[0][1], candidates[0][2], is_ambiguous

    def propagate_names_by_watershed(self):
        """
        Populate missing GNIS names based on Watershed Code and Blue Line Key.

        Logic:
        1. Index all known names by Watershed Code.
        2. If a WC has NO known names -> Leave unnamed.
        3. NEW: If a Blue Line Key has EXACTLY ONE unique name -> Assign to all unnamed segments in that BLK.
        4. If a WC has EXACTLY ONE unique name -> Bulk assign to all remaining empty segments (Optimization).
        5. If a WC has MULTIPLE names -> Use BFS to find nearest.
           If BFS fails (island segment or too far), fallback to the
           dominant name for that WC to ensure no segments remain unnamed if a name exists.
        """
        logger.info("STEP 3: PROPAGATING GNIS NAMES (Optimized with Blue Line Key)")

        # --- 1. Pre-calculate Name Stats per Watershed Code ---
        wc_name_stats = {}
        blk_name_stats = {}  # NEW: Pre-compute blue line key stats globally

        for edge in self.G.es:
            wc = edge["fwa_watershed_code"]
            name = edge["gnis_name"]
            blk = edge["blue_line_key"]

            if wc and name:
                if wc not in wc_name_stats:
                    wc_name_stats[wc] = Counter()
                # Count frequency of this name/id pair in this watershed
                wc_name_stats[wc][(name, edge["gnis_id"])] += 1

                # NEW: Also track by (wc, blk) combination
                if blk:
                    key = (wc, blk)
                    if key not in blk_name_stats:
                        blk_name_stats[key] = Counter()
                    blk_name_stats[key][(name, edge["gnis_id"])] += 1

        # --- 2. Group Unnamed Edges ---
        unnamed_groups = {}
        for edge in self.G.es:
            if edge["fwa_watershed_code"] and not edge["gnis_name"]:
                wc = edge["fwa_watershed_code"]
                if wc not in unnamed_groups:
                    unnamed_groups[wc] = []
                unnamed_groups[wc].append(edge.index)

        updated_count = 0
        bulk_assigned = 0
        blk_assigned = 0
        bfs_assigned = 0

        # --- 3. Process Groups ---
        for wc, edge_indices in tqdm(
            unnamed_groups.items(), desc="   Processing Watersheds", unit="wshed"
        ):
            # CASE A: Totally Unnamed Watershed
            # If this watershed code doesn't exist in our named stats, we can't do anything.
            if wc not in wc_name_stats:
                continue

            known_names = wc_name_stats[wc]
            unique_name_count = len(known_names)

            # Determine the "Dominant" name (most frequent) for fallback/bulk usage
            (dominant_name, dominant_id), _ = known_names.most_common(1)[0]

            # CASE B: Blue Line Key Propagation
            # Group unnamed edges by blue_line_key
            blk_groups = {}
            for idx in edge_indices:
                blk = self.G.es[idx]["blue_line_key"]
                if blk:  # Only if blue_line_key exists
                    if blk not in blk_groups:
                        blk_groups[blk] = []
                    blk_groups[blk].append(idx)

            # Process blue line key groups - assign if single unique name
            remaining_indices = set(edge_indices)

            for blk, blk_edge_indices in blk_groups.items():
                key = (wc, blk)
                if key in blk_name_stats and len(blk_name_stats[key]) == 1:
                    # Single unique name in this blue line key
                    (blk_name, blk_id), _ = blk_name_stats[key].most_common(1)[0]
                    for idx in blk_edge_indices:
                        self.G.es[idx]["gnis_name"] = blk_name
                        self.G.es[idx]["gnis_id"] = blk_id
                        blk_assigned += 1
                        remaining_indices.discard(idx)

            # Convert back to list for iteration
            remaining_indices = list(remaining_indices)

            # Skip further processing if all edges were assigned by BLK
            if not remaining_indices:
                updated_count += len(edge_indices)
                continue

            # CASE C: Single Name Optimization (O(1))
            # If there is only one name ever associated with this code, just use it.
            if unique_name_count == 1:
                for idx in remaining_indices:
                    if not self.G.es[idx]["gnis_name"]:  # Double-check still unnamed
                        self.G.es[idx]["gnis_name"] = dominant_name
                        self.G.es[idx]["gnis_id"] = dominant_id
                        bulk_assigned += 1

            # CASE D: Multiple Names (Ambiguous) -> BFS with Fallback
            else:
                for idx in remaining_indices:
                    if not self.G.es[idx]["gnis_name"]:  # Double-check still unnamed
                        edge = self.G.es[idx]

                        # 1. Try BFS to find nearest topological match
                        found_name, found_id, _ = self._find_nearest_name_bfs(
                            [edge.source, edge.target], wc
                        )

                        if found_name:
                            edge["gnis_name"] = found_name
                            edge["gnis_id"] = found_id
                            bfs_assigned += 1
                        else:
                            # 2. Fallback: BFS failed (too far or disconnected graph).
                            # Use dominant name to enforce "No unnamed segments if name exists" rule.
                            edge["gnis_name"] = dominant_name
                            edge["gnis_id"] = dominant_id
                            bfs_assigned += 1

            updated_count += len(edge_indices)

        logger.info(f"Propagation Complete: {updated_count:,} edges updated.")
        logger.info(f"   - Blue Line Key Assigned: {blk_assigned:,}")
        logger.info(f"   - Bulk Assigned (Single-Name WC): {bulk_assigned:,}")
        logger.info(f"   - BFS/Fallback Assigned (Multi-Name WC): {bfs_assigned:,}")

    def filter_unnamed_depth(self, threshold=2):
        import time

        logger.info(f"STEP 4: FILTERING DEEP UNNAMED STREAMS (Threshold={threshold})")
        start_time = time.time()

        edge_dist = {}
        queue = deque()
        edge_data = {
            e.index: {"fwa": e["fwa_watershed_code"], "src": e.source}
            for e in self.G.es
        }
        node_preds = {v.index: self.G.incident(v.index, mode="in") for v in self.G.vs}

        # 1. Initialize from Named Streams (Upstream Search)
        for e in self.G.es:
            if e["gnis_name"]:
                edge_dist[e.index] = 0
                queue.append((e.source, e["fwa_watershed_code"], 0))

        # 2. BFS Upstream
        visited_nodes = set()
        # PROGRESS BAR ADDED: BFS Upstream
        # We estimate total progress by node count, though it's imperfect
        with tqdm(
            total=self.G.vcount(), desc="  [1/3] Measuring Upstream", unit="node"
        ) as pbar:
            while queue:
                node, prev_wc, prev_dist = queue.popleft()
                if node in visited_nodes:
                    continue
                visited_nodes.add(node)
                pbar.update(1)

                for edge_idx in node_preds[node]:
                    e_info = edge_data[edge_idx]
                    cost = 0 if e_info["fwa"] == prev_wc else 1
                    new_dist = prev_dist + cost

                    if self.G.es[edge_idx]["gnis_name"]:
                        new_dist = 0

                    if edge_idx not in edge_dist or new_dist < edge_dist[edge_idx]:
                        edge_dist[edge_idx] = new_dist
                        queue.append((e_info["src"], e_info["fwa"], new_dist))

        # 3. PHASE 2.5: Correct distances (Downstream Propagation)
        logger.info("[Phase 2.5] Correcting distances (Downstream flow)...")
        corrected_distance = edge_dist.copy()

        sources = [v.index for v in self.G.vs if v.indegree() == 0]
        queue_down = deque(sources)
        processed_down = set(sources)
        upstream_processed = {v.index: 0 for v in self.G.vs}

        # PROGRESS BAR ADDED: Downstream Correction
        with tqdm(
            total=self.G.vcount(), desc="  [2/3] Downstream Correction", unit="node"
        ) as pbar:
            while queue_down:
                node = queue_down.popleft()
                pbar.update(1)

                for edge in self.G.es.select(_source=node):
                    edge_idx = edge.index
                    next_node = edge.target

                    if edge_idx not in corrected_distance:
                        continue

                    min_upstream = corrected_distance[edge_idx]
                    for pred_edge in self.G.es.select(_target=node):
                        if pred_edge.index in corrected_distance:
                            min_upstream = min(
                                min_upstream, corrected_distance[pred_edge.index]
                            )

                    if min_upstream < corrected_distance[edge_idx]:
                        corrected_distance[edge_idx] = min_upstream

                    upstream_processed[next_node] += 1
                    if upstream_processed[next_node] == self.G.vs[next_node].indegree():
                        if next_node not in processed_down:
                            queue_down.append(next_node)
                            processed_down.add(next_node)

        # --- SANITY CHECK ---
        logger.info("Verifying distance consistency (Upstream >= Downstream)...")
        violations = 0
        for edge in self.G.es:
            current_dist = corrected_distance.get(edge.index)
            if current_dist is None:
                continue

            node = edge.source
            for parent_edge in self.G.es.select(_target=node):
                parent_dist = corrected_distance.get(parent_edge.index)
                if parent_dist is not None:
                    if parent_dist < current_dist:
                        violations += 1
                        if violations <= 5:
                            logger.warning(
                                f"Violation: Edge {edge.index} (Dist {current_dist}) > Parent {parent_edge.index} (Dist {parent_dist})"
                            )

        if violations == 0:
            logger.info("✓ Consistency Check Passed.")
        else:
            logger.error(f"❌ Consistency Check Failed: {violations} violations found.")

        # 4. Mark and Remove
        to_remove = set()
        for idx in range(self.G.ecount()):
            dist = corrected_distance.get(idx, None)
            self.G.es[idx]["unnamed_depth_distance_corrected"] = dist

            if (
                dist is not None
                and not self.G.es[idx]["gnis_name"]
                and dist >= threshold
            ):
                to_remove.add(idx)

        queue_rem = deque(to_remove)

        # PROGRESS BAR ADDED: Recursive Removal
        with tqdm(
            total=len(to_remove) * 2, desc="  [3/3] Marking for Removal", unit="edge"
        ) as pbar:
            while queue_rem:
                idx = queue_rem.popleft()
                pbar.update(1)

                src_node = self.G.es[idx].source
                for upstream_idx in node_preds[src_node]:
                    if upstream_idx not in to_remove:
                        to_remove.add(upstream_idx)
                        queue_rem.append(upstream_idx)
                        pbar.total += 1  # Extend bar as we find more parents

        if to_remove:
            self.G.delete_edges(list(to_remove))
            self.G.delete_vertices([v.index for v in self.G.vs if v.degree() == 0])
            self.node_id_to_index = {v["name"]: v.index for v in self.G.vs}
            self.index_to_node_id = {v.index: v["name"] for v in self.G.vs}

        logger.info(
            f"Filtered {len(to_remove):,} edges based on depth. (Time: {time.time()-start_time:.1f}s)"
        )

    def export(self, filename="fwa_bc_primal_full.graphml"):
        logger.info(f"STEP 5: EXPORTING")

        pickle_filename = filename.rsplit(".", 1)[0] + ".gpickle"
        pickle_path = self.output_dir / pickle_filename

        node_coords = {v["name"]: (v["x"], v["y"]) for v in self.G.vs}
        edge_attrs_map = {}
        for e in self.G.es:
            lid = e["linear_feature_id"]
            if lid:
                edge_attrs_map[lid] = e.attributes()
                edge_attrs_map[lid]["source"] = self.index_to_node_id[e.source]
                edge_attrs_map[lid]["target"] = self.index_to_node_id[e.target]

        with open(pickle_path, "wb") as f:
            pickle.dump(
                {
                    "graph": self.G,
                    "node_coords": node_coords,
                    "edge_attrs": edge_attrs_map,
                },
                f,
                protocol=pickle.HIGHEST_PROTOCOL,
            )
        logger.info(f"  ✓ Saved Pickle: {pickle_path}")

        for attr in self.G.es.attributes():
            self.G.es[attr] = [x if x is not None else "" for x in self.G.es[attr]]

        graphml_path = self.output_dir / filename
        graphml_path.parent.mkdir(parents=True, exist_ok=True)
        self.G.write_graphml(str(graphml_path))
        logger.info(f"  ✓ Saved GraphML: {graphml_path}")


# --- Execution Entry Point ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--pickle", type=str, help="Load from existing pickle")
    parser.add_argument("-l", "--limit", type=int, default=None, help="Limit layers")
    parser.add_argument("-L", "--layers", type=str, nargs="+", help="Specific layers")
    parser.add_argument(
        "-u", "--filter-unnamed", action="store_true", help="Filter deep unnamed"
    )
    parser.add_argument(
        "-t", "--threshold", type=int, default=2, help="Depth threshold"
    )
    args = parser.parse_args()

    builder = FWAPrimalGraphIGraph()

    if args.pickle:
        builder.load_from_pickle(args.pickle)
    else:
        if builder.validate_paths():
            builder.build(limit=args.limit, specific_layers=args.layers)
            builder.preprocess_graph()
            builder.propagate_names_by_watershed()

            if args.filter_unnamed:
                builder.filter_unnamed_depth(threshold=args.threshold)

    filename = "fwa_bc_primal_full.graphml"
    if args.layers:
        suffix = "_".join(sorted(args.layers))
        filename = f"fwa_primal_{suffix}.graphml"

    builder.export(filename)
