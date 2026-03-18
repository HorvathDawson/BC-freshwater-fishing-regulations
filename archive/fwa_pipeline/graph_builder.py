#!/usr/bin/env python3
"""
FWA Graph Builder - Strict Consistency Version
Builds a primal graph representation of BC Freshwater Atlas (FWA) stream network.

- Outputs to output/fwa/ directory.
- Restores 'Downstream Correction' phase to preserve lake outlets.
- Includes detailed debug summary and Progress Bars.
- Features Optimized Name Propagation (Bulk Assign + Fallback).
"""
import os
import logging
import warnings
import argparse
import gc
import pickle
from typing import Dict, List, Optional, Set, Tuple, Union
from data.data_extractor import FWADataAccessor
import igraph as ig
from collections import deque, Counter
from shapely.geometry import LineString, MultiLineString
from project_config import get_config

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


def get_endpoints(
    geom: Union[LineString, MultiLineString],
) -> Tuple[
    Optional[str],
    Optional[str],
    Optional[Tuple[float, float]],
    Optional[Tuple[float, float]],
]:
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


def clean_watershed_code(code: Optional[str]) -> str:
    if not code:
        return ""
    return "-".join([p for p in code.split("-") if p != "000000"])


# --- Main Graph Class ---


class FWAPrimalGraphIGraph:

    def _get_or_create_vertex(self, node_id: str, x: float, y: float) -> int:
        """
        Add a vertex to the graph if it does not exist, or return its index if it does.
        Updates node_id_to_index and index_to_node_id mappings.
        """
        if node_id in self.node_id_to_index:
            return self.node_id_to_index[node_id]
        idx = self.G.vcount()
        self.G.add_vertex(name=node_id, x=x, y=y)
        self.node_id_to_index[node_id] = idx
        self.index_to_node_id[idx] = node_id
        return idx

    def __init__(self) -> None:
        config = get_config()
        self.project_root = config.project_root
        self.gpkg_path = config.fetch_output_gpkg_path
        self.output_dir = config.fwa_output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.G = ig.Graph(directed=True)
        self.node_id_to_index = {}
        self.index_to_node_id = {}
        self.edge_counts = {}
        self.streams_layer = "streams"  # The new unified streams layer name
        self.data_accessor = FWADataAccessor(self.gpkg_path)

    def validate_paths(self) -> bool:
        if not self.gpkg_path.exists():
            logger.error(f"GeoPackage not found: {self.gpkg_path}")
            return False
        return True

    def build(
        self, limit: Optional[int] = None, specific_layers: Optional[List[str]] = None
    ) -> None:
        import time

        start = time.time()
        logger.info("STEP 1: BUILDING GRAPH")
        # Only one streams layer now
        logger.info(f"Processing unified streams layer: '{self.streams_layer}'")
        gdf = self.data_accessor.get_layer(self.streams_layer)
        logger.info(
            f"Loaded streams layer: {len(gdf):,} rows, columns: {list(gdf.columns)}"
        )
        all_nodes = []
        all_edges = []
        total_stats = Counter()
        for idx, row in gdf.iterrows():
            stats = total_stats
            stats["total_features"] += 1
            props = row
            geom = row.geometry
            if geom is None or geom.is_empty:
                stats["skipped_no_geom_data"] += 1
                continue
            code = props.get("FWA_WATERSHED_CODE")
            if code and str(code).startswith("999-999999"):
                stats["skipped_invalid_watershed"] += 1
                continue
            lf_id = props.get("LINEAR_FEATURE_ID")
            if not lf_id:
                stats["skipped_missing_id"] += 1
                continue
            try:
                u, v, u_coord, v_coord = get_endpoints(geom)
                if not u:
                    stats["skipped_invalid_endpoints"] += 1
                    continue
                # FWADataAccessor already cleans ID/code columns to strings
                # Only GNIS_NAME needs None handling (text field, not cleaned by accessor)
                gnis_name = props.get("GNIS_NAME") or ""
                gnis_id = props.get("GNIS_ID")
                waterbody_key = props.get("WATERBODY_KEY")
                sid = props.get("LINEAR_FEATURE_ID")
                s_code = props.get("FWA_WATERSHED_CODE")
                stream_order = props.get("STREAM_ORDER")
                stream_magnitude = props.get("STREAM_MAGNITUDE")
                feature_code = props.get("FEATURE_CODE")
                blue_line_key = props.get("BLUE_LINE_KEY")
                edge_type = props.get("EDGE_TYPE")
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
                all_nodes.append((u, u_coord[0], u_coord[1]))
                all_nodes.append((v, v_coord[0], v_coord[1]))
                all_edges.append(edge_attr)
                stats["processed_ok"] += 1
            except Exception as e:
                logger.error(f"Attribute error at row {idx}: {e}\nRow data: {props}")
                raise
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
            f"{'Skipped (Empty Geom)':<25} | {total_stats['skipped_no_geom_data']:,}"
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

    def preprocess_graph(self) -> None:
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
                    # Robust check for spurious edges: Order 1, not named, not special watershed
                    stream_order = edge["stream_order"]
                    watershed_code = edge["fwa_watershed_code"] or ""
                    gnis_name = edge["gnis_name"] or ""
                    # Check conditions directly (stream_order is int or None, others are strings)
                    is_unnamed = not gnis_name or gnis_name.strip() == ""
                    is_order1 = stream_order == 1
                    is_special_wshed = watershed_code.startswith("9")
                    if is_order1 and not is_special_wshed and is_unnamed:
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

    def propagate_names_by_watershed(self) -> None:
        """
        Populate missing GNIS names using Blue Line Key backfill only.

        Logic:
        1. Index all known (name, gnis_id) pairs by (watershed_code, blue_line_key).
        2. For each unnamed edge whose (wc, blk) has EXACTLY ONE unique name,
           assign that name. Otherwise leave unnamed.

        No traversal / BFS / dominant-name fallback — unnamed edges that
        cannot be resolved by BLK stay unnamed. Downstream consumers
        (linking, FeatureNameVariation) handle naming for those.
        """
        logger.info("STEP 3: PROPAGATING GNIS NAMES (Blue Line Key only)")

        # --- 1. Pre-calculate Name Stats per (WC, BLK) ---
        blk_name_stats = {}  # (wc, blk) -> Counter of (name, gnis_id)

        for edge in self.G.es:
            wc = edge["fwa_watershed_code"]
            name = edge["gnis_name"]
            blk = edge["blue_line_key"]

            if wc and name and blk:
                key = (wc, blk)
                if key not in blk_name_stats:
                    blk_name_stats[key] = Counter()
                blk_name_stats[key][(name, edge["gnis_id"])] += 1

        # --- 2. Group Unnamed Edges by (WC, BLK) ---
        unnamed_by_blk = {}  # (wc, blk) -> [edge_index, ...]
        for edge in self.G.es:
            if edge["fwa_watershed_code"] and not edge["gnis_name"]:
                blk = edge["blue_line_key"]
                if blk:
                    key = (edge["fwa_watershed_code"], blk)
                    if key not in unnamed_by_blk:
                        unnamed_by_blk[key] = []
                    unnamed_by_blk[key].append(edge.index)

        blk_assigned = 0

        # --- 3. Assign where BLK has exactly one unique name ---
        for (wc, blk), edge_indices in tqdm(
            unnamed_by_blk.items(), desc="   BLK Backfill", unit="blk"
        ):
            key = (wc, blk)
            if key in blk_name_stats and len(blk_name_stats[key]) == 1:
                (blk_name, blk_id), _ = blk_name_stats[key].most_common(1)[0]
                for idx in edge_indices:
                    self.G.es[idx]["gnis_name"] = blk_name
                    self.G.es[idx]["gnis_id"] = blk_id
                    blk_assigned += 1

        logger.info(f"BLK Backfill Complete: {blk_assigned:,} edges assigned.")

    def annotate_unnamed_context(self) -> None:
        """Annotate unnamed edges with watershed-code name context.

        For each edge that is STILL unnamed after BLK backfill and whose
        watershed code has at least one named edge, stores:

        - ``wc_gnis_names``  – list of {gnis_name, gnis_id} dicts for every
          unique name in the same watershed code.
        - ``inherited_gnis_names`` – resolved name(s) for the unnamed edge:
            - ``None`` if the edge already has a name or has no WC peers.
            - The single WC name when only one exists.
            - The closest upstream named edge(s) (same-WC only) when
              multiple WC names exist.

        Upstream BFS rules:
          - Only traverses edges whose ``fwa_watershed_code`` matches the
            starting edge.  A different WC is simply not entered (no
            wasted computation).
          - Stops *at* a named edge (records it) but does NOT walk past
            it — further upstream of a named edge is irrelevant.
          - Once the first named edge is found at distance *d*, the BFS
            **continues draining every remaining branch at distance d**
            so that all equally-close named edges are collected, even if
            they carry a different gnis_name.
          - Results are deduplicated by (gnis_name, gnis_id).
        """
        import time

        logger.info("STEP 3b: ANNOTATING UNNAMED EDGE CONTEXT")
        start = time.time()

        # --- 1. Index all (gnis_name, gnis_id) by watershed code ---
        wc_names: Dict[str, Set[Tuple[str, str]]] = {}  # wc -> {(name, id), ...}
        for edge in self.G.es:
            wc = edge["fwa_watershed_code"]
            name = edge["gnis_name"]
            if wc and name:
                if wc not in wc_names:
                    wc_names[wc] = set()
                wc_names[wc].add((name, edge["gnis_id"] or ""))

        # --- 2. Collect unnamed edges that have at least one named WC peer ---
        unnamed_edges = []
        for edge in self.G.es:
            if not edge["gnis_name"] and edge["fwa_watershed_code"]:
                wc = edge["fwa_watershed_code"]
                if wc in wc_names:
                    unnamed_edges.append(edge.index)

        if not unnamed_edges:
            logger.info("  No unnamed edges with named WC peers — nothing to annotate.")
            return

        # --- 3. Build reverse adjacency (target node → [edge_idx]) for upstream walk ---
        reverse_adj: Dict[int, List[int]] = {}
        for edge in self.G.es:
            tgt = edge.target
            if tgt not in reverse_adj:
                reverse_adj[tgt] = []
            reverse_adj[tgt].append(edge.index)

        # --- 4. Upstream BFS helper ---
        def _bfs_upstream_named(start_node: int, wc: str) -> List[dict]:
            """Walk upstream along same-WC edges until the closest named
            edge(s) are found.

            Guarantees:
              - Different-WC branches are never entered.
              - Named edges are recorded but not traversed past.
              - ALL branches at the found distance are fully drained so
                no equally-close named edge is missed.

            Returns:
                List of {gnis_name, gnis_id} for every equally-close
                named edge, deduplicated.  Empty list if none reachable.
            """
            visited_nodes: Set[int] = {start_node}
            queue: deque = deque([(start_node, 0)])
            found_dist: Optional[int] = None
            results: List[dict] = []

            while queue:
                node, dist = queue.popleft()

                # Already found result(s) at a closer distance — skip
                if found_dist is not None and dist > found_dist:
                    continue

                for eidx in reverse_adj.get(node, []):
                    e = self.G.es[eidx]

                    # Different WC — don't enter this branch at all
                    if e["fwa_watershed_code"] != wc:
                        continue

                    if e["gnis_name"]:
                        # Named edge at dist+1 — record it
                        if found_dist is None:
                            found_dist = dist + 1
                        if dist + 1 == found_dist:
                            results.append(
                                {
                                    "gnis_name": e["gnis_name"],
                                    "gnis_id": e["gnis_id"] or "",
                                }
                            )
                        # Do NOT enqueue e.source — we stop at named edges
                        continue

                    # Unnamed same-WC edge — keep walking upstream
                    next_node = e.source
                    if next_node not in visited_nodes:
                        visited_nodes.add(next_node)
                        queue.append((next_node, dist + 1))

            # Deduplicate (parallel edges between same nodes)
            seen: Set[Tuple[str, str]] = set()
            deduped: List[dict] = []
            for r in results:
                key = (r["gnis_name"], r["gnis_id"])
                if key not in seen:
                    seen.add(key)
                    deduped.append(r)
            return deduped

        # --- 5. Annotate each unnamed edge ---
        num_edges = self.G.ecount()
        self.G.es["wc_gnis_names"] = [None] * num_edges
        self.G.es["inherited_gnis_names"] = [None] * num_edges

        annotated = 0
        inherited_single = 0
        inherited_bfs = 0

        for idx in tqdm(unnamed_edges, desc="   Annotating unnamed", unit="edge"):
            edge = self.G.es[idx]
            wc = edge["fwa_watershed_code"]
            names_in_wc = sorted(wc_names[wc])

            # Always store full WC name list
            edge["wc_gnis_names"] = [
                {"gnis_name": n, "gnis_id": gid} for n, gid in names_in_wc
            ]

            if len(names_in_wc) == 1:
                # Only one name in the entire WC — inherit directly
                n, gid = names_in_wc[0]
                edge["inherited_gnis_names"] = [{"gnis_name": n, "gnis_id": gid}]
                inherited_single += 1
            else:
                # Multiple WC names — BFS upstream to find the closest
                result = _bfs_upstream_named(edge.source, wc)
                if result:
                    edge["inherited_gnis_names"] = result
                    inherited_bfs += 1
                # If BFS finds nothing (dead end), inherited_gnis_names stays None

            annotated += 1

        elapsed = time.time() - start
        logger.info(
            f"  Annotated {annotated:,} unnamed edges. "
            f"Inherited: {inherited_single:,} (single WC name), "
            f"{inherited_bfs:,} (upstream BFS). Time: {elapsed:.1f}s"
        )

    def filter_unnamed_depth(self, threshold: int = 2) -> None:
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

    def export(self, filename: str = "fwa_bc_primal_full.gpickle") -> None:
        logger.info(f"STEP 5: EXPORTING")

        # Determine format from filename extension
        is_pickle_format = filename.endswith(".gpickle")
        is_graphml_format = filename.endswith(".graphml")

        if not (is_pickle_format or is_graphml_format):
            logger.warning(
                f"Unknown file extension for {filename}, defaulting to pickle"
            )
            is_pickle_format = True

        # Always save pickle (it's the canonical format)
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

        # Only save GraphML if explicitly requested
        if is_graphml_format:
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
    parser.add_argument(
        "--graphml",
        action="store_true",
        help="Export GraphML format (default: pickle only)",
    )
    args = parser.parse_args()

    print("=" * 80)
    print("BC FRESHWATER FISHING REGULATIONS - FWA GRAPH BUILDER")
    print("=" * 80)

    builder = FWAPrimalGraphIGraph()

    if args.pickle:
        print("\n📝 Input:")
        print(f"  Existing graph pickle: {args.pickle}")
        print()
        builder.load_from_pickle(args.pickle)
    else:

        print("\n📝 Input:")
        print(f"  GeoPackage: {builder.gpkg_path}")
        print(f"  Streams layer: {builder.streams_layer}")
        if args.layers:
            print(f"  Layers: {', '.join(args.layers)}")
        elif args.limit:
            print(f"  Layer limit: {args.limit}")
        else:
            print(f"  Layers: All (auto-detected)")

        print("\n💾 Output:")
        # Set filename before output section
        if args.graphml:
            filename = "fwa_bc_primal_full.graphml"
            if args.layers:
                suffix = "_".join(sorted(args.layers))
                filename = f"fwa_primal_{suffix}.graphml"
        else:
            filename = "fwa_bc_primal_full.gpickle"
            if args.layers:
                suffix = "_".join(sorted(args.layers))
                filename = f"fwa_primal_{suffix}.gpickle"

        if args.graphml:
            print(f"  GraphML: {builder.output_dir / filename}")
            print(
                f"  Pickle: {builder.output_dir / filename.replace('.graphml', '.gpickle')}"
            )
        else:
            print(f"  Pickle: {builder.output_dir / filename}")

        print("\n⚙️  Processing Options:")
        if args.filter_unnamed:
            print(f"  Filter unnamed streams: Yes (threshold={args.threshold})")
        else:
            print(f"  Filter unnamed streams: No")
        print()

        if builder.validate_paths():
            builder.build(limit=args.limit, specific_layers=args.layers)
            builder.preprocess_graph()
            builder.propagate_names_by_watershed()
            builder.annotate_unnamed_context()

            if args.filter_unnamed:
                builder.filter_unnamed_depth(threshold=args.threshold)

            builder.export(filename)
