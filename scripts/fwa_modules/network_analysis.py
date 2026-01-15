"""
Phase 3: Network Analysis - Tributary Relationships

Goals:
1. Build NetworkX graph with stream nodes and edges
2. Detect lake nodes (stream endpoints inside lake polygons)
3. Assign TRIBUTARY_OF via network traversal
4. Export graph for future inlet/outlet analysis

Memory Strategy:
- Graph stores only node positions and edge metadata (no geometries)
- Lake detection uses spatial index
- Network traversal is in-memory (fast)
- Export to GraphML for portability
"""

import gc
import json
import fiona
import geopandas as gpd
import pandas as pd
import networkx as nx
from pathlib import Path
from collections import deque
from typing import Dict, Set, Optional, Tuple
from shapely.geometry import Point
from .utils import clean_watershed_code, get_parent_code, setup_logging
from .models import StreamNode, StreamEdge, TributaryAssignment

logger = setup_logging(__name__)


class NetworkAnalyzer:
    """Analyzes stream network using NetworkX to determine tributary relationships."""

    def __init__(
        self,
        streams_gdb: Path,
        lakes_gdb: Path,
        output_tributary_map: Path,
        output_lake_segments: Path,
        output_graph: Path,
    ):
        """Initialize network analyzer.

        Args:
            streams_gdb: Path to preprocessed streams GeoPackage (.gpkg)
            lakes_gdb: Path to FWA_BC.gdb (for lake polygons)
            output_tributary_map: Path to save tributary relationships JSON
            output_lake_segments: Path to save lake segment assignments JSON
            output_graph: Path to save NetworkX graph (GraphML format)
        """
        self.streams_gdb = streams_gdb
        self.lakes_gdb = lakes_gdb
        self.output_tributary_map = output_tributary_map
        self.output_lake_segments = output_lake_segments
        self.output_graph = output_graph

        self.stats = {
            "total_streams": 0,
            "total_nodes": 0,
            "total_edges": 0,
            "named_streams": 0,
            "lake_nodes": 0,
            "tributaries_assigned": 0,
            "lake_tributaries": 0,
            "lake_segments": 0,
        }

    def build_network_graph(self) -> nx.DiGraph:
        """Build NetworkX directed graph from all stream layers.

        Creates nodes for stream endpoints and edges for stream segments.

        Returns:
            NetworkX DiGraph with StreamNode and StreamEdge objects
        """
        logger.info("Building stream network graph...")

        graph = nx.DiGraph()

        try:
            layers = fiona.listlayers(str(self.streams_gdb))
            total_layers = len(layers)

            for layer_idx, layer_name in enumerate(layers, 1):
                if layer_idx % 25 == 0:
                    logger.info(f"  Loading layer {layer_idx}/{total_layers}...")

                try:
                    # Load with geometries to extract endpoints in BC Albers
                    streams = gpd.read_file(str(self.streams_gdb), layer=layer_name)
                    if streams.crs != "EPSG:3005":
                        streams = streams.to_crs("EPSG:3005")

                    for idx, row in streams.iterrows():
                        feature_id = row.get("LINEAR_FEATURE_ID")
                        watershed_code = row.get("FWA_WATERSHED_CODE")
                        local_watershed_code = row.get("LOCAL_WATERSHED_CODE")
                        gnis_name = row.get("GNIS_NAME")
                        route_measure = row.get("DOWNSTREAM_ROUTE_MEASURE", 0.0)
                        geom = row.geometry

                        if (
                            not feature_id
                            or not isinstance(watershed_code, str)
                            or geom is None
                        ):
                            continue

                        clean_code = clean_watershed_code(watershed_code)
                        if not clean_code:
                            continue

                        # Clean LOCAL code too
                        local_code = None
                        if isinstance(local_watershed_code, str):
                            local_code = clean_watershed_code(local_watershed_code)

                        parent_code = get_parent_code(clean_code)

                        # Extract endpoints - handle both LineString and MultiLineString
                        try:
                            if geom.geom_type == "LineString":
                                coords = list(geom.coords)
                            elif geom.geom_type == "MultiLineString":
                                # Use first and last coordinates of the multi-part geometry
                                all_coords = []
                                for line in geom.geoms:
                                    all_coords.extend(list(line.coords))
                                coords = all_coords
                            else:
                                continue

                            if len(coords) < 2:
                                continue

                            start_point = coords[0]  # Upstream end
                            end_point = coords[-1]  # Downstream end
                        except Exception as e:
                            logger.debug(
                                f"Failed to extract coords for {feature_id}: {e}"
                            )
                            continue

                        # Create node IDs
                        start_node_id = f"{layer_name}_{idx}_start"
                        end_node_id = f"{layer_name}_{idx}_end"

                        # Create nodes
                        start_node = StreamNode(
                            node_id=start_node_id,
                            position=start_point,
                            watershed_code=watershed_code,
                        )

                        end_node = StreamNode(
                            node_id=end_node_id,
                            position=end_point,
                            watershed_code=watershed_code,
                        )

                        # Add nodes to graph
                        graph.add_node(start_node_id, data=start_node)
                        graph.add_node(end_node_id, data=end_node)

                        # Create edge
                        waterbody_key = row.get("WATERBODY_KEY")
                        edge = StreamEdge(
                            linear_feature_id=feature_id,
                            gnis_name=(
                                str(gnis_name)
                                if pd.notna(gnis_name) and str(gnis_name).strip()
                                else None
                            ),
                            clean_code=clean_code,
                            local_code=local_code,
                            parent_code=parent_code,
                            route_measure=(
                                float(route_measure) if pd.notna(route_measure) else 0.0
                            ),
                            waterbody_key=(
                                int(waterbody_key) if pd.notna(waterbody_key) else None
                            ),
                            layer_name=layer_name,
                            start_node_id=start_node_id,
                            end_node_id=end_node_id,
                        )

                        # Add edge (directed: start → end, flowing downstream)
                        graph.add_edge(start_node_id, end_node_id, data=edge)

                        self.stats["total_streams"] += 1
                        if edge.is_named:
                            self.stats["named_streams"] += 1

                    del streams
                    gc.collect()

                except Exception as e:
                    logger.warning(f"Failed to load layer {layer_name}: {e}")

            self.stats["total_nodes"] = graph.number_of_nodes()
            self.stats["total_edges"] = graph.number_of_edges()

            logger.info(f"Graph built (disconnected segments):")
            logger.info(f"  Nodes: {self.stats['total_nodes']:,}")
            logger.info(f"  Edges: {self.stats['total_edges']:,}")
            logger.info(f"  Named streams: {self.stats['named_streams']:,}")

            # Connect stream segments based on spatial proximity and watershed hierarchy
            logger.info("Connecting stream segments...")
            connections_made = self._connect_stream_segments(graph)
            logger.info(f"  Made {connections_made:,} connections between segments")

            self.stats["total_edges"] = graph.number_of_edges()
            logger.info(
                f"  Total edges after connection: {self.stats['total_edges']:,}"
            )

            return graph

        except Exception as e:
            logger.error(f"Failed to build network graph: {e}")
            return nx.DiGraph()

    def _connect_stream_segments(self, graph: nx.DiGraph) -> int:
        """Connect stream segments using FWA watershed codes and route measures.

        FWA topology:
        1. FWA_WATERSHED_CODE - Hierarchical waterway identifier
        2. LOCAL_WATERSHED_CODE - Position along parent waterway
        3. DOWNSTREAM_ROUTE_MEASURE - Distance from mouth

        Connection rules:
        - Segments with same FWA code are on same waterway (connect by route measure)
        - Tributary joins where its FWA code matches parent's LOCAL code
        - LOCAL code last non-zero value determines position (larger = more upstream)

        Args:
            graph: NetworkX graph with isolated stream segments

        Returns:
            Number of connections made
        """
        connections = 0

        # Build comprehensive lookup structures
        waterway_segments = {}  # FWA code -> list of segments
        all_segments = {}  # Maps node -> segment info

        for u, v, edge_attrs in graph.edges(data=True):
            edge_data = edge_attrs["data"]

            # Extract FWA and LOCAL codes (already cleaned in edge_data)
            fwa_code = edge_data.clean_code

            # Get original LOCAL code from cleaned streams GDB
            # We'll need to look it up per segment
            segment_info = {
                "fwa_code": fwa_code,
                "parent_code": edge_data.parent_code,
                "route_measure": edge_data.route_measure,
                "start_node": u,
                "end_node": v,
                "linear_id": edge_data.linear_feature_id,
                "edge_data": edge_data,
            }

            # Group by FWA code
            if fwa_code not in waterway_segments:
                waterway_segments[fwa_code] = []
            waterway_segments[fwa_code].append(segment_info)

            # Map both nodes to this segment
            all_segments[v] = segment_info  # Downstream end

        # Sort segments within each waterway by route measure
        for fwa_code in waterway_segments:
            waterway_segments[fwa_code].sort(key=lambda x: x["route_measure"])

        # PASS 1: Connect segments along same waterway (same FWA code)
        # Order: downstream to upstream by route measure
        # Connection direction: upstream_end → downstream_start (sequential segments)
        for fwa_code, segments in waterway_segments.items():
            for i in range(len(segments) - 1):
                downstream_seg = segments[i]  # Lower route measure (closer to mouth)
                upstream_seg = segments[
                    i + 1
                ]  # Higher route measure (farther from mouth)

                # Connect the END of upstream segment to the START of downstream segment
                # This creates a continuous flow path for BFS traversal
                graph.add_edge(upstream_seg["end_node"], downstream_seg["start_node"])
                connections += 1

        # PASS 2: Connect tributaries to parent waterways
        # Tributary's downstream end connects to parent's upstream portion
        for fwa_code, segments in waterway_segments.items():
            if not segments:
                continue

            entry_segment = segments[0]  # Most downstream (lowest route measure)
            parent_code = entry_segment["parent_code"]

            if not parent_code or parent_code not in waterway_segments:
                continue

            parent_segments = waterway_segments[parent_code]

            if parent_segments:
                # Connect tributary's downstream end to parent's most upstream start
                # This allows BFS from parent to traverse upstream into tributary
                parent_upstream = parent_segments[-1]  # Most upstream parent segment
                graph.add_edge(entry_segment["end_node"], parent_upstream["start_node"])
                connections += 1

        return connections

    def detect_lake_nodes(self, graph: nx.DiGraph) -> nx.DiGraph:
        """Detect which nodes are inside lake polygons.

        Updates node data with lake information.

        Args:
            graph: NetworkX graph with StreamNode objects

        Returns:
            Updated graph with lake node information
        """
        logger.info("Detecting lake nodes...")

        try:
            # Load lakes in BC Albers
            lakes = gpd.read_file(str(self.lakes_gdb), layer="FWA_LAKES_POLY")
            if lakes.crs != "EPSG:3005":
                lakes = lakes.to_crs("EPSG:3005")
            logger.info(f"  Loaded {len(lakes)} lake polygons")

            if lakes.empty:
                return graph

            # Build spatial index
            lake_index = lakes.sindex

            # Check each node
            lake_nodes_found = 0

            for node_id, node_attrs in list(graph.nodes(data=True)):
                node_data = node_attrs["data"]
                node_point = Point(node_data.position)

                # Find candidate lakes
                possible_lakes_idx = list(lake_index.intersection(node_point.bounds))

                if not possible_lakes_idx:
                    continue

                # Check if actually inside
                possible_lakes = lakes.iloc[possible_lakes_idx]

                for lake_idx, lake in possible_lakes.iterrows():
                    if lake.geometry.contains(node_point):
                        # Node is inside this lake
                        lake_name = lake.get("GNIS_NAME_1") or lake.get("GNIS_NAME")
                        lake_poly_id = lake.get("WATERBODY_POLY_ID")

                        if pd.notna(lake_name) and str(lake_name).strip():
                            # Update node data
                            updated_node = StreamNode(
                                node_id=node_data.node_id,
                                position=node_data.position,
                                watershed_code=node_data.watershed_code,
                                is_lake_node=True,
                                lake_name=str(lake_name),
                                lake_poly_id=(
                                    int(lake_poly_id)
                                    if pd.notna(lake_poly_id)
                                    else None
                                ),
                            )

                            graph.nodes[node_id]["data"] = updated_node
                            lake_nodes_found += 1

                        break  # Take first lake match

            self.stats["lake_nodes"] = lake_nodes_found
            logger.info(f"  Found {lake_nodes_found:,} nodes inside lakes")

        except Exception as e:
            logger.error(f"Failed to detect lake nodes: {e}")

        return graph

    def assign_tributaries(
        self, graph: nx.DiGraph, lake_segments: Dict[str, int]
    ) -> Dict[str, TributaryAssignment]:
        """Assign TRIBUTARY_OF relationships via hierarchy + network traversal.

        Three-phase approach:
        1. Named streams: Use watershed hierarchy to find parent stream
        2. Unnamed streams: Use BFS from named features
        3. Lake corrections: Override with lake name if flows into lake

        Args:
            graph: NetworkX graph with lake node data
            lake_segments: Dict mapping linear_feature_id -> lake_poly_id for lake segments

        Returns:
            Dict mapping linear_feature_id -> TributaryAssignment
        """
        logger.info("Assigning tributary relationships...")

        tributary_map = {}

        # Build lookup for lake names
        try:
            lakes = gpd.read_file(str(self.lakes_gdb), layer="FWA_LAKES_POLY")
            lake_name_map = {}
            for _, lake in lakes.iterrows():
                poly_id = lake.get("WATERBODY_POLY_ID")
                name = lake.get("GNIS_NAME_1")
                if pd.notna(poly_id) and pd.notna(name):
                    lake_name_map[int(poly_id)] = name
        except Exception as e:
            logger.warning(f"Could not load lake names: {e}")
            lake_name_map = {}

        # PHASE 1: Assign named streams based on watershed hierarchy
        logger.info("  Phase 1: Assigning named streams via hierarchy...")

        # Build map of parent_code -> stream name for named streams
        parent_name_map = {}
        for u, v, edge_attrs in graph.edges(data=True):
            # Skip connection edges
            if "data" not in edge_attrs:
                continue
            edge_data = edge_attrs["data"]
            if edge_data.is_named and edge_data.clean_code:
                parent_name_map[edge_data.clean_code] = edge_data.gnis_name

        # Assign named streams to their parent stream
        named_assigned = 0
        for u, v, edge_attrs in graph.edges(data=True):
            # Skip connection edges
            if "data" not in edge_attrs:
                continue
            edge_data = edge_attrs["data"]

            if edge_data.is_named and edge_data.parent_code:
                # Look up parent stream name
                parent_name = parent_name_map.get(edge_data.parent_code)

                if parent_name and parent_name != edge_data.gnis_name:
                    # This named stream is a tributary of a different named stream
                    tributary_map[edge_data.linear_feature_id] = TributaryAssignment(
                        linear_feature_id=edge_data.linear_feature_id,
                        tributary_of=parent_name,
                    )
                    named_assigned += 1

        logger.info(f"    Assigned {named_assigned:,} named stream tributaries")

        # PHASE 2: BFS for unnamed streams only
        logger.info("  Phase 2: Assigning unnamed streams via BFS...")

        visited_edges = set(tributary_map.keys())  # Already assigned edges

        # Collect BFS sources: all named streams, lake nodes, and lake segment upstream nodes
        named_sources = []

        for u, v, edge_attrs in graph.edges(data=True):
            # Skip connection edges
            if "data" not in edge_attrs:
                continue
            edge_data = edge_attrs["data"]

            if edge_data.is_named:
                named_sources.append(
                    (v, edge_data.gnis_name, "stream", edge_data.clean_code)
                )

            # Check if downstream node is a lake node
            end_node = graph.nodes[v]["data"]
            if end_node.is_lake_node:
                named_sources.append((v, end_node.lake_name, "lake", None))

        # Add lake BFS sources: Use WATERBODY_KEY to find ALL streams in the lake waterbody
        # Then BFS upstream from those to find tributaries
        lake_sources = []
        lake_waterbody_streams = {}  # lake_poly_id -> list of (node, stream_id) tuples

        # First, map lake_poly_id to lake WATERBODY_KEY
        # Lake segments have the lake's WATERBODY_KEY
        lake_waterbody_keys = {}  # lake_poly_id -> waterbody_key
        for lake_segment_id, lake_poly_id in lake_segments.items():
            for u, v, edge_attrs in graph.edges(data=True):
                if "data" not in edge_attrs:
                    continue
                edge_data = edge_attrs["data"]
                if (
                    edge_data.linear_feature_id == lake_segment_id
                    and edge_data.waterbody_key
                ):
                    lake_waterbody_keys[lake_poly_id] = edge_data.waterbody_key
                    break

        logger.info(f"    Found WATERBODY_KEYs for {len(lake_waterbody_keys)} lakes")

        # Now find ALL streams with matching WATERBODY_KEY (not just lake segments)
        for u, v, edge_attrs in graph.edges(data=True):
            if "data" not in edge_attrs:
                continue
            edge_data = edge_attrs["data"]

            # Check if this stream's waterbody_key matches any lake
            for lake_poly_id, waterbody_key in lake_waterbody_keys.items():
                if edge_data.waterbody_key == waterbody_key:
                    if lake_poly_id not in lake_waterbody_streams:
                        lake_waterbody_streams[lake_poly_id] = []
                    # Use upstream node as BFS source to find tributaries feeding this stream
                    lake_waterbody_streams[lake_poly_id].append(
                        (u, edge_data.linear_feature_id)
                    )

        # Convert to sources list
        for lake_poly_id, stream_list in lake_waterbody_streams.items():
            lake_name = lake_name_map.get(lake_poly_id, f"Lake {lake_poly_id}")
            for node, stream_id in stream_list:
                lake_sources.append((node, lake_name, "lake", lake_poly_id))

        # Build map of lake_poly_id -> set of stream names flowing through that lake
        lake_stream_names_map = {}  # lake_poly_id -> set of stream names
        for lake_poly_id, waterbody_key in lake_waterbody_keys.items():
            names = set()
            for u, v, edge_attrs in graph.edges(data=True):
                if "data" not in edge_attrs:
                    continue
                edge_data = edge_attrs["data"]
                if edge_data.waterbody_key == waterbody_key and edge_data.is_named:
                    names.add(edge_data.gnis_name)
            lake_stream_names_map[lake_poly_id] = names

        logger.info(
            f"    Found {len(lake_sources)} streams with lake WATERBODY_KEY as BFS sources"
        )

        # Sort sources by watershed depth (deeper = smaller tributaries = process first)
        # This ensures smaller tributaries claim their direct tributaries before larger rivers
        def get_depth(source_tuple):
            source_node, source_name, source_type, clean_code = source_tuple
            if clean_code:
                return clean_code.count("-")
            return 0  # Non-code sources (lakes from nodes) process first

        named_sources.sort(key=get_depth, reverse=True)

        logger.info(f"    Processing {len(named_sources)} named stream BFS sources")

        # BFS from each named source (only process unnamed edges)
        unnamed_assigned = 0

        for source_node, source_name, source_type, _ in named_sources:
            queue = deque([source_node])
            local_visited = set()

            while queue:
                current_node = queue.popleft()

                if current_node in local_visited:
                    continue
                local_visited.add(current_node)

                # Find upstream edges (predecessors)
                for pred_node in graph.predecessors(current_node):
                    edge_attrs = graph[pred_node][current_node]

                    # Skip connection edges (no data attribute) but continue traversal
                    if "data" not in edge_attrs:
                        queue.append(pred_node)
                        continue

                    edge_data = edge_attrs["data"]
                    edge_id = edge_data.linear_feature_id

                    # Only ASSIGN UNNAMED edges that haven't been visited yet
                    if not edge_data.is_named and edge_id not in visited_edges:
                        tributary_map[edge_id] = TributaryAssignment(
                            linear_feature_id=edge_id, tributary_of=source_name
                        )
                        visited_edges.add(edge_id)
                        unnamed_assigned += 1

                    # CRITICAL: Always continue upstream traversal, even if edge was already visited
                    # This allows us to traverse through named streams to find unnamed tributaries
                    queue.append(pred_node)

        # Process lake sources LAST with ability to override
        logger.info(
            f"    Processing {len(lake_sources)} lake segment BFS sources (with override)"
        )
        lake_override_count = 0

        # FIRST: Assign all lake segments (both endpoints in lake) to the lake name
        logger.info(f"    Assigning {len(lake_segments)} lake segments to their lakes")

        for lake_segment_id, lake_poly_id in lake_segments.items():
            lake_name = lake_name_map.get(lake_poly_id, f"Lake {lake_poly_id}")
            was_assigned = lake_segment_id in tributary_map

            tributary_map[lake_segment_id] = TributaryAssignment(
                linear_feature_id=lake_segment_id,
                tributary_of=lake_name,
                lake_poly_id=None,  # Lake segments get poly ID from lake_segment dict in output
            )

            if not was_assigned:
                unnamed_assigned += 1
            else:
                lake_override_count += 1

            visited_edges.add(lake_segment_id)
            self.stats["lake_tributaries"] += 1

        # SECOND: Assign ALL streams with lake WATERBODY_KEY to the lake
        # (includes lake segments but also streams touching/entering the lake)
        waterbody_stream_count = 0
        for u, v, edge_attrs in graph.edges(data=True):
            if "data" not in edge_attrs:
                continue
            edge_data = edge_attrs["data"]

            # Check if this stream's waterbody_key matches any lake
            for lake_poly_id, waterbody_key in lake_waterbody_keys.items():
                if edge_data.waterbody_key == waterbody_key:
                    lake_name = lake_name_map.get(lake_poly_id, f"Lake {lake_poly_id}")
                    was_assigned = edge_data.linear_feature_id in tributary_map

                    # ALWAYS override - streams in lake waterbody belong to the lake
                    tributary_map[edge_data.linear_feature_id] = TributaryAssignment(
                        linear_feature_id=edge_data.linear_feature_id,
                        tributary_of=lake_name,
                        lake_poly_id=None,
                    )

                    if was_assigned:
                        lake_override_count += 1
                    else:
                        unnamed_assigned += 1

                    visited_edges.add(edge_data.linear_feature_id)
                    self.stats["lake_tributaries"] += 1
                    waterbody_stream_count += 1
                    break

        logger.info(
            f"    Assigned {waterbody_stream_count} streams with lake WATERBODY_KEY (overrode {lake_override_count} previous assignments)"
        )

        # THEN: BFS from those streams to find upstream tributaries
        total_lake_tribs_found = 0

        # Now do the actual BFS
        for source_node, source_name, source_type, lake_poly_id in lake_sources:
            queue = deque([(source_node, None)])  # (node, current_stream_name)
            local_visited = set()
            this_source_tribs = 0

            # Get the stream names that flow through this lake
            lake_stream_names = lake_stream_names_map.get(lake_poly_id, set())

            while queue:
                current_node, current_stream = queue.popleft()

                if current_node in local_visited:
                    continue
                local_visited.add(current_node)

                # Find upstream edges (predecessors)
                for pred_node in graph.predecessors(current_node):
                    edge_attrs = graph[pred_node][current_node]

                    # Skip connection edges (no data attribute) but continue traversal
                    if "data" not in edge_attrs:
                        queue.append((pred_node, current_stream))
                        continue

                    edge_data = edge_attrs["data"]
                    edge_id = edge_data.linear_feature_id

                    # Skip streams with lake WATERBODY_KEY (already assigned above)
                    skip_waterbody = False
                    for lake_poly_id, waterbody_key in lake_waterbody_keys.items():
                        if edge_data.waterbody_key == waterbody_key:
                            skip_waterbody = True
                            break

                    if skip_waterbody:
                        continue

                    # Determine assignment logic based on stream type and context
                    should_assign = False
                    next_stream = current_stream

                    if edge_data.is_named:
                        # Named stream - check if it flows through the lake
                        if edge_data.gnis_name in lake_stream_names:
                            # This named stream flows through lake - assign to lake
                            should_assign = True
                            next_stream = edge_data.gnis_name
                        else:
                            # Different named stream - STOP (it's a tributary of current_stream or lake)
                            should_assign = False
                    else:
                        # Unnamed stream
                        if current_stream is None:
                            # We're not inside a named stream system - assign to lake
                            should_assign = True
                        else:
                            # We're inside a named stream system (current_stream) - DON'T assign
                            # This unnamed stream should be a tributary of current_stream, not the lake
                            should_assign = False

                    if should_assign:
                        # Override with lake name (tributaries outside lake get NO lake_poly_id)
                        was_assigned = edge_id in tributary_map

                        tributary_map[edge_id] = TributaryAssignment(
                            linear_feature_id=edge_id,
                            tributary_of=source_name,
                            lake_poly_id=None,  # Only actual lake segments get poly ID
                        )

                        if not was_assigned:
                            unnamed_assigned += 1
                        else:
                            lake_override_count += 1

                        visited_edges.add(edge_id)
                        self.stats["lake_tributaries"] += 1
                        this_source_tribs += 1

                        # Continue upstream
                        queue.append((pred_node, next_stream))

            if this_source_tribs > 0:
                total_lake_tribs_found += this_source_tribs

        logger.info(
            f"    Lake BFS assigned {total_lake_tribs_found} additional tributaries (overrode {lake_override_count} previous assignments)"
        )

        logger.info(f"    Assigned {unnamed_assigned:,} unnamed stream tributaries")

        self.stats["tributaries_assigned"] = named_assigned + unnamed_assigned
        logger.info(f"  Total tributaries: {self.stats['tributaries_assigned']:,}")
        logger.info(f"  Lake tributaries: {self.stats['lake_tributaries']:,}")

        return tributary_map

    def find_lake_segments(self, graph: nx.DiGraph) -> Dict[str, int]:
        """Find stream segments that pass through lakes.

        A segment is a lake segment if:
        1. Both endpoints are in the same lake, OR
        2. The stream geometry intersects a lake and >50% overlaps

        Args:
            graph: NetworkX graph with lake node data

        Returns:
            Dict mapping linear_feature_id -> lake_poly_id
        """
        logger.info("Finding lake segments...")

        lake_segment_map = {}

        # First pass: Check endpoint-based lake segments
        for u, v, edge_attrs in graph.edges(data=True):
            # Skip connection edges (no data attribute)
            if "data" not in edge_attrs:
                continue

            edge_data = edge_attrs["data"]
            start_node = graph.nodes[u]["data"]
            end_node = graph.nodes[v]["data"]

            # Check if both endpoints are in the same lake
            if start_node.is_lake_node and end_node.is_lake_node:
                if (
                    start_node.lake_poly_id == end_node.lake_poly_id
                    and start_node.lake_poly_id is not None
                ):
                    lake_segment_map[edge_data.linear_feature_id] = (
                        start_node.lake_poly_id
                    )
                    self.stats["lake_segments"] += 1

        logger.info(
            f"  Found {self.stats['lake_segments']:,} lake segments (endpoint-based)"
        )

        return lake_segment_map

    def export_graph(self, graph: nx.DiGraph):
        """Export graph to GraphML format for future analysis.

        Args:
            graph: NetworkX graph to export
        """
        logger.info("Exporting graph to GraphML...")

        try:
            # Convert attrs objects to dicts for serialization
            export_graph = nx.DiGraph()

            for node_id, node_attrs in graph.nodes(data=True):
                node_data = node_attrs["data"]
                export_graph.add_node(
                    node_id,
                    x=node_data.position[0],
                    y=node_data.position[1],
                    watershed_code=node_data.watershed_code,
                    is_lake_node=node_data.is_lake_node,
                    lake_name=node_data.lake_name or "",
                    lake_poly_id=node_data.lake_poly_id or 0,
                )

            for u, v, edge_attrs in graph.edges(data=True):
                # Skip connection edges (no stream data)
                if "data" not in edge_attrs:
                    continue
                edge_data = edge_attrs["data"]
                export_graph.add_edge(
                    u,
                    v,
                    linear_feature_id=edge_data.linear_feature_id,
                    gnis_name=edge_data.gnis_name or "",
                    clean_code=edge_data.clean_code,
                    parent_code=edge_data.parent_code or "",
                    route_measure=edge_data.route_measure,
                    layer_name=edge_data.layer_name,
                )

            # Write to GraphML
            self.output_graph.parent.mkdir(parents=True, exist_ok=True)
            nx.write_graphml(export_graph, str(self.output_graph))

            logger.info(f"Graph exported to: {self.output_graph}")

        except Exception as e:
            logger.error(f"Failed to export graph: {e}")

    def run(self) -> Tuple[Path, Path, Path]:
        """Execute network analysis.

        Returns:
            Tuple of (tributary_map_path, lake_segments_path, graph_path)
        """
        logger.info("=== Phase 3: Network Analysis ===")

        # Build network graph
        graph = self.build_network_graph()

        if graph.number_of_nodes() == 0:
            logger.error("Failed to build network graph")
            return None, None, None

        # Detect lake nodes
        graph = self.detect_lake_nodes(graph)

        # CRITICAL: Find lake segments BEFORE assigning tributaries
        # This allows lake segments to be used as BFS sources for upstream propagation
        lake_segments = self.find_lake_segments(graph)

        # Assign tributaries (using lake segments as additional sources)
        tributary_assignments = self.assign_tributaries(graph, lake_segments)

        # Mark outlet segments (segments flowing to ocean/border) as "Tailwater"
        # NOTE: out_degree == 0 means HEADWATER (no downstream flow)
        # Outlets are nodes with in_degree == 0 (no upstream flow into them)
        logger.info("Marking outlet segments as tributary of Tailwater...")
        outlet_count = 0
        for node_id in graph.nodes():
            # True outlets have incoming edges but no outgoing edges
            # AND are not already part of the network (isolated endpoints)
            if graph.in_degree(node_id) > 0 and graph.out_degree(node_id) == 0:
                # This is a true outlet node (flows off map or to ocean)
                # Mark edges ENDING at this node as Tailwater
                for pred_id in graph.predecessors(node_id):
                    edge_attrs = graph[pred_id][node_id]

                    # Skip connection edges (no data attribute)
                    if "data" not in edge_attrs:
                        continue

                    edge_data = edge_attrs["data"]
                    feature_id = edge_data.linear_feature_id

                    # Only mark as Tailwater if not already assigned
                    if feature_id not in tributary_assignments:
                        tributary_assignments[feature_id] = TributaryAssignment(
                            linear_feature_id=feature_id, tributary_of="Tailwater"
                        )
                        outlet_count += 1

        logger.info(
            f"  Marked {outlet_count} outlet segments as tributary of Tailwater"
        )

        # Merge lake segments into tributary assignments
        for feature_id, lake_poly_id in lake_segments.items():
            if feature_id in tributary_assignments:
                tributary_assignments[feature_id].lake_poly_id = lake_poly_id
            else:
                tributary_assignments[feature_id] = TributaryAssignment(
                    linear_feature_id=feature_id, lake_poly_id=lake_poly_id
                )

        # Convert to JSON-serializable format
        tributary_map_json = {
            feature_id: {
                "tributary_of": assignment.tributary_of,
                "lake_poly_id": assignment.lake_poly_id,
            }
            for feature_id, assignment in tributary_assignments.items()
        }

        # Save outputs
        self.output_tributary_map.parent.mkdir(parents=True, exist_ok=True)

        with open(self.output_tributary_map, "w") as f:
            json.dump(tributary_map_json, f, indent=2)
        logger.info(f"Tributary map saved to: {self.output_tributary_map}")

        with open(self.output_lake_segments, "w") as f:
            json.dump(lake_segments, f, indent=2)
        logger.info(f"Lake segments saved to: {self.output_lake_segments}")

        # Export graph
        self.export_graph(graph)

        # Final stats
        logger.info("=== Network Analysis Complete ===")
        logger.info(f"  Total streams: {self.stats['total_streams']:,}")
        logger.info(f"  Graph nodes: {self.stats['total_nodes']:,}")
        logger.info(f"  Graph edges: {self.stats['total_edges']:,}")
        logger.info(f"  Named streams: {self.stats['named_streams']:,}")
        logger.info(f"  Lake nodes: {self.stats['lake_nodes']:,}")
        logger.info(f"  Tributaries assigned: {self.stats['tributaries_assigned']:,}")
        logger.info(f"  Lake tributaries: {self.stats['lake_tributaries']:,}")
        logger.info(f"  Lake segments: {self.stats['lake_segments']:,}")

        return self.output_tributary_map, self.output_lake_segments, self.output_graph
