#!/usr/bin/env python3
"""
FWA Graph Processor (Primal Graph Approach)
"""

import os, sys, logging, warnings, argparse
import geopandas as gpd
import pandas as pd
import networkx as nx
import fiona
from pathlib import Path
from shapely.geometry import LineString, MultiLineString

warnings.filterwarnings("ignore")
os.environ["GDAL_SKIP"] = "DXF"
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


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

    def build(self, limit=None):
        logger.info("Step 1: Loading Segments...")

        try:
            layers = fiona.listlayers(str(self.streams_gdb))
            layers = [l for l in layers if not l.startswith("_") and len(l) <= 4]
            if limit:
                layers = layers[:limit]
        except Exception:
            return

        print(f"Processing {len(layers)} layers...", end=" ")

        for i, layer in enumerate(layers):
            try:
                gdf = gpd.read_file(str(self.streams_gdb), layer=layer)

                cols = gdf.columns
                if "LINEAR_FEATURE_ID" not in cols or "geometry" not in cols:
                    continue

                # Preprocess the layer
                gdf = self.preprocess_layer(gdf)

                if len(gdf) == 0:
                    continue

                ids = gdf["LINEAR_FEATURE_ID"].values
                codes = gdf["FWA_WATERSHED_CODE"].values
                geoms = gdf["geometry"].values
                names = (
                    gdf["GNIS_NAME"].values
                    if "GNIS_NAME" in cols
                    else [None] * len(gdf)
                )
                wbs = (
                    gdf["WATERBODY_KEY"].values
                    if "WATERBODY_KEY" in cols
                    else [None] * len(gdf)
                )
                sources = (
                    gdf["FEATURE_SOURCE"].values
                    if "FEATURE_SOURCE" in cols
                    else [None] * len(gdf)
                )
                stream_orders = (
                    gdf["STREAM_ORDER"].values
                    if "STREAM_ORDER" in cols
                    else [None] * len(gdf)
                )

                for lf_id, code, geom, name, wb, source, stream_order in zip(
                    ids, codes, geoms, names, wbs, sources, stream_orders
                ):
                    if geom is None or geom.is_empty:
                        continue

                    # DEBUG: Track specific feature
                    debug_this = pd.notna(lf_id) and int(lf_id) == 701309885

                    if debug_this:
                        logger.info(f"\n{'='*60}")
                        logger.info(f"DEBUG: Found LINEAR_FEATURE_ID = 701309885")
                        logger.info(f"  Layer: {layer}")
                        logger.info(f"  FWA_WATERSHED_CODE: {code}")
                        logger.info(f"  GNIS_NAME: {name}")
                        logger.info(f"  WATERBODY_KEY: {wb}")
                        logger.info(f"  Geometry type: {type(geom).__name__}")
                        if isinstance(geom, LineString):
                            coords = list(geom.coords)
                            logger.info(f"  Total coordinates: {len(coords)}")
                            logger.info(f"  First coord (Start/Source): {coords[0]}")
                            logger.info(f"  Last coord (End/Mouth): {coords[-1]}")
                        elif isinstance(geom, MultiLineString):
                            logger.info(f"  Number of LineStrings: {len(geom.geoms)}")
                            logger.info(
                                f"  First LineString start: {geom.geoms[0].coords[0]}"
                            )
                            logger.info(
                                f"  Last LineString end: {geom.geoms[-1].coords[-1]}"
                            )

                    # u = Start (Source), v = End (Mouth)
                    u, v, u_coord, v_coord = self.get_endpoints(geom)
                    if not u:
                        continue

                    if debug_this:
                        logger.info(f"  Extracted endpoints:")
                        logger.info(f"    u (Start/Source) node: {u} at {u_coord}")
                        logger.info(f"    v (End/Mouth) node: {v} at {v_coord}")

                    # Nodes (Size 0.1)
                    if u not in self.G:
                        self.G.add_node(
                            u, x=u_coord[0], y=u_coord[1], size=0.1, label=""
                        )
                    if v not in self.G:
                        self.G.add_node(
                            v, x=v_coord[0], y=v_coord[1], size=0.1, label=""
                        )

                    # Attributes
                    sid = str(int(lf_id)) if pd.notna(lf_id) else f"UNK-{i}"
                    s_code = str(code) if pd.notna(code) else ""

                    waterbody_key = ""
                    lake_name_val = ""

                    if pd.notna(wb) and int(wb) != 0:
                        waterbody_key = str(int(wb))
                        try:
                            k = int(wb)
                            if k in self.lake_lookup:
                                lake_name_val = self.lake_lookup[k]
                                if pd.isna(name):
                                    name = lake_name_val
                        except:
                            pass

                    edge_name = name if pd.notna(name) else ""

                    # Count parallel edges for visual offset
                    edge_key_pair = (v, u)
                    parallel_count = self.edge_counts.get(edge_key_pair, 0)
                    self.edge_counts[edge_key_pair] = parallel_count + 1

                    visual_offset = parallel_count * 0.02

                    # EDGE CREATION: v -> u (Mouth -> Source / Upstream)
                    # CRITICAL FIX: Explicitly set 'key' to the unique ID (sid)
                    # This ensures GraphML export uses this ID instead of "0"
                    self.G.add_edge(
                        v,
                        u,
                        key=sid,
                        id=sid,
                        linear_feature_id=sid,
                        fwa_code=s_code,
                        fwa_code_clean=self.clean_code(s_code),
                        name=edge_name,
                        waterbody_key=waterbody_key,
                        lake_name=lake_name_val,
                        stream_order=(
                            int(stream_order) if pd.notna(stream_order) else None
                        ),
                        length=geom.length,
                        thickness=10.0,
                        tributary_of="",
                        weight=1.0 + visual_offset,
                        edge_index=parallel_count,
                    )

                    if debug_this:
                        logger.info(f"  Edge added to graph:")
                        logger.info(
                            f"    Direction: {v} -> {u} (Mouth -> Source / Upstream)"
                        )
                        logger.info(f"    From: {v} at {v_coord}")
                        logger.info(f"    To: {u} at {u_coord}")
                        logger.info(
                            f"    Edge attributes: name='{edge_name}', fwa_code='{s_code}'"
                        )
                        logger.info(f"{'='*60}\n")

            except Exception as e:
                pass

            if i % 10 == 0:
                print(".", end="", flush=True)

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

        logger.info(f"Stream order distribution of edges to roots: {dict(sorted(order_counts.items()))}")

        # Remove the spurious edges
        for u, v, key in edges_to_remove:
            self.G.remove_edge(u, v, key)

        # Remove isolated nodes that may have been created
        # An isolated node has no edges connected to it (degree 0)
        isolated_nodes = list(nx.isolates(self.G))
        self.G.remove_nodes_from(isolated_nodes)

        logger.info(
            f"Removed {len(edges_to_remove)} order-1 edges leading to roots."
        )
        logger.info(
            f"Removed {len(isolated_nodes)} isolated nodes (nodes with no connections after edge removal)."
        )
        logger.info(
            f"Graph after cleanup: {self.G.number_of_nodes():,} Nodes, {self.G.number_of_edges():,} Edges"
        )

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
                    name = data.get("name", "")
                    lake = data.get("lake_name", "")

                    data["tributary_of"] = "Tailwater"

                    upstream_tributary = "Tailwater"
                    if lake:
                        upstream_tributary = lake

                    stack.append((v, upstream_tributary, data, edge_key))

        while stack:
            u, inherited_tributary, downstream_data, downstream_key = stack.pop()

            for v in self.G.predecessors(u):
                for edge_key in self.G[v][u]:
                    edge_id = (v, u, edge_key)
                    if edge_id in visited_edges:
                        continue
                    visited_edges.add(edge_id)

                    data = self.G.edges[v, u, edge_key]
                    current_name = data.get("name", "")
                    current_lake = data.get("lake_name", "")
                    downstream_name = downstream_data.get("name", "")
                    downstream_lake = downstream_data.get("lake_name", "")
                    downstream_tributary_of = downstream_data.get("tributary_of", "")

                    # Track if we're in/near a lake (to prevent name from overriding lake tributary)
                    in_lake_context = False

                    # Logic matches your requested priority
                    # If current stream is inside a lake OR flows into a lake, it's tributary of that lake
                    if current_lake or downstream_lake:
                        lake_name = current_lake if current_lake else downstream_lake
                        data["tributary_of"] = lake_name
                        upstream_tributary = lake_name
                        in_lake_context = True
                    elif current_name and current_name == downstream_name:
                        data["tributary_of"] = downstream_tributary_of
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

                    stack.append((v, upstream_tributary, data, edge_key))

        logger.info("Tributary enrichment complete.")

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

        logger.info(f"Exporting to {filename}...")

        # Clean attributes
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

        # Remove 'id' from edge data if it exists to avoid attribute conflicts
        # NetworkX will now use the 'key' (which we set to linear_feature_id) as the XML ID
        # self.G.graph['edge_id_from_attribute'] = 'id' # Not needed if key is set correctly

        nx.write_graphml(self.G, filename)
        logger.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("target", nargs="?", help="Name of stream to filter")
    parser.add_argument("--limit", type=int, default=None, help="Limit layers")
    parser.add_argument(
        "--debug", action="store_true", help="Mark search roots in graph"
    )
    args = parser.parse_args()

    builder = FWAPrimalGraph()

    if builder.validate_paths():
        builder.load_lakes()
        builder.build(limit=args.limit)
        builder.preprocess_graph()

        builder.enrich_tributaries(debug=args.debug)

        if args.target:
            builder.filter_watershed(args.target)
            clean_name = args.target.replace(" ", "_")
            builder.export(f"fwa_primal_{clean_name}.graphml")
        else:
            builder.export("fwa_bc_primal_full.graphml")
