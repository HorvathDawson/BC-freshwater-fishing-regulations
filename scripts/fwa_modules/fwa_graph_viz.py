#!/usr/bin/env python3
"""
FWA Graph Processor (Primal Graph Approach)

==============================================================================
ALGORITHM & HEURISTICS
==============================================================================
1. GRAPH TOPOLOGY (PRIMAL):
   - Nodes: Represents physical locations (X, Y coordinates).
   - Edges: Represents the actual stream segments containing data attributes.

2. COORDINATE SNAPPING:
   - Coordinates rounded to 3 decimal places (~1mm) to connect touching lines.

3. DIRECTIONALITY (Mouth -> Source):
   - Edges are created from the geometric End (v) to Start (u).
   - This effectively points the graph UPSTREAM.
   - We use this direction because it allows us to start at the Ocean (Roots)
     and traverse 'Successors' to propagate names into the Headwaters.

4. ATTRIBUTE LOGIC:
   - Filter: Removes "999-..." (Coastline/Undefined).
   - 'waterbody_key': Retained for all features.
   - 'lake_name': Populated if the segment is within a known lake polygon.
   - 'tributary_of': Computed via DFS starting at Mouths.

5. TRIBUTARY LOGIC:
   - Start at Nodes with In-Degree 0 (Mouths/Ocean).
   - Traverse 'Successors' (Moving Upstream).
   - Propagate the 'Parent Name' or 'Lake Name' up the tree.
==============================================================================
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

        self.G = nx.DiGraph()
        self.lake_lookup = {}

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

                for lf_id, code, geom, name, wb in zip(ids, codes, geoms, names, wbs):
                    if geom is None or geom.is_empty:
                        continue

                    if code and str(code).startswith("999-"):
                        continue

                    # u = Start (Source), v = End (Mouth)
                    u, v, u_coord, v_coord = self.get_endpoints(geom)
                    if not u:
                        continue

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

                    # EDGE CREATION: v -> u (Mouth -> Source / Upstream)
                    # This reverts to the visual style you preferred.
                    self.G.add_edge(
                        v,
                        u,
                        id=sid,
                        linear_feature_id=sid,
                        fwa_code=s_code,
                        fwa_code_clean=self.clean_code(s_code),
                        name=edge_name,
                        waterbody_key=waterbody_key,
                        lake_name=lake_name_val,
                        length=geom.length,
                        thickness=10.0,
                        tributary_of="",
                    )

            except Exception as e:
                pass

            if i % 10 == 0:
                print(".", end="", flush=True)

        print(
            f"\nGraph Built: {self.G.number_of_nodes():,} Nodes, {self.G.number_of_edges():,} Edges"
        )

    def enrich_tributaries(self, debug=False):
        """
        DFS Traversal from Roots to Leaves.

        DIRECTION: "Opposite of DAG flow" (Traversing Predecessors).
        ROOTS: Nodes with Out-Degree 0.
        """
        logger.info(
            "Step 2: Enriching Tributary Data (DFS Backwards from Out-Degree Roots)..."
        )

        # 1. Roots = Out-Degree 0
        roots = [n for n, d in self.G.out_degree() if d == 0]

        if debug:
            logger.info("Debug Mode: Initializing debug flags...")
            nx.set_node_attributes(self.G, False, "is_search_root")

            logger.info(
                f"Debug Mode: Marking {len(roots)} search roots out of {self.G.number_of_nodes()} nodes."
            )
            for r in roots:
                self.G.nodes[r]["is_search_root"] = True

        # Stack: (Current Node, Tributary Context, Downstream Edge Data)
        stack = []
        visited_edges = set()

        # Process first edges attached to roots before the main loop
        for r in roots:
            for v in self.G.predecessors(r):
                edge_id = (v, r)
                if edge_id in visited_edges:
                    continue
                visited_edges.add(edge_id)

                # Access data for this first edge
                data = self.G.edges[v, r]
                name = data.get("name", "")
                lake = data.get("lake_name", "")

                # Mark tributary_of for first edges
                data["tributary_of"] = "Tailwater"

                # Determine context for next iteration
                # First iteration streams are always tributaries of Tailwater
                # unless they start in a lake
                upstream_tributary = "Tailwater"
                if lake:
                    upstream_tributary = lake

                # Add to stack with the edge we just processed
                stack.append((v, upstream_tributary, data))

        # Main loop - now downstream_data contains data from previous iteration
        while stack:
            u, inherited_tributary, downstream_data = stack.pop()

            # (Going against the arrow: v -> u)
            for v in self.G.predecessors(u):

                # The edge exists as (v, u), not (u, v)
                edge_id = (v, u)
                if edge_id in visited_edges:
                    continue
                visited_edges.add(edge_id)

                # Access data using the correct direction [v, u]
                data = self.G.edges[v, u]
                current_name = data.get("name", "")
                current_lake = data.get("lake_name", "")
                downstream_name = downstream_data.get("name", "")
                downstream_lake = downstream_data.get("lake_name", "")
                downstream_tributary_of = downstream_data.get("tributary_of", "")

                # Determine tributary relationship and context to propagate upstream
                # Priority: Same name continuation (including lakes) > Lake transitions > Inherited context > Stream name

                # Check if same name as downstream segment (continuation of same stream/lake)
                if current_name and current_name == downstream_name:
                    # Same stream continuing - keep the same tributary_of
                    # This handles both stream->stream and stream->lake->stream with same name
                    data["tributary_of"] = downstream_tributary_of
                    upstream_tributary = inherited_tributary
                # Check if we're leaving a lake (downstream had lake, current doesn't)
                elif downstream_lake and not current_lake:
                    # Stream entering lake is tributary of the lake
                    data["tributary_of"] = downstream_lake
                    upstream_tributary = downstream_lake
                # Use inherited context if available (e.g., still in lake context)
                elif inherited_tributary:
                    # Tributary of the inherited context (could be a lake or upstream stream)
                    data["tributary_of"] = inherited_tributary
                    upstream_tributary = inherited_tributary
                # Fall back to downstream stream name
                elif downstream_name:
                    # Tributary of the downstream stream
                    data["tributary_of"] = downstream_name
                    upstream_tributary = downstream_name
                else:
                    # No context available
                    data["tributary_of"] = ""
                    upstream_tributary = ""

                # If current segment has a name different from context, update for upstream propagation
                if current_name and current_name != upstream_tributary:
                    upstream_tributary = current_name

                stack.append((v, upstream_tributary, data))

        logger.info("Tributary enrichment complete.")

    def filter_watershed(self, target_name):
        logger.info(f"Filtering for river system: '{target_name}'...")
        search_lower = target_name.lower()

        target_edges = []
        for u, v, data in self.G.edges(data=True):
            if search_lower in data.get("name", "").lower():
                target_edges.append((u, v))

        if not target_edges:
            logger.error("Target river name not found.")
            return

        logger.info("Extracting connected component...")
        undirected = self.G.to_undirected()

        seed_nodes = set()
        for u, v in target_edges:
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
        for u, v, data in self.G.edges(data=True):
            for k, val in list(data.items()):
                if val is None:
                    data[k] = ""

        # Also clean node attributes if needed
        for n, data in self.G.nodes(data=True):
            for k, val in list(data.items()):
                if val is None:
                    data[k] = ""
                # Convert bool to string for GraphML compatibility if strict
                if isinstance(val, bool):
                    data[k] = str(val)

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

        builder.enrich_tributaries(debug=args.debug)

        if args.target:
            builder.filter_watershed(args.target)
            clean_name = args.target.replace(" ", "_")
            builder.export(f"fwa_primal_{clean_name}.graphml")
        else:
            builder.export("fwa_bc_primal_full.graphml")
