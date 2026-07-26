"""TEMPORARY debug exporter — write the graph to a multi-layer GeoPackage for QGIS.

Not part of the shipped pipeline; purely for eyeballing how streams merged (blk_chains) and
split/joined (segments at confluences, nodes). Layers (EPSG:3005):
  - blk_chains : one merged line per blue line (the pre-split whole)
  - segments   : the contracted topology edges (the split pieces)
  - nodes      : confluences / ends (joins); lake nodes have no point and are omitted
Open in QGIS and colour segments by ``blk`` to see where a river was split.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd

from .models import BlkChain, Topology


def export_graph_gpkg(chains: list[BlkChain], topo: Topology, path: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.exists():
        p.unlink()  # start clean so re-runs don't append stale layers

    chain_rows = [{
        "blk": c.blk, "wsc": c.fwa_watershed_code, "gnis_id": c.gnis_id,
        "display_name": c.name_tuples[0].name if c.name_tuples else "",
        "n_fids": len(c.fids), "length_m": round(c.length_m, 1), "geometry": c.geometry,
    } for c in chains if c.geometry is not None and not c.geometry.is_empty]
    gpd.GeoDataFrame(chain_rows, geometry="geometry", crs=3005).to_file(
        p, layer="blk_chains", driver="GPKG")

    seg_rows = [{
        "segment_id": s.segment_id, "blk": s.blk, "wsc": s.wsc, "gnis_id": s.gnis_id,
        "from_node": s.from_node, "to_node": s.to_node, "edge_type": s.edge_type,
        "stream_order": s.stream_order, "stream_magnitude": s.stream_magnitude,
        "n_member_fids": len(s.member_fids), "geometry": s.geometry,
    } for s in topo.segments.values() if s.geometry is not None and not s.geometry.is_empty]
    gpd.GeoDataFrame(seg_rows, geometry="geometry", crs=3005).to_file(
        p, layer="segments", driver="GPKG")

    from shapely.geometry import Point
    node_rows = [{
        "node_id": n.node_id, "kind": n.kind.value, "is_barrier": n.is_barrier,
        "geometry": Point(n.x, n.y),
    } for n in topo.nodes.values() if n.x is not None and n.y is not None]
    if node_rows:
        gpd.GeoDataFrame(node_rows, geometry="geometry", crs=3005).to_file(
            p, layer="nodes", driver="GPKG")
