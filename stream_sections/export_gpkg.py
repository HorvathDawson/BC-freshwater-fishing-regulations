"""TEMPORARY debug exporter — write the stream graph to a multi-layer GeoPackage for QGIS.

Not part of the shipped pipeline. Layers (EPSG:3005):
  - streams     : one line per stream node (a whole BLK). Carries name tuples, downstream
                  node, and tributary count — so you see the MERGED streams, not fid pieces.
  - confluences : one point per flow edge (a tributary joining its mainstem), labelled with
                  from/to node and the confluence measure — this is how streams connect.
  - anchors     : (optional) authored split anchors (point/line), showing which BLK/WSC/gnis
                  each split targets. Lets you eyeball where cuts will land.
Open in QGIS: streams coloured by ``blk`` shows the merged rivers; confluences show joins.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import geopandas as gpd
from shapely.geometry import LineString, Point

from .models import SplitDef, StreamGraph


def _name_tuples_str(node) -> str:
    return "; ".join(f"{t.name}[{t.source.value}]" for t in node.name_tuples)


def export_graph_gpkg(graph: StreamGraph, path: str,
                      splits: Optional[list[SplitDef]] = None) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.exists():
        p.unlink()

    downstream = {}
    for e in graph.edges:
        downstream[e.from_node] = e.to_node

    stream_rows = [{
        "node_id": n.node_id, "blk": n.blk, "wsc": n.wsc, "gnis_id": n.gnis_id,
        "display_name": n.display_name, "name_tuples": _name_tuples_str(n),
        "downstream_node": downstream.get(n.node_id, ""),
        "n_tributaries": len(graph.up_adj.get(n.node_id, [])),
        "stream_order": n.stream_order, "stream_magnitude": n.stream_magnitude,
        "length_m": round(n.length_m, 1), "geometry": n.geometry,
    } for n in graph.nodes.values() if n.geometry is not None and not n.geometry.is_empty]
    gpd.GeoDataFrame(stream_rows, geometry="geometry", crs=3005).to_file(
        p, layer="streams", driver="GPKG")

    conf_rows = [{
        "from_node": e.from_node, "to_node": e.to_node, "kind": e.kind,
        "at_measure": round(e.at_measure, 1), "geometry": Point(e.x, e.y),
    } for e in graph.edges if e.x or e.y]
    if conf_rows:
        gpd.GeoDataFrame(conf_rows, geometry="geometry", crs=3005).to_file(
            p, layer="confluences", driver="GPKG")

    if splits:
        anchor_rows = []
        for s in splits:
            target = f"blk={s.blk}" if s.blk else (f"wsc={s.wsc}" if s.wsc else f"gnis={s.gnis_id}")
            a = s.anchor
            geom = None
            if a.coord is not None:
                geom = Point(a.coord)          # NOTE: EPSG:3005 unless authored is_lonlat
            elif a.coords:
                geom = LineString(a.coords)
            if geom is None:
                continue  # lake/mu_boundary/confluence anchors have no authored coord
            anchor_rows.append({"id": s.id, "type": a.type.value, "target": target,
                                "label": s.label, "is_lonlat": a.is_lonlat, "geometry": geom})
        if anchor_rows:
            # authored coords may be lon/lat; QGIS will still place 3005-labelled ones correctly.
            gpd.GeoDataFrame(anchor_rows, geometry="geometry").to_file(
                p, layer="anchors", driver="GPKG")
