"""TEMPORARY debug exporter — write the stream graph to a multi-layer GeoPackage for QGIS.

Not part of the shipped pipeline. Geometry lives in a separate ``node_id -> geom`` sidecar
(the graph itself is geometry-free); every layer here joins the graph to that sidecar.
Layers (EPSG:3005):
  - streams     : one line per STREAM piece (a BLK cut at lake-runs). name tuples, downstream
                  node, tributary count.
  - lakes       : one line per LAKE node (stitched under-lake channels), with the lake name and
                  the rivers threading it, #inlets and #outlets.
  - confluences : one point per flow edge (kind = confluence|lake_in|lake_out).
  - graph_nodes : one point per node at its mouth — the graph ITSELF as a node-link schematic
                  (kind/order/magnitude/#tributaries/root), independent of river geometry.
  - graph_edges : one straight line per flow edge, from_node-mouth -> to_node-mouth.
  - anchors     : (optional) authored split anchors (point/line).
  - split_points: (optional) every RESOLVED cut point (curated + auto border/lake), with label,
                  anchor_type, picked_up (reused an existing boundary), offset + concern.
  - obstacles   : (optional) FISS fish-passage obstacles (falls/dams) in view — the point source
                  for falls-anchored splits + future client display.
  - tributaries : (optional) the guarded ancestor set of one target node, by role + depth.
  - lake_io     : (optional) inlet/outlet points for lakes, read straight off lake-node
                  adjacency (up_adj = inlets, down_adj = outlets).

The `streams` layer IS the final section set (BLK pieces after lake + border + curated splits):
each row carries display_name, the location_identifier qualifier, a combined `full_name`, and
`out_of_bc` so the whole "what the data looks like after every step" is inspectable in QGIS.
"""

from __future__ import annotations

from collections import deque
from pathlib import Path
from typing import Optional

import geopandas as gpd
from shapely.geometry import LineString, Point

from pipeline.models import NodeKind, SplitDef, StreamGraph


def _name_tuples_str(node) -> str:
    return "; ".join(f"{t.name}[{t.source.value}]" for t in node.name_tuples)


def _mouth_point(geom) -> Optional[Point]:
    """Representative point for a node = its geometry's mouth (coords[0])."""
    if geom is None or geom.is_empty:
        return None
    x, y = geom.coords[0]
    return Point(x, y)


def _write(rows, path, layer, crs=3005):
    if rows:
        gpd.GeoDataFrame(rows, geometry="geometry", crs=crs).to_file(
            Path(path), layer=layer, driver="GPKG")
    return len(rows)


def _measure_point(graph: StreamGraph, geoms: dict, blk: str, m: float) -> Optional[Point]:
    """Point on ``blk`` at absolute route measure ``m`` (via the piece that contains it)."""
    for n in graph.nodes.values():
        if n.kind == NodeKind.stream and n.blk == blk and n.down_m - 1e-6 <= m <= n.up_m + 1e-6:
            g = geoms.get(n.node_id)
            if g is not None and not g.is_empty:
                return g.interpolate(min(max(m - n.down_m, 0.0), g.length))
    return None


def export_graph_gpkg(graph: StreamGraph, geoms: dict, path: str,
                      splits: Optional[list[SplitDef]] = None,
                      split_points: Optional[list] = None,
                      obstacles=None, area_polys: Optional[dict] = None) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.exists():
        p.unlink()

    downstream = {e.from_node: e.to_node for e in graph.edges}

    stream_rows, lake_rows = [], []
    for n in graph.nodes.values():
        g = geoms.get(n.node_id)
        if g is None or g.is_empty:
            continue
        if n.kind == NodeKind.lake:
            lake_rows.append({
                "node_id": n.node_id, "wbk": n.wbk, "display_name": n.display_name,
                "through_rivers": ", ".join(n.through_names),
                "n_inlets": len(graph.up_adj.get(n.node_id, [])),
                "n_outlets": len(graph.down_adj.get(n.node_id, [])),
                "geometry": g,
            })
        else:
            loc = n.location_identifier or ""
            stream_rows.append({
                "node_id": n.node_id, "blk": n.blk, "wsc": n.wsc, "gnis_id": n.gnis_id,
                "display_name": n.display_name, "name_tuples": _name_tuples_str(n),
                "location_identifier": loc,
                "full_name": f"{n.display_name} ({loc})" if loc else n.display_name,
                "downstream_node": downstream.get(n.node_id, ""),
                "n_tributaries": len(graph.up_adj.get(n.node_id, [])),
                "edge_types": ",".join(n.edge_types), "is_barrier": n.is_barrier,
                "out_of_bc": n.out_of_bc, "in_areas": ", ".join(n.in_areas),
                "stream_order": n.stream_order, "stream_magnitude": n.stream_magnitude,
                "length_m": round(n.length_m, 1), "geometry": g,
            })
    _write(stream_rows, p, "streams")
    _write(lake_rows, p, "lakes")

    _write([{
        "from_node": e.from_node, "to_node": e.to_node, "kind": e.kind,
        "at_measure": round(e.at_measure, 1), "geometry": Point(e.x, e.y),
    } for e in graph.edges if e.x or e.y], p, "confluences")

    # The graph ITSELF as a schematic: a point per node at its mouth + a straight line per edge.
    pts: dict[str, Point] = {}
    node_rows = []
    for n in graph.nodes.values():
        mp = _mouth_point(geoms.get(n.node_id))
        if mp is None:
            continue
        pts[n.node_id] = mp
        node_rows.append({
            "node_id": n.node_id, "kind": n.kind.value, "display_name": n.display_name,
            "stream_order": n.stream_order, "stream_magnitude": n.stream_magnitude,
            "n_tributaries": len(graph.up_adj.get(n.node_id, [])),
            "is_root": not graph.down_adj.get(n.node_id),
            "is_barrier": n.is_barrier, "geometry": mp,
        })
    _write(node_rows, p, "graph_nodes")

    edge_rows = []
    for e in graph.edges:
        a, b = pts.get(e.from_node), pts.get(e.to_node)
        if a is None or b is None:
            continue
        edge_rows.append({"from_node": e.from_node, "to_node": e.to_node, "kind": e.kind,
                          "geometry": LineString([(a.x, a.y), (b.x, b.y)])})
    _write(edge_rows, p, "graph_edges")

    if splits:
        from pipeline.splits.anchors import _line as _anchor_line, _pt as _anchor_pt
        anchor_rows = []
        for s in splits:
            target = f"blk={s.blk}" if s.blk else (f"wsc={s.wsc}" if s.wsc else f"gnis={s.gnis_id}")
            a = s.anchor
            # Reproject lon/lat anchors to EPSG:3005 (else a coord like (-116,49) lands at the
            # BC-Albers origin — the QGIS "0,0" bug). Same transform the resolver uses.
            geom = (_anchor_pt(a.coord, a.is_lonlat) if a.coord is not None
                    else (_anchor_line(a.coords, a.is_lonlat) if a.coords else None))
            if geom is None:
                continue  # lake/mu_boundary/confluence anchors have no authored coord
            anchor_rows.append({"id": s.id, "type": a.type.value, "target": target,
                                "label": s.label, "is_lonlat": a.is_lonlat, "geometry": geom})
        if anchor_rows:
            gpd.GeoDataFrame(anchor_rows, geometry="geometry", crs=3005).to_file(
                p, layer="anchors", driver="GPKG")

    if split_points:
        sp_rows = []
        for sp in split_points:
            mp = _measure_point(graph, geoms, sp.blk, sp.route_measure)
            if mp is None:
                continue
            sp_rows.append({
                "split_id": sp.split_id, "blk": sp.blk, "label": sp.label,
                "anchor_type": sp.anchor_type.value,
                "route_measure": round(sp.route_measure, 1),
                "picked_up": bool(getattr(sp, "picked_up", False)),
                "offset_m": round(getattr(sp, "offset_m", 0.0), 1),
                "concern": getattr(sp, "concern", ""), "geometry": mp,
            })
        _write(sp_rows, p, "split_points")

    if obstacles is not None and len(obstacles):
        obs_rows = []
        for r in obstacles.itertuples():
            g = getattr(r, "geometry", None)
            if g is None or g.is_empty:
                continue
            obs_rows.append({
                "obstacle_id": str(getattr(r, "FISH_OBSTACLE_POINT_ID", "")),
                "obstacle_name": str(getattr(r, "OBSTACLE_NAME", "")),
                "gazetted_name": str(getattr(r, "GAZETTED_NAME", "")),
                "wsc_50k": str(getattr(r, "WATERSHED_CODE_50K", "")),
                "height_m": getattr(r, "HEIGHT", None),
                "geometry": g if g.geom_type == "Point" else g.representative_point(),
            })
        _write(obs_rows, p, "obstacles")

    # The admin/park polygon(s) an area_boundary split cut against — so you can visually confirm
    # the split_points land ON this boundary and the 'within {area}' pieces fall INSIDE it.
    if area_polys:
        _write([{"area_name": name, "geometry": poly}
                for name, poly in area_polys.items() if poly is not None and not poly.is_empty],
               p, "areas")


def export_tributaries(graph: StreamGraph, geoms: dict, target_node_id: str, path: str,
                       layer: str = "tributaries", guarded: bool = True) -> int:
    """Write the ancestor set of ``target_node_id`` (the upstream tributary walk) as a layer.

    ``role`` = target|tributary; ``depth`` = confluences upstream of the target. The
    WSC-descendant filter is already in the edge set; ``guarded`` (default) also stops at
    EDGE_TYPE=2300 barrier nodes, matching ``graph.ancestors``.
    """
    if target_node_id not in graph.nodes:
        raise ValueError(f"target node {target_node_id!r} not in graph")

    depth: dict[str, int] = {target_node_id: 0}
    q: deque[str] = deque([target_node_id])
    while q:
        cur = q.popleft()
        for ei in graph.up_adj.get(cur, []):
            src = graph.edges[ei].from_node
            if src in depth or (guarded and graph.nodes[src].is_barrier):
                continue
            depth[src] = depth[cur] + 1
            q.append(src)

    rows = []
    for nid, d in depth.items():
        n = graph.nodes.get(nid)
        g = geoms.get(nid)
        if n is None or g is None or g.is_empty:
            continue
        rows.append({
            "node_id": nid, "kind": n.kind.value, "display_name": n.display_name,
            "role": "target" if nid == target_node_id else "tributary", "depth": d,
            "stream_order": n.stream_order, "geometry": g,
        })
    return _write(rows, path, layer)


def export_lake_io(graph: StreamGraph, geoms: dict, path: str, layer: str = "lake_io") -> int:
    """Write inlet/outlet points for every lake node, read straight off graph adjacency.

    A lake node's incoming edges (``up_adj``) are its inlets and its outgoing edges
    (``down_adj``) its outlet(s) — no fid-incidence scan needed now that lakes are nodes.
    Each point sits at the confluence coordinate and names the connecting stream.
    """
    rows = []
    for n in graph.nodes.values():
        if n.kind != NodeKind.lake:
            continue
        for ei in graph.up_adj.get(n.node_id, []):
            e = graph.edges[ei]
            other = graph.nodes.get(e.from_node)
            rows.append({"wbk": n.wbk, "lake": n.display_name, "role": "inlet",
                         "stream": (other.display_name if other else ""), "stream_node": e.from_node,
                         "geometry": Point(e.x, e.y)})
        for ei in graph.down_adj.get(n.node_id, []):
            e = graph.edges[ei]
            other = graph.nodes.get(e.to_node)
            rows.append({"wbk": n.wbk, "lake": n.display_name, "role": "outlet",
                         "stream": (other.display_name if other else ""), "stream_node": e.to_node,
                         "geometry": Point(e.x, e.y)})
    return _write(rows, path, layer)
