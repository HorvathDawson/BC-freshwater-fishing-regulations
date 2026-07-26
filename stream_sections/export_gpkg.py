"""TEMPORARY debug exporter — write the stream graph to a multi-layer GeoPackage for QGIS.

Not part of the shipped pipeline. Layers (EPSG:3005):
  - streams     : one line per stream node (a whole BLK). Carries name tuples, downstream
                  node, and tributary count — so you see the MERGED streams, not fid pieces.
  - confluences : one point per flow edge (a tributary joining its mainstem), labelled with
                  from/to node and the confluence measure — this is how streams connect.
  - graph_nodes : one point per stream node at its mouth — the graph ITSELF as a node-link
                  schematic (order/magnitude/#tributaries/root flag), independent of the
                  real river geometry. Pair with ``graph_edges`` to read pure topology.
  - graph_edges : one straight line per flow edge, tributary-mouth -> downstream-node-mouth.
  - anchors     : (optional) authored split anchors (point/line), showing which BLK/WSC/gnis
                  each split targets. Lets you eyeball where cuts will land.
  - tributaries : (optional) the ancestor set of one target node — the upstream walk, coloured
                  by ``role`` (target/tributary) and ``depth`` (# confluences upstream).
  - lake_io     : (optional) inlet/outlet points on lake boundaries — which streams flow INTO
                  a lake and which one drains OUT. Built from fid incidence (lakes aren't graph
                  nodes yet).
Open in QGIS: streams coloured by ``blk`` shows the merged rivers; confluences show joins.
"""

from __future__ import annotations

from collections import defaultdict, deque
from pathlib import Path
from typing import Optional

import geopandas as gpd
from shapely.geometry import LineString, Point

from .models import SplitDef, StreamGraph


def _name_tuples_str(node) -> str:
    return "; ".join(f"{t.name}[{t.source.value}]" for t in node.name_tuples)


def _mouth_point(node) -> Optional[Point]:
    """Representative point for a node = its mouth (geometry coords[0], the downstream end)."""
    g = node.geometry
    if g is None or g.is_empty:
        return None
    x, y = g.coords[0]
    return Point(x, y)


def _coord(node_id: str) -> Optional[tuple[float, float]]:
    """Parse an "x_y" endpoint node id (see cutting.endpoint_id) back to a coordinate."""
    try:
        xs, ys = node_id.split("_", 1)
        return float(xs), float(ys)
    except ValueError:
        return None


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

    # The graph ITSELF as a schematic (topology, not geography): a point per node at its
    # mouth, and a straight line per flow edge from tributary-mouth -> downstream-node-mouth.
    # Read graph_nodes+graph_edges together to validate connectivity without the river shapes.
    pts: dict[str, Point] = {}
    node_rows = []
    for n in graph.nodes.values():
        mp = _mouth_point(n)
        if mp is None:
            continue
        pts[n.node_id] = mp
        node_rows.append({
            "node_id": n.node_id, "blk": n.blk, "kind": n.kind.value,
            "display_name": n.display_name,
            "stream_order": n.stream_order, "stream_magnitude": n.stream_magnitude,
            "n_tributaries": len(graph.up_adj.get(n.node_id, [])),
            "is_root": not graph.down_adj.get(n.node_id),
            "downstream_node": downstream.get(n.node_id, ""), "geometry": mp,
        })
    if node_rows:
        gpd.GeoDataFrame(node_rows, geometry="geometry", crs=3005).to_file(
            p, layer="graph_nodes", driver="GPKG")

    edge_rows = []
    for e in graph.edges:
        a, b = pts.get(e.from_node), pts.get(e.to_node)
        if a is None or b is None:
            continue
        edge_rows.append({
            "from_node": e.from_node, "to_node": e.to_node,
            "at_measure": round(e.at_measure, 1),
            "geometry": LineString([(a.x, a.y), (b.x, b.y)]),
        })
    if edge_rows:
        gpd.GeoDataFrame(edge_rows, geometry="geometry", crs=3005).to_file(
            p, layer="graph_edges", driver="GPKG")

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


def export_tributaries(graph: StreamGraph, target_node_id: str, path: str,
                       layer: str = "tributaries") -> int:
    """Write the ancestor set of ``target_node_id`` (the upstream tributary walk) as a layer.

    Appends to an existing GeoPackage. ``role`` = target|tributary; ``depth`` = number of
    confluences upstream of the target (target = 0). This is the raw connectivity closure
    (``graph.ancestors``); the WSC-descendant filter + 2300 barrier are NOT applied here — this
    is exactly what the Phase-4 guarded walk will start from, so you can see the unfiltered
    reach first and later diff the guarded result against it.
    """
    if target_node_id not in graph.nodes:
        raise ValueError(f"target node {target_node_id!r} not in graph")

    depth: dict[str, int] = {target_node_id: 0}
    q: deque[str] = deque([target_node_id])
    while q:
        cur = q.popleft()
        for ei in graph.up_adj.get(cur, []):
            src = graph.edges[ei].from_node
            if src not in depth:
                depth[src] = depth[cur] + 1
                q.append(src)

    rows = []
    for nid, d in depth.items():
        n = graph.nodes.get(nid)
        if n is None or n.geometry is None or n.geometry.is_empty:
            continue
        rows.append({
            "node_id": nid, "blk": n.blk, "display_name": n.display_name,
            "role": "target" if nid == target_node_id else "tributary", "depth": d,
            "stream_order": n.stream_order, "stream_magnitude": n.stream_magnitude,
            "geometry": n.geometry,
        })
    if rows:
        gpd.GeoDataFrame(rows, geometry="geometry", crs=3005).to_file(
            Path(path), layer=layer, driver="GPKG")
    return len(rows)


def export_lake_io(fid_rows, lake_wbk_kind: dict[str, str], graph: StreamGraph, path: str,
                   layer: str = "lake_io") -> int:
    """Write inlet/outlet points for every lake present in ``fid_rows`` as a layer.

    Lakes are not graph nodes yet (they live inside a BLK chain as under-lake runs), so this
    is derived from fid incidence, mirroring what the sectionizer will do when it promotes
    lakes to their own nodes:

      - a lake's under-lake fids define its boundary nodes (their endpoint coords);
      - a non-lake fid whose UPSTREAM end sits on a lake DOWNSTREAM boundary node is an
        OUTLET (flow leaves the lake into it);
      - a non-lake fid whose DOWNSTREAM end sits on a lake UPSTREAM boundary node is an
        INLET (it flows into the lake).

    ``blk`` names the stream (via the graph node's display name) so you can confirm, e.g.,
    that Adams Lake's outlet is the Lower Adams and its inlets include the Upper Adams.
    """
    lake_fids: dict[str, list] = defaultdict(list)
    for f in fid_rows:
        wbk = str(f.wbk) if f.wbk else ""
        if wbk in lake_wbk_kind:
            lake_fids[wbk].append(f)

    rows = []
    for wbk, lf in lake_fids.items():
        lake_down = {f.down_node for f in lf}
        lake_up = {f.up_node for f in lf}
        kind = lake_wbk_kind.get(wbk, "lake")
        for f in fid_rows:
            if (str(f.wbk) if f.wbk else "") in lake_wbk_kind:
                continue  # skip lake fids (this lake or any other)
            role = None
            node = None
            if f.up_node in lake_down:
                role, node = "outlet", f.up_node
            elif f.down_node in lake_up:
                role, node = "inlet", f.down_node
            if role is None:
                continue
            xy = _coord(node)
            if xy is None:
                continue
            gn = graph.nodes.get(f.blk)
            rows.append({
                "wbk": wbk, "kind": kind, "role": role, "blk": f.blk,
                "stream_name": (gn.display_name if gn else "") or "",
                "node": node, "geometry": Point(xy),
            })
    if rows:
        gpd.GeoDataFrame(rows, geometry="geometry", crs=3005).to_file(
            Path(path), layer=layer, driver="GPKG")
    return len(rows)
