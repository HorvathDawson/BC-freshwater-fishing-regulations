"""Artifact IO + partial-rerun cache detection (05) and GeoJSON exporters for validation.

For the first validatable build we use pickle + a sibling *.meta.json. GeoParquet (per
docs/09) is a later optimization. GeoJSON exporters (EPSG:3005 -> 4326) let the graph be
eyeballed in QGIS / geojson.io.
"""

from __future__ import annotations

import json
import pickle
import time
from pathlib import Path
from typing import Any

import geopandas as gpd

from .models import Topology


def write_artifact(obj: Any, path: str, input_hashes: dict[str, str] | None = None) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "wb") as f:
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
    meta = {"input_hashes": input_hashes or {}, "build_ts": time.time()}
    p.with_suffix(p.suffix + ".meta.json").write_text(json.dumps(meta, indent=2))


def read_artifact(path: str) -> Any:
    with open(path, "rb") as f:
        return pickle.load(f)


def is_stale(path: str, input_hashes: dict[str, str]) -> bool:
    """True if the artifact is missing or its recorded input hashes differ."""
    meta_path = Path(path).with_suffix(Path(path).suffix + ".meta.json")
    if not Path(path).exists() or not meta_path.exists():
        return True
    recorded = json.loads(meta_path.read_text()).get("input_hashes", {})
    return recorded != input_hashes


# ---------------------------------------------------------------- GeoJSON export (validation)

def _to_geojson(gdf: gpd.GeoDataFrame, path: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    gdf.set_crs(3005, allow_override=True).to_crs(4326).to_file(p, driver="GeoJSON")


def export_segments_geojson(topology: Topology, path: str) -> None:
    rows = [{
        "segment_id": s.segment_id, "blk": s.blk, "wsc": s.wsc, "gnis_id": s.gnis_id,
        "from_node": s.from_node, "to_node": s.to_node, "edge_type": s.edge_type,
        "stream_order": s.stream_order, "stream_magnitude": s.stream_magnitude,
        "geometry": s.geometry,
    } for s in topology.segments.values() if s.geometry is not None and not s.geometry.is_empty]
    _to_geojson(gpd.GeoDataFrame(rows, geometry="geometry"), path)


def export_nodes_geojson(topology: Topology, path: str) -> None:
    from shapely.geometry import Point
    rows = []
    for n in topology.nodes.values():
        if n.x is None or n.y is None:
            continue  # lake nodes have no single coord; skip in point export
        rows.append({"node_id": n.node_id, "kind": n.kind.value,
                     "is_barrier": n.is_barrier, "geometry": Point(n.x, n.y)})
    if rows:
        _to_geojson(gpd.GeoDataFrame(rows, geometry="geometry"), path)
