"""Blanket area closures (pipeline/area_splits.json) — cut ALL streams that cross an admin polygon
at first-enter/last-exit, then flag the inside reaches (in_areas). Unlike splits.json area_boundary
anchors (scoped to ONE system via applies_to), these apply to every stream in the polygon.

Consumed by the graph build; a `within(area)` reg binds to the inside sections at match time.
Selector per area: layer + name_field + either which:"all" or a SQL `where`.
"""

from __future__ import annotations

import json
from pathlib import Path

from project_config import get_config
from pipeline.models import AnchorType, BlkChain, SplitPoint
from pipeline.splits.anchors import _area_transition_measures

ROOT = get_config().project_root
_GPKG = str(ROOT / "data/bc_fisheries_data.gpkg")


def load_area_split_defs(path: str | None = None) -> list[dict]:
    p = Path(path) if path else ROOT / "pipeline/area_splits.json"
    if not p.exists():
        return []
    return json.loads(p.read_text()).get("areas", [])


def load_area_polys(fwa, area_def: dict, bbox=None) -> dict:
    """{name -> (Multi)Polygon} for one area def, keyed by its name_field. `where` filters the layer
    (e.g. ecological reserves within parks_bc); absent `where` = the whole layer (which:'all')."""
    import geopandas as gpd

    kw: dict = {"engine": "pyogrio"}
    if area_def.get("where"):
        kw["where"] = area_def["where"]
    if bbox is not None:
        kw["bbox"] = tuple(bbox)
    g = gpd.read_file(_GPKG, layer=area_def["layer"], **kw)
    nf = area_def["name_field"]
    out: dict = {}
    if g.empty:
        return out
    for name, sub in g.groupby(nf):
        geom = sub.geometry.union_all()
        if geom is not None and not geom.is_empty:
            out[str(name)] = geom
    return out


def resolve_area_splits(polys_by_name: dict, chains: list[BlkChain]) -> list[SplitPoint]:
    """Cut every chain crossing each polygon at first-enter/last-exit (transition cutting)."""
    from shapely.strtree import STRtree

    keep = [c for c in chains if c.geometry is not None and not c.geometry.is_empty]
    if not keep:
        return []
    tree = STRtree([c.geometry for c in keep])
    out: list[SplitPoint] = []
    for name, poly in polys_by_name.items():
        boundary = poly.boundary
        for i in tree.query(poly):                 # bbox candidates; transition cutter filters non-crossers
            c = keep[i]
            for m in _area_transition_measures(c.geometry, poly, boundary):
                out.append(SplitPoint(split_id=f"area:{name}", blk=c.blk,
                                      route_measure=c.mouth_measure + m, fid="",
                                      label=name, anchor_type=AnchorType.area_boundary))
    return out
