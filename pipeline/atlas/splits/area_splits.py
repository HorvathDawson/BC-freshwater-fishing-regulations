"""Blanket area closures (pipeline/areas.json) — cut ALL streams that cross an admin polygon
at first-enter/last-exit, then flag the inside reaches (in_areas). Unlike splits.json area_boundary
anchors (scoped to ONE system via applies_to), these apply to every stream in the polygon.

Consumed by the graph build; a `within(area)` reg binds to the inside sections at match time.
Selector per area: layer + name_field + either which:"all" or a SQL `where`.
"""

from __future__ import annotations

import json
from pathlib import Path

from project_config import get_config
from pipeline.common.models import AnchorType, BlkChain, SplitPoint
from pipeline.atlas.splits.anchors import _area_transition_measures
from pipeline.common.curated import CURATED, SOURCE

ROOT = get_config().project_root
_GPKG = str(get_config().fwa_data_gpkg)


def load_area_split_defs(path: str | None = None) -> list[dict]:
    p = Path(path) if path else CURATED.waters.areas
    if not p.exists():
        return []
    return json.loads(p.read_text()).get("areas", [])


def load_area_polys(fwa, area_def: dict, bbox=None) -> dict:
    """{key -> (Multi)Polygon} for one area def. `where` filters the layer (e.g. ecological reserves
    within parks_bc); absent `where` = the whole layer.

    Two modes:
      * Named-area (default): union every row sharing a ``name_field`` value into one polygon, keyed
        by that name — one logical area, possibly multipart (parks, ecological reserves, historic sites).
      * Per-feature (``per_feature: true``): every row is its OWN polygon, keyed by a unique readable
        key ``"{name or label_default} [{id_field}]"``. Used for land_access, where each parcel is an
        independent closure — unioning the (mostly nameless) parcels by name would collapse thousands
        of unrelated province-wide slivers into one polygon and wreck the first-enter/last-exit cutter.
    """
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

    if area_def.get("per_feature"):
        idf = area_def.get("id_field", "")
        default = area_def.get("label_default", "Restricted area")
        for i, row in g.reset_index(drop=True).iterrows():
            geom = row.geometry
            if geom is None or geom.is_empty:
                continue
            name = str(row.get(nf) or "").strip() or default
            fid = str(row.get(idf) or i) if idf else str(i)
            out[f"{name} [{fid}]"] = geom     # id suffix keeps nameless parcels distinct
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
