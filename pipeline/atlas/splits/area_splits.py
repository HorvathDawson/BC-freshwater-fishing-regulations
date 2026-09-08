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

    if area_def.get("combine"):
        # ONE POLYGON FOR A GROUP OF UNITS, because that is the shape the regulation has.
        # The book writes "No Fishing in any stream in Management Units 1-1 to 1-6" — the
        # area is the UNION, and the boundaries between 1-2 and 1-3 are internal to it. Cut
        # per unit and the graph gains five boundaries no regulation asks for, and a rule
        # has to name six areas instead of one.
        geom = g.geometry.union_all()
        return {} if geom is None or geom.is_empty else {str(area_def["combine"]): geom}

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


def _neighbour_label(polys_by_name: dict, line, m: float, here: str, term: str) -> str:
    """What this cut SEPARATES, in words a person can act on.

    The label used to be the polygon's own name, which for an administrative area whose name
    is its number reads "2" — a boundary labelled with one number and no side. These labels
    surface in the app as the ends of a stretch ("From … / To …"), so a cut has to say what
    is on the other side of it: `Region 2 – Region 3 boundary`.

    The other side is found by stepping 300 m along the chain either way and asking which
    sibling polygon contains each point. A step is needed because the cut sits ON the shared
    edge, where both polygons and neither can claim it; 300 m clears the edge without
    skipping a genuinely narrow neighbour. Where nothing sits on the far side — the coast,
    the provincial border, a gap in the coverage — the cut names the one area it bounds.
    """
    L = line.length
    other = None
    for d in (m - 300.0, m + 300.0):
        if d < 0 or d > L:
            continue
        pt = line.interpolate(d)
        for nm, poly in polys_by_name.items():
            if nm != here and poly.contains(pt):
                other = nm
                break
        if other:
            break
    lead = f"{term} " if term else ""
    if other:
        a, b = sorted((here, other), key=lambda v: (len(v), v))
        return f"{lead}{a} – {lead}{b} boundary"
    return f"{lead}{here} boundary"


def resolve_area_splits(polys_by_name: dict, chains: list[BlkChain],
                        term: str | None = None) -> list[SplitPoint]:
    """Cut every chain crossing each polygon at first-enter/last-exit (transition cutting).

    `term` has three states, because there are three kinds of name:

      * ``None`` — label the cut with the area's own name. What a park wants: "Garibaldi
        Provincial Park" already reads as a place.
      * ``""`` — the name is already a readable phrase but the cut is an EDGE of it:
        "Management Units 1-1 to 1-6 boundary".
      * a word — the name is a bare identifier and needs it: "Region 2 – Region 3 boundary".

    In the last two the cut is labelled by what it SEPARATES rather than by whichever polygon
    it happened to belong to; see `_neighbour_label`.
    """
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
                label = (name if term is None
                         else _neighbour_label(polys_by_name, c.geometry, m, name, term))
                out.append(SplitPoint(split_id=f"area:{name}", blk=c.blk,
                                      route_measure=c.mouth_measure + m, fid="",
                                      label=label, anchor_type=AnchorType.area_boundary))
    return out
