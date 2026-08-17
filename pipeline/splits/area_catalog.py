"""Lightweight AREA CATALOG — the referenceable areas a reg can target via override.

Decoupled from cutting (DECISION 2026-08-15): most areas are *membership-only* (a `within(area)` reg
attaches to every feature intersecting the polygon; no split), while a few `cut: true` areas also split
streams at the boundary. The catalog stores just `{area_id, name, kind, cut, polygon}` — NOT per-area
section lists — so it stays small and the build stays fast; membership is computed lazily at resolve
time (intersects + feature-type filter) only for the areas a reg actually references.

    entries = build_area_catalog(fwa, area_defs, bbox)   # from areas.json defs
    write_area_catalog(entries, out / "area_catalog.gpkg")
    catalog = load_area_catalog(out / "area_catalog.gpkg")   # {area_id: {name, kind, cut, geometry}}
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from pipeline.splits.area_splits import load_area_polys

_CATALOG_LAYER = "areas"


def _slug(s: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", (s or "").lower())).strip("_")


def area_id(kind: str, name: str) -> str:
    """Stable, readable, collision-safe id: `area:{kind}:{slug(name)}` (e.g. area:watershed:liard_river)."""
    return f"area:{_slug(kind)}:{_slug(name)}"


@dataclass(frozen=True)
class AreaEntry:
    area_id: str
    name: str
    kind: str
    cut: bool
    geometry: object            # shapely (Multi)Polygon


def catalog_entries(area_defs: list[dict], polys_by_def: dict[str, dict]) -> list[AreaEntry]:
    """Pure assembly: area defs + their loaded {name: polygon} -> catalog entries. `kind` defaults to
    the def id; `cut` defaults True (the flag is the sole cut trigger). Deduplicates by area_id, and a
    cut entry wins over a membership one for the same id (cut areas are the stronger claim)."""
    by_id: dict[str, AreaEntry] = {}
    for ad in area_defs:
        kind = ad.get("kind", ad["id"])
        cut = ad.get("cut", True)
        for name, poly in (polys_by_def.get(ad["id"], {}) or {}).items():
            if poly is None or getattr(poly, "is_empty", False):
                continue
            aid = area_id(kind, name)
            prior = by_id.get(aid)
            if prior is None or (cut and not prior.cut):
                by_id[aid] = AreaEntry(area_id=aid, name=name, kind=kind, cut=cut, geometry=poly)
    return list(by_id.values())


def build_area_catalog(fwa, area_defs: list[dict], bbox=None) -> list[AreaEntry]:
    """Load each def's polygons (reusing load_area_polys) and assemble the catalog."""
    polys_by_def = {ad["id"]: load_area_polys(fwa, ad, bbox=bbox) for ad in area_defs}
    return catalog_entries(area_defs, polys_by_def)


def write_area_catalog(entries: list[AreaEntry], gpkg_path: str | Path, layer: str = _CATALOG_LAYER) -> Path:
    """Write the catalog as a gpkg layer (area_id, name, kind, cut, geometry). No-op if empty."""
    import geopandas as gpd

    gpkg_path = Path(gpkg_path)
    if not entries:
        return gpkg_path
    gdf = gpd.GeoDataFrame(
        [{"area_id": e.area_id, "name": e.name, "kind": e.kind, "cut": e.cut} for e in entries],
        geometry=[e.geometry for e in entries], crs="EPSG:3005",
    )
    gpkg_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(gpkg_path, layer=layer, driver="GPKG", engine="pyogrio")
    return gpkg_path


def load_area_catalog(gpkg_path: str | Path, layer: str = _CATALOG_LAYER) -> dict[str, dict]:
    """{area_id: {name, kind, cut, geometry}} — what the resolver needs to compute lazy membership."""
    import geopandas as gpd

    gdf = gpd.read_file(str(gpkg_path), layer=layer, engine="pyogrio")
    out: dict[str, dict] = {}
    for r in gdf.itertuples():
        out[r.area_id] = {"name": r.name, "kind": r.kind, "cut": bool(r.cut), "geometry": r.geometry}
    return out
