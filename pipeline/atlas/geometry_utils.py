"""Geometry helpers for atlas polygon processing."""

from __future__ import annotations

import logging
from collections import defaultdict

import geopandas as gpd
from shapely.ops import unary_union

logger = logging.getLogger(__name__)


def merge_overlapping_polygons(
    gdf: gpd.GeoDataFrame,
    id_field: str,
    name_field: str,
) -> gpd.GeoDataFrame:
    """Merge spatially overlapping polygons into single features.

    Connected components of polygons that share any area overlap are
    merged.  Each group gets the ``id_field`` and ``name_field`` of its
    largest member (by area), and its geometry becomes the union of all
    members.

    Non-overlapping polygons pass through unchanged.

    Returns a new GeoDataFrame with the same CRS and columns.
    """
    if gdf.empty:
        return gdf.copy()

    for col in (id_field, name_field):
        if col not in gdf.columns:
            raise ValueError(f"Required column '{col}' not found in GeoDataFrame")

    gdf = gdf[gdf.geometry.notnull()].copy()
    gdf = gdf.reset_index(drop=True)

    # --- Build adjacency via spatial index ---
    sindex = gdf.sindex
    adj: dict[int, set[int]] = defaultdict(set)
    for idx in range(len(gdf)):
        geom = gdf.geometry.iloc[idx]
        candidates = list(sindex.intersection(geom.bounds))
        for c in candidates:
            if c == idx:
                continue
            other_geom = gdf.geometry.iloc[c]
            if geom.intersects(other_geom):
                inter = geom.intersection(other_geom)
                if inter.area > 0:
                    adj[idx].add(c)
                    adj[c].add(idx)

    # --- Connected components (BFS) ---
    visited: set[int] = set()
    groups: list[list[int]] = []
    for node in adj:
        if node in visited:
            continue
        component: list[int] = []
        queue = [node]
        while queue:
            n = queue.pop()
            if n in visited:
                continue
            visited.add(n)
            component.append(n)
            for neighbor in adj[n]:
                if neighbor not in visited:
                    queue.append(neighbor)
        groups.append(component)

    # --- Build merged rows ---
    rows: list[dict] = []
    columns = list(gdf.columns)

    # Solo features (no overlap) — pass through unchanged
    for idx in range(len(gdf)):
        if idx not in visited:
            rows.append(gdf.iloc[idx].to_dict())

    # Merged groups — union geometry, take largest member's identity
    for component in groups:
        sub = gdf.iloc[component]
        areas = sub.geometry.area
        largest_idx = areas.idxmax()
        largest_row = gdf.loc[largest_idx]

        merged_geom = unary_union(sub.geometry.tolist())
        row_dict = largest_row.to_dict()
        row_dict["geometry"] = merged_geom
        rows.append(row_dict)

    result = gpd.GeoDataFrame(rows, columns=columns, crs=gdf.crs)

    n_merged = len(gdf) - len(result)
    if n_merged > 0:
        logger.info(
            f"  Merged overlapping polygons: {len(gdf)} → {len(result)} "
            f"({len(groups)} groups, {n_merged} absorbed)"
        )

    return result.reset_index(drop=True)
