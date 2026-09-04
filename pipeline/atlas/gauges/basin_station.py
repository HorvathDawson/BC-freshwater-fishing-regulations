"""Which station speaks for each watershed group — precomputed, because it changes rarely.

THE CLAIM THIS MAKES, AND ITS LIMIT. A gauge measures the water leaving its catchment. For
the watershed group it stands in, that is a direct measurement of water in that ground. For
a group with no gauge at all it is nothing, and the map draws it as unmeasured — because a
watershed group is 3,600 km2 on average and the group next door is a different river system,
not a bigger version of this one.

NO HIERARCHY, UNLIKE THE NAMED WATERSHEDS. Those nest, so a basin with no gauge could
inherit from the basin containing it. Groups do not nest — they are a flat, complete,
non-overlapping cover of the province — so there is nothing to inherit from and the
question does not arise. That is a simplification the geometry pays for: 246 shapes that
tile British Columbia exactly once.

MOST INFORMATIVE STATION WINS. Where several gauges sit in one group, the one draining the
most country speaks for it: the group is a region, the question is what the region is doing,
and the largest catchment inside it is the closest thing to an answer for the whole.

NOT A PANEL. The donor panel carries a reading BETWEEN catchments of comparable size and
prices the error; this reports what is measured inside one region. They answer different
questions at different zooms, and mixing them would give the field a precision it has not
got.
"""

from __future__ import annotations


def resolve(gauged: dict[str, list[tuple[str, float]]]) -> dict[str, tuple[str, int]]:
    """`{basin_id: (station, levels_up)}` from `{basin_id: [(station, area_km2), ...]}`.

    `levels_up` is always 0 and is kept because the client reads it and the shape should not
    change under it: with a flat cover the reading is always from inside the group, which is
    the strongest thing this field can say. It is the column to widen if the groups are ever
    swapped for something that nests.
    """
    out: dict[str, tuple[str, int]] = {}
    for basin, members in gauged.items():
        if not members:
            continue
        # Largest catchment first; the station id breaks ties so a rebuild on the same data
        # gives the same answer.
        station = sorted(members, key=lambda m: (-(m[1] or 0.0), m[0]))[0][0]
        out[basin] = (station, 0)
    return out


def gauged_groups(stations, frame) -> dict[str, list[tuple[str, float]]]:
    """`{basin_id: [(station, area_km2), ...]}` — the gauges standing in each group.

    Point-in-polygon, because a station carries a coordinate and not a group code. The
    STREAMS carry the code, so a future version could join through the gauge's matched
    section instead and skip the geometry entirely; that is worth doing when the match rate
    is high enough that the two never disagree.
    """
    import geopandas as gpd

    if not stations:
        return {}
    pts = gpd.GeoDataFrame(
        {"station": [s for s, _lon, _lat, _a in stations],
         "area_km2": [a or 0.0 for _s, _lon, _lat, a in stations]},
        geometry=gpd.points_from_xy([lon for _s, lon, _lat, _a in stations],
                                    [lat for _s, _lon, lat, _a in stations]),
        crs=4326).to_crs(frame.crs)
    hit = gpd.sjoin(pts, frame[["basin_id", "geometry"]], how="inner", predicate="within")
    out: dict[str, list[tuple[str, float]]] = {}
    for row in hit.itertuples():
        out.setdefault(row.basin_id, []).append((row.station, row.area_km2))
    return out
