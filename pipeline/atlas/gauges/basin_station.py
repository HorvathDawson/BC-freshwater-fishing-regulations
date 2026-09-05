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


def roster(gauged: dict[str, list[tuple[str, float]]],
           years_of: dict[str, int]) -> dict[str, list[tuple[str, float, int]]]:
    """`{basin_id: [(station, area_km2, years), ...]}` — every gauge standing in the group.

    THIS USED TO ELECT ONE. It returned the largest-catchment station per group, which made
    the group's colour hostage to that station in two ways that both showed as "gauged, but
    blank":

      · it could have no climatology, and then it can never produce a percentile — not now,
        ever. 13 groups elected one, including KISP, which chose SKEENA RIVER AT HAZELTON
        while other Skeena gauges carried full records.
      · it could simply not be transmitting this hour. Chilliwack has six gauges and went
        grey because the single row named the one that was quiet.

    Electing a representative was the mistake, not the choice of representative. The client
    combines the roster instead, so a group stays coloured while ANY of its gauges reports.

    Ordered by catchment, largest first, so a rebuild on the same data writes the same file.
    """
    out: dict[str, list[tuple[str, float, int]]] = {}
    for basin, members in gauged.items():
        rows = [(st, area or 0.0, years_of.get(st, 0)) for st, area in members]
        if rows:
            out[basin] = sorted(rows, key=lambda r: (-r[1], r[0]))
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
