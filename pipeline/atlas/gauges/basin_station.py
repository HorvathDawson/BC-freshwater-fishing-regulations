"""Which station speaks for each catchment — precomputed, because it changes rarely.

THE CLAIM THIS MAKES, AND ITS LIMIT. A gauge measures the water leaving its catchment. For
the catchment it stands in, that is a measurement. For a catchment UPSTREAM of it, it is a
claim about the same weather over a larger area — weaker the further it travels, and worth
making because the alternative is a blank province: only 4.5% of the ground has a gauge in
its own watershed.

SMALLEST CONTAINING WATERSHED WINS. A gauge sits inside several nested watersheds; the
tightest one is the most specific true thing to say. Everything larger inherits only if
nothing smaller has a gauge.

NOT A PANEL. The donor panel carries a reading BETWEEN catchments of comparable size and
prices the error; this walks a containment hierarchy and reports how far it walked. They
answer different questions at different zooms, and mixing them would give the field a
precision it has not got.
"""

from __future__ import annotations

#: Past this the reading has travelled through four containment levels, and "the water here
#: drains into the water measured there" has become "somewhere upstream of somewhere
#: upstream of here". Measured, that is 1.1% of the province's area; drawn, it is the part
#: of the field that would be most confident-looking and least earned.
MAX_LEVELS_UP = 4


def parents(basin: str):
    """A basin id and every id containing it, tightest first. `basin_id` has the padding off."""
    parts = basin.split("-")
    for n in range(len(parts), 0, -1):
        yield "-".join(parts[:n])


def resolve(gauged: dict[str, str], basins) -> dict[str, tuple[str, int]]:
    """`{basin_id: (station, levels_up)}`.

    `gauged` maps a basin id to the station standing in it — the caller does the spatial
    work, because it needs geometry and this does not.
    """
    out: dict[str, tuple[str, int]] = {}
    for basin in basins:
        for up, anc in enumerate(parents(basin)):
            station = gauged.get(anc)
            if station is not None:
                if up <= MAX_LEVELS_UP:
                    out[basin] = (station, up)
                break
    return out


def gauged_basins(stations, leaf_frame, all_frame) -> dict[str, str]:
    """`{basin_id: station}` for the basins a station physically stands in.

    Point-in-polygon against EVERY named watershed, not just the leaves: a gauge on a large
    river sits in a big containing watershed and in no small one, and it still has to be
    able to speak for what drains into it.
    """
    import geopandas as gpd

    if not stations:
        return {}
    pts = gpd.GeoDataFrame(
        {"station": [s for s, _lon, _lat in stations]},
        geometry=gpd.points_from_xy([lon for _s, lon, _lat in stations],
                                    [lat for _s, _lon, lat in stations]),
        crs=4326).to_crs(all_frame.crs)
    hit = gpd.sjoin(pts, all_frame[["basin_id", "km2", "geometry"]],
                    how="inner", predicate="within")
    if hit.empty:
        return {}
    # Smallest containing watershed wins, and ties break on the station id so a rebuild on
    # the same data gives the same answer.
    hit = hit.sort_values(["km2", "station"])
    return {b: r.station for b, r in hit.groupby("basin_id").first().iterrows()}
