"""Gauge positions as SPLITS — a cut on the river each station actually measures.

THE PROBLEM, IN ONE RIVER. The Fraser mainstem is 20 graph nodes for 1,375 km, and 26
stations sit on it. Every section takes the ONE station that most nearly is that water, so
a single node — hundreds of kilometres of river — paints one colour from one gauge, and the
reading at Hope is claimed for water at Lillooet. That is not a ranking problem. It is a
LENGTH problem: a gauge is the boundary between two different measurements, so the river
has to change section where the measurement changes.

WHY THIS RUNS INSIDE THE BUILD AND NEEDS NO FROZEN ARTIFACT.

    The first version matched stations to GRAPH NODES, which meant it needed a completed
    build — and the build needs its splits, so it had to be a two-pass frozen file like
    `added_streams`. That dependency was self-inflicted. Cutting a river does not need a
    node; it needs a BLUE LINE and a measure along it, and blue-line chains exist in the
    build well before anything is split, already carrying their FWA and gazetted names.

    So the only input is `data/bc_hydrometric_stations.json` — a FETCHED file, not a build
    product. The decoupling is real (the roster is refreshed by `fetch_data`, independently
    of any build) without a committed intermediate that can go stale behind a station move.

WHY NOT HYDAT. HYDAT is the historical archive: decades of daily means, keyed by station.
It carries no coordinate accurate enough to cut with and says nothing about which blue line
a station stands on. The roster ECCC publishes beside it carries the position, and the
position is the whole question here.

ONLY ON THE WATER THE STATION NAMES. A cut lands on the nearest chain within the radius
whose own name appears in the station's name — "SLESSE CREEK NEAR VEDDER CROSSING" cuts
Slesse Creek and never the Vedder, even though the Vedder is closer to some of these
stations than the creek is. A station whose name matches nothing nearby cuts NOTHING; it
does not fall back to the closest line, which is exactly the failure the whole
representativeness rule exists to prevent.
"""

from __future__ import annotations

# Cuts closer together than this on one blue line collapse to the first: two stations 40 m
# apart describe the same water, and a 40 m section is a rendering artifact rather than a
# reach anybody fishes.
MIN_GAP_M = 250.0

# A cut this near an end of the blue line is dropped — it would leave a stub too short to
# see, and the station already sits effectively on that boundary.
MIN_END_M = 150.0

# How far a station may be from the line it names. Generous, because a gauge is often on a
# bank or a bridge and the FWA line is the channel centre; the NAME is what does the work.
RADIUS_M = 2_000.0


def _normalise(s: str) -> str:
    """Lower-case, letters and digits only — the same shape on both sides of the compare."""
    return "".join(c for c in (s or "").lower() if c.isalnum() or c == " ").strip()


def waterbody_name(station_name: str) -> str:
    """The water out of an ECCC station name.

    Their convention is "<WATER> <relation> <landmark>": "CHILLIWACK RIVER AT VEDDER
    CROSSING", "FRASER RIVER NEAR AGASSIZ". Everything from the relation onwards is a
    landmark and must not be matched against — "AT VEDDER CROSSING" is why a Chilliwack
    station would otherwise cut the Vedder.
    """
    n = _normalise(station_name)
    for sep in (" at ", " near ", " below ", " above ", " abv ", " blw ", " upstream ",
                " downstream ", " d/s ", " u/s "):
        if sep in n:
            n = n.split(sep)[0]
            break
    return n.strip()


def _chain_names(chain) -> list[str]:
    """Every name this blue line answers to, normalised."""
    out, seen = [], set()
    for raw in (getattr(chain, "gnis_name", ""),
                *(t.name for t in getattr(chain, "name_tuples", ()) or ())):
        k = _normalise(raw)
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


def gauge_split_points(chains, stations: list[dict], *, radius_m: float = RADIUS_M):
    """`list[SplitPoint]` — one cut per station, on the chain it names. Deterministic.

    ``chains`` is the build's ``BlkChain`` list, which carries geometry in EPSG:3005 and the
    resolved names. Nothing here touches the graph, so it can run before a single split has
    been applied — which is the point.
    """
    import geopandas as gpd
    from shapely.geometry import Point
    from shapely.strtree import STRtree

    from pipeline.models import AnchorType
    from pipeline.models.splits import SplitPoint

    usable = [c for c in chains
              if getattr(c, "geometry", None) is not None and not c.geometry.is_empty]
    if not usable or not stations:
        return []
    tree = STRtree([c.geometry for c in usable])

    pts = gpd.GeoSeries([Point(s["lon"], s["lat"]) for s in stations],
                        crs=4326).to_crs(3005)

    found: list[tuple[str, float, str, float, str]] = []   # blk, measure, station, off, name
    for station, pt in zip(stations, pts):
        want = waterbody_name(station.get("name", ""))
        if not want:
            continue
        best: tuple[float, object] | None = None
        for ix in tree.query(pt.buffer(radius_m)):
            chain = usable[ix]
            d = chain.geometry.distance(pt)
            if d > radius_m:
                continue                      # the query is the bbox; this is the circle
            # The FWA name must APPEAR IN the gauge name, not equal it: the station says
            # "COQUITLAM RIVER" and the chain may say "Coquitlam River", but a chain called
            # "Coquitlam" alone should still match while "Coquitlam Lake" must not.
            if any(nm and nm in want for nm in _chain_names(chain)):
                if best is None or d < best[0]:
                    best = (d, chain)
        if best is None:
            continue
        d, chain = best
        local = float(chain.geometry.project(pt))
        length = float(chain.geometry.length)
        if local < MIN_END_M or length - local < MIN_END_M:
            continue                          # a stub nobody could see, on a boundary already
        base = float(getattr(chain, "mouth_measure", 0.0) or 0.0)
        found.append((str(chain.blk), round(base + local, 1),
                      str(station["station"]), round(d, 1),
                      str(station.get("name", "")).title()))

    # Collapse near-duplicates per blue line, keeping the lowest measure — deterministic,
    # and it is the downstream one, which is the reach a person is more likely to be on.
    found.sort()
    out = []
    last: tuple[str, float] | None = None
    for blk, measure, sid, off, name in found:
        if last and last[0] == blk and measure - last[1] < MIN_GAP_M:
            continue
        last = (blk, measure)
        out.append(SplitPoint(
            split_id=f"gauge__{sid}", blk=blk, route_measure=measure, fid="",
            label=f"{sid} · {name}", anchor_type=AnchorType.gauge, offset_m=off,
            # A station a few metres from a confluence or a lake outlet should REUSE that
            # boundary rather than cut a second one beside it.
            proximity_m=250.0,
        ))
    return out


def load_roster(path) -> list[dict]:
    """The fetched ECCC station list, realtime stations only, sorted.

    Only the transmitting ones: a station discontinued in 1974 still sits somewhere, but
    cutting the river at it buys a boundary no reading will ever be shown at.
    """
    import json
    from pathlib import Path

    p = Path(path)
    if not p.exists():
        return []
    rows = json.loads(p.read_text(encoding="utf-8"))
    return sorted((r for r in rows
                   if r.get("realtime") and r.get("lon") is not None
                   and r.get("lat") is not None),
                  key=lambda r: r["station"])
