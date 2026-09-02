"""Which registry item each stocked waterbody is.

MATCH ON THE IDENTIFIER FIRST. NAMES ARE THE FALLBACK.

FIDQ publishes a `WATERBODY_IDENTIFIER` (e.g. `02322SAJR`) which is the SAME STRING as the
FWA's own `WATERBODY_KEY_GROUP_CODE_50K` column — a column already present in the lakes
layer this pipeline fetches. That is an exact key join, and v1's own notes record that
putting it first "resolves the large majority of rows on its own".

Everything below about names exists for what the identifier cannot answer. Reaching for a
name search first, on data that ships an exact key, is how a stocking record ends up on the
wrong lake.

TWO THINGS v1 LEARNED THE HARD WAY, both kept:

  · **A group code can map to more than one waterbody key** — FWA's own multi-part
    waterbody grouping, or a genuine collision. Ties break on distance to FIDQ's own anchor
    point, never arbitrarily.

  · **An identifier match that no name corroborates is flagged, not accepted.** v1 called
    this `_confirmed_by_name()`. An exact key that disagrees with both names is more likely
    a stale identifier than a surprise, and silently trusting it is worse than saying so.

THE NAME FALLBACK IS STILL HARD, BECAUSE FIDQ AND THE FWA NAME THINGS DIFFERENTLY. FIDQ names a lake the way an
angler asks for it; the FWA names it the way a surveyor recorded it. So this is the same
problem as the gauge matcher and takes the same shape — name AND location, a hard radius, and
a recorded provenance for every row — but with two differences that come from the data:

1. **Stocked waters are almost all LAKES.** FIDQ gives a point per waterbody; the target is a
   lake polygon. So the test is containment-or-nearest rather than distance to a line, and
   the radius can be tighter than the gauges' 5 km.

2. **A name collision here is not a near miss, it is the wrong lake.** BC has many lakes
   sharing a name, and a stocking record attached to the wrong one tells somebody there are
   fish where there are none. Where the gauge matcher can fall back on "closest of the
   correctly-named", this must refuse: two equally-named candidates inside the radius is
   `ambiguous`, not a coin toss.

WHAT COMES OUT
    A `StockMatch` per waterbody, with `status` in matched | ambiguous | unresolved and
    `resolved_by` in name+radius | alias. Same three-status discipline as the gauges: a
    permanent, checked absence must not read as a gap somebody re-investigates every year.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pipeline.tiles.names import normalise

# A stocked lake's FIDQ point sits on or beside the lake. Tighter than the gauges' 5 km
# because the target is a polygon rather than a line, and because a wrong lake is worse
# here than a missing one.
RADIUS_M = 2000.0


@dataclass(frozen=True)
class StockMatch:
    """One FIDQ waterbody's outcome, with the evidence."""
    waterbody_id: str
    name: str
    node_id: str | None
    status: str                  # matched | ambiguous | unresolved
    resolved_by: str | None      # identifier | identifier+name | name+radius | override
    distance_m: float | None
    candidates: tuple[str, ...] = ()   # set only when ambiguous, so it can be curated
    reason: str | None = None
    #: An identifier hit that no name corroborates. Matched, and worth a human look --
    #: more likely a stale identifier than a surprise. v1's `_confirmed_by_name`.
    unconfirmed: bool = False


def match_waterbodies(waters: list[dict], geoms: dict, graph, *,
                      radius_m: float = RADIUS_M,
                      aliases: dict[str, str] | None = None,
                      by_identifier: dict[str, list[str]] | None = None) -> list[StockMatch]:
    """Match FIDQ waterbodies to graph nodes. Returns one row per input, always.

    ``waters`` is ``[{"waterbody_id", "name", "lat", "lon", "identifier"}, ...]`` from the
    FIDQ fetch. ``by_identifier`` maps an FWA ``WATERBODY_KEY_GROUP_CODE_50K`` to the node
    ids carrying it — the exact-key join, tried before any name.

    ``aliases`` maps a waterbody id straight to a NODE ID: the last resort, binding to the
    water rather than to a name, for the same reason the gauge overrides do.

    Every input gets a row even when nothing matched — a silent drop is how a stocked lake
    disappears from the app without anybody noticing it was ever expected.
    """
    import geopandas as gpd
    from shapely.geometry import Point
    from shapely.strtree import STRtree

    keys = [k for k in sorted(geoms) if k in graph.nodes]
    if not keys or not waters:
        return []
    tree = STRtree([geoms[k] for k in keys])
    pts = gpd.GeoSeries([Point(w["lon"], w["lat"]) for w in waters],
                        crs=4326).to_crs(3005)

    aliases = aliases or {}
    by_identifier = by_identifier or {}
    out: list[StockMatch] = []
    for w, pt in zip(waters, pts):
        wid = w["waterbody_id"]

        override = aliases.get(wid)
        if override:
            out.append(StockMatch(wid, w.get("name", ""),
                                  override if override in graph.nodes else None,
                                  "matched" if override in graph.nodes else "unresolved",
                                  "override" if override in graph.nodes else None, None,
                                  reason=None if override in graph.nodes
                                         else f"override names {override!r}, not in graph"))
            continue

        # TIER 1 — the exact key. FIDQ ships the FWA's own group code; use it.
        ident = (w.get("identifier") or "").strip()
        hits = [n for n in by_identifier.get(ident, ()) if n in graph.nodes] if ident else []
        if hits:
            want_n = normalise(w.get("name", ""))
            # A group code may cover several waterbody keys. Closest to FIDQ's own anchor.
            best = min(hits, key=lambda n: (geoms[n].distance(pt) if n in geoms else 1e18))
            named = any(want_n and normalise(graph.nodes[n].display_name) in want_n
                        for n in hits)
            out.append(StockMatch(
                wid, w.get("name", ""), best, "matched",
                "identifier+name" if named else "identifier",
                round(geoms[best].distance(pt), 1) if best in geoms else None,
                unconfirmed=not named,
                reason=None if named else "identifier matched but no name corroborates it"))
            continue

        # TIER 2 — name and location, for what the identifier could not answer.
        want = normalise(w.get("name", ""))
        if not want:
            out.append(StockMatch(wid, w.get("name", ""), None, "unresolved", None, None,
                                  reason="FIDQ gave no name"))
            continue

        named: list[tuple[float, str, str]] = []
        for ix in tree.query(pt.buffer(radius_m)):
            key = keys[ix]
            d = geoms[key].distance(pt)
            if d > radius_m:
                continue                    # the query is the bbox; this is the circle
            got = normalise(graph.nodes[key].display_name)
            if got and (got == want or got in want):
                named.append((d, key, got))

        if not named:
            out.append(StockMatch(wid, w["name"], None, "unresolved", None, None,
                                  reason=f"nothing named {want!r} within {radius_m:.0f} m"))
            continue

        # DISTINCT waters, not distinct pieces: one lake is one node, but a river run is
        # many, and three pieces of the same river must not read as three candidates.
        by_name: dict[str, tuple[float, str]] = {}
        for d, key, got in sorted(named):
            by_name.setdefault(got, (d, key))

        if len(by_name) > 1:
            # Two differently-named waters both matched. Refusing is the point: a stocking
            # record on the wrong lake tells somebody there are fish where there are none.
            out.append(StockMatch(
                wid, w["name"], None, "ambiguous", None, None,
                candidates=tuple(sorted(by_name)),
                reason=f"{len(by_name)} differently-named waters matched within "
                       f"{radius_m:.0f} m"))
            continue

        d, key = next(iter(by_name.values()))
        out.append(StockMatch(wid, w["name"], key, "matched", "name+radius", round(d, 1)))

    return sorted(out, key=lambda m: m.waterbody_id)


def load_aliases(path: Path | None = None) -> dict[str, str]:
    """Waterbody id -> the FWA name to look for instead of FIDQ's.

    Read only here, and never written back into `pipeline/name_variants.json` — see the
    gauge matcher for what that cost in v1.
    """
    import json

    path = path or Path(__file__).parent / "aliases.json"
    if not path.exists():
        return {}
    return {k: v for k, v in json.loads(path.read_text(encoding="utf-8")).items()
            if not k.startswith("$")}
