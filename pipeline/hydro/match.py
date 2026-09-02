"""Which node each hydrometric gauge sits on.

LOCATION ALONE IS NOT ENOUGH, and this is the whole reason the module exists. A gauge on
the Chilliwack 30 m from the bank is also 300 m from a side channel, 800 m from a slough
and 2 km from a different creek. Nearest-feature picks one of those about a fifth of the
time, and the app then reports the Chilliwack's discharge for a slough.

So: name AND location, the way ECCC's own naming convention allows.

    "CHILLIWACK RIVER AT VEDDER CROSSING"
     └── waterbody ──┘ └── qualifier + place ──┘

Splitting on the first qualifier keyword isolates the waterbody name for ~99% of stations
(measured in the v1 pass, `archive/…/HANDOFF_gauge_fwa_matching.md`: 434/450 clean single
matches once name variants were added). The type word — LAKE, CREEK, RIVER — is KEPT on
both sides of the comparison, and that single decision is what stops "ATLIN LAKE" matching
the Atlin River next to it.

THE 5 KM CAP IS LOAD-BEARING. The first prototype had none and matched a Maple Ridge
"Cedar Creek" to a Similkameen one 250 km away. A radius query rather than a nearest query
structurally prevents that; do not reintroduce an uncapped fallback.

WHAT COMES OUT
    A ``StationMatch`` per station, carrying HOW it resolved and how far away it landed —
    not a bare mapping. Provenance is not decoration here: v1's own handoff records that
    getting from 92% to 96% took hand-written aliases and three manual overrides, and
    without a record of which rows leaned on which mechanism there is no way to tell a
    match that is holding from one that is about to break.

    Three statuses, and the third is the point. ``matched`` resolved. ``unresolved`` has not
    yet — a gap to close. ``no_match`` was CHECKED and genuinely has nothing: Comox Harbour
    is tidal, and no amount of alias work will find it a freshwater node. Collapsing those
    last two into "missing" is how a permanent, correct absence gets re-investigated every
    year.

    The expensive half (5 GB of geometry, an STRtree over two million sections) stays
    separate from the cheap half (the shed walk in `shed.py`, which needs only topology) so
    the match can be cached across builds.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from dataclasses import asdict, dataclass, fields

from pipeline.tiles.names import normalise

# Longest first, so "ABOVE THE" wins over "ABOVE" and "UPSTREAM OF" over "AT".
_QUALIFIERS = (
    "UPSTREAM OF", "DOWNSTREAM OF", "ABOVE THE", "BELOW THE",
    "NEAR", "ABOVE", "BELOW", "AT",
)
_QUAL_RE = re.compile(r"\b(" + "|".join(_QUALIFIERS) + r")\b")

# A gauge is on the bank of the water it measures. Anything further is a different water.
RADIUS_M = 5000.0

# Stations that will never match a freshwater node, with the reason. Checked, not missing.
#
# Kept tiny and justified one by one. This is NOT an alias list — an alias says "the FWA
# calls this water something else"; this says "there is no such water in the FWA at all".
# v1 reached the same conclusion about the same station independently.
NO_MATCH: dict[str, str] = {
    "08HB087": "Comox Harbour — tidal salt water, deliberately outside the atlas",
}


@dataclass(frozen=True)
class StationMatch:
    """One station's outcome, addressed by keys the FWA owns rather than by a node id.

    NODE IDS ARE BUILD OUTPUT. `{blk}:{down_m}` is assigned by the sectionizer, so it moves
    the moment a river is cut differently — which the gauge splits do, deliberately, on the
    very next build. A match frozen as node ids is therefore stale by construction: it names
    sections the graph it is being read against does not have.

    THE ADDRESS IS A PLACE AND A WATER, and nothing derived.

        streams   the station's published coordinate + `wsc`, the FWA watershed code
        lakes     the station's published coordinate + `wbk`, the FWA waterbody key

    A route measure would work too — FWA measures survive re-sectioning — but it is a number
    WE compute from the coordinate, and freezing a derived value means every consumer
    inherits whatever we believed about projection on the day the file was written. The
    coordinate is the primary fact: ECCC publishes it, it does not move, and each consumer
    projects it itself against the graph in front of it.

        the build   a `gauge` point anchor at the coordinate, scoped by `wsc`
        the bundle  the nearest section on that water, taking the UPSTREAM one where the
                    gauge sits on a boundary — that is the water it has just measured

    One frozen fact, no cached derivative that can disagree with it.
    """
    station: str
    status: str                  # matched | no_match | unresolved
    resolved_by: str | None      # name+radius | alias | override
    distance_m: float | None
    reason: str | None = None
    # --- the address (empty when unmatched) ---
    lon: float | None = None     # the station's own published position
    lat: float | None = None
    wsc: str = ""                # FWA watershed code: the river and its side channels
    wbk: str = ""                # FWA waterbody key, when the station is on a lake
    # --- review aids, never an input to anything ---
    name: str = ""               # the water the atlas calls this, at the time of matching
    blk: str = ""                # the blue line it matched. Diagnostic; see the note above.
    node_id: str | None = None   # the node it matched IN THAT BUILD. Diagnostic only.


def waterbody_name(station_name: str) -> str:
    """The waterbody half of an ECCC station name, normalised for comparison.

    'CHILLIWACK RIVER AT VEDDER CROSSING' -> 'chilliwack river'
    """
    m = _QUAL_RE.search((station_name or "").upper())
    head = station_name[: m.start()] if m else station_name
    return normalise(head)


def _names(node) -> list[str]:
    """Every name this node answers to, normalised, display name first.

    Deduped so a node carrying the same string in four cases does not look like four
    candidates to anything counting them.
    """
    out: list[str] = []
    seen: set[str] = set()
    for raw in (node.display_name, *(t.name for t in getattr(node, "name_tuples", ()))):
        k = normalise(raw)
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


def _matched(station: str, node, node_id: str, how: str, distance: float | None,
             lon: float | None, lat: float | None) -> StationMatch:
    """Record a hit as WHERE the station is and WHICH WATER it is on. Nothing derived."""
    return StationMatch(
        station=station, status="matched", resolved_by=how, distance_m=distance,
        lon=lon, lat=lat,
        wsc=str(getattr(node, "wsc", "") or ""), wbk=str(getattr(node, "wbk", "") or ""),
        name=str(getattr(node, "display_name", "") or ""),
        blk=str(getattr(node, "blk", "") or ""), node_id=node_id,
    )


def match_stations(stations: list[dict], geoms: dict, graph, *,
                   radius_m: float = RADIUS_M,
                   aliases: dict[str, str] | None = None) -> list[StationMatch]:
    """Match each station to the node it sits on, recording how.

    ``aliases`` is the LAST RESORT and maps a station id straight to a ``node_id``. It binds
    to the water itself, not to a name: a name-keyed override is a second guess at the same
    ambiguous question, and would break the moment two waters shared the string or the FWA
    renamed one. A station resolving through it is marked ``resolved_by="override"`` so the
    dependence stays visible.

    ``geoms`` is the ``geometries.pkl`` sidecar (node_id -> shapely geometry, EPSG:3005).

    Two passes, and the order matters. A station whose name matches a nearby node takes
    that node however far inside the radius it is — a named match at 2 km beats an unnamed
    one at 20 m, because the unnamed one is a ditch. Only when nothing in the radius shares
    the name does the station go unmatched; it does NOT fall back to the closest feature,
    which is exactly the failure the radius cap exists to prevent.
    """
    import geopandas as gpd
    from shapely.geometry import Point
    from shapely.strtree import STRtree

    keys = [k for k in sorted(geoms) if k in graph.nodes]
    if not keys:
        return {}
    tree = STRtree([geoms[k] for k in keys])

    pts = gpd.GeoSeries(
        [Point(s["lon"], s["lat"]) for s in stations], crs=4326
    ).to_crs(3005)

    aliases = aliases or {}
    out: list[StationMatch] = []
    for station, pt in zip(stations, pts):
        sid = station["station"]
        if sid in NO_MATCH:
            out.append(StationMatch(sid, "no_match", None, None, NO_MATCH[sid]))
            continue
        override = aliases.get(sid)
        if override:
            if override in graph.nodes:
                out.append(_matched(sid, graph.nodes[override], override, "override",
                                    None, station.get("lon"), station.get("lat")))
            else:
                out.append(StationMatch(sid, "unresolved", None, None,
                                        f"override names {override!r}, not in this graph"))
            continue
        want = waterbody_name(station.get("name", ""))
        if not want:
            out.append(StationMatch(sid, "unresolved", None, None,
                                    "no waterbody name could be parsed"))
            continue
        named: list[tuple[float, str]] = []
        for ix in tree.query(pt.buffer(radius_m)):
            key = keys[ix]
            d = geoms[key].distance(pt)
            if d > radius_m:
                continue                     # the query is the bbox; this is the circle
            node = graph.nodes[key]
            # EVERY name the node carries, not just the one it displays.
            #
            # This is where the hand-written alias list came from and why it is gone. The
            # atlas already knows a reservoir by its reservoir name: the node displayed as
            # "Lower Arrow Lake" carries "Arrow Reservoir", "Revelstoke Lake" carries
            # "Revelstoke Reservoir", "Intata Reach" carries "Nechako Reservoir", and
            # "West Road (Blackwater) River" carries "West Road River" — which are exactly
            # the four ECCC uses. Comparing only against `display_name` threw all of that
            # away and then asked a human to type it back in by hand.
            #
            # `name_tuples` is the searchable set, which is what `name_variants.json`
            # feeds. Reading it here is the RIGHT direction of dependency: the matcher
            # consumes the shared names, and still never writes to them.
            for got in _names(node):
                # The FWA name must APPEAR IN the gauge name, not equal it: the station is
                # "COQUITLAM RIVER", the node may be "Coquitlam River" — but a node called
                # "Coquitlam" alone should still match, while "Coquitlam Lake" must not.
                if got and got in want:
                    named.append((d, key))
                    break
        if named:
            named.sort()                     # closest of the correctly-named candidates
            d, node = named[0]
            out.append(_matched(sid, graph.nodes[node], node, "name+radius", round(d, 1),
                                station.get("lon"), station.get("lat")))
        else:
            out.append(StationMatch(sid, "unresolved", None, None,
                                    f"nothing named {want!r} within {radius_m:.0f} m"))
    return sorted(out, key=lambda m: m.station)


#: How far a station may be from the channel it names. A gauge is on a bank or a bridge and
#: the FWA line is the channel centre; on a braided reach the far strand can be a few hundred
#: metres off. The watershed code does the real work of saying WHICH water.
PLACE_RADIUS_M = 500.0


def nodes_for(matches: list[StationMatch], graph=None,
              geoms: dict | None = None) -> dict[str, str]:
    """`{station: node_id}` IN THE GRAPH YOU HAND IT, projected from the frozen coordinate.

    Without a graph this returns the node ids recorded when the match was made — correct
    only for that same build, and the reason this argument exists at all.

    THE UPSTREAM SECTION, where the gauge sits on a boundary. After the gauge cuts, a
    station is exactly on the join between two sections and both contain its coordinate at
    their shared end. The one it belongs to is the UPSTREAM one: that is the water that has
    just flowed past it and been measured. The section below has already taken on whatever
    joins in between.

    Ranked by distance first, then by how near the projection lands to the section's own
    downstream end — which is zero for the section starting at the gauge, and large for the
    one ending there. Lakes never project; they are named directly by their waterbody key.

    Nothing outside the station's own watershed is considered, so a creek gauge twenty
    metres from a mainstem cannot be placed on it.
    """
    if graph is None:
        return {m.station: m.node_id for m in matches if m.node_id}

    import geopandas as gpd
    from shapely.geometry import Point

    out: dict[str, str] = {}
    todo: list[StationMatch] = []
    for m in matches:
        if m.status != "matched":
            continue
        # A LAKE STATION IS NAMED, NOT PROJECTED. It sits on a body of water rather than
        # along a channel, so its address is the waterbody key and the node is called after
        # it. Forgetting this emptied `lake_gauge` from 220 rows to 0.
        if m.wbk:
            if f"lake:{m.wbk}" in graph.nodes:
                out[m.station] = f"lake:{m.wbk}"
            continue
        if m.lon is not None and m.lat is not None and m.wsc and geoms is not None:
            todo.append(m)
    if not todo:
        return out

    # Candidate nodes, grouped by watershed: only water the station is actually on.
    from pipeline.utils.wsc import trim_wsc
    by_wsc: dict[str, list[str]] = {}
    for nid, n in graph.nodes.items():
        if nid.startswith("lake:") or nid not in geoms:
            continue
        w = trim_wsc(getattr(n, "wsc", "") or "")
        if w:
            by_wsc.setdefault(w, []).append(nid)

    pts = gpd.GeoSeries([Point(m.lon, m.lat) for m in todo], crs=4326).to_crs(3005)
    for m, pt in zip(todo, pts):
        best: tuple[float, float, str] | None = None
        for nid in by_wsc.get(trim_wsc(m.wsc), ()):
            g = geoms[nid]
            if g is None or g.is_empty:
                continue
            d = g.distance(pt)
            if d > PLACE_RADIUS_M:
                continue
            # `project` is distance from the geometry's START, and FWA lines run mouth to
            # source — so 0 means "this section begins at the gauge and runs upstream".
            rank = (round(d, 1), round(float(g.project(pt)), 1), nid)
            if best is None or rank < best:
                best = rank
        if best is not None:
            out[m.station] = best[2]
    return out


def load_aliases(path: Path | None = None) -> dict[str, str]:
    """Station id -> a NODE ID, for the handful nothing else can resolve.

    Binds to the water, never to a name. A name-keyed override is a second guess at the
    same ambiguous question — it breaks when two waters share the string, and it silently
    follows the wrong one when the FWA renames something.

    Expected to stay near-empty. It emptied entirely once the matcher started reading every
    name a node carries rather than only its display name: all seven entries it once held
    turned out to be names the atlas already knew.

    Read only here, and never written back into `pipeline/name_variants.json` — that file
    decides what waters are CALLED, and letting a matcher edit it is what corrupted v1's
    display names.
    """
    path = path or Path(__file__).parent / "aliases.json"
    if not path.exists():
        return {}
    return {k: v for k, v in json.loads(path.read_text(encoding="utf-8")).items()
            if not k.startswith("$")}


#: THE ONE FROZEN FACT about where BC's gauges are, committed and reviewable.
#:
#: Beside `splits.json` and `added_streams.build.json`, and generated the same way: run it
#: against a completed build, commit the answer, and the NEXT build reads it. It has to be
#: two-pass because matching compares a station's name against every name a node carries,
#: and those include `name_variants.json`, which is applied during the build.
MATCH_FILE = Path(__file__).resolve().parents[1] / "gauge_match.json"


def write_match(matches: list[StationMatch], path: Path | None = None) -> Path:
    """Freeze the match. Sorted by station, so a diff reads as a list of gauges."""
    path = path or MATCH_FILE
    path.write_text(json.dumps({
        "_about": "Where each hydrometric station sits: its own published coordinate plus "
                  "the FWA key for the water it is on — `wsc` for a stream, `wbk` for a "
                  "lake. NOT node ids, which are build output and move whenever a river is "
                  "re-sectioned, and NOT a route measure, which is a number we derive from "
                  "the coordinate rather than a fact anyone published. GENERATED by "
                  "`python -m pipeline.hydro.match --build <a completed build>`; do not "
                  "hand-edit. Read by pipeline.build (to cut rivers at their gauges) and by "
                  "pipeline.bundle (to place each station on a section).",
        "stations": [asdict(m) for m in sorted(matches, key=lambda m: m.station)],
    }, indent=1) + "\n", encoding="utf-8")
    return path


def read_match(path: Path | None = None) -> list[StationMatch]:
    """The frozen match, or an empty list if it has never been generated."""
    path = path or MATCH_FILE
    if not path.exists():
        return []
    rows = json.loads(path.read_text(encoding="utf-8")).get("stations", [])
    known = {f.name for f in fields(StationMatch)}
    return [StationMatch(**{k: v for k, v in r.items() if k in known}) for r in rows]


def load_matches(path: Path) -> list[StationMatch]:
    """Read a cached match file, or an empty list if there is none."""
    if not path.exists():
        return []
    return [StationMatch(**r) for r in json.loads(path.read_text(encoding="utf-8"))]


def save_matches(path: Path, matches: list[StationMatch]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([asdict(m) for m in matches], indent=1), encoding="utf-8")


def summarise(matches: list[StationMatch], live: set[str] | None = None) -> str:
    """One line per outcome. Printed by the build so a regression is visible immediately."""
    import collections

    rows = [m for m in matches if live is None or m.station in live]
    by = collections.Counter(m.status for m in rows)
    how = collections.Counter(m.resolved_by for m in rows if m.resolved_by)
    total = len(rows) or 1
    return (f"{by['matched']}/{len(rows)} matched ({by['matched']/total:.1%})"
            f"  · " + ", ".join(f"{n} by {k}" for k, n in sorted(how.items()))
            + f"  · {by['no_match']} no_match, {by['unresolved']} unresolved")


def main() -> None:
    """Freeze the match against a completed build. See `MATCH_FILE`."""
    import argparse
    import pickle

    from pipeline.hydro.shed import load_stations

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--build", type=Path, default=Path("output/v2/full"),
                    help="a completed build directory (graph.pkl, geometries.pkl)")
    ap.add_argument("--stations", type=Path,
                    default=Path("data/bc_hydrometric_stations.json"))
    ap.add_argument("--out", type=Path, default=MATCH_FILE)
    a = ap.parse_args()

    if not (a.build / "graph.pkl").exists():
        raise SystemExit(f"no completed build at {a.build} (needs graph.pkl + geometries.pkl)")
    with (a.build / "graph.pkl").open("rb") as fh:
        graph = pickle.load(fh)
    with (a.build / "geometries.pkl").open("rb") as fh:
        geoms = pickle.load(fh)

    matches = match_stations(load_stations(a.stations), geoms, graph,
                             aliases=load_aliases())
    print(summarise(matches))
    write_match(matches, a.out)
    riv = sum(1 for m in matches if m.status == "matched" and m.wsc and not m.wbk)
    lak = sum(1 for m in matches if m.status == "matched" and m.wbk)
    print(f"wrote {a.out}  ({riv} on streams by coord + wsc, {lak} on lakes by wbk)")
    print("  next: python -m pipeline.build   (cuts rivers at their gauges)")
    print("        python -m pipeline.bundle  (reads the same file for its sheds)")


if __name__ == "__main__":
    main()
