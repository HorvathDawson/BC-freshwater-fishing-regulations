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

from dataclasses import dataclass, asdict

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
    """One station's outcome, with the evidence."""
    station: str
    node_id: str | None
    status: str                  # matched | no_match | unresolved
    resolved_by: str | None      # name+radius | alias | override
    distance_m: float | None
    reason: str | None = None


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
            out.append(StationMatch(sid, None, "no_match", None, None, NO_MATCH[sid]))
            continue
        override = aliases.get(sid)
        if override:
            if override in graph.nodes:
                out.append(StationMatch(sid, override, "matched", "override", None))
            else:
                out.append(StationMatch(sid, None, "unresolved", None, None,
                                        f"override names {override!r}, not in this graph"))
            continue
        want = waterbody_name(station.get("name", ""))
        if not want:
            out.append(StationMatch(sid, None, "unresolved", None, None,
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
            out.append(StationMatch(sid, node, "matched", "name+radius", round(d, 1)))
        else:
            out.append(StationMatch(sid, None, "unresolved", None, None,
                                    f"nothing named {want!r} within {radius_m:.0f} m"))
    return sorted(out, key=lambda m: m.station)


def nodes_for(matches: list[StationMatch]) -> dict[str, str]:
    """Just the resolved ones, as ``{station: node}`` — what the shed walk needs."""
    return {m.station: m.node_id for m in matches if m.node_id}


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


#: Where a build keeps its own match. BESIDE THE BUILD, never in `data/`, because node ids
#: are per-build: a match cached against one graph names sections another graph does not have.
MATCH_CACHE = "gauge_nodes.json"


def match_for_build(build_dir: Path, stations: list[dict] | None = None,
                    *, refresh: bool = False) -> list[StationMatch]:
    """THE match for one build. The only supported way to ask.

    TWO CONSUMERS, ONE ANSWER. The bundle needs `{station: node}` to build sheds and the
    gauge table; `pipeline.hydro.splits` needs it to decide which water a station's cut
    belongs to. Both used to call `match_stations` themselves, which is two call sites that
    could drift apart on the alias file, the radius, or which stations were passed — and
    the failure would be silent, because each half would look internally consistent while
    describing a different set of gauges.

    Now they call this, and it caches to ``build_dir/gauge_nodes.json``. Whoever runs first
    warms it; the second reads the same bytes. Two consumers, one answer, by construction.

    The match survives a regulation edition — only a new FWA or a moved station invalidates
    it — so the cache is cheap to keep and expensive to recompute (it needs the 2 GB
    geometry sidecar and an STRtree over every node in the province).
    """
    import pickle

    from pipeline.hydro.shed import load_stations

    cache = Path(build_dir) / MATCH_CACHE
    if not refresh:
        cached = load_matches(cache)
        if cached:
            return cached

    graph_path = Path(build_dir) / "graph.pkl"
    geom_path = Path(build_dir) / "geometries.pkl"
    if not graph_path.exists() or not geom_path.exists():
        return []

    rows = stations if stations is not None else load_stations(
        Path("data/bc_hydrometric_stations.json"))
    with graph_path.open("rb") as fh:
        graph = pickle.load(fh)
    with geom_path.open("rb") as fh:
        geoms = pickle.load(fh)
    matches = match_stations(rows, geoms, graph, aliases=load_aliases())
    save_matches(cache, matches)
    return matches


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
