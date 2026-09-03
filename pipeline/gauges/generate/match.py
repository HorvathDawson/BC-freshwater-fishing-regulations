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

    THERE IS NO ALIAS FILE ANY MORE. It mapped a station id straight to a NODE ID, and a
    node id is build output — `{blk}:{down_m}` moves the moment the sectionizer cuts
    differently, which the gauge cuts do on the very next build. So an alias was stale by
    construction, and it had already emptied itself: all seven entries turned out to be
    names the atlas carried in `name_tuples` once the matcher read every name a node
    answers to. What replaced it is `bind` in the curated review, which names FWA keys —
    `wsc`/`wbk` survive a re-sectioning, so a human's decision outlives the build it was
    made against. See `pipeline/gauges/review.py`.

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

from dataclasses import asdict, dataclass, fields, replace

from pipeline.gauges import review as _review
from pipeline.deliver.tiles.names import normalise
from pipeline.common.curated import CURATED, GENERATED, SOURCE
from pipeline.gauges.matches import MATCH_FILE, StationMatch, read_match, write_match

# Longest first, so "ABOVE THE" wins over "ABOVE" and "UPSTREAM OF" over "AT".
#
# "IN" is here for the handful ECCC writes as a containment rather than a bearing —
# "GENERAL CREEK IN ALEXANDRIA I.R. NO. 3" — which without it parsed as a waterbody called
# "general creek in alexandria i r no 3".
_QUALIFIERS = (
    "UPSTREAM OF", "DOWNSTREAM OF", "ABOVE THE", "BELOW THE",
    "NEAR", "ABOVE", "BELOW", "AT", "IN",
)
_QUAL_RE = re.compile(r"\b(" + "|".join(_QUALIFIERS) + r")\b")

# WORDS THAT SAY "THIS IS NOT THAT WATER" — but only when we fell back to the parent.
#
# The name test is CONTAINMENT: the FWA name must appear in the gauge name, because the
# station is "COQUITLAM RIVER" and the node may be plain "Coquitlam". That is right, and it
# has one blind spot — a gauge name that contains a water's name while explicitly saying it
# measures something else:
#
#     TWAIN CREEK TRIBUTARY NEAR BABINE LAKE      -> matched 'Twain Creek', 1,351 m away
#     NAHMINT RIVER SOUTH TRIBUTARY MID VALLEY    -> matched 'Nahmint River', 2,504 m
#     HYDRAULIC CREEK SOUTHEAST KELOWNA DIVERSION -> matched 'Hydraulic Creek', 2,509 m
#
# A tributary's discharge is not its trunk's, and a diversion is a man-made channel. So these
# are `no_match` — CHECKED and genuinely absent — rather than `unresolved`, a gap to close.
# The kilometre of distance was the symptom; being on a different water is the cause.
#
# TWO THINGS THIS MUST NOT DO, both found by measuring it against the roster:
#
#   · It reads the WATERBODY HALF ONLY, never the whole station name. "CHAPMAN CREEK ABOVE
#     SECHELT DIVERSION" is on Chapman Creek — matched at 25 m — and the diversion is merely
#     the landmark it is named against. Testing the full string refused 82 stations, most of
#     them correct, including PUNTLEDGE RIVER BELOW DIVERSION and SALMON RIVER ABOVE CAMPBELL
#     LAKE DIVERSION.
#
#   · It only fires when the match FELL BACK TO A SHORTER NAME. The FWA carries a feature
#     literally called "Quinsam Diversion", and QUINSAM DIVERSION NEAR CAMPBELL RIVER matches
#     it exactly at 13 m. An exact name is the atlas agreeing with ECCC, which is the one
#     case where the keyword means nothing.
#
# Deliberately only these two words. "FORK" and "ARM" look similar and are not: the North Arm
# of the Fraser IS the Fraser. Those need curation, not a keyword.
_NOT_THIS_WATER = re.compile(r"\b(TRIBUTARY|DIVERSION)\b")

# A gauge is on the bank of the water it measures. Anything further is a different water.
RADIUS_M = 5000.0

# HOW BIG THE WATER SHOULD BE — the third check, and the only one ECCC supplies itself.
#
# Name and location both pass on the failure that motivated this. A Fraser mainstem station
# is called "FRASER RIVER", sits metres from water whose FWA name is "Fraser River" and whose
# watershed code is `100`, and still lands on a side-channel stub, because a stub of the
# Fraser is also called the Fraser and is also in watershed 100. Measured on the current
# match:
#
#     08MH028 FRASER RIVER AT STEVESTON        ECCC 232,000 km2 -> a node of magnitude 1
#     08MF064 FRASER RIVER NEAR CHILLIWACK     ECCC 226,000 km2 -> magnitude 3
#     08MF047 FRASER RIVER AT WAHLEACH         ECCC 217,400 km2 -> magnitude 15
#
# `DRAINAGE_AREA_GROSS` is INDEPENDENT evidence in a way nothing else here is. ECCC surveyed
# it; it is not derived from anything this pipeline computed, and it is not copied off the
# node we are trying to choose — unlike `wsc`, which is an OUTPUT of the match and therefore
# cannot referee it.
#
# Stream magnitude (the count of headwater links draining through a node) tracks drainage
# area closely enough to referee by: measured over all 2,007 stations carrying both, the
# median is 0.80 km2 per unit of magnitude with a 5th-to-95th spread of 0.29 to 2.86 — one
# decade wide, all in. So a candidate off by more than a decade is not "a slightly different
# reach", it is a different river.
KM2_PER_MAGNITUDE = 0.80

#: How far off that relationship a candidate may be and still be believed, in decades.
#: One decade is already well outside the p05..p95 spread; this is a blunder detector, not
#: a ranking. 32 of 2,007 stations exceed it today and 24 exceed thirty-fold.
AREA_TOLERANCE_DECADES = 1.0

# WHERE THE HAND REVIEW LIVES: `pipeline/gauge_review.json`, loaded by
# `pipeline.gauges.review`. It used to be a dict right here, and it outgrew that the moment a
# person sat down with the map — 23 entries, each needing the station's name, what the
# matcher had said, and why the human disagreed. Curation belongs in a curated file for the
# same reason `matching/overrides.json` is not a Python literal: it is edited by people and
# by tools, reviewed in a diff, and must not require a code change.
#
# THREE VERDICTS, and the one that surprises people is `confirmed`. Recording that a match is
# RIGHT is not a no-op — it pins the water, so an atlas release that quietly moves the
# station fails loudly instead of silently. Half of a review is in the rows that were already
# fine, and keeping only the corrections throws that half away.
#
# See `pipeline/gauges/review.py`.




def _area_km2(station: dict) -> float | None:
    """ECCC's own surveyed drainage area for this station, if it published one."""
    v = station.get("area_km2")
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def _area_fits(station: dict, node) -> bool:
    """Could a gauge draining ``area_km2`` sit on this node?

    True when either side has nothing to say — a station with no published area, or a node
    with no magnitude, is not evidence of a bad match and must not be treated as one. See
    `KM2_PER_MAGNITUDE` for where the constant comes from and what it caught.
    """
    import math

    area = _area_km2(station)
    mag = getattr(node, "stream_magnitude", None)
    if area is None or not mag or mag <= 0:
        return True
    return abs(math.log10(area / (float(mag) * KM2_PER_MAGNITUDE))) <= AREA_TOLERANCE_DECADES


def _contains_word(haystack: str, needle: str) -> bool:
    """Is ``needle`` present in ``haystack`` as whole words?

    Both are already normalised (lower case, single-spaced), so word boundaries are spaces
    and the ends of the string. See the call site for the two creeks this exists for.
    """
    if not needle or not haystack:
        return False
    i = haystack.find(needle)
    while i != -1:
        starts = i == 0 or haystack[i - 1] == " "
        ends = i + len(needle) == len(haystack) or haystack[i + len(needle)] == " "
        if starts and ends:
            return True
        i = haystack.find(needle, i + 1)
    return False


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
                   radius_m: float = RADIUS_M) -> list[StationMatch]:
    """Match each station to the node it sits on, recording how.


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

    reviewed = _review.load().stations
    out: list[StationMatch] = []
    for station, pt in zip(stations, pts):
        sid = station["station"]
        # THE HAND REVIEW WINS, and is consulted before any geometry is touched. A person
        # looked at this station on a map; nothing computed here outranks that.
        decided = reviewed.get(sid)
        if decided is not None and decided.verdict == "none":
            # CHECKED, and genuinely absent. Comox Harbour is tidal.
            out.append(StationMatch(sid, "no_match", "review", None, decided.note))
            continue
        if decided is not None and decided.verdict == "wrong":
            # THE MATCH IS WRONG AND THE RIGHT WATER IS NOT KNOWN YET — `unresolved`, which
            # means a gap to close, NOT `no_match`, which means we looked and there is
            # nothing. Filing these as no_match would retire 22 stations permanently on the
            # strength of a review that actually said "this needs the outlet stream".
            out.append(StationMatch(sid, "unresolved", "review", None, decided.note))
            continue
        if decided is not None and decided.verdict == "bind":
            # NO GRAPH NEEDED. `StationMatch` is addressed in FWA's own keys rather than by
            # node id precisely so a human who knows the answer can state it — the binding
            # IS the match. Extra keys ride in `also`; see review.py for why nothing reads
            # them yet.
            primary = decided.keys[0]
            out.append(StationMatch(
                sid, "matched", "review", None, decided.note,
                lon=station.get("lon"), lat=station.get("lat"),
                wbk=primary if primary in decided.wbk else "",
                wsc=primary if primary in decided.wsc else "",
                name=decided.station_name,
                also=tuple(decided.keys[1:]),
            ))
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
                #
                # ON A WORD BOUNDARY. Plain `got in want` is a raw substring test, and it
                # matched water that merely shares a tail of its spelling:
                #
                #     KIRKPATRICK CREEK NEAR ALKALI LAKE -> 'Patrick Creek'
                #     CASSIMAYOOK CREEK NEAR BAKER       -> 'Mayook Creek'
                #
                # Both are real, different creeks, and both were confidently mismatched at
                # under 140 m — close enough that no distance check would ever have caught
                # them. "North Beach Creek" containing "Beach Creek" survives this test,
                # because that one IS word-aligned; it is a judgement call, not a bug.
                if got and _contains_word(want, got):
                    named.append((d, key))
                    break
        if named:
            # CLOSEST OF THE CORRECTLY-NAMED AND CORRECTLY-SIZED.
            #
            # Distance alone picked the side-channel stub every time a mainstem station sat
            # nearer one than the mainstem — see `KM2_PER_MAGNITUDE`. So candidates that
            # agree with ECCC's own drainage area are preferred as a GROUP, and distance
            # then decides between them; only if none agree does it fall back to the
            # nearest, which is the old behaviour and is recorded as such.
            named.sort()
            plausible = [c for c in named if _area_fits(station, graph.nodes[c[1]])]
            d, node = (plausible or named)[0]
            how = "name+radius" if plausible or not _area_km2(station) else "name+radius?"
            # A TRIBUTARY OR A DIVERSION IS NOT ITS TRUNK — see `_NOT_THIS_WATER`. Checked
            # here rather than before the search, because the FWA sometimes carries the
            # diversion itself and an EXACT name is the atlas agreeing with ECCC.
            got = normalise(str(getattr(graph.nodes[node], "display_name", "") or ""))
            if _NOT_THIS_WATER.search(want.upper()) and got != want:
                out.append(StationMatch(
                    sid, "no_match", None, None,
                    f"{want!r} is a tributary or diversion of {got!r}; the atlas has no "
                    "separate feature for it, and the trunk's flow is not its flow"))
                continue
            hit = _matched(sid, graph.nodes[node], node, how, round(d, 1),
                           station.get("lon"), station.get("lat"))
            # A CONFIRMATION IS A TRIPWIRE, not a rubber stamp. A person pinned this station
            # to this water; if a later atlas release moves it, that is exactly the silent
            # regression the review exists to catch, so it is recorded rather than accepted.
            if decided is not None and decided.verdict == "confirmed" and decided.keys:
                if not ({hit.wbk, hit.wsc} & set(decided.keys)):
                    hit = replace(hit, resolved_by="name+radius!drifted",
                                  reason=f"confirmed on {', '.join(decided.keys)} but now "
                                         f"resolves to {hit.wbk or hit.wsc!r} ({hit.name!r})")
            out.append(hit)
        else:
            out.append(StationMatch(sid, "unresolved", None, None,
                                    f"nothing named {want!r} within {radius_m:.0f} m"))
    return sorted(out, key=lambda m: m.station)


#: THE CAP HERE IS THE MATCH'S OWN DISTANCE, NOT A SECOND OPINION ABOUT IT.
#:
#: This used to be a flat 500 m while `match_stations` accepted 5,000 m — two independent
#: answers to one question, and 79 matched stations lived in the gap between them: recorded
#: `distance_m` from 513 m to 4,652 m, median 1,015, silently dropped here after being
#: accepted there. Raising the flat cap to 5 km would only move the disagreement, because
#: this pass is not re-deciding which water a station is on. The match decided that. All this
#: does is find the same water again in a graph that has been re-sectioned since.
#:
#: So the bound is `the distance the match recorded, plus slack for the re-sectioning`. A
#: station cannot be placed further from its coordinate than the match already accepted, and
#: one matched at 30 m cannot drift onto a node 4 km away because the sectionizer moved a
#: boundary. The floor keeps a 0 m match from becoming an impossible 0 m requirement.
PLACE_FLOOR_M = 500.0
PLACE_SLACK_M = 250.0


def _place_cap(m: "StationMatch") -> float:
    """How far this station may land from its coordinate, in metres."""
    return max(PLACE_FLOOR_M, (m.distance_m or 0.0) + PLACE_SLACK_M)


def nodes_for(matches: list[StationMatch], graph, geoms: dict,
              *, report: list[str] | None = None) -> dict[str, str]:
    """`{station: node_id}` IN THE GRAPH YOU HAND IT, projected from the frozen coordinate.

    THE GRAPH IS REQUIRED. It used to be optional, and the fallback returned the `node_id`
    recorded when the match was made — a value this module's own docstring calls stale by
    construction, since `{blk}:{down_m}` moves every time the sectionizer cuts differently.
    An argument whose default silently produces last-build's answer is a trap, so there is
    no default.

    ``report`` collects one line per station that could NOT be placed. Nothing is dropped
    quietly: a station that matched and then failed to project is a station the app will
    call ungauged, and the only way that was ever visible was by differencing two files.

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
    import geopandas as gpd
    from shapely.geometry import Point

    drops = report if report is not None else []
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
            else:
                drops.append(f"{m.station} no lake:{m.wbk} node in this graph")
            continue
        if m.lon is None or m.lat is None:
            drops.append(f"{m.station} matched with no coordinate")
        elif not m.wsc:
            drops.append(f"{m.station} matched with neither wsc nor wbk")
        else:
            todo.append(m)
    if not todo:
        return out

    # Candidate nodes, grouped by watershed: only water the station is actually on.
    from pipeline.common.utils.wsc import trim_wsc
    by_wsc: dict[str, list[str]] = {}
    for nid, n in graph.nodes.items():
        if nid.startswith("lake:") or nid not in geoms:
            continue
        w = trim_wsc(getattr(n, "wsc", "") or "")
        if w:
            by_wsc.setdefault(w, []).append(nid)

    pts = gpd.GeoSeries([Point(m.lon, m.lat) for m in todo], crs=4326).to_crs(3005)
    for m, pt in zip(todo, pts):
        cap = _place_cap(m)
        best: tuple[float, float, str] | None = None
        for nid in by_wsc.get(trim_wsc(m.wsc), ()):
            g = geoms[nid]
            if g is None or g.is_empty:
                continue
            d = g.distance(pt)
            if d > cap:
                continue
            # `project` is distance from the geometry's START, and FWA lines run mouth to
            # source — so 0 means "this section begins at the gauge and runs upstream".
            rank = (round(d, 1), round(float(g.project(pt)), 1), nid)
            if best is None or rank < best:
                best = rank
        if best is not None:
            out[m.station] = best[2]
        else:
            n = len(by_wsc.get(trim_wsc(m.wsc), ()))
            why = ("no node carries that watershed code" if not n else
                   f"none of its {n} nodes are within {cap:.0f} m")
            drops.append(f"{m.station} {m.name!r} wsc={m.wsc} "
                         f"matched at {m.distance_m} m but {why}")
    return out










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

    from pipeline.gauges.consume.shed import load_stations

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--build", type=Path, default=GENERATED.build(),
                    help="a completed build directory (graph.pkl, geometries.pkl)")
    ap.add_argument("--stations", type=Path,
                    default=SOURCE / "bc_hydrometric_stations.json")
    ap.add_argument("--out", type=Path, default=MATCH_FILE)
    a = ap.parse_args()

    if not (a.build / "graph.pkl").exists():
        raise SystemExit(f"no completed build at {a.build} (needs graph.pkl + geometries.pkl)")
    with (a.build / "graph.pkl").open("rb") as fh:
        graph = pickle.load(fh)
    with (a.build / "geometries.pkl").open("rb") as fh:
        geoms = pickle.load(fh)

    matches = match_stations(load_stations(a.stations), geoms, graph)
    print(summarise(matches))
    write_match(matches, a.out)
    riv = sum(1 for m in matches if m.status == "matched" and m.wsc and not m.wbk)
    lak = sum(1 for m in matches if m.status == "matched" and m.wbk)
    print(f"wrote {a.out}  ({riv} on streams by coord + wsc, {lak} on lakes by wbk)")
    print("  next: python -m pipeline.atlas.build   (cuts rivers at their gauges)")
    print("        python -m pipeline.deliver.bundle  (reads the same file for its sheds)")


if __name__ == "__main__":
    main()
