"""Build every section's donor panel, and dictionary-compress them.

WHAT THE WALK IS. From each eligible gauge, step outward through the network in both
directions while the catchments stay within `MAX_AREA_RATIO`, and record the gauge as a
candidate for every stream section reached. Walking FROM the gauges rather than from the
sections is what makes this cheap: there are ~700 eligible stations and 1.2M sections, so
the work is bounded by the gauges and by the ratio, not by the map.

WHY THE ROLE FLIPS. Walking UPSTREAM from a gauge reaches sections for which that gauge is
DOWNSTREAM. Getting this backwards is invisible — every weight still computes, every panel
still forms, and the direction penalty is simply applied to the wrong half of the province.

THE DICTIONARY. A panel is a property of a stretch of river rather than of a reach:
everything between two confluences has the same donors with the same weights. So panels are
interned on their exact contents and each section stores a small integer. This is the same
collapse the rules work found — 2.24M rule rows to 1,656 distinct rule sets.
"""

from __future__ import annotations

import collections
import dataclasses
import heapq
import math
from dataclasses import dataclass

from pipeline.atlas.gauges.panel import (
    MAX_AREA_RATIO, OWN_LEADS_PER_QUANTITY, QUANTITIES, Donor, area_scale, may_lead, panel_for,
)
from pipeline.atlas.graph.drainage import AreaModel


@dataclass(frozen=True)
class Member:
    """A donor's own facts. Everything a screen shows is derived from these at read time."""

    station: str
    role: str                 # up | down
    area_km2: float
    years: int
    #: This station sits on REGULATED water — a dam governs what it reads.
    #:
    #: Carried to the client rather than left in the pipeline, because it changes what the
    #: number MEANS and a reader has to be told. A regulated donor is admitted only for
    #: water that is all but its own (REGULATED_MAX_RATIO), where the release schedule is
    #: what this water is actually doing; the screen still has to say so, or the app is
    #: presenting a dispatch decision as a description of the weather.
    regulated: bool = False
    #: This donor is on the SAME BLUE LINE as the section it speaks for.
    #:
    #: Not derivable from anything else the client holds, and it changes the weight by a
    #: factor of four — the water flows past both points, rather than the two merely sharing
    #: a rain shadow. See TRIBUTARY_ERROR_FACTOR in panel.py.
    same_river: bool = True
    #: This donor is the section's OWN gauge and may LEAD its answer — 1 the first choice (nearest along the water),
    #: 2 the next, and so on; 0 not. The app leads with the best-ranked one reporting the quantity asked for. See
    #: "a water's own gauge comes first" in panel.py.
    #:
    #: Depends on the target, unlike the facts above — and is interned with them anyway: it changes only where a
    #: section passes from near its gauge to far, so a stretch of river still shares one panel.
    lead: int = 0


@dataclass(frozen=True)
class Panels:
    """`section -> (panel_id, its own area)`, and `panel_id -> the donor set`.

    THE SET, NOT THE WEIGHTS. A weight depends on the target's catchment as well as the
    donor's, so baking weights in gives adjacent reaches on one river slightly different
    panels and the dictionary stops collapsing — measured, 16,127 panels against 1,898, and
    34,984 member rows against 4,337. See schema.sql.
    """

    by_section: dict[str, tuple[int, float | None]]
    members: dict[int, tuple[Member, ...]]

    def section_rows(self):
        """`(section_id, panel_id, area_km2)`."""
        for sec, (pid, area) in sorted(self.by_section.items()):
            yield (sec, pid, None if area is None else round(area, 3))

    def member_rows(self):
        """`(panel_id, ord, station, role, area_km2, years, regulated, same_river, lead)`."""
        for pid, ms in sorted(self.members.items()):
            for i, m in enumerate(ms):
                yield (pid, i, m.station, m.role, round(m.area_km2, 3), m.years,
                       1 if m.regulated else 0, 1 if m.same_river else 0, m.lead)


def _kind(node) -> str:
    k = getattr(node, "kind", "")
    return k.value if hasattr(k, "value") else str(k).split(".")[-1]


def flowing(graph, water_of: dict[str, str] | None = None, kind_of: dict[str, str] | None = None):
    """`section -> True when the walk treats it as a STREAM`: a line, or a polygon of a STREAM water.

    THE WATER'S KIND, NEVER THE GEOMETRY (owner 2026-10-22 late, "a polygon-only slough or canal gets gauges like a
    stream"). A slough or canal is a stream item (`flowing.py` folds its polygons into the river's item), yet its
    polygons are `lake:<wbk>` nodes in the graph, and the walk read that `NodeKind` as the water's kind: it counted
    the Vedder Canal as a lake crossing, gave no panel to any of the 146 stream-item polygons, and left a canal that is
    ONLY polygons (Seton Canal, Bowman, Lewis, Hansen, Taylor Slough) unmeasured wherever it sat. So a polygon of a
    stream item is a stream here — offered a panel, walked through without ending the relationship, and its own
    gauge's section — exactly as a line in its place would be. A lake item's polygon, and a polygon no water owns,
    stays a lake.

    A LINE STAYS A LINE whoever owns it: a short line a lake item owns (a narrows, an outlet stub) was always walked
    as a stream, and this ruling is about polygons only.

    Sized like a line, from its own `stream_magnitude`: the graph gives a polygon node the largest magnitude of the
    network lines under it (`graph.build_stream_graph`), which on a canal or slough is the through-line at its outlet —
    see the real-graph measure in decisions.md "a polygon-only slough or canal gets gauges like a stream".
    """
    water_of = water_of or {}
    kind_of = kind_of or {}

    def is_stream(sec: str) -> bool:
        node = graph.nodes.get(sec)
        if node is None:
            return False
        if _kind(node) == "stream":
            return True
        water = water_of.get(sec)
        return water is not None and kind_of.get(water) == "stream"
    return is_stream


def _same_blue_line(graph, water_of: dict[str, str] | None, is_stream, a: str, b: str) -> bool:
    """Whether the water flows past both sections — `Member.same_river`.

    Two LINES: the same FWA blue-line key, as always. A POLYGON has no blue-line key of its own (`blk` is "" on a lake
    node; the through-line's key is on the fids beneath it), so where a stream item's polygon is one end, the same
    WATER stands in for the same line: the Vedder Canal is the Vedder River's own water, a gauge on Nicomen Slough is
    not on the Fraser's. A true lake at either end is never the same line, as before (its `blk` is "").
    """
    na, nb = graph.nodes[a], graph.nodes[b]
    if _kind(na) == "stream" and _kind(nb) == "stream":
        return bool(na.blk) and nb.blk == na.blk
    if not (is_stream(a) and is_stream(b)):
        return False
    wa = (water_of or {}).get(a)
    return wa is not None and (water_of or {}).get(b) == wa


MAINSTEM_HOPS = 4


#: "A LARGE SHARE OF THE RIVER'S FLOW" (owner 2026-10-23, Q44) — the side channel's size over the river's at the
#: branch point, both from the graph's drainage model (`area_of`, stream magnitude): at or above it, a reading may
#: cross the branch point as Flow; below it, as nothing. DEFAULT 0.5 (Claude; an owner question in T101): the channel
#: is the larger half of the split, so its discharge and the river's are within a factor of two — the top rung of
#: the error ladder (`ERROR_BY_RATIO`, "close") with room to spare. On the real graph every named slough and side
#: channel of the Fraser, Thompson, Columbia and Kootenay sits below 0.04 (Nicomen 0.003); the pairs above it are
#: one-river splits: Bell Slough off Nelson Slough, Lewis Slough off Hanson Creek, Seton Canal off the Seton River.
LARGE_SHARE = 0.5

#: Every quantity a reading carries until it crosses a branch point.
ALL_QUANTITIES = frozenset(QUANTITIES)


def across_branch(share: float) -> frozenset[str]:
    """THE Q44 RULE, the one place it is decided: the quantities a gauge's reading keeps when the walk carries it
    across a BRANCH POINT — between a side channel (a slough, a side channel, a canal) and the river it branched from,
    in either direction, at its head or at its mouth.

    Owner 2026-10-23 (decisions.md "OWNER 2026-10-23", Q44): "a gauge on a side channel or slough never answers Level
    for the river it branched from (level is local to the backwater). It answers Flow for that river only when the side
    channel carries a large share of the river's flow; otherwise no."

    LEVEL NEVER. FLOW only when the channel's `share` of the river (`branch_points`) is at least `LARGE_SHARE`. The
    same boundary in the other direction — the river's gauges, and the creeks that reach it, speaking for the backwater
    through its head — is the same water split the same way, so it is the same rule (Bell and Nelson Slough took nine
    creek gauges 100–400 km up the Fraser that way). The walk calls this for every reading going INTO a side channel,
    and for the readings of the side channel's own gauges going OUT of it (`candidates`); a creek that only drains
    through a slough into the river is a tributary of the river, as before. What the side channel's own gauge says on
    the side channel is not a crossing, and is untouched (`own_gauges`).
    """
    return frozenset({"discharge"}) if share >= LARGE_SHARE else frozenset()


def _code_child(parent: str, child: str) -> bool:
    """`child` is a watershed code nested under `parent` (a tributary's code extends its river's)."""
    return bool(parent) and child.startswith(parent + "-")


def branch_points(graph, area_of, water_of: dict[str, str] | None, kind_of: dict[str, str] | None,
                  continues=()) -> dict[tuple[str, str], tuple[float, str]]:
    """`(from section, to section) -> (the side channel's share of the river there, the side channel's water)`, for
    every graph edge that crosses a BRANCH POINT — the edges `across_branch` rules on.

    WHERE THE GRAPH SHOWS A BRANCH, from the watershed code and the water, never the name:
      - SAME CODE, ANOTHER WATER. FWA gives a side channel its river's watershed code (`mainstem_of` relies on it), so
        an edge between two sections of the same code and two different STREAM waters is where the channel leaves or
        rejoins its river: Nicomen, Maria, Herrling, Morey … off the Fraser; Hotsprings and Baldy off the Columbia;
        Widgeon off the Pitt; Seton Canal off the Seton River; Bell off Nelson Slough. On the real graph (2026-10-23)
        128 edges, 62 water pairs, all of them a channel and its river.
      - THE RIVER FLOWING INTO A TRIBUTARY'S CODE. Water flows from a tributary's code into its river's, never the
        other way; an edge running from a code into one nested under it is a river spilling into a backwater coded
        like a tributary — the Fraser into Nelson Slough at its head. (Its mouth looks like any tributary's and is not
        a branch point here.)
    Two waters of one continued system (`continues`, T76) are one river, never a branch.

    THE SIDE CHANNEL is the smaller of the two waters there. THE SHARE is the smaller water's size over the larger's
    AROUND the branch point: the largest drainage among each
    water's sections within `MAINSTEM_HOPS` of the edge. Not the two end sections: FWA cuts a branch point into stubs
    (Nicomen leaves the Fraser through a 2.6 km² "Fraser River" piece beside a 1.4 km² slough line — 0.54 end to end,
    against the Fraser's 60,000 km² four hops on), and a share read off the stubs would let the slough speak for the
    Fraser.
    """
    if not water_of or not kind_of:
        return {}
    joined = systems(continues)
    system = lambda w: joined.get(w, w)  # noqa: E731

    def stream_water(sec: str) -> str | None:
        w = water_of.get(sec)
        return w if w is not None and kind_of.get(w) == "stream" else None

    adj: dict[str, list[str]] = collections.defaultdict(list)
    for e in graph.edges:
        adj[e.from_node].append(e.to_node)
        adj[e.to_node].append(e.from_node)

    def size_near(start: str, water: str) -> float:
        best = area_of(graph.nodes[start]) or 0.0
        seen, frontier = {start}, [start]
        for _hop in range(MAINSTEM_HOPS):
            nxt: list[str] = []
            for cur in frontier:
                for other in adj[cur]:
                    if other in seen or other not in graph.nodes:
                        continue
                    seen.add(other)
                    nxt.append(other)
                    if water_of.get(other) == water:
                        best = max(best, area_of(graph.nodes[other]) or 0.0)
            frontier = nxt
        return best

    out: dict[tuple[str, str], float] = {}
    for e in graph.edges:
        a, b = e.from_node, e.to_node
        na, nb = graph.nodes.get(a), graph.nodes.get(b)
        wa, wb = stream_water(a), stream_water(b)
        if na is None or nb is None or wa is None or wb is None or system(wa) == system(wb):
            continue
        ca, cb = getattr(na, "wsc", "") or "", getattr(nb, "wsc", "") or ""
        if not ((ca and ca == cb) or _code_child(ca, cb)):
            continue
        sa, sb = size_near(a, wa), size_near(b, wb)
        out[(a, b)] = (min(sa, sb) / max(sa, sb) if max(sa, sb) > 0 else 0.0, wa if sa <= sb else wb)
    return out


def candidates(graph, area_of, donors: list[tuple[str, str, int, bool]],
               max_ratio: float = MAX_AREA_RATIO, water_of: dict[str, str] | None = None,
               kind_of: dict[str, str] | None = None, continues=()) -> dict[str, list[tuple]]:
    """`section -> [(station, role, donor area, years, regulated, lake, same river[, quantities])]`.

    `donors` is `(station, its section, years of record, is regulated)` — already gated on
    the things that do not depend on the target, so the walk never starts for a station
    that could not speak wherever it arrived.

    `water_of` (section -> water item) and `kind_of` (water item -> stream / lake / wetland) make a STREAM water's
    polygon a stream for the walk (`flowing`). Without them a polygon is a lake, as the geometry says. With them the
    walk also knows its BRANCH POINTS (`branch_points`): a reading carried across one keeps only what `across_branch`
    allows, and its record then ends in the frozenset of those quantities (absent: every quantity, as before).
    """
    is_stream = flowing(graph, water_of, kind_of)
    branches = branch_points(graph, area_of, water_of, kind_of, continues)
    up: dict[str, list[str]] = collections.defaultdict(list)
    down: dict[str, list[str]] = collections.defaultdict(list)
    for e in graph.edges:
        up[e.to_node].append(e.from_node)
        down[e.from_node].append(e.to_node)

    out: dict[str, list[tuple]] = collections.defaultdict(list)
    for station, sec, years, regulated in donors:
        start = graph.nodes.get(sec)
        if start is None:
            continue
        a0 = area_of(start)
        if not a0:
            continue
        # THE GAUGE'S OWN SECTION FIRST — it was the one section it could not speak for.
        #
        # The walk below starts at `sec` and records only what it REACHES, so the reach the
        # station physically stands in got nothing from it. That is the best donor that will
        # ever exist for that water: same catchment, no transfer, no inference, ratio exactly
        # 1. The Skagit is the case that surfaced it — three gauges on the river, two of them
        # on the very section that had no panel at all.
        #
        # `role` is "up" rather than "down" because there is no direction to penalise: the
        # gauge is neither above nor below this water, it is in it, and DOWNSTREAM_PENALTY
        # exists to discount water that has been diluted on the way down.
        if is_stream(sec):
            out[sec].append((station, "up", a0, years, regulated, False, True))
        # A STATION ON A LAKE HAS ALREADY CROSSED ONE.
        #
        # The walk below refuses to carry a reading through a lake — storage integrates the
        # daily signal and lags it by weeks, so an inflow gauge says nothing about the
        # outflow. It tested every node it STEPPED INTO and never the one it started from,
        # so a station sitting IN a lake was free to speak for the streams around it.
        #
        # Measured on the Harrison: 08MG012 is on Harrison Lake, measures LEVEL and nothing
        # else, has 93 years of it — and was carrying 50% of the Harrison River's estimate,
        # against the river's own 75-year discharge gauge 2 reaches away. A reservoir's
        # stage is not a statement about the river below it, and `lake_gauge` exists to keep
        # those two apart everywhere else in this pipeline.
        #
        # Seeded as already-crossed rather than skipped outright, so the rule reads as what
        # it is: one lake on the path, wherever on the path it sits.
        started_in_a_lake = _kind(start) == "lake" and not is_stream(sec)
        own_water = (water_of or {}).get(sec)
        # Walking upstream reaches sections the gauge is DOWNSTREAM of, and vice versa.
        for adj, role in ((up, "down"), (down, "up")):
            # `kept`: section -> how many quantities the reading still carries there. A section is walked once, as
            # it always was, unless a later path reaches it CARRYING MORE — the way round a branch point beats the way
            # through it (the Fraser below Nicomen Slough's mouth is reached down the Fraser itself as well as out of
            # the slough). With no branch point on the way every path carries everything and this is the old seen-set.
            kept: dict[str, int] = {sec: len(ALL_QUANTITIES)}
            record: dict[str, tuple] = {}
            stack: list[tuple[str, bool, frozenset]] = [(sec, started_in_a_lake, ALL_QUANTITIES)]
            while stack:
                cur, crossed, carry = stack.pop()
                for nxt in adj[cur]:
                    node = graph.nodes.get(nxt)
                    if node is None:
                        continue
                    # A BRANCH POINT ON THE WAY — see `across_branch`. The edge as the graph has it: walking up, the
                    # water runs nxt -> cur. INTO the side channel, every reading is ruled on (the river's gauges, and
                    # the creeks that reached the river, at Nelson Slough's head); OUT of it, the readings of gauges
                    # standing ON it — Nicomen's — and not a creek's that only drained through it: Widgeon Creek's water
                    # reaches the Pitt through Widgeon Slough, all of it, as any tributary's reaches its river.
                    crossing = branches.get((cur, nxt) if role == "up" else (nxt, cur))
                    keep = carry
                    if crossing is not None and (water_of.get(nxt) == crossing[1] or own_water == crossing[1]):
                        keep = carry & across_branch(crossing[0])
                    if not keep or len(keep) <= kept.get(nxt, -1):
                        continue
                    an = area_of(node)
                    if not an or max(an, a0) / min(an, a0) > max_ratio:
                        continue
                    kept[nxt] = len(keep)
                    # A LAKE ANYWHERE ON THE PATH ENDS THE RELATIONSHIP, and it stays ended
                    # for everything beyond it — storage does not un-attenuate.
                    # A stream water's polygon is not a lake crossing — see `flowing`.
                    lake = crossed or (_kind(node) == "lake" and not is_stream(nxt))
                    if is_stream(nxt):
                        # SAME BLUE LINE OR NOT. Two donors of identical catchment size can
                        # be two different relationships — one where the water flows past
                        # both points, one where you share only the weather — and the area
                        # ratio cannot tell them apart. See TRIBUTARY_ERROR_FACTOR.
                        same = _same_blue_line(graph, water_of, is_stream, sec, nxt)
                        rec = (station, role, a0, years, regulated, lake, same)
                        record[nxt] = rec if keep == ALL_QUANTITIES else rec + (keep,)
                    stack.append((nxt, lake, keep))
            for nxt, rec in record.items():
                out[nxt].append(rec)
    return out



# FWA EDGE_TYPEs that mean "this is a secondary channel of a bigger river", not a river of
# its own: 1100 and 1350. 28,903 sections carry one, at a median stream magnitude of 1.
SIDE_CHANNEL_EDGE_TYPES = frozenset({"1100", "1350"})


def _is_side_channel(node) -> bool:
    return bool(SIDE_CHANNEL_EDGE_TYPES.intersection(getattr(node, "edge_types", ()) or ()))


def mainstem_of(graph, area_of) -> dict[str, str]:
    """`side channel section -> the section of the river it is a channel OF`.

    A SIDE CHANNEL IS NOT A SMALL RIVER, and the whole panel machinery assumed it was.

    Everything upstream of this file sizes a reach by its own accumulated catchment, which
    is the right question for a tributary — a creek draining 60 km² really is described by
    a gauge on another creek draining 60 km². A side channel is not a tributary. It is the
    SAME WATER as the river beside it, split around an island and rejoining, and its
    "catchment" is an artefact of where FWA happened to break the polygon.

    So Herrling Island Side Channel — a Fraser distributary at Chilliwack, computed area
    1.4 km² — was described by Dore River near McBride and Baker Creek at Quesnel, both
    over 400 km up the valley, both on other rivers entirely (`same_river = 0`). They were
    chosen because their catchments matched its arithmetic, and nothing else. The Fraser at
    Hope, 216,600 km², gauging the actual water in the actual channel, was metres away and
    barred by an area ratio of 150,000:1.

    The parent is found by walking the network out from the channel and taking the largest
    non-side-channel reach within `MAINSTEM_HOPS` steps, and only one LARGER than the
    channel itself — and only one on the channel's OWN river, i.e. carrying the same FWA
    watershed code. With no such reach in range the channel is not adopted at all: it keeps
    the panel its own walk gave it (often none, where its own river is silent too). It
    never borrows the answer of the river its river flows into.

    Largest rather than nearest, and searched to the full hop budget rather than stopping at
    the first river found: a braid is a mesh of small unnamed stubs welded to one big river,
    and stopping early adopted a magnitude-8 stub sitting between the channel and the Fraser.
    Four of Herrling's seven pieces went that way before the search was allowed to run out.
    """
    up: dict[str, list[str]] = collections.defaultdict(list)
    down: dict[str, list[str]] = collections.defaultdict(list)
    for e in graph.edges:
        up[e.to_node].append(e.from_node)
        down[e.from_node].append(e.to_node)

    parent: dict[str, str] = {}
    for sec, node in graph.nodes.items():
        if not _is_side_channel(node) or _kind(node) != "stream":
            continue
        best: tuple[float, str] | None = None
        mine = area_of(node) or 0.0
        seen = {sec}
        frontier = [sec]
        for _hop in range(MAINSTEM_HOPS):
            nxt: list[str] = []
            for cur in frontier:
                for adj in (up[cur], down[cur]):
                    for other in adj:
                        if other in seen:
                            continue
                        seen.add(other)
                        o = graph.nodes.get(other)
                        if o is None:
                            continue
                        nxt.append(other)
                        # ITS OWN RIVER ONLY (owner 2026-10-22, option A). FWA gives a
                        # secondary channel its river's watershed code; four hops is enough
                        # to leave that river and reach the one it flows into, and Widgeon
                        # Slough (the Pitt's code) adopted the Fraser that way. A reach on
                        # another code is walked THROUGH, never adopted.
                        if _is_side_channel(o) or _kind(o) != "stream" or o.wsc != node.wsc:
                            continue
                        a = area_of(o)
                        # BIGGER THAN THE CHANNEL, ALWAYS. A side channel adopts the water
                        # it is a channel of, which is by definition the larger reach; a
                        # stub smaller than the channel is another minor piece of the same
                        # braid, not the river.
                        if a and a > mine and (best is None or a > best[0]):
                            best = (a, other)
            frontier = nxt
        if best is not None:
            parent[sec] = best[1]
    return parent


def own_gauges(sec: str, parent: str, water_of: dict[str, str] | None,
               by_section: dict[str, tuple[int, float | None]],
               members: dict[int, tuple[Member, ...]]) -> tuple[Member, ...]:
    """The OWN gauges an adopted side-channel section keeps beside its river's answer (`mainstem_of`) — none, mostly.

    Q44's last clause (owner 2026-10-23): "the side channel and its own polygons still take their own gauge". A channel
    that is a WATER OF ITS OWN (Nicomen Slough; not the Fraser's own unnamed side piece) keeps the members its own walk
    LED with — 08MH085/086 on Nicomen Slough's lines — which the river's panel no longer holds since `across_branch`.
    The river's members stay beside them, so Flow still answers from the river where the channel's gauge reads only
    level (Herrling Island's 08MF047, Cannery Channel's 08MH028). A piece of the river's own water — a main-river gauge
    FWA snapped onto a side piece, Bella Coola at Hagensborg — is the river, and takes the river's answer as before.
    """
    w = (water_of or {}).get(sec)
    got = by_section.get(sec)
    if w is None or w == (water_of or {}).get(parent) or got is None:
        return ()
    return tuple(m for m in members[got[0]] if m.lead)


def side_channel_polygons(graph, water_of: dict[str, str], kind_of: dict[str, str],
                          adopted: dict[str, str]) -> dict[str, str]:
    """`polygon section of a STREAM water -> the side-channel line of that water whose answer it takes`.

    A LINE IN ITS PLACE, ALL THE WAY. A polygon of a slough whose lines are side channels of a bigger river (FWA
    1100/1350, adopted by `mainstem_of`) would be a side channel itself if it were a line: it would take the river's
    answer, or none. Walked on its own magnitude instead it is the Herrling Island artefact again — on the real graph
    (2026-10-22) nine polygons of Maria, Nicomen, McArthur Island, Gravel and West Road sloughs took ten creek gauges
    up to 400 km away at 1.4 km² while the slough's lines either side carried the Fraser at Hope at 60,000 km².

    The line is the nearest of the polygon's OWN water, DOWNSTREAM first, upstream only when nothing below is a line —
    the same search as `stream_polygons`, but over every line, answered or not, so a silent river silences the polygon
    too. Only a line `mainstem_of` adopted counts; a polygon beside an ordinary line keeps its own walk.
    """
    up: dict[str, list[str]] = collections.defaultdict(list)
    down: dict[str, list[str]] = collections.defaultdict(list)
    for e in graph.edges:
        up[e.to_node].append(e.from_node)
        down[e.from_node].append(e.to_node)

    out: dict[str, str] = {}
    for sec, node in graph.nodes.items():
        water = water_of.get(sec)
        if water is None or kind_of.get(water) != "stream" or _kind(node) == "stream":
            continue
        for adj in (down, up):
            seen, frontier, hits = {sec}, [sec], []
            while frontier and not hits:
                nxt: list[str] = []
                for cur in frontier:
                    for other in adj[cur]:
                        if other in seen or water_of.get(other) != water:
                            continue
                        seen.add(other)
                        nxt.append(other)
                        if _kind(graph.nodes.get(other)) == "stream":
                            hits.append(other)
                frontier = nxt
            if hits:
                side = sorted(h for h in hits if h in adopted)
                if side:
                    out[sec] = side[0]
                break
    return out


def stream_polygons(graph, water_of: dict[str, str], kind_of: dict[str, str],
                    panelled: dict[str, tuple]) -> dict[str, str]:
    """`polygon section of a STREAM water -> the line section of that water whose answer it takes` — THE FALLBACK.

    THE WATER'S KIND, NEVER THE GEOMETRY (owner 2026-10-22 late, "sloughs and canals look like streams in
    Conditions", then "a polygon-only slough or canal gets gauges like a stream"). The walk itself now treats a stream
    item's polygon as a stream (`flowing`): sized by the magnitude of the lines under it, offered a panel, walked
    through. This pass is only for a polygon the walk leaves WITHOUT an answer while a line of its own water has one —
    on the real graph (2026-10-22) two: West Road (Blackwater) lake:329047577 and Daly Slough lake:329522748, both
    beside lines that answer through their river (`mainstem_of`), not through a walk that could reach the polygon.
    Without it they would lose the answer this pass gave them before the walk changed.

    Such a polygon takes its stream's answer — the panel AND the target area of the nearest line of the SAME water that
    has one, DOWNSTREAM first (the water leaving a polygon is the water that went through it), upstream only when
    nothing below answers. The search keeps to the water's own sections and never borrows another water's answer (the
    walk is what brings other waters' gauges, exactly as it does to a line).

    A lake item's polygon is not touched — a lake's answer is `lake_gauge` (level), never a river's flow. `panelled`
    is `by_section` as the walk and side channels left it; anything already answered keeps its answer.
    """
    up: dict[str, list[str]] = collections.defaultdict(list)
    down: dict[str, list[str]] = collections.defaultdict(list)
    for e in graph.edges:
        up[e.to_node].append(e.from_node)
        down[e.from_node].append(e.to_node)

    out: dict[str, str] = {}
    for sec, node in graph.nodes.items():
        water = water_of.get(sec)
        if water is None or kind_of.get(water) != "stream":
            continue
        if sec in panelled or _kind(node) == "stream":
            continue            # a line has its own catchment and its own walk; only a polygon borrows
        for adj in (down, up):
            seen, frontier, hits = {sec}, [sec], []
            while frontier and not hits:
                nxt: list[str] = []
                for cur in frontier:
                    for other in adj[cur]:
                        if other in seen or water_of.get(other) != water:
                            continue
                        seen.add(other)
                        nxt.append(other)
                        if other in panelled and _kind(graph.nodes.get(other)) == "stream":
                            hits.append(other)
                frontier = nxt
            if hits:
                # Two answered lines at one distance (a braid below the polygon): the larger target, then the id —
                # the main channel, deterministically.
                out[sec] = max(hits, key=lambda h: (panelled[h][1] or 0.0, h))
                break
    return out


def systems(continues) -> dict[str, str]:
    """`water item -> its system` for every water in a `water_continue` link: the waters one name change joins (the
    Chilliwack and the Vedder) share one key, a chain (A as B, B as C) all of them. A water in no link is its own system
    and is not listed."""
    root: dict[str, str] = {}

    def find(w: str) -> str:
        while root.get(w, w) != w:
            w = root[w]
        return w

    for a, b in continues or ():
        ra, rb = find(a), find(b)
        if ra != rb:
            root[max(ra, rb)] = min(ra, rb)
    return {w: find(w) for w in root.keys() | set(root.values())}


def own_distances(graph, water_of: dict[str, str],
                  donors: list[tuple[str, str, int, bool]],
                  continues=(), kind_of: dict[str, str] | None = None) -> dict[str, dict[str, float]]:
    """`section -> {station: metres along the water}` for every station on the section's OWN water.

    ONE SYSTEM ACROSS A CONTINUATION (T76, owner 2026-10-22: a river that changes its name is one system; T81 Q8:
    `water_continue` only). `continues` is `(water, the water it goes on as)` — the bundle's `water_continue`, as item
    ids. A station's own water is its whole system: the Chilliwack's 08MH001 is the Vedder's own gauge, measured along
    the water through the name change, under the same gates. A tributary, a side channel and the river a water flows
    into are other water; only a `water_continue` link joins two waters.

    A station's water is the water its section belongs to (`water_of`, the registry's section -> water item — the
    same join `gauge.item_id` is made from). The walk keeps to stream sections of that water, so a lake on the way
    (a different item) ends it, as it ends the panel walk.

    THE DISTANCE is to the NEARER END of the target section, the shortest way along the water, walking up or down
    and never both. The gauge stands at the downstream end of its own section ("the section that begins at the gauge
    and runs upstream", `nodes_for`), so its own section is 0, the section below begins at the gauge (0), and the
    section above begins one gauge-section away.

    A STREAM water's polygon is walked through like its lines (`flowing`, with `kind_of`), at its graph length — 0,
    since a lake node carries no measure: the Vedder Canal does not part the Vedder from its own gauge.
    """
    is_stream = flowing(graph, water_of, kind_of)
    up: dict[str, list[str]] = collections.defaultdict(list)
    down: dict[str, list[str]] = collections.defaultdict(list)
    for e in graph.edges:
        up[e.to_node].append(e.from_node)
        down[e.from_node].append(e.to_node)

    def length(sec: str) -> float:
        return float(getattr(graph.nodes[sec], "length_m", 0.0) or 0.0)

    joined = systems(continues)
    system_of = lambda w: joined.get(w, w)  # noqa: E731

    out: dict[str, dict[str, float]] = collections.defaultdict(dict)
    for station, sec, _years, _reg in donors:
        water = water_of.get(sec)
        if water is None or sec not in graph.nodes:
            continue
        system = system_of(water)
        out[sec][station] = 0.0
        for adj, going_down in ((up, False), (down, True)):
            best = {sec: 0.0}
            heap = [(0.0, sec)]
            while heap:
                d, cur = heapq.heappop(heap)
                if d > best.get(cur, math.inf):
                    continue
                step = 0.0 if (going_down and cur == sec) else length(cur)
                for nxt in adj[cur]:
                    node = graph.nodes.get(nxt)
                    if node is None or not is_stream(nxt) or system_of(water_of.get(nxt)) != system:
                        continue
                    if d + step < best.get(nxt, math.inf):
                        best[nxt] = d + step
                        heapq.heappush(heap, (d + step, nxt))
            for nxt, d in best.items():
                if nxt != sec and d < out[nxt].get(station, math.inf):
                    out[nxt][station] = d
    return out


def build(graph, model: AreaModel,
          donors: list[tuple[str, str, int, bool]],
          max_ratio: float = MAX_AREA_RATIO,
          water_of: dict[str, str] | None = None,
          measures: dict[str, set[str]] | None = None,
          published: dict[str, float] | None = None,
          kind_of: dict[str, str] | None = None,
          continues=()) -> Panels:
    """Candidates, gates, weights, and the dictionary — the whole pass.

    `water_of` (section -> water item) is what says which donors are a section's OWN gauges; without it nothing
    leads and the usual weighting applies everywhere.

    NO ROSTER FLAG (owner 2026-10-22, "estimates use every gauge that could inform a section, chosen live"). The
    panel is built over every donor with a record, live or not, and so are the own-gauge lead candidates: which of
    them answers is decided at read time from the feed (the app re-weights the members reporting the asked quantity and
    leads with the best-ranked own gauge reporting). The stations roster's `realtime` flag used to filter the leads; it
    is a snapshot that drifts (by 2026-10-22, 13 of its 439 "live" stations had stopped transmitting — 08EG012 and
    08LG028 still led ~1,200 sections — and 6 "silent" ones had started), and a station joining or leaving ECCC's list
    must move the answer on the next feed, not on the next rebuild.

    `measures` is each station's quantities with a climatology (`gauge_stats` parameters: discharge, level) — the
    only quantities it can ever be ranked in. The panel keeps the best `MEMBERS_PER_QUANTITY` donors per quantity
    (`panel_for`). Per quantity, the nearest `OWN_LEADS_PER_QUANTITY` own gauges that may lead are kept as candidates
    and the rest are ordinary members. None treats every station as measuring both.

    `published` is each station's published catchment (`gauge.area_km2`, the stations roster's `area_km2`). It sets
    the scale of "far" (`panel.area_scale`: Hack's law wants real catchments and the area model runs low on big
    rivers) and breaks a tie between own gauges standing at the same distance and modelled size. None: the model's.

    `kind_of` (water item -> its kind: stream / lake / wetland) is what makes a STREAM water's polygon a stream for
    the walk (`flowing`) and, where the walk leaves one unanswered, lets it take its water's line answer
    (`stream_polygons`); it needs `water_of` too. Without both, every polygon is a lake, as the geometry says.

    `continues` (`(water, the water it goes on as)`, the bundle's `water_continue` as item ids) makes a continuation's
    gauges a section's own (T76, `own_distances`): the Vedder is led by the Chilliwack's 08MH001. Empty: each water
    item is its own.
    """
    area_of = lambda n: model.area_km2(getattr(n, "stream_magnitude", None),
                                       getattr(n, "wsc", ""))
    cand = candidates(graph, area_of, donors, max_ratio, water_of=water_of, kind_of=kind_of, continues=continues)
    own = own_distances(graph, water_of, donors, continues, kind_of=kind_of) if water_of else {}

    by_section: dict[str, tuple[int, float | None]] = {}
    members: dict[int, tuple[Member, ...]] = {}
    intern: dict[tuple, int] = {}
    facts = {station: (yrs, reg) for station, _sec, yrs, reg in donors}
    for sec, cs in cand.items():
        node = graph.nodes.get(sec)
        if node is None:
            continue
        # ACROSS A BRANCH POINT A READING KEEPS ONLY WHAT `across_branch` ALLOWS (the record's 8th field). A member is
        # a station, and the app answers each quantity from every member reporting it — a row cannot say "Flow only".
        # So a station that measures anything the crossing does not allow is left out here: Nicomen Slough's two
        # level gauges never reach the Fraser, and a level-and-discharge gauge on a large-share channel stays on it.
        cs = [c for c in cs if len(c) < 8
              or set(measures.get(c[0], ()) if measures is not None else ALL_QUANTITIES) <= c[7]]
        if not cs:
            continue
        at = area_of(node)
        by_station = {station: ad for station, _r, ad, *_ in cs}
        # THE OWN GAUGES THAT MAY LEAD, nearest first; ties go to the one nearer this section in size, then to the
        # one whose PUBLISHED catchment agrees with the model where it stands. Two gauges matched to one section get
        # the same modelled area: on the Chilliwack (1235783, 644 km²) the lake outlet (08MH016, 335 km² published)
        # and "above Slesse" (08MH103, 650) tie on both, and station order made the lake outlet lead the reach above
        # Slesse. See "a water's own gauge comes first" in panel.py.
        near = own.get(sec, {})
        pub = published or {}
        leads = [st for *_key, st in sorted(
            (d, abs(math.log(by_station[st] / at)), abs(math.log(area_scale(by_station[st], pub.get(st)))), st)
            for st, d in near.items()
            if st in by_station and at and by_station[st]
            and may_lead(at, by_station[st], d, pub.get(st)))]
        # AT MOST `OWN_LEADS_PER_QUANTITY` PER QUANTITY, the nearest: in Flow the best-ranked one reporting discharge
        # leads, in Level the best-ranked one reporting level, so a quantity needs its own few and no more.
        wanted: set[str] = set()
        for q in QUANTITIES:
            wanted.update([st for st in leads if measures is None or q in measures.get(st, ())]
                          [:OWN_LEADS_PER_QUANTITY])
        leads = [st for st in leads if st in wanted]
        # The gates and the cap still run HERE, at build time, because both depend on the
        # target and both are cheap once rather than per tap. What is not stored is what
        # they produced — only which donors survived.
        panel = panel_for(cs, at, keep=leads, measures=measures)
        if not panel:
            continue
        present = {d.station for d in panel}
        rank = {st: i + 1 for i, st in enumerate(st for st in leads if st in present)}
        same_by = {c[0]: (bool(c[6]) if len(c) > 6 else True) for c in cs}
        chosen = tuple(Member(d.station, d.role, by_station[d.station],
                              facts.get(d.station, (0, False))[0],
                              facts.get(d.station, (0, False))[1],
                              same_by.get(d.station, True),
                              rank.get(d.station, 0))
                       for d in panel)
        # Interned on the SET — station, role, and the donor's own facts — plus its lead rank,
        # which changes only where a section passes from near its own gauge to far, so a
        # stretch of river still shares one panel.
        key = tuple(sorted((m.station, m.role, round(m.area_km2, 3), m.years, m.regulated,
                            m.same_river, m.lead) for m in chosen))
        pid = intern.setdefault(key, len(intern))
        by_section[sec] = (pid, at)
        members.setdefault(pid, chosen)

    # A SIDE CHANNEL TAKES THE RIVER'S ANSWER — see `mainstem_of`.
    #
    # Applied here, at the END, rather than by teaching the walk about side channels: the
    # walk's job is "which stations can speak for this catchment", and the answer for a
    # side channel is not a different set of stations, it is a different QUESTION. Whatever
    # panel the parent ends up with — including none — the channel gets the same one, so
    # the two can never disagree about water that is physically the same.
    #
    # The area comes with it. The channel drains what the river drains; reporting its
    # polygon's 1.4 km² beside the Fraser's panel would be the one number on the screen
    # that contradicts all the others.
    adopted = mainstem_of(graph, area_of)
    for sec, parent in adopted.items():
        mine = own_gauges(sec, parent, water_of, by_section, members)
        got = by_section.get(parent)
        if mine and got is None:
            continue                       # the river is silent; the channel's own gauge still speaks
        if got is None:
            by_section.pop(sec, None)      # the river has no panel; neither can its channel
        elif not mine:
            by_section[sec] = got
        else:
            # ITS OWN GAUGES LEAD, THE RIVER'S MEMBERS FOLLOW — the river's own leads ranked after the channel's, so
            # the app leads Level with Nicomen's gauge and Flow (which it does not read) with the river's best.
            first = max(m.lead for m in mine)
            names = {m.station for m in mine}
            chosen = mine + tuple(dataclasses.replace(m, lead=m.lead + first if m.lead else 0)
                                  for m in members[got[0]] if m.station not in names)
            key = tuple(sorted((m.station, m.role, round(m.area_km2, 3), m.years, m.regulated,
                                m.same_river, m.lead) for m in chosen))
            pid = intern.setdefault(key, len(intern))
            members.setdefault(pid, chosen)
            by_section[sec] = (pid, got[1])

    # A STREAM WATER'S POLYGON THE WALK LEFT UNANSWERED TAKES ITS LINE'S ANSWER — the fallback, see `stream_polygons`.
    # Last, after the side channels, so it takes whatever its line finally says. The same (panel, area) pair, so the
    # polygon and its line are one colour input to the one colouring engine — nothing for a client to derive.
    if water_of and kind_of:
        for sec, line in stream_polygons(graph, water_of, kind_of, by_section).items():
            by_section[sec] = by_section[line]
        # A POLYGON OF A SIDE CHANNEL IS A SIDE CHANNEL — see `side_channel_polygons`. Last, so it overrides both its
        # own walk and the fallback: the river's answer, or none, exactly as the channel's lines.
        for sec, line in side_channel_polygons(graph, water_of, kind_of, adopted).items():
            if line in by_section:
                by_section[sec] = by_section[line]
            else:
                by_section.pop(sec, None)
    return Panels(by_section=by_section, members=members)
