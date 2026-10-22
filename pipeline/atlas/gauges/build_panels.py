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
import heapq
import math
from dataclasses import dataclass

from pipeline.atlas.gauges.panel import MAX_AREA_RATIO, OWN_LEADS_PER_QUANTITY, Donor, may_lead, panel_for

#: The quantities a gauge is ranked in, as `gauge_stats.parameter` names them and the app's Flow / Level views ask.
QUANTITIES = ("discharge", "level")
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


MAINSTEM_HOPS = 4


def candidates(graph, area_of, donors: list[tuple[str, str, int, bool]],
               max_ratio: float = MAX_AREA_RATIO) -> dict[str, list[tuple]]:
    """`section -> [(station, role, donor area, years, regulated, lake, same river)]`.

    `donors` is `(station, its section, years of record, is regulated)` — already gated on
    the things that do not depend on the target, so the walk never starts for a station
    that could not speak wherever it arrived.
    """
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
        if _kind(start) == "stream":
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
        started_in_a_lake = _kind(start) == "lake"
        # Walking upstream reaches sections the gauge is DOWNSTREAM of, and vice versa.
        for adj, role in ((up, "down"), (down, "up")):
            seen: set[str] = {sec}
            stack: list[tuple[str, bool]] = [(sec, started_in_a_lake)]
            while stack:
                cur, crossed = stack.pop()
                for nxt in adj[cur]:
                    if nxt in seen:
                        continue
                    node = graph.nodes.get(nxt)
                    if node is None:
                        continue
                    an = area_of(node)
                    if not an or max(an, a0) / min(an, a0) > max_ratio:
                        continue
                    seen.add(nxt)
                    # A LAKE ANYWHERE ON THE PATH ENDS THE RELATIONSHIP, and it stays ended
                    # for everything beyond it — storage does not un-attenuate.
                    lake = crossed or _kind(node) == "lake"
                    if _kind(node) == "stream":
                        # SAME BLUE LINE OR NOT. Two donors of identical catchment size can
                        # be two different relationships — one where the water flows past
                        # both points, one where you share only the weather — and the area
                        # ratio cannot tell them apart. See TRIBUTARY_ERROR_FACTOR.
                        same = bool(start.blk) and node.blk == start.blk
                        out[nxt].append((station, role, a0, years, regulated, lake, same))
                    stack.append((nxt, lake))
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


def own_distances(graph, water_of: dict[str, str],
                  donors: list[tuple[str, str, int, bool]]) -> dict[str, dict[str, float]]:
    """`section -> {station: metres along the water}` for every station on the section's OWN water.

    A station's water is the water its section belongs to (`water_of`, the registry's section -> water item — the
    same join `gauge.item_id` is made from). The walk keeps to stream sections of that water, so a lake on the way
    (a different item) ends it, as it ends the panel walk.

    THE DISTANCE is to the NEARER END of the target section, the shortest way along the water, walking up or down
    and never both. The gauge stands at the downstream end of its own section ("the section that begins at the gauge
    and runs upstream", `nodes_for`), so its own section is 0, the section below begins at the gauge (0), and the
    section above begins one gauge-section away.
    """
    up: dict[str, list[str]] = collections.defaultdict(list)
    down: dict[str, list[str]] = collections.defaultdict(list)
    for e in graph.edges:
        up[e.to_node].append(e.from_node)
        down[e.from_node].append(e.to_node)

    def length(sec: str) -> float:
        return float(getattr(graph.nodes[sec], "length_m", 0.0) or 0.0)

    out: dict[str, dict[str, float]] = collections.defaultdict(dict)
    for station, sec, _years, _reg in donors:
        water = water_of.get(sec)
        if water is None or sec not in graph.nodes:
            continue
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
                    if node is None or _kind(node) != "stream" or water_of.get(nxt) != water:
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
          live: set[str] | None = None,
          measures: dict[str, set[str]] | None = None) -> Panels:
    """Candidates, gates, weights, and the dictionary — the whole pass.

    `water_of` (section -> water item) is what says which donors are a section's OWN gauges; without it nothing
    leads and the usual weighting applies everywhere.

    `live` is the stations that report in REAL TIME (ECCC's `realtime` flag): only those may lead. The feed carries
    nothing else, so a historic gauge could never lead anyway — and kept on top of the member cap it only crowds the
    panel. None (synthetic callers) lets any own gauge lead.

    `measures` is each station's quantities with a climatology (`gauge_stats` parameters: discharge, level) — the
    only quantities it can ever be ranked in. Per quantity, the nearest `OWN_LEADS_PER_QUANTITY` own gauges that may
    lead are kept as candidates and the rest are ordinary members: on the lower Fraser at Mission 12 real-time own
    gauges are near enough and big enough (11 with a level record, 4 with discharge), and a panel of 16 donors is a
    table nobody reads; with 4 per quantity it holds at most 10, and Hope — 4th of the discharge ones — still leads Flow.
    None treats every station as measuring both.
    """
    area_of = lambda n: model.area_km2(getattr(n, "stream_magnitude", None),
                                       getattr(n, "wsc", ""))
    cand = candidates(graph, area_of, donors, max_ratio)
    own = own_distances(graph, water_of, donors) if water_of else {}

    by_section: dict[str, tuple[int, float | None]] = {}
    members: dict[int, tuple[Member, ...]] = {}
    intern: dict[tuple, int] = {}
    facts = {station: (yrs, reg) for station, _sec, yrs, reg in donors}
    for sec, cs in cand.items():
        node = graph.nodes.get(sec)
        if node is None:
            continue
        at = area_of(node)
        by_station = {station: ad for station, _r, ad, *_ in cs}
        # THE OWN GAUGES THAT MAY LEAD, nearest first; ties go to the one nearer this section in size. See
        # "a water's own gauge comes first" in panel.py.
        near = own.get(sec, {})
        leads = [st for _d, _r, st in sorted(
            (d, abs(math.log(by_station[st] / at)), st) for st, d in near.items()
            if st in by_station and at and by_station[st] and (live is None or st in live)
            and may_lead(at, by_station[st], d))]
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
        panel = panel_for(cs, at, keep=leads)
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
    for sec, parent in mainstem_of(graph, area_of).items():
        got = by_section.get(parent)
        if got is None:
            by_section.pop(sec, None)      # the river has no panel; neither can its channel
        else:
            by_section[sec] = got
    return Panels(by_section=by_section, members=members)
