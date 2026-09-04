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
from dataclasses import dataclass

from pipeline.atlas.gauges.panel import MAX_AREA_RATIO, Donor, panel_for
from pipeline.atlas.graph.drainage import AreaModel


@dataclass(frozen=True)
class Member:
    """A donor's own facts. Everything a screen shows is derived from these at read time."""

    station: str
    role: str                 # up | down
    area_km2: float
    years: int


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
        """`(panel_id, ord, station, role, area_km2, years)`."""
        for pid, ms in sorted(self.members.items()):
            for i, m in enumerate(ms):
                yield (pid, i, m.station, m.role, round(m.area_km2, 3), m.years)


def _kind(node) -> str:
    k = getattr(node, "kind", "")
    return k.value if hasattr(k, "value") else str(k).split(".")[-1]


def candidates(graph, area_of, donors: list[tuple[str, str, int, bool]],
               max_ratio: float = MAX_AREA_RATIO) -> dict[str, list[tuple]]:
    """`section -> [(station, role, donor area, years, regulated, crossed a lake)]`.

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
        # Walking upstream reaches sections the gauge is DOWNSTREAM of, and vice versa.
        for adj, role in ((up, "down"), (down, "up")):
            seen: set[str] = {sec}
            stack: list[tuple[str, bool]] = [(sec, False)]
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
                        out[nxt].append((station, role, a0, years, regulated, lake))
                    stack.append((nxt, lake))
    return out


def build(graph, model: AreaModel,
          donors: list[tuple[str, str, int, bool]],
          max_ratio: float = MAX_AREA_RATIO) -> Panels:
    """Candidates, gates, weights, and the dictionary — the whole pass."""
    area_of = lambda n: model.area_km2(getattr(n, "stream_magnitude", None),
                                       getattr(n, "wsc", ""))
    cand = candidates(graph, area_of, donors, max_ratio)

    by_section: dict[str, tuple[int, float | None]] = {}
    members: dict[int, tuple[Member, ...]] = {}
    intern: dict[tuple, int] = {}
    facts = {station: (yrs, reg) for station, _sec, yrs, reg in donors}
    for sec, cs in cand.items():
        node = graph.nodes.get(sec)
        if node is None:
            continue
        at = area_of(node)
        # The gates and the cap still run HERE, at build time, because both depend on the
        # target and both are cheap once rather than per tap. What is not stored is what
        # they produced — only which donors survived.
        panel = panel_for(cs, at)
        if not panel:
            continue
        by_station = {station: ad for station, _r, ad, *_ in cs}
        chosen = tuple(Member(d.station, d.role, by_station[d.station],
                              facts.get(d.station, (0, False))[0])
                       for d in panel)
        # Interned on the SET — station, role, and the donor's own facts. Nothing here
        # depends on the target, which is exactly why it collapses.
        key = tuple(sorted((m.station, m.role, round(m.area_km2, 3), m.years)
                           for m in chosen))
        pid = intern.setdefault(key, len(intern))
        by_section[sec] = (pid, at)
        members.setdefault(pid, chosen)
    return Panels(by_section=by_section, members=members)
