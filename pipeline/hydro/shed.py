"""The gauge-shed pass: which sections a hydrometric gauge is entitled to speak for.

THE PROBLEM THIS SOLVES
    BC has ~440 transmitting gauges and 2.02 million sections. A naive app answers "what is
    the flow here?" by finding the nearest station, and is wrong almost everywhere: the
    nearest gauge to a headwater creek is routinely on a river draining a thousand times
    more country. It will return a confident number about a different river.

    So the question is never "which gauge is closest". It is "is there a gauge on the SAME
    WATER, and how much of what it measures is this?"

WHAT COUNTS AS THE SAME WATER
    A gauge sits on one node. It speaks for the water that flows through it — everything
    UPSTREAM (whose flow it has already counted) and everything DOWNSTREAM until another
    tributary of consequence joins. Both directions are one walk over the flow graph; a
    section reached by neither walk is in a different drainage and gets nothing, however
    close it looks on a map.

    AND IT MUST BE IN THE GAUGE'S OWN WATERSHED. The walk alone is not enough, because
    walking downstream from a tributary arrives at the mainstem — and a gauge on a
    tributary has not seen the mainstem's water. SLESSE CREEK NEAR VEDDER CROSSING was
    speaking for 13 reaches of the Chilliwack, 7 of them rated `fair`, off a creek carrying
    a seventh of the river. The magnitude ratio cannot catch it: symmetric, it reads "the
    creek is 14% of the river" as a moderately good description, when the honest reading is
    that 86% of the water is unaccounted for.

    So a reach qualifies only if its FWA watershed code IS the gauge's or DESCENDS from it —
    "this water drains through that gauge". Directional by construction, which is the thing
    a ratio can never be. Measured on the real bundle: 3,416 of 118,331 rows refused, 642 of
    them previously rated `good`.

HOW HONEST THE ANSWER IS
    ``stream_magnitude`` is the count of headwater links draining through a node — the
    graph's own proxy for discharge, and the only one that exists for every section. The
    ratio between a section's magnitude and the gauge node's is how much of the gauge's
    water is this section's:

        ratio = min(section_mag, gauge_mag) / max(section_mag, gauge_mag)

    Symmetric on purpose. Standing upstream of a gauge, the question is what fraction of its
    reading you contribute; standing downstream, what fraction of your flow it has seen.
    Both are the same number, and both degrade for the same reason.

    The bands are deliberately pessimistic. `weak` at one part in a thousand is still worth
    showing — a rising river is a rising river — but it must never be shown as a discharge
    for that creek, and the client is told which band it got so it can say so. Below
    `weak` the honest answer is no gauge, and no row is written.

DETERMINISM
    Ties are broken on the station id, never on iteration order (AGENTS rule 19).
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from pipeline.utils.wsc import trim_wsc as _trim

# Fraction of the gauge's own drainage that this section accounts for.
#
# Chosen against the ratio distribution, not out of the air: a mainstem section between two
# confluences typically lands 0.3–0.9 of its gauge (good), a named tributary 0.01–0.1
# (fair), an unnamed headwater trickle 1e-4 (below weak → no gauge). The point of three
# bands rather than a number is that "0.037" invites a reader to treat it as precision.
TRUST_BANDS: tuple[tuple[str, float], ...] = (
    ("good", 0.10),
    ("fair", 0.01),
    ("weak", 0.001),
)

# Past this many hops the walk stops even if magnitudes still look reasonable. A very long
# river with no tributary of consequence would otherwise carry one gauge for 400 km.
_MAX_HOPS = 400

# ONE STATION PER SECTION: the one that most nearly IS this water.
#
# The instinct to keep several was wrong, and measuring it said so. A second gauge on the
# same reach is by definition further from being that reach — it drains more country, or
# less, than the one already chosen — so it can only ever be a worse answer to the same
# question. It is not a second opinion; it is a diluted one.
#
# The variety that matters survives anyway, because it lives at the RIVER level rather than
# the reach level: different reaches legitimately have different best gauges. Measured on
# the real bundle, keeping only the best per reach leaves the Stellako seeing 3 distinct
# stations across its length, the Chilliwack 6 and the Thompson 7. Before the cap those
# numbers were 43, 17 and 258 — which is the same handful of stations repeated, not more
# information.
#
# 1,346,633 rows -> 558,746 (44 MB -> 18 MB), coverage identical at 558,746 sections.


def trust_for(section_mag: Optional[int], gauge_mag: Optional[int]) -> Optional[str]:
    """The band, or None when no gauge should be offered at all.

    A missing magnitude is not a small magnitude. A lake node has no headwater count, and a
    section whose magnitude never got computed is unknown, not tiny — both return None
    rather than being quietly ranked at the bottom.
    """
    if not section_mag or not gauge_mag:
        return None
    lo, hi = sorted((int(section_mag), int(gauge_mag)))
    if hi <= 0:
        return None
    ratio = lo / hi
    for band, floor in TRUST_BANDS:
        if ratio >= floor:
            return band
    return None


@dataclass(frozen=True)
class GaugeLink:
    """One (section, gauge) claim, with the evidence for it."""
    section_id: str
    station: str
    trust: str
    ratio: float
    hops: int


def _walk(graph, start: str, adj: dict[str, list[int]], forward: bool,
          keep) -> Iterable[tuple[str, int]]:
    """Breadth-first over the flow graph, yielding (node_id, hops) for nodes ``keep`` admits.

    PRUNING IS SOUND HERE. Magnitude falls monotonically upstream and rises monotonically
    downstream, so the trust ratio decays monotonically in both directions away from the
    gauge: a node behind a failed one cannot be redeemed, and the whole branch drops. That
    makes this an optimisation on well-formed data — without it one Fraser headwater gauge
    walks the entire basin to the sea to emit nothing — and a GUARD on ill-formed data,
    where a braid or a bad magnitude would otherwise let a shed jump a river it lost.
    """
    seen = {start}
    q: deque[tuple[str, int]] = deque([(start, 0)])
    while q:
        node, hops = q.popleft()
        if not keep(node):
            continue                     # neither emitted nor expanded past
        yield node, hops
        if hops >= _MAX_HOPS:
            continue
        for ix in sorted(adj.get(node, ())):
            e = graph.edges[ix]
            nxt = e.from_node if forward else e.to_node
            if nxt not in seen and nxt in graph.nodes:
                seen.add(nxt)
                q.append((nxt, hops + 1))


def is_lake_station(graph, node_id: str) -> bool:
    """Does this station sit on a lake rather than on flowing water?

    THE TWO ARE NOT THE SAME MEASUREMENT. A lake station reports LEVEL, in metres above a
    datum; a stream station reports DISCHARGE, in cubic metres a second. A trust ratio
    between them is arithmetic on two different quantities, and the number it produces is
    meaningless however good it looks.

    It was not meaningless in a harmless way. Lake nodes carry a stream magnitude (303,932
    of 304,183 do), so before this check a lake station walked downstream exactly like a
    river station: 198 stations, and **11,049 stream sections being told a lake's level**.
    Fourteen of those stations are explicitly dams or spillways — Mica, Strathcona, Ruskin,
    the Nechako spillway — where the outflow is whatever an operator decided and says
    nothing whatever about the river below.

    Lake stations are not discarded; they are linked to the lake itself by
    ``lake_gauge_links``, which is the question they can actually answer.
    """
    n = graph.nodes.get(node_id)
    return n is not None and str(getattr(n, "kind", "")).endswith("lake")


def lake_gauge_links(graph, stations: list[dict], node_for_station: dict[str, str],
                     ) -> list[tuple[str, str]]:
    """``(lake_node_id, station)`` for every station sitting on a lake, sorted.

    No trust band, deliberately. Trust is a statement about how much of a gauge's DISCHARGE
    a reach accounts for, and a lake level has no such fraction — the gauge is either on
    this lake or it is not. Inventing a band here would put a familiar-looking word next to
    a number that does not support it.
    """
    known = {s["station"] for s in stations}
    return sorted(
        (node_for_station[st], st)
        for st in node_for_station
        if st in known and is_lake_station(graph, node_for_station[st])
    )


def drains_through(section_wsc: str, gauge_wsc: str) -> bool:
    """Does water at ``section_wsc`` flow through a gauge at ``gauge_wsc``?

    FWA watershed codes are hierarchical: a tributary's code is its trunk's code plus one
    more segment. So the gauge's code being a PREFIX of the reach's means the reach drains
    into the gauge — and the reverse means the gauge is on a tributary of the reach, which
    is the direction that must be refused.

    Dash-guarded, so `100-025956` never matches a sibling numbered `100-0259560`. An unknown
    code on either side is a no: a shed built on a guess is the failure this file exists to
    prevent.
    """
    if not section_wsc or not gauge_wsc:
        return False
    return section_wsc == gauge_wsc or section_wsc.startswith(gauge_wsc + "-")


def build_gauge_sheds(graph, stations: list[dict], node_for_station: dict[str, str],
                      prefer: set[str] | None = None) -> list[GaugeLink]:
    """Every (section, gauge) pair worth storing — ALL gauges per section, best first.

    ``node_for_station`` comes from the spatial match (``pipeline.hydro.match``) and is kept
    a separate argument on purpose: matching needs geometry and a 5 GB atlas, this needs
    neither, so the expensive half can be cached and this half re-run freely.

    ``prefer`` is the set of stations ECCC still lists as ACTIVE. Sheds are built from those
    only.

    ACTIVE, NOT "TRANSMITTING RIGHT NOW", and the difference is a whole class of station. BC
    has 27 stations that are active but silent today: seasonal gauges, shut down for the
    winter and back in the spring. Filtering on today's traffic would delete their sheds
    every autumn and rebuild them every spring, so a river would appear to lose its gauge
    for half the year. ECCC's own status is the durable fact; whether a station is talking
    this minute stays the feed's answer.

    WHY IT IS A FILTER AND NOT JUST A RANKING. A shed exists so the app can put a number on a
    reach; a station that cannot report has no number to give, so its shed is rows nobody can
    ever read. BC's roster is 1,885 discontinued against 439 transmitting, and carrying the
    dead ones cost 584,211 rows to serve 112,942 useful ones — four fifths of the table, and
    most of the bundle's remaining weight.

    A discontinued station keeps its `gauge` row and its climatology; what it loses is the
    claim to speak for two million reaches it can no longer say anything about. A station
    that resumes reporting is picked up at the next build, and the roster is refetched every
    build.

    WHY THAT IS NOT A LIVENESS COLUMN. Nothing about it is written into the bundle; it is a
    build-time RANKING input, the same way magnitude is. The distinction matters because the
    alternative was measured and is unusable: BC's roster is 1,885 discontinued stations
    against 439 transmitting, so ranking on representativeness alone gave 90% of gauged
    reaches a station that closed decades ago. The map could colour 9% of the province and
    the sheet 404'd on the rest — a technically perfect answer nobody can read a number from.

    EVERY STATION IS KEPT, not the single best one. Two gauges on one river answer different
    questions — the Chilliwack's lake-outlet station and its Vedder Crossing station
    describe genuinely different water, and somebody fishing the canyon wants the upper one.
    Rows come back sorted by section then by descending ratio, so a client that wants only
    the best takes the first and one that wants the choice has it.

    Lake stations are excluded here and handled by ``lake_gauge_links`` — see
    ``is_lake_station`` for why mixing them is not a rounding error.
    """
    by_id = {s["station"]: s for s in stations}
    found: dict[str, list[GaugeLink]] = {}

    for station in sorted(node_for_station):
        node_id = node_for_station[station]
        gauge_node = graph.nodes.get(node_id)
        if gauge_node is None or not by_id.get(station):
            continue
        if prefer is not None and station not in prefer:
            continue            # retired: its shed is rows nobody can ever read
        if is_lake_station(graph, node_id):
            continue            # a level, not a discharge — linked to the lake instead
        gauge_mag = gauge_node.stream_magnitude
        if not gauge_mag:
            continue            # a gauge we cannot scale speaks for its own node only
        gauge_wsc = _trim(getattr(gauge_node, "wsc", ""))
        if not gauge_wsc:
            continue            # nothing to bound the shed with; see `drains_through`

        # PRUNING ONLY, and deliberately still on magnitude alone. `keep` controls whether
        # the walk EXPANDS PAST a node as well as whether it yields it, and 417,420 of the
        # province's 721,353 lake nodes carry no watershed code — testing drainage here
        # would stop every walk at the first such lake and sever a river from its own
        # headwaters. Whether a reach may be CLAIMED is decided below, where it is only a
        # claim and not also a wall.
        def keep(node: str, _mag: int = gauge_mag) -> bool:
            return trust_for(graph.nodes[node].stream_magnitude, _mag) is not None

        walks = (
            _walk(graph, node_id, graph.up_adj, True, keep),    # its tributaries
            _walk(graph, node_id, graph.down_adj, False, keep),  # what it flows into
        )
        for walk in walks:
            for sec, hops in walk:
                # A lake reached from a river gauge is skipped for the same reason as the
                # reverse: the river's discharge is not the lake's level.
                if is_lake_station(graph, sec):
                    continue
                # THE GAUGE MUST BE IN THIS REACH'S DRAINAGE. The walk can arrive at water
                # the gauge has never seen — downstream from a tributary is the mainstem —
                # and the magnitude ratio cannot tell the difference, because it is
                # symmetric and the question is not. See `drains_through`.
                if not drains_through(_trim(getattr(graph.nodes[sec], "wsc", "")),
                                      gauge_wsc):
                    continue
                mag = int(graph.nodes[sec].stream_magnitude)
                band = trust_for(mag, gauge_mag)
                if band is None:                    # keep() already refused these
                    continue
                lo, hi = sorted((mag, int(gauge_mag)))
                found.setdefault(sec, []).append(
                    GaugeLink(sec, station, band, lo / hi, hops))

    # Best ratio wins; the station id breaks a tie so a rebuild is byte-identical.
    return [min(found[sec], key=lambda l: (-l.ratio, l.station)) for sec in sorted(found)]


def downstream_map(graph, sections: Iterable[str]) -> dict[str, str]:
    """section -> the section it flows into, for shed members only.

    This is the chain a client walks to answer "how does my spot reach that gauge". It is
    stored for shed members and nobody else: 2.02M pointers would be most of the bundle,
    and every hop on a trace to a gauge is by definition inside that gauge's shed.

    A section with several outgoing edges (a braid, a distributary) takes the mainstem one
    if there is one — a trace must follow the river, not wander into a side channel.
    """
    from pipeline.models.graph import MAINSTEM_EDGE_KINDS

    want = set(sections)
    out: dict[str, str] = {}
    for sec in sorted(want):
        best: tuple[int, str] | None = None
        for ix in sorted(graph.down_adj.get(sec, ())):
            e = graph.edges[ix]
            if e.to_node not in graph.nodes:
                continue
            rank = (0 if getattr(e, "kind", "") in MAINSTEM_EDGE_KINDS else 1, e.to_node)
            if best is None or rank < best:
                best = rank
        if best is not None:
            out[sec] = best[1]
    return out


def load_stations(path: Path, *, live_only: bool = False) -> list[dict]:
    """Read the fetched roster.

    ``live_only`` is the difference between "a gauge exists here" and "a gauge is talking".
    The bundle stores both kinds: a discontinued station still anchors a climatology, and a
    client that only ever saw live gauges would tell a user there is no gauge on a river
    that has had one since 1913.
    """
    import json

    rows = json.loads(path.read_text(encoding="utf-8"))
    if live_only:
        rows = [r for r in rows if r.get("realtime")]
    return sorted(rows, key=lambda r: r["station"])
