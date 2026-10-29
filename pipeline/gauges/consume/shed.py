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

from pipeline.common.utils import wsc as _wsc
from pipeline.common.utils.wsc import trim_wsc as _trim
from pipeline.gauges.liveness import speaks_now
from pipeline.gauges.consume.panels import LARGE_SHARE

#: THE SHED CLIMBS AS THE TRIBUTARY WALK CLIMBS (T117, owner 2026-10-22: one entry check, used in every
#: walk). Going UP from a gauge, each step is one `tributaries.may_enter` allows with the gauge's own
#: section as the reach (`via=UP`: the river above the gauge is its shed): never into a canal, a water the
#: gauge's river drains into, a bigger river where the code cannot say, or across a code run's divide.
#: Going DOWN is not a tributary step and is untouched. Switch it off and the climb is the raw flow graph.
SHED_CLIMBS_THROUGH_MAY_ENTER = True

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
    """One (section, gauge) claim, with the evidence for it.

    `rel` / `dist_m` (T93): how the GAUGE stands to this section (`REL_*` below) and the channel distance between them
    along the walk that found it. Every candidate carries them, admitted or not, so a policy (`POLICIES`) is a choice
    over facts already in hand — never a re-walk, never a re-match.
    """
    section_id: str
    station: str
    trust: str
    ratio: float
    hops: int
    rel: str = ""
    dist_m: int = 0
    #: (Q31) the section's water and the gauge's are DIFFERENT waters of one continued system — a `water_continue` link
    #: joins them (the Vedder goes on as... the Chilliwack under another name). Only a policy can make it matter.
    joined: bool = False

    @property
    def drains(self) -> bool:
        """This reach's water drains through the gauge (`drains_through`): the drainage gate's yes."""
        return self.rel in DRAINS_THROUGH


# ---- THE RELATIONSHIP (T93) ------------------------------------------------------------------------------------------
#
# WHERE THE GAUGE STANDS, SEEN FROM THE SECTION — the words the app says it in ("the gauge is 12 km downstream"). Decided
# by the two walks (which side of the gauge) and the FWA codes (`wsc.relation`: the same watercourse, or a branch), so it
# is a fact about the river network, not a judgement; the judgement is the policy below.
REL_ON = "on"                  # the gauge stands on this section
REL_DOWN = "down"              # downstream, on this same watercourse (same FWA code)
REL_UP = "up"                  # upstream, on this same watercourse
REL_DOWN_TRUNK = "down_trunk"  # downstream, on the river this reach's water flows into (this reach is its tributary)
REL_UP_BRANCH = "up_branch"    # on a TRIBUTARY of this reach's water: Slesse Creek's gauge seen from the Chilliwack
REL_APART = "apart"            # the walk reached it, the codes say two branches (a braid across a code run)
REL_UNKNOWN = "unknown"        # a watershed code is missing: nothing can be said

#: Every relationship, in the order the app's vocabulary lists them (emitted as `GaugeRel`, gauge-policy.generated.ts).
GAUGE_RELS = (REL_ON, REL_DOWN, REL_UP, REL_DOWN_TRUNK, REL_UP_BRANCH, REL_APART, REL_UNKNOWN)

#: THE SHIPPED ENCODING (T93, measured on the province: rel as TEXT + dist_m to 100 m cost conditions +359 kB gzip, past its
#: gate). `section_gauge.rel` is the INDEX into `GAUGE_RELS`, `section_gauge.dist_b` a distance BUCKET (both +74 kB):
#: 0 under 500 m, 1 under 1 km, then 1 km x DIST_STEP^(b-2) to the nearest step — "about 11 km", which is all a
#: channel distance off FWA 1:20k section ends can honestly say. The bundle's `gauge_relation` keeps text + exact metres.
DIST_STEP = 1.5


def rel_code(rel: str) -> int:
    return GAUGE_RELS.index(rel)


def shown_rel(link: "GaugeLink") -> str:
    """What the app is told (`section_gauge.rel`). Across a continuation (Q31) the gauge is up THIS river under its other
    name — the Vedder's 08MH001 is "upstream", not "on a tributary" (FWA's code shape); `gauge_relation` keeps the
    candidate's own `up_branch`."""
    return REL_UP if (link.joined and link.rel == REL_UP_BRANCH) else link.rel


def dist_bucket(metres: int | float) -> int:
    """The shipped distance bucket (see `DIST_STEP`); `dist_metres` is its inverse to the step's centre."""
    if metres < 500:
        return 0
    if metres < 1000:
        return 1
    return 2 + max(0, round(math.log(metres / 1000.0) / math.log(DIST_STEP)))


#: The relationships under which this reach's water has passed the gauge — `drains_through` (the gate) as a set.
DRAINS_THROUGH = frozenset({REL_ON, REL_DOWN, REL_UP, REL_DOWN_TRUNK})


def relate(section_wsc: str, gauge_wsc: str, *, hops: int, upstream_walk: bool) -> str:
    """Where a gauge stands to a section the shed walk reached. `upstream_walk`: found climbing from the gauge (so the
    section is above it, the gauge downstream). Equivalent to `drains_through` on the gate's question:
    `relate(...) in DRAINS_THROUGH` == `drains_through(section_wsc, gauge_wsc)` for every pair (pinned in the tests)."""
    r = _wsc.relation(section_wsc, gauge_wsc)
    if r == _wsc.UNKNOWN:
        return REL_UNKNOWN
    if r == _wsc.SAME:
        return REL_ON if hops == 0 else (REL_DOWN if upstream_walk else REL_UP)
    if r == _wsc.BELOW:                 # this reach's code extends the gauge's: it drains into the gauge's water
        # climbing: a tributary above the gauge. Descending (rare): a side channel FWA codes as a branch, below the
        # gauge — the gauge is upstream of it, and its water still passed the gauge
        return REL_DOWN_TRUNK if upstream_walk else REL_UP
    if r == _wsc.ABOVE:                 # the gauge's code extends this reach's: the gauge is on a branch of it
        return REL_UP_BRANCH
    return REL_APART


# ---- THE POLICY (T93, owner 2026-09-02: "the gauge rule should be a tunable policy") ---------------------------------
#
# A policy ADMITS candidates (which relationships may speak at all); the rank among the admitted is common to every
# policy (best ratio, then a transmitting station, then the id — see `choose`). Changing the policy is a bundle rebuild
# (`GAUGE_POLICY`), never a re-match: the candidates and their relationships are the same.

@dataclass(frozen=True)
class GaugePolicy:
    name: str
    admits: frozenset
    doc: str
    #: relationships admitted ONLY across a continuation (`GaugeLink.joined`) — and only for a section with no admitted
    #: gauge on its OWN watercourse: a continuation fills a gap, it never outranks the water's own gauge (measured: the
    #: Bella Coola's side channels swapped 08FB011, on the Bella Coola, for the Atnarko's 08FB006 72 km away on ratio)
    admits_joined: frozenset = frozenset()
    #: ...and only when the gauge's drainage is a large share of the reach's: the symmetric magnitude `ratio` at or above
    #: this (owner 2026-10-28, Q31 refinement — the Q44 "large share of the river's flow" floor, not a new number)
    joined_min_ratio: float = 0.0

    def admit(self, link: GaugeLink) -> bool:
        return link.rel in self.admits or (link.joined and link.rel in self.admits_joined
                                           and link.ratio >= self.joined_min_ratio)


POLICIES: dict[str, GaugePolicy] = {p.name: p for p in (
    # TODAY'S GATE, unchanged: the reach's water drains through the gauge (`drains_through`).
    GaugePolicy("drainage", DRAINS_THROUGH,
                "the reach's water drains through the gauge (the FWA-code gate since Slesse Creek)"),
    # THE DEFAULT (owner 2026-10-28, Q31): the gate, plus a gauge on a water this one CONTINUES as under another name.
    # FWA codes the Chilliwack as a branch of the Vedder, so 08MH001 at Vedder Crossing is `up_branch` from the Vedder
    # and the gate gave the Vedder the Fraser at Mission; across the `water_continue` link it is the Vedder's own river.
    # Slesse Creek is a tributary, not a continuation: its gauge stays off the Chilliwack. And only a LARGE SHARE
    # (owner 2026-10-28): the gauge's drainage at least `panels.LARGE_SHARE` (0.5, Q44's "large share of the river's
    # flow") of the reach's by magnitude — 08MH001 on the Vedder (0.985-1.0) passes; 08MH016 on a Vedder side reach
    # (0.002) and the Atnarko's 08FB006 on Bella Coola side reaches (0.001) do not.
    GaugePolicy("drainage_continued", DRAINS_THROUGH,
                "the drainage gate, plus a gauge up a water this one continues as (Vedder <- Chilliwack), "
                "when it carries a large share (Q44's LARGE_SHARE) of the reach's drainage",
                admits_joined=frozenset({REL_UP_BRANCH}), joined_min_ratio=LARGE_SHARE),
    # Stricter: one watercourse only — a tributary reach is not told its trunk river's flow.
    GaugePolicy("same_watercourse", frozenset({REL_ON, REL_DOWN, REL_UP}),
                "only a gauge on this reach's own watercourse (same FWA code), up or down"),
    # The behaviour BEFORE the gate (kept to show what the gate refuses): a gauge on a tributary may speak for its
    # trunk river, as Slesse Creek did for 13 reaches of the Chilliwack.
    GaugePolicy("ungated", frozenset({REL_ON, REL_DOWN, REL_UP, REL_DOWN_TRUNK, REL_UP_BRANCH}),
                "any gauge the walk reaches whose code relationship is known (pre-gate; Slesse speaks for the Chilliwack)"),
)}

#: The policy every build uses (owner 2026-10-28, Q31: "drainage, plus tributary gauges across a continuation link").
#: `drainage` alone was the default until then (T93), byte-identical to the gate before it.
GAUGE_POLICY = "drainage_continued"


def _walk(graph, start: str, adj: dict[str, list[int]], forward: bool,
          keep) -> Iterable[tuple[str, int, float]]:
    """Breadth-first over the flow graph, yielding (node_id, hops, metres) for nodes ``keep`` admits.

    METRES (T93) is the channel distance from the gauge to the near end of the node, along the walk's own tree. A
    station stands at the DOWNSTREAM end of its section (`place.nodes_for` takes the section that begins at the gauge
    and runs upstream), so climbing, the gauge's own section is the first stretch walked; descending, the section
    below it starts at the gauge (0 m).

    PRUNING IS SOUND HERE. Magnitude falls monotonically upstream and rises monotonically
    downstream, so the trust ratio decays monotonically in both directions away from the
    gauge: a node behind a failed one cannot be redeemed, and the whole branch drops. That
    makes this an optimisation on well-formed data — without it one Fraser headwater gauge
    walks the entire basin to the sea to emit nothing — and a GUARD on ill-formed data,
    where a braid or a bad magnitude would otherwise let a shed jump a river it lost.
    """
    ctx = None
    if forward and SHED_CLIMBS_THROUGH_MAY_ENTER:
        from pipeline.atlas.graph import tributaries as _tribs
        ctx = _tribs.reach_context(graph, {start})
    seen = {start}
    q: deque[tuple[str, int, float]] = deque([(start, 0, 0.0)])
    while q:
        node, hops, metres = q.popleft()
        if not keep(node):
            continue                     # neither emitted nor expanded past
        yield node, hops, metres
        if hops >= _MAX_HOPS:
            continue
        # the stretch between this node's near end and the next node's: this node's own length, except descending
        # from the gauge's section, whose downstream end IS the gauge
        step = float(getattr(graph.nodes[node], "length_m", 0.0) or 0.0) if (forward or node != start) else 0.0
        for ix in sorted(adj.get(node, ())):
            e = graph.edges[ix]
            nxt = e.from_node if forward else e.to_node
            if nxt in seen or nxt not in graph.nodes:
                continue
            if ctx is not None and _tribs.may_enter(ctx, nxt, node, e, via=_tribs.UP) is not None:
                continue                 # the tributary walk's own refusal (T117)
            seen.add(nxt)
            q.append((nxt, hops + 1, metres + step))


#: The water kinds that report a LEVEL, not a discharge: standing water, as the registry calls it.
STANDING_WATER = ("lake", "wetland")


def is_lake_station(graph, node_id: str, registry=None) -> bool:
    """Does this station sit on a lake rather than on flowing water?

    THE WATER KIND, NOT THE NODE'S SHAPE (T121, owner 2026-10-22): asked of the one place it is
    decided, `reach.water_kind.kind_of` — the kind of the registry item owning the section, else the
    graph node's. A river's own polygon (the Stellako's wide reach) or a slough FWA draws as a lake
    is flowing water on the page, so a station there reports the river's discharge and sheds like
    one. `registry=None` (a fixture with no registry) reads the node's kind, as `kind_of` does.

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
    from pipeline.atlas.reach.water_kind import kind_of
    return node_id in graph.nodes and kind_of(graph, registry, node_id) in STANDING_WATER


def lake_gauge_links(graph, stations: list[dict], node_for_station: dict[str, str],
                     registry=None) -> list[tuple[str, str]]:
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
        if st in known and is_lake_station(graph, node_for_station[st], registry)
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
    return _wsc.is_at_or_below(section_wsc, gauge_wsc)


def choose(found: dict[str, list[GaugeLink]], by_id: dict[str, dict],
           policy: str = GAUGE_POLICY) -> list[GaugeLink]:
    """ONE station per section: the best candidate `policy` admits, sorted by section.

    Best ratio wins. A TIE IS BROKEN ON WHETHER THE STATION IS TRANSMITTING, and only then
    on the id for determinism.

    The id alone was doing real damage, because a tie is not rare: two stations that resolve
    to the SAME node have ratio 1.0 against each other and the lower id took every section.
    Measured on the province, six active stations spoke for nothing at all —

        08MH044 FRASER RIVER AT WHONOCK          lost to 08MH024 (Fraser at Mission)
        08MH103 CHILLIWACK RIVER ABOVE SLESSE    lost to 08MH016 (outlet of Chilliwack Lake)
        08HD007 SALMON RIVER ABOVE MEMEKAY       lost to 08HD006 (near Sayward)

    — and in the Duncan and Quinsam cases the id happened to pick the DISCONTINUED station
    over the transmitting one, so the reach was handed a gauge with no reading. Sharing a
    node at all is the deeper problem (those pairs are up to 18 km apart and should be cut
    into different sections); until the cuts separate them, preferring the station a reader
    can actually get a number from is the honest tiebreak.
    """
    pol = POLICIES[policy]                    # an unknown name raises: a typo must not build a different app

    def rank(l: GaugeLink) -> tuple:
        return (-l.ratio, 0 if speaks_now(by_id.get(l.station, {})) else 1, l.station)

    own = frozenset({REL_ON, REL_DOWN, REL_UP})
    out = []
    for sec in sorted(found):
        admitted = [l for l in found[sec] if pol.admit(l)]
        if pol.admits_joined and any(l.rel in own and not l.joined for l in admitted):
            admitted = [l for l in admitted if not (l.joined and l.rel in pol.admits_joined
                                                    and l.rel not in pol.admits)]
        if admitted:
            out.append(min(admitted, key=rank))
    return out


def build_gauge_sheds(graph, stations: list[dict], node_for_station: dict[str, str],
                      prefer: set[str] | None = None,
                      *, report: list[str] | None = None, registry=None,
                      policy: str = GAUGE_POLICY,
                      candidates: list[GaugeLink] | None = None,
                      water_of: dict[str, str] | None = None, continues=()) -> list[GaugeLink]:
    """Every (section, gauge) pair worth storing — ONE gauge per section, the policy's best.

    ``policy`` (T93): which relationships may speak — `POLICIES`, default `GAUGE_POLICY` (today's drainage gate).
    ``candidates``: when a list, receives EVERY pair the walks reached, admitted or not, with its relationship and
    distance, sorted by (section, station) — the bundle's `gauge_relation` table.
    ``water_of`` (section -> water item) and ``continues`` (the settled `water_continue` item pairs,
    `continuation.item_pairs`) mark a candidate `joined` when its section's water and the gauge's are different waters
    of one continued system (`panels.systems`, the donor panels' own reading of T76). Without them nothing is joined.

    ``node_for_station`` comes from the spatial match (``pipeline.gauges.generate.match``) and is kept
    a separate argument on purpose: matching needs geometry and a 5 GB atlas, this needs
    neither, so the expensive half can be cached and this half re-run freely.

    ``prefer`` is the set of stations ECCC still lists as ACTIVE (`liveness.may_speak`). Sheds
    are built from those only.

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

    ONE STATION PER SECTION — see the module header for the measurement behind that. This
    docstring used to claim the opposite ("every station is kept, sorted by descending
    ratio"); it was a survivor of the change that introduced the cap, and the code has
    returned exactly one row per section since.

    ``report`` collects a line for every station that produces NO rows, with the reason.

    Lake stations are excluded here and handled by ``lake_gauge_links`` — see
    ``is_lake_station`` for why mixing them is not a rounding error.
    """
    by_id = {s["station"]: s for s in stations}
    from pipeline.gauges.consume.panels import systems      # lazily: panels is the heavier module
    system = systems(continues)
    found: dict[str, list[GaugeLink]] = {}
    # Why each station produced nothing. Every `continue` below records itself: a station
    # that silently vanishes here is a river the app calls ungauged, and the only way that
    # was ever noticeable was by differencing two bundles.
    dropped: dict[str, str] = {}

    for station in sorted(node_for_station):
        node_id = node_for_station[station]
        gauge_node = graph.nodes.get(node_id)
        if gauge_node is None or not by_id.get(station):
            dropped[station] = ("node not in this graph" if gauge_node is None
                                else "not in the fetched roster")
            continue
        if prefer is not None and station not in prefer:
            continue            # retired: its shed is rows nobody can ever read — expected
        if is_lake_station(graph, node_id, registry):
            continue            # a level, not a discharge — linked to the lake instead
        gauge_mag = gauge_node.stream_magnitude
        if not gauge_mag:
            dropped[station] = "gauge node carries no stream_magnitude"
            continue            # a gauge we cannot scale speaks for its own node only
        gauge_wsc = _trim(getattr(gauge_node, "wsc", ""))
        if not gauge_wsc:
            dropped[station] = "gauge node carries no watershed code"
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
        gauge_water = (water_of or {}).get(node_id)
        gauge_system = system.get(gauge_water) if gauge_water else None
        mine: dict[str, GaugeLink] = {}
        for upstream_walk, walk in zip((True, False), walks):
            for sec, hops, metres in walk:
                # A lake reached from a river gauge is skipped for the same reason as the
                # reverse: the river's discharge is not the lake's level.
                if is_lake_station(graph, sec, registry):
                    continue
                mag = int(graph.nodes[sec].stream_magnitude)
                band = trust_for(mag, gauge_mag)
                if band is None:                    # keep() already refused these
                    continue
                lo, hi = sorted((mag, int(gauge_mag)))
                # WHERE THE GAUGE STANDS (T93). Every pair the walk reaches is a CANDIDATE with its relationship;
                # whether it may speak is the policy's call (`choose`). Under `drainage` — the default — that is
                # today's gate: THE GAUGE MUST BE IN THIS REACH'S DRAINAGE. The walk can arrive at water the gauge has
                # never seen — downstream from a tributary is the mainstem — and the magnitude ratio cannot tell the
                # difference, because it is symmetric and the question is not. See `drains_through`.
                rel = relate(_trim(getattr(graph.nodes[sec], "wsc", "")), gauge_wsc,
                             hops=hops, upstream_walk=upstream_walk)
                sec_water = (water_of or {}).get(sec)
                joined = bool(gauge_system and sec_water and sec_water != gauge_water
                              and system.get(sec_water) == gauge_system)
                link = GaugeLink(sec, station, band, lo / hi, hops, rel, int(round(metres / 100.0)) * 100, joined)
                # one candidate per (section, station): a braid can reach a section from both walks — the nearer wins
                # (the ratio, and so the band and the gate, are the same either way)
                if sec not in mine or hops < mine[sec].hops:
                    mine[sec] = link
        for sec, link in mine.items():
            found.setdefault(sec, []).append(link)

    # the rank (best ratio, a transmitting station, the id) is `choose`'s, common to every policy
    if candidates is not None:
        candidates.extend(sorted((l for ls in found.values() for l in ls),
                                 key=lambda l: (l.section_id, l.station)))
    links = choose(found, by_id, policy)

    if report is not None:
        spoke = {l.station for l in links}
        for station in sorted(node_for_station):
            if station in spoke or station in dropped:
                continue
            report.append(f"{station} qualified but every section it reached was claimed "
                          f"by a station with an equal or better ratio")
        report.extend(dropped[s] for s in sorted(dropped))
    return links


#: (T252) The edges a walk DOWN takes first: the river going on (`MAINSTEM_EDGE_KINDS`) and a river ENTERING A LAKE —
#: a piece with a `lake_in` edge ends at the lake, and its other edges are braids and distributaries leaving it.
_WALK_FIRST = frozenset({"continuation", "lake_out", "lake_in"})


def _down_one(graph, sec: str) -> Optional[str]:
    """The one section `sec` flows into, for a walk down to the sea.

    In order (T252): never a channel that flows straight back into `sec` (the upper Pitt's braids leave and rejoin the
    one piece, and the walk went round them and stopped); the river going on or into its lake (`_WALK_FIRST`); the
    bigger water (stream magnitude: Pitt Lake's outlet is the Pitt, not the side channel beside it); the lowest id.
    """
    best: tuple | None = None
    for ix in sorted(graph.down_adj.get(sec, ())):
        e = graph.edges[ix]
        to = graph.nodes.get(e.to_node)
        if to is None:
            continue
        back = any(graph.edges[j].to_node == sec for j in graph.down_adj.get(e.to_node, ()))
        rank = (back, 0 if getattr(e, "kind", "") in _WALK_FIRST else 1, -(to.stream_magnitude or 0), e.to_node)
        if best is None or rank < best:
            best = rank
    return None if best is None else best[-1]


def downstream_map(graph, sections: Iterable[str]) -> dict[str, str]:
    """section -> the section it flows into, for shed members AND everything below them.

    This is the chain a client walks to answer "how does my spot reach that gauge". It is
    stored for shed members, not every section: 2.02M pointers would be most of the bundle.

    CLOSED DOWNSTREAM (T252, owner 2026-10-28). A pointer's TARGET gets a pointer of its own, and
    so on to the sea or the edge of the atlas. Seeded with shed members only, the chain used to
    stop at the first section that was not one — above all a LAKE, which never speaks for a shed
    (`lake_gauge` reads lakes), so a river walked down into Pitt Lake and ended there: the gauge
    on the Alouette below the lake read "no mapped path" from the upper Pitt. The lake's own
    pointer is its `lake_out` edge (a mainstem edge), so the walk goes on to the outlet river.
    Paths only: nothing else in the bundle reads these pointers.

    A section with several outgoing edges (a braid, a distributary) takes the mainstem one
    if there is one — a trace must follow the river, not wander into a side channel.
    """
    out: dict[str, str] = {}
    seen = set(sections)
    todo = sorted(seen, reverse=True)
    while todo:
        sec = todo.pop()
        nxt = _down_one(graph, sec)
        if nxt is None:
            continue
        out[sec] = nxt
        if nxt not in seen:
            seen.add(nxt)
            todo.append(nxt)
    return out


#: (T252) The unit `section_down.len` is stored in, metres — the app's `DOWN_LEN_UNIT_M` (app/packages/data/src/sql/
#: shared.ts) reads it back. A sum over a route's reaches is what is said, "about N km", so 100 m is finer than needed.
DOWN_LEN_UNIT_M = 100


def section_lengths(graph, down: dict[str, str]) -> dict[str, float]:
    """Metres ALONG THE FLOW PATH through each section that has a pointer (T252): what a walk
    down the pointers covers crossing it, so "N km along the river" is a sum over the path.

    A stream piece: its own length on its blue line (`length_m`, else `up_m - down_m`). A LAKE
    has no measure of its own: the river's measure window across it (`windows.polygon_window`,
    where its through-line enters and leaves), else the straight line from the inflow the walk
    arrives by (the largest, then the lowest id) to the outlet it leaves by — Pitt Lake is 25 km
    of the way from the upper Pitt to the Alouette, and a zero there would say 0.
    """
    from pipeline.atlas.graph.windows import polygon_window
    from pipeline.common.models.enums import NodeKind

    arrive: dict[str, list[str]] = {}
    for a, b in down.items():
        arrive.setdefault(b, []).append(a)
    out: dict[str, float] = {}
    for sec in sorted(down):
        n = graph.nodes[sec]
        if n.kind != NodeKind.lake:
            out[sec] = max(0.0, float(n.length_m or (n.up_m - n.down_m) or 0.0))
            continue
        w = polygon_window(graph, sec)
        if w is not None and w[1] is not None and w[2] is not None and w[2] > w[1]:
            out[sec] = float(w[2] - w[1])
            continue
        outlet = next((graph.edges[ix] for ix in sorted(graph.down_adj.get(sec, ()))
                       if graph.edges[ix].to_node == down[sec]), None)
        ins = sorted(arrive.get(sec, ()),
                     key=lambda a: (-(graph.nodes[a].stream_magnitude or 0), a))
        inlet = next((graph.edges[ix] for ix in sorted(graph.up_adj.get(sec, ()))
                      if ins and graph.edges[ix].from_node == ins[0]), None)
        out[sec] = (math.hypot(outlet.x - inlet.x, outlet.y - inlet.y)
                    if outlet is not None and inlet is not None else 0.0)
    return out


def load_stations(path: Path) -> list[dict]:
    """Read the fetched roster: EVERY station, live or not.

    The bundle stores both kinds: a discontinued station still anchors a climatology, and a
    client that only ever saw live gauges would tell a user there is no gauge on a river
    that has had one since 1913. Which may speak and which is speaking is
    `pipeline.gauges.liveness`'s answer (T122; a `live_only` filter here, unused, was a third
    spelling of it).
    """
    import json

    rows = json.loads(path.read_text(encoding="utf-8"))
    return sorted(rows, key=lambda r: r["station"])
