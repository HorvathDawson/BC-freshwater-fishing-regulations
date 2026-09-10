"""The tributary walk — ONE implementation, in the graph package where it belongs.

WHY THIS FILE ABSORBED `atlas/reach/tributaries.py`.

There were two. This module held a set-subtraction walk for a single section; the reach
package held a refusing walk over a whole reach. A comment in `common/models/graph.py`
defended the split — "two genuinely different algorithms… the algorithms should differ" —
and that was true of the algorithms and false of the outcome:

  · this module had NO Strahler guard. That guard is what stopped "McLennan Creek including
    tributaries" absorbing 419,078 sections — 20.6% of British Columbia.
  · it had no confluence-mouth seeding, so it missed the creeks joining exactly at a cut
    (51 of 196,596 bounded sections).
  · it subtracted after the fact, which is wrong on a braid: a node can be an ancestor of
    both the mainstem above and a tributary joining inside, and subtraction drops it.
  · and it had NO PRODUCTION CALLER. Every symbol in it was reached only by tests and
    comments. Its passing suite was positive evidence for a walk nothing shipped, and it was
    the module two cross-check tests were comparing the real one against.

So the surviving implementation is the guarded one, and it lives here — a tributary walk is
a graph operation, not a property of how regulations happen to be resolved.

WHAT A TRIBUTARY IS, and why the guards are not optional. Tributary scope is relative to the
RULE'S EXTENT, not to the named river: "no fishing between A and B, including tributaries"
means the streams joining THAT STRETCH. The watershed-code prefix shortcut gets this wrong
in the expensive direction — on the Kootenay it adds the Moyie and Yahk and 3,205 km of
water joining BELOW the regulated reach, which is the app announcing a closure that does not
exist.
"""

from __future__ import annotations

from pipeline.atlas.graph.graph import ancestors
from pipeline.common.models import NodeKind, StreamGraph

#: Edges by which a river continues into its own next piece. Crossing one of these OUT of
#: the reach means leaving onto the mainstem above, which is not a tributary.
# Imported, not redeclared — see pipeline/common/models/graph.py.
from pipeline.common.models.graph import MAINSTEM_EDGE_KINDS


def lake_inlets(graph: StreamGraph, lake_id: str) -> frozenset[str]:
    """Streams flowing INTO a lake node (its incoming edges)."""
    return frozenset(graph.edges[ei].from_node for ei in graph.up_adj.get(lake_id, []))


def lake_outlets(graph: StreamGraph, lake_id: str) -> frozenset[str]:
    """Streams a lake node drains OUT into (its outgoing edges) — usually one."""
    return frozenset(graph.edges[ei].to_node for ei in graph.down_adj.get(lake_id, []))


def _through_blks(graph: StreamGraph, node_id: str) -> frozenset[str]:
    """The blue lines this node's own flow leaves by.

    For a LAKE this is the outlet's blk: a lake fed and drained by the same river is a
    lake *on* that river, and the inflow sharing the outlet's blk is the river continuing
    through — not a tributary of the lake. Without this, "Lake Koocanusa and its
    tributaries" swallowed the entire Kootenay River above it (36,990 sections instead of
    10,570).

    For a stream the edge KIND already says this (`continuation`), so this returns nothing
    and the kind check does the work.
    """
    n = graph.nodes.get(node_id)
    if n is None or n.kind != NodeKind.lake:
        return frozenset()
    out = set()
    for ei in graph.down_adj.get(node_id, []):
        d = graph.nodes.get(graph.edges[ei].to_node)
        if d is not None and d.blk:
            out.add(d.blk)
    return frozenset(out)


def tributaries_of_reach(
    graph: StreamGraph,
    reach: set[str] | frozenset[str],
    *,
    blocked: set[str] | frozenset[str] = frozenset(),
    guarded: bool = True,
    window: tuple[str, float, float] | None = None,
) -> frozenset[str]:
    """Sections draining INTO `reach`, recursively. Excludes `reach` itself.

    `blocked` sections are neither returned nor traversed — that is how a
    `tributary_excludes` carve-out removes a stream *and everything above it*, which is what
    "except Burnt Bridge Creek upstream of Sitkatapa Creek" means.

    `guarded=False` drops the 2300 barrier stop; it exists only so tests can show the guard
    is doing something.
    """
    reach = frozenset(reach)
    out: set[str] = set()
    seen: set[str] = set(reach)
    stack: list[str] = sorted(reach)          # sorted: determinism, not correctness

    # A stream joining EXACTLY at the reach's lower cut belongs to the reach. When a
    # curator anchors a cut on a confluence — "upstream of the confluence with Slesse
    # Creek" — that creek's mouth sits on the point. FWA may attach it to the piece BELOW
    # the cut, in which case the walk would never see it and the named water would be
    # missing from the very reach it defines. Rare (51 of 196,596 bounded sections) and
    # entirely concentrated on confluence-anchored cuts, which is where it matters most.
    for seed in sorted(_mouths_at_lower_bound(graph, reach, window)):
        if seed not in seen and seed not in blocked:
            n = graph.nodes.get(seed)
            if n is not None and not (guarded and n.is_barrier):
                seen.add(seed)
                out.add(seed)
                stack.append(seed)

    while stack:
        node = stack.pop()
        at_boundary = node in reach
        node_n = graph.nodes.get(node)
        is_lake = at_boundary and node_n is not None and node_n.kind == NodeKind.lake
        through = _through_blks(graph, node) if is_lake else frozenset()
        for ei in graph.up_adj.get(node, []):
            e = graph.edges[ei]
            src = e.from_node

            # Leaving the reach through its own top. "The reach's own flow continuing"
            # takes two forms, and which one applies depends on what you are standing on:
            #
            #   STREAM boundary — the `continuation` / `lake_out` edge above it.
            #   LAKE boundary   — the inflow on the same blue line as the lake's OUTflow,
            #                     i.e. the river running through the lake.
            #
            # Applied ONLY at the boundary: the same edge kind inside a tributary is that
            # tributary's own continuation and must be followed.
            if at_boundary and src not in reach:
                if is_lake:
                    sn = graph.nodes.get(src)
                    if sn is not None and sn.blk and sn.blk in through:
                        continue
                elif e.kind in MAINSTEM_EDGE_KINDS:
                    continue
            if src in seen or src in blocked:
                continue
            n = graph.nodes.get(src)
            if n is None:
                continue
            if guarded and n.is_barrier:
                continue                       # the canal, and everything above it
            if guarded and _breaks_strahler(n, graph.nodes.get(node)):
                continue                       # a bigger river cannot be a tributary
            seen.add(src)
            out.add(src)
            stack.append(src)

    return frozenset(out)


def _mouths_at_lower_bound(graph: StreamGraph, reach: frozenset[str],
                           window: tuple[str, float, float] | None = None) -> set[str]:
    """Tributary mouths sitting exactly on the reach's lower cut.

    Looked for on the piece BELOW the cut: the confluence is at the same route measure, so
    FWA may hang the mouth on either side of it. Only tributaries are taken — anything on
    the reach's own blue line is the mainstem, not a joining stream.

    `window` is the `(blk, lo, hi)` the extent ACTUALLY resolved to, handed over by
    `resolve_extent`. Prefer it: re-deriving the cut from node bounds means two places have
    to agree about where the reach starts, and they can drift. Node bounds remain the
    fallback for a reach that has no single window (a `whole` extent, or a `between`
    spanning two blue lines).
    """
    out: set[str] = set()
    if window is not None:
        w_blk, w_lo, _w_hi = window
        for nid in reach:
            n = graph.nodes.get(nid)
            if n is None or n.blk != w_blk:
                continue
            out |= _mouths_at(graph, nid, n, w_lo, reach)
        return out
    for nid in reach:
        n = graph.nodes.get(nid)
        if n is None or n.lower_bound is None or not n.blk:
            continue
        m = n.lower_bound.route_measure
        out |= _mouths_at(graph, nid, n, m, reach)
    return out


def _mouths_at(graph: StreamGraph, nid: str, n, m: float,
               reach: frozenset[str]) -> set[str]:
    """Tributary mouths on the piece immediately BELOW `nid`, at route measure `m`.

    THE STRAHLER GUARD APPLIES HERE TOO. Everything joining at the confluence is a sibling of
    the reach, and one of those siblings is the river the reach FLOWS INTO — it arrives on its
    own blue line, by a `confluence` edge, at exactly this measure, so every other test here
    passes it. The walk below rejects that case with `_breaks_strahler` ("a bigger river cannot
    be a tributary"); the seeding did not, and so a rule reaching the mouth of its own water
    picked up the receiving river.

    That is how "No Fishing downstream of the main logging road bridge, May 1-31" on the
    CHEHALIS — a `downstream_of` whose window runs to measure 0, i.e. to the mouth — bound two
    sections of the HARRISON, which is the river the Chehalis empties into.
    """
    out: set[str] = set()
    for ei in graph.down_adj.get(nid, []):
        below = graph.edges[ei].to_node
        for fi in graph.up_adj.get(below, []):
            f = graph.edges[fi]
            src = graph.nodes.get(f.from_node)
            if (f.from_node not in reach and src is not None and src.blk != n.blk
                    and f.kind == "confluence" and abs(f.at_measure - m) < 0.5
                    and not _breaks_strahler(src, n)):
                out.add(f.from_node)
    return out


def _breaks_strahler(upstream, downstream) -> bool:
    """True when `upstream` is a strictly LARGER river than the piece it flows into AND the
    watershed code has no opinion.

    The WSC hierarchy is the authority: if `upstream`'s code extends `downstream`'s, FWA is
    saying this is a genuine tributary and Strahler must not override it. That happens 93
    times — nearly all lakes, whose order aggregates every inflow and so runs ahead of
    their own outlet's first piece.

    Where the codes are EQUAL, WSC is silent by design (equal codes are how real side
    channels pass), and that silence is exactly where the floodplain artifacts hide. There,
    order decides.

    Unknown orders are never judged — absence of evidence is not evidence of an artifact.
    """
    if upstream is None or downstream is None:
        return False
    a, b = upstream.stream_order, downstream.stream_order
    if a is None or b is None or a <= b:
        return False
    up_wsc, dn_wsc = upstream.wsc or "", downstream.wsc or ""
    if up_wsc and dn_wsc and up_wsc != dn_wsc and up_wsc.startswith(dn_wsc):
        return False                    # WSC says genuine tributary — it wins
    return True


def expand(
    graph: StreamGraph,
    reach: set[str] | frozenset[str],
    *,
    only: bool = False,
    excluded: set[str] | frozenset[str] = frozenset(),
    guarded: bool = True,
    window: tuple[str, float, float] | None = None,
) -> frozenset[str]:
    """The section set a tributary-scoped rule actually covers.

    `only=True` is `tributaries_only` — the tributaries WITHOUT the mainstem, which is a real
    regulation shape ("no fishing in tributaries above Holt Creek", 44 rules). The reach is
    still what defines *which* tributaries; it is just not itself in the answer.
    """
    reach = frozenset(reach)
    excluded = frozenset(excluded)
    tribs = tributaries_of_reach(graph, reach, blocked=excluded, guarded=guarded,
                                 window=window)
    base = frozenset() if only else (reach - excluded)
    return base | tribs


# --------------------------------------------------------------------------------------- #
# The older names, kept — as the SAME walk over a one-section reach.
#
# These were separate implementations. `tributaries_between` subtracted the mainstem-above
# closure from the section's ancestors; `lake_tributaries` subtracted the through-river's.
# Both are special cases of "what joins THESE sections", which is what the walk above
# answers, so they are now spellings of it rather than second opinions.
#
# THIS CHANGES THREE LAKES, deliberately. Measured over a 200-lake sample, the old
# subtraction and the guarded walk agreed on 197 and differed on 3 — and the differences ran
# both ways (+1, +8, -11). The walk is the one with the Strahler guard, the confluence-mouth
# seeding, and no braid-subtraction bug, so where they differed the walk is the answer we
# want. What is gone with them is the pair of cross-check tests that compared the two: a
# function cannot cross-check itself, and the disagreement they were reporting was between
# production and a module nothing shipped.
# --------------------------------------------------------------------------------------- #

def tributaries_between(graph: StreamGraph, section_id: str,
                        guarded: bool = True) -> frozenset[str]:
    """Everything joining ONE section, excluding the river continuing above it."""
    return tributaries_of_reach(graph, {section_id}, guarded=guarded)


def lake_tributaries(graph: StreamGraph, lake_id: str,
                     guarded: bool = True) -> frozenset[str]:
    """A lake's own tributaries, excluding the river running THROUGH it.

    Lake Koocanusa swallowed the whole Kootenay above it — 36,990 sections instead of
    10,570 — before that exclusion existed.
    """
    return tributaries_of_reach(graph, {lake_id}, guarded=guarded)


def tributary_node_ids(graph: StreamGraph, node_id: str,
                       guarded: bool = True) -> frozenset[str]:
    """EVERYTHING upstream, mainstem included — `ancestors`, under an older name.

    NOT a spelling of `tributaries_of_reach`, which is what I first made it. That walk
    excludes the river continuing above the reach; this one does not, and the difference is
    the whole catchment above a section. The name is the trap — "tributary node ids" reads
    like the tributaries and means the ancestors — so it says so here rather than in a
    caller's head.
    """
    return frozenset(ancestors(graph, node_id, guarded=guarded))


def with_tributaries(graph: StreamGraph, section_ids, guarded: bool = True) -> frozenset[str]:
    """The base waters AND everything joining them — an `[Includes Tributaries]` set."""
    base = set(section_ids)
    return frozenset(base | tributaries_of_reach(graph, base, guarded=guarded))


# Node lookups, not walks — carried over unchanged from the module this absorbed.

def piece_above(graph: StreamGraph, blk: str, boundary_label: str):
    """The stream piece of ``blk`` whose LOWER bound is the boundary labelled ``boundary_label``
    — i.e. the reach 'upstream of <label>' after a split/lake/border boundary. Returns its
    node_id (or None). This is how a reg 'X upstream of Y' resolves to a concrete node."""
    for nid, n in graph.nodes.items():
        if (n.kind == NodeKind.stream and n.blk == blk and n.lower_bound
                and n.lower_bound.label == boundary_label):
            return nid
    return None


def sections_in_reach(graph: StreamGraph, blk: str, m_lo: float, m_hi: float,
                      eps: float = 1e-6) -> frozenset[str]:
    """Stream-piece sections of ``blk`` whose span lies within [m_lo, m_hi] — for a range reg
    'X from A to C' (spans any lakes/sections between A and C). Bound measures come from a
    section's structured lower_bound/upper_bound.route_measure."""
    return frozenset(nid for nid, n in graph.nodes.items()
                     if n.kind == NodeKind.stream and n.blk == blk
                     and n.down_m >= m_lo - eps and n.up_m <= m_hi + eps)


def reach_except(graph: StreamGraph, base_ids, except_ids, guarded: bool = True) -> frozenset[str]:
    """The exact 'Includes Tributaries EXCEPT …' set: the tributary closure of the base waters
    MINUS the tributary closures of the excepted upstream reaches.

    Models 'ATNARKO/BELLA COOLA RIVERS [Includes Tributaries] EXCEPT: Burnt Bridge Cr. upstream of
    Sitkatapa Cr., Hunlen Cr. upstream of Hunlen Falls, Young Cr. upstream of Hwy 20'. Each EXCEPT
    is the upper piece a split already made (resolve via ``piece_above``); its exclusion set is
    that piece's own tributary closure. Pure set difference — the splits do all the structural
    work upstream (sectionizer), so nothing here re-walks geometry."""
    return frozenset(with_tributaries(graph, base_ids, guarded)
                     - with_tributaries(graph, except_ids, guarded))
