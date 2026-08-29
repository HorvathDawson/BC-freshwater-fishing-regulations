"""Reach-scoped tributary expansion.

The rule that matters (doc 10 ③): **tributary scope is relative to the RULE'S EXTENT, not
the named river.** "No fishing between A and B, including tributaries" means the streams
joining *that stretch* — not everything draining the whole river. The watershed-code
prefix shortcut gets this wrong in the expensive direction: on the Kootenay it adds the
Moyie and Yahk Rivers and 3,205 km of water that joins *below* the regulated reach.

## How

Walk upstream from the reach, refusing to leave it through its own top.

`pipeline.graph.tributaries.tributaries_between` already does this for a SINGLE section, by
subtracting the mainstem subtree entering via the section's `continuation`/`lake_out` edge.
This generalises it to a SET of sections — a rule's reach is many sections — and does it as
a **walk that refuses to traverse** rather than a set subtraction.

That difference matters. Subtraction removes `ancestors(mainstem_above)`, and on a braid a
node can be an ancestor of both the mainstem above *and* a tributary joining inside the
reach; subtraction would silently drop it. Refusing to *step* onto the mainstem above
cannot make that mistake — you only ever add water you actually walked to.

The distinction is applied **only at the reach boundary**. A `continuation` edge inside a
tributary is how that tributary's own upstream pieces connect, and must be followed; a
`continuation` edge leaving the reach is the mainstem carrying on above it, and must not be.

## Guards (both proven necessary by spikes, doc 12)

* **WSC-descendant filter** — already baked into the edge set at build time. Without it a
  naive walk leaked 24 of 43 Harrison River sections into the Chehalis.
* **2300 barrier** — canal/artificial connector nodes stop the walk. Blocking them took the
  Columbia/Kootenay leak from 86 sections to 0.
* **Strahler tie-break — ONLY where the watershed code is silent.** In Strahler ordering a
  confluence's downstream order is at least the max of its inputs, so an edge whose
  upstream order is higher is a network artifact: a distributary, an anabranch, or
  floodplain connectivity.

  This is needed because the WSC filter deliberately lets EQUAL codes through (that is how
  genuine side channels work), and FWA codes a big river's floodplain channels with the
  river's own code. The Fraser (order 10) is recorded as flowing INTO McLennan Creek
  (order 4-5) at four confluences near Matsqui — same code `100` both sides — so
  "McLennan Creek including tributaries" absorbed **419,078 sections, 20.6% of BC**.

  **The two guards must never disagree**, so Strahler applies only where WSC has no
  opinion — equal codes, or a code missing. Where WSC says "this IS a descendant, a real
  tributary", WSC wins. Measured over the whole graph: of 4,467 edges Strahler would
  block, 4,374 are cases WSC is silent on or would block too, and **93 are contradictions**
  — almost all lakes, whose order aggregates their inflows and so exceeds their own
  outlet's first piece. Those 93 are legitimate tributaries and are now kept.
"""

from __future__ import annotations

from pipeline.models import NodeKind, StreamGraph

#: Edges by which a river continues into its own next piece. Crossing one of these OUT of
#: the reach means leaving onto the mainstem above, which is not a tributary.
MAINSTEM_EDGE_KINDS = frozenset({"continuation", "lake_out"})


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
    """Tributary mouths on the piece immediately BELOW `nid`, at route measure `m`."""
    out: set[str] = set()
    for ei in graph.down_adj.get(nid, []):
        below = graph.edges[ei].to_node
        for fi in graph.up_adj.get(below, []):
            f = graph.edges[fi]
            src = graph.nodes.get(f.from_node)
            if (f.from_node not in reach and src is not None and src.blk != n.blk
                    and f.kind == "confluence" and abs(f.at_measure - m) < 0.5):
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
