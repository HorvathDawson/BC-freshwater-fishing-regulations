"""Tributary reachability over the single stream graph (03 S6 / 08).

Tributaries of a section = its ANCESTORS in the graph (walk incoming flow edges upstream). The
two flow guards are already baked into the graph: the WSC-descendant filter drops
braiding/cross-watershed edges at build time, and `graph.ancestors(guarded=True)` stops at
`EDGE_TYPE=2300` barrier nodes. So this module is a thin roll-up:

- `tributary_node_ids` — the guarded ancestor closure of a section.
- `tributaries_between` — for "tributaries of X between A and B": the ancestors of the A–B
  section MINUS the upstream mainstem that merely continues through B. Because a curated split
  (and a lake) joins its downstream piece by a `continuation`/`lake_out` edge, subtracting that
  edge's subtree leaves exactly the side tributaries entering the A–B reach. This only works
  because the section is a real node — i.e. splits are applied before the walk (sectionizer).
"""

from __future__ import annotations

from .graph import ancestors  # noqa: F401  (re-export the guarded closure)
from .models import NodeKind, StreamGraph

_MAINSTEM_EDGE_KINDS = frozenset({"continuation", "lake_out"})


def tributary_node_ids(graph: StreamGraph, node_id: str, guarded: bool = True) -> frozenset[str]:
    """Guarded tributary closure of ``node_id`` (all upstream nodes)."""
    return frozenset(ancestors(graph, node_id, guarded=guarded))


def tributaries_between(graph: StreamGraph, section_id: str,
                        guarded: bool = True) -> frozenset[str]:
    """Tributaries entering the mainstem WITHIN ``section_id`` only.

    = ancestors(section) minus the upstream-mainstem subtree entering at the section's upper
    bound (its incoming `continuation`/`lake_out` edge). Excludes the mainstem's own
    continuation above the reach; keeps the side tributaries joining inside the reach.
    """
    anc = set(ancestors(graph, section_id, guarded=guarded))
    above: set[str] = set()
    for ei in graph.up_adj.get(section_id, []):
        e = graph.edges[ei]
        if e.kind in _MAINSTEM_EDGE_KINDS:
            above.add(e.from_node)
            above |= ancestors(graph, e.from_node, guarded=guarded)
    return frozenset(anc - above)


def lake_inlets(graph: StreamGraph, lake_id: str) -> frozenset[str]:
    """Streams flowing INTO a lake node (its incoming edges)."""
    return frozenset(graph.edges[ei].from_node for ei in graph.up_adj.get(lake_id, []))


def lake_outlets(graph: StreamGraph, lake_id: str) -> frozenset[str]:
    """Streams a lake node drains OUT into (its outgoing edges) — usually one."""
    return frozenset(graph.edges[ei].to_node for ei in graph.down_adj.get(lake_id, []))


def lake_tributaries(graph: StreamGraph, lake_id: str, guarded: bool = True) -> frozenset[str]:
    """Tributaries of a lake EXCLUDING the through-mainstem — the inflow whose BLK also drains
    the lake as an outlet (the main river continuing through) and its upstream river system.
    Leaves the lake's side tributaries and their catchments."""
    outlet_blks = {graph.nodes[o].blk for o in lake_outlets(graph, lake_id) if graph.nodes[o].blk}
    anc = set(ancestors(graph, lake_id, guarded=guarded))
    drop: set[str] = set()
    for inlet in lake_inlets(graph, lake_id):
        if graph.nodes[inlet].blk in outlet_blks:        # the through-mainstem inflow
            drop.add(inlet)
            drop |= ancestors(graph, inlet, guarded=guarded)
    return frozenset(anc - drop)


def sections_in_reach(graph: StreamGraph, blk: str, m_lo: float, m_hi: float,
                      eps: float = 1e-6) -> frozenset[str]:
    """Stream-piece sections of ``blk`` whose span lies within [m_lo, m_hi] — for a range reg
    'X from A to C' (spans any lakes/sections between A and C). Bound measures come from a
    section's structured lower_bound/upper_bound.route_measure."""
    return frozenset(nid for nid, n in graph.nodes.items()
                     if n.kind == NodeKind.stream and n.blk == blk
                     and n.down_m >= m_lo - eps and n.up_m <= m_hi + eps)
