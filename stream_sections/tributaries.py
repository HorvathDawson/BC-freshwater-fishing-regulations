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
from .models import StreamGraph

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
