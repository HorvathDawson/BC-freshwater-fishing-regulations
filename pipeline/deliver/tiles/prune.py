"""Leaf-first pruning of unnamed headwater capillaries, before they reach a tile.

WHAT THIS IS FOR. The atlas holds 1,749,496 stream sections and only 60,611 of the graph's
2.47M nodes carry a name of their own. The overwhelming majority of the rest are nameless
draws in the top of a watershed: 1,618,723 sections have nothing at all upstream of them,
their median length is 396 m, and essentially all of them are magnitude 1. They are the
reason zoom 14 is 68.6% of the archive, and a phone downloads the whole archive.

THE RULE, and the invariant it rests on. Only a LEAF may be removed — a section with
nothing flowing into it. That single restriction is what makes named -> unnamed -> named
safe without any special case: the middle section has a named neighbour upstream, a named
section is never removed, so the middle never becomes a leaf. Removing leaves exposes new
leaves, so it iterates, and it stops on its own when every remaining leaf fails a test.

THREE TESTS, and each one is doing a different job:

    max_magnitude   How much water this section drains, as FWA counts headwaters. A leaf is
                    always magnitude 1, so this does nothing on the first pass — it is the
                    STOPPING condition. Once peeling has removed a section's children, its
                    parent becomes a leaf carrying the magnitude of everything that used to
                    be above it, and this is what refuses to keep eating into a real stream.
    min_hops        How far above the nearest NAMED water this section sits. The unnamed
                    tributary joining a named river is the one somebody might stand on and
                    ask about; the one eight junctions further up is a capillary. Measured
                    by walking upstream from every named node, so it is a property of the
                    network rather than of the section.
    max_length_m    A long nameless creek is still a creek. Optional; None means no limit.

Set `PRUNE = None` in `layers.py` and none of this runs — the export writes what it always
wrote. That is the switch to reach for first if the map looks wrong.

WHAT IT COSTS, measured on the province before it was wired in:

    max_mag  min_hops   sections     km   share of stream vertices
          1         3    514,714  314,563          18.0%
          3         4    343,763  243,989          13.3%
          3         5    233,564  172,640           9.2%   <- the default
          5         6    188,258  148,599           7.9%

AND THE THING TO KNOW BEFORE TURNING IT UP. Around 24% of what the default removes is
currently inside some rule's extent — not because anyone wrote a regulation about a
nameless draw, but because "and tributaries" sweeps the whole network above the named
water. Pruning makes that water unanswerable on the map: absent, not mis-coloured, which is
the honest failure of the two, but a failure. The looser settings are worse on this axis,
not better — (1, 3) puts 35% of its removals inside a rule extent. If this is ever turned
up, the same sections should come out of the tributary sweep too, so the bundle does not
claim to regulate water the map does not draw.
"""

from __future__ import annotations

import collections
from dataclasses import dataclass


@dataclass(frozen=True)
class PruneRule:
    """Thresholds for `prunable`. `None` anywhere means that test does not apply."""

    #: Stop peeling once a section drains more than this many headwaters.
    max_magnitude: int = 3
    #: Only peel sections at least this many junctions above the nearest named water.
    min_hops: int = 5
    #: Never peel a section longer than this. `None` for no limit.
    max_length_m: float | None = None

    def describe(self) -> str:
        L = "any length" if self.max_length_m is None else f"<= {self.max_length_m:,.0f} m"
        return (f"magnitude <= {self.max_magnitude}, "
                f">= {self.min_hops} hops above a named water, {L}")


def _named(node) -> bool:
    """Its OWN name. `through_names` is the river passing through a lake, not the lake's."""
    return bool(getattr(node, "gnis_id", None)) or bool(getattr(node, "display_name", "") or "")


def _kind(node) -> str:
    k = getattr(node, "kind", "")
    return k.value if hasattr(k, "value") else str(k)


def hops_above_named(graph) -> dict[str, int]:
    """Junctions from each node down to the nearest named water. Named nodes are 0.

    A breadth-first walk UP from every named node at once, so the answer is the distance to
    the nearest one rather than to some arbitrary one. Nodes never reached have no named
    water anywhere below them and are left out — the caller treats them as infinitely deep.
    """
    ups: dict[str, list[str]] = collections.defaultdict(list)
    for node_id, edge_ids in graph.up_adj.items():
        for ei in edge_ids:
            ups[node_id].append(graph.edges[ei].from_node)

    hops = {k: 0 for k, n in graph.nodes.items() if _named(n)}
    frontier = list(hops)
    depth = 0
    while frontier:
        depth += 1
        nxt = []
        for k in frontier:
            for u in ups.get(k, ()):
                if u not in hops:
                    hops[u] = depth
                    nxt.append(u)
        frontier = nxt
    return hops


def prunable(graph, rule: PruneRule | None) -> set[str]:
    """Section ids the tiles may leave out. Empty when `rule` is None."""
    if rule is None:
        return set()

    nodes = graph.nodes
    ups: dict[str, list[str]] = collections.defaultdict(list)
    downs: dict[str, list[str]] = collections.defaultdict(list)
    for node_id, edge_ids in graph.up_adj.items():
        for ei in edge_ids:
            u = graph.edges[ei].from_node
            ups[node_id].append(u)
            downs[u].append(node_id)

    hops = hops_above_named(graph)
    streams = {k for k, n in nodes.items() if _kind(n) == "stream"}

    def candidate(k: str) -> bool:
        n = nodes.get(k)
        if n is None or k not in streams or _named(n):
            return False
        # Unreached by the walk means nothing named lies below it — as deep as it gets.
        if hops.get(k, 1 << 30) < rule.min_hops:
            return False
        if (getattr(n, "stream_magnitude", 1) or 1) > rule.max_magnitude:
            return False
        if rule.max_length_m is not None:
            if (getattr(n, "length_m", 0.0) or 0.0) > rule.max_length_m:
                return False
        return True

    # `alive` counts upstream neighbours not yet removed; a section is a leaf at zero.
    alive = {k: len(ups.get(k, ())) for k in nodes}
    removed: set[str] = set()
    queue = collections.deque(k for k in streams if alive[k] == 0)
    while queue:
        k = queue.popleft()
        if k in removed or alive.get(k, 1) != 0 or not candidate(k):
            continue
        removed.add(k)
        for dn in downs.get(k, ()):
            alive[dn] -= 1
            if alive[dn] == 0:
                queue.append(dn)
    return removed
