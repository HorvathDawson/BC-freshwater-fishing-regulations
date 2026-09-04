"""Leaf pruning: drop unnamed headwater capillaries from the graph itself.

SEPARATE FROM THE BRAID PRUNE, and deliberately so. `prune.py` reduces a braid NEST — pieces
attached at both ends, where removing one means re-homing everything that flowed through it,
and the hard part is deciding which channels are load-bearing. This removes LEAVES, which
have nothing above them by definition, so nothing is orphaned and nothing is re-homed. The
two share a word and no code.

WHY IT LIVES IN THE GRAPH BUILD. It was written in the tile export first, which was wrong:
the tiles would have stopped drawing these sections while the registry, the bundle and the
reach builder went on carrying them. The bundle would then claim to regulate water the map
does not draw — the same "tiles and bundle must be one set" failure the app's own comments
warn about. Pruned here, one change reaches every consumer, because they all read this graph.

That also dissolves the objection to pruning at all. Roughly a quarter of what this removes
used to fall inside some rule's extent, because "and tributaries" sweeps the whole network
above a named water. Removing the sections BEFORE the sweep runs means the sweep never
reaches them and no rule ever claims them: the bundle and the map agree by construction
rather than by a second pass that has to be remembered.

THE RULE, and the invariant it rests on. Only a LEAF may be removed — nothing flowing in.
That single restriction is what makes named -> unnamed -> named safe with no special case:
the middle section has a named neighbour upstream, a named section is never removed, so the
middle never becomes a leaf. Removing leaves exposes new leaves, so it iterates and stops on
its own when every remaining leaf fails a test.

FOUR TESTS, each doing a different job:

    max_magnitude       the STOPPING condition. A leaf is always magnitude 1, so this does
                        nothing on the first pass; it bites once peeling has exposed a parent
                        carrying the magnitude of everything that used to be above it.
    min_hops            distance to the nearest NAMED water. The unnamed tributary joining a
                        named river is one somebody might stand on; eight junctions up is a
                        capillary.
    max_length_m        a long nameless creek is still a creek. Optional.
    tidal               coastal channels are kept when they are long enough to be a channel.
                        A WSC code beginning 9 is tidal — 900, 910, 915, 920, 930, 940 — and
                        those drainages are 21% of the graph. They are also where the salmon
                        are: an unnamed estuarine slough is fishable water in a way an
                        unnamed draw at 1,400 m elevation is not, and it is longer, with a
                        median of 555 m against 396 m for unnamed leaves generally.

Set `RULE = None` and nothing is removed.
"""

from __future__ import annotations

import collections
from dataclasses import dataclass, replace

from pipeline.common.models import StreamGraph


@dataclass(frozen=True)
class LeafPruneRule:
    """Thresholds for `prunable`. See the module docstring for what each one is for."""

    #: Stop peeling once a section drains more than this many headwaters.
    max_magnitude: int = 3
    #: Only peel sections at least this many junctions above the nearest named water.
    min_hops: int = 3
    #: Never peel a section longer than this. `None` for no limit.
    max_length_m: float | None = None
    #: WSC codes starting with any of these are tidal. `()` disables the exemption.
    tidal_wsc_prefixes: tuple[str, ...] = ("9",)
    #: A tidal section longer than this is kept whatever else it fails.
    tidal_min_length_m: float = 500.0

    def describe(self) -> str:
        length = "any length" if self.max_length_m is None else f"<= {self.max_length_m:,.0f} m"
        tidal = ("no tidal exemption" if not self.tidal_wsc_prefixes else
                 f"keeping tidal (wsc {'/'.join(self.tidal_wsc_prefixes)}x) over "
                 f"{self.tidal_min_length_m:,.0f} m")
        return (f"magnitude <= {self.max_magnitude}, >= {self.min_hops} hops above a named "
                f"water, {length}, {tidal}")


def _named(node) -> bool:
    """Its OWN name. `through_names` is the river passing through a lake, not the lake's."""
    return bool(getattr(node, "gnis_id", None)) or bool(getattr(node, "display_name", "") or "")


def _kind(node) -> str:
    k = getattr(node, "kind", "")
    return k.value if hasattr(k, "value") else str(k)


def hops_above_named(graph: StreamGraph) -> dict[str, int]:
    """Junctions from each node down to the NEAREST named water. Named nodes are 0.

    Breadth-first UP from every named node at once, so the answer is the distance to the
    nearest one rather than to whichever is found first. Nodes never reached have no named
    water anywhere below them; the caller treats those as infinitely deep.
    """
    ups: dict[str, list[str]] = collections.defaultdict(list)
    for node_id, edge_ids in graph.up_adj.items():
        for ei in edge_ids:
            ups[node_id].append(graph.edges[ei].from_node)

    hops = {k: 0 for k, n in graph.nodes.items() if _named(n)}
    frontier, depth = list(hops), 0
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


def prunable(graph: StreamGraph, rule: LeafPruneRule | None,
             protected: set[str] | None = None) -> set[str]:
    """Node ids the graph may lose. Empty when `rule` is None.

    `protected` is never removed whatever the thresholds say — the caller passes anything
    curation has an opinion about, the same promise `prune.py` keeps with `protected_blks`.
    """
    if rule is None:
        return set()
    keep = protected or set()

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
        if n is None or k not in streams or k in keep or _named(n):
            return False
        length = getattr(n, "length_m", 0.0) or 0.0
        wsc = getattr(n, "wsc", "") or ""
        # TIDAL FIRST: a long coastal channel is kept before any other test is asked.
        if (rule.tidal_wsc_prefixes
                and wsc.startswith(rule.tidal_wsc_prefixes)
                and length > rule.tidal_min_length_m):
            return False
        # Unreached by the walk means nothing named lies below it — as deep as it gets.
        if hops.get(k, 1 << 30) < rule.min_hops:
            return False
        if (getattr(n, "stream_magnitude", 1) or 1) > rule.max_magnitude:
            return False
        if rule.max_length_m is not None and length > rule.max_length_m:
            return False
        return True

    # `alive` counts upstream neighbours not yet removed; a node is a leaf at zero.
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


def prune_leaves(graph: StreamGraph, rule: LeafPruneRule | None,
                 protected: set[str] | None = None) -> tuple[StreamGraph, set[str]]:
    """Return (graph without those nodes, the ids removed).

    Edges are filtered and BOTH adjacency maps rebuilt from the survivors rather than
    patched: an edge index is a position in `edges`, so dropping one shifts every index
    after it, and a half-updated `up_adj` is a graph that walks into a node that is not
    there. Rebuilding is O(edges) and cannot be subtly wrong.
    """
    gone = prunable(graph, rule, protected)
    if not gone:
        return graph, gone

    nodes = {k: v for k, v in graph.nodes.items() if k not in gone}
    edges = [e for e in graph.edges
             if e.from_node not in gone and e.to_node not in gone]
    up: dict[str, list[int]] = {}
    down: dict[str, list[int]] = {}
    for i, e in enumerate(edges):
        up.setdefault(e.to_node, []).append(i)
        down.setdefault(e.from_node, []).append(i)
    return replace(graph, nodes=nodes, edges=edges, up_adj=up, down_adj=down), gone


#: What the province build uses. `--no-leaf-prune` on `pipeline.atlas.build` sets this to
#: None for a run without touching the file, which is the switch to reach for first if the
#: map looks wrong; editing here changes it for good.
#:
#: MEASURED on the province at these settings, before the tidal exemption:
#:
#:     max_mag  min_hops   sections       km   share of stream vertices
#:           3         3    ~572,000      ---   ~20.0%   <- this
#:           1         3    514,714  314,563   18.0%
#:           3         4    343,763  243,989   13.3%
#:           3         5    233,564  172,640    9.2%
#:           5         6    188,258  148,599    7.9%
#:
#: The tidal exemption then puts 44,161 of those back: a third of the candidates are in a
#: 9xx drainage, and their median length is 555 m against 396 m for unnamed leaves overall.
#:
#: HOW THE SETTING MOVED, and what each step cost. Five was a fixpoint — rerunning it
#: found 584 more sections, which is nothing. Four found 103,129 more (4.0% of stream
#: vertices) at a cost measured against the rule extents the rebuilt graph produces: 475
#: rules lost SOME water, exactly one lost more than a quarter, none lost all. Three finds
#: 338,432 more again, and 45 rules pass the quarter mark.
#:
#: THREE IS A DELIBERATE CHOICE, not a discovered safe point. A rule losing a quarter of
#: its extent here is losing unnamed capillaries three junctions above the nearest named
#: water — headwater trickle that "and its tributaries" swept up because the sweep does
#: not know where a person can stand. The extent gets smaller; the water anybody fishes
#: does not. What must never happen is a rule losing EVERYTHING, and none does at any
#: setting on this ladder, because a named water is never removable and every rule is
#: anchored to one.
#:
#: Magnitude barely matters at this depth: at 4 hops, max_magnitude 3/5/8 removes
#: 103k/108k/110k. The hop count is doing all the work, which is what you would expect —
#: distance from a named water is the thing a person can actually stand next to.
DEFAULT_RULE = LeafPruneRule(max_magnitude=3, min_hops=3, max_length_m=None,
                             tidal_wsc_prefixes=("9",), tidal_min_length_m=500.0)
