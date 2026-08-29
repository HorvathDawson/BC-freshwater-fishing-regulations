"""Reduce a braid nest to the channels that carry something, keeping every water's destinations.

A nest is a closed loop of pieces carrying the river's OWN watershed code — the river leaving itself
and rejoining. `prune.loop_nodes` finds them; this decides how much of one to keep.

Earlier attempts picked a SHAPE — keep the route the tributary takes, keep the biggest loop, keep the
outer ring — and each broke on the next nest: a nest with two receivers, a nest whose loop closes
through the river rather than inside itself, a nest that is a tree. A shape cannot be checked, only
eyeballed, which is how a rule that quietly dropped the Fraser's route into Herrling Island Side
Channel looked fine on the map.

So the rule is not a shape. It is an invariant:

    KEEP ONE ROUTE FOR EVERY WATER, TO EVERY DESTINATION IT WOULD OTHERWISE LOSE.

"Otherwise lose" is the whole subtlety, and getting it wrong is what left the nests full. Asking
whether a water reaches a destination THROUGH THE NEST is the wrong question — the right one is
whether it can reach it at all. Herrling Island Side Channel takes water straight off the Fraser trunk
outside the nest, so the nest's route to it carried nothing and Anderson Creek's nest collapses from
twelve channels to one. Nothing else in the nest is load-bearing, so nothing else is kept.

What that costs, plainly: the dropped channels exist on the ground. Water still reaches everywhere it
reached before, but the map draws one channel where there were several, and no regulation can later
point at a dropped one. That is the trade the invariant makes explicit rather than hiding.
"""

from __future__ import annotations

import heapq
from collections import defaultdict

_PROBE_CAP = 20000          # nodes a "can it get there another way" search may visit before giving up


def _name(graph, nid: str) -> str:
    n = graph.nodes.get(nid)
    return (n.display_name or "").strip() if n is not None else ""


def nest_ports(graph, comp: set[str]):
    """Where the nest meets the rest of the world: {water -> (sources, entry pieces)}, and the exits."""
    exits: set[str] = set()
    ins: dict[str, tuple[set[str], set[str]]] = defaultdict(lambda: (set(), set()))
    for nid in comp:
        for i in graph.down_adj.get(nid, []):
            if graph.edges[i].to_node not in comp:
                exits.add(graph.edges[i].to_node)
        for i in graph.up_adj.get(nid, []):
            src = graph.edges[i].from_node
            if src not in comp:
                key = _name(graph, src) or f"?{src}"
                ins[key][0].add(src)
                ins[key][1].add(nid)
    return ins, exits


def _reaches_through(graph, entries: set[str], comp: set[str], exits: set[str]) -> set[str]:
    """Which exits this water can currently get to by going through the nest."""
    seen, stack, hit = set(entries), list(entries), set()
    while stack:
        u = stack.pop()
        for i in graph.down_adj.get(u, []):
            v = graph.edges[i].to_node
            if v in exits:
                hit.add(v)
            elif v in comp and v not in seen:
                seen.add(v)
                stack.append(v)
    return hit


def _reaches_without(graph, src: str, banned: set[str], want: str, cap: int = _PROBE_CAP) -> bool:
    """Can this water still get there with the nest gone? Giving up counts as NO, so an unfinished
    search keeps the route rather than dropping one that mattered."""
    seen, stack = {src}, [src]
    while stack:
        if len(seen) > cap:
            return False
        u = stack.pop()
        for i in graph.down_adj.get(u, []):
            v = graph.edges[i].to_node
            if v == want:
                return True
            if v in banned or v in seen:
                continue
            seen.add(v)
            stack.append(v)
    return False


def _route(graph, comp: set[str], entries: set[str], exit_node: str, free: set[str]) -> set[str]:
    """Cheapest way from this water to that exit, counting pieces ALREADY KEPT as free.

    Routing each water independently made them carve parallel channels through the same nest; charging
    nothing for a piece that is already staying makes them share one."""
    best = None
    for entry in entries:
        start = (0 if entry in free else 1, 0.0)
        dist = {entry: start}
        prev: dict[str, str | None] = {entry: None}
        pq = [(start[0], start[1], entry)]
        done: set[str] = set()
        hit = None
        while pq:
            c, ln, u = heapq.heappop(pq)
            if u in done or (c, ln) > dist.get(u, (float("inf"), float("inf"))):
                continue
            done.add(u)                          # a nest is CYCLIC: never expand a piece twice
            if any(graph.edges[i].to_node == exit_node for i in graph.down_adj.get(u, [])):
                hit = u
                break
            for i in graph.down_adj.get(u, []):
                v = graph.edges[i].to_node
                if v not in comp or v in done:
                    continue
                # prefer the big channel when the cost ties: a braid's main arm, not a capillary.
                # The tiebreak must GROW along a path — a term that shrank (`ln - length`) made a
                # free-cost cycle improve forever, so Dijkstra never terminated and the heap grew
                # without bound. A long piece simply adds less.
                nd = (c + (0 if v in free else 1),
                      ln + 1.0 / (1.0 + (graph.nodes[v].length_m or 0.0)))
                if nd < dist.get(v, (float("inf"), float("inf"))):
                    dist[v] = nd
                    prev[v] = u
                    heapq.heappush(pq, (nd[0], nd[1], v))
        if hit is None:
            continue
        path, n = set(), hit
        while n is not None:
            path.add(n)
            n = prev[n]
        if best is None or len(path - free) < best[0]:
            best = (len(path - free), path)
    return best[1] if best else set()


def essential_routes(graph, comp: set[str]) -> tuple[set[str], int]:
    """Pieces of this nest that must stay. Returns (keep, demands that were already satisfied).

    A demand is one (water, destination) pair the nest currently serves. It is dropped when the water
    can reach that destination without the nest at all; otherwise one route is kept for it."""
    ins, exits = nest_ports(graph, comp)
    demands, spare = [], 0
    for _water, (srcs, entries) in ins.items():
        for ex in _reaches_through(graph, entries, comp, exits):
            if any(_reaches_without(graph, s, comp, ex) for s in srcs):
                spare += 1
                continue
            demands.append((entries, ex))
    keep: set[str] = set()
    for _ in range(3):                       # re-route against what is already kept until it settles
        nxt: set[str] = set()
        for entries, ex in sorted(demands, key=lambda d: -len(d[0])):
            nxt |= _route(graph, comp, entries, ex, nxt)
        if nxt == keep:
            break
        keep = nxt
    return keep, spare


def check_destinations(graph, comp: set[str], keep: set[str], cap: int = _PROBE_CAP) -> list[tuple]:
    """Every water that enters the nest, and any NAMED destination it would lose. Empty = the
    invariant holds. Run on the real graph, not on the reasoning that produced `keep`."""
    dropped = comp - keep
    lost = []
    ins, _exits = nest_ports(graph, comp)
    for water, (srcs, _entries) in ins.items():
        for s in srcs:
            before = _named_reach(graph, s, set(), cap)
            after = _named_reach(graph, s, dropped, cap)
            if before - after:
                lost.append((water, s, sorted(before - after)))
    return lost


def _named_reach(graph, src: str, banned: set[str], cap: int) -> set[str]:
    seen, stack, out = {src}, [src], set()
    while stack and len(seen) <= cap:
        u = stack.pop()
        for i in graph.down_adj.get(u, []):
            v = graph.edges[i].to_node
            if v in banned or v in seen:
                continue
            seen.add(v)
            stack.append(v)
            nm = _name(graph, v)
            if nm:
                out.add(nm)
    return out
