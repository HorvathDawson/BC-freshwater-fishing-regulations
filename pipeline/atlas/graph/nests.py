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
    for nid in sorted(comp):                     # DETERMINISM: `ins` keeps first-seen order
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


#: THE SURVIVING CHANNEL IS CHOSEN BY PRINCIPLE (user ruling 2026-10-06, FIX B8), never by id order:
#: where a water needs one route through a nest, the route kept is, in order,
#:   1. the one on FWA's OWN MAINSTEM (a blue line whose WATERSHED_KEY is itself) — DEFENSIVE only
#:      (code review A-4): a nest never holds the river's own mainstem (`prune.loop_nodes` excludes
#:      `_mainstem_blk`, which IS FWA's mainstem when the keys are known) and side channels carry
#:      their river's key, so in real data the survivor is decided from step 2. "FWA mainstem first"
#:      is honoured by `prune._mainstem_blk` (the river a nest hangs off is never pruned); then
#:   2. the BIGGEST channel — highest stream magnitude, then highest stream order — judged by its
#:      SMALLEST new piece (a route is only as big as its narrowest link: a widest-path search),
#:   3. then, among the widest routes, the fewer, longer pieces (the least Σ 1 / (1 + length) over
#:      its new pieces — a second pass, `_route`),
#:   4. then the node ids (the heap's last key), so the answer is a function of the data alone.
#: A piece already kept for another water is free (+inf rank, no length cost): waters share one channel.
SURVIVOR_BY_PRINCIPLE = True

_FREE = (2, float("inf"), float("inf"))


def _rank(graph, nid: str, fwa_main) -> tuple:
    """A piece's size for the survivor rule: (on FWA's mainstem, magnitude, order); higher is
    bigger. An unknown magnitude/order ranks below every known one."""
    n = graph.nodes[nid]
    mag = n.stream_magnitude if n.stream_magnitude is not None else -1
    order = n.stream_order if n.stream_order is not None else -1
    return (1 if (fwa_main and n.blk in fwa_main) else 0, mag, order)


def _route(graph, comp: set[str], entries: set[str], exit_node: str, free: set[str],
           fwa_main=None) -> set[str]:
    """The route this water keeps to that exit (`SURVIVOR_BY_PRINCIPLE`), counting pieces ALREADY
    KEPT as free so waters share one channel instead of carving parallel ones.

    TWO PASSES (code review A-5: "widest, then cheapest" is not one label-setting search — a cheaper
    sub-route can be dropped at a piece for a wider one that the next piece makes no wider):
      1. WIDEST: the best bottleneck B any entry reaches the exit with — the route's smallest new
         piece by (on FWA mainstem, magnitude, order); a bottleneck only shrinks as a route grows,
         so a widest-path Dijkstra finds it exactly;
      2. CHEAPEST AMONG THE WIDEST: Dijkstra on Σ 1 / (1 + length) over the new pieces, using only
         pieces ranked at least B (free pieces always) — the fewer, longer pieces; ties by node id
         (the heap's last key), entries in sorted order.
    A nest is CYCLIC: no piece is expanded twice."""
    if not SURVIVOR_BY_PRINCIPLE:
        return _route_by_count(graph, comp, entries, exit_node, free)

    def rank(nid: str):
        return _FREE if nid in free else _rank(graph, nid, fwa_main)

    def cost(nid: str) -> float:
        return 0.0 if nid in free else 1.0 / (1.0 + (graph.nodes[nid].length_m or 0.0))

    def at_exit(u: str) -> bool:
        return any(graph.edges[i].to_node == exit_node for i in graph.down_adj.get(u, []))

    def neg(r):
        return tuple(-x for x in r)

    # 1. the best bottleneck over every entry
    best_b = None
    for entry in sorted(entries):
        width = {entry: rank(entry)}
        pq = [(neg(width[entry]), entry)]
        done: set[str] = set()
        while pq:
            nb, u = heapq.heappop(pq)
            if u in done or nb != neg(width[u]):
                continue
            done.add(u)
            if at_exit(u):
                if best_b is None or width[u] > best_b:
                    best_b = width[u]
                break
            for i in graph.down_adj.get(u, []):
                v = graph.edges[i].to_node
                if v not in comp or v in done:
                    continue
                w = min(width[u], rank(v))
                if v not in width or w > width[v]:
                    width[v] = w
                    heapq.heappush(pq, (neg(w), v))
    if best_b is None:
        return set()

    # 2. the cheapest route using only pieces at least that wide
    best = None
    for entry in sorted(entries):                # DETERMINISM: a tie keeps the first entry tried
        if rank(entry) < best_b:
            continue
        dist = {entry: cost(entry)}
        prev: dict[str, str | None] = {entry: None}
        pq = [(dist[entry], entry)]
        done = set()
        hit = None
        while pq:
            d, u = heapq.heappop(pq)
            if u in done or d != dist[u]:
                continue
            done.add(u)
            if at_exit(u):
                hit = u
                break
            for i in graph.down_adj.get(u, []):
                v = graph.edges[i].to_node
                if v not in comp or v in done or rank(v) < best_b:
                    continue
                nd = d + cost(v)
                if v not in dist or nd < dist[v]:
                    dist[v] = nd
                    prev[v] = u
                    heapq.heappush(pq, (nd, v))
        if hit is None:
            continue
        path, n = set(), hit
        while n is not None:
            path.add(n)
            n = prev[n]
        if best is None or dist[hit] < best[0]:
            best = (dist[hit], path)
    return best[1] if best else set()


def _route_by_count(graph, comp: set[str], entries: set[str], exit_node: str, free: set[str]) -> set[str]:
    """THE RULE BEFORE THE FIX ROUND (`SURVIVOR_BY_PRINCIPLE` off, kept so a test can show the
    difference): fewest new pieces, then longer pieces, then id order.

    Cheapest way from this water to that exit, counting pieces ALREADY KEPT as free.

    Routing each water independently made them carve parallel channels through the same nest; charging
    nothing for a piece that is already staying makes them share one."""
    best = None
    for entry in sorted(entries):                # DETERMINISM: a tie keeps the first entry tried
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


def essential_routes(graph, comp: set[str], fwa_main=None) -> tuple[set[str], int]:
    """Pieces of this nest that must stay. Returns (keep, demands that were already satisfied).

    A demand is one (water, destination) pair the nest currently serves. It is dropped when the water
    can reach that destination without the nest at all; otherwise one route is kept for it — the one
    `SURVIVOR_BY_PRINCIPLE` names (`fwa_main`: FWA's own mainstem blue lines)."""
    ins, exits = nest_ports(graph, comp)
    demands, spare = [], 0
    for _water, (srcs, entries) in sorted(ins.items()):
        for ex in sorted(_reaches_through(graph, entries, comp, exits)):
            if any(_reaches_without(graph, s, comp, ex) for s in sorted(srcs)):
                spare += 1
                continue
            demands.append((entries, ex))
    # DETERMINISM (BOUND round, 2026-10-06). Every container above is a set of node-id strings, whose
    # iteration order changes with PYTHONHASHSEED; the routing below is GREEDY (a route made earlier
    # is free for the next), so the order demands are routed in decides which of two equal channels
    # survives. Unsorted, two atlas builds from identical inputs kept different anabranches on ~50
    # braided rivers (Kechika, Liard, Finlay, Herrick …) — whole blue lines present in one build and
    # not the other. The order is now a function of the node ids alone.
    demands.sort(key=lambda d: (-len(d[0]), sorted(d[0]), d[1]))
    keep: set[str] = set()
    for _ in range(3):                       # re-route against what is already kept until it settles
        nxt: set[str] = set()
        for entries, ex in demands:
            nxt |= _route(graph, comp, entries, ex, nxt, fwa_main=fwa_main)
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
