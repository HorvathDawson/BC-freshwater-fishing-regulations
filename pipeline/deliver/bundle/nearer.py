"""WHICH ✱ ROW IS NEARER A TRIBUTARY (owner ruling 2026-10-16, ROUND14 1, RULINGS T14).

Where two ✱ rows' tributary walks reach one stream and their rules compete there, the row of the water
NEAREST that stream wins: on the Duncan River's tributaries the Duncan's "bull trout daily quota = 2 (any
size)" beats Kootenay Lake's tributaries' "Bull trout catch and release".

NEARER, precisely. Two tables, both written once by the bundle (`build`) and read by the reader:

  item_down    each named water's OUTLETS followed down the atlas flow graph to the first OTHER named
               water each one meets (`item_down`): from every edge leaving the water, the path takes
               the mainstem edge first (`MAINSTEM_EDGE_KINDS`, then the lowest node id — the trace
               `gauges.consume.shed.downstream_map` follows), through unnamed sections, and stops at a
               named section. A path that comes back into the water itself (a braid) is no outlet.
  trib_nearer  (near, far) entry pairs (`nearer_pairs`): of two water rows whose rules reach some
               section together by the tributary walk, `near`'s own water (its matched items) DRAINS
               INTO `far`'s — the transitive `item_down` of one of near's waters is one of far's — and
               far's never drains into near's. On a section both reach, the flow path down from it is
               unique (the mainstem), and it meets near's water first: near is the nearer row. Rows
               whose waters do not nest (or nest both ways) are not ordered: the ladder leaves them as
               it did.

Deterministic: sorted iteration, no clocks. The reader (`read.effective_rules_bound`, step 4) only looks
the pair up (`read._nearer_of`); it never walks the graph.
"""
from __future__ import annotations

import sqlite3
from typing import Dict, Iterable, List, Mapping, Set, Tuple

from pipeline.common.models.graph import MAINSTEM_EDGE_KINDS
from pipeline.common import entry_id as EID

#: a path through unnamed sections longer than this is the sea or the edge of the atlas
MAX_STEPS = 200_000


def _next(graph, v: str):
    best = None
    for ix in sorted(graph.down_adj.get(v, ())):
        e = graph.edges[ix]
        if e.to_node not in graph.nodes:
            continue
        rank = (0 if getattr(e, "kind", "") in MAINSTEM_EDGE_KINDS else 1, e.to_node)
        if best is None or rank < best:
            best = rank
    return None if best is None else best[1]


def item_down(graph, own: Mapping[str, Set[str]]) -> Dict[str, Set[str]]:
    """{water: the named waters its outlets reach first} for every water with one (see the module
    docstring). `own` maps each named water to its section ids (graph node ids)."""
    owner: Dict[str, Set[str]] = {}
    for item in sorted(own):
        for s in own[item]:
            owner.setdefault(s, set()).add(item)
    memo: Dict[str, frozenset] = {}

    def first_named(v: str) -> frozenset:
        path, seen = [], set()
        cur, got = v, frozenset()
        while cur is not None and len(path) < MAX_STEPS:
            if cur in memo:
                got = memo[cur]
                break
            if cur in owner:
                got = frozenset(owner[cur])
                break
            if cur in seen:                          # a loop of unnamed sections: no water below
                break
            seen.add(cur)
            path.append(cur)
            cur = _next(graph, cur)
        for p in path:
            memo[p] = got
        return got

    out: Dict[str, Set[str]] = {}
    for item in sorted(own):
        mine = own[item]
        got: Set[str] = set()
        for s in sorted(mine):
            for ix in sorted(graph.down_adj.get(s, ())):
                to = graph.edges[ix].to_node
                if to in mine or to not in graph.nodes:
                    continue
                got |= first_named(to)
        got.discard(item)
        if got:
            out[item] = got
    return out


def drains_into(down: Mapping[str, Set[str]], start: Iterable[str]) -> Set[str]:
    """Every named water reached by following `item_down` from the waters `start` (not themselves,
    unless a loop comes back)."""
    seen: Set[str] = set()
    stack = sorted(start)
    while stack:
        cur = stack.pop()
        for d in sorted(down.get(cur, ())):
            if d not in seen:
                seen.add(d)
                stack.append(d)
    return seen


def nearer_pairs(down: Mapping[str, Set[str]], own: Mapping[str, Set[str]],
                 co: Iterable[frozenset]) -> Set[Tuple[str, str]]:
    """{(near, far)} over the entry pairs `co` (entries whose rules reach one section together by the
    walk): near's waters drain into far's and not the reverse. `own` maps an entry to its waters."""
    reach = {e: drains_into(down, ws) for e, ws in own.items()}
    out: Set[Tuple[str, str]] = set()
    for pair in sorted(co, key=sorted):
        a, b = sorted(pair)
        if a not in own or b not in own:
            continue
        ab, ba = bool(reach[a] & own[b]), bool(reach[b] & own[a])
        if ab and not ba:
            out.add((a, b))
        elif ba and not ab:
            out.add((b, a))
    return out


def write_item_down(db: sqlite3.Connection, graph, registry_items, cov) -> None:
    """`item_down` (item ords) from the atlas graph, for every water item of the registry."""
    from pipeline.common.registry_kinds import is_water
    ords = dict(db.execute("SELECT item_id, ord FROM item"))
    own = {i["id"]: set(i.get("section_ids") or []) for i in registry_items
           if is_water(i) and i["id"] in ords and i.get("section_ids")}
    down = item_down(graph, own)
    rows = sorted((ords[a], ords[b]) for a, bs in down.items() for b in bs)
    db.executemany("INSERT INTO item_down (ord, down_ord) VALUES (?,?)", rows)
    cov.filled("item_down", len(rows))


#: POLICY (T679 D3, owner 2026-11-12: "ignore the loops"): NEARER IS READ FROM WHERE EACH WATER EXITS — `item_down` less
#: every receiver that flows back into the water (`item_exit_down`, written by `topology.write` with
#: `topology.exit_receivers`, the one answer the water's exit already uses). On a braid the raw `item_down` nests two
#: waters both ways and leaves the pair unordered. False = the raw `item_down` (the mutation pin).
NEARER_IGNORES_LOOPS = True


def compute_trib_nearer(db: sqlite3.Connection) -> Set[Tuple[str, str]]:
    """The (near, far) pairs from the bundle's own rows: `item_exit_down` (`NEARER_IGNORES_LOOPS`), `entry.matched`
    and the rule sets' `trib` members (water rows only: `r…` entries)."""
    import json
    item_of = dict(db.execute("SELECT ord, item_id FROM item"))
    down: Dict[str, Set[str]] = {}
    table = "item_exit_down" if NEARER_IGNORES_LOOPS else "item_down"
    for a, b in db.execute(f"SELECT ord, down_ord FROM {table}"):
        down.setdefault(item_of[a], set()).add(item_of[b])
    sets: Dict[int, Set[str]] = {}
    for set_id, e in db.execute("SELECT DISTINCT set_id, entry_id FROM ruleset "
                                "WHERE via = 'trib' AND entry_id LIKE 'r%' ORDER BY set_id, entry_id"):
        sets.setdefault(set_id, set()).add(e)
    co = {frozenset((a, b)) for es in sets.values() for a in es for b in es if a < b}
    want = {e for p in co for e in p}
    own: Dict[str, Set[str]] = {}
    for e, matched in db.execute("SELECT entry_id, matched FROM entry ORDER BY entry_id"):
        if e in want:
            own[e] = set(json.loads(matched or "[]"))
    return nearer_pairs(down, own, co)


#: POLICY (T1251, owner 2026-11-27: "The most upstream row, in 0.10.0"): THE MOST UPSTREAM ROW IS NAMED. When two
#: inherited closures close a water on the same day, the card names the row whose water is the more upstream on the
#: water's way down: `trib_upstream`, the pairs `trib_nearer` orders plus the pairs whose TRIBUTARY WALKS NEST — every
#: section near's walk reaches (`ruleset` via `trib`), far's walk reaches too, and not the reverse. Nesting orders two
#: lakes on one river where `trib_nearer` cannot: Lower Campbell Lake and John Hart Lake both exit into the Campbell
#: River, which runs on through John Hart, so their outlets meet the same first water; Lower Campbell's walk lies inside
#: John Hart's. `trib_nearer` decides first (a nesting pair it orders the other way is not taken); two rows of ONE water
#: (a river's row and its tributaries' row) are never ordered by nesting. Read only to name a closure
#: (`answers.rows.evaluate`), never to decide what speaks (that stays `trib_nearer`, RULINGS T14). False = no nesting
#: pairs: the citation reads `trib_nearer` alone (the mutation pin).
WALKS_THAT_NEST_ORDER_THE_ROWS = True


def nested_pairs(walk: Mapping[str, Set[int]], own: Mapping[str, Set[str]],
                 co: Iterable[frozenset]) -> Set[Tuple[str, str]]:
    """{(near, far)} over the entry pairs `co`: near's walk (its sections, `walk`) is a strict subset of far's, and the
    two rows share no water (`own`)."""
    out: Set[Tuple[str, str]] = set()
    for pair in sorted(co, key=sorted):
        a, b = sorted(pair)
        if a not in walk or b not in walk or own.get(a, set()) & own.get(b, set()):
            continue
        if walk[a] < walk[b]:
            out.add((a, b))
        elif walk[b] < walk[a]:
            out.add((b, a))
    return out


def compute_trib_upstream(db: sqlite3.Connection) -> Set[Tuple[str, str]]:
    """The (near, far) pairs a closure's citation reads (`WALKS_THAT_NEST_ORDER_THE_ROWS`): `trib_nearer`'s, plus the
    nesting pairs it leaves unordered. From the bundle's own rows (`ruleset`, `section_ruleset`, `entry.matched`)."""
    import json
    nearer = compute_trib_nearer(db)
    if not WALKS_THAT_NEST_ORDER_THE_ROWS:
        return nearer
    sets: Dict[int, Set[str]] = {}
    for set_id, e in db.execute("SELECT DISTINCT set_id, entry_id FROM ruleset "
                                "WHERE via = 'trib' AND entry_id LIKE 'r%' ORDER BY set_id, entry_id"):
        sets.setdefault(set_id, set()).add(e)
    co = {frozenset((a, b)) for es in sets.values() for a in es for b in es if a < b}
    want = {e for p in co for e in p}
    members: Dict[int, List[int]] = {}
    for sid, set_id in db.execute("SELECT sid, set_id FROM section_ruleset ORDER BY sid"):
        if set_id in sets:
            members.setdefault(set_id, []).append(sid)
    walk: Dict[str, Set[int]] = {}
    for set_id, es in sets.items():
        for e in es & want:
            walk.setdefault(e, set()).update(members.get(set_id, ()))
    own: Dict[str, Set[str]] = {}
    for e, matched in db.execute("SELECT entry_id, matched FROM entry ORDER BY entry_id"):
        if e in want:
            own[e] = set(json.loads(matched or "[]"))
    ordered = nearer | {(b, a) for a, b in nearer}
    return nearer | {p for p in nested_pairs(walk, own, co) if p not in ordered}


def upstream_cycle(pairs: Iterable[Tuple[str, str]]) -> List[str]:
    """A circle in the (near, far) pairs (`trib_nearer`'s and the nested walks' mixed, with a third row), as the rows
    met on it (the first repeated at the end); [] when none. The build drops a contradiction pair by pair only, so a
    circle through three rows is refused here (`prove`)."""
    nxt: Dict[str, List[str]] = {}
    for a, b in sorted(pairs):
        nxt.setdefault(a, []).append(b)
    state: Dict[str, int] = {}
    for start in sorted(nxt):
        if state.get(start):
            continue
        path, stack = [], [(start, iter(nxt.get(start, ())))]
        state[start] = 1
        path.append(start)
        while stack:
            node, it = stack[-1]
            for b in it:
                if state.get(b) == 1:
                    return path[path.index(b):] + [b]
                if not state.get(b):
                    state[b] = 1
                    path.append(b)
                    stack.append((b, iter(nxt.get(b, ()))))
                    break
            else:
                state[node] = 2
                path.pop()
                stack.pop()
    return []


def write_trib_upstream(db: sqlite3.Connection, cov) -> None:
    pairs = sorted(compute_trib_upstream(db))
    db.executemany("INSERT INTO trib_upstream (near, far) VALUES (?,?)", pairs)
    cov.filled("trib_upstream", len(pairs))


def write_trib_nearer(db: sqlite3.Connection, cov) -> None:
    pairs = sorted(compute_trib_nearer(db))
    db.executemany("INSERT INTO trib_nearer (near, far) VALUES (?,?)", pairs)
    cov.filled("trib_nearer", len(pairs))


def prove(db: sqlite3.Connection) -> None:
    """The stored pairs are exactly what the bundle's rows give (DATAFLOW: computed once, proved)."""
    stored = set(db.execute("SELECT near, far FROM trib_nearer"))
    if stored != compute_trib_nearer(db):
        raise SystemExit("trib_nearer: the stored pairs are not what item_down, entry.matched and the "
                         "rule sets give — rebuild the bundle")
    stored_up = set(db.execute("SELECT near, far FROM trib_upstream"))
    loop = upstream_cycle(stored_up)
    if loop:
        raise SystemExit(f"trib_upstream: the pairs run in a circle ({' > '.join(loop)}): no row is the most upstream "
                         f"there — fix the rows' waters or walks, then rebuild the bundle")
    if stored_up != compute_trib_upstream(db):
        raise SystemExit("trib_upstream: the stored pairs are not what trib_nearer and the rule sets' walks give — "
                         "rebuild the bundle")


def _lifts_of(x: dict) -> Set[str]:
    return {f"{e['entry_id']}::{e['rule_id']}" for e in x.get("exempts") or []}


def silent_nearer(bound: Iterable[Tuple[str, str, str]], rules: Mapping[Tuple[str, str], dict],
                  nearer: Iterable[Tuple[str, str]], lifter: str) -> List[Tuple[str, str]]:
    """OWNER RULING Q1 (2026-10-16, RULINGS T15): a farther ✱ row's printed EXEMPTION reaches every one of
    its tributaries — kept — and where a NEARER water row binding the same section prints no such exemption,
    that is said (a gotcha), never decided. For the walked lifting rule `lifter` ("entry::rule") on a rule set
    `bound` [(entry, rule, via)]: [(entry, how)] — each water row of the set that is nearer the stream than
    the lifter's row (`how` "nearer_row": it reaches the stream by the walk too and its water drains into the
    lifter's, `trib_nearer`; "own_row": the stream's own row) and has no rule here lifting any rule the
    lifter lifts. Sorted; empty when every nearer row prints the exemption too (the Lardeau's tributaries)."""
    from pipeline.deliver.bundle import read
    a, _sep, _r = lifter.partition("::")
    x = rules[tuple(lifter.split("::", 1))]
    lifted = _lifts_of(x)
    pairs = set(nearer)
    by_entry: Dict[str, List[Tuple[str, str]]] = {}
    for e, r, via in bound:
        by_entry.setdefault(e, []).append((r, via))
    out = []
    for e, rs in sorted(by_entry.items()):
        if e == a or not EID.is_water_row(e):
            continue
        y = rules[(e, rs[0][0])]
        if read.source_of(y).rank != 0:                  # a water's own row, not an area row
            continue
        vias = {v for _r, v in rs}
        how = "own_row" if "reach" in vias else "nearer_row" if (e, a) in pairs else None
        if how is None:
            continue
        if any(_lifts_of(rules[(e, r)]) & lifted for r, _v in rs):
            continue
        out.append((e, how))
    return out
