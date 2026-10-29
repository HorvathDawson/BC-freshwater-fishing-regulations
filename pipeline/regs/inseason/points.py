"""THE PICK-POINTS OF ONE WATER and what an in-season extent covers (T230, owner 2026-10-29).

An in-season change names its place in the province's words ("downstream of the Region 5 boundary", "upstream of
Pitt Lake", "downstream of Keen Creek … to the river mouth at Kootenay Lake"). The curated splits (splits.json) are
only a few of those places, so a water offers every place the ATLAS already knows on its main line:

    <split id>                    a registry boundary of the water: a splits.json cut, an area cut, a length /
                                  confluence cut (`RegistryItem.boundaries`; resolved by `extent.resolve_extent`)
    lake:<wbk>:inlet / :outlet    where the main line enters / leaves a lake (a stem piece's bound on the lake)
    trib:<item_id>                the confluence of a NAMED tributary on the main line (a graph edge into the stem)
    region:<A>|<B>                a region boundary crossing, A the downstream region, B the upstream one
    mu:<A>|<B>                    a management-unit boundary crossing, the same way
    mouth                         the water's mouth (its stem's lowest measure)

None of the derived points is a map cut: they are read off the graph at a ROUTE MEASURE on the stem and select by
measure (`_by_measure`): a piece is above a point when its upper end is above it, below when its lower end is below
— so a piece the point falls inside is on both sides (never dropped). Off-stem pieces (side channels) sit where they
flow back into the stem. An extent naming only registry boundaries resolves through the builder's own resolver.

`tributaries`: "include" adds the tributary walk (`graph.tributaries.expand`, the regs' own), "only" keeps the walk
without the selected stretch ("Granby River's Tributaries").

Read only; deterministic (sorted everywhere). Never writes splits.json.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

DERIVED_RE = re.compile(r"^(mouth|lake:[\w.\-]+:(inlet|outlet)|trib:(gnis|wbk|blk|wsc):[\w.\-]+|"
                        r"region:[\w]+\|[\w]+|mu:[\w\-]+\|[\w\-]+)$")
#: a boundary id the registry mints (not in splits.json): area / length cuts
ATLAS_ID_RE = re.compile(r"^(area:.+|length:\d+:\d+|lake:\d+)$")
EPS_M = 1.0


def is_derived(pid: str) -> bool:
    return bool(DERIVED_RE.match(pid))


@dataclass(frozen=True)
class Point:
    id: str
    label: str
    kind: str                 # split | lake | trib | region | mu | mouth
    m: Optional[float]        # route measure on the stem (None: not on the stem)
    node: Optional[str]       # a stem node the point stands at (for its map coordinate)
    end: str = "down"         # which end of `node`: "down" | "up"


_NODE_ITEMS: dict = {}


def node_items(reg) -> dict[str, tuple[str, ...]]:
    """node id -> the items holding it (cached per registry object)."""
    k = id(reg)
    if k not in _NODE_ITEMS:
        out: dict[str, list[str]] = {}
        for iid in sorted(reg):
            for s in reg[iid].section_ids:
                out.setdefault(s, []).append(iid)
        _NODE_ITEMS.clear()
        _NODE_ITEMS[k] = {s: tuple(v) for s, v in out.items()}
    return _NODE_ITEMS[k]


def stem_of(reg, g, item_id: str) -> Optional[str]:
    from pipeline.deliver.bundle.lines import main_line_of
    return main_line_of(g.nodes, reg[item_id].section_ids)


def _stem_nodes(reg, g, item_id: str):
    stem = stem_of(reg, g, item_id)
    nodes = [g.nodes[s] for s in reg[item_id].section_ids if s in g.nodes]
    return stem, sorted((n for n in nodes if n.blk == stem and getattr(n.kind, "value", n.kind) == "stream"),
                        key=lambda n: (n.down_m, n.up_m, n.node_id))


def points(reg, g, item_id: str) -> list[Point]:
    """Every pick-point on the water's main line, mouth first; ids unique."""
    from pipeline.atlas.registry import regions as R
    it = reg[item_id]
    stem, nodes = _stem_nodes(reg, g, item_id)
    out: dict[str, Point] = {}
    if not nodes:
        return [Point(b.id, b.label or b.id, "split", None, None) for b in sorted(it.boundaries or (), key=lambda b: b.id)
                if b.kind != "lake"]
    out["mouth"] = Point("mouth", _mouth_label(reg, g, nodes[0].node_id), "mouth", nodes[0].down_m,
                         nodes[0].node_id, "down")
    # registry boundaries: their measure where a stem piece is bounded by them
    at_ref: dict[str, tuple[float, str, str]] = {}
    for n in nodes:
        for b, end, m in ((n.lower_bound, "down", n.down_m), (n.upper_bound, "up", n.up_m)):
            if b is not None:
                at_ref.setdefault(str(b.boundary_id), (m, n.node_id, end))
    items = node_items(reg)
    for b in sorted(it.boundaries or (), key=lambda b: b.id):
        if b.kind == "lake":
            continue                                    # inlet / outlet below
        hit = at_ref.get(str(b.ref))
        out.setdefault(b.id, Point(b.id, b.label or b.id, "split", hit[0] if hit else None,
                                   hit[1] if hit else None, hit[2] if hit else "down"))
    # lake inlets / outlets on the stem
    for n in nodes:
        for b, end, m, word in ((n.lower_bound, "down", n.down_m, "inlet"), (n.upper_bound, "up", n.up_m, "outlet")):
            if b is None or not str(b.boundary_id).startswith("lake:"):
                continue
            wbk = str(b.boundary_id)[5:]
            name = next((reg[i].name for i in items.get(f"lake:{wbk}", ())
                         if reg[i].name and reg[i].kind in ("lake", "wetland") and not i.startswith("area:")), "") \
                or "the lake"
            pid = f"lake:{wbk}:{word}"
            out.setdefault(pid, Point(pid, f"{name} {word}", "lake", m, n.node_id, end))
    # named tributaries' confluences
    stem_ids = {n.node_id: n for n in nodes}
    own = set(it.section_ids)
    for to in sorted(stem_ids):
        for ei in g.up_adj.get(to, ()):
            e = g.edges[ei]
            fr = g.nodes.get(e.from_node)
            if fr is None or e.from_node in own or getattr(fr.kind, "value", fr.kind) != "stream":
                continue
            for tid in items.get(e.from_node, ()):
                t = reg[tid]
                if tid == item_id or not t.name or t.kind != "stream" or t.part_of:
                    continue
                pid = f"trib:{tid}"
                if pid not in out:
                    out[pid] = Point(pid, f"{t.name} confluence", "trib", float(e.at_measure), e.from_node, "down")
    # region / MU crossings between consecutive stem pieces
    sreg = R.section_regions(reg, g)
    for lo, hi in zip(nodes, nodes[1:]):
        if abs(lo.up_m - hi.down_m) > EPS_M:
            continue
        for kind, a, b in (("region", sreg.get(lo.node_id, frozenset()), sreg.get(hi.node_id, frozenset())),
                           ("mu", frozenset(lo.mus), frozenset(hi.mus))):
            if a and b and a != b:
                x, y = sorted(a - b or a)[0], sorted(b - a or b)[0]
                if x == y:
                    continue
                word = "Region" if kind == "region" else "MU"
                pid = f"{kind}:{x}|{y}"
                out.setdefault(pid, Point(pid, f"{word} {x.upper()} / {word} {y.upper()} boundary", kind,
                                          lo.up_m, hi.node_id, "down"))
    return sorted(out.values(), key=lambda p: (p.m is None, p.m or 0.0, p.id))


def _mouth_label(reg, g, node_id: str) -> str:
    """"Mouth (into Kootenay Lake)": the water the lowest stem piece flows into, when it is a named one."""
    items = node_items(reg)
    for ei in sorted(g.down_adj.get(node_id, ())):
        to = g.edges[ei].to_node
        names = sorted({reg[i].name for i in items.get(to, ()) if reg[i].name and not i.startswith("area:")})
        if names:
            return f"Mouth (into {names[0]})"
    return "Mouth"


def _attach(g, node_id: str, stem: str) -> Optional[float]:
    """Where an off-stem piece flows back into the stem (walking downstream), as a stem measure."""
    seen, nid = set(), node_id
    while nid not in seen and len(seen) < 64:
        seen.add(nid)
        outs = sorted(g.down_adj.get(nid, ()), key=lambda i: g.edges[i].to_node)
        if not outs:
            return None
        e = g.edges[outs[0]]
        to = g.nodes.get(e.to_node)
        if to is not None and to.blk == stem:
            return float(e.at_measure)
        nid = e.to_node
    return None


def _by_measure(reg, g, item_id: str, op: str, ms: list[float]) -> set[str]:
    stem = stem_of(reg, g, item_id)
    out = set()
    for s in reg[item_id].section_ids:
        n = g.nodes.get(s)
        if n is None:
            continue
        if n.blk == stem:
            lo, hi = n.down_m, n.up_m
        else:
            a = _attach(g, s, stem) if stem else None
            if a is None:
                continue
            lo = hi = a
        if op == "upstream_of" and hi > ms[0] + EPS_M:
            out.add(s)
        elif op == "downstream_of" and lo < ms[0] - EPS_M:
            out.add(s)
        elif op == "between":
            a, b = sorted(ms)
            if hi > a + EPS_M and lo < b - EPS_M:
                out.add(s)
    return out


class Unresolved(ValueError):
    pass


def resolve(reg, g, ext: dict) -> dict:
    """{"sections": [...], "stretch": n, "tributaries": n} for one in-season extent (`matches.InSeasonExtent` as a
    dict). Raises Unresolved with the reason when a point is not on the water or nothing is selected."""
    from pipeline.atlas.reach import extent as X
    from pipeline.atlas.graph import tributaries as T
    item_id, op, cuts = ext["item_id"], ext["op"], list(ext.get("splits") or [])
    if item_id not in reg:
        raise Unresolved(f"{item_id}: not in the registry")
    got_cuts = None
    if op == "whole" or not any(is_derived(c) for c in cuts):
        reasons: list = []
        got = X.resolve_extent(reg, g, [item_id], {"op": op, "item_id": item_id, "splits": cuts}, reasons=reasons)
        secs = set((got or {}).get("sections") or ())
        if not secs:
            raise Unresolved("; ".join(f"{c}{': ' + d if d else ''}" for c, d in reasons) or "binds nothing")
        from pipeline.atlas.reach.build import seeding_cuts
        got_cuts = seeding_cuts(g, [got])
    else:
        pts = {p.id: p for p in points(reg, g, item_id)}
        ms = []
        for c in cuts:
            p = pts.get(c)
            if p is None or p.m is None:
                raise Unresolved(f"{c}: not a place on {reg[item_id].name}'s main line")
            ms.append(p.m)
        if op == "between" and abs(ms[0] - ms[1]) <= EPS_M:
            raise Unresolved("the two places are the same point")
        secs = _by_measure(reg, g, item_id, op, ms)
        if not secs:
            raise Unresolved("selects no section")
        stem = stem_of(reg, g, item_id)
        lows = {"upstream_of": [ms[0]], "between": [min(ms)]}.get(op, [])
        got_cuts = [(stem, m) for m in lows] if stem else None
    trib = ext.get("tributaries")
    n_trib = 0
    if trib in ("include", "only"):
        walked = T.expand(g, secs, only=(trib == "only"), cuts=got_cuts, registry=reg,
                          own=frozenset(reg[item_id].section_ids) - secs)
        if trib == "only":
            n_trib = len(walked)
            secs = set(walked)
        else:
            n_trib = len(set(walked) - secs)
            secs = secs | set(walked)
        if not secs:
            raise Unresolved("the tributary walk finds nothing")
    return {"sections": sorted(secs), "n": len(secs), "tributaries": n_trib}
