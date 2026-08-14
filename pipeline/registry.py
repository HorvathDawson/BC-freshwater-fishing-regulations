"""Build the REGISTRY — the parser's single source of truth (DESIGN decision 2026-08-14).

Groups the stream graph's section-nodes into `RegistryItem`s (gnis -> wsc -> blk for streams; wbk for
lakes) and, for each, collects the **bindable boundaries** on it — the cut-points a rule's `Extent`
selects against. Boundaries come from the nodes' section bounds: curated splits (`split:{id}`) and the
auto lake edges (`lake:{wbk}`) the graph build already made. Names/variants come straight from the
graph nodes (which merged blk-chains and applied name_variants), so a confluence like Sitkatapa —
named only in name_variants, not FWA GNIS — is labelled correctly here.

    from pipeline.registry import build_registry
    registry = build_registry(graph)        # {item_id: RegistryItem}
"""

import re
from collections import defaultdict
from dataclasses import replace

from pipeline.models import NodeKind, RegistryBoundary, RegistryItem, StreamGraph, StreamNode
from pipeline.utils.wsc import trim_wsc


def _slug(s: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", (s or "").lower())).strip("_")


def _wsc_gnis_map(graph: StreamGraph) -> dict[str, str]:
    """wsc -> a gnis carried by any node of that coded stream, so braided/unnamed segments (no gnis
    of their own) inherit their stream's gnis and don't fragment the item."""
    m: dict[str, str] = {}
    for n in graph.nodes.values():
        if n.kind != NodeKind.lake and n.gnis_id and n.wsc:
            m.setdefault(trim_wsc(n.wsc), n.gnis_id)
    return m


def _effective_gnis(n: StreamNode, wsc_gnis: dict[str, str]) -> str:
    """The gnis identifying this node's river: its own gnis, else one carried on an inherited name
    tuple (side-channel / name-variant), else its WSC's gnis. Empty if the river has no gnis at all."""
    if n.gnis_id:
        return n.gnis_id
    for t in n.name_tuples:
        if t.gnis_id:
            return t.gnis_id
    return wsc_gnis.get(trim_wsc(n.wsc), "") if n.wsc else ""


def item_id(n: StreamNode, wsc_gnis: dict[str, str]) -> str:
    """gnis (own / inherited / WSC's) -> wsc -> blk for streams; wbk for lakes. gnis-first; wsc before blk."""
    if n.kind == NodeKind.lake:
        return f"wbk:{n.wbk}"
    g = _effective_gnis(n, wsc_gnis)
    if g:
        return f"gnis:{g}"
    if n.wsc:
        return f"wsc:{trim_wsc(n.wsc)}"
    return f"blk:{n.blk}"


def _boundary(b) -> RegistryBoundary | None:
    """A section end -> a bindable boundary with a READABLE id. Splits already have a readable id
    ('goat_creek_into_atnarko_river'); lakes get slug(label) ('tenas_lake') + keep the wbk."""
    if b is None:                       # natural end (outlet/headwaters) — not a bindable cut
        return None
    bid = b.boundary_id or ""
    wbk = ""
    if bid.startswith("split:"):
        rid = bid[len("split:"):]
    elif bid.startswith("lake:"):
        wbk = bid[len("lake:"):]
        rid = _slug(b.label) or f"lake_{wbk}"
    else:
        rid = bid or _slug(b.label)     # outlet / headwaters
    return RegistryBoundary(id=rid, label=b.label or "", kind=b.kind.value, ref=bid, wbk=wbk)


def build_registry(graph: StreamGraph) -> dict[str, RegistryItem]:
    """Group section-nodes into RegistryItems (one coded stream / lake = one item) with their
    bindable boundaries. gnis-first grouping, braid-unified via the WSC->gnis map."""
    wsc_gnis = _wsc_gnis_map(graph)
    groups: dict[str, list[StreamNode]] = defaultdict(list)
    for n in graph.nodes.values():
        groups[item_id(n, wsc_gnis)].append(n)

    registry: dict[str, RegistryItem] = {}
    for iid, nodes in groups.items():
        kind = "lake" if nodes[0].kind == NodeKind.lake else "stream"
        names = [n.display_name for n in nodes if n.display_name]
        name = max(names, key=len) if names else ""          # display_names agree within an item; longest wins ties
        variants = tuple(sorted({t.name for n in nodes for t in n.name_tuples if t.name}))
        bmap: dict[str, RegistryBoundary] = {}                # keyed by readable id; disambiguate collisions
        for n in nodes:
            for end in (n.lower_bound, n.upper_bound):
                rb = _boundary(end)
                if rb is None:
                    continue
                if rb.ref not in {b.ref for b in bmap.values()}:
                    rid = rb.id
                    i = 2
                    while rid in bmap:
                        rid = f"{rb.id}_{i}"; i += 1
                    bmap[rid] = RegistryBoundary(id=rid, label=rb.label, kind=rb.kind, ref=rb.ref, wbk=rb.wbk)
        registry[iid] = RegistryItem(
            id=iid, name=name, kind=kind, variants=variants,
            section_ids=tuple(n.node_id for n in nodes),
            boundaries=tuple(bmap.values()),
        )

    # area items (kind=area): every admin closure a node was flagged INSIDE (node.in_areas, set by
    # the graph's area_boundary inside-pass). Layer-agnostic — parks_bc / parks_nat / wma / ecological
    # reserves / the historical trail all land here. A `within(area)` rule resolves to these sections.
    area_nodes: dict[str, list[StreamNode]] = defaultdict(list)
    for n in graph.nodes.values():
        for area in n.in_areas:
            area_nodes[area].append(n)
    for area, nodes in area_nodes.items():
        aid = f"area:{_slug(area)}"
        registry[aid] = RegistryItem(id=aid, name=area, kind="area", variants=(),
                                     section_ids=tuple(n.node_id for n in nodes), boundaries=())
    return registry


def add_mu_sets(registry: dict[str, RegistryItem], geoms: dict,
                mu_polys: dict) -> dict[str, RegistryItem]:
    """Enrich NAMED stream/lake items with the SET of MUs their geometry passes through (line/area ×
    WMU). Only the matcher needs this (to break same-name collisions), and only named items collide,
    so unnamed streams are skipped. A river spanning several MUs carries all of them. Mutates + returns."""
    if not mu_polys:
        return registry
    from shapely.ops import unary_union
    from shapely.strtree import STRtree

    mu_ids = list(mu_polys)
    polys = [mu_polys[m] for m in mu_ids]
    tree = STRtree(polys)
    for iid, it in list(registry.items()):
        if it.kind == "area" or not it.name:                 # named streams/lakes only
            continue
        parts = [geoms[nid] for nid in it.section_ids if geoms.get(nid) is not None]
        if not parts:
            continue
        g = unary_union(parts)
        hits = {mu_ids[i] for i in tree.query(g) if g.intersects(polys[i])}
        if hits:
            registry[iid] = replace(it, mus=tuple(sorted(hits)))
    return registry
