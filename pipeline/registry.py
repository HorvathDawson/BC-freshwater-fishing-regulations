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

from __future__ import annotations

from collections import defaultdict

from pipeline.models import NodeKind, RegistryBoundary, RegistryItem, StreamGraph, StreamNode
from pipeline.utils.wsc import trim_wsc


def group_key(n: StreamNode) -> str:
    """GROUP a river's nodes by the key every node of one coded stream shares: WSC for streams
    (spans the mainstem + its braids/unnamed segments), WBK for lakes, BLK only if WSC is missing.
    Grouping by gnis fragments a river (unnamed segments carry no gnis) — so it is NOT the group key."""
    if n.kind == NodeKind.lake:
        return f"wbk:{n.wbk}"
    if n.wsc:
        return f"wsc:{trim_wsc(n.wsc)}"
    if n.gnis_id:
        return f"gnis:{n.gnis_id}"
    return f"blk:{n.blk}"


def _display_id(group_k: str, nodes: list[StreamNode]) -> str:
    """The item's public id: prefer gnis (stable, 1:1 with the name) when the group has one; else the
    WSC group key. Lakes keep wbk."""
    if group_k.startswith(("wbk:", "gnis:", "blk:")):
        return group_k
    gnis = next((n.gnis_id for n in nodes if n.gnis_id), "")
    return f"gnis:{gnis}" if gnis else group_k


def _boundary(b) -> RegistryBoundary | None:
    if b is None:                       # natural end (outlet/headwaters) — not a bindable cut
        return None
    return RegistryBoundary(ref=b.boundary_id, label=b.label or "", kind=b.kind.value)


def build_registry(graph: StreamGraph) -> dict[str, RegistryItem]:
    """Group section-nodes into RegistryItems (by coded stream / lake) with their bindable boundaries."""
    groups: dict[str, list[StreamNode]] = defaultdict(list)
    for n in graph.nodes.values():
        groups[group_key(n)].append(n)

    registry: dict[str, RegistryItem] = {}
    for gk, nodes in groups.items():
        iid = _display_id(gk, nodes)
        kind = "lake" if nodes[0].kind == NodeKind.lake else "stream"
        names = [n.display_name for n in nodes if n.display_name]
        name = max(names, key=len) if names else ""          # display_names agree within an item; longest wins ties
        variants = tuple(sorted({t.name for n in nodes for t in n.name_tuples if t.name}))
        bmap: dict[str, RegistryBoundary] = {}
        for n in nodes:
            for end in (n.lower_bound, n.upper_bound):
                rb = _boundary(end)
                if rb and rb.ref not in bmap:
                    bmap[rb.ref] = rb
        registry[iid] = RegistryItem(
            id=iid, name=name, kind=kind, variants=variants,
            section_ids=tuple(n.node_id for n in nodes),
            boundaries=tuple(bmap.values()),
        )
    return registry
