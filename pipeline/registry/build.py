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

from pipeline.models import NameSource, NodeKind, RegistryBoundary, RegistryItem, StreamGraph, StreamNode
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


def _ref_ids(nodes) -> tuple[str, ...]:
    """Every FWA id these nodes answer to, prefixed — the ids an override might pin to this item.
    The matcher builds `id_index: ref_id -> item_id` from these to resolve curated pins.

    Streams answer to gnis (own + name-tuple gnis) + trimmed wsc + blk. LAKES answer to gnis + wbk
    ONLY: a lake node also carries the through-river's wsc/blk (the stream threading it), and emitting
    those would let a *stream* override's wsc/blk pin resolve onto the lake — a false duplicate match.
    A lake is identified by wbk (and any GNIS_ID_1/2/3 on its name tuples), never by a river code."""
    ids: set[str] = set()
    for n in nodes:
        for g in {n.gnis_id, *(t.gnis_id for t in n.name_tuples)}:
            if g:
                ids.add(f"gnis:{g}")
        if n.wbk:
            ids.add(f"wbk:{n.wbk}")
        if n.kind == NodeKind.lake:
            continue                                     # wsc/blk belong to the through-river, not the lake
        if n.wsc:
            ids.add(f"wsc:{trim_wsc(n.wsc)}")
        if n.blk:
            ids.add(f"blk:{n.blk}")
    return tuple(sorted(ids))


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


def build_registry(graph: StreamGraph, prof=None) -> dict[str, RegistryItem]:
    """Group section-nodes into RegistryItems (one coded stream / lake = one item) with their
    bindable boundaries. gnis-first grouping, braid-unified via the WSC->gnis map.

    ``prof`` (optional ``pipeline.utils.profiling.Profiler``) breaks the ~60-min stage into
    sub-phases; pass one or set env ``PIPELINE_PROFILE=1`` to see where the time goes."""
    import time
    from pipeline.utils.profiling import Profiler
    prof = prof or Profiler()

    with prof.phase("wsc_gnis map (scan all nodes)"):
        wsc_gnis = _wsc_gnis_map(graph)
    with prof.phase("group nodes by item_id"):
        groups: dict[str, list[StreamNode]] = defaultdict(list)
        for n in graph.nodes.values():
            groups[item_id(n, wsc_gnis)].append(n)

    registry: dict[str, RegistryItem] = {}
    _t_items = time.perf_counter()
    for iid, nodes in groups.items():
        kind = "lake" if nodes[0].kind == NodeKind.lake else "stream"
        names = [n.display_name for n in nodes if n.display_name]
        name = max(names, key=len) if names else ""          # display_names agree within an item; longest wins ties
        # Searchable variants = the nodes' name-tuple names, EXCEPT a side-channel name borrowed from
        # a DIFFERENT waterbody (its gnis is not one of this item's own). Blind Slough is a side channel
        # of the Stave, so its nodes carry ('Stave River', side_channel, gnis 14589) — kept on the node
        # (for grouping/relationship) but NOT a searchable alias of Blind Slough, else 'Stave River'
        # would resolve to two items. Own gnis = the item's gnis-key + any member node's own gnis.
        own_gnis = {n.gnis_id for n in nodes if n.gnis_id}
        if iid.startswith("gnis:"):
            own_gnis.add(iid.split(":", 1)[1])

        def _foreign_sc(t) -> bool:
            return (t.source == NameSource.side_channel and t.gnis_id
                    and t.gnis_id not in own_gnis)

        own_names = {t.name for n in nodes for t in n.name_tuples if t.name and not _foreign_sc(t)}
        if own_names:
            variants = tuple(sorted(own_names))              # has its own name — drop borrowed neighbour names
        else:
            # a channel with NO name of its own: keep the inherited mainstem name so it stays searchable
            # (else it has no search key at all). Its gnis grouped it under the mainstem, so no ambiguity.
            variants = tuple(sorted({t.name for n in nodes for t in n.name_tuples if t.name}))
        # NAMED items only (DESIGN §Registry): a reg can only match a named item, so the ~1.7M unnamed
        # stream pieces are pure bloat (they blow registry.json to ~458MB). Keep anything with a display
        # name OR a name variant; drop the truly-nameless. Area items are added separately below.
        if not name and not variants:
            continue
        item_slug = _slug(name) or iid.replace(":", "_")
        bmap: dict[str, RegistryBoundary] = {}                # keyed by readable id; disambiguate collisions
        seen_refs: set[str] = set()
        for n in nodes:
            for end in (n.lower_bound, n.upper_bound):
                rb = _boundary(end)
                if rb is None or rb.ref in seen_refs:
                    continue
                seen_refs.add(rb.ref)
                # curated split ids are already globally unique + encode context; auto boundaries
                # (lake/outlet/headwaters) are item-prefixed so a shared lake stays unique per river.
                base = rb.id if rb.ref.startswith("split:") else f"{item_slug}__{rb.id}"
                rid = base
                i = 2
                while rid in bmap:
                    rid = f"{base}_{i}"; i += 1
                bmap[rid] = RegistryBoundary(id=rid, label=rb.label, kind=rb.kind, ref=rb.ref, wbk=rb.wbk)
        registry[iid] = RegistryItem(
            id=iid, name=name, kind=kind, variants=variants,
            section_ids=tuple(n.node_id for n in nodes),
            boundaries=tuple(bmap.values()),
            ref_ids=_ref_ids(nodes),
        )

    prof.add("build named items + boundaries", time.perf_counter() - _t_items)

    # area items (kind=area): every admin closure a node was flagged INSIDE (node.in_areas, set by
    # the graph's area_boundary inside-pass). Layer-agnostic — parks_bc / parks_nat / wma / ecological
    # reserves / the historical trail all land here. A `within(area)` rule resolves to these sections.
    with prof.phase("area items"):
        area_nodes: dict[str, list[StreamNode]] = defaultdict(list)
        for n in graph.nodes.values():
            for area in n.in_areas:
                area_nodes[area].append(n)
        for area, nodes in area_nodes.items():
            aid = f"area:{_slug(area)}"
            registry[aid] = RegistryItem(id=aid, name=area, kind="area", variants=(),
                                         section_ids=tuple(n.node_id for n in nodes), boundaries=())
    prof.report("build_registry")
    return registry


def add_waterbody_items(registry: dict[str, RegistryItem], names: dict[str, tuple],
                        kind: str) -> dict[str, RegistryItem]:
    """Add NAMED waterbodies from an FWA polygon layer as registry items keyed by wbk, for any wbk NOT
    already an item. Covers two gaps where the graph alone misses a regulated, named waterbody:

      - ``kind='lake'``    — isolated named lakes/reservoirs with NO through-stream fid never become
                             graph nodes (Frazer Lake, etc.), so they'd otherwise be unmatchable.
      - ``kind='wetland'`` — wetlands (marshes/ponds/sloughs) are never noded (a stream overlays them).

    These items have no section_ids/boundaries (not in the graph) — just enough for the matcher to
    point an Entry at them (by name or a curated ``wbk``/``gnis`` pin); the resolver binds their polygon
    at resolve time. ``names`` = get_lake_names / get_wetland_names output ({wbk: ((name, gnis_id), ...)}).
    ref_ids = the wbk + every paired gnis. A wbk already in the registry (a graph node owns it) is
    skipped, so richer node-derived items always win. Mutates + returns."""
    for wbk, pairs in names.items():
        iid = f"wbk:{wbk}"
        if iid in registry:                              # a graph node already owns this wbk
            continue
        nms = [nm for nm, _ in pairs if nm]
        name = max(nms, key=len) if nms else ""
        if not name:
            continue
        refs = {f"wbk:{wbk}"} | {f"gnis:{gid}" for _, gid in pairs if gid}
        registry[iid] = RegistryItem(
            id=iid, name=name, kind=kind,
            variants=tuple(sorted(set(nms))),
            ref_ids=tuple(sorted(refs)),
        )
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
