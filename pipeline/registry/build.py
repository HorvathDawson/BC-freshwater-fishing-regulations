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
from collections import Counter, defaultdict
from dataclasses import replace

from pipeline.models import WATERBODY_KINDS, NameSource, NodeKind, RegistryBoundary, RegistryItem, StreamGraph, StreamNode
from pipeline.utils.wsc import trim_wsc


def _slug(s: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", (s or "").lower())).strip("_")


def _wsc_gnis_map(graph: StreamGraph) -> dict[str, str]:
    """wsc -> a gnis carried by any node of that coded stream, so braided/unnamed segments (no gnis
    of their own) inherit their stream's gnis and don't fragment the item."""
    m: dict[str, str] = {}
    for n in graph.nodes.values():
        if n.kind not in WATERBODY_KINDS and n.gnis_id and n.wsc:
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
    if n.kind in WATERBODY_KINDS:
        return f"wbk:{n.wbk}"
    g = _effective_gnis(n, wsc_gnis)
    if g:
        return f"gnis:{g}"
    if n.wsc:
        return f"wsc:{trim_wsc(n.wsc)}"
    return f"blk:{n.blk}"


def _primary_name(iid: str, nodes) -> str:
    """The name the item itself goes by: the display name of the nodes that own the item's gnis, else
    the most common display name (ties -> longest, for determinism)."""
    g = iid.split(":", 1)[1] if iid.startswith("gnis:") else ""
    own = [n.display_name for n in nodes if n.display_name and g and n.gnis_id == g]
    if own:
        return Counter(own).most_common(1)[0][0]
    names = Counter(n.display_name for n in nodes if n.display_name)
    if not names:
        return ""
    return max(names.items(), key=lambda kv: (kv[1], len(kv[0])))[0]


def split_distinct_names(groups: dict[str, list]) -> dict[str, list]:
    """Give a differently-NAMED side channel its own item instead of folding it into the mainstem.

    A channel with no GNIS of its own inherits the mainstem's name tuple (and so its gnis), which is
    what keeps unnamed braids attached to their river — correct. But when curation or the gazetteer
    gave that channel a name of its OWN, `display_name` already says it is a distinct water, and the
    synopsis regulates it as one (McArthur Island Slough on the Thompson, Squamish Powerhouse Channel
    on the Squamish). Folding it in did two kinds of damage: its regulation resolved to the whole
    mainstem (a no-powered-boats slough rule landing on all 82 sections of the Thompson), and the
    item took ITS name, because the item name was the longest member name — which is how the Fraser
    was displayed as 'Seabird Island North Side Channel' and the Squamish as 'Squamish Powerhouse
    Channel'.

    So: the mainstem keeps the item id and its own name; each other display name moves to its own item
    keyed by its lowest blk (the form `item_id` already uses for a stream with no gnis), which is also
    what a curated `blue_line_keys` pin resolves to.

    Only a node with NO gnis of its own moves. A node that OWNS the item's gnis is a gazetted reach of
    that very river, and curation renaming it ("Sicamous Narrows" for one reach of the Shuswap,
    '"Diana" Creek' for one of the Kloiya) does not make it a different water — splitting those out
    would give the river a second item answering to its own name and turn every plain "SHUSWAP RIVER"
    row ambiguous. Naming is handled for them by `_primary_name` instead."""
    out: dict[str, list] = {}
    for iid, nodes in groups.items():
        g = iid.split(":", 1)[1] if iid.startswith("gnis:") else ""
        main = _primary_name(iid, nodes)
        movable = [n for n in nodes
                   if n.display_name and n.display_name != main and n.gnis_id != g and not n.gnis_id]
        if not movable:
            out.setdefault(iid, []).extend(nodes)
            continue
        move = set(id(n) for n in movable)
        out.setdefault(iid, []).extend(n for n in nodes if id(n) not in move)
        extra: dict[str, list] = defaultdict(list)
        for n in movable:
            extra[n.display_name].append(n)
        for dn, ns in extra.items():
            blks = sorted(b for b in {n.blk for n in ns} if b)
            out.setdefault(f"blk:{blks[0]}" if blks else f"{iid}:{_slug(dn)}", []).extend(ns)
    return out


FLOW_RE = re.compile(r"\b(river|creek|brook|slough|channel)\b", re.I)
STILL_RE = re.compile(r"\b(lake|lakes|pond|reservoir|lagoon)\b", re.I)


def _norm_name(s: str) -> str:
    return " ".join((s or "").split()).casefold()


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
        if n.kind in WATERBODY_KINDS:
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
    return RegistryBoundary(id=rid, label=b.label or "", kind=b.kind.value, ref=bid, wbk=wbk,
                            aliases=tuple(b.aliases or ()))


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
        groups = split_distinct_names(groups)      # a NAMED side channel is its own water, not a reach

    registry: dict[str, RegistryItem] = {}
    _t_items = time.perf_counter()
    # Primary names of everything that will become a STREAM item — the set a lake's borrowed
    # through-river name is checked against below. Built here so it costs one pass, not one per item.
    stream_primary_names = {
        _norm_name(_primary_name(i, ns))
        for i, ns in groups.items() if ns[0].kind not in WATERBODY_KINDS
    }
    stream_primary_names.discard("")

    for iid, nodes in groups.items():
        kind = nodes[0].kind.value if nodes[0].kind in WATERBODY_KINDS else "stream"
        name = _primary_name(iid, nodes)                     # split_distinct_names left one display name per item
        # Searchable variants = the nodes' name-tuple names, EXCEPT a name borrowed from a DIFFERENT
        # waterbody (its gnis is not one of this item's own). Blind Slough is a side channel
        # of the Stave, so its nodes carry ('Stave River', side_channel, gnis 14589) — kept on the node
        # (for grouping/relationship) but NOT a searchable alias of Blind Slough, else 'Stave River'
        # would resolve to two items. Own gnis = the item's gnis-key + any member node's own gnis.
        # Own gnis = the item's gnis-key + any member node's own gnis + every gnis on a GAZETTED
        # name tuple. That last part matters for lakes: FWA gives a waterbody up to three gazetted
        # names (GNIS_NAME_1/2/3), each with its own id, so Nation Lakes legitimately answers to
        # 'Tsayta Lake' (gnis 29218) as well. Those are the feature's OWN names; only an INHERITED
        # name (side-channel / a name_variants entry keyed to a neighbour's gnis) is borrowed.
        own_gnis = {n.gnis_id for n in nodes if n.gnis_id}
        own_gnis |= {t.gnis_id for n in nodes for t in n.name_tuples
                     if t.gnis_id and t.source == NameSource.gazette}
        if iid.startswith("gnis:"):
            own_gnis.add(iid.split(":", 1)[1])

        def _foreign(t) -> bool:
            """A name tagged with SOMEONE ELSE'S gnis is a borrowed neighbour name, whatever attached
            it. The side-channel inheritance is one path; a name_variants entry keyed to the mainstem's
            gnis is the other, and it lands on the same inheriting nodes — which is how the reg name
            'FRASER RIVER' became a searchable alias of all 15 Fraser channel items at once."""
            return bool(t.gnis_id) and t.gnis_id not in own_gnis

        own_names = {t.name for n in nodes for t in n.name_tuples if t.name and not _foreign(t)}
        if kind in {k.value for k in WATERBODY_KINDS} and STILL_RE.search(name or ""):
            # A LAKE MUST NOT ANSWER TO THE RIVER THAT THREADS IT. FWA hangs the through-river's
            # gazetted name on the lake polygon (GNIS_NAME_2/3), so `_foreign` above cannot catch it:
            # the name carries the LAKE's own gnis, not a neighbour's. The result is that an exact
            # search for a river hits two items and has to be disambiguated by hand — Yakoun Lake
            # answered to 'YAKOUN RIVER', Mosquito Lake to 'PALLANT CREEK', Lakelse Lake to
            # 'LAKELSE RIVER' (the last is the binding this repo's matcher docstring names as a
            # historical wrong answer). 21 items corpus-wide.
            #
            # Only dropped when a REAL STREAM ITEM already owns that name as its primary: then the
            # name has an unambiguous home and the lake is still findable by its own. The other 7
            # cases keep it, because dropping a name nothing else answers to would make the water
            # unsearchable under a name the gazetteer really does give it.
            own_names = {v for v in own_names
                         if not (FLOW_RE.search(v) and not STILL_RE.search(v)
                                 and _norm_name(v) in stream_primary_names)}
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
        seen_refs: dict[str, str] = {}                        # ref -> the rid already minted for it
        for n in nodes:
            for end in (n.lower_bound, n.upper_bound):
                rb = _boundary(end)
                if rb is None:
                    continue
                if rb.ref in seen_refs:
                    # One boundary, two instances: a lake is the UPPER bound of the piece below it and
                    # the LOWER bound of the piece above, and an alias is recorded on just one of those
                    # edges. Skipping the repeat outright dropped the alias whenever the un-aliased
                    # edge happened to be seen first, so merge instead of discarding.
                    if rb.aliases:
                        prev = bmap[seen_refs[rb.ref]]
                        merged = tuple(dict.fromkeys((*prev.aliases, *rb.aliases)))
                        if merged != prev.aliases:
                            bmap[seen_refs[rb.ref]] = replace(prev, aliases=merged)
                    continue
                # curated split ids are already globally unique + encode context; auto boundaries
                # (lake/outlet/headwaters) are item-prefixed so a shared lake stays unique per river.
                base = rb.id if rb.ref.startswith("split:") else f"{item_slug}__{rb.id}"
                rid = base
                i = 2
                while rid in bmap:
                    rid = f"{base}_{i}"; i += 1
                bmap[rid] = RegistryBoundary(id=rid, label=rb.label, kind=rb.kind, ref=rb.ref,
                                             wbk=rb.wbk, aliases=rb.aliases)
                seen_refs[rb.ref] = rid
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
            # A blanket area is flagged with its catalog id (`area:{kind}:{slug}`) so the id stays
            # stable and collision-safe; a rule-scoped `area_boundary` split flags a bare label.
            aid = area if area.startswith("area:") else f"area:{_slug(area)}"
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


def add_curated_wbk_items(registry: dict[str, RegistryItem], name_variants: list[dict]) -> dict[str, RegistryItem]:
    """Mint registry items for waterbodies named ONLY by curation. A `name_variants` entry can target a
    wbk that is an ISOLATED, FWA-UNNAMED lake — no through-stream (so no graph node) and not picked up by
    `add_waterbody_items` (which only adds FWA-NAMED lakes). Such a wbk has no object for its curated name
    to attach to (e.g. Redstart Lake's 2nd polygon). Treat a name_variants name as making the wbk 'named':
    add an item keyed by wbk with the curated name so a `waterbody_keys` override resolves to it and it is
    searchable. Runs AFTER `add_waterbody_items` so richer FWA/graph items always win. ref_ids = the wbk
    (enough for a wbk pin); kind defaults to lake. Mutates + returns."""
    for e in name_variants:
        t = e.get("target", {})
        wbks = list(t.get("wbks", [])) + ([t["wbk"]] if t.get("wbk") else [])
        names = [n["name"] for n in e.get("names", []) if n.get("name")]
        if not wbks or not names:
            continue
        disp = next((n["name"] for n in e["names"] if n.get("name") and n.get("display")), names[0])
        for w in wbks:
            iid = f"wbk:{w}"
            if iid in registry:                          # a graph node / FWA-named item already owns it
                continue
            registry[iid] = RegistryItem(
                id=iid, name=disp, kind="lake",
                variants=tuple(sorted(set(names))),
                ref_ids=(f"wbk:{w}",),
            )
    return registry


def add_mu_sets(registry: dict[str, RegistryItem], geoms: dict,
                mu_polys: dict, wbk_polys: dict | None = None) -> dict[str, RegistryItem]:
    """Enrich NAMED stream/lake items with the SET of MUs their geometry passes through (line/area ×
    WMU). Only the matcher needs this (to break same-name collisions), and only named items collide,
    so unnamed streams are skipped. A river spanning several MUs carries all of them.

    Geometry source per item: its graph sections (``section_ids`` -> ``geoms``) when noded; else its own
    FWA waterbody polygon (``wbk_polys[wbk]``) for isolated lakes/wetlands that never became graph nodes
    — so EVERY named item gets real MUs, not just the graph-noded ones. Mutates + returns."""
    if not mu_polys:
        return registry
    from shapely.ops import unary_union
    from shapely.strtree import STRtree

    wbk_polys = wbk_polys or {}
    mu_ids = list(mu_polys)
    polys = [mu_polys[m] for m in mu_ids]
    tree = STRtree(polys)
    for iid, it in list(registry.items()):
        if it.kind == "area" or not it.name:                 # named streams/lakes only
            continue
        parts = [geoms[nid] for nid in it.section_ids if geoms.get(nid) is not None]
        if parts:
            g = unary_union(parts)
        else:                                                # isolated lake/wetland: use its own polygon
            wbk = iid.split(":", 1)[1] if iid.startswith("wbk:") else ""
            g = wbk_polys.get(wbk)
            if g is None:
                continue
        hits = {mu_ids[i] for i in tree.query(g) if g.intersects(polys[i])}
        if hits:
            registry[iid] = replace(it, mus=tuple(sorted(hits)))
    return registry
