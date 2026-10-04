"""ONE FLOWING WATER, LINES AND POLYGONS (user rulings 2026-10-03).

FWA draws a wide river or a slough as POLYGONS in its lakes layer as well as (or instead of) the
line through them, and the registry used to make each polygon a lake-kind item of its own carrying
the river's name: the Rancheria River was 42 "lake" items beside its line, East Gribbell Creek six
beside `wsc:915-679587-924307`, the Stellako `wbk:329148363` beside `gnis:7836`, Six Mile Slough
seven polygons with no line at all. A rule on the river stopped at its own polygon (the Stellako's
polygon carried the zone set only), and the polygon drew lake-blue in the middle of a river.

A FLOWING WATER IS ONE ITEM, and `item.kind` IS THE WATER KIND. `merge_flowing_polygons` runs once,
after the registry is built, over the CANDIDATES — lake/wetland items with sections whose name
flows (`pipeline.common.water_kind.flows`: head noun slough, canal, channel, river, creek; lakes are
never candidates). Same-named candidates that are CONNECTED (a graph edge, at most `MAX_GAP`
unnamed line pieces between them, outlines touching, or one gazetted feature by gnis) form a group.
Each group joins a water by THIS PRECEDENCE (FREV/sloughs F3, user rulings):

  1. a candidate carrying the gnis of a STREAM item joins it, no name test — FWA stamps the Vedder
     Canal polygon with the Vedder River's gnis 3062, the book regulates the canal as the Vedder
     ("Downstream of Vedder Crossing Bridge"), the user ruled it so; Mountain Slough's far polygon
     carries the Mountain Slough line's gnis 15743 though the Hogg Slough threads it;
  2. same name (name or variant, case-folded) AND contact — the river's own polygons (Stellako,
     Rancheria's 42 on the Little Rancheria line, East Gribbell's six, McQueen Slough);
  3. threaded by ONE stream item (its `lake_in` and `lake_out` pieces belong to one item), whatever
     the names: Hansen Slough x2 (South Hawks Creek), Lewis Slough in the Kootenay (Hanson Creek).
     The user ruled NO for a slough threaded by a creek of ANOTHER name — so this rule is OFF
     (`THREADED_JOINS`), and Hansen and Lewis stay their own stream items; the threading is still
     REPORTED (`threaded_by`). Two different items on the two sides never join;
  4. a same-named group with no stream becomes ONE stream item of its own: the shared gnis
     (`gnis:{id}`) when every piece carries one and no item has it, else the lowest wbk (Six Mile
     Slough `gnis:6438`: 7 polygons + the 5 unnamed line pieces between them);
  5. a lone candidate becomes a stream under its own id (Bowman Slough, Taylor Slough).

EVERY CANDIDATE LEAVES AS `stream` (F2): joined or not, it flows, and nothing downstream may
recompute that from its name. The unnamed line pieces that join two members are taken into the
item (they are its line). An UNNAMED polygon (in no item) that a slough, canal or channel item both
enters and leaves is that water (Nicomen Slough's four) — not for a river or a creek: 16,053
unnamed lakes are threaded by a named creek, and those are lakes.

Each absorbed item contributes its id to the survivor's `aliases` (the permanent record — see
`canonical`), its ref_ids (so a curated wbk/gnis pin still resolves through the matcher's
`id_index`), its variants (search "Hansen Slough" finds the item), its MUs and its boundaries. A
survivor's LAKE boundaries onto its own polygons are DROPPED (F6: "the river stops here, another
water begins" is false of its own polygon; refused if one carries a cut alias, so nothing bindable
vanishes silently), and another water's lake boundary onto an absorbed polygon becomes a
`confluence` boundary labelled with the river (a mouth is a mouth). A two-stream tie under rule 1
or 2 is REFUSED, never picked.

`canonical` / `canonical_ids` map an absorbed id to its item at the corpus' read points
(`pipeline.regs.parsing.io.read_entryfile`, the DFO `entries.load`, `reach.steelhead.resolve_list`);
`absorbed_refs` is the refusal everything else applies (`validate_catalogue`, `build_reaches`).
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import replace

from pipeline.common.models import RegistryItem
from pipeline.common.water_kind import flows, head_noun

#: How many UNNAMED line pieces may lie between two members of one water. Measured on the
#: province (2026-10-03): every join the rule makes needs at most one; two gives slack for a
#: connector the graph cut at a confluence.
MAX_GAP = 2

#: Head nouns whose unnamed polygons are part of the water when it both enters and leaves them.
SLOUGHLIKE = ("slough", "canal", "channel")

#: Rule 3 — a candidate threaded by ONE stream of another name joins it. User ruling 2026-10-03:
#: NO (Hansen Slough stays apart from South Hawks Creek, Lewis Slough from Hanson Creek). Kept as
#: a switch, not deleted: the measurement and the report (`threaded_by`) stay, so the ruling can be
#: revisited against numbers.
THREADED_JOINS = False

_WATER = ("stream", "lake", "wetland")


def _norm(s: str) -> str:
    return " ".join((s or "").split()).casefold()


def _kind(x) -> str:
    k = getattr(x, "kind", x)
    return str(getattr(k, "value", k) or "").lower()


def _names(it: RegistryItem) -> set[str]:
    return {_norm(n) for n in (it.name, *it.variants) if n}


def _gnis(it: RegistryItem) -> set[str]:
    return {r for r in it.ref_ids if r.startswith("gnis:")}


def _wbk_num(iid: str) -> int:
    try:
        return int(iid.split(":", 1)[1])
    except (IndexError, ValueError):
        return 1 << 62


def _own_polygon_wbks(section_ids) -> set[str]:
    return {s.split(":", 1)[1] for s in section_ids if s.startswith("lake:")}


def merge_flowing_polygons(registry: dict[str, RegistryItem], graph, polys: dict | None = None,
                           report: dict | None = None) -> dict[str, RegistryItem]:
    """Fold flowing polygons into their water (module docstring). Returns a NEW dict; `report`
    (optional) collects what was merged and why. `polys` is `{section id: outline}` (the atlas's
    `waterbody_polys.pkl`) for the outline-contact test; without it only graph contact counts."""
    rep = report if report is not None else {}
    nodes = graph.nodes

    owner: dict[str, str] = {}
    for iid, it in registry.items():
        if it.kind in _WATER:
            for s in it.section_ids:
                owner.setdefault(s, iid)

    def neighbours(sid: str):
        for ei in graph.up_adj.get(sid, ()):
            yield graph.edges[ei].from_node
        for ei in graph.down_adj.get(sid, ()):
            yield graph.edges[ei].to_node

    def unnamed_stream(sid: str) -> bool:
        n = nodes.get(sid)
        return n is not None and sid not in owner and _kind(n) == "stream"

    def reach(start: set[str]) -> dict[str, tuple[str, ...]]:
        """{owned section reachable -> the unnamed pieces crossed to get there}, through at most
        MAX_GAP unnamed line pieces."""
        out: dict[str, tuple[str, ...]] = {}
        q = deque((s, ()) for s in start)
        seen = set(start)
        while q:
            s, path = q.popleft()
            for o in neighbours(s):
                if o in seen:
                    continue
                seen.add(o)
                if o in owner:
                    out.setdefault(o, path)
                elif unnamed_stream(o) and len(path) < MAX_GAP:
                    q.append((o, path + (o,)))
        return out

    def threading(sid: str) -> tuple[set[str], set[str]]:
        """(items of the stream pieces flowing INTO this polygon, items of those it flows OUT to)."""
        ins = {owner[graph.edges[e].from_node] for e in graph.up_adj.get(sid, ())
               if graph.edges[e].from_node in owner
               and _kind(nodes.get(graph.edges[e].from_node)) == "stream"}
        outs = {owner[graph.edges[e].to_node] for e in graph.down_adj.get(sid, ())
                if graph.edges[e].to_node in owner
                and _kind(nodes.get(graph.edges[e].to_node)) == "stream"}
        return ins, outs

    cand = {iid: it for iid, it in registry.items()
            if it.kind in ("lake", "wetland") and flows(it.kind, it.name) and it.section_ids}
    by_name: dict[str, list[str]] = defaultdict(list)
    for iid, it in cand.items():
        by_name[_norm(it.name)].append(iid)

    # Same-named flowing polygons that are connected form one group.
    parent = {i: i for i in cand}

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    links: dict[str, dict[str, tuple[str, ...]]] = {}       # polygon -> {owned section: gap}
    for iid, it in cand.items():
        links[iid] = reach(set(it.section_ids))
        key = _norm(it.name)
        for s, gap in links[iid].items():
            o = owner[s]
            if o != iid and o in cand and _norm(cand[o].name) == key:
                parent[find(o)] = find(iid)
    # ...and same-named pieces carrying the same gnis are one GAZETTED feature wherever they lie
    # (Six Mile Slough's two runs, 1.8 km apart on two arms, are both gnis 6438).
    for key, ids in by_name.items():
        first: dict[str, str] = {}
        for i in ids:
            for g in _gnis(cand[i]):
                if g in first:
                    parent[find(i)] = find(first[g])
                else:
                    first[g] = i
    if polys:
        for key, ids in by_name.items():
            if len(ids) < 2:
                continue
            shapes = {i: [polys.get(s) for s in cand[i].section_ids] for i in ids}
            for a in range(len(ids)):
                for b in range(a + 1, len(ids)):
                    if find(ids[a]) == find(ids[b]):
                        continue
                    if any(p is not None and q is not None and p.distance(q) <= 1.0
                           for p in shapes[ids[a]] for q in shapes[ids[b]]):
                        parent[find(ids[b])] = find(ids[a])
    groups: dict[str, list[str]] = defaultdict(list)
    for i in cand:
        groups[find(i)].append(i)

    out = dict(registry)
    used: set[str] = set()
    merged, own_items, ties, threaded = [], [], [], []
    absorbed_wbks: dict[str, str] = {}                      # polygon wbk -> the item it joined
    for members in sorted(groups.values(), key=lambda ms: min(ms, key=_wbk_num)):
        members.sort(key=_wbk_num)
        key = _norm(cand[members[0]].name)
        gap_pieces: set[str] = set()
        contacts: dict[str, int] = defaultdict(int)         # rule 2: same name + contact
        for m in members:
            for s, gap in links[m].items():
                o = owner[s]
                if o in members:
                    gap_pieces.update(gap)                   # the line between two pieces
                    continue
                st = registry.get(o)
                if st is not None and st.kind == "stream" and key in _names(st):
                    contacts[o] += 1
                    gap_pieces.update(gap)
        # rule 1: a STREAM item whose id is a gnis a member carries — the same gazetted feature
        by_gnis = sorted({g for m in members for g in _gnis(cand[m])
                          if (st := registry.get(g)) is not None and st.kind == "stream"})
        # rule 3 (measured always, applied only under THREADED_JOINS): the ONE stream item on
        # both the in and the out side of some member
        thru: set[str] = set()
        sides: set[str] = set()
        for m in members:
            for s in cand[m].section_ids:
                ins, outs = threading(s)
                thru |= (ins & outs) - set(members)
                sides |= (ins | outs) - set(members)
        rule, survivor = "", ""
        if by_gnis:
            if len(by_gnis) > 1:
                ties.append({"name": cand[members[0]].name, "members": members, "rule": 1,
                             "streams": by_gnis})
            rule, survivor = "gnis", by_gnis[0]
        elif contacts:
            ranked = sorted(contacts, key=lambda o: (-contacts[o], -len(registry[o].section_ids), o))
            if len(ranked) > 1 and contacts[ranked[0]] == contacts[ranked[1]]:
                ties.append({"name": cand[members[0]].name, "members": members, "rule": 2,
                             "streams": ranked})
            rule, survivor = "name+contact", ranked[0]
        elif thru:
            threaded.append({"name": cand[members[0]].name, "members": members,
                             "streams": sorted(thru), "joined": THREADED_JOINS and len(thru) == 1})
            if THREADED_JOINS and len(thru) == 1:
                rule, survivor = "threaded", next(iter(thru))
        elif sides:
            threaded.append({"name": cand[members[0]].name, "members": members,
                             "streams": sorted(sides), "joined": False, "one_side": True})
        base = out[survivor] if survivor else None
        if base is None:
            if len(members) == 1:
                new_id, rule = members[0], "own"
            else:
                gn = [_gnis(cand[m]) for m in members]
                shared = set.intersection(*gn) if gn else set()
                g = sorted(shared)[0] if len(shared) == 1 else ""
                new_id = g if g and g not in used and (g not in registry or g in members) \
                    else members[0]
                rule = "group"
        else:
            new_id = survivor
        absorbed = [m for m in members if m != new_id]
        parts = ([base] if base is not None else []) + [cand[m] for m in members]
        head = parts[0]
        secs = list(dict.fromkeys(s for p in parts for s in p.section_ids))
        secs += sorted(s for s in gap_pieces if s not in set(secs))
        own_wbks = _own_polygon_wbks(secs)
        bounds, lost = [], []
        for b in {b.id: b for p in parts for b in p.boundaries}.values():
            if b.kind == "lake" and b.wbk in own_wbks:
                if b.aliases:
                    lost.append(b.id)
                continue                                     # F6: not another water
            bounds.append(b)
        if lost:
            raise SystemExit(f"registry.flowing: {new_id} ({head.name}) keeps a cut aliased onto "
                             f"its own polygon's edge, which the fold would drop: {lost} — give "
                             f"the cut its own anchor")
        item = replace(
            head, id=new_id, kind="stream", name=head.name,
            variants=tuple(sorted({v for p in parts for v in p.variants})),
            mus=tuple(sorted({m for p in parts for m in p.mus})),
            section_ids=tuple(secs),
            boundaries=tuple(bounds),
            ref_ids=tuple(sorted({r for p in parts for r in p.ref_ids} | set(absorbed))),
            aliases=tuple(sorted({a for p in parts for a in p.aliases} | set(absorbed))),
        )
        for m in members:
            out.pop(m, None)
        out[new_id] = item
        used.add(new_id)
        for s in gap_pieces:
            owner[s] = new_id
        for m in absorbed:
            for w in _own_polygon_wbks(cand[m].section_ids):
                absorbed_wbks[w] = new_id
        rec = {"id": new_id, "name": item.name, "rule": rule, "absorbed": absorbed,
               "line_pieces": sorted(gap_pieces), "polygons": len(members)}
        (merged if base is not None else own_items).append(rec)
    if ties:
        raise SystemExit("registry.flowing: a flowing polygon answers to two streams — "
                         + "; ".join(f"{t['name']} {t['members']} -> {t['streams']} (rule {t['rule']})"
                                     for t in ties))

    # Unnamed polygons a slough/canal/channel both enters and leaves.
    taken = []
    for iid, it in sorted(out.items()):
        if it.kind != "stream" or head_noun(it.name) not in SLOUGHLIKE:
            continue
        mine = set(it.section_ids)
        extra = []
        for s in it.section_ids:
            for o in neighbours(s):
                n = nodes.get(o)
                if n is None or o in owner or o in mine or _kind(n) not in ("lake", "wetland"):
                    continue
                ins = {graph.edges[e].from_node for e in graph.up_adj.get(o, ())}
                outs = {graph.edges[e].to_node for e in graph.down_adj.get(o, ())}
                if ins & mine and outs & mine:
                    extra.append(o)
                    mine.add(o)
                    owner[o] = iid
        if extra:
            refs = {f"wbk:{nodes[o].wbk}" for o in extra if nodes[o].wbk}
            # F6 here too: the slough's lake boundaries onto the polygons it took are not
            # another water's edge any more (refused if a cut was aliased onto one)
            taken_wbks = {nodes[o].wbk for o in extra if nodes[o].wbk}
            bounds, lost = [], []
            for b in it.boundaries:
                if b.kind == "lake" and b.wbk in taken_wbks:
                    if b.aliases:
                        lost.append(b.id)
                    continue
                bounds.append(b)
            if lost:
                raise SystemExit(f"registry.flowing: {iid} ({it.name}) keeps a cut aliased onto "
                                 f"the edge of a polygon it takes: {lost} — give the cut its own "
                                 f"anchor")
            out[iid] = replace(it, section_ids=it.section_ids + tuple(sorted(extra)),
                               ref_ids=tuple(sorted(set(it.ref_ids) | refs)),
                               boundaries=tuple(bounds))
            for o in extra:
                if nodes[o].wbk:
                    absorbed_wbks[nodes[o].wbk] = iid
            taken.append({"id": iid, "name": it.name, "polygons": sorted(extra)})

    # F6, the other side: ANOTHER water's lake boundary onto a polygon that is now a river's is
    # the mouth of that water at the river — a confluence, labelled with the river.
    rewritten = []
    for iid, it in list(out.items()):
        if it.kind not in _WATER or not it.boundaries:
            continue
        own_wbks = _own_polygon_wbks(it.section_ids)
        changed = False
        bs = []
        for b in it.boundaries:
            if b.kind == "lake" and b.wbk in absorbed_wbks and b.wbk not in own_wbks:
                river = out[absorbed_wbks[b.wbk]]
                bs.append(replace(b, kind="confluence", label=river.name))
                rewritten.append({"item": iid, "boundary": b.id, "river": river.id})
                changed = True
            else:
                bs.append(b)
        if changed:
            out[iid] = replace(it, boundaries=tuple(bs))

    rep.update(merged=merged, own_items=own_items, threaded_by=threaded, unnamed_polygons=taken,
               boundaries_rewritten=rewritten,
               absorbed=sum(len(r["absorbed"]) for r in merged + own_items))
    return out


# --------------------------------------------------------------------------- absorbed ids
_ALIAS: dict[int, tuple[object, dict]] = {}


def alias_map(registry) -> dict[str, str]:
    """{absorbed item id -> the item that absorbed it}. Cached per registry object."""
    hit = _ALIAS.get(id(registry))
    if hit is not None and hit[0] is registry:
        return hit[1]
    m = {a: iid for iid, it in registry.items() for a in (getattr(it, "aliases", ()) or ())}
    _ALIAS[id(registry)] = (registry, m)
    return m


def canonical(iid: str, registry) -> str:
    """The item an id names today: itself, or the item that absorbed it."""
    if iid in registry:
        return iid
    return alias_map(registry).get(iid, iid)


#: The catalogue fields that NAME a registry item (an entry's `matched`; an extent's `item_id`,
#: `item_ids`, `outside_items`; a carve-out's `item_id`/`item_ids`; a DFO water's `item_ids`).
ID_KEYS = frozenset({"matched", "item_id", "item_ids", "outside_items"})


def canonical_ids(obj, registry):
    """`obj` (an entry, a rule, a record — any JSON shape) with every item id under `ID_KEYS`
    that names an ABSORBED item replaced by the item that absorbed it (duplicates dropped).
    Returns `obj` itself when nothing changes, so a registry with no merges costs one dict
    lookup per call."""
    amap = alias_map(registry)
    if not amap:
        return obj

    def fix(v):
        if isinstance(v, str):
            return amap.get(v, v) if v not in registry else v
        if isinstance(v, list):
            got = [fix(x) for x in v]
            return list(dict.fromkeys(got)) if got != v else v
        return v

    def walk(o):
        if isinstance(o, dict):
            new = None
            for k, v in o.items():
                nv = fix(v) if k in ID_KEYS else walk(v)
                if nv is not v:
                    if new is None:
                        new = dict(o)
                    new[k] = nv
            return o if new is None else new
        if isinstance(o, list):
            got = [walk(x) for x in o]
            return o if all(a is b for a, b in zip(got, o)) else got
        return o

    return walk(obj)


def absorbed_refs(entries, registry) -> list[str]:
    """Every place an entry names an ABSORBED item — `matched`, an extent's `item_id`/`item_ids`/
    `outside_items` (entry, rule and licensing), a carve-out — as one line each, naming the item
    that holds it now. Empty = clean. The refusal every reader applies that is not the one read
    point (`canonical_ids` at the corpus loader)."""
    amap = alias_map(registry)
    if not amap:
        return []
    out: list[str] = []

    def scan(o, where: str) -> None:
        if isinstance(o, dict):
            for k, v in o.items():
                if k in ID_KEYS:
                    for i in ([v] if isinstance(v, str) else list(v or ())):
                        if i in amap and i not in registry:
                            it = registry[amap[i]]
                            out.append(f"{where} {k}: {i} was folded into {amap[i]} "
                                       f"({it.name}) — name that item")
                else:
                    scan(v, where)
        elif isinstance(o, list):
            for x in o:
                scan(x, where)

    for e in entries:
        e = e[1] if isinstance(e, tuple) else e
        if isinstance(e, dict):
            scan(e, str(e.get("entry_id") or "?"))
    return out
