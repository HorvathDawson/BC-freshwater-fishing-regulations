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
from pipeline.common.names import key
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


def _union_by_id(bounds) -> list:
    """The folded parts' boundaries, once per id — a cut-point two parts both carry keeps EVERY alias and
    name either copy holds (a `{b.id: b}` fold kept the last copy and dropped the first's aliases)."""
    out: dict = {}
    for b in bounds:
        prev = out.get(b.id)
        if prev is None:
            out[b.id] = b
            continue
        al = tuple(dict.fromkeys((*b.aliases, *prev.aliases)))
        nm = tuple(sorted(set(prev.names) | set(b.names)))
        # the last copy stands (as the old fold had it), carrying the first's ids and names too
        out[b.id] = b if (al == b.aliases and nm == b.names) else replace(b, aliases=al, names=nm)
    return list(out.values())


def _norm(s: str) -> str:
    """THE name key (`pipeline.common.names.key`, T61 2.8)."""
    return key(s)


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


def folding_polygon_wbks(graph) -> frozenset[str]:
    """The wbks of the polygons `merge_flowing_polygons` will join to a STREAM by rule 1 or 2, read off
    the GRAPH — for the split placement, which runs before any registry exists.

    Such a polygon is part of its river (F6), not another water: a curated cut that lands in it is a
    cut on the river, so it must not be aliased onto the polygon's "lake" edge (the fold drops that
    edge and refuses the alias: Cayoosh Falls in wbk 329505890, owner 2026-10-25). The test mirrors the
    fold on the names the graph already carries (`apply_name_variants` runs before the splits): a lake
    or wetland node whose display name FLOWS and that carries the gnis of a stream piece (rule 1), or
    touches — through at most `MAX_GAP` unnamed line pieces — a stream piece answering to the same
    name (rule 2)."""
    nodes = graph.nodes
    stream_gnis = {n.gnis_id for n in nodes.values() if _kind(n) == "stream" and n.gnis_id}

    def piece_names(n) -> set[str]:
        return {_norm(x) for x in (n.display_name, *(t.name for t in n.name_tuples)) if x}

    def neighbours(sid: str):
        for ei in graph.up_adj.get(sid, ()):
            yield graph.edges[ei].from_node
        for ei in graph.down_adj.get(sid, ()):
            yield graph.edges[ei].to_node

    out: set[str] = set()
    for sid, n in nodes.items():
        if _kind(n) not in ("lake", "wetland") or not n.wbk or not n.display_name \
                or not flows(_kind(n), n.display_name):
            continue
        gn = {n.gnis_id, *(t.gnis_id for t in n.name_tuples)} - {""}
        if gn & stream_gnis:
            out.add(n.wbk)                                   # rule 1
            continue
        want = _norm(n.display_name)
        q = deque([(sid, 0)])
        seen = {sid}
        while q:
            s, gap = q.popleft()
            hit = False
            for o in neighbours(s):
                m = nodes.get(o)
                if o in seen or m is None or _kind(m) != "stream":
                    continue
                seen.add(o)
                names = piece_names(m)
                if want in names:
                    hit = True                               # rule 2
                    break
                if not names and gap < MAX_GAP:
                    q.append((o, gap + 1))
            if hit:
                out.add(n.wbk)
                break
    return frozenset(out)


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
        for b in _union_by_id(b for p in parts for b in p.boundaries):
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
            # T224: each name keeps the source it came with (the head's first, then the parts')
            name_sources=tuple({r.name: r for p in reversed(parts) for r in p.name_sources}[n]
                               for n in dict.fromkeys(r.name for p in parts for r in p.name_sources)),
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


# --------------------------------------------------------------------------- T322: owner-confirmed sloughs
#: T322: fold the owner-CONFIRMED slough / canal polygons (`data/curated/waters/slough_calls.json`) in the atlas build
#: and the sidecars. OFF until T322's own side build (after the 10-31 batch rebuild, which must not carry it): turn it
#: on in that build's commit; the binding diff is reviewed before promote.
FOLD_CONFIRMED = True


def fold_confirmed_sloughs(registry: dict[str, RegistryItem], graph, wbks, report: dict | None = None
                           ) -> dict[str, RegistryItem]:
    """THE OWNER'S CONFIRMED CALLS (T322, owner 2026-10-30). Runs AFTER `merge_flowing_polygons`; returns a NEW dict.

    Each confirmed wbk is a polygon node (`lake:<wbk>`, lake or wetland). Confirmed polygons joined to each other by a
    graph edge are ONE run (a slough drawn as several polygons). Owner rule (2026-10-30, corrected): ONLY AN ISOLATED
    polygon is skipped; every other one is a stream:
        through      ONE stream item flows in AND out of the run -> joins it
        one side     a stream item only enters it, or only leaves it (and no other named stream touches it) -> joins it
        unnamed line only unnamed lines touch it -> the lake / wetland item that holds it becomes kind `stream` under its
                     own id (it joins the unnamed line; nothing new is made, the line stays unnamed)
        isolated item  no line touches it but a lake / wetland item holds it (the 31 OKANAGAN RIVER OXBOWS, owner
                     2026-10-30: "general streams") -> that item is re-kinded `stream` in place: zone / region stream
                     rules reach it, no named river's do, nothing new is made
        own name     the item holding it carries ANOTHER name than the stream it touches (owner 2026-11-03: the oxbow
                     on Park Rill) -> re-kinded `stream` in place, never absorbed, so its rules stay on it
    Joining a stream: the polygons become its sections and a lake / wetland item that held them is absorbed (an alias,
    as the flowing fold does). Everything else is left exactly as it was, and listed with why:
        isolated               no line touches it and no item holds it (skipped by the rule)
        not in the graph       no polygon node carries the wbk (drawn over a line, or pruned)
        already stream         a stream item owns it (the flowing fold or an earlier build took it)
        two streams            different named items in and out, two through, or two on its one side (never picked)
        unnamed, no item       only unnamed lines touch it and no item holds it: nothing to make a stream without
                               inventing an item, so it is HELD for the owner
        item not confirmed     a lake item holding it has polygons the owner did not confirm (never split)
        cut on its edge        a curated cut is aliased onto a lake edge the fold would drop (give it an anchor)
    F6 as in the flowing fold: the stream's own lake boundaries onto a folded polygon are dropped, and ANOTHER
    water's lake boundary onto a polygon that JOINED a stream becomes a `confluence` labelled with the stream."""
    rep = report if report is not None else {}
    nodes = graph.nodes
    wbks = {str(w) for w in wbks}
    owner: dict[str, str] = {}
    for iid, it in registry.items():
        if it.kind in _WATER:
            for s in it.section_ids:
                owner.setdefault(s, iid)
    poly_of: dict[str, list[str]] = defaultdict(list)
    for nid, n in nodes.items():
        if n.wbk in wbks and _kind(n) in ("lake", "wetland"):
            poly_of[n.wbk].append(nid)

    def kind_of_owner(sid: str) -> str:
        o = owner.get(sid)
        return registry[o].kind if o else ""

    rows: list[dict] = []
    cand: dict[str, str] = {}                               # polygon node -> its wbk
    for w in sorted(wbks, key=lambda x: (_wbk_num(f"wbk:{x}"), x)):
        ns = sorted(poly_of.get(w, ()))
        if not ns:
            rows.append({"wbk": w, "outcome": "ignored", "why": "not in the graph"})
            continue
        st = sorted({owner[s] for s in ns if kind_of_owner(s) == "stream"})
        if st:
            rows.append({"wbk": w, "outcome": "already stream", "stream": st[0], "name": registry[st[0]].name})
            continue
        for s in ns:
            cand[s] = w

    # runs: confirmed polygons joined by an edge
    parent = {s: s for s in cand}

    def find(s):
        while parent[s] != s:
            parent[s] = parent[parent[s]]
            s = parent[s]
        return s
    for e in graph.edges:
        if e.from_node in cand and e.to_node in cand:
            parent[find(e.from_node)] = find(e.to_node)
    runs: dict[str, list[str]] = defaultdict(list)
    for s in cand:
        runs[find(s)].append(s)

    out = dict(registry)
    folded_wbks: dict[str, str] = {}                        # wbk -> the stream it joined
    joined: dict[str, dict] = {}                            # stream id -> {secs, items}
    streams_own: list[str] = []                             # lake items made streams under their own id
    for members in sorted(runs.values(), key=lambda ms: sorted(ms)[0]):
        members.sort()
        mset = set(members)
        run_wbks = sorted({cand[s] for s in members}, key=lambda x: (_wbk_num(f"wbk:{x}"), x))
        ins, outs, un_in, un_out = set(), set(), 0, 0
        for s in members:
            for ei in graph.up_adj.get(s, ()):
                o = graph.edges[ei].from_node
                if o in mset:
                    continue
                if kind_of_owner(o) == "stream":
                    ins.add(owner[o])
                elif o not in owner and _kind(nodes.get(o)) == "stream":
                    un_in += 1
            for ei in graph.down_adj.get(s, ()):
                o = graph.edges[ei].to_node
                if o in mset:
                    continue
                if kind_of_owner(o) == "stream":
                    outs.add(owner[o])
                elif o not in owner and _kind(nodes.get(o)) == "stream":
                    un_out += 1
        thru = ins & outs
        base = {"wbks": run_wbks, "in": sorted(ins), "out": sorted(outs)}
        items = sorted({owner[s] for s in members if s in owner})
        why, via, tgt = "", "", ""
        if len(thru) == 1:
            via, tgt = "through", next(iter(thru))
        elif len(thru) > 1:
            why = "two streams through it"
        elif ins and outs:
            why = "two streams (different in and out)"
        elif ins or outs:
            side = ins | outs
            if len(side) == 1:
                via, tgt = ("ends in it" if ins else "leaves it"), next(iter(side))
            else:
                why = "two streams (" + ("both end in it)" if ins else "both leave it)")
        elif un_in or un_out:
            if items and all(m in owner for m in members):
                via = "unnamed line"
            else:
                why = "unnamed, no item (held: only unnamed lines touch it and no item holds it)"
        elif items and all(m in owner for m in members):
            via = "isolated item"                    # owner 10-30: the oxbows are general streams, cut off or not
        else:
            why = "isolated (no line touches it)"
        if not why:
            loose = [i for i in items if not set(registry[i].section_ids) <= mset]
            if loose:
                why = "item not confirmed: " + ", ".join(f"{i} ({registry[i].name})" for i in loose)
        if not why:
            mine = {cand[s] for s in members}
            holders = [out[tgt]] if tgt else []
            lost = [b.id for p in [*holders, *(registry[i] for i in items)] for b in p.boundaries
                    if b.kind == "lake" and b.wbk in mine and b.aliases]
            if lost:
                why = f"cut on its edge: {sorted(set(lost))}"
        if not why and tgt and items:
            # owner 2026-11-03: a water of ANOTHER name is never absorbed (an Okanagan River Oxbow touching Park Rill:
            # the oxbows' rule would spread over the rill) -> it is a stream in place, as an isolated item is
            names = {key(n) for n in (registry[tgt].name, *registry[tgt].variants)}
            if any(key(registry[i].name) not in names for i in items):
                if all(m in owner for m in members):
                    via, tgt = f"own name (touches {tgt})", ""
                else:
                    why = f"own name, partly unheld (touches {tgt})"
        if not why and not tgt:                      # unnamed line / isolated item: each item holding it is a stream now
            for i in items:
                it = out[i]
                own = _own_polygon_wbks(it.section_ids)
                out[i] = replace(it, kind="stream", boundaries=tuple(
                    b for b in it.boundaries if not (b.kind == "lake" and b.wbk in own)))
            for w in run_wbks:
                rows.append({"wbk": w, "outcome": "folded", "via": via, "stream": items[0] if len(items) == 1
                             else items, "name": registry[items[0]].name, "absorbed": [], **base})
            streams_own.extend(i for i in items if i not in streams_own)
            continue
        if why:
            for w in run_wbks:
                rows.append({"wbk": w, "outcome": "ignored", "why": why, **base})
            continue
        j = joined.setdefault(tgt, {"secs": [], "items": []})
        j["secs"] += members
        j["items"] += [i for i in items if i not in j["items"]]
        for w in run_wbks:
            folded_wbks[w] = tgt
            rows.append({"wbk": w, "outcome": "folded", "via": via, "stream": tgt, "name": registry[tgt].name,
                         "absorbed": items, **base})

    for tgt, j in sorted(joined.items()):
        parts = [out[tgt]] + [registry[i] for i in j["items"]]
        head = parts[0]
        secs = list(dict.fromkeys([*head.section_ids, *(s for p in parts[1:] for s in p.section_ids),
                                   *sorted(j["secs"])]))
        own = _own_polygon_wbks(secs)
        bounds = [b for b in _union_by_id(b for p in parts for b in p.boundaries)
                  if not (b.kind == "lake" and b.wbk in own)]          # F6: not another water
        refs = {f"wbk:{nodes[s].wbk}" for s in j["secs"] if nodes[s].wbk}
        out[tgt] = replace(
            head, section_ids=tuple(secs), boundaries=tuple(bounds),
            variants=tuple(sorted({v for p in parts for v in p.variants})),
            name_sources=tuple({r.name: r for p in reversed(parts) for r in p.name_sources}[n]
                               for n in dict.fromkeys(r.name for p in parts for r in p.name_sources)),
            mus=tuple(sorted({m for p in parts for m in p.mus})),
            ref_ids=tuple(sorted({r for p in parts for r in p.ref_ids} | refs | set(j["items"]))),
            aliases=tuple(sorted({a for p in parts for a in p.aliases} | set(j["items"]))),
        )
        for i in j["items"]:
            out.pop(i, None)

    rewritten = []
    for iid, it in list(out.items()):
        if it.kind not in _WATER or not it.boundaries:
            continue
        own = _own_polygon_wbks(it.section_ids)
        bs, changed = [], False
        for b in it.boundaries:
            if b.kind == "lake" and b.wbk in folded_wbks and b.wbk not in own:
                river = out[folded_wbks[b.wbk]]
                bs.append(replace(b, kind="confluence", label=river.name))
                rewritten.append({"item": iid, "boundary": b.id, "river": river.id})
                changed = True
            else:
                bs.append(b)
        if changed:
            out[iid] = replace(it, boundaries=tuple(bs))

    count: dict[str, int] = defaultdict(int)
    for r in rows:
        count[r["outcome"]] += 1
    rep.update(confirmed=len(wbks), counts=dict(sorted(count.items())), rows=rows,
               streams={t: {"name": out[t].name, "polygons": len(j["secs"]), "absorbed": j["items"]}
                        for t, j in sorted(joined.items())},
               own_streams={i: {"name": out[i].name, "polygons": len(out[i].section_ids)}
                            for i in sorted(streams_own)},
               boundaries_rewritten=rewritten)
    return out


# --------------------------------------------------------------------------- T679 #5: a river's widening ponds
#: POLICY (T679 #5, owner D1 / Q154, answered 2026-11-11, built at the atlas 2026-11-12): A RIVER'S UNNAMED WIDENING
#: POND IS THE RIVER, for every rule ("in streams" rules too). The pond is folded into the stream item whose main flow
#: runs through it (`graph.widenings`, over the item's whole course), so `item.kind` (stream) is its water kind
#: everywhere — the reach's kind filters, the bundle's rule keys, the tiles. The reach-only version (085b352ca) was
#: reverted (09da1b3a7): the bundle still read the pond as a lake and 161 rule keys mixed the two kinds. False = the pond
#: stays a lake (the reach's per-rule widening still takes it for whole/cut extents: T153).
WIDENING_PONDS_ARE_THE_RIVER = True


def fold_widening_ponds(registry: dict[str, RegistryItem], graph, report: dict | None = None
                         ) -> dict[str, RegistryItem]:
    """Fold every unnamed widening pond into the ONE stream item whose main flow runs through it (`graph.widenings`
    over the item's whole course). Runs after the flowing and confirmed-slough folds; returns a NEW dict.

    The pond becomes a section of the item (its `wbk:` joins `ref_ids`), and the item's own `lake` boundary onto the
    pond is dropped (F6: "the river stops here, another water begins" is false of its own widening). LEFT AS IT WAS,
    and listed with why (never picked, never silent):
        two streams    the main flow of two stream items runs through it
        cut on its edge  a cut (a gauge, a sign) is aliased onto the item's lake boundary onto the pond: folding would
                       drop the boundary the cut stands on (the reach's per-rule widening still takes the pond, T153)
    No other water's boundary can point at such a pond (it is unnamed and held by none), so nothing is relabelled."""
    from pipeline.atlas.graph import widenings as _widen
    rep = report if report is not None else {}
    if not WIDENING_PONDS_ARE_THE_RIVER:
        rep.update(folded=[], held=[], counts={"ponds": 0, "items": 0, "held": 0})
        return dict(registry)
    nodes = graph.nodes
    held: set[str] = set()
    for it in registry.values():
        if it.kind in _WATER:
            held.update(it.section_ids)

    def is_open(nid: str) -> bool:
        return _widen.unnamed(graph, nid) and nid not in held

    takers: dict[str, list[str]] = defaultdict(list)
    for iid, it in sorted(registry.items()):
        if it.kind != "stream" or not it.section_ids:
            continue
        mine = set(it.section_ids)
        for p in sorted(_widen.widenings(graph, mine, lambda n, m=mine: is_open(n) and n not in m)):
            takers[p].append(iid)
    out = dict(registry)
    left: list[dict] = []
    by_item: dict[str, list[str]] = defaultdict(list)
    for p, iids in sorted(takers.items()):
        if len(iids) > 1:
            left.append({"pond": p, "why": "two streams", "items": iids})
            continue
        by_item[iids[0]].append(p)
    folded = []
    for iid, ponds in sorted(by_item.items()):
        it = out[iid]
        wbks = {nodes[p].wbk: p for p in ponds if nodes[p].wbk}
        keep_bounds, take = [], []
        aliased = {b.wbk for b in it.boundaries if b.kind == "lake" and b.wbk in wbks and b.aliases}
        for p in ponds:
            w = nodes[p].wbk
            if w and w in aliased:
                left.append({"pond": p, "why": "cut on its edge", "items": [iid],
                             "aliases": sorted(a for b in it.boundaries if b.kind == "lake" and b.wbk == w
                                               for a in b.aliases)})
            else:
                take.append(p)
        if not take:
            continue
        taken_wbks = {nodes[p].wbk for p in take if nodes[p].wbk}
        for b in it.boundaries:
            if b.kind == "lake" and b.wbk in taken_wbks:
                continue
            keep_bounds.append(b)
        out[iid] = replace(it, section_ids=it.section_ids + tuple(sorted(take)),
                           ref_ids=tuple(sorted(set(it.ref_ids) | {f"wbk:{w}" for w in taken_wbks})),
                           boundaries=tuple(keep_bounds))
        folded.append({"id": iid, "name": it.name, "ponds": sorted(take)})
    rep.update(folded=folded, held=left,
               counts={"ponds": sum(len(f["ponds"]) for f in folded), "items": len(folded), "held": len(left)})
    # NOT FISHING WATER IS IN NO WATER (T152): the build stops here, naming each, rather than the bundle's part check
    # 30 minutes into a release (2026-11-12: four sewage basins / tailing ponds folded into creeks)
    bad = sorted((iid, s) for iid, it in out.items() if it.kind in _WATER for s in it.section_ids
                 if getattr(nodes.get(s), "not_fishing", ""))
    if bad:
        raise ValueError(f"fold_widening_ponds: {len(bad)} not-fishing polygon(s) held by a water item "
                         f"(T152: none is an item): {bad[:10]}")
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
