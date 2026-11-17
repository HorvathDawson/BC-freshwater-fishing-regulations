"""A LAKE'S LOOP IS THE LAKE'S, NOT THE RIVER'S (owner 2026-11-15, T753).

FWA draws some nameless secondary-flow lines that leave a lake and come back into the SAME lake — across a spit,
through a pond (Sproat Lake's narrows: blue line 354087683, 101 m + 66 m through the unnamed pond 329016376, 8.5 km
from the Sproat River). Such a line has no gnis of its own; it joins a river's item only because FWA gives it the
river's watershed code (`registry.build._effective_gnis`: an inherited side-channel name or `_wsc_gnis_map`), so the
angler in the narrows was answered with the lower Sproat River's rules. The same shape as the ditch rule
(`registry.build.DITCHES_LEAVE_RIVER_ITEMS`), one level up: the line is not its river.

THE CRITERIA (mechanical, name-agnostic; `loop_lines` decides per blue line, `move_lake_loops` per item):

  1. Every stream piece the graph holds on the blue line has NO NAME OF ITS OWN: no gnis, and every name tuple it
     carries is inherited (`NameSource.side_channel`) — the ditch rule's test (`registry.build.is_ditch`).
  2. Read off the graph's FLOW EDGES (not the section bounds: where FWA starts a blue line inside a lake the piece's
     lower bound is a natural end, `None` — Sproat's 354087683:898): every piece flows into exactly one node and out of
     exactly one node, and both are LAKE nodes; consecutive pieces (by route measure) meet in the same lake; the
     line's first piece flows into, and its last piece flows out of, the SAME lake X. So no stream joins the line and
     it meets no confluence and no cut (a cut joins two pieces stream to stream): it is lake water out and lake water
     back. No length limit and no distance test — the topology decides (a 2 km loop off a lake is still the lake's).
  3. The lake X decides where the pieces go, read off the registry as every earlier pass left it:
       - X is held by a LAKE or WETLAND item -> the pieces leave the river and JOIN THAT ITEM (its rules apply);
       - X is held by NO item (an unnamed lake) -> the pieces leave the river and are in no item (zone rules only);
       - X is held by a STREAM item (a flowing polygon or a widening pond folded into its river) -> LEFT: the loop
         is a braid of that river's own water.
     Every OTHER lake on the loop (a pond it passes through) must be held by no item or by X's own item; one held by
     another water -> LEFT (which water's loop it is cannot be decided).
  4. A stream item made of nothing but such pieces keeps them (its id would vanish; the ditch rule's exception).
  5. A LOOP NEVER DIVIDES ITS LAKE (Kotcho Lake, 2026-11-15; `LOOPS_NEVER_DIVIDE_A_LAKE`). A lake is never cut, so it
     holds every area it touches (its outline test) while a line is in an area only where it lies inside it. A loop
     that JOINS a lake item must therefore be in exactly the lake's CUT AREAS (`node.in_areas`, every area whose
     `areas.json` def cuts lines at its edge — `cut: clean | first_last` — and any curated area-boundary label; the
     lake's `outside_items` taken out of both sides): Kotcho Lake's 51.8 m loop lies outside the Kotcho Lake
     Ecological Reserve the lake's polygon is in, so joining would give the lake two answers ("No fishing in
     ecological reserves" on the polygon only) — a lake divided into areas, which this repo defers (lake splits are
     not made). Such a loop STAYS with its river (`AREAS_DIFFER`, listed with both area sets). A MEMBERSHIP-ONLY area
     (`cut: false`: provincial parks, WMAs, watersheds, indigenous and permit land) is not a place where a line's
     answer changes — the build never cuts there — so a difference in one does not hold the loop; it is listed on
     the moved row (`membership_only_areas_differ`: Sproat Lake's two shore parks) so it is never silent. The test
     reads the area flags, so it runs only where they are stamped (`area_defs` given): the registry built before the
     shore cuts (`atlas.build.water_registry(areas=False)`, read only for the tidal answer) has none, decides by
     topology alone and says so (`areas_checked: false`).

THE PONDS STAY AS THEY ARE: a pond the loop passes through is its own (unnamed) water; only the line pieces move.
Moving a polygon into another lake would change the rules on the pond itself, which the owner has not decided.

Nothing is silent: `move_lake_loops` reports every piece it moved (the water it left, the lake it joined, length,
ids) and every NEAR MISS — a piece of a stream item with no gnis of its own that flows out of a lake and into a lake
(`touches_lakes_both_ends`, whatever else it meets) whose line does not
qualify — with the reason. The atlas build writes the report as `lake_loop_report.json`.

The river keeps every boundary it still stands on: a boundary only a moved piece carried (the pond's edge) is dropped,
and the river's `blk:` ref for the moved line goes. No boundary id is renumbered (ids are minted before this pass) and
no section id changes (nothing is cut). The lake item gains the pieces' sections only — never their boundaries, names
or `blk:`/`wsc:` refs (a lake answers to its wbk, `registry.build._ref_ids`).

IDEMPOTENT: on a registry that already holds the move (the sidecar step reloads registry.json), no stream item holds a
loop piece, so nothing moves.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import replace

from pipeline.common.models import NameSource, NodeKind, RegistryItem

#: POLICY (owner 2026-11-15, T753, option b): a nameless line that leaves a lake and comes back into the same lake is
#: not its river's. False is the mutation pin (nothing moves; the report says so).
LAKE_LOOPS_LEAVE_RIVER_ITEMS = True
#: POLICY (owner 2026-11-15, T753, option a): ...and it is the lake's, so the lake's rules apply on it. False is the
#: mutation pin: the pieces still leave the river (option b alone) and join no item.
LAKE_LOOPS_JOIN_THEIR_LAKE = True
#: POLICY (2026-11-15, T753 follow-up on Kotcho Lake; owner: "make sure it is done in a clean way"): a loop joins its
#: lake only when it is in exactly the lake's cut areas (criterion 5), so a lake never takes a second answer. False is
#: the mutation pin: Kotcho's loop joins its lake again (and the answers step refuses the two-part lake).
LOOPS_NEVER_DIVIDE_A_LAKE = True

_STILL = ("lake", "wetland")

# near-miss reasons (a nameless lake-to-lake piece of a stream item that does not move)
NOT_ALL_BETWEEN_LAKES = "the line leaves the lakes (a piece of it does not flow from a lake into a lake)"
A_STREAM_JOINS = ("another stream joins the line (flows into or out of it) or it meets two lakes at one end: the "
                  "joining stream's receiving water would change")
TWO_LAKES = "the line starts and ends in two different lakes"
OWN_NAME = "a piece of the line has a name of its own"
CHAIN_BROKEN = "consecutive pieces of the line do not meet in the same lake"
LAKE_IS_A_STREAM = "its lake is held by a stream item (the river's own polygon or widening): a braid of that river"
OTHER_WATER_ON_LOOP = "a lake the loop passes through is another water's"
ITEM_WOULD_VANISH = "the stream item has no other piece (its id would vanish)"
AREAS_DIFFER = ("the loop's line is not in the same cut areas as its lake: joining would divide the lake into two "
                "answers (a lake divided into areas is deferred), so it stays with its river")


def membership_only_prefixes(area_defs) -> tuple[str, ...]:
    """The `in_areas` label prefixes (`area:<kind>:`) of every MEMBERSHIP-ONLY area def (`cut: false`): the areas whose
    edge the build never cuts a line at (criterion 5)."""
    from pipeline.atlas.splits.area_catalog import area_id
    from pipeline.atlas.splits.area_splits import cut_mode
    return tuple(sorted({area_id(ad.get("kind", ad["id"]), "x")[:-1] for ad in area_defs or () if cut_mode(ad) is None}))


def outside_of(area_defs) -> dict[str, set[str]]:
    """{item id: the area ids `areas.json` `outside_items` puts it outside} (`registry.outside_area_items`)."""
    out: dict[str, set[str]] = defaultdict(set)
    for ad in area_defs or ():
        for slug, items in (ad.get("outside_items") or {}).items():
            for it in items:
                out[it].add(f"area:{ad['id']}:{slug}")
    return out


def area_check(graph, lake_node: str, pieces, lake_item: str, area_defs) -> dict:
    """Criterion 5 for one loop: {"lake_areas": the lake's cut areas, "loop_areas": {piece: its cut areas} for every
    piece whose cut areas differ from the lake's, "membership_only_areas_differ": the membership-only areas the lake
    and any piece do not share}. The loop may join only when `loop_areas` is empty."""
    mo = membership_only_prefixes(area_defs)
    gone = outside_of(area_defs).get(lake_item, set())
    lake = set(graph.nodes[lake_node].in_areas) - gone
    cut_lake = {a for a in lake if not a.startswith(mo)}
    differ: dict[str, list[str]] = {}
    mo_differ: set[str] = set()
    for p in pieces:
        mine = set(graph.nodes[p].in_areas) - gone
        if {a for a in mine if not a.startswith(mo)} != cut_lake:
            differ[p] = sorted(a for a in mine if not a.startswith(mo))
        mo_differ |= {a for a in lake ^ mine if a.startswith(mo)}
    return {"lake_areas": sorted(cut_lake), "loop_areas": differ, "membership_only_areas_differ": sorted(mo_differ)}


def _neighbours(graph, nid: str) -> tuple[set[str], set[str]]:
    return ({graph.edges[i].to_node for i in graph.down_adj.get(nid, ())},
            {graph.edges[i].from_node for i in graph.up_adj.get(nid, ())})


def _is_lake(graph, nid: str) -> bool:
    n = graph.nodes.get(nid)
    return n is not None and n.kind == NodeKind.lake and bool(n.wbk)


def touches_lakes_both_ends(graph, nid: str) -> bool:
    """Does the piece flow into a lake and out of a lake (whatever else it meets)? The candidates the report lists."""
    down, up = _neighbours(graph, nid)
    return any(_is_lake(graph, x) for x in down) and any(_is_lake(graph, x) for x in up)


def lake_ends(graph, nid: str) -> tuple[str, str] | None:
    """(the lake the piece flows into, the lake it flows out of) as wbks, read off the flow edges — or None unless the
    piece has exactly one node below and one above it and both are lake nodes."""
    down, up = _neighbours(graph, nid)
    if len(down) != 1 or len(up) != 1:
        return None
    (d,), (u,) = down, up
    nd, nu = graph.nodes.get(d), graph.nodes.get(u)
    if nd is None or nu is None or nd.kind != NodeKind.lake or nu.kind != NodeKind.lake or not nd.wbk or not nu.wbk:
        return None
    return nd.wbk, nu.wbk


def borrowed_names_only(n) -> bool:
    """No gnis of its own and every name it carries inherited from its river (the ditch rule's name test)."""
    return not n.gnis_id and all(t.source == NameSource.side_channel for t in n.name_tuples)


def loop_lines(graph, blks=None) -> dict[str, dict]:
    """{blk: verdict} for every blue line with at least one stream piece that touches lakes at both ends (or only the
    `blks` given). A verdict is {"loop": True, "lake": X, "lakes": [every lake on the chain, in order], "pieces":
    [node ids by measure]} or {"loop": False, "why": <reason>, "pieces": [...]}."""
    by_blk: dict[str, list] = defaultdict(list)
    for n in graph.nodes.values():
        if n.kind == NodeKind.stream and n.blk and (blks is None or n.blk in blks):
            by_blk[n.blk].append(n)
    out: dict[str, dict] = {}
    for blk, ns in sorted(by_blk.items()):
        if not any(touches_lakes_both_ends(graph, n.node_id) for n in ns):
            continue
        ends = {n.node_id: lake_ends(graph, n.node_id) for n in ns}
        ns.sort(key=lambda n: (n.down_m, n.node_id))
        pieces = [n.node_id for n in ns]
        if not all(borrowed_names_only(n) for n in ns):
            out[blk] = {"loop": False, "why": OWN_NAME, "pieces": pieces}
            continue
        if not all(ends.values()):
            joined = all(touches_lakes_both_ends(graph, n.node_id) for n in ns)
            out[blk] = {"loop": False, "why": A_STREAM_JOINS if joined else NOT_ALL_BETWEEN_LAKES, "pieces": pieces}
            continue
        chain = [ends[n.node_id] for n in ns]                  # (into, out of) per piece, mouth first
        if any(a[1] != b[0] for a, b in zip(chain, chain[1:])):
            out[blk] = {"loop": False, "why": CHAIN_BROKEN, "pieces": pieces}
            continue
        first, last = chain[0][0], chain[-1][1]
        if first != last:
            out[blk] = {"loop": False, "why": TWO_LAKES, "pieces": pieces, "lakes": [first, last]}
            continue
        out[blk] = {"loop": True, "lake": first, "lakes": [first] + [c[1] for c in chain], "pieces": pieces}
    return out


def move_lake_loops(registry: dict[str, RegistryItem], graph, report: dict | None = None, *,
                    area_defs: list[dict] | None = None) -> dict[str, RegistryItem]:
    """Take every lake-loop piece (`loop_lines`) out of the stream item holding it and give it to its lake's item
    (module doc). `area_defs` (`areas.json` `areas`) is criterion 5's input; None = the area flags are not stamped yet
    (the pre-shore registry): topology alone, reported as `areas_checked: false`. Returns a NEW dict; fills `report`
    with `moved`, `not_moved`, `counts` and `areas_checked`."""
    rep = report if report is not None else {}
    nodes = graph.nodes
    owner: dict[str, str] = {}
    for iid, it in sorted(registry.items()):
        if it.kind in ("stream", *_STILL):
            for s in it.section_ids:
                owner.setdefault(s, iid)
    # the blue lines with a nameless lake-to-lake piece in a stream item: the candidates
    cand_blks = {nodes[s].blk for s, iid in owner.items()
                 if registry[iid].kind == "stream" and s in nodes and nodes[s].kind == NodeKind.stream
                 and not nodes[s].gnis_id and touches_lakes_both_ends(graph, s)}
    verdicts = loop_lines(graph, cand_blks)

    def piece_rows(pieces):
        return [{"section_id": p, "blk": nodes[p].blk, "down_m": round(nodes[p].down_m, 1),
                 "up_m": round(nodes[p].up_m, 1), "length_m": round(nodes[p].length_m, 1),
                 "item": owner.get(p, "")} for p in pieces]

    moved: list[dict] = []
    not_moved: list[dict] = []
    take: dict[str, set[str]] = defaultdict(set)          # stream item -> pieces leaving it
    give: dict[str, set[str]] = defaultdict(set)          # lake item -> pieces joining it
    row_of: dict[str, dict] = {}
    for blk, v in sorted(verdicts.items()):
        in_streams = [p for p in v["pieces"] if p in owner and registry[owner[p]].kind == "stream"]
        if not in_streams:
            continue
        base = {"blk": blk, "pieces": piece_rows(v["pieces"]), "moved_sections": in_streams,
                "left_items": sorted({owner[p] for p in in_streams}),
                "left_names": sorted({registry[owner[p]].name for p in in_streams}),
                "length_m": round(sum(nodes[p].length_m for p in in_streams), 1)}
        if not v["loop"]:
            not_moved.append({**base, "why": v["why"], **({"lakes": v["lakes"]} if "lakes" in v else {})})
            continue
        x = v["lake"]
        lake_item = owner.get(f"lake:{x}", "")
        base.update(lake_wbk=x, lakes_on_loop=v["lakes"], lake_item=lake_item,
                    lake_name=registry[lake_item].name if lake_item else "")
        if lake_item and registry[lake_item].kind == "stream":
            not_moved.append({**base, "why": LAKE_IS_A_STREAM})
            continue
        others = sorted({owner[f"lake:{w}"] for w in v["lakes"] if owner.get(f"lake:{w}", lake_item) != lake_item})
        if others:
            not_moved.append({**base, "why": OTHER_WATER_ON_LOOP, "other_items": others})
            continue
        join = lake_item if (lake_item and LAKE_LOOPS_JOIN_THEIR_LAKE) else ""
        if join and area_defs is not None:                       # criterion 5: never divide the lake
            chk = area_check(graph, f"lake:{x}", in_streams, join, area_defs)
            if chk["loop_areas"] and LOOPS_NEVER_DIVIDE_A_LAKE:
                not_moved.append({**base, "why": AREAS_DIFFER, **chk})
                continue
            if chk["membership_only_areas_differ"]:
                base["membership_only_areas_differ"] = chk["membership_only_areas_differ"]
        row = {**base, "joined": join, "joined_name": registry[join].name if join else ""}
        for p in in_streams:
            take[owner[p]].add(p)
            if join:
                give[join].add(p)
        row_of[blk] = row
        moved.append(row)

    # an item that would be left with nothing keeps its pieces (its id would vanish)
    for iid in sorted(take):
        if set(registry[iid].section_ids) <= take[iid]:
            for row in [r for r in moved if iid in r["left_items"]]:
                moved.remove(row)
                not_moved.append({k: v for k, v in row.items() if k not in ("joined", "joined_name")}
                                 | {"why": ITEM_WOULD_VANISH})
    keep_rows = {r["blk"] for r in moved}
    take = {iid: {p for p in ps if nodes[p].blk in keep_rows} for iid, ps in take.items()}
    give = {iid: {p for p in ps if nodes[p].blk in keep_rows} for iid, ps in give.items()}

    out = dict(registry)
    if LAKE_LOOPS_LEAVE_RIVER_ITEMS:
        for iid, ps in sorted(take.items()):
            if not ps:
                continue
            it = out[iid]
            rest = [s for s in it.section_ids if s not in ps]
            kept_refs = {b.boundary_id for s in rest if s in nodes
                         for b in (nodes[s].lower_bound, nodes[s].upper_bound) if b is not None}
            gone_refs = {b.boundary_id for s in ps
                         for b in (nodes[s].lower_bound, nodes[s].upper_bound) if b is not None} - kept_refs
            kept_blks = {nodes[s].blk for s in rest if s in nodes and nodes[s].blk}
            gone_blks = {f"blk:{nodes[s].blk}" for s in ps} - {f"blk:{b}" for b in kept_blks}
            out[iid] = replace(it, section_ids=tuple(rest),
                               boundaries=tuple(b for b in it.boundaries if b.ref not in gone_refs),
                               ref_ids=tuple(r for r in it.ref_ids if r not in gone_blks))
            for r in moved:
                if iid in r["left_items"]:
                    mine = {b.boundary_id for s in r["moved_sections"]
                            for b in (nodes[s].lower_bound, nodes[s].upper_bound) if b is not None}
                    r.setdefault("boundaries_dropped", {})[iid] = [b.id for b in it.boundaries
                                                                   if b.ref in gone_refs and b.ref in mine]
        for iid, ps in sorted(give.items()):
            if ps:
                it = out[iid]
                out[iid] = replace(it, section_ids=it.section_ids + tuple(sorted(ps - set(it.section_ids))))
    else:
        for r in moved:
            not_moved.append({**{k: v for k, v in r.items() if k not in ("joined", "joined_name")},
                              "why": "LAKE_LOOPS_LEAVE_RIVER_ITEMS is off"})
        moved = []
    rep.update(moved=moved, not_moved=sorted(not_moved, key=lambda r: (r["why"], r["blk"])),
               areas_checked=area_defs is not None,
               counts={"lines_moved": len(moved),
                       "pieces_moved": sum(len(r["moved_sections"]) for r in moved),
                       "items_left": len({i for r in moved for i in r["left_items"]}),
                       "joined_a_lake": sum(1 for r in moved if r["joined"]),
                       "joined_no_item": sum(1 for r in moved if not r["joined"]),
                       "metres_moved": round(sum(r["length_m"] for r in moved), 1),
                       "not_moved": len(not_moved)})
    return out
