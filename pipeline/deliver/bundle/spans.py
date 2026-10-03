"""WHERE A STRETCH OF A WATER RUNS: the bundle's `section_span` and `split` tables, and the one
function that turns a part's sections into runs.

THE QUESTION. The UI export groups a water's sections into `parts` (every section carrying the
same rule set and licensing set), and a page showing "closed above the Slesse Creek boundary
signs" has to say WHERE each part runs. The pipeline already knows: the sectionizer cut the river
at exactly those points and wrote each piece's two ends on the graph node (`StreamNode.lower_bound`
/ `upper_bound` — a curated split, a lake, an area or region line, the border, or nothing: the
natural mouth or source). This module REPORTS that; it infers nothing new and cuts nothing.

WHY IT IS IN THE BUNDLE. The graph is an atlas artifact and a reader may open only the bundle
(memory: bundle-is-the-only-source), so the build writes down, per section of a named STREAM, the
two facts a run needs — where it lies along the water's main stem and what ends it — and the
export composes runs from them. Sections stay handles (AGENTS 5): only the composed runs leave.

WHAT A SPAN IS. For each section of a stream water (a section belongs to one water):

  lo_m, hi_m   metres along the water's MAIN STEM, measured FROM THE MOUTH (the FWA route measure
               on the main blue line, whose 0 is the line's downstream end). The main stem is the
               blue line carrying the most of the water's length (`main_stem`). A section on
               another blue line of the same water — a side channel, a braid — is OFF the stem:
               both values are the main-stem measure where it flows back in (`off_stem` = 1), or
               NULL when it never does.
  lo, hi       what ends the section downstream (`lo`) and upstream (`hi`), as an END TOKEN
               (`end_token`): a cut-point id, or one of the named natural ends.

END TOKENS (one vocabulary, the export's `from` / `to`):
  <split id>               a cut the pipeline made at a curated split or a gauge (`split` table)
  area:<name>              an area boundary that is not a region (a park, a reserve, an MU group)
  region_line:<region>     a region boundary (`area:3` in the atlas)
  bc_border                the provincial boundary
  lake_inlet:<item_id>     the stretch ends where it flows INTO that lake
  lake_outlet:<item_id>    the stretch begins where it flows OUT of that lake
  confluence:<item_id>     a length cut the atlas placed at that tributary's mouth
  mouth, source            the water's own natural ends (no cut)
A lake that is not a named water drops its suffix (`lake_inlet`, `lake_outlet`).
"""
from __future__ import annotations

import json
import re
import sqlite3
from collections import defaultdict
from pathlib import Path

from pipeline.deliver.bundle.place_names import area_display, display_case

NATURAL_ENDS = ("mouth", "source", "bc_border", "lake_inlet", "lake_outlet")
#: Prefixed natural ends: the suffix is an item_id (or, for a region line, a region code).
PREFIXED_ENDS = ("lake_inlet:", "lake_outlet:", "confluence:", "region_line:")
#: The atlas names a region's polygon `area:<region>`; every other `area:` is a park, a reserve
#: or a group of management units, and keeps its split id.
REGION_AREA = re.compile(r"^area:([1-8]|7A|7B)$")
#: Split families the atlas mints itself (`sectionizer._AUTO_SPLIT_PREFIXES`): positions, not
#: names. One standing at a run's end gives way to an authored alias at the same place.
_AUTO = ("gauge__", "area:", "border:", "length:")


def _is_auto(split_id: str) -> bool:
    return split_id.startswith(_AUTO)


def main_stem(pieces: list[tuple[str, float]]) -> str | None:
    """The blue line carrying the most of a water's length: `pieces` are (blk, length_m). Ties go
    to the lowest blk, so the choice depends on the pieces and nothing else."""
    total: dict[str, float] = defaultdict(float)
    for blk, length in pieces:
        total[blk] += length
    if not total:
        return None
    return sorted(total.items(), key=lambda kv: (-round(kv[1], 3), kv[0]))[0][0]


def end_token(bound, side: str, *, lake_item, tributary=None) -> str:
    """The end token for one end of a section. `bound` is a `SectionBoundary` (or None: a natural
    end), `side` is "up" or "down". `lake_item(wbk)` names a lake's water (None when it is not a
    named water); `tributary(boundary)` names the water whose mouth a length cut stands at."""
    if bound is None:
        return "source" if side == "up" else "mouth"
    bid = str(bound.boundary_id)
    kind = getattr(bound.kind, "value", bound.kind)
    if kind == "lake" or bid.startswith("lake:"):
        item = lake_item(bid.split(":", 1)[1])
        head = "lake_outlet" if side == "up" else "lake_inlet"
        return f"{head}:{item}" if item else head
    if kind == "border":
        return "bc_border"
    if bid.startswith("split:"):
        sid = bid[len("split:"):]
        m = REGION_AREA.match(sid)
        if m:
            return f"region_line:{m.group(1)}"
        if _is_auto(sid):
            authored = sorted(a[len("split:"):] if a.startswith("split:") else a
                              for a in (bound.aliases or ()))
            authored = [a for a in authored if not _is_auto(a) and "@" not in a
                        and not a.startswith(("lake:", "label:"))]
            if authored:
                return authored[0]
            if sid.startswith("length:") and tributary is not None:
                t = tributary(bound)
                if t:
                    return f"confluence:{t}"
        return sid
    return bid


def compute(nodes: dict, edges, handles: dict[str, int], items: list[tuple[int, str, str, list[int]]],
            lake_items: dict[str, str]) -> list[tuple]:
    """Every (sid, lo_m, hi_m, lo, hi, off_stem) row for the STREAM waters in `items`
    ((ord, item_id, kind, [sid])). `nodes` is the graph's node dict, `edges` its FlowEdges,
    `handles` node id -> sid, `lake_items` wbk -> the lake's item_id."""
    node_of = {s: n for n, s in handles.items()}
    items_of: dict[str, set[str]] = defaultdict(set)
    for _, item_id, _, sids in items:
        for s in sids:
            items_of[node_of[s]].add(item_id)
    down = defaultdict(list)
    into = defaultdict(list)             # blk -> [(at_measure, from_node)] confluences into it
    for e in edges:
        down[e.from_node].append(e)
        to = nodes.get(e.to_node)
        if to is not None and getattr(to.kind, "value", to.kind) == "stream":
            into[to.blk].append((e.at_measure, e.from_node))

    rows = []
    for ord_, item_id, kind, sids in items:
        if kind != "stream":
            continue
        pieces = [nodes[node_of[s]] for s in sids]
        stem = main_stem([(n.blk, n.length_m) for n in pieces
                          if getattr(n.kind, "value", n.kind) == "stream"])

        def tributary(bound, _item=item_id, _stem=stem):
            """The water whose mouth a length cut on this water's stem stands at: the one named
            water (other than this one) flowing in at that measure. Two, or none: not named, and
            the cut keeps its split id."""
            m = bound.route_measure
            cands = sorted({i for at, fn in into.get(_stem, ()) if abs(at - m) < 1.0
                            for i in items_of.get(fn, ()) if i != _item})
            return cands[0] if len(cands) == 1 else None

        def attach(nid, _stem=stem) -> float | None:
            """Where an off-stem piece flows back into the main stem, walking downstream."""
            seen = set()
            while nid not in seen and len(seen) < 64:
                seen.add(nid)
                out = sorted(down.get(nid, ()), key=lambda e: e.to_node)
                if not out:
                    return None
                e = out[0]
                to = nodes.get(e.to_node)
                if to is None:
                    return None
                if getattr(to.kind, "value", to.kind) == "stream" and to.blk == _stem:
                    return e.at_measure
                nid = e.to_node
            return None

        for s, n in zip(sids, pieces):
            lo = end_token(n.lower_bound, "down", lake_item=lake_items.get, tributary=tributary)
            hi = end_token(n.upper_bound, "up", lake_item=lake_items.get, tributary=tributary)
            if n.blk == stem:
                rows.append((s, round(n.down_m), round(n.up_m), lo, hi, 0))
            else:
                at = attach(node_of[s])
                m = None if at is None else round(at)
                rows.append((s, m, m, lo, hi, 1))
    return sorted(rows)


# --------------------------------------------------------------------------------------------
# Naming the cuts — the `split` table
# --------------------------------------------------------------------------------------------
#
# A CUT IS NAMED FOR WHAT IS THERE, ONCE PER WATER. Three defects shipped in the first `split`
# table (59c188ae), all in the names:
#
#   * A LENGTH CUT NAMED ITS OWN RIVER. `length_splits` cuts a long section at an interior
#     junction and labels the cut with the joining piece's `display_name`. 1,177 of those junctions
#     are where a SIDE CHANNEL OF THE SAME RIVER flows back in (every joining piece belongs to the
#     cut's own water), so the label was the river's own name: a "Gold River" cut on the Gold
#     River. The name now comes from the waters that join there (`_length_name`): another water's
#     name (a lake draining straight in: "Trophy Lake outlet"), else "side channel", else "unnamed
#     tributary" — the last two placed by the nearest
#     named landmark ("side channel below Ucona River"), since alone they name nothing.
#   * AN OFFSET WAS DROPPED. "Halfway River → Peace River (5000 m upstream)" read "Halfway River
#     confluence", the same as the confluence itself: the Peace had four. The offset now comes
#     from the curated split's own `offset_m` / `offset_dir`: "5 km upstream of the Halfway River
#     confluence".
#   * A NAME REPEATED ON ONE WATER. Two "CNR bridge" points on the Thompson. Where a name repeats at
#     two places of one water, each is placed by its nearest named landmark ("CNR bridge below
#     Deadman River"), then by its distance from it, then upper/lower. Two ids at ONE place (the
#     same curated cut authored under two waters: `fraser_river__thompson_river_into_fraser_river`
#     and `thompson_river__…`) are one place and keep one name; the later id says so
#     (`same_place_as`) instead of inventing a difference.
#
# And an area's name is printed as a reader writes it (`place_names.display_case`: "CLAYHURST
# ECOLOGICAL RESERVE" -> "Clayhurst Ecological Reserve"), its source spelling kept in
# `official_name`. Split IDS never change — rule and licensing extents bind by them.

#: Two positions of one water closer than this (km) are one place.
_SAME_PLACE_KM = 0.01
#: A join within this many metres of a cut stands at it.
_AT_M = 1.0


class NameFacts:
    """What naming a cut needs from the graph and the bundle, gathered once (`facts`):

    item_name   item_id -> its name
    node_items  graph node id -> the named waters (item_ids) that section belongs to
    joins       blk -> [(at_measure, from_node, from_blk, display_name)]: every stream piece of
                ANOTHER blue line flowing into that blue line, and every lake draining straight
                into it (`from_blk` None: "Trophy Lake" at a length cut on its river)
    pieces      blk -> [(down_m, up_m, node id)] for its stream pieces
    offsets     curated split id -> (offset_m, offset_dir), for the curated cuts placed at an offset
    """

    def __init__(self, item_name, node_items, joins, pieces, offsets):
        self.item_name = item_name
        self.node_items = node_items
        self.joins = joins
        self.pieces = pieces
        self.offsets = offsets
        self._marks: dict = {}

    def waters_at(self, blk: str, m: float) -> set[str]:
        """The named waters the blue line `blk` belongs to at measure `m`."""
        out: set[str] = set()
        for lo, hi, nid in self.pieces.get(str(blk), ()):
            if lo - _AT_M <= m <= hi + _AT_M:
                out |= self.node_items.get(nid, set())
        return out

    def joining(self, blk: str, m: float) -> list[tuple[str, str]]:
        """(from_node, display_name) of every piece of another blue line flowing in at `m`."""
        return [(fn, fb, dn) for at, fn, fb, dn in self.joins.get(str(blk), ())
                if abs(at - m) < _AT_M]

    def landmarks(self, blk: str, own: set[str]) -> list[tuple[float, str]]:
        """(measure, name) of every NAMED water other than `own` flowing into `blk`."""
        key = (str(blk), frozenset(own))
        if key in self._marks:
            return self._marks[key]
        own_names = {self.item_name.get(i, "").lower() for i in own}
        out = set()
        for at, fn, fb, dn in self.joins.get(str(blk), ()):
            if fb is None:
                continue                                  # a lake is no landmark here
            names = {self.item_name[i] for i in self.node_items.get(fn, set()) - own
                     if self.item_name.get(i)}
            if not names and dn and dn.lower() not in own_names:
                names = {dn}
            out |= {(round(at, 2), display_case(n)) for n in names}
        self._marks[key] = sorted(out)
        return self._marks[key]


def facts(graph, handles: dict[str, int], items, offsets: dict) -> NameFacts:
    """`NameFacts` from the atlas graph, the section handles and the bundle's items
    ((ord, item_id, kind, [sid], name))."""
    node_of = {s: n for n, s in handles.items()}
    node_items: dict[str, set[str]] = defaultdict(set)
    item_name = {}
    for _, item_id, _, sids, name in items:
        item_name[item_id] = name or ""
        for s in sids:
            node_items[node_of[s]].add(item_id)
    joins: dict[str, list] = defaultdict(list)
    for e in graph.edges:
        to, fr = graph.nodes.get(e.to_node), graph.nodes.get(e.from_node)
        if to is None or fr is None or _kind(to) != "stream" or \
                _kind(fr) not in ("stream", "lake") or str(fr.blk) == str(to.blk):
            continue
        joins[str(to.blk)].append((float(e.at_measure), e.from_node,
                                   str(fr.blk) if _kind(fr) == "stream" else None,
                                   fr.display_name or ""))
    pieces: dict[str, list] = defaultdict(list)
    for nid, n in graph.nodes.items():
        if _kind(n) == "stream":
            pieces[str(n.blk)].append((float(n.down_m), float(n.up_m), nid))
    for v in joins.values():
        v.sort(key=lambda j: (j[0], j[1], j[2] or "", j[3]))
    for v in pieces.values():
        v.sort()
    return NameFacts(item_name, dict(node_items), dict(joins), dict(pieces), offsets)


def _kind(n) -> str:
    return getattr(n.kind, "value", n.kind)


def curated_offsets(defs) -> dict[str, tuple[float, str]]:
    """{split id: (offset_m, offset_dir)} for every curated split authored at an offset."""
    return {d.id: (float(d.anchor.offset_m), d.anchor.offset_dir) for d in defs
            if d.anchor.offset_m and d.anchor.offset_dir}


def distance_words(m: float) -> str:
    """500 -> "500 m", 1500 -> "1.5 km", 5000 -> "5 km"."""
    if m < 1000:
        return f"{int(round(m, -1 if m >= 100 else 0))} m"
    km = round(m / 1000, 1)
    return f"{km:g} km"


_OFFSET_TAIL = re.compile(r"\s*\([^()]*\)\s*$")


def _confluence_name(label: str, own_names: set[str], offset: tuple[float, str] | None) -> str:
    """A curated confluence "Eve River → Adam River": on the Adam it is the "Eve River confluence",
    on the Eve the "Adam River confluence" — never the water's own name. An offset cut says how far
    and which way: "5 km upstream of the Halfway River confluence"."""
    if "→" not in label:
        return label
    a, b = (x.strip() for x in label.split("→", 1))
    b = _OFFSET_TAIL.sub("", b).strip()
    own = {n.lower() for n in own_names}
    other = b if a.lower() in own and b.lower() not in own else a
    name = f"{other} confluence"
    if offset:
        name = f"{distance_words(offset[0])} {offset[1]} of the {name}"
    return name


def _length_name(f: NameFacts, blk: str, m: float, own: set[str]) -> tuple[str, bool]:
    """(name, needs a landmark) for a length cut: the named waters that join there ("Mowich Creek
    confluence", a lake draining straight in: "Trophy Lake outlet"); else a side channel of the
    cut's own water; else an unnamed tributary."""
    joiners = f.joining(blk, m)
    others: set[str] = set()
    braid = False
    own_names = {f.item_name.get(i, "").lower() for i in own}
    for fn, fb, dn in joiners:
        items = f.node_items.get(fn, set())
        if items & own:
            braid = True
        names = {f.item_name[i] for i in items - own if f.item_name.get(i)}
        if not items and dn and dn.lower() not in own_names:
            names = {dn}
        if not items and dn and dn.lower() in own_names:
            braid = True
        what = "outlet" if fb is None else "confluence"
        others |= {f"{display_case(n)} {what}" for n in names}
    if others:
        return " and ".join(sorted(others)), False
    return ("side channel" if braid else "unnamed tributary"), True


def _placed(base: str, f: NameFacts, blk: str, m: float, own: set[str], *,
            distance: bool = False) -> str:
    """`base` placed by the nearest named water joining the same blue line: "side channel below
    Ucona River" (below = downstream of its mouth). `distance` adds how far."""
    marks = [(abs(at - m), at, nm) for at, nm in f.landmarks(blk, own)
             if abs(at - m) >= _AT_M and nm.lower() not in base.lower()]
    if not marks:
        return base
    d, at, nm = min(marks)
    side = "below" if m < at else "above"
    return f"{base} {distance_words(d)} {side} {nm}" if distance else f"{base} {side} {nm}"


def split_rows(resolved: list[dict], stems: dict[str, list[tuple[str, int]]],
               f: NameFacts | None = None) -> list[tuple]:
    """The `split` table: every cut the atlas resolved, once per id — (split_id, name, kind, at,
    official_name, same_place_as), `at` a JSON list of [item_id, km] for each position on a named
    water's MAIN stem (`stems`: blk -> [(item_id, ord)] whose main stem it is). Region and area
    boundaries cross hundreds of waters, and the runs carry their km, so their `at` is left empty.
    Names are unique per water (`at`), except ids at one place (`same_place_as`)."""
    f = f or NameFacts({}, {}, {}, {}, {})
    by_id: dict[str, list[dict]] = defaultdict(list)
    for r in resolved:
        by_id[r["split_id"]].append(r)
    rows: dict[str, dict] = {}
    for sid in sorted(by_id):
        rs = by_id[sid]
        if REGION_AREA.match(sid):
            continue                                  # a region line is `region_line:<code>`
        first = sorted(rs, key=lambda r: (str(r["blk"]), r["route_measure"]))[0]
        blk, m = str(first["blk"]), float(first["route_measure"])
        kind = first["anchor_type"]
        label = first.get("label") or sid
        official = None
        own = f.waters_at(blk, m)
        landmark = False
        if kind == "area_boundary":
            area = label[len("within "):] if label.startswith("within ") else label
            area = area[:-len(" boundary")] if area.endswith(" boundary") else area
            shown = area_display(area)
            official = area if shown != area else None
            name = f"{shown} boundary"
        elif sid.startswith("length:"):
            name, landmark = _length_name(f, blk, m, own)
        elif kind == "confluence":
            name = _confluence_name(label, {f.item_name.get(i, "") for i in own},
                                    f.offsets.get(sid))
        else:
            name = label
        at = []
        if not sid.startswith("area:"):
            at = sorted({(item, round(r["route_measure"] / 1000, 2))
                         for r in rs for item, _ in stems.get(str(r["blk"]), ())})
        rows[sid] = {"base": name, "name": name, "kind": "area" if kind == "area_boundary"
                     else kind, "at": at, "official": official, "same": None,
                     "pos": (blk, m, own)}
        if landmark:
            rows[sid]["name"] = _placed(name, f, blk, m, own)
    _unique_per_water(rows, f)
    return [(sid, r["name"], r["kind"], json.dumps([list(a) for a in r["at"]]), r["official"],
             r["same"]) for sid, r in sorted(rows.items())]


def _unique_per_water(rows: dict[str, dict], f: NameFacts) -> None:
    """Make every name unique among the PLACES of each water: ids at one place share a name and
    the later ones point at the first (`same_place_as`); two places sharing a name are each placed
    by their nearest named landmark, then by their distance from it, then upper/lower."""
    for step in ("landmark", "distance", "order"):
        clashes = _clashes(rows)
        if not clashes:
            return
        for (water, _), places in sorted(clashes.items()):
            if step == "order":
                ordered = sorted(places, key=lambda p: p[0])           # by km, downstream first
                words = (["lower", "upper"] if len(ordered) == 2 else
                         ["lower", "middle", "upper"] if len(ordered) == 3 else None)
                for i, (km, ids) in enumerate(ordered):
                    tag = words[i] if words else f"{distance_words(km * 1000)} above the mouth"
                    for sid in ids:
                        r = rows[sid]
                        r["name"] = f"{tag} {r['name']}" if words else f"{r['name']}, {tag}"
                continue
            for km, ids in places:
                for sid in ids:
                    r = rows[sid]
                    blk, m, own = r["pos"]
                    r["name"] = _placed(r["base"], f, blk, m, own, distance=step == "distance")
    left = _clashes(rows)
    if left:
        raise SystemExit(f"split names: {len(left)} name(s) still repeat on one water after "
                         f"placing them — e.g. {sorted(left)[:3]}")


def _clashes(rows: dict[str, dict]) -> dict:
    """{(water, name): [(km, [ids at that place])]} for every name standing at two or more places
    of one water. Ids at one place are grouped, and every one after the first gets
    `same_place_as`."""
    seen: dict[tuple[str, str], dict[float, list[str]]] = defaultdict(dict)
    for sid, r in sorted(rows.items()):
        for water, km in r["at"]:
            places = seen[(water, r["name"])]
            near = next((k for k in places if abs(k - km) <= _SAME_PLACE_KM), None)
            if near is None:
                places[km] = [sid]
            elif sid not in places[near]:
                places[near].append(sid)
    out = {}
    for key, places in seen.items():
        for ids in places.values():
            for sid in ids[1:]:
                rows[sid]["same"] = rows[sid]["same"] or ids[0]
        if len(places) > 1 and len({s for ids in places.values() for s in ids}) > 1:
            out[key] = sorted((km, ids) for km, ids in places.items())
    return out


def write(db: sqlite3.Connection, graph, build_dir: Path, cov) -> None:
    """`section_span` and `split`, from the atlas graph, the resolved splits and `item_section`.
    REFUSED, never skipped, when the resolved splits are missing: a run ending at a cut nobody
    can name is the thing this table exists to prevent."""
    from pipeline.common.section_handles import read as _read_handles

    resolved_path = Path(build_dir) / "splits.resolved.json"
    if not resolved_path.exists():
        raise SystemExit(f"section_span: no {resolved_path} — the bundle cannot name the cuts "
                         f"a stretch runs between without the atlas's resolved splits")
    _, handles = _read_handles(build_dir)
    by_ord: dict[int, list[int]] = defaultdict(list)
    for ord_, s in db.execute("SELECT ord, sid FROM item_section ORDER BY ord, sid"):
        by_ord[ord_].append(s)
    items = [(o, i, k, by_ord.get(o, [])) for o, i, k in
             db.execute("SELECT ord, item_id, kind FROM item ORDER BY ord")]
    node_of = {s: n for n, s in handles.items()}
    lake_items: dict[str, str] = {}
    for _, item_id, kind, sids in items:
        if kind != "lake":
            continue
        for s in sids:
            nid = node_of[s]
            if nid.startswith("lake:"):
                # A lake PART (`item.part_of`: Williston's Nation Arm) is its own polygon and its
                # own water, so a river flowing into it names the part.
                lake_items[nid.split(":", 1)[1]] = item_id
    rows = compute(graph.nodes, graph.edges, handles, items, lake_items)
    eid = {t: i + 1 for i, t in enumerate(sorted({r[3] for r in rows} | {r[4] for r in rows}))}
    db.executemany("INSERT INTO span_end (eid, token) VALUES (?,?)",
                   sorted((i, t) for t, i in eid.items()))
    db.executemany("INSERT INTO section_span (sid, lo_m, hi_m, lo, hi, off_stem) "
                   "VALUES (?,?,?,?,?,?)",
                   ((s, a, b, eid[lo], eid[hi], off) for s, a, b, lo, hi, off in rows))
    cov.filled("span_end", len(eid))
    cov.filled("section_span", len(rows))

    stems: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for ord_, item_id, kind, sids in items:
        if kind != "stream":
            continue
        ns = [graph.nodes[node_of[s]] for s in sids]
        stem = main_stem([(n.blk, n.length_m) for n in ns
                          if getattr(n.kind, "value", n.kind) == "stream"])
        if stem:
            stems[stem].append((item_id, ord_))
    resolved = json.loads(resolved_path.read_text())
    from pipeline.atlas.splits.splits import load_split_defs
    from pipeline.common.curated import CURATED
    names = dict(db.execute("SELECT item_id, name FROM item"))
    f = facts(graph, handles, [(o, i, k, sids, names.get(i)) for o, i, k, sids in items],
              curated_offsets(load_split_defs(str(CURATED.waters.splits))))
    srows = split_rows(resolved, stems, f)
    db.executemany("INSERT INTO split (split_id, name, kind, at, official_name, same_place_as) "
                   "VALUES (?,?,?,?,?,?)", srows)
    cov.filled("split", len(srows))


# --------------------------------------------------------------------------------------------
# Composing runs — the reader's half, used by the UI export
# --------------------------------------------------------------------------------------------

def compose_runs(span: dict[int, tuple], touch: dict[int, set[int]], sids: list[int]) -> list[dict]:
    """A part's sections as RUNS, upstream to downstream. `span[sid]` is (lo_m, hi_m, lo, hi,
    off_stem); `touch[sid]` the sections of the same water it borders.

    A run is a maximal chain of the part's MAIN-STEM sections joined end to end (one section's
    `hi_m` is the next one's `lo_m` — the same point of one blue line): `from` is the top section's upstream end, `to` the bottom
    section's downstream end, `km_from` > `km_to` (km from the mouth). An off-stem section (a
    side channel) joins the run it borders; one that borders none is a run of its own with
    `branch: true`, its km the main-stem point it flows back in at (equal ends, or null). Each
    run carries its `sids` — the caller strips them before anything leaves the bundle."""
    stem = sorted((s for s in sids if not span[s][4]), key=lambda s: (span[s][0], span[s][1], s))
    chains: list[list[int]] = []
    for s in stem:
        if chains and span[chains[-1][-1]][1] == span[s][0]:
            chains[-1].append(s)
        else:
            chains.append([s])
    run_of = {s: i for i, c in enumerate(chains) for s in c}
    members = [list(c) for c in chains]
    off = sorted(s for s in sids if span[s][4])
    left = set(off)
    # Side sections join a chain they border, directly or through other side sections.
    changed = True
    while changed and left:
        changed = False
        for s in sorted(left):
            hits = sorted(run_of[t] for t in touch.get(s, ()) if t in run_of)
            if hits:
                run_of[s] = hits[0]
                members[hits[0]].append(s)
                left.discard(s)
                changed = True
    runs = []
    for i, c in enumerate(chains):
        top, bot = c[-1], c[0]
        runs.append({"from": span[top][3], "to": span[bot][2],
                     "km_from": round(span[top][1] / 1000, 2),
                     "km_to": round(span[bot][0] / 1000, 2), "sids": sorted(members[i])})
    # Branches that border no stem chain of the part: connected groups among themselves.
    seen: set[int] = set()
    for s in sorted(left):
        if s in seen:
            continue
        comp, stack = [], [s]
        seen.add(s)
        while stack:
            x = stack.pop()
            comp.append(x)
            for y in touch.get(x, ()):
                if y in left and y not in seen:
                    seen.add(y)
                    stack.append(y)
        comp.sort()
        ms = {span[x][0] for x in comp if span[x][0] is not None}
        km = round(max(ms) / 1000, 2) if ms else None
        # the group's ends: its highest and lowest pieces' own ends — handles run blue line by
        # blue line, mouth to source (`section_handles.sort_key`); a branch's natural ends are its
        # own source and mouth
        top, bot = comp[-1], comp[0]
        runs.append({"from": span[top][3], "to": span[bot][2], "km_from": km, "km_to": km,
                     "branch": True, "sids": comp})
    runs.sort(key=lambda r: (r["km_from"] is None, -(r["km_from"] or 0), -(r["km_to"] or 0),
                             r.get("branch", False), r["sids"][0]))
    return runs
