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


def _confluence_name(label: str) -> str:
    """"Eve River → Adam River" (a curated confluence cut) reads "Eve River confluence"."""
    return f"{label.split('→')[0].strip()} confluence" if "→" in label else label


def split_rows(resolved: list[dict], stems: dict[str, list[tuple[str, int]]]) -> list[tuple]:
    """The `split` table: every cut the atlas resolved, once per id — (split_id, name, kind, at),
    `at` a JSON list of [item_id, km] for each position on a named water's MAIN stem (`stems`:
    blk -> [(item_id, ord)] whose main stem it is). Region and area boundaries cross hundreds of
    waters, and the runs carry their km, so their `at` is left empty."""
    by_id: dict[str, list[dict]] = defaultdict(list)
    for r in resolved:
        by_id[r["split_id"]].append(r)
    out = []
    for sid in sorted(by_id):
        rs = by_id[sid]
        if REGION_AREA.match(sid):
            continue                                  # a region line is `region_line:<code>`
        first = sorted(rs, key=lambda r: (r["blk"], r["route_measure"]))[0]
        kind = first["anchor_type"]
        label = first.get("label") or sid
        name = (_confluence_name(label) if kind == "confluence" else
                label if kind != "area_boundary" or label.endswith("boundary") or
                label.startswith("within ") else f"{label} boundary")
        if kind == "area_boundary" and label.startswith("within "):
            name = f"{label[len('within '):]} boundary"
        at = []
        if not sid.startswith("area:"):
            at = sorted({(item, round(r["route_measure"] / 1000, 2))
                         for r in rs for item, _ in stems.get(str(r["blk"]), ())})
        out.append((sid, name, "area" if kind == "area_boundary" else kind,
                    json.dumps([list(a) for a in at])))
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
    srows = split_rows(resolved, stems)
    db.executemany("INSERT INTO split (split_id, name, kind, at) VALUES (?,?,?,?)", srows)
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
