"""(T796, owner 2026-11-16: "take out the lakes but add a note"; RULINGS Z10) THE NOTE ON A WATER TAKEN OUT OF A CLOSURE
AREA BY NAME: `section_note`, one sentence per section, written here so the app only shows it.

A lake is never cut, so one that only reaches into an ecological reserve or a park is a member of it and its closure
bound the whole lake (Cold Fish Lake, 0.1% inside the Gladys Lake Ecological Reserve). Z10: "A lake that lies mostly
outside the area is taken out by name" — `area_closures.json` `outside_items` (`registry.outside_area_items`), an item
id or, for a lake with no item of its own, its section handle (`lake:<waterbody id>`). Such a water still lies beside
the reserve or park on the map, so its page says, once, that the closure there does not reach it:

    "Mostly outside Gladys Lake Ecological Reserve: its closure (rule book p.9) does not apply here."

The area's name is the TILES' own (`place_names.area_display`, the name the map labels the polygon with); the pages are
those of the book's rows that close the area (a `within` extent naming the area or its family, with a full closure).
A listed water this bundle does not hold, or an area with no name on its tile layer, STOPS the build (never a silent
missing note).
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

NOTE = "Mostly outside {area}: its closure ({pages}) does not apply here."


def outside_listed(area_defs: Iterable[dict]) -> List[tuple]:
    """[(def id, area key `area:<kind>:<slug>`, slug, water ids)] of every `outside_items` list, in file order."""
    from pipeline.common.registry_kinds import area_key
    out = []
    for ad in area_defs:
        for slug, items in (ad.get("outside_items") or {}).items():
            out.append((ad["id"], area_key(f"{ad['id']}:{slug}"), slug, list(items)))
    return out


def tile_names(area_defs: Iterable[dict], layers: Path, wanted: Iterable[str]) -> Dict[str, str]:
    """{area key: the name its tile carries} for the wanted area keys, read from each def's `tile_layer` (only the
    features of its `tile_kind`, when it has one) — the same key the atlas mints (`area_catalog.area_id`)."""
    from pipeline.atlas.splits.area_catalog import area_id
    want = set(wanted)
    out: Dict[str, str] = {}
    for ad in area_defs:
        if not any(k.split(":")[1] == ad["id"] for k in want) or not ad.get("tile_layer"):
            continue
        with open(Path(layers) / f"{ad['tile_layer']}.geojsonl", encoding="utf-8") as fh:
            for line in fh:
                p = json.loads(line).get("properties") or {}
                if ad.get("tile_kind") and p.get("kind") != ad["tile_kind"]:
                    continue
                key = area_id(ad.get("kind", ad["id"]), p.get("area_id") or "")
                if key in want and p.get("name"):
                    out[key] = p["name"]
    return out


def closing_pages(db: sqlite3.Connection, def_id: str, key: str,
                  grades: Optional[Mapping[Tuple[str, str], Optional[str]]] = None) -> List[int]:
    """The printed pages of the book's rows that close this area: a full closure (`rules.closure_grade`; `grades`
    {(entry, rule): grade} when the bundle has not stored it yet, else `rule.closure_grade`) whose own extents
    (`rule.conditions`) or its row's (`entry.extents`) are `within` the area (`area_id`) or its family (`area_kind`) —
    the Chilkoot row's closure carries its extent on the rule, the zone tables' on the row."""
    from pipeline.common.registry_kinds import area_key
    pages: set = set()
    rows = db.execute("SELECT r.entry_id, r.rule_id, r.closure_grade, r.conditions, e.extents, e.pages FROM rule r "
                      "JOIN entry e USING (entry_id)").fetchall()
    for e_id, r_id, stored, cond, ex, pg in rows:
        if (grades.get((e_id, r_id)) if grades is not None else stored) != "full":
            continue
        extents = [*json.loads(ex or "[]"), *(json.loads(cond or "{}").get("extents") or [])]
        for x in extents:
            if x.get("op") != "within":
                continue
            if x.get("area_kind") == def_id or (x.get("area_id") and area_key(x["area_id"]) == key):
                pages |= set(json.loads(pg or "[]"))
    return sorted(pages)


def notes(listed: Sequence[tuple], names: Mapping[str, str], pages: Mapping[str, Sequence[int]],
          sids_of: Mapping[str, Sequence[int]]) -> Dict[int, str]:
    """{sid: note}: every section of every listed water, one sentence per area it is taken out of (said once)."""
    out: Dict[int, List[str]] = {}
    bad = []
    for def_id, key, slug, items in listed:
        if key not in names:
            bad.append(f"{key}: no tile names this area")
            continue
        pg = pages.get(key) or []
        said = NOTE.format(area=names[key], pages=("rule book p." + ", ".join(str(p) for p in pg)) if pg
                           else "rule book")
        for item in items:
            sids = sids_of.get(item) or []
            if not sids:
                bad.append(f"{key}: outside water {item} has no section in this bundle")
            for sid in sids:
                if said not in out.setdefault(sid, []):
                    out[sid].append(said)
    if bad:
        raise SystemExit("outside_note: area_closures.json and this bundle disagree:\n  " + "\n  ".join(bad))
    return {sid: " ".join(v) for sid, v in sorted(out.items())}


def write(db: sqlite3.Connection, area_defs: Sequence[dict], layers: Path, handle_by_id: Mapping[str, int],
          cov=None, grades: Optional[Mapping[Tuple[str, str], Optional[str]]] = None) -> Dict[int, str]:
    """Writes `section_note` from the area defs' `outside_items` (see the module doc). `handle_by_id`: the build's
    section handles (a `lake:` id's section). Written BEFORE the parts (`derived.part_rows`): a noted section is a part
    of its own, so a note shown on a part never speaks for more water than the lake (the Cormier Creek pond sat in a
    62-section run of its creek); `grades` the closure grades, which `derived.write` stores only after."""
    listed = outside_listed(area_defs)
    sids_of: Dict[str, List[int]] = {}
    for *_x, items in listed:
        for item in items:
            if item.startswith("lake:"):
                sids_of[item] = [handle_by_id[item]] if item in handle_by_id else []
            else:
                sids_of[item] = [s for (s,) in db.execute(
                    "SELECT x.sid FROM item_section x JOIN item i USING (ord) WHERE i.item_id = ? ORDER BY x.sid",
                    (item,))]
    names = tile_names(area_defs, layers, [k for _d, k, *_r in listed])
    pages = {k: closing_pages(db, d, k, grades) for d, k, *_r in listed}
    got = notes(listed, names, pages, sids_of)
    db.executemany("INSERT INTO section_note VALUES (?, ?)", sorted(got.items()))
    if cov is not None:
        cov.filled("section_note", len(got))
    return got
