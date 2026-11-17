"""THE REGION SHARDS (P08, MINIMAL-PACKAGING §2): `regs-<region>` holds the frames of the answer keys (akeys) whose
sections lie in that region. A reader needs one region's frames; a key in two regions is in both.

  regions of an akey   every section in B.C. has its akey (`section_akey`, akey + 1, 0 = none) and its place
                       (`section_place`, place + 1); a place names its regions (`place.regions`). An akey's regions are
                       the union over its sections. Read from the regs store alone: the shard holds no logic of its own.
  what a shard holds   the `cell` rows of its akeys, the five top frames those cells name, and every row those frames
                       reach: ladder → verdicts; rows → decided + rows; licence → holds, documents, under-16, and the
                       documents' answers. `gear_frame` and `display_frame` are self-contained; `display_rule` is per
                       rule and lives in `water`. (T810 ticket I) An OPTIONAL section's too, where the store holds it
                       (`cell` has its column): trout → its frame, the frame's table and date set, the table's sheets
                       (a NULL cell reaches nothing).

THE TABLE NAMES are the release contract's (`contract._REGION_TABLES`, `contract.OPTIONAL_SECTION_TABLES`): `TOP`,
`SHARD_TABLES` and their optional twins are held equal to it by `test_trout_section.py` (this module cannot import the
contract, which imports it).
"""
from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from typing import Callable, Dict, Iterable, List, Set

BLOCK_BITS, BLOCK = 12, 4096
REGIONS = ("1", "2", "3", "4", "5", "6", "7a", "7b", "8")
#: cell column -> its top frame table
TOP = {"ladder": "ladder_frame", "rows": "rows_frame", "gear": "gear_frame", "licence": "licence_frame",
       "display": "display_frame"}
#: every table a shard holds
SHARD_TABLES = ("cell", "ladder_frame", "ladder_verdict", "rows_frame", "rows_decided", "rows_row", "gear_frame",
                "licence_frame", "licence_hold", "licence_doc", "licence_answer", "licence_u16", "display_frame")
#: (T810 ticket I) an OPTIONAL section's cell column -> its top frame table, and every table of it a shard holds — in a
#: release whose store holds the section only (`tops`, `shard_tables`)
OPTIONAL_TOP = {"trout": "trout_frame"}
OPTIONAL_SHARD_TABLES = {"trout": ("trout_frame", "trout_table", "trout_dates", "trout_sheet")}
#: the tables whose rows reach rows of other tables (`frame_refs`), in the order the reach is followed
REACHING = ("ladder_frame", "rows_frame", "licence_frame", "licence_doc", "trout_frame", "trout_table")


def cell_sections(con: sqlite3.Connection) -> List[str]:
    """The sections a store's (or a region file's) `cell` names a frame of: its columns after akey, s."""
    cols = [c[1] for c in con.execute("PRAGMA table_info(cell)")]
    if cols[:2] != ["akey", "s"]:
        raise ValueError(f"release: cell's columns {cols} do not start with akey, s")
    return cols[2:]


def tops(con: sqlite3.Connection) -> Dict[str, str]:
    """{cell column: its top frame table} of this store: every section's (`TOP`), then each optional one it holds."""
    secs = cell_sections(con)
    want = [*TOP, *(n for n in OPTIONAL_TOP if n in secs)]
    if secs != want:
        raise ValueError(f"release: cell's sections {secs} are not {want}")
    return {c: TOP.get(c) or OPTIONAL_TOP[c] for c in secs}


def shard_tables(con: sqlite3.Connection) -> tuple:
    """Every table a shard of this store holds: `SHARD_TABLES`, then each optional section's it holds."""
    secs = cell_sections(con)
    return SHARD_TABLES + tuple(t for n, ts in OPTIONAL_SHARD_TABLES.items() if n in secs for t in ts)
#: the sid maps (`regs-sid`); `section_tidal` (T672 1b) is ADDITIVE, still regs-sid/1
SID_TABLES = ("section_akey", "section_place", "section_tidal")


def akey_regions(store: sqlite3.Connection) -> Dict[int, Set[str]]:
    """{akey: the regions its sections lie in}."""
    place_regions = {p: json.loads(r) for p, r in store.execute("SELECT place, regions FROM place")}
    akeys = dict(store.execute("SELECT block, b FROM section_akey"))
    places = dict(store.execute("SELECT block, b FROM section_place"))
    out: Dict[int, Set[str]] = defaultdict(set)
    for blk, ab in akeys.items():
        pb = places.get(blk)
        if pb is None:
            raise ValueError(f"release: section block {blk} has akeys but no places")
        for i in range(BLOCK):
            a = ab[2 * i] | (ab[2 * i + 1] << 8)
            if a:
                p = pb[i]
                if not p:
                    raise ValueError(f"release: section {(blk << BLOCK_BITS) | i} has akey {a} but no place")
                out[a].update(place_regions[p - 1])
    return dict(out)


def sid_regions(store: sqlite3.Connection) -> Dict[int, List[str]]:
    """{sid: the regions its place names} for every section with a place (owner 2026-10-28: the salmon slices)."""
    place_regions = {p: sorted(json.loads(r)) for p, r in store.execute("SELECT place, regions FROM place")}
    out: Dict[int, List[str]] = {}
    for blk, pb in store.execute("SELECT block, b FROM section_place"):
        for i, p in enumerate(pb):
            if p:
                out[(blk << BLOCK_BITS) | i] = place_regions[p - 1]
    return out


def frame_refs(name: str, frame) -> Dict[str, List[int]]:
    """The rows a decoded top frame reaches, by table (the codecs of `answers.encode`)."""
    if name == "ladder_frame":
        common, rows = frame
        return {"ladder_verdict": [common] + [r for row in rows for r in row[1:]]}
    if name == "rows_frame":
        _spp, fish, rows, _line, _strips = frame          # rows 30: the strips are a static (water), not a shard row
        # rows 33 (T161): a fish is [decided h, decided w, lead] — the lead is an origin word, not a row
        return {"rows_decided": [r for hwl in fish.values() for r in hwl[:2] if r is not None], "rows_row": list(rows)}
    if name == "licence_frame":
        h, d, *u = frame
        return {"licence_hold": [h], "licence_doc": [d], "licence_u16": list(u)}
    if name == "licence_doc":
        return {"licence_answer": list(frame)}
    if name == "trout_frame":                           # T810 ticket I: [table, dates | null, covers]
        t, d, _covers = frame
        return {"trout_table": [t], "trout_dates": [] if d is None else [d]}
    if name == "trout_table":                           # each row's sheet
        return {"trout_sheet": [r["sheet"] for b in frame["boxes"] for r in b["rows"]]}
    return {}


def row_regions(akey_regs: Dict[int, Set[str]], cells: Iterable[tuple], frame: Callable[[str, int], object],
                top: Dict[str, str] = TOP) -> Dict[str, Dict[int, Set[str]]]:
    """{table: {id: the regions whose shard holds that row}} for every blob table a shard holds — what `shard_rows`
    cuts, for all regions at once. `cells` are the `cell` rows as (akey, then one frame id per `top` column: ladder,
    rows, gear, licence, display, and an optional section's where the store holds it, None where its cell has none);
    `frame(table, id)` is a row's decoded value. The store's dictionary trainer weighs a row by how many shards ship
    it (a row in no shard ships nowhere), so the dictionary is chosen for what ships, not for the whole table."""
    tables = SHARD_TABLES + tuple(t for n, ts in OPTIONAL_SHARD_TABLES.items() if n in top for t in ts)
    out: Dict[str, Dict[int, Set[str]]] = {t: defaultdict(set) for t in tables if t != "cell"}
    for akey, *ids in cells:
        rs = akey_regs.get(akey)
        if rs:
            for tbl, i in zip(top.values(), ids):
                if i is not None:
                    out[tbl][i] |= rs
    for tbl in REACHING:                                # the rows that reach other rows, in reach order
        if tbl not in out:
            continue
        for i, rs in sorted(out[tbl].items()):
            for sub, refs in frame_refs(tbl, frame(tbl, i)).items():
                for r in refs:
                    out[sub][r] |= rs
    return {t: dict(v) for t, v in out.items()}


def shard_rows(store_path, akeys: Iterable[int]) -> Dict[str, Set[int]]:
    """{table: the ids a shard of these akeys holds} (`cell` by akey)."""
    from pipeline.deliver.store.decode import Store
    st = Store(store_path, verify=False)
    try:
        return reach_rows(st.con, akeys, st.val)
    finally:
        st.close()


def reach_rows(con: sqlite3.Connection, akeys: Iterable[int], val: Callable[[str, bytes], object]) -> Dict[str, Set[int]]:
    """{table: the ids a shard of `akeys` holds}, read from `con` (a store or a region file): the akeys' cells, their
    top frames, and every row those reach (`frame_refs`, in `REACHING` order). `val(table, blob)` decodes a row."""
    top = tops(con)
    want = sorted(set(akeys))
    out: Dict[str, Set[int]] = {t: set() for t in shard_tables(con)}
    out["cell"] = set(want)
    q = ",".join("?" * len(want)) if want else "NULL"
    for row in con.execute(f"SELECT {', '.join(top)} FROM cell WHERE akey IN ({q})", want):
        for col, i in zip(top, row):
            if i is not None:
                out[top[col]].add(i)
    for tbl in REACHING:
        if tbl not in out or not out[tbl]:
            continue
        for (_i, blob) in con.execute(f"SELECT id, j FROM {tbl} WHERE id IN ({','.join(map(str, sorted(out[tbl])))})"):
            for sub, refs in frame_refs(tbl, val(tbl, blob)).items():
                out[sub].update(refs)
    return out
