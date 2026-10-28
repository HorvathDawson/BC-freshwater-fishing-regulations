"""THE REGION SHARDS (P08, MINIMAL-PACKAGING §2): `regs-<region>` holds the frames of the answer keys (akeys) whose
sections lie in that region. A reader needs one region's frames; a key in two regions is in both.

  regions of an akey   every section in B.C. has its akey (`section_akey`, akey + 1, 0 = none) and its place
                       (`section_place`, place + 1); a place names its regions (`place.regions`). An akey's regions are
                       the union over its sections. Read from the regs store alone: the shard holds no logic of its own.
  what a shard holds   the `cell` rows of its akeys, the five top frames those cells name, and every row those frames
                       reach: ladder → verdicts; rows → decided + rows; licence → holds, documents, under-16, and the
                       documents' answers. `gear_frame` and `display_frame` are self-contained; `display_rule` is per
                       rule and lives in `water`.
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
#: the sid maps (`regs-sid`)
SID_TABLES = ("section_akey", "section_place")


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
    return {}


def row_regions(akey_regs: Dict[int, Set[str]], cells: Iterable[tuple], frame: Callable[[str, int], object]
                ) -> Dict[str, Dict[int, Set[str]]]:
    """{table: {id: the regions whose shard holds that row}} for every blob table a shard holds — what `shard_rows`
    cuts, for all regions at once. `cells` are the `cell` rows as (akey, ladder, rows, gear, licence, display);
    `frame(table, id)` is a row's decoded value. The store's dictionary trainer weighs a row by how many shards ship
    it (a row in no shard ships nowhere), so the dictionary is chosen for what ships, not for the whole table."""
    out: Dict[str, Dict[int, Set[str]]] = {t: defaultdict(set) for t in SHARD_TABLES if t != "cell"}
    for akey, *tops in cells:
        rs = akey_regs.get(akey)
        if rs:
            for tbl, i in zip(TOP.values(), tops):
                out[tbl][i] |= rs
    for tbl in ("ladder_frame", "rows_frame", "licence_frame"):        # the frames that reach other rows
        for i, rs in sorted(out[tbl].items()):
            for sub, refs in frame_refs(tbl, frame(tbl, i)).items():
                for r in refs:
                    out[sub][r] |= rs
    for i, rs in sorted(out["licence_doc"].items()):
        for r in frame("licence_doc", i):
            out["licence_answer"][r] |= rs
    return {t: dict(v) for t, v in out.items()}


def shard_rows(store_path, akeys: Iterable[int]) -> Dict[str, Set[int]]:
    """{table: the ids a shard of these akeys holds} (`cell` by akey)."""
    from pipeline.deliver.store.decode import Store
    st = Store(store_path, verify=False)
    try:
        want = sorted(set(akeys))
        out: Dict[str, Set[int]] = {t: set() for t in SHARD_TABLES}
        out["cell"] = set(want)
        q = ",".join("?" * len(want)) if want else "NULL"
        tops = defaultdict(set)
        for row in st.con.execute(f"SELECT {', '.join(TOP)} FROM cell WHERE akey IN ({q})", want):
            for col, i in zip(TOP, row):
                tops[TOP[col]].add(i)
        for tbl, ids in tops.items():
            out[tbl] |= ids
            for (i, blob) in st.con.execute(f"SELECT id, j FROM {tbl} WHERE id IN ({','.join(map(str, ids))})"):
                for sub, refs in frame_refs(tbl, st.val(tbl, blob)).items():
                    out[sub].update(refs)
        if out["licence_doc"]:
            for (i, blob) in st.con.execute(
                    f"SELECT id, j FROM licence_doc WHERE id IN ({','.join(map(str, out['licence_doc']))})"):
                out["licence_answer"].update(st.val("licence_doc", blob))
        return out
    finally:
        st.close()
