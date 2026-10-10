"""The store's reference decoder: `regs.sqlite` -> the answers file's bytes, the export subset, and
the section-handle lookup.

`Store(path)` refuses a store whose format is not this one or whose `meta.store_digest` is not its
content (any altered row); `answers_bytes()` refuses bytes whose SHA-256 is not the answers file it
was built from, `export_subset()` a subset whose digest is not the export's. Every value is read
from the store's own tables; nothing here opens the answers file or the export.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional

from pipeline.deliver.store import FORMAT
from pipeline.deliver.store.common import (BLOCK, BLOCK_BITS, BLOB_COLUMNS, EXPORT_STATIC,
                                           STEELHEAD_NAME, StoreError, answers_file_bytes,
                                           canonical, sha, store_digest, unpack)


class Store:
    def __init__(self, path, verify: bool = True):
        self.path = Path(path)
        if not self.path.exists():
            raise FileNotFoundError(f"no store at {self.path} — make it with: "
                                    "python -m pipeline.deliver store")
        self.con = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
        self.meta = dict(self.con.execute("SELECT k, v FROM meta"))
        if self.meta.get("format") != FORMAT:
            raise StoreError(f"store: format {self.meta.get('format')!r} is not {FORMAT!r}")
        if verify and store_digest(self.con) != self.meta.get("store_digest"):
            raise StoreError("store: meta.store_digest is not the store's content (altered)")
        self.zd: Dict[str, bytes] = dict(self.con.execute("SELECT tbl, d FROM zdict"))
        self._str: Optional[Dict[int, str]] = None

    def close(self) -> None:
        self.con.close()

    # ---- primitives
    def s(self, i: Optional[int]) -> Optional[str]:
        if self._str is None:
            self._str = dict(self.con.execute("SELECT id, text FROM str"))
        return None if i is None else self._str[i]

    def val(self, tbl: str, b: bytes):
        return json.loads(unpack(b, self.zd.get(tbl)))

    def static(self, k: str):
        row = self.con.execute("SELECT j FROM static WHERE k = ?", (k,)).fetchone()
        if row is None:
            raise StoreError(f"store: no static {k!r}")
        return self.val("static", row[0])

    def family(self, tbl: str) -> list:
        col = BLOB_COLUMNS[tbl]
        rows = self.con.execute(f"SELECT id, {col} FROM {tbl} ORDER BY id").fetchall()
        if [r[0] for r in rows] != list(range(len(rows))):
            raise StoreError(f"store: {tbl} ids are not 0..{len(rows) - 1}")
        return [self.val(tbl, b) for _, b in rows]

    # ---- the answers file
    def answers(self) -> dict:
        from pipeline.deliver.store.build import FAMILIES, SECTIONS
        layout = json.loads(self.meta["layout"])
        keys = []
        nseg = []
        for (a, rs, ls, sw, st, sr, pe, home, kind, tidal, seg, mom, n) in self.con.execute(
                "SELECT * FROM akey ORDER BY akey"):
            if a != len(keys) + 1:
                raise StoreError(f"store: akey {a} is out of sequence")
            keys.append([rs, ls, sw, STEELHEAD_NAME.get(st) if st is not None else None, sr,
                         json.loads(self.s(pe)), json.loads(self.s(home)), self.s(kind), tidal,
                         seg, mom])
            nseg.append(n)
        at: Dict[str, List[list]] = {name: [[] for _ in keys] for name in SECTIONS}
        for row in self.con.execute(
                f"SELECT akey, s, {', '.join(SECTIONS)} FROM cell ORDER BY akey, s"):
            a, s = row[0], row[1]
            for name, f in zip(SECTIONS, row[2:]):
                lst = at[name][a - 1]
                if len(lst) != s:
                    raise StoreError(f"store: cell ({a}, {s}) is out of sequence")
                lst.append(f)
        if any(len(at[SECTIONS[0]][k]) != n for k, n in enumerate(nseg)):
            raise StoreError("store: a key's cells are not its segments")
        items = self.con.execute("SELECT item_ord, item_id, shown, picker, unresolved FROM item "
                                 "ORDER BY item_ord").fetchall()
        pickers = {i: self.val("picker", b) for i, b in self.con.execute("SELECT id, j FROM picker")}
        parts_of: Dict[int, list] = {}
        for row in self.con.execute(
                "SELECT item_ord, part_ix, akey, ord, label_s, runs_s, place_s, hint_s, km, "
                "closed_all_year, paper_licence FROM part ORDER BY item_ord, part_ix"):
            parts_of.setdefault(row[0], []).append(row[1:])
        parts, waters = {}, {}
        for o, wid_s, shown, picker, unresolved in items:
            wid = self.s(wid_s)
            ps = parts_of.get(o, [])
            if [p[0] for p in ps] != list(range(len(ps))):
                raise StoreError(f"store: {wid}'s parts are out of sequence")
            parts[wid] = [None if p[1] is None else p[1] - 1 for p in ps]
            if shown:
                waters[wid] = {
                    "parts": [None if p[2] is None else {
                        "order": p[2], "label": self.s(p[3]), "runs": self.s(p[4]),
                        "place": self.s(p[5]), "hint": self.s(p[6]), "km": p[7],
                        "closed_all_year": bool(p[8]), "paper_licence": json.loads(p[9])}
                        for p in ps],
                    "picker": pickers[picker],
                    "unresolved_licensing": json.loads(unresolved)}
        sections = {}
        for name, sec_keys in layout["sections"].items():
            sec = {}
            for k in sec_keys:
                if k == "version":
                    sec[k] = layout["versions"][name]
                elif k == "at":
                    sec[k] = at[name]
                elif (name, k) in FAMILIES:
                    sec[k] = self.family(FAMILIES[(name, k)])
                elif (name, k) == ("display", "waters"):
                    sec[k] = waters
                else:
                    sec[k] = self.static(f"{name}.{k}")
            sections[name] = sec
        out = {}
        for k in layout["top"]:
            if k == "keys":
                out[k] = keys
            elif k == "segments":
                out[k] = [json.loads(t) for _, t in
                          self.con.execute("SELECT seg, starts FROM seglist ORDER BY seg")]
            elif k == "segment_moments":
                out[k] = [json.loads(t) for _, t in
                          self.con.execute("SELECT mom, ix FROM segmom ORDER BY mom")]
            elif k == "parts":
                out[k] = parts
            elif k == "sections":
                out[k] = sections
            else:
                out[k] = self.static(f"answers.{k}")
        return out

    def answers_bytes(self, check: bool = True) -> bytes:
        b = answers_file_bytes(self.answers())
        if check and sha(b) != self.meta["answers_sha256"]:
            raise StoreError("store: the decoded answers are not the answers file it was built from")
        return b

    # ---- the export subset
    def export_subset(self, check: bool = True) -> dict:
        rules = {}
        for _, b in self.con.execute("SELECT rix, j FROM rule ORDER BY rix"):
            r = self.val("rule", b)
            rules[f"{r['entry_id']}::{r['rule_id']}"] = r
        lics = {}
        for _, b in self.con.execute("SELECT lix, j FROM lic ORDER BY lix"):
            l = self.val("lic", b)
            lics[f"{l['entry_id']}#{l['record_id']}"] = l
        rule_ids, lic_ids = list(rules), list(lics)
        entries = {e: self.val("entry", b) for _, e, b in
                   self.con.execute("SELECT e, id, j FROM entry ORDER BY e")}

        def sets(tbl, ids):
            out = {}
            for sid, b in self.con.execute(f"SELECT set_id, j FROM {tbl} ORDER BY set_id"):
                v = self.val(tbl, b)
                out[str(sid)] = {k: [ids[i] for i in x] if isinstance(x, list) else x
                                 for k, x in v.items()}
            return out
        xparts = {i: self.val("xpart", b) for i, b in self.con.execute("SELECT id, j FROM xpart")}
        runs = {i: self.val("runs", b) for i, b in self.con.execute("SELECT id, j FROM runs")}
        parts_of: Dict[int, list] = {}
        for o, pix, xp, n, r in self.con.execute(
                "SELECT item_ord, part_ix, xp, sections, xruns FROM part ORDER BY item_ord, part_ix"):
            parts_of.setdefault(o, []).append({**xparts[xp], "sections": n, "runs": runs[r]})
        eids = list(entries)
        waters = {}
        for o, wid_s, name, kind, n, out_bc, ents, st, x in self.con.execute(
                "SELECT item_ord, item_id, name, kind, sections, outside_bc, entries, steelhead, x "
                "FROM item ORDER BY item_ord"):
            w = {"name": name, "kind": self.s(kind), "sections": n, "outside_bc": out_bc,
                 "entries": [eids[i] for i in json.loads(ents)]}
            if st is not None:
                w["steelhead"] = STEELHEAD_NAME[st]
            if x is not None:
                w.update(self.val("item", x))
            waters[self.s(wid_s)] = {**w, "parts": parts_of.get(o, [])}
        sub = {"rule_ids": rule_ids, "licensing_ids": lic_ids, "rules": rules, "licensing": lics,
               "entries": entries, **{k: self.static(f"export.{k}") for k in EXPORT_STATIC},
               "rulesets": sets("ruleset", rule_ids), "licensing_sets": sets("lset", lic_ids),
               "waters": waters}
        if check and sha(canonical(sub)) != self.meta["export_subset_sha256"]:
            raise StoreError("store: the decoded export subset is not the export it was built from")
        return sub

    # ---- section handles
    def akey_of(self, handle: int) -> int:
        """The akey of a section handle (0: no key yet)."""
        row = self.con.execute("SELECT b FROM section_akey WHERE block = ?",
                               (handle >> BLOCK_BITS,)).fetchone()
        if row is None:
            return 0
        off = 2 * (handle & (BLOCK - 1))
        return int.from_bytes(row[0][off:off + 2], "little")

    def section_akeys(self) -> Dict[int, int]:
        """Every handle with a key: {handle: akey}."""
        out = {}
        for blk, b in self.con.execute("SELECT block, b FROM section_akey ORDER BY block"):
            base = blk << BLOCK_BITS
            for i in range(BLOCK):
                a = b[2 * i] | (b[2 * i + 1] << 8)
                if a:
                    out[base + i] = a
        return out


def answers_bytes(path) -> bytes:
    s = Store(path)
    try:
        return s.answers_bytes()
    finally:
        s.close()


def export_subset(path) -> dict:
    s = Store(path)
    try:
        return s.export_subset()
    finally:
        s.close()
