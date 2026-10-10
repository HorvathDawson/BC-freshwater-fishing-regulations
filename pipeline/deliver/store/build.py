"""Build `regs.sqlite` from the answers file, the export pair and the bundle (S0).

Every table is a re-arrangement of what the inputs hold — nothing is decided here. The answers'
interned tables become the frame families AS INTERNED (their indexes are the frame ids), the keys
become `akey` rows, every section's `at` becomes `cell`, `display.waters` becomes the columnar
`item` / `part` tables, and the bundle's part partition (`part_section`) gives `section_akey`.

THE BUILD REFUSES (StoreError) an answers file not paired with the export and the bundle (digests),
an answers file not in the answers CLI's own serialisation (byte identity would be meaningless), a
section or a display shape the store has not been taught, a bundle part whose facts are not its
answers key's, a section two parts claim — and, last, a store that does not decode back to the
answers bytes and the export subset (`decode`), which is never written.
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional

from pipeline.deliver.store import FORMAT
from pipeline.deliver.store.common import (BLOB_COLUMNS, BLOCK, BLOCK_BITS, CODECS, EXPORT_STATIC,
                                           STEELHEAD_CODE, StoreError, answers_file_bytes,
                                           canonical, dumps, dumps_s, export_subset, lic_id_of,
                                           pack, rule_id_of, sha, store_digest, train_zdict)

SCHEMA = Path(__file__).with_name("schema.sql")

#: The answers sections the store holds, and each one's frame families: {(section, table key):
#: store table}. A section or table key the store does not know is refused (teach it here).
SECTIONS = ("ladder", "rows", "gear", "licence", "display")
FAMILIES = {("ladder", "verdicts"): "ladder_verdict", ("ladder", "frames"): "ladder_frame",
            ("rows", "decided"): "rows_decided", ("rows", "rows"): "rows_row",
            ("rows", "frames"): "rows_frame", ("gear", "frames"): "gear_frame",
            ("licence", "holds"): "licence_hold", ("licence", "answers"): "licence_answer",
            ("licence", "documents"): "licence_doc", ("licence", "frames"): "licence_frame",
            ("display", "frames"): "display_frame", ("display", "rules"): "display_rule"}
#: Section keys held as `static` rows (whole values).
SECTION_STATIC = {("ladder", "reasons"), ("ladder", "reason_state"),
                  ("gear", "province_methods"), ("gear", "parent"), ("gear", "methods"),
                  ("gear", "moments"), ("gear", "conduct_means"),
                  ("licence", "profiles"), ("licence", "profile_dims")}
#: The answers' top-level keys held in tables; every other one is a `static` row.
TOP_TABLES = ("keys", "segments", "segment_moments", "parts", "sections")
TOP_STATIC = ("about", "spec", "schemas", "fish", "moments", "glossary")
#: display.waters' shapes (the columnar `item` / `part` rows rebuild them in this key order).
DISPLAY_WATER_KEYS = ("parts", "picker", "unresolved_licensing")
DISPLAY_PART_KEYS = ("order", "label", "runs", "place", "hint", "km", "closed_all_year",
                     "paper_licence")
#: The export water's words held as `item` columns (the rest goes to `item.x`).
WATER_COLUMNS = ("name", "kind", "sections", "outside_bc", "entries", "steelhead", "parts")
#: The answers keys row (`encode.encode`): 9 part-key fields, segments, moments.
KEY_LEN = 11


class _Strings:
    def __init__(self):
        self.ix: Dict[str, int] = {}

    def add(self, s: Optional[str]) -> Optional[int]:
        if s is None:
            return None
        if not isinstance(s, str):
            raise StoreError(f"store: {s!r} is not a string")
        if s not in self.ix:
            self.ix[s] = len(self.ix)
        return self.ix[s]


def load_inputs(answers: Path, export_dir: Path):
    raw = Path(answers).read_bytes()
    A = json.loads(raw)
    if answers_file_bytes(A) != raw:
        raise StoreError(f"store: {answers} is not in the answers CLI's serialisation "
                         "(answers.cli._raw) — its bytes could not be rebuilt")
    E = json.loads((Path(export_dir) / "ui-rules-export.json").read_text())
    G = json.loads((Path(export_dir) / "ui-rules-guide.json").read_text())
    return raw, A, E, G


def check_pairing(A: dict, E: dict, G: dict, bmeta: Dict[str, str]) -> None:
    from pipeline.deliver.answers.encode import FORMAT as ANSWERS_FORMAT, rule_ids_digest
    if A["about"].get("format") != ANSWERS_FORMAT:
        raise StoreError(f"store: answers format {A['about'].get('format')!r} is not "
                         f"{ANSWERS_FORMAT!r}")
    for name, other in (("ui-rules-export.json", E), ("ui-rules-guide.json", G)):
        if A["about"]["bundle"] != other["about"]["bundle"]:
            raise StoreError(f"store: the answers' about.bundle is not {name}'s")
    if A["about"]["export"]["rule_ids_sha256"] != rule_ids_digest(E["rule_ids"]):
        raise StoreError("store: the answers' rule_ids_sha256 is not the export's rule_ids")
    for k in ("section_handles", "reach_digest"):
        if A["about"]["bundle"].get(k) != bmeta.get(k):
            raise StoreError(f"store: the answers' {k} {A['about']['bundle'].get(k)!r} is not the "
                             f"bundle's {bmeta.get(k)!r}")
    if bmeta.get("rule_ids_sha256") != A["about"]["export"]["rule_ids_sha256"]:
        raise StoreError("store: the bundle's rule_ids_sha256 is not the answers'")


def bundle_problems(A: dict, bundle: sqlite3.Connection) -> List[str]:
    """Every bundle part whose facts are not its answers key's (the store's akey per part is the
    answers' `parts[item][part_ix]`; this proves the bundle's partition says the same)."""
    out = []
    ords = dict(bundle.execute("SELECT ord, item_id FROM item"))
    per_item: Dict[str, int] = {}
    for (o, pix, set_id, lset, pe, sw, st, sr, home, tidal) in bundle.execute(
            "SELECT ord, part_ix, set_id, licensing_set, province_except, steelhead_water, "
            "steelhead, steelhead_rules, home_regions, tidal FROM part ORDER BY ord, part_ix"):
        wid = ords[o]
        per_item[wid] = per_item.get(wid, 0) + 1
        parts = A["parts"].get(wid)
        if parts is None or pix >= len(parts):
            out.append(f"{wid} part {pix}: in the bundle, not in the answers")
            continue
        k = parts[pix]
        if (set_id is None) != (k is None):
            out.append(f"{wid} part {pix}: rule set {set_id} but answers key {k}")
            continue
        if k is None:
            continue
        key = A["keys"][k]
        want = (key[0], key[1], key[2], STEELHEAD_CODE.get(key[3]), key[4], ",".join(key[5]),
                ",".join(key[6]), key[8])
        got = (set_id, lset, sw, st, sr, pe, home, tidal)
        if want != got:
            out.append(f"{wid} part {pix}: bundle {got} != answers key {k} {want}")
    for wid, parts in A["parts"].items():
        if per_item.get(wid, 0) != len(parts):
            out.append(f"{wid}: {len(parts)} answers parts, {per_item.get(wid, 0)} bundle parts")
    return out


def build(answers: Path, export_dir: Path, bundle: Path, out: Path, blobs: str = "zdict",
          check: bool = True) -> dict:
    """Write the store to `out` (atomically) and return its counts."""
    if blobs not in CODECS:
        raise StoreError(f"store: blob codec {blobs!r} is not one of {CODECS}")
    raw, A, E, G = load_inputs(answers, export_dir)
    from pipeline.tools.export_codec import expand
    X = expand(E, G)
    sub = export_subset(X)
    b = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    try:
        bmeta = dict(b.execute("SELECT k, v FROM meta"))
        check_pairing(A, E, G, bmeta)
        problems = bundle_problems(A, b)
        if problems:
            raise StoreError(f"store: {len(problems)} bundle parts disagree with the answers: "
                             + "; ".join(problems[:10]))
        item_ord = {wid: o for o, wid in b.execute("SELECT ord, item_id FROM item")}
        part_section = b.execute("SELECT ord, sid, part_ix FROM part_section ORDER BY ord, sid"
                                 ).fetchall()
    finally:
        b.close()

    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".tmp")
    try:
        counts = write_store(tmp, raw, A, sub, item_ord, part_section, blobs)
        if check:
            from pipeline.deliver.store.decode import Store
            s = Store(tmp)
            try:
                if s.answers_bytes() != raw:
                    raise StoreError("store: the store does not decode to the answers bytes")
                if s.export_subset() != sub:
                    raise StoreError("store: the store does not decode to the export subset")
            finally:
                s.close()
        os.replace(tmp, out)
    except BaseException:
        if tmp.exists():
            tmp.unlink()
        raise
    return counts


def write_store(path: Path, raw: bytes, A: dict, sub: dict, item_ord: Dict[str, int],
                part_section: list, codec: str = "zdict") -> dict:
    """Write a store at `path` (replaced) from the decoded inputs — the answers wire `A` (and its
    bytes `raw`), the export subset `sub`, the bundle's item ords and its `part_section` rows
    (ord, sid, part_ix) — stamp its digest and compact it. No pairing or decode check: `build`."""
    if codec not in CODECS:
        raise StoreError(f"store: blob codec {codec!r} is not one of {CODECS}")
    path = Path(path)
    if path.exists():
        path.unlink()
    con = sqlite3.connect(path)
    try:
        counts = _write(con, raw, A, sub, item_ord, part_section, codec)
        con.commit()
        con.execute("INSERT INTO meta VALUES ('store_digest', ?)", (store_digest(con),))
        con.commit()
        con.execute("VACUUM")
    finally:
        con.close()
    return counts


def _write(con, raw: bytes, A: dict, sub: dict, item_ord: Dict[str, int], part_section: list,
           codec: str) -> dict:
    con.executescript("PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF;")
    con.executescript(SCHEMA.read_text())
    S = _Strings()
    pending: Dict[str, List[tuple]] = {}            # table -> rows whose blob column is still raw

    # ---- layout: the key order of the file and of each section
    untaught = set(A) - set(TOP_TABLES) - set(TOP_STATIC)
    missing = set(TOP_TABLES) - set(A)
    if untaught or missing:
        raise StoreError(f"store: top-level keys {sorted(untaught)} are not taught to the store, "
                         f"{sorted(missing)} are missing")
    if set(A["sections"]) != set(SECTIONS):
        raise StoreError(f"store: the answers hold sections {sorted(A['sections'])}, the store "
                         f"knows {sorted(SECTIONS)} — teach it (store/build.py SECTIONS, FAMILIES)")
    layout = {"top": list(A), "sections": {n: list(s) for n, s in A["sections"].items()},
              "versions": {n: s["version"] for n, s in A["sections"].items()}}
    for name, sec in A["sections"].items():
        for k in sec:
            if k in ("version", "at") or (name, k) in FAMILIES or (name, k) in SECTION_STATIC \
                    or (name, k) == ("display", "waters"):
                continue
            raise StoreError(f"store: section {name}.{k} is not taught to the store")

    static = [(f"answers.{k}", dumps(A[k])) for k in TOP_STATIC if k in A]
    static += [(f"{n}.{k}", dumps(A["sections"][n][k])) for n, k in sorted(SECTION_STATIC)]
    static += [(f"export.{k}", dumps(sub[k])) for k in EXPORT_STATIC]
    pending["static"] = static

    # ---- keys, segments, moments
    for i, starts in enumerate(A["segments"]):
        con.execute("INSERT INTO seglist VALUES (?, ?)", (i, dumps_s(starts)))
    for i, ix in enumerate(A["segment_moments"]):
        con.execute("INSERT INTO segmom VALUES (?, ?)", (i, dumps_s(ix)))
    nseg = []
    for k, row in enumerate(A["keys"]):
        if len(row) != KEY_LEN:
            raise StoreError(f"store: keys[{k}] has {len(row)} fields, not {KEY_LEN}")
        rs, ls, sw, st, sr, pe, home, kind, tidal, seg, mom = row
        if st is not None and st not in STEELHEAD_CODE:
            raise StoreError(f"store: keys[{k}] steelhead {st!r} has no code")
        for name, v in (("steelhead_water", sw), ("steelhead_rules", sr), ("tidal", tidal)):
            if type(v) is not int:
                raise StoreError(f"store: keys[{k}] {name} {v!r} is not an integer")
        n = len(A["segments"][seg])
        nseg.append(n)
        con.execute("INSERT INTO akey VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (k + 1, rs, ls, sw, STEELHEAD_CODE.get(st), sr, S.add(dumps_s(pe)),
                     S.add(dumps_s(home)), S.add(kind), tidal, seg, mom, n))

    # ---- cell: akey x segment -> frame per section
    secs = A["sections"]
    cells = []
    for k, n in enumerate(nseg):
        ats = [secs[name]["at"][k] for name in SECTIONS]
        if any(len(a) != n for a in ats):
            raise StoreError(f"store: key {k}: the sections' `at` rows are not its {n} segments")
        cells += [(k + 1, s, *(a[s] for a in ats)) for s in range(n)]
    if any(len(secs[name]["at"]) != len(nseg) for name in SECTIONS):
        raise StoreError("store: a section's `at` is not one row per key")
    con.executemany("INSERT INTO cell VALUES (?,?,?,?,?,?,?)", cells)

    # ---- the frame families, as the answers interned them
    for (name, key), tbl in FAMILIES.items():
        pending[tbl] = [(i, dumps(v)) for i, v in enumerate(secs[name][key])]

    # ---- items and parts (display.waters, the answers' parts, the export's waters)
    DW = secs["display"]["waters"]
    waters = sub["waters"]
    if list(A["parts"]) != list(waters):
        raise StoreError("store: the answers' parts are not the export's waters, in order")
    if [w for w in A["parts"] if w in DW] != list(DW):
        raise StoreError("store: display.waters is not a subsequence of the answers' parts")
    ords = [item_ord.get(w) for w in A["parts"]]
    if None in ords or ords != sorted(ords) or len(set(ords)) != len(ords):
        raise StoreError("store: the answers' waters are not the bundle's items in item order")
    eix = {e: i for i, e in enumerate(sub["entries"])}
    items, parts, xparts, pickers, runs = [], [], {}, {}, {}
    for wid, o in zip(A["parts"], ords):
        w = waters[wid]
        shown = wid in DW
        d = DW.get(wid)
        if shown and tuple(d) != DISPLAY_WATER_KEYS:
            raise StoreError(f"store: display.waters[{wid}] keys {list(d)} are not {DISPLAY_WATER_KEYS}")
        for k, t in (("name", str), ("kind", str), ("sections", int), ("outside_bc", int),
                     ("entries", list)):
            if type(w.get(k)) is not t:
                raise StoreError(f"store: export water {wid} {k} {w.get(k)!r} is not a {t.__name__}")
        if "steelhead" in w and w["steelhead"] not in STEELHEAD_CODE:
            raise StoreError(f"store: export water {wid} steelhead {w['steelhead']!r} has no code")
        try:
            ents = [eix[e] for e in w["entries"]]
        except KeyError as e:
            raise StoreError(f"store: export water {wid} names entry {e}, which the export lacks")
        rest = {k: v for k, v in w.items() if k not in WATER_COLUMNS}
        items.append((o, S.add(wid), int(shown),
                      pickers.setdefault(dumps(d["picker"]), len(pickers)) if shown else None,
                      dumps_s(d["unresolved_licensing"]) if shown else None,
                      w["name"], S.add(w["kind"]), w["sections"], w["outside_bc"], dumps_s(ents),
                      STEELHEAD_CODE.get(w.get("steelhead")), dumps(rest) if rest else None))
        ap, ep = A["parts"][wid], w["parts"]
        dp = d["parts"] if shown else [None] * len(ap)
        if not (len(ap) == len(ep) == len(dp)):
            raise StoreError(f"store: {wid}: {len(ap)} answers parts, {len(ep)} export parts, "
                             f"{len(dp)} display parts")
        for i, (k, p, q) in enumerate(zip(ap, ep, dp)):
            if not isinstance(p.get("sections"), int) or "runs" not in p:
                raise StoreError(f"store: {wid} part {i}: no export sections count or runs")
            xj = dumps({kk: v for kk, v in p.items() if kk not in ("sections", "runs")})
            xp = xparts.setdefault(xj, len(xparts))
            if q is None:
                cols = (None,) * 8
            else:
                if tuple(q) != DISPLAY_PART_KEYS:
                    raise StoreError(f"store: display part {wid}[{i}] keys {list(q)}")
                if type(q["order"]) is not int or type(q["closed_all_year"]) is not bool \
                        or not (q["km"] is None or type(q["km"]) is float):
                    raise StoreError(f"store: display part {wid}[{i}] has an untaught type")
                cols = (q["order"], S.add(q["label"]), S.add(q["runs"]), S.add(q["place"]),
                        S.add(q["hint"]), q["km"], int(q["closed_all_year"]),
                        dumps_s(q["paper_licence"]))
            parts.append((o, i, None if k is None else k + 1, *cols, xp, p["sections"],
                          runs.setdefault(dumps(p["runs"]), len(runs))))
    pending["item"] = items
    pending["xpart"] = [(i, j) for j, i in xparts.items()]
    pending["picker"] = [(i, j) for j, i in pickers.items()]
    pending["runs"] = [(i, j) for j, i in runs.items()]
    con.executemany("INSERT INTO part VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", parts)

    # ---- section_akey: every section a keyed named part covers
    by_ord = {o: wid for wid, o in item_ord.items()}
    blocks: Dict[int, bytearray] = {}
    owner: Dict[int, int] = {}
    named = 0
    for o, sid, pix in part_section:
        wid = by_ord[o]
        if wid not in A["parts"]:
            raise StoreError(f"store: bundle item {wid} owns sections but is not in the answers")
        k = A["parts"][wid][pix]
        if k is None:
            continue                                    # outside B.C.: no key, 0
        a = k + 1
        if a > 0xFFFF:
            raise StoreError(f"store: akey {a} does not fit a u16")
        if owner.get(sid, a) != a:
            raise StoreError(f"store: section {sid} is claimed by akeys {owner[sid]} and {a}")
        owner[sid] = a
        blk = blocks.setdefault(sid >> BLOCK_BITS, bytearray(2 * BLOCK))
        off = 2 * (sid & (BLOCK - 1))
        blk[off:off + 2] = a.to_bytes(2, "little")
        named += 1
    con.executemany("INSERT INTO section_akey VALUES (?, ?)",
                    [(i, bytes(blocks[i])) for i in sorted(blocks)])

    # ---- the export words
    rules = []
    for i, (rid, r) in enumerate(sub["rules"].items()):
        if rule_id_of(r) != rid or sub["rule_ids"][i] != rid:
            raise StoreError(f"store: rule id {rid!r} is not entry_id::rule_id in rule_ids order")
        rules.append((i, dumps(r)))
    lics = []
    for i, (lid, l) in enumerate(sub["licensing"].items()):
        if lic_id_of(l) != lid or sub["licensing_ids"][i] != lid:
            raise StoreError(f"store: licensing id {lid!r} is not entry_id#record_id in order")
        lics.append((i, dumps(l)))
    pending["rule"], pending["lic"] = rules, lics
    pending["entry"] = [(i, e, dumps(v)) for i, (e, v) in enumerate(sub["entries"].items())]
    rix = {k: i for i, k in enumerate(sub["rule_ids"])}
    lix = {k: i for i, k in enumerate(sub["licensing_ids"])}
    pending["ruleset"] = _sets(sub["rulesets"], rix, "rule set")
    pending["lset"] = _sets(sub["licensing_sets"], lix, "licensing set")

    # ---- every blob through the codec (a dictionary per table under 'zdict')
    for tbl, rows in pending.items():
        col = BLOB_COLUMNS[tbl]
        names = [c[1] for c in con.execute(f"PRAGMA table_info({tbl})")]
        at = names.index(col)
        zd = None
        if codec == "zdict":
            zd = train_zdict([r[at] for r in rows if r[at] is not None])
            con.execute("INSERT INTO zdict VALUES (?, ?)", (tbl, zd))
        packed = [tuple(pack(v, codec, zd) if j == at and v is not None else v
                        for j, v in enumerate(r)) for r in rows]
        con.executemany(f"INSERT INTO {tbl} VALUES ({','.join('?' * len(names))})", packed)

    con.executemany("INSERT INTO str VALUES (?, ?)", [(i, s) for s, i in S.ix.items()])

    meta = {
        "format": FORMAT,
        "generated_by": "python -m pipeline.deliver store",
        "section_handles": A["about"]["bundle"]["section_handles"],
        "reach_digest": A["about"]["bundle"]["reach_digest"],
        "rule_ids_sha256": A["about"]["export"]["rule_ids_sha256"],
        "about_bundle": dumps_s(A["about"]["bundle"]),
        "answers_sha256": sha(raw),
        "answers_bytes": str(len(raw)),
        "export_subset_sha256": sha(canonical(sub)),
        "blob_codec": codec,
        "layout": dumps_s(layout),
    }
    counts = {"akeys": len(nseg), "cells": len(cells), "items": len(items), "parts": len(parts),
              "named_sections": named, "section_blocks": len(blocks), "strings": len(S.ix),
              "rules": len(rules), "licensing": len(lics), "entries": len(pending["entry"]),
              **{tbl: len(secs[n][k]) for (n, k), tbl in FAMILIES.items()}}
    meta["counts"] = dumps_s(counts)
    con.executemany("INSERT INTO meta VALUES (?, ?)", sorted(meta.items()))
    return counts


def _sets(sets: dict, ix: Dict[str, int], what: str) -> list:
    """Rule / licensing sets with their member ids as integer refs; the set id an integer."""
    out = []
    for sid, v in sets.items():
        if not sid.isdigit() or str(int(sid)) != sid:
            raise StoreError(f"store: {what} id {sid!r} is not an integer")
        y = {}
        for k, x in v.items():
            if isinstance(x, list):
                try:
                    y[k] = [ix[m] for m in x]
                except KeyError as e:
                    raise StoreError(f"store: {what} {sid}.{k} names {e} (no such record)")
            else:
                y[k] = x
        out.append((int(sid), dumps(y)))
    out.sort()
    return out
