"""THE TROUT TABLE'S SIZE AND THE CONTRACT'S THREE COUNTS (trout table plan, ticket A0; contract 2.9). Reads one BUILT
release; changes nothing.

    python -m pipeline.tools.audit.trout_size <release dir> [--regions 5,6] [--no-size] [--out DIR]

THE THREE COUNTS (each non-zero count amends the contract before ticket F):

  mixed rows      (Q-D4) rows-frame rows whose fish are partly trout or char (`common.trout_char()`) and partly not.
                  The contract has every trout and char fish in the table and a covered row's members all trout or
                  char; a mixed row stops the build. Counted on the row's `members`, and on `members` + `all_members`.
  angler-only     (Q-D7) angler-only rules (`angler_closure`: closed to a class of angler) that close trout or char
                  alone: by the rule (its fish scope is trout or char only) and by the answers (a trout or char fish
                  whose decided answer is closed by such a rule).
  hours window    (should-fix 1, fight #4) a trout or char fish whose answer differs inside an hours window from
                  outside it, on the same day and weekday, with the water open at both moments: the table is drawn
                  from the outside moment, so the table must say it (ticket F). A water closed inside the window is
                  tallied apart (the banner's, not the table's).

THE SIZE ESTIMATE (prototype; ticket A1 measures the real cutover form after I and J): per region file, a CRUDE
table per cell built from today's rows frame (its trout rows as stored, the trout fish's decided answers as sheets,
the next dated change per fish as the date set), interned per family and packed with a trained dictionary as the
release packs a region table (`store.common.train_zdict` / `pack`). Three files are gzipped like the release's:
the region file as built (baseline), with the section added (`cell.trout` + four tables: the side release), and with
the trout `group` (and `elsewhere` on all-trout rows, Q-D9) also cut from `rows_row` (the cutover form). The crude
body restates every trout row whole, so it is an upper-side guess of the words the builder will write; it prices
the shape (cells, interning, dictionaries), not the wording.

Output: the report (stdout), and `report.json` + `report.txt` under `--out` (default
`data/generated/audit/trout-size/<release>`). Exit 0, or 2 when the release cannot be read.

TICKET A1, THE CUTOVER FORM MEASURED (contract 2.9 steps 2-6), on a SIDE release built with `--trout-table`:

    python -m pipeline.tools.audit.trout_size <side release> --cutover [--live <release>] [--out DIR]

The side release's regs store is put back together (`store.release.province_store`) and every region file is written
again BY THE RELEASE'S OWN WRITER (`release.build.copy_file` over `shards.shard_rows`), from three stores:
  side      the store as built: its region files must be the shipped bytes (checked; the run stops if not)
  flag off  the store without the section (`cell.trout` and the four tables gone): the release `--trout-table` off
            would ship at the same code and data (exact on the mini release: `test_trout_size.py`)
  cutover   the side store at rows 34 (ticket L): every rows frame's mixed rows SPLIT (`trout.table.split_mixed`, the
            covers it gives checked against the shipped `trout_frame` covers), `group` and `elsewhere` cut from every
            row some cell's `covers` holds (Q-D9), and F0's `page_possession` as each frame's sixth slot
            (`card.page_line` over the decoded frame, `trout.table.reading`); blobs packed with the release's own
            dictionaries (no retraining: contract 2.9 step 2, the app check's m5 method)
The water files (`water`, `water-shared`, the `water-<r>` slices) are priced side as shipped and flag off as shipped
less the section's parts: its four dictionaries, its schemas, spec and `about` entry, its layout and card-check stamp
lines (`water_flag_off`). The cutover form's water files are the side release's (rows 34's schema text, a few bytes,
is not priced). Every `WEB_BUDGET` moment at HEAD is priced on each form (`release.build.web_moments` over the
manifest with the form's sizes), with the live release's own as shipped beside them.
"""
from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
import sys
import tempfile
from bisect import bisect_right
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from pipeline.common.curated import REPO_ROOT
from pipeline.deliver.answers import common
from pipeline.tools.audit import answers_diff as AD



class SizeError(Exception):
    """The size estimate cannot price this release (T810 L: it is already the cutover form)."""

TC = common.trout_char()
#: the row fields a crude table body keeps (refs to rules and pools go to `src`): `full` every trout row whole (the
#: upper-side guess), `lean` only what today's card says in words or the table states (the group card, the number, the
#: weekday words, notes, the elsewhere line, stamps, the year strip; not the engine's conds / items / facts)
BODIES = {
    "full": ("kind", "members", "all_members", "daily", "everyone", "groups", "prot", "scope", "conds", "items",
             "real_daily", "group", "elsewhere", "presence", "week", "restates", "stamps", "year"),
    "lean": ("kind", "members", "daily", "group", "elsewhere", "presence", "week", "restates", "stamps", "year"),
}
#: rule refs of a row, gathered into the crude body's `src`
REF_KEYS = ("pool", "win", "narrow")
OUT_ROOT = REPO_ROOT / "data" / "generated" / "audit" / "trout-size"
EXAMPLES = 8


def _fish(row: dict, all_members: bool = True) -> set:
    return set(row.get("members") or ()) | (set(row.get("all_members") or ()) if all_members else set())


def _law(dec: Optional[dict]):
    """A decided answer as the law it states: status, the day's number, the size lines (rule refs left out)."""
    if dec is None:
        return None
    lines = tuple(sorted(json.dumps({k: v for k, v in L.items() if k not in ("r", "outer", "rules")}, sort_keys=True)
                         for L in dec.get("lines") or ()))
    return dec["status"], dec.get("daily"), lines


def trout_law(frame: dict) -> Dict[str, tuple]:
    """{trout/char fish: (hatchery law, wild law)} of one decoded rows frame."""
    return {f: (_law(x["hatchery"]), _law(x["wild"])) for f, x in frame["fish"].items() if f in TC}


def _say(law) -> str:
    if law is None:
        return "no answer"
    st, n, lines = law
    return f"keep {n}" + (" (size rule)" if lines else "") if st == "keep" else st


def window_words(h: Optional[dict]) -> str:
    """An hours window in words: "21:00 to 05:00", "60 min after sunset to 60 min before sunrise"."""
    if not h:
        return "the hours window"

    def end(x):
        if x.get("at"):
            return x["at"]
        m = x.get("offset_min") or 0
        return x.get("solar", "?") if not m else f"{abs(m)} min {'after' if m > 0 else 'before'} {x.get('solar', '?')}"
    return f"{end(h['start'])} to {end(h['end'])}"


def say(pair) -> str:
    """A fish's (hatchery, wild) law in a few words."""
    if pair is None:
        return "not asked"
    h, w = pair
    return _say(h) if h == w else f"hatchery {_say(h)}, wild {_say(w)}"


class Audit:
    """One built release, decoded once (`answers_diff.Release`, the reference decoders), with its export rules."""

    def __init__(self, d, log=print):
        from pipeline.deliver.store.release import province_store
        self.dir = Path(d)
        self.log = log
        self.R = AD.Release(self.dir)
        self.wire = self.R.wire
        self.rows = self.R.sections["rows"]
        self.display = self.R.sections["display"]
        st = province_store(self.dir)
        try:
            sub = st.export_subset(check=True)
        finally:
            st.close()
        self.rules: Dict[str, dict] = sub["rules"]
        self.groups: Dict[str, dict] = sub["species"]["groups"]
        self._frame: Dict[int, dict] = {}
        self._names: Dict[int, List[str]] = defaultdict(list)
        names = defaultdict(Counter)
        for h, a in self.R.akey.items():
            if a:
                names[a][self.R.where(h)[2]] += 1
        for a, c in names.items():
            self._names[a] = [n for n, _ in c.most_common()]

    # --- reading
    def frame(self, ref: int) -> dict:
        if ref not in self._frame:
            self._frame[ref] = self.R.codecs["rows"].decode_frame(self.rows, ref, self.R.rule_ids, self.R.fish)
        return self._frame[ref]

    def status(self, k: int, s: int) -> Optional[str]:
        return self.display["frames"][self.display["at"][k][s]].get("status")

    def name(self, akey: int) -> str:
        n = self._names.get(akey)
        return n[0] if n else f"answer key {akey}"

    def cells(self) -> Iterable[Tuple[int, int, int]]:
        """(key index, segment, rows frame id) of every cell."""
        for k, at in enumerate(self.rows["at"]):
            for s, ref in enumerate(at):
                yield k, s, ref

    def expand(self, codes) -> set:
        out = set()
        for c in codes or ():
            g = self.groups.get(c)
            out |= set(g["members"]) if g is not None else {c}
        return out

    # --- count 1 (Q-D4)
    def mixed_rows(self) -> dict:
        by_frame = defaultdict(set)                        # frame -> keys
        for k, s, ref in self.cells():
            by_frame[ref].add(k + 1)
        res = {}
        for label, wide in (("members", False), ("members+all_members", True)):
            pats: Dict[tuple, dict] = {}
            rows_seen = set()
            for ref, keys in sorted(by_frame.items()):
                for row in self.frame(ref)["rows"]:
                    f = _fish(row, wide)
                    t, o = f & TC, f - TC
                    if not (t and o):
                        continue
                    rid = json.dumps({k: v for k, v in row.items() if k != "year"}, sort_keys=True)
                    rows_seen.add(rid)
                    p = pats.setdefault((row["kind"], tuple(sorted(t)), tuple(sorted(o))),
                                        {"rows": set(), "frames": set(), "keys": set()})
                    p["rows"].add(rid)
                    p["frames"].add(ref)
                    p["keys"] |= keys
            all_keys = set().union(*(p["keys"] for p in pats.values())) if pats else set()
            res[label] = {
                "rows": len(rows_seen), "frames": len({f for p in pats.values() for f in p["frames"]}),
                "answer_keys": len(all_keys), "waters": len({self.name(a) for a in all_keys}),
                "by_kind": dict(Counter(kind for (kind, _, _), p in pats.items() for _ in p["rows"])),
                "other_fish": dict(Counter(x for (_, _, o), p in pats.items() for x in o for _ in p["rows"])
                                   .most_common()),
                "patterns": [{"kind": kind, "trout": list(t), "other": list(o), "rows": len(p["rows"]),
                              "answer_keys": len(p["keys"]),
                              "waters": sorted({self.name(a) for a in p["keys"]})[:EXAMPLES]}
                             for (kind, t, o), p in sorted(pats.items(), key=lambda x: (-len(x[1]["keys"]), x[0]))],
            }
        return res

    # --- count 2 (Q-D7)
    def angler_only(self) -> dict:
        ang = {rid: r for rid, r in self.rules.items() if r.get("type") == "angler_closure"}
        scoped, alone = [], []
        for rid, r in sorted(ang.items()):
            fish = self.expand((r.get("fields") or {}).get("species"))
            if fish:
                scoped.append(rid)
                if fish <= TC:
                    alone.append(rid)
        ix = {i for i, rid in enumerate(self.R.rule_ids) if rid in ang}
        hits: Dict[str, set] = defaultdict(set)            # rule -> keys where it closes a trout fish
        frames = defaultdict(set)
        for k, s, ref in self.cells():
            frames[ref].add(k + 1)
        for ref, keys in frames.items():
            for f, x in self.frame(ref)["fish"].items():
                if f not in TC:
                    continue
                for o in ("hatchery", "wild"):
                    d = x[o]
                    if d is not None and d["status"] == "closed" and d["win"] in ix:
                        hits[self.R.rule_ids[d["win"]]] |= keys
        return {"angler_rules": len(ang), "name_a_fish": len(scoped), "close_trout_alone": len(alone),
                "rules_closing_trout_alone": alone,
                "answers_closed_by_one": {rid: {"answer_keys": len(ks),
                                                "waters": sorted({self.name(a) for a in ks})[:EXAMPLES]}
                                          for rid, ks in sorted(hits.items())},
                "labels": dict(Counter(r.get("label", "")[:70] for r in ang.values()).most_common())}

    # --- count 3 (should-fix 1, #4)
    def hours_window(self) -> dict:
        trout, closed = defaultdict(list), defaultdict(list)
        for k, key in enumerate(self.wire["keys"]):
            if key[10] is None:
                continue
            starts = self.wire["segments"][key[9]]
            moms = [self.wire["moments"][i] for i in self.wire["segment_moments"][key[10]]]
            if not any(m["hours"] for m in moms):
                continue
            seen = set()
            for s0, start in enumerate(starts):
                if start in seen:
                    continue
                seen.add(start)
                group = [i for i, x in enumerate(starts) if x == start]
                end = starts[bisect_right(starts, start)] - 1 if bisect_right(starts, start) < len(starts) else 366
                for w in AD.WEEKDAYS:
                    def pick(inside):
                        hit = [i for i in group if w in moms[i]["weekdays"]
                               and (moms[i]["hours"] is None or moms[i]["hours"]["in"] == inside)]
                        return hit[0] if hit else None
                    o, i = pick(False), pick(True)
                    if o is None or i is None or o == i:
                        continue
                    so, si = self.status(k, o), self.status(k, i)
                    if so in ("closed", "tidal"):
                        continue                           # no table outside: nothing to draw
                    if si in ("closed", "tidal"):
                        closed[k + 1].append((start, end, w))
                        continue
                    lo, li = trout_law(self.frame(self.rows["at"][k][o])), trout_law(self.frame(self.rows["at"][k][i]))
                    diff = {f: (say(lo.get(f)), say(li.get(f))) for f in sorted(set(lo) | set(li))
                            if lo.get(f) != li.get(f)}
                    if diff:
                        trout[k + 1].append((start, end, w, diff, moms[i]["hours"]))

        def summary(d, with_fish):
            out = []
            for a, ev in sorted(d.items(), key=lambda x: (-len(x[1]), x[0])):
                days = sorted({(e[0], e[1]) for e in ev})
                mask = 0
                for lo, hi in days:
                    mask |= AD.mask_of(lo, hi)
                x = {"answer_key": a, "water": self.name(a), "days": AD.days_words(mask),
                     "weekdays": len({e[2] for e in ev})}
                if with_fish:
                    x["fish"] = sorted({f"{f}: {b} outside, {c} inside {window_words(e[4])}"
                                        for e in ev for f, (b, c) in e[3].items()})
                out.append(x)
            return out
        return {"answer_keys": len(trout), "waters": len({self.name(a) for a in trout}),
                "fish": dict(Counter(f for ev in trout.values() for f in {f for e in ev for f in e[3]})),
                "examples": summary(trout, True)[:EXAMPLES * 2],
                "water_closed_inside": {"answer_keys": len(closed), "examples": summary(closed, False)[:EXAMPLES]}}

    # --- the size estimate
    def crude(self, k: int, s: int, ref: int, starts, moms_of,
              keys: Tuple[str, ...]) -> Optional[Tuple[dict, list, Optional[dict], list]]:
        """(body, sheets, dates, covers) of a crude trout table at (k, s); None where the cell is NULL (Q-D3)."""
        if self.status(k, s) in ("closed", "tidal"):
            return None
        fr = self.frame(ref)
        fish = sorted(f for f in fr["fish"] if f in TC)
        if not fish:
            return None
        covers = [i for i, r in enumerate(fr["rows"]) if _fish(r) & TC]
        rows = [{x: r[x] for x in keys if r.get(x) is not None} for r in (fr["rows"][i] for i in covers)]
        src = sorted({r[x] for r in (fr["rows"][i] for i in covers) for x in REF_KEYS if r.get(x) is not None})
        sheets = [{"f": f, "h": fr["fish"][f]["hatchery"], "w": fr["fish"][f]["wild"], "lead": fr["fish"][f]["lead"]}
                  for f in fish]
        # the next dated change of each fish: the first later segment, at the same moment, whose law differs
        now = trout_law(fr)
        nxt = []
        for f in fish:
            for j in range(s + 1, len(starts)):
                if starts[j] == starts[s] or moms_of(j) != moms_of(s):
                    continue
                later = trout_law(self.frame(self.rows["at"][k][j])).get(f)
                if later != now[f]:
                    nxt.append([f, starts[j], say(later)])
                    break
        return {"rows": rows, "src": src}, sheets, ({"next": nxt} if nxt else None), covers

    def _section(self, cells, keys) -> Tuple[list, Dict[str, list], dict, int]:
        """(the `cell.trout` column, {table: packed blobs}, per-table figures, NULL cells) of one region's cells."""
        from pipeline.deliver.store.common import dumps, pack, train_zdict
        fams: Dict[str, Dict[bytes, int]] = {f: {} for f in ("trout_table", "trout_dates", "trout_sheet", "trout_frame")}

        def intern(fam, v):
            return fams[fam].setdefault(dumps(v), len(fams[fam]))
        col, nulls = [], 0
        for akey, s, ref in cells:
            k = akey - 1
            key = self.wire["keys"][k]
            sm = None if key[10] is None else self.wire["segment_moments"][key[10]]
            got = self.crude(k, s, ref, self.wire["segments"][key[9]],
                             (lambda j: None) if sm is None else (lambda j, sm=sm: sm[j]), keys)
            if got is None:
                nulls += 1
                col.append((None, akey, s))
                continue
            body, sheets, dates, covers = got
            body["sheets"] = [intern("trout_sheet", x) for x in sheets]
            tid = intern("trout_table", body)
            did = None if dates is None else intern("trout_dates", dates)
            col.append((intern("trout_frame", [tid, did, covers]), akey, s))
        packed, figs = {}, {}
        for fam, blobs in fams.items():
            bs = sorted(blobs, key=blobs.get)
            zd = train_zdict(bs) if fam != "trout_frame" else None
            packed[fam] = [pack(b, "zdict" if zd else "json", zd) for b in bs]
            figs[fam] = {"distinct": len(bs), "raw": sum(len(b) for b in bs), "packed": sum(map(len, packed[fam])),
                         "zdict_gz": len(gzip.compress(zd, 9, mtime=0)) if zd else 0}
        return col, packed, figs, nulls

    def _cutover(self, con, zd_row) -> Tuple[list, dict]:
        """The `rows_row` blobs with the trout `group` (and `elsewhere` on all-trout rows, Q-D9) cut out."""
        from pipeline.deliver.store.common import dumps, pack, unpack
        new, freed, kept, differs = [], 0, 0, 0
        for rid, j in con.execute("SELECT id, j FROM rows_row ORDER BY id").fetchall():
            v = json.loads(unpack(j, zd_row))
            was = pack(dumps(v), "zdict", zd_row)
            differs += was != bytes(j)
            g = v.get("group")
            trout_group = g is not None and set(g["kinds"]) <= TC
            kept += g is not None and not trout_group
            cut = {x: y for x, y in v.items()
                   if not ((x == "group" and trout_group) or (x == "elsewhere" and _fish(v) and _fish(v) <= TC))}
            if cut != v:
                nb = pack(dumps(cut), "zdict", zd_row)
                freed += len(was) - len(nb)
                new.append((nb, rid))
        return new, {"rows": len(new), "packed_bytes_freed": freed, "other_fish_groups_kept": kept,
                     "repack_differs": differs}

    def size(self, regions: Iterable[str]) -> dict:
        from pipeline.deliver.store.release import province_store
        if "trout" in ((self.R.manifest.get("regulations") or {}).get("versions") or {}):
            # (T810 L) the estimate adds the section and cuts the trout groups: a release built since the cutover IS that
            # form, and the release's own size gates price it (A1 measured it); nothing is left to estimate
            raise SizeError(f"trout_size: {self.dir.name} already holds the trout section (the cutover form): the "
                            "estimate prices a release from before the cutover; use --no-size")
        st = province_store(self.dir)
        zd_row = st.zd.get("rows_row")
        st.close()

        def gz(path: Path) -> int:
            return len(gzip.compress(path.read_bytes(), 9, mtime=0))
        out = {}
        for r in regions:
            name = f"regs-{r}"
            raw = AD._file_bytes(self.dir, self.R.manifest, name)
            res = {"manifest_gz": self.R.manifest["files"][name]["gz"], "baseline_gz": len(gzip.compress(raw, 9, mtime=0))}
            with tempfile.TemporaryDirectory(prefix="trout-size-") as tmp:
                for variant, keys in BODIES.items():
                    p = Path(tmp) / f"{name}.{variant}.sqlite"
                    p.write_bytes(raw)
                    con = sqlite3.connect(str(p))
                    cells = con.execute("SELECT akey, s, rows FROM cell ORDER BY akey, s").fetchall()
                    col, packed, figs, nulls = self._section(cells, keys)
                    con.execute("ALTER TABLE cell ADD COLUMN trout INTEGER")
                    con.executemany("UPDATE cell SET trout = ? WHERE akey = ? AND s = ?", col)
                    for fam, bs in packed.items():
                        con.execute(f"CREATE TABLE {fam} (id INTEGER PRIMARY KEY, j BLOB NOT NULL)")
                        con.executemany(f"INSERT INTO {fam} (id, j) VALUES (?, ?)", list(enumerate(bs)))
                    con.commit()
                    con.execute("VACUUM")
                    side = gz(p)
                    new, cut = self._cutover(con, zd_row)
                    con.executemany("UPDATE rows_row SET j = ? WHERE id = ?", new)
                    con.commit()
                    con.execute("VACUUM")
                    cutover = gz(p)
                    con.close()
                    res.update(cells=len(cells), null_cells=nulls, rows_row_cut=cut)
                    res[variant] = {"families": figs, "section_packed": sum(f["packed"] for f in figs.values()),
                                    "zdicts_gz": sum(f["zdict_gz"] for f in figs.values()),
                                    "side_gz": side, "cutover_gz": cutover,
                                    "side_delta": side - res["baseline_gz"], "cutover_delta": cutover - res["baseline_gz"]}
            out[name] = res
        return out


# ============================================================================================
# Ticket A1: the cutover form, measured (contract 2.9 steps 2-6)
# ============================================================================================

#: Q-D9: the fields a covered trout row loses at rows 34
CUT_FIELDS = ("group", "elsewhere")
TOP_N = 10


def _trout_tables() -> Tuple[str, ...]:
    # (T810 L) the section's tables: required in the contract since the cutover, an ADDED section of the store
    from pipeline.deliver.release import shards as SH
    return tuple(sorted(SH.ADDED_SHARD_TABLES["trout"]))


def _gz(b: bytes) -> int:
    return len(gzip.compress(b, 9, mtime=0))


def _redump(text: str, value) -> str:
    """`value` as JSON in the formatting `text` was written in (the release's meta values are written by more than one
    `json.dumps`); refuses a text none of them reproduces."""
    styles = ({}, {"sort_keys": True}, {"separators": (",", ":")}, {"separators": (",", ":"), "sort_keys": True},
              {"separators": (",", ":"), "ensure_ascii": False}, {"separators": (",", ":"), "sort_keys": True,
                                                                     "ensure_ascii": False})
    for kw in styles:
        if json.dumps(json.loads(text), **kw) == text:
            return json.dumps(value, **kw)
    raise ValueError(f"trout_size: no JSON style writes {text[:60]!r}…")


def _no_trout_layout(text: str) -> str:
    v = json.loads(text)
    for k in ("sections", "versions"):
        if isinstance(v.get(k), dict):
            v[k].pop("trout", None)
    return _redump(text, v)


def _no_trout_stamp(text: str) -> str:
    v = json.loads(text)
    v.pop("trout_check", None)
    return _redump(text, v)


def _no_trout_meta(meta: Dict[str, str]) -> Dict[str, str]:
    """A water file's meta as the flag-off build writes it: no `trout` in the layout, no trout line in the card-check
    stamp, and the same in the store meta it carries whole."""
    out = dict(meta)
    if "layout" in out:
        out["layout"] = _no_trout_layout(out["layout"])
    if "card_check" in out:
        out["card_check"] = _no_trout_stamp(out["card_check"])
    if "store_meta" in out:
        sm = json.loads(out["store_meta"])
        if "layout" in sm:
            sm["layout"] = _no_trout_layout(sm["layout"])
        if "card_check" in sm:
            sm["card_check"] = _no_trout_stamp(sm["card_check"])
        out["store_meta"] = _redump(out["store_meta"], sm)
    return out


def water_flag_off(raw: bytes) -> bytes:
    """A water file (`water`, `water-shared`, a `water-<r>` slice) of a side release as the flag-off build ships it: the
    section's four dictionaries gone, `answers.about` / `spec` / `schemas` without it (`encode.spec_for`,
    `model.json_schemas`), its meta lines gone (`_no_trout_meta`). VACUUMed as the release finishes a file."""
    from pipeline.deliver.answers.encode import spec_for
    from pipeline.deliver.answers.model import json_schemas
    from pipeline.deliver.store.common import pack, unpack
    with tempfile.TemporaryDirectory(prefix="trout-size-w-") as tmp:
        p, q = Path(tmp) / "w.sqlite", Path(tmp) / "w2.sqlite"
        p.write_bytes(raw)
        con = sqlite3.connect(str(p))
        tabs = {t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        zd: Dict[str, bytes] = {}
        if "zdict" in tabs:
            zd = dict(con.execute("SELECT tbl, d FROM zdict"))
            con.execute("DELETE FROM zdict WHERE tbl IN (%s)" % ",".join("?" * len(_trout_tables())), _trout_tables())
        if "static" in tabs:
            got = dict(con.execute("SELECT k, j FROM static WHERE k IN ('answers.about', 'answers.spec', "
                                   "'answers.schemas')"))
            if got:
                zs = zd.get("static")
                about = json.loads(unpack(got["answers.about"], zs))
                about["sections"].pop("trout", None)
                secs = list(about["sections"])
                for k, v in (("answers.about", about), ("answers.spec", spec_for(secs)),
                             ("answers.schemas", json_schemas(secs))):
                    con.execute("UPDATE static SET j = ? WHERE k = ?",
                                (pack(json.dumps(v, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
                                      "zdict", zs), k))
        meta = dict(con.execute("SELECT k, v FROM meta"))
        for k, v in _no_trout_meta(meta).items():
            if v != meta[k]:
                con.execute("UPDATE meta SET v = ? WHERE k = ?", (v, k))
        con.commit()
        con.execute(f"VACUUM INTO '{q}'")
        con.close()
        return q.read_bytes()


def _region_meta(raw: bytes) -> Dict[str, str]:
    with tempfile.TemporaryDirectory(prefix="trout-size-m-") as tmp:
        p = Path(tmp) / "r.sqlite"
        p.write_bytes(raw)
        con = sqlite3.connect(str(p))
        out = dict(con.execute("SELECT k, v FROM meta"))
        con.close()
    return out


def region_ids(store_path: Path, r: str) -> Dict[str, set]:
    """{table: the ids region `r`'s file holds} (`shards.shard_rows`, as the release cuts them)."""
    from pipeline.deliver.release import shards as SH
    con = sqlite3.connect(str(store_path))
    try:
        mine = [a for a, rs in SH.akey_regions(con).items() if r in rs]
    finally:
        con.close()
    return SH.shard_rows(store_path, mine)


def region_file(store_path: Path, r: str, meta: Dict[str, str], out: Path, ids=None) -> bytes:
    """Region `r`'s file written from `store_path` by the release's own writer (`build.copy_file`, the plan of
    `release.build.build`); `ids` the shard's rows when they were cut before the store changed."""
    from pipeline.deliver.release import build as B
    from pipeline.deliver.release import shards as SH
    ids = region_ids(store_path, r) if ids is None else ids
    con = sqlite3.connect(str(store_path))
    try:
        # an ADDITIVE table (`store.release.ADDITIVE_TABLES`) a release cut before it does not hold is not written
        held = {t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        tables = [t for t in SH.shard_tables(con) if t in held]
    finally:
        con.close()
    plan = [("s", t, f"WHERE {'akey' if t == 'cell' else 'id'} IN ({','.join(map(str, sorted(ids[t]))) or 'NULL'})")
            for t in tables]
    B.copy_file(out, {"s": store_path}, plan, meta)
    return out.read_bytes()


def flag_off_store(side: Path, out: Path) -> Path:
    """The side store without the section: a store made from `schema.sql` alone (no `added_schema`), every table
    copied but the four trout tables, `cell` without its `trout` column, the trout dictionaries dropped."""
    from pipeline.deliver.store.build import SCHEMA
    con = sqlite3.connect(str(out))
    con.executescript(SCHEMA.read_text())
    con.execute("ATTACH ? AS s", (str(side),))
    have = {t for (t,) in con.execute("SELECT name FROM s.sqlite_master WHERE type = 'table'")}
    for (t,) in con.execute("SELECT name FROM main.sqlite_master WHERE type = 'table'").fetchall():
        if t in have:
            cols = ", ".join(c[1] for c in con.execute(f"PRAGMA main.table_info({t})"))
            con.execute(f"INSERT INTO main.{t} ({cols}) SELECT {cols} FROM s.{t}")
    # a table the side store holds and schema.sql does not (ADDITIVE_TABLES dropped by the reassembly aside) is the
    # section's, or the run is measuring something else
    extra = sorted(have - {t for (t,) in con.execute("SELECT name FROM main.sqlite_master WHERE type = 'table'")})
    if extra != sorted(_trout_tables()):
        raise ValueError(f"trout_size: the side store holds tables {extra} beyond the flag-off schema")
    con.execute("DELETE FROM main.zdict WHERE tbl IN (%s)" % ",".join("?" * len(_trout_tables())), _trout_tables())
    from pipeline.deliver.store.release import ADDITIVE_TABLES
    for t in sorted(ADDITIVE_TABLES - have):              # as the reassembly leaves a table the release did not hold
        con.execute(f"DROP TABLE main.{t}")
    con.commit()
    con.execute("DETACH s")
    con.close()
    return out


class Cutover:
    """The side store at rows 34 (module doc, "cutover"): built by `build(out)`, which returns the figures."""

    def __init__(self, side: Path):
        from pipeline.deliver.answers.trout import table as TT
        from pipeline.deliver.store.decode import Store
        self.side = side
        self.st = Store(side, verify=False)
        self.zd = self.st.zd
        self.rules = TT.Rules.from_store(self.st)
        self.rix = {r.key: i for i, r in self.rules.by_ref.items()}

    def _val(self, t, b):
        return self.st.val(t, b)

    @staticmethod
    def five_of(out: Path) -> Path:
        """Where `build(out)` leaves the cutover store before F0's sixth slot (split and cut only)."""
        return out.with_name(out.stem + ".five" + out.suffix)

    def build(self, out: Path) -> Tuple[dict, Dict[str, Dict[str, set]], Dict[int, list]]:
        """Writes the cutover store to `out` (and, before F0's slot, to `five_of(out)`) and returns (figures, {region:
        shard ids}, {rows frame id: its six-slot frame}). The shards are cut while the frames have five slots: the
        shard rule reads exactly five (`shards.frame_refs`, which L must teach the sixth) and the sixth reaches no
        row, so the cut is the same."""
        import shutil
        from pipeline.deliver.answers import card
        from pipeline.deliver.answers.trout import table as TT
        from pipeline.deliver.release import shards as SH
        from pipeline.deliver.store.common import dumps, pack
        con = self.st.con
        rows = {i: self._val("rows_row", b) for i, b in con.execute("SELECT id, j FROM rows_row")}
        decided = {i: self._val("rows_decided", b) for i, b in con.execute("SELECT id, j FROM rows_decided")}
        frames = {i: self._val("rows_frame", b) for i, b in con.execute("SELECT id, j FROM rows_frame")}
        tframes = {i: self._val("trout_frame", b) for i, b in con.execute("SELECT id, j FROM trout_frame")}
        shipped_covers: Dict[int, set] = defaultdict(set)
        for rf, tf in con.execute("SELECT rows, trout FROM cell WHERE trout IS NOT NULL"):
            shipped_covers[rf].add(tuple(tframes[tf][2]))
        # 1. every frame split (A0 amendment), its covers checked against the shipped ones
        split: Dict[int, list] = {}
        covered_content: set = set()
        cover_mismatch, mixed_split = [], 0
        for fid, fr in sorted(frames.items()):
            spp, fish, rs, line, ys = fr
            got, covers = TT.split_mixed({"rows": [{**rows[i], "_y": y} for i, y in zip(rs, ys)]})
            mixed_split += len(got["rows"]) - len(rs)
            sc = shipped_covers.get(fid)
            if sc is not None:
                if sc != {tuple(covers)}:
                    cover_mismatch.append(fid)
                for i in covers:
                    covered_content.add(dumps({k: v for k, v in got["rows"][i].items() if k != "_y"}))
            split[fid] = got["rows"]
        # 2. the covered rows cut, every row interned again (an unchanged row keeps its id)
        by_content = {dumps(v): i for i, v in sorted(rows.items(), reverse=True)}
        nxt = max(rows, default=-1) + 1
        new_rows: Dict[int, dict] = {}
        new_frames: Dict[int, list] = {}
        cut_rows, groups_cut, elsewhere_cut = set(), 0, 0
        for fid, rlist in split.items():
            spp, fish, rs, line, ys = frames[fid]
            rids, yids = [], []
            for r in rlist:
                y = r.pop("_y")
                key = dumps(r)
                if key in covered_content:
                    groups_cut += r.get("group") is not None
                    elsewhere_cut += r.get("elsewhere") is not None
                    r = {k: v for k, v in r.items() if k not in CUT_FIELDS}
                    key = dumps(r)
                    cut_rows.add(key)
                rid = by_content.get(key)
                if rid is None:
                    rid = by_content[key] = nxt
                    nxt += 1
                new_rows[rid] = r
                rids.append(rid)
                yids.append(y)
            new_frames[fid] = [spp, fish, rids, line, yids]
        # 3. the store: rows_row and rows_frame replaced (five slots), packed with the release's dictionaries
        shutil.copyfile(self.side, out)
        oc = sqlite3.connect(str(out))
        zr, zf = self.zd.get("rows_row"), self.zd.get("rows_frame")
        repack_differs = sum(pack(dumps(v), "zdict", zr) != bytes(b)
                             for i, b in con.execute("SELECT id, j FROM rows_row") for v in (rows[i],))
        oc.execute("DELETE FROM rows_row")
        oc.executemany("INSERT INTO rows_row (id, j) VALUES (?, ?)",
                       [(i, pack(dumps(v), "zdict", zr)) for i, v in sorted(new_rows.items())])
        oc.executemany("UPDATE rows_frame SET j = ? WHERE id = ?",
                       [(pack(dumps(v), "zdict", zf), i) for i, v in sorted(new_frames.items())])
        oc.commit()
        oc.close()
        ids = {r: region_ids(out, r) for r in SH.REGIONS}
        shutil.copyfile(out, self.five_of(out))           # the form without F0's slot, to price that slot apart
        # 4. F0's sixth slot: the page's possession row, over each frame as decoded
        six, with_line = {}, 0
        for fid, fr in sorted(new_frames.items()):
            spp, fish, rids, line, yids = fr
            dec = {"fish": {S: {"hatchery": None if h is None else decided[h], "wild": None if w is None else decided[w],
                                "lead": lead} for S, (h, w, lead) in fish.items()}}
            p = card.page_line(*TT.reading(dec, self.rules))
            if p is not None:
                with_line += 1
                p = {"lead": None if p["lead"] is None else {**p["lead"], "src": self.rix[p["lead"]["src"]]},
                     "also": [{**a, "src": self.rix[a["src"]]} for a in p["also"]]}
            six[fid] = fr + [p]
        oc = sqlite3.connect(str(out))
        oc.executemany("UPDATE rows_frame SET j = ? WHERE id = ?",
                       [(pack(dumps(v), "zdict", zf), i) for i, v in sorted(six.items())])
        oc.commit()
        oc.close()
        figs = {"frames": len(frames), "frames_with_a_table": len(shipped_covers),
                "cover_mismatch": cover_mismatch, "mixed_rows_split": mixed_split,
                "rows_before": len(rows), "rows_after": len(new_rows), "rows_cut": len(cut_rows),
                "groups_cut": groups_cut, "elsewhere_cut": elsewhere_cut, "repack_differs": repack_differs,
                "frames_with_a_possession_row": with_line}
        return figs, ids, six


def _family_figures(con: sqlite3.Connection, zd: Dict[str, bytes], tables) -> Dict[str, dict]:
    from pipeline.deliver.store.common import unpack
    out = {}
    for t in tables:
        rows = con.execute(f"SELECT j FROM {t}").fetchall()
        out[t] = {"distinct": len(rows), "raw": sum(len(unpack(b, zd.get(t))) for (b,) in rows),
                  "stored": sum(len(b) for (b,) in rows)}
    return out


def _moments(release: Path, sizes: Dict[str, int]) -> Dict[str, dict]:
    """Every `WEB_BUDGET` moment at HEAD (`release.build.web_moments`) over `release`'s manifest with the files' gzip
    sizes `sizes` put in; the manifest itself priced at its shipped size."""
    from pipeline.deliver.release import build as B
    from pipeline.deliver.release import contract as C
    shipped = (Path(release) / "manifest.json").read_bytes()
    m = json.loads(shipped)
    for k, n in sizes.items():
        m["files"][k]["gz"] = n
    with tempfile.TemporaryDirectory(prefix="trout-size-mo-") as tmp:
        p = Path(tmp) / "manifest.json"
        p.write_text(json.dumps(m), encoding="utf-8")
        fix = len(shipped) - p.stat().st_size
        got = B.web_moments(Path(tmp))
    out = {}
    for k, (n, f) in got.items():
        n += fix if "manifest" in f else 0
        out[k] = {"used": n, "gate": C.WEB_BUDGET[k]["gate"], "room": C.WEB_BUDGET[k]["gate"] - n, "files": f}
    return out


def _offline(manifest: dict, sizes: Dict[str, int]) -> int:
    """The offline total (`build.gate_problems`: every file but the web-only slices)."""
    from pipeline.deliver.release import contract as C
    from pipeline.deliver.release import slices as SLICES
    skip = set(SLICES.slice_marks()) | set(C.WEB_ONLY)
    return sum(sizes.get(k, int(f["gz"])) for k, f in manifest["files"].items() if k not in skip)


def _largest(st, akey_names: Dict[int, List[str]]) -> dict:
    """The ten largest table bodies and fish sheets as stored, each with the waters (and, for a sheet, the fish) it
    is shown for."""
    con = st.con
    tf_of_table: Dict[int, set] = defaultdict(set)
    for i, b in con.execute("SELECT id, j FROM trout_frame"):
        tf_of_table[st.val("trout_frame", b)[0]].add(i)
    keys_of_tf: Dict[int, set] = defaultdict(set)
    for a, tf in con.execute("SELECT akey, trout FROM cell WHERE trout IS NOT NULL"):
        keys_of_tf[tf].add(a)
    sheet_of: Dict[int, set] = defaultdict(set)              # sheet -> (table, fish name)
    bodies = []
    for i, b in con.execute("SELECT id, j FROM trout_table"):
        v = st.val("trout_table", b)
        keys = set().union(*(keys_of_tf[f] for f in tf_of_table[i])) if tf_of_table[i] else set()
        for bx in v["boxes"]:
            for r in bx["rows"]:
                sheet_of[r["sheet"]].add((i, r["name"]))
        bodies.append({"id": i, "stored": len(b), "raw": len(json.dumps(v, ensure_ascii=False, separators=(",", ":"))
                                                              .encode("utf-8")),
                       "rows": sum(len(bx["rows"]) for bx in v["boxes"]), "boxes": len(v["boxes"]),
                       "answer_keys": len(keys), "keys": keys})
    by_table = {x["id"]: x for x in bodies}

    def waters(keys) -> List[str]:
        c = Counter(n for a in keys for n in akey_names.get(a, [])[:1])
        return [n for n, _ in c.most_common(4)]
    sheets = []
    for i, b in con.execute("SELECT id, j FROM trout_sheet"):
        v = st.val("trout_sheet", b)
        uses = sheet_of.get(i, set())
        keys = set().union(*(by_table[t]["keys"] for t, _ in uses)) if uses else set()
        sheets.append({"id": i, "stored": len(b), "raw": len(json.dumps(v, ensure_ascii=False, separators=(",", ":"))
                                                              .encode("utf-8")),
                       "fish": sorted({n for _, n in uses}), "lines": len(v.get("here") or ()), "head": v.get("head"),
                       "answer_keys": len(keys), "keys": keys})
    out = {}
    for name, xs in (("bodies", bodies), ("sheets", sheets)):
        top = sorted(xs, key=lambda x: (-x["stored"], x["id"]))[:TOP_N]
        out[name] = [{**{k: v for k, v in x.items() if k != "keys"}, "waters": waters(x["keys"])} for x in top]
    return out


def measure(side: Path, live: Optional[Path] = None, log=print) -> dict:
    """Ticket A1 (module doc): the side release `side` in its three forms, every region file and water file priced,
    every web moment at HEAD; `live` the release beside it, as shipped."""
    from pipeline.deliver.release import build as B
    from pipeline.deliver.release import shards as SH
    from pipeline.deliver.store.release import province_store
    side = Path(side)
    man = AD._manifest(side)
    if "trout" not in ((man.get("regulations") or {}).get("versions") or {}):
        raise AD.DiffError(f"trout_size: {side.name} carries no trout section (build it with --trout-table)")
    R = AD.Release(side)
    names = defaultdict(Counter)
    for h, a in R.akey.items():
        if a:
            names[a][R.where(h)[2]] += 1
    akey_names = {a: [n for n, _ in c.most_common()] for a, c in names.items()}
    rep: dict = {"release": R.name, "live": None, "regions": {}, "water": {}}
    with tempfile.TemporaryDirectory(prefix="trout-size-a1-") as tmp:
        t = Path(tmp)
        st = province_store(side)
        sp = t / "side.sqlite"
        st.con.execute(f"VACUUM INTO '{sp}'")
        st.close()
        op = flag_off_store(sp, t / "off.sqlite")
        log("trout_size: the side store and its flag-off twin written; building the cutover form")
        cut = Cutover(sp)
        cp = t / "cutover.sqlite"
        figs, cut_ids, _six = cut.build(cp)
        rep["cutover"] = figs
        if figs["cover_mismatch"]:
            raise AD.DiffError(f"trout_size: {len(figs['cover_mismatch'])} rows frames split to other covers than "
                               f"their trout frames ship (frames {figs['cover_mismatch'][:5]})")
        rep["largest"] = _largest(cut.st, akey_names)
        zd = cut.st.zd
        rep["families"] = _family_figures(cut.st.con, zd, _trout_tables())
        rep["zdicts"] = {tb: {"raw": len(zd[tb]), "gz": _gz(zd[tb])} for tb in _trout_tables() if tb in zd}
        cut.st.close()
        sizes = {"side": {}, "off": {}, "cutover": {}}
        for r in SH.REGIONS:
            name = f"regs-{r}"
            shipped = AD._file_bytes(side, man, name)
            meta = _region_meta(shipped)
            b_side = region_file(sp, r, meta, t / f"{name}.side.sqlite")
            if b_side != shipped:
                raise AD.DiffError(f"trout_size: {name} written again from the reassembled store is not the shipped file")
            b_off = region_file(op, r, meta, t / f"{name}.off.sqlite")
            b_cut = region_file(cp, r, meta, t / f"{name}.cut.sqlite", ids=cut_ids[r])
            b_five = region_file(Cutover.five_of(cp), r, meta, t / f"{name}.five.sqlite", ids=cut_ids[r])
            fc = sqlite3.connect(str(t / f"{name}.side.sqlite"))
            fam = _family_figures(fc, zd, _trout_tables())
            cells = fc.execute("SELECT count(*), count(trout) FROM cell").fetchone()
            rr = fc.execute("SELECT count(*), sum(length(j)) FROM rows_row").fetchone()
            fc.close()
            cc = sqlite3.connect(str(t / f"{name}.cut.sqlite"))
            rr_cut = cc.execute("SELECT count(*), sum(length(j)) FROM rows_row").fetchone()
            rf = (cc.execute("SELECT sum(length(j)) FROM rows_frame").fetchone()[0],)
            cc.close()
            oc = sqlite3.connect(str(t / f"{name}.off.sqlite"))
            rf_off = oc.execute("SELECT sum(length(j)) FROM rows_frame").fetchone()[0]
            oc.close()
            g = {"side": _gz(b_side), "off": _gz(b_off), "cutover": _gz(b_cut)}
            for k in sizes:
                sizes[k][name] = g[k]
            rep["regions"][name] = {
                "live_gz": None, **{f"{k}_gz": v for k, v in g.items()},
                "section_stored": sum(f["stored"] for f in fam.values()), "families": fam,
                "cells": cells[0], "cells_with_a_table": cells[1],
                "rows_row": {"side": {"rows": rr[0], "stored": rr[1]}, "cutover": {"rows": rr_cut[0], "stored": rr_cut[1]}},
                "rows_frame_stored": {"off": rf_off, "cutover": rf[0]},
                "side_added": g["side"] - g["off"], "cutover_added": g["cutover"] - g["off"],
                "groups_freed": g["side"] - g["cutover"],
                # F0's page possession row, priced apart: the cutover form with and without the sixth slot
                "cutover_without_f0_gz": _gz(b_five), "f0_cost": g["cutover"] - _gz(b_five)}
        for name in sorted(k for k in man["files"] if k == "water" or k.startswith("water-")):
            raw = AD._file_bytes(side, man, name)
            off = water_flag_off(raw)
            sizes["side"][name] = sizes["cutover"][name] = _gz(raw)
            sizes["off"][name] = _gz(off)
            rep["water"][name] = {"side_gz": _gz(raw), "off_gz": _gz(off), "added": _gz(raw) - _gz(off)}
    if live is not None:
        lm = AD._manifest(Path(live))
        rep["live"] = lm.get("data_release") or Path(live).name
        for name, z in rep["regions"].items():
            z["live_gz"] = int(lm["files"][name]["gz"]) if name in lm["files"] else None
        for name, z in rep["water"].items():
            z["live_gz"] = int(lm["files"][name]["gz"]) if name in lm["files"] else None
        rep["moments_live"] = _moments(Path(live), {})
        rep["offline_live"] = _offline(lm, {})
    rep["moments"] = {k: _moments(side, v) for k, v in sizes.items()}
    rep["offline"] = {k: _offline(man, v) for k, v in sizes.items()}
    rep["largest_region"] = {k: max((n for n in v if n.startswith("regs-")), key=lambda n: v[n]) for k, v in sizes.items()}
    over = {k: m["used"] - m["gate"] for k, m in rep["moments"]["cutover"].items() if m["used"] > m["gate"]}
    rep["verdict"] = {
        "cutover_over": over,
        # owner 15:42 / RULINGS O5: raise the limit by what was measured plus 10 %, at the switch (ticket L)
        "first_tap_limit": (-(-int(rep["moments"]["cutover"]["first_tap"]["used"] * 1.10) // 10_000) * 10_000
                            if "first_tap" in over else rep["moments"]["cutover"]["first_tap"]["gate"]),
        "side_over": {k: m["used"] - m["gate"] for k, m in rep["moments"]["side"].items() if m["used"] > m["gate"]},
        # the files' own gates (`build.SIZE_GATES`) and the offline total (`build.OFFLINE_GATE`), each form
        "file_gates_over": {k: {n: g - B.SIZE_GATES[n] for n, g in v.items() if g > B.SIZE_GATES.get(n, g)}
                            for k, v in sizes.items()},
        "offline_over": {k: n - B.OFFLINE_GATE for k, n in rep["offline"].items() if n > B.OFFLINE_GATE}}
    return rep


def a1_text(rep: dict) -> str:
    L = [f"TROUT TABLE A1 on {rep['release']} (side release, --trout-table); live {rep['live'] or 'not given'}", ""]
    c = rep["cutover"]
    L.append(f"CUTOVER FORM (rows 34): {c['frames']} rows frames ({c['frames_with_a_table']} under a table); "
             f"{c['mixed_rows_split']} mixed rows split; rows {c['rows_before']} -> {c['rows_after']} "
             f"({c['rows_cut']} cut: {c['groups_cut']} groups, {c['elsewhere_cut']} elsewhere lines); "
             f"{c['frames_with_a_possession_row']} frames carry a page possession row; covers checked "
             f"({len(c['cover_mismatch'])} differ), re-pack differs on {c['repack_differs']} rows")
    L += ["", "REGION FILES, gz bytes (flag off = this code without the section; side = as shipped; cutover = rows 34)",
          f"  {'file':8} {'live':>9} {'flag off':>9} {'side':>9} {'cutover':>9} {'side+':>8} {'cut+':>8} "
          f"{'cut-side':>8} {'F0 row':>7} {'section':>8} {'tables':>6}"]
    for name, z in rep["regions"].items():
        lv = f"{z['live_gz']:,}" if z.get("live_gz") is not None else "-"
        L.append(f"  {name:8} {lv:>9} {z['off_gz']:>9,} {z['side_gz']:>9,} {z['cutover_gz']:>9,} "
                 f"{z['side_added']:>+8,} {z['cutover_added']:>+8,} {-z['groups_freed']:>+8,} {z['f0_cost']:>+7,} "
                 f"{z['section_stored']:>8,} {z['cells_with_a_table']:>6}")
    L.append("  (side+ / cut+: against flag off; cut-side: the trout groups and elsewhere lines cut, the split and F0's "
             "row added; F0 row: F0's page possession slot alone, inside cut-side; section: the four tables' blobs as "
             "stored; tables: cells with a table)")
    L += ["", "WATER FILES, gz bytes"]
    for name, z in rep["water"].items():
        lv = f"{z['live_gz']:,}" if z.get("live_gz") is not None else "-"
        L.append(f"  {name:12} live {lv:>9}  flag off {z['off_gz']:>9,}  side/cutover {z['side_gz']:>9,}  "
                 f"({z['added']:+,})")
    L += ["", "THE SECTION'S DICTIONARIES (water-shared `zdict`)"]
    for tb, z in rep["zdicts"].items():
        L.append(f"  {tb:12} {z['raw']:>7,} B raw, {z['gz']:>6,} B gz alone")
    L += ["", "THE SECTION, PROVINCE STORE"]
    for tb, f in rep["families"].items():
        L.append(f"  {tb:12} {f['distinct']:>6,} distinct, raw {f['raw']:>10,} B, stored {f['stored']:>9,} B")
    L += ["", f"WEB MOMENTS AT HEAD (largest region: flag off {rep['largest_region']['off']}, side "
              f"{rep['largest_region']['side']}, cutover {rep['largest_region']['cutover']})",
          f"  {'moment':18} {'gate':>10} {'live':>10} {'flag off':>10} {'side':>10} {'cutover':>10} {'room':>9}"]
    for k, m in rep["moments"]["cutover"].items():
        lv = f"{rep['moments_live'][k]['used']:,}" if rep.get("moments_live") else "-"
        L.append(f"  {k:18} {m['gate']:>10,} {lv:>10} {rep['moments']['off'][k]['used']:>10,} "
                 f"{rep['moments']['side'][k]['used']:>10,} {m['used']:>10,} {m['room']:>+9,}")
    L.append(f"  offline total: live {rep.get('offline_live') or 0:,}, flag off {rep['offline']['off']:,}, side "
             f"{rep['offline']['side']:,}, cutover {rep['offline']['cutover']:,}")
    v = rep["verdict"]
    L += ["", f"VERDICT: cutover over a gate: {v['cutover_over'] or 'none'}; side over a gate: {v['side_over'] or 'none'}; "
              f"first-tap limit at the switch: {v['first_tap_limit']:,}; files over their own gate: "
              f"{ {k: x for k, x in v['file_gates_over'].items() if x} or 'none'}; offline total over: "
              f"{v['offline_over'] or 'none'}"]
    for name in ("bodies", "sheets"):
        L += ["", f"THE {TOP_N} LARGEST {name.upper()} (stored bytes)"]
        for x in rep["largest"][name]:
            what = (f"{x['boxes']} boxes, {x['rows']} rows" if name == "bodies"
                    else f"{', '.join(x['fish'])}: {x['lines']} lines")
            L.append(f"  #{x['id']:<5} {x['stored']:>6,} B ({x['raw']:,} raw) {what}; {x['answer_keys']} keys — "
                     f"{', '.join(x['waters'])}")
    return "\n".join(L) + "\n"


def run(d, regions=("5", "6"), size: bool = True, log=print) -> dict:
    a = Audit(d, log=log)
    rep = {"release": a.R.name, "trout_char": sorted(TC),
           "mixed_rows": a.mixed_rows(), "angler_only": a.angler_only(), "hours_window": a.hours_window()}
    if size:
        rep["size"] = a.size(regions)
    return rep


def text(rep: dict) -> str:
    L = [f"TROUT TABLE A0 on {rep['release']} (trout and char = {', '.join(rep['trout_char'])})", ""]
    for label, m in rep["mixed_rows"].items():
        L.append(f"(1) rows mixing trout/char and other fish, by {label}: {m['rows']} distinct rows in {m['frames']} "
                 f"frames, {m['answer_keys']} answer keys, {m['waters']} waters; by kind {m['by_kind']}")
        L.append(f"    other fish: {m['other_fish']}")
        for p in m["patterns"][:6]:
            L.append(f"    {p['kind']}: {'+'.join(p['trout'])} with {'+'.join(p['other'])}: {p['rows']} rows, "
                     f"{p['answer_keys']} keys — {', '.join(p['waters'][:4])}")
    g = rep["angler_only"]
    L += ["", f"(2) angler-only rules: {g['angler_rules']}; naming any fish {g['name_a_fish']}; closing trout/char "
              f"alone {g['close_trout_alone']}; trout answers closed by one: {len(g['answers_closed_by_one'])} rules"]
    for lab, n in g["labels"].items():
        L.append(f"    {n} × {lab}")
    h = rep["hours_window"]
    L += ["", f"(3) trout answers that change inside an hours window (water open both sides): {h['answer_keys']} answer "
              f"keys, {h['waters']} waters; fish {h['fish']}; water closed inside the window: "
              f"{h['water_closed_inside']['answer_keys']} keys"]
    for x in h["examples"][:8]:
        L.append(f"    {x['water']} ({x['days']}): {'; '.join(x['fish'][:3])}")
    for name, z in (rep.get("size") or {}).items():
        c = z["rows_row_cut"]
        L += ["", f"SIZE {name}: {z['cells']} cells ({z['null_cells']} NULL); gz baseline {z['baseline_gz']:,} (manifest "
                  f"{z['manifest_gz']:,}); the trout groups cut from {c['rows']} rows_row blobs free {c['packed_bytes_freed']:,} B "
                  f"packed ({c['other_fish_groups_kept']} other-fish groups kept)"]
        for variant in BODIES:
            v = z[variant]
            L.append(f"    {variant}: section {v['section_packed']:,} B packed + zdicts {v['zdicts_gz']:,} B gz "
                     f"(water-shared); side {v['side_gz']:,} ({v['side_delta']:+,}), cutover {v['cutover_gz']:,} "
                     f"({v['cutover_delta']:+,})")
            for fam, f in v["families"].items():
                L.append(f"        {fam}: {f['distinct']} distinct, raw {f['raw']:,} B, packed {f['packed']:,} B, "
                         f"zdict {f['zdict_gz']:,} B gz")
    return "\n".join(L) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("release", type=Path)
    ap.add_argument("--regions", default="5,6")
    ap.add_argument("--no-size", action="store_true")
    ap.add_argument("--cutover", action="store_true",
                    help="(ticket A1) measure the side release's three forms (flag off, side, cutover) and every web moment")
    ap.add_argument("--live", type=Path, help="(with --cutover) the release to set beside it, as shipped")
    ap.add_argument("--out", type=Path)
    a = ap.parse_args(argv)
    try:
        if a.cutover:
            rep = measure(a.release, a.live)
        else:
            rep = run(a.release, tuple(x.strip() for x in a.regions.split(",") if x.strip()), not a.no_size)
    except AD.DiffError as e:
        print(str(e).replace("answers_diff:", "trout_size:"), file=sys.stderr)
        return 2
    except SizeError as e:
        print(str(e), file=sys.stderr)
        return 2
    out = a.out or OUT_ROOT / (rep["release"] + ("-a1" if a.cutover else ""))
    out.mkdir(parents=True, exist_ok=True)
    t = a1_text(rep) if a.cutover else text(rep)
    (out / "report.json").write_text(json.dumps(rep, indent=1, sort_keys=True, default=list), encoding="utf-8")
    (out / "report.txt").write_text(t, encoding="utf-8")
    print(t + f"\nwrote {out}/report.json, report.txt")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
