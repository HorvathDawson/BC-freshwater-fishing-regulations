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
    ap.add_argument("--out", type=Path)
    a = ap.parse_args(argv)
    try:
        rep = run(a.release, tuple(x.strip() for x in a.regions.split(",") if x.strip()), not a.no_size)
    except AD.DiffError as e:
        print(str(e).replace("answers_diff:", "trout_size:"), file=sys.stderr)
        return 2
    except SizeError as e:
        print(str(e), file=sys.stderr)
        return 2
    out = a.out or OUT_ROOT / rep["release"]
    out.mkdir(parents=True, exist_ok=True)
    t = text(rep)
    (out / "report.json").write_text(json.dumps(rep, indent=1, sort_keys=True, default=list), encoding="utf-8")
    (out / "report.txt").write_text(t, encoding="utf-8")
    print(t + f"\nwrote {out}/report.json, report.txt")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
