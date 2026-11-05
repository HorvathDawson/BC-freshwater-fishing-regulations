"""T239: WHAT CHANGED BETWEEN TWO INGESTED BOOKS — entry-level and row-level, keyed on ids.

    python -m pipeline.tools.synopsis_diff OLD_DIR NEW_DIR [--scope all|water|table]
        [--json OUT.json] [--ids-out REVIEW_IDS.txt] [--ignore FIELD ...]

OLD_DIR / NEW_DIR are catalogue dirs (`region-*.json`, `{region, entries: [...]}` — the shape of
`data/curated/regulations/entries/catalogue`). Read as plain JSON, never through the model: the two
books may be different vintages and neither has to load under today's `CatalogueEntry`.

HOW ENTRIES ARE PAIRED (stable ids first, free text last):
  1. same `entry_id` (`r{region}:{name}@{MUs}`, minted from the row alone);
  2. else same region + the same `matched` registry items (an id moved because the book re-spelled
     the row's name or MUs) — `renamed`, reported with both ids;
  3. else same region + the same printed regulations text — `renamed` too.
  Each fallback pairs only where the key is unique on both sides; anything left is added / removed.

HOW RECORDS INSIDE AN ENTRY ARE PAIRED: `rules` by `rule_id` (unique within an entry, AGENTS 8),
`licensing` by `id`; then a leftover pair with the same `verbatim` (the parser renumbered it) —
`rekeyed`. Every other entry field is compared whole (canonical JSON).

A paired entry is UNCHANGED, COSMETIC (only `COSMETIC_FIELDS` moved — the book re-paginates every
year) or CHANGED. `review_ids` (and `--ids-out`) are the NEW book's ids of every added and changed
entry: the owner reviews exactly those. Removed entries are listed for the owner to confirm the row
really left the book. Read-only; writes only the files it is told to.
"""
from __future__ import annotations

import argparse
import difflib
import json
import re
import sys
from pathlib import Path
from typing import Iterable, Optional

from pipeline.common import entry_id as EID

#: fields that move with the book's layout, not its regulations
COSMETIC_FIELDS = frozenset({"source_pages"})
#: fields that are a set of glyphs: their order is the extractor's, never the book's
ORDERLESS_FIELDS = frozenset({"symbols"})
#: record groups compared row by row, and the key each is paired on
ROW_GROUPS = {"rules": "rule_id", "licensing": "id"}
SCOPES = ("all", "water", "table")


# ── reading ────────────────────────────────────────────────────────────────────────────────────

def load_book(path: Path | str) -> dict[str, dict]:
    """{entry_id: entry} over every `region-*.json` in `path`. A dir with none is refused (an empty
    book would diff as "every entry removed"), and so is an id printed twice."""
    path = Path(path)
    files = sorted(path.glob("region-*.json"))
    if not files:
        raise FileNotFoundError(f"{path}: no region-*.json — not a catalogue dir")
    book: dict[str, dict] = {}
    for f in files:
        data = json.loads(f.read_text(encoding="utf-8"))
        for e in data["entries"]:
            eid = str(e["entry_id"])
            if eid in book:
                raise ValueError(f"{f.name}: entry_id {eid!r} appears twice")
            book[eid] = e
    return book


def in_scope(entry_id: str, scope: str) -> bool:
    if scope == "water":
        return EID.is_water_row(entry_id)
    if scope == "table":
        return EID.is_table(entry_id)
    return True


# ── comparing ──────────────────────────────────────────────────────────────────────────────────

def _canon(v) -> str:
    return json.dumps(v, sort_keys=True, ensure_ascii=False)


def _norm_text(s) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip().lower()


def _unique_by(entries: dict[str, dict], key) -> dict:
    """{key: id} for keys held by exactly one entry (None keys skipped)."""
    seen: dict = {}
    dup: set = set()
    for eid, e in entries.items():
        k = key(e)
        if k is None:
            continue
        if k in seen:
            dup.add(k)
        seen[k] = eid
    return {k: v for k, v in seen.items() if k not in dup}


def _pair(old: dict[str, dict], new: dict[str, dict], keys) -> tuple[list[tuple[str, str, str]], set, set]:
    """Pair old -> new ids: identity first, then each fallback key in order (unique on both sides).
    Returns ([(old_id, new_id, how)], unpaired old ids, unpaired new ids)."""
    pairs = [(i, i, "id") for i in sorted(old.keys() & new.keys())]
    left_old = set(old) - set(new)
    left_new = set(new) - set(old)
    for how, key in keys:
        o = _unique_by({i: old[i] for i in left_old}, key)
        n = _unique_by({i: new[i] for i in left_new}, key)
        for k in sorted(o.keys() & n.keys(), key=_canon):
            pairs.append((o[k], n[k], how))
            left_old.discard(o[k])
            left_new.discard(n[k])
    return pairs, left_old, left_new


def _entry_keys():
    def by_matched(e):
        m = e.get("matched") or []
        return (e.get("region") or "", tuple(sorted(m))) if m else None

    def by_regs(e):
        t = _norm_text(e.get("regs_verbatim"))
        return (e.get("region") or "", t) if t else None
    return (("matched", by_matched), ("regs_verbatim", by_regs))


def _field_changes(old: dict, new: dict, skip: Iterable[str]) -> dict[str, dict]:
    skip = set(skip)
    out = {}
    for f in sorted((old.keys() | new.keys()) - skip):
        a, b = old.get(f), new.get(f)
        if f in ORDERLESS_FIELDS and isinstance(a, list) and isinstance(b, list):
            a, b = sorted(map(_canon, a)), sorted(map(_canon, b))
        if _canon(a) != _canon(b):
            out[f] = {"old": old.get(f), "new": new.get(f)}
    return out


def diff_records(old: list[dict], new: list[dict], key: str, ignore: Iterable[str] = ()) -> dict:
    """Row-level diff of one record group (an entry's `rules` or `licensing`)."""
    o = {str(r.get(key)): r for r in old or []}
    n = {str(r.get(key)): r for r in new or []}
    pairs, gone, came = _pair(o, n, (("verbatim", lambda r: _norm_text(r.get("verbatim")) or None),))
    changed = []
    for ok, nk, how in pairs:
        fields = _field_changes(o[ok], n[nk], {key, *ignore})
        if fields or how != "id":
            changed.append({"key": nk, **({"old_key": ok} if ok != nk else {}),
                            "fields": fields})
    return {"added": [n[k] for k in sorted(came)], "removed": [o[k] for k in sorted(gone)],
            "changed": [c for c in changed if c["fields"]],
            "rekeyed": [{"key": c["key"], "old_key": c["old_key"]} for c in changed
                        if not c["fields"] and "old_key" in c]}


def word_diff(old: str, new: str) -> str:
    """`[-gone-]{+came+}` over words — how a changed printed passage reads in the report."""
    a, b = str(old or "").split(), str(new or "").split()
    out = []
    for op, i1, i2, j1, j2 in difflib.SequenceMatcher(a=a, b=b, autojunk=False).get_opcodes():
        if op == "equal":
            out.append(" ".join(a[i1:i2]))
            continue
        if i2 > i1:
            out.append("[-" + " ".join(a[i1:i2]) + "-]")
        if j2 > j1:
            out.append("{+" + " ".join(b[j1:j2]) + "+}")
    return " ".join(out)


def _brief(e: dict) -> dict:
    return {"entry_id": e["entry_id"], "name": e.get("name"), "region": e.get("region"),
            "source_pages": e.get("source_pages"), "regs_verbatim": e.get("regs_verbatim")}


def diff_books(old: dict[str, dict], new: dict[str, dict], *, scope: str = "all",
               ignore: Iterable[str] = ()) -> dict:
    """The whole comparison, as a JSON-able dict (see the module docstring)."""
    if scope not in SCOPES:
        raise ValueError(f"scope is one of {SCOPES}")
    ignore = set(ignore)
    old = {k: v for k, v in old.items() if in_scope(k, scope)}
    new = {k: v for k, v in new.items() if in_scope(k, scope)}
    pairs, gone, came = _pair(old, new, _entry_keys())

    changed, cosmetic, renamed = [], [], []
    unchanged = 0
    totals = {f"{g}_{s}": 0 for g in ROW_GROUPS for s in ("added", "removed", "changed", "rekeyed")}
    for oid, nid, how in sorted(pairs, key=lambda p: p[1]):
        o, n = old[oid], new[nid]
        fields = _field_changes(o, n, {*ROW_GROUPS, *ignore, "entry_id"})
        rows = {}
        for g, key in ROW_GROUPS.items():
            if g in ignore:
                continue
            d = diff_records(o.get(g) or [], n.get(g) or [], key, ignore)
            if any(d.values()):
                rows[g] = d
            for s in ("added", "removed", "changed", "rekeyed"):
                totals[f"{g}_{s}"] += len(d[s])
        if how != "id":
            renamed.append({"old_entry_id": oid, "entry_id": nid, "paired_by": how})
        substantive = {f: v for f, v in fields.items() if f not in COSMETIC_FIELDS}
        real_rows = {g: d for g, d in rows.items() if d["added"] or d["removed"] or d["changed"]}
        rec = {"entry_id": nid, **({"old_entry_id": oid} if oid != nid else {}),
               "name": n.get("name"), "region": n.get("region"), "fields": fields, "rows": rows}
        if "regs_verbatim" in fields:
            rec["regs_diff"] = word_diff(o.get("regs_verbatim"), n.get("regs_verbatim"))
        if substantive or real_rows:
            changed.append(rec)
        elif fields or rows:
            cosmetic.append(rec)
        else:
            unchanged += 1

    added = [_brief(new[i]) for i in sorted(came)]
    removed = [_brief(old[i]) for i in sorted(gone)]
    return {
        "scope": scope,
        "ignored_fields": sorted(ignore),
        "summary": {"old_entries": len(old), "new_entries": len(new), "added": len(added),
                    "removed": len(removed), "renamed": len(renamed), "changed": len(changed),
                    "cosmetic": len(cosmetic), "unchanged": unchanged, **totals},
        "added": added, "removed": removed, "renamed": renamed,
        "changed": changed, "cosmetic": cosmetic,
        "review_ids": sorted({a["entry_id"] for a in added} | {c["entry_id"] for c in changed}),
    }


# ── reporting ──────────────────────────────────────────────────────────────────────────────────

def _short(v, n: int = 160) -> str:
    s = (v if isinstance(v, str) else _canon(v)).replace("\n", " / ")
    return s if len(s) <= n else s[: n - 1] + "…"


def render_text(d: dict) -> str:
    s = d["summary"]
    out = [f"synopsis diff (scope {d['scope']}"
           + (f", ignoring {', '.join(d['ignored_fields'])}" if d["ignored_fields"] else "") + ")",
           f"  entries: old {s['old_entries']}  new {s['new_entries']}",
           f"  added {s['added']}  removed {s['removed']}  changed {s['changed']}  "
           f"cosmetic {s['cosmetic']}  unchanged {s['unchanged']}  (renamed ids: {s['renamed']})"]
    for g in ROW_GROUPS:
        out.append(f"  {g}: added {s[g + '_added']}  removed {s[g + '_removed']}  "
                   f"changed {s[g + '_changed']}  rekeyed {s[g + '_rekeyed']}")
    out.append(f"  to review (added + changed): {len(d['review_ids'])}")
    if d["added"]:
        out += ["", "ADDED"] + [f"  + {a['entry_id']}  p.{a['source_pages']}  {_short(a['regs_verbatim'])}"
                                for a in d["added"]]
    if d["removed"]:
        out += ["", "REMOVED"] + [f"  - {r['entry_id']}  p.{r['source_pages']}  {_short(r['regs_verbatim'])}"
                                  for r in d["removed"]]
    if d["renamed"]:
        out += ["", "RENAMED (paired by a stable key)"] + [
            f"  {r['old_entry_id']} -> {r['entry_id']}  (by {r['paired_by']})" for r in d["renamed"]]
    if d["changed"]:
        out += ["", "CHANGED"]
        for c in d["changed"]:
            out.append(f"  * {c['entry_id']}" + (f"  (was {c['old_entry_id']})" if "old_entry_id" in c else ""))
            if "regs_diff" in c:
                out.append(f"      regs: {_short(c['regs_diff'], 400)}")
            for f, v in c["fields"].items():
                if f == "regs_verbatim":
                    continue
                out.append(f"      {f}: {_short(v['old'])} -> {_short(v['new'])}")
            for g, r in c["rows"].items():
                key = ROW_GROUPS[g]
                for x in r["added"]:
                    out.append(f"      {g} + {x.get(key)}: {_short(x.get('verbatim'))}")
                for x in r["removed"]:
                    out.append(f"      {g} - {x.get(key)}: {_short(x.get('verbatim'))}")
                for x in r["changed"]:
                    was = f" (was {x['old_key']})" if "old_key" in x else ""
                    out.append(f"      {g} ~ {x['key']}{was}: {', '.join(x['fields'])}")
                for x in r["rekeyed"]:
                    out.append(f"      {g} = {x['old_key']} -> {x['key']} (same record, new key)")
    if d["cosmetic"]:
        out += ["", "COSMETIC (layout only — not for review)"] + [
            f"  ~ {c['entry_id']}: {', '.join(c['fields']) or 'rekeyed rows'}" for c in d["cosmetic"]]
    return "\n".join(out) + "\n"


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("old", type=Path, help="last year's catalogue dir")
    ap.add_argument("new", type=Path, help="the new book's catalogue dir")
    ap.add_argument("--scope", choices=SCOPES, default="all",
                    help="water = the water-table rows (r…), table = the province/zone tables (z…)")
    ap.add_argument("--ignore", action="append", default=[], metavar="FIELD",
                    help="a field to leave out of the comparison (entry or record level); repeatable")
    ap.add_argument("--json", type=Path, help="write the full diff as JSON here")
    ap.add_argument("--ids-out", type=Path, help="write the review ids (added + changed), one per line")
    a = ap.parse_args(argv)
    d = diff_books(load_book(a.old), load_book(a.new), scope=a.scope, ignore=a.ignore)
    sys.stdout.write(render_text(d))
    if a.json:
        a.json.parent.mkdir(parents=True, exist_ok=True)
        a.json.write_text(json.dumps(d, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    if a.ids_out:
        a.ids_out.parent.mkdir(parents=True, exist_ok=True)
        a.ids_out.write_text("".join(i + "\n" for i in d["review_ids"]), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
