"""Re-apply the curator's decisions from a pre-reparse LOCKED.json onto freshly parsed EntryFiles.

A full reparse regenerates every entry from the synopsis, which is what we want for CONTENT — the
parser now sees corrected names and ids — but it knows nothing about the human judgements made on
the old entries. Those judgements are not re-derivable: "this row is only a pointer at another
entry", "come back to this one", "these tributaries are carved out" are decisions, not parses.

Old and new entry ids do not correspond (the id scheme changed from match-derived to row-derived),
so entries are joined on what BOTH spellings agree about: region, the verbatim regs text, and the
name compared case-insensitively (the old names were the model's title-cased rewrite). Measured on
the 2026-08-31 backup: 101 of 103 join on name+regs, 2 on regs alone, 0 unmatched.

Only three fields are written, and only where the old entry set them:

    reference_only              the pointer-row flag  ("VEDDER RIVER: See Chilliwack River")
    revisit + revisit_note      the conditional-accept flag
    tributaries.excludes        curated tributary carve-outs

Rules, extents and split bindings are deliberately NOT copied back. The reparse re-derived all 22
split-bound rules on its own (18 identical, 4 differing), and a difference there is a question for
the curator, not something to overwrite in either direction — see `--report`.

Entries are NOT re-locked. The content underneath these flags is new and unreviewed; locking it
would freeze parser output a human has not seen.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.reapply_locked --report
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.reapply_locked --dry-run
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.reapply_locked
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline.regs.parsing import io

DEFAULT_BACKUP = Path("archive/entries-pre-reparse-2026-08-31/LOCKED.json")


def _norm(s: str | None) -> str:
    return " ".join((s or "").split()).casefold()


def _region(e: dict) -> str:
    return str(e.get("region") or (e.get("identity") or {}).get("region") or "")


def join(old_entries: list[dict], new: dict[str, dict]) -> tuple[dict[str, str], list[dict]]:
    """{old_entry_id: new_entry_id}, plus the old entries that could not be placed."""
    by_name: dict[tuple, list[str]] = {}
    by_regs: dict[tuple, list[str]] = {}
    for eid, e in new.items():
        ident = e.get("identity") or {}
        r = str(ident.get("region") or "")
        by_name.setdefault((r, _norm(ident.get("name"))), []).append(eid)
        by_regs.setdefault((r, (e.get("regs_verbatim") or "").strip()), []).append(eid)

    pairs: dict[str, str] = {}
    unplaced: list[dict] = []
    for o in old_entries:
        ident = o.get("identity") or {}
        r = _region(o)
        cn = by_name.get((r, _norm(ident.get("name"))), [])
        cr = by_regs.get((r, (o.get("regs_verbatim") or "").strip()), [])
        both = [x for x in cn if x in cr]
        pick = both[0] if len(both) == 1 else (cn[0] if len(cn) == 1 else (cr[0] if len(cr) == 1 else None))
        if pick is None:
            unplaced.append(o)
        else:
            pairs[o["entry_id"]] = pick
    return pairs, unplaced


def _splits(e: dict) -> set[tuple]:
    return {(ex.get("op"), s) for r in e.get("rules") or []
            for ex in r.get("extents") or [] for s in ex.get("splits") or []}


def report(old_by_id: dict[str, dict], new: dict[str, dict], pairs: dict[str, str]) -> dict:
    """What a human still has to decide — differences the reparse introduced under a locked entry."""
    out: dict[str, list] = {"splits_differ": [], "rules_differ": [], "reference_only_dropped": []}
    for oid, nid in sorted(pairs.items()):
        o, n = old_by_id[oid], new[nid]
        name = (n.get("identity") or {}).get("name")
        so, sn = _splits(o), _splits(n)
        if so and so != sn:
            out["splits_differ"].append({"entry_id": nid, "name": name,
                                         "old_only": sorted(so - sn), "new_only": sorted(sn - so)})
        lo, ln = len(o.get("rules") or []), len(n.get("rules") or [])
        if lo != ln:
            out["rules_differ"].append({"entry_id": nid, "name": name, "old": lo, "new": ln})
        if o.get("reference_only") and not n.get("reference_only"):
            out["reference_only_dropped"].append({"entry_id": nid, "name": name})
    return out


def reapply(entries_dir: Path, old_by_id: dict[str, dict], pairs: dict[str, str],
            dry_run: bool = False) -> dict:
    inv = {nid: oid for oid, nid in pairs.items()}
    counts: dict[str, list] = {"reference_only": [], "revisit": [], "tributary_excludes": []}
    for path in sorted(Path(entries_dir).glob("region-*.json")):
        region = path.stem.split("region-")[1]
        by_id = io.read_entryfile(path)
        changed = False
        for eid, e in by_id.items():
            o = old_by_id.get(inv.get(eid, ""))
            if o is None:
                continue
            if o.get("reference_only") and not e.get("reference_only"):
                e["reference_only"] = True
                counts["reference_only"].append(eid); changed = True
            if o.get("revisit") and not e.get("revisit"):
                e["revisit"] = True
                e["revisit_note"] = o.get("revisit_note") or e.get("revisit_note") or ""
                counts["revisit"].append(eid); changed = True
            ex = (o.get("tributaries") or {}).get("excludes") or []
            if ex and not (e.get("tributaries") or {}).get("excludes"):
                tribs = dict(e.get("tributaries") or {})
                tribs["excludes"] = ex
                e["tributaries"] = tribs
                counts["tributary_excludes"].append(eid); changed = True
        if changed and not dry_run:
            io.write_entryfile(path, region, by_id.values())      # atomic, via the model
    return counts


def main() -> None:
    ap = argparse.ArgumentParser(description="Re-apply curator decisions from a pre-reparse LOCKED.json.")
    ap.add_argument("--backup", default=str(DEFAULT_BACKUP), help=f"LOCKED.json (default: {DEFAULT_BACKUP})")
    ap.add_argument("--entries-dir", help="EntryFiles dir (default: pipeline/regs/parsing/entries).")
    ap.add_argument("--dry-run", action="store_true", help="report only; write nothing.")
    ap.add_argument("--report", action="store_true",
                    help="print ONLY what a human must still decide (content the reparse changed "
                    "under a locked entry); write nothing.")
    args = ap.parse_args()

    old_entries = json.loads(Path(args.backup).read_text(encoding="utf-8"))["entries"]
    old_by_id = {e["entry_id"]: e for e in old_entries}
    entries_dir = Path(args.entries_dir) if args.entries_dir else io.entries_dir()
    new = io.read_entries_dir(entries_dir)

    pairs, unplaced = join(old_entries, new)
    print(f"joined {len(pairs)}/{len(old_entries)} locked entries to the reparsed set")
    for o in unplaced:
        print(f"  UNPLACED {o['entry_id']}  {(o.get('identity') or {}).get('name')!r}")

    if args.report:
        rep = report(old_by_id, new, pairs)
        for kind, rows in rep.items():
            print(f"\n== {kind}: {len(rows)} ==")
            for r in rows:
                extra = (f"  old_only={r['old_only']} new_only={r['new_only']}" if "old_only" in r
                         else (f"  {r['old']} -> {r['new']} rules" if "old" in r else ""))
                print(f"  {r['name']}  ({r['entry_id']}){extra}")
        return

    counts = reapply(entries_dir, old_by_id, pairs, dry_run=args.dry_run)
    verb = "[dry-run] would set" if args.dry_run else "set"
    for field, ids in counts.items():
        print(f"{verb} {field} on {len(ids)} entr(ies)")
    print("\nEntries are NOT re-locked — the content under these flags is freshly parsed and unreviewed.")


if __name__ == "__main__":
    main()
