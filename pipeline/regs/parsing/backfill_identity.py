"""Restore `Entry.identity` — name, display_name, region, mus — from the exporter onto EntryFiles.

WHO an entry is about is export-time knowledge: the synopsis row's verbatim name, the registry's
name for whatever it matched, the region, the MUs. None of it is a parsing result, so none of it
should ever have come back from the model. It did, and the model rewrote it.

Measured on the 2026-08-31 parse (1,021 entries ingested before the fix):

  * 843 entries had a name that was not the synopsis's.
  * 777 of those were merely title-cased (`BEAR LAKE` -> `Bear Lake`).
  * 66 were rewritten outright, and those are the damaging ones — the model dropped the
    parenthetical that CARRIES THE REACH:
        THOMPSON RIVER (upstream of Kamloops Lake)            -> Thompson River
        MICHEL CREEK (downstream of the easternmost Hwy 3 bridge)  -> Michel Creek
        MICHEL CREEK (upstream of the easternmost Hwy 3 bridge)    -> Michel Creek
    the last two leaving one water with two entries a curator cannot tell apart. It also
    substituted the registry's name for the synopsis's ('"LINK" RIVER' -> Marble River) and
    "corrected" spellings the book actually prints (MCDONNEL LAKE -> McDonell Lake).
  * 993 lost `display_name` entirely.

`validate_candidate` now injects all four fields, so a fresh parse is correct. This repairs what
was ingested before that. Values come from a throwaway RE-EXPORT rather than being reconstructed
here, so they cannot drift from what a real parse would write — the same technique as
`backfill_matched`, and for the same reason.

Locked entries are included on purpose: `identity` is a fact about the synopsis row, not a curation
decision, and a curator who locked an entry did so looking at a name we had already corrupted.
Nothing else on the entry is touched.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.backfill_identity --dry-run
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.backfill_identity --registry output/v2/full/registry.json
"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from pipeline.regs.matching.matcher import load_overrides
from pipeline.regs.parsing import io
from pipeline.atlas.registry import default_registry_path, load_registry
from pipeline.common.curated import CURATED, SOURCE

FIELDS = ("name", "display_name", "region", "mus")


def identities(registry_path: str | Path, overrides_path: str | Path | None) -> dict[str, dict]:
    """{entry_id: {name, display_name, region, mus}} from a throwaway re-export."""
    from pipeline.regs.parsing.batch_exporter import export, load_synopsis_rows

    rows = load_synopsis_rows()
    registry = load_registry(Path(registry_path))
    overrides = load_overrides(Path(overrides_path) if overrides_path else None)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td)
        export(rows, registry, out, 500, overrides, set(), False)
        want: dict[str, dict] = {}
        for f in sorted((out / "batches").glob("batch_*.json")):
            for it in json.loads(f.read_text(encoding="utf-8"))["items"]:
                want[it["entry_id"]] = {
                    "name": it["name"],
                    "display_name": it.get("display_name", ""),
                    "region": str(it.get("region") or ""),
                    # row_mus, NOT mus: `mus` is the union of the matched ITEM's MUs (registry
                    # orientation shown to the parser), while identity.mus is the MU heading the
                    # synopsis ROW is printed under — the same thing `entry_id` is built from.
                    "mus": list(it.get("row_mus") or []),
                }
    return want


def backfill(entries_dir: Path, want: dict[str, dict], dry_run: bool = False) -> dict:
    report: dict[str, list] = {"fixed": [], "rewritten": [], "unknown": []}
    for path in sorted(Path(entries_dir).glob("region-*.json")):
        region = path.stem.split("region-")[1]
        by_id = io.read_entryfile(path)
        changed = False
        for eid, e in by_id.items():
            w = want.get(eid)
            if w is None:
                report["unknown"].append(eid)          # not produced by today's export
                continue
            ident = dict(e.get("identity") or {})
            if all(ident.get(f) == w[f] for f in FIELDS):
                continue
            before = ident.get("name") or ""
            ident.update(w)
            e["identity"] = ident
            changed = True
            report["fixed"].append(eid)
            if before and before.casefold() != w["name"].casefold():
                report["rewritten"].append((region, eid, before, w["name"]))
        if changed and not dry_run:
            io.write_entryfile(path, region, by_id.values())      # atomic, via the model
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description="Restore Entry.identity from the exporter onto EntryFiles.")
    ap.add_argument("--registry", help=f"registry.json (default: {default_registry_path()})")
    ap.add_argument("--overrides", default=str(CURATED.regulations.overrides))
    ap.add_argument("--entries-dir", help="EntryFiles dir (default: pipeline/regs/parsing/entries).")
    ap.add_argument("--dry-run", action="store_true", help="report only; write nothing.")
    args = ap.parse_args()

    want = identities(args.registry or default_registry_path(), args.overrides)
    entries_dir = Path(args.entries_dir) if args.entries_dir else io.entries_dir()
    report = backfill(entries_dir, want, dry_run=args.dry_run)

    verb = "[dry-run] would fix" if args.dry_run else "fixed"
    print(f"{verb} identity on {len(report['fixed'])} entry(ies)")
    print(f"  of which the name was REWRITTEN by the model (not just re-cased): {len(report['rewritten'])}")
    for region, eid, before, after in report["rewritten"]:
        print(f"    [{region}] {eid}\n         was: {before!r}\n         now: {after!r}")
    if report["unknown"]:
        print(f"  entries not in today's export (left alone): {len(report['unknown'])}")


if __name__ == "__main__":
    main()
