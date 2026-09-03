"""Stamp matcher-owned identity — `Entry.matched` and a combined entry's verbatim name — onto
EntryFiles already on disk.

`matched` is matcher-written metadata ("registry ids — written by the matcher, []` from the parser"),
now filled at ingest from the batch item. Entries parsed BEFORE that have an empty list, and it can't
be recovered by re-matching the entry: a combined override is keyed on the synopsis row's VERBATIM
name ("CHILLIWACK / VEDDER RIVERS (does not include Sumas River) …") while the entry only stores the
item's name ("Chilliwack River"), so re-matching the identity finds the Chilliwack and never learns
about the Vedder River or the Vedder Canal.

The exporter still has the verbatim rows, so this replays it and copies each batch item's
`[item_id, *also_item_ids]` onto the matching entry. Cheap, local, no credits, no re-parse — and it
touches ONLY `matched`, so a locked entry's curated content is untouched (its `matched` is still
stamped: the id set is a fact about the registry, not a curation decision).

The entry's NAME is not touched here — `backfill_identity` owns it. That module writes the verbatim
synopsis wording, because the "cleaned-up name is better" reasoning this file used to give was wrong:
the cleanup dropped the parenthetical that carries the reach, collapsing MICHEL CREEK's upstream and
downstream rows to one indistinguishable label. The readable form lives in `identity.display_name`,
and a combined entry's several items are surfaced in the review UI from `matched`.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.parsing.backfill_matched --dry-run
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.parsing.backfill_matched --registry output/v2/full/registry.json
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.matching.matcher import load_overrides
from pipeline.parsing import io
from pipeline.registry import default_registry_path, load_registry
from pipeline.curated import CURATED, SOURCE


def matched_ids(registry_path: str | Path, overrides_path: str | Path | None) -> dict[str, dict]:
    """{entry_id: {"matched": [item_id, *also_item_ids], "name": <entry name>}} from a throwaway
    re-export, so the values can never drift from what a real parse would write."""
    import tempfile

    from pipeline.parsing.batch_exporter import export
    from pipeline.parsing.rows import load_synopsis_rows

    registry = load_registry(str(registry_path))
    with tempfile.TemporaryDirectory() as tmp:
        export(load_synopsis_rows(), registry, Path(tmp), batch_size=500,
               overrides=load_overrides(overrides_path), existing_ids=set(), force=True)
        out: dict[str, dict] = {}
        for item in io.load_all_batch_items(Path(tmp) / "batches").values():
            if item.get("item_id"):
                out[item["entry_id"]] = {
                    "matched": [item["item_id"], *item.get("also_item_ids", [])],
                    "name": item.get("name", ""),
                }
    return out


def backfill(entries_dir: Path, ids: dict[str, dict], dry_run: bool = False) -> dict:
    report: dict[str, list] = {"stamped": [], "combined": []}
    for path in sorted(Path(entries_dir).glob("region-*.json")):
        region = path.stem.split("region-")[1]
        by_id = io.read_entryfile(path)
        changed = False
        for eid, e in by_id.items():
            want = ids.get(eid)
            if not want or e.get("matched") == want["matched"]:
                continue
            e["matched"] = want["matched"]
            changed = True
            report["stamped"].append(eid)
            if len(want["matched"]) > 1:
                report["combined"].append((region, eid, want["matched"]))
        if changed and not dry_run:
            io.write_entryfile(path, region, by_id.values())     # atomic, via the model
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description="Stamp Entry.matched (registry ids covered) onto EntryFiles.")
    ap.add_argument("--registry", help=f"registry.json (default: {default_registry_path()})")
    ap.add_argument("--overrides", default=str(CURATED.regulations.overrides))
    ap.add_argument("--entries-dir", help="EntryFiles dir (default: pipeline/parsing/entries).")
    ap.add_argument("--dry-run", action="store_true", help="report only; write nothing.")
    args = ap.parse_args()

    ids = matched_ids(args.registry or default_registry_path(), args.overrides)
    entries_dir = Path(args.entries_dir) if args.entries_dir else io.entries_dir()
    report = backfill(entries_dir, ids, dry_run=args.dry_run)

    verb = "[dry-run] would stamp" if args.dry_run else "stamped"
    print(f"{verb} `matched` on {len(report['stamped'])} entry(ies)")
    print(f"  of which COMBINED (cover several registry items): {len(report['combined'])}")
    for region, eid, want in report["combined"]:
        print(f"    [{region}] {eid}  ->  {', '.join(want)}")


if __name__ == "__main__":
    main()
