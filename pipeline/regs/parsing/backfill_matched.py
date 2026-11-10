"""Stamp matcher-owned identity — a catalogue entry's `matched` — onto the region files already
on disk.

`matched` is matcher-written metadata ("registry ids — written by the matcher, []` from the parser"),
now filled at ingest from the batch item. Entries parsed BEFORE that have an empty list, and it can't
be recovered by re-matching the entry: a combined override is keyed on the synopsis row's VERBATIM
name ("CHILLIWACK / VEDDER RIVERS (does not include Sumas River) …") while the entry only stores the
item's name ("Chilliwack River"), so re-matching the identity finds the Chilliwack and never learns
about the Vedder River or the Vedder Canal.

The exporter still has the verbatim rows, so this replays it and copies each batch item's
`[item_id, *also_item_ids]` onto the matching entry. Cheap, local, no credits, no re-parse — and it
touches ONLY `matched`, so an entry's curated content is untouched. The id set is a fact about the
registry, not a curation decision.

The entry's NAME is not touched here — ingest takes it from the batch, in the synopsis's verbatim
wording, because the "cleaned-up name is better" reasoning this file used to give was wrong: the
cleanup dropped the parenthetical that carries the reach, collapsing MICHEL CREEK's upstream and
downstream rows to one indistinguishable label. The readable form is `display_name`, and a combined
entry's several items are surfaced in the review UI from `matched`.

A `matched` ALREADY SET IS NEVER OVERWRITTEN BY DEFAULT (T546 item 5). The entry's `matched` is what the reach
build and the review app read; the matcher's answer is the second copy. The unconditional restamp would have
dropped ids a curator added (2026-11-10: 6 entries disagree, 4 of them hold an id the matcher does not give —
Seton's canal, Shuswap, Stave, Sumallo). By default an EMPTY `matched` is stamped and every other disagreement
is LISTED; `--restamp ENTRY_ID` (repeatable) restamps the named entries, `--restamp-all` all of them.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.backfill_matched --dry-run
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.backfill_matched --registry data/generated/atlas/full/registry.json
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.backfill_matched --restamp r2:nelson_creek@2-8
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.regs.matching.matcher import load_overrides
from pipeline.regs.parsing import io
from pipeline.atlas.registry import default_registry_path, load_registry
from pipeline.common.curated import CURATED, SOURCE


def matched_ids(registry_path: str | Path, overrides_path: str | Path | None) -> dict[str, dict]:
    """{entry_id: {"matched": [item_id, *also_item_ids], "name": <entry name>}} from a throwaway
    re-export, so the values can never drift from what a real parse would write."""
    import tempfile

    from pipeline.regs.parsing.batch_exporter import export
    from pipeline.regs.parsing.rows import load_synopsis_rows

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


def backfill(entries_dir: Path, ids: dict[str, dict], dry_run: bool = False, *,
             restamp: set[str] | frozenset[str] = frozenset(), restamp_all: bool = False) -> dict:
    """Stamp the matcher's ids onto every entry whose `matched` is EMPTY, and onto those `restamp` names (all,
    with `restamp_all`). Every other entry whose `matched` differs from the matcher's is left alone and listed
    in `disagree` as `(region, entry_id, entry's ids, matcher's ids)`."""
    report: dict[str, list] = {"stamped": [], "combined": [], "disagree": []}
    for path in sorted(Path(entries_dir).glob("region-*.json")):
        region = path.stem.split("region-")[1]
        by_id = io.read_entryfile(path)
        changed = False
        for eid, e in by_id.items():
            want = ids.get(eid)
            if not want or e.get("matched") == want["matched"]:
                continue
            if e.get("matched") and not restamp_all and eid not in restamp:
                report["disagree"].append((region, eid, list(e["matched"]), list(want["matched"])))
                continue
            e["matched"] = want["matched"]
            changed = True
            report["stamped"].append(eid)
            if len(want["matched"]) > 1:
                report["combined"].append((region, eid, want["matched"]))
        if changed and not dry_run:
            io.write_entryfile(path, region, by_id.values())     # atomic; the file is validated
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description="Stamp `matched` (registry ids covered) onto the "
                                 "catalogue region files.")
    ap.add_argument("--registry", help=f"registry.json (default: {default_registry_path()})")
    ap.add_argument("--overrides", default=str(CURATED.regulations.book_name_links))
    ap.add_argument("--entries-dir", help="catalogue region files dir (default: "
                    "data/curated/regulations/synopsis).")
    ap.add_argument("--dry-run", action="store_true", help="report only; write nothing.")
    ap.add_argument("--restamp", action="append", default=[], metavar="ENTRY_ID",
                    help="overwrite this entry's non-empty `matched` with the matcher's (repeatable)")
    ap.add_argument("--restamp-all", action="store_true",
                    help="overwrite EVERY differing `matched` (drops ids a curator added — read the list first)")
    args = ap.parse_args()

    ids = matched_ids(args.registry or default_registry_path(), args.overrides)
    entries_dir = Path(args.entries_dir) if args.entries_dir else io.entries_dir()
    report = backfill(entries_dir, ids, dry_run=args.dry_run, restamp=set(args.restamp),
                      restamp_all=args.restamp_all)

    verb = "[dry-run] would stamp" if args.dry_run else "stamped"
    print(f"{verb} `matched` on {len(report['stamped'])} entry(ies)")
    print(f"  of which COMBINED (cover several registry items): {len(report['combined'])}")
    for region, eid, want in report["combined"]:
        print(f"    [{region}] {eid}  ->  {', '.join(want)}")
    if report["disagree"]:
        print(f"LEFT ALONE: {len(report['disagree'])} entry(ies) whose `matched` differs from the matcher's "
              "(restamp one with --restamp ENTRY_ID):")
        for region, eid, have, want in report["disagree"]:
            print(f"    [{region}] {eid}  entry only: {sorted(set(have) - set(want))}  "
                  f"matcher only: {sorted(set(want) - set(have))}")


if __name__ == "__main__":
    main()
