"""Fill `Entry.source` — where the row is PRINTED — on EntryFiles already on disk.

A one-off. Every entry parsed from here on gets this at ingest: the exporter reads the row's
`page` and `image` into the batch payload and `validate.validate_candidate` injects the whole
nested `source`, authoritative, never trusted from the model. This module exists only for the
1,393 entries parsed before that path existed, and it does two things:

  * MIGRATES the flat `source_symbols` those entries carry into `source.symbols`, and
  * ADDS `source.pages` and `source.row_image`, which never existed.

WHY IT IS SAFE TO REPLAY. Where a row is printed is a fact about the BOOK, not a curation
decision, so it is stamped on locked entries too — the same argument `backfill_matched` makes for
`matched`. It writes one field and reads the symbols it is migrating from the entry itself, so a
curator who corrected a symbol keeps their correction.

THE JOIN. `data/generated/regs/extraction/synopsis_raw_data.json` holds the 1,393 rows the parse
was built from, each with its page and row image. Entry to row is (name, mus) on the synopsis's
own verbatim name, which `identity.name` preserves precisely because the parser is forbidden from
rewriting it. Measured: 1,393 of 1,393 entries match, none by fallback.

SEVEN ROWS ARE PRINTED TWICE. Basalt, Chipmunk, Gatcho, Naglico, Pettry, Squirrel and Toms Lake
each appear on two pages in MU 6-1 — the regional table and again in a later one. Both pages are
recorded. Naglico's two printings even carry different wording, which is a curation question and
not this module's to answer: it records where to look and says no more.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.backfill_source_pages --dry-run
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.backfill_source_pages
"""

from __future__ import annotations

import argparse
import collections
import json
import re
from pathlib import Path

from pipeline.regs.parsing import io

RAW = Path("data/generated/regs/extraction/synopsis_raw_data.json")


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("**", "")).strip().lower()


def rows_by_identity(raw_path: Path = RAW) -> dict[tuple[str, tuple[str, ...]], dict]:
    """{(name, mus): {"pages": [...], "row_image": "..."}} from the extraction.

    Pages are unioned over repeated printings; the row image is the FIRST printing's, because a
    second image of the same row is the same line of the same book.
    """
    pages: dict[tuple, list[int]] = collections.defaultdict(list)
    image: dict[tuple, str] = {}
    for block in json.loads(raw_path.read_text(encoding="utf-8")):
        for row in block["rows"]:
            key = (_norm(row.get("water")), tuple(sorted(row.get("mu") or [])))
            page = row.get("page")
            if isinstance(page, int) and page not in pages[key]:
                pages[key].append(page)
            image.setdefault(key, row.get("image") or "")
    return {k: {"pages": sorted(v), "row_image": image.get(k, "")} for k, v in pages.items()}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--raw", default=str(RAW))
    args = ap.parse_args()

    lookup = rows_by_identity(Path(args.raw))
    changed = unmatched = 0
    samples: list[str] = []
    per_file: dict[Path, tuple[str, list[dict]]] = {}

    for region in io.region_ids():
        path = io.entries_dir() / f"region-{region}.json"
        doc = json.loads(path.read_text(encoding="utf-8"))
        touched = False
        for e in doc["entries"]:
            ident = e.get("identity") or {}
            key = (_norm(ident.get("name")), tuple(sorted(ident.get("mus") or [])))
            row = lookup.get(key)
            if row is None:
                unmatched += 1
                continue
            # the symbols come from the ENTRY, not the extraction: a curator may have fixed one
            symbols = list(e.get("source", {}).get("symbols") or e.get("source_symbols") or [])
            want = {"pages": row["pages"], "symbols": symbols, "row_image": row["row_image"]}
            if e.get("source") == want and "source_symbols" not in e:
                continue
            e["source"] = want
            e.pop("source_symbols", None)
            changed += 1
            touched = True
            if len(samples) < 3:
                samples.append(f'{ident.get("name")} -> p{row["pages"]}')
        if touched:
            per_file[path] = (doc["region"], doc["entries"])

    print(f"{changed} entries to stamp, {unmatched} with no matching row")
    for s in samples:
        print("   e.g.", s)
    if args.dry_run:
        for path in per_file:
            print(f"  would write {path}")
        return 0
    for path, (region, entries) in per_file.items():
        io.write_entryfile(path, region, entries)
        print(f"  wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
