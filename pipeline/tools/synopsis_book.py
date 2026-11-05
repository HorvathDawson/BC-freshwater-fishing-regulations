"""T239: ASSEMBLE A NEW BOOK'S CATALOGUE beside last year's — never writing the curated one.

    python -m pipeline.tools.synopsis_book carry  --batches ALL/batches  --catalogue OLD --out NEW
    python -m pipeline.tools.synopsis_book replay --batches CHANGED/batches --catalogue OLD --out RESP

`carry` (a real step of the annual update, `handoff/PLAN/runbooks/annual-synopsis.md`): writes into NEW
every entry of OLD the new book still prints UNCHANGED — the same `entry_id` in a full export of the new
extraction (`batch_exporter --force`) whose `raw_regs` equal the entry's `regs_verbatim` — copied as it
is, curator work and all (display names, matched items, binds to other waters' cut-points, which a
re-ingest would refuse: they are not on the row's own menu). Province and zone tables (`z…`) are
carried whole: the parser never writes them; the owner checks those pages by hand. Rows that changed
or are new are left out — the parse writes them (`batch_exporter --only-changed`, then dispatch +
ingest with `--out NEW`). Rows the new book no longer prints are left out, so `synopsis_diff OLD NEW`
lists them as removed.

`replay` (rehearsal only, NO credits): stands in for the parse by answering every batch item with
OLD's entry of the same id (`[{index, entry}]`, the shape `dispatch` writes). Ingest then takes the
row's facts from the batch, as after a real parse. Used to dry-run the pipeline on the current book.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

from pipeline.common import entry_id as EID
from pipeline.tools.synopsis_diff import load_book


def batch_items(batches: Path) -> list[dict]:
    files = sorted(Path(batches).glob("batch_*.json"))
    if not files:
        raise FileNotFoundError(f"{batches}: no batch_*.json — run the batch exporter first")
    return [it for f in files for it in json.loads(f.read_text(encoding="utf-8"))["items"]]


def carry(batches: Path, old_dir: Path, out: Path) -> dict[str, list[str]]:
    """Write OLD's unchanged entries + every table into OUT (region files laid out as OLD's).
    Returns {"carried", "changed", "new", "gone"} entry ids."""
    raw = {str(it["entry_id"]): str(it["raw_regs"]) for it in batch_items(batches)}
    if any(out.glob("region-*.json")):
        raise FileExistsError(f"{out}: already holds region files — carry into an empty dir")
    out.mkdir(parents=True, exist_ok=True)
    seen: set[str] = set()
    got = {"carried": [], "changed": [], "new": [], "gone": []}
    for f in sorted(Path(old_dir).glob("region-*.json")):
        doc = json.loads(f.read_text(encoding="utf-8"))
        keep = []
        for e in doc["entries"]:
            eid = str(e["entry_id"])
            seen.add(eid)
            if not EID.is_water_row(eid):
                keep.append(e)
            elif eid not in raw:
                got["gone"].append(eid)
            elif raw[eid] != e.get("regs_verbatim"):
                got["changed"].append(eid)
            else:
                keep.append(e)
                got["carried"].append(eid)
        (out / f.name).write_text(json.dumps({**doc, "entries": keep}, indent=1, ensure_ascii=False) + "\n",
                                  encoding="utf-8")
    got["new"] = sorted(set(raw) - seen)
    return got


def replay(batches: Path, book: dict[str, dict], out: Path) -> tuple[int, list[str]]:
    """Returns (responses written, batch entry ids OLD has no entry for)."""
    out.mkdir(parents=True, exist_ok=True)
    n, missing = 0, []
    for f in sorted(Path(batches).glob("batch_*.json")):
        rows = []
        for it in json.loads(f.read_text(encoding="utf-8"))["items"]:
            e = book.get(str(it["entry_id"]))
            if e is None:
                missing.append(str(it["entry_id"]))
                continue
            rows.append({"index": it["index"], "entry": e})
            n += 1
        (out / f.name).write_text(json.dumps(rows, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    return n, missing


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("cmd", choices=("carry", "replay"))
    ap.add_argument("--batches", type=Path, required=True)
    ap.add_argument("--catalogue", type=Path, required=True, help="last year's catalogue dir (read only)")
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args(argv)
    if a.out.resolve() == a.catalogue.resolve():
        ap.error("--out must not be the catalogue")
    if a.cmd == "carry":
        got = carry(a.batches, a.catalogue, a.out)
        print(f"carried {len(got['carried'])} unchanged water rows + every table -> {a.out}")
        for k, why in (("changed", "printed text changed — parse"), ("new", "not in last year's book — parse"),
                       ("gone", "not in the new book — confirm removal")):
            print(f"{len(got[k])} {k} ({why})")
            for i in got[k]:
                print(f"  {i}")
        return 0
    n, missing = replay(a.batches, load_book(a.catalogue), a.out)
    print(f"replayed {n} entries -> {a.out}")
    print(f"{len(missing)} batch item(s) with no catalogue entry (a real parse writes these):")
    for m in missing:
        print(f"  {m}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
