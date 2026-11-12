"""T678 / T685: THE OWNER'S RULINGS ON SINGLE LINES — `waters/shore_held.json`, read by the coast cut.

The coast cut (`atlas.splits.coast`) is one rule (RULINGS E21): a line is cut at the edge of the tidal water it
enters. This file is the short list of single lines the owner ruled on by hand, which the rule would read otherwise
or cannot see: each row says what the line keeps (`keep`: `cut` at `cut_m` / `whole` / `fresh`), when the owner
decided it (`decided`, required) and why, in his words or the ruling's id. The build applies each row as written
(`build.cut_shores`) and lists them in `coast_owner.json`.

Until T685 the file also held 180-odd lines "at today's result" that the first coast rule could not decide (two
crossings disagreeing, estuaries drawn as river polygons). The owner answered them all on 2026-11-12 with the one
rule, so those rows are gone and nothing writes this file but a person: a row without `decided` is refused.
"""
from __future__ import annotations

import json
from pathlib import Path


def load(path) -> dict[str, dict]:
    """`{blk: row}` from the owner's file. Missing is refused (AGENTS 37); so is a row that is not a dated ruling."""
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"coast: {p} is missing — the owner's rulings on single lines (curated, hand-written; "
                         "`git checkout` it)")
    rows = json.loads(p.read_text(encoding="utf-8"))["lines"]
    out = {}
    for r in rows:
        b = str(r["blk"])
        if b in out:
            raise SystemExit(f"coast: {p}: {b} twice")
        if r.get("keep") not in ("cut", "whole", "fresh") or (r["keep"] == "cut") != ("cut_m" in r):
            raise SystemExit(f"coast: {p}: {b}: keep must be cut (with cut_m), whole or fresh")
        if not r.get("decided") or not r.get("why"):
            raise SystemExit(f"coast: {p}: {b}: a row is the owner's ruling on that line — it needs `decided` (the "
                             "date) and `why`; a line with no ruling follows the coast rule and has no row")
        out[b] = r
    return out
