"""(T388, owner 2026-11-04) THE STREAM UNDER EACH PART OF A NAMED WATER: `part_line`, a small table in the WATER file.

A web link to a named water's part carries a PLACE (AGENTS rule 5: the FWA stream key + a distance along it, T369). It
used to be made from, and resolved through, the `lines` file (1.2 MB gz, every stream section in B.C.) — so tapping a
named water fetched all of `lines` just to write the address. This table gives each part its own stretch of stream
instead, keyed by the water's `ord`, so it rides in the `water-<region>` slice the page already fetches
(`slices.WATER_SLICE_TABLES`; the app's `focus.ts` TABLES `water`). `lines` is then only for UNNAMED reaches.

  part_line (ord INTEGER NOT NULL, part_ix INTEGER NOT NULL, blk INTEGER NOT NULL,
             m0 INTEGER NOT NULL, m1 INTEGER NOT NULL, PRIMARY KEY (ord, part_ix, blk, m0)) WITHOUT ROWID

ONLY THE WATERS WHOSE LINKS CARRY A PLACE: two or more parts. A water of one part is addressed by its item alone (T369: "the item is enough"), so its rows would never be read — measured
on the 2026-11-04 bundle: 53,639 runs / 451 kB gz for every named water, 9,661 runs / 79 kB gz for the 682 that need
a place.

One row per RUN: the stream sections of one part (T517: a part is one connected run) that lie end to end on one blue line `blk`, merged into one span [m0, m1) in whole metres — the same units and
meaning as `lines` / the bundle's `section_measure` (a section ends exactly where the next one on its line starts). A
lake, wetland or river polygon has no line: its sections give no run, and a part with no stream section has no rows
(its place: none, as for a lake today). A zero-length section spans no point and is left out.

What the app does with it (`@app/ui` place.ts): the place of a part is the middle of its LONGEST run — a
real point on the part's own stream (side channels are shorter than the main stem); a named link is opened by finding
the run of THAT water holding (blk, m), never a guess.
"""
from __future__ import annotations

import json
from typing import Dict, Iterable, List, Tuple

DDL = ("CREATE TABLE part_line (ord INTEGER NOT NULL, part_ix INTEGER NOT NULL, "
       "blk INTEGER NOT NULL, m0 INTEGER NOT NULL, m1 INTEGER NOT NULL, "
       "PRIMARY KEY (ord, part_ix, blk, m0)) WITHOUT ROWID")
COLUMNS = ["ord", "part_ix", "blk", "m0", "m1"]

Run = Tuple[int, int, int, int, int]      # (ord, part_ix, blk, m0, m1)


def runs(part_sections: Iterable[Tuple[int, int, int]],
         measures: Iterable[Tuple[int, int, int, int]]) -> List[Run]:
    """The rows of `part_line`, sorted by its key.

    `part_sections` = (ord, sid, part_ix) — the bundle's `part_section`; `measures` = (sid, blk, from_m, to_m) — its
    `section_measure` (stream sections only). Sections of one (water, part) on one blue line that touch are one run;
    a gap (a lake between two pieces) starts another. Only waters of two or more parts have rows."""
    part_sections = list(part_sections)
    parts_of: Dict[int, set] = {}
    for o, _sid, p in part_sections:
        parts_of.setdefault(o, set()).add(p)
    placed = {o for o, ps in parts_of.items() if len(ps) > 1}
    line = {sid: (blk, lo, hi) for sid, blk, lo, hi in measures}
    spans: Dict[Tuple[int, int, int], List[Tuple[int, int]]] = {}
    for o, sid, p in part_sections:
        if o not in placed:                       # one part: its links carry no place
            continue
        ln = line.get(sid)
        if ln is None or ln[2] <= ln[1]:          # a polygon (no line), or a section spanning no point
            continue
        spans.setdefault((o, p, ln[0]), []).append((ln[1], ln[2]))
    out: List[Run] = []
    for (o, p, blk), ss in sorted(spans.items()):
        ss.sort()
        lo, hi = ss[0]
        for a, b in ss[1:]:
            if a <= hi:                           # end to end (or touching): the same run
                hi = max(hi, b)
            else:
                out.append((o, p, blk, lo, hi))
                lo, hi = a, b
        out.append((o, p, blk, lo, hi))
    return out


def write(con, rows: Iterable[Run]) -> int:
    """Create and fill `part_line` in `con` (main schema); returns the runs written."""
    rows = list(rows)
    con.execute(DDL)
    con.executemany("INSERT INTO part_line VALUES (?, ?, ?, ?, ?)", rows)
    return len(rows)


def from_bundle(b, measures: Iterable[Tuple[int, int, int, int]]) -> List[Run]:
    """`runs` over an open bundle `b` (its `part_section`) and its `section_measure` rows, read once by the caller for
    `lines` too."""
    return runs(b.execute("SELECT ord, sid, part_ix FROM part_section"), measures)
