"""Pointer rows ("See X"): the `see` column of an entry, and one way in per pointer printed in several
rows (`see_pointer_rows`, `ONE_WAY_IN_ACROSS_ROWS`)."""
from __future__ import annotations

import json
from pipeline.common import entry_id as EID


def _see_column(entries: dict) -> dict[str, str]:
    """`entry_id -> the `see` column`: each pointer with the RELATION it bears (`see_relation`).

    A POINTER MUST LAND. A `see` naming an entry the corpus does not hold would render as a link to
    nothing — the reader is told "see X" and X is not there — so the build stops, as it does for an
    `exempts` that lifts no rule. A pointer that names no entry says why in `unresolved`."""
    from pipeline.regs.parsing.catalogue import see_relation
    out: dict[str, str] = {}
    dangling = []
    for eid, ce in sorted(entries.items()):
        if not ce.see:
            continue
        items = []
        for s in ce.see:
            missing = [t for t in s.entry_ids if t not in entries]
            if missing:
                dangling.append(f"{eid} -> {missing}")
                continue
            if s.entry_ids:
                items.append({"verbatim": s.verbatim, "entry_ids": list(s.entry_ids),
                              "relation": see_relation(ce, [entries[t] for t in s.entry_ids])})
            else:
                items.append({"verbatim": s.verbatim, "unresolved": s.unresolved})
        out[eid] = json.dumps(items, separators=(",", ":"), ensure_ascii=False)
    if dangling:
        raise SystemExit(f"see: {len(dangling)} pointer(s) name an entry the corpus does not hold "
                         f"(e.g. {dangling[:3]}) — point at an existing entry_id, or say why it "
                         f"names none in `unresolved`")
    return out


def entry_name(ce) -> str:
    """`entry.name`: the name a row is SHOWN under — and so every "applies" line and every rule's
    `provenance.entry_name`. A water row (`r…`) by its book heading unless the registry's name for its
    water names the same water (`place_names.row_name`: Haynes Lake, never "McCulloch Reservoir");
    a zone table's entry by its authored scope (`display_name`: "Every stream in Region 1")."""
    from pipeline.deliver.bundle.place_names import row_name
    if EID.is_water_row(ce.entry_id):
        return row_name(ce.name, ce.display_name)
    return ce.display_name or ce.name


def see_name(ce) -> str:
    """The name a "See …" row and the rows it names are SAID under in `see_pointer`: the book's heading — what
    search matches (`SEARCH_POINTERS`: the name or its start) and what the pointer itself prints ("See Lake
    Revelstoke") — without its notes or the quotes round an unofficial name (`place_names.heading_name`): "LITTLE
    CAMPBELL RIVER" is Little Campbell River, never the registry's fewer-worded "Campbell River" (which made the line
    "Campbell River: see Campbell River"), and '"JONES" LAKE' is Jones Lake, found by "Jones Lake". A lake part keeps
    its authored name ("Williston Lake — Nation Arm"): the part, never the heading's bare "Nation Arm"."""
    from pipeline.deliver.bundle.place_names import heading_name
    shown = entry_name(ce)
    return shown if " — " in shown else heading_name(ce.name, quotes=False)


def see_pointer_rows(entries: dict) -> list[tuple]:
    """`see_pointer`: A GROUP NAME THAT ONLY POINTS (owner 2026-10-22). A row whose only content is
    "See …" (`pointer_only`: "TEE PEE LAKES  See Friday, Saturday, and Sunday Lakes") binds nothing
    and draws no line; what it gives a reader is the way to the waters it names. One row per named
    entry, in the pointer's order: (entry_id, name, ix, target_entry, target_name, item_id) — both
    names the book's (`see_name`), `item_id` the target row's first matched water (what opening it
    opens; NULL when it matched none). The same name to the same water from two rows (two regions'
    copies of one pointer) is said once (`ONE_WAY_IN_ACROSS_ROWS`). A TWIN ("the same row printed in
    another region's table") is no group: it names its own water under its own name, so it is left out. Only a WATER row (`r…`)
    is a name a reader looks up: a zone table's note ("Main body of Kootenay Lake") is none. Two named
    rows that are one water under one name (the Wigwam River's two rows, for Bighorn Creek) are one
    way in, not two identical ones."""
    from pipeline.regs.parsing.catalogue import see_relation
    out: list[tuple] = []
    across: set = set()
    for eid, ce in sorted(entries.items()):
        if not ce.pointer_only or not EID.is_water_row(eid):
            continue
        ix, said = 0, set()
        for s in ce.see:
            targets = [entries[t] for t in s.entry_ids if t in entries]
            if not targets or see_relation(ce, targets) == "twin":
                continue
            for te in targets:
                way = (see_name(te), te.matched[0] if te.matched else None)
                if way in said or (ONE_WAY_IN_ACROSS_ROWS and (see_name(ce),) + way in across):
                    continue
                said.add(way)
                across.add((see_name(ce),) + way)
                out.append((eid, see_name(ce), ix, te.entry_id) + way)
                ix += 1
    return out


#: T80 (T56 review): ONE POINTER PRINTED IN TWO REGIONS IS ONE WAY IN. Regions 5 and 7 both print "Blackwater River:
#: see West Road River" — the same name to the same water, which search listed twice. The first row by entry id says it.
ONE_WAY_IN_ACROSS_ROWS = True
