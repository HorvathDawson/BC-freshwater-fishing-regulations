"""A LIFT IS A SUBTRACTION, not a footnote.

"Only non-game fish may be speared" closes every game fish to the spear. "except burbot, which
may also be speared in Regions 3, 5, 6, 7 and 8" does not argue with that rule — it takes ONE
FISH out of it, and only in five regions. Written as prose beside the ban it is a note the
reader has to apply themselves; written as a subtraction it is just the ban's subject, minus
burbot, wherever the lift bites.

That is the same `excepts` the Subject already carries, so a lift needs no new machinery — only
somewhere to be applied before the fold runs. Two shapes:

    with species   narrows the target: ALL_GAME_FISH minus BB
    without        removes the target here entirely — the Fraser lifting the spring stream
                   closure does not carve a fish out of it, it disapplies it

Both are scoped by `where`, because a lift that names regions is a lift in those regions only —
applying it everywhere would open five regions' fish in the three where the ban is absolute.
"""
from __future__ import annotations
from typing import Dict, FrozenSet, List, Set, Tuple

from pipeline.regs.table.where import parse_where


def lifts_here(rules: List[dict], here: FrozenSet[str]
               ) -> Tuple[Dict[str, FrozenSet[str]], Set[str]]:
    """(target rule -> species lifted out of it, targets removed outright).

    `here` is the set of region ids this section is in; a lift whose `where` names regions bites
    only in those. A lift whose place cannot be drawn is applied — the alternative is to ignore
    an exception the book wrote, which fails toward the stricter answer on water that is
    actually open, and this table's job is to be right rather than merely safe.
    """
    narrow: Dict[str, Set[str]] = {}
    drop: Set[str] = set()
    ids = {r.get("rule") for r in rules}
    for r in rules:
        for ex in (r.get("exempts") or []):
            if parse_where(r.get("extent_text")).bites_in(here) is False:
                continue
            targets = set()
            if ex.get("target"):
                targets.add(ex["target"])
            if ex.get("default_id"):
                targets |= {i for i in ids if i and i.split(".")[0] == ex["default_id"]}
            sp = set(r.get("species") or [])
            for t in targets:
                if sp:
                    narrow.setdefault(t, set()).update(sp)
                else:
                    drop.add(t)
    return {k: frozenset(v) for k, v in narrow.items()}, drop
