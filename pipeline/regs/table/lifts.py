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
from pipeline.regs.table.corpus import rid, rule_part
from pipeline.regs.table.subject import Subject


def lifts_here(rules: List[dict], here: FrozenSet[str]
               ) -> Tuple[Dict[str, FrozenSet[str]], Set[str], List[dict]]:
    """(target rule -> species lifted out of it, targets removed outright).

    `here` is the set of region ids this section is in; a lift whose `where` names regions bites
    only in those.

    A LIFT WHOSE PLACE CANNOT BE DRAWN IS NOT APPLIED. This module first applied them, reasoning
    that ignoring an exception the book wrote fails toward the stricter answer. That reasoning is
    backwards for a CLOSURE, and the corpus has the case: Region 6's "No fishing for steelhead in
    streams, May 15 – Jun 15" carries an exemption noting "mainstem Skeena, Nass, Iskut, Stikine
    and Taku". Applied everywhere, the closure disappeared from the Babine — which the note does
    not exempt — and a reader was told a river is open in the middle of a steelhead closure.
    Held back, the closure stands and the exemption rides beside it in the reader's own words,
    which is the one form of this that cannot mislead.

    AND A RULE CANNOT LIFT ITSELF. That same rule names its OWN entry as the default it exempts,
    so it lifted itself on every water in Region 6.
    """
    narrow: Dict[str, Set[str]] = {}
    drop: Set[str] = set()
    unresolved: List[dict] = []
    # Keys are COMPOSITE (`entry::rule`) because a bare rule id is not unique; `exempts` names
    # the bare one, so the match is on the rule half and the key that comes back is whole.
    ids = {rid(r) for r in rules}
    for r in rules:
        for ex in (r.get("exempts") or []):
            bites = parse_where(r.get("extent_text")).bites_in(here)
            if bites is False:
                continue
            if bites is None:
                unresolved.append({"lifter": rid(r), "note": ex.get("note") or "",
                                   "where": r.get("extent_text") or ""})
                continue
            targets = set()
            if ex.get("target"):
                targets |= {i for i in ids if rule_part(i) == ex["target"]}
            if ex.get("default_id"):
                targets |= {i for i in ids
                            if rule_part(i).split(".")[0] == ex["default_id"]}
            targets.discard(rid(r))          # a rule cannot lift itself
            sp = set(r.get("species") or [])
            for t in targets:
                tgt = next((y for y in rules if rid(y) == t), None)
                # A LIFT THAT TAKES THE WHOLE SUBJECT IS A DROP, NOT A NARROWING. Every "Exempt
                # from spring closure" names ALL_GAME_FISH, which is the closure's own subject —
                # narrowed by it the target covered nothing, entered no chain, and vanished
                # instead of being shown as lifted. Corpus-wide, not one rung said "lifted".
                whole = (tgt is not None and sp
                         and Subject(frozenset(sp)).covers(Subject(frozenset(tgt.get("species")
                                                                             or []))))
                if sp and not whole:
                    narrow.setdefault(t, set()).update(sp)
                else:
                    drop.add(t)
    return {k: frozenset(v) for k, v in narrow.items()}, drop, unresolved


def self_lifting(rules: List[dict]) -> List[dict]:
    """Rules whose own exemption names them — a DATA defect, reported rather than absorbed.

    `lifts_here` refuses to let a rule lift itself, which stops the damage. It does not fix the
    entry, and a workaround that leaves no trace is how a corpus defect becomes permanent. One
    rule is in this state:

        z6:steelhead_stream_closure.r1 — "No fishing for steelhead in streams, May 15 – Jun 15"
        exempts: default_id "steelhead_stream_closure"  (its own entry)
        note:    "mainstem Skeena, Nass, Iskut, Stikine and Taku, where not already closed"

    The note says what the exemption is FOR: five named mainstems. The `default_id` should name
    the closure being lifted on those waters — instead it names this rule, so the closure lifted
    itself on every water in Region 6 and the Babine lost a steelhead season the book keeps.
    The fix is in the catalogue, not here: the exemption belongs on the five waters that have
    it, or the note belongs in `extent_text` where a scope can be read from it.
    """
    out = []
    for r in rules:
        me = rule_part(rid(r))
        for ex in (r.get("exempts") or []):
            if ex.get("target") == me or (ex.get("default_id")
                                          and me.split(".")[0] == ex["default_id"]):
                out.append({"rule": rid(r), "note": ex.get("note") or "",
                            "label": r.get("label") or ""})
    return out
