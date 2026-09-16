"""CLAUSES AND POOLED QUOTAS — the two facts about a number that its own fields do not say.

  SUB-LIMITS (`within`, 103 rules). Region 2 writes "trout and char — 4 per day" and then,
  INSIDE that 4: "no more than 1 over 50 cm", "no more than 2 steelhead", "no more than 1 bull
  trout, Dolly Varden and lake trout combined". These are not four answers. There is ONE
  allowance of 4, and three counters nested inside it: a fish that is over 50 cm counts against
  the 4 AND against the 1. `children_of` is the map of who is nested in whom; the ledger uses
  it to keep a clause and its parent from carving each other.

  POOLED QUOTAS (`combined`, 31 rules). "Whitefish — 15 per day, ALL SPECIES COMBINED" is 15
  fish shared across every whitefish, not 15 of each. Same shape, opposite meaning, one boolean
  apart — and a reader who gets it backwards keeps five times the legal limit. It belongs ON
  the Quota, because it changes what the number means.

  LIFTS (`exempts`). A rule may disapply another one here: "burbot may be speared in Regions 3,
  5, 6, 7, 8" lifts the province's spear ban; the Fraser lifts the spring stream closure. A
  lifted rule is not the answer and must not win — but it must still be VISIBLE, because the
  reader needs to know the general rule exists and why it does not bite. So it leaves the
  candidate list and stays in the chain, marked.
"""
from __future__ import annotations
from typing import Dict, List

from pipeline.regs.table.corpus import rid


def pooled_of(rule: dict, subject=None) -> bool:
    """Is this number shared across the fish it names, or one each?

    COMBINED IS THE DEFAULT WHEREVER MORE THAN ONE FISH IS NAMED. "Char daily quota = 1" means
    one char, and the book does not add "combined" because naming a group already says it. The
    flag marks the places the book spells it out; its absence is not the opposite claim.

    So `combined` set is a fact, and a subject covering more than one species is combined too.
    A quota on a single fish has no pool to share and is neither.
    """
    if rule.get("combined"):
        return True
    if subject is None:
        return False
    return len(subject.effective()) > 1


def children_of(rules: List[dict]) -> Dict[str, List[dict]]:
    """parent rule id -> its clauses. A clause with a clause is flattened onto the top parent,
    because the allowance it constrains is the one at the top."""
    kids: Dict[str, List[dict]] = {}
    # `within` names a BARE rule id, and a bare id is not unique — but a clause and its parent
    # are always in the same entry, so the parent is looked up within that entry alone.
    by = {rid(r): r for r in rules}
    for r in rules:
        p = r.get("within")
        if not p: continue
        entry = (r.get("entry") or r.get("entry_id"))
        key, seen = f"{entry}::{p}", set()
        while key in by and by[key].get("within") and key not in seen:
            seen.add(key); key = f"{entry}::{by[key]['within']}"
        kids.setdefault(key, []).append(r)
    return kids
