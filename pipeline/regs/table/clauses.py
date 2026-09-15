"""PROTOTYPE 7 — THE THREE GAPS: sub-limits, pooled quotas, and lifts.

WHAT IT HANDLES

  SUB-LIMITS (`within`, 103 rules). Region 2 writes "trout and char — 4 per day" and then,
  INSIDE that 4: "no more than 1 over 50 cm", "no more than 2 steelhead", "no more than 1 bull
  trout, Dolly Varden and lake trout combined". These are not four answers. There is ONE
  allowance of 4, and three constraints on what it may be made of.

  The page today has no way to say that, so each clause became its own ROW with no number of
  its own — an em dash in a column of 0s and 5s, reading as "keep none". It patched that by
  CARRYING the parent's number down into the child row, which then printed "4" twice and needed
  a sentence explaining that the two 4s are one 4. Here a sub-limit is not a row at all: it is
  a field on the row it belongs to, which is what the book says it is.

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
from dataclasses import dataclass
from typing import Dict, List, Optional

from pipeline.regs.table.subject import Subject
from pipeline.regs.table.corpus import rid


@dataclass(frozen=True)
class SubLimit:
    """A constraint on the COMPOSITION of an allowance, not an allowance of its own."""
    subject: Subject
    n: Optional[int]      # None where the clause constrains WHICH fish, not how many
    pooled: bool
    verbatim: str
    rule_id: str = ""     # so `comply` can ask whether it actually reached a row
    when: str = ""        # a clause can be seasonal: "1 trout from streams, July 1 – Oct 31"

    def sentence(self, name) -> str:
        who, q = self.subject.words(name)
        when = f", {self.when}" if self.when else ""
        if self.n is None:
            # A CLAUSE WITH NO COUNT CONSTRAINS WHICH FISH, NOT HOW MANY. "Bull trout (none
            # under 75 cm)" inside a quota of five does not allow five of anything; it says
            # which bull trout may be among them. Printed as "no more than None may be…" it
            # was nonsense, so it was dropped instead — thirty-one of them.
            return f"{who.lower()}: {q or 'no further limit'}{when}"
        pool = " combined" if self.pooled else ""
        tail = f" ({q})" if q else ""
        return f"no more than {self.n}{pool} may be {who.lower()}{tail}{when}"


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
