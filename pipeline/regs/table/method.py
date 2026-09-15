"""HOW YOU MAY FISH — the same structure as the quota table, because it is the same problem.

A way of fishing is answered by three different kinds of statement, and the page's gear table
mixed them into one ad-hoc list:

    MAY YOU AT ALL          `permitted: true/false` — "Spear fishing is permitted",
                            "No spear fishing of any kind is permitted in Region 1, 2, and 4"
    WHAT MAY YOU KEEP BY IT a retention rule whose Subject carries `method` — "Only non-game
                            fish may be speared", "except burbot ... in Regions 3, 5, 6, 7 and 8"
    HOW MUST YOU RIG IT     a constraint — single barbless hook, one line, no fin fish as bait

They are not alternatives. On a Region 5 river all three are true at once, and a reader needs
all three: you MAY spear, you may keep non-game fish AND burbot by it, and the general tackle
rules still apply. The old table could not say that, because "permitted" and "you may not keep
game fish" competed for one verdict cell — so spear fishing read "Not permitted here" on waters
where it is permitted, and the burbot exception never appeared at all.

PERMISSION IS A TOTAL ORDER like `Outcome`, for the same reason: `Forbidden < Allowed`, so the
fold is a sort and nothing has to decide anything. The region scope comes from `where.py`, which
is what finally makes the burbot case expressible: the lift is not a note, it is a rule that
bites in five regions and not in the other three.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import FrozenSet, List, Optional, Tuple

from pipeline.regs.table.subject import Subject
from pipeline.regs.table.outcome import Outcome
from pipeline.regs.table.where import Where, ANYWHERE


@dataclass(frozen=True)
class Permission:
    allowed: bool
    @property
    def rank(self) -> int: return 0 if not self.allowed else 1     # smaller = stricter
    def word(self) -> str: return "permitted" if self.allowed else "not permitted here"

FORBIDDEN, ALLOWED = Permission(False), Permission(True)


#: HOW YOU MUST RIG IT, grouped the way a reader looks for it. 751 rules — "Artificial fly
#: only", "Single barbless hook", "Fin fish may not be used as bait" — carry no `method` at all,
#: so nothing in this module saw them and the quota side filed them "not a quota". They reached
#: NEITHER table. They are angling rules, and their dimension says which part of the tackle.
RIG_TOPIC = {"bait": "Bait", "barbless": "Hooks", "hook_count": "Hooks",
             "max_gap_mm": "Hooks", "min_gap_cm": "Hooks", "lure": "Line & tackle",
             "max_lines": "Line & tackle", "max_flies": "Line & tackle",
             "max_weight_kg": "Line & tackle"}
RIG_ORDER = ["Bait", "Hooks", "Line & tackle", "Also"]
RIG_TYPES = {"bait_restriction", "tackle_restriction"}


@dataclass(frozen=True)
class MethodRung:
    rule_id: str
    authority: str
    rank: int
    where: Where
    permission: Optional[Permission] = None    # may you use it
    takes: Optional[Tuple[Subject, Outcome]] = None   # what you may keep by it
    constraint: str = ""                       # how you must rig it
    topic: str = ""                            # Bait | Hooks | Line & tackle | Also
    verbatim: str = ""
    status: str = ""


@dataclass
class MethodRow:
    method: str
    permission: Permission
    chain: List[MethodRung]
    takes: List[MethodRung]        # what you may keep by this method, strictest first
    constraints: List[MethodRung]              # ungrouped, in authority order
    unplaceable: List[MethodRung]  # true somewhere in here, but nothing can draw where
    rig: dict = None                           # topic -> rungs, the shape a reader scans


def resolve_method(method: str, rungs: List[MethodRung],
                   here: FrozenSet[str]) -> Optional[MethodRow]:
    """One way of fishing, answered for THIS section — which knows its own region."""
    mine = [r for r in rungs if r.where.bites_in(here) is not False]
    if not mine:
        return None
    # A rule whose place cannot be drawn never decides the verdict; it is shown beside it.
    undrawable = [r for r in mine if r.where.bites_in(here) is None]
    mine = [r for r in mine if r.where.bites_in(here) is True]

    perms = [r for r in mine if r.permission is not None]
    # SCOPE BREAKS A TIE; IT DOES NOT OUTRANK AUTHORITY.
    #
    # This used to sort scope FIRST, on the reasoning that "No spear fishing of any kind in
    # Regions 1, 2 and 4" names five fewer regions than "spear fishing is permitted" and so is
    # the narrower statement. True — but it bought nothing and cost a great deal. Both spear
    # rules are provincial and `Forbidden < Allowed` already decides between them; meanwhile a
    # scope-first key let a province-wide permission scoped to Region 2 beat a LAKE'S OWN "no
    # ice fishing", which is the ladder this whole module exists to keep upright.
    #
    # Within one authority, the rule that names fewer places is the more specific one and wins.
    # Across authorities, the closer authority wins, whatever either one names.
    perms.sort(key=lambda r: (r.rank,
                              0 if r.where.kind == "regions" else 1,
                              r.permission.rank))
    if not perms:
        # NOTHING SAYING YOU MAY NOT IS AN ANSWER. Angling carries no `permitted` rule anywhere
        # in the corpus — it is the activity a licence is for, and the book states permission
        # only for the methods that need it. Returning None here meant the whole angling table,
        # every bait and hook and line rule on the water, rendered as "(nothing says)".
        #
        # Said explicitly rather than assumed: the rung records that the verdict rests on the
        # ABSENCE of a prohibition, so a reader can see that is why.
        win = MethodRung("", "no rule here", 9, ANYWHERE, ALLOWED,
                         verbatim="no rule here forbids it")
        perms = [win]
    win = perms[0]
    chain = []
    for r in perms:
        # A rule that BITES HERE and lost did not lose for being somewhere else — `mine` has
        # already dropped everything scoped out. Saying "elsewhere" of a rule that applies here
        # is simply false; it lost to a closer authority, or to a narrower scope within one.
        st = ("governs" if r is win
              else "says the same thing" if r.permission == win.permission
              else f"set wider — {r.where.words() or 'province-wide'}")
        chain.append(MethodRung(r.rule_id, r.authority, r.rank, r.where, r.permission,
                                r.takes, r.constraint, r.topic, r.verbatim, st))

    # WHAT YOU MAY KEEP BY A METHOD YOU MAY NOT USE IS NOT A QUESTION. Printed under "not
    # permitted here" it read as a keep list for a forbidden method — "Snagging · not permitted
    # here · what you may keep by it: release Everything".
    takes = (sorted((r for r in mine if r.takes is not None),
                    key=lambda r: (r.takes[1].rank, r.rank))
             if win.permission.allowed else [])
    # THE RIGGING RULES DO NOT COMPETE FOR A VERDICT. "Single barbless hook" and "no fin fish
    # as bait" are both true at once; they accumulate, grouped by the part of the tackle they
    # are about, closest authority first so a river's own rule reads above the province's.
    cons = sorted((r for r in mine if r.constraint), key=lambda r: (r.rank, r.rule_id))
    rig = {}
    for r in cons:
        rig.setdefault(r.topic or "Also", []).append(r)
    rig = {t: rig[t] for t in RIG_ORDER if t in rig}
    return MethodRow(method, win.permission, chain, takes, cons, undrawable, rig)
