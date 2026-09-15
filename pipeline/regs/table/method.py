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


@dataclass(frozen=True)
class MethodRung:
    rule_id: str
    authority: str
    rank: int
    where: Where
    permission: Optional[Permission] = None    # may you use it
    takes: Optional[Tuple[Subject, Outcome]] = None   # what you may keep by it
    constraint: str = ""                       # how you must rig it
    verbatim: str = ""
    status: str = ""


@dataclass
class MethodRow:
    method: str
    permission: Permission
    chain: List[MethodRung]
    takes: List[MethodRung]        # what you may keep by this method, strictest first
    constraints: List[MethodRung]
    unplaceable: List[MethodRung]  # true somewhere in here, but nothing can draw where


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
    # A REGION-SCOPED RULE OUTRANKS A PROVINCE-WIDE ONE. "No spear fishing of any kind in
    # Regions 1, 2 and 4" is written in the provincial chapter, so by entry id it is provincial
    # — but it names five fewer regions than "spear fishing is permitted", and the narrower
    # statement is the one that governs where it applies. Scope is authority here.
    perms.sort(key=lambda r: (0 if r.where.kind == "regions" else 1,
                              r.rank, r.permission.rank))
    if not perms:
        return None
    win = perms[0]
    chain = []
    for r in perms:
        st = ("governs" if r is win
              else "says the same thing" if r.permission == win.permission
              else f"{'wider' if r.where.kind != 'regions' else 'elsewhere'} — "
                   f"{r.where.words() or 'province-wide'}")
        chain.append(MethodRung(r.rule_id, r.authority, r.rank, r.where, r.permission,
                                r.takes, r.constraint, r.verbatim, st))

    takes = sorted((r for r in mine if r.takes is not None),
                   key=lambda r: (r.takes[1].rank, r.rank))
    return MethodRow(method, win.permission, chain, takes,
                     [r for r in mine if r.constraint], undrawable)
