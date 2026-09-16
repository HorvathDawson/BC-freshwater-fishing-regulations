"""MAY I FISH THIS WAY? — the decision, with its reasons, from the same terms the table shows.

    I am on THIS water on THIS date with a rod / a spear / a set line. May I fish this way,
    and how must it be rigged? And if I catch something by it, may I keep it?

Every verdict names what decided it, with its provenance on both axes:

    No — the water is closed to fishing: Region 3 · for this water (Fraser River),
         "No Fishing between fishing boundary signs..." — and do not put any gear in the water
         while it is closed (All of B.C., "place any fishing gear in any water during a No
         Fishing period").
    Yes — single barbless hook (Region 2 · region-wide, "Single barbless hook: must be used in
         all streams of Region 2"); no bait Jul 1 – Oct 31 (Region 2 · for this water, ...)

This is also the totality check on the gear table: the oracle decides by terms the table
settled, the rows are drawn from the same terms, so a decision by something a row never shows
cannot happen — and a test drives the check FROM THE DECISION, never from the table.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from pipeline.regs.table.ledger import Allowance, Ledger
from pipeline.regs.table.method import MethodTable, MethodRow, Term, HOOK_AND_LINE
from pipeline.regs.table.oracle import Fish, Creel, Verdict, may_i_keep


@dataclass
class GearVerdict:
    may: Optional[bool]              # True / False / None = nothing here decides it
    kind: str                        # closed | banned | ok | unwritten
    method: str
    decided_by: List[object]         # the Term or the closure Allowance that decided it
    conditions: List[Term]           # how you must rig it, in force on the date
    exceptions: Dict[str, List[Term]]   # rule id -> the exceptions carved into that condition
    reasons: List[str]
    notes: List[str] = field(default_factory=list)

    @property
    def answer(self) -> str:
        return {True: "Yes", False: "No", None: "Nothing here says"}[self.may]

    def cited(self) -> frozenset:
        """Every rule this verdict rests on — what the row must show."""
        ids = set()
        for d in self.decided_by:
            ids.add(d.rule_id)
        for t in self.conditions:
            ids.add(t.rule_id)
        for xs in self.exceptions.values():
            ids |= {x.rule_id for x in xs}
        ids.discard("")
        return frozenset(ids)


def _cite(src) -> str:
    return f"{src.words()}, “{src.verbatim.strip()}”"


def may_i_fish(T: MethodTable, method: str, on: Tuple[int, int]) -> GearVerdict:
    """The verdict for one way of fishing on one date."""
    row = MethodRow(T, method)
    notes = _notes(T, row)
    c = T.shut(on)
    if c is not None:
        decided = [c] + [t for t in T.terms if t.kind == "while_closed"]
        why = [f"No — the water is closed to fishing: {_cite(c.source)}"]
        for t in decided[1:]:
            why.append(f"and do not put any gear in the water while it is closed: {_cite(t.source)}")
        return GearVerdict(False, "closed", method, decided, [], {}, why, notes)
    gov = T.standing(method, on)
    if gov.kind == "ban":
        why = (f"No — not allowed here: {_cite(gov.source)}" if not gov.is_default
               else f"No — {gov.text.lower()}. {gov.source.verbatim}")
        return GearVerdict(False, "banned", method, [gov], [], {}, [why], notes)
    conds: List[Term] = list(T.conditions(method, on))
    for topic, ts in T.rig(method, on).items():
        conds += ts
    exc = {t.rule_id: list(T.carves[method].get(t, [])) for t in conds if T.carves[method].get(t)}
    reasons = [f"Yes — {gov.text.lower() if gov.is_default else 'allowed'}: {_cite(gov.source)}"
               if not gov.is_default else f"Yes — {gov.source.verbatim}"]
    for t in conds:
        if t is gov:
            continue
        line = f"{t.plain()}"
        if t.applies.detail:
            line += f" ({t.applies.detail})"
        for x in exc.get(t.rule_id, []):
            line += f" — except {x.plain().lower()} ({x.source.words()})"
        reasons.append(f"{line}: {_cite(t.source)}")
    return GearVerdict(True, "ok", method, [gov], conds, exc, reasons, notes)


def may_i_keep_by(T: MethodTable, quota: Ledger, method: str, fish: Fish, on: Tuple[int, int],
                  creel: Creel = None, name=None) -> Verdict:
    """MAY I KEEP IT, HAVING CAUGHT IT THIS WAY — the stricter of two answers, always.

    "Only non-game fish may be speared" is a counter on the fish BY THE METHOD; the water's own
    quota table is a counter on the fish however it was caught. Both bind at once, and the
    owner's own words for it — "if no fishing, another rule says you are not allowed to put
    fishing gear in the water" — are why neither table may answer alone."""
    q = may_i_keep(quota, fish, on, creel, name)
    L = T.keep(method)
    if L is None:
        return q
    m = may_i_keep(L, fish, on, creel, name)
    strict = {"closed": 0, "release": 1, "gate": 2, "spent": 3}
    if m.keep is False and q.keep is not False:
        m.notes = list(m.notes) + [f"the water's own quota table would say {q.answer.lower()}"]
        return m
    if q.keep is False and m.keep is False:
        # Both say no: the stricter reason leads — "you may not fish for it by spear" says more
        # than "release it" — and the other rides as a note.
        lead, other = ((m, q) if strict.get(m.kind, 9) < strict.get(q.kind, 9) else (q, m))
        lead.notes = list(lead.notes) + [f"also: {other.reasons[0]}" for _ in other.reasons[:1]]
        return lead
    if q.keep is False:
        return q
    if m.keep is None:
        return q
    # both say yes: cite both
    q.reasons = list(q.reasons) + [r for r in m.reasons if r not in q.reasons]
    q.decided_by = list(q.decided_by) + [a for a in m.decided_by if a not in q.decided_by]
    return q


def _notes(T: MethodTable, row: MethodRow) -> List[str]:
    """What speaks about this method but cannot decide: a place nobody can draw, an hour of
    the day."""
    out = []
    for t, st in row.folded():
        if t.applies.kind == "somewhere":
            out.append(f"also, somewhere in here — {t.applies.detail}: {t.plain()} ({t.source.words()})")
    for t in T.within_day(row.method):
        out.append(f"also, {t.applies.detail}: {t.plain()} ({t.source.words()})")
    return out
