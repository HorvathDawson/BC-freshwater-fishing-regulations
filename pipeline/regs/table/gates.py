"""A SIZE BOUND IS NOT A COMPETITOR.

Several rules are true about one fish at one moment. "Trout/char: 4" and "none under 30 cm" do
not argue: you may keep four, and a 28 cm one is not among them. The fold picks ONE winner per
subject, which is right for the count and wrong for the bound — and the bound reached a row by
three different routes depending on how the book happened to write it:

    on the winning rule's own Subject      "Rainbow trout daily quota = 1 (none under 50 cm)"
    as a sub-limit, via `within`           "Trout/char: 5 ... none under 60 cm"
    as a Qualifier, with no count          "No trout under 25 cm"

and by NO route where the book wrote the same fact as a take of zero on the size class:
"Hatchery trout/char under 30 cm from streams: 0" competed as a release for a subject nobody
else wrote about, took a row of its own, and left the "keep 2 · hatchery only" row beside it
without the floor. Region 2's "none under 60 cm" on char reached nowhere on seven Fraser
stretches, because the only row that permits keeping is hatchery-only and the bound names no
origin — neither subject covers the other, and the check filed it as "no trigger", which is a
bucket for duties. Same sentence in Region 3, filed as a qualifier, printed.

So a row's answer is an OUTCOME — one winner, by the ladder — plus GATES, which accumulate from
every rule that speaks to the row's fish. A gate is a take of zero on a size class, and it
follows the ladder's own rule for a take of zero: it stands unless something lifts it. Two gates
on the SAME fish with the SAME kind of bound are one statement made twice, and there the
ladder's first step applies — the closer authority's bound replaces the wider one, which rides
on the row marked so. Everything else accumulates: a floor and a ceiling are both true, a bound
on bull trout and a bound on all trout are both true, and a reader sees each with its authority.

A Rung's subject carries no gate any more. Size stays on a subject only where a NUMBER counts a
size class — "1 over 50 cm" inside a 4, "5 over 50 cm per licence year" — which selects the fish
the number is about rather than sending any of them back (see `Size.is_gate`).
"""
from __future__ import annotations
from dataclasses import dataclass, replace
from typing import List, Optional

from pipeline.regs.table.size import Size
from pipeline.regs.table.subject import Subject


@dataclass(frozen=True)
class Gate:
    """One size bound, from one rule, on the fish it names — true beside whatever the count is."""
    subject: Subject          # who — fish, origin, excepts; never a size
    size: Size                # the bound, prohibition polarity, always (`is_gate`)
    rule_id: str
    authority: str            # "Provincial" | "Region 5" | "this water" ...
    rank: int                 # the same ladder Rung uses: smaller is closer
    verbatim: str
    when: str = ""            # a bound can be seasonal: "none over 30 cm, Jul 1 – Oct 31"
    status: str = ""          # "" binds here; otherwise why it does not

    @property
    def binds(self) -> bool:
        return not self.status

    def sentence(self, name, row_subject: Optional[Subject] = None) -> str:
        """A GATE WITHOUT ITS FISH IS A FLOOR ON EVERYTHING. "none under 60 cm" is about bull
        trout, Dolly Varden and lake trout; hung on a row headed "Trout and char" with no
        subject it read as a minimum size for every trout on the water. The fish is named
        wherever the gate is narrower than the row it rides on."""
        who, q = self.subject.words(name)
        text = self.size.words()
        if row_subject is None or not self.subject.covers(row_subject):
            head = who.lower() + (f" · {q}" if q else "")
            text = f"{head}: {text}"
        elif q:
            text = f"{q}: {text}"
        return text + (f", {self.when}" if self.when else "")


def attach(rows, gates: List[Gate]) -> List[Gate]:
    """Hang each gate on every row it speaks about. Returns the gates that reached no row.

    OVERLAP, NOT COVER. Region 2's "none under 60 cm" is about bull trout, Dolly Varden and
    lake trout of either origin; the Fraser's only keep row is "Trout and char · hatchery only".
    Neither subject covers the other — the bound names a narrower fish, the row a narrower origin
    — yet a hatchery bull trout is inside both, and that is the fish the bound protects. A gate
    lands on every row it has a fish in common with.

    NOTHING TO KEEP, NOTHING TO GATE. A size bound on a row that says release or closed
    constrains nothing; it rides on the row marked moot, so a reader can see the rule exists
    and the compliance check can see it landed. Only a gate with no row at all comes back.
    """
    unattached = []
    for g in gates:
        hosts = [r for r in rows if g.subject.meets(r.subject)]
        keep = [r for r in hosts if r.outcome.kind in ("quota", "unlimited")]
        for r in keep:
            r.gates = list(r.gates or []) + [g]
        if keep:
            continue
        if not hosts:
            unattached.append(g)
            continue
        moot = replace(g, status="moot here — nothing may be kept")
        for r in hosts:
            r.gates = list(r.gates or []) + [moot]
    for r in rows:
        if r.gates:
            r.gates = _settle(r.gates)
    return unattached


def _settle(gates: List[Gate]) -> List[Gate]:
    """The ladder, applied to gates. Same fish, same kind of bound: the closer authority's
    stands and the wider is marked replaced — but only by a bound that bites all year, since a
    seasonal one leaves the wider bound in force the rest of the year. Everything else stands
    together, closest authority first. One rule, once."""
    seen, uniq = set(), []
    for g in gates:
        if g.rule_id in seen:
            continue
        seen.add(g.rule_id); uniq.append(g)
    groups = {}
    for g in uniq:
        groups.setdefault((g.subject, g.size.bound), []).append(g)
    out = []
    for gs in groups.values():
        gs.sort(key=lambda g: (bool(g.status), g.rank, g.rule_id))
        top = next((g for g in gs if g.binds and not g.when), None)
        said = set()
        for g in gs:
            if g.binds and top is not None and g.rank > top.rank:
                g = replace(g, status=f"set wider ({g.authority}), replaced by one closer to "
                                      f"this water")
            elif g.binds and (g.size, g.when) in said:
                g = replace(g, status="says the same thing")
            if g.binds:
                said.add((g.size, g.when))
            out.append(g)
    out.sort(key=lambda g: (bool(g.status), g.rank, g.rule_id))
    return out
