"""MAY I KEEP IT? — the decision, with its reasons, from the same counters the table shows.

    I am holding a rainbow trout, 55 cm, hatchery, caught in THIS water on THIS date. I already
    have <creel so far> in my possession. May I keep it?

Every verdict names the counter that decided it, with its provenance:

    No — the char quota of 1 is already spent by the bull trout in your creel
         (Region 2 · region-wide, "1 char (bull trout, Dolly Varden, or lake trout)")

"No" alone is not an answer. This is also the totality check on the table: the oracle decides
by counters the ledger settled, and the rows are derived from the same counters, so a decision
by something a row never shows cannot happen — and a test says so.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from pipeline.regs.table.ledger import Allowance, Ledger
from pipeline.regs.table.subject import Origin


@dataclass(frozen=True)
class Fish:
    species: str
    length_cm: Optional[float] = None
    origin: Origin = Origin.wild     # if you cannot tell, it is wild


@dataclass
class Creel:
    """What you already have. Three clocks, each depletable on its own."""
    today: List[Fish] = field(default_factory=list)        # kept today, in this water
    held: List[Fish] = field(default_factory=list)         # in your possession right now
    this_year: List[Fish] = field(default_factory=list)    # kept this licence year

    @classmethod
    def of(cls, *fish: Fish) -> "Creel":
        """Fish kept today, still held, and counted for the year — the common case."""
        return cls(list(fish), list(fish), list(fish))

    def for_period(self, period: str) -> List[Fish]:
        return {"daily": self.today, "possession": self.held, "annual": self.this_year}.get(period, [])


@dataclass
class Check:
    """One counter, consulted: how much of it this creel has used."""
    counter: Allowance
    used: int
    @property
    def n(self): return self.counter.n
    @property
    def spent(self) -> bool:
        return self.n is not None and self.used >= self.n
    @property
    def remaining(self) -> Optional[int]:
        return None if self.n is None else max(self.n - self.used, 0)


@dataclass
class Verdict:
    keep: Optional[bool]          # True / False / None = nothing here decides it
    kind: str                     # closed | release | gate | spent | ok | unlimited | unwritten
    decided_by: List[Allowance]
    checks: List[Check]           # every counter that contains the fish, with its usage
    reasons: List[str]
    notes: List[str] = field(default_factory=list)

    @property
    def answer(self) -> str:
        return {True: "Yes", False: "No", None: "Nothing here says"}[self.keep]


def may_i_keep(ledger: Ledger, fish: Fish, on: Tuple[int, int], creel: Creel = None,
               name=None) -> Verdict:
    """The verdict for one fish on one date, given what is already in the creel."""
    name = name or (lambda c: c)
    creel = creel or Creel()
    live = ledger.counters(fish.species, fish.origin, fish.length_cm, on)
    notes = _notes(ledger, fish, name)
    if fish.length_cm is None:
        # WITHOUT A LENGTH, A SIZE RULE CANNOT BE CHECKED — and the verdict says so, rather
        # than deciding by a class the fish may or may not be in.
        unchecked = [a for a in live if not a.scope.size.is_any]
        live = [a for a in live if a.scope.size.is_any]
        for a in unchecked:
            notes.append(f"no length given, so not checked: {a.sentence(name)} "
                         f"({a.source.words()}, “{a.source.verbatim.strip()[:60]}”)")

    def cite(a: Allowance) -> str:
        return f"{a.source.words()}, “{a.source.verbatim.strip()}”"

    closed = [a for a in live if a.kind == "closed"]
    if closed:
        a = min(closed, key=lambda a: (a.rank, a.rule_id))
        return Verdict(False, "closed", [a], [], [f"No — you may not fish for it here: {cite(a)}"], notes)
    zero = [a for a in live if a.kind in ("release", "gate")]
    if zero:
        a = min(zero, key=lambda a: (a.kind != "gate", a.rank, a.rule_id))
        why = (f"No — {a.scope.size.words()} for {_who(a, name)}: {cite(a)}" if a.kind == "gate"
               else f"No — release it: {cite(a)}")
        return Verdict(False, a.kind, [a], [], [why], notes)
    checks = [Check(a, _used(ledger, a, creel, on)) for a in live if a.kind in ("quota", "unlimited")]
    spent = [c for c in checks if c.spent]
    if spent:
        # THE NARROWEST SPENT COUNTER NAMES THE REASON: "the char quota of 1" says more than
        # "the trout and char quota of 4", when both are spent by the same bull trout.
        c = min(spent, key=lambda c: (c.counter.period != "daily",
                                      len(c.counter.scope.effective()) or 999,
                                      c.counter.rank, c.counter.rule_id))
        a = c.counter
        kept = _kept(ledger, a, creel, on, name)
        return Verdict(False, "spent", [a], checks,
                       [f"No — the {_what(a, name)} of {a.n} per {_per(a)} is already spent"
                        f"{(' by the ' + kept + ' in your creel') if kept else ''}: {cite(a)}"], notes)
    if not checks:
        return Verdict(None, "unwritten", [], [],
                       ["Nothing here limits this fish — no rule on this stretch names it."], notes)
    reasons = []
    for c in sorted(checks, key=lambda c: (c.counter.period != "daily", c.counter.rank)):
        a = c.counter
        if a.n is None:
            reasons.append(f"Yes — no limit on {_who(a, name)}: {cite(a)}")
        else:
            left = c.remaining - 1
            reasons.append(f"Yes — {_what(a, name)}: {c.used} of {a.n} per {_per(a)} used, "
                           f"{left} more after this one: {cite(a)}")
    kind = "unlimited" if all(c.n is None for c in checks) else "ok"
    return Verdict(True, kind, [c.counter for c in checks], checks, reasons, notes)


def _used(ledger: Ledger, a: Allowance, creel: Creel, on) -> int:
    """How many fish already kept count against this counter — those it contains."""
    # A kept fish of unknown length counts against a size-class counter only if it is in the
    # class, and nobody can say — so it does not. The daily number still counts it.
    return sum(1 for k in creel.for_period(a.period)
               if a.contains(k.species, k.origin, k.length_cm)
               and (k.length_cm is not None or a.scope.size.is_any))


def _kept(ledger: Ledger, a: Allowance, creel: Creel, on, name) -> str:
    ks = [k for k in creel.for_period(a.period) if a.contains(k.species, k.origin, k.length_cm)]
    return ", ".join(f"{name(k.species).lower()}" + (f" ({k.length_cm:g} cm)" if k.length_cm else "")
                     for k in ks)


def _who(a: Allowance, name) -> str:
    who, q = a.scope.words(name)
    return who.lower() + (f" ({q})" if q else "")


def _what(a: Allowance, name) -> str:
    who, q = a.scope.words(name)
    pool = " combined" if a.pooled else ""
    size = f", {a.scope.size.words()}" if not a.scope.size.is_any else ""
    origin = f" ({a.scope.origin.value} only)" if a.scope.origin is not Origin.both else ""
    return f"{who.lower()}{origin} quota{pool}{size}"


def _per(a: Allowance) -> str:
    return {"daily": "day", "annual": "licence year", "possession": "possession"}.get(a.period, a.period)


def _notes(ledger: Ledger, fish: Fish, name) -> List[str]:
    """Rules that speak about this fish but cannot decide: a place nobody can draw, an hour of
    the day, a lift nobody can place."""
    out = []
    for a in ledger.allowances:
        if not a.contains(fish.species, fish.origin):
            continue
        if a.applies.kind == "somewhere":
            out.append(f"also, somewhere in here — {a.applies.detail}: {a.word()} "
                       f"({a.source.words()}, “{a.source.verbatim.strip()[:80]}”)")
        elif a.applies.within_day and ledger.in_force(a):
            out.append(f"also, {a.applies.detail}: {a.word()} ({a.source.words()})")
    for e in ledger.exemptions:
        out.append(f"an exemption nobody can place: {e.get('note')} ({e.get('lifter')})")
    return out
