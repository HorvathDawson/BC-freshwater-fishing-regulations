"""THE LEDGER: every allowance that binds on a section, as counters a fish is checked against.

WHAT IT REPLACES
  The table used to be built as ROWS — one subject, one winning outcome, and four side-lists
  bolted on (sub-limits, gates, ceilings, possession multiples). That shape could not answer
  the only question an angler asks:

      I am holding a rainbow trout, 55 cm, hatchery, in this water, today. I already have a
      bull trout in the creel. May I keep it?

  That is a BUDGET question, not a lookup. Several allowances bind the same fish at once — a
  species quota, a group quota it counts against, a size-class slot inside that quota, an
  annual limit, a possession multiple — and keeping one fish decrements every counter whose
  scope contains it. A char quota of 1 combined means the bull trout already in the creel
  forbids a rainbow, if the rainbow counts against the same shared number.

THE MODEL
  An ALLOWANCE is one counter: a scope (which fish — species, origin, size class), an outcome
  (closed, release, a number, or no limit), a period (daily, annual, possession), whether the
  number is pooled across the scope, the rule it came from, and when it applies. A closure is
  an allowance of zero you may not even fish for; a release is an allowance of zero; a size
  bound is an allowance of zero on a size class (a GATE). One vocabulary, no special cases.

  A LEDGER is every allowance a section carries, SETTLED: each one either binds, or carries a
  status saying why it does not. Settling is the domain owner's ladder, applied pairwise:

      a closer rule CARVES the fish it names out of a wider rule about the same period, unless
      the two are a clause and its parent (a clause counts INSIDE its parent, never against it);

      a take of zero stands unless a closer rule speaks about the same fish or fewer — a wider
      number does not lift a narrower protection, and nothing lifts a "No Fishing" but a lift;

      at equal authority a narrower rule carves a wider one only where it must, to be reachable
      at all ("2 hatchery steelhead over 50 cm allowed" inside "1 over 50 cm"), and on the same
      subject the stricter stands.

  A carve is DATE-AWARE: a seasonal water rule carves the region's number only on the days it
  is live, so the same ledger answers for any date without a second algorithm.

  Two stages. The BASE is the region's standing table — every region-wide rule for this kind
  of water, settled on its own, once per (region, kind), and checkable against the printed
  synopsis. A section's OVERRIDES — its own rules, area rules, rules inherited from a
  downstream water — are laid on top. The type decides which stage a rule enters (see
  `authority.Source.is_base`), so a water-scoped rule cannot leak into a base by construction.
"""
from __future__ import annotations
from dataclasses import dataclass, replace
from typing import Dict, FrozenSet, Iterable, List, Optional, Tuple

from pipeline.regs.table.applies import Applies, ALWAYS
from pipeline.regs.table.authority import Source
from pipeline.regs.table.outcome import Outcome
from pipeline.regs.table.subject import Subject, Origin

LIFTED = "lifted here — does not apply"
REPLACED_BY_CLAUSE = "replaced here by its own clause for this kind of water"
SAME = "says the same thing"
WEAKER = "not the strictest here"
ONLY_SOMEWHERE = "only somewhere in here — nothing can draw where"


@dataclass(frozen=True)
class Allowance:
    """One counter. `scope.size` is the class the number COUNTS (a cap) or the class a gate
    FORBIDS; a plain quota has no size."""
    scope: Subject
    outcome: Outcome
    source: Source
    applies: Applies = ALWAYS
    within: str = ""                 # the rule id of the allowance this is a clause of
    derived_from: Optional["Allowance"] = None   # a possession counter: N x this daily one
    multiplier: int = 0              # ...and the N
    multiplied_by: Optional[Source] = None       # ...set by this rule

    @property
    def rule_id(self) -> str: return self.source.rule_id
    @property
    def kind(self) -> str:
        return "gate" if self.scope.size.is_gate else self.outcome.kind
    @property
    def n(self) -> Optional[int]:
        return 0 if self.outcome.kind in ("closed", "release") else self.outcome.n
    @property
    def period(self) -> str: return self.outcome.period
    @property
    def pooled(self) -> bool: return self.outcome.pooled
    @property
    def is_zero(self) -> bool: return self.outcome.kind in ("closed", "release")
    @property
    def rank(self) -> int:
        return self.source.rank

    def contains(self, species: str, origin: Origin = None, length_cm=None) -> bool:
        return self.scope.contains(species, origin, length_cm)

    def word(self) -> str:
        return self.outcome.word()

    def sentence(self, name) -> str:
        """What this counter says, on its own: "keep up to 4 between them per day"."""
        if self.kind == "gate":
            return self.scope.size.words()
        s = self.outcome.sentence()
        if not self.scope.size.is_any:
            s += ", " + self.scope.size.words()
        if self.derived_from is not None:
            s = (f"{self.outcome.n} in possession — {self.multiplier} × the daily "
                 f"{self.derived_from.outcome.n} ({self.multiplied_by.words()})")
        return s


_ALL_GAME = Subject(frozenset({"ALL_GAME_FISH"}))


def shuts_the_water(s: Subject) -> bool:
    """"No Fishing" — a closure on the water, as opposed to a quota of zero for one fish."""
    return s.method is None and s.covers(_ALL_GAME)


def _year_round(a: Allowance) -> bool:
    """A rule with no dates, or one whose dates are the days it does NOT apply — "release,
    EXCEPT Apr 1–3" is the standing answer with five days out of it."""
    return a.applies.always or a.applies.unless


def _fish_scope(s: Subject) -> Subject:
    """The subject with its size class removed — the fish it names, whatever their length."""
    from pipeline.regs.table.size import ANY
    return replace(s, size=ANY) if not s.size.is_any else s


class Ledger:
    """Every allowance on a section, settled. Immutable after construction."""

    def __init__(self, allowances: Iterable[Allowance], *, lifted: Dict[str, str] = None,
                 family: Dict[str, FrozenSet[str]] = None, multiples=(), duties=(),
                 exemptions=(), water_kind: str = "", label: str = ""):
        self.water_kind, self.label = water_kind, label
        self.family = dict(family or {})
        self.duties = list(duties)               # (Subject, Source, text) — what you must DO
        self.exemptions = list(exemptions)       # lifts nobody can place, shown not applied
        self.multiples = list(multiples)         # (Subject, n, Source) possession multiples
        raw = list(allowances)
        raw += self._possession(raw, lifted or {})
        self.allowances: Tuple[Allowance, ...] = tuple(
            sorted(raw, key=lambda a: (a.rank, a.outcome.period, a.rule_id, a.within,
                                       a.derived_from is not None)))
        self.status: Dict[Allowance, str] = {a: (lifted or {}).get(a.rule_id, "")
                                             for a in self.allowances}
        for a in self.allowances:
            if a.applies.kind == "somewhere":
                self.status[a] = ONLY_SOMEWHERE
        self.carves: Dict[Allowance, List[Allowance]] = {a: [] for a in self.allowances}
        self._settle()
        # A DERIVED COUNTER FOLLOWS ITS PARENT. Twice a number that is replaced is replaced.
        for a in self.allowances:
            if a.derived_from is not None and not self.status[a]:
                self.status[a] = self.status.get(a.derived_from, "")

    # -- construction -----------------------------------------------------------------
    def _possession(self, raw: List[Allowance], lifted: Dict[str, str]) -> List[Allowance]:
        """A possession multiple is not a number — it is N × whatever the daily number turns
        out to be, so it becomes a counter only once there is a daily counter to multiply.
        Every daily quota and cap gets one, from the closest multiple that speaks about its
        fish; a possession number a rule states outright settles against it like any other.

        The derived counter carries its PARENT's provenance — it is the region's 5, doubled,
        and it takes the region's place on the ladder — and names the rule that doubled it."""
        out = []
        mults = sorted(self.multiples, key=lambda m: m[2].rank)
        for a in raw:
            if a.outcome.kind != "quota" or a.period != "daily" or a.rule_id in lifted:
                continue
            m = next((m for m in mults if m[0].covers(_fish_scope(a.scope))), None)
            if m is None:
                continue
            out.append(Allowance(a.scope, Outcome("quota", a.outcome.n * m[1], "possession",
                                                  a.pooled),
                                 a.source, a.applies, a.within, derived_from=a,
                                 multiplier=m[1], multiplied_by=m[2]))
        return out

    def nested(self, a: Allowance, b: Allowance) -> bool:
        """A clause and the allowance it is inside — or two clauses re-homed together under a
        promoted one. They count together; neither carves the other."""
        return (b.rule_id in self.family.get(a.rule_id, ()) or
                a.rule_id in self.family.get(b.rule_id, ()) or
                (a.derived_from is not None and self.nested(a.derived_from, b)) or
                (b.derived_from is not None and self.nested(a, b.derived_from)))

    def _settle(self) -> None:
        # A RULE INSIDE THE DAY DECIDES NOTHING HERE. "No Fishing from one hour after sunset to
        # one hour before sunrise" and "kokanee — Saturday and Sunday only" are true, and a
        # date cannot settle them; they ride on the row and in the verdict as notes, and they
        # neither bind a date nor take fish out of any other rule.
        live = [a for a in self.allowances
                if not self.status[a] and a.applies.can_bind and not a.applies.within_day]
        for A in live:
            for B in live:
                if A is B or A.period != B.period or self.nested(A, B):
                    continue
                if not A.scope.meets(B.scope):
                    continue
                if not self._speaks_before(B, A):
                    continue
                self.carves[A].append(B)
        # AN ALLOWANCE NOTHING IS LEFT OF. Carved out entirely, all year, by one rule that
        # covers everything it names: it is replaced, and says by what. Carved in part, or
        # only in season, it binds — for the fish and days that remain — and its carves ride
        # with it so a reader sees "trout and char, other than rainbow trout".
        for A in live:
            whole = [c for c in self.carves[A]
                     if c.applies.always and _fish_scope(c.scope).covers(_fish_scope(A.scope))]
            if whole:
                c = min(whole, key=lambda c: (c.rank, c.outcome.rank, c.rule_id))
                same = (c.outcome.same_answer(A.outcome) and c.scope == A.scope)
                self.status[A] = (SAME if same else
                                  WEAKER if (c.rank == A.rank and c.scope == A.scope) else
                                  f"replaced by {c.source.words()}")

    def _speaks_before(self, B: Allowance, A: Allowance) -> bool:
        """Does B take fish out of A? The ladder, as one pairwise test."""
        same_fish = _fish_scope(A.scope) == _fish_scope(B.scope)
        narrower = (_fish_scope(A.scope).covers(_fish_scope(B.scope))
                    and not _fish_scope(B.scope).covers(_fish_scope(A.scope)))
        if B.kind == "gate" and A.kind != "gate":
            # A BOUND CARVES NOTHING. "None under 60 cm" on char is true beside the five, not
            # instead of it — a size class sent back does not take the fish out of the count.
            return False
        if A.kind == "gate":
            # A bound is a take of zero on a size class. Only another bound of the same kind on
            # the same fish replaces it — a closer number does not lift a minimum size.
            return (B.kind == "gate" and same_fish and B.scope.size.bound == A.scope.size.bound
                    and B.rank < A.rank)
        if B.rank < A.rank:
            if A.is_zero:
                # "except closures unless they are lifted in this water's regs": a closer rule
                # about the same fish, or fewer, is that lift. A wider number is not — "Trout/
                # char: 5" does not open wild steelhead. And nothing but a lift opens a
                # "No Fishing" on the water.
                return (not shuts_the_water(A.scope)
                        and _fish_scope(A.scope).covers(_fish_scope(B.scope)))
            return True
        if B.rank > A.rank:
            return False
        # Equal authority. Same subject: the stricter stands. Narrower subject: the narrower
        # carves the wider only where the book must have meant it — a narrower cap that could
        # never bind otherwise ("2 hatchery steelhead over 50 cm allowed" inside "1 over 50
        # cm"; "20 brook trout" beside "trout and char: 4"). A narrower cap with a smaller
        # number simply counts inside the wider one, and both bind.
        if same_fish and A.scope.size == B.scope.size and A.scope.origin == B.scope.origin:
            return B.outcome.rank < A.outcome.rank
        if same_fish and A.scope.covers(B.scope) and not B.scope.covers(A.scope):
            # The same fish, a narrower origin or size class: a sub-limit that counts inside.
            return (not A.is_zero and B.outcome.kind == "quota"
                    and (A.outcome.kind == "unlimited" or B.outcome.n > A.outcome.n))
        if narrower and not A.is_zero:
            if B.is_zero:
                return True                          # a narrower release inside a number
            return (B.outcome.kind == "quota" and A.outcome.kind == "quota"
                    and B.outcome.n > A.outcome.n) or (B.outcome.kind == "unlimited")
        # A take of zero at equal authority is beaten by nothing narrower: the strictest stands.
        return False

    # -- reading ----------------------------------------------------------------------
    def in_force(self, a: Allowance) -> bool:
        return not self.status[a] and a.applies.can_bind

    def carved_out(self, a: Allowance, species: str, origin: Origin, length_cm,
                   on: Optional[Tuple[int, int]]) -> Optional[Allowance]:
        """The closer rule that took THIS fish out of `a`, on this date — or None. `on=None`
        asks about the standing answer, which only a year-round rule can change."""
        if a.derived_from is not None:
            c = self.carved_out(a.derived_from, species, origin, length_cm, on)
            if c is not None:
                return c
        for c in self.carves[a]:
            if not (c.applies.live(*on) if on is not None else _year_round(c)):
                continue
            if c.contains(species, origin, length_cm):
                return c
        return None

    def binds(self, a: Allowance, species: str, origin: Origin, length_cm=None,
              on: Optional[Tuple[int, int]] = None) -> bool:
        """Does this counter bind this fish, on this date? `on=None` asks for the standing
        answer — what holds on any day no seasonal rule speaks."""
        if not self.in_force(a) or a.applies.within_day or not a.contains(species, origin, length_cm):
            return False
        if not (a.applies.live(*on) if on is not None else _year_round(a)):
            return False
        return self.carved_out(a, species, origin, length_cm, on) is None

    def _carved_always(self, a: Allowance, species: str, origin: Origin) -> bool:
        """Is this fish taken out of `a` on EVERY date — by a carve with no dates at all?"""
        if a.derived_from is not None and self._carved_always(a.derived_from, species, origin):
            return True
        return any(c.applies.always and c.contains(species, origin) for c in self.carves[a])

    def reaches(self, a: Allowance, species: str, origin: Origin) -> bool:
        """Does this counter speak about this fish on SOME date, at some length?"""
        return (self.in_force(a) and a.contains(species, origin)
                and not self._carved_always(a, species, origin))

    def counters(self, species: str, origin: Origin, length_cm=None,
                 on: Optional[Tuple[int, int]] = None) -> List[Allowance]:
        return [a for a in self.allowances if self.binds(a, species, origin, length_cm, on)]

    def universe(self) -> FrozenSet[str]:
        """Every leaf species some allowance names — the fish the table has rows for."""
        from pipeline.regs.table.subject import expand
        out = set()
        for a in self.allowances:
            if a.scope.is_everything:
                out |= expand(frozenset({"ALL_GAME_FISH"}))
            out |= a.scope.effective()
        return frozenset(out)

    def overlay(self, overrides: Iterable[Allowance], **kw) -> "Ledger":
        """Stage 2: this base, with a section's own rules laid on top."""
        base = [a for a in self.allowances if a.derived_from is None]
        return Ledger(list(base) + list(overrides),
                      multiples=kw.pop("multiples", None) or self.multiples, **kw)
