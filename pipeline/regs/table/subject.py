"""PROTOTYPE 1 — SUBJECT: what a rule is about, as one comparable value.

WHAT IT HANDLES
  Today the subject of a rule is smeared across seven optional fields — `species`,
  `species_except`, `origin`, `over_cm`, `under_cm`, `band`, `water`, `method` — and every
  consumer rebuilds it. `speciesOf()` is called 24 times in the page. Two of those rebuilds
  disagreeing is what hid cutthroat under bull trout.

  Here it is ONE value with NO optional fields. That is the whole trick:

    origin = both        not None.  "Absent" stops meaning "unknown or all, you decide" and
                         starts meaning `both`, which is a thing you can print and compare.
    fish   = a frozenset, canonical.  Order cannot vary, so a key cannot fork.
    size   = a tagged value, never two loose ints that may or may not both be set.

  And it knows how subjects RELATE, which nothing in the corpus does today:

    covers(a, b)   a applies to everything b applies to (so a is the general rule, b narrower)
    disjoint(a, b) they cannot both describe one fish — no precedence question arises
    join(a, b)     one subject meaning both, or None if they do not merge cleanly.
                   This is "combine if rules match": wild+hatchery -> both.
"""
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import FrozenSet, Optional, Tuple

from pipeline.regs.parsing.catalogue import SPECIES_GROUPS
from pipeline.regs.table.size import ANY as SIZE_ANY, Size, size_of  # noqa: F401


class Origin(str, Enum):
    wild = "wild"; hatchery = "hatchery"; both = "both"
    def covers(self, o: "Origin") -> bool:
        return self is Origin.both or self is o


class Water(str, Enum):
    any = "any"; stream = "stream"; lake = "lake"
    def covers(self, w: "Water") -> bool:
        return self is Water.any or self is w


#: A group the catalogue lists with NO members. Two of them, and they mean opposite things —
#: which is exactly why an "open group" cannot be left for each consumer to interpret:
#:
#:   ALL_FIN_FISH   every fin fish. The top of the fish axis.
#:   NON_GAME_FISH  every fin fish that is NOT a game fish. A complement, expressible with the
#:                  `excepts` this type already has.
#:
#: Expanding both to the empty set — which is what "no members listed" naively gives — put them
#: at the BOTTOM of the lattice instead of the top, so every rule in the book "covered" them.
#: On the Fraser that pulled the kokanee, trout, whitefish, crappie and crayfish rules into the
#: protected-species row, all marked "suspended while the water is closed". The declaration
#: belongs in the data; it is written here because the data does not carry it yet.
OPEN_GROUPS = {"ALL_FIN_FISH": frozenset(),                       # universal
               "NON_GAME_FISH": frozenset({"ALL_GAME_FISH"})}     # universal minus these


def expand(codes: FrozenSet[str]) -> FrozenSet[str]:
    """Every code an umbrella stands for, TRANSITIVELY and down to LEAVES.

    Two things the flat version got wrong, both found by running this against the book:

      TRANSITIVE. A group may contain a group. Expanding one level left `TROUT` unexpanded
      inside `TROUT_CHAR`, so a rule about trout did not register under a rule about trout
      and char.

      LEAVES ONLY. `ALL_GAME_FISH`'s member list holds the individual fish and NOT the code
      `TROUT_CHAR`, even though it holds all thirty of that group's members. Comparing sets
      that still contain group codes therefore said "all game fish does not cover trout and
      char" — and the prototype duly reported a keep limit of 5 for trout on a river closed
      to every game fish. Compare on the leaves and the question does not arise.
    """
    out, stack = set(), list(codes)
    while stack:
        c = stack.pop()
        kids = SPECIES_GROUPS.get(c)
        if kids:
            stack.extend(k for k in kids if k not in out)
            out.update(kids)                 # keep members; the group CODE is not a fish
        else:
            out.add(c)                       # a leaf: an actual species
    return frozenset(out - set(SPECIES_GROUPS))


@dataclass(frozen=True)
class Subject:
    fish: FrozenSet[str] = frozenset()      # empty = everyone; groups kept AS groups
    origin: Origin = Origin.both
    size: Size = SIZE_ANY
    water: Water = Water.any
    method: Optional[str] = None
    excepts: FrozenSet[str] = frozenset()

    def __post_init__(self):
        """ONE SUBJECT, ONE SPELLING.

        `ALL_FIN_FISH` names every fin fish; naming nobody means the same thing. Held as two
        different values they covered each other while comparing unequal — antisymmetry gone,
        and `table()` then refuses to absorb either into the other, so the same answer prints
        twice under two headings. A universal group with nothing carved out IS the empty
        subject; `NON_GAME_FISH` is not, because it carries a complement.
        """
        bare = {g for g, comp in OPEN_GROUPS.items() if not comp}
        if self.fish and self.fish <= bare:
            object.__setattr__(self, "fish", frozenset())

    # -- the lattice ------------------------------------------------------------------
    @property
    def is_everything(self) -> bool:
        """Names nobody, or names an open group: either way it speaks about every fish."""
        return (not self.fish) or bool(self.fish & set(OPEN_GROUPS))

    def _open_excepts(self) -> FrozenSet[str]:
        out = set(self.excepts)
        for g in (self.fish & set(OPEN_GROUPS)):
            out |= OPEN_GROUPS[g]
        return frozenset(out)

    def effective(self) -> FrozenSet[str]:
        """The fish this subject is ACTUALLY about: the umbrella minus what it carves out.

        `excepts` was first written as a veto — "do not cover anything that touches an excepted
        fish" — and that broke the one law a lattice may not break: reflexivity. "All game fish
        OTHER THAN BURBOT" did not cover itself, because burbot is inside `ALL_GAME_FISH`, so
        the rule excluded its own subject and vanished from the table. An exception SUBTRACTS.
        """
        return expand(self.fish) - expand(self.excepts)

    def _fish_covers(self, o: "Subject") -> bool:
        """Containment on the fish axis — SYMMETRIC in the universal case.

        The first version special-cased "self is universal" and tested the carve-outs against
        `o.effective() or expand(o.fish)`. Where `o` is ALSO universal both of those are empty,
        so the intersection was vacuously empty and the answer unconditionally True — the
        excepts were never consulted. Two consequences, the same class of hole as the two this
        module has already been bitten by:

          "every fin fish EXCEPT crayfish" covered plain "everything", crayfish included;

          and the moment a rule names NON_GAME_FISH — declared in OPEN_GROUPS, not yet used by
          the corpus — the order INVERTS: the complement covers the universe, which breaks
          transitivity and antisymmetry together and would let a non-game-fish rule govern the
          kokanee row.

        Comparing complements instead makes both strict, and keeps the order sound whichever
        way the data moves.
        """
        if not self.is_everything and o.is_everything:
            return False
        if self.is_everything and o.is_everything:
            return expand(self._open_excepts()) <= expand(o._open_excepts())
        if self.is_everything:
            return not (expand(self._open_excepts()) & o.effective())
        # A subject that carves out everything it names speaks about no fish, and the empty set
        # is a subset of everything — so without this it would be covered by every rule alive.
        if not o.effective():
            return False
        return o.effective() <= self.effective()

    def covers(self, o: "Subject") -> bool:
        return (self._fish_covers(o) and self.origin.covers(o.origin)
                and self.size.covers(o.size) and self.water.covers(o.water)
                and (self.method is None or self.method == o.method))

    # `disjoint()` USED TO LIVE HERE. It had no callers anywhere in the repo, and all three of
    # its clauses were wrong — it read an open group's empty expansion as "no overlap", and
    # ignored `excepts`, `size` and `method` entirely. A loaded gun with the safety off is worse
    # than a missing feature, so it is gone rather than fixed; write it when something needs it,
    # against the laws in the test suite.

    def join(self, o: "Subject") -> Optional["Subject"]:
        """One subject meaning both, or None. Only ever across ONE axis — merging two
        differences at once invents a subject neither rule wrote."""
        diff = [f for f in ("fish", "origin", "size", "water", "method", "excepts")
                if getattr(self, f) != getattr(o, f)]
        if not diff: return self
        if len(diff) > 1: return None
        (f,) = diff
        if f == "origin" and {self.origin, o.origin} == {Origin.wild, Origin.hatchery}:
            return Subject(self.fish, Origin.both, self.size, self.water, self.method, self.excepts)
        # NOT on the fish axis. Unioning two fish sets invented "All game fish, Kokanee" — a
        # subject neither rule wrote, and a heading no reader would recognise. A row naming
        # several fish comes from ONE rule that named them; where a broad rule and a narrow one
        # agree, the broad one absorbs the narrow (see `table`), which is a different move.
        return None

    # -- how it reads -----------------------------------------------------------------
    def words(self, name, *, is_release: bool = False) -> Tuple[str, str]:
        """(who it is about, what qualifies it) — the two cells, from the value itself.

        `is_release` gates one phrase only. "Wild and hatchery" answers a question a bare
        "release" raises — is that wild ones, hatchery ones, or all of them — and a NUMBER never
        raises it. Printed on every origin-split row it lands on 22 to earn its keep on two."""
        who = ", ".join(sorted(name(c) for c in self.fish)) if self.fish else "Everything"
        q = []
        if self.origin is not Origin.both: q.append(f"{self.origin.value} only")
        elif is_release and self.fish and any(_split_by_origin(c) for c in self.fish):
            q.append("wild and hatchery")
        if not self.size.is_any: q.append(self.size.words())
        if self.water is not Water.any: q.append(f"in {self.water.value}s")
        if self.method: q.append(f"by {self.method.replace('_',' ')}")
        if self.excepts: q.append("except " + ", ".join(sorted(name(c) for c in self.excepts)))
        return who, " · ".join(q)


_ORIGIN_SPLIT: set = set()
def note_origin_split(codes) -> None:
    """Which fish the book splits by origin. `wild and hatchery` is only worth saying where
    somebody somewhere said `wild only` — otherwise it is noise on 22 rows to earn it on two."""
    _ORIGIN_SPLIT.update(codes)
def _split_by_origin(code: str) -> bool:
    return code in _ORIGIN_SPLIT
