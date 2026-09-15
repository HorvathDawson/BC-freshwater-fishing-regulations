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
        if self.is_everything:
            # ...except whatever it carves out. "Every fin fish other than burbot" does not
            # cover burbot, and "non-game fish" does not cover a game fish.
            return not (expand(self._open_excepts()) & (o.effective() or expand(o.fish)))
        if o.is_everything: return False
        return o.effective() <= self.effective()

    def covers(self, o: "Subject") -> bool:
        return (self._fish_covers(o) and self.origin.covers(o.origin)
                and self.size.covers(o.size) and self.water.covers(o.water)
                and (self.method is None or self.method == o.method))

    def disjoint(self, o: "Subject") -> bool:
        if self.fish and o.fish and not (expand(self.fish) & expand(o.fish)): return True
        if not self.origin.covers(o.origin) and not o.origin.covers(self.origin): return True
        if self.water is not Water.any and o.water is not Water.any and self.water is not o.water:
            return True
        return False

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
