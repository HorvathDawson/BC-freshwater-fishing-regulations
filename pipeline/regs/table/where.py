"""WHERE a rule bites, when that is narrower than the water it is attached to.

`extent_text` is one field doing two unrelated jobs, and the difference matters to a reader:

    "within 23 m downstream of the lower entrance to any fishway"
        A place inside this water that nothing can draw — nobody has the location of every
        fishway in British Columbia. The honest answer is to print the distance and let the
        reader recognise the spot. It is a CAVEAT beside the answer, never the answer.

    "Regions 3, 5, 6, 7 and 8"
        An administrative area the atlas HAS. Every section already knows its region. This is
        not a caveat at all — it is a hard scope, and treating it as prose is why spear fishing
        reads wrong: "No spear fishing of any kind is permitted in Region 1, 2, and 4" sat
        beside a Region 2 water as a vague note instead of governing it, and "except burbot,
        which may also be speared in Regions 3, 5, 6, 7 and 8" never applied anywhere.

Twelve rules in the corpus name an area the atlas has; 256 name somewhere it does not. They are
told apart here rather than at each of the places that has to care.
"""
from __future__ import annotations
import re
from dataclasses import dataclass
from typing import FrozenSet, Optional

#: "Regions 3, 5, 6, 7 and 8" / "Region 2" / "Regions 1, 2 and 4"
_NUMBERED = re.compile(r"\bregions?\b((?:\s*\d+\s*(?:,|and|&)?)+)", re.I)


@dataclass(frozen=True)
class Where:
    kind: str                         # anywhere | regions | undrawable
    regions: FrozenSet[str] = frozenset()
    text: str = ""

    def bites_in(self, here: FrozenSet[str]) -> Optional[bool]:
        """True / False / None where it cannot be decided (an undrawable place)."""
        if self.kind == "anywhere": return True
        if self.kind == "regions":  return bool(self.regions & here)
        return None

    def words(self) -> str:
        if self.kind == "regions":
            r = sorted(self.regions, key=lambda x: int(x))
            return "Region " + (r[0] if len(r) == 1 else ", ".join(r[:-1]) + " and " + r[-1])
        return self.text


ANYWHERE = Where("anywhere")


def parse_where(extent_text: str | None) -> Where:
    """A numbered region list is a scope; anything else is a place we cannot place.

    Deliberately strict: "the Cariboo Region" and "the Thompson-Nicola Region" are real areas
    with real boundaries, but resolving a NAME to a region number needs a table this module does
    not own, and guessing at it would put a closure on the wrong river. They stay undrawable
    until that table exists, which is a smaller lie than the alternative.
    """
    t = (extent_text or "").strip()
    if not t:
        return ANYWHERE
    m = _NUMBERED.search(t)
    if m:
        nums = re.findall(r"\d+", m.group(1))
        if nums:
            return Where("regions", frozenset(nums), t)
    return Where("undrawable", frozenset(), t)
