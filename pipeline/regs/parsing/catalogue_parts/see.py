"""`See` — a pointer to the row whose regulations govern, and what it is to the reader.

Split out of `pipeline/regs/parsing/catalogue.py`, which re-exports every name; import from
there."""

from __future__ import annotations

import re

from typing import TYPE_CHECKING, List
from pydantic import ConfigDict, Field, model_validator

from .vocab import _Terse
from .text import squash

if TYPE_CHECKING:  # annotations only — importing these at run time would be circular
    from .entry import CatalogueEntry


class See(_Terse):
    """A POINTER, NOT A RULE: "See Lonzo Creek", "A tributary of Slocan River. See Slocan River",
    "For regulations on the mainstem of the West Road River, see Region 5".

    Such a row (or clause) states no regulation of its own; it names the row whose regulations
    govern. It was an `advisory` rule quoting the pointer, which bound to the water and showed the
    words as a rule — 57 of them — and the reader could not follow it anywhere. As an edge it
    binds nothing and links to the entry it names (`entry_ids`, every one of which must exist:
    the bundle build refuses a dangling one, and `test_see_pointers` checks the corpus).

    A pointer whose target is NOT an entry (prose on another page, a sign at a trailhead) cannot
    be followed; it says so in `unresolved` instead — flagged, never guessed at.

    `verbatim` is the printed pointer, a contiguous run of the row. A row printed IN FULL in two
    region tables (MU 6-1 lakes in both Region 5 and Region 6) points at its twin with the whole
    row as `verbatim`: the book cross-lists it, and the copy under the other region's heading
    binds nothing (`see_relation` names it a `twin`).

    WHAT THE POINTER IS TO THE READER is derived, never stored (`see_relation`): an `alias` when
    this row's water IS the target's (Jones Lake -> Wahleach Lake, one lake under two names), a
    `twin` when the target prints the same row, else `see` (a different water governed by the
    target's rules)."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)
    verbatim: str = Field(..., min_length=1)
    entry_ids: List[str] = Field(default_factory=list)
    unresolved: str = ""

    @model_validator(mode="after")
    def _one_way(self) -> "See":
        if bool(self.entry_ids) == bool(self.unresolved.strip()):
            raise ValueError("a `see` names the entries it points at (`entry_ids`) OR says why it "
                             "points at none (`unresolved`) — exactly one")
        if len(set(self.entry_ids)) != len(self.entry_ids):
            raise ValueError(f"see.entry_ids repeats an entry: {self.entry_ids}")
        return self


#: THE WORDS OF A POINTER: "see X" / "also see X" / "see X regulations". Checked on `advisory`
#: rules only — a pointer written as one is refused (`CatalogueEntry`).
_POINTER_WORDS = re.compile(r"\bsee\s+(?!page\b|sign\b|note\b|tables\b|ice hut|mercury|the definition)\w")
#: ...unless what it points at is not a row: prose on another page, a sign, a warning, a
#: definition. Those stay information (a page pointer cannot be followed to an entry).
_NOT_A_WATER = re.compile(r"\bpage\b|\bsign\b|warning|definition")


def _letters(text: str) -> str:
    """Only the letters and digits of a row — two printings of one row differ in punctuation
    ("quota = 2; bait ban" / "quota = 2 Bait ban") and emphasis, never in words."""
    return re.sub(r"[^a-z0-9]", "", squash(text))


def see_relation(entry: "CatalogueEntry", targets: List["CatalogueEntry"]) -> str:
    """What a pointer IS, from the two rows — `alias`, `twin` or `see` (see `See`).

      twin   every target prints this row's words (a row cross-listed under two regions);
      alias  a row that is ONLY the pointer, whose water is among its targets' (the row is
             another name for it) — the book's "See Wahleach Lake" under JONES LAKE, which the
             alias table already holds. A row with rules of its own is never an alias: West
             Road River's tributaries row matches the river and points at the mainstem's row,
             and it is the tributaries' own rules that make it a row;
      see    otherwise: a different water (or part), governed by the target's rules."""
    if targets and all(_letters(t.regs_verbatim) == _letters(entry.regs_verbatim)
                       for t in targets):
        return "twin"
    theirs = {m for t in targets for m in t.matched}
    if entry.pointer_only and entry.matched and set(entry.matched) <= theirs:
        return "alias"
    return "see"
