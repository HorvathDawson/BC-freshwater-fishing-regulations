"""PROTOTYPE 5 — APPLICABILITY: "closed" is not one thing, and conflating them shuts rivers.

WHAT IT HANDLES
  The fold in P3 treated every `Closed` as absolute and immediately shut the Atnarko for
  kokanee on the strength of, among others:

      "Within 23 m downstream of the lower entrance to any fishway..."
      "No Fishing from one hour after sunset to one hour before sunrise"

  Both are real closures. Neither closes the river. The first names a place INSIDE the water
  that nothing can draw — nobody knows where every fishway in British Columbia is — and the
  second names a time of day. The live page has a comment about exactly this: those two buffer
  rules "rendered as a bare 'All game fish · 0 · you may not fish for it' — the province's two
  buffer rules read as a total closure of every river they are on."

  That was patched there in the wording. It is a TYPE problem: a rule's outcome and the
  CONDITIONS UNDER WHICH IT BITES are two different facts, and squashing them into one made
  "closed" ambiguous between three unrelated statements.

      Always          this outcome is the answer here, now
      Window(w)       ... during these dates/hours — true, but not the answer all year
      Somewhere(t)    ... in a place inside this water that cannot be drawn

  Only `Always` can WIN a row. The other two attach to the row as caveats, where a reader can
  act on them, and can never silently replace the answer.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class Applies:
    kind: str                       # always | window | somewhere
    detail: str = ""

    @property
    def can_win(self) -> bool:
        """A SEASON IS NOT A DISQUALIFICATION.

        This used to be `kind == "always"`, which made a windowed rule a caveat that could never
        be the answer whatever the date — so the Fording's OWN "trout and char, release all,
        Jun 15 – Mar 31" lost to Region 4's year-round 5, which is the exact inversion the page
        has fifty lines of comment about fixing.

        A window says WHEN a rule is the answer, not whether. It stays in the running and takes
        its sorted place; what it cannot do is be resolved at BUILD time, because "today" is
        something the reader's screen knows and the pipeline does not. So the ordered candidates
        ship, each carrying its own window, and the client takes the first one live now — one
        filter, not four ladders.

        A place nobody can draw is the different thing: 23 m below a fishway is not a question
        about the date, and no amount of client-side evaluation can settle it. That one still
        cannot win.
        """
        return self.kind in ("always", "window")

    @property
    def always(self) -> bool:
        """True where the rule needs no date to be the answer — the year-round default the
        pipeline can safely precompute."""
        return self.kind == "always"

    def caveat(self, outcome_word: str) -> str:
        if self.kind == "window":    return f"{outcome_word} — but only {self.detail}"
        if self.kind == "somewhere": return f"{outcome_word} — but only {self.detail}"
        return ""


ALWAYS = Applies("always")


def applies_of(windows: List[str] | None, extent_text: str | None,
               all_year: bool = True, *, section_label: str | None = None,
               from_time: str | None = None, to_time: str | None = None,
               weekdays: List[str] | None = None, unless: bool = False) -> Applies:
    """The one place this is decided. `extent_text` is the piece of water a rule names and the
    atlas could not cut — so the rule is true SOMEWHERE in here and the reader has to recognise
    the spot on the ground. That is a caveat, never an answer.

    `unless` is the catalogue's `windows_are: excepts`: the dates are when the rule does NOT
    apply. "Kokanee catch and release, EXCEPT Apr 1-3 and July 1-2" is a year-round release
    with five days out of it; read as a window it became a five-day release, and the Upper
    West Arm of Kootenay Lake printed Region 4's fifteen kokanee the rest of the year."""
    if unless and windows:
        return Applies("always", "except " + _w(windows))
    # AN EXTENT THE ATLAS ALREADY CUT IS NOT A CAVEAT. Where the section being drawn IS the
    # place the rule names, the rule is simply the answer here. Treating it as "true somewhere
    # in here" put the Kootenay's Main Body rules behind the regional quota and told a reader
    # to keep fifteen kokanee on a release-only water.
    from pipeline.regs.table.where import cut_for_this
    if extent_text and cut_for_this(extent_text, section_label):
        extent_text = None
    # A TIME OF DAY IS A WINDOW, and going unread it made one absolute. "No Fishing from one
    # hour after sunset to one hour before sunrise" reached the fold as an unconditional
    # closure, `_shuts_the_water` was true, and it took every row on the Harrison — the whole
    # river shut, around the clock, off a dusk-to-dawn rule. Eight closures corpus-wide.
    when = []
    if from_time or to_time:
        when.append(f"{from_time or '?'} to {to_time or '?'}")
    if weekdays:
        when.append(", ".join(weekdays) + " only")
    if extent_text:
        return Applies("somewhere", str(extent_text))
    if when:
        return Applies("window", " · ".join(when + ([_w(windows)] if windows and not all_year
                                                     else [])))
    if windows and not all_year:
        # A window ships as {"from":..,"to":..} objects, not strings — one more field whose
        # shape every consumer has to already know. Absorbed here, once.
        return Applies("window", _w(windows))
    return ALWAYS


_MON = ("", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


def _w(windows) -> str:
    def d(p): return f"{_MON[p['month']]} {p['day']}" if isinstance(p, dict) else str(p)
    def one(o): return (f"{d(o.get('from'))} – {d(o.get('to'))}"
                        if isinstance(o, dict) else str(o))
    return ", ".join(one(o) for o in (windows or []))
