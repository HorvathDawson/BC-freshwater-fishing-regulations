"""THE TROUT TABLE'S WORDS, dates first (trout table plan, ticket G; contract 2.4 and 2.5). Not wired into a release.

ONE MODULE STATES BOTH SIDES: every composer here has its parser here, so the proof (ticket J) reads the shipped
words back with the grammar they were written in, and a text it cannot parse fails (`WordsError`), never "free text".
F and H take every string from this module and grow it as they go; G2's catalogue lists what it can say.

THE DATE LINES (spec 3.3; owner 15:36; one example each):
  tighter   "until Sep 30, then closed to trout and char"     what tightens, never a reopening day
  opens     "until Mar 31"                                    a release that ends (keeping follows)
  looser    "rule changes Apr 1"                              a change that only loosens: no details
  reopens   "reopens Nov 1"                                   ONLY on something closed today (`is_reopening`)
  card      "Until Oct 31, then closed to all fishing"        the same line, standing alone above the table
THE "THEN" WORDS (`then_words`, joined with ", "): "release", "closed to all fishing", "closed to trout and char",
  "closed for steelhead", "hatchery only", "50 cm (20 in) or longer", "50 cm (20 in) or less", "only 1",
  "only 1 measuring 50 cm (20 in) or longer", "none between 60 cm (24 in) and 90 cm (35 in)",
  "only 2 in your possession", and F's box form "1 in all".
THE SHEET SENTENCES (`sheet_sentence`, DateSet.sheet): "From Nov 1 every wild one goes back.", "The rule changes on
  Apr 1.", "It reopens on Nov 1.", "From Apr 1 it may be kept.", "From Nov 1: only 1."
THE COLUMN RULE (contract DateSet.row, round 11): a Release / Closed column date over `COLUMN_MAX` (30) characters goes
  under the fish's name (`column_fits`; `timeline.place_dates` applies it).

A DATE IS "Mon D" from THE CALENDAR SPEC's month names (gate 4: no other calendar table). NO DATE LINE NAMES A WEEKDAY:
Python does not know the calendar year (contract 2.5), and every composer refuses a line that names one. No line says
"today" (the app's `{day}` slot is the top line's, not a date line's). No date names Feb 29 (P06: it reads Feb 28's).
"""
from __future__ import annotations

import re
from functools import lru_cache
from itertools import combinations
from typing import Dict, NamedTuple, Optional, Tuple

from pipeline.common import calendar_spec as SPEC
from pipeline.deliver import calendar as K
from pipeline.deliver.answers import common
from pipeline.deliver.answers.common import AnswersError

#: a Release / Closed column date longer than this goes under the fish's name (round 11, contract DateSet.row)
COLUMN_MAX = 30

#: Mutation pin (owner 15:36 "drop reopen unless it is currently closed"): False = a coming closure also says the day it
#: reopens (the round 11 card's "…, reopens Nov 1")
A_NO_REOPENING_ON_A_COMING_CLOSURE = True


class WordsError(AnswersError):
    """A text the table may not say, or one the grammar cannot read back."""


# --------------------------------------------------------------------------------------------
# Days
# --------------------------------------------------------------------------------------------

_TODAY = re.compile(r"\btoday\b", re.I)
_DAY = r"(" + "|".join(SPEC.MONTHS) + r") (\d{1,2})"


def day_words(day: int) -> str:
    """A leap-calendar day (1..366) as the book prints it: "Sep 30". Feb 29 is never a date line's day (P06)."""
    if day == K.LEAP:
        raise WordsError("trout words: no date line names Feb 29 (it reads Feb 28's regulations)")
    m, d = K.month_day(day)
    return f"{SPEC.MONTHS[m - 1]} {d}"


def _day_of(mon: str, d: str) -> int:
    m = SPEC.MONTHS.index(mon) + 1
    n = int(d)
    if not 1 <= n <= SPEC.LAST_DAY[m - 1] or (m, n) == (2, 29):
        raise WordsError(f"trout words: {mon} {d} is not a date a line says")
    return SPEC.day_index(m, n)


def _checked(text: str) -> str:
    if common.names_a_weekday(text):
        raise WordsError(f"trout words: a date line names a weekday: {text!r}")
    if _TODAY.search(text):
        raise WordsError(f"trout words: a date line says today: {text!r}")
    return text


# --------------------------------------------------------------------------------------------
# What tightens
# --------------------------------------------------------------------------------------------

def _n(v) -> str:
    return str(int(v)) if float(v).is_integer() else str(v)


def _scope_words(scope) -> str:
    if scope == "water":
        return "closed to all fishing"
    if scope == "trout_char":
        return f"closed to {common.lc_names(sorted(common.trout_char()))}"
    return f"closed for {common.each_name(list(scope))}"


def part_words(p: tuple) -> str:
    """One `timeline.Part` in words."""
    k = p[0]
    if k == "release":
        return "release"
    if k == "closed":
        return _scope_words(p[1])
    if k == "hatchery_only":
        return "hatchery only"
    if k == "floor":
        return common.band_words(p[1], None)
    if k == "ceil":
        return common.band_words(0, p[1])
    if k == "count":
        return f"only {_n(p[1])}"
    if k == "in_all":
        return f"{_n(p[1])} in all"
    if k == "band":
        n, a, b = p[1], p[2], (None if p[3] == float("inf") else p[3])
        if n == 0:
            if not a or b is None:
                raise WordsError("trout words: a band keeping none at an end is a size floor or ceiling")
            return f"none {common.band_words(a, b, False, False)}"
        return f"only {_n(n)} measuring {common.band_words(a, b)}"
    if k == "possession":
        return f"only {_n(p[1])} in your possession"
    raise WordsError(f"trout words: no words for {p!r}")


def then_words(then) -> str:
    """What tightens, every part in its order: "hatchery only, 50 cm (20 in) or longer"."""
    if not then:
        raise WordsError("trout words: a tighter change with nothing to say")
    if any(p[0] in ("release", "closed") for p in then) and len(then) > 1:
        raise WordsError(f"trout words: a release or a closure is said alone, not with {then!r}")
    return ", ".join(part_words(p) for p in then)


# --------------------------------------------------------------------------------------------
# The date lines
# --------------------------------------------------------------------------------------------

def until(last: int, then) -> str:
    return _checked(f"until {day_words(last)}, then {then_words(then)}")


def until_bare(last: int) -> str:
    return _checked(f"until {day_words(last)}")


def rule_changes(day: int) -> str:
    return _checked(f"rule changes {day_words(day)}")


def reopens(day: int) -> str:
    return _checked(f"reopens {day_words(day)}")


def date_line(change) -> str:
    """A `timeline.Change` as its date line (spec 3.3): the row's, a box's, a band's or the top line's."""
    k = change.kind
    if k == "tighter":
        line = until(change.last, change.then)
        if not A_NO_REOPENING_ON_A_COMING_CLOSURE and change.then[0][0] == "closed" and change.lasts:
            line += f", reopens {day_words((change.day + change.lasts - 1) % K.DAYS + 1)}"
        return line
    if k == "opens":
        return until_bare(change.last)
    if k == "looser":
        return rule_changes(change.day)
    if k == "reopens":
        return reopens(change.day)
    raise WordsError(f"trout words: no date line for a {k!r} change")


def card_line(text: str) -> str:
    """The card line above the table: a date line standing alone, so it starts with a capital ("Until Oct 31, then
    closed to all fishing")."""
    parse_date_line(text)
    return common.cap(text)


def is_reopening(text: str) -> bool:
    return parse_date_line(text).form == "reopens"


def column_fits(text: str) -> bool:
    """Whether a Release / Closed column date fits under the column's word (else it goes under the fish's name)."""
    return len(text) <= COLUMN_MAX


def sheet_sentence(change) -> str:
    """The coming change as the fish sheet says it (DateSet.sheet), by the same rule as the date line."""
    k = change.kind
    day = day_words(change.day)
    if k == "looser":
        return _checked(f"The rule changes on {day}.")
    if k == "reopens":
        return _checked(f"It reopens on {day}.")
    if k == "opens":
        return _checked(f"From {day} it may be kept.")
    if k != "tighter":
        raise WordsError(f"trout words: no sheet sentence for a {k!r} change")
    then = change.then
    if len(then) == 1:
        p = then[0]
        said = {"release": "every one goes back", "hatchery_only": "every wild one goes back"}.get(p[0])
        if said:
            return _checked(f"From {day} {said}.")
        if p[0] == "closed":
            if p[1] == "water":
                return _checked(f"From {day} the water is closed to all fishing.")
            if p[1] == "trout_char":
                return _checked(f"From {day} it is {_scope_words(p[1])}.")
            return _checked(f"From {day} it is closed.")
    return _checked(f"From {day}: {then_words(then)}.")


# --------------------------------------------------------------------------------------------
# The grammar (J reads every date text back with it)
# --------------------------------------------------------------------------------------------

class Parsed(NamedTuple):
    form: str                       # "tighter" | "opens" | "looser" | "reopens" (the change kinds)
    day: Optional[int]              # looser / reopens: the first day of the new answer
    last: Optional[int]             # tighter / opens: the last day of today's answer
    then: Tuple[tuple, ...]         # tighter: the parts


_CM = r"(\d+(?:\.5)?) cm \((\d+) in\)"
_RANGE = r"(\d+(?:\.5)?)–(\d+(?:\.5)?) cm \((\d+)–(\d+) in\)"


def _cm(v: str, inch: str) -> float:
    x = float(v)
    if common.inches(x) != int(inch):
        raise WordsError(f"trout words: {v} cm is not {inch} in")
    return int(x) if x.is_integer() else x


@lru_cache(maxsize=None)
def _closed_fish() -> Dict[str, Tuple[str, ...]]:
    tc = sorted(common.trout_char())
    out = {}
    for k in range(1, len(tc)):
        for c in combinations(tc, k):
            out[_scope_words(c)] = c
    return out


def _part_at(s: str, i: int) -> Tuple[tuple, int]:
    """The part that starts at s[i:], and where it ends."""
    rest = s[i:]
    tries = [
        (r"only (\d+) in your possession", lambda m: ("possession", int(m[1]))),
        (rf"only (\d+) measuring {_RANGE}", lambda m: ("band", int(m[1]), _cm(m[2], m[4]), _cm(m[3], m[5]))),
        (rf"only (\d+) measuring {_CM} or longer", lambda m: ("band", int(m[1]), _cm(m[2], m[3]), float("inf"))),
        (rf"only (\d+) measuring {_CM} or less", lambda m: ("band", int(m[1]), 0, _cm(m[2], m[3]))),
        (rf"none between {_CM} and {_CM}", lambda m: ("band", 0, _cm(m[1], m[2]), _cm(m[3], m[4]))),
        (r"only (\d+)", lambda m: ("count", int(m[1]))),
        (r"(\d+) in all", lambda m: ("in_all", int(m[1]))),
        (rf"{_CM} or longer", lambda m: ("floor", _cm(m[1], m[2]))),
        (rf"{_CM} or less", lambda m: ("ceil", _cm(m[1], m[2]))),
        (r"hatchery only", lambda m: ("hatchery_only",)),
        (r"release", lambda m: ("release",)),
        (r"closed to all fishing", lambda m: ("closed", "water")),
        (rf"closed to {re.escape(common.lc_names(sorted(common.trout_char())))}", lambda m: ("closed", "trout_char")),
    ]
    for pat, make in tries:
        m = re.match(pat + r"(?=, |$)", rest)
        if m:
            return make(m), i + m.end()
    if rest in _closed_fish():
        return ("closed", _closed_fish()[rest]), len(s)
    raise WordsError(f"trout words: cannot read {rest!r}")


def parse_then(s: str) -> Tuple[tuple, ...]:
    """`then_words`' inverse."""
    parts, i = [], 0
    while True:
        p, i = _part_at(s, i)
        parts.append(p)
        if i == len(s):
            break
        if not s.startswith(", ", i):
            raise WordsError(f"trout words: cannot read {s!r}")
        i += 2
    if then_words(tuple(parts)) != s:
        raise WordsError(f"trout words: {s!r} is not in the composer's form")
    return tuple(parts)


def parse_date_line(text: str) -> Parsed:
    """A date line (or the card line) back to its form, day and parts. Anything else raises WordsError."""
    _checked(text)
    s = text[:1].lower() + text[1:] if text.startswith("Until ") else text
    m = re.fullmatch(rf"until {_DAY}, then (.+)", s)
    if m:
        then = parse_then(m[3])
        if "reopens" in m[3]:
            raise WordsError(f"trout words: a coming closure carries a reopening day: {text!r}")
        return Parsed("tighter", None, _day_of(m[1], m[2]), then)
    m = re.fullmatch(rf"until {_DAY}", s)
    if m:
        return Parsed("opens", None, _day_of(m[1], m[2]), ())
    m = re.fullmatch(rf"rule changes {_DAY}", s)
    if m:
        return Parsed("looser", _day_of(m[1], m[2]), None, ())
    m = re.fullmatch(rf"reopens {_DAY}", s)
    if m:
        return Parsed("reopens", _day_of(m[1], m[2]), None, ())
    raise WordsError(f"trout words: cannot read the date line {text!r}")


def parse_sheet_sentence(text: str) -> Parsed:
    """`sheet_sentence`'s inverse."""
    _checked(text)
    m = re.fullmatch(rf"The rule changes on {_DAY}\.", text)
    if m:
        return Parsed("looser", _day_of(m[1], m[2]), None, ())
    m = re.fullmatch(rf"It reopens on {_DAY}\.", text)
    if m:
        return Parsed("reopens", _day_of(m[1], m[2]), None, ())
    m = re.fullmatch(rf"From {_DAY} it may be kept\.", text)
    if m:
        return Parsed("opens", _day_of(m[1], m[2]), None, ())
    m = re.fullmatch(rf"From {_DAY}(?::)? (.+)\.", text)
    if m:
        day, rest = _day_of(m[1], m[2]), m[3]
        fixed = {"every one goes back": (("release",),), "every wild one goes back": (("hatchery_only",),),
                 "the water is closed to all fishing": (("closed", "water"),),
                 f"it is {_scope_words('trout_char')}": (("closed", "trout_char"),)}
        if rest in fixed and not text[len(f"From {m[1]} {m[2]}"):].startswith(":"):
            return Parsed("tighter", day, None, fixed[rest])
        if rest == "it is closed":
            return Parsed("tighter", day, None, (("closed", None),))
        if text[len(f"From {m[1]} {m[2]}"):].startswith(":"):
            return Parsed("tighter", day, None, parse_then(rest))
    raise WordsError(f"trout words: cannot read the sheet sentence {text!r}")
