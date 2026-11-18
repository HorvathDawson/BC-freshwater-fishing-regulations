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
from typing import Dict, List, NamedTuple, Optional, Tuple

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


# ============================================================================================
# THE TABLE'S NOW WORDS (ticket F, contract 2.3 and 2.4): every text the table body says today, whole, with its parser
# ============================================================================================
# THE TOP LINE carries ONE `{day}` slot (contract Q-D2): "Up to 5 {day}", "Up to 2 {day}, or 5 if they’re rainbow
#   trout", "Up to 2 {day}, plus 2 Dolly Varden/bull trout", "Up to 6 rainbow trout or steelhead and 4 other trout and
#   char {day}", "Release every one {day}", "Closed for trout and char {day}", "Nothing to keep {day}".
# THE FOLD (the "Trout and char" entry's short line): "Up to 5", "Up to 2 or 5", "Up to 6 rainbow, 4 others",
#   "Release", "Closed", "None to keep".
# THE BOX: head "Keep 5 in all" / "Release" / "Closed"; label "Hatchery only"; strip parts joined by "; " ("any size",
#   "only 1 over 50 cm (20 in)", "only 1 over 50 cm (20 in), 2 if one is a hatchery steelhead", "only 1 over 50 cm
#   (20 in), not counting lake trout", "only 2 wild, each 40 cm (16 in) or less"); possession on its own line
#   ("In your possession: no more than 1 trout and char in all"); tag "Lake’s rule".
# THE BANDS: "counts toward the 5" (upto), "only 3" (own), "only 1 of these 4" / "only 3 between them" / "only 4 among them" /
#   "only 2 wild in all" (shared), "only 2 in your possession".
# A ROW'S LINES: its size ("30 cm (12 in) or longer", the kind's own words), "uses the 1 over 50 cm (20 in)", "only 1"
#   (its own limit inside a shared band), "only 1, counting any lake trout you keep", the possession line ("in your
#   possession: 1 day’s limit, not 2"), the stop duty "after keeping 1, stop fishing this river today" (the one text
#   that says "today": it is the angler's day, contract Q-D2), the weekday words; its sub "a rainbow over 50 cm (20 in)";
#   quiet lines "3 a day, but never more than 2 lake trout with you" and the steelhead lines.
# TOGETHER: "only 2 wild trout and char in all, each under 40 cm (16 in)", "only 1 from both lakes together".
# THE SHEET HEAD: "Keep · only 1 of these 4 · counts toward the 5", "Put it back", "Closed · do not fish for it".

DAY_SLOT = "{day}"
HATCHERY_ONLY = "Hatchery only"
YEAR_TITLE = "Trout and char through the year"
HEAD_RELEASE, HEAD_CLOSED = "Release", "Closed"
SHEET_RELEASE, SHEET_CLOSED = "Put it back", "Closed · do not fish for it"
FOLD_RELEASE, FOLD_CLOSED, FOLD_NONE = "Release", "Closed", "None to keep"
#: (owner 2026-11-17 23:35, "Keep 2 trout and char today (Recommended)": "Up to 2 is just confusing … up to 2 what") every
#: top line names the fish and the action
TOP_FISH = "trout and char"
TOP_RELEASE = f"Release every {TOP_FISH} {DAY_SLOT}"
TOP_CLOSED = f"Closed for {TOP_FISH} {DAY_SLOT}"
TOP_NONE = f"Keep no {TOP_FISH} {DAY_SLOT}"
#: T810 G2 Q8 (owner 15:41 "Once; steelhead says 'closed all year'"): a closed table's one fish that never reopens this
#: year, on its own row while the others' reopening day stands once under the top line
CLOSED_ALL_YEAR = "closed all year"
#: the steelhead lines (page v36 STEEL_LINE, today the app's `words.ts` STEEL_*; contract fight #6: Python's now)
STEEL_QUIET = {
    "possible_with_rules": "Steelhead rules apply here; steelhead may not be present in this water.",
    "known_with_rules": "Steelhead are known to be in this water.",
    "known_no_rules": "Steelhead have been recorded here, but no steelhead rule applies: treat any rainbow, however "
                      "big, as a rainbow trout.",
}


def _home() -> str:
    """The home exception `card.possession_duty` closes its line with (`display.rule.POSSESSION_HOME`, its one source)."""
    from pipeline.deliver.answers.display.rule import POSSESSION_HOME
    return f" ({POSSESSION_HOME})"


def _now(text: str) -> str:
    """A NOW text: no date line's day form, never "today" (the stop duty is the one exception, `stop_duty`)."""
    if _TODAY.search(text):
        raise WordsError(f"trout words: a table text says today: {text!r}")
    if "{" in text or "}" in text or text != text.strip() or not text:
        raise WordsError(f"trout words: not a whole phrase: {text!r}")
    return text


def fish_name(S: str) -> str:
    """The row's name as the book prints it ("Dolly Varden/bull trout")."""
    return _now(common.sp_name(S))


def short_fish(S: str) -> str:
    """The fold's one word for a fish: "rainbow" (rainbow trout), "steelhead"."""
    n = common.lc(common.sp_name(S))
    return n[:-len(" trout")] if n.endswith(" trout") and "/" not in n else n


def fish_words(codes) -> str:
    """Several fish in one phrase, as a kind row names them ("rainbow trout or steelhead", "brook, brown or cutthroat
    trout"; `tree.kind_names`)."""
    from pipeline.deliver.answers.tree import kind_names
    return kind_names(list(codes), conj=" or ")


# -- the top line and the fold ---------------------------------------------------------------

def top_up_to(n, alsos=(), more=(), plus=()) -> str:
    """One keep box: "Keep 2 trout and char {day}" (owner 23:35: the fish and the action), ", or 5 if they’re rainbow
    trout" for each stored `also` (the headline's), " and no more than 2 Dolly Varden/bull trout" for a lifted fish under
    its own number beside an `also`, ", plus 2 Dolly Varden/bull trout" for one without (`more` / `plus`: [(n, fish
    words)])."""
    s = f"Keep {_n(n)} {TOP_FISH} {DAY_SLOT}"
    if alsos:
        s += ", or " + " or ".join(_now(a) for a in alsos)
    for k, w in more:
        s += f" and no more than {_n(k)} {w}"
    if plus:
        s += ", plus " + common.join([f"{_n(k)} {w}" for k, w in plus])
    return s


def top_apart(named, others_n: Optional[float]) -> str:
    """Boxes counted apart: "Keep 6 rainbow trout or steelhead and 4 other trout and char {day}" (`named`: [(n, fish
    words)] for each box named by its fish; `others_n` the box of the other trout and char, None when every box is
    named)."""
    parts = [f"{_n(k)} {w}" for k, w in named]
    if others_n is not None:
        parts.append(f"{_n(others_n)} other trout and char")
    return f"Keep {common.join(parts)} {DAY_SLOT}"


def fold_up_to(n, alsos=()) -> str:
    """The folded entry's value (owner 23:35, "Make it match other rows in table"): one number in the Other fish rows'
    form, "2 a day" (the app's `others.ts` `shortValue`); several (", or 10 if they’re brook trout") keep "Up to 5 or 10"
    — the Other fish rows have no two-number form (asked of the owner, not invented)."""
    if not alsos:
        return f"{_n(n)} a day"
    return "Up to " + " or ".join(_n(k) for k in [n, *alsos])


def fold_apart(named, others_n: Optional[float]) -> str:
    parts = [f"{_n(k)} {w}" for k, w in named]
    if others_n is not None:
        parts.append(f"{_n(others_n)} others")
    return "Up to " + ", ".join(parts)


# -- the box -----------------------------------------------------------------------------------

def head_keep(n) -> str:
    return f"Keep {_n(n)} in all"


def head_keep_except(n, fish: str, most) -> str:
    """T834 (owner 13:15, "lead with 5"): a box whose region cap a water's own quota raised leads with the region's
    number and names the exception: "Keep 5, brook trout up to 20" — never "Keep 20 in all"."""
    return f"Keep {_n(n)}, {fish} up to {_n(most)}"


def tag_of(whose: str) -> str:
    """A water's own rule's tier ("This lake’s rule") -> the box's tag ("Lake’s rule")."""
    m = re.fullmatch(r"This (\w+)’s rule", whose or "")
    if not m:
        raise WordsError(f"trout words: no tag for {whose!r}")
    return f"{common.cap(m[1])}’s rule"


def strip_size(n, over_cm, extra_n=None) -> str:
    """The box's size sentence: "only 1 over 50 cm (20 in)"; with the steelhead pair (RULINGS Q16) ", 2 if one is a
    hatchery steelhead"."""
    s = f"only {_n(n)} over {common.cm_in(over_cm)}"
    if extra_n:
        s += f", {_n(n + extra_n)} if one is a hatchery steelhead"
    return s


def strip_size_except(n, over_cm, fish: str) -> str:
    """T810 G2 Q3b (owner 15:34 "once in top yes"): a size cap every fish of the box holds but a few, said ONCE in the
    strip with the fish it leaves out: "only 1 over 50 cm (20 in), not counting lake trout" (Tagish Lake), never the
    long "counting any …" line on each of its rows."""
    return f"only {_n(n)} over {common.cm_in(over_cm)}, not counting {fish}"


#: T810 G2 Q6 (owner 15:41 "Once in the top line"): every fish of the box at any size: the strip says it once
STRIP_ANY_SIZE = "any size"


def strip_wild(n, size: Optional[str]) -> str:
    """A wild quota of the box's own: "only 2 wild, each 40 cm (16 in) or less"."""
    return f"only {_n(n)} wild" + (f", each {size}" if size and size != "any size" else "")


def strip_possession(line: str) -> str:
    """Every row of the box shares one possession line: it stands once in the strip, "in your possession: …" (a strip
    part, as "only 1 over 50 cm (20 in)" is)."""
    return possession_row(line)


def strip(parts) -> Optional[str]:
    return "; ".join(parts) if parts else None


def box_possession(line: str) -> str:
    """T810 G2 Q4 (owner 15:41 "Own line, 'In your possession'"): the possession every row of a box shares, on its own
    line under the strip (`Box.possession`), with a capital: "In your possession: no more than 2 trout and char in all"."""
    return common.cap(possession_row(line))


# -- the bands ---------------------------------------------------------------------------------

def band_upto(n) -> str:
    """A row with no limit of its own: "counts toward the 5", the box's shared day total (owner 15:46: "the up to the x
    per day text is a little misleading", picked "'any of the 5'"; then 2026-11-17 21:33 on the picture pack, "The
    wording still using any of the x which I thought we changed?", picked "counts toward the 2")."""
    return f"counts toward the {_n(n)}"


def band_own(k) -> str:
    return f"only {_n(k)}"


def band_shared(k, m: int) -> str:
    """"only 1 of these 4"; a number at least the rows it spans (each row holds more than one fish: Tagish's lake trout
    and Dolly Varden "3 in the aggregate") reads "only 3 between them" over two rows, "only 4 among them" over more
    (T810 G2 Q2, owner 15:34)."""
    if k < m:
        return f"only {_n(k)} of these {m}"
    return f"only {_n(k)} between them" if m == 2 else f"only {_n(k)} among them"


def band_wild(n) -> str:
    """T810 G2 Q9 (owner, option 1): a wild quota over the box's wild twin rows, grouped under one white box: "only 2
    wild in all" (Dodd, Windsor, Horseshoe, Nanton and Ireland Lakes)."""
    return f"only {_n(n)} wild in all"


def band_wild_n(text: str) -> Optional[float]:
    """"only 2 wild in all" -> 2; None for any other band."""
    m = re.fullmatch(rf"only {_NUM} wild in all", text)
    return _num(m[1]) if m else None


def band_possession(k) -> str:
    return f"only {_n(k)} in your possession"


# -- a row -------------------------------------------------------------------------------------

def size_line(detail: str) -> str:
    """A kind's size as the card words it (`tree` `detail`, the boundary standard's words)."""
    return _now(detail)


def steelhead_sub(detail: str) -> Optional[str]:
    """A steelhead kind whose size is "rainbow trout over 50 cm (20 in)": the row's sub "a rainbow over 50 cm (20 in)"."""
    m = re.fullmatch(r"rainbow trout (over .+)", detail or "")
    return f"a rainbow {m[1]}" if m else None


def uses_the(n, over_cm) -> str:
    return f"uses the {_n(n)} over {common.cm_in(over_cm)}"


def only_line(k) -> str:
    return f"only {_n(k)}"


def count_line(k, others) -> str:
    """A count this fish shares with fish outside its band: "only 1, counting any lake trout you keep"."""
    return f"only {_n(k)}, counting any {fish_words(others)} you keep"


def size_count_line(n, over_cm, others) -> str:
    """A size cap this fish shares with fish of its box, where it is not the box's strip: "only 1 over 50 cm (20 in),
    counting any brook or lake trout you keep"."""
    return f"only {_n(n)} over {common.cm_in(over_cm)}" + (f", counting any {fish_words(others)} you keep" if others else "")


def also_line(says: str) -> str:
    """A cap the card says on the kind (`tree` `also_counts[].says`, gate-proved), as the row's line."""
    return _now(says)


def possession_row(line: str) -> str:
    """`card.possession_duty`'s line on the row: the home exception stays in the sheet."""
    home = _home()
    if not line.startswith("in your possession: ") or not line.endswith(home):
        raise WordsError(f"trout words: not a possession duty: {line!r}")
    return _now(line[:-len(home)])


def possession_sheet(line: str) -> str:
    """The same function's line in the sheet, whole, as a sentence."""
    return common.cap(line) + "."


def possession_quiet(daily, count, names: str, clause: Optional[str] = None) -> str:
    """Possession below the day's number (Cunningham): "3 a day, but never more than 2 lake trout with you"; with the
    possession's own size clause (T810 G2 Q10, owner "Box + one quiet line"): "…, and only 1 of them over 50 cm
    (20 in)" (the row says it once, here; the white box says "only 2 in your possession")."""
    s = f"{_n(daily)} a day, but never more than {_n(count)} {names} with you"
    if clause:
        m = re.fullmatch(rf"only {_NUM} (over .+)", clause)
        if not m:
            raise WordsError(f"trout words: not a possession size clause: {clause!r}")
        s += f", and only {m[1]} of them {m[2]}"
    return s


def possession_clause(line: str) -> Optional[str]:
    """A row's possession line (`possession_row`: the home exception off) -> its size clause ("only 1 over 50 cm
    (20 in)"), None without one."""
    m = re.fullmatch(r"in your possession: no more than [^,]+, (only \d+ over .+)", line)
    return m[1] if m else None


def size_release(a_cm, b_cm) -> str:
    """T810 G2 (owner 15:41, Tagish lake trout "Say what to release"): a keep range with a slot in it ("60 cm (24 in) or
    less, or 90 cm (35 in) or longer") said as what goes back, the book's exact bounds kept (60 and 90 cm may be kept):
    "release over 60 cm (24 in) and under 90 cm (35 in)"."""
    return f"release over {common.cm_in(a_cm)} and under {common.cm_in(b_cm)}"


_STOP = re.compile(r"stop fishing this (\w+) after (\d+) a day")


def stop_duty(duty: str, noun: Optional[str] = None) -> Optional[str]:
    """`card._duties`' stop duty ("stop fishing this river after 1 a day") as the table says it: "after keeping 1, stop
    fishing this creek today" — the noun is `noun` (T810 G2 Q7: the kind the water's own name says, else "water"), or
    the stored duty's where none is given; never "here". None for another duty."""
    m = _STOP.fullmatch(duty)
    return f"after keeping {m[2]}, stop fishing this {noun or m[1]} today" if m else None


def parse_duty_stop(duty: str) -> Optional[Tuple[str, int]]:
    """`card._duties`' stop duty ("stop fishing this river after 1 a day") -> (noun, n); None for another duty."""
    m = _STOP.fullmatch(duty)
    return (m[1], int(m[2])) if m else None


def week_line(says: str) -> str:
    """The rows post-pass's weekday words (`rows.say_weekdays`), whole: a NOW line (a date line never names a weekday;
    this is today's rule by weekday)."""
    return _now(says)


def steel_quiet(kind: str) -> str:
    return STEEL_QUIET[kind]


# -- together, elsewhere, the sheet ------------------------------------------------------------

def together_origin(n, origin: str, size: Optional[str]) -> str:
    """One origin's limit across two keep boxes (Lois, Khartoum): "only 2 wild trout and char in all, each under 40 cm
    (16 in)"."""
    return f"only {_n(n)} {origin} trout and char in all" + (f", each {size}" if size and size != "any size" else "")


def together_lakes(n) -> str:
    """Echoes Lakes (T805 D): "only 1 from both lakes together"."""
    return f"only {_n(n)} from both lakes together"


_ELSEWHERE_Q = re.compile(r"Kept [^?]+\?")
_TODAY_Q = " elsewhere today?"


def elsewhere_line(q: str, a: str) -> str:
    """One worked line of the card's kept-elsewhere list (`group.worked`, `rows/groups.py elsewhere_of`) as the table's
    `elsewhere` says it, under the app's "Fished elsewhere today? ›": "Kept 2 elsewhere? Keep 3 more here." The stored
    question's " elsewhere today?" loses its "today" (the link word says the day; no data text says "today", contract
    Q-D2); a question in any other form, or an answer that says "today", is refused."""
    if not _ELSEWHERE_Q.fullmatch(q):
        raise WordsError(f"trout words: not a kept-elsewhere question: {q!r}")
    if q.endswith(_TODAY_Q):
        q = q[:-len(_TODAY_Q)] + " elsewhere?"
    a = a if a.endswith((".", "?", "!")) else a + "."
    return _now(f"{q} {a}")


#: (T810 L) the kept-elsewhere SENTENCE of a row the table covers (`rows/groups.py elsewhere_of` `says`): the stored
#: sentence's "today" words, which the link word ("Fished elsewhere today?") says instead (contract Q-D2)
_SAYS_TODAY = ((" you kept anywhere else today count toward ", " you kept anywhere else count toward "),
               (" you fish today", " you fish"))


def elsewhere_says(says: str) -> str:
    """A covered row's kept-elsewhere sentence (`Row.elsewhere.says`: whose limit, its number, which fish kept elsewhere
    count toward it) as the table's `elsewhere` says it, first, before the worked lines: the same words without
    "today" (the link word says the day). A sentence this grammar cannot read back (`parse_elsewhere_says`) is
    refused, never shipped as free text."""
    out = says
    for a, b in _SAYS_TODAY:
        out = out.replace(a, b)
    parse_elsewhere_says(out)
    return _now(out)


_SAYS_OWN = re.compile(r"Your own limit, counted across every (.+?) you fish(, lakes and streams alike)?\.")
_SAYS_RULE = re.compile(
    r"(.+?)[’']s rule( for its tributaries)?: (?:no daily limit here\.|(\d+(?:\.\d+)?) a day here\. (.+?) you kept "
    r"anywhere else count toward (?:that 1|those (\d+(?:\.\d+)?))\.)"
    r"( These fish are counted apart from the region[’']s total: they don[’']t use it up\.)?"
    r"(?: They also count toward (.+?)\.)?")


def parse_elsewhere_says(text: str) -> tuple:
    """A kept-elsewhere sentence (`elsewhere_says`) -> ("own", scope) — "Your own limit, counted across every water in
    Region 6 you fish, lakes and streams alike." — or ("rule", whose, n | None, fish words, apart, also | None) — "This
    lake’s rule: 2 a day here. Trout and char other than steelhead you kept anywhere else count toward those 2."
    (n None: "no daily limit here"; `also` the total it counts toward too, in words). Anything else is refused."""
    if _TODAY.search(text):
        raise WordsError(f"trout words: a kept-elsewhere sentence says today: {text!r}")
    text = split_subject(text)[1]                       # (owner 23:29) its subject label, read by `split_subject`
    m = _SAYS_OWN.fullmatch(text)
    if m:
        return ("own", m[1])
    m = _SAYS_RULE.fullmatch(text)
    if m:
        n = None if m[3] is None else _num(m[3])
        if n is not None and (_num(m[5]) if m[5] else 1) != n:
            raise WordsError(f"trout words: a kept-elsewhere sentence counts toward another number: {text!r}")
        return ("rule", m[1], n, m[4], m[6] is not None, m[7])
    raise WordsError(f"trout words: not a kept-elsewhere sentence {text!r}")


def is_elsewhere_line(text: str) -> bool:
    """A worked kept-elsewhere line ("Kept 2 elsewhere? …"), as against the sentence that leads the list — its subject
    label, if any, set aside (`split_subject`)."""
    return split_subject(text)[1].startswith("Kept ")


#: (T810 L, owner 2026-11-17 23:29 "Yes, name the fish (Recommended)") the subject of a kept-elsewhere list's lines when
#: the list speaks of more than one: a row's or group's one fish by the table's own name ("Brook trout"), several fish
#: (the trout and char pool, a share) "Trout and char"
POOL_SUBJECT = "Trout and char"


def elsewhere_subject(codes) -> str:
    """The subject a kept-elsewhere line speaks of: the one fish of its row or group by the row's name (`fish_name`),
    else the pool, "Trout and char"."""
    got = list(dict.fromkeys(codes))
    if not got:
        raise WordsError("trout words: a kept-elsewhere line of no fish")
    return fish_name(got[0]) if len(got) == 1 else POOL_SUBJECT


def elsewhere_labeled(subject: str, text: str) -> str:
    """A kept-elsewhere sentence or worked line under its subject, when the list speaks of more than one: "Brook trout:
    Kept 2 elsewhere? Keep 8 more here." (owner 23:29: the grouping cannot be misread)."""
    if split_subject(f"{subject}: {text}") != (subject, text):
        raise WordsError(f"trout words: not a kept-elsewhere subject {subject!r}")
    return _now(f"{subject}: {text}")


def _subjects() -> Tuple[str, ...]:
    return (POOL_SUBJECT, *sorted({common.sp_name(S) for S in common.trout_char()}, key=len, reverse=True))


def split_subject(text: str) -> Tuple[Optional[str], str]:
    """A kept-elsewhere text -> (its subject label, the text after it); (None, text) where it has none. Only a subject
    `elsewhere_subject` can say is a label ("This lake’s rule: …" is a sentence, never a label)."""
    for sub in _subjects():
        if text.startswith(sub + ": "):
            return sub, text[len(sub) + 2:]
    return None, text


def sheet_keep(band_text: str, box_n) -> str:
    """The fish sheet's head on a keep row (spec 3.4): "Keep · only 1 of these 4 · counts toward the 5"; a row whose band
    is the box's own number ("counts toward the 5") says it once: "Keep · counts toward the 5"; one toward a smaller
    number of a raised box (Canuck: the 5 inside "Keep 5, brook trout up to 20") names both: "Keep · counts toward the 5
    and the 20"."""
    kind, k, _m = parse_band(band_text)
    if kind == "upto" and k == box_n:
        return f"Keep · counts toward the {_n(box_n)}"
    if kind == "upto":
        return f"Keep · counts toward the {_n(k)} and the {_n(box_n)}"
    return f"Keep · {band_text} · counts toward the {_n(box_n)}"


# -- THE FISH SHEET'S "HERE" LINES (ticket H, spec 3.4): whole sentences, never "today" ----------------------------------
# size "Size: 30 cm (12 in) or longer." / "Any size." · steelhead "Here a steelhead is a rainbow over 50 cm (20 in)." ·
# label "Hatchery only: wild ones go back." · box "Counts toward the 5 in all, together with brook trout and lake trout." ·
# own "This fish: only 3." · shared "Only 1 of these 4: this fish and brown trout, cutthroat trout and rainbow trout." ·
# uses "A kept one is always over 50 cm (20 in), so it is that one." · possession "In your possession: 1 day’s limit, not 2
# (+ the home exception, `POSSESSION_HOME`)." · stop duty (bold) "Once you have kept 1 steelhead here, stop fishing this stream for the
# rest of the day: the whole stream, not only this stretch." · licence "Licence: 10 a year · record each on your licence
# right away · Steelhead Conservation Surcharge Stamp, only if you fish for steelhead, even to release them." · any other
# row or strip line as a sentence: "Only 1, counting any lake trout you keep."

SHEET_HATCHERY_ONLY = "Hatchery only: wild ones go back."


def _sheet(text: str) -> str:
    """A sheet sentence: whole, never "today" (the sheet's "Here, today" heading is the app's), ending as a sentence."""
    if not text.endswith((".", "?", "!")):
        raise WordsError(f"trout words: a sheet line is a sentence: {text!r}")
    return _now(text)


def sentence(text: str) -> str:
    """A table phrase as a sheet sentence: "only 1, counting any lake trout you keep" -> "Only 1, counting any lake trout
    you keep."."""
    t = common.cap(text)
    return _sheet(t if t.endswith((".", "?", "!")) else t + ".")


def sheet_size(detail: str) -> str:
    return _sheet("Any size." if detail in ("any size", "any length") else f"Size: {detail}.")


def sheet_sub(sub: str) -> str:
    """The steelhead row's sub ("a rainbow over 50 cm (20 in)") as the sheet says it."""
    if not sub.startswith("a rainbow "):
        raise WordsError(f"trout words: not a steelhead sub: {sub!r}")
    return _sheet(f"Here a steelhead is {sub}.")


def sheet_box(n, others: str) -> str:
    """The box's number the row counts toward, with the box's other fish ("" alone)."""
    return _sheet(f"Counts toward the {_n(n)} in all" + (f", together with {others}." if others else "."))


def sheet_own(k) -> str:
    return _sheet(f"This fish: only {_n(k)}.")


def sheet_shared(band_text: str, others: str) -> str:
    """A shared band ("only 1 of these 4", "only 3 from these 2 together") with the fish it holds besides this one."""
    kind, _k, _m = parse_band(band_text)
    if kind != "shared":
        raise WordsError(f"trout words: not a shared band: {band_text!r}")
    return _sheet(f"{common.cap(band_text)}: this fish and {others}.")


def sheet_uses(over_cm) -> str:
    return _sheet(f"A kept one is always over {common.cm_in(over_cm)}, so it is that one.")


def sheet_stop(n, noun: str, fish: str) -> str:
    """The stop duty (bold), from `card._duties`' "stop fishing this river after 1 a day"."""
    return _sheet(f"Once you have kept {_n(n)} {fish} here, stop fishing this {noun} for the rest of the day: the whole "
                  f"{noun}, not only this stretch.")


def licence_stamp(name: str, when: str) -> str:
    """A stamp the fish needs (`stamps.stamp_out`'s name and the licence tab's sentence): "Steelhead Conservation
    Surcharge Stamp, only if you fish for steelhead, even to release them"."""
    if not when.startswith("Only if "):
        raise WordsError(f"trout words: not a licence tab's sentence: {when!r}")
    return _now(f"{name}, {when[:1].lower()}{when[1:]}")


def sheet_licence(parts) -> str:
    """The licence line: the yearly limit and record duties (`card._duties`' words) and the stamps, in that order."""
    if not parts:
        raise WordsError("trout words: a licence line says something")
    return _sheet("Licence: " + " · ".join(_now(p) for p in parts) + ".")


def parse_uses(text: str) -> Optional[Tuple[float, float]]:
    """"uses the 1 over 50 cm (20 in)" -> (1, 50); None for any other line."""
    m = re.fullmatch(rf"uses the {_NUM} over {_CM}", text)
    return (_num(m[1]), _cm(m[2], m[3])) if m else None


# -- the grammar of the NOW words (J reads them back) ------------------------------------------

_NUM = r"(\d+(?:\.\d+)?)"


def _num(s: str):
    x = float(s)
    return int(x) if x.is_integer() else x


def parse_band(text: str) -> Tuple[str, float, Optional[int]]:
    """A band text -> (kind, n, m): "counts toward the 5" ("upto", 5, None), "only 1 of these 4" ("shared", 1, 4)."""
    for kind, pat in (("upto", rf"counts toward the {_NUM}"), ("possession", rf"only {_NUM} in your possession"),
                      ("shared", rf"only {_NUM} of these (\d+)"), ("shared", rf"only {_NUM} (between) them"),
                      ("shared", rf"only {_NUM} (among) them"), ("shared", rf"only {_NUM} wild in all"),
                      ("own", rf"only {_NUM}")):
        m = re.fullmatch(pat, text)
        if m:
            if kind != "shared":
                return kind, _num(m[1]), None
            g = m[2] if m.lastindex and m.lastindex >= 2 else None
            # "between them": two rows; "among them" and "wild in all": the band's own span (J checks it holds 2+)
            return kind, _num(m[1]), (2 if g == "between" else None if g in ("among", None) else int(g))
    raise WordsError(f"trout words: cannot read the band {text!r}")


def parse_head(text: str) -> Tuple[str, Optional[float]]:
    """A box head -> (kind, n)."""
    if text == HEAD_RELEASE:
        return "release", None
    if text == HEAD_CLOSED:
        return "closed", None
    m = re.fullmatch(rf"Keep {_NUM} in all", text)
    if m:
        return "keep", _num(m[1])
    got = parse_head_except(text)
    if got is not None:
        return "keep", got[2]
    raise WordsError(f"trout words: cannot read the box head {text!r}")


def parse_head_except(text: str) -> Optional[Tuple[float, Tuple[str, ...], float]]:
    """"Keep 5, brook trout up to 20" -> (5, the excepted fish, 20); None for any other head."""
    m = re.fullmatch(rf"Keep {_NUM}, (.+) up to {_NUM}", text or "")
    if not m:
        return None
    return _num(m[1]), parse_fish(m[2]), _num(m[3])


def parse_strip_size(text: str) -> Tuple[float, float, Optional[float]]:
    """"only 1 over 50 cm (20 in)[, 2 if one is a hatchery steelhead]" -> (n, cm, n with the steelhead | None)."""
    m = re.fullmatch(rf"only {_NUM} over {_CM}(?:, {_NUM} if one is a hatchery steelhead)?", text)
    if not m:
        raise WordsError(f"trout words: cannot read the size sentence {text!r}")
    return _num(m[1]), _cm(m[2], m[3]), (_num(m[4]) if m[4] else None)


def parse_stop_duty(text: str) -> Tuple[int, str]:
    m = re.fullmatch(r"after keeping (\d+), stop fishing this (\w+) today", text)
    if not m:
        raise WordsError(f"trout words: cannot read the stop duty {text!r}")
    return int(m[1]), m[2]


def parse_top(lead: str) -> Tuple[str, Tuple]:
    """A top line -> (form, numbers): ("up_to", (2, 5, …)) with every number it states, ("release", ()), ("closed",
    ()), ("none", ()), ("apart", (6, 4))."""
    fixed = {TOP_RELEASE: "release", TOP_CLOSED: "closed", TOP_NONE: "none"}
    if lead in fixed:
        return fixed[lead], ()
    m = re.fullmatch(rf"Keep {_NUM} {TOP_FISH} \{{day\}}(.*)", lead)
    if m:
        return "up_to", (_num(m[1]), *(_num(x) for x in re.findall(rf"(?:or|than|plus|and) {_NUM}", m[2])))
    m = re.fullmatch(rf"Keep (.+) \{{day\}}", lead)
    if m and " other trout and char" in m[1] or (m and re.match(rf"{_NUM} ", m[1])):
        return "apart", tuple(_num(x) for x in re.findall(rf"(?:^|, | and ){_NUM} ", m[1]))
    raise WordsError(f"trout words: cannot read the top line {lead!r}")


def one_day(day: int, says) -> str:
    """Spec 3.3 rule 6 (Adams River, owner 15:36 "80 cm on the row"): the fish's sheet says the one looser day: "On Jul 31
    only: 60 cm (24 in) or longer."."""
    return f"On {day_words(day)} only: {', '.join(says)}."


# ============================================================================================
# THE GRAMMAR OF EVERY TABLE TEXT (ticket J, contract 2.10): the proof reads each text back with it
# ============================================================================================
# Every text the table, its date set and its sheets say has a reader here. A text none of them reads raises
# `WordsError`, and the proof (`trout/check.py`) fails the build on it: no text is "free". The texts the table reuses
# from the card (a kind's size words, `tree` kinds' `also_counts`, the kept-elsewhere lines, the possession duty, the
# weekday words) are read here too, in the forms their composers write (F's question 3: "word them through G or extend
# G, never let J skip them").
#
# Each reader returns a small tuple J turns into law. Sizes are ranges [(from cm, to cm)] with `INF` for no top; a
# range's ends (which side of a cut it holds) are not carried: J checks lengths inside the cuts.

INF = float("inf")


def _codes_by_name() -> Dict[str, Tuple[str, ...]]:
    out: Dict[str, Tuple[str, ...]] = {}
    for c in common.trout_char():
        out[common.lc(common.sp_name(c))] = (c,)
    for g, members in common._closed_groups():
        if members <= common.trout_char():
            out[common.lc(common.sp_name(g))] = tuple(sorted(members))
    return out


def parse_fish(text: str, loose: bool = False) -> Tuple[str, ...]:
    """A phrase naming trout and char fish (`tree.kind_names`, `common.lc_names` / `each_name`, a closed group by its
    name) -> their codes, in the phrase's order: "brown, cutthroat or rainbow trout" -> (GB, CT, RB). `loose`: the
    kept-elsewhere lines' "trout" (the trout fish, char left out). Any other phrase raises WordsError."""
    names = _codes_by_name()
    t = text
    if t != t.strip() or "  " in t:
        raise WordsError(f"trout words: cannot read the fish {text!r}")
    if t in names:
        return names[t]
    if loose and t == "trout":
        return ("CT", "GB", "RB", "ST")
    if loose and t == "trout or char":
        return names["trout and char"]
    toks = [x for x in re.split(r", | or | and ", t) if x]
    if not toks or ", , " in t:
        raise WordsError(f"trout words: cannot read the fish {text!r}")
    if toks[-1].endswith(" trout") and "/" not in toks[-1]:
        toks = [x if (" " in x or x == "steelhead") else f"{x} trout" for x in toks[:-1]] + [toks[-1]]
    out: List[str] = []
    for x in toks:
        got = names.get(x)
        if got is None or len(got) != 1:
            raise WordsError(f"trout words: cannot read the fish {text!r} (at {x!r})")
        out.append(got[0])
    if len(out) != len(set(out)):
        raise WordsError(f"trout words: a fish twice in {text!r}")
    return tuple(out)


def _range_one(s: str) -> Tuple[float, float]:
    pats = [
        (r"any size|any length", lambda m: (0, INF)),
        (rf"{_RANGE}", lambda m: (_cm(m[1], m[3]), _cm(m[2], m[4]))),
        (rf"between {_CM} and {_CM}", lambda m: (_cm(m[1], m[2]), _cm(m[3], m[4]))),
        (rf"over {_CM}, (?:up to|under) {_CM}", lambda m: (_cm(m[1], m[2]), _cm(m[3], m[4]))),
        (rf"{_CM} to (?:up to|under) {_CM}", lambda m: (_cm(m[1], m[2]), _cm(m[3], m[4]))),
        (rf"{_CM} or less|under {_CM}", lambda m: (0, _cm(m[1], m[2]) if m[1] else _cm(m[3], m[4]))),
        (rf"{_CM} or longer|over {_CM}", lambda m: (_cm(m[1], m[2]) if m[1] else _cm(m[3], m[4]), INF)),
    ]
    for pat, make in pats:
        m = re.fullmatch(pat, s)
        if m:
            return make(m)
    raise WordsError(f"trout words: cannot read the size {s!r}")


def parse_size(text: str) -> Tuple[Tuple[float, float], ...]:
    """A size as the card and the table word it (`card._detail_words`, `common.band_words`, `law_size`) -> its ranges:
    "30 cm (12 in) or longer" -> ((30, inf),); "60 cm (24 in) or less, or 90 cm (35 in) or longer" -> two;
    "any length, 50 cm (20 in) or less" (a lifted floor) -> ((0, 50),); "catch and release" -> () (keeps none)."""
    if text == "catch and release":
        return ()
    m = re.fullmatch(rf"release over {_CM} and under {_CM}", text)
    if m:
        return ((0, _cm(m[1], m[2])), (_cm(m[3], m[4]), INF))
    m = re.fullmatch(rf"any length, ({_CM} or less)", text)
    if m:
        return (_range_one(m[1]),)
    m = re.fullmatch(rf"({_CM} or longer), any length", text)
    if m:
        return (_range_one(m[1]),)
    return tuple(_range_one(p) for p in text.split(", or "))


def parse_possession(text: str) -> tuple:
    """A possession line on a row or a strip (`possession_row`, `card.possession_duty` less its home exception) ->
    ("count", n, fish, clauses [(k, ranges)]) — "in your possession: no more than 2 lake trout, only 1 over 50 cm
    (20 in)" — or ("days", k, the page's k | None) — "in your possession: 1 day’s limit, not 2"."""
    m = re.fullmatch(r"in your possession: (.+)", text)
    if not m:
        raise WordsError(f"trout words: not a possession line {text!r}")
    s = m[1]
    m = re.fullmatch(rf"(?:1 day’s limit|{_NUM} days’ limits)(?:, not {_NUM})?", s)
    if m:
        return ("days", _num(m[1]) if m[1] else 1, _num(m[2]) if m[2] else None)
    m = re.fullmatch(rf"no more than {_NUM} (.+?)( in all)?((?:, only {_NUM} (?:measuring )?[^,]+(?:, or [^,]+)*)*)", s)
    if not m:
        raise WordsError(f"trout words: cannot read the possession line {text!r}")
    fish = parse_fish(m[2])
    clauses = []
    for c in re.findall(rf", only {_NUM} ((?:measuring )?.+?)(?=, only |$)", m[4] or ""):
        k, w = c
        clauses.append((_num(k), parse_size(w[len("measuring "):] if w.startswith("measuring ") else w)))
    if bool(m[3]) != (len(fish) > 1):
        raise WordsError(f"trout words: 'in all' is said over several fish only: {text!r}")
    return ("count", _num(m[1]), fish, tuple(clauses))


def parse_also(text: str) -> tuple:
    """A cap the card says on a kind (`tree._size_also`, `tree._count_also`, `tree.across_says`), the table's row line:
      ("size", n, cm, mine | None, others, then [(n, cm)])   "up to 1 over 50 cm (20 in)[, and up to 2 over 60 cm …]
                                                              [, if you haven’t already kept a X over 50 cm (20 in) |
                                                              , counting any X over 50 cm (20 in)]", "up to 1
                                                              cutthroat trout over 50 cm (20 in)", "none over …";
                                                              "none over C if you’ve already kept a X over C" (n 1:
                                                              this fish and X together hold 1); "no more than 2 over C,
                                                              counting any X over C"
      ("count", n, mine | None, others)                       "up to 2 may be X[, counting any Y you keep]", "no more
                                                              than 3 counting any X you keep"
      ("across", n, origin, others)                           "no more than 2 wild trout and char in all, counting any
                                                              wild rainbow trout you keep\""""
    m = re.fullmatch(rf"none over {_CM} if you’ve already kept a (.+) over {_CM}", text)
    if m:
        if (m[1], m[2]) != (m[4], m[5]):
            raise WordsError(f"trout words: two sizes in {text!r}")
        return ("size", 1, _cm(m[1], m[2]), None, parse_fish(m[3]), ())
    m = re.fullmatch(rf"no more than {_NUM} over {_CM}, counting any (.+) over {_CM}", text)
    if m:
        return ("size", _num(m[1]), _cm(m[2], m[3]), None, parse_fish(m[4]), ())
    m = re.fullmatch(rf"no more than {_NUM} (wild|hatchery) (.+) in all, counting any (?:wild |hatchery )?(.+) you keep",
                     text)
    if m:
        return ("across", _num(m[1]), m[2], parse_fish(m[4]))
    m = re.fullmatch(rf"no more than {_NUM} counting any (.+) you keep", text)
    if m:
        return ("count", _num(m[1]), None, parse_fish(m[2]))
    m = re.fullmatch(rf"up to {_NUM} may be (.+?)(?:, counting any (.+) you keep)?", text)
    if m:
        return ("count", _num(m[1]), parse_fish(m[2]), parse_fish(m[3]) if m[3] else ())
    m = re.fullmatch(rf"(none|up to {_NUM}) (?:([A-Za-z][A-Za-z/ ,]*?) )?over {_CM}((?:, and (?:none|up to {_NUM}) over {_CM})*)"
                     rf"(?:, if you haven’t already kept a (.+) over {_CM}|, counting any (.+) over {_CM})?", text)
    if m:
        n = 0 if m[1] == "none" else _num(m[2])
        cm = _cm(m[4], m[5])
        then = tuple((0 if a == "none" else _num(b), _cm(c, d))
                     for a, b, c, d in re.findall(rf", and (none|up to {_NUM}) over {_CM}", m[6] or ""))
        others: Tuple[str, ...] = ()
        if m[10]:
            if _cm(m[11], m[12]) != cm or n != 1:
                raise WordsError(f"trout words: cannot read {text!r}")
            others = parse_fish(m[10])
        elif m[13]:
            if _cm(m[14], m[15]) != cm:
                raise WordsError(f"trout words: cannot read {text!r}")
            others = parse_fish(m[13])
        return ("size", n, cm, parse_fish(m[3]) if m[3] else None, others, then)
    raise WordsError(f"trout words: cannot read the kind's cap {text!r}")


def parse_week(text: str) -> Tuple[Tuple[Optional[Tuple[str, ...]], Tuple[Tuple[str, Optional[float], frozenset], ...]], ...]:
    """The rows post-pass's weekday words (`rows.say_weekdays`): "Keep up to 5 on Saturdays and Sundays; release Monday
    to Friday" -> ((fish | None, ((status, n, weekdays), …)), …); several fish of a row each named ("Rainbow trout:
    keep up to 5 …")."""
    from pipeline.deliver.answers.rows.weekdays import weekdays_words
    inv = {}
    for mask in range(1, K.ALL_WEEK + 1):                 # every set of weekdays, said by `when_words` (its one home)
        names = K.weekday_names(mask)
        inv[weekdays_words(names)] = frozenset(names)
    verbs = [("keep any number", "no_limit"), ("release", "release"), ("no fishing for them", "closed")]
    out = []
    for chunk in re.split(r"; (?=[A-Z])", text):
        m = re.fullmatch(r"([A-Z][^:]*): (.+)", chunk)
        fish, body = (parse_fish(common.lc(m[1]) if not m[1].startswith("Dolly") else m[1]), m[2]) if m \
            else (None, chunk)
        parts = []
        for p in re.split(r"; ", body[:1].lower() + body[1:]):
            mm = re.fullmatch(rf"keep up to {_NUM} (.+)", p)
            if mm and mm[2] in inv:
                parts.append(("keep", _num(mm[1]), inv[mm[2]]))
                continue
            for w, st in verbs:
                if p.startswith(w + " ") and p[len(w) + 1:] in inv:
                    parts.append((st, None, inv[p[len(w) + 1:]]))
                    break
            else:
                raise WordsError(f"trout words: cannot read the weekday words {text!r}")
        out.append((fish, tuple(parts)))
    return tuple(out)


def parse_row_line(text: str) -> tuple:
    """One of a keep row's `lines` -> what it says:
      ("size", ranges) · ("uses", n, cm) · ("only", k) · ("count", k, others) "only 1, counting any lake trout you
      keep" · ("size_count", n, cm, others) "only 1 over 50 cm (20 in)[, counting any … you keep]" · ("wild", n, ranges
      | None) "only 2 wild[, each …]" · ("possession", …) · ("stop", n, noun) · ("week", …) · ("also", …) a kind's cap."""
    for kind, f in (("possession", parse_possession), ("stop", parse_stop_duty)):
        try:
            got = f(text)
        except WordsError:
            continue
        return (kind, *got) if kind == "stop" else ("possession", got)
    if text == CLOSED_ALL_YEAR:
        return ("closed_year",)
    u = parse_uses(text)
    if u is not None:
        return ("uses", *u)
    m = re.fullmatch(rf"only {_NUM}", text)
    if m:
        return ("only", _num(m[1]))
    m = re.fullmatch(rf"only {_NUM}, counting any (.+) you keep", text)
    if m:
        return ("count", _num(m[1]), parse_fish(m[2]))
    m = re.fullmatch(rf"only {_NUM} over {_CM}(?:, counting any (.+) you keep)?", text)
    if m:
        return ("size_count", _num(m[1]), _cm(m[2], m[3]), parse_fish(m[4]) if m[4] else ())
    m = re.fullmatch(rf"only {_NUM} wild(?:, each (.+))?", text)
    if m:
        return ("wild", _num(m[1]), parse_size(m[2]) if m[2] else None)
    try:
        return ("size", parse_size(text))
    except WordsError:
        pass
    try:
        return ("also", parse_also(text))
    except WordsError:
        pass
    try:
        return ("week", parse_week(text))
    except WordsError:
        pass
    raise WordsError(f"trout words: cannot read the row line {text!r}")


def parse_strip(text: str) -> Tuple[tuple, ...]:
    """A box's strip (its parts joined by "; ") -> each part: ("size", n, cm, n with a hatchery steelhead | None),
    ("size_except", n, cm, the fish it leaves out), ("any",) "any size", ("wild", n, ranges | None), ("possession", …)."""
    out = []
    for p in text.split("; "):
        if p == STRIP_ANY_SIZE:
            out.append(("any",))
            continue
        m = re.fullmatch(rf"only {_NUM} over {_CM}, not counting (.+)", p)
        if m:
            out.append(("size_except", _num(m[1]), _cm(m[2], m[3]), parse_fish(m[4])))
            continue
        if p.startswith("in your possession: "):
            out.append(("possession", parse_possession(p)))
            continue
        m = re.fullmatch(rf"only {_NUM} wild(?:, each (.+))?", p)
        if m:
            out.append(("wild", _num(m[1]), parse_size(m[2]) if m[2] else None))
            continue
        out.append(("size", *parse_strip_size(p)))
    return tuple(out)


def parse_together(text: str) -> tuple:
    """`together` / `top.second`: ("origin", n, origin, ranges | None) or ("lakes", n)."""
    m = re.fullmatch(rf"only {_NUM} (wild|hatchery) trout and char in all(?:, each (.+))?", text)
    if m:
        return ("origin", _num(m[1]), m[2], parse_size(m[3]) if m[3] else None)
    m = re.fullmatch(rf"only {_NUM} from both lakes together", text)
    if m:
        return ("lakes", _num(m[1]))
    raise WordsError(f"trout words: cannot read the limit across boxes {text!r}")


def parse_quiet(text: str) -> tuple:
    """A row's quiet line: ("steel", kind) or ("possession", the day's number, the possession count, fish, clauses
    [(k, ranges)]: "…, and only 1 of them over 50 cm (20 in)")."""
    for k, v in STEEL_QUIET.items():
        if text == v:
            return ("steel", k)
    m = re.fullmatch(rf"{_NUM} a day, but never more than {_NUM} (.+?) with you(?:, and only {_NUM} of them over {_CM})?",
                     text)
    if m:
        clause = ((_num(m[4]), ((_cm(m[5], m[6]), INF),)),) if m[4] else ()
        return ("possession", _num(m[1]), _num(m[2]), parse_fish(m[3]), clause)
    raise WordsError(f"trout words: cannot read the quiet line {text!r}")


def parse_fold(text: str) -> Tuple[str, Tuple]:
    """The folded entry's value -> (form, numbers): ("up_to", (2, 5)), ("apart", (6, 4)), ("release" | "closed" |
    "none", ())."""
    fixed = {FOLD_RELEASE: "release", FOLD_CLOSED: "closed", FOLD_NONE: "none"}
    if text in fixed:
        return fixed[text], ()
    m = re.fullmatch(rf"{_NUM} a day", text)
    if m:
        return "up_to", (_num(m[1]),)
    m = re.fullmatch(rf"Up to {_NUM}((?: or {_NUM})+)", text)
    if m:
        return "up_to", (_num(m[1]), *(_num(x) for x in re.findall(_NUM, m[2])))
    m = re.fullmatch(r"Up to (.+)", text)
    if m:
        parts = m[1].split(", ")
        nums = []
        for i, p in enumerate(parts):
            mm = re.fullmatch(rf"{_NUM} (.+)", p)
            if not mm or (mm[2] == "others" and i != len(parts) - 1):
                raise WordsError(f"trout words: cannot read the fold {text!r}")
            nums.append(_num(mm[1]))
        if len(parts) > 1:
            return "apart", tuple(nums)
    raise WordsError(f"trout words: cannot read the fold {text!r}")


def parse_top_full(lead: str) -> tuple:
    """A top line, whole: ("up_to", n, alsos [(n, fish, ranges | None, origin | None)], more [(n, fish)], plus [(n,
    fish)]) — `top_up_to`'s parts — or ("apart", [(n, fish)], others n | None), or ("release" | "closed" | "none",)."""
    fixed = {TOP_RELEASE: "release", TOP_CLOSED: "closed", TOP_NONE: "none"}
    if lead in fixed:
        return (fixed[lead],)
    m = re.fullmatch(rf"Keep {_NUM} {TOP_FISH} \{{day\}}(.*)", lead)
    if m:
        n, rest = _num(m[1]), m[2]
        alsos, more, plus = [], [], []
        mm = re.fullmatch(r", plus (.+)", rest)
        if mm:
            for p in re.split(r", | and (?=\d)", mm[1]):
                q = re.fullmatch(rf"{_NUM} (.+)", p)
                if not q:
                    raise WordsError(f"trout words: cannot read the top line {lead!r}")
                plus.append((_num(q[1]), parse_fish(q[2])))
            return ("up_to", n, (), (), tuple(plus))
        mm = re.fullmatch(r", or (.+?)((?: and no more than \d+(?:\.\d+)? [^,]+?)*)", rest) if rest else None
        if rest and not mm:
            raise WordsError(f"trout words: cannot read the top line {lead!r}")
        if mm:
            for a in re.split(r" or (?=\d)", mm[1]):
                q = re.fullmatch(rf"{_NUM} if they’re (?:(wild|hatchery) )?(.+?)(?: ({_CM} or less|{_CM} or longer|"
                                 rf"under {_CM}|over {_CM}))?", a)
                if not q:
                    raise WordsError(f"trout words: cannot read the top line {lead!r}")
                alsos.append((_num(q[1]), parse_fish(q[3]), parse_size(q[4]) if q[4] else None, q[2]))
            for k, w in re.findall(rf" and no more than {_NUM} ([^,]+?)(?= and no more than|$)", mm[2] or ""):
                more.append((_num(k), parse_fish(w)))
        return ("up_to", n, tuple(alsos), tuple(more), ())
    m = re.fullmatch(r"Keep (.+) \{day\}", lead)
    if m:
        body = m[1]
        others = None
        mm = re.fullmatch(rf"(.+) and {_NUM} other trout and char", body)
        if mm:
            body, others = mm[1], _num(mm[2])
        named = []
        for p in re.split(r", (?=\d)| and (?=\d)", body):
            q = re.fullmatch(rf"{_NUM} (.+)", p)
            if not q:
                raise WordsError(f"trout words: cannot read the top line {lead!r}")
            named.append((_num(q[1]), parse_fish(q[2])))
        return ("apart", tuple(named), others)
    raise WordsError(f"trout words: cannot read the top line {lead!r}")


_SAY_ANSWER = (
    (rf"Keep {_NUM} more here\.", lambda m: ("more", _num(m[1]))),
    (r"None more here\.", lambda m: ("more", 0)),
    (rf"Keep {_NUM} more here, but no more than {_NUM} (.+) from (stream|lake)s in all(?: \((.+) may go past it here\))?\.",
     lambda m: ("more_but", _num(m[1]), _num(m[2]), m[3], m[4], m[5])),
    (rf"Keep {_NUM} more here\. Fish from (stream|lake)s count toward the {_NUM} a day, not toward the {_NUM} from "
     rf"(stream|lake)s\.", lambda m: ("more_tally", _num(m[1]), m[2], _num(m[3]), _num(m[4]), m[5])),
    (rf"It counts here too: at most {_NUM} over {_CM} a day in all(?: — only {_NUM} of them may be anything other than "
     rf"a (hatchery )?steelhead)?\.", lambda m: ("over", _num(m[1]), _cm(m[2], m[3]), _num(m[4]) if m[4] else None,
                                                 bool(m[5]))),
)


def parse_elsewhere(text: str) -> tuple:
    """A kept-elsewhere line (`elsewhere_line`) -> (question, answer): the question ("kept", n, place) — "Kept 2
    elsewhere?", "Kept 2 on another stream?", "Kept 1 at a lake?" — or ("over", fish, cm) — "Kept trout or char over
    50 cm (20 in) elsewhere?"; the answer one of `_SAY_ANSWER`'s forms. A subject label before it (owner 23:29,
    `elsewhere_labeled`) is set aside."""
    text = split_subject(text)[1]
    m = re.fullmatch(r"(Kept [^?]+\?) (.+)", text)
    if not m:
        raise WordsError(f"trout words: not a kept-elsewhere line {text!r}")
    q, a = m[1], m[2]
    mm = re.fullmatch(rf"Kept {_NUM} (elsewhere|on another (stream|lake)|at a (stream|lake))\?", q)
    if mm:
        place = mm[3] or mm[4] or "elsewhere"
        question = ("kept", _num(mm[1]), place)
    else:
        mm = re.fullmatch(rf"Kept (.+) over {_CM} elsewhere\?", q)
        if not mm:
            raise WordsError(f"trout words: cannot read the kept-elsewhere question {q!r}")
        fish = parse_fish(common.lc(mm[1]) if not mm[1].startswith("Dolly") else mm[1], loose=True)
        question = ("over", fish, _cm(mm[2], mm[3]))
    for pat, make in _SAY_ANSWER:
        mm = re.fullmatch(pat, a)
        if mm:
            return question, make(mm)
    raise WordsError(f"trout words: cannot read the kept-elsewhere answer {a!r}")


def parse_sheet_head(text: str) -> tuple:
    """A sheet's head -> ("keep", band text | None, box n) | ("release",) | ("closed",)."""
    if text == SHEET_RELEASE:
        return ("release",)
    if text == SHEET_CLOSED:
        return ("closed",)
    m = re.fullmatch(rf"Keep · counts toward the {_NUM} and the {_NUM}", text)
    if m:
        return ("keep", band_upto(_num(m[1])), _num(m[2]))
    m = re.fullmatch(rf"Keep · (?:(.+) · )?counts toward the {_NUM}", text)
    if m:
        if m[1] is not None:
            parse_band(m[1])
        return ("keep", m[1], _num(m[2]))
    raise WordsError(f"trout words: cannot read the sheet head {text!r}")


def _decap(t: str) -> str:
    return t[:1].lower() + t[1:]


def parse_licence_part(text: str) -> tuple:
    """A part of the sheet's licence line: ("annual", n, band words | None), ("record", band words | None),
    ("stamp", name, when) — the name the licence table gives its document (J holds it to the post-pass licence answer)."""
    if text != text.strip() or "  " in text:
        raise WordsError(f"trout words: cannot read the licence part {text!r}")
    m = re.fullmatch(rf"{_NUM} (?:(.+) )?a year", text)
    if m:
        return ("annual", _num(m[1]), m[2])
    m = re.fullmatch(r"record each(?: one (.+))? on your licence right away", text)
    if m:
        return ("record", m[1])
    m = re.fullmatch(r"(.+?), (only if .+)", text)
    if m:
        return ("stamp", m[1], m[2])
    raise WordsError(f"trout words: cannot read the licence part {text!r}")


def parse_sheet_line(text: str) -> tuple:
    """One "Here" line of a fish sheet -> what it says:
      ("size", ranges) "Size: …." / "Any size." · ("sub", cm) "Here a steelhead is a rainbow over 50 cm (20 in)." ·
      ("hatchery",) · ("box", n, others | ()) · ("own", k) · ("shared", band text, others) · ("uses", cm) ·
      ("possession", …) the duty whole, with its home exception · ("stop", n, fish, noun) · ("licence", parts) ·
      ("one_day", day, the day's lines read as row lines) · ("line", what) any other row, strip, together or quiet
      line as a sentence."""
    _sheet(text)
    if text in ("Any size.",):
        return ("size", ((0, INF),))
    m = re.fullmatch(r"Size: (.+)\.", text)
    if m:
        return ("size", parse_size(m[1]))
    m = re.fullmatch(rf"Here a steelhead is a rainbow over {_CM}\.", text)
    if m:
        return ("sub", _cm(m[1], m[2]))
    if text == SHEET_HATCHERY_ONLY:
        return ("hatchery",)
    m = re.fullmatch(rf"Counts toward the {_NUM} in all(?:, together with (.+))?\.", text)
    if m:
        return ("box", _num(m[1]), _parse_named(m[2]) if m[2] else ())
    m = re.fullmatch(rf"This fish: only {_NUM}\.", text)
    if m:
        return ("own", _num(m[1]))
    m = re.fullmatch(r"(Only .+?): this fish and (.+)\.", text)
    if m:
        band = _decap(m[1])
        if parse_band(band)[0] != "shared":
            raise WordsError(f"trout words: cannot read the sheet line {text!r}")
        return ("shared", band, _parse_named(m[2]))
    m = re.fullmatch(rf"A kept one is always over {_CM}, so it is that one\.", text)
    if m:
        return ("uses", _cm(m[1], m[2]))
    m = re.fullmatch(r"In your possession: (.+) \((.+)\)\.", text)
    if m:
        if f" ({m[2]})" != _home():
            raise WordsError(f"trout words: a possession sentence without its home exception {text!r}")
        return ("possession", parse_possession(f"in your possession: {m[1]}"))
    m = re.fullmatch(r"Once you have kept (\d+) (.+) here, stop fishing this (\w+) for the rest of the day: the whole "
                     r"(\w+), not only this stretch\.", text)
    if m:
        if m[3] != m[4]:
            raise WordsError(f"trout words: the stop duty names two waters {text!r}")
        return ("stop", int(m[1]), parse_fish(m[2]), m[3])
    m = re.fullmatch(r"Licence: (.+)\.", text)
    if m:
        return ("licence", tuple(parse_licence_part(p) for p in m[1].split(" · ")))
    m = re.fullmatch(rf"On {_DAY} only: (.+)\.", text)
    if m:
        return ("one_day", _day_of(m[1], m[2]), tuple(parse_row_line(x) for x in m[3].split(", ")))
    inner = text[:-1]
    for kind, f, t in (("quiet", parse_quiet, text), ("quiet", parse_quiet, inner), ("row", parse_row_line, _decap(inner)),
                       ("strip", parse_strip, _decap(inner)), ("together", parse_together, _decap(inner))):
        try:
            return ("line", kind, t, f(t))
        except WordsError:
            continue
    raise WordsError(f"trout words: cannot read the sheet line {text!r}")


def _parse_named(text: str) -> Tuple[Tuple[str, Optional[str]], ...]:
    """The other fish a sheet line names ("brook trout, wild rainbow trout and lake trout") -> ((code, origin |
    None), …)."""
    out = []
    for x in re.split(r", | and ", text):
        o = None
        for org in common.ORIGINS:
            if x.startswith(org + " "):
                o, x = org, x[len(org) + 1:]
        out += [(c, o) for c in parse_fish(x)]
    return tuple(out)
