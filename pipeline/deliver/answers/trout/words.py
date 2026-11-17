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


# ============================================================================================
# THE TABLE'S NOW WORDS (ticket F, contract 2.3 and 2.4): every text the table body says today, whole, with its parser
# ============================================================================================
# THE TOP LINE carries ONE `{day}` slot (contract Q-D2): "Up to 5 {day}", "Up to 2 {day}, or 5 if they’re rainbow
#   trout", "Up to 2 {day}, plus 2 Dolly Varden/bull trout", "Up to 6 rainbow trout or steelhead and 4 other trout and
#   char {day}", "Release every one {day}", "Closed for trout and char {day}", "Nothing to keep {day}".
# THE FOLD (the "Trout and char" entry's short line): "Up to 5", "Up to 2 or 5", "Up to 6 rainbow, 4 others",
#   "Release", "Closed", "None to keep".
# THE BOX: head "Keep 5 in all" / "Release" / "Closed"; label "Hatchery only"; strip parts joined by "; " ("only 1
#   over 50 cm (20 in)", "only 1 over 50 cm (20 in), 2 if one is a hatchery steelhead", "only 2 wild, each 40 cm
#   (16 in) or less", "In your possession: no more than 1 trout and char in all"); tag "Lake’s rule".
# THE BANDS: "up to the 5" (upto), "only 3" (own), "only 1 of these 4" (shared), "only 2 in your possession".
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
TOP_RELEASE = f"Release every one {DAY_SLOT}"
TOP_CLOSED = f"Closed for trout and char {DAY_SLOT}"
TOP_NONE = f"Nothing to keep {DAY_SLOT}"
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
    """One keep box: "Up to 2 {day}", ", or 5 if they’re rainbow trout" for each stored `also` (the headline's), " and
    no more than 2 Dolly Varden/bull trout" for a lifted fish under its own number beside an `also`, ", plus 2 Dolly
    Varden/bull trout" for one without (`more` / `plus`: [(n, fish words)])."""
    s = f"Up to {_n(n)} {DAY_SLOT}"
    if alsos:
        s += ", or " + " or ".join(_now(a) for a in alsos)
    for k, w in more:
        s += f" and no more than {_n(k)} {w}"
    if plus:
        s += ", plus " + common.join([f"{_n(k)} {w}" for k, w in plus])
    return s


def top_apart(named, others_n: Optional[float]) -> str:
    """Boxes counted apart: "Up to 6 rainbow trout or steelhead and 4 other trout and char {day}" (`named`: [(n, fish
    words)] for each box named by its fish; `others_n` the box of the other trout and char, None when every box is
    named)."""
    parts = [f"{_n(k)} {w}" for k, w in named]
    if others_n is not None:
        parts.append(f"{_n(others_n)} other trout and char")
    return f"Up to {common.join(parts)} {DAY_SLOT}"


def fold_up_to(n, alsos=()) -> str:
    return "Up to " + " or ".join(_n(k) for k in [n, *alsos])


def fold_apart(named, others_n: Optional[float]) -> str:
    parts = [f"{_n(k)} {w}" for k, w in named]
    if others_n is not None:
        parts.append(f"{_n(others_n)} others")
    return "Up to " + ", ".join(parts)


# -- the box -----------------------------------------------------------------------------------

def head_keep(n) -> str:
    return f"Keep {_n(n)} in all"


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


def strip_wild(n, size: Optional[str]) -> str:
    """A wild quota of the box's own: "only 2 wild, each 40 cm (16 in) or less"."""
    return f"only {_n(n)} wild" + (f", each {size}" if size and size != "any size" else "")


def strip_possession(line: str) -> str:
    """Every row of the box shares one possession line: it stands once in the strip, "in your possession: …" (a strip
    part, as "only 1 over 50 cm (20 in)" is)."""
    return possession_row(line)


def strip(parts) -> Optional[str]:
    return "; ".join(parts) if parts else None


# -- the bands ---------------------------------------------------------------------------------

def band_upto(n) -> str:
    return f"up to the {_n(n)}"


def band_own(k) -> str:
    return f"only {_n(k)}"


def band_shared(k, m: int) -> str:
    """"only 1 of these 4"; a number at least the rows it spans (each row holds more than one fish: Tagish's lake trout
    and Dolly Varden "3 in the aggregate") reads "only 3 from these 2 together"."""
    return f"only {_n(k)} of these {m}" if k < m else f"only {_n(k)} from these {m} together"


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


def possession_quiet(daily, count, names: str) -> str:
    """Possession below the day's number (Cunningham): "3 a day, but never more than 2 lake trout with you"."""
    return f"{_n(daily)} a day, but never more than {_n(count)} {names} with you"


_STOP = re.compile(r"stop fishing this (\w+) after (\d+) a day")


def stop_duty(duty: str) -> Optional[str]:
    """`card._duties`' stop duty ("stop fishing this river after 1 a day") as the table says it: "after keeping 1, stop
    fishing this river today" (the noun is the stored duty's, never "here"); None for another duty."""
    m = _STOP.fullmatch(duty)
    return f"after keeping {m[2]}, stop fishing this {m[1]} today" if m else None


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


def elsewhere_line(q: str, a: str) -> str:
    return _now(f"{q} {a}")


def sheet_keep(band_text: str, box_n) -> str:
    return f"Keep · {band_text} · counts toward the {_n(box_n)}"


# -- the grammar of the NOW words (J reads them back) ------------------------------------------

_NUM = r"(\d+(?:\.\d+)?)"


def _num(s: str):
    x = float(s)
    return int(x) if x.is_integer() else x


def parse_band(text: str) -> Tuple[str, float, Optional[int]]:
    """A band text -> (kind, n, m): "up to the 5" ("upto", 5, None), "only 1 of these 4" ("shared", 1, 4)."""
    for kind, pat in (("upto", rf"up to the {_NUM}"), ("possession", rf"only {_NUM} in your possession"),
                      ("shared", rf"only {_NUM} of these (\d+)"), ("shared", rf"only {_NUM} from these (\d+) together"),
                      ("own", rf"only {_NUM}")):
        m = re.fullmatch(pat, text)
        if m:
            return kind, _num(m[1]), (int(m[2]) if kind == "shared" else None)
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
    raise WordsError(f"trout words: cannot read the box head {text!r}")


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
    m = re.fullmatch(rf"Up to {_NUM} \{{day\}}(.*)", lead)
    if m:
        return "up_to", (_num(m[1]), *(_num(x) for x in re.findall(rf"(?:or|than|plus|and) {_NUM}", m[2])))
    m = re.fullmatch(rf"Up to (.+) \{{day\}}", lead)
    if m and " other trout and char" in m[1] or (m and re.match(rf"{_NUM} ", m[1])):
        return "apart", tuple(_num(x) for x in re.findall(rf"(?:^|, | and ){_NUM} ", m[1]))
    raise WordsError(f"trout words: cannot read the top line {lead!r}")


def one_day(day: int, says) -> str:
    """Spec 3.3 rule 6 (Adams River, owner 15:36 "80 cm on the row"): the fish's sheet says the one looser day: "On Jul 31
    only: 60 cm (24 in) or longer."."""
    return f"On {day_words(day)} only: {', '.join(says)}."
