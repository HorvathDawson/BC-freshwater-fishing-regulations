"""THE PER-FISH TIMELINE AND THE DATE RULE (trout table plan, ticket E; spec 3.3, contract 2.5). Not wired into a
release: ticket I's `compose_key` will call it once per answer key, over all the key's segments.

For every (answer key, segment, fish, row) this module gives the row's ANSWER and its NEXT DATED CHANGE. It reads the
engine's answers (the rows frames' `fish`: per fish and origin the decided state, the day's number and the size lines;
the display frame's whole-water closure; the closing rule's fish), never a word. Words are `words.py`'s.

THE ANSWER of a row (a row is a fish, or a fish and origin where wild fish have a quota of their own: "RB",
"RB/wild"): its state (keep / release / closed, the fish's leading origin's, `common.lead_name`, the one picker), the
count it allows at every length for each origin (the day's number, lowered by a size line's take, 0 in a release band),
and its possession (a number of fish, where the fish's own possession differs from the page's line; None = the page's).
A row the frame does not answer is ABSENT: it has no row, so no date.

THE WALK (spec 3.3 rule 1). From the segment's start, step over the key's START DAYS, wrapping into next year, and take
the first start whose answer differs. No new day cuts: segments already cut wherever a section's value changes.
  * MOMENTS. One start repeats once per moment group (answers 2.1). A segment compares against the moments of a later
    start that share a weekday with it and sit on the same side of an hours window (the table is drawn from outside
    the window, `inside=false`; a segment inside it is compared inside). A later start whose moments give the
    segment's weekdays different answers reads "rule changes" when every one is looser than today's or the same
    (some weekdays keep today's answer); a tighter weekday-split change has no words and stops the build (TimelineError), never a guess.
  * FEB 29. Day 60 is never a reading day: a Feb 29 reads Feb 28's regulations (P06, `calendar_spec.reading_day`).
    A start on day 60 takes effect on Mar 1 (day 61); a segment holding only day 60 is never read and never dated.
    So no date ever names Feb 29: a change on Mar 1 is "until Feb 28" / "Mar 1".
  * THE LAST DAY. "until <day>" names the last reading day of today's answer: the day before the change (Dec 31
    before a Jan 1 change; Feb 28 before a Mar 1 change).

THE CHANGE (spec 3.3 rules 2 to 4; owner 15:36):
  looser   keep -> keep where every length and origin legal today stays legal at the same or a higher count and the
           possession does not shrink: "rule changes <day>", no details
  tighter  keep -> keep tighter in any part (a change tighter in one part and looser in another counts as tighter),
           keep -> release, keep -> closed, release -> closed: "until <last day>, then <words>"; the words name only
           what tightens (`Then` parts); a coming closure NEVER carries a reopening day
  opens    release -> keep (a change of state): "until <last day>" (the release ends; keeping follows)
  reopens  closed today -> release or keep: "reopens <day>". Only something closed TODAY reopens (the closed table's
           top line, or a closed fish's row: Bish Creek's steelhead)
A change whose new answer lasts under `SHORT_GAP` days is still dated and is listed in `Change.short` (a build warning,
spec 3.3 rule 5: the shortest in r-46e4355a7277 is 15 days, Denetiah; Adams River's one-day Jul 31 is the planted
case).

THE PLACEMENT (spec 3.3 rule 7, contract 2.5; owner 2026-11-18 00:52 "said once"): `place_dates`: the card line says the
date most rows of the table share, else a box's the date most of its rows share, a white box (an own or shared band)
one all its rows share that differs from the level above, a row only its own different date (the most specific
level wins); a closed or release-every-one table's shared date sits under its top line; a Release /
Closed column date over `words.COLUMN_MAX` characters goes under the fish's name.

What this module does not see (F's and J's): joint limits (a box's total, a shared band, `together`); the size bands'
edge words (a Decided line carries its numbers, not which end it holds, so a band is worded in the boundary standard's
inclusive default; F passes the rule's own edges when it words a strip).
"""
from __future__ import annotations

import math
from typing import Callable, Dict, Iterable, List, Mapping, NamedTuple, Optional, Sequence, Tuple, Union

from pipeline.deliver import calendar as K
from pipeline.deliver.answers import common
from pipeline.deliver.answers.common import AnswersError

INF = math.inf
#: the trout and char of the table (THE predicate, `common.trout_char()`)
TC = common.trout_char()
#: a change whose new answer lasts fewer days than this is dated and warned about (spec 3.3 rule 5)
SHORT_GAP = 10

# Mutation pins (False = the broken rule; each has a test that fails with it off):
#: rule 2 (owner 15:36): a looser-only change is dated "rule changes <day>". False = the old rule: skip it and date the
#: next stricter change
A_LOOSER_SAYS_NO_DETAILS = True
#: rule 3: a change tighter in any part is worded. False = every keep -> keep change counts as looser
A_TIGHTER_IS_WORDED = True
#: rule 3: a closure names what closes (the display's whole-water closure, else the closing rule's fish). False = every
#: closure is a whole-water closure
A_CLOSURE_SCOPE_FROM_THE_RULE = True
#: P06: Feb 29 reads Feb 28's regulations. False = day 60 is read like any day
A_FEB29_READS_FEB28 = True
#: the walk wraps into next year. False = it stops at Dec 31
A_YEAR_WRAPS = True
#: "until <day>" names the last day of today's answer. False = it names the change's first day (a date one day late)
A_UNTIL_SAYS_THE_LAST_DAY = True


class TimelineError(AnswersError):
    """A change the date rule cannot state truthfully: the build stops (AGENTS W46), it never guesses."""


# --------------------------------------------------------------------------------------------
# The answer of a row
# --------------------------------------------------------------------------------------------

#: a closure's scope: "water" (the whole water, the display frame's), "trout_char" (every trout and char fish), or the
#: closed fish's codes
Scope = Union[str, Tuple[str, ...]]
#: the size lines a count is read from (`model.rows` LRel / LCap / LOuterSize)
_SIZE_LINES = ("rel", "cap", "outersize")


class Law(NamedTuple):
    """One origin's decided answer as the law it states: state, the day's number (None = no limit) and its size lines
    [(t, from cm, to cm | None, take | None)], sorted. Release and closed carry no number and no line."""
    state: str
    daily: Optional[float]
    lines: Tuple[Tuple[str, float, Optional[float], Optional[int]], ...]


_STATE = {"keep": "keep", "no_limit": "keep", "release": "release", "closed": "closed"}


def law_of(dec: Optional[dict]) -> Optional[Law]:
    """A stored `Decided` (dict) -> its `Law`; None for an origin no rule speaks for."""
    if dec is None:
        return None
    st = _STATE.get(dec["status"])
    if st is None:
        raise TimelineError(f"answers: trout timeline: unknown decided status {dec['status']!r}")
    if st != "keep":
        return Law(st, None, ())
    daily = None if dec["status"] == "no_limit" else dec.get("daily")
    lines = tuple(sorted((L["t"], L["a"], L.get("b"), L.get("take")) for L in dec.get("lines") or ()
                         if L["t"] in _SIZE_LINES))
    return Law("keep", daily, lines)


def count_at(law: Optional[Law], length: float) -> float:
    """How many fish of this length the law lets you keep today (INF = no limit; 0 = none)."""
    if law is None or law.state != "keep":
        return 0
    n = INF if law.daily is None else law.daily
    for t, a, b, take in law.lines:
        if (a or 0) <= length < (INF if b is None else b):
            n = 0 if t == "rel" else n if take is None else min(n, take)
    return n


def _edges(*laws: Optional[Law]) -> List[float]:
    pts = {0.0, INF}
    for law in laws:
        for _t, a, b, _n in (law.lines if law else ()):
            pts.add(float(a or 0))
            pts.add(INF if b is None else float(b))
    return sorted(pts)


def pieces(laws: Sequence[Optional[Law]], law: Optional[Law]) -> List[Tuple[float, float, float]]:
    """`law`'s count over the length pieces cut at every edge of `laws` (edges are whole or half cm, so a piece's middle
    is never an edge): [(from, to, count)]."""
    pts = _edges(*laws)
    out = []
    for x, y in zip(pts, pts[1:]):
        mid = x + 0.25 if y == INF else (x + y) / 2
        out.append((x, y, count_at(law, mid)))
    return out


class Answer(NamedTuple):
    """A row's answer on one segment (see the module's docstring)."""
    state: str                                        # keep | release | closed
    lead: str                                         # the leading origin (`common.lead_name`), or the row's own
    laws: Tuple[Tuple[str, Optional[Law]], ...]       # (origin, law): both on a fish row, one on an origin row
    possession: Optional[float]                       # the row's own possession (None: the page's line)
    scope: Optional[Scope]                            # closed: what closes; else None

    @property
    def key(self) -> tuple:
        """What two answers are compared by: a closed or release answer is its state (whatever closes it); a keep
        answer is its laws and possession."""
        if self.state != "keep":
            return (self.state,)
        return (self.state, self.laws, self.possession)


def closure_scope(species: Iterable[str]) -> Scope:
    """The closing rule's fish -> what the date line says closes: "trout_char" when it holds every trout and char fish
    (or an open subject: all fin fish), else the trout and char fish it names."""
    raw = list(species or ())
    from pipeline.regs.parsing import catalogue as C
    got = set(common.expand(raw))
    if any(c in C.OPEN_SUBJECTS for c in raw) or TC <= got:
        return "trout_char"
    fish = tuple(sorted(got & TC))
    if not fish:
        raise TimelineError(f"answers: trout timeline: a closing rule of {raw} closes no trout or char fish")
    return fish


def answer_of(frame: dict, row: str, *, water_closed: bool = False,
              scope_of: Optional[Callable[[int], Iterable[str]]] = None,
              possession: Optional[Mapping[str, float]] = None) -> Optional[Answer]:
    """One row's answer from one segment's rows frame. `row` "RB" (the fish) or "RB/wild" (one origin of a twin pair);
    `water_closed` the display frame's whole-water closure at this segment; `scope_of(win)` the fish of the rule a
    closed answer's `win` names; `possession` {fish: its own possession count} carried out of the derive."""
    S, _, o = row.partition("/")
    if S not in TC:
        raise TimelineError(f"answers: trout timeline: {S!r} is not a trout or char fish")
    fa = (frame.get("fish") or {}).get(S)
    if not fa:
        return None
    h, w = fa.get("hatchery"), fa.get("wild")
    if o:
        if o not in common.ORIGINS:
            raise TimelineError(f"answers: trout timeline: row {row!r}: {o!r} is not an origin")
        dec = fa.get(o)
        if dec is None:
            return None
        lead, laws = o, ((o, law_of(dec)),)
    else:
        lead = common.lead_name(h, w)
        if lead is None:
            return None
        dec = fa[lead]
        laws = (("hatchery", law_of(h)), ("wild", law_of(w)))
    state = _STATE[dec["status"]]
    if water_closed:
        return Answer("closed", lead, laws, None, "water")
    scope = None
    if state == "closed":
        if scope_of is None:
            raise TimelineError(f"answers: trout timeline: {row} is closed and no rule's fish were given")
        scope = closure_scope(scope_of(dec["win"]))
    poss = (possession or {}).get(S) if state == "keep" else None
    return Answer(state, lead, laws, poss, scope)


# --------------------------------------------------------------------------------------------
# The change
# --------------------------------------------------------------------------------------------

#: a part of what tightens (`words.then_words` says each): ("release",), ("closed", scope), ("hatchery_only",),
#: ("floor", cm), ("ceil", cm), ("count", n), ("band", n, from cm, to cm), ("possession", n)
Part = tuple
KINDS = ("looser", "tighter", "opens", "reopens")


class Change(NamedTuple):
    """The next dated change of a row."""
    kind: str                       # one of KINDS
    day: int                        # the first day of the new answer (1..366, never 60)
    last: int                       # the last day of today's answer (1..366, never 60)
    then: Tuple[Part, ...]          # tighter only: what tightens; else ()
    lasts: Optional[int]            # reading days the new answer holds before its own next change (None: the year)

    @property
    def short(self) -> bool:
        """Spec 3.3 rule 5: a new answer that lasts under SHORT_GAP days (a build warning, still dated)."""
        return self.lasts is not None and self.lasts < SHORT_GAP


def _looser_or_equal(a: Answer, b: Answer) -> bool:
    """Every length and origin legal in `a` stays legal in `b` at the same or a higher count, and the row's own
    possession does not shrink (both keep)."""
    bd = dict(b.laws)
    for o, la in a.laws:
        if la is None:
            continue
        lb = bd.get(o)
        if lb is None:
            return False
        for (_x, _y, na), (_x2, _y2, nb) in zip(pieces([la, lb], la), pieces([la, lb], lb)):
            if nb < na:
                return False
    if a.possession is not None and b.possession is not None and b.possession < a.possession:
        return False
    if a.possession is None and b.possession is not None:
        return False
    return True


def _keeps(law: Optional[Law]) -> bool:
    return law is not None and law.state == "keep" and any(n > 0 for _x, _y, n in pieces([law], law))


def _keep_then(a: Answer, b: Answer, row: str) -> Tuple[Part, ...]:
    """A tighter keep -> keep change: the parts that tighten, in words order."""
    ad, bd = dict(a.laws), dict(b.laws)
    parts: List[Part] = []
    if "hatchery" in ad and "wild" in ad and _keeps(ad["wild"]) and not _keeps(bd.get("wild")) \
            and _keeps(bd.get("hatchery")):
        parts.append(("hatchery_only",))
    lb = bd[b.lead]
    la = ad.get(b.lead)
    if not _keeps(la):
        la = ad[a.lead]
    pa, pb = pieces([la, lb], la), pieces([la, lb], lb)
    ka = [(x, y) for x, y, n in pa if n > 0]
    kb = [(x, y) for x, y, n in pb if n > 0]
    if not kb:
        raise TimelineError(f"answers: trout timeline: {row} keeps nothing on a keep answer")
    lo_a, hi_a = ka[0][0], ka[-1][1]
    lo_b, hi_b = kb[0][0], kb[-1][1]
    if lo_b > lo_a:
        parts.append(("floor", lo_b))
    if hi_b < hi_a:
        parts.append(("ceil", hi_b))
    inside = [(x, y, na, nb) for (x, y, na), (_x, _y, nb) in zip(pa, pb) if x >= lo_b and y <= hi_b]
    drops = [(x, y, nb) for x, y, na, nb in inside if nb < na]
    if drops:
        ns = {n for _x, _y, n in drops}
        run = all(drops[i][1] == drops[i + 1][0] for i in range(len(drops) - 1))
        if len(ns) != 1 or not run:
            raise TimelineError(f"answers: trout timeline: {row}'s limit tightens in {len(drops)} length bands at "
                                "once; the date line has words for one")
        n = ns.pop()
        if len(drops) == len(inside):
            parts.append(("count", n))
        else:
            parts.append(("band", n, drops[0][0], drops[-1][1]))
    if a.possession != b.possession and b.possession is not None and \
            (a.possession is None or b.possession < a.possession):
        parts.append(("possession", b.possession))
    if not parts:
        raise TimelineError(f"answers: trout timeline: {row}'s answer tightens and nothing in it can be said")
    return tuple(parts)


def _classify(a: Answer, b: Optional[Answer], row: str) -> Tuple[str, Tuple[Part, ...]]:
    if b is None:
        raise TimelineError(f"answers: trout timeline: no rule speaks for {row} after a day it is answered; the date "
                            "line has no words for that")
    if a.key == b.key:
        return "same", ()
    if a.state == "closed":
        return "reopens", ()
    if b.state == "closed":
        scope = b.scope if A_CLOSURE_SCOPE_FROM_THE_RULE else "water"
        return "tighter", (("closed", scope),)
    if a.state == "release":
        return ("opens", ()) if b.state == "keep" else ("same", ())
    if b.state == "release":
        return "tighter", (("release",),)
    if _looser_or_equal(a, b) or not A_TIGHTER_IS_WORDED:
        return "looser", ()
    return "tighter", _keep_then(a, b, row)


# --------------------------------------------------------------------------------------------
# One answer key's year
# --------------------------------------------------------------------------------------------

class KeyYear:
    """One answer key's segments: `starts` (a start day repeats once per moment group), its rows frames (one per
    segment), its `moments` (per segment `Moment.as_json`, None for a key with one moment), and per segment the display
    frame's whole-water closure, the rule -> fish lookup and the possession facts."""

    def __init__(self, starts: Sequence[int], frames: Sequence[dict], moments: Optional[Sequence[dict]] = None, *,
                 water_closed: Optional[Sequence[bool]] = None,
                 scope_of: Optional[Callable[[int], Iterable[str]]] = None,
                 possession: Optional[Sequence[Mapping[str, float]]] = None):
        n = len(starts)
        if len(frames) != n:
            raise TimelineError(f"answers: trout timeline: {len(frames)} rows frames for {n} segments")
        if moments is None and len(set(starts)) != n:
            raise TimelineError("answers: trout timeline: a key that repeats a start day needs its moments")
        if moments is not None and len(moments) != n:
            raise TimelineError(f"answers: trout timeline: {len(moments)} moments for {n} segments")
        if list(starts) != sorted(starts) or not starts or starts[0] != 1:
            raise TimelineError("answers: trout timeline: segments start on day 1, in day order")
        for x in (water_closed, possession):
            if x is not None and len(x) != n:
                raise TimelineError("answers: trout timeline: one value per segment")
        self.starts, self.frames = list(starts), list(frames)
        self.at = [K.Moment.from_json(m) for m in moments] if moments is not None else [K.ALWAYS] * n
        self.water_closed = list(water_closed) if water_closed is not None else [False] * n
        self.scope_of = scope_of
        self.possession = list(possession) if possession is not None else [None] * n
        self.days = sorted(set(self.starts))
        self._memo: Dict[Tuple[int, str], Optional[Answer]] = {}

    # -- days
    def effective(self, t: int) -> Optional[int]:
        """The first READING day of the segments starting on `t`: `t`, except a start on Feb 29 (day 60, never read,
        P06), which takes effect on Mar 1 or, when the next start is Mar 1, never (None)."""
        if t != K.LEAP or not A_FEB29_READS_FEB28:
            return t
        i = self.days.index(t)
        nxt = self.days[i + 1] if i + 1 < len(self.days) else K.DAYS + 1
        return None if nxt == K.LEAP + 1 else K.LEAP + 1

    @staticmethod
    def day_before(day: int) -> int:
        """The reading day before `day` (wrapping; Feb 29 is skipped: it reads Feb 28's)."""
        d = day - 1 if day > 1 else K.DAYS
        return d - 1 if d == K.LEAP and A_FEB29_READS_FEB28 else d

    # -- answers
    def answer(self, j: int, row: str) -> Optional[Answer]:
        k = (j, row)
        if k not in self._memo:
            self._memo[k] = answer_of(self.frames[j], row, water_closed=self.water_closed[j], scope_of=self.scope_of,
                                      possession=self.possession[j])
        return self._memo[k]

    def _seen_by(self, j: int, t: int) -> List[int]:
        """The segments starting on `t` that segment `j`'s table sees: a shared weekday, the same side of the hours
        window (a segment with no window is outside it)."""
        me = self.at[j]
        inside = bool(me.inside)
        got = [i for i, s in enumerate(self.starts) if s == t and self.at[i].weekdays & me.weekdays
               and (self.at[i].inside is None or self.at[i].inside == inside)]
        if not got:
            raise TimelineError(f"answers: trout timeline: segment {j} sees no moment of the start on day {t}")
        return got

    def _answers_at(self, j: int, t: int, row: str) -> List[Optional[Answer]]:
        out, keys = [], set()
        for i in self._seen_by(j, t):
            a = self.answer(i, row)
            k = None if a is None else a.key
            if k not in keys:
                keys.add(k)
                out.append(a)
        return out

    def rows(self, j: int, twins: Iterable[str] = ()) -> List[str]:
        """The rows segment `j` answers: each trout and char fish the frame answers, and for each fish in `twins` its
        two origin rows."""
        out = []
        for S in sorted((self.frames[j].get("fish") or {})):
            if S not in TC:
                continue
            for r in ([f"{S}/hatchery", f"{S}/wild"] if S in twins else [S]):
                if self.answer(j, r) is not None:
                    out.append(r)
        return out

    # -- the walk
    def _later(self, j: int):
        """(index into `days`, start day, first reading day) of every later start, wrapping, today's last."""
        n = len(self.days)
        p = self.days.index(self.starts[j])
        for q in range(p + 1, p + n):
            if q >= n and not A_YEAR_WRAPS:
                return
            t = self.days[q % n]
            e = self.effective(t)
            if e is not None:
                yield q, t, e

    def next_change(self, j: int, row: str) -> Optional[Change]:
        """Segment `j`'s next dated change for `row` (spec 3.3), or None when the row's answer holds all year (or
        it has no row today)."""
        a = self.answer(j, row)
        if a is None:
            return None
        for q, t, e in self._later(j):
            bs = self._answers_at(j, t, row)
            kinds = {_classify(a, b, row) for b in bs}
            moved = kinds - {("same", ())}
            if not moved:
                continue
            if len(moved) == 1 and len(kinds) == 1:
                kind, then = moved.pop()
            elif all(k in ("looser", "opens", "reopens") for k, _ in moved):
                # some of the segment's weekdays keep today's answer, the others loosen: only "rule changes" is
                # true of all of them
                kind, then = "looser", ()
            else:
                raise TimelineError(f"answers: trout timeline: {row} changes on day {e} differently on different "
                                    f"weekdays ({sorted(k for k, _ in kinds)}); the date line names no weekday")
            if kind == "looser" and not A_LOOSER_SAYS_NO_DETAILS:
                continue
            last = self.day_before(e) if A_UNTIL_SAYS_THE_LAST_DAY else e
            return Change(kind, e, last, then, self._lasts(j, row, q, e, bs))
        return None

    def _lasts(self, j: int, row: str, q0: int, e0: int, bs: List[Optional[Answer]]) -> Optional[int]:
        """Reading days the new answer `bs` holds from day `e0` before the next start whose answer differs."""
        keys = {None if b is None else b.key for b in bs}
        n = len(self.days)
        for q in range(q0 + 1, q0 + n):
            t = self.days[q % n]
            e = self.effective(t)
            if e is None:
                continue
            got = {None if b is None else b.key for b in self._answers_at(j, t, row)}
            if got != keys:
                span = (e - e0) % K.DAYS or K.DAYS
                leap = any((e0 + k - 1) % K.DAYS + 1 == K.LEAP for k in range(span))
                return span - (1 if leap and A_FEB29_READS_FEB28 else 0)
        return None


def timeline(year: KeyYear, twins: Iterable[str] = ()) -> List[Dict[str, Optional[Change]]]:
    """Every segment's rows -> their next change (None: none all year)."""
    twins = tuple(twins)
    return [{r: year.next_change(j, r) for r in year.rows(j, twins)} for j in range(len(year.starts))]


def short_changes(year: KeyYear, twins: Iterable[str] = ()) -> List[Tuple[int, str, Change]]:
    """The build warnings of spec 3.3 rule 5: (segment, row, change) for every change whose new answer is short."""
    return [(j, r, c) for j, got in enumerate(timeline(year, twins)) for r, c in got.items() if c and c.short]


# --------------------------------------------------------------------------------------------
# The year bar and the placement
# --------------------------------------------------------------------------------------------

def year_bar(states: Iterable[str], water_open: bool) -> bool:
    """The "Trout and char through the year" bar shows only when every trout and char fish shares one state today,
    all release or all closed, and the water is open (owner 12:42)."""
    got = set(states)
    return water_open and len(got) == 1 and got <= {"release", "closed"}


class BoxRows(NamedTuple):
    """A table box as the placement sees it: its kind, its rows' keys in order, its bands (from, to, kind)."""
    kind: str                                   # keep | release | closed
    rows: Tuple[str, ...]
    bands: Tuple[Tuple[int, int, str], ...] = ()


#: owner 2026-11-18 00:52 ("until May 31, then closed to all fishing" on 5 of the Vedder's rows): a date most rows of a box
#: (or of the whole table) share is said once, at the box (or under the top line, the card line); only the rows whose
#: date differs say their own. A level's date holds every row under it WITHOUT a date of its own (the most specific
#: wins: row, then band, then box, then the card line). False = the pin: a level only where every row shares it
A_DATE_SAID_ONCE = True


def _common(got: Sequence[Optional[str]]) -> Optional[str]:
    """The date most of `got` share, where every row has one, two or more share it and no other date as many; None
    otherwise (a row with no date under a level's date would read as changing)."""
    if not got or any(x is None for x in got):
        return None
    if len(set(got)) == 1:
        return got[0]
    if not A_DATE_SAID_ONCE:
        return None
    counts = sorted(((got.count(x), x) for x in set(got)), reverse=True)
    return counts[0][1] if counts[0][0] >= 2 and counts[0][0] > counts[1][0] else None


def place_dates(boxes: Sequence[BoxRows], dates: Mapping[str, Optional[str]], form: Optional[str] = None) -> dict:
    """Spec 3.3 rule 7: each row's date text (`words.date_line`; None: no change) placed ONCE. `form` "closed" or
    "release" for a closed or release-every-one table (its shared date goes under the top line). The card line (or a
    box) says the date most of its rows share (`_common`); a band or a row says only what differs from the level above
    it. Returns the `DateSet` placement fields {top, card, box {"b"}, band {"b:k"}, row {"b:r"}}."""
    from pipeline.deliver.answers.trout import words as W
    out: dict = {"top": None, "card": None, "box": {}, "band": {}, "row": {}}
    every = [dates.get(r) for b in boxes for r in b.rows]
    if every and every[0] is not None and len(set(every)) == 1 and form in ("closed", "release"):
        out["top"] = every[0]
        return out
    above = _common(every) if form not in ("closed", "release") else None
    if above is not None and W.is_reopening(above):
        if len(set(every)) == 1:
            raise TimelineError("answers: trout timeline: a reopening date on an open table's card line")
        above = None
    if above is not None:
        out["card"] = W.card_line(above)
    for bi, b in enumerate(boxes):
        got = [dates.get(r) for r in b.rows]
        if b.kind != "closed" and any(x is not None and W.is_reopening(x) for x in got):
            raise TimelineError(f"answers: trout timeline: a reopening date in a {b.kind} box: only a fish closed today "
                                "reopens")
        here = above
        mine = _common(got)
        if mine is not None and mine != above and (b.kind == "keep" or W.column_fits(mine)) \
                and sum(x != mine for x in got) < sum(x != above for x in got):
            out["box"][str(bi)] = mine
            here = mine
        said = set()
        if b.kind == "keep":
            for ki, (fr, to, kind) in enumerate(b.bands):
                span = got[fr:to + 1]
                if kind != "upto" and span and span[0] is not None and len(set(span)) == 1 and span[0] != here:
                    out["band"][f"{bi}:{ki}"] = span[0]
                    said |= set(range(fr, to + 1))
        for ri, x in enumerate(got):
            if ri in said or x == here:
                continue
            if x is None:
                # (only where no level above says a date: `_common` asks every row for one)
                continue
            out["row"][f"{bi}:{ri}"] = x
    return out
