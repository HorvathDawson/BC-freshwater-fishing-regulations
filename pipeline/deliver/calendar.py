"""THE CALENDAR — the one definition of a day, a year's cuts and a run of days (DATAFLOW §1.3 C).

Every stage of the delivery (the verdicts, the status index, the export, the answers) imports this
module; none spells a calendar of its own (`test_one_data_flow.py` gate 4: `_day_index` and
`_LAST_DAY` are read here and in the catalogue, nowhere else).

  Day        1..366 on the catalogue's LEAP calendar (`catalogue._day_index`): Jan 1 = 1, Feb 29 =
             60, Mar 1 = 61 in EVERY year, Dec 31 = 366. The only date unit between stages.
  MonthDay   (month, day) — used only to ASK the reader (`read.in_force` takes one) and to write
             words. MMDD integers are display-word internals and never cross a boundary.
  InForce    a `when` on one day: no / yes / part (`read.IN_FORCE`, wire 0 / 1 / 2).

THE CUT. A key's year is cut wherever any `when` the reader reads changes its `InForce`: every
member rule's `when` and every lift's `when` (`rule_vectors` + `segments`). Two days whose vectors
all read alike get the same answer from the reader — that is the reader's own signature
(`read.effective_rules_bound` reads a day only through `in_force` of those `when`s).
"""
from __future__ import annotations

import json
from functools import lru_cache
from typing import Iterable, List, NamedTuple, NewType, Optional, Sequence, Tuple

from pipeline.regs.parsing.catalogue import _LAST_DAY, _day_index

#: The days of the leap calendar.
DAYS = 366

Day = NewType("Day", int)
MonthDay = Tuple[int, int]


class CalendarError(ValueError):
    """A day, a run or a signature that is not on the leap calendar."""


def day_of(on) -> Day:
    """`datetime.date` or `(month, day)` -> 1..366 on the catalogue's leap calendar."""
    m, d = (on.month, on.day) if hasattr(on, "month") else on
    return Day(_day_index(m, d))


def month_day(day: int) -> MonthDay:
    """1..366 -> (month, day), the inverse of `day_of` (day 60 is Feb 29)."""
    if not 1 <= day <= DAYS:
        raise CalendarError(f"calendar: day {day} is not 1..{DAYS}")
    m = 1
    while day > _LAST_DAY[m]:
        day -= _LAST_DAY[m]
        m += 1
    return m, day


#: Every day of the year as (month, day), by day index (1..366).
MD: dict = {_day_index(m, d): (m, d) for m in range(1, 13) for d in range(1, _LAST_DAY[m] + 1)}

#: Feb 29. The book prints its dates for a year without one: a note reads a set of days over such
#: a year (`runs`), never naming a stray "Feb 29".
LEAP: Day = Day(_day_index(2, 29))


# --------------------------------------------------------------------------------------------
# A `when` over the year, and the year's cuts
# --------------------------------------------------------------------------------------------

def _in_force_code() -> dict:
    from pipeline.deliver.bundle import read
    return {name: code for code, name in enumerate(read.IN_FORCE)}


# --------------------------------------------------------------------------------------------
# THE MOMENT: which days of the week, and which hours of the day, a reading holds (answers 2.1)
# --------------------------------------------------------------------------------------------
#
# A `when` may hold on some WEEKDAYS ("Kokanee 5 per day, on Saturdays and Sundays", p.37) or
# some HOURS ("No fishing, one hour after sunset to one hour before sunrise", p.23). Such a rule
# DECIDES for the weekdays / hours it covers, and is not in force at the others (user ruling
# 2026-10-08, review D2/G5; before it stood "beside" and decided nothing). A day of the year is
# therefore not enough to ask the reader: it is asked at a MOMENT — a class of weekdays (a
# bitmask, Monday = bit 0 .. Sunday = bit 6) and, where a rule of the key holds some hours, whether
# the moment is INSIDE that window or outside it. A key's moments partition its week x clock
# (`moments`): the weekday sets of its rules cut the week; its one hours window (a key with two
# different windows is refused — none exists) cuts the day in two. A key with no such rule has
# one moment, `ALWAYS`, and reads exactly as before.

WEEKDAYS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
ALL_WEEK = (1 << len(WEEKDAYS)) - 1


def weekday_mask(names: Iterable[str]) -> int:
    """`when.weekdays` (the catalogue's names) -> a bitmask, Monday = bit 0."""
    m = 0
    for n in names:
        if n not in WEEKDAYS:
            raise CalendarError(f"calendar: {n!r} is not a weekday ({', '.join(WEEKDAYS)})")
        m |= 1 << WEEKDAYS.index(n)
    return m


def weekday_names(mask: int) -> List[str]:
    return [n for i, n in enumerate(WEEKDAYS) if mask >> i & 1]


def hours_key(hours: dict) -> str:
    """A `when.hours` window as one canonical string (what two rules share when they print the
    same window)."""
    return json.dumps(hours, sort_keys=True, separators=(",", ":"))


class Moment(NamedTuple):
    """WHEN IN THE WEEK AND THE DAY a reading holds: `weekdays` a bitmask (1..127), `hours` the
    canonical window (`hours_key`) of the key's hours rule or None, `inside` whether the moment is
    inside that window (None iff `hours` is None)."""
    weekdays: int = ALL_WEEK
    hours: Optional[str] = None
    inside: Optional[bool] = None

    def check(self) -> "Moment":
        if not 1 <= self.weekdays <= ALL_WEEK:
            raise CalendarError(f"calendar: moment weekdays {self.weekdays} is not 1..{ALL_WEEK}")
        if (self.hours is None) != (self.inside is None):
            raise CalendarError(f"calendar: moment {self}: `inside` goes with `hours`")
        return self

    @classmethod
    def from_json(cls, x: Optional[dict]) -> "Moment":
        """`as_json`'s inverse (None: ALWAYS) — a case's or a segment's `at`."""
        if x is None:
            return cls()
        h = x.get("hours")
        return cls(weekday_mask(x["weekdays"]),
                   None if h is None else hours_key({k: v for k, v in h.items() if k != "in"}),
                   None if h is None else bool(h["in"])).check()

    def as_json(self) -> dict:
        """The moment as the answers file states it: `weekdays` (names, Monday first) and `hours`
        (null, or the window with `in`: true inside it, false at every other hour)."""
        return {"weekdays": weekday_names(self.weekdays),
                "hours": None if self.hours is None else {**json.loads(self.hours),
                                                          "in": bool(self.inside)}}


#: The moment of a key with no weekday or hours rule: every day of the week, every hour.
ALWAYS = Moment()


def _conditional_whens(rules: Sequence[dict]) -> List[dict]:
    rules = list(rules)
    out = [x.get("when") for x in rules]
    out += [lift.get("when") for x in rules for lift in x.get("exempts") or [] if "when" in lift]
    return [w for w in out if w]


def moments(rules: Iterable[dict]) -> List[Moment]:
    """The moments a key's rules (and their lifts) cut the week and the day into, in order:
    weekday classes by their first day (Monday first), and within each, outside the hours window
    before inside it. One moment, `ALWAYS`, when no rule holds on some weekdays or hours only."""
    whens = _conditional_whens(list(rules))
    masks = sorted({weekday_mask(w["weekdays"]) for w in whens if w.get("weekdays")})
    windows = sorted({hours_key(w["hours"]) for w in whens if w.get("hours")})
    if len(windows) > 1:
        raise CalendarError(f"calendar: one rule key holds {len(windows)} different hours windows "
                            f"{windows} — the moment has room for one; teach `moments` the overlap")
    classes = [ALL_WEEK]
    for m in masks:
        classes = [c for x in classes for c in (x & m, x & ~m & ALL_WEEK) if c]
    classes.sort(key=lambda c: (c & -c, c))
    hs = [(None, None)] if not windows else [(windows[0], False), (windows[0], True)]
    return [Moment(c, h, i).check() for c in classes for h, i in hs]


@lru_cache(maxsize=None)
def _vector(when_json: str, at: Optional[Moment]) -> Tuple[int, ...]:
    from pipeline.deliver.bundle import read
    code = _in_force_code()
    when = json.loads(when_json)
    return tuple(code[read.in_force(when, month_day(d), at)] for d in range(1, DAYS + 1))


def when_vector(when, at: Optional[Moment] = None) -> Tuple[int, ...]:
    """A `when` as its 366 day codes (`read.IN_FORCE` index: 0 no, 1 yes, 2 part), asked at a
    moment (`at`; None: not known — a weekday or hours rule then reads "part")."""
    return _vector(json.dumps(when or None, sort_keys=True), at)


@lru_cache(maxsize=None)
def _changes(vec: Tuple[int, ...]) -> frozenset:
    return frozenset(d for d in range(2, DAYS + 1) if vec[d - 1] != vec[d - 2])


def rule_vectors(rules: Iterable[dict], at: Optional[Moment] = None) -> List[Tuple[int, ...]]:
    """Every `when` the reader reads for these rules, in order: each rule's own and each of its
    lifts', asked at moment `at`. Two days whose vectors all read alike get one answer from the
    reader."""
    rules = list(rules)
    out = [when_vector(x.get("when"), at) for x in rules]
    for x in rules:
        for lift in x.get("exempts") or []:
            if "when" in lift:
                out.append(when_vector(lift.get("when"), at))
    return out


def moment_segments(vectors_by_moment: Sequence[Sequence[Tuple[int, ...]]]
                    ) -> Tuple[List[List[List[int]]], List[Tuple[int, int]]]:
    """`segments` per moment, the readings SHARED across moments: (`runs` per moment, each
    [[start_day, reading]] covering 1..366; `readings` [(first day, moment index)] — the day and
    moment each distinct reading is first asked at, numbered moment by moment in day order). A
    reading is a signature of every `when`'s code, so the same signature at two moments is one
    reading (Babine's weekend angler closure, out of its dates, reads as every weekday does). With
    one moment this is exactly `segments`."""
    seen: dict = {}
    readings: List[Tuple[int, int]] = []
    out: List[List[List[int]]] = []
    for m, vectors in enumerate(vectors_by_moment):
        cuts = {1}
        for v in vectors:
            cuts |= _changes(v)
        runs: List[List[int]] = []
        for d in sorted(cuts):
            sig = tuple(v[d - 1] for v in vectors)
            i = seen.get(sig)
            if i is None:
                i = seen[sig] = len(readings)
                readings.append((d, m))
            if not runs or runs[-1][1] != i:
                runs.append([d, i])
        out.append(runs)
    return out, readings


def segments(vectors: Sequence[Tuple[int, ...]]) -> Tuple[List[List[int]], List[int]]:
    """The year cut where any vector changes: `runs` [[start_day, reading]] (contiguous, covering
    1..366, the first starting on day 1) and `readings` [first day of each distinct reading],
    numbered in day order. A reading recurs (both sides of a winter closure read alike) and is
    computed once."""
    cuts = {1}
    for v in vectors:
        cuts |= _changes(v)
    runs: List[List[int]] = []
    seen: dict = {}
    readings: List[int] = []
    for d in sorted(cuts):
        sig = tuple(v[d - 1] for v in vectors)
        i = seen.get(sig)
        if i is None:
            i = seen[sig] = len(readings)
            readings.append(d)
        if not runs or runs[-1][1] != i:
            runs.append([d, i])
    return runs, readings


def per_day(runs: Sequence[Sequence[int]]) -> List[int]:
    """`segments`' runs as one reading index per day, 1..366."""
    out: List[int] = []
    for i, (d, r) in enumerate(runs):
        end = runs[i + 1][0] if i + 1 < len(runs) else DAYS + 1
        out += [r] * (end - d)
    if len(out) != DAYS:
        raise CalendarError("calendar: runs must cover every day of the year")
    return out


def segments_of(signatures: Sequence) -> List[int]:
    """The start days of the runs of equal signatures over days 1..366 (one signature per day).
    Day 1 always starts a segment: a reading running across New Year is two segments."""
    if len(signatures) != DAYS:
        raise CalendarError(f"calendar: a signature per day must cover {DAYS} days")
    starts = [1]
    for d in range(2, DAYS + 1):
        if signatures[d - 1] != signatures[d - 2]:
            starts.append(d)
    return starts


def reading_of(runs: Sequence[Sequence[int]], day: int) -> int:
    """The reading a day falls in, from `segments`' runs (sorted by start day)."""
    if not 1 <= day <= DAYS:
        raise CalendarError(f"calendar: day {day} is not 1..{DAYS}")
    got = None
    for d, r in runs:
        if d > day:
            break
        got = r
    if got is None:
        raise CalendarError("calendar: runs do not start on day 1")
    return got


# --------------------------------------------------------------------------------------------
# A set of days in words: runs over a year without Feb 29 (the book's year)
# --------------------------------------------------------------------------------------------

def runs(days) -> List[List[int]]:
    """A set of day indexes as `[[from_month, from_day, to_month, to_day], ...]` on a circular
    year without Feb 29 (a run ending Dec 31 and one starting Jan 1 are one run: "Oct 1-May 31")."""
    ds = sorted(set(days) - {LEAP})
    if not ds:
        return []

    def step(a, b):
        return b == a + 1 or (a == LEAP - 1 and b == LEAP + 1)
    out, start = [], ds[0]
    for prev, cur in zip(ds, ds[1:]):
        if not step(prev, cur):
            out.append((start, prev))
            start = cur
    out.append((start, ds[-1]))
    if len(out) > 1 and out[0][0] == 1 and out[-1][1] == len(MD):
        out = [(out[-1][0], out[0][1])] + out[1:-1]
    return [[*MD[a], *MD[b]] for a, b in out]


def run_days(rs: list) -> List[int]:
    """`runs` back to day indexes, in order (a wrapping run from its first day), Feb 29 left out."""
    out: List[int] = []
    for a, b, c, d in rs:
        i, j = _day_index(a, b), _day_index(c, d)
        out += list(range(i, j + 1)) if i <= j else list(range(i, len(MD) + 1)) + \
            list(range(1, j + 1))
    return [d for d in out if d != LEAP]
