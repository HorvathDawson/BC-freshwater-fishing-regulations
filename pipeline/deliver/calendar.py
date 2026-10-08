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
from typing import Iterable, List, NewType, Sequence, Tuple

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


@lru_cache(maxsize=None)
def _vector(when_json: str) -> Tuple[int, ...]:
    from pipeline.deliver.bundle import read
    code = _in_force_code()
    when = json.loads(when_json)
    return tuple(code[read.in_force(when, month_day(d))] for d in range(1, DAYS + 1))


def when_vector(when) -> Tuple[int, ...]:
    """A `when` as its 366 day codes (`read.IN_FORCE` index: 0 no, 1 yes, 2 part)."""
    return _vector(json.dumps(when or None, sort_keys=True))


@lru_cache(maxsize=None)
def _changes(vec: Tuple[int, ...]) -> frozenset:
    return frozenset(d for d in range(2, DAYS + 1) if vec[d - 1] != vec[d - 2])


def rule_vectors(rules: Iterable[dict]) -> List[Tuple[int, ...]]:
    """Every `when` the reader reads for these rules, in order: each rule's own and each of its
    lifts'. Two days whose vectors all read alike get one answer from the reader."""
    rules = list(rules)
    out = [when_vector(x.get("when")) for x in rules]
    for x in rules:
        for lift in x.get("exempts") or []:
            if "when" in lift:
                out.append(when_vector(lift.get("when")))
    return out


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
