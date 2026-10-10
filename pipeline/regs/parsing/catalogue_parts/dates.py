"""THE CALENDAR: printed dates and clocks, `DateRange` / `Clock` / `Hours` / `When`, and the one
place dates become days (`range_days`, `_days`).

Split out of `pipeline/regs/parsing/catalogue.py`, which re-exports every name; import from
there."""

from __future__ import annotations

import re

from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field, model_validator

from pipeline.common import calendar_spec as SPEC

from .vocab import _Terse


#: Month name -> number, and the last day of each, from THE CALENDAR SPEC
#: (`pipeline.common.calendar_spec`, the one source every language's copy is generated from).
#: February is 29 ON PURPOSE: the book says "February", which includes the 29th in the years it
#: exists, and 28 would quietly shorten it.
_MONTHS = {m.lower(): i + 1 for i, m in enumerate(SPEC.MONTHS)}
_LAST_DAY = {m: n for m, n in enumerate(SPEC.LAST_DAY, start=1)}


def parse_clock(text: str) -> Optional["Clock"]:
    """A printed time -> a `Clock`. "21:00" and "21:00 hours" are the same instant spelled two
    ways and both sat in the corpus; "one hour after sunset" is not a clock time at all."""
    t = " ".join((text or "").split()).lower().replace(" hours", "")
    if not t:
        return None
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", t)
    if m:
        return Clock(at=f"{int(m.group(1)):02d}:{m.group(2)}")
    m = re.fullmatch(r"(?:(one|an|\d+)\s*(hour|hours|minute|minutes|min)\s*)?"
                     r"(before|after)?\s*(sunrise|sunset)", t)
    if not m:
        return None
    n, unit, side, ev = m.groups()
    mins = 0
    if n:
        v = 1 if n in ("one", "an") else int(n)
        mins = v * (60 if unit.startswith("hour") else 1)
        if side == "before":
            mins = -mins
    return Clock(solar=Solar(ev), offset_min=mins)


def parse_date_range(text: str) -> Optional["DateRange"]:
    """One printed window -> a `DateRange`. Returns None where the text does not parse, so a
    caller fails loudly rather than inventing a season.

    Handles the four spellings the corpus uses: "Nov 1-Apr 30", "Nov 1 - Apr 30" (the same range,
    and 74 rules split between them), "May 1-31" (same month, end unqualified) and a bare month.
    """
    t = " ".join((text or "").split())
    if not t:
        return None
    parts = re.split(r"\s*(?:-|\u2013|\u2014|\bto\b)\s*", t, maxsplit=1)
    lo = _point(parts[0])
    if lo is None:
        return None
    if len(parts) == 1:
        if lo[1] is not None:
            return None                                  # a lone date, not a range
        return DateRange(from_month=lo[0], from_day=1,
                         to_month=lo[0], to_day=_LAST_DAY[lo[0]])
    hi = _point(parts[1])
    if hi is None:
        m = re.fullmatch(r"(\d{1,2})", parts[1].strip())  # "May 1-31": the month carries over
        if not m or lo[1] is None:
            return None
        hi = (lo[0], int(m.group(1)))
    if lo[1] is None or hi[1] is None:
        return None
    return DateRange(from_month=lo[0], from_day=lo[1], to_month=hi[0], to_day=hi[1])


def _point(text: str):
    """"Nov 1" -> (11, 1); "February" -> (2, None); anything else -> None."""
    t = text.strip().rstrip(",")
    m = re.fullmatch(r"([A-Za-z]+)\.?\s*(\d{1,2})", t)
    if m:
        mo = _month(m.group(1))
        return (mo, int(m.group(2))) if mo else None
    m = re.fullmatch(r"([A-Za-z]+)\.?", t)
    if m:
        mo = _month(m.group(1))
        return (mo, None) if mo else None
    return None


def _month(name: str) -> Optional[int]:
    n = name.strip().lower()
    return _MONTHS.get("sep" if n.startswith("sept") else n[:3])


def _day_index(month: int, day: int) -> int:
    return SPEC.day_index(month, day)


def range_days(r: "DateRange") -> List[int]:
    """THE DAYS A PRINTED RANGE HOLDS, on the leap calendar (1..366), wrapping New Year — the one
    place a range's dates become days (`_days`, `complement`; `read.in_force` reads `_days`).

    A RANGE PRINTED TO FEB 28 RUNS THROUGH FEB 29 (ruling 2026-10-06): the 2025-2027 synopsis is
    written for years without a Feb 29, and "Jan 1-Feb 28" means through the end of February — in a
    leap year the Nicola below the lake is catch and release on Feb 29, not closed. A range that
    STARTS Mar 1 (or on any other day) is untouched."""
    a, b = _day_index(r.from_month, r.from_day), _day_index(r.to_month, r.to_day)
    if (r.to_month, r.to_day) == (2, 28):
        b += 1
    if a <= b:
        return list(range(a, b + 1))
    return list(range(a, SPEC.DAYS + 1)) + list(range(1, b + 1))


def complement(ranges: List["DateRange"]) -> List["DateRange"]:
    """THE DAYS THESE RANGES DO NOT COVER, on a circular year.

    This is what retires `windows_are: "excepts"`. "Open June 16-Apr 30 each year" is a CLOSURE
    whose printed dates are the days it does not apply; its complement, May 1 - June 15, is the
    closure's own season and needs no flag to read correctly.
    """
    covered = set()
    for r in ranges:
        covered.update(range_days(r))
    total = SPEC.DAYS
    free = [d for d in range(1, total + 1) if d not in covered]
    if not free:
        return []
    runs, start = [], free[0]
    for prev, cur in zip(free, free[1:]):
        if cur != prev + 1:
            runs.append((start, prev)); start = cur
    runs.append((start, free[-1]))
    # A run ending on Dec 31 and one starting Jan 1 are ONE run on a circle.
    if len(runs) > 1 and runs[0][0] == 1 and runs[-1][1] == total:
        runs = [(runs[-1][0], runs[0][1])] + runs[1:-1]
    return [DateRange(from_month=_md(a)[0], from_day=_md(a)[1],
                      to_month=_md(b)[0], to_day=_md(b)[1]) for a, b in runs]


def _md(idx: int):
    return SPEC.month_day(idx)


class Solar(str, Enum):
    sunrise = "sunrise"
    sunset = "sunset"


class Clock(BaseModel):
    """A TIME OF DAY, either off the clock or off the sun.

    The book writes both and the corpus stored both as prose — "21:00", "21:00 hours" and "one
    hour after sunset" all sat in the same string field, so the first two were the same instant
    spelled two ways and the third was not a time at all. A solar time cannot be resolved to a
    clock without a date and a latitude, which is the client's to do and not the parser's, so it
    is carried as what it is.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    at: Optional[str] = Field(default=None, pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    solar: Optional[Solar] = None
    #: Minutes from the solar event. NEGATIVE IS BEFORE — "30 minutes before sunrise" is
    #: `{solar: sunrise, offset_min: -30}`, and the sign is the whole difference between
    #: fishing legally and not.
    offset_min: int = 0

    @model_validator(mode="after")
    def _one_kind(self) -> "Clock":
        if bool(self.at) == bool(self.solar):
            raise ValueError("a time is a clock time OR a solar time, not both and not neither")
        if self.at and self.offset_min:
            raise ValueError("an offset belongs to a solar time; put it in the clock time")
        return self

    def words(self) -> str:
        if self.at:
            return self.at
        n = abs(self.offset_min)
        if not n:
            return self.solar.value
        unit = f"{n} minutes" if n % 60 else ("one hour" if n == 60 else f"{n // 60} hours")
        return f"{unit} {'before' if self.offset_min < 0 else 'after'} {self.solar.value}"


class DateRange(BaseModel):
    """A RANGE OF CALENDAR DAYS, no year. Both ends INCLUSIVE, per the synopsis: "When no date
    is listed, the regulations apply ALL YEAR. Start and end dates are INCLUSIVE."

    A range may WRAP the year end — "Nov 1-Apr 30" is one winter, not an error — so `to` before
    `from` is meaningful and is not rejected.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    from_month: int = Field(ge=1, le=12)
    from_day: int = Field(ge=1, le=31)
    to_month: int = Field(ge=1, le=12)
    to_day: int = Field(ge=1, le=31)

    @model_validator(mode="after")
    def _real_days(self) -> "DateRange":
        for m, d, side in ((self.from_month, self.from_day, "from"),
                           (self.to_month, self.to_day, "to")):
            if d > _LAST_DAY[m]:
                raise ValueError(f"{side}: day {d} does not exist in month {m}")
        return self

    def words(self) -> str:
        nm = SPEC.MONTHS
        return f"{nm[self.from_month - 1]} {self.from_day}-{nm[self.to_month - 1]} {self.to_day}"


class Hours(BaseModel):
    """A RANGE WITHIN THE DAY. Either end may be a clock time or a solar one, so "from one hour
    after sunset to one hour before sunrise" is sayable. It WRAPS midnight the same way a
    `DateRange` wraps the year end, and needs no flag for that either."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    start: Clock
    end: Clock

    def words(self) -> str:
        return f"{self.start.words()} to {self.end.words()}"


class When(_Terse):
    """WHEN A RULE BINDS — the days, the hours and the weekdays, said once.

    This replaces `windows` (210 distinct FREE-TEXT strings, where "Nov 1-Apr 30" and
    "Nov 1 - Apr 30" were the same range spelled two ways), `windows_are`, `from_time`, `to_time`
    and `weekdays`.

    THERE IS NO `excepts` FLAG. `windows_are: "excepts"` meant "these are the days the rule does
    NOT hold" — a flag that inverted the field beside it, which is exactly what `band` did to the
    size fields. Three rules carried it. The complement of a circular range is another circular
    range, so the days a rule DOES hold are always writable: Fulton River's "Open June 16-Apr 30
    each year" is a closure, and it is stored as the closure's own days, May 1 - June 15.

    `dates` EMPTY MEANS ALL YEAR, per the synopsis: "When no date is listed, the regulations apply
    ALL YEAR. Start and end dates are INCLUSIVE."

    TERSE (`_Terse`): an empty list here is a default and says nothing, so it is never dumped —
    every designation shipped `"unparsed":[],"weekdays":[]` beside its dates.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    dates: List[DateRange] = Field(default_factory=list)
    hours: Optional[Hours] = None
    weekdays: List[str] = Field(default_factory=list)
    #: SEASONS THE PARSER COULD NOT READ, kept verbatim. This is the time analogue of
    #: `unresolved_locators`, and it exists for the same reason: an unparsed season and an ABSENT
    #: one are opposite facts, and a rule published as though it had no season when its source
    #: says "To be determined" is open all year to a reader. The DFO feed scrapes rows whose date
    #: cell is prose, and refusing them would have meant dropping the rule or inventing a window.
    unparsed: List[str] = Field(default_factory=list)

    def is_empty(self) -> bool:
        return not (self.dates or self.hours or self.weekdays or self.unparsed)

    def words(self) -> str:
        bits = [" and ".join(d.words() for d in self.dates)] if self.dates else []
        if self.hours:
            bits.append(self.hours.words())
        if self.weekdays:
            bits.append(" and ".join(f"{d}s" for d in self.weekdays))
        if self.unparsed:
            bits.append(" and ".join(self.unparsed))
        return ", ".join(b for b in bits if b)


def _days(ranges: List["DateRange"]) -> set:
    got: set = set()
    for r in ranges:
        got.update(range_days(r))
    return got
