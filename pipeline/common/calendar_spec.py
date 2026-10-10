"""THE CALENDAR SPEC — the one definition of the regulations' calendar, for every language.

The calendar was spelled four times: the catalogue's `dates` part (Python), the delivery's
`calendar.py` (Python), `@app/core` `statusIndex.ts` (TypeScript) and the reference page's script
(`page_v36.js`), each with its own month table, cumulative-days table or weekday list. This module
is the source; `python -m pipeline.tools.emit_calendar` writes the TypeScript / JSON / page JS from
it, and `--check` fails CI when a generated copy drifts (AGENTS 40). `test_dataflow_gates.py`
gate 4 refuses a hand-written copy anywhere else.

WHAT IS SHARED: the constants and the two arithmetic rules every copy implements (a date's day,
a day's date). WHAT IS NOT: platform logic — reading a JS `Date` in local time, a page's 365-day
display year, parsing printed dates — stays where it runs and reads these constants.

  LEAP CALENDAR  Day 1..366: Jan 1 = 1, Feb 29 = 60, Mar 1 = 61 in EVERY year, Dec 31 = 366, so a
                 printed "Mar 1" is the same day whether or not the year has a Feb 29. February is
                 29 ON PURPOSE: the book says "February", which includes the 29th in the years it
                 exists, and 28 would quietly shorten it.
  COMMON YEAR    A year WITHOUT Feb 29: the same months with February 28 long. The book prints its
                 dates for one (the 2025-2027 synopsis), so a display of the printed year walks it,
                 and so does a 365-day day-of-year circle (run timing).
  WEEKDAYS       Monday first (a weekday bitmask's bit 0), the catalogue's `when.weekdays` names.
"""
from __future__ import annotations

from typing import Tuple

#: The months as the book abbreviates them, January first.
MONTHS: Tuple[str, ...] = ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
                           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

#: The last day of each month on the LEAP calendar (February has 29).
LAST_DAY: Tuple[int, ...] = (31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)

#: The days of the leap calendar.
DAYS: int = sum(LAST_DAY)

#: The days before each month on the leap calendar (January: 0).
DAYS_BEFORE: Tuple[int, ...] = tuple(sum(LAST_DAY[:i]) for i in range(len(LAST_DAY)))

#: Feb 29's day: 60.
LEAP_DAY: int = DAYS_BEFORE[1] + 29

#: The last day of each month in a common year (no Feb 29) — the book's printed year.
COMMON_YEAR_LAST_DAY: Tuple[int, ...] = tuple(n - (m == 2)
                                              for m, n in enumerate(LAST_DAY, start=1))

#: The days of the week, Monday first.
WEEKDAYS: Tuple[str, ...] = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
                             "Sunday")


def day_index(month: int, day: int) -> int:
    """(month, day) -> its day 1..366 on the leap calendar: the days before the month + the day."""
    if not 1 <= month <= len(LAST_DAY):
        raise ValueError(f"calendar: month {month} is not 1..{len(LAST_DAY)}")
    return DAYS_BEFORE[month - 1] + day


def month_day(day: int) -> Tuple[int, int]:
    """1..366 -> (month, day), the inverse of `day_index` (day 60 is Feb 29)."""
    if not 1 <= day <= DAYS:
        raise ValueError(f"calendar: day {day} is not 1..{DAYS}")
    m = 1
    while day > LAST_DAY[m - 1]:
        day -= LAST_DAY[m - 1]
        m += 1
    return m, day


def as_json() -> dict:
    """The spec as data — what every generated copy is rendered from."""
    return {"months": list(MONTHS), "last_day": list(LAST_DAY), "days": DAYS,
            "days_before": list(DAYS_BEFORE), "leap_day": LEAP_DAY,
            "common_year_last_day": list(COMMON_YEAR_LAST_DAY), "weekdays": list(WEEKDAYS)}
