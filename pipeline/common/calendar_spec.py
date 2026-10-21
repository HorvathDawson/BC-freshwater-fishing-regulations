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
  READING DAY    (owner, 2026-10-20, P06/D31) The year keeps its 365 reading days: Feb 29 READS Feb 28's
                 regulations (day 59), on its OWN real weekday, for weekday and hours rules. Storage keeps
                 its 366 slots (Mar 1 = 61); slot 60 is never read. `reading_day` + `weekday_of` are the one
                 rule; the app, the reference page and the Python tap all call a generated copy of them.
  LEAP YEAR      `is_leap_year` (Gregorian: every 4th year, but not a century unless it divides by 400) says
                 whether a year HAS a Feb 29, so `month_last_day` knows a date that does not exist (Feb 29,
                 2027). It decides only validity, never the reading day; the app's tap reads its generated copy.
"""
from __future__ import annotations

import datetime
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


def reading_day(month: int, day: int) -> int:
    """The day whose regulations a date READS: `day_index`, except Feb 29, which reads Feb 28's (59)."""
    return day_index(2, 28) if (month, day) == (2, 29) else day_index(month, day)


def is_leap_year(year: int) -> bool:
    """Whether `year` has a Feb 29 (the Gregorian rule: 2028 and 2000 do, 2026 and 2100 do not)."""
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def month_last_day(year: int, month: int) -> int:
    """The last day of `month` in `year`: the leap calendar's in a leap year, else the common year's."""
    if not 1 <= month <= len(LAST_DAY):
        raise ValueError(f"calendar: month {month} is not 1..{len(LAST_DAY)}")
    return (LAST_DAY if is_leap_year(year) else COMMON_YEAR_LAST_DAY)[month - 1]


def weekday_of(year: int, month: int, day: int) -> str:
    """The date's REAL weekday (Feb 29 keeps its own: Fri 2036-02-29 is a Friday, not Feb 28's Thursday)."""
    return WEEKDAYS[datetime.date(year, month, day).weekday()]


#: The worked cases every generated copy is tested against: (year, month, day) -> reading day and
#: weekday. Fri 2036-02-29 is the Zymoetz below Limonite Creek's "Fridays, Saturdays, and Sundays" case.
CASE_DATES: Tuple[Tuple[int, int, int], ...] = ((2027, 2, 28), (2028, 2, 28), (2028, 2, 29), (2028, 3, 1),
                                                (2036, 2, 28), (2036, 2, 29), (2036, 3, 1))


def cases() -> list:
    return [{"date": f"{y:04d}-{m:02d}-{d:02d}", "reading_day": reading_day(m, d), "weekday": weekday_of(y, m, d)}
            for y, m, d in CASE_DATES]


#: The years every generated `isLeapYear` is tested against: the century rule both ways (1900, 2100 / 2000,
#: 2400), the book's year (2026), and the Feb 29 cases' years (2028, 2036).
LEAP_YEAR_CASE_YEARS: Tuple[int, ...] = (1900, 2000, 2026, 2028, 2036, 2100, 2400)


def leap_year_cases() -> list:
    return [{"year": y, "leap": is_leap_year(y)} for y in LEAP_YEAR_CASE_YEARS]


def as_json() -> dict:
    """The spec as data — what every generated copy is rendered from."""
    return {"months": list(MONTHS), "last_day": list(LAST_DAY), "days": DAYS,
            "days_before": list(DAYS_BEFORE), "leap_day": LEAP_DAY,
            "common_year_last_day": list(COMMON_YEAR_LAST_DAY), "weekdays": list(WEEKDAYS),
            "cases": cases()}
