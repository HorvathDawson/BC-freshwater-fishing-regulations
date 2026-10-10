"""The DFO feed's date windows — one `Dates` cell ("Apr 1 to Jun 30") to a `DateWindow`.

The DFO pages print a season as free text in a table cell; `typed.interpret_dates` runs this on
every one, and a cell that does not parse goes to review rather than becoming a window the page
never printed.

Handled: "Apr 1 - Jun 30", "Jan 1 to Dec 31", "Sept 1 – Oct 15", "April 1 through June 30", a lone
"Apr 1" (single day), the same-month shorthand "May 1-31". Separators: -, –, —, "to", "through".
Month names: full, 3-letter, and "Sept". A window whose end precedes its start ("Nov 1 - Mar 31")
crosses the new year. The synopsis catalogue has its own, stricter reader
(`catalogue.parse_date_range`), which refuses a lone day: a synopsis season is always a range.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from pipeline.common import calendar_spec as SPEC

_MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}
#: The last day of each month on the leap calendar (THE CALENDAR SPEC).
_MAX_DAY = dict(enumerate(SPEC.LAST_DAY, start=1))

_SEP = re.compile(r"\s*(?:-|–|—|to|through|thru|until)\s*", re.IGNORECASE)
_DATE = re.compile(r"^([A-Za-z]+)\.?\s+(\d{1,2})$")
_BARE_DAY = re.compile(r"^(\d{1,2})$")


@dataclass(frozen=True)
class DateWindow:
    """An inclusive calendar window, month/day only (regs recur yearly, so no year). A single date has
    start == end. A window whose end precedes its start (e.g. Nov 1 -> Mar 31) crosses the new year."""
    start_month: int
    start_day: int
    end_month: int
    end_day: int

    @property
    def crosses_year(self) -> bool:
        return (self.end_month, self.end_day) < (self.start_month, self.start_day)

    def __str__(self) -> str:
        names = {v: k[:3].capitalize() for k, v in _MONTHS.items() if len(k) == 3 or k == "sept"}
        s = f"{names.get(self.start_month, self.start_month)} {self.start_day}"
        e = f"{names.get(self.end_month, self.end_month)} {self.end_day}"
        return s if (self.start_month, self.start_day) == (self.end_month, self.end_day) else f"{s} - {e}"


def _parse_one(token: str) -> "tuple[int, int] | None":
    m = _DATE.match(token.strip())
    if not m:
        return None
    mon = _MONTHS.get(m.group(1).lower())
    day = int(m.group(2))
    if mon is None or not (1 <= day <= _MAX_DAY[mon]):
        return None
    return mon, day


def _parse_end(token: str, start_month: int) -> "tuple[int, int] | None":
    """The END of a window: a full "Month Day", or the common same-month shorthand where only the day
    is given ("May 1-31" -> end = May 31), inheriting the start month."""
    full = _parse_one(token)
    if full is not None:
        return full
    m = _BARE_DAY.match(token.strip())
    if m:
        day = int(m.group(1))
        if 1 <= day <= _MAX_DAY[start_month]:
            return start_month, day
    return None


def parse_date_window(text: str) -> "DateWindow | None":
    """One date string -> a DateWindow, or None if it doesn't parse to a real calendar window.
    Tolerates markdown bold the parser may copy from the synopsis ("Dec 1-**Aug 15**") and the
    same-month day shorthand ("May 1-31")."""
    if not text or not text.strip():
        return None
    text = text.replace("*", "").strip()                     # strip markdown bold (**Aug 15**)
    parts = _SEP.split(text, maxsplit=1)
    if len(parts) == 1:
        one = _parse_one(parts[0])
        return DateWindow(*one, *one) if one else None
    a = _parse_one(parts[0])
    if a is None:
        return None
    b = _parse_end(parts[1], a[0])                            # end may inherit the start month
    if b is None:
        return None
    return DateWindow(a[0], a[1], b[0], b[1])
