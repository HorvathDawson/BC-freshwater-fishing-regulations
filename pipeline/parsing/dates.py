"""Date-window parsing for parsed rules — turn the verbatim `Rule.dates` strings into validated
structured windows.

The parser stores dates VERBATIM (exact substrings of rule_text, so the chain-of-custody holds);
this module derives the structured form deterministically from those strings. Because the structure is
COMPUTED, not authored by the model, it can't be hallucinated — and if a verbatim string doesn't parse
to a real calendar window (e.g. "Jun 31", a garbled month), `date_parse_errors` reports it and
`Rule` validation fails. That's the guard against "a hallucinated date that reads plausibly."

Handled: "Apr 1 - Jun 30", "Jan 1 to Dec 31", "Sept 1 – Oct 15", "April 1 through June 30", a lone
"Apr 1" (single day). Separators: -, –, —, "to", "through". Month names: full, 3-letter, and "Sept".
Cross-year windows ("Nov 1 - Mar 31") are stored as-is; the consumer handles wrap-around.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}
_MAX_DAY = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}

_SEP = re.compile(r"\s*(?:-|–|—|to|through|thru|until)\s*", re.IGNORECASE)
_DATE = re.compile(r"^([A-Za-z]+)\.?\s+(\d{1,2})$")


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


def parse_date_window(text: str) -> "DateWindow | None":
    """One date string -> a DateWindow, or None if it doesn't parse to a real calendar window."""
    if not text or not text.strip():
        return None
    parts = _SEP.split(text.strip(), maxsplit=1)
    if len(parts) == 1:
        one = _parse_one(parts[0])
        return DateWindow(*one, *one) if one else None
    a, b = _parse_one(parts[0]), _parse_one(parts[1])
    if a is None or b is None:
        return None
    return DateWindow(a[0], a[1], b[0], b[1])


def parse_date_windows(dates: list[str]) -> list[DateWindow]:
    """Parse each string; skip any that don't parse (use `date_parse_errors` to enforce)."""
    out: list[DateWindow] = []
    for d in dates:
        w = parse_date_window(d)
        if w is not None:
            out.append(w)
    return out


def date_parse_errors(dates: list[str]) -> list[str]:
    """Error strings for any date that doesn't resolve to a real calendar window — the hallucination
    guard used by Rule validation. Empty = all dates parse cleanly."""
    return [f"date '{d}' does not parse to a valid calendar window (bad month/day or format)"
            for d in dates if parse_date_window(d) is None]
