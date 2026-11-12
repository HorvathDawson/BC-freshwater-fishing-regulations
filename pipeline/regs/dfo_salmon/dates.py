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


# ── THE ROBUST READER (T676) ─────────────────────────────────────────────────────────────────────────────────────────
# The feeds worker types the LIVE feed's date cells with `app/workers/feeds/src/dfo/dates.ts` `readDates`, which reads
# shapes `parse_date_window` refuses ("<day> until further notice", "Aug 2 To Aug 31", "1 Oct to 31 Dec", "Oct 1st to
# Dec 31st", year suffixes, month typos, lists of ranges). This is that reader, ported line for line, so ONE reading of
# a date cell exists in both languages: `pipeline.tools.emit.dfo_vectors` writes what this returns for every recorded
# cell (plus the named shapes and a seeded fuzz) and the worker's `dfo.vectors.test.ts` must reproduce each one.
#
# NOT YET WIRED into `typed.to_rules`: the release still types its rows with `parse_date_window`. Reading "until
# further notice" (and the other shapes) the robust way in the RELEASE changes what an angler is shown and waits on the
# owner's nod (T676 item 1); until then this function is the pinned port, not a second reading in use.
#
# JavaScript regex semantics, kept: `\s` is JS's whitespace class (`_JS_WS`, not Python's `str.isspace`), `\b` / `\w` /
# `\d` are ASCII (`re.ASCII`), a non-global JS `replace` replaces the first match only (`count=1`), `$` is the end of the
# string (`\Z`).

_JS_WS = "[\t\n\v\f\r    -     　﻿]"
_RX = re.ASCII | re.IGNORECASE
_FULL = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october",
         "november", "december"]

_R_TBD = re.compile(rf"\bto{_JS_WS}+be{_JS_WS}+determined\b|\btbd\b", _RX)
_R_UFN = re.compile(rf"\b(?:until|till|til){_JS_WS}+fu\w*{_JS_WS}+noti\w*\b|\bufn\b", _RX)
_R_LEAD = re.compile(rf"^(?:(?:effective|from|starting|beginning|commencing|opens?|open{_JS_WS}+from|as{_JS_WS}+of|on)"
                     rf"{_JS_WS}+)+", _RX)
_R_YEAR = re.compile(rf",?{_JS_WS}*\b(?:19|20)\d{{2}}\b,?", _RX)
_R_ORD = re.compile(rf"(\d){_JS_WS}*(?:st|nd|rd|th)\b", _RX)
_R_SEP = re.compile(rf"{_JS_WS}*(?:[-‐‑‒–—―−]+{_JS_WS}*(?:to\b)?|\bto\b|\bthrough\b|\bthru\b|\buntil\b|\btill?\b"
                    rf"|\bup{_JS_WS}+to\b){_JS_WS}*", _RX)
_R_LIST = re.compile(rf"{_JS_WS}*(?:[,;&]|\band\b){_JS_WS}*", _RX)
_R_WS = re.compile(rf"{_JS_WS}+")
_R_EDGE = re.compile(rf"^(?:{_JS_WS}|[,;:])+|(?:{_JS_WS}|[,;:.])+\Z")
_R_TAIL_CONJ = re.compile(rf"{_JS_WS}+(?:or|and)\Z", _RX)
_R_DOT = re.compile(rf"\.(?={_JS_WS}|\Z)")
_R_MON_DAY = re.compile(rf"^([A-Za-z]+){_JS_WS}*(\d{{1,2}})\Z")
_R_DAY_MON = re.compile(rf"^(\d{{1,2}}){_JS_WS}*([A-Za-z]+)\Z")
_R_MON = re.compile(r"^([A-Za-z]+)\Z")
_R_DAY = re.compile(r"^(\d{1,2})\Z")
_R_LOWER = re.compile(r"^[a-z]+\Z")


def _js_trim(s: str) -> str:
    return re.sub(rf"^{_JS_WS}+|{_JS_WS}+\Z", "", s)


@dataclass(frozen=True)
class DateCell:
    """A DFO date cell read robustly: `empty` (blank: all year), `tbd` ("To be determined"), `ok` (`ranges`, each
    (from_month, from_day, to_month, to_day), may wrap the year end; `open_ended`: the cell said "until further
    notice", the LAST range runs on) or `unparsed` (`raw` — never guessed)."""
    kind: str
    ranges: tuple = ()
    open_ended: bool = False
    raw: str = ""

    def to_json(self) -> dict:
        """The worker's `DateCell` shape, key for key (the vectors compare it as JSON)."""
        if self.kind == "ok":
            return {"kind": "ok", "openEnded": self.open_ended,
                    "ranges": [{"from_month": a, "from_day": b, "to_month": c, "to_day": d}
                               for a, b, c, d in self.ranges]}
        return {"kind": "unparsed", "raw": self.raw} if self.kind == "unparsed" else {"kind": self.kind}


def month_of(word: str) -> "int | None":
    """A month word -> 1..12: any prefix of the full name of 3+ letters ("sept", "octob"), else ONE close match (typos)."""
    import difflib

    w = re.sub(r"\.\Z", "", word.lower())
    if len(w) < 3 or not _R_LOWER.match(w):
        return None
    for i, m in enumerate(_FULL):
        if m.startswith(w):
            return i + 1
    if len(w) < 4:
        return None                                   # three-letter typos are too ambiguous ("jum", "mat")
    near = difflib.get_close_matches(w, _FULL, 2, 0.75)
    if len(near) == 1:
        return _FULL.index(near[0]) + 1
    # abbreviated typo ("Spet", "Agu"): compare with the same-length prefixes
    pre = difflib.get_close_matches(w, [m[:len(w)] for m in _FULL], 2, 0.75)
    if len(pre) == 1:
        return next(i + 1 for i, m in enumerate(_FULL) if m[:len(w)] == pre[0])
    return None


def _point(text: str):
    """"Oct 1" / "1 Oct" / "Oct" (day None) / bare "15" (month None) -> (month, day), or None."""
    t = _R_WS.sub(" ", _R_DOT.sub("", _js_trim(text)))
    m = _R_MON_DAY.match(t)
    if m:
        mo = month_of(m.group(1))
        return (mo, int(m.group(2))) if mo else None
    m = _R_DAY_MON.match(t)
    if m:
        mo = month_of(m.group(2))
        return (mo, int(m.group(1))) if mo else None
    m = _R_MON.match(t)
    if m:
        mo = month_of(m.group(1))
        return (mo, None) if mo else None
    m = _R_DAY.match(t)
    return (None, int(m.group(1))) if m else None


def _valid(m: int, d: int) -> bool:
    return 1 <= m <= 12 and 1 <= d <= _MAX_DAY[m]


def _range(text: str, open_end: bool):
    parts = _R_SEP.split(text)
    if not parts or len(parts) > 2 or any(_js_trim(p) == "" for p in parts):
        return None                                   # "Mar to": dangling
    a = _point(parts[0])
    if a is None or a[0] is None:
        return None
    if len(parts) == 1:
        if open_end:
            d = a[1] if a[1] is not None else 1
            return (a[0], d, 12, 31) if _valid(a[0], d) else None
        if a[1] is None:
            return (a[0], 1, a[0], _MAX_DAY[a[0]])    # "August"
        return (a[0], a[1], a[0], a[1]) if _valid(a[0], a[1]) else None
    b = _point(parts[1])
    if b is None:
        return None
    fm, fd = a[0], (a[1] if a[1] is not None else 1)
    tm = b[0] if b[0] is not None else a[0]           # "Oct 1-15"
    td = b[1] if b[1] is not None else (_MAX_DAY[b[0]] if b[0] is not None else None)
    if td is None or (b[0] is None and a[1] is None):  # "Oct - 15": no
        return None
    if not _valid(fm, fd) or not _valid(tm, td):
        return None
    return (fm, fd, tm, td)


def read_dates(cell: "str | None") -> DateCell:
    """The DFO date cell -> its reading; `dates.ts` `readDates`, line for line (see the section header). Pure."""
    raw = _js_trim(_R_WS.sub(" ", (cell or "").replace("*", "")))
    if not raw:
        return DateCell("empty")
    if _R_TBD.search(raw):
        return DateCell("tbd")
    t, open_ended = raw, False
    if _R_UFN.search(t):
        open_ended = True
        t = _js_trim(_R_UFN.sub(" ", t, count=1))
    t = _R_ORD.sub(r"\1", _R_YEAR.sub(" ", t))
    t = _js_trim(_R_WS.sub(" ", t))
    t = _R_TAIL_CONJ.sub("", _R_EDGE.sub("", _R_LEAD.sub("", t, count=1)), count=1)
    if not t:
        # "Until further notice" alone: in force now, with no start printed
        return DateCell("ok", ((1, 1, 12, 31),), True) if open_ended else DateCell("unparsed", raw=raw)
    one = _range(t, open_ended)                      # one range first ("Oct 1 - 15" is not a list)
    if one:
        return DateCell("ok", (one,), open_ended)
    items = [p for p in _R_LIST.split(t) if _js_trim(p) != ""]
    if len(items) < 2:
        return DateCell("unparsed", raw=raw)
    ranges = []
    for i, item in enumerate(items):
        r = _range(item, open_ended and i == len(items) - 1)
        if not r:
            return DateCell("unparsed", raw=raw)
        ranges.append(r)
    return DateCell("ok", tuple(ranges), open_ended)
