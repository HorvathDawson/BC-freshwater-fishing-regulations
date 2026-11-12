"""typed — a scraped DFO row becomes a typed catalogue rule.

The DFO pages are a TABLE, not prose, so nothing here needs a model: every column is already
structured and the conversion is a pure function of the row. That is the whole reason this move
costs no credits, unlike the synopsis reparse it mirrors.

**This module owns the whole reading of the limits cell**, `decode()` included. The transcription
in `parse.py` hands over the columns as printed and reads nothing out of them: a decode split
across two modules is one wording fixed in two places, and it is how "Finfish closure" came to
mean nothing at all for ten rows.

One row can be several rules. `"2 per day, bait ban — FN0851"` is a retention limit AND a bait
restriction, and they are different types because they never compete — you obey both:

    "2 per day, bait ban"   ->   retention_limit(take=2) + bait_restriction(gear: bait ban)

THE BIT THAT MATTERS. `take=0` does not mean closed. `may_target` is what separates "do not fish
for this" from "fish for it and release it", and the catalogue refuses a `take=0` rule that does
not say which. DFO writes the distinction in the limits cell and `decode()` reads it:

    no_fishing     -> may_target=False    "No fishing for chinook", "Finfish closure"
    non_retention  -> may_target=True     "Non-retention", "Open for salmon catch and release"

Getting that backwards is the 605-rule defect `pipeline/regs/parsing/catalogue.py` exists to
prevent, so it is asserted here rather than inferred from `take`.

A size sub-limit is a SEPARATE rule pointing at its parent with `within`, which is the convention
the synopsis corpus already uses (`z2:daily_quotas.r2` is "1 over 50 cm" within "Trout/char: 4").
`"4 per day, only 2 over 50 cm"` is therefore two rules, not one rule with two numbers — a shape
that cannot say "2 over 50" and lose the 4.

`verbatim` is always a span of the row's own text so the entry's chain of custody holds:
`CatalogueEntry` requires every rule's verbatim to be a contiguous substring of `regs_verbatim`.
"""

from __future__ import annotations

import difflib
import re
from typing import List, Optional

from pipeline.regs.parsing.catalogue import (
    CatalogueRule,
    DateRange,
    Hours,
    LengthBand,
    Origin,
    Period,
    RuleType,
    When,
    parse_clock,
)
from pipeline.regs.dfo_salmon.dates import read_dates

#: DFO's species column, verbatim -> catalogue codes. Eight distinct values across 438 rules and
#: nine regions. Written out rather than resolved by phrase because the bare words DFO uses
#: ("Chinook") do not resolve against the species table on their own — it holds "Chinook Salmon".
SPECIES: dict[str, List[str]] = {
    "all": ["SA"],                       # a salmon page's "All" is all SALMON, never all fish
    "chinook": ["CH"],
    "coho": ["CO"],
    "sockeye": ["SK"],
    "pink": ["PK"],
    "chum": ["CM"],
    "sockeye, pink and chum": ["SK", "PK", "CM"],
    "sockeye, pink, chum": ["SK", "PK", "CM"],
    "eulachon": ["EU"],
}

#: The salmon codes. A row about any other fish (eulachon) is never a salmon rule.
SALMON_CODES = frozenset({"SA", "CH", "CO", "SK", "PK", "CM"})
#: the advisory's `review_reason` prefix that marks a row about another fish (the bundle shows it as a line)
OTHER_FISH = "another fish, not salmon: "
#: "All | No fishing for eulachon" (Region 6 B(ii)): the species cell says All, the row names ONE other fish. Read as
#: "All salmon", it closed chum on the lower Skeena (T229, owner 2026-10-28: eulachon is not a salmon closure — the row
#: is its own line).
_RE_NO_FISHING_FOR = re.compile(r"\bno\s+fishing\s+for\s+([a-z]+)\b", re.I)

#: a count, in digits or (archived pages, "only two over 50 cm") in words
_NUM = r"(\d+|one|two|three|four|five|six)"
_WORDS = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6}


def _num(s: str) -> int:
    return _WORDS[s.lower()] if s.lower() in _WORDS else int(s)


#: "N per day, only M over X cm" and its wordings. The sub-limit keeps its own count.
_RE_ONLY_OVER = re.compile(
    rf"only\s+{_NUM}\s+(?:of\s+which\s+may\s+be\s+)?(?:over|greater\s+than|more\s+than)\s+(\d+)\s*cm",
    re.I)
#: (T320, archived pages) "Daily limit of 4 of which 1 may be over 65 cm in length" — the count AFTER "of which"
_RE_OF_WHICH_N = re.compile(
    r"of\s+which\s+(\d+)\s+may\s+be\s+(?:over|more\s+than|greater\s+than)\s+(\d+)\s*cm", re.I)
#: (T320) "2 per day, 1 of which can be unmarked — FN1013": of the day's fish, up to N may be WILD (unmarked); the
#: rest hatchery-marked. A wild allowance inside the count, its own rule (`origin: wild`, `within` the parent).
_RE_UNMARKED = re.compile(r"(\d+)\s+of\s+which\s+(?:can|may)\s+be\s+unmarked", re.I)
#: (T320, archived pages) "Monthly Quota = 1 over 65 cm.", "Monthly limit=1 over 65 cm", "Monthly limit of 6 over 50 cm
#: from the South Thompson River" — a monthly sub-limit, its own rule (`period: monthly`, `within` the parent)
_RE_MONTHLY = re.compile(r"monthly\s+(?:quota|limit)\s*(?:=|of|is)?\s*(\d+)\s+over\s+(\d+)\s*cm", re.I)
#: (T320, archived pages) "Daily limit of 4 …" — the day's count written as a noun
_RE_DAILY_OF = re.compile(r"\bdaily\s+limit\s+of\s+(\d+)\b", re.I)
#: "…, 1 of which may be more than 77 cm. in length" — the same sub-limit, written as a clause.
_RE_WHICH_MAY = re.compile(
    r"(\d+)\s+of\s+which\s+may\s+be\s+(?:more\s+than|greater\s+than|over)\s+(\d+)\s*cm",
    re.I)
#: "none over 50 cm" — a sub-limit of zero, which is a release rule at that size, not a closure.
_RE_NONE_OVER = re.compile(r"none\s+over\s+(\d+)\s*cm", re.I)
#: "maximum size 35 cm", "80 cm or less" — an upper bound with no count beside it.
_RE_MAX_SIZE = re.compile(r"(?:maximum\s+size\s+(\d+)\s*cm|(\d+)\s*cm\s+or\s+less)", re.I)
#: "from 05:00 h until 22:00 h only"
_RE_TIMES = re.compile(r"from\s+(\d{1,2}:\d{2})\s*h?\s+(?:until|to)\s+(\d{1,2}:\d{2})\s*h?", re.I)
#: (T321) "one hour before sunrise to one hour after sunset" (Region 2, 2025 in-season rows): the same hours, off the sun
#: (`catalogue.parse_clock` reads each end as a solar `Clock`, a negative offset BEFORE the event)
_SOLAR = r"(?:(?:one|an|\d+)\s*(?:hours?|minutes?|min)\s+(?:before|after)\s+)?(?:sunrise|sunset)"
_RE_SOLAR_TIMES = re.compile(rf"(?:from\s+)?({_SOLAR})\s+(?:until|to)\s+({_SOLAR})", re.I)
#: "single barbless hook less than 15mm from point to shank"
_RE_GAP = re.compile(r"less\s+than\s+(\d+)\s*mm(?:\s+from\s+point\s+to\s+shank)?", re.I)
_RE_FLY = re.compile(r"fly[- ]fishing\s+only", re.I)
#: The in-season notice that set the row. Kept as provenance, never as identity — these turn over.
_RE_FN = re.compile(r"\bFN\s*(\d{3,4})\b", re.I)
#: A row DFO has not decided yet. 11 of them; they are a real state, not a parse failure.
_RE_TBD = re.compile(r"to\s+be\s+determined", re.I)
#: HOW THIS MODULE TYPES A ROW, as a number the typed feed carries (`feed.build` `typed_format`). Bump it when a row
#: types differently: the release refuses typed files of another number (`deliver.salmon.load_inputs`), so rows typed by
#: older code never ship. 2 (T671 D2): a row nobody could type keeps the species its cell names. 3 (T676): the date
#: cell is read by `dates.read_dates` (open-ended rows, `check` on an unreadable cell).
FORMAT = 3
#: (T671 D2) the advisory's `review_reason` prefixes the bundle reads: a row DFO has not set, and a row that is a gear
#: PERMISSION (it opens and closes no fish). Every other advisory that is not about another fish is a row nobody could
#: type: `deliver.salmon.unread_row` shows it as "check DFO" for the species its cell names, on the days it holds.
TBD = "DFO has not set this row yet"
GEAR_PERMISSION = "a gear permission"
#: (T676) a rule whose row's date cell did not read carries this as its `check` (the worker's `DATES_UNPARSED`)
DATES_UNPARSED = "dates unparsed — check DFO"

# --------------------------------------------------------------------------------------- #
# The decode — limits text -> what it says
#
# This used to live in `parse.py` as derived flags on the row, which put a reading of the rules
# column inside the module whose job is faithful transcription, and then dragged it through two
# more dataclasses to reach the one consumer that ever read it: this file. Nothing else in the
# repo branched on those flags — `churn` compares the raw text — so the decode belongs here, with
# the rest of the lookup, where one wording is fixed in one place.
#
# Every pattern below is wider than the flags it replaces, and each widening recovered rows that
# were typing to nothing at all.
# --------------------------------------------------------------------------------------- #

#: The count and the words DFO puts between it and "per day". An explicit vocabulary rather than
#: a wildcard, so "no more than 2 fish per day" cannot read as a limit of 2 by accident.
#: "4 hatchery marked ONLY per day" (4 rows) and "4 PINK per day" (1) stated a quota and carried
#: none, because the flag pattern allowed only "hatchery marked" in that gap.
_QUALIFIER = r"(?:(?:hatchery[- ]marked|only|chinook|coho|sockeye|pink|chum|salmon)\s+)*"
_RE_DAILY = re.compile(rf"\b(\d+)\s+{_QUALIFIER}per\s+day\b", re.I)
#: "No fishing", DFO's own typo for it, and the banner wording. A "Finfish closure" is a closure
#: of everything, and 10 rows of them decoded to nothing — an open-looking water the page closes.
_RE_NO_FISHING = re.compile(r"\bno\s+fish(?:ing|inf)\b|\bfinfish\s+closure\b"
                            r"|\bclosed\s+to\s+fishing\b|\bno\s+angling\b", re.I)   # the last two: archived pages (T320)
#: Release, written four ways. Only "Non-retention" was matched; "No retention of coho",
#: "No retention of salmon" and "Open for salmon catch and release" say the same thing and were
#: dropped. The distinction they carry is exactly the one `may_target` turns on.
_RE_NON_RETENTION = re.compile(r"\bnon[- ]retention\b|\bno\s+retention\s+of\b|"
                               r"\bcatch\s+and\s+release\b", re.I)
#: (T338) A CLOSURE OF A STRETCH, inside a limits cell: "4 per day, only 1 over 65 cm. No fishing for chinook from a point
#: 100 m above Fort Babine bridge to Nichyeskwa Creek." closes that stretch only — never the water the row is about. It is
#: not read as the cell's closure; the row keeps its limit and is flagged with the sentence (the app shows DFO's words).
_RE_PART_CLOSURE = re.compile(r"\bno\s+fishing\s+for\s+[a-z ,]+?\s+from\s+[^.]+?\s+to\s+[^.]+\.?", re.I)
_RE_HATCHERY = re.compile(r"hatchery[- ]marked", re.I)
_RE_BAIT_BAN = re.compile(r"\bbait\s+ban\b|\bno\s+natural\s+bait\b|\bnatural\s+bait\s+not\s+allowed\b", re.I)
#: (T320, archived pages) "Natural bait allowed — FN1038": a permission (a bait ban lifted), no rule type states it
_RE_BAIT_ALLOWED = re.compile(r"\bnatural\s+bait\s+allowed\b", re.I)
#: "Single, barbless hook in tidal and non-tidal portions of all streams" — the comma cost the flag.
_RE_BARBLESS = re.compile(r"\bsingle,?\s+barbless\s+hook\b", re.I)


def decode(limits: str) -> dict:
    """What the limits cell says, read once. The source text is never replaced by this."""
    text = limits or ""
    part = _RE_PART_CLOSURE.search(text)
    whole = _RE_PART_CLOSURE.sub(" ", text) if part else text      # what the cell says of the WHOLE water
    daily: Optional[int] = None
    if (m := _RE_DAILY.search(text) or _RE_DAILY_OF.search(text)):
        daily = int(m.group(1))
    elif _RE_NON_RETENTION.search(text) or _RE_NO_FISHING.search(whole):
        daily = 0
    return {
        "no_fishing": bool(_RE_NO_FISHING.search(whole)),
        "part_closure": part.group(0).strip() if part else None,
        "non_retention": bool(_RE_NON_RETENTION.search(text)),
        "hatchery_marked_only": bool(_RE_HATCHERY.search(text)),
        "bait_ban": bool(_RE_BAIT_BAN.search(text)),
        "single_barbless_hook": bool(_RE_BARBLESS.search(text)),
        "daily_limit": daily,
    }


# --------------------------------------------------------------------------------------- #
# The dates decode
#
# Moved here from `locations.py` for the same reason the flags left `parse.py`: a `Dates` cell is
# a rule's, not a locator's, and its five derived fields were carried through RuleRecord and read
# by nobody. Only `validate.py` ever called this, and it called it on the raw string.
#
# It earns its place here rather than merely sitting here. `CatalogueRule` does NOT check that a
# window parses — the retired prose `Rule` did, via `date_parse_errors` — so a rule can be built
# today with a window of "Smarch 40 to Bluneteen 99" and nothing objects. `to_rules` now runs this
# and sends an unreadable window to review, which restores the guard the catalogue dropped.
# --------------------------------------------------------------------------------------- #


#: "Apr 1 until further notice", "Until further notice", "May 8 2018 until further
#: notice" — an open-ended window, 35+ occurrences across the archives. It is a real
#: shape, not a parse failure: the season has a start and no announced end.
_RE_OPEN_ENDED = re.compile(r"\buntil\s+fu\w*\s+notice\b", re.I)
_RE_YEAR = re.compile(r"\b(19|20)\d{2}\b")


def _first_pass_parses(raw: str) -> bool:
    from pipeline.regs.dfo_salmon.dates import parse_date_window

    if _RE_OPEN_ENDED.search(raw):
        return True
    return parse_date_window(raw) is not None


_MONTHS = ["january", "february", "march", "april", "may", "june", "july",
           "august", "september", "october", "november", "december"]


def _repair_dates(raw: str) -> str:
    """Fix source typos that have exactly one possible reading. Nothing ambiguous.

    Observed in the archives: `"Aprl 1 to Jun 15"` (2022 Region 6) and
    `"Nov 01- to Dec 31"` (Region 5b, five versions). Both have one reading, so
    normalising them is safe — and `interpret_dates` records what it changed, so the
    repair is auditable rather than invisible.
    """
    import difflib

    out = raw
    # A stray separator glued to the day: "Nov 01- to Dec 31" -> "Nov 01 to Dec 31".
    out = re.sub(r"(\d)\s*[-–—]\s+(?=to\b)", r"\1 ", out, flags=re.I)
    # A trailing calendar year: "June 15 to July 14 2017". The window is the same
    # every season, so the year adds nothing and blocks the parse.
    out = re.sub(r"\s+(?:19|20)\d{2}\s*$", "", out)

    # A misspelled month, ONLY when one real month is a clear closest match.
    def fix_word(m):
        w = m.group(0)
        lw = w.lower()
        if any(lw == mo or mo.startswith(lw) for mo in _MONTHS):
            return w                                  # already valid or a prefix
        near = difflib.get_close_matches(lw, _MONTHS, n=2, cutoff=0.8)
        if len(near) == 1:
            return near[0][:3].capitalize()
        return w

    out = re.sub(r"\b[A-Za-z]{3,9}(?=\.?\s+\d)", fix_word, out)
    return out


def interpret_dates(text: str) -> dict:
    """Structured form of a `Dates` cell, alongside the verbatim string.

    Returns `{start, end, open_ended, parsed}`. `parsed` False means a human should
    look — it is how a source typo ("Aprl 1 to Jun 15", "Nov 01- to Dec 31") surfaces
    instead of silently becoming a window that is not what the page says.
    """
    from pipeline.regs.dfo_salmon.dates import parse_date_window

    raw = (text or "").strip()
    out = {"start": None, "end": None, "open_ended": False, "parsed": False,
           "repaired_from": None}
    if not raw:
        return out

    if not _first_pass_parses(raw):
        fixed = _repair_dates(raw)
        if fixed != raw and _first_pass_parses(fixed):
            out["repaired_from"] = raw
            raw = fixed

    if _RE_OPEN_ENDED.search(raw):
        out["open_ended"] = True
        head = _RE_YEAR.sub("", _RE_OPEN_ENDED.sub("", raw)).strip(" .,-–—")
        if head:
            w = parse_date_window(head)
            if w is not None:
                out["start"] = str(w).split(" - ")[0].split(" to ")[0].strip()
        out["parsed"] = True
        return out

    w = parse_date_window(raw)
    if w is not None:
        out["parsed"] = True
        text_w = str(w)
        for sep in (" - ", " to ", " – "):
            if sep in text_w:
                a, b = text_w.split(sep, 1)
                out["start"], out["end"] = a.strip(), b.strip()
                break
        else:
            out["start"] = out["end"] = text_w.strip()
    return out


def species_for(text: str) -> List[str]:
    """DFO's species cell -> catalogue codes. Empty when the cell names something unmapped."""
    key = (text or "").strip().lower()
    if key in SPECIES:
        return list(SPECIES[key])
    # (T320, archived pages) a list the table above does not spell: "Chinook, Sockeye, Pink, Chum", "Sockeye, Pink &
    # Chum" — every part a species name, else unmapped (never a guess)
    parts = [x for x in re.split(r"\s*(?:,|&|\band\b)\s*", key) if x]
    if len(parts) > 1 and all(SPECIES.get(x) and x != "all" for x in parts):
        return [c for x in parts for c in SPECIES[x]]
    return []


def _span(haystack: str, pattern: re.Pattern) -> Optional[str]:
    """The matched text itself, so `verbatim` quotes the row rather than paraphrasing it."""
    m = pattern.search(haystack or "")
    return m.group(0) if m else None


def _first(*values):
    for v in values:
        if v is not None:
            return v
    return None



def _when(cell, raw_dates: str, times) -> Optional[When]:
    """The row's season and hours as one `when` — the date cell as `dates.read_dates` read it (T676): its ranges, and
    `open_ended` where it said "until further notice"; a cell it cannot read is kept VERBATIM in `unparsed` rather than
    dropped (an unreadable season and no season are opposite facts); a blank cell says nothing (all year). The feeds
    worker builds the same (`typed.ts` `robustWhenOf`)."""
    dates, unparsed, open_ended = [], [], False
    if cell.kind == "ok":
        dates = [DateRange(from_month=a, from_day=b, to_month=c, to_day=d) for a, b, c, d in cell.ranges]
        open_ended = cell.open_ended
    elif cell.kind != "empty":
        unparsed = [raw_dates]                       # unparsed, or "to be determined" in the date cell
    hours = None
    if times is not None:
        a, b = parse_clock(times.group(1)), parse_clock(times.group(2))
        if a is None or b is None:
            raise ValueError(f"times {times.group(0)!r} do not parse")
        hours = Hours(start=a, end=b)
    if not dates and not unparsed and hours is None:
        return None
    return When(dates=dates, hours=hours, unparsed=unparsed, open_ended=open_ended)

def to_rules(rec: dict, rule_base: str, order: int) -> List[CatalogueRule]:
    """One scraped row -> the rules it states. `rule_base` is the entry's id stem.

    Returns [] only for a row that states nothing at all; every other row yields at least one
    rule, because `CatalogueEntry` refuses an entry whose rules say nothing.
    """
    gear = (rec.get("limits_gear") or "").strip()
    says = decode(gear)
    codes = species_for(rec.get("species") or "")

    # The season. An empty cell is a fact — "when no date is listed, the regulations apply ALL
    # YEAR" — so only a cell that says something unreadable is a problem. `windows` carries the
    # REPAIRED string when an unambiguous source typo had to be fixed to read it ("Aprl 1 to
    # Jun 15"), because a window nobody can parse is worth less than one we can say we corrected.
    # (T676, owner 2026-11-12) THE DATE CELL IS READ BY `dates.read_dates` — the feeds worker's reader, ported, so the
    # release and the live feed read one cell one way: "Aug 2 To Aug 31", "July 1 until Aug 8", day-month order,
    # ordinals, years, typos, lists. "<day> until further notice" RUNS FROM THAT DAY WITH NO END and does whatever the
    # row says (a closure closes, an opening opens): `when.open_ended` on the range from that day (`deliver.salmon`
    # `holds_kind`: before its start it speaks only where no dated row of its place does). A cell it still cannot read
    # KEEPS the row's typed rules, each with the season in `when.unparsed` and `check` (`DATES_UNPARSED`): the species
    # reads "check" every day — never an advisory that dropped the limit.
    raw_dates = (rec.get("dates") or "").strip()
    cell = read_dates(raw_dates)
    open_ended = cell.kind == "ok" and cell.open_ended
    unparsed_dates = cell.kind == "unparsed"
    fn = _RE_FN.search(gear)
    notice = f"FN{int(fn.group(1)):04d}" if fn else None     # "FN 679" and "FN0679" are one notice
    rid = f"{rule_base}.r{order}"
    out: List[CatalogueRule] = []
    # T321: a clause the wording registry does not hold — the row is unparsed (`wording.py`)
    from pipeline.regs.dfo_salmon.wording import reason as _wording_reason
    new_wording = _wording_reason(gear)

    def base(times=None, **kw) -> dict:
        d = dict(verbatim=gear or (rec.get("species") or "row"), notice=notice)
        w = _when(cell, raw_dates, times)
        if w is not None:
            d["when"] = w
        d.update(kw)
        if unparsed_dates:
            d["check"] = DATES_UNPARSED
        return d

    # --- a row DFO has not settled ----------------------------------------
    # "To be determined" is a published state, not a gap in the scrape, so it is stored as an
    # advisory that names itself for review rather than being dropped or guessed at.
    # (T671 D2) THE ROW'S SPECIES STAYS ON IT (`own_sp`): "Chum | To be determined" is unread for chum only — typed with
    # no species it read "check DFO" for all five (Courtenay / Puntledge), and with readable dates it answered for none
    # (Kamloops Lake's "Sockeye | Apr 1 to Mar 31 | To be determined" read closed). A cell naming no fish the table
    # knows keeps no species: the bundle then reads the row for every salmon, never for none.
    own_sp = {"species": codes} if codes and set(codes) <= SALMON_CODES else {}
    if _RE_TBD.search(gear) or cell.kind == "tbd":
        return [CatalogueRule(rule_id=rid, type=RuleType.advisory,
                              review_reason=f"{TBD} ('to be determined')",
                              **base(**own_sp))]

    # A row about ANOTHER FISH ("All | No fishing for eulachon"): not a salmon rule. An advisory the bundle shows as its
    # own line (`deliver.salmon._other_fish`; EU is not on the book's species list, so it cannot be a species code).
    if (m := _RE_NO_FISHING_FOR.search(gear)) and (named := SPECIES.get(m.group(1).lower())) \
            and not set(named) & SALMON_CODES:
        return [CatalogueRule(rule_id=rid, type=RuleType.advisory,
                              review_reason=f"{OTHER_FISH}{m.group(1).lower()}", **base())]

    if not codes and gear:
        return [CatalogueRule(rule_id=rid, type=RuleType.advisory,
                              review_reason=f"unmapped species cell {rec.get('species')!r}",
                              **base())]

    # --- the retention rule, and the bit that must be explicit -------------
    take: Optional[int] = None
    may_target: Optional[bool] = None
    zero_quota = False
    if says["no_fishing"]:
        take, may_target = 0, False          # may not fish for it
    elif says["non_retention"] and open_ended:
        # (T358, owner 2026-11-03) "Non-retention … until further notice" reads CLOSED: an open-ended row holds all
        # year, and the owner's rule for it is closed, not release-only (a change comes as a new row)
        take, may_target = 0, False
    elif says["non_retention"]:
        take, may_target = 0, True           # fish for it, release it
    elif says["daily_limit"] is not None:
        take = int(says["daily_limit"])
        if take == 0:
            # A BARE NUMERIC ZERO SAYS NEITHER. "0 per day" is a quota, and DFO states closures
            # in words when it means one ("No fishing for coho", "Finfish closure") — so this is
            # most likely a release rule. Most likely is not a reading, and defaulting it is the
            # 605-rule defect in its original shape: whichever way it is guessed, the guess is
            # invisible. It publishes the conservative way (closed) and is flagged, so the page
            # never reads as more open than the source proves.
            take, may_target, zero_quota = 0, False, True

    parent_id = ""
    if take is not None:
        times = _RE_TIMES.search(gear) or _RE_SOLAR_TIMES.search(gear)
        # The closure wording is the whole cell; a quota's cell may also carry gear clauses, so
        # quote just the count where we can find it.
        quoted = _span(gear, re.compile(r"\b\d+\s+(?:hatchery[- ]marked\s+)?per\s+day", re.I))
        parent_id = rid
        out.append(CatalogueRule(
            rule_id=rid, type=RuleType.retention_limit, species=codes, take=take,
            may_target=may_target,
            origin=Origin.hatchery if says["hatchery_marked_only"] else None,
            review_reason=("a bare '0 per day' says the quota is zero but not whether you may "
                           "fish at all; published closed, confirm against the page")
                          if zero_quota else
                          # T320: a number in the cell no reading accounts for — the row keeps its words, flagged,
                          # and the app shows them instead of a number (`quota.residue`); T321: a clause DFO never
                          # used before (`wording.reason`) — the same
                          (f"a closure of part of the water inside the limits cell: {says['part_closure']!r}"
                           if says.get("part_closure") else None) or _unread(gear) or new_wording,
            **base(times=times, verbatim=quoted or gear or (rec.get("species") or "row"))))

        # --- the size sub-limit, as its own rule pointing at the parent ----
        # ("none over 50 cm" is a SIZE rule: its band carries its own take 0 and the rule none —
        # `CatalogueRule` refuses a `take` no band uses, 2026-09-29.)
        sub = _first(_RE_ONLY_OVER.search(gear), _RE_WHICH_MAY.search(gear), _RE_OF_WHICH_N.search(gear))
        if sub:
            out.append(CatalogueRule(
                rule_id=f"{rid}b", type=RuleType.retention_limit, species=codes,
                take=_num(sub.group(1)), lengths=[LengthBand(min_cm=int(sub.group(2)))],
                within=parent_id,
                **base(verbatim=sub.group(0))))
        elif (none_over := _RE_NONE_OVER.search(gear)):
            out.append(CatalogueRule(
                rule_id=f"{rid}b", type=RuleType.retention_limit, species=codes,
                may_target=True,
                lengths=[LengthBand(min_cm=int(none_over.group(1)), take=0)], within=parent_id,
                **base(verbatim=none_over.group(0))))
        elif (mx := _RE_MAX_SIZE.search(gear)):
            cm = int(mx.group(1) or mx.group(2))
            out.append(CatalogueRule(
                rule_id=f"{rid}b", type=RuleType.retention_limit, species=codes,
                may_target=True, lengths=[LengthBand(min_cm=cm, take=0)],
                within=parent_id,
                **base(verbatim=mx.group(0))))

    # --- (T320) a wild allowance and a monthly sub-limit, each its own rule under the day's count ---------
    if parent_id and take:
        if (wild := _RE_UNMARKED.search(gear)):
            out.append(CatalogueRule(
                rule_id=f"{rid}w", type=RuleType.retention_limit, species=codes, take=int(wild.group(1)),
                origin=Origin.wild, within=parent_id, **base(verbatim=wild.group(0))))
        if (mo := _RE_MONTHLY.search(gear)):
            out.append(CatalogueRule(
                rule_id=f"{rid}m", type=RuleType.retention_limit, species=codes, take=int(mo.group(1)),
                period=Period.monthly, lengths=[LengthBand(min_cm=int(mo.group(2)))], within=parent_id,
                **base(verbatim=mo.group(0))))

    # T321: a row with new wording and no limit rule — its gear rules carry the flag
    gear_reason = new_wording if parent_id == "" else ""

    # --- gear. Never scoped by `species` — a hook rule binds everything you
    # --- may catch. What it IS scoped by is what you are fishing FOR.
    if says["bait_ban"]:
        quoted = _span(gear, re.compile(r"no\s+natural\s+bait[\w\s]*|bait\s+ban|natural\s+bait\s+not\s+allowed", re.I))
        out.append(CatalogueRule(
            rule_id=f"{rid}_bait", type=RuleType.bait_restriction,
            gear=[{"slot": "bait", "ban": ["any_bait"]}],
            when_targeting=codes, review_reason=gear_reason, **base(verbatim=quoted or gear)))

    if says["single_barbless_hook"]:
        gap = _RE_GAP.search(gear)
        quoted = _span(gear, re.compile(r"single,?\s+barbless\s+hook[\w\s,.]*", re.I))
        out.append(CatalogueRule(
            rule_id=f"{rid}_hook", type=RuleType.tackle_restriction,
            gear=[{"slot": "barb", "only": ["barbless"]}, {"slot": "points_per_hook", "max": 1}]
                 + ([{"slot": "hook_gap_mm", "max": int(gap.group(1))}] if gap else []),
            when_targeting=codes, review_reason=gear_reason, **base(verbatim=quoted or gear)))

    if _RE_FLY.search(gear):
        out.append(CatalogueRule(
            rule_id=f"{rid}_fly", type=RuleType.tackle_restriction,
            gear=[{"slot": "method", "only": ["fly_fishing"]}],
            when_targeting=codes, review_reason=gear_reason, **base(verbatim=_span(gear, _RE_FLY) or gear)))

    # A row that stated nothing the scrape could type still has to survive into the entry, or the
    # regulation silently disappears between the page and the app.
    if not out:
        out.append(CatalogueRule(
            rule_id=rid, type=RuleType.advisory,
            review_reason=(f"dates do not parse to a calendar window: {raw_dates!r}" if unparsed_dates else
                           _unread(gear) or new_wording or (f"{GEAR_PERMISSION} (natural bait allowed): no rule type states it"
                                             if _RE_BAIT_ALLOWED.search(gear) else
                                             "row states no limit, closure or gear rule the scrape could type")),
            **base(**own_sp)))
    return out


def _unread(text: str) -> str:
    """(T320) why a limits cell is not fully read ("" when every number in it is): `quota.unread_reason`."""
    from pipeline.regs.dfo_salmon.quota import unread_reason
    return unread_reason(text)


def row_text(rec: dict) -> str:
    """The row as one line, and the haystack every rule's `verbatim` must be a substring of."""
    parts = [(rec.get("species") or "").strip(), (rec.get("dates") or "").strip(),
             (rec.get("limits_gear") or "").strip()]
    return " | ".join(p for p in parts if p)
