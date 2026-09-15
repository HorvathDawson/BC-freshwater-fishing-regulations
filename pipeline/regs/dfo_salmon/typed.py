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

    "2 per day, bait ban"   ->   retention_limit(take=2) + bait_restriction(allowed=False)

THE BIT THAT MATTERS. `take=0` does not mean closed. `may_target` is what separates "do not fish
for this" from "fish for it and release it", and the catalogue refuses a `take=0` rule that does
not say which. DFO writes the distinction in the limits cell and `decode()` reads it:

    no_fishing     -> may_target=False    "No fishing for chinook", "Finfish closure"
    non_retention  -> may_target=True     "Non-retention", "Open for salmon catch and release"

Getting that backwards is the 605-rule defect `pipeline/regs/parsing/catalogue.py` exists to
prevent, so it is asserted here rather than inferred from `take`.

A size sub-limit is a SEPARATE rule pointing at its parent with `within`, which is the convention
the synopsis corpus already uses (`z2:trout_char_quota.r2` is "1 over 50 cm" within "Trout/char: 4").
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
    Bait,
    CatalogueRule,
    Lure,
    Origin,
    RuleType,
)

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

#: "N per day, only M over X cm" and its wordings. The sub-limit keeps its own count.
_RE_ONLY_OVER = re.compile(
    r"only\s+(\d+)\s+(?:of\s+which\s+may\s+be\s+)?(?:over|greater\s+than|more\s+than)\s+(\d+)\s*cm",
    re.I)
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
#: "single barbless hook less than 15mm from point to shank"
_RE_GAP = re.compile(r"less\s+than\s+(\d+)\s*mm\s+from\s+point\s+to\s+shank", re.I)
_RE_FLY = re.compile(r"fly[- ]fishing\s+only", re.I)
#: The in-season notice that set the row. Kept as provenance, never as identity — these turn over.
_RE_FN = re.compile(r"\bFN\s*(\d{3,4})\b", re.I)
#: A row DFO has not decided yet. 11 of them; they are a real state, not a parse failure.
_RE_TBD = re.compile(r"to\s+be\s+determined", re.I)

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
_RE_NO_FISHING = re.compile(r"\bno\s+fish(?:ing|inf)\b|\bfinfish\s+closure\b", re.I)
#: Release, written four ways. Only "Non-retention" was matched; "No retention of coho",
#: "No retention of salmon" and "Open for salmon catch and release" say the same thing and were
#: dropped. The distinction they carry is exactly the one `may_target` turns on.
_RE_NON_RETENTION = re.compile(r"\bnon[- ]retention\b|\bno\s+retention\s+of\b|"
                               r"\bcatch\s+and\s+release\b", re.I)
_RE_HATCHERY = re.compile(r"hatchery[- ]marked", re.I)
_RE_BAIT_BAN = re.compile(r"\bbait\s+ban\b|\bno\s+natural\s+bait\b", re.I)
#: "Single, barbless hook in tidal and non-tidal portions of all streams" — the comma cost the flag.
_RE_BARBLESS = re.compile(r"\bsingle,?\s+barbless\s+hook\b", re.I)


def decode(limits: str) -> dict:
    """What the limits cell says, read once. The source text is never replaced by this."""
    text = limits or ""
    daily: Optional[int] = None
    if (m := _RE_DAILY.search(text)):
        daily = int(m.group(1))
    elif _RE_NON_RETENTION.search(text) or _RE_NO_FISHING.search(text):
        daily = 0
    return {
        "no_fishing": bool(_RE_NO_FISHING.search(text)),
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
# today with `windows=["Smarch 40 to Bluneteen 99"]` and nothing objects. `to_rules` now runs this
# and sends an unreadable window to review, which restores the guard the catalogue dropped.
# --------------------------------------------------------------------------------------- #


#: "Apr 1 until further notice", "Until further notice", "May 8 2018 until further
#: notice" — an open-ended window, 35+ occurrences across the archives. It is a real
#: shape, not a parse failure: the season has a start and no announced end.
_RE_OPEN_ENDED = re.compile(r"\buntil\s+fu\w*\s+notice\b", re.I)
_RE_YEAR = re.compile(r"\b(19|20)\d{2}\b")


def _first_pass_parses(raw: str) -> bool:
    from pipeline.regs.parsing.dates import parse_date_window

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
    from pipeline.regs.parsing.dates import parse_date_window

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
    return list(SPECIES.get((text or "").strip().lower(), []))


def _span(haystack: str, pattern: re.Pattern) -> Optional[str]:
    """The matched text itself, so `verbatim` quotes the row rather than paraphrasing it."""
    m = pattern.search(haystack or "")
    return m.group(0) if m else None


def _first(*values):
    for v in values:
        if v is not None:
            return v
    return None


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
    raw_dates = (rec.get("dates") or "").strip()
    when = interpret_dates(raw_dates)
    if not raw_dates:
        windows = []
    elif when["repaired_from"]:
        windows = [_repair_dates(raw_dates)]
    else:
        windows = [raw_dates]
    unreadable = bool(raw_dates) and not when["parsed"]
    fn = _RE_FN.search(gear)
    reason = f"FN{fn.group(1)}" if fn else ""
    rid = f"{rule_base}.r{order}"
    out: List[CatalogueRule] = []

    def base(**kw) -> dict:
        d = dict(verbatim=gear or (rec.get("species") or "row"), windows=list(windows),
                 reason=reason)
        d.update(kw)
        return d

    # --- a row DFO has not settled ----------------------------------------
    # "To be determined" is a published state, not a gap in the scrape, so it is stored as an
    # advisory that names itself for review rather than being dropped or guessed at.
    if _RE_TBD.search(gear):
        return [CatalogueRule(rule_id=rid, type=RuleType.advisory, needs_review=True,
                              review_reason="DFO has not set this row yet ('to be determined')",
                              **base())]

    # A season nobody can read must not publish as though it had none — an unparsed window and an
    # absent one are opposite facts, and `CatalogueRule` does not tell them apart on its own.
    if unreadable:
        return [CatalogueRule(rule_id=rid, type=RuleType.advisory, needs_review=True,
                              review_reason=f"dates do not parse to a calendar window: "
                                            f"{raw_dates!r}",
                              **base())]

    if not codes and gear:
        return [CatalogueRule(rule_id=rid, type=RuleType.advisory, needs_review=True,
                              review_reason=f"unmapped species cell {rec.get('species')!r}",
                              **base())]

    # --- the retention rule, and the bit that must be explicit -------------
    take: Optional[int] = None
    may_target: Optional[bool] = None
    zero_quota = False
    if says["no_fishing"]:
        take, may_target = 0, False          # may not fish for it
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
        times = _RE_TIMES.search(gear)
        # The closure wording is the whole cell; a quota's cell may also carry gear clauses, so
        # quote just the count where we can find it.
        quoted = _span(gear, re.compile(r"\b\d+\s+(?:hatchery[- ]marked\s+)?per\s+day", re.I))
        parent_id = rid
        out.append(CatalogueRule(
            rule_id=rid, type=RuleType.retention_limit, species=codes, take=take,
            may_target=may_target,
            origin=Origin.hatchery if says["hatchery_marked_only"] else None,
            from_time=times.group(1) if times else None,
            to_time=times.group(2) if times else None,
            needs_review=zero_quota,
            review_reason=("a bare '0 per day' says the quota is zero but not whether you may "
                           "fish at all; published closed, confirm against the page")
                          if zero_quota else "",
            **base(verbatim=quoted or gear or (rec.get("species") or "row"))))

        # --- the size sub-limit, as its own rule pointing at the parent ----
        sub = _first(_RE_ONLY_OVER.search(gear), _RE_WHICH_MAY.search(gear))
        if sub:
            out.append(CatalogueRule(
                rule_id=f"{rid}b", type=RuleType.retention_limit, species=codes,
                take=int(sub.group(1)), over_cm=int(sub.group(2)), within=parent_id,
                **base(verbatim=sub.group(0))))
        elif (none_over := _RE_NONE_OVER.search(gear)):
            out.append(CatalogueRule(
                rule_id=f"{rid}b", type=RuleType.retention_limit, species=codes,
                take=0, may_target=True, over_cm=int(none_over.group(1)), within=parent_id,
                **base(verbatim=none_over.group(0))))
        elif (mx := _RE_MAX_SIZE.search(gear)):
            cm = int(mx.group(1) or mx.group(2))
            out.append(CatalogueRule(
                rule_id=f"{rid}b", type=RuleType.retention_limit, species=codes,
                take=0, may_target=True, over_cm=cm, within=parent_id,
                **base(verbatim=mx.group(0))))

    # --- gear. Never scoped by `species` — a hook rule binds everything you
    # --- may catch. What it IS scoped by is what you are fishing FOR.
    if says["bait_ban"]:
        quoted = _span(gear, re.compile(r"no\s+natural\s+bait[\w\s]*|bait\s+ban", re.I))
        out.append(CatalogueRule(
            rule_id=f"{rid}_bait", type=RuleType.bait_restriction, bait=Bait.any, allowed=False,
            when_targeting=codes, **base(verbatim=quoted or gear)))

    if says["single_barbless_hook"]:
        gap = _RE_GAP.search(gear)
        quoted = _span(gear, re.compile(r"single,?\s+barbless\s+hook[\w\s,.]*", re.I))
        out.append(CatalogueRule(
            rule_id=f"{rid}_hook", type=RuleType.tackle_restriction, barbless=True, hook_count=1,
            max_gap_mm=int(gap.group(1)) if gap else None,
            when_targeting=codes, **base(verbatim=quoted or gear)))

    if _RE_FLY.search(gear):
        out.append(CatalogueRule(
            rule_id=f"{rid}_fly", type=RuleType.tackle_restriction, lure=Lure.fly_fishing,
            when_targeting=codes, **base(verbatim=_span(gear, _RE_FLY) or gear)))

    # A row that stated nothing the scrape could type still has to survive into the entry, or the
    # regulation silently disappears between the page and the app.
    if not out:
        out.append(CatalogueRule(
            rule_id=rid, type=RuleType.advisory, needs_review=True,
            review_reason="row states no limit, closure or gear rule the scrape could type",
            **base()))
    return out


def row_text(rec: dict) -> str:
    """The row as one line, and the haystack every rule's `verbatim` must be a substring of."""
    parts = [(rec.get("species") or "").strip(), (rec.get("dates") or "").strip(),
             (rec.get("limits_gear") or "").strip()]
    return " | ".join(p for p in parts if p)
