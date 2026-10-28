"""THE ENTRY'S SEVERITY (owner 2026-10-26) — whether there is a REAL issue to review, in one word.

The queue showed every risk factor and pre-check count as a badge ("cut", "several waters",
"3 pre-checks"), so an entry with nothing wrong looked as busy as one that binds nothing. One chip
now says how much the entry needs a reviewer, and its reasons sit on the chip's hover:

    high    a real issue: the parser is unsure (`review_reason`), a missing split (an undrawn part
            that names a place, a locator nothing draws), unresolved / unplaced, an ambiguous cut
            not yet decided, a rule or row binding nothing, a failed gate, a curated cut no rule
            uses (owner 2026-10-26: missing splits and unused curated cuts are HIGH)
    medium  a cut / extent-shape risk that is real: a split still to verify, a quote not in its row
    low     facts and hints only: it cuts its water, covers several waters, a cut decided by hand,
            unquoted row text, an "on parts" note
    none    nothing at all

The queue orders by it first (`rank`), then by `tiers.queue_rank`. It is read from the pre-check
findings (`precheck.prechecks`, THE ONE TABLE of what is checked), the tier risk factors and the
count of unused curated cuts — never re-derived from the entry.
"""
from __future__ import annotations

from typing import Iterable, Sequence

LEVELS: tuple[str, ...] = ("high", "medium", "low", "none", "later")

#: LAKE SPLIT LATER (owner 2026-10-28): a LAKE's undrawn part / unresolved place is a lake-area
#: closure that needs a lake split, parse-wise fine, and lake splits are deferred (the regulations pane
#: shows "Outlet area only (see map N)", T208). An entry whose ONLY real issue is that sits in the
#: lowest level, "later" (below "none"), hidden by default. Any other high/medium reason keeps it.
LAKE_LATER_CODES = frozenset({"undrawn_part", "unresolved_locator", "unresolved", "unplaced_pieces"})

#: pre-check code -> (level, the short reason on the chip's hover). A code missing here is HIGH:
#: a new check is a real issue until someone says otherwise.
CODE_LEVEL: dict[str, tuple[str, str]] = {
    "gate": ("high", "fails the gate"),
    "binds_nothing": ("high", "binds nothing"),
    "unresolved": ("high", "unresolved rule"),
    "unplaced_pieces": ("high", "unplaced pieces"),
    "ambiguous_cut": ("high", "ambiguous cut"),
    "parser_uncertain": ("high", "parser-uncertain"),
    "unresolved_locator": ("high", "missing split"),     # names a place nothing draws
    "undrawn_part": ("high", "missing split"),           # a named part nothing draws
    "split_to_verify": ("medium", "split to verify"),
    "quote_not_in_row": ("medium", "quote not in its row"),
    "cut_picked": ("low", "cut decided by hand"),
    "unquoted_text": ("low", "unquoted row text"),
    "unidentified_part": ("low", "\"on parts\" note"),   # T207: a note, nothing to draw
}

#: tier risk factors that are only facts about the entry (the real ones come as pre-checks)
RISK_LEVEL: dict[str, tuple[str, str]] = {
    "cut": ("low", "cuts its water"),
    "several waters": ("low", "several waters"),
}

UNUSED_SPLITS = ("high", "unused curated cut")


def entry_severity(findings: Iterable[dict], *, risks: Sequence[str] = (),
                   unused_splits: int = 0, lake: bool = False) -> dict:
    """{"level": one of `LEVELS`, "reasons": [str, ...]} — the reasons most severe first, each once
    with its count ("parser-uncertain ×2: x.r1, x.r3").

    `lake`: the entry's water is a lake. Then findings in `LAKE_LATER_CODES` are set aside; when
    that leaves nothing high or medium (and at least one was set aside) the level is "later"."""
    findings = list(findings)
    if lake:
        deferred = [f for f in findings if f["code"] in LAKE_LATER_CODES]
        if deferred:
            rest = entry_severity([f for f in findings if f not in deferred], risks=risks,
                                  unused_splits=unused_splits)
            if rest["level"] in ("low", "none"):
                n = len(deferred)
                return {"level": "later", "reasons": [
                    f"later: lake split{f' ×{n}' if n > 1 else ''} ("
                    + ", ".join(sorted({str(f.get('rule_id') or '') for f in deferred} - {''})) + ")"]
                    + rest["reasons"]}
    got: dict[tuple[str, str], list[str]] = {}
    for f in findings:
        lv, why = CODE_LEVEL.get(f["code"], ("high", f["code"]))
        got.setdefault((lv, why), []).append(str(f.get("rule_id") or ""))
    for k in risks:
        if k in RISK_LEVEL:
            got.setdefault(RISK_LEVEL[k], []).append("")
    if unused_splits:
        got.setdefault(UNUSED_SPLITS, []).extend([""] * unused_splits)
    if not got:
        return {"level": "none", "reasons": []}
    keys = sorted(got, key=lambda k: (LEVELS.index(k[0]), list(got).index(k)))
    reasons = []
    for lv, why in keys:
        ids = sorted({i for i in got[(lv, why)] if i})
        n = len(got[(lv, why)])
        reasons.append(f"{lv}: {why}" + (f" ×{n}" if n > 1 else "")
                       + (f" ({', '.join(ids)})" if ids else ""))
    return {"level": keys[0][0], "reasons": reasons}


def rank(level: str) -> int:
    """The level's place in the queue: high first, none last."""
    return LEVELS.index(level)


def matches(level: str, wanted: str | None) -> bool:
    """The queue's severity filter: a level, or `issues` (high and medium); None/"" is every entry."""
    if wanted == "all":
        return True
    if not wanted:
        return level != "later"          # lake split later is hidden unless asked for
    if wanted == "issues":
        return level in ("high", "medium")
    return level == wanted


FILTERS: tuple[str, ...] = ("issues", *LEVELS, "all")
