"""THE REVIEW TIERS (PLAN §3) — which pile an ENTRY is reviewed in, and THE ORDER of the one review
queue (T135, PLAN §4 phase 4).

    P1     closures with a risk factor
    Z      zone entries (the region chapters and the provincial pages)
    P2     plain closures
    P3     non-closure entries with a risk factor
    P4     plain entries
    sweep  pointer and empty rows

The risk factors (`RISKS`, in the order PLAN §3 says P1 starts with): `no match` (a water row whose
`matched` names no item this build has — it binds nothing), `unresolved` (the reach run could not
bind a rule), `undrawn part` (a rule names a part of its water nothing draws), `ambiguous cut` (a
cut matched several places on its line and the builder took one, ruling E16), then `cut`, `several
waters`, `flag` (a parser review_reason or an unresolved locator). `unresolved` and `ambiguous cut`
come from the reach run (`runfacts.RunFacts`); without a run they are not known and not claimed.

ENTRIES ONLY (T184, owner 2026-10-24). A split point is no review target of its own: it is
checked BY PROXY, on the map of every entry whose rules name it (each split drawn and labelled with
its name), and a wrong one is a "Place: wrong" mark on such an entry. What is left of the old S0 tier
is a pre-check: a rule binding through a split still to verify (`split_needs_verify`: an open
auto-proposal, a "Verify:" note, or an old split mark gone stale) says so (`precheck`
`split_to_verify`), so the reviewer looks at that cut on the map.

One function each: `entry_tier` for an entry, `queue_rank` for its place in the one queue,
`split_needs_verify` / `splits_to_verify` for the splits a pre-check names. The review screen's
progress, ETA and queue count by them.

A closure is `rules.closure_grade` (THE ONE CLOSURE PREDICATE), never re-derived here.
"""
from __future__ import annotations

from pipeline.common.entry_id import is_table
import re

TIERS: tuple[str, ...] = ("P1", "Z", "P2", "P3", "P4", "sweep")

#: THE RISK FACTORS, most urgent first (PLAN §3: P1 "starts with the rows that bind nothing, the
#: unresolved closures, the closures with undrawn parts and the closures with ambiguous cuts")
RISKS: tuple[str, ...] = ("no match", "unresolved", "undrawn part", "ambiguous cut", "cut",
                          "several waters", "flag")

#: extent ops that CUT a water (the rest: whole, within, steelhead_waters)
CUT_OPS = frozenset({"between", "upstream_of", "downstream_of", "rest"})

_AUTO_PROPOSAL = re.compile(r"^\[auto-proposal ([A-Z])\]")


def flagged(entry: dict) -> bool:
    """A rule or licensing record carries a `review_reason` (or a rule an unresolved locator) — a
    reason present IS the flag; the model has no separate one."""
    return (any(r.get("review_reason") or r.get("unresolved_locators")
                for r in entry.get("rules") or [])
            or any(x.get("review_reason") for x in entry.get("licensing") or []))


def risks(entry: dict, *, binds: bool, run=None) -> list[str]:
    """The risk factors (`RISKS`, in that order) the entry shows. `binds`: whether its `matched`
    names an item of the served build. `run`: the reach run's facts (`runfacts.RunFacts`) — the
    `unresolved` and `ambiguous cut` factors are read from it; with no run they are not claimed."""
    rules = entry.get("rules") or []
    exts = [x for r in rules for x in r.get("extents") or [] if isinstance(x, dict)]
    eid = str(entry.get("entry_id", ""))
    got = {
        # a zone entry names no water BY DEFINITION: binding no item is not a finding about it
        "no match": not binds and not is_table(eid),
        "unresolved": run is not None and bool(run.unresolved_of(eid)),
        "undrawn part": any(r.get("undrawn_part") for r in rules),
        # a cut decided by hand (`cut_pick`) is a decision, not a risk (`runfacts.is_curated_pick`)
        "ambiguous cut": run is not None and bool(run.open_ambiguous_of(eid)),
        "cut": any(x.get("op") in CUT_OPS or x.get("splits") for x in exts),
        "several waters": len(entry.get("matched") or []) > 1,
        "flag": flagged(entry),
    }
    return [k for k in RISKS if got[k]]


def is_closure_entry(entry: dict) -> bool:
    from pipeline.deliver.bundle.rules import closure_grade
    return any(closure_grade(r) for r in entry.get("rules") or [])


def entry_tier(entry: dict, *, binds: bool, run=None) -> str:
    """The entry's tier. `binds`: whether its `matched` names an item of the served build; `run`:
    the reach run's facts (`risks`)."""
    if is_table(entry.get("entry_id", "")):
        return "Z"
    if not entry.get("rules"):
        return "sweep"
    risky = bool(risks(entry, binds=binds, run=run))
    if is_closure_entry(entry):
        return "P1" if risky else "P2"
    return "P3" if risky else "P4"


def queue_rank(entry: dict, *, binds: bool, run=None, severe: int = 0) -> tuple[int, int, int]:
    """THE ENTRY'S PLACE IN THE ONE REVIEW QUEUE (PLAN §3, §4 phase 4), to sort by before the
    book's order: its tier (`TIERS`), then its most urgent risk factor (`RISKS`; none sorts last),
    then the more severity-A pre-checks (`precheck`, `severe`) the earlier."""
    tier = entry_tier(entry, binds=binds, run=run)
    got = risks(entry, binds=binds, run=run)
    return (TIERS.index(tier), RISKS.index(got[0]) if got else len(RISKS), -severe)


def split_grade(split: dict) -> str | None:
    """The auto-proposal grade a split's `_note` OPENS with (`[auto-proposal L]`), or None. Only a
    leading tag counts: a later one is history."""
    m = _AUTO_PROPOSAL.match(str(split.get("_note") or ""))
    return m.group(1) if m else None


def split_needs_verify(split: dict, mark_status: str | None = None) -> bool:
    """A split point still to verify: one flagged `"verify": true`, or an old split mark that went
    STALE (`mark_status`; split marks are history now, T184, but one that no longer holds is not a
    confirmation). Every split in splits.json was placed by hand during curation (owner, 2026-10-26):
    its `_note` — "[auto-proposal L] … Verify: …" included — is history, never an open question.
    How sure a location is lives in `confidence` (T207), not here."""
    return split.get("verify") is True or mark_status == "stale"


def splits_to_verify(splits: dict, mark_status: dict | None = None) -> frozenset[str]:
    """THE SPLIT POINTS STILL TO VERIFY in a splits.json: `split_needs_verify` of each with its mark
    status now (`mark_status`: split id -> status, from `marks.split_states`). What a rule binding
    through one is pre-checked for (`precheck` `split_to_verify`) — the review app and the
    pre-check CLI both ask here."""
    mark_status = mark_status or {}
    return frozenset(sp["id"] for wb in splits.get("waterbodies", []) for sp in wb.get("splits", [])
                     if sp.get("id") and split_needs_verify(sp, mark_status.get(sp["id"])))
