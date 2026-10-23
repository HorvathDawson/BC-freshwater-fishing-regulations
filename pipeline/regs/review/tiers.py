"""THE REVIEW TIERS (PLAN §3) — which pile an entry or a split point is reviewed in.

    S0     split points still to verify (an open auto-proposal, a note that says "Verify:", or a
           split mark gone stale)
    P1     closures with a risk factor (a cut, several waters, a flag, no match)
    Z      zone entries (the region chapters and the provincial pages)
    P2     plain closures
    P3     non-closure entries with a risk factor
    P4     plain entries
    sweep  pointer and empty rows

One function each: `entry_tier` for an entry, `split_needs_verify` for a split. The review screen's
progress and ETA (T134) count by them. What they can say from the entry alone is all they say:
the ambiguous-cut and unresolved-reach factors (from `rule_diagnostic.jsonl`) and the precheck
severities are T135's queue, which builds on these two functions rather than beside them.

A closure is `rules.closure_grade` (THE ONE CLOSURE PREDICATE), never re-derived here.
"""
from __future__ import annotations

import re

TIERS: tuple[str, ...] = ("S0", "P1", "Z", "P2", "P3", "P4", "sweep")

#: extent ops that CUT a water (the rest: whole, within, steelhead_waters)
CUT_OPS = frozenset({"between", "upstream_of", "downstream_of", "rest"})

_AUTO_PROPOSAL = re.compile(r"^\[auto-proposal ([A-Z])\]")


def flagged(entry: dict) -> bool:
    """A rule or licensing record carries a `review_reason` (or a rule an unresolved locator) — a
    reason present IS the flag; the model has no separate one."""
    return (any(r.get("review_reason") or r.get("unresolved_locators")
                for r in entry.get("rules") or [])
            or any(x.get("review_reason") for x in entry.get("licensing") or []))


def risks(entry: dict, *, binds: bool) -> list[str]:
    """The risk factors PLAN §3 names that the entry itself shows, in that order: `cut`,
    `several waters`, `flag`, `no match` (a water row whose `matched` names no item this build has
    — it binds nothing; `binds` says whether it does)."""
    out = []
    exts = [x for r in entry.get("rules") or [] for x in r.get("extents") or []
            if isinstance(x, dict)]
    if any(x.get("op") in CUT_OPS or x.get("splits") for x in exts):
        out.append("cut")
    if len(entry.get("matched") or []) > 1:
        out.append("several waters")
    if flagged(entry):
        out.append("flag")
    if not binds:
        out.append("no match")
    return out


def is_closure_entry(entry: dict) -> bool:
    from pipeline.deliver.bundle.rules import closure_grade
    return any(closure_grade(r) for r in entry.get("rules") or [])


def entry_tier(entry: dict, *, binds: bool) -> str:
    """The entry's tier. `binds`: whether its `matched` names an item of the served build."""
    if str(entry.get("entry_id", "")).startswith("z"):
        return "Z"
    if not entry.get("rules"):
        return "sweep"
    risky = bool(risks(entry, binds=binds))
    if is_closure_entry(entry):
        return "P1" if risky else "P2"
    return "P3" if risky else "P4"


def split_grade(split: dict) -> str | None:
    """The auto-proposal grade a split's `_note` OPENS with (`[auto-proposal L]`), or None. Only a
    leading tag counts: a later one is history."""
    m = _AUTO_PROPOSAL.match(str(split.get("_note") or ""))
    return m.group(1) if m else None


def split_needs_verify(split: dict, mark_status: str | None = None) -> bool:
    """Tier S0: an open auto-proposal, a note asking for a check ("Verify: …"), or a split mark
    that went STALE (`mark_status`, the split's mark state): a confirmed point whose anchor or kind
    changed since is a point to confirm again, never one the queue stops showing."""
    return (split_grade(split) is not None or "Verify:" in str(split.get("_note") or "")
            or mark_status == "stale")


#: S0 order: L grade first (PLAN §3), then M, H, then a "Verify:" note with no grade
GRADE_ORDER = {"L": 0, "M": 1, "H": 2, None: 3}
