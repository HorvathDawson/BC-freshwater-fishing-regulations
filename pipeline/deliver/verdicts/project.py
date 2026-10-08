"""THE CLOSED PREDICATE over stored verdicts (DATAFLOW §1.3 G) — the status index's definition,
applied to the reader's stored answer instead of a fresh reader call:

    closed(key, reading) := for EVERY game fish, the verdict (origin none) holds a rule that
                            SPEAKS, is a FULL closure (`rule.closure_grade`), and is not partly
                            lifted.

Computed once, by the verdicts stage, into `reading.closed`; `check` recomputes it with this same
function from the stored frames (a check of the column, not a second definition).
"""
from __future__ import annotations

from typing import Callable, Iterable, Mapping, Optional

from pipeline.deliver import types as T

SPEAKS = T.code(T.RuleState.speaks)


def closes(rows: Iterable[tuple], grade: Mapping[int, Optional[str]]) -> bool:
    """Does a verdict (its `VerdictRow`-shaped rows: rule, state code, reason, by, lifters) close
    the water to its fish? `grade` is every rule's stored `closure_grade` by `RuleIx`."""
    return any(r[1] == SPEAKS and grade[r[0]] == "full" and not r[4] for r in rows)


def closed(verdict_of: Callable[[str], Iterable[tuple]], grade: Mapping[int, Optional[str]]) -> bool:
    """Closed on a reading: every game fish's origin-none verdict closes (`verdict_of(fish)`)."""
    return all(closes(verdict_of(f), grade) for f in T.GAME_FISH)


def beyond_base(rule: dict, via: str) -> bool:
    """Is this bound rule MORE than the base — a rule of a water table's row (`r<n>:` entries)?
    Everything a zone or provincial table writes (`z<n>:` / `zp:`) is the base, including the
    rules those tables write about named waters; a row reaching a tributary by the walk is the
    row's rule there too (`via` is accepted for symmetry and unused). A key is `own` when any
    bound rule is (`key_meta.own`: the status index's floor, the water's "own regulations")."""
    return not str(rule["entry"]).startswith("z")
