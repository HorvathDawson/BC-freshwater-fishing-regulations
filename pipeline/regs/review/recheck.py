"""CHECK AGAIN (T607, owner 2026-11-10: "can you flag it so it doesn't look done anymore?") — a small curated list of
places the owner asked to look at again, without touching a mark.

    data/curated/review/recheck.json   {"recheck": [{scope, target, reason, by, date, mark}]}

A listed place is NOT DONE in the checklist: it reads "Check again: <reason>", sorts with the to-do places and is not
hidden with the done ones. Its mark STAYS IN FORCE for the build until he saves again (nothing here changes what
binds). An entry is cleared BY ITSELF: `mark` is the digest of the mark that was in force when the place was listed
(`mark_digest`); once the mark in force for the target is any other mark — he saved again — the entry is inert
(`active` leaves it out). The marks store is never edited; entries are added to the file on the owner's word only.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterable, Mapping, Optional


def path() -> Path:
    """beside the marks store (moved with the whole tree by `CURATED_ROOT`)"""
    from pipeline.common.curated import CURATED
    return Path(CURATED.review.marks).parent / "recheck.json"


def mark_digest(mark) -> str:
    """which mark an entry was made on: 12 hex of the mark's own stored line"""
    from pipeline.regs.review import marks as M
    return hashlib.sha256(M.line(mark).encode("utf-8")).hexdigest()[:12]


def load(p: Optional[Path] = None) -> list[dict]:
    """the entries as written; [] when there is no file. A malformed entry is refused (ValueError), never skipped."""
    p = p or path()
    if not p.exists():
        return []
    rows = json.loads(p.read_text(encoding="utf-8")).get("recheck") or []
    for r in rows:
        missing = [k for k in ("scope", "target", "reason", "by", "date", "mark") if not str(r.get(k) or "").strip()]
        if missing:
            raise ValueError(f"recheck.json: an entry for {r.get('target')!r} lacks {missing}")
    return list(rows)


def active(entries: Iterable[Mapping], inforce: Mapping) -> dict[tuple[str, str], dict]:
    """THE ENTRIES STILL ASKING, by (scope, target): those whose place's mark in force is still the mark the entry
    was made on. `inforce`: (scope, target) -> the mark in force (None / absent = no mark: the entry is inert too —
    there is nothing done to un-do). Pure."""
    out: dict[tuple[str, str], dict] = {}
    for e in entries:
        key = (str(e["scope"]), str(e["target"]))
        m = inforce.get(key)
        if m is not None and mark_digest(m) == e["mark"]:
            out[key] = {"reason": e["reason"], "by": e["by"], "date": e["date"]}
    return out
