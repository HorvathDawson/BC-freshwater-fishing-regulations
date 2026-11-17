"""OWNER CARRIES (owner 2026-11-17 08:37, on DFO marks that read "binding moved" after his own later decisions: "Why
would we have to do this ... they should all be good?") — a small curated list of DFO marks HELD over a move the owner
himself decided after making them, without touching a mark.

    data/curated/review/owner_carries.json   {"carries": [{scope, target, mark_date, mark, binding, decision, words}]}

An entry holds its mark exactly as a landed fix does (`proposals.applied_keys` -> `marks.status` `landed`), and only
while BOTH still stand:

    mark      the digest of the mark in force it was written for (`recheck.mark_digest`): a new save makes it inert
    binding   the place's binding print the decision produced (`dfo.prints` `binding`): any later move of the place
              (another extent, another water flag that it reads) reads stale again

`decision` names the owner decision that moved the place (ticket, item, commit, his words for it); `words` his words
asking for the carry. Entries are added on the owner's word only; the marks store is never edited.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Mapping, Optional

FIELDS = ("scope", "target", "mark_date", "mark", "binding", "decision", "words")


def path() -> Path:
    """beside the marks store (moved with the whole tree by `CURATED_ROOT`)"""
    from pipeline.common.curated import CURATED
    return Path(CURATED.review.marks).parent / "owner_carries.json"


def load(p: Optional[Path] = None) -> list[dict]:
    """the entries as written; [] when there is no file. A malformed entry is refused (ValueError), never skipped."""
    p = p or path()
    if not p.exists():
        return []
    rows = json.loads(p.read_text(encoding="utf-8")).get("carries") or []
    for r in rows:
        missing = [k for k in FIELDS if not r.get(k)]
        if missing:
            raise ValueError(f"owner_carries.json: an entry for {r.get('target')!r} lacks {missing}")
        if r["scope"] != "dfo":
            raise ValueError(f"owner_carries.json: {r['target']!r} is a {r['scope']} mark; only a DFO place is carried")
    return list(rows)


def held(entries: Iterable[Mapping], inforce: Mapping, binding_now: Mapping[str, Optional[str]]) -> set:
    """THE KEYS OF THE MARKS AN ENTRY STILL HOLDS: the mark in force for its target is the one it was written for (by
    digest and date) and the place's binding print NOW is the one the decision produced. `inforce`: (scope, target) ->
    the mark in force; `binding_now`: target -> its binding print now (None: the place is gone). Pure."""
    from pipeline.regs.review.recheck import mark_digest
    out: set = set()
    for e in entries:
        m = inforce.get((e["scope"], e["target"]))
        if (m is not None and m.date == e["mark_date"] and mark_digest(m) == e["mark"]
                and binding_now.get(e["target"]) == e["binding"]):
            out.add(m.key)
    return out
