"""T325: THE CLOSED REVIEW DOMAINS — domains that FEED NOTHING, which the decisions (`review.export.load`) and the review app skip.

Owner 2026-10-30: there is no lock step. Every domain whose marks feed a curated file stays open and its marks reach
that file by write-through (the review app runs `review.apply.run` after each save); a domain whose queue is empty
just shows "done". This list is ONLY for a domain nothing reads (ponds, T329: a pond call only confirms the lake
default). marks.jsonl is never edited. A closed domain:

  * is left out of the decisions (`review.export.load` drops its marks, so no writer sees them);
  * is not applied (`review.apply` skips it);
  * is read-only in the review app (its queue answers 423, a mark on its scopes is refused, the tab shows a
    "closed" notice).

Reopen = remove the entry by hand. The list lives beside the marks store (`CURATED.review.closed`); a
marks store elsewhere (a test's temp copy) has its own list beside it, absent = nothing closed.

    {"format": "closed/1", "_about": "...", "closed": [
      {"domain": "ponds", "scopes": ["q154"], "date": "2026-10-30", "by": "owner", "why": "...", "final": null}]}
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

FORMAT = "closed/1"
ABOUT = ("T325: review domains that FEED NOTHING (ponds). The exporter (`pipeline.regs.review.export`), the apply "
         "(`pipeline.regs.review.apply`, run by the review app's write-through) and the review app skip each one; "
         "marks.jsonl is never edited. Every other domain stays open: its marks reach its curated file by "
         "write-through. Reopen: remove the entry.")


def path(marks: Optional[Path] = None) -> Path:
    """The closed list: the curated one (`CURATED.review.closed`), or the one beside `marks` when a marks
    store other than the curated one is named."""
    from pipeline.common.curated import CURATED
    default_marks = Path(CURATED.review.marks)
    if marks is None or Path(marks).resolve() == default_marks.resolve():
        return Path(CURATED.review.closed)
    return Path(marks).parent / "closed.json"


def entries(p: Optional[Path] = None) -> dict[str, dict]:
    """domain -> its entry. An absent file = nothing closed; a malformed one RAISES (never read as "nothing")."""
    p = Path(p) if p is not None else path()
    if not p.exists():
        return {}
    doc = json.loads(p.read_text(encoding="utf-8"))
    if doc.get("format") != FORMAT:
        raise ValueError(f"{p}: format {doc.get('format')!r}, expected {FORMAT}")
    return {e["domain"]: e for e in doc.get("closed") or []}


def domains(p: Optional[Path] = None) -> frozenset[str]:
    return frozenset(entries(p))


def scopes(p: Optional[Path] = None) -> dict[str, str]:
    """mark scope -> the closed domain that holds it."""
    from pipeline.regs.review.export import DOMAINS
    return {s: d for d in entries(p) for s in DOMAINS.get(d, ())}


def write(got: dict[str, dict], p: Optional[Path] = None) -> Path:
    from pipeline.common.curated_write import write_any       # T600: backed up and logged
    p = Path(p) if p is not None else path()
    rows = [got[d] for d in sorted(got)]
    head = json.dumps({"_about": ABOUT, "format": FORMAT}, ensure_ascii=False, sort_keys=True)[:-1]
    body = ",\n".join(json.dumps(r, ensure_ascii=False, sort_keys=True) for r in rows)
    write_any(p, f"{head}, \"closed\": [\n{body}\n]}}\n" if rows else f"{head}, \"closed\": []}}\n",
              tool="pipeline.regs.review.closed")
    return p
