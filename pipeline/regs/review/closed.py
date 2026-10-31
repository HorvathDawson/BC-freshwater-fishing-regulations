"""T325: THE CLOSED REVIEW DOMAINS — the list the exporter and the review app skip.

Owner 2026-10-30 (option a): marks.jsonl is NEVER edited. A finished review is closed by listing its domain here
(`python -m pipeline.regs.review.lock <domain>`); reopened by removing the entry (`lock --reopen <domain>`, or by
hand). A closed domain:

  * is not exported (`review.export` writes no `decided/<domain>.json`, and removes a leftover one);
  * is not applied (`review.apply` skips it);
  * is read-only in the review app (its queue answers 423 "locked", a mark on its scopes is refused, the tab says
    "locked").

Its marks stay in marks.jsonl, unchanged, as history. Its decisions live in its final curated file (or, for ponds,
nowhere: the owner ruled nothing reads them).

The list lives beside the marks store (`CURATED.regulations.review.closed`); a marks store elsewhere (a test's temp
copy) has its own list beside it, absent = nothing closed.

    {"format": "closed/1", "_about": "...", "closed": [
      {"domain": "ponds", "scopes": ["q154"], "date": "2026-10-30", "by": "owner", "why": "...", "final": null}]}
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

FORMAT = "closed/1"
ABOUT = ("T325: review domains that are CLOSED (locked). The exporter (`pipeline.regs.review.export`), the apply "
         "command (`pipeline.regs.review.apply`) and the review app skip each one; marks.jsonl is never edited. Close: "
         "`python -m pipeline.regs.review.lock <domain>`. Reopen: remove the entry (`lock --reopen <domain>`).")


def path(marks: Optional[Path] = None) -> Path:
    """The closed list: the curated one (`CURATED.regulations.review.closed`), or the one beside `marks` when a marks
    store other than the curated one is named."""
    from pipeline.common.curated import CURATED
    default_marks = Path(CURATED.regulations.review.marks)
    if marks is None or Path(marks).resolve() == default_marks.resolve():
        return Path(CURATED.regulations.review.closed)
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
    from pipeline.regs.parsing.io import atomic_write
    p = Path(p) if p is not None else path()
    rows = [got[d] for d in sorted(got)]
    head = json.dumps({"_about": ABOUT, "format": FORMAT}, ensure_ascii=False, sort_keys=True)[:-1]
    body = ",\n".join(json.dumps(r, ensure_ascii=False, sort_keys=True) for r in rows)
    atomic_write(p, f"{head}, \"closed\": [\n{body}\n]}}\n" if rows else f"{head}, \"closed\": []}}\n")
    return p
