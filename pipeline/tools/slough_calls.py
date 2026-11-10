"""T322: WRITE THE OWNER'S CONFIRMED SLOUGH / CANAL CALLS (`data/curated/waters/slough_calls.json`) from the marks'
decisions (`review.export.load`, computed in memory from marks.jsonl, T408) — the apply step. The registry reads this
file (`registry.flowing.fold_confirmed_sloughs`), never marks.jsonl.

A call is applied when the mark in force on `slough:<wbk>` is VERIFIED, `call` "stream", `P` y, and still holds
against the candidate it was made on (`slough.prints_of`; a redrawn outline makes it stale -> reported, not written).
A "not stream" call, an unsure (P ?) mark and a PRE-MARK (never a mark) are not written: the fold never touches them.

What the fold does with each written wbk (owner 2026-10-30): a polygon with a stream flowing IN and OUT joins that
stream; with no stream through it, it is IGNORED (it is not a stream then). A new unnamed stream is never made.

    python -m pipeline.tools.slough_calls            # write the file
    python -m pipeline.tools.slough_calls --check    # exit 1 when the file is not what the marks say
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

FORMAT = "slough_calls/1"
ABOUT = ("T322: the owner's CONFIRMED slough / canal calls (scope slough, verified, call stream, P y), written from "
         "the slough marks (review/marks.jsonl) by `python -m pipeline.tools.slough_calls` and the review app's "
         "write-through (`pipeline.regs.review.apply`) — never edit by hand. Read by the atlas registry, which folds "
         "each polygon into the ONE stream that flows in and out of it (`registry.flowing.fold_confirmed_sloughs`); "
         "with no stream through it, it is ignored.")


def confirmed(m) -> bool:
    """A slough mark that confirms a stream: verified, call stream, P y."""
    return (m.scope == "slough" and m.verdict == "verified" and m.call == "stream"
            and (m.answers or {}).get("P") == "y")


def build(decided, prints_of=None) -> tuple[list[dict], list[dict]]:
    """(rows to write, skipped [{wbk, why}]) from the decided data. `prints_of(wbk)` -> the candidate's prints now
    (default `review.slough.prints_of`); None = the candidate is gone."""
    from pipeline.regs.review import marks as M
    if prints_of is None:
        from pipeline.regs.review.slough import prints_of
    rows, skipped = [], []
    for m in decided.scope("slough"):
        if not confirmed(m):
            continue
        now = prints_of(m.target)
        if now is None:
            skipped.append({"wbk": m.target, "why": "candidate gone (not in the finder / automark)"})
            continue
        st = M.status(m, M.Prints.model_validate(now))
        if st["status"] != "verified":
            skipped.append({"wbk": m.target, "why": "stale: " + ", ".join(st["reasons"])})
            continue
        prov = decided.provenance.get(m.key) or {}
        rows.append({"wbk": str(m.target), "call": "stream", "reviewer": m.reviewer, "date": m.date,
                     "mark": prov.get("mark"), "note": m.note})
    rows.sort(key=lambda r: (int(r["wbk"]) if r["wbk"].isdigit() else 1 << 62, r["wbk"]))
    return rows, skipped


def document(rows: list[dict]) -> str:
    head = json.dumps({"_about": ABOUT, "format": FORMAT}, ensure_ascii=False, sort_keys=True)[:-1]
    body = ",\n".join(json.dumps(r, ensure_ascii=False, sort_keys=True) for r in rows)
    return f"{head}, \"waters\": [\n{body}\n]}}\n" if rows else f"{head}, \"waters\": []}}\n"


def load(path: Optional[Path] = None) -> frozenset[str]:
    """The confirmed wbks the registry folds. A missing file RAISES (AGENTS 37), naming the command."""
    if path is None:
        from pipeline.common.curated import CURATED
        path = CURATED.waters.slough_calls
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"no {p} — make it: python -m pipeline.tools.slough_calls")
    doc = json.loads(p.read_text(encoding="utf-8"))
    if doc.get("format") != FORMAT:
        raise ValueError(f"{p}: format {doc.get('format')!r}, expected {FORMAT}")
    return frozenset(str(r["wbk"]) for r in doc["waters"] if r.get("call") == "stream")


def main(argv: Optional[list[str]] = None) -> int:
    # T545: ONE WRITE PATH — the sloughs domain of `review.apply` (atomic, the shrink guard, every dropped row named)
    from pipeline.regs.review import apply as AP
    argv = sys.argv[1:] if argv is None else argv
    if "--check" in argv:
        got = AP.run(["sloughs"], dry_run=True)
        same = got["rc"] == 0 and not got["files"]
        print("slough_calls.json is current" if same else
              "slough_calls.json is OUT OF DATE — python -m pipeline.tools.slough_calls")
        return 0 if same else 1
    return AP.run(["sloughs"])["rc"]


if __name__ == "__main__":
    raise SystemExit(main())
