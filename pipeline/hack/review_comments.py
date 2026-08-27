"""Collect review comments you drop into the planning docs into one reviewable list.

ONE universal convention — an HTML comment (invisible in rendered markdown, present in the source,
greppable everywhere). Drop these anywhere in any doc as you read offline:

    <!-- @REVIEW: this confluence looks like the north fork, double-check -->
    <!-- @Q: why is Dean's canyon 3-5 km from the mouth, not the famous upper canyon? -->
    <!-- @BLOCKER: don't author splits.json for the Skeena rows until I confirm the reach labels -->

An agent (or you) answers inline right below, so it reads as a thread:

    <!-- @REPLY: confirmed north fork; fixed in <commit> -->

Then, back in service:

    .venv/bin/python -m pipeline.hack.review_comments            # scan the docs
    .venv/bin/python -m pipeline.hack.review_comments --open     # only unanswered
    .venv/bin/python -m pipeline.hack.review_comments path/ ...   # custom paths

Works in code/JSON too (a `@REVIEW:` inside a `#`/`//` comment, or in a locator `notes` field —
for the locators JSON prefer the `label` loop's `n`/`a` actions). Markdown is the primary target.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

TAG = re.compile(r"@(REVIEW|BLOCKER|Q|QUESTION|TODO|REPLY)\b[:\-\s]*(.*)", re.I)
_OPEN = {"REVIEW", "BLOCKER", "Q", "QUESTION", "TODO"}
DEFAULT = ["pipeline/docs", "pipeline/splits.schema.md"]
_MARK = {"BLOCKER": "🔴", "Q": "❓", "QUESTION": "❓", "REPLY": "  ↳", "TODO": "☐"}


def _files(paths: list[str]) -> list[Path]:
    """Explicit files scanned as-is (any type); directories scanned for markdown only (so the
    tool's own docstring examples in .py aren't picked up)."""
    out: list[Path] = []
    for p in map(Path, paths):
        if p.is_file():
            out.append(p)
        elif p.is_dir():
            out += sorted(p.rglob("*.md"))
    return out


def scan(paths: list[str]):
    hits = []
    for f in _files(paths):
        try:
            lines = f.read_text(errors="ignore").splitlines()
        except Exception:
            continue
        for i, line in enumerate(lines, 1):
            m = TAG.search(line)
            if m:
                tag = m.group(1).upper()
                txt = m.group(2).strip().rstrip("->").strip()
                hits.append((f, i, tag, txt))
    return hits


def main() -> None:
    ap = argparse.ArgumentParser(description="Collect @REVIEW/@Q/@BLOCKER/@REPLY comments from docs.")
    ap.add_argument("paths", nargs="*", default=DEFAULT, help=f"files/dirs (default {DEFAULT})")
    ap.add_argument("--open", action="store_true", help="only unanswered (no @REPLY on the next few lines)")
    args = ap.parse_args()
    hits = scan(args.paths)
    if args.open:
        replies = {(f, i) for f, i, t, _ in hits if t == "REPLY"}
        hits = [h for h in hits if h[2] in _OPEN
                and not any((h[0], h[1] + d) in replies for d in range(1, 4))]
    n_open = sum(1 for _, _, t, _ in hits if t in _OPEN)
    n_rep = sum(1 for _, _, t, _ in hits if t == "REPLY")
    print(f"{n_open} comment(s), {n_rep} reply(ies){' — unanswered only' if args.open else ''}\n")
    cur = None
    for f, i, tag, txt in hits:
        if f != cur:
            print(f"\n{f}"); cur = f
        print(f"  {_MARK.get(tag, '💬')} {tag:8s} :{i}  {txt[:140]}")


if __name__ == "__main__":
    main()
