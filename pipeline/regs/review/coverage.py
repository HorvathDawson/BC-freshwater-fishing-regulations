"""THE COVERAGE STRIP (T134, PLAN §4 phase 3): which words of the printed row no rule quotes.

Check R asks "every clause is a rule and nothing is invented". The strip answers half of it at a
glance: the row's text with each stretch that some rule's (or licensing record's) `verbatim`
quotes marked `quoted`, and every other stretch marked `unquoted` — a clause no rule carries. A
stretch with no letter or digit in it (", ", "; ") is `gap`: punctuation is not a clause.

A quote is found in the row by `catalogue.squash` — THE comparison normaliser, the one the gate
uses to call a quote "in its own verbatim" — so the strip and the gate never disagree about
whether a quote is there. The row is shown as `clean_verbatim` gives it (the extraction's `**` and
`[Includes Tributaries]` out); `_folded` walks that text character by character so a match in the
squashed text maps back to the characters the reviewer reads. `test_review_screen.py` pins that
`_folded` IS `squash` over every row of the corpus.

A quote the row does not contain is listed in `missing` (by record id) — invented, or reworded.
"""
from __future__ import annotations

import re
from typing import Iterable, Sequence

from pipeline.regs.parsing.catalogue import clean_verbatim, squash

_DASHES = "‐‑‒–—―−"
_ALNUM = re.compile(r"[0-9A-Za-zÀ-ÿ]")
_EDGES = re.compile(r"([^0-9A-Za-zÀ-ÿ]*)(.*?)([^0-9A-Za-zÀ-ÿ]*)", re.S)


def _folded(text: str) -> tuple[str, list[int]]:
    """`squash(text)` and, for each of its characters, the index in `text` it came from.

    Restates `squash` step by step over a CLEANED text (no `[Includes Tributaries]`); the corpus
    test proves the folded string equals `squash(text)` for every row, so the two cannot drift."""
    # 1. per line: a leading blockquote / bullet marker is a space (squash's two line rules)
    keep: list[tuple[str, int]] = []
    i, n = 0, len(text)
    at_line_start = True
    while i < n:
        if at_line_start:
            j = i
            while j < n and text[j] in " \t\r\f\v":
                j += 1
            m_q = re.match(r"[>|]\s?", text[j:j + 2]) if j < n else None
            m_b = re.match(r"[-•]\s+", text[j:]) if j < n else None
            if m_q and text[j] in ">|":
                keep.append((" ", j))
                i = j + m_q.end()
                at_line_start = False
                continue
            if m_b:
                keep.append((" ", j))
                i = j + m_b.end()
                at_line_start = False
                continue
            at_line_start = False
        ch = text[i]
        if ch == "\n":
            at_line_start = True
        if ch == "*":
            i += 1
            continue
        if ch in _DASHES:
            ch = "-"
        keep.append((ch, i))
        i += 1
    # 2. whitespace runs -> one space, stripped; lower case
    out: list[str] = []
    idx: list[int] = []
    for ch, at in keep:
        if ch.isspace():
            if out and out[-1] != " ":
                out.append(" ")
                idx.append(at)
            continue
        for c in ch.lower():
            out.append(c)
            idx.append(at)
    while out and out[-1] == " ":
        out.pop()
        idx.pop()
    return "".join(out), idx


def coverage(verbatim: str, quotes: Iterable[tuple[str, str]]) -> dict:
    """The row (`clean_verbatim(verbatim)`) cut into `segments` — `{text, kind}` with kind `quoted`
    | `unquoted` | `gap` — plus `unquoted_chars` (letters and digits no quote covers) and `missing`
    (the ids of quotes the row does not contain). `quotes` is `(record id, its verbatim)`."""
    shown = clean_verbatim(verbatim or "")
    folded, idx = _folded(shown)
    covered = [False] * len(shown)
    missing: list[str] = []
    for rid, quote in quotes:
        q = squash(quote or "")
        if not q:
            continue
        start, hit = 0, False
        while True:
            at = folded.find(q, start)
            if at < 0:
                break
            hit = True
            lo, hi = idx[at], idx[at + len(q) - 1]
            for k in range(lo, hi + 1):
                covered[k] = True
            start = at + 1
        if not hit:
            missing.append(rid)
    segments: list[dict] = []
    for k, ch in enumerate(shown):
        kind = "quoted" if covered[k] else "unquoted"
        if segments and segments[-1]["_c"] == kind:
            segments[-1]["text"] += ch
        else:
            segments.append({"text": ch, "_c": kind})
    out: list[dict] = []

    def add(text: str, kind: str) -> None:
        if not text:
            return
        if out and out[-1]["kind"] == kind:
            out[-1]["text"] += text
        else:
            out.append({"text": text, "kind": kind})

    for s in segments:
        if s["_c"] == "quoted":
            add(s["text"], "quoted")
            continue
        # an unquoted stretch's leading and trailing punctuation is a gap, not part of the clause
        m = _EDGES.fullmatch(s["text"])
        add(m.group(1), "gap")
        add(m.group(2), "unquoted")
        add(m.group(3), "gap")
    unquoted = sum(len(_ALNUM.findall(s["text"])) for s in out if s["kind"] == "unquoted")
    return {"segments": out, "unquoted_chars": unquoted, "missing": missing}


def entry_coverage(entry: dict) -> dict:
    """The strip for one catalogue entry: its row against every rule's and licensing record's
    quote."""
    quotes: list[tuple[str, str]] = [(str(r.get("rule_id")), r.get("verbatim") or "")
                                     for r in entry.get("rules") or []]
    quotes += [(str(x.get("id")), x.get("verbatim") or "") for x in entry.get("licensing") or []]
    return coverage(entry.get("regs_verbatim") or "", quotes)


def folded_matches_squash(texts: Sequence[str]) -> list[str]:
    """The rows whose fold is NOT `squash` (empty when the two agree) — the pin's own check."""
    return [t for t in texts if _folded(clean_verbatim(t))[0] != squash(clean_verbatim(t))]
