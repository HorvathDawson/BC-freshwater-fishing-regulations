"""The synopsis's "Definitions" page (PRINTED p.80), read off the PDF deterministically (T288).

The page is three columns of `**term:** text` paragraphs in 9.5 pt type; a term is the run of bold words that opens a
line at the column's left edge and ends in ":" (a bold word inside a definition, "does not", is emphasis, not a term).
The game-fish table under the third column ("Freshwater game fish are defined as follows:") is not a definition and
stops the read. No LLM, no guessing: the words are the PDF's, whitespace-folded; a line that ends in a hyphen joins the
next without a space only when the hyphen splits a word ("land-locked" never breaks across lines on this page).

`definitions()` -> [(term, text)] in the page's order — the list the glossary's completeness test pins
(`pipeline/tests/test_glossary_complete.py`), and the check that the curated transcription
(`data/curated/regulations/reference/definitions.md`, which the bundle's `glossary_source` copies) says the same words.
"""
from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import List, Optional, Tuple

#: The definitions page, by its PRINTED number (the repo PDF puts it on PDF p.86: PDF - 6 after printed 40).
PRINTED_PAGE = 80
_BODY = 9.5          # the definitions' type size; the title (15 pt), the game-fish table (7.6 pt) and the footer differ
_STOP = "Freshwater game fish are defined as follows:"


def _lines(page) -> List[Tuple[float, list]]:
    """The page's body lines, column by column, each its words left to right."""
    words = [w for w in page.extract_words(extra_attrs=["fontname", "size"])
             if abs(w["size"] - _BODY) < 0.3 and w["x0"] > 30]   # x0 <= 30: the line-number gutter
    # the columns' left edges: the three x positions most bold words (the terms) start at — a definition's own lines
    # hang 6 pt further in
    from collections import Counter
    edges = [x for x, _ in Counter(round(w["x0"]) for w in words if _bold(w)).most_common(3)]
    edges.sort()
    if len(edges) != 3:
        raise ValueError(f"definitions page: expected 3 columns, found left edges {edges}")
    bounds = [(e - 4, (edges[i + 1] - 4) if i + 1 < len(edges) else 10_000) for i, e in enumerate(edges)]
    out: List[Tuple[float, list]] = []
    for (a, b), edge in zip(bounds, edges):
        col = sorted((w for w in words if a <= w["x0"] < b), key=lambda w: w["top"])
        lines: List[list] = []
        for w in col:
            if lines and abs(w["top"] - lines[-1][0]["top"]) < 3:
                lines[-1].append(w)
            else:
                lines.append([w])
        for ln in lines:
            ln.sort(key=lambda w: w["x0"])
            out.append((edge, ln))
    return out


def _bold(w) -> bool:
    return "Bold" in w["fontname"]


def read_page(page) -> List[Tuple[str, str]]:
    """[(term, text)] from a pdfplumber page holding the definitions."""
    items: List[List[str]] = []      # [term, text]
    for edge, ln in _lines(page):
        text = " ".join(w["text"] for w in ln)
        if text.startswith(_STOP):
            break
        lead = []
        if abs(ln[0]["x0"] - edge) < 2 and _bold(ln[0]):
            for w in ln:
                if not _bold(w):
                    break
                lead.append(w)
                if w["text"].endswith(":"):
                    break
        if lead and lead[-1]["text"].endswith(":"):
            term = " ".join(w["text"] for w in lead)[:-1]
            rest = " ".join(w["text"] for w in ln[len(lead):])
            items.append([term, rest])
        elif items:
            prev = items[-1][1]
            joined = (prev[:-1] + text) if re.search(r"[a-z]-$", prev) and re.match(r"[a-z]", text) else f"{prev} {text}"
            items[-1][1] = joined.strip()
        else:
            raise ValueError(f"definitions page: text before the first term: {text!r}")
    out = [(t, re.sub(r"\s+", " ", x).strip()) for t, x in items]
    if len({t.lower() for t, _ in out}) != len(out):
        raise ValueError("definitions page: a term printed twice")
    return out


def definitions(pdf_path: Optional[Path] = None) -> List[Tuple[str, str]]:
    """The p.80 definitions of the repo synopsis, in the page's order. A missing PDF or page is an error."""
    from pipeline.common.synopsis_pages import default_pdf
    p = Path(pdf_path or default_pdf()).resolve()
    if not p.exists():
        raise FileNotFoundError(f"definitions: {p} not found")
    return list(_cached(p))


@lru_cache(maxsize=2)
def _cached(p: Path) -> Tuple[Tuple[str, str], ...]:
    import pdfplumber
    from pipeline.common.synopsis_pages import pdf_page
    n = pdf_page(PRINTED_PAGE, p)
    if not n:
        raise ValueError(f"definitions: {p} has no printed p.{PRINTED_PAGE}")
    with pdfplumber.open(str(p)) as pdf:
        return tuple(read_page(pdf.pages[n - 1]))


if __name__ == "__main__":      # python -m pipeline.common.synopsis_definitions — print the page's terms
    for t, x in definitions():
        print(f"**{t}**: {x}\n")
