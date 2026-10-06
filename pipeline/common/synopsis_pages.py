"""The synopsis's two page numbers, and the ONE map between them.

Every page reference in this repo is a PRINTED page: the number in the page's running footer, the
book's own address ("see page 24" means the printed 24), and what an entry's `source_pages` /
`pages` carry. The PDF's page index is a different number and the offset is not constant:
printed = PDF - 2 up to printed 40, then four unnumbered centre pages (PDF 43-46), then
printed = PDF - 6 from the map on PDF 47 (printed 41) on. A link `fishing_synopsis.pdf#page=N`
takes the PDF index, so a link built from a printed page lands two to six pages early.

`printed_to_pdf()` reads the map off the repo copy's own footers with `printed_page_number`, the
extractor's reader (it lives here so the export and the curation app can use it without loading the
extractor's numpy/sklearn stack; `extract_synopsis` imports it from here). Nothing is hand-typed.
It ships once, in the UI export's guide file (`guide.pdf_pages`), and the curation app serves it
(`GET /api/synopsis/pages`). Every link into the PDF goes through it.
"""
from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import Optional

from pipeline.common.curated import SOURCE

#: The repo copy of the synopsis (`SOURCE / "fishing_synopsis.pdf"`) — the edition the entries
#: were read from (gov.bc.ca's file is the same edition, but its bytes move day to day).
PDF_NAME = "fishing_synopsis.pdf"


def default_pdf() -> Path:
    return Path(SOURCE / PDF_NAME)


#: The running footer every numbered page carries: "40 2025-2027 BC Freshwater Fishing Regulations
#: Synopsis" on a left page, "... Synopsis 41" on a right one. Read off the page's own footer rather
#: than from an offset table, because the offset is not constant: the four-page centre gloss after
#: printed p. 40 is unnumbered and the map on PDF p. 47 is printed 41, so printed = PDF - 2 up to PDF
#: 42 and PDF - 6 from PDF 47.
_FOOTER_PAGE = re.compile(
    r"(?:^|\s)(\d{1,3})\s+\d{4}-\d{4} BC Freshwater Fishing Regulations Synopsis"
    r"|\d{4}-\d{4} BC Freshwater Fishing Regulations Synopsis\s+(\d{1,3})(?:\s|$)")


def printed_page_number(page) -> Optional[int]:
    """The page number PRINTED on this (pdfplumber) page, from its running footer; None when it has
    none.

    THIS IS THE NUMBER A ROW'S `page` CARRIES. It is the book's own address — its cross-references
    say "see page 24" and mean the printed 24 — and it is what a reader holding the synopsis turns
    to. The PDF's page index is a different number (printed 42 is PDF p. 48), and storing that as
    `page` gave every water entry a page the book does not print on it. The index is kept beside it
    as `pdf_page`, and it still keys the row images.

    `dedupe_chars()` first: the footer is set in a faux-bold that draws each glyph twice, so the
    raw text reads "1144" on printed p. 14 — indistinguishable from a real "11" without it."""
    h = page.height
    foot = page.within_bbox((0, h * 0.95, page.width, h)).dedupe_chars().extract_text() or ""
    got = {int(g) for m in _FOOTER_PAGE.finditer(foot) for g in m.groups() if g}
    return got.pop() if len(got) == 1 else None


def _read_map(pdf_path: Path) -> dict[int, int]:
    import pdfplumber
    seen: dict[int, int] = {}
    with pdfplumber.open(str(pdf_path)) as pdf:
        for i, page in enumerate(pdf.pages):
            n = printed_page_number(page)
            if n and n not in seen:
                seen[n] = i + 1
    if not seen:
        return {}
    out: dict[int, int] = {}
    offset = None
    for n in range(max(seen), 0, -1):
        if n in seen:
            offset = seen[n] - n
            out[n] = seen[n]
        elif offset is not None:
            # an unnumbered page takes the offset of the NEXT numbered one: the map printed 41
            # sits on PDF 47, after the centre gloss, at printed 42's offset (PDF 48), not 40's
            out[n] = n + offset
    return dict(sorted(out.items()))


def printed_to_pdf(pdf_path: Optional[Path] = None) -> dict[int, int]:
    """printed page -> 1-based PDF page, every printed number from 1 to the last one the footers
    print. A printed page with no footer of its own (a map) takes the next numbered page's
    offset. Empty when the PDF is missing."""
    return dict(_cached(Path(pdf_path or default_pdf()).resolve()))


@lru_cache(maxsize=4)
def _cached(pdf_path: Path) -> dict[int, int]:
    return _read_map(pdf_path) if pdf_path.exists() else {}


def pdf_page(printed: int, pdf_path: Optional[Path] = None) -> Optional[int]:
    """The 1-based PDF page a printed page is on; None for a number the book does not print."""
    return printed_to_pdf(pdf_path).get(int(printed))
