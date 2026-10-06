"""The book's pages, for checking an entry against what is printed.

An entry's `source_pages` are PRINTED page numbers — the book's own address, the number on the
page a reader holds (`extract_synopsis.printed_page_number`). The PDF's page index is a different
number and the offset is not constant (printed = PDF - 2 up to printed 40, PDF - 6 after the
centre gloss). The app used to open `fishing_synopsis.pdf#page=<printed>`, which landed two to six
pages early — on Dean River's row it opened Region 4's tables.

So the printed -> PDF map is the pipeline's one map (`pipeline.common.synopsis_pages`), read off
the repo copy's own footers with the extractor's function — the same map the UI export ships as
`guide.pdf_pages`. The PDF and a rendered image of a page are served from the repo copy
(`data/source/fishing_synopsis.pdf`) — the edition the entries were read from; gov.bc.ca's file
changes bytes day to day.
"""
from __future__ import annotations

import io
from functools import lru_cache

from pipeline.common import synopsis_pages as _pages

PDF_PATH = _pages.default_pdf()


def printed_to_pdf() -> dict[int, int]:
    """printed page -> 1-based PDF page (`pipeline.common.synopsis_pages.printed_to_pdf`)."""
    return _pages.printed_to_pdf(PDF_PATH)


def pdf_page(printed: int) -> int | None:
    return _pages.pdf_page(printed, PDF_PATH)


@lru_cache(maxsize=32)
def page_png(printed: int, scale: float = 1.6) -> bytes | None:
    """A PNG of the printed page, rendered from the repo copy; None for a page it does not have."""
    n = pdf_page(printed)
    if n is None:
        return None
    import pypdfium2 as pdfium
    doc = pdfium.PdfDocument(str(PDF_PATH))
    try:
        img = doc[n - 1].render(scale=scale).to_pil()
    finally:
        doc.close()
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()
