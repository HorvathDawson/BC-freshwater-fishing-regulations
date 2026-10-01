"""The book's pages, for checking an entry against what is printed.

An entry's `source_pages` are PRINTED page numbers — the book's own address, the number on the
page a reader holds (`extract_synopsis.printed_page_number`). The PDF's page index is a different
number and the offset is not constant (printed = PDF - 2 up to printed 40, PDF - 6 after the
centre gloss). The app used to open `fishing_synopsis.pdf#page=<printed>`, which landed two to six
pages early — on Dean River's row it opened Region 4's tables.

So the printed -> PDF map is read off the repo copy's own footers, once, with the extractor's
function; a page with no footer (a map) takes the next numbered page's offset. The PDF and
a rendered image of a page are served from the repo copy (`data/source/fishing_synopsis.pdf`) —
the edition the entries were read from; gov.bc.ca's file changes bytes day to day.
"""
from __future__ import annotations

import io
from functools import lru_cache

from pipeline.common.curated import SOURCE

PDF_PATH = SOURCE / "fishing_synopsis.pdf"


@lru_cache(maxsize=1)
def printed_to_pdf() -> dict[int, int]:
    """printed page -> 1-based PDF page, every printed number up to the last one read."""
    import pdfplumber
    from pipeline.regs.extraction.extract_synopsis import printed_page_number
    seen: dict[int, int] = {}
    with pdfplumber.open(str(PDF_PATH)) as pdf:
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


def pdf_page(printed: int) -> int | None:
    return printed_to_pdf().get(int(printed))


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
