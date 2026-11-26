"""T1156: ONE COPY OF THE SYNOPSIS BOOK under `source/`. A second full book is a trap for checks (a stale copy lacks the
red in-season corrections, so a check against it shows false differences).

    python -m pipeline.tools.one_book        # exit 1, naming each offender, when a second book is under source/

ALLOWED: `fishing_synopsis.pdf` (the book) and `fishing_synopsis.<edition>.pdf` (the dated archive copy the annual runbook
makes on purpose, e.g. `fishing_synopsis.2025-27.pdf`), directly in `source/`.

A "FULL BOOK" is recognised without a byte hash (the served bytes drift day to day), by either of:
  * its NAME says so: a PDF whose name holds `synopsis` or `regulations` (any case);
  * its CONTENT: 20 or more pages (the book is 88; the nine region chapters are 4-9, a bathymetry map is 1) AND a first
    page that prints FRESHWATER and SYNOPSIS (the cover), so a renamed copy is caught and the DFO evidence PDFs under
    `source/dfo_tidal_areas/` (long, but not this book) are not.
So the nine region PDFs (`region_<n>_<name>.pdf`, 4-9 pages) pass. The walk covers all of `source/`, including the
git-ignored `source/official/`. A PDF that cannot be opened is an error naming it, never a pass. Where `source/` holds
no such PDF (CI, a fresh clone: `official/` is git-ignored and absent) the guard passes trivially, and says so.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ALLOWED = re.compile(r"^fishing_synopsis(\.\d{4}-\d{2,4})?\.pdf$")
NAMED = re.compile(r"synopsis|regulations", re.I)
BOOK_PAGES = 20


def cover_and_pages(pdf: Path) -> tuple[int, str]:
    import pypdfium2 as pdfium
    try:
        doc = pdfium.PdfDocument(str(pdf))
    except Exception as e:
        raise ValueError(f"{pdf}: cannot be opened as a PDF ({e}); remove it or fix it") from e
    try:
        n = len(doc)
        return n, (doc[0].get_textpage().get_text_range().upper() if n else "")
    finally:
        doc.close()


def second_books(source: Path) -> list[str]:
    """Every PDF under `source` that is a full book and is not an allowed copy: 'relative/path: why'."""
    source = Path(source)
    if not source.is_dir():
        raise FileNotFoundError(f"{source}: the source folder is missing")
    out = []
    for pdf in sorted(source.rglob("*.pdf")):
        if pdf.parent == source and ALLOWED.match(pdf.name):
            continue
        rel = pdf.relative_to(source).as_posix()
        if NAMED.search(pdf.name):
            out.append(f"{rel}: named like the book")
            continue
        n, cover = cover_and_pages(pdf)
        if n >= BOOK_PAGES and "FRESHWATER" in cover and "SYNOPSIS" in cover:
            out.append(f"{rel}: {n} pages and the synopsis cover")
    return out


def main() -> int:
    from pipeline.common.curated import SOURCE
    bad = second_books(Path(SOURCE))
    if not bad:
        print("one book: ok (no second full synopsis PDF under source/; passes trivially where source/official/ is absent)")
        return 0
    for b in bad:
        print(f"SECOND SYNOPSIS BOOK {b}", file=sys.stderr)
    print("the book is source/fishing_synopsis.pdf; validate against nothing else (T1156). Delete the copy.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
