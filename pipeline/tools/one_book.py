"""T1156: ONE COPY OF THE SYNOPSIS BOOK under `source/`. A second full book is a trap for checks (a stale copy lacks the
red in-season corrections, so a check against it shows false differences).

    python -m pipeline.tools.one_book        # exit 1, naming each offender, when a second book is under source/

ALLOWED: `fishing_synopsis.pdf` (the book) and `fishing_synopsis.<yyyy-yy>.pdf` (the dated archive copy the annual
runbook makes on purpose, e.g. `fishing_synopsis.2025-27.pdf`), directly in `source/`. A STALE copy renamed that way
passes BY DESIGN: the annual runbook is what makes those, and a name cannot tell last year's book from a copy of it.

A "FULL BOOK" is recognised without a byte hash (the served bytes drift day to day). Every file whose suffix is `.pdf`
in any case is judged, found through symlinked folders too (a broken link is an error naming it), in this order:
  1. its NAME holds `synopsis` or `regulations` (any case): a book, whatever it holds.
  2. SIZE FLOOR: under 1 MiB nothing is opened. The real book is 23.9 MB (88 pages); the largest region chapter is 1.7 MB
     (4-9 pages); the many bathymetry maps are far smaller, so they are never opened.
  3. 5 MiB or more and 60 or more pages: a book, whatever its text says (an image-only cover or no cover is caught).
  4. otherwise 20 or more pages AND FRESHWATER and SYNOPSIS on one of its first 3 pages: a book (the cover rule).
The nine region PDFs (1.7 MB and under, 4-9 pages) and the two tracked DFO evidence PDFs under `source/dfo_tidal_areas/`
(1.7 MB, 74 pages, no cover words) pass by these rules, not by a path list. Known gap: an unnamed copy under 5 MiB with an
image-only cover passes. A PDF that is opened and cannot be is an error naming it, never a pass.

WHAT EACH TREE HOLDS: CI has the tracked files only (`source/official/`, the bathymetry PDFs and the rest are
git-ignored and absent), so there it checks the tracked files and passes trivially without a second book; a machine with
`source/official/` filled is checked on the whole folder.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable

ALLOWED = re.compile(r"^fishing_synopsis(\.\d{4}-\d{2,4})?\.pdf$")
NAMED = re.compile(r"synopsis|regulations", re.I)
MIN_OPEN_BYTES = 1 << 20
BIG_BYTES = 5 << 20
BIG_PAGES = 60
BOOK_PAGES = 20
COVER_PAGES = 3


def pdfs_under(source: Path) -> list[Path]:
    """Every `.pdf` (any case) under `source`, through symlinked folders; a folder cycle is walked once."""
    out, seen = [], set()
    for root, dirs, files in os.walk(source, followlinks=True):
        real = os.path.realpath(root)
        if real in seen:
            dirs[:] = []
            continue
        seen.add(real)
        out += [Path(root) / f for f in files if f.lower().endswith(".pdf")]
    return sorted(out)


def tracked_pdfs(source: Path) -> list[Path]:
    """The PDFs git tracks under `source` (what CI holds)."""
    names = subprocess.run(["git", "ls-files", "-z", "--", "."], cwd=source, capture_output=True, text=True,
                           check=True).stdout.split("\0")
    return sorted(source / n for n in names if n.lower().endswith(".pdf"))


def pages_and_cover(pdf: Path) -> tuple[int, str]:
    import pypdfium2 as pdfium
    try:
        doc = pdfium.PdfDocument(str(pdf))
    except Exception as e:
        raise ValueError(f"{pdf}: cannot be opened as a PDF ({e}); remove it or fix it") from e
    try:
        n = len(doc)
        return n, " ".join(doc[i].get_textpage().get_text_range() for i in range(min(n, COVER_PAGES))).upper()
    finally:
        doc.close()


def second_books(source: Path, files: Iterable[Path] | None = None) -> list[str]:
    """Every PDF under `source` (or just `files`) that is a full book and not an allowed copy: 'relative/path: why'."""
    source = Path(source)
    if not source.is_dir():
        raise FileNotFoundError(f"{source}: the source folder is missing")
    out = []
    for pdf in (pdfs_under(source) if files is None else sorted(files)):
        if pdf.parent == source and ALLOWED.match(pdf.name):
            continue
        rel = pdf.relative_to(source).as_posix()
        if NAMED.search(pdf.name):
            out.append(f"{rel}: named like the book")
            continue
        try:
            size = pdf.stat().st_size
        except OSError as e:
            raise ValueError(f"{rel}: cannot be read ({e}); a broken link or a bad file: remove it") from e
        if size < MIN_OPEN_BYTES:
            continue
        n, cover = pages_and_cover(pdf)
        if size >= BIG_BYTES and n >= BIG_PAGES:
            out.append(f"{rel}: {n} pages, {size >> 20} MiB (a book is {BIG_PAGES}+ pages at {BIG_BYTES >> 20} MiB+)")
        elif n >= BOOK_PAGES and "FRESHWATER" in cover and "SYNOPSIS" in cover:
            out.append(f"{rel}: {n} pages and the synopsis cover")
    return out


def main() -> int:
    from pipeline.common.curated import SOURCE
    bad = second_books(Path(SOURCE))
    if not bad:
        print("one book: ok (no second full synopsis PDF under source/; trivially so where source/official/ is absent)")
        return 0
    for b in bad:
        print(f"SECOND SYNOPSIS BOOK {b}", file=sys.stderr)
    print("the book is source/fishing_synopsis.pdf; validate against nothing else (T1156). Delete the copy.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
