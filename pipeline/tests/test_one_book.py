"""(T1156) The one-book guard (pipeline/tools/one_book.py): a second full synopsis PDF under `source/` fails, naming it.
The book is `fishing_synopsis.pdf`; validate against nothing else. The region chapters (4-9 pages) are not the book.
Trees are built in tmp_path."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from pipeline.tools import one_book as OB

MiB = 1 << 20
REGIONS = ["region_1_vancouver_island.pdf", "region_2_lower_mainland.pdf", "region_3_thompson.pdf", "region_4_kootenay.pdf",
           "region_5_cariboo.pdf", "region_6_skeena.pdf", "region_7a_omineca.pdf", "region_7b_peace.pdf", "region_8_okanagan.pdf"]


def _pdf(path: Path, pages: int, cover: str = "FRESHWATER FISHING REGULATIONS SYNOPSIS", size: int = 0) -> None:
    """A minimal PDF of `pages` pages; the first prints `cover` (empty = no text); padded to `size` bytes."""
    objs = ["<< /Type /Catalog /Pages 2 0 R >>",
            "<< /Type /Pages /Kids [" + " ".join(f"{4 + 2 * i} 0 R" for i in range(pages)) + f"] /Count {pages} >>",
            "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"]
    for i in range(pages):
        text = cover if i == 0 else f"page {i}"
        stream = f"BT /F1 12 Tf 20 100 Td ({text}) Tj ET" if text else ""
        objs.append(f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 200] /Resources << /Font << /F1 3 0 R >> >> /Contents {5 + 2 * i} 0 R >>")
        objs.append(f"<< /Length {len(stream)} >>\nstream\n{stream}\nendstream")
    body, offs = "%PDF-1.4\n", []
    for n, o in enumerate(objs, 1):
        offs.append(len(body))
        body += f"{n} 0 obj\n{o}\nendobj\n"
    xref = len(body)
    body += f"xref\n0 {len(objs) + 1}\n0000000000 65535 f \n" + "".join(f"{o:010d} 00000 n \n" for o in offs)
    body += f"trailer\n<< /Size {len(objs) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n"
    data = body.encode()
    data += b"%" + b"." * max(0, size - len(data) - 2) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def _tree(tmp_path: Path) -> Path:
    src = tmp_path / "source"
    _pdf(src / "fishing_synopsis.pdf", 25, size=2 * MiB)
    return src


def test_clean_tree_passes(tmp_path):
    assert OB.second_books(_tree(tmp_path)) == []


def test_a_book_copy_in_official_fails_naming_it(tmp_path):
    src = _tree(tmp_path)
    _pdf(src / "official" / "copy.pdf", 25, size=2 * MiB)
    _pdf(src / "official" / "2025-2027_freshwater_fishing_regulations_synopsis.pdf", 2)
    bad = OB.second_books(src)
    assert len(bad) == 2 and any("official/copy.pdf" in b for b in bad) and any("synopsis.pdf" in b for b in bad)


def test_a_big_many_page_book_is_caught_whatever_its_text_says(tmp_path):
    src = _tree(tmp_path)
    _pdf(src / "official" / "book.pdf", 70, cover="", size=6 * MiB)
    _pdf(src / "official" / "image_cover.pdf", 70, cover="Hello", size=6 * MiB)
    assert sorted(b.split(":")[0] for b in OB.second_books(src)) == ["official/book.pdf", "official/image_cover.pdf"]


def test_the_suffix_is_read_in_any_case(tmp_path):
    src = _tree(tmp_path)
    _pdf(src / "official" / "COPY.PDF", 70, cover="", size=6 * MiB)
    assert [b.split(":")[0] for b in OB.second_books(src)] == ["official/COPY.PDF"]


def test_a_copy_in_a_symlinked_folder_is_found(tmp_path):
    src = _tree(tmp_path)
    _pdf(tmp_path / "elsewhere" / "book.pdf", 70, cover="", size=6 * MiB)
    os.symlink(tmp_path / "elsewhere", src / "linked")
    os.symlink(src, src / "loop")
    assert [b.split(":")[0] for b in OB.second_books(src)] == ["linked/book.pdf"]


def test_a_broken_link_is_an_error_naming_it(tmp_path):
    src = _tree(tmp_path)
    os.symlink(tmp_path / "gone.pdf", src / "dangling.pdf")
    with pytest.raises(ValueError, match="dangling.pdf"):
        OB.second_books(src)


def test_a_dated_archive_copy_passes(tmp_path):
    src = _tree(tmp_path)
    _pdf(src / "fishing_synopsis.2025-27.pdf", 70, size=6 * MiB)
    assert OB.second_books(src) == []


def test_a_region_chapter_is_not_a_second_copy_of_the_book(tmp_path):
    src = _tree(tmp_path)
    for n, pages in zip(REGIONS, [8, 7, 6, 8, 7, 9, 6, 4, 5]):  # a region chapter: the sizes and page counts it has
        _pdf(src / "official" / n, pages, cover="Region", size=int(1.7 * MiB))
    assert OB.second_books(src) == []


def test_a_long_small_pdf_that_is_not_the_book_passes(tmp_path):
    """The two tracked DFO evidence PDFs: 74 pages, 1.7 MB, no cover words: under the size to be a book by pages."""
    src = _tree(tmp_path)
    _pdf(src / "dfo_tidal_areas" / "waves.pdf", 74, cover="DFO tidal waters", size=int(1.7 * MiB))
    assert OB.second_books(src) == []


def test_the_cover_rule_catches_a_small_unnamed_copy(tmp_path):
    src = _tree(tmp_path)
    _pdf(src / "official" / "x.pdf", 25, size=2 * MiB)
    assert len(OB.second_books(src)) == 1


def test_a_small_file_is_never_opened(tmp_path):
    src = _tree(tmp_path)
    (src / "bathymetry_pdfs").mkdir()
    (src / "bathymetry_pdfs" / "corrupt.pdf").write_bytes(b"not a pdf")
    assert OB.second_books(src) == []


def test_an_unopenable_big_pdf_is_an_error(tmp_path):
    src = _tree(tmp_path)
    (src / "official").mkdir()
    (src / "official" / "x.pdf").write_bytes(b"not a pdf" + b"." * (2 * MiB))
    with pytest.raises(ValueError, match="x.pdf"):
        OB.second_books(src)


def test_the_tracked_source_tree_has_one_book():
    """What CI holds: the tracked files only. Passes trivially without a second book (source/official/ is absent)."""
    from pipeline.common.curated import SOURCE
    assert OB.second_books(Path(SOURCE), OB.tracked_pdfs(Path(SOURCE))) == []


@pytest.mark.needs_source
def test_the_whole_source_tree_has_one_book(request):
    """A machine with the fetched files (source/official/, bathymetry_pdfs/): the whole folder."""
    from pipeline.common.curated import SOURCE
    from pipeline.tests.conftest import need
    assert OB.second_books(need(request, "source", Path(SOURCE), "bash scripts/bootstrap.sh")) == []
