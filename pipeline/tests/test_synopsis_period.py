"""THE SYNOPSIS PERIOD — the regulations' last day in force, read off the book's own cover (owner 2026-10-22 late:
"the release stamps `valid_until` (the synopsis period end) so the app can say when the regulations lapse").

One reader (`synopsis_pages.synopsis_period`), one stamp (the bundle's meta `valid_until`, which the release copies into
`manifest.regulations.valid_until`). No date is typed anywhere: these tests hold the reader to the words the cover prints.
"""
from __future__ import annotations

import re

import pytest

from pipeline.common import synopsis_pages as SP

#: the cover (PDF p.1) and the inside page (PDF p.2) of the 2025-2027 synopsis, as pdfplumber reads them (trimmed)
COVER = "2025 - 2027\nFRESHWATER FISHING\nREGULATIONS SYNOPSIS\nEFFECTIVE APRIL 1, 2025 THROUGH MARCH 31, 2027\n"
INSIDE = ("The regulations in this publication are valid from April 1, 2025, to March 31, 2027. "
          "Visit the website for in-season changes.")


def test_the_cover_wording_reads_as_the_whole_period():
    assert SP.period_from_text(COVER) == SP.SynopsisPeriod("2025-04-01", "2027-03-31")


def test_the_inside_wording_reads_as_the_same_period():
    assert SP.period_from_text(INSIDE) == SP.SynopsisPeriod("2025-04-01", "2027-03-31")


def test_both_wordings_together_agree_into_one_period():
    assert SP.period_from_text(COVER + INSIDE) == SP.SynopsisPeriod("2025-04-01", "2027-03-31")


def test_two_pages_that_disagree_are_refused_never_one_picked():
    other = INSIDE.replace("March 31, 2027", "March 31, 2028")
    with pytest.raises(ValueError, match=r"disagree.*2027-03-31.*2028-03-31"):
        SP.period_from_text(COVER + other)


def test_a_book_that_states_no_period_is_refused():
    with pytest.raises(ValueError, match="states no period"):
        SP.period_from_text("2025-2027 BC Freshwater Fishing Regulations Synopsis 11")


def test_a_date_that_does_not_exist_is_refused():
    with pytest.raises(ValueError, match="not a date"):
        SP.period_from_text("EFFECTIVE APRIL 1, 2025 THROUGH FEBRUARY 30, 2027")


def test_a_missing_book_is_a_hard_error_not_an_empty_stamp(tmp_path):
    with pytest.raises(FileNotFoundError, match="no_such.pdf"):
        SP.synopsis_period(tmp_path / "no_such.pdf")


#: Tracked in git (`test_synopsis_pages.test_the_pdf_is_there`): a missing PDF FAILS these, never skips them.
_PDF = SP.default_pdf()


def test_the_repo_synopsis_reads_its_own_period():
    assert SP.synopsis_period(_PDF) == SP.SynopsisPeriod("2025-04-01", "2027-03-31")


def test_the_book_title_names_the_same_years_as_the_period():
    """BOOK_TITLE is the footer's words, typed once; a new edition that updates the PDF but not the title fails here."""
    from pipeline.deliver.answers.common import BOOK_TITLE
    p = SP.synopsis_period(_PDF)
    years = re.match(r"^(\d{4})–(\d{4}) ", BOOK_TITLE)
    assert years is not None and (years.group(1), years.group(2)) == (p.valid_from[:4], p.valid_until[:4])


def test_the_bundle_stamps_the_period_end_as_valid_until():
    """The bundle's meta `valid_until` (which the release copies into the manifest) is the period's last day."""
    from pipeline.deliver.bundle.build import edition_valid_until
    assert edition_valid_until(_PDF.parent) == "2027-03-31"


def test_the_bundle_stamp_refuses_a_source_dir_without_the_book(tmp_path):
    from pipeline.deliver.bundle.build import edition_valid_until
    with pytest.raises(FileNotFoundError, match=SP.PDF_NAME):
        edition_valid_until(tmp_path)


def test_the_bundle_meta_row_is_written_from_the_stamp():
    """MUTATION PIN: the meta row `valid_until` is the stamp, not a literal (it was `""` until 2026-10-22, and every
    release then said nothing of when its regulations lapse). Scraped from the source: building a bundle needs the
    whole atlas."""
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "deliver" / "bundle" / "build.py").read_text()
    assert re.findall(r"^\s*_valid_until = (.+)$", src, re.M) == ["edition_valid_until(data_dir)"]
    assert re.findall(r'\("valid_until", ([^)]*)\)', src) == ['_valid_until or ""']
