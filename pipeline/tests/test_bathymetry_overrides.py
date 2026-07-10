"""Validation tests for the curated bathymetry override table.

Loads pipeline/matching/bathymetry_overrides.json and verifies structural
integrity: every entry links a map title to a single FWA waterbody key with a
well-formed PDF sheet list, keys/sheets are internally consistent, and (when the
built deploy tree is present) the referenced survey PDFs exist on disk.
"""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from typing import Dict, List

import pytest

from pipeline.matching.bathymetry_matcher import (
    BATHYMETRY_OVERRIDES_PATH,
    load_bathymetry_overrides,
    normalize_map_title,
)

_PDF_RE = re.compile(r"^\d+\.pdf$")
_BATHY_DIR = (
    Path(__file__).resolve().parents[2]
    / "output"
    / "pipeline"
    / "deploy"
    / "bathymetry"
)


@pytest.fixture(scope="module")
def overrides() -> List[Dict[str, object]]:
    assert BATHYMETRY_OVERRIDES_PATH.exists(), (
        f"bathymetry_overrides.json not found at {BATHYMETRY_OVERRIDES_PATH}"
    )
    entries = load_bathymetry_overrides()
    assert entries, "override table is empty"
    return entries


def test_required_fields_and_types(overrides: List[Dict[str, object]]) -> None:
    for e in overrides:
        title = e.get("map_title")
        assert isinstance(title, str) and title.strip(), f"bad map_title: {e!r}"
        assert normalize_map_title(title), f"map_title normalises to empty: {title!r}"

        pdfs = e.get("pdfs", [])
        assert isinstance(pdfs, list) and pdfs, f"pdfs must be a non-empty list: {e!r}"
        for pdf in pdfs:
            assert isinstance(pdf, str) and _PDF_RE.match(pdf), (
                f"malformed pdf filename {pdf!r} in {title!r}"
            )

        if e.get("ignore"):
            # Deliberately-unlinked survey (correct waterbody not yet known).
            assert e.get("waterbody_key") is None, (
                f"ignored entry {title!r} should have a null waterbody_key"
            )
            continue

        wbk = e.get("waterbody_key")
        assert isinstance(wbk, int) and not isinstance(wbk, bool) and wbk > 0, (
            f"waterbody_key must be a positive int: {e!r}"
        )


def test_no_duplicate_map_titles(overrides: List[Dict[str, object]]) -> None:
    norm = [normalize_map_title(e["map_title"]) for e in overrides]
    dupes = [t for t, n in Counter(norm).items() if n > 1]
    assert not dupes, f"duplicate normalised map titles: {dupes}"


def test_no_pdf_referenced_twice(overrides: List[Dict[str, object]]) -> None:
    """A survey sheet must map to exactly one waterbody across the table."""
    pdf_to_titles: Dict[str, List[str]] = {}
    for e in overrides:
        for pdf in e.get("pdfs", []):
            pdf_to_titles.setdefault(pdf.lower(), []).append(e["map_title"])
    clashes = {pdf: t for pdf, t in pdf_to_titles.items() if len(t) > 1}
    assert not clashes, f"PDF sheet(s) claimed by multiple overrides: {clashes}"


@pytest.mark.skipif(
    not _BATHY_DIR.exists(), reason="deploy bathymetry tree not built"
)
def test_referenced_pdfs_exist_on_disk(overrides: List[Dict[str, object]]) -> None:
    missing = [
        pdf
        for e in overrides
        for pdf in e.get("pdfs", [])
        if not (_BATHY_DIR / pdf).exists()
    ]
    assert not missing, f"override PDFs absent from {_BATHY_DIR}: {missing}"
