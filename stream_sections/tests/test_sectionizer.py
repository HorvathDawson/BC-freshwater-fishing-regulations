"""Section cutting tests (03 S5, 04)."""

import pytest

pytestmark = pytest.mark.skip(reason="pipeline.sections.sectionizer not implemented yet")


def test_coverage_equals_blk_coverage():
    """Sections of a BLK cover it end-to-end with no gaps or overlaps."""


def test_adams_river_splits_at_lake():
    """Adams River yields 'downstream of Adams Lake' + 'upstream of Adams Lake' sections."""


def test_location_identifier_table():
    """0 splits -> None; 1 split -> up/downstream of X; 2 splits -> between X and Y (04 table)."""


def test_location_identifier_unique_within_gnis():
    """Duplicate generated identifiers within a gnis fail loud."""


def test_per_section_minzoom_from_own_magnitude():
    """min_zoom recomputed from the section's own max magnitude, not per-BLK."""
