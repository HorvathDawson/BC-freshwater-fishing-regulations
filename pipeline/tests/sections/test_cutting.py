"""Geometry-cut + section_id tests (03 S5, 02)."""

import pytest

pytestmark = pytest.mark.skip(reason="pipeline.sections.cutting not implemented yet")


def test_substring_accuracy_cm():
    """A cut at route measure M lands within ~cm of the intended point."""


def test_two_point_linestring_fallback():
    """2-point LineStrings fall back to the nearest fid boundary (cannot measure-cut)."""


def test_section_id_stable_under_unrelated_split():
    """Adding a split on a different stretch does NOT change an unrelated section's id."""


def test_section_id_changes_when_split_inside():
    """A split inside a section mints two new ids (expected)."""
