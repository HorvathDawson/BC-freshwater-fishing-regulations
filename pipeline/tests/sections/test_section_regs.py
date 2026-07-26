"""Section matching + MU-overlay tests (07, 08)."""

import pytest

pytestmark = pytest.mark.skip(reason="section match step not implemented yet")


def test_similkameen_single_section_across_mu():
    """A river crossing an MU boundary with no specific difference stays ONE section."""


def test_fraser_curated_mu_boundary_split():
    """Fraser's per-zone differences come from curated mu_boundary splits -> one reg set each."""


def test_base_regs_are_mu_overlay_not_per_section():
    """Zone-wide/provincial regs resolve to mu_id -> base_reg_set, not stored on sections."""


def test_effective_includes_tributaries_ported():
    """effective_includes_tributaries logic matches the legacy behavior (parse-level)."""
