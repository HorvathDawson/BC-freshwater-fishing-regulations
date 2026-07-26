"""Step 5 (03 S5, 04): cut BLK chains into Sections at lakes + splits.

Cut ONLY at lake boundaries and split points (never at tributary confluences). Generate
``location_identifier`` from the two bounds (04 table), recompute per-section min_zoom from
the section's own max magnitude, and validate: coverage == BLK coverage (no gaps) and
location_identifier uniqueness within a gnis (fail loud).
"""

from __future__ import annotations

from .models import BlkChain, Section, SplitPoint


def build_sections(chains: list[BlkChain], split_points: list[SplitPoint],
                   lake_manmade_wbks: set[str]) -> list[Section]:
    """Return sections with cut geometry, bounds, location_identifier, min_zoom."""
    raise NotImplementedError
