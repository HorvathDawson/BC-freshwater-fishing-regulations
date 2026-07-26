"""BLK-chain build tests (03 S1)."""

import pytest

pytestmark = pytest.mark.skip(reason="pipeline.sections.blk_chains not implemented yet")


def test_fids_ordered_and_contiguous():
    """Fids sort by DOWNSTREAM_ROUTE_MEASURE and consecutive fids share an endpoint vertex."""


def test_route_span_matches_length():
    """up_m - down_m == geom length (~cm) for each fid span."""


def test_waterbody_runs_from_wbk_not_edge_type():
    """Under-lake runs come from WATERBODY_KEY in lakes/manmade; double-line rivers stay open."""


def test_sentinel_blk_skipped():
    """999-999999 sentinel WSC/BLKs are excluded."""
