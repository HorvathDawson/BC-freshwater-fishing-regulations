"""Topology graph tests (03 S3-4). Includes the two braiding/lake regression cases."""

import pytest

pytestmark = pytest.mark.skip(reason="pipeline.sections.topology not implemented yet")


def test_lake_node_collapse_no_orphans():
    """Every under-lake segment folds into its lake node; no orphaned segments, no double-count."""


def test_reverse_adjacency_is_upstream():
    """up_adj walks toward headwaters; down_adj toward the mouth."""


def test_chehalis_harrison_no_leak_via_wsc_filter():
    """An upstream walk from the Chehalis mouth must NOT include Harrison River segments.
    Spike (10 S1): SCC condensation is a no-op (FWA is already a DAG); the WSC-DESCENDANT
    filter is what stops the leak (145/145 Chehalis, 0 Harrison). Assert the filter is applied."""


def test_kootenay_columbia_no_leak_via_2300_barrier():
    """A walk crossing the Kootenay/Columbia canal must be stopped by the EDGE_TYPE=2300
    barrier (blk 356366076). Spike (10 S2): lake-collapse does NOT work (canal bypasses the
    lake polygon, catches 0/6 segs); the 2300 rule is required (leak 86->0)."""


def test_missing_edge_type_raises():
    """A segment lacking edge_type must raise (strict guard preserved from legacy)."""
