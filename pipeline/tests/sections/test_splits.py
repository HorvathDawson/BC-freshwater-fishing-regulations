"""Split anchor resolution tests (04)."""

import pytest

pytestmark = pytest.mark.skip(reason="pipeline.sections.splits/anchors not implemented yet")


def test_each_anchor_type_resolves_to_route_measure():
    """lake / confluence / linear_feature_id / landmark / point / border / mu_boundary -> (blk, measure)."""


def test_barrier_split_becomes_topology_node():
    """barrier:true inserts a barrier node and splits the segment; non-barrier does not."""


def test_resolved_sidecar_is_deterministic():
    """splits.resolved.json is stable across runs for the same inputs."""
