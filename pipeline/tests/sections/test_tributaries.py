"""Tributary reachability tests (03 S6, 08)."""

import pytest

pytestmark = pytest.mark.skip(reason="pipeline.sections.tributaries not implemented yet")


def test_upstream_walk_stops_at_lake_barrier():
    """From the Adams upstream section, the walk does not cross Adams Lake."""


def test_tributary_only_excludes_named_section():
    """tributary_only regs assign only the trib set, not the named section itself."""


def test_directionality_prevents_mainstem_backtracking():
    """On the condensed DAG, an upstream walk never descends the mainstem (no WSC exclusion)."""


def test_seed_set_cache():
    """Repeated walks with the same seed set hit the cache."""
