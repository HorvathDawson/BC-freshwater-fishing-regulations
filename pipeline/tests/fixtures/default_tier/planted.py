"""(T1193) four planted tests for test_default_tier.py: one of each tier. Named outside `test_*.py`, so only a run that
names this file collects it. needs_tiles has no autouse data check (conftest.py `_the_marked_data`), so it passes
without data when a run selects it."""
import pytest


def test_no_data():
    pass


@pytest.mark.needs_tiles
def test_needs_data():
    pass


@pytest.mark.slow
def test_slow():
    pass


@pytest.mark.needs_cache
@pytest.mark.slow
def test_slow_and_needs_data():
    pass
