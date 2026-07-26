"""(name, source) tuple tests (03 S2)."""

import pytest

pytestmark = pytest.mark.skip(reason="pipeline.sections.names not implemented yet")


def test_name_source_priority_order():
    """Display name = override > gazette > side-channel > upstream-inherited."""


def test_side_channel_inherits_main_name():
    """Seabird side channel (unnamed BLK) gets (Fraser, side-channel) from the same-WSC main BLK."""


def test_upstream_inherited_for_unnamed_headwaters():
    """Still-unnamed BLKs inherit the nearest upstream named edge's name, tagged accordingly."""
