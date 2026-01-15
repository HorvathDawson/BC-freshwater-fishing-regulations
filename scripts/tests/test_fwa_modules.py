"""
Unit tests for FWA modules.

Run with: python -m pytest tests/test_fwa_modules.py -v
"""

import pytest
import attrs
from fwa_modules.utils import clean_watershed_code, get_parent_code, get_code_depth
from fwa_modules.models import (
    StreamNode,
    StreamEdge,
    TributaryAssignment,
    ZoneAssignment,
    StreamMetadata,
)


class TestWatershedCodeUtils:
    """Test watershed code parsing utilities."""

    def test_clean_watershed_code_valid(self):
        """Test cleaning valid watershed codes."""
        assert clean_watershed_code("100-239874-000000") == "100-239874"
        assert clean_watershed_code("100-239874-112384") == "100-239874-112384"
        assert clean_watershed_code("200-000000-000000") == "200"

    def test_clean_watershed_code_invalid(self):
        """Test handling of invalid watershed codes."""
        assert clean_watershed_code(None) is None
        assert clean_watershed_code(123) is None
        assert clean_watershed_code("") is None

    def test_get_parent_code(self):
        """Test parent code extraction."""
        assert get_parent_code("100-239874-112384") == "100-239874"
        assert get_parent_code("100-239874") == "100"
        assert get_parent_code("100") is None
        assert get_parent_code(None) is None

    def test_get_code_depth(self):
        """Test depth calculation."""
        assert get_code_depth("100") == 1
        assert get_code_depth("100-239874") == 2
        assert get_code_depth("100-239874-112384") == 3
        assert get_code_depth("100-000000-000000") == 1  # Zeros don't count
        assert get_code_depth(None) == 0


class TestStreamNode:
    """Test StreamNode dataclass."""

    def test_create_basic_node(self):
        """Test creating a basic stream node."""
        node = StreamNode(
            node_id="test_1_start",
            position=(1234567.0, 987654.0),
            watershed_code="100-239874",
        )

        assert node.node_id == "test_1_start"
        assert node.position == (1234567.0, 987654.0)
        assert node.watershed_code == "100-239874"
        assert node.is_lake_node is False
        assert node.lake_name is None

    def test_create_lake_node(self):
        """Test creating a lake node."""
        node = StreamNode(
            node_id="test_2_end",
            position=(1234567.0, 987654.0),
            watershed_code="100-239874",
            is_lake_node=True,
            lake_name="Harrison Lake",
            lake_poly_id=12345,
        )

        assert node.is_lake_node is True
        assert node.lake_name == "Harrison Lake"
        assert node.lake_poly_id == 12345

    def test_node_immutable(self):
        """Test that nodes are immutable (frozen)."""
        node = StreamNode(node_id="test", position=(0.0, 0.0), watershed_code="100")

        with pytest.raises(attrs.exceptions.FrozenInstanceError):
            node.node_id = "modified"

    def test_node_hashable(self):
        """Test that nodes are hashable (can be dict keys)."""
        node1 = StreamNode(node_id="test1", position=(0.0, 0.0), watershed_code="100")

        node2 = StreamNode(node_id="test1", position=(0.0, 0.0), watershed_code="100")

        # Same values should be equal
        assert node1 == node2

        # Should be hashable
        node_set = {node1, node2}
        assert len(node_set) == 1


class TestStreamEdge:
    """Test StreamEdge dataclass."""

    def test_create_named_edge(self):
        """Test creating a named stream edge."""
        edge = StreamEdge(
            linear_feature_id="12345",
            gnis_name="Fraser River",
            clean_code="100-239874",
            parent_code="100",
            route_measure=1500.0,
            layer_name="100A",
            start_node_id="100A_1_start",
            end_node_id="100A_1_end",
        )

        assert edge.linear_feature_id == "12345"
        assert edge.gnis_name == "Fraser River"
        assert edge.is_named is True

    def test_create_unnamed_edge(self):
        """Test creating an unnamed stream edge."""
        edge = StreamEdge(
            linear_feature_id="12346",
            gnis_name=None,
            clean_code="100-239874-112384",
            parent_code="100-239874",
            route_measure=500.0,
            layer_name="100A",
            start_node_id="100A_2_start",
            end_node_id="100A_2_end",
        )

        assert edge.gnis_name is None
        assert edge.is_named is False

    def test_edge_with_empty_name(self):
        """Test that empty string is not considered named."""
        edge = StreamEdge(
            linear_feature_id="12347",
            gnis_name="  ",  # Whitespace only
            clean_code="100",
            parent_code=None,
            route_measure=0.0,
            layer_name="100A",
            start_node_id="100A_3_start",
            end_node_id="100A_3_end",
        )

        assert edge.is_named is False


class TestTributaryAssignment:
    """Test TributaryAssignment dataclass."""

    def test_create_river_tributary(self):
        """Test creating a river tributary assignment."""
        assignment = TributaryAssignment(
            linear_feature_id="12345", tributary_of="Fraser River", distance_to_named=1
        )

        assert assignment.tributary_of == "Fraser River"
        assert assignment.lake_poly_id is None
        assert assignment.distance_to_named == 1

    def test_create_lake_tributary(self):
        """Test creating a lake tributary assignment."""
        assignment = TributaryAssignment(
            linear_feature_id="12346", tributary_of="Harrison Lake", lake_poly_id=98765
        )

        assert assignment.tributary_of == "Harrison Lake"
        assert assignment.lake_poly_id == 98765

    def test_create_lake_segment(self):
        """Test creating a lake segment (inside lake, not tributary)."""
        assignment = TributaryAssignment(linear_feature_id="12347", lake_poly_id=98765)

        assert assignment.tributary_of is None
        assert assignment.lake_poly_id == 98765


class TestZoneAssignment:
    """Test ZoneAssignment dataclass."""

    def test_create_single_zone(self):
        """Test feature in single zone."""
        assignment = ZoneAssignment()
        assignment.add_zone("2", 1.0)

        assert assignment.primary_zone() == "2"
        assert assignment.is_clipped is False
        assert len(assignment.zones) == 1

    def test_create_multi_zone(self):
        """Test feature spanning multiple zones."""
        assignment = ZoneAssignment()
        assignment.add_zone("2", 0.6)
        assignment.add_zone("3", 0.3)
        assignment.add_zone("1", 0.1)

        # Should be sorted by proportion
        assert assignment.zones == ["2", "3", "1"]
        assert assignment.proportions == [0.6, 0.3, 0.1]
        assert assignment.primary_zone() == "2"
        assert assignment.is_clipped is True

    def test_add_zone_sorts(self):
        """Test that adding zones keeps them sorted."""
        assignment = ZoneAssignment()
        assignment.add_zone("3", 0.2)
        assignment.add_zone("1", 0.5)
        assignment.add_zone("2", 0.3)

        # Should auto-sort by proportion descending
        assert assignment.zones == ["1", "2", "3"]
        assert assignment.proportions == [0.5, 0.3, 0.2]


class TestStreamMetadata:
    """Test StreamMetadata dataclass."""

    def test_create_metadata(self):
        """Test creating stream metadata."""
        metadata = StreamMetadata(
            linear_feature_id="12345",
            watershed_code="100-239874",
            gnis_name="Fraser River",
            route_measure=1500.0,
            layer_name="100A",
            start_point=(1234567.0, 987654.0),
            end_point=(1234600.0, 987700.0),
        )

        assert metadata.linear_feature_id == "12345"
        assert metadata.start_point == (1234567.0, 987654.0)
        assert metadata.end_point == (1234600.0, 987700.0)

    def test_metadata_immutable(self):
        """Test that metadata is immutable."""
        metadata = StreamMetadata(
            linear_feature_id="12345",
            watershed_code="100",
            gnis_name=None,
            route_measure=0.0,
            layer_name="test",
            start_point=(0.0, 0.0),
            end_point=(1.0, 1.0),
        )

        with pytest.raises(attrs.exceptions.FrozenInstanceError):
            metadata.linear_feature_id = "modified"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
