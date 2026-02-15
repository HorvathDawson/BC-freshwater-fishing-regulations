"""
Tests for MetadataGazetteer

Tests loading and querying the stream_metadata.pickle file.
"""

import pytest
import pickle
from pathlib import Path
from fwa_modules.linking import (
    MetadataGazetteer,
    FWAFeature,
    WaterbodyLinker,
    LinkStatus,
)


# ==========================================
#       FIXTURES
# ==========================================


@pytest.fixture
def sample_metadata():
    """Sample metadata structure matching stream_metadata.pickle format."""
    return {
        "zone_metadata": {
            "4": {
                "zone_number": "4",
                "mgmt_units": ["4-15", "4-16"],
                "total_mgmt_units": 2,
                "mgmt_unit_details": {
                    "4-15": {
                        "full_id": "4-15",
                        "region_name": "Kootenay Region",
                        "game_zone_id": "4-15",
                        "game_zone_name": "Elk",
                        "bounds": [115.0, 49.0, 116.0, 50.0],
                    }
                },
            },
            "2": {
                "zone_number": "2",
                "mgmt_units": ["2-10", "2-11"],
                "total_mgmt_units": 2,
                "mgmt_unit_details": {},
            },
        },
        "streams": {
            "100001": {
                "linear_feature_id": "100001",
                "gnis_name": "Elk River",
                "fwa_watershed_code": "100-abc-123",
                "waterbody_key": "1001",
                "stream_order": 5,
                "zones": ["4"],
                "mgmt_units": ["4-15"],
                "cross_boundary": False,
            },
            "100002": {
                "linear_feature_id": "100002",
                "gnis_name": "Michel Creek",
                "fwa_watershed_code": "100-abc-456",
                "waterbody_key": "1002",
                "stream_order": 3,
                "zones": ["4"],
                "mgmt_units": ["4-15"],
                "cross_boundary": False,
            },
            "200001": {
                "linear_feature_id": "200001",
                "gnis_name": "Mill Creek",
                "fwa_watershed_code": "200-def-789",
                "waterbody_key": "2001",
                "stream_order": 2,
                "zones": ["2"],
                "mgmt_units": ["2-10"],
                "cross_boundary": False,
            },
            "400001": {
                "linear_feature_id": "400001",
                "gnis_name": "Mill Creek",  # Same name, different zone
                "fwa_watershed_code": "400-ghi-000",
                "waterbody_key": "4001",
                "stream_order": 2,
                "zones": ["4"],
                "mgmt_units": ["4-16"],
                "cross_boundary": False,
            },
        },
        "lakes": {
            "500001": {
                "waterbody_key": "500001",
                "gnis_name": "Adams Lake",
                "feature_type": "lakes",
                "zones": ["3"],
                "mgmt_units": ["3-12"],
            },
            "200002": {
                "waterbody_key": "200002",
                "gnis_name": "Alouette Lake",
                "feature_type": "lakes",
                "zones": ["2"],
                "mgmt_units": ["2-11"],
            },
        },
        "wetlands": {},
        "manmade": {},
    }


@pytest.fixture
def temp_metadata_file(tmp_path, sample_metadata):
    """Create temporary metadata pickle file."""
    metadata_path = tmp_path / "test_metadata.pickle"

    with open(metadata_path, "wb") as f:
        pickle.dump(sample_metadata, f)

    return metadata_path


@pytest.fixture
def metadata_gazetteer(temp_metadata_file):
    """MetadataGazetteer loaded with test data."""
    return MetadataGazetteer(temp_metadata_file)


# ==========================================
#       TEST 1: LOADING & INDEXING
# ==========================================


def test_load_metadata_file(temp_metadata_file, sample_metadata):
    """Test: Can load metadata pickle file."""
    gazetteer = MetadataGazetteer(temp_metadata_file)

    assert gazetteer.metadata is not None
    assert len(gazetteer.metadata["streams"]) == 4
    assert len(gazetteer.metadata["lakes"]) == 2
    assert len(gazetteer.metadata["zone_metadata"]) == 2


def test_build_name_index(metadata_gazetteer):
    """Test: Name index built from streams and lakes."""
    # Should have indexed: Elk River, Michel Creek, Mill Creek (2x), Adams Lake, Alouette Lake
    assert len(metadata_gazetteer.name_index) == 5  # 5 unique names

    # Check specific entries
    assert "Elk River" in metadata_gazetteer.name_index
    assert "Michel Creek" in metadata_gazetteer.name_index
    assert "Mill Creek" in metadata_gazetteer.name_index
    assert "Adams Lake" in metadata_gazetteer.name_index
    assert "Alouette Lake" in metadata_gazetteer.name_index


def test_index_contains_fwa_features(metadata_gazetteer):
    """Test: Index contains FWAFeature objects."""
    elk_river_features = metadata_gazetteer.name_index["Elk River"]

    assert len(elk_river_features) == 1
    assert isinstance(elk_river_features[0], FWAFeature)
    assert elk_river_features[0].name == "Elk River"
    assert elk_river_features[0].fwa_id == "100001"
    assert elk_river_features[0].geometry_type == "multilinestring"
    assert "4" in elk_river_features[0].zones


# ==========================================
#       TEST 2: SEARCH BY NAME
# ==========================================


def test_search_unique_stream_name(metadata_gazetteer):
    """Test: Search for unique stream name returns single result."""
    results = metadata_gazetteer.search("Elk River")

    assert len(results) == 1
    assert results[0].name == "Elk River"
    assert results[0].fwa_id == "100001"


def test_search_unique_lake_name(metadata_gazetteer):
    """Test: Search for unique lake name returns single result."""
    results = metadata_gazetteer.search("Adams Lake")

    assert len(results) == 1
    assert results[0].name == "Adams Lake"
    assert results[0].geometry_type == "polygon"


def test_search_ambiguous_name(metadata_gazetteer):
    """Test: Search for ambiguous name returns multiple results."""
    results = metadata_gazetteer.search("Mill Creek")

    assert len(results) == 2
    fwa_ids = {r.fwa_id for r in results}
    assert "200001" in fwa_ids  # Region 2
    assert "400001" in fwa_ids  # Region 4


def test_search_with_region_filter(metadata_gazetteer):
    """Test: Region filter resolves ambiguity."""
    results = metadata_gazetteer.search("Mill Creek", region="4")

    assert len(results) == 1
    assert results[0].fwa_id == "400001"
    assert "4" in results[0].zones


def test_search_case_insensitive(metadata_gazetteer):
    """Test: Search is case-insensitive."""
    results_upper = metadata_gazetteer.search("ELK RIVER")
    results_lower = metadata_gazetteer.search("elk river")
    results_mixed = metadata_gazetteer.search("Elk River")

    assert len(results_upper) == 1
    assert len(results_lower) == 1
    assert len(results_mixed) == 1
    assert results_upper[0].fwa_id == results_lower[0].fwa_id == results_mixed[0].fwa_id


def test_search_not_found(metadata_gazetteer):
    """Test: Search for non-existent name returns empty list."""
    results = metadata_gazetteer.search("Nonexistent Creek")

    assert len(results) == 0


# ==========================================
#       TEST 3: METADATA RETRIEVAL
# ==========================================


def test_get_stream_metadata(metadata_gazetteer):
    """Test: Can retrieve full stream metadata by linear_feature_id."""
    metadata = metadata_gazetteer.get_stream_metadata("100001")

    assert metadata is not None
    assert metadata["gnis_name"] == "Elk River"
    assert metadata["fwa_watershed_code"] == "100-abc-123"
    assert metadata["zones"] == ["4"]
    assert metadata["stream_order"] == 5


def test_get_polygon_metadata(metadata_gazetteer):
    """Test: Can retrieve full polygon metadata by waterbody_key."""
    metadata = metadata_gazetteer.get_polygon_metadata("500001")

    assert metadata is not None
    assert metadata["gnis_name"] == "Adams Lake"
    assert metadata["feature_type"] == "lakes"
    assert metadata["zones"] == ["3"]


def test_get_polygon_metadata_with_type_filter(metadata_gazetteer):
    """Test: Can filter polygon metadata by feature type."""
    metadata = metadata_gazetteer.get_polygon_metadata("500001", feature_type="lakes")

    assert metadata is not None
    assert metadata["feature_type"] == "lakes"

    # Should not find in wetlands
    metadata_wetlands = metadata_gazetteer.get_polygon_metadata(
        "500001", feature_type="wetlands"
    )
    assert metadata_wetlands is None


def test_get_zone_metadata(metadata_gazetteer):
    """Test: Can retrieve zone metadata."""
    zone_meta = metadata_gazetteer.get_zone_metadata("4")

    assert zone_meta is not None
    assert zone_meta["zone_number"] == "4"
    assert len(zone_meta["mgmt_units"]) == 2
    assert "4-15" in zone_meta["mgmt_units"]


def test_get_features_in_zone(metadata_gazetteer):
    """Test: Can get all features in a zone."""
    streams_in_zone_4 = metadata_gazetteer.get_features_in_zone(
        "4", feature_type="streams"
    )

    assert len(streams_in_zone_4) == 3  # Elk River, Michel Creek, Mill Creek
    names = {s["gnis_name"] for s in streams_in_zone_4}
    assert "Elk River" in names
    assert "Michel Creek" in names
    assert "Mill Creek" in names


# ==========================================
#       TEST 4: INTEGRATION WITH LINKER
# ==========================================


def test_linker_with_metadata_gazetteer(metadata_gazetteer):
    """Test: WaterbodyLinker works with MetadataGazetteer."""
    linker = WaterbodyLinker(metadata_gazetteer)

    result = linker.link_waterbody("Elk River", region="Region 4")

    assert result.status == LinkStatus.SUCCESS
    assert result.matched_feature.name == "Elk River"
    assert result.matched_feature.fwa_id == "100001"


def test_linker_handles_ambiguous_with_region(metadata_gazetteer):
    """Test: Linker resolves ambiguity with region filter."""
    linker = WaterbodyLinker(metadata_gazetteer)

    result = linker.link_waterbody("Mill Creek", region="Region 2")

    assert result.status == LinkStatus.SUCCESS
    assert result.matched_feature.fwa_id == "200001"


def test_linker_ambiguous_without_region(metadata_gazetteer):
    """Test: Linker returns ambiguous without region filter."""
    linker = WaterbodyLinker(metadata_gazetteer)

    result = linker.link_waterbody("Mill Creek")

    assert result.status == LinkStatus.AMBIGUOUS
    assert len(result.candidate_features) == 2


def test_end_to_end_with_real_metadata_structure(temp_metadata_file):
    """
    Test: End-to-end workflow with metadata gazetteer.

    Simulates MVP linking process.
    """
    # Create gazetteer from metadata
    gazetteer = MetadataGazetteer(temp_metadata_file)

    # Create linker
    linker = WaterbodyLinker(gazetteer)

    # Link main waterbody
    result = linker.link_waterbody("ELK RIVER", region="Region 4")

    assert result.is_success

    # Get full metadata for linked feature
    stream_metadata = gazetteer.get_stream_metadata(result.matched_feature.fwa_id)

    assert stream_metadata["zones"] == ["4"]
    assert stream_metadata["mgmt_units"] == ["4-15"]
    assert stream_metadata["fwa_watershed_code"] == "100-abc-123"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
