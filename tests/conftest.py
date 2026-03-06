"""
Shared fixtures for regulation_mapping test suite.

Provides lightweight fakes for PipelineResult, MetadataGazetteer, and
MergedGroup so that tests can exercise the store / exporter logic without
loading real GPKG data.
"""

from __future__ import annotations

import tempfile
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from unittest.mock import MagicMock

import pytest
from shapely.geometry import LineString, Point, Polygon

from fwa_pipeline.metadata_builder import FeatureType
from regulation_mapping.regulation_types import MergedGroup, PipelineResult


# ---------------------------------------------------------------------------
# Lightweight fake gazetteer
# ---------------------------------------------------------------------------


class FakeGazetteer:
    """Mimics the subset of MetadataGazetteer used by CanonicalDataStore."""

    def __init__(
        self,
        stream_metadata: Optional[Dict[str, dict]] = None,
        valid_stream_ids: Optional[set] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        self._stream_metadata = stream_metadata or {}
        self._valid_stream_ids = valid_stream_ids or set(self._stream_metadata.keys())
        # FeatureType → {id → meta}
        self.metadata: dict = metadata or {}
        # Mimic the data_accessor attribute
        self.data_accessor = None
        # Mimic the reprojected admin cache
        self._reprojected_admin_cache: dict = {}

    def get_valid_stream_ids(self) -> set:
        return self._valid_stream_ids

    def get_stream_metadata(self, fid: str) -> Optional[dict]:
        return self._stream_metadata.get(fid)

    def get_features(self) -> list:
        return []


# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------


def make_merged_group(
    group_id: str = "g1",
    feature_ids: tuple = ("f1",),
    regulation_ids: tuple = ("reg_001_rule0",),
    feature_type: str = FeatureType.STREAM.value,
    gnis_name: str = "Test Creek",
    waterbody_key: Optional[str] = None,
    blue_line_key: Optional[str] = "BLK_100",
    fwa_watershed_code: Optional[str] = "100-000000",
    zones: tuple = ("3",),
    mgmt_units: tuple = ("3-15",),
    region_names: tuple = ("Thompson",),
    name_variants: tuple = (),
    display_name_override: str = "",
    inherited_gnis_name: str = "",
) -> MergedGroup:
    """Create a ``MergedGroup`` with sensible defaults for testing."""
    return MergedGroup(
        group_id=group_id,
        feature_ids=feature_ids,
        regulation_ids=regulation_ids,
        feature_type=feature_type,
        gnis_name=gnis_name,
        waterbody_key=waterbody_key,
        blue_line_key=blue_line_key,
        fwa_watershed_code=fwa_watershed_code,
        feature_count=len(feature_ids),
        zones=zones,
        mgmt_units=mgmt_units,
        region_names=region_names,
        name_variants=name_variants,
        display_name_override=display_name_override,
        inherited_gnis_name=inherited_gnis_name,
    )


def make_pipeline_result(
    merged_groups: Optional[Dict[str, MergedGroup]] = None,
    feature_to_regs: Optional[Dict[str, List[str]]] = None,
    regulation_names: Optional[Dict[str, str]] = None,
    feature_to_linked_regulation: Optional[Dict[str, Set[str]]] = None,
    gazetteer: Optional[Any] = None,
    admin_area_reg_map: Optional[Dict[str, Dict[str, set]]] = None,
    admin_regulation_ids: Optional[set] = None,
    regulation_details: Optional[Dict[str, Dict[str, Any]]] = None,
) -> PipelineResult:
    """Create a ``PipelineResult`` with sensible defaults for testing."""
    return PipelineResult(
        merged_groups=merged_groups or {},
        feature_to_regs=feature_to_regs or {},
        regulation_names=regulation_names or {},
        feature_to_linked_regulation=feature_to_linked_regulation or defaultdict(set),
        gazetteer=gazetteer or FakeGazetteer(),
        admin_area_reg_map=admin_area_reg_map or {},
        admin_regulation_ids=admin_regulation_ids or set(),
        regulation_details=regulation_details or {},
    )


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------


def make_line(
    x_start: float = 0, y_start: float = 0, length: float = 1000
) -> LineString:
    """Create a simple horizontal LineString in EPSG:3005-ish coordinates."""
    return LineString([(x_start, y_start), (x_start + length, y_start)])


def make_polygon(x: float = 0, y: float = 0, size: float = 1000) -> Polygon:
    """Create a simple square Polygon in EPSG:3005-ish coordinates."""
    return Polygon(
        [
            (x, y),
            (x + size, y),
            (x + size, y + size),
            (x, y + size),
            (x, y),
        ]
    )


# ---------------------------------------------------------------------------
# Temp directory fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_dir(tmp_path):
    """Provide a clean temporary directory for test outputs."""
    return tmp_path
