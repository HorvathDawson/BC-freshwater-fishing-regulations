"""Validation tests for curated override and feature display name data.

These tests load the canonical overrides.json and feature_display_names.json
and verify structural integrity, ensuring every entry round-trips through
the data models and satisfies business rules.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

import pytest

from regulation_mapping_v2.matching.match_table import (
    FEATURE_DISPLAY_NAMES_PATH,
    FeatureDisplayName,
    OVERRIDES_PATH,
    OverrideEntry,
)

# ---------------------------------------------------------------------------
# Known valid regions (v2 naming convention)
# ---------------------------------------------------------------------------

_VALID_REGIONS = {
    "REGION 1 - Vancouver Island",
    "REGION 2 - Lower Mainland",
    "REGION 3 - Thompson-Nicola",
    "REGION 4 - Kootenay",
    "REGION 5 - Cariboo",
    "REGION 6 - Skeena",
    "REGION 7A - Omineca",
    "REGION 7B - Peace",
    "REGION 8 - Okanagan",
}

# MU codes follow the pattern: digit(s) - digit(s)  e.g. "4-3", "7A-38"
_MU_PATTERN = re.compile(r"^\d+[A-Za-z]?-\d+$")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def raw_overrides() -> List[Dict[str, Any]]:
    """Load raw JSON data from overrides.json."""
    assert OVERRIDES_PATH.exists(), f"overrides.json not found at {OVERRIDES_PATH}"
    with open(OVERRIDES_PATH, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def overrides(raw_overrides: List[Dict[str, Any]]) -> List[OverrideEntry]:
    """Deserialise every raw dict into an OverrideEntry."""
    return [OverrideEntry.from_dict(d) for d in raw_overrides]


@pytest.fixture(scope="module")
def raw_feature_display_names() -> List[Dict[str, Any]]:
    """Load raw JSON data from feature_display_names.json."""
    if not FEATURE_DISPLAY_NAMES_PATH.exists():
        pytest.skip("feature_display_names.json not present")
    with open(FEATURE_DISPLAY_NAMES_PATH, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Structural tests — overrides.json
# ---------------------------------------------------------------------------


class TestOverridesDeserialization:
    """Every entry must round-trip through from_dict / to_dict."""

    def test_all_entries_deserialise(
        self, raw_overrides: List[Dict[str, Any]]
    ) -> None:
        """from_dict should not raise for any entry."""
        for i, raw in enumerate(raw_overrides):
            try:
                OverrideEntry.from_dict(raw)
            except Exception as exc:
                pytest.fail(f"Entry {i} failed from_dict: {exc}\n{raw}")

    def test_round_trip(self, overrides: List[OverrideEntry]) -> None:
        """to_dict → from_dict should produce an equivalent object."""
        for i, entry in enumerate(overrides):
            rebuilt = OverrideEntry.from_dict(entry.to_dict())
            assert rebuilt.criteria.name_verbatim == entry.criteria.name_verbatim, (
                f"Entry {i}: name_verbatim mismatch after round-trip"
            )
            assert rebuilt.criteria.region == entry.criteria.region
            assert rebuilt.criteria.mus == entry.criteria.mus
            assert rebuilt.gnis_ids == entry.gnis_ids
            assert rebuilt.skip == entry.skip


# ---------------------------------------------------------------------------
# Business rule tests
# ---------------------------------------------------------------------------


class TestOverrideBusinessRules:
    """Validate override entries against pipeline business rules."""

    def test_non_skip_entries_have_identifier(
        self, overrides: List[OverrideEntry]
    ) -> None:
        """Every non-skip entry must resolve to at least one feature."""
        missing = []
        for i, e in enumerate(overrides):
            if not e.skip and not e.has_match:
                missing.append(
                    f"  [{i}] {e.criteria.name_verbatim} "
                    f"({e.criteria.region}, MUs={e.criteria.mus})"
                )
        assert not missing, (
            f"{len(missing)} non-skip entries have no identifier:\n"
            + "\n".join(missing)
        )

    def test_no_duplicate_criteria_keys(
        self, overrides: List[OverrideEntry]
    ) -> None:
        """No two entries should share the same (region, name, mus) key."""
        seen: Counter = Counter()
        for e in overrides:
            key = (e.criteria.region, e.criteria.name_verbatim, tuple(sorted(e.criteria.mus)))
            seen[key] += 1
        dupes = {k: v for k, v in seen.items() if v > 1}
        assert not dupes, (
            f"{len(dupes)} duplicate criteria keys:\n"
            + "\n".join(f"  {k}: {v}x" for k, v in dupes.items())
        )

    def test_criteria_name_not_empty(
        self, overrides: List[OverrideEntry]
    ) -> None:
        for i, e in enumerate(overrides):
            assert e.criteria.name_verbatim.strip(), (
                f"Entry {i} has empty name_verbatim"
            )

    def test_criteria_region_valid(
        self, overrides: List[OverrideEntry]
    ) -> None:
        """Every entry must have a region from the known set."""
        bad = []
        for i, e in enumerate(overrides):
            if e.criteria.region not in _VALID_REGIONS:
                bad.append(f"  [{i}] region={e.criteria.region!r}")
        assert not bad, (
            f"{len(bad)} entries with unknown region:\n" + "\n".join(bad)
        )

    def test_mu_format(self, overrides: List[OverrideEntry]) -> None:
        """MU codes should match the expected pattern (e.g. '4-3')."""
        bad = []
        for i, e in enumerate(overrides):
            for mu in e.criteria.mus:
                if not _MU_PATTERN.match(mu):
                    bad.append(f"  [{i}] {e.criteria.name_verbatim}: mu={mu!r}")
        assert not bad, (
            f"{len(bad)} entries with malformed MU codes:\n" + "\n".join(bad)
        )

    def test_skip_entries_have_reason(
        self, overrides: List[OverrideEntry]
    ) -> None:
        """Skip entries should explain why they are skipped."""
        missing = []
        for i, e in enumerate(overrides):
            if e.skip and not e.skip_reason:
                missing.append(
                    f"  [{i}] {e.criteria.name_verbatim} (skip=True, no reason)"
                )
        assert not missing, (
            f"{len(missing)} skip entries without skip_reason:\n"
            + "\n".join(missing)
        )


# ---------------------------------------------------------------------------
# Field-level validation
# ---------------------------------------------------------------------------


class TestOverrideFieldIntegrity:
    """Validate individual field values."""

    def test_gnis_ids_are_numeric_strings(
        self, overrides: List[OverrideEntry]
    ) -> None:
        bad = []
        for i, e in enumerate(overrides):
            for gid in e.gnis_ids:
                if not gid.isdigit():
                    bad.append(f"  [{i}] {e.criteria.name_verbatim}: gnis_id={gid!r}")
        assert not bad, (
            f"{len(bad)} non-numeric gnis_ids:\n" + "\n".join(bad)
        )

    def test_name_variants_not_duplicate_primary(
        self, overrides: List[OverrideEntry]
    ) -> None:
        """name_variants should not repeat the primary name_verbatim."""
        bad = []
        for i, e in enumerate(overrides):
            if e.criteria.name_verbatim in e.name_variants:
                bad.append(
                    f"  [{i}] {e.criteria.name_verbatim} appears in its own name_variants"
                )
        assert not bad, (
            f"{len(bad)} entries with primary name in name_variants:\n"
            + "\n".join(bad)
        )

    def test_admin_targets_structure(
        self, overrides: List[OverrideEntry]
    ) -> None:
        """admin_targets dicts must have 'layer' and 'feature_id'."""
        bad = []
        for i, e in enumerate(overrides):
            for j, target in enumerate(e.admin_targets):
                if "layer" not in target or "feature_id" not in target:
                    bad.append(
                        f"  [{i}] {e.criteria.name_verbatim} admin_target[{j}]: {target}"
                    )
        assert not bad, (
            f"{len(bad)} admin_targets missing required keys:\n" + "\n".join(bad)
        )

    def test_all_entries_have_type_override(
        self, raw_overrides: List[Dict[str, Any]]
    ) -> None:
        """Every raw dict should have type='override'."""
        bad = [
            i for i, d in enumerate(raw_overrides) if d.get("type") != "override"
        ]
        assert not bad, f"Entries without type='override': indices {bad}"


# ---------------------------------------------------------------------------
# Feature display names
# ---------------------------------------------------------------------------


class TestFeatureDisplayNames:
    def test_all_entries_deserialise(
        self, raw_feature_display_names: List[Dict[str, Any]]
    ) -> None:
        for i, raw in enumerate(raw_feature_display_names):
            try:
                FeatureDisplayName.from_dict(raw)
            except Exception as exc:
                pytest.fail(f"FeatureDisplayName {i} failed: {exc}\n{raw}")

    def test_display_name_not_empty(
        self, raw_feature_display_names: List[Dict[str, Any]]
    ) -> None:
        for i, raw in enumerate(raw_feature_display_names):
            fd = FeatureDisplayName.from_dict(raw)
            assert fd.display_name.strip(), f"Entry {i} has empty display_name"

    def test_has_at_least_one_key(
        self, raw_feature_display_names: List[Dict[str, Any]]
    ) -> None:
        """Each FeatureDisplayName must have a BLK or WBK."""
        for i, raw in enumerate(raw_feature_display_names):
            fd = FeatureDisplayName.from_dict(raw)
            assert fd.blue_line_keys or fd.waterbody_keys, (
                f"Entry {i} ({fd.display_name}) has no BLK or WBK"
            )
