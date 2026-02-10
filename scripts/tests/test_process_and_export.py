#!/usr/bin/env python
"""
Test RegulationMapper.process_and_export() end-to-end pipeline.
"""

import sys
import json
import shutil
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from fwa_modules.linking import (
    ManualCorrections,
    WaterbodyLinker,
    RegulationMapper,
    ScopeFilter,
    TributaryEnricher,
    MetadataGazetteer,
    PipelineResult,
)


def test_process_and_export_returns_pipeline_result():
    """Test that process_and_export returns a PipelineResult."""
    print("Setting up pipeline test...")

    script_dir = Path(__file__).parent.parent
    gazetteer_path = script_dir / "output" / "fwa_modules" / "enriched_kml_points.json"
    parsed_regs_path = script_dir / "output" / "parse_synopsis" / "parsed_results.json"
    output_dir = script_dir / "output" / "test_pipeline_export"

    # Clean output directory
    if output_dir.exists():
        shutil.rmtree(output_dir)

    # Load test data
    with open(parsed_regs_path, "r") as f:
        parsed_data = json.load(f)

    # Use small subset for faster testing
    test_regulations = parsed_data[:10]

    print(f"Testing with {len(test_regulations)} regulations")

    # Setup pipeline
    gazetteer = MetadataGazetteer(gazetteer_path)
    linker = WaterbodyLinker(gazetteer, ManualCorrections({}, {}, {}, {}))
    scope_filter = ScopeFilter()
    tributary_enricher = TributaryEnricher()  # No graph for faster test

    mapper = RegulationMapper(linker, scope_filter, tributary_enricher)

    # Run pipeline
    result = mapper.process_and_export(test_regulations, output_dir=output_dir)

    # Check return type
    assert isinstance(result, PipelineResult), "Should return PipelineResult"
    print("✓ Returns PipelineResult")

    # Check fields
    assert hasattr(result, "feature_to_regs")
    assert hasattr(result, "merged_groups")
    assert hasattr(result, "stats")
    assert hasattr(result, "exported_files")
    print("✓ PipelineResult has all fields")


def test_process_and_export_with_output_dir():
    """Test that files are exported when output_dir is provided."""
    print("\nTesting export to directory...")

    script_dir = Path(__file__).parent.parent
    gazetteer_path = script_dir / "output" / "fwa_modules" / "enriched_kml_points.json"
    parsed_regs_path = script_dir / "output" / "parse_synopsis" / "parsed_results.json"
    output_dir = script_dir / "output" / "test_pipeline_export"

    with open(parsed_regs_path, "r") as f:
        parsed_data = json.load(f)

    test_regulations = parsed_data[:5]

    gazetteer = MetadataGazetteer(gazetteer_path)
    linker = WaterbodyLinker(gazetteer, ManualCorrections({}, {}, {}, {}))
    scope_filter = ScopeFilter()
    tributary_enricher = TributaryEnricher()

    mapper = RegulationMapper(linker, scope_filter, tributary_enricher)

    result = mapper.process_and_export(test_regulations, output_dir=output_dir)

    # Check files were created
    assert result.exported_files is not None
    assert "feature_to_regs" in result.exported_files
    assert "merged_features" in result.exported_files

    feature_file = result.exported_files["feature_to_regs"]
    merged_file = result.exported_files["merged_features"]

    assert feature_file.exists()
    assert merged_file.exists()

    print(f"✓ Created: {feature_file}")
    print(f"✓ Created: {merged_file}")


def test_process_and_export_without_output_dir():
    """Test that no files are exported when output_dir is None."""
    print("\nTesting without export...")

    script_dir = Path(__file__).parent.parent
    gazetteer_path = script_dir / "output" / "fwa_modules" / "enriched_kml_points.json"
    parsed_regs_path = script_dir / "output" / "parse_synopsis" / "parsed_results.json"

    with open(parsed_regs_path, "r") as f:
        parsed_data = json.load(f)

    test_regulations = parsed_data[:5]

    gazetteer = MetadataGazetteer(gazetteer_path)
    linker = WaterbodyLinker(gazetteer, ManualCorrections({}, {}, {}, {}))
    scope_filter = ScopeFilter()
    tributary_enricher = TributaryEnricher()

    mapper = RegulationMapper(linker, scope_filter, tributary_enricher)

    result = mapper.process_and_export(test_regulations, output_dir=None)

    # Check no files were exported
    assert result.exported_files is None
    print("✓ No files exported when output_dir=None")

    # But other data should still be present
    assert result.feature_to_regs is not None
    assert result.merged_groups is not None
    assert result.stats is not None
    print("✓ In-memory data still available")


def test_process_and_export_statistics():
    """Test that statistics are populated correctly."""
    print("\nTesting statistics...")

    script_dir = Path(__file__).parent.parent
    gazetteer_path = script_dir / "output" / "fwa_modules" / "enriched_kml_points.json"
    parsed_regs_path = script_dir / "output" / "parse_synopsis" / "parsed_results.json"

    with open(parsed_regs_path, "r") as f:
        parsed_data = json.load(f)

    test_regulations = parsed_data[:20]

    gazetteer = MetadataGazetteer(gazetteer_path)
    linker = WaterbodyLinker(gazetteer, ManualCorrections({}, {}, {}, {}))
    scope_filter = ScopeFilter()
    tributary_enricher = TributaryEnricher()

    mapper = RegulationMapper(linker, scope_filter, tributary_enricher)

    result = mapper.process_and_export(test_regulations)

    stats = result.stats

    print(f"  Total regulations: {stats.total_regulations}")
    print(f"  Linked regulations: {stats.linked_regulations}")
    print(f"  Failed to link: {stats.failed_to_link_regulations}")
    print(f"  Total rules: {stats.total_rules_processed}")
    print(f"  Unique features: {stats.unique_features_with_rules}")
    print(f"  Total mappings: {stats.total_rule_to_feature_mappings}")

    assert stats.total_regulations == len(test_regulations)
    assert (
        stats.linked_regulations + stats.failed_to_link_regulations
        == stats.total_regulations
    )
    assert stats.unique_features_with_rules == len(result.feature_to_regs)

    print("✓ Statistics are correct")


def test_process_and_export_data_consistency():
    """Test that data is consistent across result fields."""
    print("\nTesting data consistency...")

    script_dir = Path(__file__).parent.parent
    gazetteer_path = script_dir / "output" / "fwa_modules" / "enriched_kml_points.json"
    parsed_regs_path = script_dir / "output" / "parse_synopsis" / "parsed_results.json"

    with open(parsed_regs_path, "r") as f:
        parsed_data = json.load(f)

    test_regulations = parsed_data[:10]

    gazetteer = MetadataGazetteer(gazetteer_path)
    linker = WaterbodyLinker(gazetteer, ManualCorrections({}, {}, {}, {}))
    scope_filter = ScopeFilter()
    tributary_enricher = TributaryEnricher()

    mapper = RegulationMapper(linker, scope_filter, tributary_enricher)

    result = mapper.process_and_export(test_regulations)

    # Count total features in merged groups
    total_features_in_groups = sum(
        group.feature_count for group in result.merged_groups.values()
    )

    # Should match or be close to unique features
    print(f"  Features in feature_to_regs: {len(result.feature_to_regs)}")
    print(f"  Total features in groups: {total_features_in_groups}")
    print(f"  Number of groups: {len(result.merged_groups)}")

    assert total_features_in_groups <= len(
        result.feature_to_regs
    ), "Groups should have <= total features"

    print("✓ Data is consistent")


if __name__ == "__main__":
    print("=" * 80)
    print("Testing RegulationMapper.process_and_export()")
    print("=" * 80)

    test_process_and_export_returns_pipeline_result()
    test_process_and_export_with_output_dir()
    test_process_and_export_without_output_dir()
    test_process_and_export_statistics()
    test_process_and_export_data_consistency()

    print()
    print("=" * 80)
    print("✓ All process_and_export tests passed!")
    print("=" * 80)
