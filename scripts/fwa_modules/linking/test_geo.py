"""
Test script for RegulationPipeline

Runs the full pipeline:
1. Regulation mapping (link → scope → enrich → map → merge)
2. Geographic export (load geometries → merge → export GPKG/PMTiles)
"""

import sys
from pathlib import Path

# Add parent directories to path
script_dir = Path(__file__).parent
sys.path.insert(0, str(script_dir.parent.parent))

from fwa_modules.linking.regulation_pipeline import RegulationPipeline


def main():
    print("=" * 80)
    print("REGULATION PIPELINE TEST")
    print("=" * 80)

    # Paths
    scripts_dir = Path(__file__).parent.parent.parent  # scripts/
    workspace_root = scripts_dir.parent  # workspace root
    output_dir = scripts_dir / "output" / "fwa_modules"

    metadata_path = output_dir / "stream_metadata.pickle"
    parsed_regs_path = scripts_dir / "output" / "parse_synopsis" / "parsed_results.json"
    graph_path = output_dir / "fwa_bc_primal_full.gpickle"

    # GDB paths
    data_dir = (
        workspace_root
        / "data"
        / "ftp.geobc.gov.bc.ca"
        / "sections"
        / "outgoing"
        / "bmgs"
        / "FWA_Public"
    )
    streams_gdb = data_dir / "FWA_STREAM_NETWORKS_SP" / "FWA_STREAM_NETWORKS_SP.gdb"
    polygons_gdb = data_dir / "FWA_BC" / "FWA_BC.gdb"
    zones_gdb = data_dir / "WAA_WILDLIFE_MGMT_UNITS_SVW.gpkg"

    # Verify required paths exist
    if not metadata_path.exists():
        print(f"❌ Metadata not found: {metadata_path}")
        return

    if not parsed_regs_path.exists():
        print(f"❌ Regulations not found: {parsed_regs_path}")
        return

    print("\n📁 Input Files:")
    print(f"  Metadata: {metadata_path}")
    print(f"  Regulations: {parsed_regs_path}")
    print(
        f"  Graph: {graph_path if graph_path.exists() else 'Not found (tributary enrichment disabled)'}"
    )
    print(f"  Streams GDB: {streams_gdb if streams_gdb.exists() else 'Not found'}")
    print(f"  Polygons GDB: {polygons_gdb if polygons_gdb.exists() else 'Not found'}")

    # Initialize pipeline
    print("\n" + "=" * 80)
    print("INITIALIZING PIPELINE")
    print("=" * 80)

    pipeline = RegulationPipeline(
        metadata_path=metadata_path,
        graph_path=graph_path if graph_path.exists() else None,
        streams_gdb_path=streams_gdb if streams_gdb.exists() else None,
        polygons_gdb_path=polygons_gdb if polygons_gdb.exists() else None,
    )

    print(f"  ✓ Loaded {len(pipeline.gazetteer.name_index):,} unique names")

    # Run full pipeline
    print("\n" + "=" * 80)
    print("RUNNING FULL PIPELINE")
    print("=" * 80)

    result, exported_files = pipeline.run_full_pipeline(
        regulations_path=parsed_regs_path,
        output_dir=output_dir,
        zones_path=zones_gdb if zones_gdb.exists() else None,
        export_merged=True,
        export_individual=True,
    )

    # Print results
    print("\n📊 Pipeline Results:")
    print(f"  Features mapped: {result.stats.unique_features_with_rules:,}")
    print(f"  Merged groups: {len(result.merged_groups):,}")
    print(
        f"  Linked regulations: {result.stats.linked_regulations}/{result.stats.total_regulations}"
    )

    print("\n✅ PIPELINE COMPLETE")
    print("=" * 80)

    print("\n📁 Output Files:")
    for name, filepath in exported_files.items():
        if filepath and filepath.exists():
            size_mb = filepath.stat().st_size / (1024 * 1024)
            print(f"  ✓ {filepath.name} ({size_mb:.1f} MB)")

    print("\n💡 Next Steps:")
    print("  1. Open GPKG in QGIS to inspect attributes")
    print("  2. View PMTiles in web map viewer")
    print("  3. Compare merged vs individual geometry results")


if __name__ == "__main__":
    main()
