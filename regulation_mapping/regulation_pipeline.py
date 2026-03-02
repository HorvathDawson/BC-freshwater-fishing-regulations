"""
Regulation Pipeline Orchestrator

Centralizes all pipeline coordination logic for regulation mapping and export.
Provides a simple high-level interface that handles component initialization
and orchestrates the full pipeline flow.
"""

import argparse
import shutil
from pathlib import Path
from typing import Optional
from collections import Counter
import json

from .linker import WaterbodyLinker
from fwa_pipeline.metadata_gazetteer import MetadataGazetteer
from .linking_corrections import (
    DIRECT_MATCHES,
    SKIP_ENTRIES,
    UNGAZETTED_WATERBODIES,
    ADMIN_DIRECT_MATCHES,
    NAME_VARIATION_LINKS,
    ManualCorrections,
)
from .regulation_mapper import RegulationMapper, PipelineResult
from .scope_filter import ScopeFilter
from .tributary_enricher import TributaryEnricher
from .geo_exporter import RegulationGeoExporter
from .logger_config import get_logger
from project_config import get_config

logger = get_logger(__name__)


class RegulationPipeline:
    """
    High-level orchestrator for the full regulation processing pipeline.

    Handles:
    - Component initialization (gazetteer, linker, mapper, exporter)
    - Regulation loading
    - Pipeline execution (link → scope → enrich → map → merge)
    - Geographic export (GPKG, PMTiles)
    """

    def __init__(
        self,
        metadata_path: Path,
        graph_path: Optional[Path] = None,
        gpkg_path: Optional[Path] = None,
    ):
        """
        Initialize pipeline with required data sources.

        Args:
            metadata_path: Path to fwa_metadata.pickle
            graph_path: Path to graph pickle (optional, for tributary enrichment)
            gpkg_path: Path to FWA GeoPackage (optional, for export)
        """
        self.metadata_path = metadata_path
        self.graph_path = graph_path
        self.gpkg_path = gpkg_path

        # Store parsed regulations for later export
        self.parsed_regulations = None
        # Directory containing row images (set during process_regulations)
        self._row_images_dir: Optional[Path] = None

        # Initialize components
        self._init_components()

    def _init_components(self):
        """Initialize all pipeline components."""
        # Load gazetteer
        self.gazetteer = MetadataGazetteer(self.metadata_path)

        # Set GPKG path for spatial intersection (admin metadata comes from pickle)
        if self.gpkg_path and self.gpkg_path.exists():
            self.gazetteer.set_gpkg_path(self.gpkg_path)

        # Inject ungazetted waterbodies into gazetteer metadata so they
        # participate in merging, export, and name search like FWA features.
        for uw in UNGAZETTED_WATERBODIES.values():
            self.gazetteer.inject_ungazetted_waterbody(
                ungazetted_id=uw.ungazetted_id,
                name=uw.name,
                zones=uw.zones,
                mgmt_units=uw.mgmt_units,
                geometry_type=uw.geometry_type,
                note=uw.note,
            )

        # Initialize manual corrections
        manual_corrections = ManualCorrections(
            direct_matches=DIRECT_MATCHES,
            skip_entries=SKIP_ENTRIES,
            ungazetted_waterbodies=UNGAZETTED_WATERBODIES,
            admin_direct_matches=ADMIN_DIRECT_MATCHES,
            name_variation_links=NAME_VARIATION_LINKS,
        )

        # Initialize linker
        linker = WaterbodyLinker(
            gazetteer=self.gazetteer,
            manual_corrections=manual_corrections,
        )

        # Initialize scope filter
        scope_filter = ScopeFilter()

        # Initialize tributary enricher (with or without graph)
        if self.graph_path and self.graph_path.exists():
            tributary_enricher = TributaryEnricher(
                graph_source=self.graph_path,
                metadata_gazetteer=self.gazetteer,
            )
        else:
            tributary_enricher = TributaryEnricher(metadata_gazetteer=self.gazetteer)

        # Initialize regulation mapper (orchestrates all regulation sources)
        self.mapper = RegulationMapper(
            linker=linker,
            scope_filter=scope_filter,
            tributary_enricher=tributary_enricher,
            gpkg_path=self.gpkg_path,
        )

    def process_regulations(
        self,
        regulations_path: Path,
        include_zone_regulations: bool = False,
    ) -> PipelineResult:
        """
        Run the full regulation mapping pipeline.

        Loads parsed regulations and delegates all orchestration to the mapper,
        which handles synopsis, admin area, and provincial regulation sources
        before merging features into groups.

        Args:
            regulations_path: Path to parsed_results.json
            include_zone_regulations: If True, process zone-level default
                regulations. Defaults to False to keep test runs fast.

        Returns:
            PipelineResult from mapper.run()
        """
        # Load regulations
        with open(regulations_path) as f:
            parsed_data = json.load(f)

        regulations = (
            parsed_data
            if isinstance(parsed_data, list)
            else parsed_data.get("regulations", [])
        )

        # Store regulations for later export
        self.parsed_regulations = regulations

        # Discover row_images directory relative to the parsed_results.json
        row_images_candidate = (
            regulations_path.parent.parent / "extract_synopsis" / "row_images"
        )
        if row_images_candidate.is_dir():
            self._row_images_dir = row_images_candidate
            logger.info(f"Found row images directory: {self._row_images_dir}")

        # Mapper orchestrates all regulation sources and merging
        return self.mapper.run(
            regulations=regulations,
            include_zone_regulations=include_zone_regulations,
        )

    def export_geography(
        self,
        pipeline_result: PipelineResult,
        output_dir: Path,
        export_merged: bool = True,
        export_individual: bool = True,
        export_regulations_json: bool = True,
    ):
        """
        Export regulation geometries to GPKG and PMTiles.

        Args:
            pipeline_result: Result from process_regulations()
            output_dir: Directory for output files
            export_merged: Whether to export merged geometries
            export_individual: Whether to export individual geometries
            export_regulations_json: Whether to export regulations.json for frontend

        Returns:
            Dict of exported file paths
        """
        if not self.gpkg_path:
            raise ValueError("gpkg_path required for export")

        # Compute cache directory from output_dir
        cache_dir = output_dir / ".geom_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)

        # Initialize exporter strictly with the results from the mapper step
        exporter = RegulationGeoExporter(
            pipeline_result=pipeline_result,
            gpkg_path=self.gpkg_path,
            cache_dir=cache_dir,
        )

        exported_files = {}

        # Export regulations JSON + search index first (fast, and needed by frontend
        # even if the heavier geometry exports fail)
        if export_regulations_json and self.parsed_regulations:
            waterbody_data = output_dir / "waterbody_data.json"
            exporter.export_waterbody_data(waterbody_data)
            exported_files["waterbody_data"] = waterbody_data

            # Copy row images next to waterbody_data.json for web serving
            if self._row_images_dir and self._row_images_dir.is_dir():
                dest_images_dir = output_dir / "row_images"
                if dest_images_dir.exists():
                    shutil.rmtree(dest_images_dir)
                shutil.copytree(self._row_images_dir, dest_images_dir)
                logger.info(
                    f"Copied row images to {dest_images_dir} "
                    f"({sum(1 for _ in dest_images_dir.glob('*.png'))} images)"
                )
                exported_files["row_images"] = dest_images_dir

        # Export merged geometries
        if export_merged:
            merged_gpkg = output_dir / "regulations_merged.gpkg"
            merged_pmtiles = output_dir / "regulations_merged.pmtiles"

            logger.info("Exporting merged geometries...")
            if gpkg_path := exporter.export_gpkg(merged_gpkg, merge_geometries=True):
                exported_files["merged_gpkg"] = gpkg_path

            if pmtiles_path := exporter.export_pmtiles(
                merged_pmtiles, merge_geometries=True
            ):
                exported_files["merged_pmtiles"] = pmtiles_path

        # Export individual geometries (GPKG only - PMTiles not needed)
        if export_individual:
            individual_gpkg = output_dir / "regulations_individual.gpkg"

            logger.info("Exporting individual geometries...")
            if gpkg_path := exporter.export_gpkg(
                individual_gpkg, merge_geometries=False
            ):
                exported_files["individual_gpkg"] = gpkg_path

        return exported_files

    def run_full_pipeline(
        self,
        regulations_path: Path,
        output_dir: Path,
        export_merged: bool = True,
        export_individual: bool = True,
        export_regulations_json: bool = True,
        frontend_output_dir: Optional[Path] = None,
        include_zone_regulations: bool = False,
    ):
        """
        Run the complete pipeline: mapping + export.

        Args:
            regulations_path: Path to parsed_results.json
            output_dir: Directory for output files
            export_merged: Whether to export merged geometries
            export_individual: Whether to export individual geometries
            export_regulations_json: Whether to export regulations.json for frontend
            frontend_output_dir: Optional directory for regulations.json (defaults to output_dir)
            include_zone_regulations: If True, process zone-level default
                regulations. Defaults to False to keep test runs fast.

        Returns:
            Tuple of (pipeline_result, exported_files)
        """
        # Process regulations
        result = self.process_regulations(
            regulations_path,
            include_zone_regulations=include_zone_regulations,
        )

        # Free FWA layer cache (large GeoDataFrames used only for admin
        # spatial intersection during mapping).  The exporter loads its own
        # geometry copies, so keeping these around doubles peak memory.
        if self.gazetteer._fwa_layer_cache:
            logger.info(
                f"Clearing {len(self.gazetteer._fwa_layer_cache)} cached FWA "
                f"layer(s) from gazetteer to free memory before export phase"
            )
            self.gazetteer._fwa_layer_cache.clear()

        # Export geometries (if GDB paths provided)
        exported_files = {}
        if self.gpkg_path:
            exported_files = self.export_geography(
                pipeline_result=result,
                output_dir=output_dir,
                export_merged=export_merged,
                export_individual=export_individual,
                export_regulations_json=export_regulations_json,
            )

        return result, exported_files


# --- Statistics Display Helpers ---

from .cli_helpers import RED, YELLOW, GREEN, BLUE, RESET


def _format_percentage(count, total):
    """Format a percentage with color coding."""
    if total == 0:
        return "N/A"
    pct = (count / total) * 100
    if pct >= 90:
        color = GREEN
    elif pct >= 70:
        color = YELLOW
    else:
        color = RED
    return f"{color}{pct:.1f}%{RESET}"


def _print_mapping_statistics(
    result: PipelineResult, pipeline: RegulationPipeline, verbose: bool = False
):
    """Print detailed mapping statistics."""
    mapper_stats = result.stats
    scope_stats = pipeline.mapper.scope_filter.get_stats()
    tributary_stats = pipeline.mapper.tributary_enricher.get_stats()

    print(f"\n📊 Regulation Linking Statistics:")
    print(f"  Total regulations:          {mapper_stats.total_regulations}")
    print(
        f"  Successfully linked:        {mapper_stats.linked_regulations} "
        f"({_format_percentage(mapper_stats.linked_regulations, mapper_stats.total_regulations)})"
    )
    print(f"  Failed to link:             {mapper_stats.failed_to_link_regulations}")
    print(f"  Bad regulations:            {mapper_stats.bad_regulation}")

    if verbose:
        print("\n  Link Status Breakdown:")
        for status, count in mapper_stats.link_status_counts.most_common():
            pct = (count / mapper_stats.total_regulations) * 100
            print(f"    {status:20s}: {count:4d} ({pct:5.1f}%)")

    print(f"\n📊 Rule Processing Statistics:")
    print(
        f"  Total rules processed:      {mapper_stats.total_rules_processed} (from {mapper_stats.linked_regulations} regulations)"
    )
    print(
        f"  Rule → feature mappings:    {mapper_stats.total_rule_to_feature_mappings}"
    )
    print(f"  Unique features with rules: {mapper_stats.unique_features_with_rules:,}")

    if mapper_stats.total_rules_processed > 0:
        avg_mappings = (
            mapper_stats.total_rule_to_feature_mappings
            / mapper_stats.total_rules_processed
        )
        print(f"  Avg features per rule:      {avg_mappings:.1f}")
        avg_rules = (
            mapper_stats.total_rules_processed / mapper_stats.linked_regulations
            if mapper_stats.linked_regulations > 0
            else 0
        )
        print(f"  Avg rules per regulation:   {avg_rules:.1f}")

    print(f"\n📊 Scope Filter Statistics:")
    print(
        f"  Scope types encountered:    {', '.join(scope_stats['scope_types_seen']) or 'None'}"
    )
    print(f"  Fallback to WHOLE_SYSTEM:   {scope_stats['fallback_count']}")

    print(f"\n📊 Tributary Enricher Statistics:")
    print(f"  Enrichment requests:        {tributary_stats['enrichment_requests']}")
    print(f"  Cache hits:                 {tributary_stats['cache_hits']}")
    print(f"  Cache size:                 {tributary_stats['cache_size']}")
    print(
        f"  Total tributaries found:    {tributary_stats.get('total_tributaries_found', 0)}"
    )
    print(
        f"  Total base features:        {tributary_stats.get('total_base_features', 0)}"
    )
    print(
        f"  Stream seeds used:          {tributary_stats.get('total_stream_seeds', 0)}"
    )
    print(f"  Lake seeds used:            {tributary_stats.get('total_lake_seeds', 0)}")
    if tributary_stats["enrichment_requests"] > 0:
        avg_tributaries = (
            tributary_stats.get("total_tributaries_found", 0)
            / tributary_stats["enrichment_requests"]
        )
        print(f"  Avg tributaries/request:    {avg_tributaries:.1f}")

    print(f"\n📊 Feature Merging Statistics:")
    print(f"  Total features:             {len(result.feature_to_regs):,}")
    print(f"  Merged groups:              {len(result.merged_groups):,}")
    if len(result.feature_to_regs) > 0:
        reduction_pct = (
            1 - len(result.merged_groups) / len(result.feature_to_regs)
        ) * 100
        print(
            f"  Reduction:                  {reduction_pct:.1f}% ({len(result.feature_to_regs):,} → {len(result.merged_groups):,})"
        )

    if verbose:
        # Analyze group sizes
        group_sizes = Counter()
        for group in result.merged_groups.values():
            group_sizes[group.feature_count] += 1

        print("\n  Groups by size:")
        for size, count in sorted(group_sizes.items())[:10]:  # Show first 10
            print(f"    {size:3d} feature(s): {count:5d} groups")
        if len(group_sizes) > 10:
            print(f"    ... and {len(group_sizes) - 10} more size categories")

        # Show largest groups
        print("\n  Top 5 largest merged groups:")
        top_groups = sorted(
            result.merged_groups.values(), key=lambda g: g.feature_count, reverse=True
        )[:5]
        for group in top_groups:
            print(
                f"    {group.group_id}: {group.feature_count} features, {len(group.regulation_ids)} regulations"
            )

        # Feature distribution analysis
        if result.feature_to_regs:
            print("\n  Feature Mapping Distribution:")

            rule_counts = Counter()
            for feature_id, rule_list in result.feature_to_regs.items():
                rule_counts[len(rule_list)] += 1

            print("\n  Features by number of rules:")
            for num_rules, count in sorted(rule_counts.items()):
                print(f"    {num_rules:3d} rule(s): {count:5d} features")

            # Show top 5 features with most rules
            print("\n  Top 5 features with most rules:")
            top_features = sorted(
                result.feature_to_regs.items(), key=lambda x: len(x[1]), reverse=True
            )[:5]

            for feature_id, rule_ids in top_features:
                # Try to get feature name from gazetteer
                feature_name = "unknown"
                try:
                    # Look up feature in gazetteer to get name
                    from fwa_pipeline.metadata_gazetteer import FeatureType

                    for ftype in [
                        FeatureType.STREAM,
                        FeatureType.LAKE,
                        FeatureType.WETLAND,
                        FeatureType.MANMADE,
                    ]:
                        if ftype in pipeline.gazetteer.metadata:
                            features = pipeline.gazetteer.metadata[ftype]
                            if feature_id in features:
                                feature_info = features[feature_id]
                                feature_name = feature_info.get("gnis_name", "unnamed")
                                break
                except Exception as e:
                    logger.debug(
                        f"Could not resolve feature name for {feature_id}: {e}"
                    )

                print(
                    f"    {feature_id:30s}: {len(rule_ids):5d} rules - {feature_name}"
                )


def main():
    """CLI entry point for regulation mapping pipeline."""
    config = get_config()

    # Set up argument parser with config-based defaults
    parser = argparse.ArgumentParser(
        description="BC Freshwater Fishing Regulations - Regulation Mapping Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline with defaults from config
  python -m regulation_mapping.regulation_pipeline

  # Run mapping only (no geometry export)
  python -m regulation_mapping.regulation_pipeline --map-only

  # Export merged geometries only
  python -m regulation_mapping.regulation_pipeline --merged-only

  # Export individual geometries only
  python -m regulation_mapping.regulation_pipeline --individual-only
        """,
    )

    parser.add_argument(
        "--metadata",
        type=Path,
        default=config.fwa_metadata_path,
        help=f"Path to stream metadata pickle (default: {config.fwa_metadata_path})",
    )

    parser.add_argument(
        "--regulations",
        type=Path,
        default=config.synopsis_parsed_results_path,
        help=f"Path to parsed regulations JSON (default: {config.synopsis_parsed_results_path})",
    )

    parser.add_argument(
        "--graph",
        type=Path,
        default=config.fwa_graph_path,
        help=f"Path to FWA graph pickle (default: {config.fwa_graph_path})",
    )

    parser.add_argument(
        "--gpkg-path",
        type=Path,
        default=config.fetch_output_gpkg_path,
        help=f"Path to FWA GeoPackage (default: {config.fetch_output_gpkg_path})",
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=config.regulation_mapping_output_dir,
        help=f"Output directory (default: {config.regulation_mapping_output_dir})",
    )

    parser.add_argument(
        "--map-only",
        action="store_true",
        help="Only run regulation mapping, skip geometry export",
    )

    parser.add_argument(
        "--merged-only",
        action="store_true",
        help="Only export merged geometries",
    )

    parser.add_argument(
        "--individual-only",
        action="store_true",
        help="Only export individual geometries",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show detailed statistics and analysis",
    )

    parser.add_argument(
        "--include-zones",
        action="store_true",
        help="Include zone-level default regulations (can touch millions of features)",
    )

    args = parser.parse_args()

    # Verify required inputs exist
    if not args.metadata.exists():
        print(f"❌ Metadata not found: {args.metadata}")
        print("   Run: python -m fwa_pipeline.metadata_builder")
        return 1

    if not args.regulations.exists():
        print(f"❌ Regulations not found: {args.regulations}")
        print("   Run: python -m synopsis_pipeline.parse_synopsis")
        return 1

    if not args.gpkg_path.exists():
        print(f"❌ GeoPackage not found: {args.gpkg_path}")
        return 1

    # Determine export flags
    if args.map_only:
        export_merged = False
        export_individual = False
    elif args.merged_only:
        export_merged = True
        export_individual = False
    elif args.individual_only:
        export_merged = False
        export_individual = True
    else:
        # Default: export both
        export_merged = True
        export_individual = True

    # Print configuration
    print("=" * 80)
    print("BC FRESHWATER FISHING REGULATIONS - REGULATION MAPPING PIPELINE")
    print("=" * 80)

    print("\n📁 Input:")
    print(f"  Metadata: {args.metadata}")
    print(f"  Regulations: {args.regulations}")
    print(
        f"  Graph: {args.graph if args.graph.exists() else 'Not found (tributary enrichment disabled)'}"
    )
    if not args.map_only:
        print(
            f"  GeoPackage: {args.gpkg_path if args.gpkg_path.exists() else 'Not found'}"
        )
    print("\n📁 Output:")
    print(f"  Directory: {args.output_dir}")
    if args.map_only:
        print(f"  Mode: Mapping only (no geometry export)")
    else:
        print(f"  Merged geometries: {'Yes' if export_merged else 'No'}")
        print(f"  Individual geometries: {'Yes' if export_individual else 'No'}")
        print(f"  Regulations JSON: Yes")
        print(f"  Search index: Yes")

    print("\n⚙️  Configuration:")
    print(f"  Verbose output: {'Yes' if args.verbose else 'No'}")
    print(f"  Tributary enrichment: {'Enabled' if args.graph.exists() else 'Disabled'}")
    print(f"  Zone boundary buffer: Yes (500m, always enabled)")
    if not args.map_only:
        print(
            f"  Geometry export: {'Enabled' if args.gpkg_path.exists() else 'Disabled'}"
        )
    print()

    # Initialize pipeline
    print("\n" + "=" * 80)
    print("INITIALIZING PIPELINE")
    print("=" * 80)

    pipeline = RegulationPipeline(
        metadata_path=args.metadata,
        graph_path=args.graph if args.graph.exists() else None,
        gpkg_path=args.gpkg_path,
    )

    print(f"  ✓ Loaded {len(pipeline.gazetteer.name_index):,} unique waterbody names")

    # Run pipeline
    print("\n" + "=" * 80)
    print("RUNNING PIPELINE")
    print("=" * 80)

    result, exported_files = pipeline.run_full_pipeline(
        regulations_path=args.regulations,
        output_dir=args.output_dir,
        export_merged=export_merged,
        export_individual=export_individual,
        export_regulations_json=True,
        include_zone_regulations=args.include_zones,
    )

    # Print results
    print("\n" + "=" * 80)
    print("PIPELINE RESULTS")
    print("=" * 80)

    _print_mapping_statistics(result, pipeline, args.verbose)

    if exported_files:
        print("\n📁 Exported Files:")
        for name, filepath in exported_files.items():
            if filepath and filepath.exists():
                size_mb = filepath.stat().st_size / (1024 * 1024)
                print(f"  ✓ {filepath.name} ({size_mb:.1f} MB)")

    print("\n" + GREEN + "✅ PIPELINE COMPLETE" + RESET)
    print("=" * 80)

    if not args.map_only:
        print("\n💡 Next Steps:")
        print("  1. Copy output files to map-webapp/public/data/")
        print("  2. Open GPKG in QGIS to inspect attributes")
        print("  3. Test search functionality with search_index.json")
    else:
        print("\n💡 Tip: Run without --map-only to export geometries (GPKG/PMTiles)")

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
