"""
Regulation Pipeline Orchestrator

Centralizes all pipeline coordination logic for regulation mapping and export.
Provides a simple high-level interface that handles component initialization
and orchestrates the full pipeline flow.
"""

from pathlib import Path
from typing import Optional
import json

from .linker import WaterbodyLinker
from .metadata_gazetteer import MetadataGazetteer
from .name_variations import (
    NAME_VARIATIONS,
    DIRECT_MATCHES,
    SKIP_ENTRIES,
    UNMARKED_WATERBODIES,
    ManualCorrections,
)
from .regulation_mapper import RegulationMapper
from .scope_filter import ScopeFilter
from .tributary_enricher import TributaryEnricher
from .geo_exporter import RegulationGeoExporter


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
        streams_gdb_path: Optional[Path] = None,
        polygons_gdb_path: Optional[Path] = None,
    ):
        """
        Initialize pipeline with required data sources.

        Args:
            metadata_path: Path to stream_metadata.pickle
            graph_path: Path to graph pickle (optional, for tributary enrichment)
            streams_gdb_path: Path to FWA_STREAM_NETWORKS_SP.gdb (optional, for export)
            polygons_gdb_path: Path to FWA_BC.gdb (optional, for export)
        """
        self.metadata_path = metadata_path
        self.graph_path = graph_path
        self.streams_gdb_path = streams_gdb_path
        self.polygons_gdb_path = polygons_gdb_path

        # Initialize components
        self._init_components()

    def _init_components(self):
        """Initialize all pipeline components."""
        # Load gazetteer
        self.gazetteer = MetadataGazetteer(self.metadata_path)

        # Initialize manual corrections
        manual_corrections = ManualCorrections(
            name_variations=NAME_VARIATIONS,
            direct_matches=DIRECT_MATCHES,
            skip_entries=SKIP_ENTRIES,
            unmarked_waterbodies=UNMARKED_WATERBODIES,
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

        # Initialize regulation mapper
        self.mapper = RegulationMapper(
            linker=linker,
            scope_filter=scope_filter,
            tributary_enricher=tributary_enricher,
        )

    def process_regulations(
        self,
        regulations_path: Path,
        output_dir: Path,
    ):
        """
        Run the full regulation mapping pipeline.

        Args:
            regulations_path: Path to parsed_results.json
            output_dir: Directory for output files

        Returns:
            PipelineResult from mapper.process_and_export()
        """
        # Load regulations
        with open(regulations_path) as f:
            parsed_data = json.load(f)

        regulations = (
            parsed_data
            if isinstance(parsed_data, list)
            else parsed_data.get("regulations", [])
        )

        # Run pipeline
        result = self.mapper.process_and_export(
            regulations=regulations,
            output_dir=output_dir,
        )

        return result

    def export_geometries(
        self,
        pipeline_result,
        output_dir: Path,
        zones_path: Optional[Path] = None,
        export_merged: bool = True,
        export_individual: bool = True,
    ):
        """
        Export regulation geometries to GPKG and PMTiles.

        Args:
            pipeline_result: Result from process_regulations()
            output_dir: Directory for output files
            zones_path: Optional path to zones GPKG
            export_merged: Whether to export merged geometries
            export_individual: Whether to export individual geometries

        Returns:
            Dict of exported file paths
        """
        if not self.streams_gdb_path or not self.polygons_gdb_path:
            raise ValueError(
                "streams_gdb_path and polygons_gdb_path required for export"
            )

        # Initialize exporter
        exporter = RegulationGeoExporter(
            mapper=self.mapper,
            pipeline_result=pipeline_result,
            streams_gdb_path=self.streams_gdb_path,
            polygons_gdb_path=self.polygons_gdb_path,
        )

        exported_files = {}

        # Export merged geometries
        if export_merged:
            gpkg_merged = output_dir / "regulations_merged.gpkg"
            pmtiles_merged = output_dir / "regulations_merged.pmtiles"

            exporter.export_gpkg(
                gpkg_merged,
                merge_geometries=True,
                include_all_features=False,
                zones_path=zones_path,
            )
            exporter.export_pmtiles(pmtiles_merged, merge_geometries=True)

            exported_files["gpkg_merged"] = gpkg_merged
            exported_files["pmtiles_merged"] = pmtiles_merged

        # Export individual geometries
        if export_individual:
            gpkg_individual = output_dir / "regulations_individual.gpkg"
            pmtiles_individual = output_dir / "regulations_individual.pmtiles"

            exporter.export_gpkg(
                gpkg_individual,
                merge_geometries=False,
                include_all_features=False,
                zones_path=zones_path,
            )
            exporter.export_pmtiles(pmtiles_individual, merge_geometries=False)

            exported_files["gpkg_individual"] = gpkg_individual
            exported_files["pmtiles_individual"] = pmtiles_individual

        return exported_files

    def run_full_pipeline(
        self,
        regulations_path: Path,
        output_dir: Path,
        zones_path: Optional[Path] = None,
        export_merged: bool = True,
        export_individual: bool = True,
    ):
        """
        Run the complete pipeline: mapping + export.

        Args:
            regulations_path: Path to parsed_results.json
            output_dir: Directory for output files
            zones_path: Optional path to zones GPKG
            export_merged: Whether to export merged geometries
            export_individual: Whether to export individual geometries

        Returns:
            Tuple of (pipeline_result, exported_files)
        """
        # Process regulations
        result = self.process_regulations(regulations_path, output_dir)

        # Export geometries (if GDB paths provided)
        exported_files = {}
        if self.streams_gdb_path and self.polygons_gdb_path:
            exported_files = self.export_geometries(
                pipeline_result=result,
                output_dir=output_dir,
                zones_path=zones_path,
                export_merged=export_merged,
                export_individual=export_individual,
            )

        return result, exported_files
