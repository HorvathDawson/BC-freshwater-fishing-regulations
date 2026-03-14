"""
Unified V2 pipeline runner.

Steps:
    atlas   — build the atlas pickle from graph + GPKG
    tiles   — export atlas pickle → PMTiles via tippecanoe
    enrich  — run the 5-phase regulation index builder
    all     — atlas → tiles → enrich (default)

Usage:
    python -m regulation_mapping_v2.run_pipeline                  # run all
    python -m regulation_mapping_v2.run_pipeline --step atlas     # atlas only
    python -m regulation_mapping_v2.run_pipeline --step tiles     # tiles only
    python -m regulation_mapping_v2.run_pipeline --step enrich    # enrich only
    python -m regulation_mapping_v2.run_pipeline --step all       # full pipeline
    python -m regulation_mapping_v2.run_pipeline --dry-run        # enrich dry-run
"""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


def _step_atlas(cfg: dict) -> Path:
    """Build atlas pickle from graph + GPKG."""
    from .atlas.freshwater_atlas import FreshWaterAtlas

    graph_path = Path(cfg["output"]["fwa"]["graph"])
    gpkg_path = Path(cfg["data"]["fetch"]["output_gpkg"])
    atlas_path = Path(cfg["output"]["regulation_mapping_v2"]["atlas"])

    log.info(f"Graph : {graph_path}")
    log.info(f"GPKG  : {gpkg_path}")

    t0 = time.perf_counter()
    atlas = FreshWaterAtlas(graph_path, gpkg_path)
    atlas.save(atlas_path)
    log.info(f"Atlas built in {time.perf_counter() - t0:.1f}s")
    return atlas_path


def _step_tiles(cfg: dict, atlas_path: Path | None = None) -> Path:
    """Export atlas pickle → PMTiles via tippecanoe."""
    from .atlas.freshwater_atlas import FreshWaterAtlas
    from .tiles.tile_exporter import TileExporter

    if atlas_path is None:
        atlas_path = Path(cfg["output"]["regulation_mapping_v2"]["atlas"])

    pmtiles_path = Path(cfg["output"]["regulation_mapping_v2"]["pmtiles"])

    log.info(f"Atlas : {atlas_path}")
    log.info(f"Output: {pmtiles_path}")

    t0 = time.perf_counter()
    atlas = FreshWaterAtlas.load(atlas_path)
    exporter = TileExporter(atlas)
    exporter.export(pmtiles_path)
    log.info(f"Tiles exported in {time.perf_counter() - t0:.1f}s")
    return pmtiles_path


def _step_enrich(cfg: dict, config_path: Path, dry_run: bool = False) -> Path:
    """Run the 5-phase regulation index builder."""
    from .enrichment.builder import build

    t0 = time.perf_counter()
    output_path = build(config_path=config_path, dry_run=dry_run)
    log.info(f"Enrichment done in {time.perf_counter() - t0:.1f}s")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="V2 pipeline runner")
    parser.add_argument(
        "--step",
        choices=["atlas", "tiles", "enrich", "all"],
        default="all",
        help="Pipeline step to run (default: all)",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config.yaml (default: config.yaml)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Enrich step: load and merge only — skip atlas/graph processing",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    cfg = yaml.safe_load(config_path.read_text())

    atlas_path = None

    if args.step in ("atlas", "all"):
        atlas_path = _step_atlas(cfg)

    if args.step in ("tiles", "all"):
        _step_tiles(cfg, atlas_path)

    if args.step in ("enrich", "all"):
        _step_enrich(cfg, config_path, dry_run=args.dry_run)

    log.info("Done.")


if __name__ == "__main__":
    main()
