"""Regulation index builder — orchestrates all 5 phases.

Usage:
    python -m regulation_mapping_v2.enrichment.builder
    python -m regulation_mapping_v2.enrichment.builder --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import tempfile
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Maps override/base_reg layer names → tile layer names
_LAYER_NAME_MAP = {
    "parks_bc": "eco_reserves",
    "eco_reserves": "eco_reserves",
    "parks_nat": "parks_nat",
    "wma": "wma",
    "historic_sites": "historic_sites",
    "watersheds": "watersheds",
    "osm_admin_boundaries": "osm_admin",
    "aboriginal_lands": "aboriginal_lands",
}

# Layers that show ALL features regardless of regulation status.
# Layers NOT listed here default to "regulated_only" — only features
# with an explicit admin_target reference are shown on the map.
_SHOW_ALL_LAYERS = {"eco_reserves", "parks_nat", "aboriginal_lands"}


def _collect_admin_visibility(
    records: list,
    base_regs_path: Path,
) -> Dict[str, Dict[str, Any]]:
    """Collect admin visibility config, grouped by tile layer name.

    Sources:
        - OverrideEntry.admin_targets from synopsis records
        - BaseRegulationDef.admin_targets from base_regulations.json

    Returns::

        {
          "eco_reserves":    {"display": "all"},
          "wma":             {"display": "regulated_only", "regulated_ids": ["5364"]},
          ...
        }

    Layers in ``_SHOW_ALL_LAYERS`` get ``display: "all"`` — the frontend
    shows every feature in the tile layer.  Other layers get
    ``display: "regulated_only"`` with the explicit list of admin IDs that
    have regulations, so the frontend can filter the tile features.
    """
    from regulation_mapping_v2.matching.match_table import OverrideEntry

    from .models import BaseRegulationDef

    layer_ids: Dict[str, Set[str]] = defaultdict(set)
    seen_layers: Set[str] = set()

    # 1. Synopsis overrides
    for rec in records:
        entry = rec.match_entry
        if isinstance(entry, OverrideEntry) and entry.admin_targets:
            for target in entry.admin_targets:
                layer = _LAYER_NAME_MAP.get(target["layer"], target["layer"])
                seen_layers.add(layer)
                fid = target.get("feature_id")
                if fid:
                    layer_ids[layer].add(fid)

    # 2. Base regulations
    with open(base_regs_path, encoding="utf-8") as f:
        base_data = json.load(f)
    for d in base_data:
        reg = BaseRegulationDef.from_dict(d)
        if reg.disabled:
            continue
        if reg.admin_targets:
            for target in reg.admin_targets:
                layer = _LAYER_NAME_MAP.get(target["layer"], target["layer"])
                seen_layers.add(layer)
                fid = target.get("feature_id")
                if fid:
                    layer_ids[layer].add(fid)

    result: Dict[str, Dict[str, Any]] = {}
    for layer in sorted(seen_layers | _SHOW_ALL_LAYERS):
        if layer in _SHOW_ALL_LAYERS:
            result[layer] = {"display": "all"}
        else:
            ids = sorted(layer_ids.get(layer, set()))
            if ids:
                result[layer] = {"display": "regulated_only", "regulated_ids": ids}
    return result


def build(config_path: Path = Path("config.yaml"), dry_run: bool = False) -> Path:
    """Run the full 5-phase pipeline.

    Returns the path to the written regulation_index.json.
    """
    from regulation_mapping_v2.atlas.freshwater_atlas import FreshWaterAtlas
    from regulation_mapping_v2.matching.match_table import (
        OVERRIDES_PATH,
        OverrideEntry,
    )

    from . import base_reg_assigner, feature_resolver, loader, reach_builder
    from .models import BaseRegulationDef
    from .tributary_enricher import TributaryEnricherV2

    t_start = time.perf_counter()

    # Load config
    cfg = yaml.safe_load(config_path.read_text())
    project_root = Path(cfg.get("project_root", "."))

    # Resolve paths
    raw_path = (
        project_root / cfg["output"]["synopsis"]["extract"] / "synopsis_raw_data.json"
    )
    match_table_path = (
        project_root / cfg["output"]["regulation_mapping_v2"]["match_table"]
    )
    overrides_path = OVERRIDES_PATH
    session_path = (
        project_root
        / cfg["output"]["regulation_mapping_v2"]["parsing"]
        / "session_state.json"
    )
    atlas_path = project_root / cfg["output"]["regulation_mapping_v2"]["atlas"]
    graph_path = project_root / cfg["output"]["fwa"]["graph"]
    gpkg_path = project_root / cfg["data_accessor"]["gpkg_path"]
    output_path = (
        project_root / cfg["output"]["regulation_mapping_v2"]["regulation_index"]
    )

    # ── Phase 1: Load & Merge ────────────────────────────────────────
    t0 = time.perf_counter()
    records = loader.load_and_merge(
        raw_path=raw_path,
        match_table_path=match_table_path,
        overrides_path=overrides_path,
        session_path=session_path,
    )
    logger.info("Phase 1 done in %.1fs", time.perf_counter() - t0)

    if dry_run:
        logger.info("[DRY RUN] Would process %d records. Stopping.", len(records))
        return output_path

    # ── Load atlas + metadata ────────────────────────────────────────
    t0 = time.perf_counter()
    logger.info("Loading atlas from %s", atlas_path)
    atlas = FreshWaterAtlas.load(atlas_path)
    logger.info("Atlas loaded in %.1fs", time.perf_counter() - t0)

    t0 = time.perf_counter()
    metadata = feature_resolver.build_metadata_from_graph(graph_path)
    feature_resolver.enrich_metadata_with_polygons(metadata, atlas)
    logger.info("Metadata built in %.1fs", time.perf_counter() - t0)

    # ── Phase 2: Feature Resolution ─────────────────────────────────
    t0 = time.perf_counter()
    resolved, assignments = feature_resolver.resolve_features(records, atlas, metadata)
    logger.info("Phase 2 done in %.1fs", time.perf_counter() - t0)

    # ── Phase 3: Tributary Enrichment ────────────────────────────────
    t0 = time.perf_counter()
    enricher = TributaryEnricherV2(graph_path)
    enricher.enrich_tributaries(resolved, assignments, atlas)
    logger.info("Phase 3 done in %.1fs", time.perf_counter() - t0)

    # ── Phase 4: Base Regulation Assignment ──────────────────────────
    t0 = time.perf_counter()
    base_regs, reach_level_reg_ids = base_reg_assigner.assign_base_regulations(
        atlas, metadata, assignments, gpkg_path=gpkg_path
    )
    logger.info("Phase 4 done in %.1fs", time.perf_counter() - t0)

    # ── Phase 5: Reach Build + Output ────────────────────────────────
    t0 = time.perf_counter()
    index = reach_builder.build_regulation_index(
        atlas,
        assignments,
        base_regs,
        records,
        reach_level_reg_ids=reach_level_reg_ids,
    )
    logger.info("Phase 5 done in %.1fs", time.perf_counter() - t0)

    # ── Write output (atomic) ────────────────────────────────────────
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Atomic write: temp file → rename (Dave's concern)
    tmp_fd, tmp_path = tempfile.mkstemp(
        dir=output_path.parent, suffix=".tmp", prefix="reg_index_"
    )
    try:
        with open(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(index, f, ensure_ascii=False)
        Path(tmp_path).replace(output_path)
    except Exception:
        Path(tmp_path).unlink(missing_ok=True)
        raise

    # ── Admin visibility export ──────────────────────────────────────
    admin_vis = _collect_admin_visibility(
        records, base_reg_assigner.DEFAULT_BASE_REGS_PATH
    )
    admin_vis_path = output_path.parent / "admin_visibility.json"
    with open(admin_vis_path, "w", encoding="utf-8") as f:
        json.dump(admin_vis, f, ensure_ascii=False, indent=2)
    logger.info("Admin visibility → %s (%d layers)", admin_vis_path, len(admin_vis))

    total = time.perf_counter() - t_start
    logger.info(
        "Pipeline complete in %.1fs — wrote %s",
        total,
        output_path,
    )

    return output_path


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Build regulation_index.json from synopsis + atlas + base regs"
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config.yaml (default: config.yaml)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Load and merge only — skip atlas/graph processing",
    )
    args = parser.parse_args()

    build(config_path=Path(args.config), dry_run=args.dry_run)


if __name__ == "__main__":
    main()
