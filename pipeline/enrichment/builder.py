"""Regulation index builder — orchestrates all 5 phases.

Usage:
    python -m pipeline.enrichment.builder
    python -m pipeline.enrichment.builder --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
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
    "land_access": "land_access",
    "aboriginal_lands": "aboriginal_lands",
}

# Layers that show ALL features regardless of regulation status.
# Layers NOT listed here default to "regulated_only" — only features
# with an explicit admin_target reference are shown on the map.
# land_access is "all" so watersheds/DND/private land render even though
# most of them are only covered by the unscoped (no feature_id)
# LAND_ACCESS_CLOSED / LAND_ACCESS_RESTRICTED advisories, not a specific
# feature_id target like the Malcolm Knapp closure.
_SHOW_ALL_LAYERS = {"eco_reserves", "parks_nat", "aboriginal_lands", "land_access"}


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
    from pipeline.matching.match_table import OverrideEntry

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

    Returns the path to the deploy output directory.
    """
    from pipeline.atlas.freshwater_atlas import FreshWaterAtlas
    from pipeline.deploy.r2_sharder import shard_from_dict
    from pipeline.deploy.mobile_sharder import build_mobile_sqlite
    from pipeline.matching.match_table import (
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
        project_root
        / cfg["output"]["pipeline"]["extraction"]
        / "synopsis_raw_data.json"
    )
    match_table_path = project_root / cfg["output"]["pipeline"]["match_table"]
    overrides_path = OVERRIDES_PATH
    session_path = (
        project_root / cfg["output"]["pipeline"]["parsing"] / "session_state.json"
    )
    atlas_path = project_root / cfg["output"]["pipeline"]["atlas"]
    graph_path = project_root / cfg["output"]["pipeline"]["graph"]["graph"]
    gpkg_path = project_root / cfg["data_accessor"]["gpkg_path"]
    deploy_dir = project_root / cfg["output"]["pipeline"]["deploy"]
    shard_version = cfg["output"]["pipeline"].get("shard_version", 1)
    # Optional OSM populated-places gazetteer for nearest-town search tagging.
    # Absent file degrades gracefully — search entries carry no nearby_towns.
    places_path = project_root / "data" / "bc_places.json"

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
        return deploy_dir

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
    from pipeline.enrichment.anglerinfo_accessor import load_anglerinfo
    from pipeline.enrichment.nearby_towns import NearbyTownsIndex

    # Regenerate anglerinfo_matches.json from the fetched anglerinfo.db so the
    # build always reads a fresh export. Non-fatal + graceful: if the db was
    # never built (it's fetched separately, at data-fetch time), the export is
    # skipped and load_anglerinfo() degrades to empty enrichment.
    try:
        from pipeline.recurring.anglerinfo.paths import DB_PATH
        from pipeline.recurring.anglerinfo import export as wbdb_export

        if DB_PATH.exists():
            wbdb_export.main([])
        else:
            logger.info(
                "anglerinfo.db not found at %s — skipping matches export "
                "(run data.fetch_data to build it)", DB_PATH,
            )
    except Exception:
        logger.warning("anglerinfo_matches export failed — continuing", exc_info=True)

    matches = load_anglerinfo()
    towns_index = NearbyTownsIndex.from_file(places_path)
    logger.info("Loaded %d towns for nearest-town search tagging", len(towns_index))
    index = reach_builder.build_regulation_index(
        atlas,
        assignments,
        base_regs,
        records,
        reach_level_reg_ids=reach_level_reg_ids,
        match_table_path=str(match_table_path),
        wbk_bathymetry=matches.wbk_bathymetry,
        wbk_names=matches.wbk_names,
        wbk_marker=matches.wbk_marker,
        towns_index=towns_index,
    )
    logger.info("Phase 5 done in %.1fs", time.perf_counter() - t0)

    # ── Deploy: shard into R2-ready files ──────────────────────────────
    t0 = time.perf_counter()
    deploy_dir.mkdir(parents=True, exist_ok=True)

    # Flat (unsharded) wbk -> reach_id map, for pipeline.recurring.stocking_resolver
    # (and any future consumer) to resolve a waterbody_key to a reach without
    # needing the full sharded poly_reaches/ tree.
    poly_reaches_path = deploy_dir / "poly_reaches.json"
    with open(poly_reaches_path, "w", encoding="utf-8") as f:
        json.dump(index["poly_reaches"], f, ensure_ascii=False)
    logger.info("Poly reaches → %s (%d waterbodies)", poly_reaches_path, len(index["poly_reaches"]))

    summary = shard_from_dict(index, deploy_dir, shard_version)
    logger.info(
        "Deploy shards written in %.1fs — %d fid, %d reach, %d poly shards",
        time.perf_counter() - t0,
        summary["fid_shards"],
        summary["reach_shards"],
        summary["poly_shards"],
    )

    # ── Deploy: mobile SQLite bundle (offline React Native app) ────────
    t0 = time.perf_counter()
    mobile_summary = build_mobile_sqlite(index, deploy_dir, shard_version)
    logger.info(
        "Mobile SQLite bundle written in %.1fs — %.1f MB (%.1f MB gz) → %s",
        time.perf_counter() - t0,
        mobile_summary["db_bytes"] / 1_048_576,
        mobile_summary["gz_bytes"] / 1_048_576,
        mobile_summary["mobile_dir"],
    )

    # ── Admin visibility export ──────────────────────────────────────
    admin_vis = _collect_admin_visibility(
        records, base_reg_assigner.DEFAULT_BASE_REGS_PATH
    )
    admin_vis_path = deploy_dir / "admin_visibility.json"
    with open(admin_vis_path, "w", encoding="utf-8") as f:
        json.dump(admin_vis, f, ensure_ascii=False, indent=2)
    logger.info("Admin visibility → %s (%d layers)", admin_vis_path, len(admin_vis))

    # ── Copy match_table.json to deploy dir (for R2 / Python Worker) ─
    import shutil

    mt_dest = deploy_dir / "match_table.json"
    shutil.copy2(match_table_path, mt_dest)
    logger.info("Match table → %s", mt_dest)

    # ── Copy row_images to deploy dir ────────────────────────────────
    extraction_images = (
        project_root / cfg["output"]["pipeline"]["extraction"] / "row_images"
    )
    if extraction_images.is_dir():
        import shutil

        dest_images = deploy_dir / "row_images"
        if dest_images.exists():
            shutil.rmtree(dest_images)
        shutil.copytree(extraction_images, dest_images)
        logger.info("Row images → %s", dest_images)

    # ── Copy bathymetry survey PDFs to deploy dir ────────────────
    # Depth-map PDFs live in data/bathymetry_pdfs/ (populated by
    # data/fetch_data.py). Copying them into deploy/bathymetry/ makes the deploy
    # tree the single artifact that both the production R2 sync
    # (scripts/seed-r2.sh) and the local dev seed (scripts/seed.mjs) serve depth
    # maps from.
    bathy_pdf_src = project_root / "data" / "bathymetry_pdfs"
    if bathy_pdf_src.is_dir():
        import shutil

        dest_bathy = deploy_dir / "bathymetry"
        dest_bathy.mkdir(parents=True, exist_ok=True)
        copied = 0
        for pdf in sorted(bathy_pdf_src.glob("*.pdf")):
            shutil.copy2(pdf, dest_bathy / pdf.name)
            copied += 1
        logger.info("Bathymetry PDFs → %s (%d files)", dest_bathy, copied)

    # ── Copy basemap tiles to deploy dir ──────────────────────
    # bc.pmtiles is the OSM/Protomaps map background. It's not produced by the
    # pipeline — data/fetch_data.py downloads it from R2 into data/. Copy it into
    # deploy/ so the same tree serves the basemap at /bc.pmtiles.
    basemap_src = project_root / "data" / "bc.pmtiles"
    if basemap_src.is_file():
        import shutil

        shutil.copy2(basemap_src, deploy_dir / "bc.pmtiles")
        logger.info("Basemap tiles → %s", deploy_dir / "bc.pmtiles")

    # ── In-season changes: scrape + resolve ──────────────────────────
    # Produces in_season.json as an initial seed in the deploy folder.
    # Non-fatal: if the scrape fails (network, page changes), warn and
    # continue — the daily GHA cron will keep it updated anyway.
    t0 = time.perf_counter()
    try:
        from pipeline.recurring.in_season_scraper import scrape_in_season_changes
        from pipeline.recurring.in_season_resolver import (
            resolve_to_reaches,
            _load_match_table,
        )

        scraped = scrape_in_season_changes()
        total_changes = sum(len(s.rows) for s in scraped.sections)
        logger.info("Scraped %d in-season changes", total_changes)

        table = _load_match_table(mt_dest, overrides_path)

        tier0_path = deploy_dir / "tier0.json"
        with open(tier0_path, encoding="utf-8") as f:
            tier0_data = json.load(f)

        result = resolve_to_reaches(scraped, table, tier0_data)
        in_season_path = deploy_dir / "in_season.json"
        with open(in_season_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        matched = result["stats"]["matched"]
        unmatched = result["stats"]["unmatched"]
        logger.info(
            "In-season changes → %s (%d matched, %d unmatched, %.1fs)",
            in_season_path,
            matched,
            unmatched,
            time.perf_counter() - t0,
        )
    except Exception:
        logger.warning(
            "In-season scrape/resolve failed — skipping (%.1fs)",
            time.perf_counter() - t0,
            exc_info=True,
        )

    # ── Stocking info: resolve to reach IDs ───────────────────────────
    # Produces stocking.json in the deploy folder, same shape/role as
    # in_season.json above — a standalone recurring artifact the frontend
    # fetches directly (not baked into the reach shards, which only rebuild
    # on a full pipeline run). This keeps stocking data refreshable on its
    # own lightweight cadence, same as in-season notices.
    # Non-fatal: anglerinfo.db (the underlying fetch+match data) is refreshed
    # on its own, separate, manual cadence — see pipeline/recurring/anglerinfo.
    t0 = time.perf_counter()
    try:
        from pipeline.recurring.stocking_resolver import resolve_stocking

        result = resolve_stocking(poly_reaches_path=poly_reaches_path)
        stocking_path = deploy_dir / "stocking.json"
        with open(stocking_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        stats = result["stats"]
        logger.info(
            "Stocking data → %s (%d/%d resolved to a reach, %.1fs)",
            stocking_path,
            stats["resolved_to_reach"],
            stats["total"],
            time.perf_counter() - t0,
        )
    except Exception:
        logger.warning(
            "Stocking resolve failed — skipping (%.1fs)",
            time.perf_counter() - t0,
            exc_info=True,
        )

    total = time.perf_counter() - t_start
    logger.info(
        "Pipeline complete in %.1fs — deploy dir: %s",
        total,
        deploy_dir,
    )

    return deploy_dir


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
