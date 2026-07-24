#!/usr/bin/env python3
"""
build_db.py — one-command build of ``anglerinfo.db`` from scratch:
fetch every external feed, then run the full match chain in order.

This is the *expensive, external-network* half of the waterbody enrichment
(FIDQ stocking + WSA bathymetry + WDIC + gofishbc, all behind rate-limited /
WAF-fronted sites). It belongs at data-fetch time alongside the other
downloads — it is invoked by ``data/fetch_data.py`` (the "fetch all" stage),
not by the regulations build. The cheap half (reading the finished db into
``anglerinfo_matches.json``) is :mod:`pipeline.recurring.anglerinfo.export`,
run as part of the regulations build.

Both the db and the exported JSON live under the pipeline output tree — see
:mod:`pipeline.recurring.anglerinfo.paths`.

Steps (order matters — each match stage reads the previous stage's table):
    fetch_all           stocking + bathymetry + wdic  → raw tables
    match               → match_wbid
    match_fwa_gazette   → match_fwa_gazette   (needs the FWA gpkg)
    match_fwa_identifier→ match_fwa_identifier
    match_fwa_override  → match_fwa_override
    match_wbid_gazette  → match_wbid_gazette
    match_final         → match_final (the aggregated result export.py reads)

CLI
---
    python -m pipeline.recurring.anglerinfo.build_db
    python -m pipeline.recurring.anglerinfo.build_db --skip-fetch   # match only (db already fetched)
    python -m pipeline.recurring.anglerinfo.build_db --export       # also write anglerinfo_matches.json at the end
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import List

logger = logging.getLogger(__name__)

# Match chain, in dependency order — each stage's main() reads the table the
# previous stage wrote (see each module's docstring).
_MATCH_STEPS = (
    "match",
    "match_fwa_gazette",
    "match_fwa_identifier",
    "match_fwa_override",
    "match_wbid_gazette",
    "match_final",
)


def build_db(skip_fetch: bool = False, do_export: bool = False) -> None:
    """Run the fetch + match chain end-to-end, writing anglerinfo.db.

    Raises on the first failing stage (fail loud) rather than committing a
    silently-incomplete db — a partial match chain would export bad matches.
    """
    from . import paths

    paths.DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    if not skip_fetch:
        logger.info("=== anglerinfo: fetch (stocking + bathymetry + wdic) ===")
        from . import fetch_all

        rc = fetch_all.main([])
        if rc != 0:
            raise RuntimeError(f"fetch_all failed (exit {rc})")

    import importlib

    for step in _MATCH_STEPS:
        logger.info("=== anglerinfo: %s ===", step)
        module = importlib.import_module(f".{step}", __package__)
        rc = module.main([])
        if rc != 0:
            raise RuntimeError(f"{step} failed (exit {rc})")

    if do_export:
        logger.info("=== anglerinfo: export → anglerinfo_matches.json ===")
        from . import export

        export.main([])

    logger.info("anglerinfo build complete → %s", paths.DB_PATH)


def main(argv: List[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(
        description="Build anglerinfo.db (fetch + full match chain)."
    )
    parser.add_argument(
        "--skip-fetch", action="store_true",
        help="Skip the external fetch stage; run the match chain against the existing db.",
    )
    parser.add_argument(
        "--export", action="store_true",
        help="Also write anglerinfo_matches.json after the match chain completes.",
    )
    args = parser.parse_args(argv)

    build_db(skip_fetch=args.skip_fetch, do_export=args.export)
    return 0


if __name__ == "__main__":
    sys.exit(main())
