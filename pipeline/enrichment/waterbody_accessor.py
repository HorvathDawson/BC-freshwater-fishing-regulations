"""
waterbody_accessor.py — build-time reader for the precomputed waterbody
matches artifact, replacing pipeline/matching/bathymetry_matcher.py's live
re-matching (identifier/name+wsc/spatial-overlap tiers run fresh against the
gpkg on every build).

The matching itself now happens once, offline, in
pipeline/recurring/waterbody_db/ (fetch + multi-stage match chain against
FIDQ stocking, WSA bathymetry, and gofishbc markers), and its export.py
writes the result to a single static JSON
(config.yaml's data.waterbody_matches, data/bathymetry_pdfs/waterbody_matches.json
by default). This module has zero dependency on that matching machinery —
it only reads the JSON, so a regulations build never touches waterbody_db.db
or the FWA gpkg for this data.

CLI
---
    python -m pipeline.enrichment.waterbody_accessor
    python -m pipeline.enrichment.waterbody_accessor --path /path/to/waterbody_matches.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from project_config import ProjectConfig

logger = logging.getLogger(__name__)


@dataclass
class WaterbodyMatches:
    """str(WATERBODY_KEY)-keyed dicts read from the precomputed export.

    wbk_bathymetry: [{"pdf", "title", "name"}, ...] — depth-map survey sheets.
    wbk_names:      [{"name", "source": "bathymetry"|"stocking"}, ...] — every
                     known alternate name for the waterbody from either source.
    wbk_marker:     {<amenity/access whitelist fields>} — gofishbc marker info.
    """

    wbk_bathymetry: Dict[str, List[Dict[str, str]]] = field(default_factory=dict)
    wbk_names: Dict[str, List[Dict[str, str]]] = field(default_factory=dict)
    wbk_marker: Dict[str, Dict[str, Any]] = field(default_factory=dict)


def default_waterbody_matches_path(cfg: Optional[ProjectConfig] = None) -> Path:
    cfg = cfg or ProjectConfig()
    return cfg.get_path("data", "waterbody_matches", default="data/bathymetry_pdfs/waterbody_matches.json")


def load_waterbody_matches(path: Optional[Path] = None) -> WaterbodyMatches:
    """Load the precomputed waterbody matches. Never raises — degrades to
    three empty dicts (with a warning) when the file is absent, so a build
    without this optional dataset still works, same graceful-degrade
    contract the old build_wbk_bathymetry_map() had."""
    p = path or default_waterbody_matches_path()
    if not p.exists():
        logger.warning(
            "Waterbody matches: not found at %s — no bathymetry/marker "
            "enrichment or stocking/bathymetry name variants will be attached "
            "to reaches. Run: python -m pipeline.recurring.waterbody_db.export",
            p,
        )
        return WaterbodyMatches()

    with open(p, encoding="utf-8") as f:
        data = json.load(f)

    matches = WaterbodyMatches(
        wbk_bathymetry=data.get("wbk_bathymetry", {}),
        wbk_names=data.get("wbk_names", {}),
        wbk_marker=data.get("wbk_marker", {}),
    )
    logger.info(
        "Waterbody matches: %d wbk(s) with bathymetry, %d wbk(s) with names, "
        "%d wbk(s) with marker info (generated_at=%s)",
        len(matches.wbk_bathymetry), len(matches.wbk_names), len(matches.wbk_marker),
        data.get("generated_at", "unknown"),
    )
    return matches


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description="Load and summarize the precomputed waterbody matches artifact.")
    parser.add_argument("--path", type=Path, default=None, help="waterbody_matches.json path (default from config.yaml).")
    args = parser.parse_args(argv)

    matches = load_waterbody_matches(args.path)
    print(f"wbk_bathymetry: {len(matches.wbk_bathymetry)} waterbody_key(s)")
    print(f"wbk_names:      {len(matches.wbk_names)} waterbody_key(s)")
    print(f"wbk_marker:     {len(matches.wbk_marker)} waterbody_key(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
