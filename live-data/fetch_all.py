#!/usr/bin/env python3
"""
fetch_all.py — runs every live-data/ feed's own fetch step in one pass.
Currently just waterbody_db (stocking/FIDQ + gofishbc markers + bathymetry/WSA
survey CSV + polygons + the full WDIC 1:50K reference layer, all in one
shared DB — see waterbody_db/README.md). The original standalone `stocking/`,
`bathymetry/`, and `common/` POCs this absorbed have been removed now that
waterbody_db/'s own matching chain fully supersedes them.

Each feed stays runnable standalone (this just sequences the same
entrypoints as separate subprocesses — see each feed's own README/CLI
section); this script adds nothing but ordering + a single command for
"fetch everything live-data/ currently tracks". Matching
(waterbody_db/match.py and its downstream match_fwa_*/match_final.py passes)
is deliberately not run here — those are separate, review-worthy steps, not
a fetch concern.

CLI
---
    cd live-data
    python fetch_all.py                     # waterbody_db (only feed currently)
    python fetch_all.py --skip waterbody_db  # skip one or more feeds (repeatable)
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent
FEEDS = ["waterbody_db"]


def _run(cwd: Path, args: List[str]) -> None:
    cmd = [sys.executable, *args]
    logger.info("--- (cd %s && %s) ---", cwd.name, " ".join(cmd[1:]))
    subprocess.run(cmd, cwd=cwd, check=True)


def fetch_waterbody_db() -> None:
    _run(ROOT / "waterbody_db", ["fetch_all.py"])


FETCHERS = {
    "waterbody_db": fetch_waterbody_db,
}


def main(argv: List[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(description="Fetch every live-data/ feed in one pass.")
    parser.add_argument(
        "--skip", action="append", choices=FEEDS, default=[],
        help="Skip a feed (repeatable).",
    )
    args = parser.parse_args(argv)

    for feed in FEEDS:
        if feed in args.skip:
            logger.info("--- skipping %s ---", feed)
            continue
        FETCHERS[feed]()

    return 0


if __name__ == "__main__":
    sys.exit(main())
