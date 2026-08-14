#!/usr/bin/env python3
"""
fetch_all.py — runs this folder's three fetchers (fetch_stocking.py,
fetch_bathymetry.py, fetch_wdic.py — see this dir's README) in one pass, all
writing into the shared anglerinfo.db.

Order matters for `match.py`'s inputs but not for correctness: fetch_wdic.py
is self-contained (it fetches the entire WDIC layer, not just identifiers
the other two reference — see its own docstring), so it can run in any
order relative to the other two. It's run last here only so a single
`python fetch_all.py` leaves every table match.py needs populated in one
command, matching live-data/fetch_all.py's top-level "fetch everything"
convention one level up.

CLI
---
    python -m pipeline.recurring.anglerinfo.fetch_all                   # stocking, bathymetry, wdic, in order
    python -m pipeline.recurring.anglerinfo.fetch_all --skip stocking     # skip one or more (repeatable)
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)

HERE = Path(__file__).parent
FEEDS = ["stocking", "bathymetry", "wdic"]


def fetch_stocking() -> None:
    from . import fetch_stocking
    fetch_stocking.main(["update"])


def fetch_bathymetry() -> None:
    from . import fetch_bathymetry
    fetch_bathymetry.main([])


def fetch_wdic() -> None:
    from . import fetch_wdic
    fetch_wdic.main([])


FETCHERS = {
    "stocking": fetch_stocking,
    "bathymetry": fetch_bathymetry,
    "wdic": fetch_wdic,
}


def main(argv: List[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(description="Fetch stocking + bathymetry + WDIC into this folder's shared anglerinfo.db.")
    parser.add_argument(
        "--skip", action="append", choices=FEEDS, default=[],
        help="Skip a feed (repeatable).",
    )
    args = parser.parse_args(argv)

    for feed in FEEDS:
        if feed in args.skip:
            logger.info("--- skipping %s ---", feed)
            continue
        logger.info("--- %s ---", feed)
        FETCHERS[feed]()

    return 0


if __name__ == "__main__":
    sys.exit(main())
