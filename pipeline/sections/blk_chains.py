"""Step 1 (03 S1): merge FWA linear features into per-BLK chains.

Group `streams` rows by BLUE_LINE_KEY (via FWADataAccessor), sort by DOWNSTREAM_ROUTE_MEASURE
(mouth->source), stitch geometry, record route spans + under-lake WaterbodyRuns, aggregate
order/magnitude. Skip the 999-999999 sentinel WSC/BLKs. Names are attached by names.py.
"""

from __future__ import annotations

from typing import Iterable

from .models import BlkChain


def build_blk_chains(gpkg_path: str, lake_manmade_wbks: set[str]) -> list[BlkChain]:
    """Return one BlkChain per blue line. ``lake_manmade_wbks`` classifies under-lake runs."""
    raise NotImplementedError
