"""The rank of a place is decided once, where the gazetteer is fetched.

`data/fetch_data.py` assigns a rank over nine place kinds and writes it into every record.
`pipeline/deliver/tiles/export.py` used to recompute it from a four-key table, so 3,459 of 4,677
places carried a rank the gazetteer disagreed with — and because the tile's minzoom is
derived from it, 1,731 localities were drawn from z10 instead of z14.

Two tables for one fact is the failure; this pins that there is one.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from pipeline.common.curated import CURATED, SOURCE

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "data"))


def test_the_exporter_has_no_rank_table_of_its_own():
    src = (ROOT / "pipeline/deliver/tiles/export.py").read_text()
    assert "_PLACE_RANK" not in src, (
        "the exporter is deciding rank again; it must read the one the fetch wrote")
    assert 'p.get("rank"' in src


def test_every_fetched_place_carries_a_rank():
    places = json.loads((SOURCE / "bc_places.json").read_text())
    assert places, "no gazetteer fetched"
    missing = [p["name"] for p in places if "rank" not in p]
    assert not missing, f"{len(missing)} places have no rank: {missing[:5]}"


def test_the_rank_covers_every_kind_the_fetch_asks_for():
    import fetch_data

    kinds = set(fetch_data._PLACE_KINDS)
    places = json.loads((SOURCE / "bc_places.json").read_text())
    seen = {p["place"] for p in places}
    assert seen <= kinds, f"the gazetteer holds kinds the rank table does not: {seen - kinds}"
