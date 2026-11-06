"""T416: the pins manifest names every git-ignored input a fresh clone needs — data/source files, and `generated/…`
keys for the generated inputs no stage makes (the synopsis extraction, its row boxes)."""
from __future__ import annotations

import pytest

from pipeline.common.curated import GENERATED, SOURCE
from pipeline.tools import source_pins as P


def test_a_generated_key_lives_under_data_generated_and_round_trips():
    rb = GENERATED.regs.extraction / "row_boxes.json"
    assert P.key_of(rb) == "generated/regs/extraction/row_boxes.json"
    assert P.local(P.key_of(rb)) == rb
    assert P.key_of(SOURCE / "hydat.sqlite3") == "hydat.sqlite3"
    assert P.local("hydat.sqlite3") == SOURCE / "hydat.sqlite3"
    with pytest.raises(SystemExit, match="under neither"):
        P.key_of(P.ROOT / "pipeline" / "build.py")


def test_the_release_inputs_a_fresh_clone_lacked_are_pinned():
    # each was found missing on the fresh clone (T416): no basemap -> `basemap: null`; no row boxes -> no highlights
    files = P.load()["files"]
    for k in ("basemap.pmtiles", "generated/regs/extraction/row_boxes.json",
              "generated/regs/extraction/synopsis_raw_data.json", "bc_fisheries_data.gpkg", "hydat.sqlite3"):
        assert k in files and len(files[k]["sha256"]) == 64 and files[k]["url"].startswith("https://")
