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


def test_pin_copies_the_fetchs_layer_dates_into_the_manifest(tmp_path, monkeypatch):
    # T561: the per-layer source days the fetch wrote beside a GeoPackage travel with its pin
    import json

    from pipeline.common import source_dates
    src = tmp_path / "source"
    src.mkdir()
    g = src / "bc_fisheries_data.gpkg"
    g.write_bytes(b"gpkg")
    source_dates.record(g, "streams", "2024-03-26", "FWA publication")
    monkeypatch.setattr(P, "SOURCE", src)
    monkeypatch.setattr(P, "MANIFEST", src / "pins.json")
    monkeypatch.setattr(P.subprocess, "run", lambda *a, **k: None)
    assert P.pin([str(g)], "upstream") == 0
    pinned = json.loads((src / "pins.json").read_text())["files"]["bc_fisheries_data.gpkg"]
    assert pinned["layer_dates"] == {"streams": {"date": "2024-03-26", "basis": "FWA publication"}}
