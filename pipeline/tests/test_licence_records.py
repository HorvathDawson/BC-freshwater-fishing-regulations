"""(owner 2026-10-28) OpenStreetMap-derived committed files carry an ODbL `_licence` record IN the file, are listed in
data/LICENSE-ODbL.md, and every loader skips the record: it never changes what a build reads."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from pipeline.common.licence import entries, odbl

ROOT = Path(__file__).resolve().parents[2]
NOTICE = (ROOT / "data/LICENSE-ODbL.md").read_text(encoding="utf-8")

LICENSED = ["data/source/bc_places.json", "data/source/municipal/osm_streams.geojson",
            "data/curated/waters/added_streams.json", "data/curated/waters/added_lakes.geojson",
            "data/curated/waters/mined_names.json", "data/curated/waters/splits.json",
            "handoff/PLAN/curation-review/tidal/tidal-osm-landmarks.json"]


def _doc(rel: str):
    return json.loads((ROOT / rel).read_text(encoding="utf-8"))


@pytest.mark.parametrize("rel", LICENSED)
def test_each_osm_file_carries_the_odbl_record_and_is_listed(rel):
    lic = _doc(rel)["_licence"]
    assert lic["spdx"] == "ODbL-1.0" and lic["attribution"] == "© OpenStreetMap contributors"
    assert lic["url"] == "https://opendatacommons.org/licenses/odbl/1-0/" and lic["applies_to"]
    assert f"`{rel}`" in NOTICE


def test_entries_unwraps_a_licensed_list_and_passes_a_bare_one():
    assert entries([1, 2], "places") == [1, 2]
    assert entries({"_licence": odbl("x"), "places": [3]}, "places") == [3]
    with pytest.raises(ValueError):
        entries({"_licence": odbl("x")}, "places")


def _at_head(rel: str):
    """The file as committed before the record (or with it, once committed): the loaders' answer must not change."""
    out = subprocess.run(["git", "show", f"HEAD:{rel}"], cwd=ROOT, capture_output=True, text=True)
    return json.loads(out.stdout) if out.returncode == 0 else None


def test_mined_names_loader_reads_the_same_entries():
    from pipeline.atlas.graph.names import load_name_variants
    got = load_name_variants(ROOT / "data/curated/waters/mined_names.json")
    assert got and all("target" in e and "names" in e for e in got)
    head = _at_head("data/curated/waters/mined_names.json")
    if head is not None:
        assert got == entries(head, "entries")
    # every OSM name the record covers is a `source: "osm"` record
    assert any(n["source"] == "osm" for e in got for n in e["names"])


def test_name_variants_bare_list_still_loads():
    from pipeline.atlas.graph.names import load_name_variants
    assert isinstance(load_name_variants(ROOT / "data/curated/waters/name_variants.json"), list)


def test_places_readers_skip_the_record(tmp_path):
    places = entries(_doc("data/source/bc_places.json"), "places")
    assert len(places) > 2000 and all("name" in p and "lat" in p for p in places)
    head = _at_head("data/source/bc_places.json")
    if head is not None:
        assert places == entries(head, "places")
    import sqlite3
    from pipeline.deliver.bundle.build import _places
    small = tmp_path / "bc_places.json"
    small.write_text(json.dumps({"_licence": odbl("x"), "places": places[:5]}))
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE place (n, place_id, name, kind, pop, lat, lon)")

    class Cov:
        def filled(self, *a):
            pass
    assert _places(db, small, tmp_path / "unused.geojson", Cov()) == places[:5]
    assert [r[0] for r in db.execute("SELECT name FROM place ORDER BY n")] == [p["name"] for p in places[:5]]


def test_splits_record_names_real_splits_and_loaders_ignore_it():
    from pipeline.atlas.splits.splits import load_split_defs, split_index
    doc = _doc("data/curated/waters/splits.json")
    idx = split_index(doc)
    assert doc["_licence"]["ids"] and set(doc["_licence"]["ids"]) <= set(idx)
    assert len(load_split_defs(str(ROOT / "data/curated/waters/splits.json"))) == len(idx)


def test_added_lakes_and_streams_loaders_ignore_the_record():
    from pipeline.atlas.waters.added_lakes.ingest import load
    from pipeline.atlas.waters.added_streams.build_dataset import load_build
    lakes = load(ROOT / "data/curated/waters/added_lakes.geojson")
    assert len(lakes) == len(_doc("data/curated/waters/added_lakes.geojson")["features"])
    streams = load_build(ROOT / "data/curated/waters/added_streams.json")
    assert [s["blk"] for s in streams if s["source"] == "osm"] == ["-2300000000"]
