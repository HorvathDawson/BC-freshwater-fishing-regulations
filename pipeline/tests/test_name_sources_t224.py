"""T224 (owner 2026-10-28): every name carries its REAL source, only a gazetted name is official, and a mined name
the owner accepted reaches the registry, the bundle's `name_source` and the review app's Names mode.

Pure: a temp marks store and temp candidates; nothing under data/curated is written."""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

from pipeline.common.models import NameRecord, NameSource, NameTuple, RegistryItem
from pipeline.common.name_sources import LABELS, CITY_LABELS, is_official, label_of

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"


# ------------------------------------------------------------------------------------------------ labels
def test_every_source_has_a_label_and_only_gazette_is_official():
    assert set(LABELS) == set(NameSource)
    assert [s for s in NameSource if is_official(s)] == [NameSource.gazette]
    assert NameTuple("Kilgard Creek", NameSource.municipal, label=CITY_LABELS["abbotsford"]).source_label \
        == "City of Abbotsford stream layer"
    assert NameTuple("X", NameSource.osm).source_label == "OpenStreetMap"
    assert not NameTuple("X", NameSource.municipal).official and NameTuple("X", NameSource.gazette).official
    assert label_of("no-such-code") == LABELS[NameSource.alias]


def test_municipal_and_osm_never_beat_an_official_name():
    from pipeline.atlas.graph.names import _PRIORITY
    for s in (NameSource.municipal, NameSource.osm):
        assert _PRIORITY[NameSource.gazette] < _PRIORITY[s] < _PRIORITY[NameSource.alias]


# ------------------------------------------------------------------------------------------------ graph -> registry
def test_apply_name_variants_carries_label_and_ref():
    from pipeline.atlas.graph.names import apply_name_variants
    from pipeline.common.models import StreamGraph, StreamNode, NodeKind
    import dataclasses
    fields = {f.name for f in dataclasses.fields(StreamNode)}
    node = StreamNode(**{k: v for k, v in dict(node_id="111:0", kind=NodeKind.stream, blk="111", down_m=0.0,
                                                up_m=10.0, display_name="", name_tuples=()).items() if k in fields})
    g = StreamGraph(nodes={"111:0": node}, edges=[]) if "edges" in {f.name for f in dataclasses.fields(StreamGraph)} \
        else StreamGraph(nodes={"111:0": node})
    apply_name_variants(g, [{"target": {"blks": ["111"]}, "names": [
        {"name": "Kilgard Creek", "source": "municipal", "source_label": "City of Abbotsford stream layer",
         "source_ref": "abbotsford#314607", "display": True},
        {"name": "Lonzo Creek (North Fork)", "source": "gauge", "source_ref": "ECCC station 08MH079"}]}])
    n = g.nodes["111:0"]
    assert n.display_name == "Kilgard Creek"            # display: true wins over the higher-ranked gauge name
    by = {t.name: t for t in n.name_tuples}
    assert by["Kilgard Creek"].source_label == "City of Abbotsford stream layer"
    assert by["Kilgard Creek"].ref == "abbotsford#314607"
    assert by["Lonzo Creek (North Fork)"].source_label == "ECCC hydrometric station name"


def test_name_records_pick_the_best_source_and_flag_only_gazette_official():
    from pipeline.atlas.registry.build import name_records
    recs = name_records(["Fraser River", "FRASER RIVER", "Kilgard Creek", "Pinned"], [
        NameTuple("Fraser River", NameSource.regulation), NameTuple("Fraser River", NameSource.gazette),
        NameTuple("FRASER RIVER", NameSource.regulation),
        NameTuple("Kilgard Creek", NameSource.municipal, label="City of Abbotsford stream layer", ref="r")])
    by = {r.name: r for r in recs}
    assert by["Fraser River"].source == "gazette" and by["Fraser River"].official
    assert by["FRASER RIVER"].source == "regulation" and not by["FRASER RIVER"].official
    assert by["Kilgard Creek"] == NameRecord("Kilgard Creek", "municipal", "City of Abbotsford stream layer", "r", False)
    assert by["Pinned"].source == "override" and not by["Pinned"].official


def test_registry_io_round_trips_name_sources():
    from pipeline.atlas.registry.io import _item_from_dict, _item_to_dict
    it = RegistryItem(id="wsc:1", name="Kilgard Creek", kind="stream", variants=("Kilgard Creek",),
                      name_sources=(NameRecord("Kilgard Creek", "municipal", "City of Abbotsford stream layer",
                                               "abbotsford#1", False),))
    d = _item_to_dict(it)
    assert d["name_sources"] == [{"name": "Kilgard Creek", "source": "municipal",
                                  "label": "City of Abbotsford stream layer", "ref": "abbotsford#1", "official": False}]
    assert _item_from_dict(d).name_sources == it.name_sources
    assert "name_sources" not in _item_to_dict(RegistryItem(id="area:x", name="x", kind="area"))


def test_added_streams_names_carry_their_city_and_excluded_fwa_is_official():
    from pipeline.atlas.build import _added_name_variant_entries
    out = _added_name_variant_entries([
        {"kind": "added", "name": "Willow Creek", "target_blk": "-2000000392", "target_gnis": "", "source": "burnaby"},
        {"kind": "duplicate", "name": "Tillicum Slough", "target_blk": "356346715", "target_gnis": "",
         "source": "burnaby"},
        {"kind": "excluded_fwa", "name": "Still Creek", "target_blk": "-2000000392", "target_gnis": "12345",
         "source": "burnaby"}])
    n = [e["names"][0] for e in out]
    assert n[0]["source"] == "municipal" and n[0]["source_label"] == "City of Burnaby waterways layer"
    assert n[1]["source"] == "municipal"
    assert n[2]["source"] == "gazette" and n[2]["source_ref"] == "gnis:12345"


def test_curated_variants_are_labelled_with_their_real_source():
    """The audit's fixes hold: no Burnaby-map name or FWA gazetted name is a bare `alias`; every added-streams
    variant names its city; no curated entry targets an FWA line the added streams removed."""
    nv = json.loads((REPO / "data/curated/waters/name_variants.json").read_text())
    for e in nv:
        for n in e["names"]:
            note = n.get("note") or ""
            assert not (n["source"] == "alias" and ("City of Burnaby" in note or "FWA gazetted name" in note)), n
    added = json.loads((REPO / "data/curated/waters/added_streams.json").read_text())
    # every variant names its city, or (T222) OpenStreetMap for a stream taken from OSM (osm_streams.geojson)
    assert all(v.get("source") in CITY_LABELS or v.get("source") == "osm" for v in added["name_variants"])
    dead = {"356101591", "356116640", "356311092", "356206901", "356216839", "356093488", "356189846"}
    assert not any(set(e["target"].get("blks") or ()) & dead for e in nv)


# ------------------------------------------------------------------------------------------------ bundle
def test_bundle_name_source_rows_skip_official_names():
    from pipeline.deliver.bundle.build import name_source_rows
    items = [
        {"id": "wsc:1", "name": "Kilgard Creek", "name_sources": [
            {"name": "Kilgard Creek", "source": "municipal", "label": "City of Abbotsford stream layer", "official": False},
            {"name": "Lonzo Creek (North Fork)", "source": "gauge", "label": "", "official": False}]},
        {"id": "gnis:2", "name": "Fraser River", "name_sources": [
            {"name": "Fraser River", "source": "gazette", "label": "BC Geographical Names (official)", "official": True},
            {"name": "FRASER RIVER", "source": "regulation", "label": "Freshwater Fishing Synopsis", "official": False}]},
        {"id": "gnis:3", "name": "Old", "variants": []}]
    rows = name_source_rows(items, {("wsc:1", "Lonzo Creek (North Fork)"), ("gnis:2", "FRASER RIVER")})
    assert rows == [("wsc:1", "Kilgard Creek", "municipal", "City of Abbotsford stream layer"),
                    ("wsc:1", "Lonzo Creek (North Fork)", "gauge", "ECCC hydrometric station name"),
                    ("gnis:2", "FRASER RIVER", "regulation", "Freshwater Fishing Synopsis")]


def test_core_contract_carries_name_source():
    from pipeline.deliver.release.build import CORE_TABLES
    from pipeline.deliver.release.contract import FILES
    assert "name_source" in CORE_TABLES
    assert FILES["core"]["tables"]["name_source"] == ["item_id", "name", "src", "label"]
    schema = (REPO / "pipeline/deliver/bundle/schema.sql").read_text()
    assert "CREATE TABLE name_source (item_id TEXT NOT NULL, name TEXT NOT NULL, src TEXT NOT NULL" in schema


# ------------------------------------------------------------------------------------------------ review + accept
def _cand(blk, name, conf, rel="unnamed", auto=False, rivals=(), src="osm", label="OpenStreetMap"):
    return {"target": {"kind": "stream", "blk": blk}, "current_names": [], "registry_items": [], "proposed": name,
            "relation": rel, "confidence": conf, "auto_accept_eligible": auto, "rival_names": list(rivals),
            "corroborated_by": [],
            "sources": [{"source": src, "source_label": label, "source_ref": "ref", "official": False,
                         "confidence": conf, "flags": [],
                         "evidence": {"blk_overlap": 0.9, "source_overlap": 0.9, "lonlat": [-122.2, 49.05]}}]}


CANDS = [
    _cand("356314888", "Kilgard Creek", "medium", rivals=["Lonzo Creek"], src="municipal:abbotsford",
          label="City of Abbotsford stream layer"),
    _cand("356314888", "Lonzo Creek", "high", rivals=["Kilgard Creek"], src="eccc",
          label="ECCC hydrometric station name"),
    _cand("1", "Auto Creek", "high", auto=True),
    _cand("2", "Review Creek", "medium"),
    _cand("3", "Low Creek", "low"),
]


@pytest.fixture()
def env(tmp_path, monkeypatch):
    (tmp_path / "cands.json").write_text(json.dumps({"candidates": CANDS}))
    (tmp_path / "marks.jsonl").write_text("")
    monkeypatch.setenv("CURATION_NAME_CANDIDATES", str(tmp_path / "cands.json"))
    monkeypatch.setenv("CURATION_MARKS", str(tmp_path / "marks.jsonl"))
    monkeypatch.setenv("CURATION_BACKUP_DIR", str(tmp_path / "backups"))
    monkeypatch.syspath_prepend(str(BACKEND))
    for mod in ("app", "reuse", "variants", "writes", "review"):
        sys.modules.pop(mod, None)
    from pipeline.regs.review import names as N
    N._load.cache_clear()
    import app as app_mod                                     # noqa: E402
    from fastapi.testclient import TestClient
    yield {"client": TestClient(app_mod.app), "dir": tmp_path, "N": N}
    for mod in ("app", "reuse", "variants", "writes", "review"):
        sys.modules.pop(mod, None)


def _digest(d: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(d.rglob("*")):
        if p.is_file():
            h.update(str(p).encode())
            h.update(p.read_bytes())
    return h.hexdigest()


def test_names_queue_groups_and_fates(env):
    c = env["client"]
    q = c.get("/api/names/queue").json()
    by = {r["id"]: r for r in q}
    assert [r["group"] for r in q][:2] == ["owner", "owner"]
    assert by["blk:356314888~kilgard-creek"]["display"] and by["blk:356314888~kilgard-creek"]["fate"] == "accepted"
    assert by["blk:356314888~lonzo-creek"]["proposed"] == "Lonzo Creek (North Fork)"
    assert by["blk:1~auto-creek"]["group"] == "auto" and by["blk:1~auto-creek"]["fate"] == "accepted"
    assert by["blk:2~review-creek"]["fate"] == "waiting"
    assert by["blk:2~review-creek"]["sources"] == ["OpenStreetMap"]
    assert c.get("/api/names/queue", params={"group": "nope"}).status_code == 422
    d = c.get("/api/names/candidates/blk:2~review-creek").json()
    assert d["sources"][0]["label"] == "OpenStreetMap" and d["map"]["features"]
    assert c.get("/api/names/candidates/blk:0~x").status_code == 404
    p = c.get("/api/names/progress").json()
    assert p["by_group"] == {"owner": 2, "auto": 1, "review": 2} and p["spot"]["total"] == 1


def test_a_name_mark_is_one_commit_and_decides_acceptance(env):
    c = env["client"]
    curated = _digest(REPO / "data/curated")
    base = {"scope": "name", "answers": {"P": "y"}, "reviewer": "test"}
    assert c.post("/api/marks", json={**base, "target": "blk:2~review-creek"}).status_code == 422   # no call
    assert c.post("/api/marks", json={**base, "target": "blk:2~review-creek", "call": "maybe"}).status_code == 422
    r = c.post("/api/marks", json={**base, "targets": ["blk:2~review-creek", "blk:1~auto-creek"], "call": "accept"})
    assert r.status_code == 200 and r.json()["n"] == 2, r.text
    r = c.post("/api/marks", json={**base, "target": "blk:1~auto-creek", "call": "reject"})
    assert r.status_code == 200, r.text
    assert c.post("/api/marks", json={**base, "targets": ["blk:9~nope"], "call": "accept"}).status_code == 404
    lines = [json.loads(x) for x in (env["dir"] / "marks.jsonl").read_text().splitlines()]
    assert [(m["target"], m["call"]) for m in lines] == [
        ("blk:2~review-creek", "accept"), ("blk:1~auto-creek", "accept"), ("blk:1~auto-creek", "reject")]
    assert _digest(REPO / "data/curated") == curated
    # the tool: auto + owner + accepted marks − rejected marks; every name with its real source
    from pipeline.tools.mined_names import build
    got = build(env["dir"] / "marks.jsonl")
    names = {(tuple(e["target"]["blks"]), n["name"]): n for e in got for n in e["names"]}
    assert set(names) == {(("356314888",), "Kilgard Creek"), (("356314888",), "Lonzo Creek (North Fork)"),
                          (("2",), "Review Creek")}
    assert names[(("356314888",), "Kilgard Creek")]["display"] is True
    assert names[(("356314888",), "Kilgard Creek")]["source"] == "municipal"
    assert names[(("356314888",), "Lonzo Creek (North Fork)")]["source"] == "gauge"
    assert all(not n["official"] for n in names.values())
