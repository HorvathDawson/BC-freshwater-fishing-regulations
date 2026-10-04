"""ONE DATA FLOW: every derived fact is computed once and read everywhere else.

Phase 1 of the data-flow review (2026-10-03). Each test here pins one of the places a fact used
to be computed twice, so a second body cannot creep back:

  closure        `rules.closure_grade` — the status index, the competition and the export ask it
  tidal/outside  the reach run writes `tidal.jsonl` / `outside_bc.jsonl`; the bundle reads them
  region home    the atlas writes `region_home.json`; the reach, the review app and the bundle
                 read it (`section_home`)
  steelhead      `rules` (steelhead rules apply) is stored and proved by the bundle, read by the
                 export
  curated files  the bundle reads NO curated waters file: `part_of` is the registry's, cut labels
                 and offsets are the atlas's `splits.resolved.json`
  promoted atlas an atlas a shipped bundle or tile set carries is immutable

The grep gates read the source; the parity tests (slow) read the built artifacts.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
from pathlib import Path

import pytest

from pipeline.common.curated import GENERATED, REPO_ROOT
from pipeline.deliver.bundle.rules import CLOSURE_CONDITIONS, closure_grade

BUNDLE_DIR = REPO_ROOT / "pipeline" / "deliver" / "bundle"
DELIVER = REPO_ROOT / "pipeline" / "deliver"
TOOLS = REPO_ROOT / "pipeline" / "tools"


def _sources(*dirs: Path) -> dict[Path, str]:
    return {p: p.read_text(encoding="utf-8") for d in dirs for p in sorted(d.rglob("*.py"))}


# --------------------------------------------------------------------------- grep gates

def test_the_bundle_reads_no_curated_waters_file():
    """`CURATED.` under pipeline/deliver/bundle may name only the corpus (`regulations.entries`).
    `part_of`, cut labels and offsets come from the atlas; the steelhead list through its own
    loader (a fingerprint check, not a fact)."""
    bad = []
    for p, text in _sources(BUNDLE_DIR).items():
        for m in re.finditer(r"CURATED\.([A-Za-z_.]+)", text):
            if not m.group(1).startswith("regulations.entries"):
                bad.append(f"{p.name}: CURATED.{m.group(1)}")
    assert bad == []
    for p, text in _sources(BUNDLE_DIR).items():
        assert "load_split_defs" not in text, p.name
        assert "added_lakes" not in text.replace("added_lakes.geojson", ""), p.name


def test_closure_is_decided_in_one_place():
    """`take == 0 and may_target == 0` (or `not may_target`) is spelled once, in
    `rules.closure_grade`; every other reader calls it."""
    hits = []
    for p, text in _sources(DELIVER, TOOLS).items():
        for i, line in enumerate(text.splitlines(), 1):
            if re.search(r"may_target\"\)\s*(==|!=|is)\s*0\b|not x\.get\(\"may_target\"\)", line):
                hits.append(f"{p.relative_to(REPO_ROOT)}:{i}")
    assert len(hits) == 1 and hits[0].startswith("pipeline/deliver/bundle/rules.py:"), hits
    for p, text in _sources(DELIVER, TOOLS).items():
        if p.name != "rules.py":
            assert "def is_full_closure" not in text or "closure_grade(x) ==" in text, p.name


def test_the_bundle_derives_neither_tidal_nor_outside_nor_homes_nor_stems_twice():
    texts = _sources(DELIVER, TOOLS)
    for p, text in texts.items():
        assert "tidal_sections(" not in text and "tidal_owner(" not in text, p.name
        assert "outside_bc(registry" not in text, p.name
        assert "regions.homes(" not in text and " homes(" not in text, p.name
    spans = (BUNDLE_DIR / "spans.py").read_text()
    assert spans.count("main_stem(") == 2            # the def and its one call, in `compute`
    export = (TOOLS / "export_ui_rules.py").read_text()
    assert "steelhead_known k WHERE" not in export    # the view, not its CASE re-spelled
    assert "STEELHEAD_RULE_MEMBERS" not in export     # no by-content second definition
    curation = (REPO_ROOT / "curation-review" / "backend")
    for name in ("reuse.py",):
        assert "regions.homes(" not in (curation / name).read_text()
    assert "attach_from(" in (REPO_ROOT / "pipeline/atlas/reach/cli.py").read_text()
    assert "attach_from(" in (curation / "reuse.py").read_text()


def test_the_review_app_builds_beside_the_served_atlas_and_promotes_explicitly():
    text = (REPO_ROOT / "curation-review/backend/rebuild.py").read_text()
    assert "GENERATED.build()" not in text.split("_CMD")[1].split("]")[0]
    assert "_next" in text and "def promote" in text
    assert "/api/rebuild/promote" in (REPO_ROOT / "curation-review/backend/app.py").read_text()


def test_the_tiles_carry_only_what_is_read():
    from pipeline.deliver.tiles.layers import BY_NAME, contract
    assert BY_NAME["stream"].attrs == ("section_id", "name", "ord")
    assert BY_NAME["lake"].attrs == ("section_id", "name", "water", "area_m2")
    assert BY_NAME["wetland"].attrs == ("section_id", "name", "water")
    on_disk = json.loads((REPO_ROOT / "pipeline/deliver/tiles/tile-contract.json").read_text())
    assert on_disk["layers"]["stream"]["attrs"] == list(BY_NAME["stream"].attrs)
    assert on_disk == contract()
    export = (REPO_ROOT / "pipeline/deliver/tiles/export.py").read_text()
    water_features = export[export.index("def export_streams"):export.index("def _kind(node)")]
    for dead in ('"alt"', '"mus"', '"areas"', '"item"', "_identity("):
        assert dead not in water_features, dead


def test_the_app_has_one_status_definition():
    text = (REPO_ROOT / "app/packages/core/src/status.ts").read_text()
    assert "export function waterStatus(" not in text
    assert "statusOn" in text and "waterStatusOn" in text


# --------------------------------------------------------------------------- closure_grade

def _closure(**kw):
    return {"type": "retention_limit", "take": 0, "may_target": 0, **kw}


def test_closure_grade_full_partial_none():
    assert closure_grade(_closure()) == "full"
    for k in CLOSURE_CONDITIONS:
        assert closure_grade(_closure(**{k: "x"})) == "partial", k
    assert closure_grade(_closure(undrawn_part="200 m of the bridge")) == "partial"
    assert closure_grade(_closure(not_yet_mapped={"display": "prominent"})) == "partial"
    assert closure_grade(_closure(may_target=1)) is None            # a release, not a closure
    assert closure_grade(_closure(take=2)) is None
    assert closure_grade(_closure(may_target=None)) is None         # unknown is not "no"
    assert closure_grade({"type": "gear", "take": 0, "may_target": 0}) is None
    assert closure_grade({"take": 0, "may_target": 0}) == "full"      # a bare statement


def test_the_three_readers_ask_closure_grade():
    from pipeline.deliver import status_index as SI
    from pipeline.tools import export_ui_rules as X
    full, part, none = _closure(), _closure(side="west"), _closure(may_target=1)
    assert [SI.is_full_closure(x) for x in (full, part, none)] == [True, False, False]
    assert [X._closure(x) for x in (full, part, none)] == [True, True, False]
    from pipeline.deliver.bundle.read import stricter
    quota = {"type": "retention_limit", "take": 2, "may_target": 1}
    release = {"type": "retention_limit", "take": 0, "may_target": 1}
    # a FULL closure beats a quota and a release; a conditioned one is no longer "shut" (it
    # still beats a quota, as the outright release it also is)
    assert stricter(full, quota) and stricter(full, release) and not stricter(release, full)
    assert stricter(part, quota) and not stricter(part, release)


# --------------------------------------------------------------------------- tidal / homes

def test_tidal_is_one_body_strict_for_the_run_lenient_for_the_editor():
    from pipeline.atlas.reach.outside import tidal_owner, tidal_sections
    from pipeline.common.models import RegistryItem
    reg = {"wbk:1": RegistryItem(id="wbk:1", name="Nitinat Lake", kind="lake",
                                 section_ids=("lake:1",))}
    rows = [("1", {"entry_id": "r1:nitinat_lake@1-3", "tidal": True, "matched": ["wbk:1"]}),
            ("1", {"entry_id": "r1:other@1-1", "matched": ["wbk:1"]})]
    assert tidal_owner(rows, reg) == {"lake:1": "r1:nitinat_lake@1-3"}
    assert tidal_sections(rows, reg) == frozenset({"lake:1"})
    bad = [("1", {"entry_id": "r1:ghost@1-1", "tidal": True, "matched": ["wbk:9"]})]
    with pytest.raises(SystemExit, match="place no section"):
        tidal_owner(bad, reg)
    assert tidal_sections(bad, reg) == frozenset()


def test_the_run_writes_tidal_and_outside_and_the_reader_refuses_their_absence(tmp_path):
    from pipeline.atlas.reach import io as rio
    from pipeline.atlas.reach.build import ReachResult
    from pipeline.atlas.reach.models import BuildReport
    res = ReachResult([], [], BuildReport(build="t", handles="x"), tidal={"lake:1": "r1:a@1-1"},
                      outside=frozenset({"s:9", "s:2"}))
    counts = rio.write_run(tmp_path, res, [])
    assert counts["tidal"] == 1 and counts["outside_bc"] == 2
    assert rio.read_table(tmp_path, "tidal") == [{"entry_id": "r1:a@1-1", "section_id": "lake:1"}]
    assert [r["section_id"] for r in rio.read_table(tmp_path, "outside_bc")] == ["s:2", "s:9"]
    rep = json.loads((tmp_path / "report.json").read_text())
    assert len(rep["tidal_digest"]) == 16 and len(rep["outside_digest"]) == 16
    (tmp_path / "tidal.jsonl").unlink()
    with pytest.raises(SystemExit, match="predates the `tidal` table"):
        rio.read_table(tmp_path, "tidal")


def test_region_homes_are_written_once_and_read_back(tmp_path, monkeypatch):
    from pipeline.atlas.registry import regions
    monkeypatch.setattr(regions, "homes", lambda build, registry: {"s:2": "3", "lake:1": "8"})
    got = regions.write_homes(tmp_path, {})
    assert json.loads((tmp_path / regions.HOME_FILE).read_text()) == {"lake:1": "8", "s:2": "3"}
    assert regions.read_homes(tmp_path) == got

    from pipeline.common.models import NodeKind, RegistryItem, StreamNode

    class G:
        nodes = {"lake:1": StreamNode(node_id="lake:1", kind=NodeKind.lake, wbk="1"),
                 "s:2": StreamNode(node_id="s:2", kind=NodeKind.stream, blk="s")}
    g = G()
    # the straddling LAKE is told apart by the registry's kind (a river's polygon would be a river)
    reg = {"wbk:1": RegistryItem(id="wbk:1", name="A Lake", kind="lake", section_ids=("lake:1",))}
    assert regions.attach_from(tmp_path, g, reg) == got
    assert regions.in_region(g, "3", {"s:2", "s:5", "lake:1"}) == {"s:2", "s:5", "lake:1"}
    assert regions.in_region(g, "8", {"s:2", "s:5", "lake:1"}) == {"s:5", "lake:1"}
    reg = {"wbk:1": RegistryItem(id="wbk:1", name="A Slough", kind="stream", section_ids=("lake:1",))}
    regions.attach_from(tmp_path, g, reg)
    assert regions.in_region(g, "3", {"s:2", "s:5", "lake:1"}) == {"s:2", "s:5"}   # held to home 8
    with pytest.raises(SystemExit, match="predates region homes"):
        regions.read_homes(tmp_path / "nope")


def test_the_bundle_reads_the_atlas_split_labels_and_offsets(tmp_path):
    from pipeline.deliver.bundle.place_names import resolved_labels
    from pipeline.deliver.bundle.spans import resolved_offsets
    rows = [{"split_id": "a__falls", "label": "The Falls (100 m downstream)", "anchor_type": "point",
             "source": "curated", "anchor_offset_m": 100.0, "anchor_offset_dir": "downstream"},
            {"split_id": "gauge:08HA", "label": "gauge 08HA", "anchor_type": "gauge",
             "source": "gauge"},
            {"split_id": "area:Park", "label": "within Park boundary", "anchor_type": "area_boundary",
             "source": "area"}]
    (tmp_path / "splits.resolved.json").write_text(json.dumps(rows))
    assert resolved_labels(tmp_path) == {"a__falls": "The Falls (100 m downstream)"}
    assert resolved_offsets(rows) == {"a__falls": (100.0, "downstream")}
    (tmp_path / "splits.resolved.json").write_text(json.dumps([{k: v for k, v in r.items()
                                                                if k != "source"} for r in rows]))
    with pytest.raises(SystemExit, match="predates it"):
        resolved_labels(tmp_path)


def test_sidecars_enrich_resolved_from_the_curated_defs():
    from pipeline.atlas.sidecars import enrich_resolved
    from pipeline.common.models.splits import SplitDef
    d = SplitDef.from_dict({"id": "a__falls", "label": "The Falls", "blk": "1",
                            "anchor": {"type": "point", "coord": [0, 0], "offset_m": 100,
                                       "offset_dir": "downstream"}})
    rows = [{"split_id": "a__falls", "anchor_type": "point", "label": "The Falls"},
            {"split_id": "gauge:1", "anchor_type": "gauge", "label": "g"},
            {"split_id": "length:1:5", "anchor_type": "confluence", "label": "x"}]
    got = enrich_resolved(rows, [d])
    assert [r["source"] for r in got] == ["curated", "gauge", "length"]
    assert got[0]["anchor_offset_m"] == 100.0 and got[0]["anchor_offset_dir"] == "downstream"
    with pytest.raises(SystemExit, match="changed since this atlas was built"):
        enrich_resolved([{"split_id": "zzz", "anchor_type": "point", "label": "?"}], [d])


def test_the_sidecars_write_what_a_fresh_build_writes(tmp_path, monkeypatch):
    """`python -m pipeline.atlas.sidecars` is a MIGRATION: for the same inputs its three files
    must be byte-equal to a fresh build's (same functions, same serialisation, same key order),
    and with `--out` it writes nothing into the atlas it reads (a promoted atlas is immutable)."""
    from pipeline.atlas.registry import regions
    from pipeline.atlas.sidecars import enrich_resolved
    from pipeline.atlas.splits.splits import RESOLVED_KEYS, dump_resolved, write_resolved
    from pipeline.common.models.splits import AnchorType, SplitDef, SplitPoint
    d = SplitDef.from_dict({"id": "a__falls", "label": "The Falls", "blk": "1",
                            "anchor": {"type": "point", "coord": [0, 0], "offset_m": 100,
                                       "offset_dir": "downstream"}})
    # what the build writes (every producer stamps `source`; a curated anchor its offset)
    fresh = [SplitPoint(split_id="a__falls", blk="1", route_measure=12.345, fid="", label="The Falls",
                        anchor_type=AnchorType.point, source="curated", anchor_offset_m=100.0,
                        anchor_offset_dir="downstream", concern="two candidates"),
             SplitPoint(split_id="length:1:5", blk="1", route_measure=50.0, fid="", label="x",
                        anchor_type=AnchorType.confluence, source="length", picked_up=True)]
    write_resolved(fresh, str(tmp_path / "fresh.json"))
    # what an atlas from before the fields holds: the same rows without them
    old = [{k: v for k, v in r.items() if k not in ("source", "anchor_offset_m", "anchor_offset_dir")}
           for r in json.loads((tmp_path / "fresh.json").read_text())]
    assert dump_resolved(enrich_resolved(old, [d])) == (tmp_path / "fresh.json").read_text()
    assert all(tuple(k for k in RESOLVED_KEYS if k in r) == tuple(r) for r in enrich_resolved(old, [d]))
    # region homes: measured from the atlas, written into `out_dir` only
    monkeypatch.setattr(regions, "homes", lambda build, registry: {"s:2": "3"})
    build, out = tmp_path / "build", tmp_path / "out"
    build.mkdir()
    out.mkdir()
    regions.write_homes(build, {}, out_dir=out)
    assert (out / regions.HOME_FILE).exists() and not (build / regions.HOME_FILE).exists()
    assert regions.write_homes(build, {}) == {"s:2": "3"} and (build / regions.HOME_FILE).exists()


def test_a_promoted_atlas_is_recognised_by_its_shipped_digest(tmp_path):
    from pipeline.common.section_handles import write as write_handles, digest_for
    from pipeline.common.vintage import promoted_atlas
    build = tmp_path / "full"
    build.mkdir()
    write_handles(["a:1", "a:2"], build)
    tiles = tmp_path / "tiles"
    tiles.mkdir()
    bundle = tmp_path / "bundle.sqlite"
    assert promoted_atlas(build, tiles, bundle) is None
    (tiles / "atlas.meta.json").write_text(json.dumps({"section_handles": digest_for(build)}))
    assert promoted_atlas(build, tiles, bundle) == str(tiles / "atlas.pmtiles")
    (tiles / "atlas.meta.json").write_text(json.dumps({"section_handles": "0" * 16}))
    db = sqlite3.connect(bundle)
    db.execute("CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT)")
    db.execute("INSERT INTO meta VALUES ('section_handles', ?)", (digest_for(build),))
    db.commit()
    db.close()
    assert promoted_atlas(build, tiles, bundle) == str(bundle)
    assert promoted_atlas(tmp_path / "none", tiles, bundle) is None


def test_promote_refuses_an_unfinished_candidate(tmp_path):
    from pipeline.atlas.promote import promote
    with pytest.raises(SystemExit, match="not a finished atlas"):
        promote(tmp_path, dry_run=True)


# --------------------------------------------------------------------------- parity (slow)

def _built():
    bundle = Path(os.environ.get("UI_EXPORT_BUNDLE") or GENERATED.bundle / "bundle.sqlite")
    if not bundle.exists():
        pytest.skip(f"no bundle at {bundle}")
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    meta = dict(db.execute("SELECT k, v FROM meta"))
    build = Path(meta["build"])
    run = Path(os.environ.get("UI_EXPORT_REACHES") or GENERATED.reaches / meta["reach_run"])
    if not (run / "tidal.jsonl").exists() or not (build / "region_home.json").exists():
        pytest.skip(f"{bundle} predates the persisted facts (run {run}, atlas {build})")
    return db, build, run


@pytest.mark.slow
def test_the_bundle_carries_exactly_the_runs_tidal_and_outside_sections():
    from pipeline.atlas.reach.io import read_table
    from pipeline.common.section_handles import read as read_handles
    db, build, run = _built()
    _, sid = read_handles(build)
    want = {(sid[r["section_id"]], r["entry_id"]) for r in read_table(run, "tidal")}
    assert set(db.execute("SELECT sid, entry_id FROM tidal")) == want and want
    out = {sid[r["section_id"]] for r in read_table(run, "outside_bc")}
    assert {s for (s,) in db.execute("SELECT sid FROM outside_bc")} == out and out


@pytest.mark.slow
def test_the_bundle_carries_exactly_the_atlas_region_homes():
    from pipeline.atlas.registry.regions import read_homes
    from pipeline.common.section_handles import read as read_handles
    db, build, run = _built()
    _, sid = read_handles(build)
    want = {(sid[h], r) for h, r in read_homes(build).items()}
    assert set(db.execute("SELECT sid, region FROM section_home")) == want and want


@pytest.mark.slow
def test_steelhead_rules_in_the_bundle_reproduce_the_run():
    from pipeline.atlas.reach.io import STEELHEAD_TABLE, read_table
    from pipeline.common.section_handles import read as read_handles
    db, build, run = _built()
    _, sid = read_handles(build)
    rows = read_table(run, STEELHEAD_TABLE)
    want = {sid[r["section_id"]] for r in rows if r["rules"]}
    got = {s for (s,) in db.execute("SELECT sid FROM section_steelhead_rules")}
    assert got == want and want
    assert {s for (s,) in db.execute("SELECT sid FROM steelhead_water")} <= got
    # every section the run marks is covered by the stored form, known or by its set
    assert {sid[r["section_id"]] for r in rows} <= {
        s for (s,) in db.execute("SELECT sid FROM section_steelhead")}
