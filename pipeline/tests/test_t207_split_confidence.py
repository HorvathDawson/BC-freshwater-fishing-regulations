"""T207 item 3 (owner 2026-10-26): a curated split's optional `confidence` (high | medium | low; absent is
high) travels end to end — the loader/model, splits.resolved.json, the registry boundary, the bundle's
`split` and `part_mark`, the export's splits and the part labels, the release contract and the app's
generated pack — so a reader is told "approximate location (low confidence)" wherever the cut is named.
One test per hop, each against the code that writes or reads it."""
from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from pipeline.atlas.registry import add_boundary_confidence, load_registry, write_registry
from pipeline.atlas.splits.splits import anchor_digest, load_split_defs, write_resolved
from pipeline.common.curated import CURATED
from pipeline.common.models.enums import AnchorType
from pipeline.common.models.registry import RegistryBoundary, RegistryItem
from pipeline.common.models.splits import SplitDef, SplitPoint, confidence_words

REPO = Path(__file__).resolve().parents[2]
POINT = {"id": "x__falls", "label": "the falls", "gnis_id": "1",
         "anchor": {"type": "point", "coord": [-125.0, 50.0], "is_lonlat": True}}


def test_the_model_defaults_to_high_and_refuses_anything_else():
    assert SplitDef.from_dict(POINT).confidence == "high"
    assert SplitDef.from_dict({**POINT, "confidence": "low"}).confidence == "low"
    with pytest.raises(ValueError, match="confidence"):
        SplitDef.from_dict({**POINT, "confidence": "approximate"})


def test_the_curated_white_river_pool_cuts_are_low_and_load():
    defs = {d.id: d for d in load_split_defs(str(CURATED.waters.splits))}
    for sid in ("white_river__100_m_below_salmon_viewing_platform", "white_river__100_m_above_salmon_viewing_platform"):
        assert defs[sid].confidence == "low"
        assert defs[sid].anchor.offset_m == 100
    assert defs["fulton_river__spawning_channels_upstream_end"].confidence == "high"
    # owner 2026-10-26: Chuckwalla River's Ten Mile Pool signs are approximate too
    assert {d.confidence for sid, d in defs.items() if "ten_mile_pool" in sid} == {"low"}


def test_confidence_never_moves_the_cut():
    """Display only: adding it leaves the anchor digest (the review vintage) as it was."""
    assert anchor_digest(POINT) == anchor_digest({**POINT, "confidence": "low"})


def test_the_one_sentence():
    assert [confidence_words(c) for c in (None, "high", "medium", "low")] == [
        "", "", "approximate location (medium confidence)", "approximate location (low confidence)"]


def _point(conf: str) -> SplitPoint:
    return SplitPoint(split_id=f"x__{conf}", blk="1", route_measure=10.0, fid="", label="the falls",
                      anchor_type=AnchorType.point, source="curated", confidence=conf)


def test_splits_resolved_json_carries_it_only_when_not_high(tmp_path):
    p = tmp_path / "splits.resolved.json"
    write_resolved([_point("high"), _point("low")], str(p))
    rows = {r["split_id"]: r for r in json.loads(p.read_text())}
    assert "confidence" not in rows["x__high"] and rows["x__low"]["confidence"] == "low"


def test_the_registry_boundary_carries_it_and_round_trips(tmp_path):
    b = RegistryBoundary(id="x__falls", label="the falls", kind="split", ref="split:x__falls")
    lake = RegistryBoundary(id="x__lake", label="Lake", kind="lake", ref="lake:9")
    reg = {"gnis:1": RegistryItem(id="gnis:1", name="X", kind="stream", boundaries=(b, lake))}
    got = add_boundary_confidence(reg, [SplitDef.from_dict({**POINT, "confidence": "medium"})])
    assert [x.confidence for x in got["gnis:1"].boundaries] == ["medium", "high"]
    write_registry(got, tmp_path / "registry.json")
    raw = json.loads((tmp_path / "registry.json").read_text())["items"][0]["boundaries"]
    assert raw[0]["confidence"] == "medium" and "confidence" not in raw[1]
    assert [x.confidence for x in load_registry(tmp_path / "registry.json")["gnis:1"].boundaries] == ["medium", "high"]


def test_the_bundle_split_row_and_part_mark_carry_it():
    from pipeline.deliver.bundle import spans as SP
    from pipeline.deliver.bundle import topology as T
    rows = SP.split_rows([{"split_id": "x__falls", "blk": "100", "route_measure": 10.0, "label": "the falls",
                           "anchor_type": "point", "confidence": "low"},
                          {"split_id": "x__bridge", "blk": "100", "route_measure": 900.0, "label": "the bridge",
                           "anchor_type": "point"}], {})
    assert {r[0]: r[7] for r in rows} == {"x__falls": "low", "x__bridge": None}
    splits = {"x__falls": {"name": "the falls", "confidence": "low"}, "x__bridge": {"name": "the bridge"}}
    down, up = {"km": 1.0, "runs": [{"from": "x__falls"}]}, {"km": 5.0, "runs": []}
    assert T.split_at(down, up, splits)["confidence"] == "low"
    raw = [{"lon": 1.0, "lat": 2.0, "down": 0, "up": 1, "label": "the falls", "confidence": "low"},
           {"lon": 3.0, "lat": 4.0, "down": 1, "up": 2, "label": "the bridge", "confidence": None}]
    assert [m.get("confidence") for m in T.merge_marks(raw)] == ["low", None]
    sql = (REPO / "pipeline/deliver/bundle/schema.sql").read_text()
    assert "lake_id TEXT, confidence TEXT) WITHOUT ROWID" in sql and "up_parts TEXT NOT NULL, confidence TEXT" in sql


def test_the_export_carries_it_and_a_part_label_says_it():
    from pipeline.deliver.answers.display.part import end_name
    from pipeline.deliver.export.read import read_splits
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE split (split_id TEXT PRIMARY KEY, name TEXT NOT NULL, kind TEXT NOT NULL, at TEXT NOT "
               "NULL, official_name TEXT, same_place_as TEXT, lake_id TEXT, confidence TEXT)")
    db.executemany("INSERT INTO split VALUES (?,?,?,?,?,?,?,?)", [
        ("x__falls", "salmon viewing platform", "point", '[["gnis:1", 0.84]]', None, None, None, "low"),
        ("x__bridge", "Sayward Road bridge", "point", '[["gnis:1", 1.14]]', None, None, None, None)])
    splits = read_splits(db)
    assert splits["x__falls"]["confidence"] == "low" and "confidence" not in splits["x__bridge"]
    doc = {"splits": splits, "waters": {}}
    assert end_name(doc, "x__falls") == "the salmon viewing platform, approximate location (low confidence)"
    assert end_name(doc, "x__bridge") == "the Sayward Road bridge"


def test_the_release_contract_and_the_app_s_generated_pack_carry_it():
    from pipeline.deliver.release.contract import FILES
    assert FILES["water"]["tables"]["part_mark"][-1] == "confidence"
    from pipeline.deliver.export.contract import REQUIRED_COLUMNS
    assert "confidence" in REQUIRED_COLUMNS["split"]
    r = subprocess.run([sys.executable, "-m", "pipeline.tools.emit.pack", "--check"], cwd=REPO,
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr
