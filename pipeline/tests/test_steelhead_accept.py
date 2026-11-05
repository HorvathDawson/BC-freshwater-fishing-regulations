"""T324: `python -m pipeline.regs.steelhead.accept` — the diff it prints and what `--write` writes."""
from __future__ import annotations

import json

import pytest

from pipeline.atlas.reach.steelhead import fingerprint
from pipeline.regs.steelhead import accept as A


def _gen(waters):
    return {"$comment": ["generator words"], "waters": waters,
            "generated": {"source": "data/generated/steelhead/steelhead_waters.json",
                          "generator": "pipeline.regs.steelhead.known_waters", "date": "2026-11-05",
                          "fingerprint": fingerprint(waters)}}


def test_the_diff_names_added_dropped_and_renoted_waters():
    cur = {"waters": [{"item_id": "gnis:1", "note": "a"}, {"item_id": "gnis:2", "note": "b"}]}
    gen = _gen([{"item_id": "gnis:2", "note": "b2"}, {"item_id": "gnis:3", "note": "c"}])
    d = A.diff(cur, gen)
    assert [w["item_id"] for w in d["added"]] == ["gnis:3"]
    assert [w["item_id"] for w in d["dropped"]] == ["gnis:1"]
    assert [(o["note"], n["note"]) for o, n in d["renoted"]] == [("b", "b2")]


def test_write_keeps_the_curated_comment_and_takes_the_generated_list(tmp_path, monkeypatch):
    cur_path, gen_path = tmp_path / "steelhead_waters.json", tmp_path / "gen.json"
    cur = {"$comment": ["the curated words"], "waters": [{"item_id": "gnis:1", "note": "a"}]}
    cur_path.write_text(json.dumps(cur), encoding="utf-8")
    gen_path.write_text(json.dumps(_gen([{"item_id": "gnis:3", "note": "c"}])), encoding="utf-8")
    monkeypatch.setattr(A, "CURATED", type("C", (), {"waters": type("W", (), {"steelhead_waters": cur_path})()})())
    monkeypatch.setattr(A, "REPO_ROOT", tmp_path)
    assert A.main(["--generated", str(gen_path)]) == 0
    assert json.loads(cur_path.read_text()) == cur            # no --write: nothing written
    assert A.main(["--generated", str(gen_path), "--write"]) == 0
    got = json.loads(cur_path.read_text())
    assert got["$comment"] == ["the curated words"] and got["waters"] == [{"item_id": "gnis:3", "note": "c"}]
    assert got["generated"]["fingerprint"] == fingerprint(got["waters"])


def test_a_hand_edited_generated_list_is_refused(tmp_path, monkeypatch):
    gen = _gen([{"item_id": "gnis:3", "note": "c"}])
    gen["waters"].append({"item_id": "gnis:4"})                   # edited after the fingerprint
    (tmp_path / "gen.json").write_text(json.dumps(gen), encoding="utf-8")
    with pytest.raises(SystemExit, match="fingerprint"):
        A.main(["--generated", str(tmp_path / "gen.json")])
