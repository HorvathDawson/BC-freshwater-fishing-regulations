"""Owner 2026-11-02: one view per water — places (a wording and its rewordings are one item), every
wording with its page dates, and a missed wording attached as a VARIANT through a mark."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.regs.review import dfo as D
from pipeline.regs.review import marks as M
from pipeline.regs.review import proposals as P


def _loc(lid, text, status="active", dup=None, conf=None):
    return {"location_id": lid, "water": "A River", "water_id": "1:a", "status": status,
            "duplicate_of": dup, "duplicate_confirmed": conf,
            "source_text": {"waters": "A River", "specific_area": text}}


def _locs(*ls):
    w = {"water_id": "1:a", "item_ids": ["gnis:1"]}
    return {x["location_id"]: ("1", x, w) for x in ls}


def test_places_fold_rewordings_and_confirmed_variants_live_first():
    locs = _locs(_loc("old", "from the weir downstream", "dormant"),
                 _loc("cur", "from the weir downstream to the mouth"),
                 _loc("re", "from the weir downstream to the mouth", "dormant"),
                 _loc("var", "below the hatchery weir", "dormant", dup="cur", conf=True),
                 _loc("no", "below the hatchery weir", "dormant", dup="cur", conf=False))
    got = D.places(locs, "1:a")
    assert got[0]["id"] == "cur" and set(got[0]["members"]) == {"cur", "re", "var"}
    assert {p["id"] for p in got[1:]} == {"old", "no"}                  # contested stays apart


def test_a_variant_mark_proposes_the_entry_models_own_link(tmp_path):
    d = tmp_path / "dfo"
    d.mkdir()
    doc = {"region": "1", "locations": [_loc("1:a:x", "words one"), _loc("1:a:y", "words two", "dormant")]}
    (d / "region-1.json").write_text(json.dumps(doc, indent=1))
    (tmp_path / "s.json").write_text('{"waterbodies": []}')
    src = P.Sources(entries_dir=tmp_path, dfo_dir=d, splits_path=tmp_path / "s.json")
    m = M.Mark(scope="dfo", target="1:a:y", verdict="verified", answers={"T": "y"}, variant_of="1:a:x",
               reviewer="o", date="2026-11-02", pdf_sha256="0"*64, prints=M.Prints(text="t", records=(M.RecordPrint(key="entry", meaning="m", binding="b"),)))
    assert P._proposes(m)
    p = P._variant_proposal(m, "mark-dfo-y", {}, src)
    assert not p["problems"]
    assert {(e["field"], e["after"]) for e in p["edits"]} == {("duplicate_of", "1:a:x"),
                                                               ("duplicate_confirmed", True)}
    out = P.materialize([p], src)
    loc = next(x for x in json.loads(next(iter(out.values())))["locations"] if x["location_id"] == "1:a:y")
    assert loc["duplicate_of"] == "1:a:x" and loc["duplicate_confirmed"] is True


def test_a_variant_mark_answers_only_t_and_only_on_dfo():
    pr = M.Prints(text="t", records=(M.RecordPrint(key="entry", meaning="m", binding="b"),))
    with pytest.raises(ValueError):
        M.Mark(scope="dfo", target="a", verdict="verified", answers={"T": "y"}, variant_of="a",
               reviewer="o", date="2026-11-02", pdf_sha256="0"*64, prints=pr)
    assert '"variant_of"' not in M.line(M.Mark(scope="dfo", target="a", verdict="verified",
                                               answers={"T": "y", "R": "y", "P": "y"}, reviewer="o",
                                               date="2026-11-02", pdf_sha256="0"*64, prints=pr))


def test_wordings_date_each_fingerprint_from_the_saved_pages(tmp_path):
    got = D.wordings(tmp_path)                                        # no pages: nothing dated
    assert got == {}
    loc = {"fingerprints": ["abc"], "source_text": {"specific_area": "the words"}}
    assert D.location_wordings(loc, tmp_path) == [{"fingerprint": "abc", "text": "the words", "dates": []}]
