"""T449 (owner 2026-11-06): a DFO place that lies WHOLLY IN TIDAL WATER (the Nanaimo's "tidal portions … from Cedar
Bridge downstream", below its tidal limit at the Cedar Road bridge) is PROPOSED as "tidal water" by a cheap rule; the
owner's mark accepts ("tidal water") or rejects ("not tidal") it; an accepted place binds nothing and is not missing
(the write-through sets `tidal_water`; the reach row says so; the bundle does not paint its water "not mapped")."""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from pipeline.regs.dfo_salmon import entries as E
from pipeline.regs.review import dfo as D
from pipeline.regs.review import marks as M
from pipeline.regs.review import proposals as P

NANAIMO_1 = "in Subarea 17-14 (tidal portions of the Nanaimo River) from Cedar Bridge downstream"
NANAIMO_2 = ("in Subarea 17-14 (tidal portions of the Nanaimo River), that portion of the Nanaimo River from the Cedar "
             "Bridge to the white square boundary signs approximately 400 m downstream of the Cedar Bridge")
CUT = SimpleNamespace(blk="354154938", name="Nanaimo River", limit="split", split="nanaimo_river__cedar_road_bridge",
                      at=None)
LABELS = {"nanaimo_river__cedar_road_bridge": "Cedar Road bridge"}
WATER = {"water_id": "1:nanaimo-river", "item_ids": ["gnis:16505"]}


def _loc(text, op, extents=(), **kw):
    return {"location_id": "1:x", "water": "Nanaimo River", "water_id": "1:nanaimo-river",
            "source_text": {"specific_area": text, "op": op}, "binding": {"extents": list(extents)}, **kw}


def _pr():
    return M.Prints(text="t", records=(M.RecordPrint(key="entry", meaning="m", binding="b"),))


def _mark(**kw):
    base = dict(scope="dfo", target="1:x", reviewer="o", date="2026-11-06", pdf_sha256="0" * 64, prints=_pr())
    return M.Mark(**{**base, **kw})


def test_the_two_nanaimo_places_are_proposed_tidal_on_the_cedar_road_cut():
    for text, op in ((NANAIMO_1, "downstream_of"), (NANAIMO_2, "between")):
        got = D.tidal_proposal(_loc(text, op), WATER, [CUT], LABELS)
        assert got is not None and got["split"] == CUT.split and "cedar" in got["words"], got
        assert "tidal portions" in got["why"] and "Cedar Road bridge" in got["why"]


@pytest.mark.parametrize("text, op", [
    # bounded BY the tidal limit, i.e. the freshwater above it — never proposed
    ("from the Seymour River Dam to the tidal boundary, including tributaries", "between"),
    ("upstream of tidal water fishing boundary signs located below the Hwy 101 Bridge", "between"),
    ("All streams flowing into tidal water Area 5 .", "described"),
    # in tidal words, but running UP from the cut / not naming it
    ("tidal portions of the Nanaimo River upstream of Cedar Bridge", "upstream_of"),
    ("tidal portions of the Nanaimo River from the Haslam Creek confluence downstream", "downstream_of"),
])
def test_a_place_bounded_by_the_limit_or_not_naming_it_is_not_proposed(text, op):
    assert D.tidal_proposal(_loc(text, op), WATER, [CUT], LABELS) is None


def test_no_limit_an_extent_or_an_accepted_place_is_not_proposed():
    assert D.tidal_proposal(_loc(NANAIMO_1, "downstream_of"), WATER, [], LABELS) is None
    assert D.tidal_proposal(_loc(NANAIMO_1, "downstream_of", [{"op": "whole"}]), WATER, [CUT], LABELS) is None
    assert D.tidal_proposal(_loc(NANAIMO_1, "downstream_of", tidal_water=True), WATER, [CUT], LABELS) is None
    mouth = SimpleNamespace(blk="1", name="A Creek", limit="mouth", split=None, at=None)
    got = D.tidal_proposal(_loc("the tidal waters of A Creek", "whole"), WATER, [mouth], LABELS)
    assert got is not None and got["limit"] == "mouth"


def test_the_tidal_calls_ride_only_a_dfo_mark_shaped_right():
    ok = _mark(verdict="verified", answers={"T": "y", "R": "y", "P": "y"}, call="tidal water")
    assert ok.call == "tidal water"
    assert _mark(verdict="deferred", answers={}, note="needs its extent", call="not tidal").call == "not tidal"
    with pytest.raises(ValueError, match="verified"):
        _mark(verdict="unsure", answers={"P": "?"}, call="tidal water")
    with pytest.raises(ValueError, match="binds nothing"):
        _mark(verdict="verified", answers={"T": "y", "R": "y", "P": "y"}, call="tidal water", lake_split_later=True)
    with pytest.raises(ValueError):
        _mark(verdict="verified", answers={"T": "y", "R": "y", "P": "y"}, call="stream")
    with pytest.raises(ValueError):
        M.Mark(scope="entry", target="e", reviewer="o", date="2026-11-06", pdf_sha256="0" * 64,
               prints=M.Prints(text="t", records=(M.RecordPrint(key="entry", meaning="m", binding="b"),)),
               verdict="verified", answers={"T": "y", "R": "y", "P": "y", "A": "y"}, call="tidal water")


def _src(tmp_path, **loc_kw):
    d = tmp_path / "dfo"
    d.mkdir()
    doc = {"region": "1", "locations": [_loc(NANAIMO_1, "downstream_of", **loc_kw)]}
    (d / "region-1.json").write_text(json.dumps(doc, indent=1))
    (tmp_path / "s.json").write_text('{"waterbodies": []}')
    return P.Sources(entries_dir=tmp_path, dfo_dir=d, splits_path=tmp_path / "s.json")


def test_an_accepting_mark_writes_tidal_water_and_a_reject_takes_it_back(tmp_path):
    src = _src(tmp_path)
    acc = _mark(verdict="verified", answers={"T": "y", "R": "y", "P": "y"}, call="tidal water")
    assert P._proposes(acc)
    p = P._tidal_water_proposal(acc, "mark-dfo-x", {}, src)
    assert not p["problems"] and [(e["kind"], e["field"], e["after"]) for e in p["edits"]] == [
        ("dfo_loc", "tidal_water", True)]
    loc = json.loads(next(iter(P.materialize([p], src).values())))["locations"][0]
    assert loc["tidal_water"] is True
    E.EntryLocation.from_dict({**loc, "region": "1", "region_number": 1, "kind": "water", "precedence": 3})   # the model takes it

    (tmp_path / "b").mkdir()
    src2 = _src(tmp_path / "b", tidal_water=True)
    rej = _mark(verdict="deferred", answers={}, note="no", call="not tidal")
    p2 = P._tidal_water_proposal(rej, "mark-dfo-x", {}, src2)
    assert [(e["field"], e["after"]) for e in p2["edits"]] == [("tidal_water", P._MISSING)]
    assert "tidal_water" not in json.loads(next(iter(P.materialize([p2], src2).values())))["locations"][0]


def test_accepting_a_place_with_an_extent_is_refused(tmp_path):
    src = _src(tmp_path, extents=[{"op": "whole"}])
    p = P._tidal_water_proposal(_mark(verdict="verified", answers={"T": "y", "R": "y", "P": "y"},
                                      call="tidal water"), "mark-dfo-x", {}, src)
    assert p["problems"] and "binds nothing" in p["problems"][0]


def test_the_entry_model_keeps_tidal_water_only_when_set_and_refuses_it_with_an_extent():
    base = {"location_id": "1:x", "region": "1", "region_number": 1, "kind": "water", "precedence": 3,
            "water": "Nanaimo River"}
    assert "tidal_water" not in E.EntryLocation.from_dict(base).to_dict()
    assert E.EntryLocation.from_dict({**base, "tidal_water": True}).to_dict()["tidal_water"] is True
    with pytest.raises(ValueError, match="binds nothing"):
        E.EntryLocation.from_dict({**base, "tidal_water": True,
                                   "binding": {"extents": [{"op": "whole"}]}})
