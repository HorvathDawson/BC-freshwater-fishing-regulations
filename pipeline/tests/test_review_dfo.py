"""T164: the DFO review mode's marks — a DFO location as a review target, NEW split proposals.

The mark model (`marks.Mark`, scope `dfo`; `marks.NewSplit`), the prints a DFO mark is held against
(`review.dfo`: P goes stale only on the curated geography, R on a re-scrape of the rows) and the
ticket that carries the proposals to T185. Reads no generated data: the DFO files are written here.
"""
from __future__ import annotations

import json

import pytest

from pipeline.regs.review import dfo as D
from pipeline.regs.review import marks as M
from pipeline.regs.review import tickets as T

SHA = "0" * 64
LOC = "1:test-river:below-the-bridge"


def _world(tmp_path, *, rows=("Coho | Oct 1 to Dec 31 | 2 per day",), extents=()):
    ent, typed = tmp_path / "entries", tmp_path / "typed"
    ent.mkdir(exist_ok=True)
    typed.mkdir(exist_ok=True)
    (ent / "region-1.json").write_text(json.dumps({
        "region": "1", "region_number": 1, "region_name": "Vancouver Island", "structure": {},
        "scopes": [],
        "waters": [{"water_id": "1:test-river", "region": "1", "region_number": 1,
                    "name": "Test River", "item_ids": ["gnis:1"], "tributaries": False}],
        "locations": [{"location_id": LOC, "region": "1", "region_number": 1, "kind": "water",
                       "precedence": 3, "water": "Test River", "water_id": "1:test-river",
                       "fingerprints": ["fp1"], "status": "active",
                       "source_text": {"waters": "Test River", "specific_area": "below the bridge",
                                       "op": "downstream_of", "anchor_types": ["bridge"]},
                       "binding": {"extents": list(extents)}}]}))
    (typed / "region-1.json").write_text(json.dumps({"locators": [
        {"fingerprint": "fp1", "rows": list(rows), "rules": []}]}))
    return ent, typed


def _mark(prints, **kw):
    base = {"scope": "dfo", "target": LOC, "verdict": "wrong",
            "answers": {"T": "y", "R": "y", "P": "n"}, "problem": "split missing",
            "note": "no cut at the bridge", "reviewer": "t", "date": "2026-10-25",
            "pdf_sha256": SHA, "prints": prints}
    return M.Mark.model_validate({**base, **kw})


BRIDGE = {"ref": "n1", "anchor": "bridge", "name": "Hwy 19 bridge", "coord": (-125.3, 50.0),
          "side": "downstream", "quote": "below the bridge"}


def test_a_dfo_location_is_marked_on_t_r_p(tmp_path):
    ent, typed = _world(tmp_path)
    p = D.prints_of(LOC, ent, typed)
    assert p["text"] == "Test River\nbelow the bridge" and [r["key"] for r in p["records"]] == ["entry"]
    assert M.CHECKS["dfo"] == ("T", "R", "P")
    assert M.verdict_from_answers("dfo", {"T": "y", "R": "y", "P": "y"}) == "verified"
    with pytest.raises(ValueError, match="a dfo mark answers"):
        _mark(p, answers={"T": "y", "R": "y", "P": "n", "A": "y"})
    with pytest.raises(ValueError, match="names its rule_id"):
        _mark(p, rule_id="x")
    with pytest.raises(ValueError, match="DFO mark holds exactly"):
        _mark({**p, "records": [*p["records"], {"key": "rule:x", "meaning": "m", "binding": "b"}]})


def test_a_rescrape_stales_r_and_only_the_curated_geography_stales_p(tmp_path):
    ent, typed = _world(tmp_path)
    m = _mark(D.prints_of(LOC, ent, typed))
    now = M.Prints.model_validate(D.prints_of(LOC, ent, typed))
    assert M.stale_reasons(m, now) == []
    (tmp_path / "b").mkdir()
    (tmp_path / "c").mkdir()
    ent2, typed2 = _world(tmp_path / "b", rows=("Coho | Oct 1 to Dec 31 | 4 per day",))
    assert M.stale_reasons(m, M.Prints.model_validate(D.prints_of(LOC, ent2, typed2))) == \
        [M.StaleReason.rule_changed]
    ent3, typed3 = _world(tmp_path / "c",
                          extents=[{"op": "downstream_of", "splits": ["test_river__bridge"]}])
    assert M.stale_reasons(m, M.Prints.model_validate(D.prints_of(LOC, ent3, typed3))) == \
        [M.StaleReason.binding_moved]


def test_a_new_split_rides_a_wrong_or_unsure_mark_and_an_extent_fix_may_name_it(tmp_path):
    ent, typed = _world(tmp_path)
    p = D.prints_of(LOC, ent, typed)
    fix = {"scope": "entry", "op": "downstream_of", "points": [{"kind": "new", "split_id": "n1"}]}
    m = _mark(p, new_splits=[BRIDGE], extent_fix=[fix])
    assert m.new_splits[0].anchor == "bridge" and m.extent_fix[0].points[0].kind == "new"
    assert "new_splits" in json.loads(M.line(m))
    assert "new_splits" not in json.loads(M.line(_mark(p)))             # old lines unchanged
    with pytest.raises(ValueError, match="does not propose"):
        _mark(p, extent_fix=[fix])
    with pytest.raises(ValueError, match="wrong or unsure"):
        _mark(p, verdict="verified", answers={"T": "y", "R": "y", "P": "y"}, problem=None,
              new_splits=[BRIDGE])
    with pytest.raises(ValueError, match="its own ref"):
        _mark(p, new_splits=[BRIDGE, BRIDGE])


@pytest.mark.parametrize("bad, words", [
    ({"ref": "n1", "anchor": "bridge"}, "click the map"),
    ({"ref": "n1", "anchor": "sign", "coord": (-80.0, 43.0)}, "in BC"),
    ({"ref": "n1", "anchor": "confluence"}, "names its tributary"),
    ({"ref": "n1", "anchor": "lake_outlet"}, "names its lake"),
    ({"ref": "n1", "anchor": "split"}, "names that split"),
    ({"ref": "n1", "anchor": "weir", "coord": (-125.3, 50.0)}, "anchor"),
])
def test_an_unanchored_new_split_is_refused(bad, words):
    with pytest.raises(ValueError, match=words):
        M.NewSplit.model_validate(bad)


def test_a_ticket_carries_the_new_splits_and_reads_them_back(tmp_path):
    ent, typed = _world(tmp_path)
    m = _mark(D.prints_of(LOC, ent, typed), new_splits=[{**BRIDGE, "blk": "354", "measure": 120.0,
                                                          "on_item": "gnis:1"}])
    text = T.ticket_text(m)
    assert "## Proposed new splits" in text and "at the bridge Hwy 19 bridge" in text
    assert "dfo_salmon" in text                                         # the work names the file
    got = T.parse_ticket(text)
    assert got == T.mark_fields(m)
    assert M.NewSplit.model_validate(got["new_splits"][0]) == m.new_splits[0]
