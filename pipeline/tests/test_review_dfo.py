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


# --------------------------------------------------------------------------- T210: shared proposals

def _marks_with(tmp_path, *specs):
    """One DFO mark per (target, new_splits) — the same fixture world, different targets."""
    ent, typed = _world(tmp_path)
    p = D.prints_of(LOC, ent, typed)
    return [_mark(p, target=target, new_splits=ns, reviewer=f"r{i}")
            for i, (target, ns) in enumerate(specs)]


def test_the_same_anchor_is_one_proposed_split_whoever_proposed_it(tmp_path):
    q = {"ref": "n2", "anchor": "confluence", "name": "Quinsam River", "item_id": "gnis:27883",
         "on_item": "gnis:1", "blk": "354", "measure": 4199.5, "coord": (-125.294097, 50.037899)}
    again = {**q, "ref": "n3", "name": "quinsam"}                       # same tributary, other ref + name
    near = {**BRIDGE, "ref": "n4", "blk": "354", "measure": 4205.0, "on_item": "gnis:1"}   # 5.5 m on
    far = {**BRIDGE, "ref": "n5", "blk": "354", "measure": 4300.0, "on_item": "gnis:1",
           "coord": (-125.2, 50.1)}
    marks = _marks_with(tmp_path, ("a", [q]), ("b", [again]), ("c", [near]), ("d", [far]),
                        ("e", [{**BRIDGE, "ref": "n6", "coord": (-125.29409, 50.03790)}]))
    got = M.proposals(marks)
    assert [p["id"] for p in got] == ["n2", "n5"]                       # the first name/ref kept
    assert got[0]["split"].name == "Quinsam River"
    assert [w["target"] for w in got[0]["marks"]] == ["a", "b", "c", "e"]
    assert [w["target"] for w in got[1]["marks"]] == ["d"]
    assert M.proposals([]) == []


def test_a_clashing_ref_on_another_cut_gets_a_free_id(tmp_path):
    a = {**BRIDGE, "ref": "n1", "coord": (-125.3, 50.0)}
    b = {**BRIDGE, "ref": "n1", "coord": (-125.1, 50.2)}
    assert [p["id"] for p in M.proposals(_marks_with(tmp_path, ("a", [a]), ("b", [b])))] == ["n1", "n1-2"]


def test_place_y_may_carry_an_extent_fix_on_existing_cuts_only(tmp_path):
    ent, typed = _world(tmp_path)
    p = D.prints_of(LOC, ent, typed)
    ok = {"scope": "entry", "op": "downstream_of", "points": [{"kind": "split", "split_id": "a_split"}]}
    yes = {"verdict": "verified", "answers": {"T": "y", "R": "y", "P": "y"}, "problem": None, "note": ""}
    assert _mark(p, **yes, extent_fix=[ok]).verdict == "verified"       # the cuts are right: Place y
    new = {"scope": "entry", "op": "downstream_of", "points": [{"kind": "coord", "coord": (-125.3, 50.0)}]}
    with pytest.raises(ValueError, match="wrong or unsure"):
        _mark(p, **yes, extent_fix=[new])                               # a cut that does not exist yet
    with pytest.raises(ValueError, match="wrong or unsure"):
        _mark(p, **yes, new_splits=[BRIDGE])


def test_a_ticket_says_a_split_is_already_proposed(tmp_path):
    q = {"ref": "n2", "anchor": "confluence", "name": "Quinsam River", "item_id": "gnis:27883",
         "on_item": "gnis:1", "blk": "354", "measure": 4199.5, "coord": (-125.294097, 50.037899)}
    marks = _marks_with(tmp_path, ("a", [q]), ("b", [{**q, "ref": "n3"}]))
    tickets = dict(T.export(marks))
    first, second = tickets["review-wrong-dfo-a.md"], tickets["review-wrong-dfo-b.md"]
    assert "also proposed on 1 other mark" in first and "ONE cut `n2`" in first
    assert "ALREADY PROPOSED as `n2`" in second and "first on `a`" in second
    assert T.parse_ticket(second)["new_splits"][0]["ref"] == "n3"       # the mark's own line unchanged


# --------------------------------------------------------------------------- the saved extent shows again

def test_a_dfo_mark_with_an_extent_fix_round_trips_and_the_latest_mark_wins(tmp_path):
    """Owner 2026-10-27: "didn't save the ones I did before". The fix IS in the store; the screen
    must read it back. Saved -> loaded it is the same fix; a LATER mark replaces it (the latest
    wins on display), and a later mark with no fix is the reviewer taking it off."""
    ent, typed = _world(tmp_path)
    p = D.prints_of(LOC, ent, typed)
    fix = {"scope": "entry", "op": "between", "items": ["gnis:1"], "tributaries": True,
           "points": [{"kind": "split", "split_id": "a_split"},
                      {"kind": "confluence", "name": "Quinsam River", "item_id": "gnis:27883"}]}
    store = tmp_path / "marks.jsonl"
    store.write_text("")
    M.append(store, [_mark(p, extent_fix=[fix], date="2026-10-25")])
    got = M.latest(M.load(store))[("dfo", LOC, None)]
    assert [f.model_dump(exclude_none=True) for f in got.extent_fix] == [
        {**fix, "points": [{"kind": "split", "split_id": "a_split"},
                           {"kind": "confluence", "name": "Quinsam River", "item_id": "gnis:27883"}]}]
    newer = {**fix, "op": "downstream_of", "points": fix["points"][:1]}
    M.append(store, [_mark(p, extent_fix=[newer], date="2026-10-26")])
    assert M.latest(M.load(store))[("dfo", LOC, None)].extent_fix[0].op == "downstream_of"
    M.append(store, [_mark(p, date="2026-10-27")])
    assert M.latest(M.load(store))[("dfo", LOC, None)].extent_fix == []


def test_the_dfo_screen_opens_on_the_saved_fix_and_says_when_it_saved_it():
    """The editor PRE-FILLS from the mark in force (so marking again carries the fix on instead of
    silently dropping it), is open on every location, draws the saved extent in its own colour,
    and confirms or refuses a save in words. Source pins: there is no JS runner in the repo."""
    from pathlib import Path
    root = Path(__file__).resolve().parents[2] / "curation-review/frontend/src/components"
    dfo, editor, mp = ((root / n).read_text() for n in ("DfoReview.tsx", "ExtentFixEditor.tsx", "MapPanel.tsx"))
    assert "useState<ExtentFix | null>(saved)" in dfo and "savedFixOf(was)" in dfo
    assert "your extent (saved {was?.date" in dfo and "fixWords(saved" in dfo
    assert "<ExtentFixEditor defaultOpen" in dfo and "useState(!!value || defaultOpen)" in editor
    assert 'data-testid="dfo-save-extent"' in dfo and "saved ✓" in dfo
    assert 'data-testid="dfo-errors"' in dfo and "NOT SAVED" in dfo and 'role="alert"' in dfo
    assert "saved={savedFc}" in dfo and "saved-lines" in mp
