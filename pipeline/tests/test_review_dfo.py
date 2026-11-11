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
from pipeline.tests import under_test

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


def test_a_rescrape_stales_no_mark_and_only_the_curated_geography_stales_p(tmp_path):
    """T366 (owner 2026-11-03): only a PLACE or its binding is re-reviewed — a re-scrape that changes the rows stales
    no DFO mark (the change goes to the digest); the curated geography moving still does."""
    ent, typed = _world(tmp_path)
    m = _mark(D.prints_of(LOC, ent, typed))
    now = M.Prints.model_validate(D.prints_of(LOC, ent, typed))
    assert M.stale_reasons(m, now) == []
    (tmp_path / "b").mkdir()
    (tmp_path / "c").mkdir()
    ent2, typed2 = _world(tmp_path / "b", rows=("Coho | Oct 1 to Dec 31 | 4 per day",))
    moved = M.Prints.model_validate(D.prints_of(LOC, ent2, typed2))
    assert moved.records[0].meaning != now.records[0].meaning      # the rows did change ...
    assert M.stale_reasons(m, moved) == []                          # ... and the place answer stands
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


def test_the_dfo_screen_logic_runs():
    """T525: what a DFO mark does to the place's extent — keep / not sure / wrong carry no fix, a change carries its
    pieces only when it changes something and says keep / replace / add, pieces are added and dropped on the place's
    one tributary call, a piece previews alone or in the union, every mark names the extent it was made on, the save
    is held in words — run by node on the screen's own functions (`frontend/src/dfoExtent.ts`, `extentWords.ts`), and
    a refused save says when a later save of its item is in the store (`refusals.supersededBy`): every
    `frontend/tests/*.test.ts`."""
    import shutil
    import subprocess
    from pathlib import Path
    node = shutil.which("node")
    # the review app runs on node; without it the screen's logic is unchecked, which must fail, never skip
    assert node is not None, "node is not installed: the DFO screen's functions cannot be run"
    front = Path(__file__).resolve().parents[2] / "curation-review/frontend"
    got = subprocess.run([node, "--test", "--test-reporter=tap", "tests/*.test.ts"], cwd=front,
                         capture_output=True, text=True)
    assert "# fail 0" in got.stdout and "# pass 0" not in got.stdout, got.stdout
    assert got.returncode == 0, got.stdout + got.stderr


def test_every_curated_extent_opens_in_the_editor_or_says_why():
    """T525: a change starts FROM the place's extent — every curated extent the editor can say opens as pieces that land
    as exactly that extent again (`extent_of`), its tributary flag as it is (none stays inherited); one holding a field
    an extent fix has no word for opens on nothing and says which field, so no change can drop it."""
    from pathlib import Path
    from pipeline.regs.review import proposals as P
    opened = refused = 0
    for f in sorted(Path(under_test.curated().dfo_salmon.places).glob("region-*.json")):
        for loc in json.loads(f.read_text(encoding="utf-8"))["locations"]:
            b = loc.get("binding") or {}
            exs = b.get("extents") or []
            fixes, why = P.fixes_of_extents(exs, b.get("tributaries"))
            if why:
                refused += 1
                assert not fixes and all("cannot express" in w or ": " in w for w in why), (loc["location_id"], why)
                continue
            back = [P.extent_of(x, lambda _i, p: p.split_id, rest_at_build=True) for x in fixes]
            assert [e for e, _p in back] == exs and not any(p for _e, p in back), loc["location_id"]
            assert all(x.tributaries == b.get("tributaries") for x in fixes)
            opened += bool(exs)
    assert opened > 100 and refused > 0
    fixes, why = P.fixes_of_extents([{"op": "within", "area_id": "area:drains_to:pfma_28",
                                      "within_area": "area:region:2"}], None)
    assert fixes == [] and why == ["extent 1 (within) holds within_area, which an extent fix cannot express: a hand edit"]


def test_a_fix_replacing_an_extent_it_cannot_say_again_is_refused(tmp_path):
    """T525: an extent fix writes op / cuts / waters / area only, so replacing a curated extent that holds more (a
    `within_area`) would drop it: the proposal names the field and is not applicable — the file keeps it."""
    from pipeline.regs.review import proposals as P
    held = {"op": "within", "area_id": "area:drains_to:pfma_28", "within_area": "area:region:1"}
    ent, typed = _world(tmp_path, extents=[held])
    splits = tmp_path / "splits.json"
    splits.write_text(json.dumps({"waterbodies": []}))
    m = _mark(D.prints_of(LOC, ent, typed),
              extent_fix=[{"scope": "entry", "op": "whole", "items": ["gnis:1"], "points": []}])
    got = [p for p in P.build([m], P.Sources(entries_dir=ent, dfo_dir=ent, splits_path=splits))
           if p["kind"] == "extent"]
    assert len(got) == 1 and not got[0]["applicable"]
    assert got[0]["problems"] == ["entry: extent 1 (within) holds within_area, which an extent fix cannot express: "
                                  "a hand edit"]


def _backend(monkeypatch):
    """the review backend's `dfo` module, fresh (its readers patched per test)"""
    import sys
    from pathlib import Path
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "curation-review" / "backend"))
    sys.modules.pop("dfo", None)
    import dfo
    return dfo


def test_a_mark_on_an_extent_that_moved_since_is_refused(monkeypatch):
    """T525: a DFO mark names the extent the owner was shown (`extent_before`); `mark_for` refuses one that names none,
    and one made on an extent the file no longer has — for every member of a group save too, so a member whose extent
    differs is never labelled with it."""
    B = _backend(monkeypatch)
    ext = [{"op": "between", "splits": ["test_river__a", "test_river__b"], "item_id": "gnis:1"}]
    here = {"location_id": LOC, "binding": {"extents": ext, "tributaries": None}}
    other = {"location_id": LOC + "~2", "binding": {"extents": [{"op": "whole"}], "tributaries": None}}
    monkeypatch.setattr(B, "_locations", lambda: {LOC: ("1", here, None), LOC + "~2": ("1", other, None)})
    monkeypatch.setattr(B, "_groups", lambda: {LOC: {"id": "g", "members": [{"id": LOC}, {"id": LOC + "~2"}]}})
    body = {"scope": "dfo", "target": LOC, "verdict": "verified", "answers": {"T": "y", "R": "y", "P": "y"},
            "reviewer": "t"}
    with pytest.raises(ValueError, match="extent_before"):
        B.mark_for(body, date="2026-11-09", pdf_sha256=SHA)
    with pytest.raises(ValueError, match="changed since you opened it"):
        B.mark_for({**body, "extent_before": {"extents": [{"op": "whole"}], "tributaries": None}},
                   date="2026-11-09", pdf_sha256=SHA)
    seen = B.seen_extent(here)
    assert seen == {"extents": ext, "tributaries": None}
    # an older wording is written only when listed with the extent the owner saw it hold (owner 2026-11-09) …
    with pytest.raises(ValueError, match="is not listed"):
        B.marks_for_group({**body, "extent_before": seen, "targets": [LOC + "~2"]}, date="2026-11-09",
                          pdf_sha256=SHA)
    # … and that extent must be its own now (R3 per wording)
    with pytest.raises(ValueError, match=r"~2: its extent changed since you opened it"):
        B.marks_for_group({**body, "extent_before": seen, "targets": [LOC + "~2"],
                           "members": {LOC + "~2": {"extent_before": seen}}}, date="2026-11-09", pdf_sha256=SHA)


def test_a_fix_is_compared_as_the_place_binds(monkeypatch):
    """T524 review: the pieces of a place are a union (no order), and a tributary call is compared as the place BINDS
    with it — its own flag, else its water's (the DFO title) — so a fix calling what the place already inherits
    replaces nothing."""
    B = _backend(monkeypatch)
    a = {"op": "downstream_of", "splits": ["x__a"], "item_id": "gnis:1"}
    b = {"op": "upstream_of", "splits": ["y__b"], "item_ids": ["gnis:3", "gnis:2"]}
    assert B.same_extents([a, b], [{**b, "item_ids": ["gnis:2", "gnis:3"]}, a])
    assert not B.same_extents([a], [a, b]) and not B.same_extents([a], [{**a, "within_area": "area:region:1"}])
    loc = {"binding": {"tributaries": None}}
    assert B.tributaries_now(loc, {"tributaries": True}) is True
    assert B.tributaries_now({"binding": {"tributaries": False}}, {"tributaries": True}) is False
    assert B.tributaries_now(loc, None) is None
    monkeypatch.setattr(B, "rekeyed", lambda mark: mark)
    fix = {"scope": "entry", "op": "downstream_of", "items": ["gnis:1"], "points": [{"kind": "split", "split_id": "x__a"}]}
    place = {"binding": {"extents": [a], "tributaries": None}}
    title = {"tributaries": True}
    m = _mark({"text": "t", "records": [{"key": "entry", "meaning": "m", "binding": "b"}]},
              extent_fix=[{**fix, "tributaries": True}])
    assert B.fix_vs_current(m, place, title)["state"] == "applied"           # what the title already says
    assert B.fix_vs_current(m, place, {"tributaries": False})["state"] == "replaces"


def test_a_piece_half_made_is_refused():
    """T525 (owner 2026-11-09, the Chilliwack shot): a place's second piece with no point picked is refused by the mark
    model, naming what it lacks — the screen holds the save too (`dfoExtent.incompletePieces`)."""
    whole = {"scope": "entry", "op": "between", "items": ["gnis:1"],
             "points": [{"kind": "split", "split_id": "test_river__a"}, {"kind": "split", "split_id": "test_river__b"}]}
    empty = {"scope": "entry", "op": "downstream_of", "items": [], "points": []}
    with pytest.raises(ValueError, match="takes 1 split point, not 0"):
        _mark({"text": "t", "records": [{"key": "entry", "meaning": "m", "binding": "b"}]}, extent_fix=[whole, empty])
    with pytest.raises(ValueError, match="a split point names its split"):
        _mark({"text": "t", "records": [{"key": "entry", "meaning": "m", "binding": "b"}]},
              extent_fix=[whole, {**empty, "points": [{"kind": "split"}]}])


def test_the_apply_refuses_a_mark_made_on_an_extent_that_has_moved(tmp_path):
    """T525: a DFO mark keeps the extent it was made on (`Mark.extent_before`). The apply lands its fix only while the
    file still has that extent — a place that moved after the owner looked (a hand edit, another mark) is refused,
    naming what the mark saw; once the mark's own fix has landed it changes nothing and is not held."""
    from pipeline.regs.review import proposals as P
    a = {"op": "between", "splits": ["test_river__a", "test_river__b"], "item_id": "gnis:1"}
    fix = {"scope": "entry", "op": "downstream_of", "items": ["gnis:1"],
           "points": [{"kind": "split", "split_id": "test_river__a"}]}
    splits = tmp_path / "splits.json"
    splits.write_text(json.dumps({"waterbodies": []}))

    def built(file_extents, seen):
        ent, typed = _world(tmp_path, extents=file_extents)
        m = _mark(D.prints_of(LOC, ent, typed), extent_fix=[fix],
                  extent_before={"extents": seen, "tributaries": None})
        return [p for p in P.build([m], P.Sources(entries_dir=ent, dfo_dir=ent, splits_path=splits))
                if p["kind"] == "extent"]
    ok = built([a], [a])
    assert len(ok) == 1 and not any("changed since" in x for x in ok[0]["problems"])
    moved = built([{**a, "splits": ["test_river__a", "test_river__c"]}], [a])
    assert len(moved) == 1 and not moved[0]["applicable"]
    assert moved[0]["problems"] == [f"entry: the extent changed since this mark was made (it saw "
                                    f"{json.dumps({'extents': [a], 'tributaries': None})}): mark the place again"]
    landed = built([{"op": "downstream_of", "splits": ["test_river__a"], "item_id": "gnis:1"}], [a])
    assert landed == []                                  # its own fix is the extent now: applied, nothing to do
    with pytest.raises(ValueError, match="only a DFO mark names the extent"):
        M.Mark.model_validate({"scope": "entry", "target": "x", "verdict": "unsure", "answers": {"P": "?"},
                               "reviewer": "t", "date": "2026-11-09", "pdf_sha256": SHA,
                               "prints": {"text": "t", "records": [{"key": "entry", "meaning": "m", "binding": "b"}]},
                               "extent_before": {"extents": []}})


@pytest.mark.needs_atlas            # the cut labels' registry boundaries and the water names come from the built registry
def test_every_cut_a_place_names_reads_by_its_label(monkeypatch):
    """T525 (owner 2026-11-09: "the owner reviews by words"): a cut off this map (the Forks' Bulkley cuts on the Morice
    place) is labelled from the curated cuts; a string that is no cut gets no label."""
    B = _backend(monkeypatch)
    got = B.cut_labels({"fix": [{"splits": ["bulkley_river__morice_river_into_bulkley_river_u100m"]}],
                        "text": "not a cut"})
    assert got == {"bulkley_river__morice_river_into_bulkley_river_u100m":
                   "the Forks fishing boundary signs, 100 m up the upper Bulkley"}
    assert B.item_names({"pieces": [{"items": ["gnis:2195"]}], "text": "not a water"}) == {"gnis:2195": "Bulkley River"}


def _group_world(monkeypatch, B, *, fourth=None):
    """a place and its older wordings: ~2 binds the same extent, ~3 another one, ~4 (optional) as given"""
    ext = [{"op": "between", "splits": ["test_river__a", "test_river__b"], "item_id": "gnis:1"}]
    loc = lambda lid, exts, words: {"location_id": lid, "status": "active" if lid == LOC else "historic",  # noqa: E731
                                    "source_text": {"specific_area": words}, "binding": {"extents": exts}}
    locs = {LOC: ("1", loc(LOC, ext, "between the bridge and the falls"), None),
            LOC + "~2": ("1", loc(LOC + "~2", list(ext), "between the bridge & the falls"), None),
            LOC + "~3": ("1", loc(LOC + "~3", [{"op": "whole", "item_id": "gnis:1"}], "the whole river"), None)}
    if fourth is not None:
        locs[LOC + "~4"] = ("1", loc(LOC + "~4", fourth, "inside the park"), None)
    monkeypatch.setattr(B, "_locations", lambda: locs)
    monkeypatch.setattr(B, "_groups", lambda: {LOC: {"id": "g", "canonical": LOC,
                                                     "members": [{"id": i} for i in locs]}})
    monkeypatch.setattr(B.D, "prints", lambda *_a, **_k: {"text": "t", "records": [
        {"key": "entry", "meaning": "m", "binding": "b"}]})
    monkeypatch.setattr(B, "_dedupe_against_store", lambda s, **_k: (s, {}, []))
    return ext, locs


def test_a_save_on_older_wordings_writes_each_ticked_one_and_gives_a_differing_one_this_extent(monkeypatch):
    """owner 2026-11-09: "it should save extent to all variants of location text" — and "we cant blindly assume
    variants": the save writes ONLY the wordings ticked and listed (each with the extent the owner saw it hold); one
    binding the same takes the check, one that DIFFERS takes the shown place's extent as a fix (on existing cuts, its own
    `extent_before`) plus the check — never a "correct" stamped on another extent; a defer gives no extent."""
    B = _backend(monkeypatch)
    ext, locs = _group_world(monkeypatch, B)
    seen = {i: B.seen_extent(l) for i, (_s, l, _w) in locs.items()}
    body = {"scope": "dfo", "target": LOC, "verdict": "verified", "answers": {"T": "y", "R": "y", "P": "y"},
            "reviewer": "t", "extent_before": seen[LOC], "targets": [LOC, LOC + "~2", LOC + "~3"],
            "members": {LOC + "~2": {"extent_before": seen[LOC + "~2"]},
                        LOC + "~3": {"extent_before": seen[LOC + "~3"]}}}
    w = B.wording_check(locs[LOC][1], None, locs[LOC + "~3"][1], None)
    assert w == {"same_extent": False, "cannot_change": [], "cannot_take": []}
    assert B.wording_check(locs[LOC][1], None, locs[LOC + "~2"][1], None)["same_extent"] is True
    marks = B.marks_for_group(body, date="2026-11-09", pdf_sha256=SHA)
    assert [m.target for m in marks] == [LOC, LOC + "~2", LOC + "~3"] and {m.group for m in marks} == {"g"}
    assert [m.verdict for m in marks] == ["verified"] * 3
    assert marks[0].extent_fix == [] and marks[1].extent_fix == []
    fix = marks[2].extent_fix
    assert len(fix) == 1 and fix[0].op == "between" and [p.split_id for p in fix[0].points] == ["test_river__a",
                                                                                              "test_river__b"]
    assert marks[2].extent_before.extents == [{"op": "whole", "item_id": "gnis:1"}]
    # unticked wordings are not written: only the target
    only = B.marks_for_group({**body, "targets": [LOC], "members": {}}, date="2026-11-09", pdf_sha256=SHA)
    assert [m.target for m in only] == [LOC]
    # a defer decides nothing: it carries no extent to a differing wording
    d = B.marks_for_group({**body, "verdict": "deferred", "answers": {}, "note": "unclear"}, date="2026-11-09",
                          pdf_sha256=SHA)
    assert all(m.extent_fix == [] for m in d)
    # a wording listed but not ticked is refused: the server never adds one on its own
    with pytest.raises(ValueError, match="listed but not ticked"):
        B.marks_for_group({**body, "targets": [LOC, LOC + "~2"]}, date="2026-11-09", pdf_sha256=SHA)


def test_an_older_wording_holding_what_a_fix_cannot_say_is_refused_by_name(monkeypatch):
    """R4 per wording: a ticked wording whose extent would change while it holds a field no extent fix can say (a
    `within_area`) is refused by its words — nothing is saved, and it is left for a hand edit."""
    B = _backend(monkeypatch)
    held = [{"op": "within", "area_id": "area:drains_to:pfma_28", "within_area": "area:region:1"}]
    _ext, locs = _group_world(monkeypatch, B, fourth=held)
    seen = {i: B.seen_extent(l) for i, (_s, l, _w) in locs.items()}
    assert B.wording_check(locs[LOC][1], None, locs[LOC + "~4"][1], None)["cannot_change"]
    body = {"scope": "dfo", "target": LOC, "verdict": "verified", "answers": {"T": "y", "R": "y", "P": "y"},
            "reviewer": "t", "extent_before": seen[LOC], "targets": [LOC, LOC + "~3", LOC + "~4"],
            "members": {t: {"extent_before": seen[t]} for t in (LOC + "~3", LOC + "~4")}}
    with pytest.raises(ValueError, match="“inside the park” cannot take this extent .*within_area"):
        B.marks_for_group(body, date="2026-11-09", pdf_sha256=SHA)


def test_a_check_on_older_wordings_carries_each_ones_own_digest(monkeypatch):
    """T532 + owner 2026-11-09: a checklist on ticked wordings records the rows the owner checked, with each wording's
    OWN digest as it will bind — the same-extent one its digest now, the differing one its words over the shown extent
    (so the check is in force once that extent lands)."""
    B = _backend(monkeypatch)
    import checks
    ext, locs = _group_world(monkeypatch, B)
    seen = {i: B.seen_extent(l) for i, (_s, l, _w) in locs.items()}
    rows = [{"n": 1, "kind": "water", "state": "ok"}]
    body = {"scope": "dfo", "target": LOC, "verdict": "verified", "answers": {"P": "y"}, "reviewer": "t",
            "extent_before": seen[LOC], "checklist": {"digest": "a" * 16, "rows": rows},
            "targets": [LOC, LOC + "~2", LOC + "~3"],
            "members": {t: {"extent_before": seen[t]} for t in (LOC + "~2", LOC + "~3")}}
    marks = B.marks_for_group(body, date="2026-11-09", pdf_sha256=SHA)
    two, three = locs[LOC + "~2"][1], locs[LOC + "~3"][1]
    assert marks[1].checklist.digest == checks.digest_as(two, None)
    assert marks[2].checklist.digest == checks.digest_as(three, None, locs[LOC][1], None) != checks.digest_as(three, None)
    assert [r.model_dump() for r in marks[2].checklist.rows] == [{**rows[0], "ticked": False, "wrong": False}]
