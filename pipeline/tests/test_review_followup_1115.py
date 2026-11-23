"""T755 FOLLOW-UP (2026-11-15): the four review-app items left after the owner's overnight notes and T742.

  border     the BC border is a pickable end: `border:<blk>:<n>` cuts (the Tatshenshini leaves BC at km 102 and comes
             back at km 146) are read from the atlas's own section bounds, named by the one namer (`reuse.cut_name`)
             and offered by every picker — the map list, the name search, the regs editor's boundaries
  cannot     a place whose extent the editor cannot say again (Blanchard's `within_area`) says why in plain words and
             what the owner can do; each shape the 15 unmarked places of the T742 scan held gets a reason, and every
             stored place either opens in the editor or says why
  speed      a save rebuilds only the saved place's rows and checklist place (`reuse.marks_delta`,
             `dfo._patched_rows`, `checks._patched_places`); a save that proposes no cut keeps the proposed cuts
  old        "old extent still in force": the last save is wrong / unsure with no fix while an older extent holds —
             one predicate (`audit.old_extent_in_force`) for the checklist flag and the audit's section B

Data-free: stand-ins for the atlas, and the curated DFO place files the repo holds.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "curation-review" / "backend"))

from pipeline.regs.review import audit as A  # noqa: E402


class _Reuse:
    """THE `reuse` THE BACKEND MODULES USE NOW: another test may reload them, so a module-level import would patch a
    stale copy — every read goes through `dfo.reuse` at the moment it is made"""
    def __getattr__(self, k):
        import dfo
        return getattr(dfo.reuse, k)


reuse = _Reuse()


@pytest.fixture()
def R():
    import dfo
    return dfo.reuse

ROOT = Path(__file__).resolve().parents[2]

# ------------------------------------------------------------------------------------------------ 1. the BC border

#: the Tatshenshini's two border cuts as the served atlas holds them (blk 360886456, T742 investigation, [ran])
TAT = {"border:360886456:0": {"blk": "360886456", "measure": 102477.87, "below_out": False, "above_out": True},
       "border:360886456:1": {"blk": "360886456", "measure": 145669.52, "below_out": True, "above_out": False}}
TAT_LL = {102477.87: (-137.2138, 60.0008), 145669.52: (-136.8848, 59.9995)}


@pytest.fixture()
def border(monkeypatch, R):
    """the border cuts read from stand-in section bounds, at their own points"""
    from shapely.geometry import LineString, Point
    reuse = R
    monkeypatch.setattr(reuse, "_border_rows", lambda: TAT)
    monkeypatch.setattr(reuse, "_point_at", lambda blk, m: Point(*TAT_LL[round(m, 2)]))
    monkeypatch.setattr(reuse, "_geo", lambda g, kind, props: {"geometry": {"coordinates": [g.x, g.y]}})
    monkeypatch.setattr(reuse, "_splits_meta", lambda: {})
    monkeypatch.setattr(reuse, "_blk_index", lambda: {"360886456": [(102477.87, 145669.52, "360886456:102477")]})
    monkeypatch.setattr(reuse, "_geoms", lambda: {"360886456:102477": LineString([(0, 0), (43191.65, 0)])})
    tat = NS(name="Tatshenshini River", boundaries=[NS(id="length:360886456:22759"), NS(id="border:360886456:0"),
                                                     NS(id="border:360886456:1")], ref_ids=())
    monkeypatch.setattr(reuse, "_registry", lambda: {"gnis:25410": tat})
    monkeypatch.setattr(reuse, "region_ends", lambda iid: ())        # no region line here (and no graph to read)
    reuse.border_end.cache_clear(), reuse.border_ends.cache_clear()
    yield
    reuse.border_end.cache_clear(), reuse.border_ends.cache_clear()


def test_the_neighbour_is_read_from_where_the_line_runs():
    assert reuse.border_neighbour(-137.2138, 60.0008) == "Yukon"                  # the Tatshenshini
    assert reuse.border_neighbour(-121.5, 60.0) == "Northwest Territories"
    assert reuse.border_neighbour(-120.5, 49.0) == "Washington"
    assert reuse.border_neighbour(-116.5, 49.0) == "Idaho"
    assert reuse.border_neighbour(-115.0, 49.0) == "Montana"
    assert reuse.border_neighbour(-120.0, 56.0) == "Alberta"                     # the 120th meridian
    assert reuse.border_neighbour(-116.29, 51.45) == "Alberta"                   # the divide (Kicking Horse Pass)
    assert reuse.border_neighbour(-132.2, 56.7) == "Alaska"                      # the Stikine
    assert reuse.border_neighbour(-123.0, 52.0) is None                          # not on the outline: never guessed


def test_both_tatshenshini_crossings_read_apart_and_say_which_side_leaves_bc(border):
    a, b = reuse.border_ends("gnis:25410")
    assert a["label"] == "BC–Yukon border · km 102 — outside BC upstream (to km 146)"
    assert b["label"] == "BC–Yukon border · km 146 — outside BC downstream (to km 102)"
    assert a["bindable"] and a["coord"] == [-137.2138, 60.0008]
    # the one namer, and the pin, on every water
    assert reuse.cut_name("border:360886456:1") == b["label"]
    assert reuse.cut_point_on("border:360886456:0") == (-137.2138, 60.0008)
    assert reuse.border_end("length:360886456:22759") is None


def test_a_border_cut_has_a_point_though_splits_resolved_has_no_row(border):
    """`_split_point` (the map, the checklist pins, the regs side's split points) reads the section bound's measure"""
    p = reuse._split_point("border:360886456:1")
    assert p is not None and round(p.x, 1) == 43191.6


def test_every_picker_offers_the_border(border, monkeypatch, R):
    reuse = R
    # the regs editor's boundary list: named, pickable, carrying its crossing
    got = reuse.with_region_ends([{"id": "border:360886456:0", "label": "BC boundary", "kind": "border",
                                   "item_id": "gnis:25410"}], lambda b: b.get("item_id"))
    assert got[0]["label"].startswith("BC–Yukon border · km 102") and got[0]["pickable"] and got[0]["border_end"]
    # the name search ("BC boundary" returned [] before): the asker's water's border cuts
    monkeypatch.setattr(reuse, "_split_records", lambda: {})
    hits = reuse.search_splits("border", first=["Tatshenshini River"])
    assert [h["id"] for h in hits] == ["border:360886456:0", "border:360886456:1"]
    assert {h["kind"] for h in hits} == {"BC border"} and all(h["own"] for h in hits)
    assert reuse.search_splits("yukon", first=["Tatshenshini River"])            # by the neighbour too
    assert reuse.search_splits("border", first=["Fraser River"]) == []           # never another water's
    # the DFO map list: placed, never "the served atlas has no point for it"
    import dfo
    monkeypatch.setattr(reuse, "_boundary_index", lambda: {
        "border:360886456:1": ("gnis:25410", NS(id="border:360886456:1", wbk="", label="BC boundary"))})
    f = dfo._split_feature("border:360886456:1", rules=[], to_verify=False, item_id="gnis:25410")
    assert "unplaced" not in f and f["geometry"]["coordinates"][0] == pytest.approx(43191.65)   # on its section
    assert f["properties"]["label"].startswith("BC–Yukon border · km 146")


# ------------------------------------------------------------------------------------------------ 2. cannot edit

def test_blanchard_says_why_it_cannot_be_edited_and_what_to_do():
    import dfo
    from pipeline.regs.review import proposals as P
    ex = [{"op": "whole", "item_id": "gnis:13637", "within_area": "area:region:6"}]
    _pieces, why = P.fixes_of_extents(ex, None)
    got = dfo.cannot_edit_words(ex, why)
    assert got["why"][0] == ('The extent is held only inside Region 6, and the editor has no way to say "only inside '
                             'an area" on a water.')
    assert "edit button is off" in got["why"][-1]
    assert "confirm it as it is" in got["can"] and "mark it wrong" in got["can"]
    assert dfo.cannot_edit_words([{"op": "whole", "item_id": "gnis:1"}], []) is None


def test_each_field_the_editor_cannot_say_has_its_words():
    import dfo
    ex = [{"op": "within", "area_id": "area:drains_to:pfma_5", "feature_types": ["stream"],
           "outside_areas": ["area:region:6"]},
          {"op": "upstream_of", "splits": ["c"], "item_id": "gnis:2936", "watershed": True}]
    got = dfo.cannot_edit_words(ex, ["extent 1 (within) holds …", "extent 2 (upstream_of) holds …", "something else"])
    text = " ".join(got["why"])
    assert "Extent 1 holds only streams" in text and "Extent 1 leaves out Region 6" in text
    assert "Extent 2 takes the whole watershed" in text
    assert "something else" in text                     # a reason it does not know is shown as it is, never dropped


#: THE SHAPES THE 15 UNMARKED PLACES OF THE T742 SCAN (2026-11-15) HELD, built here (which live places are in that state
#: moves with every save of his): (the place's extents, the plain words each must show). `area:mu:x` / `area:sz:k`
#: are named by the stand-in area names below.
SHAPES = {
    "a region default leaving out an area (1:default:region)":
        ([{"op": "within", "area_id": "area:region:1", "outside_areas": ["area:mu:x"]}],
         ["The extent leaves out MU 6-12 and 6-13"]),
    "two extents held inside a region (2:default:areas-28-29)":
        ([{"op": "within", "area_id": "area:pfma:28", "within_area": "area:region:2"},
          {"op": "within", "area_id": "area:pfma:29", "within_area": "area:region:2"}],
         ["Extent 1 is held only inside Region 2", "Extent 2 is held only inside Region 2"]),
    "a water held inside a sign zone (6:skeena-river:all-waters-within-the-4-triangular)":
        ([{"op": "whole", "item_id": "gnis:1", "within_area": "area:sz:k"}],
         ["The extent is held only inside Kispiox confluence sign zone"]),
    "streams only, leaving out areas (6:e:area:areas-5~2)":
        ([{"op": "within", "area_id": "area:pfma:5", "feature_types": ["stream"], "outside_areas": ["area:mu:x"]}],
         ["The extent holds only streams", "The extent leaves out MU 6-12 and 6-13"]),
    "a watershed above a point (6:b(i):section)":
        ([{"op": "upstream_of", "item_id": "gnis:1", "splits": ["c"], "watershed": True}],
         ["The extent takes the whole watershed above its point"]),
    "a watershed below a point (6:b(ii):section)":
        ([{"op": "downstream_of", "item_id": "gnis:1", "splits": ["c"], "watershed": True}],
         ["The extent takes the whole watershed below its point"]),
}


def _why(loc: dict) -> list[str]:
    from pipeline.regs.review import proposals as P
    b = loc.get("binding") or {}
    return P.fixes_of_extents(b.get("extents") or [], b.get("tributaries"),
                              tributaries_only=bool(b.get("tributaries_only")),
                              tributary_excludes=list(b.get("tributary_excludes") or []))[1]


def _shown(loc: dict) -> tuple[dict, dict | None]:
    """the place's extent as the checklist / DFO tab open on it (`extent_now`), and the region cascade's words"""
    import dfo
    import region_cascade
    # T775 item 2: `extent_now` lists the water's places for ANY place, so it builds an `EntryLocation`: the bare shape
    # here gets the fields every real place carries
    whole = {"region": "1", "region_number": 1, "kind": "water", "precedence": 3, "water": "River", **loc}
    return dfo.extent_now(whole, None), region_cascade._cannot_edit(loc)


def _plain(why: list[str], got: dict | None, at) -> None:
    """a place the editor cannot change gets a plain reason and what to do, the same on every screen"""
    assert got and got["can"] and "confirm it as it is" in got["can"] and len(got["why"]) >= 2, at
    assert "edit button is off" in got["why"][-1], at
    assert not any("hand edit" in w for w in got["why"]), at                  # plain words, not the model's
    assert len(got["why"]) > len(why), at                                     # every reason said, plus the button


@pytest.fixture()
def no_walk(monkeypatch):
    """`extent_now`'s tributary-walk answer reads the atlas; it is no part of the reason"""
    import dfo
    monkeypatch.setattr(dfo, "walks_now", lambda loc, water: None)
    monkeypatch.setattr(dfo.reuse, "_area_name_map", lambda: {"area:mu:x": "MU 6-12 and 6-13",
                                                              "area:sz:k": "Kispiox confluence sign zone"})


@pytest.mark.parametrize("shape", list(SHAPES))
def test_each_shape_the_editor_cannot_say_shows_its_reason(shape, no_walk):
    """each shape the 15 held gets its plain reason with what to do — through `extent_now` (the checklist, the DFO
    tab) and the region cascade's words, the same; the same place without the field converts"""
    from pipeline.regs.review import proposals as P
    extents, words = SHAPES[shape]
    loc = {"location_id": "x", "binding": {"extents": extents}}
    why = _why(loc)
    assert why, shape
    now, got = _shown(loc)
    assert now["pieces"] == [] and now["cannot_change"] == why and now["cannot_edit"] == got, shape
    _plain(why, got, shape)
    text = " ".join(got["why"])
    for w in words:
        assert w in text, (shape, w, text)
    plain = [{k: v for k, v in ex.items() if k in P.EXPRESSIBLE} for ex in extents]
    loc = {"location_id": "x", "binding": {"extents": plain}}
    now, got = _shown(loc)
    assert _why(loc) == [] and now["pieces"] and now["cannot_edit"] is None and got is None, shape


def test_every_stored_place_converts_or_says_why(no_walk):
    """THE DATA UNDER TEST, whatever he has saved: every curated place either opens in the editor (no reason, no
    words) or shows a plain reason with what to do, the same through `extent_now` and the region cascade"""
    from pipeline.tests import under_test
    n = 0
    for f in sorted(Path(under_test.curated().dfo_salmon.places).glob("region-*.json")):
        for loc in json.loads(f.read_text(encoding="utf-8")).get("locations") or ():
            lid, why = loc["location_id"], _why(loc)
            now, got = _shown(loc)
            assert now["cannot_change"] == why and now["cannot_edit"] == got, lid
            if why:
                _plain(why, got, lid)
                assert now["pieces"] == [], lid
            else:
                assert got is None, lid
            n += 1
    assert n, "the curated place files hold places"


def test_the_editor_and_the_checklist_show_it(tmp_path):
    """the screen side: `cannotEditOf` is what CheckReview / DfoReview / the cascade render (`CannotEditNote`)"""
    src = (ROOT / "curation-review/frontend/src")
    check = (src / "components/CheckReview.tsx").read_text(encoding="utf-8")
    assert "!onEdit && cannotEditOf(d.extent_now)" in check and "<CannotEditNote" in check
    assert "cannotEditOf(now)" in (src / "components/DfoReview.tsx").read_text(encoding="utf-8")
    assert "c.cannot_edit" in (src / "components/RegionCascade.tsx").read_text(encoding="utf-8")


# ------------------------------------------------------------------------------------------------ 3. speed after a save

def _line(target, scope="dfo", **kw) -> str:
    return json.dumps({"scope": scope, "target": target, "rule_id": None, "verdict": "verified", **kw}) + "\n"


def test_marks_delta_names_only_what_was_appended(tmp_path, monkeypatch, R):
    reuse = R
    p = tmp_path / "marks.jsonl"
    p.write_text(_line("1:a") + _line("1:b"), encoding="utf-8")
    monkeypatch.setattr(reuse, "MARKS_PATH", p)
    s0 = reuse.marks_stamp()
    with p.open("a", encoding="utf-8") as fh:
        fh.write(_line("1:c"))
    s1 = reuse.marks_stamp()
    d = reuse.marks_delta(s0, s1)
    assert d == {"keys": frozenset({("dfo", "1:c", None)}), "new_splits": False, "cleared": False}
    with p.open("a", encoding="utf-8") as fh:
        fh.write(_line("r1:e", scope="rule", new_splits=[{"ref": "x"}]) + _line("1:a", verdict="cleared"))
    d = reuse.marks_delta(s1, reuse._stamp((p,)))
    assert d["new_splits"] and d["cleared"] and ("rule", "r1:e", None) in d["keys"]
    # a rewrite (not an append) or a stamp never recorded: None — the caller rebuilds everything
    s2 = reuse.marks_stamp()
    p.write_text(_line("1:z"), encoding="utf-8")
    assert reuse.marks_delta(s2, reuse._stamp((p,))) is None
    assert reuse.marks_delta((("x", 1, 2),), reuse._stamp((p,))) is None
    assert reuse.marks_delta(s2, s2)["keys"] == frozenset()


def test_a_save_that_proposes_no_cut_keeps_the_proposals(tmp_path, monkeypatch, R):
    reuse = R
    p = tmp_path / "marks.jsonl"
    p.write_text(_line("1:a"), encoding="utf-8")
    monkeypatch.setattr(reuse, "MARKS_PATH", p)
    monkeypatch.setattr(reuse, "_PROPOSALS_AT", {})
    monkeypatch.setattr(reuse, "_PROPOSALS_LAST", [])
    first = reuse.proposals_stamp()
    with p.open("a", encoding="utf-8") as fh:
        fh.write(_line("1:b"))
    assert reuse.proposals_stamp() == first                      # nothing proposed: the proposed cuts are kept
    with p.open("a", encoding="utf-8") as fh:
        fh.write(_line("1:c", new_splits=[{"ref": "n1"}]))
    moved = reuse.proposals_stamp()
    assert moved != first and moved == reuse._stamp((p,))       # a cut proposed: they are made again


def test_a_confirmed_closed_area_s_new_cuts_move_the_proposals(tmp_path, monkeypatch, R):
    """T785: a closed-area mark carries its new cuts in `piece.new_splits`, not at the top; `marks.cut_proposals_of`
    proposes them, so the save that confirms it must move the proposals stamp (it stood still: 1215 groups -> 1216,
    same stamp)."""
    reuse = R
    p = tmp_path / "marks.jsonl"
    p.write_text(_line("1:a"), encoding="utf-8")
    monkeypatch.setattr(reuse, "MARKS_PATH", p)
    monkeypatch.setattr(reuse, "_PROPOSALS_AT", {})
    monkeypatch.setattr(reuse, "_PROPOSALS_LAST", [])
    first = reuse.proposals_stamp()
    s0 = reuse.marks_stamp()
    piece = {"points": [{"kind": "new", "split_id": "n9"}], "new_splits": [{"ref": "n9"}]}
    with p.open("a", encoding="utf-8") as fh:
        fh.write(_line("3:area:x", scope="closed_area", piece=piece))
    assert reuse.marks_delta(s0, reuse._stamp((p,)))["new_splits"]
    moved = reuse.proposals_stamp()
    assert moved != first and moved == reuse._stamp((p,))


def test_a_save_rebuilds_only_the_saved_places_rows(tmp_path, monkeypatch, R):
    reuse = R
    import dfo
    p = tmp_path / "marks.jsonl"
    p.write_text(_line("1:river:a"), encoding="utf-8")
    monkeypatch.setattr(reuse, "MARKS_PATH", p)
    locs = {"1:river:a": ("1", {"location_id": "1:river:a", "water_id": "1:river"}, None),
            "1:river:b": ("1", {"location_id": "1:river:b", "water_id": "1:river"}, None),
            "1:other:c": ("1", {"location_id": "1:other:c", "water_id": "1:other"}, None)}
    made = []

    def row(slug, loc, water, m, live=None):
        made.append(loc["location_id"])
        return {"id": loc["location_id"], "status": "active", "state": "bound", "region": "1", "water": "w"}
    monkeypatch.setattr(dfo, "_locations", lambda: locs)
    monkeypatch.setattr(dfo, "_states", lambda: {})
    monkeypatch.setattr(dfo, "_live_waters", lambda: set())
    monkeypatch.setattr(dfo, "_row", row)
    fixed = (reuse._stamp((tmp_path / "x",)), (), (), object(), None)
    inputs = lambda keys: (reuse.marks_stamp(), *fixed[:4], frozenset(keys), locs)         # noqa: E731
    i0 = inputs(())
    rows0 = [row("1", locs[k][1], None, None) for k in locs]
    made.clear()
    with p.open("a", encoding="utf-8") as fh:
        fh.write(_line("1:river:a"))
    got = dfo._patched_rows((i0, rows0), inputs(()))
    assert sorted(made) == ["1:river:a", "1:river:b"]             # the saved place and its water's other place
    assert next(r for r in got if r["id"] == "1:other:c") is rows0[2]       # the rest: the same objects
    # the landed set moved on a key the save does not name: rebuild everything
    assert dfo._patched_rows((i0, rows0), inputs({("dfo", "1:other:c", None)})) is None
    # a rule save leaves every DFO row as it was
    i1 = inputs(())
    with p.open("a", encoding="utf-8") as fh:
        fh.write(_line("r1:e", scope="rule"))
    assert dfo._patched_rows((i1, rows0), inputs(())) is rows0


# ------------------------------------------------------------------------------------------------ 4. old extent in force

def test_old_extent_in_force_is_one_predicate():
    ex = [{"op": "upstream_of", "splits": ["c1"]}]
    assert A.old_extent_in_force({"verdict": "wrong"}, ex)
    assert A.old_extent_in_force({"verdict": "unsure", "extent_fix": []}, ex)
    assert not A.old_extent_in_force({"verdict": "wrong", "extent_fix": [{"op": "whole"}]}, ex)  # a fix, not landed
    assert not A.old_extent_in_force({"verdict": "verified"}, ex)
    assert not A.old_extent_in_force({"verdict": "wrong"}, [])                  # nothing in force
    assert not A.old_extent_in_force(None, ex)


def test_the_audit_says_the_flag():
    ex = [{"op": "upstream_of", "splits": ["c1"]}]
    p = {"location_id": "1:river:a", "region": "1", "water_id": "1:river", "status": "active", "kind": "water",
         "source_text": {}, "binding": {"extents": ex}}
    inp = A.Inputs(lines=[(1, {"scope": "dfo", "target": "1:river:a", "rule_id": None, "verdict": "wrong",
                               "note": "bad"})], places={"1:river:a": p}, waters={})
    got = A.check_B(inp)
    assert len(got) == 1 and A.OLD_EXTENT_FLAG in got[0].found and got[0].key == "B|wrong|1:river:a|"


def test_the_checklist_row_carries_the_flag(monkeypatch):
    import checks
    import dfo
    from pipeline.regs.review import marks as M
    base = {"scope": "dfo", "target": "1:river:b", "answers": {"T": "y", "R": "y", "P": "n"}, "reviewer": "d",
            "date": "2026-11-15", "pdf_sha256": "0" * 64, "problem": "wrong place", "note": "the cut is off",
            "prints": {"text": "t", "records": [{"key": "entry", "meaning": "m", "binding": "b"}]}}
    wrong = M.Mark.model_validate({**base, "verdict": "wrong"})
    a = {"location_id": "1:river:a", "binding": {"extents": [{"op": "whole"}]}}
    b = {"location_id": "1:river:b", "duplicate_of": "1:river:a", "duplicate_confirmed": True, "binding": {}}
    monkeypatch.setattr(dfo, "_locations", lambda: {"1:river:a": ("1", a, None), "1:river:b": ("1", b, None)})
    got = dfo.old_extent(wrong, b)                          # it rides 1:river:a's extent
    assert got == {"verdict": "wrong", "date": "2026-11-15", "flag": A.OLD_EXTENT_FLAG, "rides": "1:river:a"}
    assert dfo.old_extent(wrong, {"location_id": "x", "binding": {}}) is None
    assert checks._matches({"old_extent": got, "check": "checked"}, "old")
    assert not checks._matches({"check": "checked"}, "old")
