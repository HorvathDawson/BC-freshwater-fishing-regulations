"""THE OWNER'S OVERNIGHT NOTES, 2026-11-15 (marks 2578, 2583, 2586, 2651): what the review app could not say or got wrong.

  channels      "make sure that when i checked fraser as water for the mainstem items it includes all channels too.
                like sea bird island etc." — an extent of a DFO place naming a water holds on its own side channels
                (`reach.channels`), whoever made it
  tributaries   "this one should be tributaries only?" / "make sure this excludes atnarko" — a DFO mark says
                tributaries only and the tributaries it leaves out; the line of an older mark is the line it was; the
                apply writes the place's `tributaries_only` / `tributary_excludes`
  other_places  Qualicum (refused at the write): "all the other places on this water" takes no water — refused AT THE
                SAVE with what to change, never only later; a saved one still reads
  suggester     "all tributaries of the Bulkley River other than the Suskwa River" is tributaries only, the Suskwa out;
                "X and tributaries" takes the tributaries in; "mainstem" leaves them out

Data-free: stand-in registries and graphs, and DFO places typed here.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline.regs.review import marks as M
from pipeline.regs.review import proposals as P

SHA = "0" * 64
BELLA_TRIBS = "5b:bella-coola-river-not-including-at:all-tributaries-to-the-bella-coola"
#: the place as the curated file held it on 2026-11-15 (region 5b), typed here
BELLA_LOC = {
    "location_id": BELLA_TRIBS, "region": "5b", "region_number": 5, "kind": "water", "precedence": 3,
    "water": "Bella Coola River (not including Atnarko River)", "water_id": "5b:bella-coola-river-not-including-atnarko-river",
    "aliases": [], "section": None, "section_history": [], "inherits": [],
    # the place is built here: its wording fingerprint is this test's own (nothing reads it), never a scraped one
    "fingerprints": [hashlib.sha256(b"All tributaries to the Bella Coola River.").hexdigest()[:16]],
    "source_text": {"waters": "Bella Coola River (not including Atnarko River)",
                    "specific_area": "All tributaries to the Bella Coola River.", "landmarks": [], "op": "tributary_set",
                    "anchor_types": ["confluence"], "excludes": [], "areas": []},
    "status": "active", "duplicate_of": None, "duplicate_score": None, "duplicate_confirmed": None, "locked": False,
    "reviewed_by": "", "reviewed_at": "", "review_note": "",
    "binding": {"extents": [{"op": "whole", "item_id": "gnis:11611"}], "tributaries": True, "tributaries_only": False,
                "tributary_excludes": [], "sections_cached": None, "notes": [], "spatial_caveat": False}}
BELLA_WATER = {"water_id": "5b:bella-coola-river-not-including-atnarko-river", "region": "5b", "region_number": 5,
               "name": "Bella Coola River (not including Atnarko River)", "aliases": [], "sections": [],
               "item_ids": ["gnis:11611"], "tributaries": True, "match": {"status": "matched", "via": "exact name"}}


def _mark(**kw) -> M.Mark:
    base = {"scope": "dfo", "target": BELLA_TRIBS, "verdict": "verified", "answers": {"T": "y", "R": "y", "P": "y"},
            "reviewer": "dawson", "date": "2026-11-15", "pdf_sha256": SHA,
            "prints": {"text": "t", "records": [{"key": "entry", "meaning": "m", "binding": "b"}]}}
    return M.Mark.model_validate({**base, **kw})


def _fix(**kw) -> dict:
    return {"scope": "entry", "op": "whole", "points": [], "items": ["gnis:11611"], "tributaries": True, **kw}


@pytest.fixture()
def src(tmp_path) -> P.Sources:
    dfo = tmp_path / "dfo"
    dfo.mkdir()
    (dfo / "region-5b.json").write_text(json.dumps({"region": "5b", "region_number": 5, "region_name": "5B",
                                                    "structure": {}, "scopes": [], "waters": [BELLA_WATER],
                                                    "locations": [BELLA_LOC]}, indent=1) + "\n")
    (tmp_path / "cat").mkdir()
    (tmp_path / "splits.json").write_text(json.dumps({"waterbodies": []}))
    (tmp_path / "build").mkdir()
    (tmp_path / "build" / "registry.json").write_text(json.dumps({"items": []}))
    return P.Sources(entries_dir=tmp_path / "cat", dfo_dir=dfo, splits_path=tmp_path / "splits.json",
                     build=tmp_path / "build")


# ---------------------------------------------------------------------------- tributaries only / excepted (2583, 2586)

def test_a_dfo_mark_says_tributaries_only_and_the_tributaries_it_leaves_out_and_round_trips():
    m = _mark(extent_fix=[_fix(tributaries_only=True, tributary_excludes=["gnis:17209"])])
    line = M.line(m)
    back = M.Mark.model_validate_json(line)
    assert back.extent_fix[0].tributaries_only is True and back.extent_fix[0].tributary_excludes == ["gnis:17209"]
    assert M.line(back) == line


def test_a_mark_saved_before_the_fields_reads_and_writes_the_line_it_was():
    """2583 as the store holds it: no tributaries_only, no tributary_excludes — the same bytes back, no call made"""
    old = _mark(extent_fix=[_fix()])
    line = M.line(old)
    assert '"tributaries_only"' not in line and '"tributary_excludes"' not in line
    f = M.Mark.model_validate_json(line).extent_fix[0]
    assert f.tributaries_only is None and f.tributary_excludes is None


def test_the_model_refuses_what_cannot_be_meant():
    for bad, why in ((_fix(tributaries=False, tributaries_only=True), "cannot also leave them out"),
                     (_fix(tributaries=False, tributary_excludes=["gnis:17209"]), "walk that takes"),
                     (_fix(tributary_excludes=["gnis:11611"]), "cannot also be an excepted"),
                     (_fix(tributary_excludes=["gnis:1", "gnis:1"]), "each tributary once")):
        with pytest.raises(ValueError, match=why):
            M.ExtentFix.model_validate(bad)
    with pytest.raises(ValueError, match="DFO place's call"):
        M.Mark.model_validate({"scope": "entry", "target": "x", "verdict": "unsure",
                               "answers": {"T": "y", "R": "y", "P": "?", "A": "y"}, "reviewer": "d",
                               "date": "2026-11-15", "pdf_sha256": SHA,
                               "prints": {"text": "t", "records": [{"key": "entry", "meaning": "m", "binding": "b"}]},
                               "extent_fix": [_fix(tributaries_only=True)]})


def test_the_apply_writes_tributaries_only_and_the_excepted_waters_onto_the_place(src):
    m = _mark(extent_fix=[_fix(tributaries_only=True, tributary_excludes=["gnis:17209"])])
    [p] = [p for p in P.build([m], src) if p["kind"] == "extent"]
    assert p["applicable"], p["problems"]
    got = {e["field"]: e["after"] for e in p["edits"]}
    assert got == {"tributaries_only": True, "tributary_excludes": [{"op": "whole", "item_id": "gnis:17209"}]}
    [text] = P.materialize([p], src).values()
    from pipeline.regs.dfo_salmon import entries as E
    b = E.EntryLocation.from_dict(json.loads(text)["locations"][0]).binding
    assert b.tributaries_only is True and [x.item_id for x in b.tributary_excludes] == ["gnis:17209"]
    # once landed, the same mark is applied: no proposal
    src2 = P.Sources(src.entries_dir, src.dfo_dir, src.splits_path, src.build)
    Path(src.dfo_dir / "region-5b.json").write_text(text)
    applied: set = set()
    assert [x for x in P.build([m], src2, applied=applied) if x["kind"] == "extent"] == []
    assert m.key in applied


def test_an_older_mark_makes_no_call_on_them(src):
    """2583 again: its fix names no `tributaries_only`, so the apply leaves the place's as they are (and it is landed)"""
    applied: set = set()
    m = _mark(extent_fix=[_fix()])
    assert [x for x in P.build([m], src, applied=applied) if x["kind"] == "extent"] == []
    assert m.key in applied


def test_the_editor_opens_on_the_places_own_call():
    fixes, why = P.fixes_of_extents([{"op": "whole", "item_id": "gnis:11611"}], True, tributaries_only=True,
                                    tributary_excludes=[{"op": "whole", "item_id": "gnis:17209"}])
    assert not why and fixes[0].tributaries_only is True and fixes[0].tributary_excludes == ["gnis:17209"]
    _f, why = P.fixes_of_extents([{"op": "whole", "item_id": "gnis:11611"}], True,
                                 tributary_excludes=[{"op": "upstream_of", "item_id": "gnis:1", "splits": ["s"]}])
    assert why and "not one water whole" in why[0]


# ---------------------------------------------------------------------------- other_places (2651, Qualicum)

def test_other_places_with_a_water_is_refused_at_the_save_and_a_saved_one_still_reads():
    f = M.ExtentFix.model_validate({"scope": "entry", "op": "other_places", "points": [], "items": ["gnis:27804"]})
    [why] = M.new_fix_problems(f)
    assert "takes no water of its own" in why and "Untick the water(s) (gnis:27804)" in why
    assert M.new_fix_problems(M.ExtentFix.model_validate({"scope": "entry", "op": "other_places", "points": []})) == []


# ---------------------------------------------------------------------------- a named water's own channels (2578)

def _world():
    """the Fraser (F: f1 -> f2 -> f3), Maria Slough (S: leaves at f1, rejoins at f3), a lake on the line (L: f3 -> l
    -> f4), and a tributary (T: flows into f2)"""
    edges, down, up = [], {}, {}

    def link(a, b):
        edges.append(SimpleNamespace(from_node=a, to_node=b))
        down.setdefault(a, []).append(len(edges) - 1)
        up.setdefault(b, []).append(len(edges) - 1)
    for a, b in (("f1", "f2"), ("f2", "f3"), ("f1", "s1"), ("s1", "f3"), ("f3", "l1"), ("l1", "f4"), ("t1", "f2")):
        link(a, b)
    nodes = {n: SimpleNamespace(length_m=1000.0) for n in ("f1", "f2", "f3", "f4", "s1", "l1", "t1")}
    g = SimpleNamespace(nodes=nodes, edges=edges, down_adj=down, up_adj=up)
    reg = {"gnis:39325": SimpleNamespace(section_ids=("f1", "f2", "f3", "f4"), kind="stream"),
           "gnis:13499": SimpleNamespace(section_ids=("s1",), kind="stream"),
           "wbk:1": SimpleNamespace(section_ids=("l1",), kind="lake"),
           "gnis:9": SimpleNamespace(section_ids=("t1",), kind="stream"),
           "area:region:2": SimpleNamespace(section_ids=("f1", "s1"), kind="area")}
    return reg, g


def test_a_named_water_takes_its_own_side_channels_never_a_lake_or_a_tributary():
    from pipeline.atlas.reach import channels as CH
    reg, g = _world()
    assert CH.side_channels(reg, g, "gnis:39325") == ("gnis:13499",)
    among = ["gnis:39325", "gnis:13499", "wbk:1", "gnis:9"]
    assert CH.own_channels(reg, g, ["gnis:39325"], among) == ["gnis:13499"]
    # a channel the curated water leaves out stays out
    assert CH.own_channels(reg, g, ["gnis:39325"], ["gnis:39325"]) == []
    ex = {"op": "between", "splits": ["a", "b"], "item_id": "gnis:39325"}
    assert CH.with_own_channels(ex, reg, g, among) == {"op": "between", "splits": ["a", "b"],
                                                       "item_ids": ["gnis:39325", "gnis:13499"]}
    assert CH.with_own_channels({"op": "whole"}, reg, g, among) == {"op": "whole"}


def test_only_a_dfo_place_and_only_under_the_policy(monkeypatch):
    from pipeline.atlas.reach import channels as CH
    reg, g = _world()
    rule = {"rule_id": "r", "extents": [{"op": "whole", "item_id": "gnis:39325"}]}
    dfo = {"entry_id": "e", "matched": ["gnis:39325", "gnis:13499"], CH.CHANNELS_KEY: True}
    assert CH.expand_rule(dfo, rule, reg, g)["extents"][0]["item_ids"] == ["gnis:39325", "gnis:13499"]
    assert CH.expand_rule({**dfo, CH.CHANNELS_KEY: False}, rule, reg, g) is None       # a regulation: unchanged
    monkeypatch.setattr(CH, "DFO_NAMED_WATER_TAKES_ITS_CHANNELS", False)                 # the mutation pin
    assert CH.expand_rule(dfo, rule, reg, g) is None


def test_the_dfo_reach_input_carries_the_key():
    from pipeline.atlas.reach import channels as CH
    from pipeline.regs.dfo_salmon import entries as E
    entry, _rules = E.to_reach_input(E.EntryLocation.from_dict(BELLA_LOC), [{}], E.WaterBinding(
        **{k: v for k, v in BELLA_WATER.items() if k in {f.name for f in __import__("dataclasses").fields(E.WaterBinding)}}))
    assert entry[CH.CHANNELS_KEY] is True


# ---------------------------------------------------------------------------- the suggester (Bulkley, Bella Coola)

class _Look:
    """the registry by name: Suskwa / Morice / Two Mile Creek as Bulkley tributaries, a second Two Mile Creek elsewhere"""
    HITS = {"suskwa river": [{"id": "gnis:2637", "wsc": ["400-431358-585806-151208"]}],
            "morice river": [{"id": "gnis:14918", "wsc": ["400-431358-585806-929401"]}],
            "two mile creek": [{"id": "gnis:30327", "wsc": ["400-431358-585806-076543"]},
                               {"id": "gnis:999", "wsc": ["900-111111"]}],
            "atnarko river": [{"id": "gnis:17209", "wsc": ["910-920000-180000"]}]}

    def stream(self, name):
        return self.HITS.get(name.lower(), [])


def _water(name="Bulkley River", iid="gnis:2195", wsc="400-431358-585806"):
    from pipeline.regs.dfo_salmon.suggest import Water
    return Water([iid], {iid: name}, {iid: [wsc]}, {iid: "stream"})


def test_all_tributaries_other_than_named_waters_is_tributaries_only_with_them_out():
    from pipeline.regs.dfo_salmon import suggest as S
    tf = S.tributary_form("all tributaries of the Bulkley River other than Morice River and tributaries, Suskwa River "
                          "and tributaries, and Two Mile Creek", "Bulkley River", _water(), _Look())
    assert tf["only"] and tf["names"] == ["Morice River", "Suskwa River", "Two Mile Creek"]
    assert tf["excepted"] == ["gnis:14918", "gnis:2637", "gnis:30327"] and not tf["unresolved"]
    bella = S.tributary_form("All tributaries to the Bella Coola River.", "Bella Coola River (not including Atnarko River)",
                             _water("Bella Coola River", "gnis:11611", "910-920000"), _Look())
    assert bella["only"] and bella["excepted"] == ["gnis:17209"]


def test_with_and_without_words_and_what_is_never_a_tributary_left_out():
    from pipeline.regs.dfo_salmon import suggest as S
    w, look = _water(), _Look()
    assert S.tributary_form("", "Alouette River and tributaries", w, look)["with"]
    assert S.tributary_form("Tatshenshini mainstem and all other tributaries.", "", w, look)["with"]
    got = S.tributary_form("Mainstem waters of the Fraser River", "Fraser River", w, look)
    assert got["without"] and not got["with"]
    assert S.tributary_form("", "Somass River tributaries", w, look)["only"]
    # an exception that is not a water: never a tributary left out
    for t in ("downstream of the weir (excluding the Morrison Creek closure below).",
              "Region 5 not including the Fraser River watershed", "including tributaries, except in the area described below"):
        assert S.tributary_form(t, "", w, look)["names"] == [], t


def test_a_suggestion_carries_the_call_and_a_name_the_data_does_not_pin_is_low():
    from pipeline.regs.dfo_salmon import suggest as S
    loc = {"location_id": "6:bulkley-river:all-tributaries-of-the-bulkley-riv", "water": "Bulkley River",
           "source_text": {"waters": "Bulkley River",
                           "specific_area": "all tributaries of the Bulkley River other than the Suskwa River."},
           "binding": {"extents": []}}
    r = S.suggest_one(loc, _water(), [], _Look())
    sg = r["suggestion"]
    assert (sg["op"], sg["tributaries"], sg["tributaries_only"], sg["tributary_excludes"]) == ("whole", True, True,
                                                                                            ["gnis:2637"])
    loc2 = {**loc, "source_text": {**loc["source_text"], "specific_area": "all tributaries of the Bulkley River other "
                                                                         "than Nowhere Creek."}}
    r2 = S.suggest_one(loc2, _water(), [], _Look())
    assert r2["confidence"] == "low" and "NOT PINNED" in r2["reason"]
    # "X and tributaries" in the heading: tributaries in
    loc3 = {"location_id": "x", "source_text": {"waters": "Alouette River and tributaries", "specific_area": ""},
            "binding": {"extents": []}}
    assert S.suggest_one(loc3, _water("Alouette River", "gnis:1"), [], _Look())["suggestion"]["tributaries"] is True


def test_a_bound_place_whose_call_differs_from_the_words_is_offered_a_suggestion():
    from pipeline.regs.dfo_salmon import suggest as S
    w = _water("Bella Coola River", "gnis:11611", "910-920000")
    assert S.tributary_form_differs(BELLA_LOC, w, _Look())
    done = {**BELLA_LOC, "binding": {**BELLA_LOC["binding"], "tributaries_only": True,
                                     "tributary_excludes": [{"op": "whole", "item_id": "gnis:17209"}]}}
    assert not S.tributary_form_differs(done, w, _Look())


def test_a_bound_places_suggestion_is_its_own_extent_with_dfos_call():
    from pipeline.regs.dfo_salmon import suggest as S
    r = S.bound_row(BELLA_LOC, _water("Bella Coola River", "gnis:11611", "910-920000"), _Look())
    sg = r["suggestion"]
    assert (sg["op"], sg["items"], sg["tributaries"], sg["tributaries_only"], sg["tributary_excludes"]) == (
        "whole", ["gnis:11611"], True, True, ["gnis:17209"])
    assert "tributaries only" in r["reason"]


# ---------------------------------------------------------------------------- the WATER's tributary signal (owner 2026-11-15)

def test_every_place_under_a_water_whose_heading_takes_tributaries_takes_them_unless_its_words_narrow_it():
    """"need to make sure all rules under a water with (includes tributaries) in the name gets tributaries too": the
    water's heading ("and tributaries") or DFO's "(including tributaries)" qualifier (the water's flag) is inherited by
    every place under it; a place whose own words say main stem or tributaries only keeps its own meaning, listed as a
    conflict for the owner"""
    from pipeline.regs.dfo_salmon import suggest as S
    w, look = _water("Kitsumkalum River", "gnis:3942", "400-1"), _Look()
    plain = S.tributary_form("downstream of the railway bridge", "Kitsumkalum River", w, look, water_flag=True)
    assert plain["water_says"] and plain["with"] and not plain["conflict"]
    assert S.tributary_form("downstream of the railway bridge", "Alouette River and tributaries", w, look)["with"]
    main = S.tributary_form("Mainstem waters downstream of the bridge", "Nass River", w, look, water_flag=True)
    assert main["without"] and not main["with"] and "main stem" in main["conflict"]
    only = S.tributary_form("All tributaries to the Bella Coola River.", "Bella Coola River", w, look, water_flag=True)
    assert only["only"] and "tributaries only" in only["conflict"]
    # no signal: nothing said
    assert not S.tributary_form("downstream of the bridge", "Kitsumkalum River", w, look)["with"]
    loc = {"location_id": "x", "source_text": {"waters": "Kitsumkalum River", "specific_area": "downstream of the bridge"},
           "binding": {"extents": []}}
    r = S.with_tributary_form({"suggestion": {"op": "downstream_of", "tributaries": None}, "reason": ""}, loc, w, look,
                              water_flag=True)
    assert r["suggestion"]["tributaries"] is True and "tributary_conflict" not in r
    loc_m = {**loc, "source_text": {**loc["source_text"], "specific_area": "mainstem waters downstream of the bridge"}}
    r = S.with_tributary_form({"suggestion": {"op": "downstream_of", "tributaries": None}, "reason": ""}, loc_m, w, look,
                              water_flag=True)
    assert r["suggestion"]["tributaries"] is False and "CONFLICT" in r["reason"] and r["tributary_conflict"]


# ---------------------------------------------------------------------------- the binding: tributaries only leaves the water out

@pytest.fixture(scope="module")
def real(request):
    import sys
    from pipeline.tests import under_test
    from pipeline.tests.conftest import ATLAS_HINT, need
    need(request, "atlas", under_test.atlas(request) / "graph.pkl", ATLAS_HINT)
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "curation-review/backend"))
    import reuse
    return reuse._graph(), reuse._registry()


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_bella_coola_tributaries_only_holds_none_of_the_mainstem_and_atnarko_is_left_out(real):
    """(data lane 2026-11-15) mark 2583 saved as the whole Bella Coola with its tributaries covers the mainstem, which
    then reads closed where DFO gives chinook 4 a day. Tributaries only, with the Atnarko excepted, holds no section of
    the Bella Coola and none of the Atnarko."""
    from pipeline.atlas.reach.build import build_reach
    from pipeline.regs.dfo_salmon import entries as E
    g, reg = real
    loc = {**BELLA_LOC, "binding": {**BELLA_LOC["binding"], "tributaries_only": True,
                                    "tributary_excludes": [{"op": "whole", "item_id": "gnis:17209"}]}}
    import dataclasses
    water = E.WaterBinding(**{k: v for k, v in BELLA_WATER.items() if k in {f.name for f in dataclasses.fields(E.WaterBinding)}})
    entry, rules = E.to_reach_input(E.EntryLocation.from_dict(loc), [{}], water)
    b, _ = build_reach(entry, rules[0], reg, g)
    got = set(b.sections)
    assert got, "tributaries only bound nothing"
    assert not got & set(reg["gnis:11611"].section_ids)            # the Bella Coola itself: out
    assert not got & set(reg["gnis:17209"].section_ids)            # the Atnarko: left out
    whole, _ = build_reach(*[x if i == 0 else x[0] for i, x in enumerate(E.to_reach_input(
        E.EntryLocation.from_dict(BELLA_LOC), [{}], water))], reg, g)
    assert set(whole.sections) & set(reg["gnis:11611"].section_ids)   # the mark as saved: it covers the mainstem


# ---------------------------------------------------------------------------- mouth / tidal ends (owner 2026-11-15)

class _Snap(_Look):
    """snaps every hunted point onto one line at a fixed measure"""
    def __init__(self, blk, m):
        self.blk, self.m = blk, m

    def snap(self, lonlat, on_items):
        return {"item_id": on_items[0], "coord": list(lonlat), "blk": self.blk, "measure": self.m, "distance_m": 1.0}


def _hunt(op, ends, cut):
    return {"op": op, "ends": ends, "notes": "n"}, {"c1": {"anchor": "sign", "name": "signs", "lon": -123.7, "lat": 49.4,
                                                          "confidence": "high", "description": "d", **cut}}


@pytest.mark.parametrize("where,m,boundary", [("Chapman Creek", 176.9, ("chapman_creek__tidal_boundary", 180.5)),
                                              ("Stave River", 384.0, ("stave_river__cpr_bridge_tidal_boundary", 384.7))])
def test_a_hunted_point_on_an_existing_tidal_boundary_is_that_cut_never_a_new_one(where, m, boundary):
    """"make sure all the mouth/ tidal splits are at the correct one (not the proposed one but at the tidal ones we give
    every stream)": Chapman 3.6 m, Stave 0.7 m off their DFO tidal boundaries"""
    from pipeline.regs.dfo_salmon import suggest as S
    entry, cuts = _hunt("upstream_of", [{"cut": "c1"}], {})
    row = {"endpoints": [], "matched": "none", "suggestion": None}
    got = S.apply_hunt(row, entry, cuts, _water(where, "gnis:1", "900-1"), _Snap("360857589", m),
                       existing=lambda blk, mm: S.nearest_cut([(boundary[1], boundary[0])], mm))
    sg = got["suggestion"]
    assert sg["points"] == [{"kind": "split", "split_id": boundary[0]}] and sg["new_splits"] == []
    assert "no new cut" in got["reason"]


@pytest.mark.parametrize("where", ["Illiance River", "Kitsault River", "Wilauks Creek"])
def test_a_hunted_point_on_the_lines_end_is_the_mouth_an_open_end(where):
    """"upstream of white triangular fishing boundary signs near the mouth": the hunt placed a NEW cut at m 0, the
    line's end (a cut there breaks both halves). It is the whole river now, trimmed at its tidal boundary by the run."""
    from pipeline.regs.dfo_salmon import suggest as S
    entry, cuts = _hunt("upstream_of", [{"cut": "c1"}], {})
    got = S.apply_hunt({"endpoints": [], "matched": "none", "suggestion": None}, entry, cuts,
                       _water(where, "gnis:2", "900-2"), _Snap("360867921", 0.0), existing=lambda *_: None)
    sg = got["suggestion"]
    assert (sg["op"], sg["points"], sg["new_splits"]) == ("whole", [], [])
    assert "is the mouth" in got["reason"]
    # "from A down to the mouth": downstream of A
    entry2 = {"op": "between", "ends": [{"split": "a_cut"}, {"cut": "c1"}], "notes": "n"}
    got2 = S.apply_hunt({"endpoints": [], "matched": "none", "suggestion": None}, entry2, cuts,
                        _water(where, "gnis:2", "900-2"), _Snap("360867921", 0.0), existing=lambda *_: None)
    assert (got2["suggestion"]["op"], got2["suggestion"]["points"]) == ("downstream_of",
                                                                        [{"kind": "split", "split_id": "a_cut"}])


def test_a_bound_places_row_keeps_its_own_extent_and_carries_the_hunt_as_evidence():
    """(CI 2026-11-15, Stave) a bound place offered its tributary call still carries the split hunt (every hunted place
    on the queue does, `test_dfo_suggest`), and the hunt's extent is not put over the curated one"""
    from pipeline.regs.dfo_salmon import suggest as S
    w = _water("Bella Coola River", "gnis:11611", "910-920000")
    row = S.bound_row(BELLA_LOC, w, _Look())
    entry, cuts = _hunt("upstream_of", [{"cut": "c1"}], {})
    got = S.with_hunt(row, entry, cuts, w, _Snap("1", 500.0), bound=True, existing=lambda *_: None)
    assert got["hunt"]["status"] == "found" and got["suggestion"] == row["suggestion"]
    unbound = S.with_hunt({"endpoints": [], "matched": "none", "suggestion": None}, entry, cuts, w, _Snap("1", 500.0),
                          bound=False, existing=lambda *_: None)
    assert unbound["suggestion"]["op"] == "upstream_of" and unbound["hunt"]["status"] == "found"


def test_one_named_tributary_is_not_the_tributary_walk():
    """Stave: "… including its tributary containing the fishway" names one stream; it does not take the tributaries in"""
    from pipeline.regs.dfo_salmon import suggest as S
    w = _water()
    assert not S.tributary_form("downstream of the dam, including its tributary containing the fishway", "Stave River",
                                w, _Look())["with"]
    assert S.tributary_form("downstream of the dam, including its tributaries", "Stave River", w, _Look())["with"]
