"""THE FOUR ANSWERS THE APP KEYED AS EXCLUSIONS (owner rule 2026-10-21: the pipeline answers, the app only reads) —
no data, CI's no-data tier. The live twin, against the golden oracle, is `test_answers_pipeline_needs_live.py`.

  1. licence 7: the steelhead stamp leaves the documents where the card says steelhead are closed
     (`stamps.drop_closed_steelhead_stamp`; the prototype's round 20i O3 render-time drop, v33 UR4 fix 7)
  2. licence 7: an under-16's `buy` document carries its alternatives, `or` [{need, names}] (`licence.under_16_summary`)
  3. the release statics carry the printed -> PDF page map (`store.common.export_subset` `pdf_pages`, the export
     guide's, `pipeline.common.synopsis_pages`)
  4. rows 32: every row carries the documents its fish need by its answer, `stamps` (`stamps.row_stamps`; the
     prototype's round UR7 stampsFor on an Other fish row)

Every expected value is written out whole, from the prototype's rule — never read back from the implementation.
"""
from __future__ import annotations

import copy
import json

import pytest

from pipeline.deliver.answers import licence as LI
from pipeline.deliver.answers import model as M
from pipeline.deliver.answers import stamps as ST
from pipeline.deliver.answers.common import AnswersError

NAMES = {"steelhead_stamp": "Steelhead Conservation Surcharge Stamp", "basic_licence": "basic angling licence",
         "salmon_stamp": "Conservation Surcharge Stamp for salmon", "yukon_licence": "Yukon angling licence",
         "white_sturgeon_licence": "White Sturgeon Conservation Licence",
         "kootenay_rb_stamp": "Conservation Surcharge Stamp for Kootenay Lake rainbow trout"}
BASIC = {"doc": "basic_licence", "when": {"act": "fishing"}, "base": True, "prices": {"year": 41.15}}
ST_DOC = {"doc": "steelhead_stamp", "when": {"act": "targeting", "species": ["ST"]}, "base": False,
          "prices": {"year": 26.29}}
SALMON = {"doc": "salmon_stamp", "when": {"act": "retaining", "species": ["CH", "CO"]}, "base": False,
          "prices": {"year": 17.14}}
WSG_DOC = {"doc": "white_sturgeon_licence", "when": {"act": "targeting", "species": ["WSG"]}, "base": False,
           "prices": {"year": 28.57}}
KLRB_DOC = {"doc": "kootenay_rb_stamp", "when": {"act": "retaining", "species": ["RB"], "lengths": [{"min_cm": 50}]},
            "base": False, "prices": {"year": 17.14}}


def _closed_st_group():
    return {"headline": "Trout and char", "tree": [{"node": "tier", "children": [
        {"node": "kind", "species": ["CT"], "n": 2, "sizes": {"cm": [[0, None]], "in": []}}]}],
            "not_kept": [{"species": ["ST"], "status": "closed"}]}


def _bc_frame():
    """The Bella Coola's card (bc1_501's shape): a trout group whose not-kept line closes steelhead, a salmon row."""
    return {"rows": [{"kind": "keep", "members": ["CT", "DV"], "group": _closed_st_group()},
                     {"kind": "keep", "members": ["CH", "CO"], "group": None}]}


def _lic(*profiles):
    return {"holds": {"holds": [1]}, "profiles": [{"documents": list(p), "none_needed": False, "requirements": []}
                                                  for p in profiles]}


# 1. the steelhead stamp where steelhead are closed ---------------------------------------------------------------
def test_1_the_stamp_leaves_every_profiles_documents_where_the_card_closes_steelhead():
    rows, lic = _bc_frame(), _lic([BASIC, ST_DOC, SALMON], [BASIC], [])
    before = copy.deepcopy(lic)
    [got] = ST.drop_closed_steelhead_stamp([rows], [lic])
    assert lic == before                                                              # the input is untouched
    assert got == {"holds": {"holds": [1]}, "profiles": [
        {"documents": [BASIC, SALMON], "none_needed": False, "requirements": []},
        {"documents": [BASIC], "none_needed": False, "requirements": []},
        {"documents": [], "none_needed": False, "requirements": []}]}
    assert got["profiles"][1] is lic["profiles"][1]                                   # unchanged profiles are shared


@pytest.mark.parametrize("why,mutate", [
    # MUTATION PINS: each takes one leg of the condition away, and the stamp must stay
    ("steelhead released, not closed", lambda f: f["rows"][0]["group"]["not_kept"][0].update(status="release")),
    ("a released steelhead line beside it", lambda f: f["rows"][0]["group"]["not_kept"].append(
        {"species": ["ST", "DV"], "status": "release"})),
    ("a kind keeps steelhead", lambda f: f["rows"][0]["group"]["tree"][0]["children"].append(
        {"node": "kind", "species": ["ST"], "n": 1, "sizes": {"cm": [], "in": []}})),
    ("a row answers keep for steelhead", lambda f: f["rows"].append({"kind": "keep", "members": ["ST"], "group": None})),
    ("a row answers release for steelhead", lambda f: f["rows"].append(
        {"kind": "release", "members": ["ST", "DV"], "group": None})),
    ("no group closes steelhead (the prototype read only a group's not-kept line)",
     lambda f: f["rows"][0]["group"].update(not_kept=[])),
])
def test_1_the_stamp_stays_wherever_steelhead_may_be_fished(why, mutate):
    rows, lic = _bc_frame(), _lic([BASIC, ST_DOC])
    mutate(rows)
    assert ST.drop_closed_steelhead_stamp([rows], [lic])[0] is lic, why


def test_1_a_closed_steelhead_row_beside_the_group_still_drops_it():
    rows, lic = _bc_frame(), _lic([BASIC, ST_DOC])
    rows["rows"].append({"kind": "closed", "members": ["ST"], "group": None})       # closed: never fished
    assert ST.drop_closed_steelhead_stamp([rows], [lic])[0]["profiles"][0]["documents"] == [BASIC]


def test_1_a_stamp_needed_to_fish_at_all_in_a_steelhead_period_is_the_waters_and_stays():
    babine = {**ST_DOC, "when": {"act": "fishing", "on": "steelhead_period"},
              "also_when": [{"act": "targeting", "species": ["ST"]}]}
    lic = _lic([BASIC, babine])
    assert ST.drop_closed_steelhead_stamp([_bc_frame()], [lic])[0] is lic
    # a stamp named for more fish than steelhead is not steelhead's alone
    lic2 = _lic([BASIC, {**ST_DOC, "when": {"act": "targeting", "species": ["ST", "RB"]}}])
    assert ST.drop_closed_steelhead_stamp([_bc_frame()], [lic2])[0] is lic2
    # a narrower need that is not steelhead's keeps it (Babine's order, reversed: to fish for steelhead, and at all in
    # the period)
    lic3 = _lic([BASIC, {**ST_DOC, "also_when": [{"act": "fishing", "on": "steelhead_period"}]}])
    assert ST.drop_closed_steelhead_stamp([_bc_frame()], [lic3])[0] is lic3
    # every need steelhead's: to fish for them, and to keep them — left out
    keep_too = {**ST_DOC, "also_when": [{"act": "retaining", "species": ["ST"]}]}
    assert ST.drop_closed_steelhead_stamp([_bc_frame()], [_lic([BASIC, keep_too])])[0]["profiles"][0]["documents"] == [BASIC]


def test_1_tidal_and_misaligned():
    tidal = {"holds": {"tidal": {"tidal": True}}, "profiles": [{"tidal": True}]}
    assert ST.drop_closed_steelhead_stamp([_bc_frame()], [tidal])[0] is tidal
    with pytest.raises(AnswersError):
        ST.drop_closed_steelhead_stamp([_bc_frame()], [])
    assert ST.steelhead_closed(_bc_frame()) is True
    assert ST.steelhead_closed({"rows": []}) is False


# 2. an under-16's document with its alternatives -----------------------------------------------------------------
def _ans(none, docs=(), paths=None):
    reqs = [{"req": 98, "when": {"act": "fishing"}, "paths": paths}] if paths else []
    return {"documents": list(docs), "none_needed": none, "requirements": reqs}


def _youth_answers(adult_docs):
    P = LI.profiles()
    ix = {(p["residency"], p["age"]): i for i, p in enumerate(P)
          if p["guidance"] == "non_guided" and p["status"] == "none"}
    youth = [{"accompanied_by": {"holding": "what_this_fishing_requires", "who": {"age": ["16_plus"]}},
              "quota": "counts_to_companion"},
             {"as": {"age": ["16_plus"], "residency": ["non_resident", "non_resident_alien"]}, "quota": "own"}]
    answers = [_ans(True)] * len(P)
    for res in ("non_resident", "non_resident_alien"):
        answers[ix[(res, "16_plus")]] = _ans(False, adult_docs)
        answers[ix[(res, "under_16")]] = _ans(False, paths=youth)
    return answers


def test_2_a_youths_buy_carries_the_16_plus_documents_alternatives():
    teslin = {**BASIC, "prices": {"year": 62.87}, "or": [{"need": ["yukon_licence"], "alt": 7, "prices": {}}]}
    got = LI.under_16_summary(_youth_answers([teslin, SALMON]), NAMES)
    assert [u["buy"] for u in got] == [[], [
        {"doc": "basic_licence", "name": "Basic angling licence", "prices": {"year": 62.87},
         "or": [{"need": ["yukon_licence"], "names": ["Yukon angling licence"]}]}], [
        {"doc": "basic_licence", "name": "Basic angling licence", "prices": {"year": 62.87},
         "or": [{"need": ["yukon_licence"], "names": ["Yukon angling licence"]}]}]]
    _v(M.UnderSixteen, got[1])                                                        # the entry is its model


def test_2_no_alternatives_no_or():
    got = LI.under_16_summary(_youth_answers([{**BASIC, "prices": {"year": 62.87}}]), NAMES)
    assert got[1]["buy"] == [{"doc": "basic_licence", "name": "Basic angling licence", "prices": {"year": 62.87}}]


def _v(model, x):
    return model.model_validate_json(json.dumps(x))


def test_2_the_model_holds_an_alternative_to_its_names():
    buy = {"doc": "basic_licence", "name": "Basic angling licence", "prices": {}}
    _v(M.BuyDoc, {**buy, "or": [{"need": ["a", "b"], "names": ["A", "B"]}]})
    for bad in ({**buy, "or": []}, {**buy, "or": [{"need": ["a", "b"], "names": ["A"]}]},
                {**buy, "or": [{"need": [], "names": []}]}):
        with pytest.raises(Exception):
            _v(M.BuyDoc, bad)


# 3. the printed -> PDF page map in the store's statics -----------------------------------------------------------
def test_3_the_store_carries_the_export_guides_page_map_whole():
    from pipeline.deliver.store.common import EXPORT_STATIC, export_subset
    assert "pdf_pages" in EXPORT_STATIC
    X = {"rules": {}, "licensing": {}, "entries": {}, "licences": {}, "species": {}, "rulesets": {},
         "licensing_sets": {}, "waters": {},
         "guide": {"gear": {"conduct": {"acts": {}}}, "pdf_pages": {"4": 6, "40": 42, "41": 47, "80": 86}}}
    assert export_subset(X)["pdf_pages"] == {"4": 6, "40": 42, "41": 47, "80": 86}


# 4. rows 32: the documents a row's fish need ---------------------------------------------------------------------
WSG_STAMP = {"doc": "white_sturgeon_licence", "act": "targeting", "name": "White Sturgeon Conservation Licence",
             "when": "Only if you fish for white sturgeon, even to release them"}
KLRB_STAMP = {"doc": "kootenay_rb_stamp", "act": "retaining",
              "name": "Conservation Surcharge Stamp for Kootenay Lake rainbow trout",
              "when": "Only if you keep rainbow trout over 50 cm"}


def test_4_every_row_carries_the_documents_its_fish_need_by_its_answer():
    lic = _lic([BASIC, WSG_DOC, KLRB_DOC, ST_DOC])
    frame = {"rows": [{"kind": "release", "members": ["WSG"], "group": None},          # vedder: white sturgeon release
                      {"kind": "keep", "members": ["RB"], "group": None, "daily": 10},  # klmb: rainbow trout keep 10
                      {"kind": "release", "members": ["RB"], "group": None},            # released: no keep document
                      {"kind": "closed", "members": ["WSG", "ST"], "group": None},      # closed: never fished
                      {"kind": "no_limit", "members": ["YP"], "group": None}]}          # no document names it
    before = copy.deepcopy(frame)
    [got] = ST.stamp_licence_documents([frame], [lic], NAMES, 0)
    assert frame == before
    assert [r.get("stamps") for r in got["rows"]] == [[WSG_STAMP], [KLRB_STAMP], None, None, None]
    assert got["rows"][2] is frame["rows"][2]                                         # unchanged rows are shared
    assert "stamps" not in got["rows"][2]


def test_4_a_row_holding_steelhead_needs_the_stamp_to_fish_for_them():
    lic = _lic([BASIC, ST_DOC])
    [got] = ST.stamp_licence_documents([{"rows": [{"kind": "keep", "members": ["GB", "CT", "RB", "ST"], "group": None}]}],
                                       [lic], NAMES, 0)
    assert got["rows"][0]["stamps"] == [{"doc": "steelhead_stamp", "act": "targeting",
                                         "name": "Steelhead Conservation Surcharge Stamp",
                                         "when": "Only if you fish for steelhead, even to release them"}]
    with pytest.raises(AnswersError):                                                 # never stamped twice
        ST.stamp_licence_documents([got], [lic], NAMES, 0)


def test_4_the_model_holds_a_rows_stamps_to_its_answer():
    row = json.loads(json.dumps(_ROW))
    _v(M.Row, {**row, "stamps": [KLRB_STAMP, WSG_STAMP]})
    rel = {**row, "kind": "release", "pool": None, "win": 3, "daily": None, "scope": None, "conds": None,
           "items": None, "elsewhere": None}
    _v(M.Row, {**rel, "stamps": [WSG_STAMP]})
    for bad in ({**rel, "stamps": [KLRB_STAMP]},                                      # a release row keeps none
                {**rel, "kind": "closed", "stamps": [WSG_STAMP]},                     # a closed row is never fished
                {**row, "stamps": []}):                                               # absent, never empty
        with pytest.raises(Exception):
            _v(M.Row, bad)


# a keep row as the model holds one (the smallest one that validates)
_ROW = {"kind": "keep", "pool": 1, "win": None, "members": ["RB"], "all_members": ["RB"], "daily": 10, "narrow": None,
        "everyone": [], "groups": [], "prot": None, "wins": None, "lift_notes": [],
        "scope": {"of": "water", "entry": None, "share": False, "apart": False, "narrow_all": False},
        "conds": [], "items": [], "elsewhere": {"says": "x", "ask": []}, "year": [["keep", 365]]}


# the versions ------------------------------------------------------------------------------------------------------
def test_the_versions_say_what_changed():
    from pipeline.deliver.answers import answers as A
    from pipeline.deliver.answers.encode import SPEC
    v = {s.name: s.version for s in A.SECTIONS}
    assert (v["rows"], v["licence"]) == (32, 7)
    assert (SPEC["sections"]["rows"]["version"], SPEC["sections"]["licence"]["version"]) == ("32", "7")
