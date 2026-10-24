"""THE ANSWERS/2 MODELS (`pipeline/deliver/answers/model/`, DATAFLOW P7): strict, no extra
field, every nullable field null exactly where its model says; the vocabularies are the closed
enums; the JSON Schema and the TypeScript the app reads are generated and current."""
from __future__ import annotations

import json

import pytest

from pipeline.deliver.types import GAME_FISH

from pipeline.deliver import types as T
from pipeline.deliver.answers import model as M

DECIDED = {"status": "keep", "win": 3, "daily": 2, "narrow": None,
           "lines": [{"t": "rel", "r": 3, "a": 0, "b": 30}], "roles": [[3, "governs", None]],
           "lift_notes": []}
ROW = {"kind": "keep", "pool": 3, "win": None, "members": ["RB"], "all_members": ["RB"],
       "daily": 2, "narrow": None, "everyone": [], "groups": [], "prot": None, "wins": None,
       "lift_notes": [], "scope": {"of": "water", "entry": None, "share": False, "apart": False,
                                   "narrow_all": False},                  # rows 31: narrow_all
       "conds": [], "items": [], "real_daily": None, "group": None,
       "elsewhere": {"says": "This river’s own limit.", "ask": []},     # answers 2.5 (rows 6)
       "year": [["keep", 365]]}                                          # rows 30: the year strip
FRAME = {"spp": ["RB"], "fish": {"RB": {"hatchery": DECIDED, "wild": DECIDED, "lead": "hatchery"}}, "rows": [ROW],
         "steelhead_line": None}


def _ok(name, v):
    M.validate_section(name, [v])


def _bad(name, v):
    with pytest.raises(M.ShapeError):
        M.validate_section(name, [v])


def test_a_good_frame_validates():
    _ok("rows", FRAME)
    _ok("display", {"status": "own", "closing": [], "strip": 0})
    _ok("display", {"status": "own", "closing": [], "strip": 4 | 16})        # 2.6: fly + bait ban
    _ok("display", {"status": "closed", "closing": [[7, list(GAME_FISH)]], "closers": [7], "strip": 1})


def test_a_stray_field_a_wrong_type_or_a_word_outside_the_vocabulary_is_refused():
    _bad("rows", {**FRAME, "extra": 1})
    _bad("rows", {**FRAME, "rows": [{**ROW, "daily": "2"}]})                 # strict: no coercion
    _bad("rows", {**FRAME, "rows": [{**ROW, "kind": "nolimit"}]})            # answers/1's word
    _bad("rows", {**FRAME, "fish": {"RB": {"hatchery": {**DECIDED, "lines": [
        {"t": "rel", "r": 3, "a": 0, "b": 30, "take": 1}]}, "wild": None, "lead": "hatchery"}}})  # a line's stray field
    _bad("display", {"status": "open", "closing": [], "strip": 0})
    _bad("display", {"status": "own", "strip": 0})                           # 2.1: closing named
    _bad("display", {"status": "closed", "closing": [[7, ["RB"]]], "closers": [7], "strip": 1})  # a fish left open
    _bad("display", {"status": "own", "closing": [[7, []]], "strip": 0})     # closes nothing
    _bad("display", {"status": "own", "closing": []})                        # 2.6: the strip named
    _bad("display", {"status": "own", "closing": [], "strip": 1})            # closed <=> status
    _bad("display", {"status": "closed", "closing": [[7, list(GAME_FISH)]], "closers": [7], "strip": 1 | 16})
    _bad("display", {"status": "own", "closing": [], "strip": 4 | 8})        # one fly answer
    _bad("display", {"status": "own", "closing": [], "strip": 32})


def test_every_nullable_field_is_null_exactly_where_its_model_says():
    _bad("rows", {**FRAME, "fish": {"RB": {"hatchery": {**DECIDED, "status": "release"},
                                           "wild": None, "lead": "hatchery"}}})  # daily <=> keep
    _bad("rows", {**FRAME, "rows": [{**ROW, "kind": "release", "pool": None, "win": 3,
                                     "scope": None}]})                       # conds on a no-pool row
    _bad("rows", {**FRAME, "rows": [{**ROW, "scope": {"of": "water", "entry": "z1:x", "share": False,
                                                      "apart": False, "narrow_all": False}}]})  # entry <=> not water
    _bad("rows", {**FRAME, "rows": [{**ROW, "items": [{"members": ["RB"], "bands": None,
                                                       "back": False, "xref": False, "sub": None,
                                                       "conds": [], "against": 4}]}]})
    _bad("rows", {**FRAME, "rows": [{**ROW, "elsewhere": None}]})          # 2.5: elsewhere <=> keep
    _bad("rows", {**FRAME, "rows": [{**ROW, "group": {"pool": 3, "total": 2, "kinds": ["RB"],
                                                      "subcaps": [], "says": "Up to 2."}}]})


def test_the_vocabularies_are_the_closed_enums():
    sch = json.dumps(M.json_schemas())
    for e in (T.DecidedStatus, T.Role, T.DisplayKind, T.LossReason):
        for m in e:
            assert json.dumps(m.value) in sch, (e.__name__, m.value)
    assert '"nolimit"' not in sch


def test_the_generated_schema_and_types_are_current():
    assert M.main(["--check"]) == 0
