"""THE ANSWERS/2 MODELS (`pipeline/deliver/answers/model.py`, DATAFLOW P7): strict, no extra
field, every nullable field null exactly where its model says; the vocabularies are the closed
enums; the JSON Schema and the TypeScript the app reads are generated and current."""
from __future__ import annotations

import json

import pytest

from pipeline.deliver import types as T
from pipeline.deliver.answers import model as M

DECIDED = {"status": "keep", "win": 3, "daily": 2, "narrow": None,
           "lines": [{"t": "rel", "r": 3, "a": 0, "b": 30}], "roles": [[3, "governs", None]],
           "lift_notes": []}
ROW = {"kind": "keep", "pool": 3, "win": None, "members": ["RB"], "all_members": ["RB"],
       "daily": 2, "narrow": None, "everyone": [], "groups": [], "prot": None, "wins": None,
       "lift_notes": [], "scope": {"of": "water", "entry": None, "share": False, "apart": False},
       "conds": [], "items": [], "real_daily": None}
FRAME = {"spp": ["RB"], "fish": {"RB": {"hatchery": DECIDED, "wild": DECIDED}}, "rows": [ROW],
         "steelhead_line": None}


def _ok(name, v):
    M.validate_section(name, [v])


def _bad(name, v):
    with pytest.raises(M.ShapeError):
        M.validate_section(name, [v])


def test_a_good_frame_validates():
    _ok("rows", FRAME)
    _ok("display", {"status": "own"})


def test_a_stray_field_a_wrong_type_or_a_word_outside_the_vocabulary_is_refused():
    _bad("rows", {**FRAME, "extra": 1})
    _bad("rows", {**FRAME, "rows": [{**ROW, "daily": "2"}]})                 # strict: no coercion
    _bad("rows", {**FRAME, "rows": [{**ROW, "kind": "nolimit"}]})            # answers/1's word
    _bad("rows", {**FRAME, "fish": {"RB": {"hatchery": {**DECIDED, "lines": [
        {"t": "rel", "r": 3, "a": 0, "b": 30, "take": 1}]}, "wild": None}}})  # a line's stray field
    _bad("display", {"status": "open"})


def test_every_nullable_field_is_null_exactly_where_its_model_says():
    _bad("rows", {**FRAME, "fish": {"RB": {"hatchery": {**DECIDED, "status": "release"},
                                           "wild": None}}})                  # daily <=> keep
    _bad("rows", {**FRAME, "rows": [{**ROW, "kind": "release", "pool": None, "win": 3,
                                     "scope": None}]})                       # conds on a no-pool row
    _bad("rows", {**FRAME, "rows": [{**ROW, "scope": {"of": "water", "entry": "z1:x", "share": False,
                                                      "apart": False}}]})    # entry <=> not water
    _bad("rows", {**FRAME, "rows": [{**ROW, "items": [{"members": ["RB"], "bands": None,
                                                       "back": False, "xref": False, "sub": None,
                                                       "conds": [], "against": 4}]}]})


def test_the_vocabularies_are_the_closed_enums():
    sch = json.dumps(M.json_schemas())
    for e in (T.DecidedStatus, T.Role, T.DisplayKind, T.LossReason):
        for m in e:
            assert json.dumps(m.value) in sch, (e.__name__, m.value)
    assert '"nolimit"' not in sch


def test_the_generated_schema_and_types_are_current():
    assert M.main(["--check"]) == 0
