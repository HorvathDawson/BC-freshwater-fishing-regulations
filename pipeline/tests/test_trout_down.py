"""(T892) A TROUT ROW SET BY ANOTHER WATER'S TRIBUTARY RULE SAYS SO: `TRow.down` (`answers/trout/down.py`), its words
(`trout/words.py` `row_down`), its shape (`model/trout.py`), J's read-back (`trout/check.py` `down_problems`) and the
build's wiring (`answers._trout_compose_key`).

Anchor (b) of the test rules: hand cases, each the T892 reviews' case in miniature (the app's `flow.test.ts` holds the same
cases on its side; `app/tools/release-tributary-lines.test.ts` holds the two to each other over a whole release).
"""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from pipeline.deliver.answers.model.trout import TRow
from pipeline.deliver.answers.trout import check as CHK
from pipeline.deliver.answers.trout import down as D
from pipeline.deliver.answers.trout import words as W


def _r(key, rank, says, name=None, kind="quota", part=None):
    return D.DownRule(key, rank, part, name, (kind, says))


RULES = {
    1: _r("bella::r1", 1, "Release trout and char", "Bella Coola River"),
    2: _r("z5::size", 3, "Only 1 over 50 cm (20 in) a day", kind="sizecap"),
    3: _r("z5::quota", 3, "Keep up to 4 trout and char a day"),
    4: _r("lonzo::r1", 1, "Hatchery trout 30 cm (12 in) or longer", "Lonzo Creek", kind="size"),
    5: _r("z5::sub", 3, "At most 1 trout a day from streams", kind="subcap"),
    6: _r("dutch::r1", 1, "Keep up to 2 trout a day", "Dutch Creek"),
    7: _r("z5::same", 3, "Keep up to 2 trout a day"),
    8: _r("twin::r1", 1, "Release wild trout", "Twin Lakes"),
    9: _r("fed::r1", -1, "Keep up to 2 trout a day (federal)", "Somewhere"),
}
rule = RULES.__getitem__


def dec(win, *roles, narrow=None):
    return {"win": win, "narrow": narrow, "roles": [[ix, role, by] for ix, role, by in roles]}


def row(src, head="Release", origin=None):
    return {"key": "RB" if origin is None else f"RB/{origin}", "fish": "RB", "origin": origin, "src": list(src),
            "sheet": {"head": head}}


def both(d):
    return {"hatchery": d, "wild": d}


# ------------------------------------------------------------------------------------------------------------ the words

def test_the_words_are_the_other_fish_rows_words():
    assert [D.line(["Dutch Creek"]), D.line(["Twin Lakes"]), D.line(["Kootenay River", "Duck Lake"]),
            D.line(["A Creek", "Bass Lakes", "C River"]), D.line([])] == [
        "Dutch Creek’s rule for its tributaries",
        "Twin Lakes’ rule for its tributaries",
        "Kootenay River’s and Duck Lake’s rules for their tributaries",
        "A Creek’s, Bass Lakes’ and C River’s rules for their tributaries",
        None]
    assert W.is_row_down("Dutch Creek’s rule for its tributaries", ["Dutch Creek"])
    assert not W.is_row_down("Dutch Creek’s rule for its tributaries", ["Duck Lake"])
    with pytest.raises(W.WordsError):
        W.row_down([" Dutch Creek"])


# ------------------------------------------------------------------------------------------------------------ the names

def test_a_row_every_answer_of_which_a_tributary_rule_wins_names_its_water():
    fa = both(dec(1, (1, "governs", None)))
    assert D.names_of(row([1]), fa, rule) == ["Bella Coola River"]


def test_a_region_size_cap_beside_it_keeps_the_line_nygaard():
    """Owner 2026-11-19 ~01:25 ("this is good"): the number is the tributary rule's, the size cap the region's."""
    fa = both(dec(6, (6, "governs", None), (2, "limit", None)))
    assert D.names_of(row([6, 2], head="Keep 2 · only 1 over 50 cm (20 in)"), fa, rule) == ["Dutch Creek"]


def test_a_tributary_rule_that_only_sets_one_origins_size_names_nothing_shamrock():
    fa = {"hatchery": dec(3, (3, "governs", None), (4, "floor", None)), "wild": dec(3, (3, "governs", None))}
    assert D.names_of(row([3, 4], head="Keep 4"), fa, rule) == []


def test_a_number_lowered_by_another_rule_names_nothing():
    fa = both(dec(6, (6, "governs", None), (5, "narrows", None), narrow=1))
    assert D.names_of(row([6, 5], head="Keep 1"), fa, rule) == []


def test_review3_a_subcap_drawn_as_deciding_with_narrow_unset_names_nothing(monkeypatch):
    fa = both(dec(6, (6, "governs", None), (5, "limit", None)))
    r = row([6, 5], head="Keep 2 · only 1")
    assert D.names_of(r, fa, rule) == []
    monkeypatch.setattr(D, "A_LOWERED_ELSEWHERE_REMOVES_IT", False)
    assert D.names_of(r, fa, rule) == ["Dutch Creek"]                   # the mutation: the line wrongly kept


def test_no_rail_no_line(monkeypatch):
    """A rule of `src` with no stored role (the sheet keeps its Sources list), or a head number no step says."""
    fa = both(dec(6, (6, "governs", None)))
    assert D.names_of(row([6, 3], head="Keep 2"), fa, rule) == []
    assert D.names_of(row([6], head="Keep 2 · counts toward the 6"), fa, rule) == []
    assert D.names_of(row([6], head="Keep 2"), fa, rule) == ["Dutch Creek"]
    monkeypatch.setattr(D, "A_ONLY_WHERE_THE_RAIL_IS_DRAWN", False)
    assert D.names_of(row([6, 3], head="Keep 2"), fa, rule) == ["Dutch Creek"]   # the mutation: a line no rail shows


def test_a_twin_row_speaks_for_its_own_origin():
    fa = {"hatchery": dec(3, (3, "governs", None)), "wild": dec(8, (8, "governs", None))}
    assert D.names_of(row([8], origin="wild"), fa, rule) == ["Twin Lakes"]
    assert D.names_of(row([3], head="Keep 4", origin="hatchery"), fa, rule) == []
    assert D.names_of(row([3, 8], head="Keep 4"), fa, rule) == []             # one row for both: the hatchery are Region's


def test_a_deciding_step_the_rail_drops_as_a_repeat_names_nothing():
    """The rail draws each sentence once: Region 5's same words come first (its level), the tributary rule is dropped."""
    fa = both(dec(6, (7, "contains", None), (6, "governs", None)))
    assert D.names_of(row([6, 7], head="Keep 2"), fa, rule) == []


def test_a_federal_or_part_rule_is_no_tributary_one():
    assert D.rank_on(-1, True) == -1 and D.rank_on(0, True) == 1 and D.rank_on(3, False) == 3
    fa = both(dec(9, (9, "governs", None)))
    assert D.names_of(row([9], head="Keep 2"), fa, rule) == []
    parted = {**RULES, 6: _r("dutch::r1", 1, "Keep up to 2 trout a day", "Dutch Creek", part="the upper half")}
    assert D.names_of(row([6], head="Keep 2"), both(dec(6, (6, "governs", None))), parted.__getitem__) == []


def test_a_tributary_rule_with_no_entry_name_stops_the_build():
    named = {**RULES, 6: _r("dutch::r1", 1, "Keep up to 2 trout a day")}
    with pytest.raises(D.AnswersError):
        D.names_of(row([6], head="Keep 2"), both(dec(6, (6, "governs", None))), named.__getitem__)


def test_a_row_set_elsewhere_never_reads_a_title():
    """The first setter that is no tributary rule ends the row: no title is worked out for it (the build's cost)."""
    asked = []
    lazy = {3: D.DownRule("z5::quota", 3, None, None, lambda: asked.append(3) or ("quota", "Keep up to 4"))}
    assert D.names_of(row([3], head="Keep 4"), both(dec(3, (3, "governs", None))), lazy.__getitem__) == [] and asked == []


def test_with_down_writes_the_line_only_where_there_is_one():
    t = {"boxes": ({"rows": (row([1]), {**row([3], head="Keep 4"), "key": "CT", "fish": "CT"})},)}
    frame = {"fish": {"RB": both(dec(1, (1, "governs", None))), "CT": both(dec(3, (3, "governs", None)))}}
    got = D.with_down(t, frame, rule)["boxes"][0]["rows"]
    assert [r.get("down") for r in got] == ["Bella Coola River’s rule for its tributaries", None]
    assert "down" not in got[1]                                         # absent, never null: a pre-T892 table's bytes


# ------------------------------------------------------------------------------------------------------------ the shape

def _trow(**kw):
    return {"key": "RB", "fish": "RB", "name": "Rainbow trout", "origin": None, "label": None, "split": False,
            "sub": None, "lines": [], "quiet": [], "src": [1], "sheet": {"head": "Release", "here": [], "src": [1]}, **kw}


def _valid(**kw):
    """As the build validates a value: the wire's JSON, strict."""
    return TRow.model_validate_json(json.dumps(_trow(**kw)))


def test_the_model_takes_the_field_optional_and_refuses_another_form():
    assert _valid().down is None
    assert _valid(down="Twin Lakes’ rule for its tributaries").down
    assert _valid(down="A’s and B’s rules for their tributaries").down
    for bad in ("from Dutch Creek", "Dutch Creek’s rule", " Dutch Creek’s rule for its tributaries", ""):
        with pytest.raises(ValidationError):
            _valid(down=bad)


# ------------------------------------------------------------------------------------------------------------ J's read

def _cell(text, win, via="trib", rank=0, name="Dutch Creek", narrows=()):
    roles = [[0, "governs", None]] + [[i, "narrows", None] for i in narrows]
    frame = {"fish": {"RB": both({"win": win, "narrow": 1 if narrows else None, "roles": roles})}}
    by_key = {"dutch::r1": SimpleNamespace(via=via, f={}, x={"_rank": rank, "entry_name": name}),
              "z5::sub": SimpleNamespace(via="reach", f={}, x={"_rank": 3, "entry_name": "Region 5"})}
    c = SimpleNamespace(today=frame, P=SimpleNamespace(by_key=by_key), ids=["dutch::r1", "z5::sub"])
    T = SimpleNamespace(rows=[SimpleNamespace(key="RB", fish="RB", origin=None, r={"down": text})])
    return c, T


def test_j_reads_the_line_back_against_the_stored_answers(monkeypatch):
    ok = "Dutch Creek’s rule for its tributaries"
    assert CHK.down_problems(*_cell(ok, 0)) == []
    assert CHK.down_problems(*_cell("Duck Lake’s rule for its tributaries", 0))           # another water
    assert CHK.down_problems(*_cell(ok, 0, via="reach"))                                  # the water's own rule
    assert CHK.down_problems(*_cell(ok, 0, rank=-1))                                      # federal
    assert CHK.down_problems(*_cell(ok, 0, narrows=(1,)))                                 # lowered by Region 5
    assert CHK.down_problems(*_cell(ok, 0, name=""))                                      # no name to say
    c, T = _cell(ok, 0)
    c.P = None
    assert CHK.down_problems(c, T)                                                        # never passed unproved
    monkeypatch.setattr(CHK, "A_DOWN_PROOF", False)
    assert CHK.down_problems(*_cell("Duck Lake’s rule for its tributaries", 0)) == []     # the mutation


# ------------------------------------------------------------------------------------------------------------ the wiring

def test_the_build_hands_the_keys_rules_to_the_table(monkeypatch):
    """`answers._trout_compose_key` passes `down` (the key's `trib` set): without it no row of a release says it."""
    from pipeline.deliver.answers import answers as A
    from pipeline.deliver.answers.trout import table as TT
    seen = {}

    def fake(rows, disp, starts, moments, rules, **kw):
        seen.update(kw)
        return [None] * len(starts)
    monkeypatch.setattr(TT, "compose_key", fake)
    ctx = SimpleNamespace(cache={"trout_rules": object(), "key_trib": frozenset({"dutch::r1"}), "key_names": ()}, B=None)
    A._trout_compose_key({"rows": [{}], "display": [{}], "licence": [{}]}, [1], None, ctx)
    assert callable(seen.get("down"))
