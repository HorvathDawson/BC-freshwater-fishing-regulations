"""T103 — ONE size shape (owner 2026-10-22: one decision = one function used everywhere).

"What size rule does this quota carry: a minimum, a floor, a ceiling, or a slot?" is decided by
`display.size_shape(lengths)` and nowhere else. Before it, `_plain` picked `mn`/`floor`/`ceil` with no open-end
test and no slot, while `_title` had both — so a no-keep slot (Morley Lake's lake trout, none between 60 and
90 cm) read as a floor AND a ceiling: "Keep up to 1 lake trout a day, 90 cm (35 in) or longer, none over 60 cm"
beside the title "At most 1 lake trout over 90 cm". 24 plain strings carried a size rule nobody can meet.

  * whole strings on the real rules' fields (AGENTS 33);
  * THE GATE (`display.size_problems`, run by `display.build_rules`): every size the title and plain name is
    one of the cut points `bands()` cuts at, and no "X or longer, none over Y" has Y below X — the old
    `_plain` pick fails it;
  * THE PIN: no other function of `pipeline/deliver/answers` reads a rule's `lengths` bounds itself.
"""
from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline.deliver.answers import display as D
from pipeline.deliver.answers.common import AnswersError

ROOT = Path(__file__).resolve().parents[2]
ANSWERS = ROOT / "pipeline/deliver/answers"


def _rule(rule, species, lengths, take=None, **kw):
    return {"entry": "r6:morley_lake@6-25", "rule": rule, "type": "retention_limit", "family": "retention",
            "species": species, "lengths": lengths, "take": take, **kw}


# the live rules' own fields (bundle r-36fbc5326506)
LT_SLOT = _rule("morley_lake.r13c", ["LT"], [{"min_cm": 60, "max_cm": 90, "take": 0}], within="morley_lake.r13")
PIKE_SLOT = _rule("pike.r1", ["NP"], [{"min_cm": 70, "max_cm": 100, "take": 0}, {"min_cm": 100}], take=1,
                  within="pike.r0")
GR_SLOT = _rule("grayling.r1", ["GR"], [{"min_cm": 36, "max_cm": 44, "take": 0}, {"min_cm": 44}], take=1,
                within="grayling.r0")
WINDOW = _rule("trout.r1", ["TROUT_CHAR"], [{"min_cm": 30, "max_cm": 50}, {"max_cm": 30, "take": 0},
                                            {"min_cm": 50, "take": 0}], take=1)
FLOOR = _rule("trout.r2", ["RB"], [{"min_cm": 50}, {"max_cm": 50, "take": 0}], take=1)
CEIL = _rule("trout.r3", ["CT"], [{"max_cm": 50}, {"min_cm": 50, "take": 0}], take=2)
REL_UNDER = _rule("trout.r4", ["RB"], [{"max_cm": 50, "take": 0, "closed": True}], origin="hatchery")


def test_size_shape_names_each_range_by_its_open_ends():
    S = D.size_shape
    none = {"min": None, "floor": None, "ceil": None, "slot": None, "floor_closed": False}
    assert S(LT_SLOT["lengths"]) == {**none, "slot": (60, 90)}
    assert S(PIKE_SLOT["lengths"]) == {**none, "min": 100, "slot": (70, 100)}
    assert S(WINDOW["lengths"]) == {**none, "floor": 30, "ceil": 50}
    assert S(FLOOR["lengths"]) == {**none, "min": 50, "floor": 50}
    assert S(REL_UNDER["lengths"]) == {**none, "floor": 50, "floor_closed": True}
    assert S([]) == S(None) == none


def test_a_no_keep_slot_is_said_as_a_slot_in_plain():
    assert D.plain(LT_SLOT) == "Keep only lake trout, none over 60 cm (24 in), under 90 cm (35 in)."
    assert D.title(LT_SLOT) == "No lake trout over 60 cm (24 in), under 90 cm (35 in)"
    assert D.plain(PIKE_SLOT) == ("Only 1 northern pike over 100 cm (39 in) a day, none over 70 cm (28 in), "
                                  "under 100 cm (39 in).")
    assert D.title(PIKE_SLOT) == "At most 1 northern pike over 100 cm (39 in) a day"
    assert D.plain(GR_SLOT) == ("Only 1 Arctic grayling over 44 cm (17 in) a day, none over 36 cm (14 in), "
                                "under 44 cm (17 in).")


def test_the_other_shapes_keep_their_words():
    assert D.plain(WINDOW) == "Keep up to 1 trout or char a day, 30 cm (12 in) or longer, none over 50 cm (20 in)."
    assert D.title(WINDOW) == "Keep up to 1 trout or char a day (none under 30 cm (12 in), none over 50 cm (20 in))"
    assert D.plain(FLOOR) == "Keep up to 1 rainbow trout a day, 50 cm (20 in) or longer."
    assert D.plain(CEIL) == "Keep up to 2 cutthroat trout a day, none over 50 cm (20 in)."
    assert D.plain(REL_UNDER) == "Release every hatchery rainbow trout 50 cm (20 in) or less."
    assert D.title(REL_UNDER) == "Release hatchery rainbow trout 50 cm (20 in) or less"


# --- the gate -----------------------------------------------------------------------------------------------

def _build(*rules):
    B = SimpleNamespace(index={(x["entry"], x["rule"]): i for i, x in enumerate(rules)},
                        rules={(x["entry"], x["rule"]): x for x in rules})
    return D.build_rules(B)


def test_the_gate_passes_every_shape():
    got = _build(LT_SLOT, PIKE_SLOT, GR_SLOT, WINDOW, FLOOR, CEIL, REL_UNDER)
    assert len(got) == 7


def _old_pick(lengths):
    """The `_plain` pick before T103: no open-end test, no slot."""
    ln = lengths or []
    mn = next((r for r in ln if r.get("min_cm") is not None and r.get("take") is None), None)
    floor = next((r for r in ln if r.get("max_cm") is not None and r.get("take") == 0), None)
    ceil = next((r for r in ln if r.get("min_cm") is not None and r.get("take") == 0), None)
    return {"min": mn and mn["min_cm"], "floor": floor and floor["max_cm"], "ceil": ceil and ceil["min_cm"],
            "slot": None, "floor_closed": bool(floor and floor.get("closed"))}


def test_the_gate_fails_on_the_old_plain_pick(monkeypatch):
    monkeypatch.setattr(D.rule, "size_shape", _old_pick)
    with pytest.raises(AnswersError, match=r"morley_lake.r13c.*none over 60 cm.*below.*90"):
        _build(LT_SLOT)


def test_the_gate_fails_on_a_bound_no_band_cuts_at(monkeypatch):
    real = D._plain
    monkeypatch.setattr(D.rule, "_plain", lambda x, names=None: real(x, names).replace("50 cm (20 in)", "55 cm (22 in)"))
    with pytest.raises(AnswersError, match=r"trout.r3.*55 cm.*cut points"):
        _build(CEIL)


# --- the pin: no second copy --------------------------------------------------------------------------------

#: functions that read `lengths` bounds for ANOTHER decision: which side of a cut a fish exactly at it falls
#: to (`caps.rule_sides`, RULINGS P37), and a licensing document's own lengths (`stamps`, not a rule's)
ALLOWED = {("display/rule.py", "size_shape"), ("display/rule.py", "cut_points"), ("display/rule.py", "bands"),
           ("caps.py", "rule_sides"), ("stamps.py", "_len_words"), ("stamps.py", "documents_of"),
           ("stamps.py", "pick")}


def _bounds_readers(name: str, src: str):
    bad = []
    tree = ast.parse(src)
    for fn in ast.walk(tree):
        if not isinstance(fn, ast.FunctionDef):     # a lambda counts for the function it sits in
            continue
        inner = {id(n) for f2 in ast.walk(fn) if f2 is not fn and isinstance(f2, ast.FunctionDef)
                 for n in ast.walk(f2)}
        for n in ast.walk(fn):
            if id(n) in inner:
                continue
            if isinstance(n, ast.Constant) and n.value in ("min_cm", "max_cm") and (name, fn.name) not in ALLOWED:
                bad.append(f"{name}:{n.lineno} {fn.name}")
    return bad


def test_no_other_function_reads_lengths_bounds():
    bad = []
    for p in sorted(ANSWERS.rglob("*.py")):
        name = p.relative_to(ANSWERS).as_posix()
        if name == "model.py":                        # the schema's field names
            continue
        bad += _bounds_readers(name, p.read_text(encoding="utf-8"))
    assert not bad, bad


def test_the_pin_catches_a_reinlined_bounds_read():
    src = (ANSWERS / "display" / "rule.py").read_text(encoding="utf-8")
    mutated = src.replace('    z = size_shape(ln)                              # T103: the one size shape, `_title`\'s too\n',
                          '    z = size_shape(ln)\n    mn = next((r for r in ln if r.get("min_cm") is not None), None)\n', 1)
    assert mutated != src
    assert _bounds_readers("display/rule.py", mutated) == [f"display/rule.py:{_line_of(mutated, 'mn = next((r for r in ln if r.get("min_cm")')} _plain"]


def _line_of(src: str, needle: str) -> int:
    return src[:src.index(needle)].count("\n") + 1
