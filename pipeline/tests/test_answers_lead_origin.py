"""T120 — ONE LEADING ORIGIN (owner 2026-10-22: one decision = one function used everywhere) — no data.

"When two origins answer for the same fish (H and W), which one leads?" is decided by `common.lead_origin` and
nowhere else: the main row (`rows.main_res`), the year strip (`year.leading_answer`) and the weekday split
(`rows._week_answer`, `rows[].week`) all call it. Before T120 the weekday split took the MOST PERMISSIVE keeper
(H keep 2 / W keep 5 read "keep up to 5") and the STRICTER non-keeper (H release / W closed read "no fishing"),
so a weekday row could say a bigger quota than the main row gave.

The pin: an AST scan of `pipeline/deliver/answers/` refuses any other function that picks a leader between the
origins — a `return X` guarded by a keep test of X, or a max/min ranking answers by their status — and a mutation
test proves the scan catches both old shapes.

T161 (decision Q66, 2026-10-23): the pick is SHIPPED — every stored fish answer carries `lead` ("hatchery" | "wild" |
None, `common.lead_name`, rows 33) so the app's `mainRes` only reads it (its pin: `app/tools/lead-origin.test.ts`).
The answers model holds every stored lead to the one function; mutation pins: a copy that disagrees is refused by the
model (`test_the_model_holds_the_stored_lead_to_the_one_function`), a rows 32 frame by the codec.
"""
from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest

from pipeline.deliver import calendar as K
from pipeline.deliver.answers import common
from pipeline.deliver.answers import model as M
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.answers import year as Y
from pipeline.deliver.answers.common import AnswersError
from pipeline.deliver.answers.encode import RowsCodec
from pipeline.deliver.release.shards import frame_refs

SAT = K.Moment(K.weekday_mask(["Saturday", "Sunday"]))
WEEK = K.Moment(K.weekday_mask(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]))


def _a(status, daily=None, win="r"):
    return {"status": status, "daily": daily, "win": win}


# the decision -----------------------------------------------------------------------------------
def test_the_keepable_origin_leads_hatchery_first():
    h, w = _a("keep", 2, "h"), _a("keep", 5, "w")
    assert common.lead_origin(h, w) is h                      # H keep 2 / W keep 5: H, never the bigger 5
    h, w = _a("release", win="h"), _a("keep", 5, "w")
    assert common.lead_origin(h, w) is w                      # only W keeps: W
    h, w = _a("no_limit", win="h"), _a("keep", 5, "w")
    assert common.lead_origin(h, w) is h


def test_no_keeper_hatchery_leads_else_wild():
    h, w = _a("release", win="h"), _a("closed", win="w")
    assert common.lead_origin(h, w) is h                      # H release / W closed: H, never the stricter W
    h, w = _a("closed", win="h"), _a("release", win="w")
    assert common.lead_origin(h, w) is h
    w = _a("closed", win="w")
    assert common.lead_origin(None, w) is w
    assert common.lead_origin(None, None) is None


def test_main_res_and_leading_answer_are_the_one_function():
    for h, w in [(_a("keep", 2), _a("keep", 5)), (_a("release"), _a("closed")), (_a("closed"), _a("keep", 1)),
                 (None, _a("release")), (_a("release"), None), (None, None)]:
        lead = common.lead_origin(h, w)
        assert RW.main_res(h, w) is lead
        assert Y.leading_answer({"hatchery": h, "wild": w}) is lead
    assert Y.leading_answer(None) is None


# the weekday row leads with the main row's origin -----------------------------------------------
def _frame(h, w):
    return {"fish": {"KO": {"hatchery": h, "wild": w}},
            "rows": [{"all_members": ["KO"], "members": ["KO"]}]}


def test_the_weekday_answer_is_the_main_rows_origin():
    h, w = _a("release", win="e::h"), _a("closed", win="e::w")
    assert RW._week_answer(_frame(h, w), "KO") == ("release", None, "e::h")
    h, w = _a("keep", 2, "e::h"), _a("keep", 5, "e::w")
    assert RW._week_answer(_frame(h, w), "KO") == ("keep", 2, "e::h")
    assert RW._week_answer(_frame(None, None), "KO") is None


def test_a_weekday_split_says_the_main_rows_quota_and_status():
    sat = _frame(_a("keep", 2, "e::r2"), _a("keep", 5, "e::r5"))
    mon = _frame(_a("release", win="e::r1"), _a("closed", win="e::r0"))
    got = RW.say_weekdays([sat, mon], [SAT.as_json(), WEEK.as_json()])
    for frame, today in zip(got, ("Sat–Sun", "Mon–Fri")):
        (row,) = frame["rows"]
        wk = row["week"]
        assert wk["says"] == "Keep up to 2 on Saturdays and Sundays; release Monday to Friday"
        assert wk["status"] == "Weekends: keep 2 · Weekdays: release"
        assert [(p["days"], p["status"], p["n"], p["says"], p["rules"], p["today"]) for p in wk["parts"]] == [
            ("Sat–Sun", "keep", 2, "keep up to 2", ["e::r2"], today == "Sat–Sun"),
            ("Mon–Fri", "release", None, "release every one", ["e::r1"], today == "Mon–Fri")]
        # the weekday row's leader is the main row's
        for f, part in ((sat, wk["parts"][0]), (mon, wk["parts"][1])):
            lead = RW.main_res(f["fish"]["KO"]["hatchery"], f["fish"]["KO"]["wild"])
            assert (lead["status"], lead["win"]) == (part["status"], part["rules"][0])


# the pin: no second copy ------------------------------------------------------------------------
ANSWERS = Path(common.__file__).parent


def _status_test(node: ast.AST) -> bool:
    """A KEEP test of an answer's status: `_keepish(...)` / `rows._keepish(...)`, or a status read (`x["status"]`,
    `x.get("status")`) against the keep statuses (`KEEPS` / `common.KEEPS` / `_KEEPS`, or "keep" / "no_limit").
    A status test for anything else (`display.compose`'s `value.get("status") == "tidal"`) picks no leader."""
    def ident(n):
        return n.id if isinstance(n, ast.Name) else n.attr if isinstance(n, ast.Attribute) else None
    reads = keeps = False
    for n in ast.walk(node):
        if isinstance(n, ast.Call) and ident(n.func) == "_keepish":
            return True
        if isinstance(n, ast.Subscript) and isinstance(n.slice, ast.Constant) and n.slice.value == "status":
            reads = True
        if isinstance(n, ast.Call) and ident(n.func) == "get" and \
                n.args and isinstance(n.args[0], ast.Constant) and n.args[0].value == "status":
            reads = True
        if ident(n) in ("KEEPS", "_KEEPS") or (isinstance(n, ast.Constant) and n.value in ("keep", "no_limit")):
            keeps = True
    return reads and keeps


def _picks_a_leader(fn: ast.AST) -> bool:
    """A function that picks one of several origin answers by status: `if <status test of X>: return X`, or a
    max/min whose key ranks by status."""
    def origin_word(v):
        # T161 review: naming the leader ("hatchery" / "wild") off a keep test is the same decision as returning it
        return isinstance(v, ast.Constant) and v.value in common.ORIGINS
    for n in ast.walk(fn):
        if isinstance(n, ast.If) and _status_test(n.test):
            tested = {x.id for x in ast.walk(n.test) if isinstance(x, ast.Name)}
            for b in n.body:
                if isinstance(b, ast.Return) and isinstance(b.value, ast.Name) and b.value.id in tested:
                    return True
                if isinstance(b, ast.Return) and origin_word(b.value):
                    return True
        if isinstance(n, ast.IfExp) and _status_test(n.test) and origin_word(n.body):
            return True
        # `X if <keep test of X> else <another answer>` (a filter's `... else None` picks nothing)
        if isinstance(n, ast.IfExp) and _status_test(n.test) and isinstance(n.body, ast.Name) and \
                n.body.id in {x.id for x in ast.walk(n.test) if isinstance(x, ast.Name)} and \
                not isinstance(n.orelse, ast.Constant):
            return True
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id in ("max", "min"):
            for kw in n.keywords:
                if kw.arg == "key" and any(isinstance(x, ast.Constant) and x.value == "status"
                                           for x in ast.walk(kw.value)):
                    return True
    return False


def _leaders(src: str, name: str):
    tree = ast.parse(src)
    return [f"{name}:{fn.name}" for fn in ast.walk(tree)
            if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)) and _picks_a_leader(fn)
            and not any(_picks_a_leader(c) and c is not fn for c in ast.walk(fn)
                        if isinstance(c, (ast.FunctionDef, ast.AsyncFunctionDef)))]


def test_no_function_in_answers_but_lead_origin_picks_a_leading_origin():
    got = []
    for p in sorted(ANSWERS.glob("*.py")):
        got += _leaders(p.read_text(), p.name)
    assert got == ["common.py:lead_origin"]


def test_the_scan_catches_both_old_copies():
    """Mutation pin: the pre-T120 `main_res` and `_week_answer` bodies are each caught."""
    old_main = '''
def main_res(H, W):
    if H and _keepish(H["status"]):
        return H
    if W and _keepish(W["status"]):
        return W
    return H or W
'''
    old_week = '''
def _week_answer(frame, S):
    v = (frame.get("fish") or {}).get(S) or {}
    got = [x for x in (v.get("hatchery"), v.get("wild")) if x is not None]
    rank = {"closed": 0, "release": 1, "keep": 2, "no_limit": 3}
    keep = [x for x in got if x["status"] in ("keep", "no_limit")]
    best = max(got, key=lambda x: (rank.get(x["status"], -1), x.get("daily") or 0)) if keep else \\
        min(got, key=lambda x: rank.get(x["status"], 9))
    return best["status"], best.get("daily"), best["win"]
'''
    old_year = '''
def leading_answer(answer):
    h, w = answer.get("hatchery"), answer.get("wild")
    if h is not None and h["status"] in _KEEPS:
        return h
    return h if h is not None else w
'''
    assert _leaders(old_main, "rows.py") == ["rows.py:main_res"]
    assert _leaders(old_week, "rows.py") == ["rows.py:_week_answer"]
    assert _leaders(old_year, "year.py") == ["year.py:leading_answer"]
    one_line = '''
def lead(H, W):
    return H if H and _keepish(H["status"]) else (W if W and _keepish(W["status"]) else (H or W))
'''
    assert _leaders(one_line, "x.py") == ["x.py:lead"]
    # a filter of ONE answer is no leader (`rows._kept`)
    assert _leaders('def k(m):\n    return m if m and _keepish(m["status"]) else None\n', "x.py") == []


def test_the_callers_delegate():
    assert "common.lead_origin(" in inspect.getsource(RW.main_res)
    assert "common.lead_origin(" in inspect.getsource(Y.leading_answer)
    assert "lead_origin(" in inspect.getsource(RW._week_answer)


def test_the_scan_catches_get_and_qualified_keep_tests():
    """Mutation pin (review): a copy reading the status with `.get("status")`, or testing it through a qualified
    `rows._keepish(...)` / `x in common.KEEPS`, is the same decision and is caught."""
    via_get = '''
def lead(h, w):
    if h and h.get("status") in KEEPS:
        return h
    return h or w
'''
    via_attr = '''
def lead(h, w):
    if w and rows._keepish(w.get("status")):
        return w
    return h or w
'''
    via_keeps = '''
def lead(h, w):
    if h and h.get("status") in common.KEEPS:
        return h
    return h or w
'''
    for src in (via_get, via_attr, via_keeps):
        assert _leaders(src, "x.py") == ["x.py:lead"], src
    # a filter of ONE answer through `.get` is still no leader
    assert _leaders('def k(m):\n    return m if m and m.get("status") in KEEPS else None\n', "x.py") == []


def test_the_scan_catches_a_literal_keep_test_and_spares_other_status_tests():
    """Mutation pin (review): `h["status"] == "keep"` is the same decision; a status test that is not a keep test
    (`display.compose`: `if value.get("status") == "tidal": return value`) is not a leader."""
    literal = 'def lead(h, w):\n    if h and h["status"] in ("keep", "no_limit"):\n        return h\n    return h or w\n'
    assert _leaders(literal, "x.py") == ["x.py:lead"]
    tidal = 'def compose(value, others):\n    if value.get("status") == "tidal":\n        return value\n    return 1\n'
    assert _leaders(tidal, "x.py") == []


# T161 — THE LEADING ORIGIN IS SHIPPED (decision Q66): the rows frame carries `fish[S].lead`, the app only reads it ----
_STATUSES = (None, "keep", "no_limit", "release", "closed")


def _dec(status, win):
    """A model-valid decided answer (daily set <=> keep)."""
    return {"status": status, "win": win, "daily": 2 if status == "keep" else None, "narrow": None,
            "lines": [], "roles": [[win, "governs", None]], "lift_notes": []}


def test_lead_name_names_exactly_the_origin_lead_origin_picks():
    """Whole value over every pair of statuses: `lead_name` is `lead_origin`'s pick, as a word."""
    got, want = {}, {}
    for hs in _STATUSES:
        for ws in _STATUSES:
            h = None if hs is None else _a(hs, win="h")
            w = None if ws is None else _a(ws, win="w")
            got[(hs, ws)] = common.lead_name(h, w)
            m = common.lead_origin(h, w)
            want[(hs, ws)] = None if m is None else {"h": "hatchery", "w": "wild"}[m["win"]]
    assert got == want
    assert (got[("release", "keep")], got[("keep", "keep")], got[("closed", "release")], got[(None, "closed")],
            got[(None, None)]) == ("wild", "hatchery", "hatchery", "wild", None)


def test_the_stored_fish_answer_carries_its_lead_whole():
    h, w = _a("release", win="h"), _a("keep", 5, "w")
    assert RW.fish_answer(h, w) == {"hatchery": h, "wild": w, "lead": "wild"}
    assert RW.fish_answer(None, None) == {"hatchery": None, "wild": None, "lead": None}
    # produce stores every fish through it (one place builds a stored fish answer), and it names the one function's pick
    assert "fish_answer(" in inspect.getsource(RW.produce)
    assert "common.lead_name(" in inspect.getsource(RW.fish_answer)


def _frame_with(h, w, lead):
    return {"spp": ["RB"], "fish": {"RB": {"hatchery": h, "wild": w, "lead": lead}}, "rows": [],
            "steelhead_line": None}


def test_the_model_holds_the_stored_lead_to_the_one_function():
    """The answers model refuses a frame whose `lead` is not `common.lead_name`'s — so a COPY of the choice that
    disagrees (mutation: a hatchery-unless-absent copy, which reads H release / W keep as hatchery) fails the build."""
    h, w = _dec("release", 1), _dec("keep", 2)
    M.validate_section("rows", [_frame_with(h, w, "wild")])
    copy = "hatchery" if h is not None else "wild"                     # the mutated copy's answer
    with pytest.raises(M.ShapeError, match="is not the leading origin 'wild'"):
        M.validate_section("rows", [_frame_with(h, w, copy)])
    with pytest.raises(M.ShapeError):
        M.validate_section("rows", [_frame_with(h, w, None)])          # a lead left out beside answers
    with pytest.raises(M.ShapeError):
        M.validate_section("rows", [_frame_with(None, None, "hatchery")])   # a lead with no answer
    with pytest.raises(M.ShapeError):
        M.validate_section("rows", [{**_frame_with(h, w, "wild"), "fish": {"RB": {"hatchery": h, "wild": w}}}])
    M.validate_section("rows", [_frame_with(None, None, None)])


def test_the_wire_carries_the_lead_third_and_refuses_a_frame_without_it():
    h, w = _dec("release", 1), _dec("keep", 2)
    tables, refs = RowsCodec().encode([_frame_with(h, w, "wild")], {}, {})
    assert tables["frames"] == [[["RB"], {"RB": [0, 1, "wild"]}, [], None, []]]
    sec = dict(tables)
    assert RowsCodec().decode_frame(sec, refs[0], [], []) == _frame_with(h, w, "wild")
    sec["frames"] = [[["RB"], {"RB": [0, 1]}, [], None, []]]           # a rows 32 frame
    with pytest.raises(AnswersError, match=r"holds RB as \[0, 1\] — not \[decided h, decided w, lead\]"):
        RowsCodec().decode_frame(sec, 0, [], [])
    # the shard closure reads the two answer slots only: the lead word is no row reference
    assert frame_refs("rows_frame", tables["frames"][0]) == {"rows_decided": [0, 1], "rows_row": []}


def test_the_scan_catches_a_lead_name_that_re_decides():
    """Mutation pin (T161 review): a `lead_name` that names the origin from its own keep test — same answers today, no
    call to `lead_origin` — is a second copy of the decision; the scan used to pass it (it looked for a returned
    answer, not a returned origin word)."""
    copy = ('def lead_name(h, w):\n    if h is None and w is None:\n        return None\n'
            '    return "hatchery" if h is not None and h["status"] in KEEPS else ('
            '"wild" if w is not None and w["status"] in KEEPS else ("hatchery" if h is not None else "wild"))\n')
    assert _leaders(copy, "common.py") == ["common.py:lead_name"]
    as_if = 'def lead_name(h, w):\n    if h and h["status"] in KEEPS:\n        return "hatchery"\n    return "wild"\n'
    assert _leaders(as_if, "common.py") == ["common.py:lead_name"]
    # the shipped one names lead_origin's pick by identity and tests no status
    assert _leaders(inspect.getsource(common.lead_name), "common.py") == []
