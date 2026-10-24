"""THE KEEP TAB'S LAST CHOICES (rows 31, display 18; owner rule 2026-10-21: the pipeline answers, the app only reads) —
no data, CI's no-data tier. The live twin is `test_answers_keep_answers_live.py`.

Five page-v36 / prototype choices the Keep presenters made in TypeScript, now made here with the page's own rule:
  1. display `closers` (page closersAt)         `display.closers_of`
  2. rows `scope.narrow_all` (page narrowAll)    `rows.scope_of`
  3. rows `parent` (page parentRow)              `rows.parent_of`
  4. a deciding record-it rule is a `duty`       `rows.stored_role` (page ladderHtml)
  5. `stamps` on a kind / a released fish        `stamps.py` (prototype round UR7 stampsFor)

Every expected value is written out whole, from the page's rule — never read back from the implementation.
"""
from __future__ import annotations

import copy
import json

import pytest

from pipeline.deliver.answers import model as M
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.answers import stamps as ST
from pipeline.deliver.answers.common import AnswersError
from pipeline.deliver.answers.display import closers_of
from pipeline.deliver.types import GAME_FISH

ALL = list(GAME_FISH)


# 1. closers ------------------------------------------------------------------------------------------------------
def test_1_closers_are_the_closures_closing_the_most_fish():
    # two blanket closures and a white sturgeon one beside them (cow0_1010's shape): the blanket ones
    assert closers_of([[1, ALL], [4, ["WSG"]], [6, ALL]]) == [1, 6]
    # none closes every fish: the widest, and every one not wholly inside a wider one, in closing's order
    assert closers_of([[1, ["RB", "CT"]], [4, ["WSG"]], [2, ["RB"]]]) == [1, 4]
    assert closers_of([[1, ALL], [9, ["KO"]]]) == [1]
    assert closers_of([]) == []


def test_1_the_display_model_holds_closers_to_a_closed_frame():
    shut = {"status": "closed", "closing": [[7, ALL], [9, ["WSG"]]], "strip": 1}
    M.validate_section("display", [{**shut, "closers": [7]}])
    for bad in ({**shut},                                                          # closed: closers named
                {**shut, "closers": []},                                           # never empty
                {**shut, "closers": [8]},                                          # not a closing rule
                {**shut, "closers": [9, 7]},                                       # closing's order
                {"status": "own", "closing": [[9, ["WSG"]]], "closers": [9], "strip": 0}):   # open: none
        with pytest.raises(M.ShapeError):
            M.validate_section("display", [bad])


# 2. narrow_all ---------------------------------------------------------------------------------------------------
class _R:
    def __init__(self, key, species, except_=(), **f):
        self.key, self.species, self.except_, self.f, self.x = key, list(species), list(except_), f, f
        self.entry_id, self.via, self.k, self.rank = key.split("::")[0], "reach", "pool", 3


class _P:
    kind = "stream"
    by_key: dict = {}


def _scope(monkeypatch, members, narrow):
    monkeypatch.setattr(RW.wire, "badge_of", lambda p: "region")
    pool = _R("z2:trout_char_quota::trout_char_quota.r1", ["RB", "CT", "ST"], take=4)
    row = {"kind": "keep", "pool": pool, "members": members, "narrow": narrow, "everyone": [], "groups": []}
    return RW.scope_of(_P(), row)


def test_2_narrow_all_is_the_narrowing_clause_binding_every_fish_of_the_row(monkeypatch):
    r4 = _R("z2:trout_char_quota::trout_char_quota.r4", ["RB", "CT", "ST"], water="stream", take=2)
    r4_no_st = _R("z2:trout_char_quota::trout_char_quota.r4", ["RB", "CT", "ST"], ["ST"], water="stream", take=2)
    assert _scope(monkeypatch, ["CT", "RB", "ST"], r4) == {
        "of": "region", "entry": "z2:trout_char_quota", "share": True, "apart": False, "narrow_all": True}
    assert _scope(monkeypatch, ["CT", "RB", "ST"], r4_no_st)["narrow_all"] is False   # ST excepted
    assert _scope(monkeypatch, ["CT", "DV"], r4)["narrow_all"] is False               # DV not named
    assert _scope(monkeypatch, ["CT"], None) == {
        "of": "region", "entry": "z2:trout_char_quota", "share": False, "apart": False, "narrow_all": False}


# 3. parent -------------------------------------------------------------------------------------------------------
def _row(pool, members, all_members=None, groups=()):
    return {"pool": pool, "members": list(members), "all_members": list(all_members or members),
            "groups": [{"members": list(m), "facts": [{"t": t} for t in ts]} for m, ts in groups]}


def test_3_parent_is_the_first_other_pool_holding_and_cross_referencing_the_rows_fish():
    trout = _row(1, ["CT", "DV", "LT"], groups=[(["RB"], ["xref", "cap"])])     # holds RB by an xref
    rb = _row(2, ["RB"])
    plain = _row(None, ["KO"])
    assert [RW.parent_of([trout, rb, plain], i) for i in range(3)] == [None, 0, None]
    # holding the fish is not enough: the other pool must cross-reference one of them
    assert RW.parent_of([_row(1, ["CT"], groups=[(["RB"], ["cap"])]), rb], 1) is None
    # a cross-reference of fish it does not all hold: no parent
    assert RW.parent_of([_row(1, ["CT"], groups=[(["RB"], ["xref"])]), _row(2, ["RB", "KO"])], 1) is None
    # held through all_members (fish that go back) with the xref in a group naming one of them
    assert RW.parent_of([_row(1, ["CT"], ["CT", "RB"], groups=[(["RB"], ["xref"])]), rb], 1) == 0


def _held(pool, members, total, draws=()):
    return {**_row(pool, members), "total": total, "draws": list(draws)}


def test_3_t139_a_row_whose_total_is_another_rules_is_held_by_the_row_carrying_that_totals_group():
    """T139 (Beattie Lake): the lake's trout row (pool `lake`, total Zone B's `zone`, which it carries as a group) and
    the Liard's bull trout row (pool `liard`, total `zone` too, no cross-reference anywhere): the bull trout row is held
    by the trout row. MUTATION `A_ROW_HELD_BY_ITS_TOTALS_GROUP = False` fails the first assert ([None, None, None])."""
    trout = _held("lake", ["CT", "EB", "RB"], "zone", draws=["zone"])
    dv = _held("liard", ["DV"], "zone")
    ko = _held("ko", ["KO"], "ko", draws=["ko"])
    assert [RW.parent_of([trout, dv, ko], i) for i in range(3)] == [None, 0, None]
    # a row whose total is its own pool is never held this way (it carries its own group)
    assert RW.parent_of([_held("zone", ["CT"], "zone", draws=["zone"]), _held("dv", ["DV"], "dv")], 1) is None
    # ... not even where another row carries that pool's group (review: the case above passes without the
    # `T != pool` guard, since no other row draws "dv"; this one is the guard's own pin — MUTATION dropping
    # `T != o["pool"]` gives 0 here. No shipped frame has two keep rows of one pool, so the guard is defensive.)
    assert RW.parent_of([_held("zone", ["CT"], "zone", draws=["zone"]), _held("zone", ["DV"], "zone")], 1) is None
    # nor where no other row carries the total (Duncan River's DV on Crazy Creek when the rest is released: open)
    assert RW.parent_of([_held("duncan", ["DV"], "zone", draws=["zone"]), _held("ko", ["KO"], "ko")], 0) is None
    # the cross-reference hold comes first
    assert RW.parent_of([_held("a", ["CT"], "zone", draws=["zone"]),
                         {**_held("b", ["CT"], "b"), "groups": [{"members": ["DV"], "facts": [{"t": "xref"}]}],
                          "all_members": ["CT", "DV"]},
                         dv], 2) == 1


def test_3_t139_mutation_without_the_second_hold_the_bull_trout_row_is_unheld(monkeypatch):
    monkeypatch.setattr(RW.groups, "A_ROW_HELD_BY_ITS_TOTALS_GROUP", False)
    trout = _held("lake", ["CT", "EB", "RB"], "zone", draws=["zone"])
    assert [RW.parent_of([trout, _held("liard", ["DV"], "zone")], i) for i in range(2)] == [None, None]


def test_3_the_rows_model_holds_parent_to_another_pooled_row():
    base = {"kind": "release", "pool": None, "win": 5, "members": ["DV"], "all_members": ["DV"], "daily": None,
            "narrow": None, "everyone": [], "groups": [], "prot": None, "wins": None, "lift_notes": [], "scope": None,
            "year": [["release", 365]]}
    pool = {**base, "kind": "no_limit", "pool": 3, "win": None, "conds": [], "items": [], "elsewhere": None}
    frame = {"spp": ["DV"], "fish": {}, "steelhead_line": None}
    M.validate_section("rows", [{**frame, "rows": [pool, {**pool, "parent": 0}]}])
    for rows in ([pool, {**pool, "parent": 1}],            # itself
                 [pool, {**pool, "parent": 2}],            # no such row
                 [base, {**pool, "parent": 0}],            # not a pooled row
                 [pool, {**base, "parent": 0}]):           # only a pooled row has a parent
        with pytest.raises(M.ShapeError):
            M.validate_section("rows", [{**frame, "rows": rows}])


# 4. a deciding record-it rule is a duty ---------------------------------------------------------------------------
class _B:
    rules = {("z4:kootenay", "r1"): {"record_retention": True}, ("z4:kootenay", "r2"): {}}


class _PB:
    B = _B()


def test_4_a_record_it_rule_holding_a_deciding_role_is_stored_as_a_duty():
    P = _PB()
    assert [RW.stored_role(P, "z4:kootenay::r1", r) for r in ("floor", "governs", "season", "possession", "duty")] \
        == ["duty", "duty", "duty", "duty", "duty"]
    # a losing role stays what it is (the "doesn't apply" list); a rule with no record duty keeps its role
    assert [RW.stored_role(P, "z4:kootenay::r1", r) for r in ("moot", "replaced", "lifted")] == ["moot", "replaced",
                                                                                                    "lifted"]
    assert RW.stored_role(P, "z4:kootenay::r2", "floor") == "floor"
    with pytest.raises(KeyError):                                                   # never a guess
        RW.stored_role(P, "z4:kootenay::r9", "floor")


# 5. stamps -------------------------------------------------------------------------------------------------------
NAMES = {"steelhead_stamp": "Steelhead Conservation Surcharge Stamp",
         "shuswap_rainbow_stamp": "Conservation Surcharge Stamp for Shuswap rainbow trout",
         "shuswap_char_stamp": "Conservation Surcharge Stamp for Shuswap Lake char",
         "basic_licence": "basic angling licence", "yukon": "Yukon angling licence"}
BASIC = {"doc": "basic_licence", "when": {"act": "fishing"}, "base": True, "prices": {}}
ST_DOC = {"doc": "steelhead_stamp", "when": {"act": "targeting", "species": ["ST"]}, "base": False, "prices": {}}
RB_DOC = {"doc": "shuswap_rainbow_stamp", "when": {"act": "retaining", "species": ["RB"], "lengths": [{"min_cm": 50}]},
          "base": False, "prices": {}}
CHAR_DOC = {"doc": "shuswap_char_stamp", "when": {"act": "retaining", "species": ["LT", "DV"],
                                                  "lengths": [{"min_cm": 60}]}, "base": False, "prices": {}}


def _kind(species, n, cm):
    return {"node": "kind", "species": species, "n": n, "sizes": {"cm": cm, "in": []}}


def test_5_the_words_are_the_licence_tabs():
    assert ST.when_words(ST_DOC["when"]) == "Only if you fish for steelhead, even to release them"
    assert ST.when_words(RB_DOC["when"]) == "Only if you keep rainbow trout over 50 cm"
    assert ST.when_words(CHAR_DOC["when"]) == "Only if you keep lake trout and Dolly Varden/bull trout over 60 cm"
    docs = ST.documents_of({"documents": [BASIC, ST_DOC, CHAR_DOC]})
    assert [ST.stamp_out(x, NAMES) for x in docs] == [
        {"doc": "steelhead_stamp", "act": "targeting", "name": "Steelhead Conservation Surcharge Stamp",
         "when": "Only if you fish for steelhead, even to release them"},
        {"doc": "shuswap_char_stamp", "act": "retaining", "name": "Conservation Surcharge Stamp for Shuswap Lake char",
         "when": "Only if you keep lake trout and Dolly Varden/bull trout over 60 cm"}]
    alt = ST.documents_of({"documents": [{**ST_DOC, "or": [{"need": ["yukon"], "prices": {}}]}]})
    assert ST.stamp_out(alt[0], NAMES)["name"] == "Steelhead Conservation Surcharge Stamp or Yukon angling licence"
    assert ST.stamp_out(alt[0], NAMES)["when"] == "Only if you fish for steelhead, even to release them. Either one does"


def test_5_a_fish_document_on_a_period_or_with_narrower_needs_is_refused():
    with pytest.raises(AnswersError):
        ST.documents_of({"documents": [{**ST_DOC, "when": {**ST_DOC["when"], "on": "steelhead_period"}}]})
    with pytest.raises(AnswersError):
        ST.documents_of({"documents": [{**RB_DOC, "also_when": [{"act": "targeting", "species": ["RB"]}]}]})


def test_5_which_documents_a_kind_needs():
    docs = ST.documents_of({"documents": [BASIC, ST_DOC, RB_DOC, CHAR_DOC]})
    pick = lambda k: [x["doc"] for x in ST.pick(docs, k["species"], fish=True, keep=k["n"] != 0,
                                                 max_cm=ST.kind_max_cm(k))]
    assert pick(_kind(["RB"], 2, [[0, None]])) == ["shuswap_rainbow_stamp"]            # any length: over 50 too
    assert pick(_kind(["RB"], 2, [[0, 50]])) == []                                     # never over 50
    assert pick(_kind(["RB"], 2, [[30, 51]])) == ["shuswap_rainbow_stamp"]             # up to 51: over 50
    assert pick(_kind(["RB"], 0, [[0, None]])) == []                                   # keeps none
    assert pick(_kind(["RB"], None, [[0, None]])) == ["shuswap_rainbow_stamp"]         # no number: keeps
    assert pick(_kind(["DV", "LT"], 1, [[60, None]])) == ["shuswap_char_stamp"]
    assert pick(_kind(["LT"], 1, [])) == []                                            # no band: 0 cm (page)
    assert pick(_kind(["ST", "RB"], 0, [])) == ["steelhead_stamp"]                     # fished: a fish-for doc
    assert ST.kind_max_cm(_kind(["RB"], 1, [[0, 30], [40, 60]])) == 60


def test_5_a_group_gets_its_stamps_and_a_shared_frame_is_never_changed():
    lic = {"profiles": [{"documents": [BASIC, ST_DOC, RB_DOC]}, {"documents": []}]}
    kind_rb = _kind(["RB"], 2, [[0, None]])
    kind_eb = _kind(["EB"], 2, [[0, None]])
    group = {"tree": [{"node": "tier", "children": [kind_rb, kind_eb]}],
             "not_kept": [{"species": ["ST", "DV"], "status": "release"}, {"species": ["ST"], "status": "closed"}]}
    frame = {"rows": [{"kind": "keep", "members": [], "group": group}, {"kind": "closed", "members": ["ST"], "group": None}]}
    before = copy.deepcopy(frame)
    [got] = ST.stamp_licence_documents([frame], [lic], NAMES, 0)
    assert frame == before                                                            # the input is untouched
    g = got["rows"][0]["group"]
    assert g["tree"][0]["children"][0]["stamps"] == [
        {"doc": "shuswap_rainbow_stamp", "act": "retaining", "name": "Conservation Surcharge Stamp for Shuswap rainbow trout",
         "when": "Only if you keep rainbow trout over 50 cm"}]
    assert "stamps" not in g["tree"][0]["children"][1]                               # brook trout: no document
    assert g["not_kept"][0]["stamps"] == [{"doc": "steelhead_stamp", "act": "targeting",
                                           "name": "Steelhead Conservation Surcharge Stamp",
                                           "when": "Only if you fish for steelhead, even to release them"}]
    assert "stamps" not in g["not_kept"][1]                                          # closed: never fished
    assert got["rows"][1] is frame["rows"][1]
    # profile 1 (another angler) needs nothing: the frame is returned as it is
    assert ST.stamp_licence_documents([frame], [lic], NAMES, 1)[0] is frame
    # the Keep tab's angler is the prototype's: a B.C. resident 16 or older, not guided, no status
    from pipeline.deliver.answers import licence as L
    assert L.profiles()[L.profile_index(ST.DEFAULT_PROFILE)] == ST.DEFAULT_PROFILE
    with pytest.raises(AnswersError):
        ST.stamp_licence_documents([frame], [lic, lic], NAMES, 0)


def _v(model, x):
    """A wire value through the model as the build checks one: from its JSON (strict: lists are tuples there)."""
    return model.model_validate_json(json.dumps(x))


def test_5_rows_are_built_with_the_licence_answers_they_read():
    from pipeline.deliver.answers import answers as A
    names = lambda chosen: sorted(s.name for s in A.with_needs([s for s in A.SECTIONS if s.name in chosen]))
    assert names(["rows"]) == ["licence", "rows"]
    assert names(["display"]) == ["display", "gear", "licence", "rows"]          # display -> rows -> licence
    assert names(["gear"]) == ["gear"]


def test_5_the_model_holds_stamps_to_what_the_fish_may_do():
    st = {"doc": "steelhead_stamp", "act": "targeting", "name": "Steelhead Conservation Surcharge Stamp",
          "when": "Only if you fish for steelhead, even to release them"}
    keep = {"doc": "shuswap_rainbow_stamp", "act": "retaining", "name": "Conservation Surcharge Stamp for Shuswap "
            "rainbow trout", "when": "Only if you keep rainbow trout over 50 cm"}
    nk = {"species": ["ST"], "status": "release", "says": "Steelhead: release every one", "by": 1, "rules": [1],
          "whose": "Region 3’s rule"}
    _v(M.NotKept, {**nk, "stamps": [st]})
    for bad in ({**nk, "stamps": [keep]}, {**nk, "status": "closed", "stamps": [st]}, {**nk, "stamps": []}):
        with pytest.raises(Exception):
            _v(M.NotKept, bad)
    kind = {"node": "kind", "species": ["RB"], "name": "Rainbow trout", "n": 2, "says": "Up to 2 rainbow trout",
            "detail": "", "sizes": {"cm": [[0, None]], "in": [[0, None]]}, "also_counts": [], "duties": [],
            "sentence": "Up to 2 rainbow trout"}
    _v(M.TreeKind, {**kind, "stamps": [keep, st]})
    with pytest.raises(Exception):
        _v(M.TreeKind, {**kind, "n": 0, "stamps": [keep]})
