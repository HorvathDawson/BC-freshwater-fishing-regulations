"""THE ROWS 34 CUTOVER (trout table plan, T810 ticket L; contract Q-D9, the A0 amendment, 2.11): `answers.cut_over`, the
gates that hold it, and J's proofs over every shipped rows frame. Written for the independent review of lane/t810-l
(2026-11-17), one test per finding:

  1. the kept-elsewhere SENTENCE of each covered keep row (`Row.elsewhere.says`) is on the table (`table.elsewhere`,
     first, through G's `words.elsewhere_says`), and J reads it back (`check.elsewhere_said_problems`);
  2. what the cut drops is held: the build stops where a covered row's kept-elsewhere text is not on its table
     (`answers.A_ELSEWHERE_HELD`), and J fails a shipped table that lost its sentence or states another number;
  3. a split row's parts carry their own fish's stamps and year strips (`A_SPLIT_RESTAMPED`, `A_SPLIT_YEARS_RECUT`):
     a focused frame, since the mini world has no row mixing trout with another fish under a table;
  4. F0's page possession row is a build gate over every shipped frame (`check.page_possession_problems`: the value
     words, the lead, every also-line, derived again from the frame's roles and the bundle's rules);
  5. the cut itself is proved over every frame, NULL cells too (`check.cut_problems`): a covered row carries no group
     and no kept-elsewhere line; every other keep row keeps its line;
  7. every row the table does not cover is the card's row as it was (its group and kept-elsewhere line kept).

Anchors: planted frames (no data), and the mini chain (`mini_chain`: built here; the shipped answers and the same
answers with the cutover off).
"""
from __future__ import annotations

import copy
import json

import pytest

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers.trout import check as J
from pipeline.deliver.answers.trout import table as T
from pipeline.deliver.answers.trout import words as W
from pipeline.tests import mini_chain

TC = C.trout_char()

# ------------------------------------------------------------------------------------------ a focused planted frame

SAYS = "This river’s rule: 2 a day here. Trout and char you kept anywhere else today count toward those 2."
KO_SAYS = "This river’s rule: 5 a day here. Kokanee you kept anywhere else today count toward those 5."
WORKED = [{"q": "Kept 2 elsewhere today?", "a": "None more here", "at": "elsewhere"},
          {"q": "If you keep 2 here?", "a": "x", "at": "here"}]
STAMP_DOC = {"doc": "steelhead_stamp", "when": {"act": "targeting", "species": ["ST"]}}
LICENCE = {"profiles": [{"documents": [STAMP_DOC]}]}


def _ans(status):
    return {"hatchery": {"status": status}, "wild": None, "lead": "hatchery"}


def _frames():
    """Two segments of one key: a release row mixing steelhead with kokanee (its stamp the steelhead's; its year the
    most permissive of both), a trout keep row with its group and kept-elsewhere line, a kokanee keep row with its own."""
    mixed = {"kind": "release", "members": ["ST", "KO"], "all_members": ["ST", "KO"], "groups": [],
             "stamps": [{"doc": "steelhead_stamp", "act": "targeting", "name": "Steelhead stamp", "when": "w"}],
             "year": [["keep", 365]]}
    trout = {"kind": "keep", "pool": 1, "members": ["RB"], "all_members": ["RB"], "groups": [], "daily": 2,
             "scope": {"of": "water"}, "elsewhere": {"says": SAYS, "ask": []},
             "group": {"kinds": ["RB"], "worked": WORKED}, "year": [["keep", 365]]}
    ko = {"kind": "keep", "pool": 2, "members": ["KO"], "all_members": ["KO"], "groups": [], "daily": 5,
          "scope": {"of": "water"}, "elsewhere": {"says": KO_SAYS, "ask": [["Kept 5 elsewhere today?", "None more"]]},
          "group": {"kinds": ["KO"], "worked": []}, "year": [["keep", 365]]}
    f0 = {"fish": {"ST": _ans("release"), "KO": _ans("keep"), "RB": _ans("keep")}, "rows": [mixed, trout, ko]}
    f1 = {"fish": {"ST": _ans("release"), "KO": _ans("closed"), "RB": _ans("keep")}, "rows": [mixed, trout, ko]}
    return [f0, f1]


def _cells(frames, elsewhere=None):
    out = []
    for f in frames:
        _split, covers = T.split_mixed(f)
        want = T.elsewhere_said(f) if elsewhere is None else elsewhere
        out.append({"covers": covers, "table": {"elsewhere": tuple(want)}})
    return out


def _cut(frames, cells):
    return A.cut_over(frames, cells, [LICENCE, LICENCE], [1, 200], None, {}, 0)


def test_the_cut_splits_drops_and_keeps_what_it_should():
    frames = _frames()
    before = copy.deepcopy(frames)
    got = _cut(frames, _cells(frames))
    assert frames == before                                        # the inputs are shared: never changed
    st, ko_part, rb, ko = got[0]["rows"]
    assert st["members"] == ["ST"] and ko_part["members"] == ["KO"]
    # finding 3: each part its own fish's stamps and year
    assert st["stamps"][0]["doc"] == "steelhead_stamp" and "stamps" not in ko_part
    assert st["year"] == [["release", 365]]
    assert ko_part["year"][0][0] == "keep" and ko_part["year"][-1][0] == "closed"
    # Q-D9: the covered trout row loses group and elsewhere; finding 7: the kokanee row is the card's row as it was
    assert "group" not in rb and "elsewhere" not in rb
    assert {k: v for k, v in ko.items() if k != "year"} == {k: v for k, v in before[0]["rows"][2].items() if k != "year"}


def test_the_table_says_every_kept_elsewhere_text_it_takes_over():
    """Finding 1: the covered row's sentence (its "today" said by the link word) leads, then the group's worked lines
    about fish kept elsewhere ("here" lines are not under that link)."""
    assert T.elsewhere_said(_frames()[0]) == [
        "This river’s rule: 2 a day here. Trout and char you kept anywhere else count toward those 2.",
        "Kept 2 elsewhere? None more here."]


def test_mutation_a_lost_kept_elsewhere_text_stops_the_build(monkeypatch):
    """Finding 2: a table without the covered row's sentence (or one of its worked lines) stops the build; the pin off
    lets it through."""
    frames = _frames()
    for lost in ([], ["Kept 2 elsewhere? None more here."]):
        with pytest.raises(C.AnswersError, match="kept-elsewhere text the trout table does not say"):
            _cut(frames, _cells(frames, lost))
    monkeypatch.setattr(A, "A_ELSEWHERE_HELD", False)
    _cut(frames, _cells(frames, []))


def test_mutation_a_split_part_keeping_the_mixed_rows_stamps(monkeypatch):
    monkeypatch.setattr(A, "A_SPLIT_RESTAMPED", False)
    frames = _frames()
    _st, ko_part, _rb, _ko = _cut(frames, _cells(frames))[0]["rows"]
    assert ko_part["stamps"][0]["doc"] == "steelhead_stamp"         # the kokanee carry the steelhead stamp: wrong


def test_mutation_a_split_part_keeping_the_mixed_rows_year(monkeypatch):
    monkeypatch.setattr(A, "A_SPLIT_YEARS_RECUT", False)
    frames = _frames()
    st, _ko_part, _rb, _ko = _cut(frames, _cells(frames))[0]["rows"]
    assert st["year"] == [["keep", 365]]                            # steelhead "keep" all year: wrong


def test_a_segment_with_no_table_keeps_its_frame():
    frames = _frames()
    got = A.cut_over(frames, [None, None], [LICENCE, LICENCE], [1, 200], None, {}, 0)
    assert got == frames


def test_the_kept_elsewhere_sentence_grammar_reads_every_form_and_refuses_today():
    assert W.parse_elsewhere_says(W.elsewhere_says(SAYS)) == ("rule", "This river", 2, "Trout and char", False, None)
    own = "Your own limit, counted across every water in Region 6 you fish today, lakes and streams alike."
    assert W.parse_elsewhere_says(W.elsewhere_says(own)) == ("own", "water in Region 6")
    for bad in (SAYS, "This river’s rule: 2 a day here. Trout you kept anywhere else count toward that 1.",
                "Keep a few elsewhere."):
        with pytest.raises(W.WordsError):
            W.parse_elsewhere_says(bad)


# ------------------------------------------------------------------------------------------------- the mini chain

class _Chain:
    def __init__(self, tmp_path_factory):
        d = mini_chain.deliver(tmp_path_factory)
        self.bundle = str(d / "bundle.sqlite")
        self.data, _guide = C.load_export(d)
        self.model = E.decode(json.loads((d / "ui-rules-answers.json").read_bytes()), self.data)
        self.pre = mini_chain.trout_inputs(tmp_path_factory)

    def prove(self, k, cells=None, frames=None) -> list:
        m = copy.copy(self.model)
        m.sections = dict(self.model.sections)
        if cells is not None:
            m.sections["trout"] = list(m.sections["trout"])
            m.sections["trout"][k] = cells
        if frames is not None:
            m.sections["rows"] = list(m.sections["rows"])
            m.sections["rows"][k] = frames
        return J.check_model(m, self.bundle, self.data, keys=[k], log=lambda *_: None)["problems"]

    def first(self, pred):
        for k, cells in enumerate(self.model.sections["trout"]):
            for j, c in enumerate(cells):
                if pred(k, j, c):
                    return k, j
        raise AssertionError("no cell of the mini chain has the planted feature")


@pytest.fixture(scope="module")
def chain(tmp_path_factory):
    return _Chain(tmp_path_factory)


def _says_cell(chain):
    return chain.first(lambda k, j, c: c is not None and any(not W.is_elsewhere_line(e) for e in c["table"]["elsewhere"]))


def test_every_covered_keep_row_has_its_sentence_on_the_shipped_table(chain):
    n = 0
    for k, cells in enumerate(chain.model.sections["trout"]):
        for j, c in enumerate(cells):
            if c is None:
                continue
            pre = chain.pre.sections["rows"][k][j]
            assert set(T.elsewhere_said(pre)) <= set(c["table"]["elsewhere"]), (k, j)
            n += sum(1 for e in c["table"]["elsewhere"] if not W.is_elsewhere_line(e))
    assert n > 0


def test_mutation_j_fails_a_table_that_lost_its_sentence(chain, monkeypatch):
    """Finding 2 (the reviewer's plant: an empty `elsewhere` passed the gate): J fails it, and a sentence stating another
    number; the pin off silences both."""
    k, j = _says_cell(chain)
    cells = copy.deepcopy(chain.model.sections["trout"][k])
    cells[j]["table"]["elsewhere"] = ()
    assert any("no kept-elsewhere sentence" in p for p in chain.prove(k, cells))
    wrong = copy.deepcopy(chain.model.sections["trout"][k])
    wrong[j]["table"]["elsewhere"] = tuple(e.replace("2 a day here", "3 a day here").replace("those 2", "those 3")
                                           for e in wrong[j]["table"]["elsewhere"])
    assert any("no covered keep row keeps 3 a day" in p for p in chain.prove(k, wrong))
    monkeypatch.setattr(J, "A_ELSEWHERE_SAID_PROOF", False)
    assert not any("kept-elsewhere sentence" in p for p in chain.prove(k, cells))


def test_the_shipped_page_possession_is_proved_over_every_frame(chain):
    got = J.check_model(chain.model, chain.bundle, chain.data, log=lambda *_: None)
    assert got["problems"] == []
    assert sum(1 for per in chain.model.sections["rows"] for f in per if f.get("page_possession")) > 0


@pytest.mark.parametrize("plant,mark", [
    (lambda pp: pp["lead"].update(value="2 days"), "the frame's possession roles give"),
    (lambda pp: pp["lead"].update(says="Possession: 3 days’ quota."), "the frame's possession roles give"),
    (lambda pp: pp["also"].append({"says": "Possession: x.", "src": pp["lead"]["src"]}), "the frame's possession"),
], ids=["value", "sentence", "extra-also"])
def test_mutation_a_wrong_page_possession_fails_the_gate(chain, monkeypatch, plant, mark):
    """Finding 4 (the reviewer's plant: value "2 days" passed): the value words, the sentence, the also-lines, each
    against the frame's own roles and the bundle's rules. The pin off silences it."""
    k, j = chain.first(lambda k, j, c: (chain.model.sections["rows"][k][j].get("page_possession") or {}).get("lead"))
    frames = copy.deepcopy(chain.model.sections["rows"][k])
    plant(frames[j]["page_possession"])
    assert any(mark in p for p in chain.prove(k, frames=frames))
    monkeypatch.setattr(J, "A_PAGE_POSSESSION_PROOF", False)
    assert not any("page_possession" in p for p in chain.prove(k, frames=frames))


def test_mutation_a_lost_page_possession_fails_the_gate(chain):
    k, j = chain.first(lambda k, j, c: chain.model.sections["rows"][k][j].get("page_possession"))
    frames = copy.deepcopy(chain.model.sections["rows"][k])
    frames[j]["page_possession"] = None
    assert any("the frame's possession roles give" in p for p in chain.prove(k, frames=frames))


def test_mutation_the_cut_is_proved_on_every_frame(chain, monkeypatch):
    """Finding 5: a covered row that kept its line, an uncovered keep row (a NULL cell's) that lost it — each fails J's
    cut proof; the pin off silences it."""
    k, j = chain.first(lambda k, j, c: c is not None and any(
        chain.model.sections["rows"][k][j]["rows"][i].get("kind") == "keep" for i in c["covers"]))
    frames = copy.deepcopy(chain.model.sections["rows"][k])
    i = next(i for i in chain.model.sections["trout"][k][j]["covers"] if frames[j]["rows"][i]["kind"] == "keep")
    frames[j]["rows"][i]["elsewhere"] = {"says": SAYS, "ask": []}
    assert any("still carries its kept-elsewhere line" in p for p in chain.prove(k, frames=frames))
    # the same keep row under a NULL cell (no table: nothing covered) without its line
    cells = list(chain.model.sections["trout"][k])
    cells[j] = None
    assert any("lost its kept-elsewhere line" in p for p in chain.prove(k, cells=cells))
    monkeypatch.setattr(J, "A_CUT_PROOF", False)
    assert not any("kept-elsewhere line" in p for p in chain.prove(k, frames=frames))


def test_every_row_the_table_does_not_cover_is_the_cards_row(chain):
    """Finding 7 (spec L's test): over every frame of the mini chain, each row the table does not cover is the row the
    card made (its group and kept-elsewhere line kept); each covered row lost exactly `group` and `elsewhere`. The mini
    world's own groups are all trout (the planted kokanee group above holds the other side)."""
    n = {"covered": 0, "kept": 0, "parts": 0}
    for k, cells in enumerate(chain.model.sections["trout"]):
        for j, c in enumerate(cells):
            pre = chain.pre.sections["rows"][k][j]
            post = chain.model.sections["rows"][k][j]
            if c is None:
                assert post["rows"] == pre["rows"], (k, j)
                n["kept"] += len(pre["rows"])
                continue
            split, covers = T.split_mixed(pre)
            whole = {id(r) for r in pre["rows"]}
            for i, (a, b) in enumerate(zip(split["rows"], post["rows"])):
                drop = A.COVERED_DROP if i in covers else ()
                # a split part is stamped again for its own fish (finding 3): no steelhead stamp without steelhead
                part = id(a) not in whole
                skip = ("year", "stamps") if part else ("year",)
                assert {x: v for x, v in a.items() if x not in drop and x not in skip} == \
                    {x: v for x, v in b.items() if x not in skip}, (k, j, i)
                if part:
                    n["parts"] += 1
                    assert "ST" in b["all_members"] or not any(st["doc"] == "steelhead_stamp"
                                                               for st in b.get("stamps") or ()), (k, j, i)
                n["covered" if i in covers else "kept"] += 1
    assert n["covered"] > 0 and n["kept"] > 0 and n["parts"] > 0


def test_the_mini_world_splits_a_steelhead_and_white_sturgeon_release_row(chain, monkeypatch):
    """The app chat's cutover candidate found it on the live bundle: a release row of steelhead and another fish carries
    the steelhead presence line, and the split must leave it on the steelhead's part (`model.Row` refuses it on a row
    without ST). The mini world holds the case: Stub Creek's "Steelhead and white sturgeon catch and release" (mini
    corpus, `stub_creek.r3`). Mutation: the line copied onto the sturgeon part stops the build (the rows model refuses
    it)."""
    from pipeline.deliver.answers import licence as L
    from pipeline.deliver.answers import model as M
    from pipeline.deliver.answers.stamps import DEFAULT_PROFILE
    hits = []
    for k, cells in enumerate(chain.model.sections["trout"]):
        for j, c in enumerate(cells):
            pre = chain.pre.sections["rows"][k][j]
            if c is not None and any(r.get("presence") and set(r["all_members"]) == {"ST", "WSG"} for r in pre["rows"]):
                hits.append((k, j))
    assert hits, "the mini world holds no steelhead + sturgeon release row with the presence line under a table"
    k, j = hits[0]
    rows = chain.model.sections["rows"][k][j]["rows"]
    st = next(r for r in rows if r["all_members"] == ["ST"])
    ko = next(r for r in rows if r["all_members"] == ["WSG"] and r["kind"] == "release")
    assert st.get("presence") is not None and ko.get("presence") is None
    real = T._only

    def keeps_presence(row, keep):
        o = real(row, keep)
        if row.get("presence") is not None:
            o["presence"] = row["presence"]
        return o
    monkeypatch.setattr(T, "_only", keeps_presence)
    pix = L.profile_index(DEFAULT_PROFILE)
    cut = A.cut_over(chain.pre.sections["rows"][k], chain.model.sections["trout"][k], chain.pre.sections["licence"][k],
                     chain.model.segments[k], chain.model.moments[k], L.document_names(chain.bundle), pix)
    with pytest.raises(M.ShapeError, match="presence"):
        M.validate_section("rows", cut)


# ------------------------------------------- Region 8's brook trout from streams (the book, p.68; the live J's 13 keys)

def _r8_pool():
    import types
    return types.SimpleNamespace(f={"water": "stream", "take": 20}, x={"entry_name": "Region 8"},
                                 key="z8:daily_quotas::trout_char_quota.r5", entry_id="z8:daily_quotas")


def _r8_elsewhere(monkeypatch):
    from pipeline.deliver.answers.rows import groups as G
    monkeypatch.setattr(G, "over_elsewhere", lambda *a, **k: [])
    pool = _r8_pool()
    row = {"daily": 20, "pool": pool, "narrow": None, "members": ["EB"], "kind": "keep"}
    sc = {"of": "region", "share": False, "apart": False, "narrow_all": False}
    model = {"R": {"EB": {"H": None, "W": {"status": "keep"}}}}
    return G.elsewhere_of(None, model, row, sc, pool, None)


def test_a_one_water_pool_says_its_water_and_keeps_on_that_water(monkeypatch):
    """p.68 "And you may retain: 20 brook trout from streams": only brook trout kept today from Region 8 STREAMS count
    toward the 20 — the sentence says "every Region 8 stream" (the share's own form, P44) and the worked line keeps them
    "on another stream", never "elsewhere" (the engine and J hold 2 kept on another stream to 18 more here). Mutation:
    the old words ("every water in Region 8", "Kept 2 elsewhere today?")."""
    from pipeline.deliver.answers.rows import groups as G
    got = _r8_elsewhere(monkeypatch)
    assert got["says"] == "Your own limit, counted across every Region 8 stream you fish today."
    assert [(x["q"], x["a"], x["at"]) for x in got["lines"]] == [("Kept 2 on another stream?", "Keep 18 more here",
                                                                   "stream")]
    assert W.parse_elsewhere_says(W.elsewhere_says(got["says"])) == ("own", "Region 8 stream")
    assert W.parse_elsewhere(W.elsewhere_line(got["lines"][0]["q"], got["lines"][0]["a"]))[0] == ("kept", 2, "stream")
    monkeypatch.setattr(G, "A_ONE_WATER_POOL_SAYS_ITS_WATER", False)
    old = _r8_elsewhere(monkeypatch)
    assert old["says"] == "Your own limit, counted across every water in Region 8 you fish today."
    assert old["lines"][0]["q"] == "Kept 2 elsewhere today?"


def test_a_fish_kept_elsewhere_is_exempt_from_the_zone_rules_its_own_zone_rule_lifts(monkeypatch):
    """Inkaneep Creek (Region 8), day 1: after 2 brook trout kept on another Region 8 stream, the book still allows 4
    other trout here — the brook trout are "counted apart from the trout/char quota" (the 20's `exempts` names the 5 and
    the 4 from streams for brook trout). The engine's kept-elsewhere path skips a zone rule a fish is exempt from by an
    in-force zone rule that applies to it (`Engine._zone_exempt`), in `counts` and in `checks`. Mutation: the pin off
    counts the brook trout toward the 4 again (2 more here, not 4)."""
    import types
    from pipeline.deliver.answers import engine as EN

    def rule(rid, species, take, water=None, exempts=()):
        return types.SimpleNamespace(entry_id="z8:daily_quotas", rule_id=rid, key=f"z8:daily_quotas::{rid}",
                                     species=species, except_=(), x={}, f={"take": take, "water": water,
                                                                          "exempts": list(exempts)})
    ex = [{"entry_id": "z8:daily_quotas", "rule_id": r, "species": ["EB"]}
          for r in ("trout_char_quota.r1", "trout_char_quota.r3")]
    total = rule("trout_char_quota.r1", ["EB", "RB", "CT"], 5)
    share = rule("trout_char_quota.r3", ["EB", "RB", "CT"], 4, "stream")
    brook = rule("trout_char_quota.r5", ["EB"], 20, "stream", ex)
    E = EN.Engine.__new__(EN.Engine)
    E.P = types.SimpleNamespace(kind="stream")
    E.zone = [total, share, brook]
    E._zone = {id(r) for r in E.zone}
    eb, rb = ("EB", "wild", 30.0, "stream"), ("RB", "wild", 30.0, "stream")
    assert not E.counts(share, eb) and not E.counts(total, eb) and E.counts(brook, eb)
    assert E.counts(share, rb) and E.counts(total, rb)
    assert not E.checks(share, eb) and E.checks(brook, eb)
    lake_eb = ("EB", "wild", 30.0, "lake")                         # the 20 is from streams: no exemption at a lake
    assert E.counts(total, lake_eb)
    monkeypatch.setattr(EN, "A_ZONE_EXEMPTS_ELSEWHERE", False)
    assert E.counts(share, eb) and E.checks(share, eb)
