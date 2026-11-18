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


#: every scope label with a qualifier the live build says (T810 L, the coordinator 2026-11-18: "counted across every
#: Region 1, excluding Haida Gwaii stream you fish" spliced the label mid-sentence) -> its stream and its water sentence
QUALIFIED = {
    "Region 1, excluding Haida Gwaii": (
        "Your own limit, counted across every stream in Region 1 (excluding Haida Gwaii) you fish today.",
        "Your own limit, counted across every water in Region 1 (excluding Haida Gwaii) you fish today, lakes and "
        "streams alike."),
    "Haida Gwaii (MUs 6-12, 6-13)": (
        "Your own limit, counted across every stream in Haida Gwaii (MUs 6-12, 6-13) you fish today.",
        "Your own limit, counted across every water in Haida Gwaii (MUs 6-12, 6-13) you fish today, lakes and streams "
        "alike."),
    "LIARD RIVER WATERSHED (see map on page 63)": (
        "Your own limit, counted across every stream in LIARD RIVER WATERSHED (see map on page 63) you fish today.",
        "Your own limit, counted across every water in LIARD RIVER WATERSHED (see map on page 63) you fish today, lakes "
        "and streams alike."),
    "BOWRON LAKE Park waters other than Bowron Lake": (
        "Your own limit, counted across every stream in BOWRON LAKE Park (other than Bowron Lake) you fish today.",
        "Your own limit, counted across every water in BOWRON LAKE Park (other than Bowron Lake) you fish today, lakes "
        "and streams alike."),
    "Region 2": ("Your own limit, counted across every Region 2 stream you fish today.",
                 "Your own limit, counted across every water in Region 2 you fish today, lakes and streams alike."),
}


@pytest.mark.parametrize("label", sorted(QUALIFIED))
def test_a_scope_label_with_a_qualifier_is_never_spliced_mid_sentence(monkeypatch, label):
    """Each label's kept-elsewhere sentence, a pool of streams and a pool of every water: the qualifier stays in brackets
    after the name ("every stream in Region 1 (excluding Haida Gwaii) you fish"); a plain name keeps its short form
    ("every Region 2 stream"). G's grammar reads each back, its scope whole."""
    import types
    from pipeline.deliver.answers.rows import groups as G
    monkeypatch.setattr(G, "over_elsewhere", lambda *a, **k: [])
    stream, water = QUALIFIED[label]
    for f, want in (({"water": "stream", "take": 20}, stream), ({"take": 20}, water)):
        pool = types.SimpleNamespace(f=f, x={"entry_name": label}, key="z:r", entry_id="z")
        row = {"daily": 20, "pool": pool, "narrow": None, "members": ["EB"], "kind": "keep"}
        sc = {"of": "region", "share": False, "apart": False, "narrow_all": False}
        got = G.elsewhere_of(None, {"R": {"EB": {"H": None, "W": {"status": "keep"}}}}, row, sc, pool, None)
        assert got["says"] == want
        scope = W.parse_elsewhere_says(W.elsewhere_says(got["says"]))[1]
        assert scope == want.split("counted across every ")[1].split(" you fish")[0]
    assert ", excluding" not in stream and " waters other than" not in stream


def test_a_qualified_total_takes_no_possessive():
    """"They also count toward Region 1, excluding Haida Gwaii’s 4 trout a day" reads "… toward the 4 trout a day across
    Region 1 (excluding Haida Gwaii)"; a plain name keeps "Zone A’s"."""
    from pipeline.deliver.answers.rows import groups as G
    assert G.scope_words("Region 1, excluding Haida Gwaii") == "Region 1 (excluding Haida Gwaii)"
    assert G.scope_words("Zone A") == "Zone A"
    assert G.every_of("Regions 2 and 8", "stream", both=True) == "every stream in Regions 2 and 8"


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


# ------------------------------------------------- the owner's 23:29 / 23:32 / 23:35 answers (labels, inner band, words)

def test_a_list_of_two_subjects_labels_every_line_and_keeps_each_together(monkeypatch):
    """Owner 23:29 ("Yes, name the fish"): more than one subject -> every line starts with its subject, each subject's
    lines together in today's order; one subject -> no labels. Mutation: the pin off leaves them unlabelled."""
    from pipeline.deliver.answers.trout import table as TT
    pool = {"kind": "keep", "members": ["RB", "CT"], "elsewhere": {
        "says": "Your own limit, counted across every water in Zone A you fish today, lakes and streams alike.",
        "ask": []}, "group": {"kinds": ["RB", "CT"], "worked": [
            {"q": "Kept 2 elsewhere today?", "a": "Keep 3 more here", "at": "elsewhere"}]}}
    brook = {"kind": "keep", "members": ["EB"], "elsewhere": {
        "says": "This lake’s rule: 10 a day here. Brook trout you kept anywhere else today count toward those 10.",
        "ask": [["Kept 2 elsewhere today?", "Keep 8 more here"]]}}
    got = TT.elsewhere_lines([pool, brook], [pool["group"]])
    assert got == [
        "Trout and char: Your own limit, counted across every water in Zone A you fish, lakes and streams alike.",
        "Trout and char: Kept 2 elsewhere? Keep 3 more here.",
        "Brook trout: This lake’s rule: 10 a day here. Brook trout you kept anywhere else count toward those 10.",
        "Brook trout: Kept 2 elsewhere? Keep 8 more here."]
    for t in got:                                  # every labelled line reads back
        sub, rest = W.split_subject(t)
        assert sub in ("Trout and char", "Brook trout")
        (W.parse_elsewhere if W.is_elsewhere_line(t) else W.parse_elsewhere_says)(t)
    assert TT.elsewhere_lines([pool], [pool["group"]])[0].startswith("Your own limit")      # one subject: none
    monkeypatch.setattr(TT, "A_ELSEWHERE_SUBJECT_NAMED", False)
    assert not any(W.split_subject(t)[0] for t in TT.elsewhere_lines([pool, brook], [pool["group"]]))


def test_mutation_j_fails_a_label_where_the_list_has_one_subject(chain):
    """J's label rule on the shipped tables: a list of one subject carrying a label fails (the two-subject plants are on
    the live waters below)."""
    k, j = _says_cell(chain)
    cells = copy.deepcopy(chain.model.sections["trout"][k])
    cells[j]["table"]["elsewhere"] = tuple(W.elsewhere_labeled("Brook trout", e) for e in cells[j]["table"]["elsewhere"])
    assert any("carries a label" in p for p in chain.prove(k, cells))


def test_the_top_line_and_fold_name_the_fish_and_the_action(chain):
    """Owner 23:35 ("Keep 2 trout and char today"; the fold "Make it match other rows in table"): every shipped top line
    names the fish and the action, and a one-number fold is the Other fish rows' "N a day"."""
    n = 0
    for cells in chain.model.sections["trout"]:
        for c in cells:
            if c is None:
                continue
            lead, fold = c["table"]["top"]["lead"], c["table"]["fold"]["value"]
            assert lead.startswith(("Keep ", "Release every trout and char", "Closed for trout and char")), lead
            assert "Up to" not in lead
            if W.parse_fold(fold)[0] == "up_to" and len(W.parse_fold(fold)[1]) == 1:
                assert fold.endswith(" a day"), fold
            n += 1
    assert n > 0


def test_mutation_j_fails_a_top_line_number_the_engine_does_not_keep(chain):
    k, j = chain.first(lambda k, j, c: c is not None and W.parse_top(c["table"]["top"]["lead"])[0] == "up_to")
    cells = copy.deepcopy(chain.model.sections["trout"][k])
    n = W.parse_top(cells[j]["table"]["top"]["lead"])[1][0]
    cells[j]["table"]["top"]["lead"] = W.top_up_to(n + 3)
    assert chain.prove(k, cells)


# --- the live waters (the owner's pictures): Kathie, Canuck, Similkameen; the Fraser at Mission -----------------------

def _live_tables(request, names, md=(7, 15)):
    from pipeline.deliver import calendar as K
    from pipeline.tests import under_test
    bundle, export_dir = under_test.bundle(request), under_test.export_dir(request)
    data, guide = C.load_export(export_dir)
    items = {w["name"]: i for i, w in data["waters"].items() if w.get("name") in names}
    m = A.build(str(bundle), export_dir, workers=1, items=sorted(items.values()), export=(data, guide),
                log=lambda *_: None)
    out = {}
    for name, item in items.items():
        cells = [m.sections["trout"][k][E.segment_index(m.segments[k], K.day_of(md), m.moments[k], "Wednesday", inside=False)]
                 for k in dict.fromkeys(x for x in m.parts[item] if x is not None)]
        out[name] = next((c for c in cells if c is not None), None)
        out[name + " (all parts)"] = cells
    return out, m, str(bundle), data


@pytest.mark.needs_bundle
def test_kathie_canuck_and_similkameen_name_each_subject(request):
    got, m, bundle, data = _live_tables(request, ("Kathie Lake", "Canuck Lake", "Similkameen River"))
    assert list(got["Kathie Lake"]["table"]["elsewhere"]) == [
        "Trout and char: Your own limit, counted across every water in Zone A you fish, lakes and streams alike.",
        "Brook trout: This lake’s rule: 10 a day here. Brook trout you kept anywhere else count toward those 10. These "
        "fish are counted apart from the region’s total: they don’t use it up. They also count toward Zone A’s 5 trout "
        "and char a day."]
    assert list(got["Canuck Lake"]["table"]["elsewhere"]) == [
        "Trout and char: Your own limit, counted across every water in Region 4 you fish, lakes and streams alike.",
        "Brook trout: This lake’s rule: 20 a day here. Brook trout you kept anywhere else count toward those 20. These "
        "fish are counted apart from the region’s total: they don’t use it up. They also count toward Region 4’s 5 "
        "trout and char a day."]
    assert got["Kathie Lake"]["table"]["top"]["lead"] == "Keep 5 trout and char {day}, or 10 if they’re brook trout"


@pytest.mark.needs_bundle
def test_mutation_j_fails_a_dropped_label_on_kathie(request):
    got, m, bundle, data = _live_tables(request, ("Kathie Lake",))
    k = next(k for k, cells in enumerate(m.sections["trout"]) if any(c is not None and c == got["Kathie Lake"]
                                                                     for c in cells))
    cells = copy.deepcopy(m.sections["trout"][k])
    j = next(j for j, c in enumerate(cells) if c == got["Kathie Lake"])
    cells[j]["table"]["elsewhere"] = (W.split_subject(cells[j]["table"]["elsewhere"][0])[1],
                                      *cells[j]["table"]["elsewhere"][1:])
    m.sections["trout"] = list(m.sections["trout"])
    m.sections["trout"][k] = cells
    probs = J.check_model(m, bundle, data, keys=[k], log=lambda *_: None)["problems"]
    assert any("a line without its subject" in p for p in probs), probs


def _one_box(t, keys):
    b = next(b for b in t["table"]["boxes"] if b["kind"] == "keep" and set(keys) <= {r["key"] for r in b["rows"]})
    return b, [r["key"] for r in b["rows"]]


def _mission(got):                                     # the Region 2 part at Mission: a "Keep 2 in all" box of both
    return next(c for c in got["Fraser River (all parts)"] if c is not None and any(
        b["kind"] == "keep" and b["head"] == "Keep 2 in all" and b["label"] == W.HATCHERY_ONLY
        and {"DV", "LT"} <= {r["key"] for r in b["rows"]} for b in c["table"]["boxes"]))


@pytest.mark.needs_bundle
def test_the_fraser_at_mission_states_the_dolly_varden_and_lake_trout_count_once(request, monkeypatch):
    """Owner 23:32 ("One shared 'only 1' box"), and 2026-11-18 on the simplify examples ("fraser also never simplified
    that but then removed the text that allowed a user to infer it"): Region 2's Dolly Varden and lake trout, both kept
    only at 60 cm or longer under the strip's "only 1 over 50 cm", get ONE band "only 1 between them" (the conclusion
    stated once, as Kamloops Lake shows it); each row keeps "uses the 1 over 50 cm (20 in)" as the reason; no row says
    "only 1, counting any …". Mutation: the switch off (the coordinator's first hold) says the count on each row."""
    from pipeline.deliver.answers.trout import table as TT
    got, *_ = _live_tables(request, ("Fraser River",))
    b, keys = _one_box(_mission(got), ("DV", "LT"))
    i, j = keys.index("DV"), keys.index("LT")
    assert abs(i - j) == 1
    band = next(x for x in b["bands"] if x["from"] == min(i, j))
    assert (band["to"], band["text"], band["kind"]) == (max(i, j), "counts toward the 2 · only 1 between them", "shared")
    assert not any(t.startswith("uses the ") for r in b["rows"] for t in r["lines"])   # owner 01:07: dropped
    assert not any(x.startswith("only 1, counting any") for r in b["rows"] for x in r["lines"])
    monkeypatch.setattr(TT, "A_BAND_NAMES_THE_TOTAL", False)       # owner 00:52's "counts toward the 2 · " pin
    got, *_ = _live_tables(request, ("Fraser River",))
    b, keys = _one_box(_mission(got), ("DV", "LT"))
    assert "only 1 between them" in [x["text"] for x in b["bands"]]
    monkeypatch.setattr(TT, "INNER_BAND_WHEN_THE_STRIP_SAYS_IT", False)
    got, *_ = _live_tables(request, ("Fraser River",))
    b, keys = _one_box(_mission(got), ("DV", "LT"))
    assert all(x["kind"] == "upto" for x in b["bands"]), b["bands"]
    assert sum(x.startswith("only 1, counting any") for r in b["rows"] for x in r["lines"]) == 2


@pytest.mark.needs_bundle
@pytest.mark.parametrize("on", [False, True], ids=["off (default)", "on"])
def test_the_fraser_proves_with_the_uses_the_line_either_way(request, monkeypatch, on):
    """(Owner 00:52 / 01:07: dropped, the switch off by default) the Fraser at Mission says "uses the 1 over 50 cm" only
    with the switch on, and J proves every table of the river either way."""
    from pipeline.deliver.answers.trout import table as TT
    monkeypatch.setattr(TT, "USES_THE_LINE", on)
    got, m, bundle, data = _live_tables(request, ("Fraser River",))
    b, _keys = _one_box(_mission(got), ("DV", "LT"))
    assert any(t.startswith("uses the ") for r in b["rows"] for t in r["lines"]) == on
    ks = sorted({k for k, cells in enumerate(m.sections["trout"]) if any(c is not None for c in cells)})
    assert J.check_model(m, bundle, data, keys=ks, log=lambda *_: None)["problems"] == []


@pytest.mark.needs_bundle
def test_the_vedder_orders_ungrouped_first_and_nests_the_inner_group_last(request, monkeypatch):
    """Owner 2026-11-18 00:52 ("at Vedder the DV/LT 'only 1' group cut through '2 of these 6'"): Jan 1, the rainbow trout
    no band groups stands first; the band's six rows follow, its Dolly Varden and lake trout (their own "only 1"
    between them) last and together. Mutation: the pin off puts the band's rows back in the build's order."""
    from pipeline.deliver.answers.trout import table as TT
    monkeypatch.setattr(TT, "A_GROUPS_LAST", True)              # HELD OFF (owner 01:07): the switch's own case
    got, *_ = _live_tables(request, ("Vedder River",), md=(1, 1))
    b = next(b for b in got["Vedder River"]["table"]["boxes"] if b["kind"] == "keep")
    keys = [r["key"] for r in b["rows"]]
    assert keys[0] == "RB" and keys[-2:] == ["DV", "LT"], keys
    assert [(x["from"], x["to"]) for x in b["bands"]] == [(0, 0), (1, 6)]
    monkeypatch.setattr(TT, "A_GROUPS_LAST", False)
    got, *_ = _live_tables(request, ("Vedder River",), md=(1, 1))
    b = next(b for b in got["Vedder River"]["table"]["boxes"] if b["kind"] == "keep")
    assert [r["key"] for r in b["rows"]][-2:] != ["DV", "LT"]


@pytest.mark.needs_bundle
def test_rainbow_stands_above_steelhead_where_they_read_alike(request, monkeypatch):
    """Owner 2026-11-18 01:07: at Mission the Fraser's rainbow trout stand just above its steelhead (both "counts toward
    the 2"); the Vedder's rainbow, under a rule of its own ("4 if they're hatchery rainbow trout"), stays where it is.
    Mutation: the pin off leaves the build's order."""
    from pipeline.deliver.answers.trout import table as TT
    got, *_ = _live_tables(request, ("Fraser River",))
    b, keys = _one_box(_mission(got), ("RB", "ST"))
    assert keys.index("RB") + 1 == keys.index("ST"), keys
    monkeypatch.setattr(TT, "A_RAINBOW_ABOVE_STEELHEAD", False)
    got, *_ = _live_tables(request, ("Fraser River",))
    b, keys = _one_box(_mission(got), ("RB", "ST"))
    assert keys.index("RB") > keys.index("ST"), keys


def _bowron(got):                                      # Istsi Creek, Jan 1-Mar 31: Bowron Lake Park's 1 trout/char
    return next(c for c in got["Istsi Creek (all parts)"] if c is not None)


@pytest.mark.needs_bundle
def test_bowron_park_creeks_say_their_possession_once(request, monkeypatch):
    """Owner 2026-11-18 (Bowron Lake Park's creeks): the park's flat 1 (p.43) and Region 7A's lake trout "1 daily quota"
    (p.58) each come to the day's 1: ONE box line "In your possession: just 1, the same as the day"; no row says its own
    possession. J reads it back against both rules. Mutation: the pin off says each row's own line again."""
    from pipeline.deliver.answers.trout import table as TT
    got, m, bundle, data = _live_tables(request, ("Istsi Creek",), md=(2, 15))
    t = _bowron(got)
    b = next(b for b in t["table"]["boxes"] if b["kind"] == "keep")
    assert (b["head"], b["possession"]) == ("Keep 1 in all", "In your possession: just 1, the same as the day")
    assert "LT" in {r["key"] for r in b["rows"]}
    assert not any(x.startswith("in your possession") for r in b["rows"] for x in r["lines"])
    k = next(k for k, cells in enumerate(m.sections["trout"]) if any(c is not None and c == t for c in cells))
    assert J.check_model(m, bundle, data, keys=[k], log=lambda *_: None)["problems"] == []
    # a wrong number on the line fails the proof
    cells = copy.deepcopy(m.sections["trout"][k])
    jj = next(jj for jj, c in enumerate(cells) if c == t)
    next(b for b in cells[jj]["table"]["boxes"] if b["kind"] == "keep")["possession"] = \
        "In your possession: just 2, the same as the day"
    m.sections["trout"] = list(m.sections["trout"])
    m.sections["trout"][k] = cells
    assert J.check_model(m, bundle, data, keys=[k], log=lambda *_: None)["problems"]
    monkeypatch.setattr(TT, "A_POSSESSION_SAME_AS_THE_DAY", False)
    got, *_ = _live_tables(request, ("Istsi Creek",), md=(2, 15))
    b = next(b for b in _bowron(got)["table"]["boxes"] if b["kind"] == "keep")
    assert b["possession"] is None
    assert any(x.startswith("in your possession") for r in b["rows"] for x in r["lines"])


@pytest.mark.needs_bundle
def test_chilliwack_lake_drops_only_4_wild_behind_its_switch(request, monkeypatch):
    """Owner 2026-11-18, PENDING one more picture: Chilliwack Lake's "only 4 wild" inside "Keep 4 in all" never limits
    anything. Off by default (the rows say it); the switch on drops it and the proof still holds."""
    from pipeline.deliver.answers.trout import table as TT
    got, m, bundle, data = _live_tables(request, ("Chilliwack Lake",))
    rows = [r for b in got["Chilliwack Lake"]["table"]["boxes"] for r in b["rows"]]
    assert any("only 4 wild" in r["lines"] for r in rows)
    monkeypatch.setattr(TT, "WILD_QUOTA_AT_THE_BOX_NUMBER_UNSAID", True)
    got, m, bundle, data = _live_tables(request, ("Chilliwack Lake",))
    t = got["Chilliwack Lake"]
    assert not any("only 4 wild" in r["lines"] for b in t["table"]["boxes"] for r in b["rows"])
    assert {r["key"] for b in t["table"]["boxes"] for r in b["rows"]} >= {"RB/wild", "RB/hatchery"}
    ks = sorted({k for k, cells in enumerate(m.sections["trout"]) if any(c is not None for c in cells)})
    assert J.check_model(m, bundle, data, keys=ks, log=lambda *_: None)["problems"] == []


@pytest.mark.needs_bundle
def test_big_bend_creek_draws_one_shared_band_the_strip_does_not_say(request, monkeypatch):
    """Owner 23:32, built where nothing else says it (a1-real: Big Bend Creek, the Nechako and the Liard): brown,
    cutthroat, rainbow trout and steelhead, all kept from 30 cm, share "1 total from these 4" inside "Keep 2 in all"; no
    row says "only 1, counting any …". Mutation: the pin off says the count on each row again."""
    from pipeline.deliver.answers.trout import table as TT
    four = ("GB", "CT", "RB", "ST")

    def big_bend(got):
        return next(c for c in got["Big Bend Creek (all parts)"] if c is not None and any(
            b["kind"] == "keep" and b["head"] == "Keep 2 in all" and set(four) <= {r["key"] for r in b["rows"]}
            for b in c["table"]["boxes"]))
    got, *_ = _live_tables(request, ("Big Bend Creek",))
    b, keys = _one_box(big_bend(got), four)
    at = sorted(keys.index(k) for k in four)
    assert at == list(range(at[0], at[0] + 4))
    assert [x for x in b["bands"] if x["from"] == at[0]] == [
        {"from": at[0], "to": at[-1], "text": "counts toward the 2 · 1 total from these 4", "kind": "shared"}]
    assert not any(x.startswith("only 1, counting any") for r in b["rows"] for x in r["lines"])
    monkeypatch.setattr(TT, "A_INNER_CAP_IS_A_BAND", False)
    got, *_ = _live_tables(request, ("Big Bend Creek",))
    b, _keys = _one_box(big_bend(got), four)
    assert any(x.startswith("only 1, counting any") for r in b["rows"] for x in r["lines"])


def _r4(got):                                          # a Region 4 stream's default, Jun 15-Oct 31: "Keep 2 in all"
    return next(c for c in got["Bugaboo Creek (all parts)"] if c is not None and any(
        b["kind"] == "keep" and {"CT", "RB"} <= {r["key"] for r in b["rows"]} for b in c["table"]["boxes"]))


@pytest.mark.needs_bundle
def test_region_4_cutthroat_and_rainbow_share_one_size_band(request, monkeypatch):
    """Owner 2026-11-18 on Region 4's cutthroat and rainbow over 50 cm ("A but still has to mention the overall limit"):
    the two rows together under ONE band "counts toward the 2 · only 1 over 50 cm (20 in) between them" (`Band.kind`
    "size", trout 2); neither row says "only 1 over 50 cm (20 in), counting any …"; the box's "Keep 2 in all" stays.
    Mutation: the pin off says the cap on each row again."""
    from pipeline.deliver.answers.trout import table as TT
    got, *_ = _live_tables(request, ("Bugaboo Creek",))
    t = _r4(got)
    b, keys = _one_box(t, ("CT", "RB"))
    i, j = keys.index("CT"), keys.index("RB")
    assert (b["head"], j - i) == ("Keep 2 in all", 1)
    assert [x for x in b["bands"] if x["from"] == i] == [
        {"from": i, "to": j, "text": "counts toward the 2 · only 1 over 50 cm (20 in) between them", "kind": "size"}]
    assert not any(" over 50 cm (20 in), counting any" in x for r in b["rows"] for x in r["lines"])
    assert [r["sheet"]["head"] for r in b["rows"][i:j + 1]] == [
        "Keep · only 1 over 50 cm (20 in) between them · counts toward the 2"] * 2
    assert "Only 1 over 50 cm (20 in) between them: this fish and rainbow trout." in [
        h["t"] for h in b["rows"][i]["sheet"]["here"]]
    from pipeline.deliver.answers.model.trout import TroutFrame
    TroutFrame.model_validate_json(json.dumps(t, ensure_ascii=False))
    monkeypatch.setattr(TT, "A_SIZE_CAP_IS_A_BAND", False)
    got, *_ = _live_tables(request, ("Bugaboo Creek",))
    b, _keys = _one_box(_r4(got), ("CT", "RB"))
    assert not any(x["kind"] == "size" for x in b["bands"])
    assert sum(" over 50 cm (20 in), counting any" in x for r in b["rows"] for x in r["lines"]) == 2


@pytest.mark.needs_bundle
@pytest.mark.parametrize("plant", ["one higher", "gone"])
def test_mutation_j_fails_a_wrong_or_missing_size_band(request, plant):
    """J reads the size band as a limit ("these rows, over 50 cm: at most 1"): the band set one higher, or removed (its
    rows each "counts toward the 2"), lets through two cutthroat or rainbow over 50 cm the engine refuses."""
    got, m, bundle, data = _live_tables(request, ("Bugaboo Creek",))
    t = _r4(got)
    k = next(k for k, cells in enumerate(m.sections["trout"]) if any(c is not None and c == t for c in cells))
    cells = copy.deepcopy(m.sections["trout"][k])
    assert J.check_model(m, bundle, data, keys=[k], log=lambda *_: None)["problems"] == []
    j = next(j for j, c in enumerate(cells) if c == t)
    b = next(b for b in cells[j]["table"]["boxes"] if any(x["kind"] == "size" for x in b["bands"]))
    x = next(x for x in b["bands"] if x["kind"] == "size")
    if plant == "one higher":
        x["text"] = W.band_size(2, 2, 50, 2)
        for r in b["rows"][x["from"]:x["to"] + 1]:
            r["sheet"]["head"] = W.sheet_keep(x["text"], 2)
            r["sheet"]["here"] = [h for h in r["sheet"]["here"] if not h["t"].startswith("Only 1 over")]
    else:
        at = b["bands"].index(x)
        b["bands"][at:at + 1] = [{"from": i, "to": i, "text": W.band_upto(2), "kind": "upto"}
                                 for i in range(x["from"], x["to"] + 1)]
        for r in b["rows"][x["from"]:x["to"] + 1]:
            r["sheet"]["head"] = W.sheet_keep(W.band_upto(2), 2)
            r["sheet"]["here"] = [h for h in r["sheet"]["here"] if not h["t"].startswith("Only 1 over")]
    m.sections["trout"] = list(m.sections["trout"])
    m.sections["trout"][k] = cells
    probs = J.check_model(m, bundle, data, keys=[k], log=lambda *_: None)["problems"]
    assert any("the engine refuses it" in p for p in probs), probs
