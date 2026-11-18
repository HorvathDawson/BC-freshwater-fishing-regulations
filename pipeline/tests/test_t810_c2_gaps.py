"""T810 L: the builder gaps the C2 proof bundle found (scratchpad/c2l-proof: rebuild3 atlas, the C2 corpus, T834/T17
code), pinned without data — each a focused frame or the mini world, each mutation-checked:

  the top line capped at its box (`table.A_TOP_NEVER_PAST_THE_BOX`; Andi Creek: "Keep 4", not 5);
  kept-elsewhere subjects per covered row (`table.A_GROUP_LINES_SAY_THEIR_ROWS_SUBJECT`; Kitimat, Humphrys);
  the share's lake line under a smaller cap, and the lifted-kind skip (`rows/groups.A_LAKE_LINE_UNDER_ITS_CAP`; Qualcho);
  the possession "measuring" clause (`table.A_POSSESSION_MEASURING_IS_A_CLAUSE`; Bennett Lake's lake trout).
"""
from __future__ import annotations

import copy
import types

import pytest

from pipeline.deliver.answers.trout import table as T
from pipeline.deliver.answers.trout import words as W


# --------------------------------------------------------------------------------------- the top line capped at its box

@pytest.fixture(scope="module")
def mini_body(tmp_path_factory):
    """A mini-world keep box with its group, built (`Body.build`), from the rows the trout builder reads."""
    from pipeline.tests import mini_chain
    m = mini_chain.trout_inputs(tmp_path_factory)
    _data, _guide, rules = mini_chain.export(tmp_path_factory)
    for k, cells in enumerate(m.sections["rows"]):
        for j, fr in enumerate(cells or ()):
            if fr is None or T.is_null(fr, m.sections["display"][k][j]):
                continue
            b = T.Body(T.split_mixed(fr)[0], rules, m.sections["licence"][k][j])
            b.build()
            keep = [x for x in b.boxes if x["kind"] == "keep"]
            if len(keep) == 1 and keep[0]["group"] is not None and keep[0]["first"] is None:
                return b
    raise AssertionError("the mini world has no one-keep-box table with a group")


@pytest.mark.parametrize("on", [True, False], ids=["capped", "mutation: the headline's own number"])
def test_the_top_line_never_says_more_than_its_box(mini_body, monkeypatch, on):
    """The group's headline counts each fish's own number, never "these fish together keep 1": with it 3 above the box
    (Andi Creek on the C2 bundle: 5 against "Keep 4 in all"), the top line says the box's number. Off: the headline's."""
    b = mini_body
    box = next(x for x in b.boxes if x["kind"] == "keep")
    saved = box["group"]
    box["group"] = copy.deepcopy(saved)
    box["group"]["headline"] = {**box["group"]["headline"], "n": box["n"] + 3, "also": []}
    try:
        monkeypatch.setattr(T, "A_TOP_NEVER_PAST_THE_BOX", on)
        lead, _fold = b._top()
    finally:
        box["group"] = saved
    n = W.parse_top_full(lead)[1]
    assert n == (box["n"] if on else box["n"] + 3), lead


# ---------------------------------------------------------------------------------- kept-elsewhere subjects per covered row

ZONE = "Your own limit, counted across every water in Zone A you fish, lakes and streams alike."


def test_a_groups_worked_lines_carry_their_rows_subject(monkeypatch):
    """Kitimat in winter: the box holds lake trout and hatchery rainbow; the rainbow row's group (its pool every trout
    and char) says its worked line under the row's own subject, "Rainbow trout", as J reads the subjects (one per
    covered keep row) — never "Trout and char". Off: the group's pool subject."""
    rows = [{"kind": "keep", "members": ["LT"], "elsewhere": {"says": ZONE, "ask": []}}]
    g = {"kinds": ["RB", "CT", "GB", "LT", "EB"], "worked": [{"q": "Kept 2 elsewhere?", "a": "Keep 3 more here",
                                                             "at": "elsewhere"}]}
    got = T.elsewhere_lines(rows, [(g, ["RB"])])
    assert got == [W.elsewhere_labeled("Lake trout", W.elsewhere_says(ZONE)),
                   W.elsewhere_labeled("Rainbow trout", W.elsewhere_line("Kept 2 elsewhere?", "Keep 3 more here"))]
    monkeypatch.setattr(T, "A_GROUP_LINES_SAY_THEIR_ROWS_SUBJECT", False)
    assert W.split_subject(T.elsewhere_lines(rows, [(g, ["RB"])])[-1])[0] == W.POOL_SUBJECT


# ------------------------------------------------------------------- the share's lake line under a smaller cap (Qualcho)

def _qualcho(monkeypatch, lifted=()):
    """A Region 5 stream in winter, focused: lake trout the one kind kept here, inside the 2 from streams of the 5 a day
    and under the char cap of 2; `lifted`: a kind a water's own quota lifts past the share."""
    from pipeline.deliver.answers.rows import groups as G
    monkeypatch.setattr(G, "over_elsewhere", lambda *a, **k: [])
    nw = types.SimpleNamespace(f={"water": "stream", "take": 2}, x={"entry_name": "Region 5"}, key="z5:share",
                               entry_id="z5:daily_quotas", species=["LT", "RB", "CT"], except_=())
    T_ = types.SimpleNamespace(f={"take": 5}, x={"entry_name": "Region 5"}, key="z5:total", entry_id="z5:daily_quotas")
    own = types.SimpleNamespace(f={"take": 3}, x={"entry_name": "this stream"}, key="w:own", entry_id="w", species=["LT"],
                                except_=())
    P = types.SimpleNamespace(cands=[], by_key={"w:own": own})
    grp = {"subcaps": [{"r": "z5:share", "water": "stream", "of": ["LT"], "take": 2, "over_cm": None,
                        "lifted": [{"by": "w:own", "members": ["LT"]}] if lifted else []},
                       {"r": "z5:char", "water": None, "of": ["LT", "DV", "EB"], "take": 2, "over_cm": None,
                        "lifted": []}],
           "kinds": ["LT"], "pool": "z5:total"}
    row = {"daily": 2, "pool": T_, "narrow": nw, "members": ["LT"], "kind": "keep"}
    sc = {"of": "region", "share": True, "apart": False, "narrow_all": False}
    model = {"R": {"LT": {"H": None, "W": {"status": "keep"}}}}
    got = G.elsewhere_of(P, model, row, sc, T_, grp)
    return [x for x in got["lines"] if x["q"].startswith("Kept 1 ") and x["q"].endswith("at a lake?")]


@pytest.mark.parametrize("on", [True, False], ids=["the fish named", "mutation: one line for any fish"])
def test_the_lake_line_names_the_capped_fish(monkeypatch, on):
    """One lake trout kept at a lake uses one of the char cap's 2: "Kept 1 lake trout at a lake? Keep 1 more here."; any
    other trout or char kept there leaves the share's 2: "Kept 1 other trout or char at a lake? Keep 2 more here." (the
    re-review of 3f273c172: one unnamed line read as any fish and understated). Each line reads back; off: one line, 2."""
    from pipeline.deliver.answers.rows import groups as G
    monkeypatch.setattr(G, "A_LAKE_LINE_UNDER_ITS_CAP", on)
    lines = _qualcho(monkeypatch)
    got = [(x["q"], x["here"]) for x in lines]
    if on:
        assert got == [("Kept 1 lake trout at a lake?", 1), ("Kept 1 other trout or char at a lake?", 2)], got
        assert [W.parse_elsewhere(W.elsewhere_line(x["q"], x["a"]))[0] for x in lines] == [
            ("kept", 1, "lake", ("LT",)), ("kept", 1, "lake", "other")]
        assert lines[1]["species"] and "LT" not in lines[1]["species"]
    else:
        assert got == [("Kept 1 at a lake?", 2)], got


def test_the_lake_line_leaves_a_lifted_kind_to_its_own_number(monkeypatch):
    """A kind a water's own quota lifts past the share is worked out from that quota (the lifted lake trout's 3): one
    line, never cut to the smaller cap less 1."""
    from pipeline.deliver.answers.rows import groups as G
    monkeypatch.setattr(G, "A_LAKE_LINE_UNDER_ITS_CAP", True)
    lines = _qualcho(monkeypatch, lifted=True)
    assert len(lines) == 1 and lines[0]["here"] != 1, lines


# --------------------------------------------------------------------------- the possession "measuring" clause (Bennett)

BENNETT = ("in your possession: no more than 2 lake trout, only 1 measuring 90 cm (35 in) or longer "
           "(fish at home don’t count)")


@pytest.mark.parametrize("on", [True, False], ids=["a clause", "mutation: a bare count"])
def test_a_measuring_possession_clause_is_said(on, monkeypatch):
    """Bennett Lake's lake trout: "no more than 2 …, only 1 measuring 90 cm (35 in) or longer" beside its own "only 2" is
    still said (the clause is law the row's 2 does not say). Off: taken for a bare 2, left unsaid (J's one baseline
    problem)."""
    monkeypatch.setattr(T, "A_POSSESSION_MEASURING_IS_A_CLAUSE", on)
    assert T.possession_says_more(BENNETT) is on
    assert T.possession_says_more("in your possession: no more than 2 lake trout (fish at home don’t count)") is False


# ------------------------------------------------------------- a planted T17 frame (the mini world's Fixture River)

def test_a_water_quota_no_larger_than_its_pool_sits_inside_it(tmp_path_factory, monkeypatch):
    """RULINGS T17 planted in the mini world: Fixture River's "Dolly Varden daily quota = 2" (fixture_river.r5) sits inside
    the river's own "Trout/char daily quota = 2": the Dolly Varden row "counts toward the 2" with no "only 2" (the
    Lardeau's bull trout 2 inside Region 4's 2 from streams, Madden Creek). J proves the table. Mutation: the same table
    with "only 2" planted on the Dolly Varden is still law (the stated-cap proof holds it exact) but no longer the T17
    form this test pins."""
    import json
    from pipeline.tests import mini_chain
    m = mini_chain.trout_inputs(tmp_path_factory)
    data, _guide, rules = mini_chain.export(tmp_path_factory)
    item = next(i for i, w in data["waters"].items() if w.get("name") == "Fixture River")
    k = next(x for x in m.parts[item] if x is not None)
    got = T.compose_key(m.sections["rows"][k], m.sections["display"][k], m.segments[k], m.moments[k], rules,
                        licences=m.sections["licence"][k])
    c = next(json.loads(json.dumps(f, ensure_ascii=False)) for f in got if f is not None)

    def t17(cell) -> bool:
        b = next(b for b in cell["table"]["boxes"] if b["kind"] == "keep")
        i = next(i for i, r in enumerate(b["rows"]) if r["fish"] == "DV")
        band = next(x for x in b["bands"] if x["from"] <= i <= x["to"])
        return band["text"] == W.band_upto(W.parse_head(b["head"])[1]) and "only 2" not in b["rows"][i]["lines"]
    assert any(r["fish"] == "DV" for b in c["table"]["boxes"] if b["kind"] == "keep" for r in b["rows"])
    assert t17(c)
    bad = copy.deepcopy(c)
    b = next(b for b in bad["table"]["boxes"] if b["kind"] == "keep")
    i = next(i for i, r in enumerate(b["rows"]) if r["fish"] == "DV")
    b["bands"] = tuple({**x, "kind": "own", "text": W.band_in(W.parse_head(b["head"])[1], W.band_own(2))}
                       if x["from"] == x["to"] == i else x for x in b["bands"])
    assert not t17(bad)


class _Engine:
    """A focused engine: after one lake trout kept at a lake, 1 more here; after any other fish, 2 (Qualcho)."""
    def fish(self, kinds, places=("here",), origins=("hatchery", "wild")):
        return [(S, o, 30.0, p) for S in kinds for p in places for o in origins]

    def can_keep(self, got, f):
        return True

    def most(self, fish, base=(), cap=99):
        return 1 if base and base[0][0] == "LT" else 2


@pytest.mark.parametrize("on", [True, False], ids=["every fish", "mutation: some one fish"])
def test_j_holds_a_lake_line_for_every_fish_it_covers(monkeypatch, on):
    """J (no data, a focused engine): "Kept 1 at a lake? Keep 1 more here." covers every trout and char kept at a lake;
    it holds for a lake trout only, so J fails it. Off: true of some one fish of the box passes it."""
    from pipeline.deliver.answers.trout import check as J
    monkeypatch.setattr(J, "A_ELSEWHERE_EVERY_FISH", on)
    got = J._elsewhere_holds(_Engine(), ("kept", 1, "lake", None), ("more_tally", 1), ["LT"], "stream", "lake",
                             ["LT", "RB", "CT"], every=True)
    assert (got is True) is (not on), got
    # a line that names the lake trout holds
    assert J._elsewhere_holds(_Engine(), ("kept", 1, "lake", ("LT",)), ("more_tally", 1), ["LT"], "stream", "lake",
                              ["LT"]) is True


# --------------------------------------- every kept-elsewhere line exact for every fish (the re-review of 5788a7914)

class _Abies:
    """A focused engine for Abies Creek on the C2 bundle (Region 6, from Jul 2): after 2 lake trout or 2 Dolly Varden
    kept elsewhere, 2 more here (their own cap of 3); after 2 of any other trout or char, 3."""
    def __init__(self, *_a, **_k):
        pass

    def fish(self, kinds, places=("here",), origins=("hatchery", "wild")):
        return [(S, o, 30.0, p) for S in kinds for p in places for o in origins]

    def can_keep(self, got, f):
        return f[3] == "lake" or f[0] not in ("DV",)

    def most(self, fish, base=(), cap=99):
        return 2 if base and base[0][0] in ("LT", "DV") else 3


def _abies_lines(monkeypatch):
    from pipeline.deliver.answers import engine as EN
    from pipeline.deliver.answers.rows import groups as G
    monkeypatch.setattr(EN, "Engine", _Abies)
    P = types.SimpleNamespace(cands=[types.SimpleNamespace(key="z6:total")], kind="stream")
    kinds = ["EB", "GB", "CT", "LT", "RB", "ST"]
    model = {"R": {S: {"H": None, "W": {"status": "keep"}} for S in kinds}}
    row = {"members": kinds}
    line = {"q": "Kept 2 elsewhere today?", "a": "Keep 3 more here, but no more than 1 brook trout from streams in all",
            "kept": 2, "at": "elsewhere", "species": kinds, "origin": "wild", "here": None, "over_cm": None,
            "most": "exactly", "kind": "tally", "of": None}
    return G._exact_lines(P, model, row, {"kinds": kinds}, [line])


@pytest.mark.parametrize("on", [True, False], ids=["named by fish", "mutation: one line for any fish"])
def test_a_worked_line_that_differs_by_fish_names_its_fish(monkeypatch, on):
    """Abies Creek: "Kept 2 elsewhere? Keep 3 more here, but …" overstated after 2 lake trout or Dolly Varden (2 more):
    the lines name the fish — "Kept 2 Dolly Varden/bull trout or lake trout elsewhere today? Keep 2 more here." — and the
    original words stand for "Kept 2 other trout or char elsewhere today?". Off: the one line for any fish."""
    from pipeline.deliver.answers.rows import groups as G
    monkeypatch.setattr(G, "A_WORKED_LINE_EXACT_PER_FISH", on)
    got = [(x["q"], x["a"], x["here"]) for x in _abies_lines(monkeypatch)]
    if on:
        assert got == [("Kept 2 Dolly Varden/bull trout or lake trout elsewhere today?", "Keep 2 more here", 2),
                       ("Kept 2 other trout or char elsewhere today?",
                        "Keep 3 more here, but no more than 1 brook trout from streams in all", None)], got
        for q, a, _h in got:
            W.parse_elsewhere(W.elsewhere_line(q.replace(" today", ""), a)) if a.endswith("here") else None
    else:
        assert got == [("Kept 2 elsewhere today?", "Keep 3 more here, but no more than 1 brook trout from streams in all",
                        None)], got


@pytest.mark.parametrize("on", [True, False], ids=["every line", "mutation: the box's kinds, some one"])
def test_j_holds_an_unnamed_same_water_line_for_every_fish(monkeypatch, on):
    """J (a focused engine): "Kept 2 elsewhere? Keep 3 more here." on Abies Creek is false after 2 lake trout (2 more):
    held to every trout and char it covers, it fails. Off (the box's kinds, some one of them): it passed."""
    from pipeline.deliver.answers.trout import check as J
    monkeypatch.setattr(J, "A_ELSEWHERE_EVERY_LINE", on)
    monkeypatch.setattr(J, "_engine", lambda P: _Abies())
    text = "Kept 2 elsewhere? Keep 3 more here."
    T_ = types.SimpleNamespace(elsewhere=[(text, W.parse_elsewhere(text))],
                               boxes=[{"kind": "keep", "rows": [types.SimpleNamespace(fish=S) for S in ("RB", "LT")]}])
    c = types.SimpleNamespace(P=types.SimpleNamespace(kind="stream"), today={"rows": []},
                              cell={"covers": [], "table": {"elsewhere": [text]}})
    probs = J.elsewhere_problems(c, T_)
    assert bool(probs) is on, probs
