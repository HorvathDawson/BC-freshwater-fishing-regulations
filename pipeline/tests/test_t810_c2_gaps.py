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
    return next(x for x in got["lines"] if x["q"].startswith("Kept 1 at a lake"))


@pytest.mark.parametrize("on", [True, False], ids=["the char cap less 1", "mutation: the share's own words"])
def test_the_lake_line_counts_the_smaller_cap(monkeypatch, on):
    """One lake trout kept at a lake uses one of the char cap's 2: "Keep 1 more here" (the engine's 1), not 2."""
    from pipeline.deliver.answers.rows import groups as G
    monkeypatch.setattr(G, "A_LAKE_LINE_UNDER_ITS_CAP", on)
    line = _qualcho(monkeypatch)
    assert line["here"] == (1 if on else 2), line
    assert line["a"].startswith(f"Keep {1 if on else 2} more here"), line


def test_the_lake_line_leaves_a_lifted_kind_to_its_own_number(monkeypatch):
    """A kind a water's own quota lifts past the share is worked out from that quota (the lifted lake trout's 3), never
    cut to the smaller cap less 1."""
    from pipeline.deliver.answers.rows import groups as G
    monkeypatch.setattr(G, "A_LAKE_LINE_UNDER_ITS_CAP", True)
    line = _qualcho(monkeypatch, lifted=True)
    assert line["here"] != 1, line


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
