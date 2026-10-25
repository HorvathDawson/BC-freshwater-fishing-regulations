"""T183 follow-ups (owner 2026-10-24): ON A PIECE IN TWO REGIONS EVERY CAP IS THE STRICTER OF THE TWO REGIONS'.

A straddling stream piece binds both regions' tables (T183). Where both print the same total ("Trout/char: 5"), the
reader shows one (same as peer) and the card used to build its group on that table alone — leaving out the other
region's stricter "from streams" cap, so the card said 5 where 2 may be kept (the card gate's 62 problems on the T183
side run). Now:

  * the group of the winning total takes the PEER total's clauses (`rows.groups.STRADDLERS_TAKE_BOTH_TABLES_CAPS`),
    the stricter of two clauses saying the same thing, and drops the "kept elsewhere" examples (elsewhere — in which
    region?);
  * a cap over FEWER fish never displaces the other region's wider one (`read.ladder.A_NARROWER_CAP_NEVER_DISPLACES_A_
    WIDER`): Region 6's "1 trout from streams July 1-Oct 31" beside Zone 7A's "2 from streams" — both bind, so 2 in all;
  * both regions' size caps bind (`card.TWO_TABLES_SIZE_CAPS_BOTH_BIND`): Region 2's "1 over 50 cm (2 hatchery steelhead
    over 50 cm allowed)" beside Region 8's "1 over 50 cm" — 1 (owner), no "— 2 if one is a hatchery steelhead";
  * the row's badge names both regions (`scope.both`, "Regions 3 and 5").

No data: `fixtures/t183_two_regions.json` holds the REAL rule sets of five straddling pieces (one per shape the side run
found) and every rule they bind, run through the real reader, the real rows and the real card gate. Mutation pins:
each flag above set False fails its shape's test here.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.answers.tree import walk
from pipeline.deliver.bundle import read

FIXTURE = Path(__file__).parent / "fixtures" / "t183_two_regions.json"
PATH = "fixture:t183_two_regions"
def _fish():
    from pipeline.regs.parsing.catalogue import BOOK_SPECIES
    return sorted(BOOK_SPECIES)
ORIGINS = ("none", "hatchery", "wild")
JUL15, JAN15 = (7, 15), (1, 15)
#: shape -> (steelhead water, steelhead presence) as the side bundle keyed the piece
FLAGS = {"fraser_3_5": (True, "known"), "big_bend_6_7a": (False, "possible"),
         "contact_6_7b": (False, "possible"), "west_road_5_6": (True, "known"),
         "copper_2_8": (False, "possible")}


class _B:
    def __init__(self, rules, sets):
        self.rules, self.sets = rules, sets


def _load():
    blob = json.loads(FIXTURE.read_text(encoding="utf-8"))
    got = read._RULES_BY_PATH.get(PATH)
    if got is None:
        got = {(x["entry"], x["rule"]): dict(x) for x in blob["rules"]}
        for x in got.values():
            x["_rank"] = read.source_of(x).rank
        read._RULES_BY_PATH[PATH] = got
    sets = {name: [tuple(r) for r in s["set"]] for name, s in blob["sets"].items()}
    return _B(got, sets)


def _part(shape, md):
    B = _load()
    sw, pres = FLAGS[shape]
    bound = B.sets[shape]
    lad = {f: {o: A.ladder_verdict(read.effective_rules_bound(
        bound, sw, md, f, PATH, steelhead_rules_here=True, origin=None if o == "none" else o, trace=True))
        for o in ORIGINS} for f in _fish()}
    # the open-subject gates (protected species, salmon): the reader's state, asked as the answers build asks it
    from pipeline.regs.parsing.catalogue import OPEN_SUBJECTS, PROTECTED_FISH
    opens = {}
    for e, r, _v in bound:
        x = B.rules[(e, r)]
        sp = x.get("species") or []
        if x.get("type") == "retention_limit" and sp and all(
                (c in OPEN_SUBJECTS and c != "ALL_FIN_FISH") or c in PROTECTED_FISH for c in sp):
            ans = A.ladder_verdict(read.effective_rules_bound(bound, sw, md, RW.open_fish(x), PATH,
                                                              steelhead_rules_here=True, trace=True))
            y = ans[f"{e}::{r}"]
            opens[f"{e}::{r}"] = (y[0], y[1])
    sets = {shape: bound}
    return RW.Part(_B(B.rules, sets), shape, sw, True, "stream", pres, lad, md, opens)


def _card(shape, md=JUL15):
    P = _part(shape, md)
    return RW.produce(P), P


def _trout(out):
    (row,) = [r for r in out["rows"] if r.get("group") and "trout_char_quota" in r["group"]["pool"]]
    return row, row["group"]


def _sizes(g):
    return [z for b in g["buckets"] for z in b.get("sizes") or []]


# ------------------------------------------------------------------------------------------- the five shapes

def test_fraser_in_regions_3_and_5_is_2_from_streams():
    """Region 3 "Trout/char: 5, 4 from streams" and Region 5 "Trout/char: 5, 2 from streams": 2 from streams, the
    group naming Region 5's cap; the badge names both regions; no kept-elsewhere example; the card gate proves it."""
    out, P = _card("fraser_3_5")
    row, g = _trout(out)
    assert g["headline"]["n"] == 2 and "from streams" in g["headline"]["says"]
    share = [s for s in g["subcaps"] if s["water"] == "stream"]
    assert [(s["r"], s["take"]) for s in share] == [("z5:trout_char_quota::trout_char_quota.r3", 2)]
    assert row["scope"]["both"] == "Regions 3 and 5"
    assert g["worked"] == []
    assert CC.check_card(P, out) == []


def test_big_bend_in_regions_6_and_7a_both_stream_caps_bind():
    """Region 6 "1 trout from streams July 1-Oct 31" (trout, char excepted) and Zone 7A "2 from streams" (trout and
    char): both bind — 1 trout, 2 in all from streams. In January Region 6's is out of season: Zone 7A's 2."""
    for md in (JUL15, JAN15):
        out, P = _card("big_bend_6_7a", md)
        _row, g = _trout(out)
        assert g["headline"]["n"] == 2, (md, g["headline"])
        z7a = next(s for s in g["subcaps"] if s["r"] == "z7a:trout_char_quota::trout_char_quota.r3")
        assert {"RB", "EB"} <= set(z7a["of"]) or md == JAN15
        assert CC.check_card(P, out) == [], md


def test_contact_creek_in_regions_6_and_7b_is_2_from_streams():
    out, P = _card("contact_6_7b")
    _row, g = _trout(out)
    assert g["headline"]["n"] == 2
    assert CC.check_card(P, out) == []


def test_west_road_in_regions_5_and_6_is_1():
    """Region 5 "2 from streams", Region 6 "1 trout from streams July 1-Oct 31" and the river's own rule: 1."""
    out, P = _card("west_road_5_6")
    _row, g = _trout(out)
    assert g["headline"]["n"] == 1
    assert CC.check_card(P, out) == []


def test_copper_creek_in_regions_2_and_8_keeps_1_over_50_cm():
    """Region 2 "1 over 50 cm (2 hatchery steelhead over 50 cm allowed)" and Region 8 "1 over 50 cm": 1 (owner). No
    size group says steelhead do not count, and no tier offers "— 2 if one is a hatchery steelhead"."""
    out, P = _card("copper_2_8")
    _row, g = _trout(out)
    assert _sizes(g) and all(z["not_counting"] == [] for z in _sizes(g))
    assert not any("if one is a" in (x.get("says") or "") for x, _p in walk(g["tree"]))
    assert CC.check_card(P, out) == []


# ------------------------------------------------------------------------------------------- mutation pins

def test_mutation_one_tables_caps_says_5_where_2_may_be_kept(monkeypatch):
    from pipeline.deliver.answers.rows import groups
    monkeypatch.setattr(groups, "STRADDLERS_TAKE_BOTH_TABLES_CAPS", False)
    out, P = _card("fraser_3_5")
    assert any("headline says 5" in p for p in CC.check_card(P, out))


def test_mutation_a_narrower_cap_displacing_the_wider_keeps_3(monkeypatch):
    from pipeline.deliver.bundle.read import ladder
    monkeypatch.setattr(ladder, "A_NARROWER_CAP_NEVER_DISPLACES_A_WIDER", False)
    out, P = _card("big_bend_6_7a")
    from pipeline.deliver.answers.engine import Engine
    _row, g = _trout(out)
    E = Engine(P, g["pool"])
    assert E.most(E.fish(g["kinds"], ("here",)), cap=10) == 3


def test_mutation_each_size_cap_alone_offers_a_second_steelhead(monkeypatch):
    from pipeline.deliver.answers import card
    monkeypatch.setattr(card, "TWO_TABLES_SIZE_CAPS_BOTH_BIND", False)
    out, P = _card("copper_2_8")
    assert any("hatchery steelhead" in p or "do not count" in p for p in CC.check_card(P, out))


@pytest.mark.parametrize("shape", sorted(FLAGS))
def test_every_shape_draws_on_both_tables(shape):
    """The group of every shape takes a cap of each region's table (the badge's `both`)."""
    from pipeline.deliver.answers.rows.groups import two_tables
    out, _P = _card(shape)
    row, g = _trout(out)
    assert two_tables(g)
    assert row["scope"]["of"] != "region" or row["scope"]["both"].startswith("Regions ")
