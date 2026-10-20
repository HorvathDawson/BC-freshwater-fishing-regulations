"""ROUND UR5 (user panel 5; rows 27): the over-the-size question asked of fish kept elsewhere names the fish the group's size
clause holds HERE, not every fish its rule names.

Kootenay Lake main body (Oct 10): Region 4's "Only 1 rainbow trout or cutthroat trout over 50 cm a day" reaches the trout and char
group only for cutthroat (the lake's own rainbow rule — 10 a day, 20 a year over 50 cm — keeps rainbow apart, and replaces that
clause for them). The card asked "Kept rainbow trout or cutthroat trout over 50 cm elsewhere today? It counts here too: at most 1
over 50 cm a day in all." — a retiree and an ESL reader both read it as a daily cap on big rainbows (UR5 D1).
"""
from __future__ import annotations

from types import SimpleNamespace as N

from pipeline.deliver.answers import rows as RW

RB_CT = N(x={"species": ["RB", "CT"]})


def _ask(of, members=("EB", "GB", "CT", "LT", "DV")):
    P = N(steelhead_water=False, by_key={3149: RB_CT})
    grp = {"subcaps": [{"r": 3149, "take": 1, "water": None, "over_cm": 50, "under_cm": None, "of": list(of), "lifted": []}]}
    return [(x["q"], x["a"]) for x in RW.over_elsewhere(P, {"R": {}}, {"members": list(members)}, grp)]


def test_the_question_names_the_fish_the_clause_holds_here():
    assert _ask(["CT"]) == [("Kept cutthroat trout over 50 cm (20 in) elsewhere today?",
                             "It counts here too: at most 1 over 50 cm (20 in) a day in all.")]


def test_the_rule_s_own_words_where_the_clause_holds_all_its_fish():
    assert _ask(["RB", "CT"], members=("RB", "CT", "EB")) == [
        ("Kept rainbow trout or cutthroat trout over 50 cm (20 in) elsewhere today?",
         "It counts here too: at most 1 over 50 cm (20 in) a day in all.")]
