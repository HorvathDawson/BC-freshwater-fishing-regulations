"""ROUND20h (owner, gear 11 / rows 25 / glossary 4):

1. A BAIT ALLOWANCE NAMES THE BAN IT IS AN EXCEPTION TO. The roe "still OK" on the Vedder Oct 10 is the province's YEAR-ROUND
   fin-fish ban's exception ("The use of fin fish (dead or alive) or parts of fin fish other than roe is prohibited throughout
   the province", p.8) — not the May 1–31 bait ban, which bans roe too. The allowed roe row carries `excepted_from` {element,
   by}: the banned element whose ban excepts it, on that day only; on a bait-ban day roe is banned and carries none.
2. POSSESSION SAID ONCE. The glossary term's `says` adds only what the plain line ("Possession: 2 days’ quota. Fish at home
   don’t count.") does not: where possession holds.
3. EACH CARD'S OWN POSSESSION EXAMPLE. `group.possession` {per_daily, of, n, by, example, doubles}: the possession rule's
   multiplier on the group's TOTAL (p.21 "Possession quotas = 2 daily quotas"; p.80 "the possession quota is two times the daily
   quota", the daily quota "the maximum number of fish of a given species, group of species, or size class that you may keep
   in one calendar day" — Region 2's trout/char 4), never the stream share: "With 4 trout and char a day you may have 8: 2 days’
   fish." and, where a share sits under the total, "Possession doubles the 4 a day, not the 2 from streams."
"""
from __future__ import annotations

import pytest

from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import gear as GE
from pipeline.deliver.answers import glossary as G
from pipeline.deliver.bundle.glossary_source import rows_from_book
from pipeline.deliver.bundle import read
from pipeline.tests import answers28_fixture as F

OCT10 = (10, 10)


def _x(**kw):
    return {"family": "gear_and_method", "type": "bait_restriction", **kw}


# 1 -----------------------------------------------------------------------------------------------------------------
def test_an_allowance_names_the_ban_it_is_an_exception_to():
    fin = GE.Rule("fin", 3, _x(gear=[{"slot": "bait", "ban": ["fin_fish"], "except": ["roe"]}]))
    a = GE.resolve([fin], "stream", [])
    roe = next(b for b in a["bait"] if b["element"] == "roe")
    assert roe["ok"] and roe["excepted_from"] == {"element": "fin_fish", "by": ["fin", 0]}
    assert next(b for b in a["bait"] if b["element"] == "fin_fish")["why"] == "banned_roe_ok"
    # a bait-ban day: roe is banned too, so it is nobody's exception
    ban = GE.Rule("ban", 0, _x(gear=[{"slot": "bait", "ban": ["any_bait"]}]))
    b = GE.resolve([fin, ban], "stream", [])
    roe = next(x for x in b["bait"] if x["element"] == "roe")
    assert not roe["ok"] and "excepted_from" not in roe
    # no ban with an exception: no allowance said
    c = GE.resolve([], "stream", [])
    assert all("excepted_from" not in x for x in c["bait"])


# 2 -----------------------------------------------------------------------------------------------------------------
def _doc(per_daily=2, total=5):
    return {"rules": {"z1:possession_quota::possession_quota.r1": {
                "entry_id": "z1:possession_quota", "rule_id": "possession_quota.r1",
                "fields": {"per_daily": per_daily}},
            "z1:trout_char_quota::trout_char_quota.r1": {
                "entry_id": "z1:trout_char_quota", "rule_id": "trout_char_quota.r1",
                "fields": {"species": ["TROUT_CHAR"], "take": total}}},
            "entries": {"z1:possession_quota": {"pages": [13]}}, "licensing": {}, "licences": {}}


def test_the_possession_term_does_not_repeat_the_plain_line():
    t = G.t_possession(G.Book(rows_from_book()), G.Data(_doc()))
    assert t["says"] == "The fish you have with you at any time — in camp, a cooler or a vehicle."
    assert "quota" not in t["says"] and "home" not in t["says"]
    assert G.VERSION >= 4


# 3 -----------------------------------------------------------------------------------------------------------------
POSS = ("z2:possession_quota", "possession_quota.r1")


@pytest.fixture
def with_possession(monkeypatch):
    got = F._load().rules
    x = {
        "entry": POSS[0], "entry_id": POSS[0], "entry_name": "Region 2", "rule": POSS[1], "rule_id": POSS[1],
        "dimension": "possession", "extent_text": None, "extents": [{"area_id": "area:region:2", "op": "within"}],
        "family": "retention", "label": "Possession quotas = 2 daily quotas", "may_target": None, "parts": "{}",
        "per_daily": 2, "period": "possession", "scope": "area", "species": ["ALL_GAME_FISH"], "species_except": [],
        "type": "retention_limit", "uncertain": 0, "undrawn_part": None, "unresolved": None,
        "verbatim": "Possession quotas = 2 daily quotas"}
    x["_rank"] = read.source_of(x).rank
    monkeypatch.setitem(got, POSS, x)


def test_each_card_says_its_own_possession_example(with_possession):
    out, P = F.card(F.VEDDER, OCT10)
    g = F.trout_group(out)
    assert g["possession"] == {
        "per_daily": 2, "of": 4, "n": 8, "by": "::".join(POSS),
        "example": "With 4 trout and char a day you may have 8: 2 days’ fish.",
        "doubles": "Possession doubles the 4 a day, not the 2 from streams."}
    out, P = F.card(F.LAKE, OCT10)                       # a lake: no share, nothing to tell apart
    g = F.trout_group(out)
    assert g["possession"]["example"] == "With 4 trout and char a day you may have 8: 2 days’ fish."
    assert g["possession"]["doubles"] is None
    assert CC.check_card(P, out) == []


def test_no_possession_rule_no_example():
    out, _P = F.card(F.VEDDER, OCT10)
    assert F.trout_group(out)["possession"] is None


def test_the_gate_refuses_a_wrong_possession_number(with_possession):
    out, P = F.card(F.VEDDER, OCT10)
    assert CC.check_card(P, out) == []
    g = F.trout_group(out)
    g["possession"] = {**g["possession"], "n": 4, "example": "With 4 trout and char a day you may have 4: 2 days’ fish."}
    assert any("possession" in p for p in CC.check_card(P, out))
