"""T771 (second look 2026-11-15, Skeena sockeye pair; Tatshenshini) — two equal places that disagree stop the build.

`chain_order` sorts a chain by tier, then size, then the rides flag, then the location id. Two places of ONE tier,
region and size that answer a fish differently on a day the chain reaches them are ordered by their ids alone: the
place whose name sorts first speaks, and nothing said so (the Skeena sockeye pair disagreed on 49 days and the right
one won by name). The salmon step now STOPS and names both (`salmon.equal_place_conflicts`), unless the pair is
ordered by structure (a rest-of and its listed place) or listed with the owner's reason in `EQUAL_PLACES_ACCEPTED`.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from pipeline.deliver import salmon as S
from pipeline.regs.dfo_salmon import entries as E


def _place(lid, fp, extents=None):
    return E.EntryLocation.from_dict({
        "location_id": lid, "region": "6", "region_number": 6, "kind": "water", "precedence": 3, "water": "River",
        "water_id": "6:river", "status": "active", "fingerprints": [fp], "source_text": {"specific_area": lid},
        "binding": {"extents": extents or [{"op": "whole", "item_id": "gnis:river"}], "tributaries": False}})


def _sockeye(rid, take, frm=(7, 1), to=(8, 31), may_target=None):
    r = {"rule_id": rid, "type": "retention_limit", "verbatim": rid, "species": ["SK"], "take": take,
         "when": {"dates": [{"from_month": frm[0], "from_day": frm[1], "to_month": to[0], "to_day": to[1]}]}}
    if may_target is not None:
        r["may_target"] = may_target
    return r


def _inputs(rules_a, rules_b, first=(), first_rules=(), extents_b=None):
    """The pair covers s1 and s2; each of `first` (narrower places, one section each: s1, then s2) answers before it."""
    a, b = _place("6:river:a-open", "fa"), _place("6:river:b-shut", "fb", extents_b)
    locs = [a, b]
    feed = [{"fingerprint": "fa", "rules": rules_a}, {"fingerprint": "fb", "rules": rules_b}]
    placed = [{"location_id": a.location_id, "mapped": True, "sections": ["s1", "s2"]},
              {"location_id": b.location_id, "mapped": True, "sections": ["s1", "s2"]}]
    for i, lid in enumerate(first, 1):
        locs.append(_place(lid, f"f{i}"))
        feed.append({"fingerprint": f"f{i}", "rules": list(first_rules)})
        placed.append({"location_id": lid, "mapped": True, "sections": [f"s{i}"]})
    ef = SimpleNamespace(region_name="R6", locations=locs, water=lambda wid: None)
    return {"6": ef}, {"6": {"locators": feed}}, placed, {"s1": 1, "s2": 2}


def test_two_equal_places_that_disagree_stop_the_build_and_are_named():
    """MUTATION: `equal_place_conflicts` returning [] builds the chain and the id decides (a-open answers)."""
    with pytest.raises(S.SalmonError) as ex:
        S.build(*_inputs([_sockeye("2 per day", 2)], [_sockeye("No fishing", 0, may_target=False)]),
                section_regions=None)
    msg = str(ex.value)
    assert "T771" in msg and "6:river:a-open | 6:river:b-shut: SK on day 0701" in msg


def test_equal_places_that_agree_or_speak_on_other_days_pass():
    same = S.build(*_inputs([_sockeye("2 per day", 2)], [_sockeye("2 per day", 2)]), section_regions=None)
    assert same[0]["chains"][dict(same[1])[1]]["l"] == ["6:river:a-open", "6:river:b-shut"]
    S.build(*_inputs([_sockeye("2 per day", 2, (7, 1), (7, 31))], [_sockeye("No fishing", 0, (8, 1), (8, 31), False)]),
            section_regions=None)


def test_a_place_before_the_pair_that_speaks_makes_their_order_moot():
    """A narrower place closing sockeye all year on s1 only: s2's chain still reaches the pair — the build stops. With
    one such place on each section the pair is never reached: no stop."""
    shut = [_sockeye("No fishing (mouth)", 0, (4, 1), (3, 31), False)]
    pair = ([_sockeye("2 per day", 2)], [_sockeye("No fishing", 0, may_target=False)])
    with pytest.raises(S.SalmonError):
        S.build(*_inputs(*pair, ["6:river:0-mouth"], shut), section_regions=None)
    S.build(*_inputs(*pair, ["6:river:0-mouth", "6:river:0-mouth-2"], shut), section_regions=None)


def test_an_accepted_pair_and_a_rest_with_its_listed_place_pass(monkeypatch):
    args = _inputs([_sockeye("2 per day", 2)], [_sockeye("No fishing", 0, may_target=False)])
    monkeypatch.setattr(S, "EQUAL_PLACES_ACCEPTED", {frozenset(("6:river:a-open", "6:river:b-shut")): "owner: test"})
    S.build(*args, section_regions=None)
    monkeypatch.setattr(S, "EQUAL_PLACES_ACCEPTED", {})
    # the rest-of stands behind its listed place by structure (`rest_after`), whatever the ids
    after = {"6:river:a-open": frozenset({"6:river:b-shut"})}
    assert S.equal_place_conflicts(["6:river:a-open", "6:river:b-shut"], lambda l: (0, 1, l), lambda l: [[0, 1]] * 5,
                                   after, lambda l: 6) == []
    assert S.equal_place_conflicts(["6:river:a-open", "6:river:b-shut"], lambda l: (0, 1, l),
                                   lambda l: [[0, 1 if l.endswith("open") else 2]] * 5, {}, lambda l: 6)[0][:4] == \
        ("6:river:a-open", "6:river:b-shut", "CH", 0)
    # two regions' places cascade apart (T668): never a tie between them
    assert S.equal_place_conflicts(["6:a", "7:b"], lambda l: (0, 1, l), lambda l: [[0, 1 if l == "6:a" else 2]] * 5,
                                   {}, lambda l: int(l[0])) == []


def test_two_closures_in_different_words_are_one_answer():
    """The Morice pair (2026-11-22 replay): "No fishing for chinook" beside "No fishing for salmon" — both closed, one
    answer; the build goes on. An opening's words are its conditions: two openings in different words stop it."""
    S.build(*_inputs([_sockeye("No fishing for sockeye", 0, may_target=False)],
                     [_sockeye("No fishing for salmon", 0, may_target=False)]), section_regions=None)
    assert S.same_answer({"s": "closed", "t": "No fishing for chinook"}, {"s": "closed", "t": "No fishing for salmon"})
    assert not S.same_answer({"s": "open", "t": "2 per day"}, {"s": "open", "t": "2 per day, barbless"})


def test_a_rest_moved_between_two_equal_places_does_not_hide_them():
    """(PR #47 review) R (size 2) lists A; A and B (size 5) tie. `rest_after` gives the chain (A, R, B): A opens, B
    shuts, and only the id puts A first. MUTATION: stopping at the first unequal key (`break`) returns [] here."""
    size = {"6:river:a-open": 5, "6:river:b-shut": 5, "6:river:r-rest": 2}
    at = lambda l: (0, size[l], l)  # noqa: E731
    after = {"6:river:r-rest": frozenset({"6:river:a-open"})}
    key = S.rest_after(tuple(sorted(size, key=at)), after)
    assert key == ("6:river:a-open", "6:river:r-rest", "6:river:b-shut")
    verdict = {"6:river:a-open": 1, "6:river:b-shut": 2, "6:river:r-rest": 3}
    got = S.equal_place_conflicts(key, at, lambda l: [[0, verdict[l]]] * len(S.SPECIES), after, lambda l: 6)
    assert {g[:2] for g in got} == {("6:river:a-open", "6:river:b-shut")}
    assert got[0][2:] == (S.SPECIES[0], 0, 1, 2)
    # on (A, B) alone the pair is reported the same way
    assert S.equal_place_conflicts(key[::2], at, lambda l: [[0, verdict[l]]] * len(S.SPECIES), after,
                                   lambda l: 6) == got
