"""T92 — every gear phrase a rule prints lands in a stored field (omission, not inversion).

The gear model stops an INVERSION being written (`only: ["barbless"]`, no polarity flag); it does
not stop a conjunct being DROPPED. Eight lakes printed "single barbless hook" and stored only
`barb`, so a barbless treble was legal there per the data, and every field → text check stayed
green because it starts from the field. This check starts from the PRINTED phrase.

Each mutation below deletes (or changes) exactly the field a phrase needs and asserts the check
names that phrase on that rule and nothing else.
"""

from __future__ import annotations

import copy
import json

import pytest

from pipeline.regs.parsing.catalogue import CatalogueEntry
from pipeline.regs.parsing.catalogue_parts.gear_omission import (
    GEAR_PHRASES, KEPT_GEAR_GAPS, gear_misses)
from pipeline.regs.parsing.validate_catalogue import post_model_checks

WHOLE = [{"op": "whole"}]


def _entry(*rules, regs: str | None = None, entry_id: str = "r1:test_lake@1-1") -> CatalogueEntry:
    rules = [dict(r, extents=WHOLE) for r in rules]
    if regs is None:
        regs = "\n".join(r["verbatim"] for r in rules)
    return CatalogueEntry.model_validate(
        {"entry_id": entry_id, "name": "TEST LAKE", "regs_verbatim": regs, "rules": rules})


def _misses(entry: CatalogueEntry, kept=None) -> list[tuple[str, str]]:
    return [(rid, phrase) for _i, rid, phrase in gear_misses(entry, kept={} if kept is None
                                                              else kept)]


# Real corpus shapes (curated catalogue, 2026-10-22), extents stripped.
SINGLE_BARBLESS = {"rule_id": "t.r1", "type": "tackle_restriction",
                   "verbatim": "single barbless hook",
                   "gear": [{"slot": "barb", "only": ["barbless"]},
                            {"slot": "points_per_hook", "max": 1}]}
BAIT_BAN = {"rule_id": "t.r1", "type": "bait_restriction", "verbatim": "Bait ban",
            "gear": [{"slot": "bait", "ban": ["any_bait"]}]}
FLY_ONLY = {"rule_id": "t.r1", "type": "tackle_restriction", "verbatim": "Fly fishing only",
            "gear": [{"slot": "method", "only": ["fly_fishing"]}]}
ARTIFICIAL_FLY = {"rule_id": "t.r1", "type": "tackle_restriction",
                  "verbatim": "Artificial fly only",
                  "gear": [{"slot": "lure", "only": ["artificial_fly"]}]}
TREBLE = {"rule_id": "t.r1", "type": "tackle_restriction", "verbatim": "no treble hooks",
          "gear": [{"slot": "points_per_hook", "max": 1}]}
NO_SET_LINING = {"rule_id": "t.r1", "type": "method_rule", "verbatim": "no set lining",
                 "gear": [{"slot": "method", "ban": ["set_lining"]}]}
NO_ICE_FISHING = {"rule_id": "t.r1", "type": "method_rule", "verbatim": "No Ice Fishing",
                  "gear": [{"slot": "method", "ban": ["ice_fishing"]}]}
ICE_HUTS = {"rule_id": "t.r1", "type": "method_rule",
            "verbatim": "Failure to remove ice fishing huts from lakes before spring breakup is "
                        "an offence",
            "while": ["ice_fishing"], "conduct": ["remove_ice_hut_before_breakup"]}
ELECTRIC = {"rule_id": "t.r1", "type": "vessel_rule", "verbatim": "Electric motor only - max 7.5 kW",
            "aspect": "propulsion", "level": "electric_only"}
ENGINE = {"rule_id": "t.r1", "type": "vessel_rule",
          "verbatim": "engine power restriction - 7.5 kW (10 hp)",
          "aspect": "propulsion", "level": "power_capped", "max_power_kw": 7.5}
NO_POWERED = {"rule_id": "t.r1", "type": "vessel_rule", "verbatim": "No powered boats",
              "aspect": "propulsion", "level": "unpowered"}
NO_ANGLING_POWERED = {"rule_id": "t.r1", "type": "method_rule",
                      "verbatim": "No angling from powered boats",
                      "gear": [{"slot": "method", "ban": ["angling"],
                                "when": {"angler": "in_powered_boat"}}]}


#: A clause about something else. A rule that states nothing is refused by the model ("needs
#: `gear`, `conduct` or `exempts`"), so a dropped clause leaves a sibling clause behind it.
OTHER = {"slot": "lines_per_angler", "max": 1}


def _without_slot(rule: dict, slot: str) -> dict:
    """The rule with every `slot` clause gone — and `OTHER` in its place if none is left."""
    r = copy.deepcopy(rule)
    r["gear"] = [c for c in r["gear"] if c["slot"] != slot] or [dict(OTHER)]
    return r


def _with(rule: dict, **kw) -> dict:
    r = copy.deepcopy(rule)
    for k, v in kw.items():
        if v is None:
            r.pop(k, None)
        else:
            r[k] = v
    return r


#: (the stored rule, the mutation that drops one phrase's field, the phrases it must name)
MUTATIONS = [
    (SINGLE_BARBLESS, _without_slot(SINGLE_BARBLESS, "points_per_hook"), ["single_hook"]),
    (SINGLE_BARBLESS, _without_slot(SINGLE_BARBLESS, "barb"), ["barbless"]),
    (BAIT_BAN, _without_slot(BAIT_BAN, "bait"), ["bait_ban"]),
    (FLY_ONLY, _without_slot(FLY_ONLY, "method"), ["fly_fishing_only"]),
    (ARTIFICIAL_FLY, _without_slot(ARTIFICIAL_FLY, "lure"), ["artificial_fly"]),
    (TREBLE, _without_slot(TREBLE, "points_per_hook"), ["treble"]),
    (NO_SET_LINING, _without_slot(NO_SET_LINING, "method"), ["set_line"]),
    (NO_ICE_FISHING, _without_slot(NO_ICE_FISHING, "method"), ["ice_fishing"]),
    (ICE_HUTS, _with(ICE_HUTS, conduct=["warn_others_of_ice_hole"]), ["ice_hut"]),
    (ICE_HUTS, _with(ICE_HUTS, conduct=["warn_others_of_ice_hole"], **{"while": None}),
     ["ice_fishing", "ice_hut"]),
    (ELECTRIC, _with(ELECTRIC, level="power_capped", max_power_kw=7.5), ["electric_motor"]),
    (ENGINE, _with(ENGINE, level="unpowered", max_power_kw=None), ["engine_power"]),
    (NO_POWERED, _with(NO_POWERED, level="electric_only"), ["powered_boats"]),
    (NO_ANGLING_POWERED,
     _with(NO_ANGLING_POWERED, gear=[{"slot": "method", "ban": ["angling"]}]),
     ["powered_boats"]),
]


@pytest.mark.parametrize("stored,mutated,named", MUTATIONS,
                         ids=[f"{m[2]}" for m in MUTATIONS])
def test_dropping_the_field_a_phrase_needs_is_named(stored, mutated, named):
    assert _misses(_entry(stored)) == []
    assert _misses(_entry(mutated)) == [("t.r1", p) for p in named]


def test_every_phrase_in_the_vocabulary_has_a_mutation_pin():
    assert {p for _s, _m, names in MUTATIONS for p in names} == {p.name for p in GEAR_PHRASES}


def test_a_sibling_quoting_the_same_sentence_holds_the_field():
    """"Bait ban and single barbless hook" is two rules sharing one verbatim: each phrase is held
    by one of them, and dropping the tackle rule names exactly its two phrases."""
    words = "Bait ban and single barbless hook"
    bait = _with(BAIT_BAN, verbatim=words)
    tackle = _with(SINGLE_BARBLESS, rule_id="t.r2", verbatim=words)
    assert _misses(_entry(bait, tackle)) == []
    assert _misses(_entry(bait)) == [("t.r1", "single_hook"), ("t.r1", "barbless")]


def test_a_sibling_quoting_a_different_clause_does_not_launder():
    """The same three words printed twice in one row are two conjuncts; storing the first does
    not account for the second."""
    first = SINGLE_BARBLESS
    second = {"rule_id": "t.r2", "type": "retention_limit", "species": ["TROUT_CHAR"], "take": 0,
              "may_target": True,
              "verbatim": "Trout/char catch and release, single barbless hook"}
    e = _entry(first, second, regs="single barbless hook; Trout/char catch and release, "
                                   "single barbless hook")
    assert _misses(e) == [("t.r2", "single_hook"), ("t.r2", "barbless")]


def test_an_exemption_names_what_it_lifts_and_stores_the_lift():
    lift = {"rule_id": "t.r1", "type": "tackle_restriction",
            "verbatim": "EXEMPT from single barbless hooks",
            "exempts": [{"default_id": "single_barbless_hook"}]}
    assert _misses(_entry(lift)) == []
    assert _misses(_entry(_with(lift, exempts=None, gear=[OTHER]))) == [("t.r1", "single_hook"),
                                                          ("t.r1", "barbless")]


def test_an_exemption_printed_earlier_in_the_clause_covers_a_split_verbatim():
    """McMillan Creek: "Exempt from spring closure and from bait ban" split, the bait half quoted
    as "from bait ban". The clause says exempt; the rule carries the lift."""
    lift = {"rule_id": "t.r1", "type": "bait_restriction", "verbatim": "from bait ban",
            "gear": [{"slot": "bait", "allow": ["any_bait"]}],
            "exempts": [{"default_id": "bait_ban_streams"}]}
    e = _entry(lift, regs="Exempt from spring closure and from bait ban")
    assert _misses(e) == []


def test_an_exemption_in_another_clause_does_not_cover_a_ban():
    """"Exempt from spring closure; bait ban" — the `;` ends the exemption, so a bait ban stored
    as nothing on a lift rule is still a dropped ban."""
    lift = {"rule_id": "t.r1", "type": "retention_limit", "species": ["ALL_GAME_FISH"],
            "verbatim": "Exempt from spring closure; bait ban",
            "exempts": [{"default_id": "spring_stream_closure"}]}
    assert _misses(_entry(lift)) == [("t.r1", "bait_ban")]


def test_a_phrase_written_in_a_note_is_held_by_the_escape():
    """`note` is the model's escape for what its vocabulary cannot say, and it costs a
    review_reason, so the gap is visible there. A note that does not print the phrase holds
    nothing."""
    rule = {"rule_id": "t.r1", "type": "bait_restriction",
            "verbatim": "Bait ban except when ice fishing", "review_reason": "write when.method",
            "gear": [{"slot": "bait", "ban": ["any_bait"],
                      "when": {"note": "except when ice fishing"}}]}
    assert _misses(_entry(rule)) == []
    rule["gear"][0]["when"]["note"] = "except in winter"
    assert _misses(_entry(rule)) == [("t.r1", "ice_fishing")]


def test_a_kept_gap_is_silenced_only_on_its_own_rule_and_phrase():
    gap = _without_slot(BAIT_BAN, "bait")
    e = _entry(gap)
    assert _misses(e, kept={("r1:test_lake@1-1", "t.r1", "bait_ban"): "why"}) == []
    assert _misses(e, kept={("r1:test_lake@1-1", "t.r1", "barbless"): "why"}) == [
        ("t.r1", "bait_ban")]


def test_the_post_model_gate_reports_each_miss_on_its_rule():
    e = _entry(BAIT_BAN, _with(_without_slot(SINGLE_BARBLESS, "barb"), rule_id="t.r2"))
    got = [(p, m) for p, m in post_model_checks(e) if "gear phrase" in m]
    assert got == [(["rules", 1], "t.r2: prints 'barbless' (gear phrase `barbless`) and no rule "
                                  "quoting it stores `barb: only [barbless]` — the conjunct is "
                                  "dropped from every answer")]


# --------------------------------------------------------------------------- #
# The corpus
# --------------------------------------------------------------------------- #

def _corpus() -> list[CatalogueEntry]:
    from pipeline.common.curated import CURATED
    out = []
    for p in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json")):
        for e in json.loads(p.read_text())["entries"]:
            out.append(CatalogueEntry.model_validate(e))
    return out


@pytest.fixture(scope="module")
def corpus():
    return _corpus()


def test_the_catalogue_has_no_unexplained_gear_omission(corpus):
    got = [(e.entry_id, rid, phrase) for e in corpus for _i, rid, phrase in gear_misses(e)]
    assert got == []


def test_every_kept_gap_still_exists_and_says_why(corpus):
    """A kept gap whose rule was fixed (or whose phrase moved) is a stale silencer: drop it."""
    raw = {(e.entry_id, rid, phrase) for e in corpus
           for _i, rid, phrase in gear_misses(e, kept={})}
    assert set(KEPT_GEAR_GAPS) == raw
    assert all(len(why) > 40 for why in KEPT_GEAR_GAPS.values())
