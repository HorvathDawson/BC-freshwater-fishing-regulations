"""T229 (owner 2026-10-28): the DFO salmon cascade reads as the Region 6 page does.

* inside a tier the MORE SPECIFIC place answers first (fewer sections): the Kitsumkalum-mouth closure before the
  "mainstem downstream of the CNR bridge" opening around it — the page's own "excluding the areas below";
* "All | No fishing for eulachon" is not a salmon closure: it decides no salmon species and shows as its own line.
"""
from pipeline.deliver import salmon as S
from pipeline.regs.dfo_salmon import typed as T

JUL_20 = 720


def _rule(rid, sp, take, frm=(4, 1), to=(3, 31), may_target=None):
    r = {"rule_id": rid, "type": "retention_limit", "verbatim": rid, "species": sp, "take": take,
         "when": {"dates": [{"from_month": frm[0], "from_day": frm[1], "to_month": to[0], "to_day": to[1]}]}}
    if may_target is not None:
        r["may_target"] = may_target
    return r


def test_the_more_specific_place_answers_first_within_a_tier():
    prec = {"6:skeena-river:mainstem-waters-downstream-of-cnr": 3, "6:skeena-river:mainstem-waters-near-the-kitsumkal": 3,
            "6:b(ii):section": 1, "6:a:region": 0}
    size = {"6:skeena-river:mainstem-waters-downstream-of-cnr": 123, "6:skeena-river:mainstem-waters-near-the-kitsumkal": 4,
            "6:b(ii):section": 15649, "6:a:region": 612941}
    chain = sorted(prec, key=S.chain_order(prec, size))
    assert chain == ["6:skeena-river:mainstem-waters-near-the-kitsumkal", "6:skeena-river:mainstem-waters-downstream-of-cnr",
                     "6:b(ii):section", "6:a:region"]
    rules = {"6:skeena-river:mainstem-waters-downstream-of-cnr": [_rule("open", ["CO"], 4, (7, 15), (8, 31))],
             "6:skeena-river:mainstem-waters-near-the-kitsumkal": [_rule("shut", ["SA"], 0, (7, 1), (8, 31), False)],
             "6:b(ii):section": [_rule("b2", ["CO"], 0, may_target=False)], "6:a:region": []}
    assert S.verdict_on([rules[l] for l in chain], "CO", JUL_20)["s"] == "closed"


def test_a_tier_still_beats_size_and_an_undrawn_place_sorts_last():
    prec = {"w": 3, "sec": 1}
    key = S.chain_order(prec, {"w": 10_000, "sec": 5})
    assert sorted(["sec", "w"], key=key) == ["w", "sec"]
    assert sorted(["b", "a"], key=S.chain_order({"a": 3, "b": 3}, {"b": 7})) == ["b", "a"]


def test_eulachon_is_not_a_salmon_closure_but_its_own_line():
    [r] = T.to_rules({"species": "All", "dates": "Apr 1 to Mar 31", "limits_gear": "No fishing for eulachon"}, "x", 0)
    d = r.model_dump(mode="json", exclude_defaults=True, by_alias=True)
    assert d["type"] == "advisory" and "species" not in d
    assert all(S.verdict_on([[d]], sp, JUL_20) is None for sp in S.SPECIES)
    assert S.gear_on([[d]], JUL_20) == ("No fishing for eulachon (Apr 1–Mar 31)",)
    # "All | No fishing for salmon" is still every salmon
    [s] = T.to_rules({"species": "All", "dates": "Jan 1 to Jun 15", "limits_gear": "No fishing for salmon"}, "x", 1)
    assert s.species == ["SA"] and s.take == 0
