"""The gear resolver (`pipeline.deliver.answers.gear`) — consumer Stage 7.1-7.6.

1. AGAINST THE PAGE: `fixtures/answers_page_v35.json.gz` holds, for each of the page's 28 waters,
   every part and six dates (deduplicated: 287 cases), the rules the page held active and what its
   own `settleGear` / `gearParts` made of them (counts, specs, elements, circumstantial clauses,
   the hook and bait tags, bait per element, every way-to-fish chip, the Always count). The port,
   fed the same rules, must agree — with decision G1 (a rule-level `when_targeting` is a
   circumstance) switched off for the comparison, since the page does not read it.
2. RULINGS, mutation-pinned on small hand-made cases.
3. THE LIVE BUNDLE: handing the reader only the gear-relevant bindings gives the same states as
   the whole set; the answers are deterministic; the province's lawful methods are the guide's.
"""
from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from pipeline.deliver.answers import display as X
from pipeline.deliver.answers import gear as G

FIXTURE = Path(__file__).parent / "fixtures" / "answers_page_v35.json.gz"
HOOK_TAG = {"single_barbless": "Single barbless hook", "single": "Single hook",
            "any_barbless": "Any barbless hook", "any": "Any hook",
            "trebles_and_barbs": "Trebles and barbs OK"}
INFO_WHY = {"not allowed here": "not_allowed_here", "not a lawful way to sport fish": "not_lawful",
            "fly fishing only here": "fly_fishing_only_here"}


@pytest.fixture(scope="module")
def page():
    return json.loads(gzip.decompress(FIXTURE.read_bytes()))


def _rule(fx, k, rank, g1=False):
    r = fx["rules"][k]
    x = {**r["fields"], "type": r["type"], "family": r["family"], "dimension": r["dimension"]}
    if not g1:
        x.pop("when_targeting", None)
    par = fx["rules"].get(f"{k.split('::')[0]}::{x['condition_of']}") \
        if x.get("condition_of") else None
    return G.Rule(k, rank, x, G.cond_methods(x, par["fields"] if par else None))


def _resolve(fx, case, g1=False):
    act = [_rule(fx, k, rk, g1) for k, rk in case["active"]]
    return G.resolve([r for r in act if r.x.get("family") in G.GEAR_FAMILIES], case["kind"],
                     fx["province_methods"],
                     timed=[_rule(fx, k, rk, g1) for k, rk in case["timed"]],
                     while_rules=[r for r in act if X.kind_of(r.x) == "while"])


def _diff(a: dict, case: dict) -> list:
    errs = []
    if {s: [v["by"]] + v["over"] for s, v in a["counts"].items()} != case["counts"]:
        errs.append("counts")
    if a["specs"] != case["specs"]:
        errs.append("specs")
    if {k: v["by"] + [v["verdict"]] for k, v in a["elements"].items()} != case["elems"]:
        errs.append("elements")
    if [c["clause"] for c in a["circumstantial"]] != case["circ"]:
        errs.append("circumstantial")
    if HOOK_TAG[a["hook"]] not in case["tags"]:
        errs.append("hook")
    if ("No bait" if a["bait_ban"] else "Some bait OK") not in case["tags"] or \
            a["bait_ban"] != case["baitBan"]:
        errs.append("bait tag")
    if (a["fly"] == "artificial_fly_only") != \
            ("Artificial fly only: floats and sinkers OK" in case["tags"]):
        errs.append("fly")
    if [{"e": b["element"], "ok": b["ok"]} for b in a["bait"]] != case["bait"]:
        errs.append("bait")
    if [[w["method"], w["allowed"], w["by"][0] if w["by"] else None] for w in a["ways"]] != \
            [[c["k"], c["allow"], c["r"]] for c in case["chips"]]:
        errs.append("ways")
    for w, c in zip(a["ways"], case["chips"]):
        if ("not for game fish" in c["info"]) != ("ALL_GAME_FISH" in (w.get("not_for") or [])):
            errs.append(f"not for game fish: {w['method']}")
        if not w["allowed"] and w.get("why") != INFO_WHY.get(c["info"]):
            errs.append(f"why: {w['method']}")
    if sum(len(rs) for acts in a["conduct"].values() for _, rs in acts) != case["alwaysN"]:
        errs.append("always")
    return errs


def test_the_resolver_matches_the_page(page):
    bad = [(c["at"], e) for c in page["expect"]["gear"] if (e := _diff(_resolve(page, c), c))]
    assert not bad, bad[:5]
    cases = page["expect"]["gear"]
    assert len(cases) > 250
    # the cases exercise what matters: overruled counts, element bans, circumstantial clauses
    assert any(len(v) > 1 for c in cases for v in c["counts"].values())
    assert any(e[2] == "ban" for c in cases for e in c["elems"].values())
    assert any(c["circ"] for c in cases)


def test_ban_before_allow_is_what_the_page_does(page, monkeypatch):
    """Mutation: resolving an allow before a ban at equal rank breaks agreement with the page."""
    real = G.says_about
    monkeypatch.setattr(G, "says_about",
                        lambda c, el: {"ban": "allow", "allow": "ban"}.get(real(c, el)))
    assert any(_diff(_resolve(page, c), c) for c in page["expect"]["gear"])


def test_g1_rule_level_targeting_is_a_circumstance(page):
    """Decision G1: white sturgeon's 'dead fin fish may be used' (zp:bait r3, when_targeting WSG)
    rides beside the fin-fish ban as a circumstance, never as a main clause."""
    case = next(c for c in page["expect"]["gear"] if any(k == "zp:bait::bait.r3"
                                                         for k, _ in c["active"]))
    off, on = _resolve(page, case), _resolve(page, case, g1=True)
    clause = ["zp:bait::bait.r3", 0]
    assert clause not in [c["clause"] for c in off["circumstantial"]]
    got = next(c for c in on["circumstantial"] if c["clause"] == clause)
    assert got["targeting"] == ["WSG"]
    fin = next(b for b in on["bait"] if b["element"] == "fin_fish")
    assert {"clause": clause, "targeting": ["WSG"]} in fin.get("also_allowed", [])


def _x(**kw):
    return {"family": "gear_and_method", "type": "bait_restriction", **kw}


def test_rulings_on_hand_made_cases():
    zone = G.Rule("zone", 3, _x(gear=[{"slot": "bait", "ban": ["any_bait"]}]))
    water = G.Rule("water", 0, _x(gear=[{"slot": "bait", "allow": ["roe"]}]))
    a = G.resolve([zone], "stream", [])
    assert a["bait_ban"] and [b["ok"] for b in a["bait"]] == [False, False, False, False]
    # the closer place wins an element; worms follow only a whole-bait ban
    b = G.resolve([zone, water], "stream", [])
    assert {x["element"]: x["ok"] for x in b["bait"]} == \
        {"worms": False, "roe": True, "invertebrate": False, "fin_fish": False}
    fin = G.Rule("fin", 3, _x(gear=[{"slot": "bait", "ban": ["fin_fish"], "except": ["roe"]}]))
    c = G.resolve([fin], "stream", [])
    assert {x["element"]: x["ok"] for x in c["bait"]}["worms"] is True
    # equal rank: a ban before an allow
    allow = G.Rule("allow", 3, _x(gear=[{"slot": "bait", "allow": ["fin_fish"]}]))
    d = G.resolve([allow, fin], "stream", [])
    assert d["elements"]["bait:fin_fish"]["verdict"] == "ban"
    # a clause for the other kind of water drops out; on a key of unknown kind it is a circumstance
    lake = G.Rule("lake", 3, _x(gear=[{"slot": "bait", "ban": ["any_bait"],
                                       "when": {"water": "lake"}}]))
    assert G.resolve([lake], "stream", [])["elements"] == {}
    unk = G.resolve([lake], None, [])
    assert unk["elements"] == {} and unk["circumstantial"][0]["while"] == ["water=lake"]
    # a method nothing answers: lawful or not
    ways = {w["method"]: w["why"] for w in G.resolve([], "lake", ["angling"])["ways"]}
    assert ways["angling"] == "not_allowed_here" and ways["netting"] == "not_lawful"
    assert "ice_fishing" not in {w["method"] for w in G.resolve([], "stream", [])["ways"]}


def test_hook_answer():
    single = G.Rule("s", 3, _x(type="tackle_restriction", gear=[
        {"slot": "barb", "only": ["barbless"]}, {"slot": "points_per_hook", "max": 1}]))
    assert G.resolve([single], "stream", [])["hook"] == "single_barbless"
    barbless = G.Rule("b", 3, _x(type="tackle_restriction",
                                 gear=[{"slot": "barb", "only": ["barbless"]}]))
    assert G.resolve([barbless], "stream", [])["hook"] == "any_barbless"
    assert G.resolve([], "stream", [])["hook"] == "trebles_and_barbs"


# --------------------------------------------------------------------------------------------
# The live bundle
# --------------------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def bundle():
    from pipeline.deliver.answers import common
    p = common.bundle_path()
    if not Path(p).exists():
        pytest.skip(f"no bundle at {p}")
    return common.load(p)


def test_the_gear_subset_reads_as_the_whole_set(bundle):
    """Handing the reader only the gear-relevant bindings changes no gear rule's state."""
    from pipeline.deliver.answers.common import month_day
    from pipeline.deliver.bundle import read
    B = bundle
    keys = list(B.keys)[:: max(1, len(B.keys) // 40)]
    def ask(bound, key, md, fish):
        return {(x["entry"], x["rule"]): (x["state"], bool(x.get("partly_lifted")))
                for x in read.effective_rules_bound(bound, key.steelhead_water, md, fish, B.path,
                                                    steelhead_rules_here=key.steelhead_rules)
                if B.rules[(x["entry"], x["rule"])].get("family") in G.GEAR_FAMILIES
                or B.rules[(x["entry"], x["rule"])].get("while")}
    checked = 0
    for key in keys:
        full = B.sets.get(key.set_id, [])
        sub = G.gear_subset(B, full)
        for day in (15, 135, 196, 300):
            md = month_day(day)
            for fish in ("RB", "WSG", "KO"):
                assert ask(sub, key, md, fish) == ask(full, key, md, fish), (key, md, fish)
                checked += 1
    assert checked >= 300


def _key_of(B, item_id):
    from pipeline.deliver.answers import display
    got = sorted({v["rule"] for (w, _), v in display.parts_of_bundle(B).items() if w == item_id})
    if not got:
        pytest.skip(f"{item_id} is not in this bundle")
    return got[0]


def test_kootenay_lake_keeps_the_shore_line_count(bundle):
    """Decision G2, pinned both ways. Kootenay Lake's "unlimited rods from a boat" shares the
    province's line rule's (type, dimension), so the whole-set reader drops the province's rule —
    "1 line" from shore with it. Asked rule by rule, the shore count stands and the boat's
    unlimited rods ride beside it as a circumstance."""
    from pipeline.deliver.answers.common import month_day
    from pipeline.deliver.bundle import read
    B = bundle
    key = _key_of(B, "wbk:-20")                                  # Kootenay Lake — Main Body
    province = ("zp:terminal_tackle", "terminal_tackle.r1")
    whole = read.effective_rules_bound(B.sets[key.set_id], key.steelhead_water, month_day(200),
                                       "RB", B.path, steelhead_rules_here=key.steelhead_rules)
    assert province not in {(x["entry"], x["rule"]) for x in whole}, \
        "the reader no longer drops the province's line rule here: revisit decision G2"
    a = G.gear_answer(B, key, month_day(200), G.province_methods(B.rules.values()), G.rule_id)
    lines = a["counts"]["lines_per_angler"]
    assert lines["by"] == ["zp:terminal_tackle::terminal_tackle.r1", 1]
    assert any(c.get("while") == ["in_boat"] for c in lines.get("also", []))


def test_lifts_are_the_readers(bundle):
    """The Quatse's dated bait ban lifts Region 1's all-year stream bait ban on the days its lift
    holds (G5 of the 2026-10-03 rulings); on the other days the zone's ban stands."""
    from pipeline.deliver.answers.common import month_day
    from pipeline.deliver.bundle import read
    B = bundle
    lifter = next((k for k in B.rules if k[1] == "quatse_river.r4x"), None)
    if lifter is None:
        pytest.skip("no Quatse row in this bundle")
    lift = next(l for l in B.rules[lifter]["exempts"] if l["entry_id"] == "z1:bait_ban_streams")
    key = next(k for k in B.keys if any((e, r) == lifter for e, r, _ in B.sets[k.set_id]))
    lawful = G.province_methods(B.rules.values())
    zone = "z1:bait_ban_streams::bait_ban_streams.r1"
    on = next(d for d in range(1, 367) if read.in_force(lift["when"], month_day(d)) == "yes")
    off = next(d for d in range(1, 367) if read.in_force(lift["when"], month_day(d)) == "no")
    a_on = G.gear_answer(B, key, month_day(on), lawful, G.rule_id)
    a_off = G.gear_answer(B, key, month_day(off), lawful, G.rule_id)
    assert {"rule": zone, "lifted_by": [G.rule_id(lifter)]} in a_on["lifted"]
    assert zone not in {x["rule"] for x in a_off["lifted"]}
    assert zone in a_off["decides"] + a_off["repeats"]


def test_gear_answers_are_deterministic_and_lawful_methods_are_the_guides(bundle):
    B = bundle
    keys = list(B.keys)[:60]
    a = G.build(B, keys, log=lambda *_: None)
    b = G.build(B, keys, log=lambda *_: None)
    pub = lambda d: json.dumps({k: v for k, v in d.items() if k != "_key_index"},  # noqa: E731
                               sort_keys=True)
    assert pub(a) == pub(b)
    assert a["province_methods"] == ["angling", "crayfish_trapping", "ice_fishing",
                                     "set_lining", "spear_fishing"]
    # every key has an answer for every day, and each answer names rules by export index
    n = len(B.rules)
    for y in a["years"]:
        assert y[0][0] == 1 and all(p[0] < q[0] for p, q in zip(y, y[1:]))
    for ans in a["answers"]:
        for v in ans["elements"].values():
            assert 0 <= v["by"][0] < n
