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
    from pipeline.deliver.calendar import month_day
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


_STORES: dict = {}


def _store(B):
    """The bundle's stored verdicts (`verdicts.sqlite` beside it): what gear reads (P6)."""
    from pipeline.deliver.verdicts.store import VerdictStore
    if B.path not in _STORES:
        _STORES[B.path] = VerdictStore.open(Path(B.path).with_name("verdicts.sqlite"), B.path)
    return _STORES[B.path]


def _answer(B, key, md, lawful, ref=G.rule_id, moment=None):
    """The gear answer for a key on a day at a moment (a key with one: None): its reading's, from
    the stored verdicts."""
    from pipeline.deliver.calendar import day_of
    st = _store(B)
    k = B.key_ix[key]
    return G.gear_answer(B, key, md, lawful, ref, store=st,
                         reading=st.reading_of(k, day_of(md), moment),
                         at=st.moments(k)[moment or 0])


def _key_of(B, item_id):
    from pipeline.deliver.answers import common
    db = common.connect(B.path)
    try:
        got = sorted({common.RuleKey(rs, bool(sw), bool(sr)) for rs, sw, sr in db.execute(
            "SELECT r.set_id, EXISTS (SELECT 1 FROM steelhead_water w WHERE w.sid = s.sid), "
            "EXISTS (SELECT 1 FROM section_steelhead_rules x WHERE x.sid = s.sid) "
            "FROM item i JOIN item_section s ON s.ord = i.ord "
            "JOIN section_ruleset r ON r.sid = s.sid WHERE i.item_id = ?", (item_id,))})
    finally:
        db.close()
    if not got:
        pytest.skip(f"{item_id} is not in this bundle")
    return got[0]


def test_kootenay_lake_keeps_the_shore_line_count(bundle):
    """The dimension fix (was decision G2's workaround), pinned both ways. Kootenay Lake's
    "unlimited rods from a boat" holds only in a boat (`lines_per_angler@angler=in_boat`), so the
    reader no longer sets it against the province's line rule (`lines_per_angler` +
    `lines_per_angler@angler=alone_in_boat&water=lake`): both speak, the shore count stands and the
    boat's unlimited rods ride beside it as a circumstance."""
    from pipeline.deliver.calendar import month_day
    from pipeline.deliver.bundle import read
    B = bundle
    key = _key_of(B, "wbk:-20")                                  # Kootenay Lake — Main Body
    province = ("zp:terminal_tackle", "terminal_tackle.r1")
    kootenay = next(k for k in B.rules if k[1] == "kootenay_lake_main_body.r1")
    assert B.rules[province]["dimension"] != B.rules[kootenay]["dimension"]
    whole = {(x["entry"], x["rule"]): x["state"] for x in read.effective_rules_bound(
        B.sets[key.set_id], key.steelhead_water, month_day(200), "RB", B.path,
        steelhead_rules_here=key.steelhead_rules)}
    assert whole.get(province) == "speaks" and whole.get(kootenay) == "speaks"
    a = _answer(B, key, month_day(200), G.province_methods(B.rules.values()))
    lines = a["counts"]["lines_per_angler"]
    assert lines["by"] == ["zp:terminal_tackle::terminal_tackle.r1", 1]
    assert any(c.get("while") == ["in_boat"] for c in lines.get("also", []))


def test_the_no_gear_during_a_closure_duty_speaks_beside_a_regions_duty(bundle):
    """The dimension fix: a duty is keyed by its acts. Region 5's ice-hut duty ("remove the hut
    before break-up", while ice fishing) used to share `conduct` with the province's "no gear in
    the water during a closure" and displace it on every Region 5 lake; now both speak, and the
    province's own ice-hut duty (two acts, one of them "warn others of an ice hole") too."""
    from pipeline.deliver.calendar import month_day
    from pipeline.deliver.bundle import read
    B = bundle
    closure = ("zp:further_prohibitions", "further_prohibitions.r1")
    huts = ("z5:ice_fishing_huts", "ice_fishing_huts.r1")
    if huts not in B.rules:
        pytest.skip("no Region 5 ice-hut duty in this bundle")
    key = next((k for k in B.keys if {closure, huts} <= {b[:2] for b in B.sets[k.set_id]}), None)
    assert key is not None
    assert B.rules[huts]["dimension"] == "conduct:remove_ice_hut_before_breakup@while=ice_fishing"
    assert B.rules[closure]["dimension"] == "conduct:no_gear_in_water_during_closure"
    got = {(x["entry"], x["rule"]): x["state"] for x in read.effective_rules_bound(
        B.sets[key.set_id], key.steelhead_water, month_day(20), "RB", B.path,
        steelhead_rules_here=key.steelhead_rules)}
    assert got.get(closure) == "speaks" and got.get(huts) == "speaks"
    a = _answer(B, key, month_day(20), G.province_methods(B.rules.values()))
    never = dict(a["conduct"].get("never", []))
    assert G.rule_id(closure) in never.get("no_gear_in_water_during_closure", [])


def test_lifts_are_the_readers(bundle):
    """The Quatse's dated bait ban lifts Region 1's all-year stream bait ban on the days its lift
    holds (G5 of the 2026-10-03 rulings); on the other days the zone's ban stands."""
    from pipeline.deliver.calendar import month_day
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
    a_on = _answer(B, key, month_day(on), lawful)
    a_off = _answer(B, key, month_day(off), lawful)
    assert {"rule": zone, "state": "lifted", "reason": "lifted",
            "by": G.rule_id(lifter)} in a_on["overruled"]
    # off the lift's days the zone's ban is not lifted: it stands, or (May 1-Nov 30) the river's own
    # dated bait ban says the same thing at the closer rung and displaces it — bait is banned either way
    off_zone = [x for x in a_off["overruled"] if x["rule"] == zone]
    assert all(x["state"] == "displaced" and x["by"].startswith("r1:quatse_river")
               for x in off_zone)
    assert off_zone or zone in a_off["decides"] + a_off["repeats"]
    assert a_off["elements"]["bait:roe"]["verdict"] == "ban"


def test_gear_answers_are_deterministic_and_lawful_methods_are_the_guides(bundle):
    B = bundle
    keys = list(B.keys)[:60]
    lawful = G.province_methods(B.rules.values())
    ix = {G.rule_id(k): i for i, k in enumerate(sorted(B.rules, key=G.rule_id))}
    a = [G.gear_year(B, k, lawful, ref=lambda x: ix[G.rule_id(x)], store=_store(B)) for k in keys]
    b = [G.gear_year(B, k, lawful, ref=lambda x: ix[G.rule_id(x)], store=_store(B)) for k in keys]
    assert json.dumps(a, sort_keys=True, default=str) == json.dumps(b, sort_keys=True, default=str)
    assert lawful == ["angling", "crayfish_trapping", "ice_fishing", "set_lining", "spear_fishing"]
    # every key has an answer from day 1, and each answer names rules by export index
    n = len(B.rules)
    for y in a:
        days = sorted(y)
        assert days[0] == 1
        for ans in y.values():
            for v in ans["elements"].values():
                assert 0 <= v["by"][0] < n


def test_no_gear_in_the_water_during_a_closure_speaks_in_every_region(bundle, monkeypatch):
    """C11 (FIX round, user review 2026-10-06): "no gear in the water during a No Fishing period"
    (`zp:further_prohibitions.r1`) is in the gear answer's conduct of EVERY rule key of every region
    (1, 2, 3, 4, 5, 6, 7A, 7B, 8 and every straddle), winter and summer — only tidal water (Nitinat
    Lake: no provincial rule) has none. MUTATION: give a region's duty the closure's dimension (the
    V1 defect: one `conduct` key for every duty) and it is displaced there."""
    import re
    from pipeline.deliver.bundle import read
    B = bundle
    closure = ("zp:further_prohibitions", "further_prohibitions.r1")
    lawful = G.province_methods(B.rules.values())
    regions, missing = set(), []
    for key in B.keys:
        bound = B.sets.get(key.set_id, [])
        zones = {e.split(":")[0] for e, _r, _v in bound if re.match(r"z[0-9]", e)}
        if not zones:
            assert closure not in {b[:2] for b in bound}, "only tidal water carries no zone"
            continue
        regions |= zones
        for md in ((1, 15), (7, 15)):
            for mo in range(len(_store(B).moments(B.key_ix[key]))):
                acts = {a for m in _answer(B, key, md, lawful, moment=mo)["conduct"].values()
                        for a, _ in m}
                if "no_gear_in_water_during_closure" not in acts:
                    missing.append((key, md, mo))
    assert not missing, missing[:10]
    assert regions >= {"z1", "z2", "z3", "z4", "z5", "z6", "z7a", "z7b", "z8"}
    huts = ("z5:ice_fishing_huts", "ice_fishing_huts.r1")
    if huts in B.rules:
        rules = read._rules_of(B.path)
        monkeypatch.setitem(rules, huts, {**rules[huts], "dimension": rules[closure]["dimension"]})
        key = next(k for k in B.keys if {closure, huts} <= {b[:2] for b in B.sets[k.set_id]})
        # the stored verdicts are the reader's: the mutation is seen where they are made
        from pipeline.deliver.calendar import month_day
        got = {(x["entry"], x["rule"]): x["state"] for x in read.effective_rules_bound(
            B.sets[key.set_id], key.steelhead_water, month_day(20), "RB", B.path,
            steelhead_rules_here=key.steelhead_rules)}
        assert got.get(closure) != "speaks", "the mutation must displace it"


def test_line_counts_kootenay_boat_unlimited_shore_province_other_lakes_two_alone_in_a_boat(bundle):
    """User confirmation 2026-10-06 (book p.37 + the province's line rule): on Kootenay Lake's main
    body, from shore the province's 1 line; in a boat unlimited rods (the row's clause, which takes
    the place of the province's "2 lines if alone in a boat on a lake" there); on every OTHER lake
    the province's 1 line with "alone in a boat: 2" beside it (Kamloops Lake)."""
    from pipeline.deliver.calendar import month_day
    B = bundle
    lawful = G.province_methods(B.rules.values())
    province = "zp:terminal_tackle::terminal_tackle.r1"
    a = _answer(B, _key_of(B, "wbk:-20"), month_day(200), lawful)
    lines = a["counts"]["lines_per_angler"]
    assert lines["by"] == [province, 1]                                   # shore: 1 line
    also = {tuple(c.get("while") or ()): c["clause"] for c in lines.get("also", [])}
    assert also.get(("in_boat",), [""])[0].endswith("kootenay_lake_main_body.r1")
    assert ("alone_in_boat",) not in also, "the row's boat clause replaces the province's 2"
    k = _key_of(B, "wbk:329563838")                                       # Kamloops Lake
    a = _answer(B, k, month_day(200), lawful)
    lines = a["counts"]["lines_per_angler"]
    assert lines["by"] == [province, 1]
    assert [c["clause"] for c in lines.get("also", []) if c.get("while") == ["alone_in_boat"]] \
        == [[province, 0]], lines
