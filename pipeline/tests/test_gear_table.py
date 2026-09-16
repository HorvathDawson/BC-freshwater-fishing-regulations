"""The gear table: two stages, one ladder, and a decision every row can be checked against.

Every check here is driven from the DECISION (`may_i_fish`) or from the raw corpus, never
seeded from the table — a check seeded from the table launders its own failures, and that
has happened three times in this project.
"""
from __future__ import annotations
import pytest

from pipeline.regs.table.applies import Applies, ALWAYS, applies_of
from pipeline.regs.table.authority import Source, Authority, Scope
from pipeline.regs.table.ledger import LIFTED, SAME, shuts_the_water
from pipeline.regs.table.method import (Term, MethodTable, MethodRow, METHODS, HOOK_AND_LINE,
                                        COVERED, MOOT, REPLACED, OPENED, CLOSED_BY, EXCEPTION,
                                        CONDITION, default_term)
from pipeline.regs.table.method_build import (table, base, region_base, provincial_base, is_gear,
                                              terms_of, explained_by_scope)
from pipeline.regs.table.method_oracle import may_i_fish, may_i_keep_by
from pipeline.regs.table.oracle import Fish

REGIONS = ["1", "2", "3", "4", "5", "6", "7a", "7b", "8"]


@pytest.fixture(scope="module")
def sections():
    from pipeline.regs.table.build import (D, WATERS, section_rules, section_regions,
                                           section_kind, section_label)
    out = []
    for w in WATERS:
        for run in range(len(D[w].get("runs") or [])):
            rules = section_rules(w, run)
            if not rules:
                continue
            here, kind, label = section_regions(w, run), section_kind(w), section_label(w, run)
            out.append((w, run, kind, rules, here, label, table(rules, kind, here, label)))
    assert len(out) >= 100
    return out


@pytest.fixture(scope="module")
def corpus():
    from pipeline.regs.table.corpus import rules, rid
    return {rid(x): x for x in rules()}


def _src(tier, rule_id="x::r", regions=frozenset(), region="4"):
    auth, scope = {"water": (Authority.region, Scope.water), "trib": (Authority.region, Scope.inherited),
                   "area": (Authority.region, Scope.area), "region": (Authority.region, Scope.region),
                   "prov": (Authority.province, Scope.region),
                   "superior": (Authority.superior, Scope.region)}[tier]
    return Source(auth, scope, region, "", frozenset(regions), rule_id, f"verbatim of {rule_id}")


def _win(*spans):
    return applies_of([{"from": {"month": a, "day": b}, "to": {"month": c, "day": d}} for a, b, c, d in spans],
                      None, all_year=False)


# --------------------------------------------------------------------------- #
# The guarantee: nothing goes missing, and nothing leaks between the stages.
# --------------------------------------------------------------------------- #
def test_no_gear_rule_reaches_no_table(sections):
    from pipeline.regs.table.method_comply import audit
    for w, run, kind, rules, here, label, _ in sections:
        _, gone = audit(rules, here, kind, label)
        assert not gone, f"{w} stretch {run + 1}: {gone}"


def test_a_water_scoped_rule_cannot_enter_a_base(sections):
    for w, run, kind, rules, here, label, T in sections:
        B = base(rules, kind, here)
        for t in B.terms:
            assert t.source.scope is Scope.region, (w, run + 1, t.rule_id, t.source.scope)
    for reg in REGIONS:
        for kind in ("stream", "lake"):
            for t in region_base(reg, kind).terms:
                assert t.is_base and t.source.scope is Scope.region, (reg, kind, t.rule_id)


def test_a_stream_rule_cannot_reach_a_lake(sections, corpus):
    for w, run, kind, rules, here, label, T in sections:
        for t in T.terms:
            wk = corpus[t.rule_id].get("water") if t.rule_id in corpus else None
            assert wk in (None, "", kind), (w, run + 1, t.rule_id, wk, kind)


def test_a_section_base_is_inside_its_regions_base(sections):
    """What a section draws on is what its region's table says — the page builder may drop
    a region-wide rule from a section, never invent one."""
    for w, run, kind, rules, here, label, T in sections:
        allowed = set()
        for r in here:
            allowed |= region_base(r, kind).universe()
        extra = base(rules, kind, here).universe() - allowed
        assert not extra, (w, run + 1, sorted(extra))


# --------------------------------------------------------------------------- #
# The default, stated: angling by licence; everything else only where the book allows.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("region,kind,verdict", [
    ("2", "lake", "not allowed"), ("2", "stream", "not allowed"), ("4", "lake", "not allowed"),
    ("6", "lake", "allowed"), ("7a", "lake", "allowed"), ("7b", "lake", "not allowed"),
    ("8", "lake", "not allowed"),
])
def test_set_lining_is_allowed_only_where_the_book_permits_it(region, kind, verdict):
    row = MethodRow(region_base(region, kind), "set_lining")
    assert row.verdict_word() == verdict
    gov = row.standing()
    if region in ("2", "4", "8"):
        assert gov.is_default, gov.rule_id
        assert "Regions 6 and 7A" in gov.source.verbatim and "lakes" in gov.source.verbatim
        assert "All other methods of taking fin fish and crayfish are illegal" in gov.source.verbatim
    if region == "7b":
        assert gov.rule_id == "z7b:set_lining::set_lining.r1"


def test_angling_is_permitted_by_licence_where_nothing_bans_it():
    for reg in REGIONS:
        for kind in ("stream", "lake"):
            gov = MethodRow(region_base(reg, kind), "angling").standing()
            assert gov.kind == "permit" and gov.is_default


@pytest.mark.parametrize("region,verdict", [("1", "not allowed"), ("2", "not allowed"), ("4", "not allowed"),
                                            ("3", "allowed"), ("5", "allowed"), ("6", "allowed"),
                                            ("7a", "allowed"), ("7b", "allowed"), ("8", "allowed")])
def test_spear_fishing_is_banned_in_regions_1_2_and_4_and_permitted_elsewhere(region, verdict):
    for kind in ("stream", "lake"):
        row = MethodRow(region_base(region, kind), "spear_fishing")
        assert row.verdict_word() == verdict, (region, kind)
        if verdict == "not allowed":
            assert row.standing().rule_id == "zp:spear_fishing::spear_fishing.r3"
            behind = dict(row.folded())
            permit = next(t for t in row.candidates() if t.rule_id == "zp:allowable_methods::allowable_methods.r4")
            assert behind[permit] == CLOSED_BY


# --------------------------------------------------------------------------- #
# A lift is a subtraction, where it bites, and never of itself.
# --------------------------------------------------------------------------- #
def test_burbot_is_lifted_from_the_spear_ban_only_in_the_five_regions():
    for reg in REGIONS:
        T = region_base(reg, "lake")
        freed = {f for fish, _ in T.lifted_fish.get("spear_fishing", []) for f in fish}
        L = T.keep("spear_fishing")
        ban = next(a for a in L.allowances if a.rule_id == "zp:spear_fishing::spear_fishing.r1")
        if reg in ("3", "5", "6", "7a", "7b", "8"):
            assert freed == {"BB"}, reg
            assert "BB" in ban.scope.excepts and not ban.contains("BB"), reg
        else:
            assert not freed, reg
            assert ban.contains("BB"), reg


def test_the_set_lining_bait_lift_bites_under_set_lining_alone(sections):
    seen = 0
    for w, run, kind, rules, here, label, T in sections:
        if kind != "lake" or "6" not in here:
            continue
        fin = next(t for t in T.terms if t.rule_id == "zp:bait::bait.r1")
        assert T.status_of("angling", fin) in ("", SAME, COVERED), (w, T.status_of("angling", fin))
        assert T.in_force("angling", fin)
        assert T.status_of("set_lining", fin) == LIFTED
        seen += 1
    assert seen >= 1


def test_the_fraser_bait_exemption_does_not_lift_the_ban_on_the_chilliwack(sections):
    for w, run, kind, rules, here, label, T in sections:
        if w not in ("Chilliwack River", "Coquihalla River"):
            continue
        fin = next(t for t in T.terms if t.rule_id == "zp:bait::bait.r1")
        assert T.in_force("angling", fin), f"{w} stretch {run + 1}: the fin-fish ban is lifted"
        r3 = next(t for t in T.terms if t.rule_id == "zp:bait::bait.r3")
        assert r3.applies.kind == "somewhere"


def test_nothing_lifts_itself(sections):
    for w, run, kind, rules, here, label, T in sections:
        for t in T.terms:
            assert t.rule_id not in t.lifts, (w, run + 1, t.rule_id)


@pytest.mark.parametrize("text,regions,explained", [
    ("lakes of Region 6 and Zone A of Region 7", {"6", "7a"}, True),
    ("Regions 3, 5, 6, 7 and 8", {"3", "5", "6", "7a", "7b", "8"}, True),
    ("Fraser, Lower Pitt and Lower Harrison Rivers, Region 2", {"2"}, False),
    ("huts must be removed before spring breakup", set(), False),
])
def test_an_extent_the_typed_scope_already_says_is_not_a_caveat(text, regions, explained):
    assert explained_by_scope(text, frozenset(regions)) is explained


# --------------------------------------------------------------------------- #
# The ladder, on synthetic terms.
# --------------------------------------------------------------------------- #
def _standing(method, kind, tier, rid, **kw):
    return Term(method, kind, _src(tier, rid), kw.pop("applies", ALWAYS), text=rid, **kw)


def test_this_water_overrides_regional_overrides_provincial_and_nothing_opens_a_superior_ban():
    prov = _standing("ice_fishing", "permit", "prov", "zp::p")
    reg = _standing("ice_fishing", "ban", "region", "z4::b")
    water = _standing("ice_fishing", "permit", "water", "r4::w")
    T = MethodTable([prov, reg])
    assert T.standing("ice_fishing") is reg and T.status_of("ice_fishing", prov) == CLOSED_BY
    T = MethodTable([prov, reg, water])
    assert T.standing("ice_fishing") is water and T.status_of("ice_fishing", reg) == OPENED
    sup = _standing("ice_fishing", "ban", "superior", "zp::park")
    T = MethodTable([prov, reg, water, sup])
    assert T.standing("ice_fishing") is sup


def test_a_ban_beats_a_permit_at_one_rank():
    p = _standing("spear_fishing", "permit", "prov", "zp::p")
    b = _standing("spear_fishing", "ban", "prov", "zp::b")
    assert MethodTable([p, b]).standing("spear_fishing") is b


def test_a_seasonal_ban_governs_in_season_and_the_permit_the_rest_of_the_year():
    p = _standing("ice_fishing", "permit", "prov", "zp::p")
    b = _standing("ice_fishing", "ban", "water", "r::b", applies=_win((4, 1, 6, 30)))
    T = MethodTable([p, b])
    assert T.standing("ice_fishing", (5, 1)) is b
    assert T.standing("ice_fishing", (8, 1)) is p
    cal = T.calendar("ice_fishing")
    assert [c["verdict"] for c in cal] == ["permit", "ban", "permit"] or [c["verdict"] for c in cal] == ["permit", "ban"]


def _rig(tier, rid, key, says, allows=False, method="", applies=ALWAYS, lifts=frozenset(), only_when=""):
    topic = {"barbless": "Hooks", "hook_count": "Hooks", "max_lines": "Lines", "lure": "Lures and flies"}.get(
        key, "Bait" if key.startswith("bait:") else "Also")
    return Term(method, "rig", _src(tier, rid), applies, text=rid, topic=topic, key=key,
                says=tuple(sorted(says)), allows=allows, lifts=frozenset(lifts), only_when=only_when)


def test_rig_conditions_fold_by_the_ladder_and_never_compete():
    barb_prov = _rig("prov", "zp::barb", "barbless", [("barbless", "True"), ("water", "stream")])
    single_prov = _rig("prov", "zp::single", "hook_count", [("hook_count", "1"), ("water", "stream")])
    sbh_reg = _rig("region", "z4::sbh", "barbless", [("barbless", "True"), ("hook_count", "1"), ("water", "stream")])
    fin_prov = _rig("prov", "zp::fin", "bait:fin_fish", [("bait", "fin_fish")])
    roe_prov = _rig("prov", "zp::roe", "bait:roe", [("bait", "roe")], allows=True)
    ban_reg = _rig("region", "z4::bait", "bait:any", [])
    one_line = _rig("prov", "zp::line", "max_lines", [("max_lines", "1")])
    rods = _rig("water", "r4::rods", "max_lines", [("max_lines", "0")])
    T = MethodTable([barb_prov, single_prov, sbh_reg, fin_prov, roe_prov, ban_reg, one_line, rods])
    st = lambda t: T.status_of("angling", t)
    assert st(sbh_reg) == "" and st(barb_prov) == COVERED and st(single_prov) == COVERED
    assert st(ban_reg) == "" and st(fin_prov) == COVERED and st(roe_prov) == MOOT
    assert st(rods) == "" and st(one_line) == REPLACED
    printed = {t.rule_id for ts in T.rig("angling").values() for t in ts}
    assert printed == {"z4::sbh", "z4::bait", "r4::rods"}
    # folded terms are still in force — the ladder folds the page, not the law
    assert T.in_force("angling", fin_prov) and not T.in_force("angling", roe_prov)


def test_a_narrower_allowance_is_an_exception_on_the_ban_not_a_line_of_its_own():
    fin = _rig("prov", "zp::fin", "bait:fin_fish", [("bait", "fin_fish")])
    dead = _rig("water", "r2::dead", "bait:dead_fin_fish", [("bait", "dead_fin_fish")], allows=True,
                only_when="when fishing for white sturgeon")
    T = MethodTable([fin, dead])
    assert T.status_of("angling", dead) == EXCEPTION
    assert T.carves["angling"][fin] == [dead]
    assert T.status_of("angling", fin) == ""


def test_a_lift_that_names_a_method_lifts_under_that_method_alone():
    fin = _rig("prov", "zp::fin", "bait:fin_fish", [("bait", "fin_fish")])
    lift = Term("set_lining", "lift", _src("prov", "zp::lift"), ALWAYS, text="dead fin fish when set lining",
                lifts=frozenset({"zp::fin"}))
    T = MethodTable([fin, lift])
    assert T.status_of("set_lining", fin) == LIFTED
    assert T.status_of("angling", fin) == "" and T.status_of("ice_fishing", fin) == ""


def test_a_seasonal_rule_inside_another_folds_and_beside_a_year_round_one_prints():
    own = _rig("water", "r4::own", "bait:any", [], applies=_win((6, 15, 10, 31)))
    trib = _rig("trib", "r4::trib", "bait:any", [], applies=_win((6, 15, 8, 31)))
    fin = _rig("prov", "zp::fin", "bait:fin_fish", [("bait", "fin_fish")])
    T = MethodTable([own, trib, fin])
    assert T.status_of("angling", trib) in (SAME, COVERED)
    assert T.status_of("angling", fin) == ""          # a seasonal ban does not fold a year-round one
    assert [t.rule_id for t in T.rig("angling")["Bait"]] == ["zp::fin", "r4::own"]
    assert T.rig("angling", (7, 1))["Bait"][-1] is own and "r4::own" not in [t.rule_id for t in T.rig("angling", (12, 1))["Bait"]]


def test_a_permit_behind_a_permit_is_a_condition_unless_it_says_nothing_new():
    prov = Term("ice_fishing", "permit", _src("prov", "zp::ice"), text="one line and one lure",
                says=(("max_lines", "1"),))
    huts = Term("ice_fishing", "permit", _src("region", "z5::huts"), text="WARNING remove your hut")
    dup = Term("ice_fishing", "permit", _src("prov", "zp::dup"), text="ice fishing is permitted")
    T = MethodTable([prov, huts, dup])
    assert T.standing("ice_fishing") is huts
    assert T.status_of("ice_fishing", prov) == CONDITION
    assert T.status_of("ice_fishing", dup) == SAME
    assert [t.rule_id for t in T.conditions("ice_fishing")] == ["zp::ice", "z5::huts"]


# --------------------------------------------------------------------------- #
# The closure cross-check: what the quota table shuts, the gear table shuts.
# --------------------------------------------------------------------------- #
_DATES = [(m, d) for m in range(1, 13) for d in (1, 15)]


def test_a_water_closed_to_fishing_reads_closed_in_the_gear_table(sections):
    from pipeline.regs.table.build import ledger
    shut_somewhere = 0
    for w, run, kind, rules, here, label, T in sections:
        L = ledger(rules, kind, here, label)
        for on in _DATES:
            quota_shut = [a for a in L.allowances
                          if a.kind == "closed" and shuts_the_water(a.scope) and L.in_force(a)
                          and not a.applies.within_day and a.derived_from is None and a.applies.live(*on)]
            gear = T.shut(on)
            assert (gear is not None) == bool(quota_shut), (w, run + 1, on)
            if gear is not None:
                shut_somewhere += 1
                assert gear.rule_id in {a.rule_id for a in quota_shut}
                for m in METHODS:
                    v = may_i_fish(T, m, on)
                    assert v.may is False and v.kind == "closed"
                    assert any(d.rule_id == "zp:further_prohibitions::further_prohibitions.r1" for d in v.decided_by), \
                        "the no-gear-in-the-water rule must be cited on a closure"
    assert shut_somewhere > 50


def test_what_you_may_keep_by_a_method_is_the_stricter_of_both_tables(sections):
    from pipeline.regs.table.build import ledger, name
    checked = 0
    for w, run, kind, rules, here, label, T in sections:
        if T.keep("spear_fishing") is None or T.shut((7, 15)) is not None:
            continue
        L = ledger(rules, kind, here, label)
        for sp in ("BB", "RB", "SA"):
            v = may_i_keep_by(T, L, "spear_fishing", Fish(sp, 40), (7, 15), name=name)
            if sp in ("RB", "SA"):
                assert v.keep is False, (w, run + 1, sp, v.reasons)
                assert v.decided_by[0].rule_id.startswith("zp:spear_fishing::"), v.decided_by[0].rule_id
            elif any("BB" in fish for fish, _ in T.lifted_fish.get("spear_fishing", [])):
                # burbot is freed from the spear ban here; the water's own quota decides
                assert v.keep is not False or not v.decided_by[0].rule_id.startswith("zp:spear_fishing::")
            else:
                assert v.keep is False and v.decided_by[0].rule_id == "zp:spear_fishing::spear_fishing.r1"
            checked += 1
    assert checked > 100


# --------------------------------------------------------------------------- #
# TOTALITY, driven from the decision: what decided it is on the row, and what the row
# prints in force is what the decision consulted.
# --------------------------------------------------------------------------- #
def _rendered_ids(row: dict, d: dict) -> set:
    ids = set()
    def take(t):
        ids.add(t["rule"])
        for x in t.get("exceptions", []): take(x)
    for t in row["candidates"] + row["conditions"] + row["folded"] + row["hours"]:
        take(t)
    for ts in row["rig"].values():
        for t in ts: take(t)
    for ts in d["band"]["rig"].values():
        for t in ts: take(t)
    for k in row["keep"]:
        ids.add(k["rule"])
    for c in d["closures"]:
        ids.add(c["rule"])
    for t in d["while_closed"]:
        ids.add(t["rule"])
    ids.discard("")
    return ids


def _dates_for(T: MethodTable):
    days = set(_DATES)
    for t in T.terms:
        for (fm, fd), (tm, td) in t.applies.windows:
            days |= {(fm, fd), (tm, td)}
    for a in T.closures:
        for (fm, fd), (tm, td) in a.applies.windows:
            days |= {(fm, fd), (tm, td)}
    return sorted(days)


def test_every_rule_the_verdict_rests_on_is_on_the_rendered_row_and_vice_versa(sections):
    from pipeline.regs.table.method_provenance import section as section_json
    verdicts = 0
    for w, run, kind, rules, here, label, T in sections:
        d = section_json(w, run)
        rows = {r["method"]: r for r in d["rows"]}
        for m in rows:
            shown = _rendered_ids(rows[m], d)
            for on in _dates_for(T):
                v = may_i_fish(T, m, on)
                verdicts += 1
                missing = v.cited() - shown
                assert not missing, f"{w} stretch {run + 1} · {m} · {on}: decided by {sorted(missing)}, not on the row"
                if v.may:
                    # the converse: every condition printed in force on this date was consulted
                    printed = {t.rule_id for ts in T.rig(m, on).values() for t in ts}
                    printed |= {t.rule_id for t in T.conditions(m, on)}
                    unconsulted = printed - v.cited()
                    assert not unconsulted, f"{w} stretch {run + 1} · {m} · {on}: printed but never consulted {sorted(unconsulted)}"
    assert verdicts > 20000


def test_the_hoist_prints_a_shared_condition_once_and_never_on_a_member_row(sections):
    from pipeline.regs.table.method_provenance import section as section_json
    for w, run, kind, rules, here, label, T in sections:
        d = section_json(w, run)
        band = d["band"]
        sigs = {(t["rule"], t["status"]) for ts in band["rig"].values() for t in ts}
        for r in d["rows"]:
            if r["method"] not in band["rows"]:
                continue
            own = {(t["rule"], t["status"]) for ts in r["rig_own"].values() for t in ts}
            assert not (own & sigs), (w, run + 1, r["method"], own & sigs)
            whole = {(t["rule"], t["status"]) for ts in r["rig"].values() for t in ts}
            assert whole == own | sigs, (w, run + 1, r["method"])


def test_the_page_never_prints_a_rule_id_or_a_species_code(sections):
    import re
    from pipeline.regs.table.method_provenance import section as section_json
    ident = re.compile(r"\b[rz]\w*:\w+::|\b[A-Z]{2,4}(?:_[A-Z]+)+\b")
    for w, run, kind, rules, here, label, T in sections[:30]:
        d = section_json(w, run)
        for r in d["rows"]:
            for t in r["candidates"] + r["conditions"] + r["folded"]:
                assert not ident.search(t["plain"]), (w, t["plain"])
            for k in r["keep"]:
                assert not ident.search(k["fish"] + " " + k["word"]), (w, k)


# --------------------------------------------------------------------------- #
# THE PRINT DIFF. The regional panel of each synopsis chapter (scratchpad/synopsis/*.pdf,
# page 2, "General Regulations"), and page 10 of the full synopsis for the province, read
# by hand into the lines a reader would tick off. The computed base must match; every
# disagreement is enumerated with its cause, so a new one fails the suite.
# --------------------------------------------------------------------------- #
PRINT = {
    # region: {kind: {method: verdict}}, plus the region-authored rig lines the panel prints
    "1":  {"rig": {"stream": {"No bait", "Single barbless hook, from streams"}, "lake": set()},
           "spear_fishing": "not allowed", "set_lining": "not allowed"},
    "2":  {"rig": {"stream": {"Single barbless hook, from streams"}, "lake": set()},
           "spear_fishing": "not allowed", "set_lining": "not allowed"},
    "3":  {"rig": {"stream": {"Single barbless hook, from streams"}, "lake": set()},
           "spear_fishing": "allowed", "set_lining": "not allowed"},
    "4":  {"rig": {"stream": {"Single barbless hook, from streams"}, "lake": set()},
           "spear_fishing": "not allowed", "set_lining": "not allowed"},
    "5":  {"rig": {"stream": {"Single barbless hook, from streams"}, "lake": set()},
           "spear_fishing": "allowed", "set_lining": "not allowed"},
    "6":  {"rig": {"stream": {"Single barbless hook, from streams"}, "lake": set()},
           "spear_fishing": "allowed", "set_lining": {"stream": "not allowed", "lake": "allowed"}},
    "7a": {"rig": {"stream": {"No bait", "Single barbless hook, from streams"}, "lake": set()},
           "spear_fishing": "allowed", "set_lining": {"stream": "not allowed", "lake": "allowed"}},
    "7b": {"rig": {"stream": {"No bait", "Single barbless hook, from streams", "Fin fish may not be used as bait"},
                   "lake": {"Fin fish may not be used as bait"}},
           "spear_fishing": "allowed", "set_lining": "not allowed"},
    "8":  {"rig": {"stream": {"Single barbless hook, from streams"}, "lake": set()},
           "spear_fishing": "allowed", "set_lining": "not allowed"},
}

#: Disagreements with the print that are CURATION defects, reported and not papered over.
KNOWN_PRINT_DEFECTS = {
    ("6", "stream", "set_lining"): "zp:set_lining.r2/.r3/.r4 carry no `water: lake`, so the province's "
                                   "conditions on set lining permit it on streams the book closes to it",
    ("7a", "stream", "set_lining"): "same: zp:set_lining.r2/.r3/.r4 lack `water: lake`",
    ("7b", "stream", "rig"): "z7b:bait.r2 (fin fish, all waters of Zone B) is folded under the stream bait "
                             "ban — the print states both; the table prints the wider one and folds the narrower",
}


def print_diff():
    """Every (region, kind) against the print. Returns the list of disagreements."""
    out = []
    for reg, p in PRINT.items():
        for kind in ("stream", "lake"):
            T = region_base(reg, kind)
            for m in ("spear_fishing", "set_lining"):
                want = p[m] if isinstance(p[m], str) else p[m][kind]
                got = MethodRow(T, m).verdict_word()
                if got != want:
                    out.append({"region": reg, "kind": kind, "what": m, "print": want, "table": got,
                                "known": KNOWN_PRINT_DEFECTS.get((reg, kind, m), "")})
            got_rig = {t.plain() for ts in T.rig("angling").values() for t in ts
                       if t.source.authority is Authority.region}
            want_rig = p["rig"][kind]
            if got_rig != want_rig:
                out.append({"region": reg, "kind": kind, "what": "rig", "print": sorted(want_rig),
                            "table": sorted(got_rig), "known": KNOWN_PRINT_DEFECTS.get((reg, kind, "rig"), "")})
    return out


def test_the_base_gear_tables_match_the_printed_synopsis_except_where_curation_is_known_wrong():
    diff = print_diff()
    unknown = [d for d in diff if not d["known"]]
    assert not unknown, unknown
    assert {(d["region"], d["kind"], d["what"]) for d in diff} == set(KNOWN_PRINT_DEFECTS), \
        "a known defect has gone away — remove it from KNOWN_PRINT_DEFECTS"


PROVINCE_PRINT = {
    # page 10, "Provincial Regulations": what a licence entitles you to, and what is unlawful
    "stream": {"Fin fish may not be used as bait", "Freshwater invertebrates may be used, from streams",
               "Roe may be used", "Barbless hook, from streams", "Single hook, from streams",
               "1 line per angler", "No more than 1 artificial fly on the line",
               "No more than 1 kg of weight on the line — does not apply to downrigger weights",
               "angle with a downrigger, provided the fishing line is attached to the downrigger by a quick-release mechanism",
               "Use a light in any manner to attract fish, unless the light is submerged and attached to the fishing line within 1 m of the hook."},
    "lake": {"Fin fish may not be used as bait", "Freshwater invertebrates may not be used as bait, from lakes",
             "Roe may be used", "Single hook", "1 line per angler", "2 lines per angler, from lakes — alone in a boat",
             "No more than 1 artificial fly on the line",
             "No more than 1 kg of weight on the line — does not apply to downrigger weights",
             "angle with a downrigger, provided the fishing line is attached to the downrigger by a quick-release mechanism",
             "Use a light in any manner to attract fish, unless the light is submerged and attached to the fishing line within 1 m of the hook."},
}


def test_the_provincial_base_matches_page_10():
    for kind, want in PROVINCE_PRINT.items():
        T = provincial_base(kind)
        got = {t.plain() for ts in T.rig("angling").values() for t in ts}
        assert got == want, (kind, got ^ want)
        verdicts = {m: MethodRow(T, m).verdict_word() for m in METHODS if T.speaks_about(m)}
        assert verdicts == {"angling": "allowed", "ice_fishing": "allowed", "set_lining": "not allowed",
                            "spear_fishing": "allowed", "crayfish_trapping": "allowed", "netting": "not allowed",
                            "snagging": "not allowed", "chumming": "not allowed"}, verdicts


# --------------------------------------------------------------------------- #
# THE CHECKS CAN FAIL. A totality test that compares a set against the set it was built
# from passes for every possible table; these prove the two guarantees notice a loss.
# --------------------------------------------------------------------------- #
def test_the_totality_check_notices_a_condition_the_page_forgot_to_draw(sections):
    """Drop one in-force condition from the rendered JSON of a Region 1 stream and the
    decision still cites it — the check must fail on the mutated render."""
    from pipeline.regs.table.method_provenance import section as section_json
    w, run, kind, rules, here, label, T = next(s for s in sections if s[2] == "stream" and "1" in s[4])
    d = section_json(w, run)
    victim = "z1:bait_ban_streams::bait_ban_streams.r1"
    assert any(t["rule"] == victim for ts in d["band"]["rig"].values() for t in ts)
    for topic in list(d["band"]["rig"]):
        d["band"]["rig"][topic] = [t for t in d["band"]["rig"][topic] if t["rule"] != victim]
    for r in d["rows"]:
        for topic in list(r["rig"]):
            r["rig"][topic] = [t for t in r["rig"][topic] if t["rule"] != victim]
        r["folded"] = [t for t in r["folded"] if t["rule"] != victim]
    row = next(r for r in d["rows"] if r["method"] == "angling")
    v = may_i_fish(T, "angling", (7, 15))
    assert victim in v.cited()
    assert victim not in _rendered_ids(row, d), "the mutation did not take"
    assert v.cited() - _rendered_ids(row, d) == {victim}


def test_the_compliance_check_notices_a_rule_the_generator_drops(sections, monkeypatch):
    """Make the generator lose one rule on the way in and COMPLIES must not print."""
    import pipeline.regs.table.method_build as mb
    from pipeline.regs.table.method_comply import audit
    w, run, kind, rules, here, label, T = next(s for s in sections if s[2] == "stream" and "1" in s[4])
    victim = "z1:single_barbless_hook::single_barbless_hook.r1"
    real = mb.terms_of
    def lossy(rules_, water_kind, here_=frozenset(), label_=""):
        terms, ledgers, lf, un = real(rules_, water_kind, here_, label_)
        return [t for t in terms if t.rule_id != victim], ledgers, lf, un
    monkeypatch.setattr(mb, "terms_of", lossy)
    mb._base.cache_clear()
    try:
        _, gone = audit(rules, here, kind, label)
    finally:
        mb._base.cache_clear()
    assert gone == [victim], gone


def test_a_rule_filed_as_the_other_kind_of_water_must_be_found_there(sections, monkeypatch):
    """`not-here` is proven by finding the rule in the other kind's table, not by reading the
    field that sent it away. Flip a stream rule to `water: lake` and hide it from the lake
    table too: the audit must report it."""
    import pipeline.regs.table.method_build as mb
    from pipeline.regs.table.method_comply import audit
    w, run, kind, rules, here, label, T = next(s for s in sections if s[2] == "stream" and "1" in s[4])
    victim = "z1:single_barbless_hook::single_barbless_hook.r1"
    mutated = [dict(x, water="lake") if f"{x['entry']}::{x['rule']}" == victim else x for x in rules]
    real = mb.terms_of
    def lossy(rules_, water_kind, here_=frozenset(), label_=""):
        terms, ledgers, lf, un = real(rules_, water_kind, here_, label_)
        return [t for t in terms if t.rule_id != victim], ledgers, lf, un
    monkeypatch.setattr(mb, "terms_of", lossy)
    mb._base.cache_clear()
    try:
        _, gone = audit(mutated, here, kind, label)
    finally:
        mb._base.cache_clear()
    assert gone == [victim], gone
