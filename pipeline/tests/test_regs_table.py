"""The laws the quota ledger rests on, and the defects that proved they were not held.

`comply` proves nothing is LOST. It cannot prove anything is RIGHT — every defect below passed
it. These are the properties that do, written against the real corpus where the corpus is what
broke them. The last section is the ORACLE: the only question an angler asks, answered with the
rule that decided it, and the proof that what decides is what the table shows.
"""
from __future__ import annotations

import collections
import pytest

from pipeline.regs.table.subject import Subject, Origin, expand
from pipeline.regs.table.outcome import Outcome, outcome_of, CLOSED, RELEASE
from pipeline.regs.table.size import size_of
from pipeline.regs.table.authority import Authority, Scope, Source, source_of
from pipeline.regs.table.ledger import (Allowance, Ledger, LIFTED, REPLACED_BY_CLAUSE,
                                        shuts_the_water)
from pipeline.regs.table.rows import rows
from pipeline.regs.table.oracle import may_i_keep, Fish, Creel
from pipeline.regs.table.build import (ledger, base, section_rules, section_regions,
                                       section_label, section_kind, name, D, WATERS)


# --------------------------------------------------------------------------- #
# `covers` must be a partial order. It has been broken three times.
# --------------------------------------------------------------------------- #
def _subjects():
    return [Subject(frozenset({"ALL_GAME_FISH"})), Subject(frozenset({"TROUT_CHAR"})),
            Subject(frozenset({"ALL_FIN_FISH"})), Subject(frozenset({"NON_GAME_FISH"})),
            Subject(frozenset({"PROTECTED_SPECIES"})), Subject(frozenset({"KO"})),
            Subject(frozenset({"ST"}), Origin.wild), Subject(frozenset({"ST"}), Origin.hatchery),
            Subject(frozenset({"ST"})), Subject(frozenset()),
            Subject(frozenset({"ALL_GAME_FISH"}), excepts=frozenset({"BB"})),
            Subject(frozenset({"ALL_FIN_FISH"}), excepts=frozenset({"CRA"}))]


def test_covers_is_reflexive():
    for s in _subjects():
        assert s.covers(s), f"{sorted(s.fish)} except {sorted(s.excepts)} does not cover itself"


def test_covers_is_transitive():
    for a in _subjects():
        for b in _subjects():
            if not a.covers(b):
                continue
            for c in _subjects():
                if b.covers(c):
                    assert a.covers(c), f"{sorted(a.fish)} > {sorted(b.fish)} > {sorted(c.fish)}"


def test_covers_is_antisymmetric():
    for a in _subjects():
        for b in _subjects():
            if a != b and a.covers(b) and b.covers(a):
                pytest.fail(f"{sorted(a.fish)}/{sorted(a.excepts)} and "
                            f"{sorted(b.fish)}/{sorted(b.excepts)} cover each other")


def test_an_open_group_does_not_invert_the_order():
    ng = Subject(frozenset({"NON_GAME_FISH"}))
    allf = Subject(frozenset({"ALL_FIN_FISH"}))
    assert not ng.covers(allf)
    assert allf.covers(ng)
    assert not ng.covers(Subject(frozenset({"KO"}))), "kokanee is a game fish"


def test_an_exception_narrows_and_does_not_widen():
    a = Subject(frozenset({"ALL_FIN_FISH"}), excepts=frozenset({"CRA"}))
    assert not a.covers(Subject(frozenset())), "carving CRA out cannot cover everything"
    assert Subject(frozenset()).covers(a)


def test_a_subject_that_cancels_itself_is_covered_by_nothing():
    empty = Subject(frozenset({"KO"}), excepts=frozenset({"KO"}))
    assert not Subject(frozenset({"BB"})).covers(empty)


def test_meets_is_symmetric_and_reflexive_where_a_subject_names_a_fish():
    hatch = Subject(frozenset({"TROUT_CHAR"}), Origin.hatchery)
    char = Subject(frozenset({"BT", "DV", "LT"}))
    assert not hatch.covers(char) and not char.covers(hatch)
    assert hatch.meets(char) and char.meets(hatch)
    for a in _subjects():
        for b in _subjects():
            assert a.meets(b) == b.meets(a)
        if a.effective() or a.is_everything:
            assert a.meets(a)
    assert not Subject(frozenset({"KO"})).meets(Subject(frozenset({"BB"})))
    assert not Subject(frozenset({"ST"}), Origin.wild).meets(Subject(frozenset({"ST"}), Origin.hatchery))


def test_a_subject_knows_which_fish_it_contains():
    """`covers` compares subjects; a person holding a fish is not holding a subject."""
    s = Subject(frozenset({"TROUT_CHAR"}), Origin.hatchery, size_of(50, None, take=1, within="p"))
    assert s.contains("RB", Origin.hatchery, 55)
    assert not s.contains("RB", Origin.hatchery, 45), "45 cm is not in the over-50 class"
    assert not s.contains("RB", Origin.wild, 55), "wild is not hatchery"
    assert not s.contains("KO", Origin.hatchery, 55), "kokanee is not a trout"
    assert s.contains("RB", Origin.hatchery, None), "a length nobody gave is not tested"
    gate = Subject(frozenset({"BT"}), size=size_of(None, 60, take=0))
    assert gate.contains("BT", Origin.wild, 40) and not gate.contains("BT", Origin.wild, 70)
    assert Subject(frozenset({"ALL_GAME_FISH"}), excepts=frozenset({"BB"})).contains("RB")
    assert not Subject(frozenset({"ALL_GAME_FISH"}), excepts=frozenset({"BB"})).contains("BB")


# --------------------------------------------------------------------------- #
# `Outcome` must be a total order on what it actually means
# --------------------------------------------------------------------------- #
def test_stricter_is_an_order_and_closed_wins():
    got = sorted([Outcome("unlimited"), Outcome("quota", 15), Outcome("quota", 2),
                  RELEASE, CLOSED], key=lambda o: o.rank)
    assert [o.word() for o in got] == ["0", "release", "2", "15", "∞"]


def test_pooled_is_stricter_than_per_species_at_the_same_number():
    assert Outcome("quota", 6, pooled=True).rank < Outcome("quota", 6).rank


def test_a_quota_of_zero_cannot_be_constructed():
    with pytest.raises(ValueError):
        Outcome("quota", 0)
    with pytest.raises(ValueError):
        Outcome("quota", None)


def test_may_target_is_an_int_and_still_means_closed():
    assert outcome_of(0, 0, False, "daily") is CLOSED
    assert outcome_of(0, 1, False, "daily") is RELEASE
    assert outcome_of(None, None, False, "daily") is None


# --------------------------------------------------------------------------- #
# Size polarity — inverted on 112 rules
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kw,expect", [
    (dict(over_cm=None, under_cm=60, take=1), "none under 60 cm"),      # r2:cultus_lake
    (dict(over_cm=50, under_cm=None, take=0), "none over 50 cm"),
    (dict(over_cm=50, under_cm=None, take=1, within="p"), "counting those over 50 cm"),
    (dict(over_cm=None, under_cm=30, take=2, within="p"), "none under 30 cm"),
    (dict(over_cm=50, under_cm=None, take=5, period="annual"), "counting those over 50 cm"),
    (dict(over_cm=100, under_cm=70, take=1, band=True), "none between 70 cm and 100 cm"),
])
def test_size_polarity(kw, expect):
    assert size_of(**kw).words() == expect


# --------------------------------------------------------------------------- #
# Provenance: two typed axes, never one integer.
# --------------------------------------------------------------------------- #
def _rule(entry, rule):
    from pipeline.regs.table.corpus import rules
    return next(x for x in rules() if x["entry"] == entry and x["rule"] == rule)


def test_authority_and_scope_are_separate_axes():
    """A Region 5 rule written for one named river is authority=regional but scope=this water.
    Under a single rank it sat at the region's rung and bound all twenty Fraser stretches."""
    s = source_of(_rule("zp:steelhead", "steelhead.r2"))
    assert (s.authority, s.scope, s.is_base) == (Authority.province, Scope.region, True)
    s = source_of(_rule("z4:trout_char_quota", "trout_char_quota.r1"))
    assert (s.authority, s.scope, s.region, s.is_base) == (Authority.region, Scope.region, "4", True)
    s = source_of(_rule("z1:summer_stream_closure", "summer_stream_closure.r1"))
    assert (s.authority, s.scope, s.is_base) == (Authority.region, Scope.area, False)
    assert "MUs 1-1 to 1-6" in s.words()
    s = source_of(_rule("z3:shuswap_annual", "shuswap_annual.r1"))
    assert (s.authority, s.scope, s.is_base) == (Authority.region, Scope.water, False)
    s = source_of(_rule("r4:kootenay_lake_main_body_for_location_see_map_on_page_34@4-19",
                        "kootenay_lake_main_body.r4"))
    assert (s.authority, s.scope, s.is_base, s.who) == (Authority.region, Scope.water, False, "this water")
    s = source_of(dict(_rule("r4:kootenay_lake_s_tributaries@4-19+4-7", "kootenay_lake_tributaries.r1"),
                       via="trib"))
    assert (s.scope, s.who, s.rank) == (Scope.inherited, "inherited", 1)
    s = source_of(_rule("zp:protected_species", "protected_species.r1"))
    assert (s.authority, s.rank, s.who) == (Authority.superior, -1, "Federal or Parks")
    s = source_of(_rule("zp:spear_fishing", "spear_fishing.r2"))
    assert s.regions == {"3", "5", "6", "7a", "7b", "8"} and "Regions 3, 5" in s.words()


def test_the_ladder_orders_scope_before_authority():
    """"This water overrides regional" is about what a rule binds to, not who wrote it."""
    water = Source(Authority.province, Scope.water)
    region = Source(Authority.region, Scope.region, "4")
    prov = Source(Authority.province, Scope.region)
    area = Source(Authority.region, Scope.area, "1")
    inh = Source(Authority.region, Scope.inherited, "4")
    assert water.rank < inh.rank < area.rank < region.rank < prov.rank


# --------------------------------------------------------------------------- #
# The ledger, against the live corpus
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def tables():
    """Built the way the pipeline builds them — WITH the section's regions and label."""
    out = []
    for w in WATERS:
        kind = section_kind(w)
        for run in range(len(D[w].get("runs") or [])):
            rs = section_rules(w, run)
            if rs:
                L = ledger(rs, kind, section_regions(w, run), section_label(w, run))
                out.append((w, run, kind, rs, L, rows(L, name)))
    return out


def _one(tables, w, run):
    return next(t for t in tables if t[0] == w and t[1] == run)


def _row(t, species, origin=Origin.wild):
    """The row a fish of this species and origin sits in."""
    for r in t:
        if species in r.fish and r.origin in (Origin.both, origin):
            return r
    raise AssertionError(f"no row for {species}/{origin.value} in "
                         f"{[(r.heading(name), r.qualifier()) for r in t]}")


def test_no_fish_is_in_two_rows(tables):
    for w, run, _, _, _, t in tables:
        seen = collections.Counter((sp, o) for r in t for sp in r.fish
                                   for o in ((Origin.wild, Origin.hatchery)
                                             if r.origin is Origin.both else (r.origin,)))
        dupe = [k for k, n in seen.items() if n > 1]
        assert not dupe, f"{w} stretch {run + 1}: {dupe[:3]} sit in two rows"


def test_every_row_shows_every_counter_that_binds_it(tables):
    """NOTHING IN FORCE IS HIDDEN. Every counter the ledger says binds a fish on some date is
    on that fish's row — a cap inside the number, a bound, the annual clock, the possession
    clock — because the row is derived from the counters and not the other way round."""
    for w, run, _, _, L, t in tables:
        for r in t:
            o = r.origin if r.origin is not Origin.both else Origin.wild
            for sp in r.fish:
                for a in L.allowances:
                    if L.reaches(a, sp, o):
                        assert a in r.counters, (w, run + 1, r.heading(name), a.rule_id)


def test_a_species_closure_does_not_shut_a_water_that_is_open(tables):
    """Region 4's "Bass: 0 quota" is a quota of zero, not a closure of Kootenay Lake, and the
    lake's own "Bass daily quota = unlimited" replaces it — upward."""
    _, _, _, _, L, t = _one(tables, "Kootenay Lake", 0)
    bass = _row(t, "LMB")
    assert bass.headline().outcome.kind == "unlimited"
    assert bass.headline().source.scope is Scope.water
    assert any(a.rule_id.endswith("species_quotas.r1") and "replaced by" in st
               for a, st in bass.behind)


def test_a_broad_local_quota_does_not_override_a_narrow_wider_protection(tables):
    """"All wild steelhead must be released" is provincial; Region 4's "Trout/char: 5" is
    closer and says nothing about wild steelhead. The release stands, on every section."""
    for w, run, _, _, _, t in tables:
        for r in t:
            if "ST" in r.fish and r.origin is not Origin.hatchery:
                h = r.headline()
                assert h is None or h.outcome.kind != "quota", (
                    f"{w} stretch {run + 1} offers wild steelhead to keep: {h.word()}")


def test_a_superior_authority_is_not_outranked(tables):
    """Fishing in a National Park is prohibited unless the National Parks regulations open it.
    A regional quota does not open it."""
    for w, run, _, _, L, t in tables:
        park = [a for a in L.allowances if a.source.authority is Authority.superior
                and a.kind == "closed" and shuts_the_water(a.scope) and L.in_force(a)
                and a.applies.always]
        if not park:
            continue
        for r in t:
            h = r.headline()
            assert h is not None and h.outcome.kind == "closed" and \
                h.source.authority is Authority.superior, (w, run + 1, r.heading(name))


def test_nothing_counts_under_a_headline_of_zero(tables):
    """A possession multiple is a multiplier on a daily limit, and an annual ceiling is a
    ceiling on a number. Under release or closed both are moot, and the row says so."""
    for w, run, _, _, _, t in tables:
        for r in t:
            h = r.headline()
            if h is None or not h.is_zero:
                continue
            for a in r.counters:
                if not a.is_zero:
                    assert r.moot(a), (w, run + 1, r.heading(name), a.rule_id)


def test_no_new_self_lifting_rules_appear():
    from pipeline.regs.table.corpus import rules
    from pipeline.regs.table.lifts import self_lifting
    got = {x["rule"] for x in self_lifting(rules())}
    known = {"z6:steelhead_stream_closure::steelhead_stream_closure.r1"}
    assert got <= known, f"new self-lifting rule(s) in the corpus: {sorted(got - known)}"


# --------------------------------------------------------------------------- #
# The order: authority wins, except a take of zero. Four real cases, as counters.
# --------------------------------------------------------------------------- #
def _src(tier, rule_id):
    return {"prov": Source(Authority.province, Scope.region, "", "", frozenset(), rule_id, rule_id),
            "region": Source(Authority.region, Scope.region, "4", "", frozenset(), rule_id, rule_id),
            "water": Source(Authority.region, Scope.water, "4", "a lake", frozenset(), rule_id, rule_id),
            }[tier]


def _alw(rule_id, tier, fish, outcome, origin=Origin.both):
    return Allowance(Subject(frozenset(fish), origin), outcome, _src(tier, rule_id))


def _answer(allowances, species, origin=Origin.wild):
    L = Ledger(allowances)
    h = _row(rows(L), species, origin).headline()
    return h.outcome, h.rule_id


def test_a_take_of_zero_stands_over_a_closer_number_for_a_broader_group():
    out, gov = _answer([_alw("zp:steelhead.r2", "prov", {"ST"}, RELEASE, Origin.wild),
                        _alw("z4:trout_char.r1", "region", {"TROUT_CHAR"}, Outcome("quota", 5, pooled=True))],
                       "ST", Origin.wild)
    assert out == RELEASE and gov == "zp:steelhead.r2"


def test_b_the_closest_authority_wins_across_subjects_when_nothing_is_closed():
    """Kootenay Lake's own "rainbow trout daily quota = 10" over Region 4's "Trout/char: 5":
    the water is the closer authority and the answer is 10 — and the region's 5 still
    answers for the rest of the group, minus the rainbow."""
    alws = [_alw("r4:kootenay_lake_main_body.r4", "water", {"RB"}, Outcome("quota", 10)),
            _alw("z4:trout_char.r1", "region", {"TROUT_CHAR"}, Outcome("quota", 5, pooled=True))]
    out, gov = _answer(alws, "RB")
    assert out.n == 10 and gov == "r4:kootenay_lake_main_body.r4"
    out, gov = _answer(alws, "CT")
    assert out.n == 5
    L = Ledger(alws)
    tc = next(a for a in L.allowances if a.rule_id == "z4:trout_char.r1")
    assert L.reaches(tc, "CT", Origin.wild) and not L.reaches(tc, "RB", Origin.wild)


def test_c_at_equal_authority_the_narrower_subject_speaks_first():
    """Region 8's "20 brook trout from streams" beside its "Trout/char: 4 from streams": a
    narrower rule with a larger number can only mean the brook trout are outside the four."""
    out, gov = _answer([_alw("z8:trout_char.r3", "region", {"TROUT_CHAR"}, Outcome("quota", 4, pooled=True)),
                        _alw("z8:trout_char.r5", "region", {"EB"}, Outcome("quota", 20))], "EB")
    assert out.n == 20 and gov == "z8:trout_char.r5"


def test_d_a_water_replaces_a_regional_number_upward():
    out, gov = _answer([_alw("r4:some_lake.r1", "water", {"TROUT_CHAR"}, Outcome("quota", 10, pooled=True)),
                        _alw("z4:trout_char.r1", "region", {"TROUT_CHAR"}, Outcome("quota", 5, pooled=True))],
                       "RB")
    assert out.n == 10 and gov == "r4:some_lake.r1"


def test_the_mirror_case_still_holds():
    """A WATER's "trout and char: 2" over the province's "rainbow trout: 5": the water is the
    closer authority and 2 is the answer for a rainbow, even though the province named it."""
    out, gov = _answer([_alw("r:water.r1", "water", {"TROUT_CHAR"}, Outcome("quota", 2, pooled=True)),
                        _alw("zp:rb.r1", "prov", {"RB"}, Outcome("quota", 5))], "RB")
    assert out.n == 2 and gov == "r:water.r1"


def test_a_species_closure_is_lifted_by_the_water_but_a_superior_one_is_not():
    """"Bass: 0 quota, CLOSED TO FISHING (see tables for exceptions)" — the book says the
    water tables are the exceptions, so a water's own rule on the same fish replaces it. A
    superior closure is outside the ladder, and a "No Fishing" on the water is lifted by
    nothing but a lift."""
    out, gov = _answer([_alw("r:water.r1", "water", {"RB"}, RELEASE),
                        _alw("z4:x.r1", "region", {"TROUT_CHAR"}, CLOSED)], "RB")
    assert out == RELEASE and gov == "r:water.r1"
    sup = Allowance(Subject(frozenset({"TROUT_CHAR"})), CLOSED,
                    Source(Authority.superior, Scope.region, rule_id="zp:sara.r1", verbatim="x"))
    out, gov = _answer([_alw("r:water.r1", "water", {"RB"}, RELEASE), sup], "RB")
    assert out == CLOSED and gov == "zp:sara.r1"
    out, gov = _answer([_alw("r:water.r1", "water", {"ALL_GAME_FISH"}, CLOSED),
                        _alw("z4:x.r1", "region", {"RB"}, Outcome("quota", 5))], "RB")
    assert out == CLOSED
    out, gov = _answer([_alw("z4:x.r1", "region", {"ALL_GAME_FISH"}, CLOSED),
                        _alw("r:water.r1", "water", {"RB"}, Outcome("quota", 5))], "RB")
    assert out == CLOSED, "a water's number does not open a No Fishing on the water"


def test_a_clause_counts_inside_its_parent_and_never_carves_it():
    """"1 bull trout" inside "Trout/char: 5" is one of the five, not a sixth."""
    src = _src("region", "z4:tc.r1")
    parent = Allowance(Subject(frozenset({"TROUT_CHAR"})), Outcome("quota", 5, pooled=True), src)
    cap = Allowance(Subject(frozenset({"BT", "DV"})), Outcome("quota", 1, pooled=True),
                    _src("region", "z4:tc.r4"), within="z4:tc.r1")
    L = Ledger([parent, cap], family={"z4:tc.r4": frozenset({"z4:tc.r1"})})
    assert L.reaches(parent, "BT", Origin.wild) and L.reaches(cap, "BT", Origin.wild)
    assert L.carves[parent] == [] and L.carves[cap] == []


def test_a_gate_carves_nothing():
    """"None under 60 cm" on char is true beside the five, not instead of it."""
    parent = _alw("z3:tc.r1", "region", {"TROUT_CHAR"}, Outcome("quota", 5, pooled=True))
    gate = Allowance(Subject(frozenset({"BT", "DV", "LT"}), size=size_of(None, 60, take=None)),
                     RELEASE, _src("region", "z3:tc.r4b"))
    L = Ledger([parent, gate])
    assert gate.kind == "gate" and L.reaches(parent, "BT", Origin.wild)
    assert L.carves[parent] == []


def test_brook_trout_on_the_okanagan_is_twenty(tables):
    seen = 0
    for w, run, _, _, _, t in tables:
        if w != "Okanagan River":
            continue
        assert _row(t, "EB").headline().outcome.n == 20, (w, run + 1)
        seen += 1
    assert seen == 3


def test_a_stream_clause_does_not_retire_its_parent_on_a_lake(tables):
    from pipeline.regs.table.corpus import rid
    for w, run, kind, rules, L, _ in tables:
        may_retire = {f"{c.get('entry')}::{c.get('within')}" for c in rules
                      if c.get("within") and c.get("water") == kind
                      and c.get("take") is not None}
        for a, st in L.status.items():
            if st == REPLACED_BY_CLAUSE:
                assert a.rule_id in may_retire, (w, run + 1, kind, a.rule_id)


def test_kootenay_lake_main_body_is_the_corpus_form_of_case_b(tables):
    _, _, _, _, L, t = _one(tables, "Kootenay Lake", 0)
    rb = _row(t, "RB")
    assert rb.headline().outcome.n == 10 and rb.headline().source.who == "this water"
    assert rb.fish == {"RB"}
    ct = _row(t, "CT")
    assert ct.headline().outcome.n == 5 and ct.headline().source.who == "Region 4"
    # ...and the group row says it no longer speaks for the rainbow.
    tc = next(a for a in ct.counters if a.rule_id.endswith("trout_char_quota.r1"))
    assert any(c.scope.fish == {"RB"} for c in L.carves[tc])


# --------------------------------------------------------------------------- #
# A named-water list qualified by a region is not a region scope.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("text,kind,regions", [
    ("Regions 3, 5, 6, 7 and 8", "regions", {"3", "5", "6", "7", "8"}),
    ("Regions 1, 2 and 4", "regions", {"1", "2", "4"}),
    ("Fraser, Lower Pitt and Lower Harrison Rivers, Region 2", "undrawable", set()),
    ("Hells Gate upstream to the Region 3 boundary", "undrawable", set()),
    ("Fraser watershed within Region 6", "undrawable", set()),
    ("non-tidal portion of the Fraser River in Region 2", "undrawable", set()),
    ("Regions 3-8", "undrawable", set()),
])
def test_a_region_mention_scopes_only_when_it_is_the_whole_extent(text, kind, regions):
    from pipeline.regs.table.where import parse_where
    w = parse_where(text)
    assert (w.kind, set(w.regions)) == (kind, regions)


# The bait-exemption property moved to the gear table's own suite; the gear table is being
# ported to the ledger by its owner and this file does not reach into its API.


# --------------------------------------------------------------------------- #
# White sturgeon: a closure the book scopes by population, and a table that cannot.
# --------------------------------------------------------------------------- #
def test_the_contradicted_closures_are_the_sara_listing_and_no_others():
    from pipeline.regs.table.corpus import rules
    from pipeline.regs.table.lifts import contradicted_closures
    got = {(x["closure"], x["opened_by"]) for x in contradicted_closures(rules())}
    openers = {"z1:species_quotas::species_quotas.r5", "z2:species_quotas::species_quotas.r7",
               "z3:species_quotas::species_quotas.r7", "z5:white_sturgeon::white_sturgeon.r2",
               "zp:white_sturgeon_licence::white_sturgeon_licence.r2"}
    assert got == {("z7a:sara_sturgeon::sara_sturgeon.r1", o) for o in openers}, sorted(got)


def test_the_fraser_sturgeon_fishery_is_catch_and_release_where_the_book_says_so(tables):
    seen = 0
    for w, run, _, _, L, t in tables:
        if w != "Fraser River":
            continue
        r = _row(t, "WSG")
        h = r.headline()
        regions = section_regions(w, run)
        if regions <= {"2", "3"} and run + 1 not in (5, 6):
            assert h is not None and h.outcome == RELEASE, (run + 1, h)
            assert h.source.authority is Authority.region and h.source.scope is Scope.region
            assert any(a.rule_id == "z7a:sara_sturgeon::sara_sturgeon.r1" for a, _ in r.behind)
            seen += 1
        elif run + 1 >= 17:
            assert h is not None and h.outcome == CLOSED and h.rule_id.endswith("fraser_river.r5"), run + 1
    assert seen == 11    # 13 stretches in Regions 2 and 3, less Hell's Gate (2)


def test_a_take_of_zero_beats_a_closer_number_only_where_the_book_means_it(tables):
    """Measured: the exception changes the answer only for wild steelhead — and for the bull
    trout of the Elk and upper Kootenay, where the tributary walk reaches past "Does not
    include the Kootenay River upstream from Kootenay Lake to the U.S. border", which is prose
    and not `tributary_excludes`. A data defect, named so it cannot pass as the fold's."""
    known = {("Elk River", 1, "BT"), ("Kootenay River", 7, "BT"),
             ("Kootenay River", 8, "BT"), ("Kootenay River", 10, "BT")}
    wider = []
    for w, run, _, _, L, t in tables:
        for r in t:
            h = r.headline()
            if h is None or not h.is_zero:
                continue
            closer = [a for a in r.counters if a.outcome.kind in ("quota", "unlimited")
                      and a.period == "daily" and a.rank < h.rank and not a.within
                      and (a.applies.always or a.applies.unless)]
            if not closer:
                continue
            for sp in r.fish:
                if sp != "ST" and (w, run + 1, sp) not in known:
                    wider.append((w, run + 1, sp, h.rule_id))
                if (w, run + 1, sp) in known:
                    assert h.rule_id.endswith("kootenay_lake_tributaries.r1") and h.source.scope is Scope.inherited
    assert not wider, wider[:6]


def test_a_stream_rule_never_reaches_a_lake_ledger_by_any_route(tables):
    from pipeline.regs.table.corpus import rid
    for w, run, kind, rules, L, _ in tables:
        other = "lake" if kind == "stream" else "stream"
        bad = {rid(x) for x in rules if x.get("water") == other}
        for a in L.allowances:
            assert a.rule_id not in bad, f"{w} stretch {run + 1} is a {kind}, and {a.rule_id} is about {other}s"


# --------------------------------------------------------------------------- #
# A size bound is a gate, not a competitor.
# --------------------------------------------------------------------------- #
def test_a_size_bound_reaches_the_keep_row_it_narrows_across_origin(tables):
    """Region 2's "none under 60 cm" is about char of either origin; the Fraser's only keep
    row is hatchery-only. A hatchery bull trout is inside both, and the bound is on its row."""
    for w, run, _, _, _, t in tables:
        if w != "Fraser River" or run > 2:
            continue
        bt = _row(t, "BT", Origin.hatchery)
        ids = {a.rule_id for a in bt.counters if a.kind == "gate"}
        assert "z2:trout_char_quota::trout_char_quota.r5b" in ids, (run + 1, ids)
        assert "z2:trout_char_quota::trout_char_quota.r8" in ids, (run + 1, ids)
        rb = _row(t, "RB", Origin.hatchery)
        assert "z2:trout_char_quota::trout_char_quota.r8" in {a.rule_id for a in rb.counters}


def test_a_take_of_zero_on_a_size_class_is_the_gate_and_not_a_headline(tables):
    for w, run, _, _, L, t in tables:
        for r in t:
            h = r.headline()
            assert h is None or h.kind != "gate", (w, run + 1, r.heading(name))
            for a in r.counters:
                if a.kind == "gate":
                    assert L.carves[a] == [] or all(c.kind == "gate" for c in L.carves[a])


def test_every_size_bound_handed_in_rides_on_a_row(tables):
    from pipeline.regs.table.corpus import rid
    from pipeline.regs.table.build import _water_of
    for w, run, kind, rules, L, t in tables:
        by = {rid(x): x for x in rules}
        on_rows = {a.rule_id for r in t for a in r.counters}
        for x in rules:
            if str(x.get("type") or "") != "retention_limit" or x.get("method"):
                continue
            size = size_of(x.get("over_cm"), x.get("under_cm"), take=x.get("take"),
                           within=x.get("within"), band=bool(x.get("band")),
                           period=x.get("period") or "daily")
            wk = _water_of(x, by)
            if not size.is_gate or (wk and wk != kind):
                continue
            a = next(a for a in L.allowances if a.rule_id == rid(x) and a.kind == "gate")
            assert rid(x) in on_rows or L.status[a], (w, run + 1, rid(x), x.get("verbatim"))


def test_a_licence_rule_with_a_length_in_it_is_not_a_size_bound(tables):
    _, _, _, _, L, t = _one(tables, "Shuswap Lake", 0)
    for a in L.allowances:
        assert "shuswap_lake.r13" not in a.rule_id and "shuswap_lake.r14" not in a.rule_id


def test_the_shuswap_rainbow_row_carries_its_floor_as_a_gate(tables):
    """"Rainbow trout daily quota = 1 (none under 50 cm)" is one rule saying two things."""
    _, _, _, _, L, t = _one(tables, "Shuswap Lake", 0)
    rb = _row(t, "RB")
    assert rb.headline().outcome.n == 1
    gates = [a for a in rb.counters if a.kind == "gate" and L.in_force(a)]
    assert [g.scope.size.words() for g in gates] == ["none under 50 cm"]
    assert gates[0].rule_id == rb.headline().rule_id


def test_a_stream_clause_is_not_a_condition_on_a_lake(tables):
    for w, run, kind, _, L, _ in tables:
        if kind == "lake":
            assert not any(a.rule_id == "z8:trout_char_quota::trout_char_quota.r4"
                           for a in L.allowances), (w, run + 1)


# --------------------------------------------------------------------------- #
# What a section is handed: the page's spans, and the walk.
# --------------------------------------------------------------------------- #
def test_an_empty_span_list_binds_no_stretch():
    from pipeline.regs.table.corpus import section_rules, rid
    closure = "r5:atnarko_bella_coola_rivers_includes_tributaries_except_burnt@5-11+5-6+5-8::atnarko_bella_coola_rivers.r2"
    got = {run: {rid(x) for x in section_rules("Atnarko River", run)[0]} for run in range(6)}
    assert closure not in got[0] and closure not in got[4] and closure not in got[5]
    assert closure in got[1] and closure in got[2] and closure in got[3]
    assert closure not in {rid(x) for x in section_rules("Bella Coola River", 0)[0]}
    chilliwack = {rid(x).split("::")[-1] for x in section_rules("Chilliwack River", 0)[0]}
    assert "chilliwack_vedder_rivers.r9" not in chilliwack
    assert "chilliwack_vedder_rivers.r3" in chilliwack


def test_a_rule_reached_by_the_tributary_walk_is_inherited(tables):
    _, _, _, _, L, t = _one(tables, "Fording River", 0)
    elk = [a for a in L.allowances if "elk_river_s_tributaries" in a.rule_id]
    assert elk and all(a.source.scope is Scope.inherited and a.rank == 1 for a in elk)
    own = [a for a in L.allowances if "fording_river_downstream" in a.rule_id]
    assert own and all(a.source.scope is Scope.water and a.rank == 0 for a in own)


# --------------------------------------------------------------------------- #
# The answer on a date.
# --------------------------------------------------------------------------- #
def test_a_row_with_a_season_of_its_own_is_its_own_row(tables):
    """Shuswap's "Lake trout — release, Oct 15 – Jan 31" makes lake trout its own row, with
    the season on it — not absorbed into "Trout and char"."""
    _, _, _, _, L, t = _one(tables, "Shuswap Lake", 0)
    lt = _row(t, "LT")
    assert lt.fish == {"LT"}
    assert lt.headline().outcome.n == 1 and lt.headline().rule_id.endswith("shuswap_lake.r9")
    assert any("Oct 15" in a.applies.detail and a.outcome == RELEASE for a in lt.counters)
    assert lt.headline((11, 1)).outcome == RELEASE
    _, _, _, _, L, t = _one(tables, "Skeena River", 0)
    rb = _row(t, "RB")
    assert {a.outcome.word() for a in rb.counters if not a.applies.always and a.period == "daily"
            and a.kind != "gate"} == {"release", "1"}


def test_a_lifted_rule_never_decides_a_date():
    from pipeline.regs.table.provenance import section
    d = section("Fraser River", 8, (3, 1))            # Thompson River → Fraser River
    burbot = next(r for r in d["rows"] if r["fish"] == ["BB"])
    assert burbot["answer_today"] == "2", burbot["today_by"]
    assert any(c["status"] == LIFTED for c in burbot["behind"])


def test_a_narrower_rung_does_not_answer_for_the_whole_row():
    """Region 6's "No fishing for steelhead, May 15 – Jun 15" shuts steelhead, not every
    trout and char on the Skeena."""
    from pipeline.regs.table.provenance import section
    d = section("Skeena River", 0, (6, 1))
    by = {tuple(r["fish"]) + (r["qualifier"],): r for r in d["rows"]}
    assert by[("ST", "wild only")]["answer_today"] == "0"
    trout = next(r for r in d["rows"] if "RB" in r["fish"])
    assert trout["answer_today"] == "release"
    char = next(r for r in d["rows"] if "AC" in r["fish"])
    assert char["answer_today"] == "5"


def test_a_closure_within_the_day_is_not_a_days_answer():
    from pipeline.regs.table.provenance import section
    d = section("Harrison River", 0, (3, 1))
    ko = next(r for r in d["rows"] if r["fish"] == ["KO"])
    assert ko["answer_today"] == "release", ko["today_by"]
    d = section("Kootenay Lake", 2, (7, 15))
    ko = next(r for r in d["rows"] if r["fish"] == ["KO"])
    assert ko["answer_today"] == "5" and ko["year_round"]


def test_dates_the_book_writes_as_exceptions_are_read_as_exceptions():
    from pipeline.regs.table.provenance import section
    d = section("Kootenay Lake", 1, (4, 2))
    ko = next(r for r in d["rows"] if r["fish"] == ["KO"])
    assert ko["keep"] == "release" and ko["answer_today"] == "5", (ko["keep"], ko["answer_today"])
    # ...and on that day the 5 is not moot: a page that hides moot counters must show it.
    five = next(c for c in ko["counters"] if c["keep"] == "5")
    assert not five["moot"] and five["rule"] == ko["today_by"]
    assert next(c for c in ko["counters"] if c["keep"] == "release")["moot"] is False
    d = section("Kootenay Lake", 1, (8, 1))
    ko = next(r for r in d["rows"] if r["fish"] == ["KO"])
    assert ko["answer_today"] == "release"


def test_the_calendar_says_when_the_headline_never_holds():
    from pipeline.regs.table.provenance import section
    d = section("Skeena River", 0)
    trout = next(r for r in d["rows"] if "RB" in r["fish"] and not r["qualifier"])
    assert trout["keep"] == "5" and not trout["year_round"]
    assert [(s["from"], s["to"], s["keep"]) for s in trout["calendar"]] == [
        ([11, 1], [6, 30], "release"), ([7, 1], [10, 31], "1")]


def test_a_subject_only_seasons_speak_to_has_no_standing_number():
    """White sturgeon on the Fraser in Region 5, below Williams Lake River, has one rule:
    "No Fishing for sturgeon Sept 15 – July 15". There is no standing number and the row
    says so; the calendar carries the closure."""
    from pipeline.regs.table.provenance import section
    d = section("Fraser River", 14)
    wsg = next(r for r in d["rows"] if r["fish"] == ["WSG"])
    assert wsg["keep"] is None
    assert [s["keep"] for s in wsg["calendar"]] == ["0", None]


# --------------------------------------------------------------------------- #
# TWO STAGES: the region's standing table, then this water's overrides.
# --------------------------------------------------------------------------- #
def test_nothing_water_scoped_can_enter_a_base(tables):
    """A base table is made of region-wide rules and nothing else — by type, not by guard."""
    for w, run, kind, rules, L, _ in tables:
        B = base(rules, kind)
        for a in B.allowances:
            assert a.source.scope is Scope.region, (w, run + 1, a.rule_id)
        # ...and every override on the section is NOT region-wide.
        base_ids = {a.rule_id for a in B.allowances}
        for a in L.allowances:
            if a.rule_id not in base_ids:
                assert a.source.scope is not Scope.region, (w, run + 1, a.rule_id)


def test_a_base_is_computed_once_per_region_and_kind(tables):
    """Every Region 2 stream stretch draws on the same standing table — the same object."""
    fraser = [t for t in tables if t[0] == "Fraser River" and t[1] < 3]
    bases = {id(base(rules, kind)) for _, _, kind, rules, _, _ in fraser}
    assert len(bases) == 1
    chilliwack = _one(tables, "Chilliwack River", 0)
    assert base(chilliwack[3], "stream") is base(fraser[0][3], "stream")


def test_region_2_lakes_and_streams_have_different_base_quotas(tables):
    """PROOF 4. Region 2's trout base is 4 on a lake and, on a stream, 2 hatchery or wild
    release. A stream number never answers on a lake and vice versa — this has regressed."""
    _, _, _, rules, _, _ = _one(tables, "Fraser River", 0)
    stream = base(rules, "stream")
    lake = base(rules, "lake")
    s_rb = _row(rows(stream), "RB", Origin.hatchery).headline()
    assert s_rb.outcome.n == 2 and s_rb.scope.origin is Origin.hatchery
    assert _row(rows(stream), "RB", Origin.wild).headline().outcome == RELEASE
    l_rb = _row(rows(lake), "RB", Origin.wild).headline()
    assert l_rb.outcome.n == 4 and l_rb.rule_id.endswith("trout_char_quota.r1")
    assert not any(a.rule_id.endswith("trout_char_quota.r4") for a in lake.allowances)
    assert not any(a.rule_id.endswith("trout_char_quota.r6") for a in lake.allowances)
    # And on Kootenay LAKE, Region 4's "2 from streams" is nowhere.
    _, _, _, rules, L, t = _one(tables, "Kootenay Lake", 0)
    assert not any(a.rule_id.endswith("trout_char_quota.r3") for a in L.allowances)
    assert _row(t, "CT").headline().outcome.n == 5


# --------------------------------------------------------------------------- #
# THE ORACLE: may I keep it? — with the rule that decided, and its provenance.
# --------------------------------------------------------------------------- #
def _ledger(w, run):
    return ledger(section_rules(w, run), section_kind(w), section_regions(w, run), section_label(w, run))


def test_proof_1_a_pooled_group_quota_is_spent_by_any_member(tables):
    """A bull trout in the creel blocks a lake trout where the char quota is pooled ("1 char,
    bull trout, Dolly Varden or lake trout") — and does NOT block a rainbow on Kootenay Lake,
    where the rainbow has its own number."""
    L = _ledger("Fraser River", 0)                                   # Region 2 stream
    bt = Fish("BT", 65, Origin.hatchery)
    v = may_i_keep(L, Fish("LT", 65, Origin.hatchery), (7, 15), Creel.of(bt), name)
    assert v.keep is False and v.kind == "spent"
    assert v.decided_by[0].rule_id == "z2:trout_char_quota::trout_char_quota.r5"
    assert "bull trout" in v.reasons[0] and "Region 2 · region-wide" in v.reasons[0]
    v = may_i_keep(L, Fish("LT", 65, Origin.hatchery), (7, 15), Creel(), name)
    assert v.keep is True
    # Per-species: Kootenay Lake's bull trout (its own 1) does not touch its rainbow (its own 10).
    K = _ledger("Kootenay Lake", 0)
    v = may_i_keep(K, Fish("RB", 40), (7, 15), Creel.of(Fish("BT", 60)), name)
    assert v.keep is True and all(c.counter.scope.fish == {"RB"} for c in v.checks)
    # ...but on the Upper West Arm the water's own "trout/char 2 (only 1 bull trout)" is pooled.
    U = _ledger("Kootenay Lake", 1)
    v = may_i_keep(U, Fish("RB", 40), (7, 15), Creel.of(Fish("BT", 60), Fish("CT", 40)), name)
    assert v.keep is False and v.decided_by[0].rule_id.endswith("kootenay_lake_upper_west_arm.r2")
    v = may_i_keep(U, Fish("BT", 60), (7, 15), Creel.of(Fish("BT", 60)), name)
    assert v.keep is False and v.decided_by[0].rule_id.endswith("kootenay_lake_upper_west_arm.r3")
    # Whitefish, 15 all species combined: fifteen lake whitefish spend it for mountain whitefish.
    v = may_i_keep(K, Fish("MW", 30), (7, 15), Creel.of(*[Fish("LW", 30)] * 15), name)
    assert v.keep is False and v.decided_by[0].rule_id == "z4:species_quotas::species_quotas.r10"


def test_proof_2_a_size_class_inside_a_quota_is_a_live_constraint_on_the_row(tables):
    """"5 trout and char, of which 1 rainbow or cutthroat over 50 cm": a 55 cm cutthroat is
    refused when one over-50 is already held, allowed when none is — and the cap is ON the
    cutthroat's row, not in a panel."""
    K = _ledger("Kootenay Lake", 0)
    cap = "z4:trout_char_quota::trout_char_quota.r2"
    v = may_i_keep(K, Fish("CT", 55), (7, 15), Creel.of(Fish("CT", 52)), name)
    assert v.keep is False and v.decided_by[0].rule_id == cap, v.reasons
    assert "over 50 cm" in v.reasons[0]
    v = may_i_keep(K, Fish("CT", 55), (7, 15), Creel.of(Fish("CT", 40)), name)
    assert v.keep is True
    v = may_i_keep(K, Fish("CT", 45), (7, 15), Creel.of(Fish("CT", 52)), name)
    assert v.keep is True, "45 cm is not in the over-50 class"
    _, _, _, _, _, t = _one(tables, "Kootenay Lake", 0)
    assert cap in {a.rule_id for a in _row(t, "CT").counters}
    assert cap not in {a.rule_id for a in _row(t, "RB").counters}, "the water's 10 is any size"


def test_proof_3_hatchery_and_wild_are_decided_apart(tables):
    L = _ledger("Fraser River", 0)
    v = may_i_keep(L, Fish("RB", 40, Origin.wild), (7, 15), Creel(), name)
    assert v.keep is False and v.kind == "release"
    assert v.decided_by[0].rule_id == "z2:trout_char_quota::trout_char_quota.r6"
    v = may_i_keep(L, Fish("RB", 40, Origin.hatchery), (7, 15), Creel(), name)
    assert v.keep is True and v.decided_by[0].rule_id == "z2:trout_char_quota::trout_char_quota.r4"
    v = may_i_keep(L, Fish("RB", 25, Origin.hatchery), (7, 15), Creel(), name)
    assert v.keep is False and v.kind == "gate" and "none under 30 cm" in v.reasons[0]
    assert v.decided_by[0].rule_id == "z2:trout_char_quota::trout_char_quota.r8"


def test_proof_5_daily_annual_and_possession_are_three_counters(tables):
    K = _ledger("Kootenay Lake", 0)
    ten = [Fish("RB", 40)] * 10
    v = may_i_keep(K, Fish("RB", 40), (7, 15), Creel(today=ten, held=ten, this_year=ten), name)
    assert v.keep is False and v.decided_by[0].period == "daily" and v.decided_by[0].n == 10
    twenty = [Fish("RB", 40)] * 20
    v = may_i_keep(K, Fish("RB", 40), (7, 15), Creel(today=[], held=twenty, this_year=twenty), name)
    assert v.keep is False and v.decided_by[0].period == "possession" and v.decided_by[0].n == 20
    assert v.decided_by[0].multiplied_by.rule_id == "zp:quota_defaults::quota_defaults.r1"
    big = [Fish("RB", 55)] * 20
    v = may_i_keep(K, Fish("RB", 55), (7, 15), Creel(today=[], held=[], this_year=big), name)
    assert v.keep is False and v.decided_by[0].period == "annual"
    assert v.decided_by[0].rule_id.endswith("kootenay_lake_main_body.r6")
    v = may_i_keep(K, Fish("RB", 45), (7, 15), Creel(today=[], held=[], this_year=big), name)
    assert v.keep is True, "the annual 20 counts fish over 50 cm only"


def test_proof_6_closed_and_release_speak_the_same_vocabulary(tables):
    P = _ledger("Kootenay River", 8)                                  # Kootenay National Park
    for sp in ("RB", "BT", "MW", "BB"):
        v = may_i_keep(P, Fish(sp, 40), (7, 15), Creel(), name)
        assert v.keep is False and v.kind == "closed"
        assert v.decided_by[0].source.authority is Authority.superior, v.reasons
    K = _ledger("Kootenay Lake", 0)
    v = may_i_keep(K, Fish("KO", 30), (7, 15), Creel(), name)
    assert v.keep is False and v.kind == "release"
    assert v.decided_by[0].rule_id.endswith("kootenay_lake_main_body.r2")
    assert "for this water" in v.reasons[0]
    v = may_i_keep(K, Fish("NP", 60), (7, 15), Creel(), name)
    assert v.keep is False and v.kind == "closed" and "Region 4 · region-wide" in v.reasons[0]


def test_the_oracle_is_total_and_decides_only_by_what_the_rows_show(tables):
    """For every section, every row's fish, both origins, three lengths and three dates: the
    oracle answers, and every counter it decided by is on that fish's row."""
    for w, run, _, _, L, t in tables:
        for r in t:
            sp = r.species
            for o in ((Origin.wild, Origin.hatchery) if r.origin is Origin.both else (r.origin,)):
                shown = {a.rule_id for a in r.counters}
                for length in (20, 45, 70):
                    for on in ((1, 15), (5, 1), (7, 15), (10, 15)):
                        v = may_i_keep(L, Fish(sp, length, o), on, Creel(), name)
                        if v.keep is None:
                            # Undecided only where the row is honestly empty on that day: a
                            # fish the region names in a seasonal closure and nowhere else.
                            assert not any(L.binds(a, sp, o, length, on) for a in r.counters), (
                                w, run + 1, r.heading(name), sp, o, length, on)
                            assert not any((a.applies.always or a.applies.unless)
                                           and a.contains(sp, o, length) for a in r.counters), (
                                w, run + 1, r.heading(name), sp, o, length, on)
                        for a in v.decided_by:
                            assert a.rule_id in shown, (w, run + 1, r.heading(name), sp, o, length, on, a.rule_id)


def test_every_verdict_names_a_rule_and_its_provenance(tables):
    for w, run, _, _, L, t in tables[:20]:
        for r in t:
            v = may_i_keep(L, Fish(r.species, 45, r.origin if r.origin is not Origin.both else Origin.wild),
                           (7, 15), Creel(), name)
            for reason, a in zip(v.reasons, v.decided_by):
                assert a.source.words() in reason and "“" in reason, reason


# --------------------------------------------------------------------------- #
# What the reader sees: size on every row, plain names, and the province alone.
# --------------------------------------------------------------------------- #
def test_every_row_carries_a_size_statement(tables):
    """Silence is not an answer. 46 of 102 sections have no size gate at all; a reader there
    could not tell "any size" from "we did not say". Every row says one or the other, and
    every size the oracle can decide by is in that statement."""
    for w, run, _, _, L, t in tables:
        for r in t:
            for on in (None, (7, 15), (1, 15)):
                size = r.size(on, name)
                h = r.headline(on)
                if h is not None and h.is_zero:
                    continue
                assert size, (w, run + 1, r.heading(name), on)
                shown = {x["rule"] for x in size}
                o = r.origin if r.origin is not Origin.both else Origin.wild
                for a in r.live(on):
                    if a.period == "daily" and not a.scope.size.is_any:
                        assert a.rule_id in shown, (w, run + 1, r.heading(name), a.rule_id)
    _, _, _, _, _, t = _one(tables, "Kootenay Lake", 0)
    assert [x["says"] for x in _row(t, "RB").size(None, name)] == ["any size"]
    assert [x["says"] for x in _row(t, "CT").size(None, name)] == ["no more than 1 over 50 cm"]
    _, _, _, _, _, t = _one(tables, "Fraser River", 0)
    # Steelhead is not among the fish sharing the "1 over 50 cm": Region 2's "2 hatchery
    # steelhead over 50 cm allowed" took it out of that cap and gave it its own.
    assert {x["says"] for x in _row(t, "BT", Origin.hatchery).size(None, name)} == {
        "none under 60 cm", "none under 30 cm",
        "no more than 1 over 50 cm — shared with " + ", ".join(sorted(
            name(c).lower() for c in expand(frozenset({"TROUT_CHAR"})) - {"BT", "DV", "LT", "ST"}))}


def test_the_rest_of_a_group_is_named_as_the_rest(tables):
    """"Trout and char other than bull trout, cutthroat trout, dolly varden, rainbow trout,
    steelhead" names ten fish by excluding five. Every excluded fish has a row of its own, so
    the rest is "Other trout and char" — and no species code reaches the page."""
    for w, run, _, _, _, t in tables:
        for r in t:
            h = r.heading(name)
            assert "other than" not in h, (w, run + 1, h)
            for code in r.fish:
                assert code not in h.split(), (w, run + 1, h)
    _, _, _, _, _, t = _one(tables, "Kootenay Lake", 0)
    assert _row(t, "EB").heading(name) == "Any other trout or char"


def test_a_fish_only_the_province_names_is_set_apart_not_dropped(tables):
    """Region 4's printed table has no steelhead line — the Columbia above its dams has none —
    yet "all wild steelhead must be released" reaches Kootenay Lake because it is province-
    wide. The row stays (a protection is never dropped); it is flagged so the page can set it
    apart. On the Skeena, whose region names steelhead, the flag is off."""
    _, _, _, _, L, t = _one(tables, "Kootenay Lake", 0)
    assert _row(t, "ST", Origin.wild).province_only and _row(t, "ST", Origin.hatchery).province_only
    assert not _row(t, "RB").province_only and not _row(t, "EB").province_only
    assert _row(t, "ST", Origin.wild).headline().outcome == RELEASE
    v = may_i_keep(L, Fish("ST", 70, Origin.wild), (7, 15), Creel(), name)
    assert v.keep is False and v.decided_by[0].rule_id == "zp:steelhead::steelhead.r2"
    _, _, _, _, _, t = _one(tables, "Skeena River", 0)
    assert not _row(t, "ST", Origin.wild).province_only


# --------------------------------------------------------------------------- #
# A counter counts exactly the fish it binds — proved through the oracle, over real creels.
# --------------------------------------------------------------------------- #
def _used_counters(v):
    return {c.counter for c in v.checks if c.used > 0}


def test_a_shared_counter_is_symmetric_and_visible_on_both_rows(tables):
    """For every fish X and every fish Y that shares a counter with it: keeping Y and asking
    about X must consult the same counters as keeping X and asking about Y, and every counter
    that a kept Y charges must be visible on BOTH rows. Driven from the oracle over real
    creels, not from the rows — a check seeded from the rows cannot find a counter the rows
    omit. Kootenay Lake's bull trout, given its own 1 by the water, was still being charged
    against the region's "1 bull trout (Dolly Varden)" on the Dolly Varden's row."""
    for w, run, _, _, L, t in tables:
        row_of = {}
        for r in t:
            for sp in r.fish:
                for o in ((Origin.wild, Origin.hatchery) if r.origin is Origin.both else (r.origin,)):
                    row_of[(sp, o)] = r
        for r in t:
            x = r.species
            o = r.origin if r.origin is not Origin.both else Origin.wild
            partners = set()
            for a in r.counters:
                if a.period == "daily" and len(a.scope.effective()) > 1:
                    partners |= {y for y in a.scope.effective() if y not in r.fish and (y, o) in row_of}
            for y in sorted(partners)[:4]:
                for length in (45, 65):
                    xy = may_i_keep(L, Fish(x, length, o), (7, 15), Creel.of(Fish(y, length, o)), name)
                    yx = may_i_keep(L, Fish(y, length, o), (7, 15), Creel.of(Fish(x, length, o)), name)
                    shown_x = set(row_of[(x, o)].counters)
                    shown_y = set(row_of[(y, o)].counters)
                    for a in _used_counters(xy):
                        assert a in shown_x and a in shown_y, (w, run + 1, x, y, length, a.rule_id)
                    for a in _used_counters(yx):
                        assert a in shown_x and a in shown_y, (w, run + 1, y, x, length, a.rule_id)
                    # The same counters in both directions — where both fish reach the count.
                    # A fish sent back by a bound or a release is refused before any counter
                    # is consulted, and that answer has no counters to compare.
                    if xy.checks and yx.checks:
                        assert _used_counters(xy) == _used_counters(yx), (
                            w, run + 1, x, y, length,
                            sorted(a.rule_id for a in _used_counters(xy) ^ _used_counters(yx)))


def test_a_counter_binds_and_counts_the_same_fish(tables):
    """The symmetry, stated directly: for every shared counter in force, the fish it binds
    (and so the rows it sits on) are exactly the fish a kept one is charged against."""
    for w, run, _, _, L, t in tables:
        on_rows = {}
        for r in t:
            for a in r.counters:
                on_rows.setdefault(a, set()).update(r.fish)
        for a in L.allowances:
            if not L.in_force(a) or a.period != "daily" or len(a.scope.effective()) < 2:
                continue
            for o in (Origin.wild, Origin.hatchery):
                for y in sorted(a.scope.effective()):
                    charged = L.binds(a, y, o, 45, (7, 15))
                    if charged:
                        assert y in on_rows.get(a, set()), (w, run + 1, a.rule_id, y, o.value)


def test_kootenay_char_is_the_same_answer_in_both_directions():
    """Keep a bull trout, ask about a Dolly Varden; keep a Dolly Varden, ask about a bull
    trout. The water's "Bull trout daily quota = 1" took bull trout out of Region 4's shared
    "1 bull trout (Dolly Varden)", so neither kept fish spends the other's counter."""
    K = _ledger("Kootenay Lake", 0)
    bt_then_dv = may_i_keep(K, Fish("DV", 45), (7, 15), Creel.of(Fish("BT", 60)), name)
    dv_then_bt = may_i_keep(K, Fish("BT", 62), (7, 15), Creel.of(Fish("DV", 45)), name)
    assert bt_then_dv.keep is True and dv_then_bt.keep is True
    assert not [c for c in bt_then_dv.checks if c.used], bt_then_dv.reasons
    # ...and a second Dolly Varden is still refused by the region's cap, now "1 Dolly Varden".
    v = may_i_keep(K, Fish("DV", 45), (7, 15), Creel.of(Fish("DV", 45)), name)
    assert v.keep is False and v.decided_by[0].rule_id == "z4:trout_char_quota::trout_char_quota.r4"


# --------------------------------------------------------------------------- #
# The presented table: one entry per fish, and a band statement true of every member.
# --------------------------------------------------------------------------- #
def test_the_presented_table_names_each_fish_once_and_hoists_only_what_holds_for_all(tables):
    """A constraint stated on a band is visible for every member — and it must BE true of
    every member, or hoisting it would print a fact on a fish it does not bind. Every
    counter on a row is visible either on its line or on its band, none is lost."""
    from pipeline.regs.table.provenance import section
    for w, run, _, _, _, _ in tables[:30]:
        d = section(w, run)
        pr = d["present"]
        by_key = {r["key"]: r for r in d["rows"]}
        seen = collections.Counter(sp for e in pr["entries"] for sp in e["fish"])
        assert all(n == 1 for n in seen.values()), (w, run + 1, [k for k, n in seen.items() if n > 1])
        for e in pr["entries"]:
            assert "other than" not in e["heading"]
            for l in e["lines"]:
                r = by_key[l["row"]]
                shown = {c["rule"] for c in r["counters"]}
                if l["in_band"]:
                    b = pr["bands"][e["band"]]
                    for h in b["hoisted_all"]:
                        assert h in shown, (w, run + 1, e["heading"], h)
                    assert b["id"] in shown
                    if b["origin"]:
                        assert r["qualifier"] == b["origin"]
