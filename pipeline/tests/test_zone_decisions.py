"""The zone-verification decisions of 2026-09-24, each pinned against the model AND the corpus.

The provincial and regional rules bind every water, so an error in them is an error on thousands
of sections. Each test below names the decision it holds and the printed line it comes from. The
placement checks at the bottom read a bundle (`UI_EXPORT_BUNDLE`, else the shipped one) and are
marked slow.
"""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.regs.parsing import catalogue as C
from pipeline.regs.parsing.catalogue import CatalogueRule, label, label_parts


@pytest.fixture(scope="module")
def corpus():
    from pipeline.regs.parsing.io import read_all_entries
    return {eid: C.CatalogueEntry.model_validate(e) for eid, e in read_all_entries().items()}


def _rule(corpus, eid, rid) -> CatalogueRule:
    return next(r for r in corpus[eid].rules if r.rule_id == rid)


def _r(**kw):
    kw.setdefault("rule_id", "t.r1")
    kw.setdefault("verbatim", "verbatim sentence")
    return CatalogueRule(**kw)


def _lifts(corpus, eid, rid):
    """What the bundle resolves a rule's `exempts` to — the same function the bundle calls."""
    from pipeline.deliver.bundle.rules import _exempts
    zones, rules_of = {}, {}
    for ce in corpus.values():
        rules_of[ce.entry_id] = {r.rule_id: r for r in ce.rules}
        if ce.entry_id.startswith("z"):
            zones.setdefault(ce.entry_id.split(":", 1)[1], []).append(ce.entry_id)
    got = _exempts(eid, _rule(corpus, eid, rid), zones, rules_of)
    return json.loads(got) if got else []


# ============================================================================ decision 1a
def test_a_rule_that_only_lifts_never_competes(corpus):
    """z6 steelhead r2 "Exemptions include mainstem portions of the Skeena, Nass, Iskut, Stikine
    and Taku" is a lift with no number. Keyed `daily` at the water's rank it displaced the
    province's and the region's "release all wild steelhead" on the five mainstems."""
    lift = _rule(corpus, "z6:steelhead_stream_closure", "steelhead_stream_closure.r2")
    assert lift.lift_only and lift.dimension == "lift"
    for eid, rid in (("zp:steelhead", "steelhead.r2"), ("z6:trout_char_quota", "trout_char_quota.r9")):
        other = _rule(corpus, eid, rid)
        assert (lift.type, lift.dimension) != (other.type, other.dimension)
    # a lift with a number of its own is not lift-only: it states a quota AND lifts
    both = _r(type="retention_limit", species=["BASS"], unlimited=True,
              exempts=[{"target": "species_quotas.r1", "entry_id": "z4:species_quotas"}],
              extents=[{"op": "whole"}])
    assert not both.lift_only and both.dimension == "daily"


def test_every_lift_only_rule_is_keyed_lift(corpus):
    for ce in corpus.values():
        for r in ce.rules:
            if r.exempts and r.lift_only:
                assert r.dimension == "lift", (ce.entry_id, r.rule_id)
            if r.dimension == "lift":
                assert r.exempts, (ce.entry_id, r.rule_id)


# ============================================================================ decision 1b
def test_a_rules_conditions_are_in_its_key(corpus):
    """zp "release all wild steelhead" (daily, origin wild) must not share a key with Region 6's
    "Trout/char: 5" — or the region's number silences the release."""
    wild = _rule(corpus, "zp:steelhead", "steelhead.r2")
    region = _rule(corpus, "z6:trout_char_quota", "trout_char_quota.r1")
    assert wild.dimension == "daily@origin=wild" and region.dimension == "daily"
    assert _rule(corpus, "zp:steelhead", "steelhead.r4").dimension == "daily@origin=hatchery&record"
    streams = _rule(corpus, "z5:trout_char_quota", "trout_char_quota.r3")      # "2 from streams"
    assert streams.dimension == "daily@water=stream"
    setline = _rule(corpus, "zp:set_lining", "set_lining.r3")
    assert setline.dimension == "daily@while=set_lining"
    size = _rule(corpus, "z2:trout_char_quota", "trout_char_quota.r5b")      # "none under 60 cm"
    assert size.dimension == "daily/size"


def test_a_water_quota_still_replaces_the_zone_quota_for_the_same_thing(corpus):
    """The intended displacement: a water's plain quota shares the zone's plain key."""
    water = _rule(corpus, "r2:deer_lake_burnaby@2-8", "deer_lake_burnaby.r1")   # Trout/char 2
    zone = _rule(corpus, "z2:trout_char_quota", "trout_char_quota.r1")         # Trout/char 4
    assert (water.type, water.dimension) == (zone.type, zone.dimension) == ("retention_limit",
                                                                            "daily")


def test_the_zone_exception_a_quota_no_longer_displaces_is_an_explicit_lift(corpus):
    """"Kokanee: 10 (none from streams, except Peace River)": the Peace's kokanee quota used to
    displace the stream release by sharing its key; with the condition in the key it lifts it."""
    for eid, rid in (("r7:peace_river_downstream_of_boundary_signs_1_200m_downstream_o@7-31",
                      "peace_river.r2"),
                     ("r7:peace_river_from_hwy_29_bridge_to_the_site_c_dam@7-31", "peace_river.r1")):
        assert [(x["entry_id"], x["rule_id"]) for x in _lifts(corpus, eid, rid)] == [
            ("z7b:species_quotas", "species_quotas.r5")]


# ============================================================================ decision 3
def test_spear_fishing_is_a_method_rule_province_wide(corpus):
    sp = {r.rule_id: r for r in corpus["zp:spear_fishing"].rules}
    assert all(r.type is C.RuleType.method_rule for r in sp.values())
    assert label_parts(sp["spear_fishing.r1"])["what"] == "No spear fishing for game fish"
    assert label_parts(sp["spear_fishing.r2"])["what"] == "Spear fishing for burbot allowed"
    assert label_parts(sp["spear_fishing.r4"])["what"] == \
        "No spear fishing for salmon and protected species"
    # none of them shares a key with the province's unconditional allow, which Region 1/2/4's ban
    # does — and lifts, so the ban outranks the allow there (the tie the table found)
    allow = _rule(corpus, "zp:allowable_methods", "allowable_methods.r4")
    keys = {r.rule_id: r.dimension for r in sp.values()}
    assert keys["spear_fishing.r3"] == allow.dimension == "method:spear_fishing"
    assert all(v != allow.dimension for k, v in keys.items() if k != "spear_fishing.r3")
    assert [(x["entry_id"], x["rule_id"]) for x in _lifts(corpus, "zp:spear_fishing",
                                                           "spear_fishing.r3")] == [
        ("zp:allowable_methods", "allowable_methods.r4")]
    # burbot lifts the game-fish ban ONLY when fishing for burbot
    (lift,) = _lifts(corpus, "zp:spear_fishing", "spear_fishing.r2")
    assert lift["rule_id"] == "spear_fishing.r1" and lift["when_targeting"] == ["BB"]


# ============================================================================ decision 4
def test_bait_is_one_domain_and_a_ban_meets_the_invertebrate_allowance(corpus):
    inv = _rule(corpus, "zp:bait", "bait.r4")
    ban = _rule(corpus, "z1:bait_ban_streams", "bait_ban_streams.r1")
    assert inv.dimension == ban.dimension == "bait"
    # a PARTIAL ban keeps its bait: Zone B's fin fish ban never displaces the lake invertebrate ban
    fin = _rule(corpus, "z7b:bait", "bait.r2")
    lake = _rule(corpus, "zp:bait", "bait.r5")
    live = _rule(corpus, "zp:conduct", "conduct.r8")
    assert fin.dimension == "bait:fin_fish" and lake.dimension == "bait:invertebrate"
    assert live.dimension == "bait:live_fin_fish"
    # the roe cap is a possession limit, not a use of bait
    assert _rule(corpus, "zp:bait", "bait.r6").dimension == "bait_possession:roe"
    # the one printed exemption stays a lift, and a targeted rule stays its own subject
    assert _rule(corpus, "zp:bait", "bait.r3").dimension == "bait/WSG"


def test_an_exemption_from_a_bait_ban_does_not_reach_the_live_fish_ban():
    exempt = _r(type="bait_restriction", gear=[{"slot": "bait", "allow": ["any_bait"]}],
                extents=[{"op": "whole"}])
    live = _r(type="bait_restriction", gear=[{"slot": "bait", "ban": ["live_fin_fish"]}],
              extents=[{"op": "whole"}])
    assert exempt.dimension == "bait" and exempt.dimension != live.dimension


# ============================================================================ decision 5
BLANKET = {"z1:summer_stream_closure", "z3:spring_stream_closure", "z4:spring_stream_closure",
           "z5:spring_stream_closure", "z6:skeena_nass_winter_closure", "z6:iskut_fraser_closure",
           "z7a:spring_stream_closure", "z8:spring_stream_closure"}


@pytest.mark.parametrize("eid,rid,lifts", [
    ("r5:williams_lake_river@5-2", "williams_lake_river.r1",
     {("z5:spring_stream_closure", "spring_stream_closure.r1")}),
    ("r5:chimney_creek@5-2", "chimney_creek.r1",
     {("z5:spring_stream_closure", "spring_stream_closure.r1")}),
    ("r6:babine_river@6-8", "babine_river.r3",
     {("z6:skeena_nass_winter_closure", "skeena_nass_winter_closure.r1")}),
    ("r6:hevenor_mcqueen_creek@6-30", "hevenor_creek.r3",
     {("z6:skeena_nass_winter_closure", "skeena_nass_winter_closure.r1")}),
    ("r6:station_creek@6-9", "station_creek.r3",
     {("z6:skeena_nass_winter_closure", "skeena_nass_winter_closure.r1")}),
    ("r6:two_mile_creek@6-8", "two_mile_creek.r3",
     {("z6:skeena_nass_winter_closure", "skeena_nass_winter_closure.r1")}),
    ("r6:seeley_creek_outlet_of_seeley_lake@6-9", "seeley_creek.r2",
     {("z6:skeena_nass_winter_closure", "skeena_nass_winter_closure.r1")}),
    ("r3:crazy_creek@3-35", "crazy_creek.r2",
     {("z3:spring_stream_closure", "spring_stream_closure.r1")}),
])
def test_open_all_year_lifts_the_blanket_closure_and_never_a_species_one(corpus, eid, rid, lifts):
    got = {(x["entry_id"], x["rule_id"]) for x in _lifts(corpus, eid, rid)}
    assert got == lifts
    assert all(e in BLANKET for e, _ in got)
    assert _rule(corpus, eid, rid).lift_only


def test_west_road_mainstem_lifts_the_spring_closure_and_keeps_its_own(corpus):
    eid = "r5:west_road_blackwater_river@5-12+5-13"
    lifter = next(r for r in corpus[eid].rules if r.exempts)
    assert lifter.includes_tributaries is False            # "tributaries subject to spring closure"
    assert {(x["entry_id"], x["rule_id"]) for x in _lifts(corpus, eid, lifter.rule_id)} == {
        ("z5:spring_stream_closure", "spring_stream_closure.r1")}
    own = _rule(corpus, eid, "west_road_blackwater_river.r1")
    assert own.take == 0 and own.may_target is False and own.when.words() == "Nov 1-Jun 14"


# ============================================================================ decision 6
def test_region_4_reopened_species_lift_the_closure_and_the_notice(corpus):
    closed = {"BASS": "species_quotas.r1", "NP": "species_quotas.r7", "WP": "species_quotas.r8",
              "YP": "species_quotas.r11"}
    n = 0
    for ce in corpus.values():
        if not ce.entry_id.startswith("r4:"):
            continue
        for r in ce.rules:
            sp = set(r.species) & set(closed)
            reopens = (r.take or 0) > 0 or r.unlimited or r.may_target is True
            if r.type is not C.RuleType.retention_limit or not sp or not reopens or r.within:
                continue
            n += 1
            got = {(x["entry_id"], x["rule_id"]) for x in _lifts(corpus, ce.entry_id, r.rule_id)}
            want = {("z4:species_quotas", closed[s]) for s in sp} | {
                ("z4:invasive_species_notice", "invasive_species_notice.r1")}
            assert got == want, (ce.entry_id, r.rule_id)
    assert n == 52


# ============================================================================ decision 7
def test_zone_b_bull_trout_are_catch_and_release_and_the_liard_row_speaks(corpus):
    z = {r.rule_id: r for r in corpus["z7b:trout_char_quota"].rules}
    bt = z["trout_char_quota.r10"]
    assert bt.species == ["BT"] and bt.take == 0 and bt.may_target is True
    assert bt.extents == [{"op": "within", "area_id": "area:region:7b"}]
    assert "trout_char_quota.r5" not in z and "trout_char_quota.r9b" not in z
    liard = {r.rule_id: r for r in
             corpus["r7:liard_river_watershed_see_map_on_page_63@7-53"].rules}
    for rid in ("liard_river_watershed.r1", "liard_river_watershed.r2"):
        assert (liard[rid].type, liard[rid].dimension) == (bt.type, bt.dimension)


# ============================================================================ decision 8
def test_steelhead_over_50_are_not_in_the_trout_one_over_50(corpus):
    for eid, rid in (("z1:trout_quota", "trout_quota.r2"),
                     ("z2:trout_char_quota", "trout_char_quota.r2")):
        r = _rule(corpus, eid, rid)
        assert r.species_except == ["ST"] and "ST" in C.expand_species(list(r.species))


# ============================================================================ decision 9
def test_national_parks_close_by_default_and_provincial_licences_do_not_bind_there(corpus):
    np_ = {r.rule_id: r for r in corpus["zp:superior_closures"].rules}
    assert np_["superior_closures.r1"].take == 0 and np_["superior_closures.r1"].may_target is False
    note = np_["superior_closures.r1b"]
    assert note.type is C.RuleType.advisory and note.condition_of == "superior_closures.r1"
    assert not note.exempts
    for eid, lid in (("zp:basic_licence", "basic_licence"),
                     ("zp:basic_licence", "under_16_non_resident"),
                     ("zp:steelhead", "steelhead_targeting"), ("zp:salmon_stamp", "salmon_stamp"),
                     ("zp:licence_administration", "produce_licence"),
                     ("zp:licence_administration", "carry_paper_licence")):
        rec = next(x for x in corpus[eid].licensing if x.id == lid)
        assert rec.extents == [{"op": "within", "area_kind": "region",
                                "outside_area_kind": "national_parks"}], (eid, lid)


def test_outside_area_kind_subtracts_the_family(tmp_path):
    """The resolver's half: every `area:<kind>:*` member is taken out; an empty family fails."""
    from pipeline.atlas.reach.extent import resolve_extent

    class It:
        def __init__(self, ids):
            self.section_ids, self.boundaries = ids, ()

    class G:
        nodes: dict = {}

    reg = {"area:region:1": It(["a", "b", "c"]), "area:national_parks:x": It(["b"]),
           "area:national_parks:y": It(["c"])}
    ex = {"op": "within", "area_id": "area:region:1", "outside_area_kind": "national_parks"}
    got = resolve_extent(reg, G(), [], ex)
    assert got["sections"] == ["a"]
    why: list = []
    assert resolve_extent(reg, G(), [], dict(ex, outside_area_kind="nope"), why) is None
    assert why == [("outside_area_kind_matches_nothing", "nope")]


# ============================================================================ decision 10
def test_youth_disabled_waters_are_closed_to_all_but_authorized_anglers_and_companions(corpus):
    rows = [(ce.entry_id, r) for ce in corpus.values() for r in ce.rules
            if r.type is C.RuleType.angler_closure and "Youth/Disabled" in r.verbatim]
    assert len(rows) == 19
    for eid, r in rows:
        member = next(m for m in corpus[eid].rules if m.type is C.RuleType.program_membership)
        assert r.closed_to == C.Who(age=["16_plus"])
        assert r.closed_to_except == [C.Who(residency=["resident"], status=["disabled"]),
                                      C.Who(role=["companion"])]
        assert r.when == member.when and r.extents == member.extents, eid
    assert label_parts(rows[0][1])["what"] == (
        "Angling closed to anglers 16 and over, except disabled B.C. residents and companions of "
        "an authorized angler")
    assert "zp:youth_disabled_waters" in corpus


def test_an_exception_must_meet_the_closure():
    with pytest.raises(ValueError, match="subtracts nothing"):
        _r(type="angler_closure", closed_to={"age": ["16_plus"]},
           closed_to_except=[{"age": ["under_16"]}], extents=[{"op": "whole"}])
    with pytest.raises(ValueError, match="carves anglers out of an angler_closure"):
        _r(type="retention_limit", species=["RB"], take=1, closed_to_except=[{"age": ["16_plus"]}],
           extents=[{"op": "whole"}])


def test_the_four_record_duties_sit_beside_their_stamps(corpus):
    want = {("zp:salmon_stamp", "salmon_stamp.r1"): (["CH"], None),
            ("zp:kootenay_rainbow_stamp", "kootenay_rainbow_stamp.r1"): (["RB"], 50),
            ("zp:shuswap_char_stamp", "shuswap_char_stamp.r1"): (["LT", "BT"], 60),
            ("zp:shuswap_rainbow_stamp", "shuswap_rainbow_stamp.r1"): (["RB"], 50)}
    for (eid, rid), (sp, cm) in want.items():
        r = _rule(corpus, eid, rid)
        assert r.record_retention and r.species == sp and r.take is None
        assert [b.min_cm for b in (r.lengths or [])] == ([cm] if cm else [])
        assert "record" in r.dimension
        stamp = corpus[eid].licensing[0]
        if eid != "zp:salmon_stamp":
            assert r.extents == stamp.extents
    assert label(_rule(corpus, "zp:kootenay_rainbow_stamp", "kootenay_rainbow_stamp.r1")) == (
        "Rainbow trout over 50 cm — record your retention on your licence immediately")


def test_the_transport_duties_are_registered_acts(corpus):
    acts = [a for r in corpus["zp:transporting_catch"].rules for a in r.conduct]
    assert len(acts) == 6 and all(a in C.CONDUCT_ACTS for a in acts)
    assert corpus["zp:transporting_catch"].source_pages == [10]


# ============================================================================ decision 11
def test_cutthroat_is_both_cutthroats(corpus):
    assert C.expand_species(["CT"]) == ["WCT", "CCT"]
    r4 = _rule(corpus, "z4:trout_char_quota", "trout_char_quota.r2")
    assert "WCT" in C.expand_species(list(r4.species))
    # one fish in the book's words: a cutthroat quota is not "all species combined"
    assert label(_rule(corpus, "r1:cowichan_lake_including_bear_lake@1-4", "cowichan_lake.r1")) \
        == "Cutthroat trout — 2 per day (none over 50 cm)"


# ============================================================================ decision 12
def test_whitefish_is_the_closed_lists_whitefish():
    assert C.SPECIES_GROUPS["WHITEFISH"] == ("LW", "MW")
    agf = set(C.expand_species(["ALL_GAME_FISH"]))
    assert "PW" not in agf and "RW" not in agf


# ============================================================================ decision 13
def test_set_lining_is_banned_outside_the_lakes_that_lift_it(corpus):
    e = corpus["zp:set_lining"]
    assert e.extents == [{"op": "within", "area_kind": "region"}]
    r1b = _rule(corpus, "zp:set_lining", "set_lining.r1b")
    assert r1b.extents == [{"op": "within", "area_kind": "region"}]


def test_the_under_30_size_rules_have_no_rule_level_take(corpus):
    for eid, rid in (("z6:trout_char_quota", "trout_char_quota.r6"),
                     ("z7a:trout_char_quota", "trout_char_quota.r9"),
                     ("z7b:trout_char_quota", "trout_char_quota.r7"),
                     ("z7b:arctic_grayling", "arctic_grayling.r2"),
                     ("z1:hg_quota", "hg_quota.r5"), ("z2:trout_char_quota", "trout_char_quota.r8")):
        r = _rule(corpus, eid, rid)
        assert r.take is None and r.lengths and r.dimension.startswith("daily/size"), (eid, rid)


def test_stop_fishing_after_the_steelhead_quota_is_on_regions_2_and_6_only(corpus):
    assert not any(r.type is C.RuleType.stop_fishing_after_quota
                   for r in corpus["zp:steelhead"].rules)
    for reg in ("2", "6"):
        (r,) = corpus[f"z{reg}:hatchery_steelhead_stop"].rules
        assert r.extents == [{"op": "within", "area_id": f"area:region:{reg}"}]
        assert label(r).startswith("Stop fishing the water for the rest of the day")


def test_gear_and_bait_hold_in_streams_not_from_them(corpus):
    assert label(_rule(corpus, "z3:single_barbless_hook", "single_barbless_hook.r1")) == \
        "Single barbless hook, in streams"
    assert "roe" in label(_rule(corpus, "zp:bait", "bait.r6"))
    assert "not downrigger weights" in label(_rule(corpus, "zp:terminal_tackle",
                                                    "terminal_tackle.r4"))
    assert "within 1 m of the hook" in label(_rule(corpus, "zp:terminal_tackle",
                                                    "terminal_tackle.r5"))
    assert "excluding" not in label(_rule(corpus, "z2:species_quotas", "species_quotas.r1"))


# ============================================================================ placement (bundle)
BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or "data/generated/bundle/bundle.sqlite")


@pytest.fixture(scope="module")
def db():
    if not BUNDLE.exists():
        pytest.skip("no bundle")
    con = sqlite3.connect(str(BUNDLE))
    have = con.execute("select count(*) from rule where entry_id='z7b:trout_char_quota' "
                       "and rule_id='trout_char_quota.r10'").fetchone()[0]
    if not have:
        pytest.skip(f"{BUNDLE} predates the zone decisions — point UI_EXPORT_BUNDLE at a side build")
    return con


def _sections(db, eid, rid) -> set:
    return {s for (s,) in db.execute(
        "select sr.sid from ruleset r join section_ruleset sr on sr.set_id = r.set_id "
        "where r.entry_id = ? and r.rule_id = ?", (eid, rid))}


@pytest.mark.slow
def test_set_lining_ban_binds_the_province(db):
    ban = _sections(db, "zp:set_lining", "set_lining.r1b")
    lift = _sections(db, "zp:set_lining", "set_lining.r1")
    assert lift < ban and len(ban) > 1_900_000


@pytest.mark.slow
def test_zone_b_bull_trout_placement(db):
    zone = _sections(db, "z7b:trout_char_quota", "trout_char_quota.r10")
    liard = _sections(db, "r7:liard_river_watershed_see_map_on_page_63@7-53",
                      "liard_river_watershed.r2")
    assert len(zone) > 300_000 and liard < zone


@pytest.mark.slow
def test_bowron_park_waters_are_bound_and_bowron_lake_is_not(db):
    got = _sections(db, "r5:bowron_lake_park_waters_other_than_bowron_lake@5-16",
                    "bowron_lake_park_waters.r3")
    lake = {s for (s,) in db.execute(
        "select s.sid from item i join item_section s on s.ord = i.ord "
        "where i.item_id = 'wbk:329058703'")}
    assert got and not (got & lake)


@pytest.mark.slow
def test_provincial_licences_do_not_bind_inside_national_parks(db):
    """Placed `province` (no rows), minus the national parks, which the bundle lists once — and
    the parks it lists are exactly where the park closure binds."""
    parks = _sections(db, "zp:superior_closures", "superior_closures.r1")
    placement, record = db.execute("select placement, record from requirement where "
                                   "entry_id='zp:basic_licence' and req_id='basic_licence'").fetchone()
    assert placement == "province"
    assert json.loads(record)["extents"][0]["outside_area_kind"] == "national_parks"
    listed = {s for (s,) in db.execute(
        "select sid from province_except where area_kind='national_parks'")}
    assert parks and listed == parks


def test_a_province_wide_record_minus_a_family_is_still_province_wide():
    """Placed as sections it would be a row per section of B.C. minus seven parks."""
    from pipeline.atlas.reach.licensing import is_province_wide
    assert is_province_wide([{"op": "within", "area_kind": "region",
                              "outside_area_kind": "national_parks"}])
    assert not is_province_wide([{"op": "within", "area_kind": "region",
                                  "outside_area": "area:national_parks:x"}])


@pytest.mark.slow
def test_a_water_quota_never_sits_under_a_smaller_conditioned_zone_count(db, corpus):
    """Decision 1 keyed a rule's conditions (`water`, `origin`), so a water's plain quota no
    longer displaces "2 from streams". Where the water's own number for the same fish is LARGER
    — Duncan River's "rainbow trout daily quota = 5" on a stream under Region 4's "2 from
    streams" — the two cannot both hold: the water row must lift the zone count, or the angler
    gets two limits where the book prints one (review 2026-09-24; 10 such rows were missed).
    Size gates and releases are not checked here; only counts."""
    rules = {(eid, r.rule_id): r for eid, ce in corpus.items() for r in ce.rules}
    sets: dict = {}
    for s, e, r in db.execute("select set_id, entry_id, rule_id from ruleset"):
        sets.setdefault(s, []).append((e, r))
    bad = set()
    for rows in sets.values():
        zs = [k for k in rows if k[0].startswith("z") and k in rules]
        ws = [k for k in rows if k[0].startswith("r") and k in rules]
        for z in zs:
            rz = rules[z]
            if (rz.type is not C.RuleType.retention_limit or not rz.take or rz.lengths
                    or (rz.water is None and rz.origin is None)):
                continue
            fish = set(C.expand_species(list(rz.species)))
            for w in ws:
                rw = rules[w]
                if (rw.type is not C.RuleType.retention_limit or not rw.take or rw.lift_only
                        or rw.within or rw.clock != rz.clock or rw.dimension == rz.dimension
                        or not fish & set(C.expand_species(list(rw.species)))
                        or (rz.origin and rw.origin and rz.origin != rw.origin)):
                    continue
                if rw.take > rz.take and not any(x.entry_id == z[0] and x.target == z[1]
                                                 for x in rw.exempts):
                    bad.add((w, z))
    assert not bad, sorted(bad)
