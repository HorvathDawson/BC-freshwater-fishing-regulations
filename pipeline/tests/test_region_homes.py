"""A WATER TAKES THE ZONE RULES OF THE REGION IT LIES IN (user ruling 2026-09-25).

A lake is never cut, so one drawn across a region line was a member of both `area:region:*` items
and bound BOTH regions' standing tables — Mara Lake carried Region 3's and Region 8's "Trout/char:
5" side by side. `registry.regions` gives every straddling section its HOME region (largest share of
its outline's area, or of its line's length), and `extent.area_sections` resolves `area:region:N`
through it. A regional ROW is still limited by the polygons it touches (Region 8's own Mara Lake
row binds the lake).

The bundle is `UI_EXPORT_BUNDLE`, else the shipped one; the atlas `ATLAS_BUILD`, else the promoted.
Mutation: `in_region` returning its input unchanged fails the extent and bundle tests below.
"""
from __future__ import annotations

import json
import os
import sqlite3
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline.atlas.reach import extent
from pipeline.atlas.registry import regions
from pipeline.deliver.bundle import read as R

BUNDLE = str(Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE))
MARA = "wbk:329518146"
SHUSWAP = "r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26"


def _reg(**members):
    return {f"area:region:{r}": SimpleNamespace(section_ids=tuple(s)) for r, s in members.items()}


# --------------------------------------------------------------------------- pure
def test_the_home_is_the_largest_share_and_a_tie_goes_to_the_smaller_id():
    assert regions.home_of({"3": 0.61, "8": 0.39}) == "3"
    assert regions.home_of({"5": 0.2223, "6": 0.2925, "7a": 0.4852}) == "7a"
    assert regions.home_of({"8": 0.5, "3": 0.5}) == "3"


def test_straddlers_are_the_sections_in_more_than_one_region():
    reg = _reg(**{"3": ["lake:1", "a:0"], "8": ["lake:1", "b:0"], "7a": ["c:0"]})
    assert regions.straddlers(reg) == {"lake:1": ("3", "8")}


def test_a_region_area_resolves_a_stream_piece_to_its_home_and_a_lake_to_both():
    """A stream piece is its home region's; a LAKE straddling the line is in both (user ruling
    2026-09-25: Ahbau Lake, Mara Lake — both zone bases, the most strict applies). MUTATION:
    `in_region` holding lakes to their home fails the lake half; returning its input unchanged
    fails the stream half."""
    reg = _reg(**{"3": ["lake:1", "a:0", "s:9"], "8": ["lake:1", "b:0", "s:9"]})
    g = SimpleNamespace(nodes={})
    assert extent.area_sections(reg, g, "area:region:8") == {"lake:1", "b:0", "s:9"}  # not attached
    regions.attach(g, {"lake:1": "3", "s:9": "3"})
    assert extent.area_sections(reg, g, "area:region:3") == {"lake:1", "a:0", "s:9"}
    assert extent.area_sections(reg, g, "area:region:8") == {"lake:1", "b:0"}


def test_only_region_areas_are_held_to_a_home():
    reg = {"area:park:x": SimpleNamespace(section_ids=("lake:1",)),
           **_reg(**{"3": ["lake:1"], "8": ["lake:1"]})}
    g = SimpleNamespace(nodes={})
    regions.attach(g, {"lake:1": "3"})
    assert extent.area_sections(reg, g, "area:park:x") == {"lake:1"}


def test_a_regional_row_is_still_limited_by_the_polygons_it_touches():
    """Region 8's Mara Lake row ("No powered boats south of the CPR bridge") binds the lake: the
    row limit reads the registry's touching membership, never the home map."""
    from pipeline.atlas.reach.outside import region_sections
    reg = _reg(**{"3": ["lake:1"], "8": ["lake:1"]})
    assert "lake:1" in region_sections(("8",), reg)


# --------------------------------------------------------------------------- the bundle
@pytest.fixture(scope="module")
def db():
    if not Path(BUNDLE).exists():
        pytest.skip("no bundle")
    con = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    if not con.execute("select 1 from sqlite_master where name = 'steelhead_water'").fetchone():
        pytest.skip(f"{BUNDLE} predates the region homes — point UI_EXPORT_BUNDLE at a side build")
    yield con
    con.close()


def _bound(db, item: str) -> set[tuple[str, str]]:
    return {(e, r) for e, r in db.execute(
        "select distinct r.entry_id, r.rule_id from item i join item_section s on s.ord = i.ord "
        "join section_ruleset sr on sr.sid = s.sid join ruleset r on r.set_id = sr.set_id "
        "where i.item_id = ?", (item,))}


AHBAU = "wbk:329097613"


def _lake_sid(db, item: str) -> int:
    (sid,) = db.execute("select s.sid from item i join item_section s on s.ord = i.ord "
                        "where i.item_id = ? limit 1", (item,)).fetchone()
    return sid


def test_mara_lake_takes_both_regions_bases_and_keeps_region_8s_row(db):
    """Mara Lake is 61 % Region 3 by area and printed under Shuswap Lake (Region 3, p.31); the book
    also prints it in Region 8 (p.70): "See Shuswap Lake in Region 3 / No powered boats south of the
    CPR bridge". A lake straddling a region line takes BOTH bases (user ruling 2026-09-25), the
    most strict applying — Region 8's "Bass: 0 quota, CLOSED TO FISHING" among them."""
    got = _bound(db, MARA)
    assert ("z3:trout_char_quota", "trout_char_quota.r1") in got
    assert ("z8:trout_char_quota", "trout_char_quota.r1") in got
    assert ("r8:mara_lake@8-26", "mara_lake.r2") in got
    assert any(e == SHUSWAP for e, _ in got)
    # no rule of Region 8 takes it out by hand
    for (ext,) in db.execute("select conditions from rule where entry_id like 'z8:%'"):
        assert MARA not in json.dumps(json.loads(ext or "{}").get("extents") or [])


def test_ahbau_lake_takes_region_5_and_zone_7a_bases(db):
    got = _bound(db, AHBAU)
    assert ("z5:trout_char_quota", "trout_char_quota.r1") in got
    assert ("z7a:trout_char_quota", "trout_char_quota.r1") in got


def test_only_lakes_carry_two_regions_standing_tables(db):
    """Before R2: 41 rule sets (214 sections) carried two regions' region-wide rules, lakes and
    stream pieces alike. R2 gave every one a single home; the second half of the ruling puts the
    LAKES back in both regions (most strict wins) — a stream piece still has one home."""
    region_wide = {}
    for e, r, c in db.execute("select entry_id, rule_id, conditions from rule "
                              "where entry_id like 'z%' and entry_id not like 'zp:%'"):
        ex = json.loads(c or "{}").get("extents") or []
        if ex and all(x.get("op") == "within" and str(x.get("area_id", "")).startswith(
                "area:region:") for x in ex):
            region_wide[(e, r)] = e.split(":")[0][1:]
    per_set = defaultdict(set)
    for s, e, r in db.execute("select set_id, entry_id, rule_id from ruleset"):
        if (e, r) in region_wide:
            per_set[s].add(region_wide[(e, r)])
    two = {s for s, z in per_set.items() if len(z) > 1}
    handles = _handles()
    bad = [handles[sid] for sid, s in db.execute("select sid, set_id from section_ruleset")
           if s in two and not handles[sid].startswith("lake:")]
    assert not bad, bad[:5]
    assert two, "no straddling lake carries both bases"


def _handles() -> dict[int, str]:
    from pipeline.common.curated import GENERATED
    b = Path(os.environ.get("ATLAS_BUILD") or GENERATED.build())
    p = b / "section_handles.txt"
    if not p.exists():
        pytest.skip("no section handles")
    with p.open() as f:
        return {i: line.rstrip("\n") for i, line in enumerate(f, 1)}


def test_ahbau_and_mara_the_most_strict_applies(db):
    """Mara: Region 8 closes bass ("Bass: 0 quota, CLOSED TO FISHING (see tables for exceptions)",
    p.68); Region 3 closes it too — the closures speak and nothing keeps a bass. Ahbau: both
    regions' "Trout/char: 5" (equal) speak for a rainbow."""
    mara = _lake_sid(db, MARA)
    got = {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(mara, (7, 1), "SMB", BUNDLE)
           if x["state"] == "speaks" and x["type"] == "retention_limit"}
    assert "z8:species_quotas::species_quotas.r1" in got
    assert all(x.get("take") == 0 for x in R.effective_rules(mara, (7, 1), "SMB", BUNDLE)
               if x["state"] == "speaks" and x["type"] == "retention_limit"
               and x["entry"].startswith("z"))
    ahbau = _lake_sid(db, AHBAU)
    rb = {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(ahbau, (7, 1), "RB", BUNDLE)
          if x["state"] == "speaks"}
    assert {"z5:trout_char_quota::trout_char_quota.r1",
            "z7a:trout_char_quota::trout_char_quota.r1"} <= rb


# --------------------------------------------------------------------------- a water's own row
def test_shared_waters_are_printed_by_rows_of_two_regions():
    """The Fraser's rows (2, 3, 5, 7) are per-region; West Road's tributaries rows (6, 7) do not
    make the mainstem's Region 5 row per-region — they are about tributaries only."""
    from pipeline.atlas.reach.outside import shared_waters
    ents = [{"entry_id": "r2:fraser@2-4", "matched": ["F"], "rules": [{"rule_id": "a"}]},
            {"entry_id": "r3:fraser@3-14", "matched": ["F"], "rules": [{"rule_id": "a"}]},
            {"entry_id": "r5:west_road@5-13", "matched": ["W"], "rules": [{"rule_id": "a"}]},
            {"entry_id": "r6:west_road_tribs@6-1", "matched": ["W"],
             "rules": [{"rule_id": "a", "tributaries_only": True}]},
            {"entry_id": "r7:west_road_tribs@7-10", "matched": ["W"],
             "rules": [{"rule_id": "a", "tributaries_only": True}]},
            {"entry_id": "r7:pointer@7-1", "matched": ["W"], "rules": []}]
    assert shared_waters(ents) == frozenset({"F"})


def test_a_whole_water_row_reaches_every_region_its_water_lies_in():
    """West Road's Region 5 row binds its Zone 7A pieces; the Fraser's Region 5 row stays in
    Region 5. MUTATION: `region_limit` ignoring `shared` holds the West Road row to Region 5.
    A water row is not held to any region at all (`WATER_ROW_WALKS_CROSS_REGIONS`, 2026-09-29) —
    so its walk crosses the line too; with the switch off it is held to its water's regions."""
    import pipeline.atlas.reach.outside as O
    from pipeline.atlas.reach.outside import region_limit
    reg = {**_reg(**{"5": ["w:1", "f:1"], "7a": ["w:2", "f:2"]}),
           "W": SimpleNamespace(section_ids=("w:1", "w:2")),
           "F": SimpleNamespace(section_ids=("f:1", "f:2"))}
    west = {"entry_id": "r5:west_road@5-13", "matched": ["W"]}
    fraser = {"entry_id": "r5:fraser@5-2", "matched": ["F"]}
    assert region_limit(west, reg, frozenset({"F"})) is None
    assert region_limit(fraser, reg, frozenset({"F"})) == {"w:1", "f:1"}
    assert region_limit(west, reg) == {"w:1", "f:1"}           # not computed: never widened
    O.WATER_ROW_WALKS_CROSS_REGIONS = False
    try:
        assert region_limit(west, reg, frozenset({"F"})) == {"w:1", "w:2", "f:1", "f:2"}
    finally:
        O.WATER_ROW_WALKS_CROSS_REGIONS = True


def _blanket(rid, frm, to, water="stream", species=("ALL_GAME_FISH",)):
    from pipeline.regs.parsing.catalogue import CatalogueRule
    return CatalogueRule.model_validate({
        "rule_id": rid, "type": "retention_limit", "verbatim": "No fishing in any stream",
        "species": list(species), "take": 0, "may_target": False, "water": water,
        "when": {"dates": [{"from_month": frm[0], "from_day": frm[1], "to_month": to[0],
                            "to_day": to[1]}]},
        "extents": [{"op": "within", "area_id": "area:region:9", "feature_types": [water]}]})


def test_a_printed_lift_of_a_spring_closure_reaches_the_same_closure_in_the_waters_regions():
    """"Exempt from spring closure" printed in Region 5 lifts Zone 7A's spring closure too, where
    the row's water lies in 7A — never a summer closure, never a species closure. MUTATION:
    `_equivalent_closures` returning [] leaves Zone 7A's closure on West Road's 7A pieces."""
    from pipeline.deliver.bundle import rules as rules_mod
    z5 = _blanket("spring_stream_closure.r1", (4, 1), (6, 30))
    z7 = _blanket("spring_stream_closure.r1", (4, 1), (6, 30))
    z1 = _blanket("summer_stream_closure.r1", (7, 15), (8, 31))
    st = _blanket("steelhead.r1", (5, 15), (6, 15), species=("ST",))
    blankets = rules_mod.blanket_closures([
        type("E", (), {"entry_id": "z5:spring_stream_closure", "rules": [z5]}),
        type("E", (), {"entry_id": "z7a:spring_stream_closure", "rules": [z7]}),
        type("E", (), {"entry_id": "z1:summer_stream_closure", "rules": [z1]}),
        type("E", (), {"entry_id": "z6:steelhead", "rules": [st]})])
    assert "6" not in blankets                                  # a species closure is not blanket
    got = rules_mod._equivalent_closures("z5:spring_stream_closure", z5, {"5", "7a", "1"},
                                         blankets)
    assert [e for e, _ in got] == ["z7a:spring_stream_closure"]
    assert rules_mod._equivalent_closures("z6:steelhead", st, {"7a"}, blankets) == []


def _kinded(rid, frm, to, kind, verbatim="No fishing in any stream"):
    from pipeline.regs.parsing.catalogue import CatalogueRule
    return CatalogueRule.model_validate({
        **_blanket(rid, frm, to).model_dump(by_alias=True, exclude_none=True),
        "verbatim": verbatim, "closure_kind": kind})


def _kinded_blankets():
    from pipeline.deliver.bundle import rules as rules_mod
    z5 = _kinded("spring_stream_closure.r1", (4, 1), (6, 30), "spring",
                 "Spring closure: No fishing in any stream in Fraser River Watershed of Region 5")
    winter = _kinded("skeena_nass_winter_closure.r1", (1, 1), (6, 15), "winter")
    spring6 = _kinded("iskut_fraser_closure.r2", (4, 1), (6, 30), "spring")
    z7 = _kinded("spring_stream_closure.r1", (4, 1), (6, 30), "spring",
                 "No fishing (spring closure): in any stream of Zone A, Apr 1 – June 30.")
    z1 = _kinded("summer_stream_closure.r1", (7, 15), (8, 31), "summer",
                 "Summer closure: No Fishing in any stream in Management Units 1-1 to 1-6")
    blankets = rules_mod.blanket_closures([
        type("E", (), {"entry_id": "z5:spring_stream_closure", "rules": [z5]}),
        type("E", (), {"entry_id": "z6:skeena_nass_winter_closure", "rules": [winter]}),
        type("E", (), {"entry_id": "z6:iskut_fraser_closure", "rules": [spring6]}),
        type("E", (), {"entry_id": "z7a:spring_stream_closure", "rules": [z7]}),
        type("E", (), {"entry_id": "z1:summer_stream_closure", "rules": [z1]})])
    return z5, blankets


def test_a_spring_exemption_lifts_the_other_regions_spring_closure_never_its_winter_one():
    """USER RULING 2026-09-26: an `equivalent` lift matches the KIND of closure the row names.
    The Nechako's "Exempt from spring closure" reaches Region 6's spring closure (Apr 1-June 30)
    and never its Skeena/Nass WINTER closure (Jan 1-June 15), though the dates overlap; nor a
    summer one. MUTATION: comparing seasons instead of `closure_kind` in `_equivalent_closures`
    (the old rule) adds the winter closure and fails the first assert."""
    from pipeline.deliver.bundle import rules as rules_mod
    z5, blankets = _kinded_blankets()
    got = rules_mod._equivalent_closures("z5:spring_stream_closure", z5, {"1", "5", "6", "7a"},
                                         blankets, "Exempt from spring closure.")
    assert [(e, z.rule_id) for e, z in got] == [
        ("z6:iskut_fraser_closure", "iskut_fraser_closure.r2"),
        ("z7a:spring_stream_closure", "spring_stream_closure.r1")]
    # The row's words name no kind ("Mainstem open all year"): the closure it names says which.
    got = rules_mod._equivalent_closures("z5:spring_stream_closure", z5, {"6"}, blankets,
                                         "Mainstem open all year")
    assert [e for e, _ in got] == ["z6:iskut_fraser_closure"]


def test_a_lift_of_a_closure_of_unknown_kind_is_refused_or_falls_back_never_guessed():
    """A lift of known kind meeting a closure of NO known kind stops the build — the dates would
    have to guess. A contradiction (the row says spring, names a winter closure) and a row naming
    two kinds stop it too. Only where NOTHING names a kind does the season overlap decide.
    MUTATION: skipping an unkinded closure instead of refusing fails the first `raises`."""
    from pipeline.deliver.bundle import rules as rules_mod
    z5, blankets = _kinded_blankets()
    unkinded = _blanket("spring_stream_closure.r1", (4, 1), (6, 14))
    blankets4 = {**blankets, **rules_mod.blanket_closures([
        type("E", (), {"entry_id": "z4:spring_stream_closure", "rules": [unkinded]})])}
    with pytest.raises(SystemExit, match="no known kind"):
        rules_mod._equivalent_closures("z5:spring_stream_closure", z5, {"4"}, blankets4,
                                       "Exempt from spring closure")
    winter = blankets["6"][0][1]
    with pytest.raises(SystemExit, match="names the winter one"):
        rules_mod._equivalent_closures("z6:skeena_nass_winter_closure", winter, {"7a"},
                                       blankets, "Exempt from spring closure")
    with pytest.raises(SystemExit, match="spring/winter"):
        rules_mod._equivalent_closures("z5:spring_stream_closure", z5, {"7a"}, blankets,
                                       "not closed under the winter/spring closure regulation")
    # No kind anywhere: the old rule, the same water over an overlapping season.
    got = rules_mod._equivalent_closures("z4:spring_stream_closure", unkinded, {"5", "1"},
                                         blankets, "EXEMPT from the Apr 1-June 14 closure")
    assert [e for e, _ in got] == ["z5:spring_stream_closure"]


def test_closure_kind_names_a_closure_and_agrees_with_its_own_words():
    """MUTATION: dropping the `closure_kind` block from `CatalogueRule._check` fails both."""
    from pipeline.regs.parsing.catalogue import CatalogueRule
    with pytest.raises(ValueError, match="the sentence names the summer closure"):
        _kinded("summer_stream_closure.r1", (7, 15), (8, 31), "spring",
                "Summer closure: No Fishing in any stream")
    raw = _blanket("x.r1", (4, 1), (6, 30)).model_dump(by_alias=True, exclude_none=True)
    with pytest.raises(ValueError, match="closure_kind names a closure"):
        CatalogueRule.model_validate({**raw, "take": 2, "may_target": True,
                                      "closure_kind": "spring"})


def test_every_blanket_closure_in_the_corpus_says_its_kind():
    """Each region's blanket stream closure carries `closure_kind`, so no lift reaching it has to
    guess: z1 summer; z3, z4, z5, z7a, z8 and z6's Iskut/Fraser spring; z6's Skeena/Nass winter."""
    from pipeline.deliver.bundle import rules as rules_mod
    from pipeline.regs.parsing.catalogue import CatalogueEntry
    from pipeline.common.curated import CURATED
    docs = [CatalogueEntry.model_validate(e)
            for p in sorted(Path(CURATED.regulations.entries.catalogue).glob("region-*.json"))
            for e in json.loads(p.read_text(encoding="utf-8"))["entries"]]
    got = {(eid, r.rule_id): (r.closure_kind.value if r.closure_kind else None)
           for reg in rules_mod.blanket_closures(docs).values() for eid, r in reg}
    assert None not in got.values(), [k for k, v in got.items() if v is None]
    assert got[("z6:skeena_nass_winter_closure", "skeena_nass_winter_closure.r1")] == "winter"
    assert got[("z6:iskut_fraser_closure", "iskut_fraser_closure.r2")] == "spring"
    assert got[("z1:summer_stream_closure", "summer_stream_closure.r1")] == "summer"
    assert len(got) == 11


def test_the_nechako_and_west_road_lift_region_6s_spring_closure_not_its_winter_one(db):
    """In the bundle: the rows' printed spring exemptions name Region 6's Fraser-watershed spring
    closure as `equivalent`, and no Skeena/Nass winter closure — which by dates they did."""
    for eid, rid in (("r7:nechako_river@7-12", "nechako_river.r1"),
                     ("r5:west_road_blackwater_river@5-12+5-13", "west_road_blackwater_river.r6")):
        (ex,) = db.execute("select exempts from rule where entry_id = ? and rule_id = ?",
                           (eid, rid)).fetchone()
        lifted = {(x["entry_id"], x["rule_id"]) for x in json.loads(ex) if "equivalent" in x}
        assert ("z6:iskut_fraser_closure", "iskut_fraser_closure.r2") in lifted
        assert not any(e == "z6:skeena_nass_winter_closure" for e, _ in lifted), eid


def test_west_road_7a_mainstem_piece_takes_zone_7as_table_and_its_own_row(db):
    """356364550:15264 is 54 % Zone 7A by length: it takes Zone 7A's spring closure, and the
    Region 5 row (p.47) binds it, lifting that closure ("the regional spring closure does not add
    to its own mainstem closure"). On June 20 its own row's closure (Nov 1-June 14) is over and
    no spring closure speaks."""
    handles = _handles()
    sid = next(i for i, h in handles.items() if h == "356364550:15264")
    wr = "r5:west_road_blackwater_river@5-12+5-13"
    rs = {(e, r) for e, r in db.execute(
        "select r.entry_id, r.rule_id from section_ruleset s join ruleset r on r.set_id = s.set_id "
        "where s.sid = ?", (sid,))}
    assert (wr, "west_road_blackwater_river.r6") in rs
    assert ("z7a:spring_stream_closure", "spring_stream_closure.r1") in rs
    assert not any(e.startswith("z5:") for e, _ in rs)
    got = {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(sid, (6, 20), "RB", BUNDLE)
           if x["state"] == "speaks"}
    assert "z7a:spring_stream_closure::spring_stream_closure.r1" not in got


@pytest.mark.parametrize("item,entry,regs", [
    ("gnis:26104", "r5:west_road_blackwater_river@5-12+5-13", ("6", "7a")),
    ("gnis:3273", "r5:klinaklini_river@5-6", ("1",)),
])
def test_a_rows_water_is_bound_in_every_region_it_lies_in(db, item, entry, regs):
    """The row binds its water's pieces in the other regions too (West Road in 6 and 7A; the
    Klinaklini, printed only in Region 5 (p.45), in Region 1 — its `within_area` clip removed)."""
    handles = _handles()
    from pipeline.atlas.registry import load_registry
    from pipeline.common.curated import GENERATED
    reg = load_registry(str(Path(os.environ.get("ATLAS_BUILD") or GENERATED.build())
                            / "registry.json"))
    sid_of = {h: i for i, h in handles.items()}
    for r in regs:
        others = set(reg[item].section_ids) & set(reg[f"area:region:{r}"].section_ids)
        assert others, (item, r)
        for h in sorted(others)[:3]:
            got = {e for (e,) in db.execute(
                "select r.entry_id from section_ruleset s join ruleset r on r.set_id = s.set_id "
                "where s.sid = ?", (sid_of[h],))}
            assert entry in got, (item, r, h)


# --------------------------------------------------------------------------- the atlas (slow)
@pytest.fixture(scope="module")
def atlas():
    from pipeline.atlas.registry import load_registry
    from pipeline.common.curated import GENERATED
    b = Path(os.environ.get("ATLAS_BUILD") or GENERATED.build())
    if not (b / "registry.json").exists():
        pytest.skip("no built atlas")
    return b, load_registry(str(b / "registry.json"))


@pytest.mark.slow
def test_every_straddler_has_a_measured_home_and_mara_is_region_3s(atlas):
    b, reg = atlas
    s = regions.straddlers(reg)
    shares = regions.region_shares(b, s)
    assert set(shares) == set(s)
    assert all(abs(sum(v.values()) - 1) < 0.02 for v in shares.values())
    mara = shares["lake:329518146"]
    assert mara["3"] > 0.6 and regions.home_of(mara) == "3"
