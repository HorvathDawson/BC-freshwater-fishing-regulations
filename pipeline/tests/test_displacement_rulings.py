"""DISPLACEMENT RULINGS OF 2026-09-29 — pinned on synthetic sections and on the waters the book prints.

  * A SIZE RULE displaces only over the lengths it speaks about (N-5): "Rainbow trout over 50 cm
    catch and release" leaves the quota for smaller rainbow speaking; the catalogue refuses a
    `take` that no band uses (the encoding that made such a rule a count).
  * A WATER TABLE'S AREA ROW ranks as an area (SP-3): Duck Lake's own row beats the CVWMA's.
  * A DISPLACED RULE DISPLACES NOTHING (SP-4): Cheslatta Lake's region quotas stand on Nov 1-30.
  * An overridden release releases nothing (SP-12).
  * A water row's own dates override a zone release limited to a kind of water (user ruling,
    2026-09-29), and the overridden release displaces nothing.
  * Exemptions: Region 5 listed streams, Fulton River, Thompson River; Bella Coola's EXCEPT;
    Nahatlatch open from June 1; Beaver Creek (not listed) under Region 5's spring closure.

Each synthetic test names the mutation that fails it. The real-section tests need a bundle built
from this corpus (`UI_EXPORT_BUNDLE`); they skip on one that predates it.
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.bundle import read as R
from pipeline.tests.test_competition import _tiny

BUNDLE = str(Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE))


def _dates(fm, fd, tm, td):
    return {"dates": [{"from_month": fm, "from_day": fd, "to_month": tm, "to_day": td}]}


def _speaks(path, fish, on) -> set:
    return {x["rule"] for x in R.effective_rules(1, on, fish, path) if x["state"] == "speaks"}


# ------------------------------------------------------------------------------ F: size rules
ZONE_5 = {"entry": "z6:q", "rule": "q.r1", "species": ["TROUT_CHAR"], "take": 5, "_rank": 3}
OVER_50 = {"entry": "z6:q", "rule": "q.r2", "species": ["TROUT_CHAR"], "take": 1,
           "within": "q.r1", "lengths": [{"min_cm": 50}], "_rank": 3}


def test_covers_reads_length_ranges():
    every = {}
    big = {"lengths": [{"min_cm": 50, "take": 0}]}
    two = {"lengths": [{"max_cm": 50}, {"min_cm": 50, "take": 0}]}
    assert R.covers(every, big) and R.covers(two, every) and R.covers(two, big)
    assert not R.covers(big, every) and not R.covers(big, {"lengths": [{"min_cm": 40}]})
    assert R.covers({"lengths": [{"max_cm": 29, "take": 0}, {"min_cm": 30}]}, every)
    # capped at 50 cm (a rainbow where a bigger one is a steelhead): "none over 50" says nothing
    assert R.covers({"lengths": [{"max_cm": 50}]}, every, top=50.0)
    assert not R.covers({"lengths": [{"max_cm": 50}]}, every)


def test_a_size_rule_never_silences_the_quota_for_smaller_fish(tmp_path):
    """Lakelse Lake's "Rainbow trout over 50 cm catch and release", in the encoding the prompt used
    to teach (take 0 beside a band that has its own take: a `daily` count): it names the rainbow,
    so by naming it beat Region 6's "Trout/char: 5" and a 30 cm rainbow had no limit at all.
    MUTATION: removing the `covers` guard from `beats` silences q.r1."""
    path = _tiny(tmp_path, [ZONE_5, OVER_50, {
        "entry": "r6:lakelse", "rule": "lk.r1", "species": ["RB"], "take": 0, "may_target": 1,
        "lengths": [{"min_cm": 50, "take": 0}], "_rank": 0}])
    got = _speaks(path, "RB", (7, 15))
    assert {"q.r1", "lk.r1"} <= got
    # over the lengths it does speak about it is the stricter word: "1 over 50 cm" is gone
    assert "q.r2" not in got


def test_chilko_lakes_own_quota_speaks_beside_its_70_cm_rule(tmp_path):
    """Chilko Lake: "Trout/char daily quota = 2 (no rainbow trout over 70 cm …)". The size rule must
    not take the lake's own 2 away for rainbow. MUTATION: no `covers` guard -> ck.r1 silent."""
    path = _tiny(tmp_path, [ZONE_5, {
        "entry": "r5:chilko", "rule": "ck.r1", "species": ["TROUT_CHAR"], "take": 2, "_rank": 0},
        {"entry": "r5:chilko", "rule": "ck.r2", "species": ["RB"], "take": 0, "may_target": 1,
         "lengths": [{"min_cm": 70, "take": 0}], "_rank": 0}])
    assert _speaks(path, "RB", (7, 15)) >= {"ck.r1", "ck.r2"}


def test_the_catalogue_refuses_a_take_no_band_uses():
    """MUTATION: deleting the `take fills no band` check in `CatalogueRule._check` fails this."""
    from pydantic import ValidationError

    from pipeline.regs.parsing.catalogue import CatalogueRule
    base = {"rule_id": "x.r1", "type": "retention_limit", "verbatim": "no trout over 50 cm",
            "species": ["TROUT_CHAR"], "may_target": True, "extents": [{"op": "whole"}]}
    with pytest.raises(ValidationError, match="fills no band"):
        CatalogueRule.model_validate(dict(base, take=0, lengths=[{"min_cm": 50, "take": 0}]))
    ok = CatalogueRule.model_validate(dict(base, lengths=[{"min_cm": 50, "take": 0}]))
    assert ok.dimension == "daily/size"
    # a band without its own take still uses the rule's: "1 over 50 cm" keeps its count
    CatalogueRule.model_validate(dict(base, take=1, lengths=[{"min_cm": 50}], may_target=None))


def test_the_catalogue_refuses_tributaries_only_without_tributaries():
    """The prompt's "never beside includes_tributaries: false". MUTATION: deleting the check."""
    from pydantic import ValidationError

    from pipeline.regs.parsing.catalogue import CatalogueRule
    base = {"rule_id": "x.r1", "type": "retention_limit", "verbatim": "No Fishing in tributaries",
            "species": ["ALL_GAME_FISH"], "take": 0, "may_target": False,
            "extents": [{"op": "whole"}], "tributaries_only": True}
    with pytest.raises(ValidationError, match="binds nothing"):
        CatalogueRule.model_validate(dict(base, includes_tributaries=False))
    CatalogueRule.model_validate(base)


def test_no_catalogue_rule_carries_a_take_no_band_uses():
    """The 39 size rules N-5 found (and 3 clauses of the same shape) are migrated; the model now
    refuses the shape, so the corpus loading is the proof."""
    from pipeline.regs.parsing.catalogue import CatalogueFile
    cat = Path(__file__).resolve().parents[2] / "data/curated/regulations/entries/catalogue"
    bad = []
    for p in sorted(cat.glob("region-*.json")):
        doc = json.loads(p.read_text(encoding="utf-8"))
        CatalogueFile.model_validate(doc)
        bad += [(e["entry_id"], r["rule_id"]) for e in doc["entries"] for r in e.get("rules") or []
                if r.get("take") is not None and r.get("lengths")
                and all("take" in b for b in r["lengths"])]
    assert bad == []


# ------------------------------------------------------------------------------ H: area rows
def test_a_water_tables_area_row_ranks_as_an_area():
    """MUTATION: dropping the area branch of `source_of` for `r<n>:` rows ranks it 0 again."""
    cvwma = {"entry": "r4:cvwma@4-6", "rule": "c.r1", "entry_name": "CVWMA WATERS",
             "extents": [{"op": "within", "area_id": "area:wma:creston_valley"}]}
    assert R.source_of(cvwma).scope is R.Scope.area and R.source_of(cvwma).rank == 2
    lake = {"entry": "r4:duck@4-6", "rule": "d.r1", "extents": [{"op": "whole"}]}
    assert R.source_of(lake).rank == 0
    # a `within` naming no area (an unbound place) is not an area row
    assert R.source_of({"entry": "r4:wood@4-40", "rule": "w.r1",
                        "extents": [{"op": "within"}]}).rank == 0


# ------------------------------------------------------------------------------ I: displacement
R6_LT_REL = {"entry": "z6:q", "rule": "q.r8", "species": ["LT"], "take": 0, "may_target": 1,
             "_rank": 3, "when": _dates(9, 15, 11, 30)}
R6_3DVLT = {"entry": "z6:q", "rule": "q.r3", "species": ["DV", "LT"], "take": 3,
            "within": "q.r1", "_rank": 3}
CHES_Q = {"entry": "r6:ches", "rule": "ches.r2", "species": ["LT"], "take": 3, "_rank": 0,
          "when": _dates(11, 1, 9, 14)}


def test_a_displaced_rule_displaces_nothing(tmp_path):
    """Cheslatta Lake, lake trout, Nov 15: Region 6's release (Sept 15-Nov 30) is overridden by the
    lake's own dated quota, so it no longer takes the region's 5 and its '3 DV/LT' clause with it
    — they speak beside the lake's 3, as on Jul 1. MUTATION: reverting step 4 to "beaten by any
    competitor" silences q.r1 and q.r3 on Nov 15."""
    path = _tiny(tmp_path, [ZONE_5, R6_3DVLT, R6_LT_REL, CHES_Q])
    assert _speaks(path, "LT", (11, 15)) == {"q.r1", "q.r3", "ches.r2"}
    assert _speaks(path, "LT", (7, 1)) == {"q.r1", "q.r3", "ches.r2"}
    # the release still displaces the region's quota where nothing overrides it
    (tmp_path / "b").mkdir()
    path = _tiny(tmp_path / "b", [ZONE_5, R6_3DVLT, R6_LT_REL])
    assert _speaks(path, "LT", (11, 15)) == {"q.r8"}


def test_an_overridden_release_releases_nothing(tmp_path):
    """SP-12: beside a water "Wild trout/char catch and release", the region's lake trout release
    that the lake's own dates overrode must not count as releasing HATCHERY lake trout, or the
    region's 5 is silenced for them. MUTATION: dropping `overridden` from step 5 silences q.r1."""
    path = _tiny(tmp_path, [ZONE_5, R6_LT_REL, CHES_Q, {
        "entry": "r6:ches", "rule": "ches.r9", "species": ["TROUT_CHAR"], "take": 0,
        "may_target": 1, "origin": "wild", "dimension": "daily@origin=wild", "_rank": 0}])
    assert "q.r1" in _speaks(path, "LT", (11, 15))


# ----------------------------------------------------------- the latent gap: water-kind releases
ZONE_DV_STREAMS = {"entry": "z3:q", "rule": "q.r6", "species": ["DV"], "take": 0,
                   "may_target": 1, "water": "stream", "dimension": "daily@water=stream",
                   "_rank": 3, "when": _dates(8, 1, 10, 31),
                   "extents": [{"op": "within", "area_id": "area:region:3",
                                "feature_types": ["stream"]}]}
Z3_5 = dict(ZONE_5, entry="z3:q")


def test_a_water_rows_own_dates_override_a_stream_release(tmp_path):
    """Region 3's "Bull trout (Dolly Varden) from streams, Aug 1-Oct 31" against a creek's own
    "Bull trout daily quota = 1, Sept 1-Oct 31": on Sept 15 the creek's 1 speaks and the release
    does not — and, overridden, it no longer silences Region 3's 5 (step 4b). On Aug 15 the
    creek's quota is not in force: the release speaks and silences the 5. MUTATION: deleting
    step 4a leaves q.r6 speaking on Sept 15 and the 5 silenced."""
    creek = {"entry": "r3:creek", "rule": "ck.r1", "species": ["DV"], "take": 1, "_rank": 0,
             "when": _dates(9, 1, 10, 31)}
    path = _tiny(tmp_path, [Z3_5, ZONE_DV_STREAMS, creek])
    assert _speaks(path, "DV", (9, 15)) == {"ck.r1", "q.r1"}
    assert _speaks(path, "DV", (8, 15)) == {"q.r6"}


def test_a_water_row_with_no_dates_leaves_the_stream_release(tmp_path):
    """The ruling needs the row's OWN dates (Shuswap's case, for a water-kind release)."""
    creek = {"entry": "r3:creek", "rule": "ck.r1", "species": ["DV"], "take": 1, "_rank": 0}
    path = _tiny(tmp_path, [Z3_5, ZONE_DV_STREAMS, creek])
    assert "q.r6" in _speaks(path, "DV", (9, 15))


# ------------------------------------------------------------------------ on the built bundle
@pytest.fixture(scope="module")
def db():
    if not Path(BUNDLE).exists():
        pytest.skip("no bundle")
    con = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    if not con.execute("select count(*) from rule where rule_id = 'fulton_river.r1b'").fetchone()[0]:
        pytest.skip(f"{BUNDLE} predates the 2026-09-29 corpus — point UI_EXPORT_BUNDLE at it")
    yield con
    con.close()


def _sid(db, eid, rid, via="reach"):
    got = db.execute("select min(s.sid) from ruleset r join section_ruleset s on s.set_id = r.set_id "
                     "where r.entry_id = ? and r.rule_id = ? and r.via = ?", (eid, rid, via))
    sid = got.fetchone()[0]
    assert sid is not None, (eid, rid)
    return sid


def _says(sid, on, fish) -> set:
    return {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(sid, on, fish, BUNDLE)
            if x["state"] == "speaks" and x["type"] == "retention_limit"}


def test_lakelse_and_chilko_keep_their_quotas_for_rainbow(db):
    lk = "r6:lakelse_lake@6-11"
    got = _says(_sid(db, lk, "lakelse_lake.r1"), (7, 15), "RB")
    assert {f"{lk}::lakelse_lake.r1", "z6:trout_char_quota::trout_char_quota.r1"} <= got
    ck = "r5:chilko_lake@5-4"
    got = _says(_sid(db, ck, "chilko_lake.r1"), (7, 15), "RB")
    assert {f"{ck}::chilko_lake.r1", f"{ck}::chilko_lake.r2"} <= got


def test_duck_lake_answers_to_its_own_row_not_the_cvwmas(db):
    duck = "r4:duck_lake_permit_required_see_note_on_page_34@4-6"
    cv = "r4:creston_valley_wildlife_management_area_cvwma_waters@4-6"
    sid = db.execute("select min(s.sid) from ruleset a join ruleset b on a.set_id = b.set_id "
                     "join section_ruleset s on s.set_id = a.set_id where a.entry_id = ? "
                     "and b.entry_id = ? and a.via = 'reach'", (duck, cv)).fetchone()[0]
    assert sid is not None
    assert _says(sid, (7, 15), "LMB") >= {f"{duck}::duck_lake.r1"}
    assert not any(x.startswith(cv) for x in _says(sid, (7, 15), "LMB") | _says(sid, (7, 15), "YP"))


def test_cheslatta_keeps_region_6s_quotas_on_nov_1_to_30(db):
    ch = "r6:cheslatta_lake@6-4"
    got = _says(_sid(db, ch, "cheslatta_lake.r2"), (11, 15), "LT")
    q = "z6:trout_char_quota::trout_char_quota."
    assert {f"{ch}::cheslatta_lake.r2", q + "r1", q + "r3"} <= got
    assert q + "r8" not in got


def test_michel_creeks_own_dated_release_replaces_region_4s_stream_release(db):
    mc = "r4:michel_creek_upstream_of_the_easternmost_hwy_3_bridge@4-23"
    got = _says(_sid(db, mc, "michel_creek_upper.r2"), (1, 15), "RB")
    assert f"{mc}::michel_creek_upper.r2" in got
    assert "z4:trout_char_winter_release::trout_char_winter_release.r1" not in got


@pytest.mark.parametrize("eid,rid,on", [
    ("r5:chilcotin_river@5-12+5-13+5-14", "chilcotin_river.r2x", (6, 20)),
    ("r5:chilko_river@5-5", "chilko_river.r1x", (6, 20)),
    ("r5:horsefly_river_from_quesnel_lake_to_horsefly_river_falls@5-2", "horsefly_river.r1x",
     (6, 15)),
    ("r5:quesnel_river@5-2", "quesnel_river.r2x", (6, 20)),
    ("r5:watch_creek@5-1", "watch_creek.r1x", (6, 1)),
    ("r5:baker_creek@5-13", "baker_creek.r2x", (6, 1)),
])
def test_region_5_listed_streams_are_out_of_the_spring_closure(db, eid, rid, on):
    """p.48: "No fishing in any stream in Fraser River Watershed of Region 5 … Apr 1-June 30,
    EXCEPT the mainstem of the Fraser River and other streams listed in the tables" (N-2)."""
    sid = _sid(db, eid, rid)
    assert "z5:spring_stream_closure::spring_stream_closure.r1" not in _says(sid, on, "RB")


def test_fulton_river_is_open_june_16_to_april_30(db):
    """p.57 "Open June 16-Apr 30 each year" lifts the Skeena winter closure (N-3); the row's own
    May 1-June 15 closure still closes it."""
    f = "r6:fulton_river@6-8"
    sid = _sid(db, f, "fulton_river.r1b")
    assert "z6:skeena_nass_winter_closure::skeena_nass_winter_closure.r1" not in _says(
        sid, (2, 15), "RB")
    assert f"{f}::fulton_river.r1" in _says(sid, (6, 1), "RB")


def test_thompson_rivers_additional_opening_in_may(db):
    """p.34 "Additional opening from the CNR Bridge downstream of Deadman River to CNR Bridge
    upstream of Bonaparte River, May 1-31" (LS-6): the row's Oct 1-May 31 closure and Region 3's
    spring closure are lifted there in May; the catch and release still binds."""
    t = "r3:thompson_river_downstream_of_signs_at_kamloops_lake_outlet_t@3-13+3-14+3-18"
    sid = _sid(db, t, "thompson_river_downstream_of_kamloops_lake.r3o")
    got = _says(sid, (5, 15), "RB")
    assert f"{t}::thompson_river_downstream_of_kamloops_lake.r1" not in got
    assert "z3:spring_stream_closure::spring_stream_closure.r1" not in got
    assert f"{t}::thompson_river_downstream_of_kamloops_lake.r3" in got


def test_bella_coolas_spring_exception_replaces_the_rivers_quota(db):
    """p.49: "Trout/char daily quota = 1 (none under 25 cm and all cutthroat trout catch and
    release) EXCEPT: on Bella Coola R. MAINSTEM ONLY, trout/char daily quota = 2 … Apr 1-May 31
    ONLY" (N-7): on the mainstem in April the 1 and the cutthroat release do not speak."""
    a = "r5:atnarko_bella_coola_rivers_includes_tributaries_except_burnt@5-11+5-6+5-8"
    sid = _sid(db, a, "atnarko_bella_coola_rivers.r5")
    got = _says(sid, (4, 15), "CT")
    assert f"{a}::atnarko_bella_coola_rivers.r5" in got
    assert not {f"{a}::atnarko_bella_coola_rivers.r3", f"{a}::atnarko_bella_coola_rivers.r4"} & got
    assert f"{a}::atnarko_bella_coola_rivers.r3" in _says(sid, (7, 15), "CT")


@pytest.fixture(scope="module")
def db18(db):
    if not db.execute("select count(*) from rule where rule_id = 'nicola_river.r3x'").fetchone()[0]:
        pytest.skip(f"{BUNDLE} predates the 2026-09-30 rulings (Nahatlatch, Beaver Creek)")
    return db


NAHATLATCH = "r3:nahatlatch_river@3-15"
Z3_SPRING = "z3:spring_stream_closure::spring_stream_closure.r1"
Z5_SPRING = "z5:spring_stream_closure::spring_stream_closure.r1"


@pytest.mark.parametrize("on,closed_by", [
    ((6, 15), {Z3_SPRING}),                                         # closed to June 30
    ((5, 15), {Z3_SPRING, f"{NAHATLATCH}::nahatlatch_river.r2"}),   # both
    ((7, 15), set()),
])
def test_nahatlatch_below_the_lake_is_closed_to_june_30(db18, on, closed_by):
    """p.31 "Downstream of Nahatlatch Lake (…), open until Dec 31; No Fishing Jan 1-May 31": the
    row's own closure and Region 3's Jan 1-June 30 spring closure BOTH hold (user decision
    2026-10-05, reversing the 2026-09-30 lift — "open until Dec 31" prints no opening inside the
    closure). Mutation: restore nahatlatch_river.r2x and June opens."""
    assert not db18.execute("select count(*) from rule where rule_id = 'nahatlatch_river.r2x'"
                            ).fetchone()[0]
    sid = db18.execute(
        "select min(sr.sid) from ruleset r join section_ruleset sr on sr.set_id = r.set_id "
        "where r.entry_id = ? and r.rule_id = 'nahatlatch_river.r2' and r.set_id in (select "
        "set_id from ruleset where entry_id = 'z3:spring_stream_closure')", (NAHATLATCH,)
    ).fetchone()[0]
    got = _says(sid, on, "RB")
    closures = {r for r in got if r in (Z3_SPRING, f"{NAHATLATCH}::nahatlatch_river.r2")}
    assert closures == closed_by
    if not closed_by:   # positive control: the stretch's quota speaks once it is open
        assert f"{NAHATLATCH}::nahatlatch_river.r3" in got


def test_beaver_creek_is_under_region_5s_spring_closure(db18):
    """BEAVER CREEK chain of lakes (p.43) is about the lakes ("No Fishing for bass"); the creek is
    not a listed stream, so Region 5's spring closure holds on it (user ruling 2026-09-30).
    Mutation: restore beaver_creek_chain_of_lakes.r1x."""
    b = "r5:beaver_creek_chain_of_lakes@5-2"
    assert not db18.execute("select count(*) from rule where entry_id = ? and rule_id = ?",
                            (b, "beaver_creek_chain_of_lakes.r1x")).fetchone()[0]
    assert Z5_SPRING in _says(_sid(db18, b, "beaver_creek_chain_of_lakes.r1"), (6, 15), "RB")


@pytest.mark.parametrize("eid,rid", [
    ("r5:beaver_lake@5-2", "beaver_lake.r1"), ("r5:chambers_lake@5-2", "chambers_lake.r1"),
    ("r5:robert_lake@5-2", "robert_lake.r1"), ("r5:rye_lake@5-2", "rye_lake.r1"),
])
def test_the_beaver_creek_chain_lakes_keep_their_bass_closure(db18, eid, rid):
    got = _says(_sid(db18, eid, rid), (7, 15), "LMB")
    assert f"{eid}::{rid}" in got


def test_daily_and_possession_rows_carry_both(db):
    """N-8: "daily and possession quotas = N" is two rules."""
    for eid, rid in [("r6:murray_lake@6-4", "murray_lake.r2p"),
                     ("r7:charlie_lake@7-33", "charlie_lake.r1p"),
                     ("r7:charlie_lake@7-33", "charlie_lake.r2p"),
                     ("r7:lower_blue_lake@7-21", "lower_blue_lake.r2p")]:
        got = db.execute("select dimension from rule where entry_id = ? and rule_id = ?",
                         (eid, rid)).fetchone()
        assert got and got[0].startswith("possession"), (eid, rid)


def test_kitimat_no_longer_restates_the_provincial_quota(db):
    k = "r6:kitimat_river_angling_regulations_for_the_kitimat_river_are@6-3"
    assert not db.execute("select 1 from rule where entry_id = ? and rule_id = 'kitimat_river.r6'",
                          (k,)).fetchone()
    see = json.loads(db.execute("select see from entry where entry_id = ?", (k,)).fetchone()[0])
    assert any("zp:steelhead" in (s.get("entry_ids") or []) for s in see)


def test_lower_coal_creek_is_not_classified(db):
    """The Elk River's tributary designations reach lower Coal Creek only by the walk; Coal
    Creek's own row says 'Part described is NOT a Classified Water'. MUTATION: removing
    `licensing._own_row_not_classified` makes the build refuse (unlisted conflict)."""
    coal = "r4:coal_creek_downstream_of_old_mf_m_railway_bridge_7_km_upstre@4-23"
    sids = [s for (s,) in db.execute(
        "select s.sid from section_licensing s join licensing_set l on l.set_id = s.set_id "
        "where l.entry_id = ? and l.kind = 'not_classified'", (coal,))]
    assert sids
    for sid in sids:
        kinds = {(k, v) for k, v in db.execute(
            "select l.kind, l.via from section_licensing s join licensing_set l "
            "on l.set_id = s.set_id where s.sid = ?", (sid,))}
        assert not any(k == "designation" for k, _ in kinds), kinds


def test_a_waters_own_not_classified_beats_a_walked_designation():
    """Pure: on a section Coal Creek's own row binds (`reach`) as NOT classified, the Elk River's
    designation that reached it by the tributary walk (`trib`) is dropped; a designation bound by
    its own extents stays (that conflict still stops the build). MUTATION: returning the input
    unchanged from `_own_row_not_classified` fails this."""
    from pipeline.deliver.bundle.licensing import _own_row_not_classified
    nc = ("not_classified", "r4:coal", "not_classified", "reach")
    walked = ("designation", "r4:elk", "elk_river", "trib")
    named = ("designation", "r4:other", "x", "reach")
    got = _own_row_not_classified({"a": {nc, walked}, "b": {walked}, "c": {nc, named}})
    assert got == {"a": {nc}, "b": {walked}, "c": {nc, named}}


@pytest.mark.parametrize("eid,rid,off,on", [
    ("r1:somass_river@1-7", "somass_river.r2", (1, 15), (7, 1)),
    ("r1:sproat_river@1-7", "sproat_river.r2", (1, 15), (7, 1)),
    ("r1:quatse_river@1-13", "quatse_river.r4", (1, 15), (7, 1)),
    ("r1:stamp_river@1-7", "stamp_river.r4", (1, 15), (7, 1)),
])
def test_region_1_rows_with_a_dated_bait_ban_are_its_exceptions(db, eid, rid, off, on):
    """p.15: "Bait ban: applies to all streams of Region 1, all year, with some important
    exceptions. Check the tables." A row printing a dated bait ban is one of them (G-031)."""
    zone = "z1:bait_ban_streams::bait_ban_streams.r1"
    sid = _sid(db, eid, rid + "x", via=db.execute(
        "select via from ruleset where entry_id = ? and rule_id = ? limit 1",
        (eid, rid + "x")).fetchone()[0])
    bait = lambda d: {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(sid, d, "RB", BUNDLE)  # noqa: E731
                      if x["state"] == "speaks" and x["type"] == "bait_restriction"}
    assert zone not in bait(off)
    assert f"{eid}::{rid}" in bait(on)
