"""THE USER'S RULINGS OF 2026-09-28 (second set), each pinned on the book's own words.

1. A WATER ROW PRINTING ITS OWN DATES FOR A FISH overrides a dated zone rule for that fish, on the
   days both hold only. Cheslatta and Murray lakes (Region 6): "Lake trout catch and release,
   Sept 15-Oct 31" and "Lake trout daily and possession quotas = 3" (Nov 1-Sept 14) against Region
   6's "you must release … Lake trout from Fraser and Skeena Watersheds, Sept 15-Nov 30" — on Nov
   1-30 the lake's 3 speaks. Only compatible rules (the same fish or subject; a quota or release
   against a dated release or quota), never a zone closure; a water row with no dates of its own
   leaves the dated zone release speaking (Shuswap — `test_trout_scope_and_dated_releases`).
2. THE SIZE-CLAUSE CAUTION ONLY WHERE THE ROW PRINTS "(any size)" — Kootenay Lake's "rainbow trout
   daily quota = 10 (any size)", Duncan, Lardeau, Quesnel. The override itself is unchanged.
3. CHINOOK IS A NAMED FISH IN THE SALMON GROUP — not a game fish, not on p.80's list;
   `zp:salmon_stamp.r1` names it, and its label reads "Adult chinook" as the verbatim does.

Mutation checks: `<scratchpad>/R13/mutate.py` breaks each guard below in a scratch copy of the code
and confirms these tests fail (see the round's report).
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.bundle import read as R
from pipeline.deliver.bundle import rules as RM
from pipeline.regs.parsing import catalogue as C

BUNDLE = str(Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE))
CAT = Path("data/curated/regulations/entries/catalogue")


def _tiny(tmp_path, rules: list) -> str:
    path = str(tmp_path / f"tiny{len(list(tmp_path.iterdir()))}.sqlite")
    con = sqlite3.connect(path)
    con.execute("create table section_ruleset (sid integer, set_id integer)")
    con.execute("create table ruleset (set_id integer, entry_id text, rule_id text, via text)")
    con.execute("insert into section_ruleset values (1, 0)")
    con.executemany("insert into ruleset values (0, ?, ?, 'reach')",
                    [(x["entry"], x["rule"]) for x in rules])
    con.commit()
    con.close()
    R._RULES_BY_PATH[path] = {(x["entry"], x["rule"]): dict(
        {"type": "retention_limit", "dimension": "daily", "family": "retention"}, **x)
        for x in rules}
    return path


def _states(path, fish, on) -> dict:
    return {x["rule"]: x["state"] for x in R.effective_rules(1, on, fish, path)}


def _dates(fm, fd, tm, td) -> dict:
    return {"dates": [{"from_month": fm, "from_day": fd, "to_month": tm, "to_day": td}]}


# ======================================================== 1 — a water's own dates, on the overlap
R6_LT = {"entry": "z6:q", "rule": "q.r8", "species": ["LT"], "take": 0, "may_target": 1,
         "_rank": 3, "when": _dates(9, 15, 11, 30)}


def _cheslatta(tmp_path, *water) -> str:
    return _tiny(tmp_path, [R6_LT, *[dict({"entry": "r6:ches", "_rank": 0}, **w) for w in water]])


RELEASE = {"rule": "ches.r1", "species": ["LT"], "take": 0, "may_target": 1,
           "when": _dates(9, 15, 10, 31)}
QUOTA = {"rule": "ches.r2", "species": ["LT"], "take": 3, "when": _dates(11, 1, 9, 14)}


def test_cheslattas_own_dates_override_region_6s_release_on_the_overlap(tmp_path):
    """Nov 1-30: both in force, the lake's 3 speaks alone. Sept 15-Oct 31: the lake's own release.
    Dec-Sept 14: the region's release is not in force. MUTATION: `water_dates_override` answering
    False leaves the region's release beside the lake's 3 on Nov 15."""
    path = _cheslatta(tmp_path, RELEASE, QUOTA)
    assert _states(path, "LT", (11, 15)) == {"ches.r2": "speaks"}
    assert _states(path, "LT", (11, 30)) == {"ches.r2": "speaks"}
    assert _states(path, "LT", (10, 1)) == {"ches.r1": "speaks"}
    assert _states(path, "LT", (12, 15)) == {"ches.r2": "speaks"}
    assert _states(path, "LT", (7, 1)) == {"ches.r2": "speaks"}


def test_a_water_row_with_no_dates_of_its_own_leaves_the_dated_release(tmp_path):
    """Shuswap's case, restated here beside the override: the same quota with no dates keeps the
    region's release speaking on its dates. MUTATION: dropping the own-dates requirement from
    `water_dates_override` silences it."""
    path = _cheslatta(tmp_path, {"rule": "ches.r2", "species": ["LT"], "take": 3})
    assert _states(path, "LT", (11, 15)) == {"q.r8": "speaks", "ches.r2": "speaks"}


def test_only_the_same_fish_or_subject_overrides(tmp_path):
    """A water rule for ANOTHER fish never touches the zone's lake trout release, and a water GROUP
    quota ("Trout/char: 3", dated) does not name the lake trout the zone names — the release still
    speaks. MUTATION: dropping the same-fish test lets the group quota silence it."""
    path = _cheslatta(tmp_path, {"rule": "ches.r5", "species": ["RB"], "take": 3,
                                 "when": _dates(11, 1, 9, 14)})
    assert _states(path, "LT", (11, 15)) == {"q.r8": "speaks"}
    assert _states(path, "RB", (11, 15)) == {"ches.r5": "speaks"}
    path = _cheslatta(tmp_path, {"rule": "ches.r6", "species": ["TROUT_CHAR"], "take": 3,
                                 "when": _dates(11, 1, 9, 14)})
    assert _states(path, "LT", (11, 15))["q.r8"] == "speaks"


def test_a_narrower_water_rule_does_not_override(tmp_path):
    """"Hatchery lake trout: 3" says nothing of a wild lake trout: the region's release stays for
    every lake trout. The bundle keys a hatchery quota apart (`daily@origin=hatchery`), so it never
    meets the release; the guard holds even where the keys agree. MUTATION: dropping the origin
    test lets it silence the release."""
    path = _cheslatta(tmp_path, dict(QUOTA, origin="hatchery"))
    assert _states(path, "LT", (11, 15))["q.r8"] == "speaks"


def test_a_zone_closure_is_never_overridden_by_a_waters_dates(tmp_path):
    """A blanket closure (take 0, may not fish) on printed dates still closes the water, whatever
    the water's own-dated quota says."""
    path = _tiny(tmp_path, [
        {"entry": "z6:s", "rule": "s.r1", "species": ["ALL_GAME_FISH"], "take": 0,
         "may_target": 0, "_rank": 3, "when": _dates(4, 1, 6, 30)},
        dict({"entry": "r6:ches", "_rank": 0}, **dict(QUOTA, when=_dates(5, 1, 10, 31)))])
    assert _states(path, "LT", (5, 15))["s.r1"] == "speaks"


def test_only_a_water_row_overrides_by_its_dates(tmp_path):
    """A zone table's own dated quota is no water row: it never silences the same table's dated
    release (MUTATION: dropping the place test lets the table's quota silence its own release).
    The other way is the table's own tie-break (Phase 3, generalised step 4b; AGENTS 57): on the
    days both hold, the release displaces its own table's keeper in the same base dimension — the
    7B grayling release May 1-Jun 15 over that table's "2 per day", as the book reads it."""
    path = _tiny(tmp_path, [R6_LT, {"entry": "z6:q", "rule": "q.r9", "species": ["LT"],
                                    "take": 3, "_rank": 3, "when": _dates(11, 1, 9, 14)}])
    got = _states(path, "LT", (11, 15))
    assert got.get("q.r8") == "speaks" and got.get("q.r9") != "speaks", got
    # outside the release's dates the table's 3 speaks alone (the positive control)
    assert _states(path, "LT", (9, 1)).get("q.r9") == "speaks"


def test_a_dated_zone_quota_gives_way_to_the_waters_dated_quota_on_the_overlap(tmp_path):
    """The ruling reads both kinds: a retention quota or release against a dated release OR QUOTA
    for that fish. A water's "Rainbow trout: 2, June 15-Oct 31" against a zone's dated "Trout: 1,
    July 1-Oct 31": on the overlap the water's 2 speaks alone (without the ruling the two are
    different statements and sit beside each other)."""
    path = _tiny(tmp_path, [
        {"entry": "z6:q", "rule": "q.r4", "species": ["TROUT_CHAR"], "species_except": ["CHAR"],
         "take": 1, "_rank": 3, "when": _dates(7, 1, 10, 31)},
        {"entry": "r6:river", "rule": "river.r1", "species": ["RB"], "take": 2, "_rank": 0,
         "when": _dates(6, 15, 10, 31)}])
    assert _states(path, "RB", (8, 1)) == {"river.r1": "speaks"}
    # a lake trout is no rainbow: the water rule never speaks for it (per fish)
    assert _states(path, "LT", (8, 1)) == {}


def _sid(db, eid, rid):
    return db.execute("select min(sr.sid) from ruleset r join section_ruleset sr on "
                      "sr.set_id = r.set_id where r.entry_id = ? and r.rule_id = ? and "
                      "r.via = 'reach'", (eid, rid)).fetchone()[0]


@pytest.mark.parametrize("eid,lake", [("r6:cheslatta_lake@6-4", "cheslatta_lake"),
                                      ("r6:murray_lake@6-4", "murray_lake")])
def test_cheslatta_and_murray_on_the_built_bundle(eid, lake):
    """On the built bundle, where Region 6's lake trout release is bound on the lake."""
    if not Path(BUNDLE).exists():
        pytest.skip("no bundle")
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    sid = _sid(db, eid, f"{lake}.r2")
    zone = "z6:trout_char_quota::trout_char_quota.r8"
    bound = {f"{e}::{r}" for e, r in db.execute(
        "select r.entry_id, r.rule_id from ruleset r join section_ruleset sr on "
        "sr.set_id = r.set_id where sr.sid = ?", (sid,))}
    assert zone in bound
    got = lambda on: {f"{x['entry']}::{x['rule']}": x["state"]  # noqa: E731
                      for x in R.effective_rules(sid, on, "LT", BUNDLE)
                      if x["type"] == "retention_limit"}
    nov = got((11, 15))
    assert nov.get(f"{eid}::{lake}.r2") == "speaks" and zone not in nov
    oct_ = got((10, 1))
    assert oct_.get(f"{eid}::{lake}.r1") == "speaks" and zone not in oct_


# ================================================================ 2 — "(any size)" and only it
def _size_clause():
    V = C.CatalogueRule.model_validate
    region = [{"op": "within", "area_kind": "region"}]
    parent = V({"rule_id": "q.r1", "type": "retention_limit", "verbatim": "Trout/char: 5",
                "species": ["TROUT_CHAR"], "take": 5, "extents": region})
    clause = V({"rule_id": "q.r2", "type": "retention_limit",
                "verbatim": "1 rainbow trout or cutthroat trout over 50 cm",
                "species": ["RB", "CT"], "take": 1, "within": "q.r1",
                "lengths": [{"min_cm": 50}], "extents": region})
    return {"z4:q": {"q.r1": parent, "q.r2": clause}}


def _lift(verbatim: str) -> list:
    r = C.CatalogueRule.model_validate(
        {"rule_id": "k.r4", "type": "retention_limit", "verbatim": verbatim, "species": ["RB"],
         "take": 10, "extents": [{"op": "whole"}],
         "exempts": [{"entry_id": "z4:q", "target": "q.r2"}]})
    return json.loads(RM._exempts("r4:k", r, {}, _size_clause()))


def test_the_caution_only_where_the_row_prints_any_size():
    """Kootenay Lake's "(any size)" carries it; a row printing its own sizes (Gwillim) or none
    (Jewel) lifts the clause with no caution. The lift itself is the same in all three.
    MUTATION: `prints_any_size` answering True puts it back on the other two."""
    kootenay = _lift("rainbow trout daily quota = 10 (any size)")
    assert kootenay[0]["caution"]["kind"] == RM.SIZE_CLAUSE_OVERRIDE
    assert "'(any size)'" in kootenay[0]["caution"]["says"]
    for v in ("Rainbow trout daily quota = 10",
              "Rainbow trout daily quota = 10 (none under 40 cm or over 60 cm)"):
        got = _lift(v)
        assert [(x["entry_id"], x["rule_id"]) for x in got] == [("z4:q", "q.r2")]
        assert "caution" not in got[0], v


def test_prints_any_size_reads_the_book_s_words():
    row = lambda v: type("R", (), {"verbatim": v})  # noqa: E731
    assert RM.prints_any_size(row("rainbow trout daily quota = 10 (any size)"))
    assert RM.prints_any_size(row("Lake trout daily quota = 5 ( Any Size )"))
    assert not RM.prints_any_size(row("Brook trout daily quota = 20"))
    assert not RM.prints_any_size(row("Hatchery steelhead (adipose clipped, >50 cm) daily "
                                      "quota = 2"))


def test_the_built_bundle_cautions_exactly_the_four_any_size_rows():
    if not Path(BUNDLE).exists():
        pytest.skip("no bundle")
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    got = {}
    for e, r, v, ex in db.execute("select entry_id, rule_id, verbatim, exempts from rule "
                                  "where exempts is not null"):
        if any(x.get("caution") for x in json.loads(ex)):
            got[f"{e}::{r}"] = v
    if len(got) > 4:
        pytest.skip(f"{BUNDLE} predates the (any size) ruling — point UI_EXPORT_BUNDLE at one")
    assert sorted(k.split("::")[1] for k in got) == [
        "duncan_river.r4", "kootenay_lake_main_body.r4", "lardeau_river.r4", "quesnel_lake.r3"]
    assert all("(any size)" in v for v in got.values())


# ===================================================== 3 — chinook, a named fish of the SALMON group
def test_chinook_is_a_salmon_not_a_game_fish():
    assert C.SALMON_FISH == {"CH": "SALMON"}
    assert "CH" in C.KNOWN_SPECIES and "CH" not in C.BOOK_SPECIES
    assert "CH" not in C.SPECIES_GROUPS["ALL_GAME_FISH"]
    assert C.SPECIES_GROUPS["SALMON"] == () and C.expand_species(["SALMON"]) == ["SALMON"]
    assert C._SPECIES_WORDS["CH"] == "Chinook"


def _rule(**kw):
    base = {"rule_id": "x.r1", "type": "retention_limit", "extents": [{"op": "whole"}]}
    return C.CatalogueRule.model_validate(dict(base, **kw))


def test_a_book_row_may_name_chinook_but_no_other_federal_salmon():
    """MUTATION: emptying `SALMON_FISH` refuses the chinook row."""
    ok = _rule(verbatim="Chinook daily quota = 1", species=["CH"], take=1)
    C.CatalogueEntry(entry_id="r9:x", name="X", regs_verbatim=ok.verbatim, rules=[ok])
    coho = _rule(verbatim="Coho daily quota = 1", species=["CO"], take=1)
    with pytest.raises(ValueError, match="'CO' — not on the book's species list"):
        C.CatalogueEntry(entry_id="r9:x", name="X", regs_verbatim=coho.verbatim, rules=[coho])


def test_chinook_excepted_from_the_game_fish_subtracts_nothing():
    """"All game fish other than chinook" reads as if chinook were one. MUTATION: dropping the
    check lets it through."""
    with pytest.raises(ValueError, match="chinook is a salmon, not a game fish"):
        _rule(verbatim="No fishing", species=["ALL_GAME_FISH"], species_except=["CH"], take=0,
              may_target=False)
    _rule(verbatim="Release all salmon other than chinook", species=["SALMON"],
          species_except=["CH"], take=0, may_target=True)


def test_adult_chinook_is_said_by_life_stage_both_ways():
    """MUTATION: dropping the life_stage checks from `_check` lets each through."""
    v = "You must immediately record your retention of adult chinook salmon on your licence."
    with pytest.raises(ValueError, match="set life_stage: adult"):
        _rule(verbatim=v, species=["CH"], record_retention=True)
    with pytest.raises(ValueError, match="prints no 'adult chinook'"):
        _rule(verbatim="Record your retention of chinook", species=["CH"],
              record_retention=True, life_stage="adult")
    with pytest.raises(ValueError, match="defined for CH only"):
        _rule(verbatim=v, species=["SALMON"], record_retention=True, life_stage="adult")
    r = _rule(verbatim=v, species=["CH"], record_retention=True, life_stage="adult")
    assert C.label(r) == "Adult chinook — record your retention on your licence immediately"


def test_the_salmon_stamp_rule_names_adult_chinook():
    doc = json.loads((CAT / "region-provincial.json").read_text(encoding="utf-8"))
    e = next(x for x in doc["entries"] if x["entry_id"] == "zp:salmon_stamp")
    ce = C.CatalogueEntry.model_validate(e)
    r = next(x for x in ce.rules if x.rule_id == "salmon_stamp.r1")
    assert r.species == ["CH"] and r.life_stage is C.LifeStage.adult
    assert "adult chinook" in r.verbatim
    assert C.label(r) == "Adult chinook — record your retention on your licence immediately"


def test_a_salmon_rule_speaks_for_chinook_and_no_game_fish():
    """MUTATION: dropping the SALMON -> named salmon line from `speaks_for` fails the first."""
    salmon = {"species": ["SALMON"]}
    assert R.speaks_for(salmon, "CH")
    assert not R.speaks_for(salmon, "RB") and not R.speaks_for(salmon, "KO")
    assert R.speaks_for({"species": ["CH"]}, "CH")
    assert not R.speaks_for({"species": ["ALL_GAME_FISH"]}, "CH")
    assert not R.speaks_for({"species": ["SALMON"], "species_except": ["CH"]}, "CH")
