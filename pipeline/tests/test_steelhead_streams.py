"""STEELHEAD RULES BIND STREAMS (user ruling 2026-10-01) — EXCEPT THE WILD RELEASE.

The province's steelhead rules (p.8: the annual hatchery quota of 10, the record duty, and the
Conservation Surcharge Stamp) are `water: stream` and reach only streams in the regions whose OWN
tables name steelhead; every zone rule naming steelhead binds the streams of its own area
(`feature_types: [stream]`).

THE WILD RELEASE BINDS LAKES TOO (second ruling 2026-10-01: "You must release all wild steelhead"
applies in lakes and streams). The province's "All wild steelhead must be released" binds every
water of the steelhead regions (1, 2, 3, 5, 6), with no `water` and no `feature_types`; each
zone's "And you must release: All wild steelhead" line (Regions 3 and 5 print "ALL STEELHEAD")
binds every water of its own area. Lakes are never steelhead water (`anadromous_rainbow` flags
streams only), so on a lake the release answers a steelhead question and never a rainbow's. A zone rule carries no `water`:
that field is part of the competition key, and keyed apart the zone's "ALL STEELHEAD" stopped
beating the water rows' trout/char quotas it beats by naming (a measured regression). A steelhead rule never applies to a lake unless the lake's own
row mentions steelhead (Khartoum and Lois lakes): big lake rainbow fall under the rainbow size
quota, not the steelhead quota.

Corpus tests read the catalogue; placement tests read a bundle (`UI_EXPORT_BUNDLE`, else the
shipped one) — the OUTPUT, never the extents that produced it. Each check is pinned by a
mutation that must turn it red.
"""
from __future__ import annotations

import copy
import json
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.regs.parsing import catalogue as C
from pipeline.tools import export_ui_rules as X

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or X.BUNDLE)

#: The regions whose own tables name steelhead, from the book's regional pages: 1 (p.15 "All
#: wild steelhead", "2 hatchery steelhead over 50 cm"), 2 (p.23, the same), 3 (p.30 "ALL
#: STEELHEAD"), 5 (p.48 "ALL STEELHEAD"), 6 (p.55 "all wild steelhead", the May 15-June 15
#: stream closure). 7A's one mention is the Stellako's "(Steelhead Stamp not required)" — a stamp
#: waiver, no steelhead rule; 4, 7B and 8 print none.
QUALIFYING = ("1", "2", "3", "5", "6")
PROVINCE_RULES = ("steelhead.r1", "steelhead.r2", "steelhead.r4")
#: The wild-steelhead releases — the only steelhead rules that bind lakes too (the book prints each
#: under "And you must release:"; p.8 for the province).
WILD_RELEASES = {
    ("zp:steelhead", "steelhead.r2"),                 # "All wild steelhead must be released."
    ("z1:trout_quota", "trout_quota.r5"),             # p.15 "All wild steelhead"
    ("z1:hg_quota", "hg_quota.r6"),                   # p.15 Haida Gwaii, the same line
    ("z2:trout_char_quota", "trout_char_quota.r7"),   # p.23 "All wild steelhead"
    ("z3:trout_char_quota", "trout_char_quota.r5"),   # p.30 "ALL STEELHEAD"
    ("z5:trout_char_quota", "trout_char_quota.r6"),   # p.48 "ALL STEELHEAD"
    ("z6:trout_char_quota", "trout_char_quota.r9"),   # p.55 "all wild steelhead"
}
#: Lakes whose own rows print "steelhead" ("Rainbow trout/hatchery steelhead quota = 6 in the
#: aggregate"): the only lakes a steelhead rule may show on.
OWN_STEELHEAD_LAKES = {"r2:khartoum_lake@2-12", "r2:lois_lake@2-12"}


@pytest.fixture(scope="module")
def raw():
    from pipeline.regs.parsing.io import read_all_entries
    return copy.deepcopy(read_all_entries())


def qualifying_regions(raw: dict) -> tuple:
    """Regions with a zone entry or water row carrying a rule whose species names steelhead."""
    from pipeline.regs.parsing.io import read_all_entries  # noqa: F401  (same reader)
    got = set()
    for eid, e in raw.items():
        if eid.startswith("zp:") or not any("ST" in (r.get("species") or [])
                                            for r in e.get("rules") or []):
            continue
        got.add(eid.split(":", 1)[0][1:] if eid.startswith("z") else str(e.get("region") or ""))
    return tuple(sorted(got))


def _streams_in(regions, **extra) -> list:
    return [{"op": "within", "area_id": f"area:region:{n}", "feature_types": ["stream"], **extra}
            for n in regions]


def _waters_in(regions) -> list:
    return [{"op": "within", "area_id": f"area:region:{n}"} for n in regions]


def is_wild_release(r: dict) -> bool:
    """The catalogue twin of `export_ui_rules.is_wild_steelhead_release`: steelhead alone, take 0,
    fishable (not a closure), wild or every origin, no date, size, water kind, record or clause."""
    return (r.get("type") == "retention_limit" and r.get("species") == ["ST"]
            and r.get("take") == 0 and r.get("may_target") is True
            and r.get("origin") in (None, "wild")
            and not any(r.get(k) for k in ("when", "lengths", "water", "record_retention",
                                           "within", "while", "when_targeting")))


def steelhead_scope_problems(raw: dict) -> list[str]:
    """Every provincial or zone rule/record about steelhead that could reach a lake — other than a
    wild-steelhead release — or a wild release that does NOT reach the lakes of its area, or a
    provincial one that reaches a region whose tables do not name steelhead."""
    out = []
    regions = qualifying_regions(raw)
    for eid, e in sorted(raw.items()):
        if not eid.startswith("z"):
            continue
        for r in e.get("rules") or []:
            about = "ST" in (r.get("species") or []) or (
                not r.get("species") and "steelhead" in r["verbatim"].lower())
            if not about:
                continue
            tag = f"{eid}::{r['rule_id']}"
            if (eid, r["rule_id"]) in WILD_RELEASES or is_wild_release(r):
                if (eid, r["rule_id"]) not in WILD_RELEASES or not is_wild_release(r):
                    out.append(f"{tag}: the list of wild releases and the rule disagree")
                if r.get("water") is not None:
                    out.append(f"{tag}: a wild release keyed to water {r.get('water')!r}")
                if not r.get("extents") or any(x.get("feature_types") for x in r["extents"]):
                    out.append(f"{tag}: a wild release that does not reach lakes")
                if eid.startswith("zp:") and r.get("extents") != _waters_in(regions):
                    out.append(f"{tag}: extents are not the waters of regions {regions}")
                continue
            if eid.startswith("zp:") and r.get("water") != "stream":
                out.append(f"{tag}: water {r.get('water')!r}, not stream")
            if not eid.startswith("zp:") and r.get("water") not in (None, "stream"):
                out.append(f"{tag}: water {r.get('water')!r}")
            if not r.get("extents") or any(x.get("feature_types") != ["stream"]
                                           for x in r["extents"]):
                out.append(f"{tag}: an extent draws more than streams")
            if eid.startswith("zp:") and r.get("extents") != _streams_in(regions):
                out.append(f"{tag}: extents are not the streams of regions {regions}")
        for x in e.get("licensing") or []:
            if "ST" not in ((x.get("doing") or {}).get("species") or []):
                continue
            want = _streams_in(regions, outside_area_kind="national_parks")
            if x.get("water") != "stream" or x.get("extents") != want:
                out.append(f"{eid}#{x['id']}: not the streams of regions {regions}")
    return out


# ------------------------------------------------------------------------------------- corpus
def test_the_qualifying_regions_are_the_ones_whose_tables_name_steelhead(raw):
    assert qualifying_regions(raw) == QUALIFYING


def test_every_provincial_and_zone_steelhead_rule_binds_streams_only(raw):
    assert steelhead_scope_problems(raw) == []
    # the zone rules keep their competition key (no `water`): "ALL STEELHEAD" is still `daily`
    z3 = C.CatalogueEntry.model_validate(raw["z3:trout_char_quota"])
    assert next(r for r in z3.rules if r.rule_id == "trout_char_quota.r5").dimension == "daily"
    e = raw["zp:steelhead"]
    assert [r["rule_id"] for r in e["rules"]] == list(PROVINCE_RULES)
    model = C.CatalogueEntry.model_validate(e)
    dims = {r.rule_id: r.dimension for r in model.rules}
    assert dims == {"steelhead.r1": "annual@origin=hatchery&water=stream",
                    "steelhead.r2": "daily@origin=wild",            # lakes and streams
                    "steelhead.r4": "daily@origin=hatchery&water=stream&record"}
    # every listed wild release is in the corpus, and is one
    for eid, rid in WILD_RELEASES:
        assert is_wild_release(next(r for r in raw[eid]["rules"] if r["rule_id"] == rid)), rid


def _r(raw, eid, rid):
    return next(r for r in raw[eid]["rules"] if r["rule_id"] == rid)


@pytest.mark.parametrize("mutate", [
    lambda raw: raw["zp:steelhead"]["rules"][1].update(
        extents=[{"op": "within", "area_kind": "region"}]),     # the release in Regions 4, 7, 8
    lambda raw: raw["zp:steelhead"]["rules"][1].update(water="stream"),   # release off lakes
    lambda raw: raw["zp:steelhead"]["rules"][1]["extents"][0].update(feature_types=["stream"]),
    lambda raw: _r(raw, "z6:trout_char_quota", "trout_char_quota.r9")["extents"][0].update(
        feature_types=["stream"]),                              # a zone release off lakes
    lambda raw: raw["zp:steelhead"]["rules"][0].pop("water"),
    lambda raw: raw["zp:steelhead"]["rules"][2]["extents"].append(
        {"op": "within", "area_id": "area:region:4", "feature_types": ["stream"]}),
    lambda raw: raw["zp:steelhead"]["licensing"][0]["extents"][0].pop("feature_types"),
    lambda raw: _r(raw, "z6:hatchery_steelhead_stop", "hatchery_steelhead_stop.r1")[
        "extents"][0].pop("feature_types"),                     # stop-after-quota onto lakes
    lambda raw: _r(raw, "z2:trout_char_quota", "trout_char_quota.r3")[
        "extents"][0].pop("feature_types"),                     # the hatchery 2 onto lakes
])
def test_the_scope_check_catches_a_steelhead_rule_that_reaches_a_lake(raw, mutate):
    bad = copy.deepcopy(raw)
    mutate(bad)
    assert steelhead_scope_problems(bad)


def test_a_steelhead_row_in_region_4_would_make_it_qualify(raw):
    """The regions are read from the data, not listed: a Region 4 water row naming steelhead
    brings Region 4 in (and the provincial extents, unchanged, then fail the check)."""
    bad = copy.deepcopy(raw)
    eid = next(k for k, v in bad.items() if k.startswith("r4:") and v.get("rules"))
    bad[eid]["rules"][0] = dict(bad[eid]["rules"][0], species=["ST"])
    assert qualifying_regions(bad) == ("1", "2", "3", "4", "5", "6")
    assert any("extents are not the streams" in p for p in steelhead_scope_problems(bad))


# ----------------------------------------------------------------------------- the bundle
@pytest.fixture(scope="module")
def db():
    con = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    yield con
    con.close()


def _sections(db, item_id: str) -> list[int]:
    return [s for (s,) in db.execute("SELECT s.sid FROM item i JOIN item_section s "
                                     "ON s.ord = i.ord WHERE i.item_id = ?", (item_id,))]


def _rules_on(db, sid: int) -> set[str]:
    return {f"{e}::{r}" for e, r in db.execute(
        "SELECT r.entry_id, r.rule_id FROM section_ruleset s JOIN ruleset r "
        "ON r.set_id = s.set_id WHERE s.sid = ?", (sid,))}


def _requirements_on(db, sid: int) -> set[str]:
    return {f"{e}#{r}" for e, r in db.execute(
        "SELECT entry_id, req_id FROM requirement_section WHERE sid = ?", (sid,))}


PROVINCE = {f"zp:steelhead::{r}" for r in PROVINCE_RULES}
STAMP = "zp:steelhead#steelhead_targeting"


def test_a_lake_gets_only_the_provincial_wild_release(db):
    """No section of any lake the bundle names carries a provincial steelhead rule other than the
    wild release, or the stamp; lakes of the steelhead regions do carry the release."""
    got = {r for (r,) in db.execute(
        "SELECT DISTINCT r.rule_id FROM item i JOIN item_section s ON s.ord = i.ord "
        "JOIN section_ruleset sr ON sr.sid = s.sid JOIN ruleset r ON r.set_id = sr.set_id "
        "WHERE i.kind = 'lake' AND r.entry_id = 'zp:steelhead'")}
    assert got == {"steelhead.r2"}
    hit = db.execute(
        "SELECT COUNT(DISTINCT i.item_id) FROM item i JOIN item_section s ON s.ord = i.ord "
        "JOIN requirement_section q ON q.sid = s.sid "
        "WHERE i.kind = 'lake' AND q.entry_id = 'zp:steelhead'").fetchone()[0]
    assert hit == 0
    # the stamp is placed on sections, never province-wide (which would hold on every lake)
    (placement,) = db.execute("SELECT placement FROM requirement WHERE entry_id = 'zp:steelhead' "
                              "AND req_id = 'steelhead_targeting'").fetchone()
    assert placement == "sections"
    # Khartoum Lake keeps its own steelhead rule and gets the wild releases back (province and
    # Region 2's "All wild steelhead"); no other steelhead rule
    (sid,) = _sections(db, "wbk:329197063")
    got = {x for x in _rules_on(db, sid) if "steelhead" in x}
    assert got == {"zp:steelhead::steelhead.r2"}
    assert {"r2:khartoum_lake@2-12::khartoum_lake.r5",
            "z2:trout_char_quota::trout_char_quota.r7"} <= _rules_on(db, sid)
    assert "z2:trout_char_quota::trout_char_quota.r3" not in _rules_on(db, sid)


def test_a_stream_in_a_qualifying_region_gets_them(db):
    """Capilano River (Region 2) carries all three provincial steelhead rules and the stamp."""
    secs = _sections(db, "gnis:5922")
    assert secs
    for sid in secs:
        assert PROVINCE <= _rules_on(db, sid), sid
        assert STAMP in _requirements_on(db, sid), sid


QUALIFYING_ZONES = tuple(f"z{n}:" for n in QUALIFYING)


@pytest.mark.parametrize("item_id,name", [("gnis:16880", "Elk River"),          # Region 4
                                          ("gnis:26324", "Similkameen River")])  # Region 8 (+2)
def test_a_stream_in_a_region_whose_tables_do_not_name_steelhead_does_not(db, item_id, name):
    """Every section is read by its HOME region (the zone table it binds): a piece bound to no
    qualifying region's table carries no provincial steelhead rule and no stamp. The Similkameen
    has a piece homed in Region 2, which does."""
    secs = _sections(db, item_id)
    outside = [s for s in secs if not any(r.startswith(QUALIFYING_ZONES)
                                          for r in _rules_on(db, s))]
    assert outside, name
    for sid in outside:
        assert not (PROVINCE & _rules_on(db, sid)), (name, sid)
        assert STAMP not in _requirements_on(db, sid), (name, sid)
    for sid in set(secs) - set(outside):
        assert PROVINCE <= _rules_on(db, sid), (name, sid)


# ----------------------------------------------------------------------------- the export
@pytest.fixture(scope="module")
def doc() -> dict:
    return X.build(BUNDLE)


def test_the_export_shows_no_steelhead_rule_on_a_lake(doc):
    assert X.steelhead_lake_problems(doc) == []
    # the two lakes whose rows print "steelhead" keep their own steelhead rule
    for eid in OWN_STEELHEAD_LAKES:
        e = doc["entries"][eid]
        assert "steelhead" in e["printed"].lower()
        assert any(X.is_steelhead_rule(doc["rules"][i]) for i in e["rules"]), eid


def _lake_part(doc, own: bool):
    for item, w in sorted(doc["waters"].items()):
        mentions = any(e in OWN_STEELHEAD_LAKES for e in w["entries"])
        if w["kind"] == "lake" and mentions == own and w["parts"][0]["ruleset"]:
            return item, w["parts"][0]
    raise AssertionError("no such lake")


def test_the_export_allows_only_the_wild_release_on_lakes(doc):
    """The wild releases (province and zones) are the only steelhead rules `is_wild_steelhead_release`
    passes; the hatchery quotas, the annual 10, the record duty, stop-after-quota and the Region 6
    stream closure are not."""
    wild = {i for i, x in doc["rules"].items()
            if X.is_wild_steelhead_release(x) and x["entry_id"].startswith("z")}
    assert wild == {f"{e}::{r}" for e, r in WILD_RELEASES}
    # a water row's own steelhead release is one too, and binds only its own water (Brunette and
    # Capilano rivers' "steelhead catch and release")
    for i in ("zp:steelhead::steelhead.r1", "zp:steelhead::steelhead.r4",
              "z2:trout_char_quota::trout_char_quota.r3",
              "z6:hatchery_steelhead_stop::hatchery_steelhead_stop.r1",
              "z6:steelhead_stream_closure::steelhead_stream_closure.r1"):
        assert X.is_steelhead_rule(doc["rules"][i]) and not X.is_wild_steelhead_release(
            doc["rules"][i]), i


@pytest.mark.parametrize("rid", ["zp:steelhead::steelhead.r1", "zp:steelhead::steelhead.r4",
                                 "z2:trout_char_quota::trout_char_quota.r3",
                                 "z6:hatchery_steelhead_stop::hatchery_steelhead_stop.r1"])
def test_the_export_check_refuses_a_steelhead_rule_on_a_lake(doc, rid):
    """Mutation: a streams-only steelhead rule put on one lake's ruleset is refused; the wild
    release put there is not."""
    bad = {k: doc[k] for k in ("rules", "licensing", "entries", "waters", "licensing_sets")}
    bad["rulesets"] = copy.deepcopy(doc["rulesets"])
    item, part = _lake_part(doc, own=False)
    bad["rulesets"][part["ruleset"]].setdefault("reach", []).append(
        "z1:trout_quota::trout_quota.r5")
    assert X.steelhead_lake_problems(bad) == []
    bad["rulesets"][part["ruleset"]]["reach"].append(rid)
    got = X.steelhead_lake_problems(bad)
    assert any(p.startswith(f"steelhead rule {rid} shows on") and item in p for p in got), got


def test_the_export_check_allows_only_the_lakes_own_row(doc):
    """On Lois Lake only Lois Lake's own steelhead rule and the wild releases may show: Region 2's
    "2 hatchery steelhead" put there is refused (its row mentions steelhead, but the rule is not
    its row's)."""
    bad = {k: doc[k] for k in ("rules", "licensing", "entries", "waters", "licensing_sets")}
    bad["rulesets"] = copy.deepcopy(doc["rulesets"])
    part = doc["waters"]["wbk:329197058"]["parts"][0]
    ids = [i for via, v in bad["rulesets"][part["ruleset"]].items() if via != "sections"
           for i in v]
    assert {"r2:lois_lake@2-12::lois_lake.r4", "zp:steelhead::steelhead.r2",
            "z2:trout_char_quota::trout_char_quota.r7"} <= set(ids), ids
    assert X.steelhead_lake_problems(bad) == []
    bad["rulesets"][part["ruleset"]].setdefault("reach", []).append(
        "z2:trout_char_quota::trout_char_quota.r3")
    assert any("z2:trout_char_quota::trout_char_quota.r3" in p and "wbk:329197058" in p
               for p in X.steelhead_lake_problems(bad))


def test_the_export_check_refuses_a_province_wide_stamp(doc):
    bad = {k: doc[k] for k in ("rules", "entries", "waters", "rulesets", "licensing_sets")}
    bad["licensing"] = copy.deepcopy(doc["licensing"])
    bad["licensing"][STAMP]["placement"] = "province"
    assert f"steelhead record {STAMP} is placed province-wide — it holds on every lake" \
        in X.steelhead_lake_problems(bad)
    assert json.dumps(doc["licensing"][STAMP]["placement"]) == '"sections"'


# ------------------------------------------------------------- steelhead water (p.86), streams only
#: Rows naming steelhead that are not steelhead water, and why (coordinator round 2026-10-01).
NOT_STEELHEAD_WATER = {
    "r2:khartoum_lake@2-12", "r2:lois_lake@2-12",                       # lakes
    "r2:fraser_river_upstream_of_the_cpr_bridge_at_mission@2-4",        # the whole Fraser item
    "r3:fraser_river@3-14", "r5:fraser_river@5-2",
    "r5:chilko_river@5-5", "r5:horsefly_river_from_quesnel_lake_to_horsefly_river_falls@5-2",
    "r5:west_road_blackwater_river@5-12+5-13", "r6:stellako_river@6-4+7-12",
    "r7:stellako_river@7-12",                                           # stamp waivers only
}


#: flagged by ruling ("Rainbow over 50 cm on Chilliwack is steelhead"); its row prints no "steelhead"
CHILLIWACK = "r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4"


def test_every_stream_row_naming_steelhead_is_steelhead_water(raw):
    named = {k for k, e in raw.items() if not k.startswith("z")
             and "steelhead" in (e.get("regs_verbatim") or "").lower()}
    flagged = {k for k, e in raw.items() if e.get("anadromous_rainbow")} - {CHILLIWACK}
    assert flagged == named - NOT_STEELHEAD_WATER
    assert "r6:kitimat_river_angling_regulations_for_the_kitimat_river_are@6-3" in flagged
    assert "r5:dean_river@5-9" in flagged


def test_no_lake_is_steelhead_water(db):
    """`steelhead_water` holds stream sections only: Vedder Canal (a lake item of the flagged
    Chilliwack/Vedder row) and Khartoum/Lois are out."""
    n = db.execute("SELECT COUNT(*) FROM steelhead_water w JOIN item_section s ON s.sid = w.sid "
                   "JOIN item i ON i.ord = s.ord WHERE i.kind = 'lake'").fetchone()[0]
    assert n == 0
    dean = _sections(db, "gnis:16075")
    assert dean and all(db.execute("SELECT 1 FROM steelhead_water WHERE sid = ?", (s,)).fetchone()
                        for s in dean)


def test_a_big_rainbow_on_kitimat_is_a_steelhead(db):
    """Kitimat: "Hatchery rainbow trout (adipose clipped, <50 cm) daily quota = 5" speaks for a
    rainbow; "Hatchery steelhead (>50 cm) daily quota = 2" for the fish over 50 cm — asked as ST."""
    from pipeline.deliver.bundle import read as R
    eid = "r6:kitimat_river_angling_regulations_for_the_kitimat_river_are@6-3"
    sid = next(s for s in _sections(db, "gnis:3225")
               if f"{eid}::kitimat_river.r4" in _rules_on(db, s))
    assert db.execute("SELECT 1 FROM steelhead_water WHERE sid = ?", (sid,)).fetchone()
    rb = {x["rule"] for x in R.effective_rules(sid, (7, 15), "RB", str(BUNDLE))
          if x["state"] == "speaks"}
    st = {x["rule"] for x in R.effective_rules(sid, (7, 15), "ST", str(BUNDLE))
          if x["state"] == "speaks"}
    assert "kitimat_river.r4" in rb and "kitimat_river.r5" not in rb
    assert "kitimat_river.r5" in st and "kitimat_river.r4" not in st
    # Region 6's "1 over 50 cm" speaks for no rainbow here: that fish is a steelhead
    assert "trout_char_quota.r2" not in rb


def test_the_flag_test_catches_a_flagged_lake(raw):
    bad = copy.deepcopy(raw)
    bad["r2:lois_lake@2-12"]["anadromous_rainbow"] = True
    named = {k for k, e in bad.items() if not k.startswith("z")
             and "steelhead" in (e.get("regs_verbatim") or "").lower()}
    flagged = {k for k, e in bad.items() if e.get("anadromous_rainbow")} - {CHILLIWACK}
    assert flagged != named - NOT_STEELHEAD_WATER


# ------------------------------------------------------- the wild release on a lake (2026-10-01)
@pytest.mark.parametrize("item_id,name,over50", [
    ("wbk:329385412", "Morice Lake", "z6:trout_char_quota::trout_char_quota.r2"),    # Region 6
    ("wbk:329291805", "Stave Lake", "z2:trout_char_quota::trout_char_quota.r2"),     # Region 2
    ("wbk:329400588", "Murtle Lake", "z3:trout_char_quota::trout_char_quota.r3"),    # Region 3
])
def test_on_a_lake_the_wild_release_answers_a_steelhead_never_a_rainbow(db, item_id, name,
                                                                         over50):
    """"You must release all wild steelhead" applies in lakes too, but a lake is never steelhead
    water: asked about a STEELHEAD the release speaks; asked about a RAINBOW (of any size) no
    steelhead rule speaks, and the region's "1 over 50 cm" does — a big lake rainbow may be kept."""
    from pipeline.deliver.bundle import read as R
    wild = {f"{e}::{r}" for e, r in WILD_RELEASES}
    secs = _sections(db, item_id)
    assert secs, name
    for sid in secs:
        assert not db.execute("SELECT 1 FROM steelhead_water WHERE sid = ?", (sid,)).fetchone()
        for day in ((1, 15), (7, 1)):
            st = {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(sid, day, "ST", str(BUNDLE))
                  if x["state"] == "speaks"}
            rb = R.effective_rules(sid, day, "RB", str(BUNDLE))
            rb_speaks = {f"{x['entry']}::{x['rule']}" for x in rb if x["state"] == "speaks"}
            assert st & wild, (name, day, st)
            assert not {f"{x['entry']}::{x['rule']}" for x in rb} & wild, (name, day)
            assert not any("ST" in (x.get("species") or []) and x.get("take") == 0 for x in rb)
            assert over50 in rb_speaks, (name, day, rb_speaks)
