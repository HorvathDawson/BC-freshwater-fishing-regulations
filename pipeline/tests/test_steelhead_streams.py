"""STEELHEAD RULES BIND STREAMS (user rulings 2026-10-01) — AND HOW SURE WE ARE STEELHEAD ARE THERE.

Every provincial and zone steelhead rule — the wild release, the annual hatchery quota of 10, the
record duty, the Conservation Surcharge Stamp, each zone's hatchery quota, release line and "stop
fishing after the hatchery quota" — reaches only STREAMS of the regions whose OWN tables name
steelhead (1, 2, 3, 5, 6): the province's by `within area:region:N` + `feature_types: [stream]`, each
zone's of its own area. 295c4ac3 extended the wild release to every lake; the third ruling reverted
that. THE EXCEPTION: a lake whose own row names steelhead (Khartoum and Lois lakes, "Rainbow
trout/hatchery steelhead quota = 6 in the aggregate") carries the wild release too — added to the
releases of its region as an extent of its own (`whole`, `item_id`, `feature_types: [lake]`). The
release keeps its competition key (no `water`): keyed apart, the zone's "ALL STEELHEAD" stopped
beating the water rows' trout/char quotas it beats by naming (a measured regression, 0114b4c2).

`steelhead: known | possible` (`pipeline.atlas.reach.steelhead`): known = a stream of a row naming
steelhead (`anadromous_rainbow`) or a tributary stream of one (the build's walk, region-agnostic),
or an own-row steelhead lake; possible = any other stream the provincial steelhead rules bind.
`anadromous_rainbow` (a rainbow over 50 cm IS a steelhead) holds on known streams only.

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
PROVINCE_RULES = ("steelhead.r1", "steelhead.r2", "steelhead.r2b", "steelhead.r4")
#: The wild-steelhead releases (the book prints each under "And you must release:"; p.8 for the
#: province) — the steelhead rules an own-row steelhead lake carries.
WILD_RELEASES = {
    ("zp:steelhead", "steelhead.r2"),                 # "All wild steelhead must be released."
    ("z1:trout_quota", "trout_quota.r5"),             # p.15 "All wild steelhead"
    ("z1:hg_quota", "hg_quota.r6"),                   # p.15 Haida Gwaii, the same line
    ("z2:trout_char_quota", "trout_char_quota.r7"),   # p.23 "All wild steelhead"
    ("z3:trout_char_quota", "trout_char_quota.r5"),   # p.30 "ALL STEELHEAD"
    ("z5:trout_char_quota", "trout_char_quota.r6"),   # p.48 "ALL STEELHEAD"
    ("z6:trout_char_quota", "trout_char_quota.r9"),   # p.55 "all wild steelhead"
    # the TWINS: the same lines, bound to the lakes whose own row names steelhead (Khartoum, Lois)
    ("zp:steelhead", "steelhead.r2b"),
    ("z2:trout_char_quota", "trout_char_quota.r7b"),
}
#: the twins, by the release they repeat
TWINS = {("zp:steelhead", "steelhead.r2b"): "steelhead.r2",
         ("z2:trout_char_quota", "trout_char_quota.r7b"): "trout_char_quota.r7"}
#: Lakes whose own rows print "steelhead" ("Rainbow trout/hatchery steelhead quota = 6 in the
#: aggregate"): the only lakes a steelhead rule may show on — and they carry the wild release.
OWN_STEELHEAD_LAKES = {"r2:khartoum_lake@2-12", "r2:lois_lake@2-12"}
KHARTOUM, LOIS = "wbk:329197063", "wbk:329197058"


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


def own_steelhead_lakes(raw: dict) -> dict:
    """Region -> the lake items of WATER rows with a rule naming steelhead (read from the corpus by
    the item ids the rows match: a lake row's matched items are its lakes)."""
    out: dict = {}
    for eid, e in raw.items():
        if eid.startswith("z") or not any("ST" in (r.get("species") or [])
                                          for r in e.get("rules") or []):
            continue
        if eid in OWN_STEELHEAD_LAKES:
            out.setdefault(str(e["region"]), set()).update(e.get("matched") or [])
    return {k: sorted(v) for k, v in out.items()}


def _lakes_of(raw, regions) -> list:
    lakes = own_steelhead_lakes(raw)
    return [{"op": "whole", "item_id": i, "feature_types": ["lake"]}
            for n in regions for i in lakes.get(n, [])]


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
    wild-release TWIN bound to exactly the own-row steelhead lakes of its area — a release whose
    lakes lack their twin, or a provincial one that reaches a region whose tables do not name
    steelhead."""
    out = []
    regions = qualifying_regions(raw)
    for eid, e in sorted(raw.items()):
        if not eid.startswith("z"):
            continue
        area = regions if eid.startswith("zp:") else (eid.split(":", 1)[0][1:],)
        twins = set()
        for r in e.get("rules") or []:
            about = "ST" in (r.get("species") or []) or (
                not r.get("species") and "steelhead" in r["verbatim"].lower())
            if not about:
                continue
            tag = f"{eid}::{r['rule_id']}"
            exts = list(r.get("extents") or [])
            wild = (eid, r["rule_id"]) in WILD_RELEASES or is_wild_release(r)
            if wild and ((eid, r["rule_id"]) not in WILD_RELEASES or not is_wild_release(r)):
                out.append(f"{tag}: the list of wild releases and the rule disagree")
            if wild and r.get("water") is not None:
                out.append(f"{tag}: a wild release keyed to water {r.get('water')!r}")
            twin = TWINS.get((eid, r["rule_id"]))
            if twin is not None:
                twins.add(twin)
                base = next((x for x in e["rules"] if x["rule_id"] == twin), None)
                same = base is not None and {k: v for k, v in base.items() if k not in (
                    "rule_id", "extents", "review_reason")} == {k: v for k, v in r.items() if k not in (
                        "rule_id", "extents", "review_reason")}
                if not same:
                    out.append(f"{tag}: not the same line as {twin}")
                if not exts or exts != _lakes_of(raw, area):
                    out.append(f"{tag}: lakes {[x.get('item_id') for x in exts]} are not the "
                               f"own-row steelhead lakes of its area")
                continue
            if not exts or any(x.get("feature_types") != ["stream"] for x in exts):
                out.append(f"{tag}: an extent draws more than streams")
            if eid.startswith("zp:") and r.get("water") != ("stream" if not wild else None):
                out.append(f"{tag}: water {r.get('water')!r}")
            if not eid.startswith("zp:") and r.get("water") not in (None, "stream"):
                out.append(f"{tag}: water {r.get('water')!r}")
            if eid.startswith("zp:") and exts != _streams_in(regions):
                out.append(f"{tag}: extents are not the streams of regions {regions}")
        for r in e.get("rules") or []:
            if (eid, r["rule_id"]) in WILD_RELEASES and (eid, r["rule_id"]) not in TWINS \
                    and _lakes_of(raw, area) and r["rule_id"] not in twins:
                out.append(f"{eid}::{r['rule_id']}: the own-row steelhead lakes of its area have "
                           f"no twin of it")
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
                    "steelhead.r2": "daily@origin=wild",            # its key, kept
                    "steelhead.r2b": "daily@origin=wild",           # its twin on the two lakes
                    "steelhead.r4": "daily@origin=hatchery&water=stream&record"}
    # Khartoum and Lois are the only own-row steelhead lakes, both in Region 2
    assert own_steelhead_lakes(raw) == {"2": sorted([KHARTOUM, LOIS])}
    # every listed wild release is in the corpus, and is one
    for eid, rid in WILD_RELEASES:
        assert is_wild_release(next(r for r in raw[eid]["rules"] if r["rule_id"] == rid)), rid


def _r(raw, eid, rid):
    return next(r for r in raw[eid]["rules"] if r["rule_id"] == rid)


_STAVE = {"op": "whole", "item_id": "wbk:329291805", "feature_types": ["lake"]}


@pytest.mark.parametrize("mutate", [
    lambda raw: raw["zp:steelhead"]["rules"][1].update(
        extents=[{"op": "within", "area_kind": "region"}]),     # the release in Regions 4, 7, 8
    lambda raw: raw["zp:steelhead"]["rules"][1].update(water="stream"),   # its key moves
    lambda raw: raw["zp:steelhead"]["rules"][1]["extents"][0].pop("feature_types"),  # R1 lakes
    lambda raw: _r(raw, "z6:trout_char_quota", "trout_char_quota.r9")["extents"][0].pop(
        "feature_types"),                                       # a zone release onto lakes
    lambda raw: _r(raw, "zp:steelhead", "steelhead.r2b")["extents"].append(_STAVE),  # a 3rd lake
    lambda raw: _r(raw, "zp:steelhead", "steelhead.r2b")["extents"].pop(),     # Khartoum loses it
    lambda raw: raw["zp:steelhead"]["rules"].pop(2),                          # no twin at all
    lambda raw: _r(raw, "z2:trout_char_quota", "trout_char_quota.r7b").update(take=1),
    lambda raw: _r(raw, "z2:trout_char_quota", "trout_char_quota.r7")["extents"].append(
        {"op": "whole", "item_id": KHARTOUM, "feature_types": ["lake"]}),  # the release, not a twin
    lambda raw: _r(raw, "z2:trout_char_quota", "trout_char_quota.r3")["extents"].append(
        {"op": "whole", "item_id": KHARTOUM, "feature_types": ["lake"]}),    # hatchery 2 on a lake
    lambda raw: raw["zp:steelhead"]["rules"][0].pop("water"),
    lambda raw: raw["zp:steelhead"]["rules"][3]["extents"].append(
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


#: the provincial steelhead rules every steelhead-region stream carries (not the lakes' twin)
PROVINCE = {f"zp:steelhead::{r}" for r in PROVINCE_RULES if r != "steelhead.r2b"}
STAMP = "zp:steelhead#steelhead_targeting"
WILD = {f"{e}::{r}" for e, r in WILD_RELEASES}
#: every provincial or zone rule about steelhead, by its id
STEELHEAD_RULES = """SELECT entry_id || '::' || rule_id FROM rule WHERE entry_id LIKE 'z%'
    AND (species LIKE '%"ST"%' OR (species = '[]' AND lower(verbatim) LIKE '%steelhead%'))"""


def _steelhead_lakes(db) -> set[str]:
    """Every lake item any section of which carries a provincial or zone steelhead rule."""
    ids = {x for (x,) in db.execute(STEELHEAD_RULES)}
    return {i for i, e, r in db.execute(
        "SELECT DISTINCT i.item_id, r.entry_id, r.rule_id FROM item i JOIN item_section s "
        "ON s.ord = i.ord JOIN section_ruleset sr ON sr.sid = s.sid JOIN ruleset r "
        "ON r.set_id = sr.set_id WHERE i.kind = 'lake' AND r.entry_id LIKE 'z%'")
        if f"{e}::{r}" in ids}


def test_no_lake_carries_a_steelhead_rule_but_khartoum_and_lois(db):
    """No section of any lake the bundle names carries a provincial or zone steelhead rule — the
    wild release included — except Khartoum and Lois lakes; no lake carries the stamp."""
    assert len({x for (x,) in db.execute(STEELHEAD_RULES)} & WILD) == len(WILD)
    assert _steelhead_lakes(db) == {KHARTOUM, LOIS}
    hit = db.execute(
        "SELECT COUNT(DISTINCT i.item_id) FROM item i JOIN item_section s ON s.ord = i.ord "
        "JOIN requirement_section q ON q.sid = s.sid "
        "WHERE i.kind = 'lake' AND q.entry_id = 'zp:steelhead'").fetchone()[0]
    assert hit == 0
    # the stamp is placed on sections, never province-wide (which would hold on every lake)
    (placement,) = db.execute("SELECT placement FROM requirement WHERE entry_id = 'zp:steelhead' "
                              "AND req_id = 'steelhead_targeting'").fetchone()
    assert placement == "sections"


@pytest.mark.parametrize("item_id,own", [(KHARTOUM, "r2:khartoum_lake@2-12::khartoum_lake.r5"),
                                         (LOIS, "r2:lois_lake@2-12::lois_lake.r4")])
def test_khartoum_and_lois_get_the_wild_release(db, item_id, own):
    """Their own rows name steelhead, so they carry the wild release (the province's and Region
    2's "All wild steelhead") beside their own rule — and no other steelhead rule; asked about a
    steelhead, a wild release speaks."""
    from pipeline.deliver.bundle import read as R
    secs = _sections(db, item_id)
    assert secs
    ids = {x for (x,) in db.execute(STEELHEAD_RULES)}
    for sid in secs:
        on = _rules_on(db, sid)
        assert {own, "zp:steelhead::steelhead.r2b",
                "z2:trout_char_quota::trout_char_quota.r7b"} <= on
        assert on & ids == {"zp:steelhead::steelhead.r2b",
                            "z2:trout_char_quota::trout_char_quota.r7b"}, on & ids
        st = {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(sid, (7, 1), "ST", str(BUNDLE))
              if x["state"] == "speaks"}
        assert st & WILD, st
        assert R.steelhead_presence(db, sid) == "known"
        assert not db.execute("SELECT 1 FROM steelhead_water WHERE sid = ?", (sid,)).fetchone()


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


# -------------------------------------------------------------- steelhead: known | possible
THOMPSON = "r3:thompson_river_downstream_of_signs_at_kamloops_lake_outlet_t@3-13+3-14+3-18"


def _presence(db, item_id: str) -> dict:
    from pipeline.deliver.bundle import read as R
    out: dict = {}
    for sid in _sections(db, item_id):
        out.setdefault(R.steelhead_presence(db, sid), []).append(sid)
    return out


def test_the_thompson_below_kamloops_lake_is_known(db):
    """The Thompson row names steelhead (Class II, Steelhead Stamp mandatory) for its own stretch,
    downstream of Kamloops Lake: every section its rules bind is known. Above the lake the river is
    not that row's water, and is "possible" like any other Region 3 stream."""
    rid = next(r for (r,) in db.execute("SELECT rule_id FROM rule WHERE entry_id = ?", (THOMPSON,)))
    got = _presence(db, "gnis:39492")
    below = {s for s in _sections(db, "gnis:39492") if f"{THOMPSON}::{rid}" in _rules_on(db, s)}
    assert below and below <= set(got.get("known", ())), "below Kamloops Lake"
    assert set(got.get("known", ())) == below
    assert set(got) <= {"known", "possible"}


@pytest.mark.parametrize("item_id,name", [("gnis:39400", "Nicola River"),
                                          ("gnis:38393", "Bonaparte River"),
                                          ("gnis:30592", "Deadman River")])
def test_a_tributary_of_the_thompson_is_known(db, item_id, name):
    """A tributary stream joining the Thompson below Kamloops Lake is known — by the walk from the
    row's own stretch, whether or not the row says "including tributaries" — and a big rainbow
    there is a steelhead."""
    got = _presence(db, item_id)
    assert set(got) == {"known"}, (name, {k: len(v) for k, v in got.items()})
    for sid in got["known"]:
        assert db.execute("SELECT 1 FROM steelhead_water WHERE sid = ?", (sid,)).fetchone(), name


@pytest.mark.parametrize("item_id,name", [("gnis:24531", "Horsefly River"),       # Region 5
                                          ("gnis:27764", "Williams Lake River"),  # Region 5
                                          ("gnis:21205", "San Jose River")])      # Region 5
def test_an_interior_cariboo_stream_is_possible(db, item_id, name):
    """A Cariboo stream no row names steelhead on, and no tributary of one: the steelhead rules
    apply, steelhead may not be present — and a big rainbow is a rainbow."""
    got = _presence(db, item_id)
    assert set(got) == {"possible"}, (name, {k: len(v) for k, v in got.items()})
    for sid in got["possible"]:
        assert PROVINCE <= _rules_on(db, sid), name
        assert not db.execute("SELECT 1 FROM steelhead_water WHERE sid = ?", (sid,)).fetchone()


def test_a_region_4_stream_and_a_lake_are_absent(db):
    """The Elk River (Region 4) and Stave Lake (Region 2) carry no steelhead attribute."""
    assert set(_presence(db, "gnis:16880")) == {None}
    assert set(_presence(db, "wbk:329291805")) == {None}


def test_anadromous_rainbow_is_exactly_the_known_streams(db):
    """A big rainbow is a steelhead on every known stream section and nowhere else: never on a
    possible stream, never on a lake (Khartoum and Lois are known, and not steelhead water)."""
    q = lambda sql: {s for (s,) in db.execute(sql)}                       # noqa: E731
    sw = q("SELECT sid FROM steelhead_water")
    known = q("SELECT sid FROM section_steelhead WHERE code = 1")
    possible = q("SELECT sid FROM section_steelhead WHERE code = 2")
    lakes = q("SELECT s.sid FROM item i JOIN item_section s ON s.ord = i.ord WHERE i.kind = 'lake'")
    assert sw and possible and not sw & possible
    assert sw <= known and not sw & lakes
    assert known - sw == known & lakes == set(_sections(db, KHARTOUM) + _sections(db, LOIS))
    # every possible section, and every known stream, carries the provincial steelhead rules
    bad = db.execute(
        "SELECT COUNT(*) FROM section_steelhead h WHERE h.sid NOT IN (SELECT sid FROM "
        "section_ruleset sr JOIN ruleset r ON r.set_id = sr.set_id WHERE "
        "r.entry_id = 'zp:steelhead' AND r.rule_id IN ('steelhead.r1', 'steelhead.r2b'))"
    ).fetchone()[0]
    assert bad == 0


@pytest.mark.parametrize("item_id,name", [("gnis:14707", "Pennask Creek"),     # Region 8
                                          ("gnis:9237", "Brenda Creek")])      # Region 8
def test_the_known_walk_stops_where_the_steelhead_rules_stop(db, item_id, name):
    """The Thompson's tributary walk crosses into Region 8 (the Nicola system's headwaters); no
    steelhead rule applies there, so those creeks are neither known nor steelhead water — a big
    rainbow is a rainbow, and Region 8's "1 over 50 cm" speaks for it."""
    from pipeline.deliver.bundle import read as R
    secs = _sections(db, item_id)
    assert secs, name
    outside = [s for s in secs if not PROVINCE & _rules_on(db, s)]
    assert outside, name
    for sid in outside:
        assert R.steelhead_presence(db, sid) is None, (name, sid)
        assert not db.execute("SELECT 1 FROM steelhead_water WHERE sid = ?", (sid,)).fetchone()
        rb = {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(sid, (7, 1), "RB", str(BUNDLE))
              if x["state"] == "speaks"}
        assert "z8:trout_char_quota::trout_char_quota.r2" in rb, (name, rb)


def test_the_presence_pass_drops_what_the_rules_do_not_bind():
    """Unit: a known section no provincial steelhead rule binds is dropped and reported."""
    from types import SimpleNamespace as NS
    from pipeline.atlas.reach import steelhead as SH
    g = NS(nodes={s: NS(kind=NS(value="stream")) for s in ("a", "b", "c")})
    pr = SH.Presence({}, g)
    pr._put("a", 0, "r3:x", "reach")
    pr._put("b", 1, "r3:x", "trib")
    rows, rep = pr.finish([NS(entry_id=SH.PROVINCE_STEELHEAD, sections=("a", "c"))])
    assert {(r["section_id"], r["steelhead"]) for r in rows} == {("a", "known"), ("c", "possible")}
    assert rep["dropped_outside_steelhead_rules"] == {"r3:x": 1}
    assert rep["known_without_steelhead_rules"] == 0


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


def _bad(doc):
    bad = {k: doc[k] for k in ("rules", "licensing", "entries", "waters", "licensing_sets")}
    bad["rulesets"] = copy.deepcopy(doc["rulesets"])
    return bad


def test_the_export_knows_the_wild_releases(doc):
    """The wild releases (province and zones) are the steelhead rules `is_wild_steelhead_release`
    passes; the hatchery quotas, the annual 10, the record duty, stop-after-quota and the Region 6
    stream closure are not."""
    wild = {i for i, x in doc["rules"].items()
            if X.is_wild_steelhead_release(x) and x["entry_id"].startswith("z")}
    assert wild == WILD
    for i in ("zp:steelhead::steelhead.r1", "zp:steelhead::steelhead.r4",
              "z2:trout_char_quota::trout_char_quota.r3",
              "z6:hatchery_steelhead_stop::hatchery_steelhead_stop.r1",
              "z6:steelhead_stream_closure::steelhead_stream_closure.r1"):
        assert X.is_steelhead_rule(doc["rules"][i]) and not X.is_wild_steelhead_release(
            doc["rules"][i]), i


@pytest.mark.parametrize("rid", ["zp:steelhead::steelhead.r2",                 # the wild release
                                 "z1:trout_quota::trout_quota.r5",             # a zone release
                                 "zp:steelhead::steelhead.r1", "zp:steelhead::steelhead.r4",
                                 "z2:trout_char_quota::trout_char_quota.r3",
                                 "z6:hatchery_steelhead_stop::hatchery_steelhead_stop.r1"])
def test_the_export_check_refuses_a_steelhead_rule_on_a_lake(doc, rid):
    """Mutation: any steelhead rule — a wild release included — put on a lake whose own row does
    not name steelhead is refused."""
    bad = _bad(doc)
    item, part = _lake_part(doc, own=False)
    assert X.steelhead_lake_problems(bad) == []
    bad["rulesets"][part["ruleset"]].setdefault("reach", []).append(rid)
    got = X.steelhead_lake_problems(bad)
    assert any(p.startswith(f"steelhead rule {rid} shows on") and item in p for p in got), got


def test_the_export_check_allows_only_the_lakes_own_row_and_the_release(doc):
    """On Lois Lake: Lois Lake's own steelhead rule and the wild releases; Region 2's "2 hatchery
    steelhead" put there is refused, and so is Lois without a wild release."""
    part = doc["waters"][LOIS]["parts"][0]
    ids = [i for via, v in doc["rulesets"][part["ruleset"]].items() if via != "sections"
           for i in v]
    assert {"r2:lois_lake@2-12::lois_lake.r4", "zp:steelhead::steelhead.r2b",
            "z2:trout_char_quota::trout_char_quota.r7b"} <= set(ids), ids
    bad = _bad(doc)
    bad["rulesets"][part["ruleset"]].setdefault("reach", []).append(
        "z2:trout_char_quota::trout_char_quota.r3")
    assert any("z2:trout_char_quota::trout_char_quota.r3" in p and LOIS in p
               for p in X.steelhead_lake_problems(bad))
    bad = _bad(doc)
    for via, v in bad["rulesets"][part["ruleset"]].items():
        if via != "sections":
            bad["rulesets"][part["ruleset"]][via] = [i for i in v if i not in WILD]
    assert any(p.startswith(f"lake {LOIS} part 0: its own row names steelhead")
               for p in X.steelhead_lake_problems(bad))


def test_the_export_check_refuses_a_province_wide_stamp(doc):
    bad = {k: doc[k] for k in ("rules", "entries", "waters", "rulesets", "licensing_sets")}
    bad["licensing"] = copy.deepcopy(doc["licensing"])
    bad["licensing"][STAMP]["placement"] = "province"
    assert f"steelhead record {STAMP} is placed province-wide — it holds on every lake" \
        in X.steelhead_lake_problems(bad)
    assert json.dumps(doc["licensing"][STAMP]["placement"]) == '"sections"'


def test_the_export_carries_steelhead_per_part_and_water(doc):
    """`waters[].parts[].steelhead` and the water's roll-up: the Thompson known (its lower parts)
    and possible above Kamloops Lake; Nicola known; Horsefly possible; Elk and Stave Lake absent;
    Khartoum known. Every known stream part, and only those, reads a big rainbow as a steelhead."""
    W = doc["waters"]
    assert X.steelhead_presence_problems(doc) == []
    assert W["gnis:39492"]["steelhead"] == "known"
    assert W["gnis:39400"]["steelhead"] == "known"
    assert W["gnis:24531"]["steelhead"] == "possible"
    assert W[KHARTOUM]["steelhead"] == "known" and W[LOIS]["steelhead"] == "known"
    assert W["gnis:39400"]["steelhead_source"] == [THOMPSON]
    assert "steelhead_source" not in W[KHARTOUM] and "steelhead_source" not in W["gnis:24531"]
    assert "steelhead" not in W["gnis:16880"] and "steelhead" not in W["wbk:329291805"]
    for item, w in W.items():
        for p in w["parts"]:
            assert bool(p.get("anadromous_rainbow")) == (
                p.get("steelhead") == "known" and w["kind"] == "stream"), item
    fd = doc["field_dictionary"]["water.parts[]"]
    assert "may not be present" in fd["steelhead"]
    got = doc["about"]["counts"]["sections"]["steelhead"]
    assert got["known"] > 0 and got["possible"] > 0


@pytest.mark.parametrize("mutate,expect", [
    (lambda w: w["gnis:24531"]["parts"][0].update(anadromous_rainbow=True),
     "anadromous_rainbow on a possible stream"),
    (lambda w: w["wbk:329291805"]["parts"][0].update(steelhead="possible"), "a lake marked"),
    (lambda w: w["gnis:39400"].update(steelhead="possible"), "parts say 'known'"),
    (lambda w: w["gnis:39400"]["parts"][0].pop("anadromous_rainbow"),
     "a known stream where a big rainbow is not a steelhead"),
    (lambda w: w["gnis:16880"]["parts"][0].update(steelhead="possible"), "no steelhead rule"),
])
def test_the_presence_check_catches_a_mutation(doc, mutate, expect):
    """Mutation: each way the attribute and the steelhead definition can disagree is refused."""
    bad = dict(doc)
    bad["waters"] = copy.deepcopy(doc["waters"])
    assert X.steelhead_presence_problems(bad) == []
    mutate(bad["waters"])
    got = X.steelhead_presence_problems(bad)
    assert any(expect in p for p in got), got


# ------------------------------------------------------------- steelhead water (p.86)
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


# ------------------------------------------------------------ a lake answers to no steelhead rule
@pytest.mark.parametrize("item_id,name,over50", [
    ("wbk:329385412", "Morice Lake", "z6:trout_char_quota::trout_char_quota.r2"),    # Region 6
    ("wbk:329291805", "Stave Lake", "z2:trout_char_quota::trout_char_quota.r2"),     # Region 2
    ("wbk:329400588", "Murtle Lake", "z3:trout_char_quota::trout_char_quota.r3"),    # Region 3
])
def test_a_lake_answers_to_no_steelhead_rule(db, item_id, name, over50):
    """A lake whose own row does not name steelhead carries no steelhead rule (295c4ac3's
    every-lake wild release reverted): asked about a steelhead or a rainbow, no steelhead rule is
    in play, and the region's "1 over 50 cm" speaks for a big rainbow."""
    from pipeline.deliver.bundle import read as R
    secs = _sections(db, item_id)
    assert secs, name
    ids = {x for (x,) in db.execute(STEELHEAD_RULES)}
    for sid in secs:
        assert not db.execute("SELECT 1 FROM steelhead_water WHERE sid = ?", (sid,)).fetchone()
        assert R.steelhead_presence(db, sid) is None
        assert not _rules_on(db, sid) & ids, name
        for day in ((1, 15), (7, 1)):
            st = {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(sid, day, "ST", str(BUNDLE))}
            rb = {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(sid, day, "RB", str(BUNDLE))
                  if x["state"] == "speaks"}
            assert not st & ids, (name, day, st & ids)
            assert over50 in rb, (name, day, rb)
