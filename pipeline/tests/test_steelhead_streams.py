"""STEELHEAD RULES BIND STREAMS (user rulings 2026-10-01) — AND HOW SURE WE ARE STEELHEAD ARE THERE.

Every provincial and zone steelhead rule — the wild release, the annual hatchery quota of 10, the
record duty, the Conservation Surcharge Stamp, each zone's hatchery quota, release line and "stop
fishing after the hatchery quota" — reaches only STREAMS of the regions whose OWN tables name
steelhead (1, 2, 3, 5, 6): the province's by `within area:region:N` + `feature_types: [stream]`, each
zone's of its own area. 295c4ac3 extended the wild release to every lake; the third ruling reverted
that. THE EXCEPTION: a lake whose own row names steelhead (Khartoum and Lois lakes, "Rainbow
trout/hatchery steelhead quota = 6 in the aggregate") carries the WHOLE provincial steelhead set
(user ask 2026-10-02) — the annual hatchery 10, the wild release, the record duty and the stamp —
and its region's wild release, through lake COPIES (`steelhead.r1b`/`r2b`/`r4b`,
`trout_char_quota.r7b`, licensing `steelhead_targeting_lakes`: `whole`, `item_id`,
`feature_types: [lake]`, no `water`), so the stream rules keep their extents and their competition
keys (keyed apart, the zone's "ALL STEELHEAD" stopped beating the water rows' trout/char quotas it
beats by naming — a measured regression, 0114b4c2). Every water whose own row names steelhead
carries the set wherever the row's steelhead lines bind in Regions 1, 2, 3, 5 and 6, and no other
place does (`province_set_problems`, over the whole bundle).

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
PROVINCE_RULES = ("steelhead.r1", "steelhead.r1b", "steelhead.r2", "steelhead.r2b",
                  "steelhead.r4", "steelhead.r4b")
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
#: the twins, by the rule they repeat — the wild releases (user ruling 2026-10-01), and the rest of
#: the provincial set: the annual hatchery 10 and its record duty (user ask 2026-10-02). A twin of a
#: rule keyed `water: stream` drops the key (it is on a lake).
TWINS = {("zp:steelhead", "steelhead.r2b"): "steelhead.r2",
         ("z2:trout_char_quota", "trout_char_quota.r7b"): "trout_char_quota.r7",
         ("zp:steelhead", "steelhead.r1b"): "steelhead.r1",
         ("zp:steelhead", "steelhead.r4b"): "steelhead.r4"}
#: the stamp's twin, bound to the own-row steelhead lakes
STAMP_TWIN = "steelhead_targeting_lakes"
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
                    "rule_id", "extents", "review_reason", "water")} == {
                        k: v for k, v in r.items() if k not in (
                            "rule_id", "extents", "review_reason", "water")}
                if not same:
                    out.append(f"{tag}: not the same line as {twin}")
                if not exts or exts != _lakes_of(raw, area):
                    out.append(f"{tag}: lakes {[x.get('item_id') for x in exts]} are not the "
                               f"own-row steelhead lakes of its area")
                if r.get("water") is not None:
                    out.append(f"{tag}: a lake twin keyed to water {r.get('water')!r}")
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
        stamps = set()
        for x in e.get("licensing") or []:
            if "ST" not in ((x.get("doing") or {}).get("species") or []):
                continue
            stamps.add(x["id"])
            if x["id"] == STAMP_TWIN:
                base = next((y for y in e["licensing"] if y["id"] == "steelhead_targeting"), None)
                strip = lambda y: {k: v for k, v in (y or {}).items()          # noqa: E731
                                   if k not in ("id", "extents", "review_reason", "water")}
                if strip(base) != strip(x) or x.get("water") is not None \
                        or x.get("extents") != _lakes_of(raw, area):
                    out.append(f"{eid}#{x['id']}: not the stamp bound to the own-row steelhead "
                               f"lakes")
                continue
            want = _streams_in(regions, outside_area_kind="national_parks")
            if x.get("water") != "stream" or x.get("extents") != want:
                out.append(f"{eid}#{x['id']}: not the streams of regions {regions}")
        if eid.startswith("zp:") and stamps and _lakes_of(raw, area) and STAMP_TWIN not in stamps:
            out.append(f"{eid}: the own-row steelhead lakes have no stamp")
        if eid.startswith("zp:") and _lakes_of(raw, area):
            ids = {r["rule_id"] for r in e.get("rules") or []}
            for (te, tr), base in TWINS.items():
                if te == eid and base in ids and tr not in ids:
                    out.append(f"{eid}::{base}: the own-row steelhead lakes have no twin of it")
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
                    "steelhead.r1b": "annual@origin=hatchery",      # its twin on the two lakes
                    "steelhead.r2": "daily@origin=wild",            # its key, kept
                    "steelhead.r2b": "daily@origin=wild",           # its twin on the two lakes
                    "steelhead.r4": "daily@origin=hatchery&water=stream&record",
                    "steelhead.r4b": "daily@origin=hatchery&record"}
    assert [x["id"] for x in e["licensing"]] == ["steelhead_targeting", STAMP_TWIN]
    # Khartoum and Lois are the only own-row steelhead lakes, both in Region 2
    assert own_steelhead_lakes(raw) == {"2": sorted([KHARTOUM, LOIS])}
    # every listed wild release is in the corpus, and is one
    for eid, rid in WILD_RELEASES:
        assert is_wild_release(next(r for r in raw[eid]["rules"] if r["rule_id"] == rid)), rid


def _r(raw, eid, rid):
    return next(r for r in raw[eid]["rules"] if r["rule_id"] == rid)


_STAVE = {"op": "whole", "item_id": "wbk:329291805", "feature_types": ["lake"]}


@pytest.mark.parametrize("mutate", [
    lambda raw: _r(raw, "zp:steelhead", "steelhead.r2").update(
        extents=[{"op": "within", "area_kind": "region"}]),     # the release in Regions 4, 7, 8
    lambda raw: _r(raw, "zp:steelhead", "steelhead.r2").update(water="stream"),   # its key moves
    lambda raw: _r(raw, "zp:steelhead", "steelhead.r2")["extents"][0].pop("feature_types"),
    lambda raw: _r(raw, "z6:trout_char_quota", "trout_char_quota.r9")["extents"][0].pop(
        "feature_types"),                                       # a zone release onto lakes
    lambda raw: _r(raw, "zp:steelhead", "steelhead.r2b")["extents"].append(_STAVE),  # a 3rd lake
    lambda raw: _r(raw, "zp:steelhead", "steelhead.r2b")["extents"].pop(),     # Khartoum loses it
    lambda raw: raw["zp:steelhead"].update(rules=[r for r in raw["zp:steelhead"]["rules"]
                                                  if r["rule_id"] != "steelhead.r2b"]),  # no twin
    lambda raw: raw["zp:steelhead"].update(rules=[r for r in raw["zp:steelhead"]["rules"]
                                                  if r["rule_id"] != "steelhead.r1b"]),  # no 10
    lambda raw: raw["zp:steelhead"].update(rules=[r for r in raw["zp:steelhead"]["rules"]
                                                  if r["rule_id"] != "steelhead.r4b"]),  # no duty
    lambda raw: raw["zp:steelhead"].update(licensing=[x for x in raw["zp:steelhead"]["licensing"]
                                                      if x["id"] != STAMP_TWIN]),  # no lake stamp
    lambda raw: _r(raw, "zp:steelhead", "steelhead.r1b").update(take=6),     # not the same line
    lambda raw: _r(raw, "zp:steelhead", "steelhead.r4b").update(water="stream"),  # "in streams"
    lambda raw: _r(raw, "zp:steelhead", "steelhead.r1b")["extents"].append(_STAVE),  # a 3rd lake
    lambda raw: raw["zp:steelhead"]["licensing"][1]["extents"].append(_STAVE),  # stamp on Stave
    lambda raw: _r(raw, "z2:trout_char_quota", "trout_char_quota.r7b").update(take=1),
    lambda raw: _r(raw, "z2:trout_char_quota", "trout_char_quota.r7")["extents"].append(
        {"op": "whole", "item_id": KHARTOUM, "feature_types": ["lake"]}),  # the release, not a twin
    lambda raw: _r(raw, "z2:trout_char_quota", "trout_char_quota.r3")["extents"].append(
        {"op": "whole", "item_id": KHARTOUM, "feature_types": ["lake"]}),    # hatchery 2 on a lake
    lambda raw: _r(raw, "zp:steelhead", "steelhead.r1").pop("water"),
    lambda raw: _r(raw, "zp:steelhead", "steelhead.r4")["extents"].append(
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


#: the provincial steelhead rules every steelhead-region stream carries (not the lakes' twins)
PROVINCE = {f"zp:steelhead::{r}" for r in PROVINCE_RULES if not r.endswith("b")}
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
    wild release included — or the stamp, except Khartoum and Lois lakes."""
    assert len({x for (x,) in db.execute(STEELHEAD_RULES)} & WILD) == len(WILD)
    assert _steelhead_lakes(db) == {KHARTOUM, LOIS}
    hit = {i for (i,) in db.execute(
        "SELECT DISTINCT i.item_id FROM item i JOIN item_section s ON s.ord = i.ord "
        "JOIN requirement_section q ON q.sid = s.sid "
        "WHERE i.kind = 'lake' AND q.entry_id = 'zp:steelhead'")}
    assert hit == {KHARTOUM, LOIS}
    # the stamp is placed on sections, never province-wide (which would hold on every lake)
    (placement,) = db.execute("SELECT placement FROM requirement WHERE entry_id = 'zp:steelhead' "
                              "AND req_id = 'steelhead_targeting'").fetchone()
    assert placement == "sections"


#: the lake copies of the provincial set, and Region 2's release
LAKE_SET = {"zp:steelhead::steelhead.r1b", "zp:steelhead::steelhead.r2b",
            "zp:steelhead::steelhead.r4b", "z2:trout_char_quota::trout_char_quota.r7b"}


@pytest.mark.parametrize("item_id,own", [(KHARTOUM, "r2:khartoum_lake@2-12::khartoum_lake.r5"),
                                         (LOIS, "r2:lois_lake@2-12::lois_lake.r4")])
def test_khartoum_and_lois_get_the_provincial_set(db, item_id, own):
    """Their own rows name steelhead, so they carry the whole provincial steelhead set — the annual
    hatchery 10, the wild release, the record duty (the lake copies r1b, r2b, r4b) and the stamp
    (`steelhead_targeting_lakes`) — and Region 2's "All wild steelhead", beside their own rule, and
    no other steelhead rule (not Region 2's "2 hatchery steelhead", a stream rule); asked about a
    steelhead, a wild release and the annual 10 speak."""
    from pipeline.deliver.bundle import read as R
    secs = _sections(db, item_id)
    assert secs
    ids = {x for (x,) in db.execute(STEELHEAD_RULES)}
    for sid in secs:
        on = _rules_on(db, sid)
        assert {own} | LAKE_SET <= on
        assert on & ids == LAKE_SET, on & ids
        assert _requirements_on(db, sid) & {STAMP, f"zp:steelhead#{STAMP_TWIN}"} == {
            f"zp:steelhead#{STAMP_TWIN}"}
        st = {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(sid, (7, 1), "ST", str(BUNDLE))
              if x["state"] == "speaks"}
        assert st & WILD, st
        assert "zp:steelhead::steelhead.r1b" in st, st
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


# ================================================================================================
# EVERY STEELHEAD ROW'S WATER CARRIES THE PROVINCIAL STEELHEAD SET — AND NOTHING ELSE DOES
# (user ask 2026-10-02: "make sure all ones with steelhead rules get the steelhead provincial rules")
# ================================================================================================
#
# Over the WHOLE corpus and the WHOLE bundle, by class of sections (one class per (rule set,
# licensing set, lake item, steelhead code) the bundle's sections carry together):
#
#   FORWARD  every section a WATER row's steelhead line binds (a rule naming `ST`, or naming no
#            fish and printing "steelhead") carries the four members of the provincial set — the
#            annual hatchery 10, the wild release, the record duty, the stamp — unless the section
#            is outside the steelhead regions (it carries no zone table of 1, 2, 3, 5 or 6).
#   REVERSE  a section carrying any member of the set is in a steelhead region, is no lake but an
#            own-row steelhead lake, and is marked known | possible (the presence pass, which reads
#            the GRAPH's kind: a possible section is a stream).
#
# The members are read from the corpus by what each line SAYS (`province_member`), never by rule
# id, so the lake copies count as the members they copy. The rows are read from the corpus too.
# Tenas Lake (Atnarko/Bella Coola row) is bound by that row's spring closure ("upstream of
# Tweedsmuir Park plus Tenas Lake") and NOT by its steelhead line ("No Fishing for steelhead"), so
# it is not a steelhead row's water — pinned below.

STEELHEAD_ZONES = {f"z{n}" for n in QUALIFYING}
TENAS = "wbk:329021804"
ATNARKO = "r5:atnarko_bella_coola_rivers_includes_tributaries_except_burnt@5-11+5-6+5-8"
SET_MEMBERS = ("annual hatchery quota", "wild release", "record duty", "stamp")


def province_member(r: dict, licensing: bool = False):
    """Which member of the provincial steelhead set a `zp:steelhead` line is (catalogue shape)."""
    if licensing:
        return "stamp" if "ST" in ((r.get("doing") or {}).get("species") or []) else None
    if r.get("species") != ["ST"]:
        return None
    if r.get("period") == "annual" and r.get("origin") == "hatchery" and (r.get("take") or 0) > 0:
        return "annual hatchery quota"
    if is_wild_release(r):
        return "wild release"
    if r.get("record_retention"):
        return "record duty"
    return None


def members_of(raw: dict) -> dict:
    """id (`e::r` / `e#id`) -> member, for every line of the provincial steelhead entry."""
    e = raw["zp:steelhead"]
    out = {f"zp:steelhead::{r['rule_id']}": province_member(r) for r in e["rules"]}
    out |= {f"zp:steelhead#{x['id']}": province_member(x, True) for x in e.get("licensing") or []}
    return {k: v for k, v in out.items() if v}


def steelhead_row_rules(raw: dict) -> set[str]:
    """Every rule of a WATER row that names steelhead."""
    return {f"{eid}::{r['rule_id']}" for eid, e in raw.items() if not eid.startswith("z")
            for r in e.get("rules") or []
            if "ST" in (r.get("species") or [])
            or (not r.get("species") and "steelhead" in str(r.get("verbatim") or "").lower())}


@pytest.fixture(scope="module")
def classes(db):
    """(rule set, licensing set, lake item or None, steelhead code or None) -> sections."""
    rows = db.execute(
        "SELECT rs, ls, lake, st, COUNT(*) FROM ("
        " SELECT sr.sid, sr.set_id AS rs, sl.set_id AS ls,"
        "  (SELECT i.item_id FROM item_section s JOIN item i ON i.ord = s.ord"
        "   WHERE s.sid = sr.sid AND i.kind = 'lake' ORDER BY i.item_id LIMIT 1) AS lake,"
        "  CASE WHEN EXISTS (SELECT 1 FROM steelhead_known k WHERE k.sid = sr.sid) THEN 1"
        "   ELSE (SELECT h.code FROM steelhead_set h WHERE h.set_id = sr.set_id) END AS st"
        " FROM section_ruleset sr LEFT JOIN section_licensing sl ON sl.sid = sr.sid)"
        " GROUP BY 1, 2, 3, 4").fetchall()
    return {(rs, ls, lake, st): n for rs, ls, lake, st, n in rows}


@pytest.fixture(scope="module")
def sets(db):
    rules: dict = {}
    for s, e, r in db.execute("SELECT set_id, entry_id, rule_id FROM ruleset"):
        rules.setdefault(s, set()).add(f"{e}::{r}")
    recs: dict = {}
    for s, e, r in db.execute("SELECT set_id, entry_id, record_id FROM licensing_set"):
        recs.setdefault(s, set()).add(f"{e}#{r}")
    return rules, recs


def province_set_problems(classes: dict, rules: dict, recs: dict, members: dict,
                          rows: set, own_lakes: set) -> list[str]:
    """FORWARD and REVERSE (above), one line per offending class."""
    out = []
    for (rs, ls, lake, st), n in sorted(classes.items(), key=lambda kv: str(kv[0])):
        on = rules.get(rs, set())
        held = on | recs.get(ls, set())
        have = {members[i] for i in held if i in members}
        region = any(i.split(":", 1)[0] in STEELHEAD_ZONES for i in on)
        tag = f"set {rs}/{ls} lake {lake} ({n} sections)"
        named = sorted(on & rows)
        if named and region:
            gone = [m for m in SET_MEMBERS if m not in have]
            if gone:
                out.append(f"{tag}: {named[0]} names steelhead, but the provincial "
                           f"{', '.join(gone)} is missing")
        if have and not region:
            out.append(f"{tag}: provincial steelhead outside the steelhead regions")
        if have and lake is not None and lake not in own_lakes:
            out.append(f"{tag}: provincial steelhead on a lake whose own row does not name it")
        if have and st is None:
            out.append(f"{tag}: provincial steelhead on a section that is no steelhead stream "
                       f"or own-row lake (no known | possible)")
    return out


def _inputs(raw, classes, sets):
    rules, recs = sets
    own = {i for v in own_steelhead_lakes(raw).values() for i in v}
    return classes, rules, recs, members_of(raw), steelhead_row_rules(raw), own


def test_the_provincial_set_is_four_members_and_the_rows_are_read_from_the_corpus(raw):
    m = members_of(raw)
    assert sorted(set(m.values())) == sorted(SET_MEMBERS)
    assert m == {"zp:steelhead::steelhead.r1": "annual hatchery quota",
                 "zp:steelhead::steelhead.r1b": "annual hatchery quota",
                 "zp:steelhead::steelhead.r2": "wild release",
                 "zp:steelhead::steelhead.r2b": "wild release",
                 "zp:steelhead::steelhead.r4": "record duty",
                 "zp:steelhead::steelhead.r4b": "record duty",
                 "zp:steelhead#steelhead_targeting": "stamp",
                 f"zp:steelhead#{STAMP_TWIN}": "stamp"}
    rows = {r.split("::")[0] for r in steelhead_row_rules(raw)}
    assert {"r2:khartoum_lake@2-12", "r2:lois_lake@2-12", "r2:capilano_river@2-8",
            ATNARKO} <= rows
    assert all(raw[e]["region"] in QUALIFYING for e in rows), sorted(rows)


def test_every_steelhead_row_s_water_carries_the_provincial_set(raw, classes, sets):
    """FORWARD and REVERSE over every section of the bundle."""
    assert province_set_problems(*_inputs(raw, classes, sets)) == []


def test_tenas_lake_is_bound_by_atnarko_s_closure_not_its_steelhead_line(db, raw):
    """The one lake a steelhead row binds by another line: the Atnarko row's "No Fishing upstream
    of Tweedsmuir Provincial Park plus Tenas Lake, Apr 1-June 30" (every game fish). Its steelhead
    line ("No Fishing for steelhead") does not bind the lake, so the lake is not a steelhead row's
    water: no steelhead rule, no stamp, no steelhead attribute."""
    from pipeline.deliver.bundle import read as R
    secs = _sections(db, TENAS)
    assert secs
    st_rows = steelhead_row_rules(raw)
    for sid in secs:
        on = _rules_on(db, sid)
        assert f"{ATNARKO}::atnarko_bella_coola_rivers.r1" in on
        assert not on & st_rows and not any(i.startswith("zp:steelhead") for i in on)
        assert not any(i.startswith("zp:steelhead") for i in _requirements_on(db, sid))
        assert R.steelhead_presence(db, sid) is None


def test_no_row_outside_the_steelhead_regions_has_a_steelhead_rule(raw):
    """A row of Region 4, 7A, 7B or 8 that names steelhead would need deciding from the book. The
    only one that prints the word is the Stellako's Zone 7A row (and its Region 6 twin), and it
    prints a STAMP WAIVER on its Class II designation — "(Steelhead Stamp not required)" — not a
    steelhead rule: nothing to add there (7A's tables name no steelhead)."""
    outside = sorted(eid for eid, e in raw.items() if not eid.startswith("z")
                     and str(e.get("region")) not in QUALIFYING
                     and "steelhead" in str(e.get("regs_verbatim") or "").lower())
    assert outside == ["r7:stellako_river@7-12"]
    e = raw["r7:stellako_river@7-12"]
    assert "Steelhead Stamp not required" in e["regs_verbatim"]
    assert not any("ST" in (r.get("species") or []) or "steelhead" in r["verbatim"].lower()
                   for r in e.get("rules") or [])
    assert not {r for r in steelhead_row_rules(raw) if r.split("::")[0] in outside}


def _class_of(classes, sets, test):
    rules, recs = sets
    return next(k for k in sorted(classes, key=str) if test(k, rules.get(k[0], set()),
                                                            recs.get(k[1], set())))


def _mutated(raw, classes, sets, fn):
    cl, rules, recs, members, rows, own = _inputs(raw, classes, sets)
    cl, rules, recs = dict(cl), {k: set(v) for k, v in rules.items()}, \
        {k: set(v) for k, v in recs.items()}
    rows = set(rows)
    fn(cl, rules, recs, rows)
    return province_set_problems(cl, rules, recs, members, rows, own)


@pytest.mark.parametrize("name", [
    "khartoum loses the annual 10", "lois loses the lake stamp", "capilano loses the record duty",
    "a steelhead stream loses the stamp", "stave lake gains the annual 10",
    "a region 4 stream gains the wild release", "a steelhead section with no presence code",
])
def test_the_set_check_catches_a_mutation(raw, classes, sets, name):
    """Mutation: each way a steelhead row's water can miss the set, or the set can reach a place it
    must not, turns the check red."""
    rules, recs = sets
    assert province_set_problems(*_inputs(raw, classes, sets)) == []

    def drop(rule, lake):
        def fn(cl, rules, recs, rows):
            k = _class_of(classes, sets, lambda k, r, q: k[2] == lake)
            (rules if "::" in rule else recs)[k[0] if "::" in rule else k[1]].discard(rule)
        return fn

    def capilano(cl, rules, recs, rows):
        k = _class_of(classes, sets, lambda k, r, q: any(
            x.startswith("r2:capilano_river@2-8::") for x in r & rows))
        rules[k[0]].discard("zp:steelhead::steelhead.r4")

    def stream_stamp(cl, rules, recs, rows):
        k = _class_of(classes, sets, lambda k, r, q: k[2] is None and r & rows
                      and STAMP in q)
        recs[k[1]].discard(STAMP)

    def stave(cl, rules, recs, rows):
        k = _class_of(classes, sets, lambda k, r, q: k[2] == "wbk:329291805")
        rules[k[0]].add("zp:steelhead::steelhead.r1")

    def region4(cl, rules, recs, rows):
        k = _class_of(classes, sets, lambda k, r, q: k[2] is None and any(
            x.startswith("z4:") for x in r) and not any(x.split(":", 1)[0] in STEELHEAD_ZONES
                                                       for x in r))
        rules[k[0]].add("zp:steelhead::steelhead.r2")

    def no_code(cl, rules, recs, rows):
        k = _class_of(classes, sets, lambda k, r, q: k[3] == 2)
        cl[(k[0], k[1], k[2], None)] = cl.pop(k)

    fns = {"khartoum loses the annual 10": drop("zp:steelhead::steelhead.r1b", KHARTOUM),
           "lois loses the lake stamp": drop(f"zp:steelhead#{STAMP_TWIN}", LOIS),
           "capilano loses the record duty": capilano,
           "a steelhead stream loses the stamp": stream_stamp,
           "stave lake gains the annual 10": stave,
           "a region 4 stream gains the wild release": region4,
           "a steelhead section with no presence code": no_code}
    assert _mutated(raw, classes, sets, fns[name])


def test_a_row_outside_the_regions_naming_steelhead_needs_no_set(raw, classes, sets):
    """The exemption, pinned: a Region 4 stream whose row named steelhead would carry no
    provincial set (the province's steelhead rules bind 1, 2, 3, 5 and 6) and is not reported —
    that row is for a person to decide from the book (`test_no_row_outside_…`)."""
    def fn(cl, rules, recs, rows):
        k = _class_of(classes, sets, lambda k, r, q: any(x.startswith("r4:") for x in r))
        rows |= {x for x in rules[k[0]] if x.startswith("r4:")}
    assert _mutated(raw, classes, sets, fn) == []


# ----------------------------------------------------------------------- the same, on the export
def _export_bad(doc):
    bad = {k: doc[k] for k in ("rules", "licensing", "entries", "waters")}
    bad["rulesets"] = copy.deepcopy(doc["rulesets"])
    bad["licensing_sets"] = copy.deepcopy(doc["licensing_sets"])
    return bad


def test_the_export_carries_the_provincial_set_on_every_steelhead_row_s_water(doc):
    assert X.steelhead_set_problems(doc) == []
    for item in (KHARTOUM, LOIS):
        p = doc["waters"][item]["parts"][0]
        rules = X._members(doc["rulesets"], p["ruleset"], doc["rules"])
        recs = X._members(doc["licensing_sets"], p["licensing_set"], doc["licensing"])
        assert X.province_set_missing(rules, recs, doc["rules"], doc["licensing"]) == []
        assert f"zp:steelhead#{STAMP_TWIN}" in recs


@pytest.mark.parametrize("item,where,drop,expect", [
    (LOIS, "ruleset", "zp:steelhead::steelhead.r1b", "annual hatchery quota"),
    (KHARTOUM, "licensing_set", f"zp:steelhead#{STAMP_TWIN}", "stamp"),
    ("gnis:5922", "licensing_set", STAMP, "stamp"),                       # Capilano River
    ("gnis:5922", "ruleset", "zp:steelhead::steelhead.r4", "record duty"),
])
def test_the_export_set_check_catches_a_missing_member(doc, item, where, drop, expect):
    bad = _export_bad(doc)
    part = next(p for p in bad["waters"][item]["parts"] if p[where] and any(
        drop in v for v in bad["rulesets" if where == "ruleset" else "licensing_sets"][
            p[where]].values() if isinstance(v, list)))
    table = bad["rulesets" if where == "ruleset" else "licensing_sets"][part[where]]
    for via, v in table.items():
        if isinstance(v, list):
            table[via] = [i for i in v if i != drop]
    got = X.steelhead_set_problems(bad) + X.steelhead_lake_problems(bad)
    assert any(item in g and expect in g for g in got), got


@pytest.mark.parametrize("item,where,add,expect", [
    ("gnis:16880", "ruleset", "zp:steelhead::steelhead.r1", "outside the steelhead regions"),
    ("wbk:329291805", "licensing_set", STAMP, "whose own row does not"),        # Stave Lake
])
def test_the_export_set_check_catches_the_set_where_it_must_not_be(doc, item, where, add, expect):
    bad = _export_bad(doc)
    bad["waters"] = dict(bad["waters"], **{item: copy.deepcopy(doc["waters"][item])})
    part = next(p for p in bad["waters"][item]["parts"] if p["ruleset"])
    tables = bad["rulesets" if where == "ruleset" else "licensing_sets"]
    if part[where] is None:                  # a lake with no licensing set: give it one
        part[where] = "mutant"
        tables["mutant"] = {"sections": part["sections"]}
    tables[part[where]].setdefault("reach", []).append(add)
    got = X.steelhead_set_problems(bad) + X.steelhead_lake_problems(bad)
    assert any(item in g and expect in g for g in got) or any(
        add in g and item in g for g in got), got


def test_the_guide_has_a_sample_water_for_each_steelhead_kind(doc):
    """`guide.cases`: a known steelhead stream (a row's own water), a possible one, a lake of a
    steelhead region with no steelhead rule, and Khartoum or Lois — each with its `steelhead` and
    a reference answer that shows (or does not show) the provincial steelhead rules."""
    got = {c["mechanism"]: c for c in doc["guide"]["cases"]["cases"]}
    k, p = got["steelhead_known"], got["steelhead_possible"]
    lake, own = got["lake_no_steelhead"], got["own_row_steelhead_lake"]
    assert (k["steelhead"], k["water"]["kind"], k.get("anadromous_rainbow")) == (
        "known", "stream", True)
    assert set(doc["waters"][k["water"]["item_id"]]["entries"]) & set(
        doc["waters"][k["water"]["item_id"]]["steelhead_source"])
    assert (p["steelhead"], p["water"]["kind"], p.get("anadromous_rainbow")) == (
        "possible", "stream", None)
    assert lake["water"]["kind"] == "lake" and "steelhead" not in lake
    assert own["water"]["item_id"] in (KHARTOUM, LOIS) and own["steelhead"] == "known"
    for c in (k, p, own):
        speaks = {a["id"] for a in c["expect"] if a["state"] == "speaks"}
        assert any(i.startswith("zp:steelhead::") for i in speaks), c["mechanism"]
        assert any(i.startswith("zp:steelhead#") for i in c["expect_licensing"]), c["mechanism"]
    assert not any(X.is_steelhead_rule(doc["rules"][a["id"]]) for a in lake["expect"])
    assert not any(i.startswith("zp:steelhead#") for i in lake["expect_licensing"])
    assert "zp:steelhead::steelhead.r1b" in {a["id"] for a in own["expect"]}
    for m in ("steelhead_known", "steelhead_possible", "lake_no_steelhead",
              "own_row_steelhead_lake"):
        assert got[m]["what_to_show"] and m in doc["guide"]["cases"]["mechanisms"]
