"""STEELHEAD RULES BIND STREAMS (user rulings 2026-10-01) — AND EVERY WATER A STEELHEAD ROW BINDS,
WHEREVER IT IS (2026-10-02) — AND HOW SURE WE ARE STEELHEAD ARE THERE.

Every provincial and zone steelhead rule — the wild release, the annual hatchery quota of 10, the
record duty, the Conservation Surcharge Stamp, each zone's hatchery quota, release line and "stop
fishing after the hatchery quota" — reaches the STREAMS of the regions whose OWN tables name
steelhead (1, 2, 3, 5, 6): the province's by `within area:region:N` + `feature_types: [stream]`, each
zone's of its own area. 295c4ac3 extended the wild release to every lake; the third ruling reverted
that. Beyond those streams, EVERY WATER A RULE OF A STEELHEAD ROW BINDS carries the WHOLE provincial
set (user rulings 2026-10-02, revised): a STEELHEAD ROW names steelhead in a rule, is flagged
`anadromous_rainbow`, or speaks of the Steelhead Stamp (its waiver included: Chilko, Horsefly, West
Road, the Stellako) — Tenas Lake by the Atnarko's spring closure, Khartoum and Lois, the Vedder Canal,
the Stellako in Zone 7A. NO TRIBUTARY WALK. It reaches them through the TWINS `steelhead.r1b`/`r2b`/
`r4b` and the stamp's `steelhead_targeting_known` (`{op: steelhead_waters, siblings: [<base>]}`: the
book-known set minus the base's sections), so the stream rules keep their extents and competition
keys; a book-known LAKE also gets its zone's wild release through that line's twin `<release>b`
(`area_id`: the zone's own area) — Tenas Region 5's "ALL STEELHEAD", Khartoum and Lois Region 2's.

`steelhead: known | possible` (`pipeline.atlas.reach.steelhead`): known = a steelhead row's water, or
a water on the CURATED LIST — a presence indicator that binds no rule or stamp (the Okanagan River
in Region 8 is known and carries no steelhead rule); possible = any other stream the provincial
steelhead rules bind. `anadromous_rainbow` (a rainbow over 50 cm IS a steelhead) holds on EVERY
known STREAM section (the registry's water kind: a slough or canal is a stream), book or list
(user ruling 2026-10-03) — the list's one effect on an answer.

Corpus tests read the catalogue; placement tests read a bundle (`UI_EXPORT_BUNDLE`, else the
shipped one) — the OUTPUT, never the extents that produced it. Each check is pinned by a
mutation that must turn it red. The list and the twins' mechanism are pinned on a toy corpus in
`test_steelhead_waters.py`.
"""
from __future__ import annotations

import copy
import json
import os
import sqlite3
from collections import defaultdict
from pathlib import Path

import pytest

from pipeline.regs.parsing import catalogue as C
from pipeline.tools import export_ui_rules as X
from pipeline.tests.conftest import need, BUNDLE_HINT

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or X.BUNDLE)

#: The regions whose own tables name steelhead, from the book's regional pages: 1 (p.13 "All
#: wild steelhead", "2 hatchery steelhead over 50 cm"), 2 (p.21, the same), 3 (p.28 "ALL
#: STEELHEAD"), 5 (p.42 "ALL STEELHEAD"), 6 (p.49 "all wild steelhead", the May 15-June 15
#: stream closure). 7A's one mention is the Stellako's "(Steelhead Stamp not required)" — a stamp
#: waiver, no steelhead RULE (its row is a steelhead row, so its water carries the provincial set
#: through the twins; its zone gains no steelhead line); 4, 7B and 8 print none.
QUALIFYING = ("1", "2", "3", "5", "6")
PROVINCE_RULES = ("steelhead.r1", "steelhead.r1b", "steelhead.r2", "steelhead.r2b",
                  "steelhead.r4", "steelhead.r4b")
#: The wild-steelhead releases (the book prints each under "And you must release:"; p.6 for the
#: province) — the steelhead rules an own-row steelhead lake carries.
WILD_RELEASES = {
    ("zp:steelhead", "steelhead.r2"),                 # "All wild steelhead must be released."
    ("z1:trout_quota", "trout_quota.r5"),             # p.13 "All wild steelhead"
    ("z1:hg_quota", "hg_quota.r6"),                   # p.13 Haida Gwaii, the same line
    ("z2:trout_char_quota", "trout_char_quota.r7"),   # p.21 "All wild steelhead"
    ("z3:trout_char_quota", "trout_char_quota.r5"),   # p.28 "ALL STEELHEAD"
    ("z5:trout_char_quota", "trout_char_quota.r6"),   # p.42 "ALL STEELHEAD"
    ("z6:trout_char_quota", "trout_char_quota.r9"),   # p.49 "all wild steelhead"
    # the TWINS: the same lines, bound to the book-known waters their base does not bind
    ("zp:steelhead", "steelhead.r2b"),
    ("z1:trout_quota", "trout_quota.r5b"),
    ("z1:hg_quota", "hg_quota.r6b"),
    ("z2:trout_char_quota", "trout_char_quota.r7b"),
    ("z3:trout_char_quota", "trout_char_quota.r5b"),
    ("z5:trout_char_quota", "trout_char_quota.r6b"),
    ("z6:trout_char_quota", "trout_char_quota.r9b"),
}
#: the twins, by the rule they repeat — the wild releases (user ruling 2026-10-01), and the rest of
#: the provincial set: the annual hatchery 10 and its record duty (user ask 2026-10-02). A twin of a
#: rule keyed `water: stream` drops the key (it is on a lake).
TWINS = {("zp:steelhead", "steelhead.r2b"): "steelhead.r2",
         ("zp:steelhead", "steelhead.r1b"): "steelhead.r1",
         ("zp:steelhead", "steelhead.r4b"): "steelhead.r4",
         ("z1:trout_quota", "trout_quota.r5b"): "trout_quota.r5",
         ("z1:hg_quota", "hg_quota.r6b"): "hg_quota.r6",
         ("z2:trout_char_quota", "trout_char_quota.r7b"): "trout_char_quota.r7",
         ("z3:trout_char_quota", "trout_char_quota.r5b"): "trout_char_quota.r5",
         ("z5:trout_char_quota", "trout_char_quota.r6b"): "trout_char_quota.r6",
         ("z6:trout_char_quota", "trout_char_quota.r9b"): "trout_char_quota.r9"}
#: the stamp's twin, bound to the known steelhead waters the stamp does not reach
STAMP_TWIN = "steelhead_targeting_known"
#: the provincial twins' one extent: the known steelhead waters minus their base
SH_OP = "steelhead_waters"
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
                want = [{"op": SH_OP, "siblings": [twin]}]
                if base is not None and not eid.startswith("zp:"):
                    bx = (base.get("extents") or [{}])[0]
                    want[0]["area_id"] = bx.get("area_id")
                    if bx.get("outside_area"):
                        want[0]["outside_area"] = bx["outside_area"]
                    want[0]["feature_types"] = ["lake", "wetland"]
                if not exts or exts != want:
                    out.append(f"{tag}: extents {exts} are not the book-known steelhead waters "
                               + ("less its base" if eid.startswith("zp:")
                                  else "of its base's area less its base"))
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
                    and r["rule_id"] not in twins:
                out.append(f"{eid}::{r['rule_id']}: the book-known lakes of its area have no "
                           f"twin of it")
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
                        or x.get("extents") != [{"op": SH_OP, "siblings": ["steelhead_targeting"],
                                                 "outside_area_kind": "national_parks"}]:
                    out.append(f"{eid}#{x['id']}: not the stamp bound to the known steelhead "
                               f"waters")
                continue
            want = _streams_in(regions, outside_area_kind="national_parks")
            if x.get("water") != "stream" or x.get("extents") != want:
                out.append(f"{eid}#{x['id']}: not the streams of regions {regions}")
        if eid.startswith("zp:") and stamps and STAMP_TWIN not in stamps:
            out.append(f"{eid}: the known steelhead waters have no stamp")
        if eid.startswith("zp:"):
            ids = {r["rule_id"] for r in e.get("rules") or []}
            for (te, tr), base in TWINS.items():
                if te == eid and base in ids and tr not in ids:
                    out.append(f"{eid}::{base}: the known steelhead waters have no twin of it")
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
                    "steelhead.r1b": "annual@origin=hatchery",      # its twin on the known waters
                    "steelhead.r2": "daily@origin=wild",            # its key, kept
                    "steelhead.r2b": "daily@origin=wild",           # its twin on the known waters
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
    lambda raw: _r(raw, "zp:steelhead", "steelhead.r2b")["extents"].append(_STAVE),  # Stave too
    lambda raw: _r(raw, "zp:steelhead", "steelhead.r2b")["extents"].pop(),     # no place at all
    lambda raw: _r(raw, "zp:steelhead", "steelhead.r2b")["extents"][0].update(
        siblings=["steelhead.r1"]),                             # the wrong base subtracted
    lambda raw: _r(raw, "zp:steelhead", "steelhead.r1b").update(
        extents=_lakes_of(raw, ("2",))),                        # back to hand-picked lakes
    lambda raw: raw["zp:steelhead"]["licensing"][1]["extents"][0].pop(
        "outside_area_kind"),                                   # the stamp into national parks
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
    lambda raw: _r(raw, "z2:trout_char_quota", "trout_char_quota.r7b").update(
        extents=_lakes_of(raw, ("2",))),                        # back to hand-picked lakes
    lambda raw: raw["z5:trout_char_quota"].update(rules=[
        r for r in raw["z5:trout_char_quota"]["rules"]
        if r["rule_id"] != "trout_char_quota.r6b"]),            # Region 5 without its twin
    lambda raw: _r(raw, "z1:trout_quota", "trout_quota.r5b")["extents"][0].pop(
        "outside_area"),                                        # Region 1's release onto Haida Gwaii
    lambda raw: _r(raw, "z3:trout_char_quota", "trout_char_quota.r5b")["extents"][0].update(
        area_id="area:region:5"),                               # a zone twin outside its zone
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
def db(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
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


def _real_lakes(db) -> set[str]:
    """Lake items: `item.kind` IS the water kind (a slough or canal is a `stream` item since the
    registry decides it — AGENTS 55), so nothing here looks at a name."""
    return {i for (i,) in db.execute("SELECT item_id FROM item WHERE kind = 'lake'")}


def _steelhead_lakes(db) -> set[str]:
    """Every lake any section of which carries a provincial or zone steelhead rule."""
    ids = {x for (x,) in db.execute(STEELHEAD_RULES)}
    return {i for i, e, r in db.execute(
        "SELECT DISTINCT i.item_id, r.entry_id, r.rule_id FROM item i JOIN item_section s "
        "ON s.ord = i.ord JOIN section_ruleset sr ON sr.sid = s.sid JOIN ruleset r "
        "ON r.set_id = sr.set_id WHERE i.kind = 'lake' AND r.entry_id LIKE 'z%'")
        if f"{e}::{r}" in ids} & _real_lakes(db)


#: lakes that are steelhead water: Khartoum and Lois (their own rows print "hatchery steelhead").
#: Tenas Lake, bound by the Atnarko's spring closure, is NOT (FIX D13, user ruling 2026-10-06: a
#: lake only by its own row; its row prints "No Fishing Apr 1-June 30"). The Vedder Canal — a polygon of the flagged Chilliwack/Vedder row's water — is the
#: Vedder River's own (`registry.flowing`, user ruling 2026-10-03): its section is the river's
VEDDER_CANAL = "gnis:3062"
VEDDER_CANAL_SECTION = "lake:329707189"
STEELHEAD_ROW_LAKES = {"wbk:329197063", "wbk:329197058"}
#: waters drawn as polygons that only the curated list makes known — Gravel Slough, Maria Slough
#: (their polygons folded into their lines), the Alouette's polygon (into the Alouette River):
#: STREAMS (AGENTS 55), so the Region 2 stream steelhead rules bind them and they are steelhead
#: water (user ruling 2026-10-03)
LISTED_SLOUGHS = {"gnis:8009", "gnis:13499", "gnis:9630"}
LISTED_POLYGONS = {"lake:329083360", "lake:329178010", "lake:329292631"}


def _known_lakes(db) -> set[str]:
    return {i for (i,) in db.execute(
        "SELECT DISTINCT i.item_id FROM item i JOIN item_section s ON s.ord = i.ord "
        "JOIN section_steelhead h ON h.sid = s.sid WHERE i.kind = 'lake' AND h.code = 1")} \
        & _real_lakes(db)


def _book_lakes(db) -> set[str]:
    """Lakes a steelhead row binds (`steelhead_source` names a row, not only the curated list)."""
    return {i for (i,) in db.execute(
        "SELECT DISTINCT i.item_id FROM item i JOIN steelhead_source h ON h.ord = i.ord "
        "WHERE i.kind = 'lake' AND h.entry_id != 'curated list'")} & _real_lakes(db)


@pytest.mark.needs_bundle
def test_a_lake_carries_a_steelhead_rule_only_when_a_steelhead_row_binds_it(db):
    """A provincial or zone steelhead rule — the wild release included — or the stamp is on a lake
    only where the lake's OWN row is a steelhead row (Khartoum, Lois; not Tenas, FIX D13), and no other (a slough or
    canal is a stream, AGENTS 55; the list binds no rule)."""
    # every wild release is in the bundle but Region 1's two twins: no lake of Region 1 is bound
    # by a steelhead row, so they are unresolved (`no_sections`), not shipped as rules on nothing
    shipped = {x for (x,) in db.execute(STEELHEAD_RULES)} & WILD
    assert WILD - shipped <= {"z1:trout_quota::trout_quota.r5b", "z1:hg_quota::hg_quota.r6b",
                              "z5:trout_char_quota::trout_char_quota.r6b"}   # Tenas was Region 5's only
    book = _book_lakes(db)
    assert book == STEELHEAD_ROW_LAKES, book ^ STEELHEAD_ROW_LAKES
    assert _known_lakes(db) == book                 # the list names no lake (only sloughs)
    assert _steelhead_lakes(db) == book
    hit = {i for (i,) in db.execute(
        "SELECT DISTINCT i.item_id FROM item i JOIN item_section s ON s.ord = i.ord "
        "JOIN requirement_section q ON q.sid = s.sid "
        "WHERE i.kind = 'lake' AND q.entry_id = 'zp:steelhead'")} & _real_lakes(db)
    assert hit == book
    # the stamp is placed on sections, never province-wide (which would hold on every lake)
    (placement,) = db.execute("SELECT placement FROM requirement WHERE entry_id = 'zp:steelhead' "
                              "AND req_id = 'steelhead_targeting'").fetchone()
    assert placement == "sections"


#: the lake copies of the provincial set, and Region 2's release
LAKE_SET = {"zp:steelhead::steelhead.r1b", "zp:steelhead::steelhead.r2b",
            "zp:steelhead::steelhead.r4b", "z2:trout_char_quota::trout_char_quota.r7b"}


@pytest.mark.needs_bundle
@pytest.mark.parametrize("item_id,own", [(KHARTOUM, "r2:khartoum_lake@2-12::khartoum_lake.r5"),
                                         (LOIS, "r2:lois_lake@2-12::lois_lake.r4")])
def test_khartoum_and_lois_get_the_provincial_set(db, item_id, own):
    """Their own rows name steelhead, so they carry the whole provincial steelhead set — the annual
    hatchery 10, the wild release, the record duty (the twins r1b, r2b, r4b) and the stamp
    (`steelhead_targeting_known`) — and Region 2's "All wild steelhead", beside their own rule, and
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


@pytest.mark.needs_bundle
def test_a_stream_in_a_qualifying_region_gets_them(db):
    """Capilano River (Region 2) carries all three provincial steelhead rules and the stamp."""
    secs = _sections(db, "gnis:5922")
    assert secs
    for sid in secs:
        assert PROVINCE <= _rules_on(db, sid), sid
        assert STAMP in _requirements_on(db, sid), sid


QUALIFYING_ZONES = tuple(f"z{n}:" for n in QUALIFYING)


@pytest.mark.needs_bundle
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


def _steelhead_water(db, sid) -> bool:
    return db.execute("SELECT 1 FROM steelhead_water WHERE sid = ?", (sid,)).fetchone() is not None


@pytest.mark.needs_bundle
def test_the_thompson_below_kamloops_lake_is_the_book_s_steelhead_water(db):
    """The Thompson row names steelhead (Class II, Steelhead Stamp mandatory) for its own stretch,
    downstream of Kamloops Lake: every section its rules bind is known AND steelhead water. Above
    the lake the river is not that row's water: it is known only because the curated list names
    the Thompson — and, a known stream, a big rainbow there is a steelhead too (user ruling
    2026-10-03), though no rule of the Thompson row binds it."""
    rid = next(r for (r,) in db.execute("SELECT rule_id FROM rule WHERE entry_id = ?", (THOMPSON,)))
    got = _presence(db, "gnis:39492")
    below = {s for s in _sections(db, "gnis:39492") if f"{THOMPSON}::{rid}" in _rules_on(db, s)}
    assert below and set(got) == {"known"}
    above = set(_sections(db, "gnis:39492")) - below
    assert above, "the list reaches the Thompson above Kamloops Lake"
    sw = {s for s in _sections(db, "gnis:39492") if _steelhead_water(db, s)}
    assert sw == set(_sections(db, "gnis:39492"))


@pytest.mark.needs_bundle
@pytest.mark.parametrize("item_id,name", [("gnis:39400", "Nicola River"),
                                          ("gnis:38393", "Bonaparte River"),
                                          ("gnis:30592", "Deadman River")])
def test_a_tributary_of_the_thompson_is_known_only_by_the_list(db, item_id, name):
    """NO TRIBUTARY WALK (user ruling 2026-10-02): a stream joining the Thompson below Kamloops Lake
    is not the Thompson row's water. These are KNOWN because the curated list names them — and, known
    streams, a big rainbow there is a steelhead (user ruling 2026-10-03); no steelhead row's rule
    binds them."""
    got = _presence(db, item_id)
    assert set(got) == {"known"}, (name, {k: len(v) for k, v in got.items()})
    srcs = {e for (e,) in db.execute("SELECT h.entry_id FROM steelhead_source h JOIN item i "
                                     "ON i.ord = h.ord WHERE i.item_id = ?", (item_id,))}
    assert srcs == {"curated list"}, (name, srcs)
    for sid in got["known"]:
        assert _steelhead_water(db, sid), name


@pytest.mark.needs_bundle
@pytest.mark.parametrize("item_id,name", [("gnis:27764", "Williams Lake River"),  # Region 5
                                          ("gnis:21205", "San Jose River")])      # Region 5
def test_an_interior_cariboo_stream_is_possible(db, item_id, name):
    """A Cariboo stream no row names steelhead on, and no tributary of one: the steelhead rules
    apply, steelhead may not be present — and a big rainbow is a rainbow."""
    got = _presence(db, item_id)
    assert set(got) == {"possible"}, (name, {k: len(v) for k, v in got.items()})
    for sid in got["possible"]:
        assert PROVINCE <= _rules_on(db, sid), name
        assert not db.execute("SELECT 1 FROM steelhead_water WHERE sid = ?", (sid,)).fetchone()


#: the OUTRIGHT stamp-waiver rows ("(Steelhead Stamp not required)"): steelhead rows by that
#: alone (user ruling 2026-10-02; the Dean's waiver row is flagged as well)
WAIVER_ROWS = {"r5:chilko_river@5-5": "gnis:13741",
               "r5:horsefly_river_from_quesnel_lake_to_horsefly_river_falls@5-2": "gnis:24531",
               "r5:west_road_blackwater_river@5-12+5-13": "gnis:26104",
               "r6:stellako_river@6-4+7-12": "gnis:7836", "r7:stellako_river@7-12": "gnis:7836"}


@pytest.mark.needs_bundle
def test_the_stamp_waiver_rows_are_steelhead_rows(raw, db):
    """"(Steelhead Stamp not required)" prints steelhead (user ruling 2026-10-02): the Chilko,
    Horsefly, West Road and both Stellako rows are steelhead rows. Every section their rules bind is
    known, carries the provincial set, and — a stream — is steelhead water. The Horsefly row binds
    the river below the falls; above them the river stays "possible"."""
    from pipeline.atlas.reach import steelhead as SH
    for eid in WAIVER_ROWS:
        e = raw[eid]
        assert SH.steelhead_row(e) and SH.prints_steelhead(e), eid
        assert not SH.names_steelhead(e) and not e.get("anadromous_rainbow"), eid
        secs = {s for (s,) in db.execute(
            "SELECT DISTINCT sr.sid FROM section_ruleset sr JOIN ruleset r ON r.set_id = sr.set_id "
            "WHERE r.entry_id = ?", (eid,))}
        assert secs, eid
        for sid in secs:
            from pipeline.deliver.bundle import read as R
            assert R.steelhead_presence(db, sid) == "known", (eid, sid)
            have = {i.split("::")[1] for i in _rules_on(db, sid) if i.startswith("zp:")}
            assert {"steelhead.r1", "steelhead.r2", "steelhead.r4"} <= {h.rstrip("b") for h in have}
    got = _presence(db, "gnis:24531")
    assert set(got) == {"known", "possible"}
    assert all(_steelhead_water(db, s) for s in got["known"])


@pytest.mark.needs_bundle
def test_the_stellako_in_zone_7a_carries_the_set_by_its_waiver_row(db):
    """The Stellako's Zone 7A pieces lie past the steelhead regions; the waiver rows bind them, so
    they carry the provincial set through the twins and the stamp twin, and are steelhead water.
    The stamp twin is PLACED there, and the row's outright waiver ("Class II water when open
    (Steelhead Stamp not required)") lifts it whenever the water is open (user ruling 2026-10-02:
    no steelhead stamp of any kind; `read.requirements_in_force`)."""
    from pipeline.deliver.bundle import read as R
    secs = _sections(db, "gnis:7836")
    beyond = [s for s in secs if not any(r.startswith(QUALIFYING_ZONES) for r in _rules_on(db, s))]
    assert beyond
    twins = {f"zp:steelhead::{r}" for r in ("steelhead.r1b", "steelhead.r2b", "steelhead.r4b")}
    for sid in beyond:
        on = _rules_on(db, sid)
        assert twins <= on and not on & PROVINCE, on
        assert f"zp:steelhead#{STAMP_TWIN}" in _requirements_on(db, sid)
        assert _steelhead_water(db, sid)
        got = R.requirements_in_force(db, sid, (7, 1))
        assert f"zp:steelhead#{STAMP_TWIN}" in got["waived"], got
        assert not any(k.startswith("zp:steelhead#") for k in got["holds"]), got


@pytest.mark.needs_bundle
def test_a_region_4_stream_and_a_lake_are_absent(db):
    """The Elk River (Region 4) and Stave Lake (Region 2) carry no steelhead attribute."""
    assert set(_presence(db, "gnis:16880")) == {None}
    assert set(_presence(db, "wbk:329291805")) == {None}


OKANAGAN, INKANEEP = "gnis:32069", "gnis:10228"


@pytest.mark.needs_bundle
@pytest.mark.parametrize("item_id", [OKANAGAN, INKANEEP, "gnis:29653"])     # + Vaseux Creek
def test_a_listed_river_outside_the_steelhead_regions_is_known_with_no_steelhead_rule(db, item_id):
    """THE CURATED LIST (user rulings 2026-10-02, 2026-10-03): the Okanagan River, Inkaneep and
    Vaseux creeks (Region 8) are on the list, so every section is KNOWN — and they carry NO
    steelhead rule and no stamp, so they are NOT steelhead water: "if no steelhead rules exist,
    rainbow rules still apply to a steelhead". A 55 cm rainbow is asked about as a rainbow: on the
    Okanagan its own "Rainbow trout catch and release" answers; on Inkaneep, Region 8's "1 over 50
    cm"."""
    from pipeline.deliver.bundle import read as R
    secs = _sections(db, item_id)
    assert secs
    ids = {x for (x,) in db.execute(STEELHEAD_RULES)} | PROVINCE | {
        f"zp:steelhead::{r}" for r in ("steelhead.r1b", "steelhead.r2b", "steelhead.r4b")}
    for sid in secs:
        assert R.steelhead_presence(db, sid) == "known"
        assert not _rules_on(db, sid) & ids
        assert not {r for r in _requirements_on(db, sid) if r.startswith("zp:steelhead#")}
        assert not _steelhead_water(db, sid)
        assert not {x for x in R.effective_rules(sid, (7, 1), "ST", str(BUNDLE))
                    if x["entry"] == "zp:steelhead"}
        day = (1, 15)                                   # outside Region 8's spring stream closure
        rb = {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(sid, day, "RB", str(BUNDLE))
              if x["state"] == "speaks"}
        if item_id == OKANAGAN:
            assert "r8:okanagan_river@8-1::okanagan_river.r5" in rb, rb   # every rainbow released
        else:
            assert "z8:trout_char_quota::trout_char_quota.r2" in rb, rb   # "1 over 50 cm"


COWICHAN = "gnis:12227"


@pytest.mark.needs_bundle
def test_the_list_changes_answers_only_through_steelhead_water(db, tmp_path):
    """MUTATION ON THE BUNDLE, the Cowichan River (Region 1; known only by the curated list, where
    steelhead rules apply): the list's ONE effect on `effective_rules` is `steelhead_water`. (1)
    Clear its `anadromous` and the rainbow answer DOES change — Region 1's "1 over 50 cm" speaks for
    a rainbow again. (2) From there, take the list's mark off (its sections fall back to
    "possible") and no answer changes — the reader never looks at the indicator itself. (3) The
    other way, on Inkaneep Creek (Region 8, no steelhead rule): marking it steelhead water WOULD
    change the rainbow answer, which is why it is not (user ruling 2026-10-03)."""
    import shutil
    from pipeline.deliver.bundle import read as R

    def answers(path, secs):
        return {(sid, fish, day): sorted((x["entry"], x["rule"], x["state"]) for x in
                                         R.effective_rules(sid, day, fish, str(path)))
                for sid in secs[:3] for fish in ("RB", "ST") for day in ((1, 15), (7, 1))}

    def mutate(name, src, secs, *sql):
        path = tmp_path / f"{name}.sqlite"
        shutil.copyfile(src, path)
        con = sqlite3.connect(path)
        for q in sql:
            con.execute(q.format(",".join(map(str, secs))))
        con.commit()
        con.close()
        return path

    secs = _sections(db, COWICHAN)
    assert secs and all(_steelhead_water(db, s) for s in secs)
    base = answers(BUNDLE, secs)
    plain = mutate("not_steelhead_water", BUNDLE, secs,
                   "UPDATE steelhead_known SET anadromous = 0 WHERE sid IN ({})")
    got = answers(plain, secs)
    assert got != base
    rb = {r for (sid, fish, day), xs in got.items() if fish == "RB" and day == (1, 15)
          for e, r, st in xs if st == "speaks" and e == "z1:trout_quota"}
    assert "trout_quota.r2" in rb, rb
    # what a build WITHOUT the list writes for these sections: no `steelhead_known` row, their
    # rule sets "possible" with steelhead rules applying (an R1 stream carries the provincial
    # set) — `section_steelhead_rules` reads the set row once the known row is gone, and the
    # Cowichan's sets have none (every section in them is known). Without the set row the
    # mutation also deleted "rules apply", and RU-6 re-asked ST as RB (review gate 6).
    unlisted = mutate("unlisted", plain, secs, "DELETE FROM steelhead_known WHERE sid IN ({})",
                      "INSERT OR IGNORE INTO steelhead_set(set_id, code, rules) SELECT DISTINCT "
                      "set_id, 2, 1 FROM section_ruleset WHERE sid IN ({})")
    c2 = sqlite3.connect(f"file:{unlisted}?mode=ro", uri=True)
    assert all(R.steelhead_presence(c2, s) != "known" for s in secs)
    assert all(R.steelhead_rules(c2, s) for s in secs)       # rules still apply, as built
    c2.close()
    assert answers(unlisted, secs) == got
    ink = _sections(db, INKANEEP)
    marked = mutate("inkaneep_steelhead_water", BUNDLE, ink,
                    "UPDATE steelhead_known SET anadromous = 1 WHERE sid IN ({})")
    assert answers(marked, ink) != answers(BUNDLE, ink)


@pytest.mark.needs_bundle
def test_anadromous_rainbow_is_exactly_known_stream_where_steelhead_rules_apply(db, raw):
    """A big rainbow is a steelhead EXACTLY on KNOWN ∧ STREAM (the registry's kind: a slough or canal
    is one — the Vedder Canal) ∧ STEELHEAD RULES APPLY (the section carries every rule of the
    provincial set, base or twin) — known by a steelhead row or by the curated list (user ruling
    2026-10-03) — and nowhere else: never a possible stream, never a lake (Khartoum, Lois, Tenas),
    never a known water no steelhead rule binds (the Okanagan River, the Fraser in Zone 7A)."""
    from pipeline.atlas.reach import steelhead as SH
    q = lambda sql, *a: {s for (s,) in db.execute(sql, a)}                # noqa: E731
    sw = q("SELECT sid FROM steelhead_water")
    known = q("SELECT sid FROM section_steelhead WHERE code = 1")
    possible = q("SELECT sid FROM section_steelhead WHERE code = 2")
    # the bundle keeps no kind for an unnamed section: a NAMED non-stream water is the only
    # non-stream it can name (`item.kind` is the water kind)
    still = {s for (s,) in db.execute(
        "SELECT s.sid FROM item i JOIN item_section s ON s.ord = i.ord WHERE i.kind != 'stream'")}
    per_set: dict = defaultdict(set)
    for k, r in db.execute("SELECT set_id, rule_id FROM ruleset WHERE entry_id = ?",
                           (SH.PROVINCE_STEELHEAD,)):
        per_set[k].add(r[:-1] if r.endswith("b") else r)
    full = {k for k, v in per_set.items() if set(SH.PROVINCE_SET_RULES) <= v}
    apply = {s for s, k in db.execute("SELECT sid, set_id FROM section_ruleset") if k in full}
    assert sw and possible and not sw & possible
    assert sw <= known & apply, len(sw - (known & apply))
    assert not sw & still
    assert (known & apply) - sw <= still, len((known & apply) - sw - still)
    # known with no steelhead rule: the Okanagan, Inkaneep, Vaseux, the Fraser's Zone 7A pieces
    no_rules = known - apply
    assert set(_sections(db, OKANAGAN)) <= no_rules and set(_sections(db, INKANEEP)) <= no_rules
    assert set(_sections(db, VEDDER_CANAL)) <= sw and set(_sections(db, COWICHAN)) <= sw
    for i in STEELHEAD_ROW_LAKES:
        assert not set(_sections(db, i)) & sw, i
    # every possible section carries the whole set
    assert possible <= apply


@pytest.mark.needs_bundle
@pytest.mark.parametrize("item_id,name", [("gnis:14707", "Pennask Creek"),     # Region 8
                                          ("gnis:9237", "Brenda Creek")])      # Region 8
def test_the_walk_is_gone_past_the_steelhead_regions(db, item_id, name):
    """Before 2026-10-02 (revised) the Thompson's tributary walk made these Nicola headwater creeks of
    Region 8 known, steelhead water, and gave them the provincial set through the twins. No walk
    now, and they are not on the curated list: in Region 8 they are ABSENT, carry no steelhead rule,
    and Region 8's "1 over 50 cm" speaks for a big rainbow."""
    from pipeline.deliver.bundle import read as R
    secs = _sections(db, item_id)
    assert secs, name
    outside = [s for s in secs if not any(r.startswith(QUALIFYING_ZONES) for r in _rules_on(db, s))]
    assert outside, name
    twins = {f"zp:steelhead::{r}" for r in ("steelhead.r1b", "steelhead.r2b", "steelhead.r4b")}
    for sid in outside:
        assert R.steelhead_presence(db, sid) is None, (name, sid)
        assert not _steelhead_water(db, sid)
        assert not _rules_on(db, sid) & (twins | PROVINCE), name
        assert f"zp:steelhead#{STAMP_TWIN}" not in _requirements_on(db, sid)
        rb = {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(sid, (7, 1), "RB", str(BUNDLE))
              if x["state"] == "speaks"}
        if name == "Pennask Creek":
            # its own Region 3 row, "No Fishing upstream of Pennask Lake" (p.31), binds since the
            # RULES round's FIX batch (the whole creek is the lake's inlet): closed, not Region 8's
            assert "r3:pennask_creek@3-12::pennask_creek.r1" in rb, (name, rb)
        else:
            assert "z8:trout_char_quota::trout_char_quota.r2" in rb, (name, rb)


def _presence_fixture(SH, NS):
    kinds = {"a": "stream", "b": "stream", "c": "stream", "lk": "lake", "kl": "lake", "x": "stream",
             "vc": "lake", "l1": "stream", "l8": "stream"}
    g = NS(nodes={s: NS(kind=NS(value=k)) for s, k in kinds.items()})
    # the Vedder Canal is a STREAM by the registry's kind (a polygon of the Vedder River's item;
    # here its own item for the fixture) — the one place that decides it
    reg = {"wbk:vc": NS(kind="stream", name="Vedder Canal", section_ids=("vc",)),
           "wbk:lk": NS(kind="lake", name="Tenas Lake", section_ids=("lk",)),
           "wbk:kl": NS(kind="lake", name="Khartoum Lake", section_ids=("kl",)),
           "gnis:l": NS(kind="stream", name="Listed Creek", section_ids=("l1",)),
           "gnis:8": NS(kind="stream", name="Okanagan River", section_ids=("l8",))}
    pr = SH.Presence(reg, g)
    pr.add_row({"entry_id": "r5:atnarko", "rules": [{"species": ["ST"]}]},
               own=lambda: (NS(sections=("b",), via_tributary=()), []))
    pr.add_row({"entry_id": "r2:khartoum", "rules": [{"species": ["ST"]}]},
               own=lambda: (NS(sections=("kl",), via_tributary=()), []))
    pr.add_row({"entry_id": "r5:waiver", "rules": [],
                "licensing": [{"steelhead_stamp_waived": {"verbatim": "Steelhead Stamp not "
                                                                      "required"}}]})
    pr.add_row({"entry_id": "r5:other", "rules": [{"species": ["RB"]}]})
    pr.add_list([(SH.ListWater(item_id="gnis:l"), "gnis:l"), (SH.ListWater(item_id="gnis:8"),
                                                              "gnis:8")])
    base = ("a", "b", "c", "l1", "vc")
    binds = [NS(entry_id="r5:atnarko", rule_id="r1", sections=("lk", "b")),
             NS(entry_id="r2:khartoum", rule_id="r1", sections=("kl",)),
             NS(entry_id="r5:waiver", rule_id="r1", sections=("vc",)),
             NS(entry_id="r5:other", rule_id="r1", sections=("c",))]
    binds += [NS(entry_id=SH.PROVINCE_STEELHEAD, rule_id=r, sections=base)
              for r in SH.PROVINCE_SET_RULES]
    binds += [NS(entry_id=SH.PROVINCE_STEELHEAD, rule_id=r + "b", sections=("kl", "x"))
              for r in SH.PROVINCE_SET_RULES]
    twins = {(SH.PROVINCE_STEELHEAD, r + "b") for r in SH.PROVINCE_SET_RULES}
    return pr, binds, twins


def test_the_presence_pass_knows_what_steelhead_rows_bind_and_the_list_names():
    """Unit: book-known = the steelhead rows' own water and the STREAM sections their rules bind,
    whatever their region; a LAKE only as a row's own water (Khartoum's row: known; Tenas Lake, bound
    by the Atnarko's closure: not — FIX D13, `LAKES_ONLY_BY_OWN_ROW`); the list adds known sections
    (and no rule); possible is the base rules' streams less the known; the twins' sections are not
    possible water; `rules` = bound by every member of the provincial set; anadromous = known ∧
    stream (a canal is one) ∧ rules."""
    from types import SimpleNamespace as NS
    from pipeline.atlas.reach import steelhead as SH
    pr, binds, twins = _presence_fixture(SH, NS)
    rows, rep = pr.finish(binds, twins=twins)
    got = {r["section_id"]: (r["steelhead"], r["regulations"], r["listed"], r["rules"],
                             r["anadromous"]) for r in rows}
    assert got == {"a": ("possible", False, False, True, False),
                   "c": ("possible", False, False, True, False),
                   "b": ("known", True, False, True, True), "kl": ("known", True, False, True, False),
                   "vc": ("known", True, False, True, True), "l1": ("known", False, True, True, True),
                   "l8": ("known", False, True, False, False)}
    assert rep["known_lakes"] == 1 and rep["steelhead_rows"] == 3 and rep["anadromous"] == 3
    assert rep["anadromous_list_only"] == 1 and rep["known_no_rules"] == 1
    assert rep["by_entry"] == {"r2:khartoum": {"sections": 1}, "r5:atnarko": {"sections": 1},
                               "r5:waiver": {"sections": 1}}
    assert rep["known_list_only"] == 2 and rep["known_regulations"] == 3
    # a section missing ONE member of the set is not where steelhead rules apply
    assert "l1" not in SH.Presence.rules_apply(binds[:-4])


def test_mutation_a_lake_another_line_binds_was_book_known(monkeypatch):
    """MUTATION (FIX D13): with `LAKES_ONLY_BY_OWN_ROW` off, Tenas Lake (bound only by the
    Atnarko's closure) is book-known again."""
    from types import SimpleNamespace as NS
    from pipeline.atlas.reach import steelhead as SH
    monkeypatch.setattr(SH, "LAKES_ONLY_BY_OWN_ROW", False)
    pr, binds, twins = _presence_fixture(SH, NS)
    rows, _rep = pr.finish(binds, twins=twins)
    assert {r["section_id"] for r in rows if r["regulations"]} >= {"lk", "kl"}


# ----------------------------------------------------------------------------- the export
@pytest.fixture(scope="module")
def doc() -> dict:
    return X.build(BUNDLE)


@pytest.mark.needs_bundle
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
        if w["kind"] == "lake" and mentions == own and w["parts"][0]["ruleset"] and (
                own or not w["parts"][0].get("steelhead")):
            return item, w["parts"][0]
    raise AssertionError("no such lake")


def _bad(doc):
    bad = {k: doc[k] for k in ("rules", "licensing", "entries", "waters", "licensing_sets")}
    bad["rulesets"] = copy.deepcopy(doc["rulesets"])
    return bad


@pytest.mark.needs_bundle
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


@pytest.mark.needs_bundle
@pytest.mark.parametrize("rid", ["zp:steelhead::steelhead.r2",                 # the wild release
                                 "z1:trout_quota::trout_quota.r5",             # a zone release
                                 "zp:steelhead::steelhead.r1", "zp:steelhead::steelhead.r4",
                                 "z2:trout_char_quota::trout_char_quota.r3",
                                 "z6:hatchery_steelhead_stop::hatchery_steelhead_stop.r1"])
def test_the_export_check_refuses_a_steelhead_rule_on_a_lake(doc, rid):
    """Mutation: any steelhead rule — a wild release included — put on a lake that is not known
    steelhead water is refused."""
    bad = _bad(doc)
    item, part = _lake_part(doc, own=False)
    assert X.steelhead_lake_problems(bad) == []
    bad["rulesets"][part["ruleset"]].setdefault("reach", []).append(rid)
    got = X.steelhead_lake_problems(bad)
    assert any(p.startswith(f"steelhead rule {rid} shows on") and item in p for p in got), got


@pytest.mark.needs_bundle
def test_the_export_check_allows_only_the_lakes_own_row_and_the_release(doc):
    """On Lois Lake: Lois Lake's own steelhead rule and the wild releases; Region 2's "2 hatchery
    steelhead" put there is refused, and so is Lois without the provincial wild release."""
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
    assert any(p.startswith(f"water {LOIS} part 0:") and "wild release" in p
               for p in X.steelhead_set_problems(bad))


@pytest.mark.needs_bundle
def test_the_export_check_refuses_a_province_wide_stamp(doc):
    bad = {k: doc[k] for k in ("rules", "entries", "waters", "rulesets", "licensing_sets")}
    bad["licensing"] = copy.deepcopy(doc["licensing"])
    bad["licensing"][STAMP]["placement"] = "province"
    assert f"steelhead record {STAMP} is placed province-wide — it holds on every lake" \
        in X.steelhead_lake_problems(bad)
    assert json.dumps(doc["licensing"][STAMP]["placement"]) == '"sections"'


@pytest.mark.needs_bundle
def test_the_export_carries_steelhead_per_part_and_water(doc):
    """`waters[].parts[].steelhead`, the water's roll-up and why (`steelhead_source`): the Thompson
    known (the book below Kamloops Lake, the curated list above); Nicola known by the list alone (no
    steelhead_rows, but a big rainbow is a steelhead: 2026-10-03); Horsefly known by its waiver row below the falls
    and possible above; Williams Lake River possible; Elk and Stave Lake absent; Khartoum known by
    its row; the Okanagan known by the list with no steelhead rule. A big rainbow is a steelhead
    exactly on the KNOWN flowing parts, book or list (`steelhead_presence_problems`)."""
    W = doc["waters"]
    assert X.steelhead_presence_problems(doc) == []
    assert W["gnis:39492"]["steelhead"] == "known"
    assert W["gnis:39492"]["steelhead_source"] == ["regulations", "curated list"]
    assert W["gnis:39492"]["steelhead_rows"] == [THOMPSON]
    assert W["gnis:39400"]["steelhead"] == "known"
    assert W["gnis:39400"]["steelhead_source"] == ["curated list"]
    assert "steelhead_rows" not in W["gnis:39400"]
    assert all(p.get("anadromous_rainbow") for p in W["gnis:39400"]["parts"])
    assert all(p["steelhead_rules"] is True for p in W["gnis:39400"]["parts"])     # DF4
    assert W["gnis:24531"]["steelhead"] == "known"
    assert {p.get("steelhead") for p in W["gnis:24531"]["parts"]} == {"known", "possible"}
    assert W["gnis:27764"]["steelhead"] == "possible" and "steelhead_source" not in W["gnis:27764"]
    assert W[KHARTOUM]["steelhead"] == "known" and W[LOIS]["steelhead"] == "known"
    assert W[KHARTOUM]["steelhead_source"] == ["regulations"]
    assert W[KHARTOUM]["steelhead_rows"] == ["r2:khartoum_lake@2-12"]
    assert W[OKANAGAN]["steelhead"] == "known" and W[OKANAGAN]["steelhead_source"] == ["curated list"]
    for p in W[OKANAGAN]["parts"]:
        assert not any(X.is_steelhead_rule(doc["rules"][i])
                       for i in X._members(doc["rulesets"], p["ruleset"], doc["rules"]))
        assert not p.get("anadromous_rainbow") and p["steelhead_rules"] is False
    assert W[OKANAGAN]["steelhead_rules"] is False
    assert "steelhead" not in W["gnis:16880"] and "steelhead" not in W["wbk:329291805"]
    assert all(p.get("anadromous_rainbow") for p in W[VEDDER_CANAL]["parts"])
    assert W[VEDDER_CANAL]["kind"] == "stream" and "drawn_as" not in W[VEDDER_CANAL]
    assert W[VEDDER_CANAL]["absorbed"] == ["wbk:329707189"]       # the canal's polygon, folded in
    fd = doc["field_dictionary"]["water.parts[]"]
    assert "may not be present" in fd["steelhead"] and "BINDS NO RULE" in fd["steelhead"]
    assert "2026-10-03" in fd["anadromous_rainbow"] and "curated list" in fd["anadromous_rainbow"]
    assert X.STEELHEAD_NO_RULES in fd["steelhead_rules"]
    assert X.STEELHEAD_NO_RULES in doc["field_dictionary"]["water"]["steelhead_rules"]
    assert "steelhead_rows" in doc["field_dictionary"]["water"]
    got = doc["about"]["counts"]["sections"]["steelhead"]
    assert got["known"] > 0 and got["possible"] > 0


@pytest.mark.needs_bundle
@pytest.mark.parametrize("mutate,expect", [
    (lambda w: w["gnis:27764"]["parts"][0].update(anadromous_rainbow=True),
     "anadromous_rainbow on a possible stream"),
    (lambda w: w["gnis:39400"]["parts"][0].pop("anadromous_rainbow"),       # listed: a steelhead
     "a known stream where steelhead rules apply and a big rainbow is not a steelhead"),
    (lambda w: w[OKANAGAN]["parts"][0].update(anadromous_rainbow=True),     # no steelhead rule
     "no steelhead rule applies to"),
    (lambda w: w[OKANAGAN]["parts"][0].pop("steelhead_rules"), "steelhead_rules None on a known"),
    (lambda w: w[OKANAGAN].pop("steelhead_rules"), "steelhead_rules None for a known water"),
    (lambda w: w["gnis:39400"]["parts"][0].update(steelhead_rules=False),
     "steelhead_rules False on a known part where steelhead rules apply"),
    (lambda w: w["gnis:39400"].update(steelhead_rules=False), "steelhead_rules False for a known"),
    (lambda w: w[KHARTOUM]["parts"][0].update(anadromous_rainbow=True),
     "anadromous_rainbow on a known lake"),
    (lambda w: w["wbk:329291805"]["parts"][0].update(steelhead="possible"), "a lake marked"),
    (lambda w: w["gnis:39400"].update(steelhead="possible"), "parts say 'known'"),
    (lambda w: w["gnis:5922"]["parts"][0].pop("anadromous_rainbow"),
     "a known stream where steelhead rules apply and a big rainbow is not a steelhead"),
    (lambda w: w["gnis:16880"]["parts"][0].update(steelhead="possible"), "no steelhead rule"),
    (lambda w: w["gnis:39400"].update(steelhead_source=["regulations"]), "disagree"),
    (lambda w: w["gnis:39400"].pop("steelhead_source"), "for a known water"),
])
def test_the_presence_check_catches_a_mutation(doc, mutate, expect):
    """Mutation: each way the attribute, its source and the steelhead definition can disagree is
    refused."""
    bad = dict(doc)
    bad["waters"] = copy.deepcopy(doc["waters"])
    assert X.steelhead_presence_problems(bad) == []
    mutate(bad["waters"])
    got = X.steelhead_presence_problems(bad)
    assert any(expect in p for p in got), got


# ------------------------------------------------------------- steelhead water (p.80)
#: Rows naming steelhead that are not steelhead water, and why (coordinator round 2026-10-01).
NOT_STEELHEAD_WATER = {
    "r2:khartoum_lake@2-12", "r2:lois_lake@2-12",                       # lakes
    "r2:fraser_river_upstream_of_the_cpr_bridge_at_mission@2-4",        # the whole Fraser item
    "r3:fraser_river@3-14", "r5:fraser_river@5-2",
    "r5:chilko_river@5-5", "r5:horsefly_river_from_quesnel_lake_to_horsefly_river_falls@5-2",
    "r5:west_road_blackwater_river@5-12+5-13", "r6:stellako_river@6-4+7-12",
    "r7:stellako_river@7-12",       # stamp waivers: not FLAGGED, though steelhead rows (2026-10-02)
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


@pytest.mark.needs_bundle
def test_no_lake_is_steelhead_water(db):
    """`steelhead_water` holds STREAM sections only — a stream's polygon among them: the Vedder
    Canal's (its river is the flagged Chilliwack/Vedder row's water), the listed sloughs' and the
    Alouette's polygons (AGENTS 55); no `lake` item's section, Khartoum/Lois/Tenas included."""
    from pipeline.common.section_handles import read as _read_handles
    got = {(i, n) for i, n in db.execute(
        "SELECT DISTINCT i.item_id, i.name FROM steelhead_water w JOIN item_section s "
        "ON s.sid = w.sid JOIN item i ON i.ord = s.ord WHERE i.kind != 'stream'")}
    assert got == set(), got
    _, sid = _read_handles(Path(dict(db.execute("SELECT k, v FROM meta"))["build"]))
    sw = {s for (s,) in db.execute("SELECT sid FROM steelhead_water")}
    for nid in LISTED_POLYGONS | {VEDDER_CANAL_SECTION}:
        assert sid[nid] in sw, nid
    owners = {i for (i,) in db.execute(
        "SELECT DISTINCT i.item_id FROM steelhead_water w JOIN item_section s ON s.sid = w.sid "
        "JOIN item i ON i.ord = s.ord")}
    assert {VEDDER_CANAL} | LISTED_SLOUGHS <= owners
    dean = _sections(db, "gnis:16075")
    assert dean and all(db.execute("SELECT 1 FROM steelhead_water WHERE sid = ?", (s,)).fetchone()
                        for s in dean)


@pytest.mark.needs_bundle
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
@pytest.mark.needs_bundle
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
# EVERY WATER A STEELHEAD ROW BINDS CARRIES THE PROVINCIAL STEELHEAD SET — WHEREVER IT IS — AND
# NOTHING ELSE DOES; THE CURATED LIST ADDS NONE OF IT (user rulings 2026-10-02, revised)
# ================================================================================================
#
# Over the WHOLE corpus and the WHOLE bundle, by class of sections (one class per (rule set,
# licensing set, lake item, steelhead code) the bundle's sections carry together):
#
#   FORWARD  every section ANY rule of a STEELHEAD ROW binds (a water row with a rule naming `ST`,
#            flagged `anadromous_rainbow`, or speaking of the Steelhead Stamp — read from the
#            corpus) is known and carries the four members of the provincial set — the annual
#            hatchery 10, the wild release, the record duty, the stamp — in ANY region.
#   REVERSE  a section carrying any member of the set is (a) a stream of a steelhead region (it
#            carries a zone table of 1, 2, 3, 5 or 6, and a code: possible, or known by the list)
#            or (b) bound by a steelhead row. Known by the curated list alone is NO reason.
#
# The members are read from the corpus by what each line SAYS (`province_member`), never by rule
# id, so the twins count as the members they copy.

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


def steelhead_rows(raw: dict) -> set[str]:
    """THE STEELHEAD ROWS: water rows any of whose rules or licensing records PRINTS steelhead (a
    rule naming `ST` or saying "steelhead"; the Steelhead Stamp in any wording), or flagged
    `anadromous_rainbow` (user ruling 2026-10-02, as corrected). Read here from the corpus, by
    hand: the words, not the model's fields."""
    return {eid for eid, e in raw.items() if not eid.startswith("z")
            and (e.get("anadromous_rainbow")
                 or any("ST" in (r.get("species") or []) or "steelhead" in r["verbatim"].lower()
                        for r in e.get("rules") or [])
                 or any("steelhead" in json.dumps(x).lower()
                        .replace("steelhead_stamp_during", "").replace("steelhead_stamp_waived", "")
                        for x in e.get("licensing") or []))}


def test_the_steelhead_rows_are_57_and_no_designation_without_steelhead_words_is_one(raw):
    """57 steelhead rows; the 25 Classified Water rows whose designation prints no steelhead (all in
    Region 4: "Class II water when open, including tributaries") are none of them."""
    from pipeline.atlas.reach import steelhead as SH
    rows = steelhead_rows(raw)
    assert rows == {k for k, e in raw.items() if SH.steelhead_row(e)} and len(rows) == 57
    plain = {k for k, e in raw.items() if not k.startswith("z")
             and any(x.get("kind") == "designation" for x in e.get("licensing") or [])
             and k not in rows}
    assert len(plain) == 25 and all(k.startswith("r4:") for k in plain), sorted(plain)


def steelhead_row_rules(raw: dict) -> set[str]:
    """EVERY rule of a steelhead row — any line binds its water into the set (Tenas Lake)."""
    rows = steelhead_rows(raw)
    return {f"{eid}::{r['rule_id']}" for eid in rows for r in raw[eid].get("rules") or []}


@pytest.fixture(scope="module")
def classes(db):
    """(rule set, licensing set, lake item or None, steelhead code or None) -> sections. `item.kind`
    is the water kind: a slough or canal is a `stream` item (AGENTS 55), not a lake."""
    db.create_function("real_lake", 2, lambda k, n: int(k == "lake"))
    rows = db.execute(
        "SELECT rs, ls, lake, st, COUNT(*) FROM ("
        " SELECT sr.sid, sr.set_id AS rs, sl.set_id AS ls,"
        "  (SELECT i.item_id FROM item_section s JOIN item i ON i.ord = s.ord"
        "   WHERE s.sid = sr.sid AND real_lake(i.kind, i.name) ORDER BY i.item_id LIMIT 1) AS lake,"
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
                          rows: set, own_lakes: set = frozenset()) -> list[str]:
    """FORWARD and REVERSE (above), one line per offending class. `rows` are the rules of the
    steelhead rows (`steelhead_row_rules`); `own_lakes` the lakes a steelhead row is written for
    (`steelhead_row_lakes`) — a lake is steelhead water only as one (FIX D13)."""
    out = []
    for (rs, ls, lake, st), n in sorted(classes.items(), key=lambda kv: str(kv[0])):
        on = rules.get(rs, set())
        held = on | recs.get(ls, set())
        have = {members[i] for i in held if i in members}
        region = any(i.split(":", 1)[0] in STEELHEAD_ZONES for i in on)
        tag = f"set {rs}/{ls} lake {lake} ({n} sections)"
        named = sorted(on & rows) if lake is None or lake in own_lakes else []
        if named:
            gone = [m for m in SET_MEMBERS if m not in have]
            if gone:
                out.append(f"{tag}: {named[0]} binds it, but the provincial {', '.join(gone)} is "
                           f"missing")
        if named and st != 1:
            out.append(f"{tag}: {named[0]} (a steelhead row) binds it, but it is not known")
        # being "known" by the curated list is no reason: the list changes no regulation
        if have and not ((st in (1, 2) and region and lake is None) or named):
            out.append(f"{tag}: provincial steelhead on a section that is no stream of a "
                       f"steelhead region and bound by no steelhead row")
        if st == 2 and not region:
            out.append(f"{tag}: \"possible\" outside the steelhead regions")
    return out


def steelhead_row_lakes(raw: dict) -> set[str]:
    """The waters the steelhead rows are written for (their `matched`): a lake among them is
    steelhead water by its OWN row (Khartoum, Lois)."""
    return {i for eid in steelhead_rows(raw) for i in raw[eid].get("matched") or ()}


def _inputs(raw, classes, sets):
    rules, recs = sets
    return (classes, rules, recs, members_of(raw), steelhead_row_rules(raw),
            steelhead_row_lakes(raw))


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
    # every steelhead row lies in a steelhead region but the Stellako's Zone 7A row (a waiver row)
    assert {e for e in rows if raw[e]["region"] not in QUALIFYING} == {"r7:stellako_river@7-12"}
    from pipeline.atlas.reach import steelhead as SH
    assert rows == {e for e, x in raw.items() if SH.steelhead_row(x)} & rows
    assert {e for e, x in raw.items() if SH.steelhead_row(x) and x.get("rules")} == rows


@pytest.mark.needs_bundle
def test_every_steelhead_row_s_water_carries_the_provincial_set(raw, classes, sets):
    """FORWARD and REVERSE over every section of the bundle."""
    assert province_set_problems(*_inputs(raw, classes, sets)) == []


@pytest.mark.needs_bundle
def test_tenas_lake_is_not_steelhead_water_by_another_rows_closure(db, raw):
    """FIX D13 (user ruling 2026-10-06): a lake is steelhead water only where its OWN row prints
    steelhead. Tenas Lake is bound by the Atnarko row's "No Fishing upstream of Tweedsmuir
    Provincial Park plus Tenas Lake, Apr 1-June 30" — that closure still binds it — but its own row
    (`r5:tenas_lake`) prints only "No Fishing Apr 1-June 30": no provincial steelhead twin, no
    stamp twin, no Region 5 "ALL STEELHEAD" twin, not known. Asked about a big rainbow in
    September, the rainbow's rules answer (a resident rainbow is a rainbow)."""
    from pipeline.deliver.bundle import read as R
    secs = _sections(db, TENAS)
    assert secs
    assert "steelhead" not in raw["r5:tenas_lake@5-11"]["regs_verbatim"].lower()
    for sid in secs:
        on = _rules_on(db, sid)
        assert f"{ATNARKO}::atnarko_bella_coola_rivers.r1" in on, "the closure still binds it"
        assert not {f"zp:steelhead::{r}" for r in ("steelhead.r1b", "steelhead.r2b",
                                                   "steelhead.r4b")} & on
        assert not on & PROVINCE
        assert "z5:trout_char_quota::trout_char_quota.r6b" not in on
        assert f"zp:steelhead#{STAMP_TWIN}" not in _requirements_on(db, sid)
        assert R.steelhead_presence(db, sid) != "known"
        assert not db.execute("SELECT 1 FROM steelhead_water WHERE sid = ?", (sid,)).fetchone()
        st = {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(sid, (9, 1), "ST", str(BUNDLE))
              if x["state"] == "speaks"}
        assert not {r for r in st if r.startswith("zp:steelhead")}, st


def test_no_row_outside_the_steelhead_regions_has_a_steelhead_rule(raw):
    """A row of Region 4, 7A, 7B or 8 that names steelhead would need deciding from the book. The
    only one that prints the word is the Stellako's Zone 7A row (and its Region 6 twin), and it
    prints a STAMP WAIVER on its Class II designation — "(Steelhead Stamp not required)" — not a
    steelhead rule. It IS a steelhead row (user ruling 2026-10-02: steelhead are mentioned), so its
    water carries the provincial set through the twins; 7A's own tables gain no steelhead line."""
    from pipeline.atlas.reach import steelhead as SH
    outside = sorted(eid for eid, e in raw.items() if not eid.startswith("z")
                     and str(e.get("region")) not in QUALIFYING
                     and "steelhead" in str(e.get("regs_verbatim") or "").lower())
    assert outside == ["r7:stellako_river@7-12"]
    e = raw["r7:stellako_river@7-12"]
    assert "Steelhead Stamp not required" in e["regs_verbatim"]
    assert not any("ST" in (r.get("species") or []) or "steelhead" in r["verbatim"].lower()
                   for r in e.get("rules") or [])
    assert SH.steelhead_row(e) and SH.prints_steelhead(e)
    assert {r for r in steelhead_row_rules(raw) if r.split("::")[0] in outside}


def _class_of(classes, sets, test):
    rules, recs = sets
    return next(k for k in sorted(classes, key=str) if test(k, rules.get(k[0], set()),
                                                            recs.get(k[1], set())))


def _mutated(raw, classes, sets, fn):
    cl, rules, recs, members, rows, own_lakes = _inputs(raw, classes, sets)
    cl, rules, recs = dict(cl), {k: set(v) for k, v in rules.items()}, \
        {k: set(v) for k, v in recs.items()}
    rows = set(rows)
    fn(cl, rules, recs, rows)
    return province_set_problems(cl, rules, recs, members, rows, own_lakes)


@pytest.mark.needs_bundle
@pytest.mark.parametrize("name", [
    "khartoum loses the annual 10", "lois loses the lake stamp", "capilano loses the record duty",
    "a steelhead stream loses the stamp", "stave lake gains the annual 10",
    "a region 4 stream gains the wild release", "a steelhead section with no presence code",
    "tenas gains the wild release", "a stream past the regions loses the stamp twin",
    "a steelhead row's water is not known", "a possible stream outside the regions",
    "a listed region 8 stream gains the annual 10",
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
        k = _class_of(classes, sets, lambda k, r, q: k[2] is None and k[3] is None and any(
            x.startswith("z4:") for x in r) and not any(x.split(":", 1)[0] in STEELHEAD_ZONES
                                                       for x in r))
        rules[k[0]].add("zp:steelhead::steelhead.r2")

    def beyond(cl, rules, recs, rows):
        k = _class_of(classes, sets, lambda k, r, q: k[3] == 1 and k[2] is None and not any(
            x.split(":", 1)[0] in STEELHEAD_ZONES for x in r)
            and f"zp:steelhead#{STAMP_TWIN}" in q)
        recs[k[1]].discard(f"zp:steelhead#{STAMP_TWIN}")

    def not_known(cl, rules, recs, rows):
        k = _class_of(classes, sets, lambda k, r, q: k[3] == 1 and r & rows)
        cl[(k[0], k[1], k[2], 2)] = cl.pop(k)

    def possible_outside(cl, rules, recs, rows):
        k = _class_of(classes, sets, lambda k, r, q: k[2] is None and k[3] is None and any(
            x.startswith("z4:") for x in r))
        cl[(k[0], k[1], k[2], 2)] = cl.pop(k)

    def listed_gains(cl, rules, recs, rows):
        k = _class_of(classes, sets, lambda k, r, q: k[3] == 1 and k[2] is None and not (
            r & rows) and any(x.startswith("z8:") for x in r))
        rules[k[0]].add("zp:steelhead::steelhead.r1b")

    def tenas_gains(cl, rules, recs, rows):
        k = _class_of(classes, sets, lambda k, r, q: k[2] == TENAS)
        rules[k[0]].add("zp:steelhead::steelhead.r2b")

    def no_code(cl, rules, recs, rows):
        k = _class_of(classes, sets, lambda k, r, q: k[3] == 2)
        cl[(k[0], k[1], k[2], None)] = cl.pop(k)

    fns = {"khartoum loses the annual 10": drop("zp:steelhead::steelhead.r1b", KHARTOUM),
           "lois loses the lake stamp": drop(f"zp:steelhead#{STAMP_TWIN}", LOIS),
           "capilano loses the record duty": capilano,
           "a steelhead stream loses the stamp": stream_stamp,
           "stave lake gains the annual 10": stave,
           "a region 4 stream gains the wild release": region4,
           "a steelhead section with no presence code": no_code,
           # FIX D13: Tenas Lake is NOT steelhead water (its own row prints none); the set there is
           # a defect even though the Atnarko's closure binds it
           "tenas gains the wild release": tenas_gains,
           "a stream past the regions loses the stamp twin": beyond,
           "a steelhead row's water is not known": not_known,
           "a listed region 8 stream gains the annual 10": listed_gains,
           "a possible stream outside the regions": possible_outside}
    assert _mutated(raw, classes, sets, fns[name])


@pytest.mark.needs_bundle
def test_a_row_outside_the_regions_naming_steelhead_needs_the_set(raw, classes, sets):
    """No exemption any more (user ruling 2026-10-02: known waters carry the set wherever they are):
    a Region 4 stream whose row named steelhead must be known and carry the whole provincial set —
    the check reports it until the reach run gives it them."""
    def fn(cl, rules, recs, rows):
        k = _class_of(classes, sets, lambda k, r, q: k[3] is None and k[2] is None
                      and any(x.startswith("r4:") for x in r))     # a STREAM (a lake: own row only)
        rows |= {x for x in rules[k[0]] if x.startswith("r4:")}
    got = _mutated(raw, classes, sets, fn)
    assert any("is missing" in g for g in got) and any("not known" in g for g in got), got


# ----------------------------------------------------------------------- the same, on the export
def _export_bad(doc):
    bad = {k: doc[k] for k in ("rules", "licensing", "entries", "waters")}
    bad["rulesets"] = copy.deepcopy(doc["rulesets"])
    bad["licensing_sets"] = copy.deepcopy(doc["licensing_sets"])
    return bad


@pytest.mark.needs_bundle
def test_the_export_carries_the_provincial_set_on_every_steelhead_row_s_water(doc):
    assert X.steelhead_set_problems(doc) == []
    for item in (KHARTOUM, LOIS):
        p = doc["waters"][item]["parts"][0]
        rules = X._members(doc["rulesets"], p["ruleset"], doc["rules"])
        recs = X._members(doc["licensing_sets"], p["licensing_set"], doc["licensing"])
        assert X.province_set_missing(rules, recs, doc["rules"], doc["licensing"]) == []
        assert f"zp:steelhead#{STAMP_TWIN}" in recs


@pytest.mark.needs_bundle
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


@pytest.mark.needs_bundle
@pytest.mark.parametrize("item,where,add,expect", [
    ("gnis:16880", "ruleset", "zp:steelhead::steelhead.r1", "no stream of the steelhead regions"),
    ("wbk:329291805", "licensing_set", STAMP, "not known steelhead water"),     # Stave Lake
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


@pytest.mark.needs_bundle
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
        doc["waters"][k["water"]["item_id"]]["steelhead_rows"])
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
    # 2026-10-06 (FIX D13): a lake a steelhead row binds by ANOTHER line is NOT steelhead water
    # (Tenas Lake); a known stream past the regions still carries the set
    other, beyond = (got["lake_bound_by_a_steelhead_rows_other_line"],
                     got["steelhead_known_beyond_the_regions"])
    assert other["water"]["kind"] == "lake" and "steelhead" not in other
    assert other["water"]["item_id"] not in STEELHEAD_ROW_LAKES
    assert not any(X.is_steelhead_rule(doc["rules"][a["id"]]) for a in other["expect"]
                   if a["id"] in doc["rules"])
    assert not any(i.startswith("zp:steelhead#") for i in other["expect_licensing"])
    assert (beyond["steelhead"], beyond["water"]["kind"], beyond.get("anadromous_rainbow")) == (
        "known", "stream", True)
    for c in (beyond,):
        speaks = {a["id"] for a in c["expect"] if a["state"] == "speaks"}
        assert "zp:steelhead::steelhead.r1b" in speaks or "zp:steelhead::steelhead.r2b" in speaks, \
            (c["mechanism"], speaks)
        assert f"zp:steelhead#{STAMP_TWIN}" in c["expect_licensing"], c["mechanism"]
    # 2026-10-02 (revised): a water only the curated list makes known — no steelhead rule speaks;
    # 2026-10-03: no steelhead rule applies, so a rainbow of any size is a rainbow
    lst = got["steelhead_known_by_the_list"]
    assert (lst["steelhead"], lst["water"]["kind"], lst.get("anadromous_rainbow"), lst["fish"]) == (
        "known", "stream", None, "RB")
    assert X.STEELHEAD_NO_RULES in lst["what_to_show"]
    if lst["water"]["item_id"] == OKANAGAN:
        assert any(a["id"] == "r8:okanagan_river@8-1::okanagan_river.r5" and a["state"] == "speaks"
                   for a in lst["expect"]), lst["expect"]
    assert doc["waters"][lst["water"]["item_id"]]["steelhead_source"] == ["curated list"]
    assert not any(X.is_steelhead_rule(doc["rules"][a["id"]]) for a in lst["expect"]
                   if a["id"] in doc["rules"])
    assert not any(i.startswith("zp:steelhead#") for i in lst["expect_licensing"])
    for m in ("steelhead_known", "steelhead_possible", "lake_no_steelhead",
              "own_row_steelhead_lake", "lake_bound_by_a_steelhead_rows_other_line",
              "steelhead_known_beyond_the_regions", "steelhead_known_by_the_list"):
        assert got[m]["what_to_show"] and m in doc["guide"]["cases"]["mechanisms"]


@pytest.mark.needs_bundle
def test_steelhead_rules_false_is_exactly_the_known_parts_no_steelhead_rule_applies_to(doc):
    """`steelhead_rules: false` (user ruling 2026-10-03) is on EVERY known part that does not carry
    the provincial steelhead rules (`steelhead_rules_apply`) and on no other part; a water carries it
    exactly when it is known and no part carries the rules. The Okanagan River, Inkaneep and Vaseux
    creeks carry it whole; the Fraser carries it on its Zone 7A parts only (steelhead rules apply
    on its other parts). Mutation: the check refuses it moved either way."""
    W = doc["waters"]
    # DF4 (2026-10-08): every part with a rule set says whether the steelhead rules apply, true
    # or false — the bundle's answer, the one `steelhead_rules_apply` re-reads off the rule set
    for i, w in W.items():
        for p in w["parts"]:
            if p["ruleset"] is not None:
                assert p["steelhead_rules"] is X.steelhead_rules_apply(doc, p), (i, p)
            else:
                assert "steelhead_rules" not in p, i
    flagged = {(i, n) for i, w in W.items() for n, p in enumerate(w["parts"])
               if p.get("steelhead_rules") is False and p.get("steelhead") == "known"}
    want = {(i, n) for i, w in W.items() for n, p in enumerate(w["parts"])
            if p.get("steelhead") == "known" and not X.steelhead_rules_apply(doc, p)}
    assert flagged == want and flagged
    assert {i for i, w in W.items() if w.get("steelhead_rules") is False} == {
        OKANAGAN, INKANEEP, "gnis:29653"}
    fraser = W["gnis:39325"]
    assert "steelhead_rules" not in fraser
    assert any(p.get("steelhead_rules") is False for p in fraser["parts"])
    assert any(X.steelhead_rules_apply(doc, p) for p in fraser["parts"])
    for p in fraser["parts"]:
        if p.get("steelhead_rules") is False:
            assert not p.get("anadromous_rainbow")
    assert X.STEELHEAD_NO_RULES == ("Steelhead have been recorded here, but no steelhead rule "
                                    "applies: treat any rainbow, however big, as a rainbow trout.")
    bad = copy.deepcopy(doc)
    bad["waters"]["gnis:39325"]["parts"][next(n for n, p in enumerate(fraser["parts"])
                                              if X.steelhead_rules_apply(doc, p)
                                              and p.get("steelhead") == "known")][
        "steelhead_rules"] = False
    assert any("steelhead_rules False" in x for x in X.steelhead_presence_problems(bad))
