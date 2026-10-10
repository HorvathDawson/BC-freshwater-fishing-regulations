"""POINTERS ARE NOT RULES (user ruling, 2026-09-25).

A row whose only content is a pointer — '"MARSHALL" CREEK 2-4 See Lonzo Creek' (p.24) — states no
regulation. It was an `advisory` rule quoting the pointer, bound to the water and shown as a rule
(57 rows and clauses). It is now `CatalogueEntry.see`: an edge to the entries it names, validated
(the targets exist, or `unresolved` says why none), carried to the bundle (`entry.see`) and the
export, and binding nothing. What the pointer IS — `alias` (one water, two names), `twin` (one
row printed under two regions), `see` (another water) — is derived, never stored.

Every check here is pinned by breaking the input and watching it refuse (mutation), per
"checks seeded from input launder".
"""
from __future__ import annotations

import copy
import json
import os
import sqlite3

import pytest

from pipeline.regs.parsing.catalogue import CatalogueEntry, See, see_relation
from pipeline.tests.conftest import need, predates, BUNDLE_HINT

from pipeline.deliver.bundle import read as _R

BUNDLE = os.environ.get("UI_EXPORT_BUNDLE") or _R.BUNDLE


@pytest.fixture(scope="module")
def raw():
    from pipeline.regs.parsing.io import read_all_entries
    return read_all_entries()


@pytest.fixture(scope="module")
def corpus(raw):
    return {k: CatalogueEntry.model_validate(v) for k, v in raw.items()}


# --------------------------------------------------------------------------- the model
def test_a_see_names_its_targets_or_says_why_it_names_none():
    See(verbatim="See Lonzo Creek", entry_ids=["r2:lonzo_marshall_creek@2-4"])
    See(verbatim="See sign at trailhead", unresolved="a sign, not a row")
    with pytest.raises(ValueError, match="exactly one"):
        See(verbatim="See Lonzo Creek")
    with pytest.raises(ValueError, match="exactly one"):
        See(verbatim="See Lonzo Creek", entry_ids=["x"], unresolved="and why")


def test_a_pointer_row_is_a_see_and_never_an_advisory_rule(raw):
    m = raw["r2:marshall_creek@2-4"]
    ce = CatalogueEntry.model_validate(m)
    assert ce.pointer_only and not ce.rules
    assert ce.see[0].entry_ids == ["r2:lonzo_marshall_creek@2-4"]
    # MUTATION: the old shape — the pointer as an advisory rule — is refused
    old = copy.deepcopy(m)
    old.pop("see")
    old["rules"] = [{"rule_id": "marshall_creek.r1", "type": "advisory",
                     "verbatim": "See Lonzo Creek", "extents": [{"op": "whole"}]}]
    with pytest.raises(ValueError, match="is a pointer to another row — write it as `see`"):
        CatalogueEntry.model_validate(old)
    # ...and a row with neither rules nor a pointer says nothing
    empty = copy.deepcopy(m)
    empty.pop("see")
    with pytest.raises(ValueError, match="says nothing"):
        CatalogueEntry.model_validate(empty)
    # a pointer quotes the row, and never points at itself
    bad = copy.deepcopy(m)
    bad["see"] = [{"verbatim": "See Lonzo River", "entry_ids": ["r2:lonzo_marshall_creek@2-4"]}]
    with pytest.raises(ValueError, match="not a contiguous substring"):
        CatalogueEntry.model_validate(bad)
    bad["see"] = [{"verbatim": "See Lonzo Creek", "entry_ids": ["r2:marshall_creek@2-4"]}]
    with pytest.raises(ValueError, match="points at its own entry"):
        CatalogueEntry.model_validate(bad)


def test_a_page_pointer_stays_information():
    """"See ice hut warning, page 63" names prose, not a row: it is not refused as a pointer."""
    CatalogueEntry.model_validate({
        "entry_id": "r9:x@9-9", "name": "X", "regs_verbatim": "See ice hut warning, page 63",
        "rules": [{"rule_id": "x.r1", "type": "advisory", "verbatim": "See ice hut warning, page 63",
                   "extents": [{"op": "whole"}]}]})


# --------------------------------------------------------------------------- the corpus
def test_every_pointer_lands(corpus):
    """Every `see` target is an entry of the corpus — the check the bundle build makes too."""
    n = 0
    for eid, ce in corpus.items():
        for s in ce.see:
            n += 1
            assert s.entry_ids, (eid, s.verbatim)
            for t in s.entry_ids:
                assert t in corpus, (eid, t)
    # + South Thompson River, Young Creek (review, 2026-09-25); + Region 4's Kootenay Lake annual
    # quota line, now a pointer to the Main Body row that states it (ruling G, 2026-09-26)
    assert n == 75      # + Kitimat -> zp:steelhead (2026-09-29); + Nahatlatch Lake -> z3:trout_char_quota (Q37, 2026-10-07)


def test_no_pointer_row_binds_a_rule(corpus):
    only = sorted(e for e, ce in corpus.items() if ce.pointer_only)
    assert len(only) == 53    # + z4:kootenay_annual (ruling G)
    for e in only:
        assert not corpus[e].rules and not corpus[e].licensing


@pytest.mark.parametrize("eid,rel", [
    # the 24 "self-pointers" of cross_references.json are aliases: one water, two names
    ("r2:marshall_creek@2-4", "alias"), ("r2:jones_lake@2-3", "alias"),
    ("r4:revelstoke_lake@4-38", "alias"), ("r6:copper_river@6-9", "alias"),
    # another water, governed by the named row
    ("r4:koch_creek@4-16", "see"), ("r1:panther_lake@1-5", "see"),
    ("r6:bulkley_river@6-9", "see"),
    # a tributaries row pointing at its mainstem's row has rules of its own: never an alias
    ("r7:west_road_blackwater_river_s_tributaries@7-10", "see"),
    # the MU 6-1 lakes printed in both Region 5 and Region 6
    ("r5:squirrel_lake@6-1", "twin"), ("r5:naglico_lake@6-1", "twin"),
])
def test_what_a_pointer_is_is_derived_from_the_rows(corpus, eid, rel):
    ce = corpus[eid]
    assert see_relation(ce, [corpus[t] for t in ce.see[0].entry_ids]) == rel


def test_aliases_and_twins_are_counted(corpus):
    from collections import Counter
    got = Counter(see_relation(ce, [corpus[t] for t in s.entry_ids])
                  for ce in corpus.values() for s in ce.see)
    assert got == {"alias": 34, "see": 34, "twin": 7}    # + z4:kootenay_annual (G), + Kitimat -> zp:steelhead, + Nahatlatch (Q37)


def test_a_region_5_copy_of_a_region_6_lake_binds_nothing(corpus):
    """MU 6-1 is Region 6. The seven lakes the Region 5 table ALSO prints (pp.43-47) are the same
    rows as Region 6's (pp.50-54): the Region 5 copy points at its twin and binds nothing, so no
    Region 5 authority ever speaks on a Region 6 lake."""
    for n in ("basalt_lake", "chipmunk_lake", "gatcho_lake", "naglico_lake", "pettry_lake",
              "squirrel_lake", "toms_lake"):
        r5, r6 = corpus[f"r5:{n}@6-1"], corpus[f"r6:{n}@6-1"]
        assert r5.pointer_only and r5.see[0].entry_ids == [r6.entry_id]
        assert r6.rules, n


# --------------------------------------------------------------------------- the bundle
@pytest.fixture(scope="module")
def db(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    con = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    if "see" not in {r[1] for r in con.execute("PRAGMA table_info(entry)")}:
        predates(f"{BUNDLE} predates `entry.see`")
    yield con
    con.close()


@pytest.mark.needs_bundle
def test_the_bundle_carries_the_pointer_and_no_rule_for_it(db):
    see = json.loads(db.execute("select see from entry where entry_id = ?",
                                ("r2:marshall_creek@2-4",)).fetchone()[0])
    assert see == [{"verbatim": "See Lonzo Creek", "entry_ids": ["r2:lonzo_marshall_creek@2-4"],
                    "relation": "alias"}]
    assert db.execute("select count(*) from rule where entry_id = ?",
                      ("r2:marshall_creek@2-4",)).fetchone()[0] == 0
    # every pointer-only row ships no rule rows
    for eid, s in db.execute("select entry_id, see from entry where see is not null"):
        for x in json.loads(s):
            assert x["relation"] in ("alias", "twin", "see")


@pytest.mark.needs_bundle
def test_lonzo_creeks_rules_reach_marshall_creek(db):
    """"MARSHALL" CREEK and LONZO ("Marshall") CREEK are one water (gnis:1860): Lonzo's rules
    cover every section of it, so the pointer needs no rule of its own."""
    sids = [r[0] for r in db.execute("select s.sid from item i join item_section s on s.ord = "
                                     "i.ord where i.item_id = 'gnis:1860'")]
    assert sids
    for sid in sids:
        entries = {r[0] for r in db.execute(
            "select rs.entry_id from section_ruleset sr join ruleset rs on rs.set_id = "
            "sr.set_id where sr.sid = ? and rs.entry_id like 'r%'", (sid,))}
        assert "r2:lonzo_marshall_creek@2-4" in entries, sid
        assert "r2:marshall_creek@2-4" not in entries


def test_a_dangling_pointer_stops_the_build(corpus):
    from pipeline.deliver.bundle import rules as rules_mod
    entries = {k: corpus[k] for k in ("r2:marshall_creek@2-4", "r2:lonzo_marshall_creek@2-4")}
    got = rules_mod._see_column(entries)
    assert json.loads(got["r2:marshall_creek@2-4"])[0]["relation"] == "alias"
    # MUTATION: the target missing from the corpus
    with pytest.raises(SystemExit, match="name an entry the corpus does not hold"):
        rules_mod._see_column({"r2:marshall_creek@2-4": corpus["r2:marshall_creek@2-4"]})


# --------------------------------------------------------------------------- cross-region rows
MARA = "wbk:329518146"


def test_mara_lake_takes_region_3s_base(corpus):
    """MARA LAKE is printed in Region 3 (MU 3-26, "See Shuswap Lake") and in Region 8 (MU 8-26,
    "See Shuswap Lake in Region 3"). One lake, never cut, touching both region polygons, so both
    regions' region-wide tables bound it and tied. USER RULING (2026-09-25): a water takes the zone
    rules of the region it LIES IN — Mara Lake is 61 % Region 3 by area — decided for every
    straddling water by `registry.regions` (see test_region_homes), and the pointer row never moves
    it. So no Region 8 rule takes the lake out by hand (the last round's `outside_items` inference
    is reverted), and Region 8's own Mara Lake row still binds."""
    for eid, ce in corpus.items():
        for r in ce.rules:
            for x in r.extents or []:
                assert MARA not in (x.get("outside_items") or []), (eid, r.rule_id)
    assert corpus["r8:mara_lake@8-26"].rules and corpus["r8:mara_lake@8-26"].see


@pytest.mark.needs_bundle
def test_mara_lake_carries_both_regions_bases_in_the_bundle(db):
    """Mara Lake straddles the Region 3 / Region 8 line (61/39 by area) and is never cut: it takes
    BOTH regions' zone rules, the most strict applying (user ruling 2026-09-25, second half) — and
    a pointer row still never moves it."""
    sids = [r[0] for r in db.execute("select s.sid from item i join item_section s on s.ord = "
                                     "i.ord where i.item_id = ?", (MARA,))]
    assert sids
    for sid in sids:
        zones = {r[0].split(":")[0] for r in db.execute(
            "select rs.entry_id from section_ruleset sr join ruleset rs on rs.set_id = sr.set_id "
            "where sr.sid = ? and rs.entry_id like 'z%' and rs.entry_id not like 'zp:%'", (sid,))}
        assert zones == {"z3", "z8"}, (sid, zones)
        rows = {r[0] for r in db.execute(
            "select rs.entry_id from section_ruleset sr join ruleset rs on rs.set_id = sr.set_id "
            "where sr.sid = ? and rs.entry_id like 'r%'", (sid,))}
        assert "r8:mara_lake@8-26" in rows          # its own "No powered boats" still binds
        assert "r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26" in rows


@pytest.mark.needs_bundle
def test_a_region_6_lake_printed_in_region_5_takes_region_6s_base(db):
    """Chipmunk Lake (MU 6-1) is printed in both tables; only Region 6's row and Region 6's base
    bind it."""
    sids = [r[0] for r in db.execute("select s.sid from item i join item_section s on s.ord = "
                                     "i.ord where i.item_id = 'wbk:329126718'")]
    for sid in sids:
        entries = {r[0] for r in db.execute(
            "select rs.entry_id from section_ruleset sr join ruleset rs on rs.set_id = sr.set_id "
            "where sr.sid = ?", (sid,))}
        assert {e.split(":")[0] for e in entries if e[0] in "zr" and not e.startswith("zp")} \
            == {"z6", "r6"}, entries
