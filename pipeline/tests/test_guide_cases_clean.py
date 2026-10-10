"""THE CLEAN ROUND'S GUIDE CASES AND TEXTS (2026-10-05), read from the export model.

A consumer review found: no guide case for RU-3; the two-regions text saying "equal rules both
speak" against RU-8 (identical statements shown once); and the 60 cm contradiction — the ladder
text for a zone release limited to a water kind said a size clause of another dimension "stays
beside" it while the RU-4 text implied a release with no water kind empties it. The user ruled
(2026-10-05, option A): a zone size clause made moot by ANY outright release or closure of the
fish is not shown (`read` step 5b, `ladder.moot_size_clause`); the texts say so and these tests
pin both the words and the reader answers they cite.

Every case's `expect` is the reference reader's (`read.effective_rules`); each test re-asks it.
`UI_EXPORT_BUNDLE` points the suite at a side bundle.
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.bundle import read as RD
from pipeline.tools import export_ui_rules as X
from pipeline.tests.conftest import need, BUNDLE_HINT

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or X.BUNDLE)

Z3 = "z3:trout_char_quota::trout_char_quota"
#: mechanism -> (water item_id, date, fish, ids that speak, ids that must be absent)
NEW = {
    "same_row_dated_release": ("gnis:39492", "05-15", "RB",
                               ["r3:thompson_river_downstream_of_signs_at_kamloops_lake_outlet_t"
                                "@3-13+3-14+3-18::thompson_river_downstream_of_kamloops_lake.r3"],
                               ["r3:thompson_river_downstream_of_signs_at_kamloops_lake_outlet_t"
                                "@3-13+3-14+3-18::thompson_river_downstream_of_kamloops_lake.r2"]),
    "same_row_dated_closure": ("wbk:329303451", "12-15", "RB", ["r2:alta_lake@2-9::alta_lake.r1"],
                               ["r2:alta_lake@2-9::alta_lake.r2"]),
    "zone_release_any_water": (None, "05-15", "GR", ["z7b:arctic_grayling::arctic_grayling.r4"],
                               ["z7b:arctic_grayling::arctic_grayling.r1",
                                "z7b:arctic_grayling::arctic_grayling.r3"]),
    "size_release_vs_size_clause": (None, "07-01", "RB",
                                    ["r6:lakelse_lake@6-11::lakelse_lake.r1",
                                     "z6:trout_char_quota::trout_char_quota.r1"],
                                    ["z6:trout_char_quota::trout_char_quota.r2"]),
    "closure_any_key": (None, "05-01", "CT", ["z4:spring_stream_closure::spring_stream_closure.r1"],
                        ["z4:trout_char_quota::trout_char_quota.r1",
                         "z4:trout_char_quota::trout_char_quota.r2",
                         "z4:trout_char_quota::trout_char_quota.r3"]),
    "two_regions_same_statement": (None, "07-01", "RB",
                                   ["z5:trout_char_quota::trout_char_quota.r1",
                                    "z5:trout_char_quota::trout_char_quota.r2"],
                                   ["z7a:trout_char_quota::trout_char_quota.r1",
                                    "z7a:trout_char_quota::trout_char_quota.r2"]),
    "zone_size_clause_moot": ("wbk:329054723", "11-01", "LT",
                              [f"{Z3}.r7"], [f"{Z3}.r4b", f"{Z3}.r1", f"{Z3}.r4"]),
    "water_release_silences_size_clause": ("wbk:329518152", "11-01", "LT",
                                           ["r3:griffin_lake@3-34::griffin_lake.r1"],
                                           [f"{Z3}.r7", f"{Z3}.r4b", f"{Z3}.r1"]),
    "bridge_lake_same_statement": ("wbk:329060770", "07-01", "LT",
                                   ["r5:bridge_lake@5-2::bridge_lake.r1",
                                    "z5:trout_char_quota::trout_char_quota.r1"],
                                   ["z5:trout_char_quota::trout_char_quota.r5"]),
    "bridge_lake_dated_zone_release": ("wbk:329060770", "11-15", "LT",
                                       ["r5:bridge_lake@5-2::bridge_lake.r1",
                                        "z5:trout_char_quota::trout_char_quota.r7"], []),
    "alta_lake_clause_size_release": ("wbk:329303451", "07-01", "RB",
                                      ["r2:alta_lake@2-9::alta_lake.r2",
                                       "r2:alta_lake@2-9::alta_lake.r3",
                                       "z2:trout_char_quota::trout_char_quota.r2"],
                                      ["z2:trout_char_quota::trout_char_quota.r1"]),
}


@pytest.fixture(scope="module")
def doc(request) -> dict:
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    return X.build(BUNDLE)


@pytest.fixture(scope="module")
def by_mech(doc) -> dict:
    out: dict = {}
    for c in doc["guide"]["cases"]["cases"]:
        out.setdefault(c["mechanism"], []).append(c)
    return out


def _sid(case) -> int:
    """The section the case is answered on: the first of its water carrying its part's sets."""
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    try:
        got = db.execute(
            "SELECT MIN(s.sid) FROM item i JOIN item_section s ON s.ord = i.ord "
            "LEFT JOIN section_ruleset r ON r.sid = s.sid "
            "LEFT JOIN section_licensing l ON l.sid = s.sid WHERE i.item_id = ? "
            "AND r.set_id IS ? AND l.set_id IS ?",
            (case["water"]["item_id"], None if case["ruleset"] is None else int(case["ruleset"]),
             None if case["licensing_set"] is None else int(case["licensing_set"]))).fetchone()
    finally:
        db.close()
    return got[0]


@pytest.mark.needs_bundle
@pytest.mark.parametrize("mech", sorted(NEW))
def test_every_new_mechanism_has_a_clean_case(by_mech, mech):
    """RU-3 (the Thompson's CNR stretch in May, Alta Lake's winter closure), RU-4, RU-5, RU-7,
    RU-8, the two 60 cm cases and the two lakes the consumer asked about each have a case whose
    `expect` shows exactly what the mechanism claims."""
    item, date, fish, speak, absent = NEW[mech]
    cases = by_mech.get(mech)
    assert cases, f"no case for {mech}"
    c = cases[0]
    if item:
        assert c["water"]["item_id"] == item
    assert (c["date"], c["fish"]) == (date, fish)
    speaking = {e["id"] for e in c["expect"] if e["state"] == "speaks"}
    ids = {e["id"] for e in c["expect"]}
    assert set(speak) <= speaking, (mech, sorted(set(speak) - speaking))
    assert not set(absent) & ids, (mech, sorted(set(absent) & ids))
    assert set(speak) | set(absent) <= set(c["because"])


@pytest.mark.needs_bundle
@pytest.mark.parametrize("mech", sorted(NEW))
def test_the_expect_is_the_reference_readers(by_mech, mech):
    """A case is the reference semantics, never a hand-written answer: re-asked, the reader gives
    the same rules in the same states."""
    c = by_mech[mech][0]
    m, d = (int(x) for x in c["date"].split("-"))
    got = [{"id": f"{x['entry']}::{x['rule']}", "state": x["state"],
            **({"partly_lifted": True} if x.get("partly_lifted") else {})}
           for x in RD.effective_rules(_sid(c), (m, d), c["fish"], str(BUNDLE))]
    assert got == c["expect"]


@pytest.mark.needs_bundle
def test_the_thompson_cnr_stretch_speaks_its_2_only_after_the_spring_closure(by_mech):
    """The `same_row_release` text's months, re-asked: May the release, June the zone's spring
    stream closure (strict lift ruling), July to September the row's 2."""
    c = by_mech["same_row_dated_release"][0]
    sid = _sid(c)
    two = "thompson_river_downstream_of_kamloops_lake.r2"

    def speaks(on):
        return {x["rule"] for x in RD.effective_rules(sid, on, "RB", str(BUNDLE))
                if x["state"] == "speaks"}
    assert two not in speaks((5, 15)) and two not in speaks((6, 15))
    assert "spring_stream_closure.r1" in speaks((6, 15))
    assert two in speaks((7, 1)) and two in speaks((9, 30))


# ---- the texts ----------------------------------------------------------------------------
@pytest.mark.needs_bundle
def test_the_two_regions_text_states_ru8(doc):
    t = doc["guide"]["ladder"]["two_regions"]
    assert "equal rules both speak" not in t
    assert "shown once" in t and "two_regions_same_statement" in t


@pytest.mark.needs_bundle
def test_the_60cm_texts_say_what_the_reader_does(doc):
    """The three ladder texts send the size clause to `moot_size_clause` (not shown), and none
    says it stays beside any more."""
    L = doc["guide"]["ladder"]
    for k in ("zone_release_by_water", "zone_release_any_water", "closure_any_key"):
        assert "none under 60 cm" in L[k] and "moot_size_clause" in L[k], k
        assert "stays beside" not in L[k].lower(), k
    m = L["moot_size_clause"]
    assert "superior" in m and "hatchery-only" in m and "no gear in the water" in m


@pytest.mark.needs_bundle
def test_eleven_mile_creek_hides_the_60cm_clause_under_the_zone_release_and_closure():
    """Region 3, no row. Aug 1, a bull trout: the zone's 'from streams' release speaks and 'none
    under 60 cm' is not shown; May 1, a lake trout: the spring stream closure, likewise."""
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    try:
        sid = db.execute("SELECT MIN(s.sid) FROM item i JOIN item_section s ON s.ord = i.ord "
                         "WHERE i.item_id = 'gnis:8623'").fetchone()[0]   # the Region 3 one
    finally:
        db.close()
    assert sid is not None

    def speaks(on, fish):
        return {f"{x['entry']}::{x['rule']}" for x in RD.effective_rules(sid, on, fish, str(BUNDLE))
                if x["state"] == "speaks"}
    aug, may = speaks((8, 1), "DV"), speaks((5, 1), "LT")
    assert f"{Z3}.r6" in aug and f"{Z3}.r4b" not in aug
    assert "z3:spring_stream_closure::spring_stream_closure.r1" in may and f"{Z3}.r4b" not in may


@pytest.mark.needs_bundle
def test_the_set_ids_and_splits_notes_ship(doc):
    F = doc["field_dictionary"]["file"]
    assert "VALID ONLY WITHIN ONE BUNDLE DIGEST" in F["rulesets"] and "set_keys" in F["rulesets"]
    assert "set_keys" in F and "NOT SHIPPED" in F["set_keys"]
    assert "LAKE EDGE ships only where" in F["splits"] and "25,423" in F["splits"]
    assert "set_keys" in doc["field_dictionary"]["encoding"]
    assert "match sets across exports" in doc["guide"]["how_to_read"]["ids"]
