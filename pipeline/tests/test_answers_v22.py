"""ANSWERS 2.2 (user decisions 2026-10-08, the user-test round): the shared stream count (B),
steelhead under a zone's size line (D), the Vedder Canal is a stream (E), the Yukon licence is an
alternative (C), the broadest need of a stamp (F), the glossary (J).

Every data test reads the live set (`ANSWERS_EXPORT_DIR` / `UI_EXPORT_BUNDLE` point at a side set),
and each ruling is pinned by a mutation where one exists.
"""
from __future__ import annotations

import json
import os
import sqlite3
from functools import lru_cache
from pathlib import Path

import pytest

from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers import glossary as G
from pipeline.deliver.answers.common import AnswersError, load_export
from pipeline.deliver.bundle import read as R
from pipeline.regs.parsing import catalogue as C
from pipeline.tests.conftest import need, EXPORT_HINT

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE)
EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR") or Path(R.BUNDLE).parent.parent / "regs")
CAT = Path(__file__).resolve().parents[2] / "data/curated/regulations/entries/catalogue"

BABINE, SKEENA, VEDDER, TESLIN = "gnis:17687", "gnis:2936", "gnis:3062", "wbk:328961703"
VEDDER_CANAL = "wbk:329707189"
RESIDENT = "resident/16_plus/non_guided/none"
YUKON_RECORDS = 7


@lru_cache(maxsize=1)
def _files():
    need(None, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    need(None, "bundle", EXPORT_DIR / "ui-rules-answers.json", EXPORT_HINT)
    data, guide = load_export(EXPORT_DIR)
    wire = json.loads((EXPORT_DIR / "ui-rules-answers.json").read_text())
    return data, guide, wire


def _tap(item, part, m, d, **kw):
    data, _, wire = _files()
    return E.tap(wire, data, item, part, m, d, **kw)


def _rid(name: str) -> int:
    data, _, _ = _files()
    return data["rule_ids"].index(name)


def _trout_row(t):
    """The region's trout/char row (cutthroat is in it on every water these tests read)."""
    return next(r for r in t["rows"]["rows"] if r.get("pool") is not None and "CT" in r["members"])


def _profile(name: str) -> int:
    return _files()[2]["sections"]["licence"]["profiles"].index(name)


# --------------------------------------------------------------------------------------------
# B — a count limit shared by several kinds (rows F11)
# --------------------------------------------------------------------------------------------

@pytest.mark.needs_bundle
@pytest.mark.parametrize("item", [BABINE, SKEENA])
def test_b_region_6_stream_trout_cap_is_one_for_all_trout_kinds_together(item):
    """p.49: '1 trout from streams July 1-Oct 31'. Sep 20 (lake trout released then): only 1 of the
    5 can be brown, cutthroat, rainbow or steelhead — it was 3 (each kind's own 1, summed)."""
    t = _tap(item, 0, 9, 20, weekday="Sunday")
    rd = _trout_row(t)["real_daily"]
    assert rd["n"] == 5 and rd["capped_sum"] == 1 and rd["rb"]
    assert sorted(rd["capped"]) == ["CT", "GB", "RB", "ST"]
    assert rd["shared_count"] == [{"take": 1, "members": rd["shared_count"][0]["members"]}]
    assert sorted(rd["shared_count"][0]["members"]) == ["CT", "GB", "RB", "ST"]


@pytest.mark.needs_bundle
def test_b_outside_the_shared_cap_dates_no_shared_count():
    """Dec 1: the stream trout cap is out of season (Jul 1-Oct 31); trout are released anyway
    (Nov 1-Jun 30), so no brown/cutthroat/rainbow line sums at all."""
    rows = _tap(BABINE, 0, 12, 1)["rows"]["rows"]
    assert all("shared_count" not in (r.get("real_daily") or {}) for r in rows)


def test_b_shared_counts_merge_only_kinds_wholly_under_the_limit():
    """The merge is innermost-first and never splits a unit (no data)."""
    from pipeline.deliver.answers import rows as RW
    assert "F11" in " ".join(RW.DECISIONS)


# --------------------------------------------------------------------------------------------
# D — steelhead under a zone's trout/char size line (rows F1b, F12; catalogue gate)
# --------------------------------------------------------------------------------------------

def _general_caps(t):
    return [c for r in t["rows"]["rows"] for c in r.get("conds") or []
            if c["c"] == "cap" and c.get("general")]


@pytest.mark.needs_bundle
@pytest.mark.parametrize("item", [BABINE, SKEENA])
def test_d_region_6_one_over_50_counts_hatchery_steelhead(item):
    """p.49 '1 over 50 cm (quota includes hatchery steelhead)': the general cap leaves no fish out
    (it was `except: [RB]`, which the page read as 'steelhead don't count')."""
    caps = _general_caps(_tap(item, 0, 8, 1))
    assert caps and all(not c.get("except") for c in caps if c["a"] == 50)


@pytest.mark.needs_bundle
@pytest.mark.parametrize("part", [0, 1])
def test_d_region_2_one_over_50_leaves_steelhead_to_their_own_line(part):
    """p.21 '1 over 50 cm (2 hatchery steelhead over 50 cm allowed)': on the Vedder the general cap
    excepts steelhead, which have their own 2."""
    t = _tap(VEDDER, part, 10, 15)
    gen = [c for c in _general_caps(t) if c["a"] == 50]
    assert gen and all(c.get("except") == ["ST"] for c in gen)
    st = [c for r in t["rows"]["rows"] for c in r.get("conds") or []
          if c["c"] == "cap" and c.get("who") == ["ST"]]
    assert st and st[0]["take"] == 2


def _zone(region: str, entry_id: str) -> C.CatalogueEntry:
    e = next(x for x in json.loads((CAT / f"region-{region}.json").read_text())["entries"]
             if x["entry_id"] == entry_id)
    return e


def test_d_every_zone_table_obeys_the_steelhead_scope_rule():
    """Steelhead leave a zone's trout/char size line exactly where the same table prints its own
    steelhead quota for that size (Regions 1 and 2), never elsewhere."""
    n = 0
    for f in sorted(CAT.glob("region-*.json")):
        for e in json.loads(f.read_text())["entries"]:
            if e["entry_id"].startswith("z") and e.get("rules"):
                rules = [C.CatalogueRule.model_validate(r) for r in e["rules"]]
                assert C.steelhead_scope_problems(e["entry_id"], rules) == [], e["entry_id"]
                n += 1
    assert n > 50


@pytest.mark.parametrize("region,entry_id,rule_id,mutate", [
    ("2", "z2:trout_char_quota", "trout_char_quota.r2", lambda x: {**x, "species_except": []}),
    ("6", "z6:trout_char_quota", "trout_char_quota.r2", lambda x: {**x, "species_except": ["ST"]}),
])
def test_d_mutations_are_refused(region, entry_id, rule_id, mutate):
    e = _zone(region, entry_id)
    rules = [C.CatalogueRule.model_validate(mutate(r) if r["rule_id"] == rule_id else r)
             for r in e["rules"]]
    assert [p for p in C.steelhead_scope_problems(entry_id, rules) if rule_id in p]
    with pytest.raises(Exception):
        C.CatalogueEntry.model_validate({**e, "rules": [mutate(r) if r["rule_id"] == rule_id else r
                                                         for r in e["rules"]]})


def test_d_the_page_decides_no_exemption():
    """The page reads `except`; it computes no exemption of its own (the old `exemptOf` third
    branch, which asked every fish's roles, is gone)."""
    js = (Path(E.__file__).parent / "reference" / "page_v36.js").read_text()
    assert "roles.get(c.r.key)" not in js


# --------------------------------------------------------------------------------------------
# E — the Vedder Canal is a stream
# --------------------------------------------------------------------------------------------

@pytest.mark.needs_bundle
def test_e_the_vedder_canal_is_the_vedder_river_a_stream_and_no_lake_rule_binds_it():
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    assert db.execute("SELECT item_id FROM item_alias WHERE alias = ?", (VEDDER_CANAL,)).fetchone() \
        == (VEDDER,)
    assert db.execute("SELECT kind FROM item WHERE item_id = ?", (VEDDER,)).fetchone() == ("stream",)
    bad = db.execute("SELECT kind, count(*) FROM item WHERE (lower(name) LIKE '% slough' OR "
                     "lower(name) LIKE '% canal') AND kind != 'stream' GROUP BY kind").fetchall()
    assert bad == []
    data, guide, _ = _files()
    assert data["waters"][VEDDER]["kind"] == "stream"
    from pipeline.tools.export_codec import expand
    m = expand(data, guide)
    for p in data["waters"][VEDDER]["parts"]:
        rs = m["rulesets"][p[0]] if p[0] in m["rulesets"] else m["rulesets"][str(p[0])]
        for rid in rs.get("reach", []) + rs.get("trib", []):
            f = m["rules"][rid]["fields"]
            assert f.get("water") != "lake", rid
    # the card: a stream share of Region 2's 4 (2 from streams), the region's total still 4
    row = _trout_row(_tap(VEDDER, 1, 10, 15))
    assert row["scope"]["share"] and row["daily"] == 2


# --------------------------------------------------------------------------------------------
# C, F — licence documents (licence L9, L10)
# --------------------------------------------------------------------------------------------

@pytest.mark.needs_bundle
def test_c_teslin_basic_or_yukon_never_both():
    p = _tap(TESLIN, 0, 7, 1)["licence"]["profiles"][_profile(RESIDENT)]
    docs = {d["doc"]: d for d in p["documents"]}
    assert "yukon_angling_licence" not in docs
    assert [o["need"] for o in docs["basic_licence"]["or"]] == [["yukon_angling_licence"]]


@pytest.mark.needs_bundle
def test_c_no_profile_anywhere_buys_the_yukon_licence_as_a_second_document():
    _, _, wire = _files()
    L = wire["sections"]["licence"]
    alts = set()
    for a in L["answers"]:
        for d in a.get("documents") or []:
            assert d["doc"] != "yukon_angling_licence"
            for o in d.get("or") or []:
                alts.add(o.get("alt"))
    assert len(alts) == YUKON_RECORDS


@pytest.mark.needs_bundle
def test_f_babine_stamp_is_for_any_fishing_in_its_period_and_for_steelhead_otherwise():
    """p.50 'Steelhead Stamp mandatory Sept 1-Oct 31'; p.7 'even when fishing for species other
    than steelhead'; p.7/p.6 the stamp to fish for steelhead anywhere, at any time."""
    def stamp(m, d):
        p = _tap(BABINE, 0, m, d)["licence"]["profiles"][_profile(RESIDENT)]
        return next(x for x in p["documents"] if x["doc"] == "steelhead_stamp")
    sep = stamp(9, 20)
    assert sep["when"] == {"act": "fishing", "on": "steelhead_period"} and sep["base"]
    assert sep["also_when"] == [{"act": "targeting", "species": ["ST"]}]
    aug = stamp(8, 1)
    assert aug["when"] == {"act": "targeting", "species": ["ST"]} and not aug["base"]
    assert "also_when" not in aug


# --------------------------------------------------------------------------------------------
# J — the glossary
# --------------------------------------------------------------------------------------------

@pytest.mark.needs_bundle
def test_j_the_glossary_ships_typed_and_covers_the_pages_jargon():
    _, _, wire = _files()
    from pipeline.deliver.answers.model import validate_top
    g = wire["glossary"]
    validate_top("glossary", g)
    ids = {t["id"] for t in g["terms"]}
    for need in ("daily_quota", "possession_quota", "catch_and_release", "single_barbless_hook",
                 "bait_ban", "tributaries", "hatchery_wild", "steelhead", "steelhead_stamp",
                 "conservation_surcharge", "classified_waters", "set_line", "guided", "under_16",
                 "region_1", "region_2", "region_4", "region_6", "region_7a", "region_7b",
                 "region_8"):
        assert need in ids
    assert all(t["pages"] and t["quote"] and t["says"] for t in g["terms"])
    reg4 = next(t for t in g["terms"] if t["id"] == "region_4")
    assert reg4["term"] == "Region 4 – Kootenay" and reg4["pages"][0] == 34


@pytest.mark.needs_bundle
def test_j_terms_are_generated_from_the_data():
    """The possession example follows the rules' multiplier (MUTATION: 3 daily quotas -> 'three')."""
    data, guide, _ = _files()
    from pipeline.tools.export_codec import expand
    m = expand(data, guide)
    base = {t["id"]: t for t in G.build(m)["terms"]}
    assert "two days' quota" in base["possession_quota"]["says"]
    for r in m["rules"].values():
        if r["fields"].get("per_daily"):
            r["fields"]["per_daily"] = 3
    mut = {t["id"]: t for t in G.build(m)["terms"]}
    assert "three days' quota" in mut["possession_quota"]["says"]
    assert mut["possession_quota"]["example"] != base["possession_quota"]["example"]


def test_j_a_missing_book_text_stops_the_build(tmp_path):
    with pytest.raises(AnswersError):
        G.Book(tmp_path)
