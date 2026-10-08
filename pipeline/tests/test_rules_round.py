"""RULES round (2026-10-07): the user's answers to handoff/QUESTIONS.md and the audit's FIX BATCH.

One test (or a few) per ruling or fix class, on real waters, read the way a consumer reads them:
the catalogue for what the book says, the reader (`read.effective_rules`) for who speaks, and a
bundle (`UI_EXPORT_BUNDLE`, else the shipped one) for where a rule binds. The important rulings
carry a MUTATION: the policy flag switched off (or the field put back) must turn the test red.
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.bundle import read as R
from pipeline.regs.parsing import catalogue as C

ROOT = Path(__file__).resolve().parents[2]
CAT = ROOT / "data/curated/regulations/entries/catalogue"
BUNDLE = os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE


def _entries(region: str) -> dict:
    return {e["entry_id"]: e for e in json.loads((CAT / f"region-{region}.json").read_text())["entries"]}


def _rule(region: str, entry_id: str, rule_id: str) -> dict:
    got = [x for x in _entries(region)[entry_id].get("rules") or [] if x["rule_id"] == rule_id]
    assert len(got) == 1, f"{entry_id}::{rule_id}"
    return got[0]


def _walks(region: str, entry_id: str, rule_id: str) -> bool:
    """The rule's tributary scope, three-valued (AGENTS 9): its own field, else its entry's."""
    e = _entries(region)[entry_id]
    r = _rule(region, entry_id, rule_id)
    v = r.get("includes_tributaries")
    return bool(e.get("includes_tributaries")) if v is None else bool(v)


@pytest.fixture(scope="module")
def db():
    if not os.path.isfile(BUNDLE):
        pytest.skip("no bundle")
    con = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    yield con
    con.close()


def _sections(db, item_id: str, entry_id: str, rule_id: str, via: str | None = None) -> list[int]:
    q = ("SELECT DISTINCT s.sid FROM item i JOIN item_section s ON s.ord = i.ord "
         "JOIN section_ruleset sr ON sr.sid = s.sid JOIN ruleset r ON r.set_id = sr.set_id "
         "WHERE i.item_id = ? AND r.entry_id = ? AND r.rule_id = ?")
    args = [item_id, entry_id, rule_id]
    if via:
        q += " AND r.via = ?"
        args.append(via)
    return sorted(x[0] for x in db.execute(q + " ORDER BY s.sid", args))


def _states(sid: int, day, fish: str) -> dict:
    return {f"{x['entry']}::{x['rule']}": (x["state"], x.get("reason")) for x in
            R.effective_rules(sid, day, fish, BUNDLE, trace=True)}


# ---------------------------------------------------------------------------------------------
# Q3 — an exemption printed on a ✱ row never reaches the row's tributaries
# ---------------------------------------------------------------------------------------------
Q3_LIFTS = [("1", "r1:nitinat_river@1-4", "nitinat_river.r1"),
            ("1", "r1:quinsam_river@1-6", "quinsam_river.r3"),
            ("4", "r4:duncan_river@4-19", "duncan_river.r1"),
            ("4", "r4:duncan_river@4-19", "duncan_river.r2"),
            ("4", "r4:lardeau_river@4-29+4-30", "lardeau_river.r2"),
            ("4", "r4:lardeau_river@4-29+4-30", "lardeau_river.r3"),
            ("4", "r4:dutch_creek@4-26", "dutch_creek.r4"),
            ("6", "r6:hevenor_mcqueen_creek@6-30", "hevenor_creek.r3"),
            ("6", "r6:fulton_river@6-8", "fulton_river.r1b"),
            ("6", "r6:babine_river@6-8", "babine_river.r3"),
            ("8", "r8:similkameen_river@8-2", "similkameen_river.r3")]


@pytest.mark.parametrize("region,entry_id,rule_id", Q3_LIFTS)
def test_q3_a_lift_on_a_starred_row_stays_on_its_water(region, entry_id, rule_id):
    assert "Incl. Tribs" in (_entries(region)[entry_id].get("symbols") or [])
    assert _rule(region, entry_id, rule_id).get("exempts")
    assert _walks(region, entry_id, rule_id) is False


def test_q3_every_lift_on_a_starred_row_is_listed():
    """No other rule of a ✱ row both lifts and walks (Quatse r4x walks because its bait ban is a
    regulation the ✱ carries — the one printed exception the audit kept)."""
    walking = []
    for f in sorted(CAT.glob("region-*.json")):
        for e in json.loads(f.read_text())["entries"]:
            if "Incl. Tribs" not in (e.get("symbols") or []):
                continue
            for r in e.get("rules") or []:
                v = r.get("includes_tributaries")
                walks = bool(e.get("includes_tributaries")) if v is None else v
                # a LIFT-ONLY rule (no number, no sizes); a quota or size window that lifts the zone's
                # number beside it (Duncan's 5, Lonzo's 20-30 cm) is a regulation the ✱ carries
                if r.get("exempts") and walks and r.get("type") == "retention_limit" \
                        and r.get("take") is None and not r.get("unlimited") and not r.get("lengths"):
                    walking.append(f"{e['entry_id']}::{r['rule_id']}")
    assert walking == [], walking


# ---------------------------------------------------------------------------------------------
# Q4 (as corrected 2026-10-07): a row whose NAME carries the global ✱ includes tributaries for EVERY
# rule, even when a clause repeats a ✱; only a row WITHOUT the global ✱ narrows to the clause
# printing one (p.4)
# ---------------------------------------------------------------------------------------------
GLOBAL_WITH_INLINE = [
    ("1", "r1:san_juan_river@1-3", ["san_juan_river.r1", "san_juan_river.r2"]),
    ("1", "r1:sooke_river@1-2", ["sooke_river.r1", "sooke_river.r2", "sooke_river.r3"]),
    ("2", "r2:inland_lake@2-12", ["inland_lake.r1", "inland_lake.r2", "inland_lake.r3", "inland_lake.r4"]),
    ("4", "r4:bull_river@4-22", ["bull_river.r1", "bull_river.r2", "bull_river.r3"]),
    ("6", "r6:kitsumkalum_kalum_river@6-15", ["kitsumkalum_river.r1", "kitsumkalum_river.r2",
                                              "kitsumkalum_river.r5"]),
]


@pytest.mark.parametrize("region,entry_id,rules", GLOBAL_WITH_INLINE)
def test_q4_a_global_star_row_with_a_repeated_inline_star_walks_every_rule(region, entry_id, rules):
    """San Juan: "SAN JUAN RIVER ✱ … No Fishing upstream of Fleet River, No Fishing July 15-Aug 31✱"
    — the global ✱ wins; the repeated ✱ narrows nothing. MUTATION: the row without its global ✱
    reads only its own entry flag, which the walk then lacks."""
    e = _entries(region)[entry_id]
    assert "Incl. Tribs" in (e.get("symbols") or []) and e.get("includes_tributaries") is True
    for rid in rules:
        assert _walks(region, entry_id, rid), rid
    m = dict(e, includes_tributaries=None)
    assert not any(bool(r.get("includes_tributaries")) for r in m["rules"] if r["rule_id"] in rules)


@pytest.mark.parametrize("region,entry_id,walks,stays", [
    ("1", "r1:oyster_river@1-6", ["oyster_river.r2"], ["oyster_river.r1"]),
    ("1", "r1:anderson_lake@1-3", ["anderson_lake.r4"],
     ["anderson_lake.r1", "anderson_lake.r2", "anderson_lake.r3"]),
    ("7", "r7:dinosaur_lake_reservoir_downstream_of_w_a_c_bennett_dam@7-31", ["dinosaur_lake.r1"],
     ["dinosaur_lake.r2", "dinosaur_lake.r3", "dinosaur_lake.r4"]),
])
def test_q4_no_global_star_the_starred_clause_alone_walks(region, entry_id, walks, stays):
    assert "Incl. Tribs" not in (_entries(region)[entry_id].get("symbols") or [])
    for rid in walks:
        assert _walks(region, entry_id, rid), rid
    for rid in stays:
        assert not _walks(region, entry_id, rid), rid


def test_q4_every_global_star_row_walks_every_rule_it_does_not_scope_in_words():
    """Corpus-wide: on a global-✱ row the only rules that do not walk are lifts (Q3) and rules whose
    own words keep them on the water ("mainstem", a spot, an undrawn part, an area)."""
    import re
    bad = []
    for f in sorted(CAT.glob("region-*.json")):
        for e in json.loads(f.read_text())["entries"]:
            if "Incl. Tribs" not in (e.get("symbols") or []):
                continue
            for r in e.get("rules") or []:
                if r.get("includes_tributaries") is not False or r.get("exempts") \
                        or r.get("undrawn_part") or r.get("within"):     # a clause follows its quota
                    continue
                if re.search(r"mainstem|main channel|signs|bridge|falls|dam|within|radius|tunnel|"
                             r"pool|weir|fence|outlet|park|channel|lake|on parts|reservoir",
                             r.get("verbatim") or "", re.I):
                    continue
                bad.append(f"{e['entry_id']}::{r['rule_id']} {r['verbatim'][:60]!r}")
    assert bad == [], bad


# ---------------------------------------------------------------------------------------------
# Q5 — a tributary with its own row: both apply, its own row beats what it inherits
# ---------------------------------------------------------------------------------------------
GRANBY, KETTLE_T = "r8:granby_river@8-15", "r8:kettle_river_s_tributaries@8-14"


def _granby_upper(db) -> int:
    got = [s for s in _sections(db, "gnis:18775", GRANBY, "granby_river.r3")
           if s in _sections(db, "gnis:18775", KETTLE_T, "kettle_river_s_tributaries.r2", "trib")]
    if not got:
        pytest.skip("the bundle has no Granby section above Burrell Creek walked by Kettle's tributaries")
    return got[0]


def test_q5_the_kettle_walk_still_reaches_granby(db):
    """No exclusion (user): Granby River's sections carry Kettle River's tributaries' rules."""
    assert _sections(db, "gnis:18775", KETTLE_T, "kettle_river_s_tributaries.r1", "trib")


def test_q5_own_row_beats_the_inherited_release(db):
    """Upper Granby, Jun 20, rainbow: its own 'trout/char daily quota = 1' speaks; the inherited
    'Rainbow trout catch and release' (naming the fish) is displaced by the own row. MUTATION:
    OWN_ROW_BEATS_INHERITED off -> naming wins again and the inherited release speaks."""
    sid = _granby_upper(db)
    got = _states(sid, (6, 20), "RB")
    assert got[f"{GRANBY}::granby_river.r3"][0] == "speaks"
    assert got[f"{KETTLE_T}::kettle_river_s_tributaries.r2"] == ("displaced", "own_row")
    old = R.OWN_ROW_BEATS_INHERITED
    try:
        R.OWN_ROW_BEATS_INHERITED = False
        got = _states(sid, (6, 20), "RB")
        assert got[f"{KETTLE_T}::kettle_river_s_tributaries.r2"][0] == "speaks"
        assert got[f"{GRANBY}::granby_river.r3"][0] == "displaced"
    finally:
        R.OWN_ROW_BEATS_INHERITED = old


def test_q5_an_inherited_closure_still_closes(db):
    """Aug 1 (inside 'No Fishing Jul 25-Sept 15' of Kettle River's tributaries): the inherited
    closure speaks and the own quota is silent — an own row never out-ranks a closure."""
    sid = _granby_upper(db)
    got = _states(sid, (8, 1), "RB")
    assert got[f"{KETTLE_T}::kettle_river_s_tributaries.r1"][0] == "speaks"
    assert got[f"{GRANBY}::granby_river.r3"][0] == "displaced"


# ---------------------------------------------------------------------------------------------
# Q9 — a fish exactly on a printed size bound is legal
# ---------------------------------------------------------------------------------------------
def _keep(bands, cm, take=None):
    for b in bands:
        if b.holds(cm):
            return b.take if b.take is not None else take
    return None


def test_q9_none_under_x_keeps_x():
    floor = [C.LengthBand(max_cm=30, take=0)]                     # z7a/z7b "lake trout under 30"
    assert _keep(floor, 30) is None and _keep(floor, 29.9) == 0
    shared = [C.LengthBand(min_cm=60), C.LengthBand(max_cm=60, take=0)]   # "1 bull trout over 60"
    assert _keep(shared, 60, 1) == 1
    reverse = [C.LengthBand(max_cm=60, take=0), C.LengthBand(min_cm=60)]  # order no longer matters
    assert _keep(reverse, 60, 1) == 1


def test_q9_no_trout_over_x_keeps_x_and_or_more_does_not():
    ceiling = [C.LengthBand(min_cm=50, take=0)]                   # "no trout over 50 cm"
    assert _keep(ceiling, 50) is None and _keep(ceiling, 50.1) == 0
    or_more = [C.LengthBand(min_cm=40, take=0, closed=True)]      # "40 cm or more"
    assert _keep(or_more, 40) == 0 and _keep(or_more, 39.9) is None


def test_q9_mutation_inclusive_bounds_deny_x(monkeypatch):
    monkeypatch.setattr(C, "EXACT_BOUND_IS_LEGAL", False)
    assert _keep([C.LengthBand(max_cm=30, take=0)], 30) == 0


def test_q9_closed_is_only_true_and_only_on_a_release_band():
    with pytest.raises(ValueError):
        C.LengthBand(min_cm=40, closed=True)
    with pytest.raises(ValueError):
        C.LengthBand(min_cm=40, take=0, closed=False)


@pytest.mark.parametrize("region,entry_id,rule_id", [
    ("2", "r2:inland_lake@2-12", "inland_lake.r3"), ("2", "r2:khartoum_lake@2-12", "khartoum_lake.r2"),
    ("2", "r2:lois_lake@2-12", "lois_lake.r2"), ("2", "r2:ruby_lake@2-5", "ruby_lake.r2"),
    ("2", "r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4",
     "chilliwack_vedder_rivers.r6")])
def test_q9_the_books_or_more_or_less_bands_are_closed(region, entry_id, rule_id):
    r = _rule(region, entry_id, rule_id)
    assert " or more" in r["verbatim"] or " or less" in r["verbatim"]
    assert any(b.get("closed") for b in r["lengths"] if b.get("take") == 0)


def test_q9_no_other_rule_prints_or_more_or_less_on_a_release_band():
    import re
    pat = re.compile(r"\d+\s*cm\s+or\s+(more|less)", re.I)
    for f in sorted(CAT.glob("region-*.json")):
        for e in json.loads(f.read_text())["entries"]:
            for r in e.get("rules") or []:
                rel = [b for b in r.get("lengths") or [] if b.get("take") == 0]
                if rel and pat.search(r.get("verbatim") or "") and not any(b.get("closed") for b in rel):
                    # "the other 2 must be 60 cm or less" (Atlin, Laidlaw) bounds a KEEP band
                    assert "must be" in r["verbatim"], f"{e['entry_id']}::{r['rule_id']}"


# ---------------------------------------------------------------------------------------------
# §6/§7 — no record quotes the extraction's markup
# ---------------------------------------------------------------------------------------------
def test_no_record_verbatim_carries_markup():
    bad = []
    for f in sorted(CAT.glob("region-*.json")):
        for e in json.loads(f.read_text())["entries"]:
            for g in ("rules", "licensing", "see"):
                for x in e.get(g) or []:
                    if C.EXTRACTION_MARKUP.search(x.get("verbatim") or ""):
                        bad.append(f"{e['entry_id']} {g} {x.get('rule_id') or x.get('id')}")
    assert bad == []


def test_clean_verbatim_reads_like_the_book():
    assert C.clean_verbatim("**No Fishing** upstream of Bannon Creek[Includes Tributaries] July 1-Sept 30") \
        == "No Fishing upstream of Bannon Creek July 1-Sept 30"
    assert C.clean_verbatim("No Fishing** in all parts[Includes Tributaries], Dec 1-May 31") \
        == "No Fishing in all parts, Dec 1-May 31"
    assert C.clean_verbatim("Class II water**[Includes Tributaries]** Apr 1-May 31") \
        == "Class II water Apr 1-May 31"


def test_the_model_refuses_a_marked_quote():
    e = _entries("2")["r2:inland_lake@2-12"]
    bad = json.loads(json.dumps(e))
    bad["rules"][0]["verbatim"] = "**No Fishing** Nov 1-Mar 31"
    with pytest.raises(ValueError, match="markup"):
        C.CatalogueEntry.model_validate(bad)
    C.CatalogueEntry.model_validate(e)


# ---------------------------------------------------------------------------------------------
# Q17 Q26 Q29 Q32 Q46 — per-water answers
# ---------------------------------------------------------------------------------------------
def test_q17_coquihalla_fly_only_is_upstream_of_the_tunnel_jul_to_oct():
    r = _rule("2", "r2:coquihalla_river@2-17", "coquihalla_river.r2")
    assert r["extents"] == [{"op": "upstream_of",
                             "splits": ["coquihalla_river__coquihalla_othello_upper_tunnel"]}]
    assert r["when"] == {"dates": [{"from_month": 7, "from_day": 1, "to_month": 10, "to_day": 31}]}


def test_q26_bowron_park_waters_lift_no_spring_closure():
    e = _entries("5")["r5:bowron_lake_park_waters_other_than_bowron_lake@5-16"]
    assert not [r for r in e["rules"] if r.get("exempts")]


def test_q29_heffley_parts_are_a_note_on_the_whole_lake():
    for rid in ("heffley_lake.r1", "heffley_lake.r2"):
        r = _rule("3", "r3:heffley_lake_parts_of@3-27", rid)
        assert r["extents"] == [{"op": "whole"}] and r.get("undrawn_part")


def test_q32_nilkitkwa_fly_only_does_not_bind_set_lining():
    g = _rule("6", "r6:nilkitkwa_lake@6-8", "nilkitkwa_lake.r1")["gear"]
    assert g == [{"slot": "method", "only": ["fly_fishing"], "unless": [{"method": "set_lining"}]}]


def test_q46_region_5_fraser_closures_are_clipped_to_region_5():
    for rid in ("fraser_river.r1", "fraser_river.r6"):
        ext = _rule("5", "r5:fraser_river@5-2", rid)["extents"]
        assert all(x.get("within_area") == "area:region:5" for x in ext), rid


# ---------------------------------------------------------------------------------------------
# Q45 as CLARIFIED (user, 2026-10-07): trout includes char UNLESS (a) the line excludes char in so
# many words, or (b) the row / zone table specifies char separately with a RELATED rule of its own
# (same aspect — retention, size or gear — same kind of water, days that meet).
# ---------------------------------------------------------------------------------------------
def _cr(**kw):
    base = {"rule_id": "x.r1", "type": "retention_limit", "verbatim": "v", "extents": [{"op": "whole"}]}
    return C.CatalogueRule.model_validate({**base, **kw})


SIZE_TROUT = dict(rule_id="x.r1", verbatim="no trout under 30 cm", species=["TROUT_CHAR"],
                  lengths=[{"max_cm": 30, "take": 0}])
DV_RELEASE_DATED = dict(rule_id="x.r2", verbatim="bull trout release Aug 1-Oct 31", species=["DV"],
                        take=0, may_target=True,
                        when={"dates": [{"from_month": 8, "from_day": 1, "to_month": 10, "to_day": 31}]})


def test_q45_an_unrelated_char_rule_leaves_char_in_the_trout_size_limit():
    """The user's example: "no trout under 30 cm" + "bull trout release Aug 1-Oct 31" — the release
    dates do not touch the size limit, so the 30 cm limit still covers bull trout."""
    t, c = _cr(**SIZE_TROUT), _cr(**DV_RELEASE_DATED)
    assert not C.related_rules(t, c)
    assert C.trout_scope_problems("r9:x", "", [t, c]) == []
    bad = _cr(**SIZE_TROUT, species_except=["CHAR"])          # MUTATION: excluding char is refused
    assert C.trout_scope_problems("r9:x", "", [bad, c])


def test_q45_a_related_char_rule_makes_the_trout_line_trout_only():
    """Region 1: "2 from streams (must be hatchery)" beside the table's "All char" release — both
    govern how many you keep: trout only. MUTATION: drop the exclusion -> refused."""
    rules = {x["rule_id"]: x for x in _entries("1")["z1:trout_quota"]["rules"]}
    r4 = C.CatalogueRule.model_validate(rules["trout_quota.r4"])
    r7 = C.CatalogueRule.model_validate(rules["trout_quota.r7"])
    assert C.related_rules(r4, r7) and "CHAR" in r4.species_except
    all_rules = [C.CatalogueRule.model_validate(x) for x in rules.values()]
    assert not [p for p in C.trout_scope_problems("z1:trout_quota", "", all_rules) if "trout_quota.r4" in p]
    mutated = [C.CatalogueRule.model_validate({**rules["trout_quota.r4"], "species_except": []})
               if x.rule_id == "trout_quota.r4" else x for x in all_rules]
    assert [p for p in C.trout_scope_problems("z1:trout_quota", "", mutated) if "trout_quota.r4" in p]


def test_q45_explicit_exclusion_in_the_text_counts():
    t = _cr(rule_id="x.r1", verbatim="no trout (not char) under 30 cm", species=["TROUT_CHAR"],
            species_except=["CHAR"], lengths=[{"max_cm": 30, "take": 0}])
    assert C.trout_scope_problems("r9:x", "", [t]) == []


@pytest.mark.parametrize("region,entry_id,rule_id,trout_only", [
    ("2", CHILLIWACK := "r2:chilliwack_lake@2-4", "chilliwack_lake.r1", True),   # 1 bull trout over 60: size
    ("2", "r2:alta_lake@2-9", "alta_lake.r3", True),                              # 1 char none under 60: size
    ("3", "r3:coldwater_river@3-13", "coldwater_river.r2", False),               # size vs bull trout release
    ("3", "r3:nicola_river@3-13", "nicola_river.r3", True),                       # release vs bull trout release
    ("6", "z6:trout_char_quota", "trout_char_quota.r6", False),                    # size vs char releases
    ("6", "z6:trout_char_quota", "trout_char_quota.r7", True),                     # stream release vs DV stream release
])
def test_q45_corpus_reading_by_relatedness(region, entry_id, rule_id, trout_only):
    r = _rule(region, entry_id, rule_id)
    assert ("CHAR" in (r.get("species_except") or [])) is trout_only


# ---------------------------------------------------------------------------------------------
# Q6 Q7 Q8 Q12 Q18 Q19 Q21 Q24 Q25 Q28 and the FIX BATCH extents (catalogue)
# ---------------------------------------------------------------------------------------------
def test_q6_squamish_excepted_rivers_creeks_stay_closed():
    hits = [r for e in _entries("2").values() if e["entry_id"].startswith("r2:squamish")
            for r in e.get("rules") or [] if r.get("tributary_excludes")]
    assert hits and all(t.get("walk_past") for r in hits for t in r["tributary_excludes"])


def test_q7_campbell_tributaries_from_strathcona_dam():
    r = _rule("1", "r1:campbell_river@1-10", "campbell_river.r4")
    assert r["tributaries_only"] and r["extents"] == [
        {"op": "downstream_of", "splits": ["campbell_river__strathcona_dam"]}]
    assert r["tributary_excludes"] == [{"op": "whole", "item_id": "gnis:27883"}]     # Quinsam


def test_q8_lower_michel_itself_is_carved_out():
    for n in range(1, 5):
        tx = _rule("4", "r4:elk_river_s_tributaries_see_exceptions@4-2+4-23",
                   f"elk_river_s_tributaries.r{n}")["tributary_excludes"]
        mine = [t for t in tx if t.get("item_id") == "gnis:28953"]
        # lower Michel itself is excepted; its tributaries (and the creek above the bridge) are
        # still Elk River tributaries — the walk goes past it (user correction 2026-10-07)
        assert mine == [{"op": "downstream_of", "item_id": "gnis:28953",
                         "splits": ["michel_creek__easternmost_hwy_3"], "walk_past": True},
                        {"op": "upstream_of", "item_id": "gnis:28953",
                         "splits": ["michel_creek__easternmost_hwy_3"]}]


@pytest.mark.parametrize("region,entry_id,rule_id,want", [
    ("1", "r1:puntledge_river@1-6", "puntledge_river.r4",
     "puntledge_river__signs_75_m_downstream_of_puntledge_river_hatchery_fence"),
    ("1", "r1:campbell_river@1-10", "campbell_river.r3", "campbell_river__maple_street_boundary_sign"),
    ("1", "r1:somass_river@1-7", "somass_river.r1", "somass_river__tidal_boundary_at_papermill_dam"),
    ("5", "r5:quesnel_river@5-2", "quesnel_river.r1", "quesnel_river__signs_50_m_downstream_of_likely_bridge"),
])
def test_fix_gauge_bounds_are_the_books_signs(region, entry_id, rule_id, want):
    sp = [s for x in _rule(region, entry_id, rule_id)["extents"] for s in x.get("splits") or []]
    assert want in sp and not [s for s in sp if s.startswith("gauge__")]


def test_no_rule_bounds_a_reach_by_a_gauge_where_the_book_prints_signs():
    """After the FIX batch no `between` whose sentence prints signs ends at a hydrometric gauge (a
    gauge may still pin a named place the book prints, e.g. Donald on the Columbia)."""
    bad = []
    for f in sorted(CAT.glob("region-*.json")):
        for e in json.loads(f.read_text())["entries"]:
            for r in e.get("rules") or []:
                for x in r.get("extents") or []:
                    if x.get("op") == "between" and "sign" in (r.get("verbatim") or "").lower() \
                            and any(s.startswith("gauge__") for s in x.get("splits") or []):
                        bad.append(f"{e['entry_id']}::{r['rule_id']}")
    assert bad == [], bad


def test_q18_baker_creek_one_split_at_the_parks_downstream_crossing():
    sp = "baker_creek__pinnacles_park_downstream_boundary"
    e = "r5:baker_creek@5-13"
    assert _rule("5", e, "baker_creek.r1")["extents"] == [{"op": "upstream_of", "splits": [sp]}]
    for rid in ("baker_creek.r2", "baker_creek.r3", "baker_creek.r4"):
        assert _rule("5", e, rid)["extents"] == [{"op": "downstream_of", "splits": [sp]}]


def test_q19_shuswap_river_catch_and_release_on_the_whole_river():
    r = [x for x in _entries("8").values() if x["entry_id"].startswith("r8:shuswap_river")][0]
    cr = [x for x in r["rules"] if x["rule_id"].endswith(".r3")][0]
    assert cr["extents"] == [{"op": "whole"}]


def test_q21_wap_lake_is_outside_the_frog_falls_closure():
    e = [x for x in _entries("8").values() if x["entry_id"].startswith("r8:wap_creek")][0]
    r1 = [x for x in e["rules"] if x["rule_id"] == "wap_creek.r1"][0]
    assert r1["extents"][0]["outside_items"] == ["wbk:329663971"]


def test_q24_a_row_printing_also_in_mu_binds_in_that_region():
    """"CANIM RIVER (also in M.U. 5-15)": the row's own words make it a Region 5 row too
    (`outside.region_limit`); the entry id stays the synopsis row's (3-46). MUTATION: drop the
    name's MU and the row is held to Region 3."""
    from pipeline.atlas.reach import outside as O
    e = _entries("3")["r3:canim_river_also_in_m_u_5_15@3-46"]
    got = set(O._ALSO_IN.findall(e["name"]))
    assert got == {"5-15"}
    assert not O._ALSO_IN.findall("CANIM RIVER")


def test_q25_seton_canal_is_part_of_the_seton_row():
    e = [x for x in _entries("3").values() if x["entry_id"].startswith("r3:seton_river")][0]
    assert "wbk:328988335" in e["matched"]


def test_q28_chilko_5_kmh_is_a_note_on_the_whole_river():
    r = _rule("5", "r5:chilko_river@5-5", "chilko_river.r6")
    assert r["extents"] == [{"op": "whole"}] and r["undrawn_part"]


def test_fix_upper_arrow_drawdown_is_the_columbia_reach_and_a_lake_note():
    e = "r4:upper_arrow_lake_drawdown_area@4-31+4-32"
    r1 = _rule("4", e, "upper_arrow_lake_drawdown_area.r1")
    assert r1["extents"][0]["item_id"] == "gnis:37414" and not r1.get("undrawn_part")
    assert _rule("4", e, "upper_arrow_lake_drawdown_area.r1b")["undrawn_part"]


def test_fix_strathcona_park_powered_boats_binds_less_the_three_lakes():
    r = _rule("1", "r1:strathcona_park_waters@1-9", "strathcona_park_waters.r1")
    assert not r.get("unresolved_locators")
    assert {"wbk:329161786", "wbk:329069967", "wbk:329069966"} <= set(r["extents"][0]["outside_items"])


# ---------------------------------------------------------------------------------------------
# FIX §5 and the provincial rulings (catalogue)
# ---------------------------------------------------------------------------------------------
def _prov() -> dict:
    return _entries("provincial")


def test_q40_white_sturgeon_closed_everywhere_but_the_licensed_fraser():
    """p.7: catch and release ONLY in the Fraser watershed (tributaries included) from the Mission CPR
    Bridge to and including Williams Lake River; closed everywhere else (user 2026-10-07). The
    closure is province-wide and r4x lifts it on exactly the licence requirement's extents."""
    e = _prov()["zp:white_sturgeon_licence"]
    rs = {x["rule_id"]: x for x in e["rules"]}
    r4, r4x = rs["white_sturgeon_licence.r4"], rs["white_sturgeon_licence.r4x"]
    assert r4["species"] == ["WSG"] and r4["take"] == 0 and r4["may_target"] is False
    assert r4["extents"] == [{"op": "within", "area_kind": "region"}]
    lic = next(x for x in e["licensing"] if x["id"] == "white_sturgeon_licence")
    assert r4x["extents"] == lic["extents"] and r4x["exempts"][0]["target"] == "white_sturgeon_licence.r4"


def test_dead_fin_fish_for_sturgeon_where_p8_lists_it():
    """p.8 (a): "when sport fishing for sturgeon in Region 2 only on the Fraser River, Lower Pitt
    River (CPR Bridge upstream to Pitt Lake), Lower Harrison River (Fraser River upstream to Harrison
    Lake)"."""
    r3 = _rule("provincial", "zp:bait", "bait.r3")
    assert r3["when_targeting"] == ["WSG"]
    assert r3["extents"] == [
        {"op": "whole", "item_id": "gnis:39325", "within_area": "area:region:2"},
        {"op": "between", "splits": ["pitt_river__cpr_bridge", "pitt_river__pitt_lake"], "item_id": "gnis:7551"},
        {"op": "downstream_of", "splits": ["harrison_river__harrison_lake"], "item_id": "gnis:15333"}]


@pytest.mark.parametrize("region,entry_id", [("1", "z1:trout_quota"), ("2", "z2:trout_char_quota")])
def test_q44_at_most_two_over_50_in_total(region, entry_id):
    rs = {x["rule_id"]: x for x in _entries(region)[entry_id]["rules"]}
    tot = [x for x in rs.values() if x.get("take") == 2 and x.get("within")
           and x.get("lengths") == [{"min_cm": 50}] and not x.get("origin")]
    assert len(tot) == 1, "one clause caps every fish over 50 cm at 2"


@pytest.mark.parametrize("region", ["1", "2", "3", "4"])
def test_fix_possession_quota_entries_exist(region):
    e = _entries(region)[f"z{region}:possession_quota"]
    assert any(x.get("period") == "possession" for x in e["rules"])


@pytest.mark.parametrize("entry_id,parent", [("r2:khartoum_lake@2-12", "khartoum_lake.r5"),
                                             ("r2:lois_lake@2-12", "lois_lake.r4")])
def test_fix_khartoum_lois_wild_steelhead_count_none(entry_id, parent):
    kids = [x for x in _entries("2")[entry_id]["rules"] if x.get("within") == parent]
    assert any(x["species"] == ["ST"] and x.get("origin") == "wild" and x.get("take") == 0 for x in kids)


def test_fix_lonzo_window_lifts_the_hatchery_under_30_release():
    r = _rule("2", "r2:lonzo_marshall_creek@2-4", "lonzo_creek.r2")
    assert {"entry_id": "z2:trout_char_quota", "target": "trout_char_quota.r8"}.items() <= r["exempts"][0].items()


# ---------------------------------------------------------------------------------------------
# An undrawn-part rule never walks tributaries (user ruling 2026-10-07, Dinosaur Lake)
# ---------------------------------------------------------------------------------------------
def test_an_undrawn_part_rule_does_not_walk(monkeypatch):
    """Dinosaur Lake's "No Fishing from W.A.C. Bennett Dam to 100 m south of Gething Creek and between
    the anti-vortex dyke and Peace Canyon Dam✱" is held on the whole lake as a note; walking from
    there closed the tributaries of the WHOLE lake (1,109 km). MUTATION: the flag off -> it walks."""
    from pipeline.atlas.reach import classify as K
    e = _entries("7")["r7:dinosaur_lake_reservoir_downstream_of_w_a_c_bennett_dam@7-31"]
    r1 = _rule("7", e["entry_id"], "dinosaur_lake.r1")
    assert r1["undrawn_part"] and r1["includes_tributaries"] is True
    assert K.wants_tributaries(r1, e) is False
    monkeypatch.setattr(K, "UNDRAWN_PART_DOES_NOT_WALK", False)
    assert K.wants_tributaries(r1, e) is True


def test_the_rancheria_alternative_reaches_the_basin_streams():
    e = _entries("6")["r6:rancheria_river_s_tributaries@6-25"]
    assert e["matched"] == ["gnis:11267"]                       # the Little Rancheria (user: correct)
    alt = next(x for x in e["licensing"] if x["kind"] == "alternative")
    # the extent names the drainage it selects from (`item_id`), so it is not cut down to the row's
    # own water: every B.C. stream of the Rancheria basin in Region 6 (FWA has no Rancheria
    # mainstem in B.C.). MUTATION: without `item_id` it resolves to the Little Rancheria alone.
    assert alt["extents"] == [{"op": "within", "area_id": "area:basin:200-692231-770914-",
                               "within_area": "area:region:6", "feature_types": ["stream"],
                               "item_id": "area:basin:200-"}]


def test_export_pins_match_the_corpus():
    """The export's pinned lists (the bundle carries no tributary scope) equal the corpus."""
    from pipeline.tools import export_ui_rules as X
    from pipeline.atlas.reach import classify as K
    undrawn, excepted = set(), set()
    for f in sorted(CAT.glob("region-*.json")):
        for e in json.loads(f.read_text())["entries"]:
            for r in e.get("rules") or []:
                if (r.get("undrawn_part") or "").strip():
                    v = r.get("includes_tributaries")
                    if (e.get("includes_tributaries") if v is None else v) or r.get("tributaries_only"):
                        undrawn.add(f"{e['entry_id']}::{r['rule_id']}")
                if any(t.get("walk_past") and "squamish" in e["entry_id"]
                       for t in r.get("tributary_excludes") or []):
                    excepted.add(e["entry_id"])
    assert undrawn == set(X.UNDRAWN_PART_TRIBUTARIES)
    assert excepted == set(X.EXCEPTED_RIVERS_CREEKS_CLOSED)
    assert K.UNDRAWN_PART_DOES_NOT_WALK
