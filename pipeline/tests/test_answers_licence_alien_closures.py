"""T789 G8 (owner 2026-11-17: "Should each of these waters just have their own info? Don't want this on waters it
doesn't apply to"; Lakelse and Telkwa "Same thing applies to these"): THE CLOSURES TO THE NON-RESIDENT ALIEN, PER PART
— `licence_frame.alien_closures: [{rule, of, says}]`, only on a part whose rule set holds one.

  1  `licence.alien_candidates` reads each rule set's `angler_closure`s naming the non-resident alien off the bundle's
     `ruleset` binding: by export index; `of` the water where the set holds it by the tributary walk, else null; `says`
     the whole sentence — the rule's line ("all year" where it has no dates: the book prints it on every such row),
     "<water> and its tributaries: <line>" on a tributary; no other angler's, no other rule type, no set without one.
     `licence.alien_closures` keeps those that speak in a stored verdict of one of the set's rule keys
     (`VerdictStore.speaking`, the verdicts the Keep tab caveat reads): none the ladder drops (the Bulkley's under the
     Telkwa's own row). The live build, end to end (`section_scope`, `representatives`, `section_prepare`): Lakelse part
     2 and Skeena part 14 say theirs, Lakelse part 0 and Skeena part 12 nothing; the Bulkley's closure is never said
     beside a nearer row's.
  2  Such a part's rule set is in its licence key (`part_licence_key`), so its frame and only its frame carries them
     (`alien_of`); a part of the same licensing set without one keeps the plain key.
  3  The wire: the frame's 4th index (`encode.LicenceCodec`) into its own table, decoded back; the region shard reaches
     the row (`shards.frame_refs`); the model refuses it on tidal water and an empty list.
No data (but the live build's checks): CI's no-data tier.
"""
from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from pipeline.deliver.answers import licence as L
from pipeline.deliver.answers.common import TIDAL_STATE
from pipeline.deliver.answers.encode import CODECS
from pipeline.deliver.answers.model import AlienClosure, LicenceFrame
from pipeline.deliver.release import shards
from pipeline.tests import under_test

ALIEN = {"residency": ["non_resident_alien"], "guidance": ["non_guided"]}
WHAT = "Angling closed to non-guided non-resident aliens"
SKEENA = (f"{WHAT}, Jul 1-Dec 31, on Saturdays and Sundays — between the Shegunia River confluence and the Sedan Creek "
          "confluence")
LAKELSE = f"{WHAT}, all year — between Lakelse Lake and power line crossing"


# 1 ---------------------------------------------------------------------------------------------------------------------
def test_the_line_says_all_year_where_no_date_is_printed_and_names_the_water_on_a_tributary():
    lakelse = {"what": WHAT, "where": "between Lakelse Lake and power line crossing"}
    assert L.alien_line(lakelse, None) == LAKELSE
    assert L.alien_says(L.alien_line(lakelse, None), "Lakelse River") == (
        "Lakelse River and its tributaries: Angling closed to non-guided non-resident aliens, all year — between "
        "Lakelse Lake and power line crossing")
    assert L.alien_line({"what": WHAT, "when": "on Saturdays and Sundays"}, {"weekdays": ["Saturday", "Sunday"]}) == (
        "Angling closed to non-guided non-resident aliens, on Saturdays and Sundays, all year")
    # a dated rule says its dates, never "all year"
    dated = {"what": WHAT, "when": "Sep 1-Oct 31"}
    assert L.alien_line(dated, {"dates": [{"from_month": 9, "from_day": 1, "to_month": 10, "to_day": 31}]}) == (
        "Angling closed to non-guided non-resident aliens, Sep 1-Oct 31")
    assert L.alien_says("x", None) == "x"


def test_the_book_prints_all_year_on_every_dateless_closure_to_the_alien():
    """The curated rows (in git): every closure to the alien with no dates prints "all year" (pp.52-53) — what
    `alien_line` adds. Mutation: a dateless row whose words lack it fails here."""
    n, bad = 0, []
    for p in sorted(under_test.curated().regulations.synopsis.glob("region-*.json")):
        for e in json.loads(p.read_text(encoding="utf-8")).get("entries") or []:
            for r in e.get("rules") or []:
                if "non_resident_alien" in ((r.get("closed_to") or {}).get("residency") or []) \
                        and not (r.get("when") or {}).get("dates"):
                    n += 1
                    if "all year" not in (r.get("verbatim") or ""):
                        bad.append(r["rule_id"])
    assert n == 6 and not bad, (n, bad)          # Kitseguecla, Kitsumkalum r1 and r2, Kitwanga, Lakelse, Suskwa


def _db(rules, sets) -> sqlite3.Connection:
    """rules: [(entry, rule, ix, type, closed_to, parts, when)]; sets: {set_id: [(entry, rule, via)]}."""
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE rule (entry_id TEXT, rule_id TEXT, type TEXT, parts TEXT, conditions TEXT, when_ TEXT)")
    db.execute("CREATE TABLE rule_ix (ix INTEGER, entry_id TEXT, rule_id TEXT)")
    db.execute("CREATE TABLE entry (entry_id TEXT, name TEXT)")
    db.execute("CREATE TABLE ruleset (set_id INTEGER, entry_id TEXT, rule_id TEXT, via TEXT)")
    db.execute("CREATE TABLE rule_key (key_ix INTEGER, set_id INTEGER)")
    for e, r, ix, typ, to, parts, when in rules:
        db.execute("INSERT INTO rule VALUES (?, ?, ?, ?, ?, ?)",
                   (e, r, typ, json.dumps(parts), json.dumps({"closed_to": to} if to else {}),
                    None if when is None else json.dumps(when)))
        db.execute("INSERT INTO rule_ix VALUES (?, ?, ?)", (ix, e, r))
    for e in sorted({x[0] for x in rules}):
        db.execute("INSERT INTO entry VALUES (?, ?)", (e, e.split(":")[1].title() + " River"))
    for s, members in sets.items():
        db.execute("INSERT INTO rule_key VALUES (?, ?)", (s * 10, s))              # one rule key per set
        for e, r, via in members:
            db.execute("INSERT INTO ruleset VALUES (?, ?, ?, ?)", (s, e, r, via))
    return db


def everything(keys, rules) -> set:
    """Stored verdicts in which every rule asked speaks."""
    return set(rules)


SAT, SUN = {"weekdays": ["Saturday"]}, {"weekdays": ["Sunday"]}
JUL_DEC = {"dates": [{"from_month": 7, "from_day": 1, "to_month": 12, "to_day": 31}], "weekdays": ["Saturday", "Sunday"]}
RULES = [("r6:skeena", "skeena_river.r5", 2343, "angler_closure", ALIEN,
          {"what": WHAT, "when": "Jul 1-Dec 31, on Saturdays and Sundays",
           "where": "between the Shegunia River confluence and the Sedan Creek confluence"}, JUL_DEC),
         ("r6:lakelse", "lakelse_river.r1", 2257, "angler_closure", ALIEN,
          {"what": WHAT, "where": "between Lakelse Lake and power line crossing"}, None),
         ("r6:kalum", "k.r2", 2226, "angler_closure", ALIEN, {"what": WHAT, "when": "on Sundays"}, SUN),
         ("r6:kalum", "k.r1", 2225, "angler_closure", ALIEN, {"what": WHAT, "when": "on Saturdays"}, SAT),
         ("r2:x", "x.r1", 10, "angler_closure", {"residency": ["non_resident"]}, {"what": "Closed to non-residents"},
          None),
         ("r2:x", "x.r2", 11, "retention_limit", ALIEN, {"what": "not a closure"}, None),
         ("r6:bulkley", "bulkley_river.r1", 2145, "angler_closure", ALIEN, {"what": "Closed (Bulkley)"}, None),
         ("r6:telkwa", "telkwa_river.r1", 2390, "angler_closure", ALIEN, {"what": "Closed (Telkwa)"}, None)]


def test_each_rule_set_holds_its_own_closures_to_the_alien():
    db = _db(RULES, {1: [("r6:skeena", "skeena_river.r5", "reach")],                # the Skeena's stretch
                     2: [("r6:lakelse", "lakelse_river.r1", "reach")],
                     3: [("r6:kalum", "k.r1", "reach"), ("r6:kalum", "k.r2", "reach")],
                     4: [("r6:lakelse", "lakelse_river.r1", "trib")],                # a Lakelse tributary (Herman Creek)
                     5: [("r2:x", "x.r1", "reach"), ("r2:x", "x.r2", "reach")]})   # no alien closure: absent
    assert L.alien_candidates(db) == L.alien_closures(db, everything) == {
        1: [{"rule": 2343, "of": None, "says": SKEENA}],
        2: [{"rule": 2257, "of": None, "says": LAKELSE}],
        3: [{"rule": 2225, "of": None, "says": f"{WHAT}, on Saturdays, all year"},
            {"rule": 2226, "of": None, "says": f"{WHAT}, on Sundays, all year"}],
        4: [{"rule": 2257, "of": "Lakelse River", "says": f"Lakelse River and its tributaries: {LAKELSE}"}]}


def test_another_anglers_closure_is_not_the_aliens():
    # mutation: the same closure to the alien is listed
    db = _db(RULES, {5: [("r2:x", "x.r1", "reach")]})
    assert L.alien_candidates(db) == {}
    db.execute("UPDATE rule SET conditions = ? WHERE rule_id = 'x.r1'", (json.dumps({"closed_to": ALIEN}),))
    assert L.alien_candidates(db) == {5: [{"rule": 10, "of": None, "says": "Closed to non-residents, all year"}]}


def test_a_closure_the_ladder_drops_is_not_said():
    """The Telkwa's own row beats the Bulkley's inherited closure (own row, RULINGS T4): the Keep tab says only the
    Telkwa's, and so does the licence frame. Mutation: a ladder dropping nothing says both."""
    db = _db(RULES, {6: [("r6:bulkley", "bulkley_river.r1", "trib"), ("r6:telkwa", "telkwa_river.r1", "reach")]})
    asked = []

    def verdicts(keys, rules):
        asked.append((list(keys), list(rules)))
        return set(rules) - {2145}                                     # the Bulkley's lost on every verdict
    assert L.alien_closures(db, verdicts) == {6: [{"rule": 2390, "of": None, "says": "Closed (Telkwa), all year"}]}
    assert asked == [([60], [2145, 2390])]                             # the set's own rule keys, its candidates
    assert [x["rule"] for x in L.alien_closures(db, everything)[6]] == [2145, 2390]
    assert L.alien_closures(db, lambda keys, rules: set()) == {}       # none standing: the set says nothing


@pytest.mark.needs_bundle
def test_the_live_bulkley_closure_is_never_said_beside_a_nearer_rows(request):
    from pipeline.deliver.verdicts.store import VerdictStore
    b = under_test.bundle(request)
    db = sqlite3.connect(f"file:{b}?mode=ro", uri=True)
    ix = dict(db.execute("SELECT rule_id, ix FROM rule_ix WHERE entry_id LIKE 'r6:%'"))
    got = L.alien_closures(db, VerdictStore.open(under_test.verdicts(request), b).speaking)
    bulkley = ix["bulkley_river.r1"]
    nearer = {ix["morice_river.r1"], ix["telkwa_river.r1"], ix["suskwa_river.r1"]}
    said = {s: {x["rule"] for x in v} for s, v in got.items()}
    both = [s for s, v in L.alien_candidates(db).items()
            if bulkley in {x["rule"] for x in v} and nearer & {x["rule"] for x in v}]
    assert both                                                        # sets where the nearer row competes
    for s in both:                                                     # there the nearer row's is said, not the Bulkley's
        assert bulkley not in said.get(s, set()) and nearer & said.get(s, set()), s
    assert any(bulkley in v for v in said.values())                    # the Bulkley itself still says its own


@pytest.mark.needs_bundle
def test_the_live_frames_say_it_on_the_parts_it_binds_and_nowhere_else(request):
    """END TO END, as the build does it: each part's key through `section_scope`, a section per licence key through
    `representatives`, the frames through `section_prepare` (the stored verdicts, `ctx.store`). Lakelse part 2 (above the
    power line) and Skeena part 14 (Shegunia-Sedan) say theirs; Lakelse part 0 and Skeena part 12 (Chimdemash to the
    Cedarvale, between the two zones) say nothing. Mutation: `section_scope` without the alien sets, or `key_year` not adding them, and the two that say it
    say nothing."""
    from pipeline.deliver.answers import common
    from pipeline.deliver.bundle.derived import parts
    from pipeline.deliver.verdicts.store import VerdictStore
    b = str(under_test.bundle(request))
    B = common.load(b)
    db = common.connect(b)
    P = parts(db)
    cases = {("gnis:4903", 2): LAKELSE, ("gnis:4903", 0): None, ("gnis:2936", 14): SKEENA, ("gnis:2936", 12): None}
    scope = {}
    for item, i in cases:
        p = P[item][i]
        key = (p.set_id, p.licensing_set, p.steelhead_water, p.steelhead, p.steelhead_rules, p.province_except,
               p.home_regions, p.kind, p.tidal)
        scope[(item, i)] = L.section_scope(key, B)
    ctx = SimpleNamespace(cache={}, bundle=b, licence_reps=L.representatives(b, set(scope.values())),
                          store=VerdictStore.open(under_test.verdicts(request), b))
    for (item, i), want in cases.items():
        _per, values = L.section_prepare(scope[(item, i)], ctx)
        said = {x["says"] for v in values for x in v.get("alien_closures") or []}
        assert said == ({want} if want else set()), (item, i, said)
# 2 ---------------------------------------------------------------------------------------------------------------------
def _part(set_id, licensing_set=7):
    return SimpleNamespace(licensing_set=licensing_set, province_except=(), tidal=False, kind="stream", set_id=set_id)


def test_only_a_part_whose_rule_set_holds_one_carries_it():
    alien = {1: [{"rule": 2343, "says": SKEENA}]}
    on, off = L.part_licence_key(_part(1), set(), alien), L.part_licence_key(_part(9), set(), alien)
    assert on.ruleset == 1 and off.ruleset is None                    # the Skeena stretch's own key; the rest plain
    assert L.alien_of(on, alien) == [{"rule": 2343, "says": SKEENA}]
    assert L.alien_of(off, alien) is None
    # a sleepy designation's set is keyed too, and carries nothing it does not hold
    sleepy = L.part_licence_key(_part(9, licensing_set=3), {3}, alien)
    assert sleepy.ruleset == 9 and L.alien_of(sleepy, alien) is None
    # mutation: without the alien sets the stretch shares the plain key (and so every frame of it, or none)
    assert L.part_licence_key(_part(1), set()) == off


# 3 ---------------------------------------------------------------------------------------------------------------------
AW = [{"rule": 2257, "of": None, "says": LAKELSE}]
U16 = [{"residency": "resident"}]


def test_the_frame_carries_it_as_a_fourth_index_and_decodes_it_back():
    codec = CODECS["licence"]
    values = [{"holds": {"h": 1}, "profiles": [{"p": 1}], "under_16": U16, "alien_closures": AW},
              {"holds": {"h": 1}, "profiles": [{"p": 1}], "under_16": U16},                      # its water's rest
              {"holds": {"tidal": {}}, "profiles": [{"tidal": True}]}]
    tables, refs = codec.encode(values, None, None)
    assert tables["alien_closures"] == [AW]
    assert [tables["frames"][r] for r in refs] == [[0, 0, 0, 0], [0, 0, 0], [1, 1]]
    assert [codec.decode_frame(tables, r, None, None) for r in refs] == values
    assert shards.frame_refs("licence_frame", [0, 0, 0, 0]) == {
        "licence_hold": [0], "licence_doc": [0], "licence_u16": [0], "licence_alien": [0]}
    assert shards.frame_refs("licence_frame", [0, 0, 0]) == {
        "licence_hold": [0], "licence_doc": [0], "licence_u16": [0], "licence_alien": []}
    assert "licence_alien" in shards.SHARD_TABLES


def test_it_never_rides_a_frame_without_the_under_16_answer():
    with pytest.raises(ValueError, match="no under_16"):
        CODECS["licence"].encode([{"holds": {}, "profiles": [], "alien_closures": AW}], None, None)


def test_the_model_refuses_it_on_tidal_water_and_an_empty_list():
    tidal = {"holds": {"tidal": dict(TIDAL_STATE)}, "profiles": [{"tidal": True}] * 60}
    LicenceFrame.model_validate_json(json.dumps(tidal))
    with pytest.raises(ValidationError, match="no closures to the non-resident alien"):
        LicenceFrame.model_validate_json(json.dumps(dict(tidal, alien_closures=AW)))
    with pytest.raises(ValidationError):
        LicenceFrame.model_validate_json(json.dumps(dict(tidal, alien_closures=[])))
    with pytest.raises(ValidationError):
        AlienClosure.model_validate_json(json.dumps({"rule": 1, "says": ""}))
