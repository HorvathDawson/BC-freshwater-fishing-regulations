"""T789 G8 (owner 2026-11-17: "Should each of these waters just have their own info? Don't want this on waters it
doesn't apply to"; Lakelse and Telkwa "Same thing applies to these"): THE CLOSURES TO THE NON-RESIDENT ALIEN, PER PART
— `licence_frame.alien_closures: [{rule, says}]`, only on a part whose rule set holds one.

  1  `licence.alien_closures` reads each rule set's `angler_closure`s naming the non-resident alien off the bundle's
     `ruleset` binding (the Keep tab caveat's rows): by export index, its label as `says`; no other angler's, no other
     rule type, no set without one.
  2  Such a part's rule set is in its licence key (`part_licence_key`), so its frame and only its frame carries them
     (`alien_of`); a part of the same licensing set without one keeps the plain key.
  3  The wire: the frame's 4th index (`encode.LicenceCodec`) into its own table, decoded back; the region shard reaches
     the row (`shards.frame_refs`); the model refuses it on tidal water and an empty list.
No data: CI's no-data tier.
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

ALIEN = {"residency": ["non_resident_alien"], "guidance": ["non_guided"]}
SKEENA = ("Angling closed to non-guided non-resident aliens, Jul 1-Dec 31, on Saturdays and Sundays — between the "
          "Shegunia River confluence and the Sedan Creek confluence")
LAKELSE = "Angling closed to non-guided non-resident aliens — between Lakelse Lake and power line crossing"


# 1 ---------------------------------------------------------------------------------------------------------------------
def _db(rules, sets) -> sqlite3.Connection:
    """rules: [(entry, rule, ix, type, closed_to, label)]; sets: {set_id: [(entry, rule)]}."""
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE rule (entry_id TEXT, rule_id TEXT, type TEXT, label TEXT, conditions TEXT)")
    db.execute("CREATE TABLE rule_ix (ix INTEGER, entry_id TEXT, rule_id TEXT)")
    db.execute("CREATE TABLE ruleset (set_id INTEGER, entry_id TEXT, rule_id TEXT, via TEXT)")
    for e, r, ix, typ, to, label in rules:
        db.execute("INSERT INTO rule VALUES (?, ?, ?, ?, ?)",
                   (e, r, typ, label, json.dumps({"closed_to": to} if to else {})))
        db.execute("INSERT INTO rule_ix VALUES (?, ?, ?)", (ix, e, r))
    for s, members in sets.items():
        for e, r in members:
            db.execute("INSERT INTO ruleset VALUES (?, ?, ?, 'reach')", (s, e, r))
    return db


RULES = [("r6:skeena", "skeena_river.r5", 2343, "angler_closure", ALIEN, SKEENA),
         ("r6:lakelse", "lakelse_river.r1", 2257, "angler_closure", ALIEN, LAKELSE),
         ("r6:kalum", "k.r2", 2226, "angler_closure", ALIEN, "Angling closed to non-guided non-resident aliens, on Sundays"),
         ("r6:kalum", "k.r1", 2225, "angler_closure", ALIEN, "Angling closed to non-guided non-resident aliens, on Saturdays"),
         ("r2:x", "x.r1", 10, "angler_closure", {"residency": ["non_resident"]}, "Closed to non-residents"),
         ("r2:x", "x.r2", 11, "retention_limit", ALIEN, "not a closure")]


def test_each_rule_set_holds_its_own_closures_to_the_alien():
    db = _db(RULES, {1: [("r6:skeena", "skeena_river.r5")],                       # the Skeena's stretch
                     2: [("r6:lakelse", "lakelse_river.r1")],
                     3: [("r6:kalum", "k.r1"), ("r6:kalum", "k.r2")],              # Saturdays on a part, Sundays all
                     4: [("r6:kalum", "k.r2")],
                     5: [("r2:x", "x.r1"), ("r2:x", "x.r2")]})                     # no alien closure: absent
    assert L.alien_closures(db) == {
        1: [{"rule": 2343, "says": SKEENA}],
        2: [{"rule": 2257, "says": LAKELSE}],
        3: [{"rule": 2225, "says": "Angling closed to non-guided non-resident aliens, on Saturdays"},
            {"rule": 2226, "says": "Angling closed to non-guided non-resident aliens, on Sundays"}],
        4: [{"rule": 2226, "says": "Angling closed to non-guided non-resident aliens, on Sundays"}]}


def test_another_anglers_closure_is_not_the_aliens():
    # mutation: the same closure to the alien is listed
    db = _db(RULES, {5: [("r2:x", "x.r1")]})
    assert L.alien_closures(db) == {}
    db.execute("UPDATE rule SET conditions = ? WHERE rule_id = 'x.r1'", (json.dumps({"closed_to": ALIEN}),))
    assert L.alien_closures(db) == {5: [{"rule": 10, "says": "Closed to non-residents"}]}


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
AW = [{"rule": 2257, "says": LAKELSE}]
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
