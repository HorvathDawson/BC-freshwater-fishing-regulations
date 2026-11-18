"""T789 G8 (owner 2026-11-17: "yes add to 0.8.0 we can add as banners?"): THE WEEKEND CLOSURES TO NON-GUIDED
NON-RESIDENT ALIENS ON THE LICENCE FRAME — `licence_frame.alien_weekend: {says, waters: [{name, key}]} | null`.

  1  `says` is the book's sentence, printed word for word on every page it is cited from (the repo PDF, no LLM).
  2  `licence.alien_weekend` lists every water whose rules close it to the non-guided alien on a Saturday or Sunday —
     by its entry's matched items, `key` the item_id — and no every-day closure, no other angler's, no weekday-only
     one; a weekend closure the sentence does not say, or one on no water, stops the build.
  3  The curated corpus: every such rule's water prints the sentence's words (a new wording would make `says` wrong).
  4  The wire: the frame's 4th index (`encode.LicenceCodec`) into its own table, decoded back; the region shard reaches
     the row (`shards.frame_refs`); the model refuses it on tidal water and a water listed twice.
No data: CI's no-data tier.
"""
from __future__ import annotations

import json
import sqlite3

import pytest
from pydantic import ValidationError

from pipeline.common.synopsis_print import quoted
from pipeline.deliver.answers import licence as L
from pipeline.deliver.answers.common import TIDAL_STATE, AnswersError
from pipeline.deliver.answers.encode import CODECS
from pipeline.deliver.answers.model import AlienWeekend, LicenceFrame
from pipeline.deliver.release import shards
from pipeline.tests import under_test

ALIEN = {"residency": ["non_resident_alien"], "guidance": ["non_guided"]}
WEEKEND = {"weekdays": ["Saturday", "Sunday"]}


# 1 ---------------------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("page", L.ALIEN_WEEKEND_PAGES)
def test_says_is_printed_word_for_word_on_every_page_it_cites(page):
    assert quoted(L.ALIEN_WEEKEND_SAYS, [page])


def test_a_changed_sentence_is_not_the_books():
    # mutation: a day the book does not print
    assert not quoted(L.ALIEN_WEEKEND_SAYS.replace("Sundays", "Mondays"), L.ALIEN_WEEKEND_PAGES)


# 2 ---------------------------------------------------------------------------------------------------------------------
def _db(rules, entries=None, items=None) -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE item (item_id TEXT, name TEXT)")
    db.execute("CREATE TABLE entry (entry_id TEXT, item_id TEXT, matched TEXT)")
    db.execute("CREATE TABLE rule (entry_id TEXT, rule_id TEXT, type TEXT, conditions TEXT, when_ TEXT)")
    for i, n in (items or {"gnis:1": "Bulkley River", "gnis:2": "Babine River", "gnis:3": "Zymoetz River",
                           "gnis:4": "Lakelse River", "gnis:5": "Kitsumkalum River"}).items():
        db.execute("INSERT INTO item VALUES (?, ?)", (i, n))
    for e, (item, matched) in (entries or {"r6:bulkley": ("gnis:1", ["gnis:1"]), "r6:babine": ("gnis:2", []),
                                           "r6:zymoetz": ("gnis:3", ["gnis:3"]), "r6:lakelse": ("gnis:4", ["gnis:4"]),
                                           "r6:kalum": ("gnis:5", ["gnis:5"])}).items():
        db.execute("INSERT INTO entry VALUES (?, ?, ?)", (e, item, json.dumps(matched)))
    for e, rid, typ, closed_to, when in rules:
        db.execute("INSERT INTO rule VALUES (?, ?, ?, ?, ?)",
                   (e, rid, typ, json.dumps({"closed_to": closed_to} if closed_to else {}),
                    None if when is None else json.dumps(when)))
    return db


def test_every_weekend_closure_to_the_alien_is_listed_by_name_with_its_item_id():
    db = _db([("r6:bulkley", "b.r1", "angler_closure", ALIEN, dict(WEEKEND, dates=[{"from_month": 9}])),
              ("r6:babine", "a.r1", "angler_closure", ALIEN, WEEKEND),                 # matched [] -> its item_id
              ("r6:zymoetz", "z.r7", "angler_closure", ALIEN, {"weekdays": ["Friday", "Saturday", "Sunday"]}),
              ("r6:zymoetz", "z.r10", "angler_closure", ALIEN, {"weekdays": ["Friday", "Saturday", "Sunday"]}),
              ("r6:kalum", "k.r2", "angler_closure", ALIEN, {"weekdays": ["Sunday"]})])
    assert L.alien_weekend(db) == {
        "says": "Angling prohibited for non-guided non-resident aliens on Saturdays and Sundays",
        "waters": [{"name": "Babine River", "key": "gnis:2"}, {"name": "Bulkley River", "key": "gnis:1"},
                   {"name": "Kitsumkalum River", "key": "gnis:5"}, {"name": "Zymoetz River", "key": "gnis:3"}]}


def test_no_weekday_closure_no_other_angler_and_no_other_rule_is_listed():
    db = _db([("r6:lakelse", "l.r1", "angler_closure", ALIEN, None),                      # every day, all year
              ("r6:lakelse", "l.r2", "angler_closure", ALIEN, {"dates": [{"from_month": 9}]}),   # Telkwa's kind
              ("r6:bulkley", "b.r1", "angler_closure", {"residency": ["non_resident"]}, WEEKEND),
              ("r6:babine", "a.r1", "angler_closure", ALIEN, {"weekdays": ["Friday"]}),
              ("r6:kalum", "k.r1", "retention_limit", ALIEN, WEEKEND)])
    assert L.alien_weekend(db) is None
    # mutation: the same rows with a Saturday in one are listed
    db.execute("UPDATE rule SET when_ = ? WHERE rule_id = 'l.r1'", (json.dumps(WEEKEND),))
    assert [w["name"] for w in L.alien_weekend(db)["waters"]] == ["Lakelse River"]


def test_a_water_matched_by_two_items_lists_both_once():
    db = _db([("r6:skeena", "s.r5", "angler_closure", ALIEN, WEEKEND),
              ("r6:skeena", "s.r6", "angler_closure", ALIEN, WEEKEND)],
             entries={"r6:skeena": ("gnis:9", ["gnis:9", "gnis:8"])},
             items={"gnis:9": "Skeena River", "gnis:8": "Skeena River"})
    assert L.alien_weekend(db)["waters"] == [{"name": "Skeena River", "key": "gnis:8"},
                                             {"name": "Skeena River", "key": "gnis:9"}]


def test_a_weekend_closure_the_sentence_does_not_say_stops_the_build():
    guided_too = {"residency": ["non_resident_alien"]}
    with pytest.raises(AnswersError, match="does not say"):
        L.alien_weekend(_db([("r6:bulkley", "b.r1", "angler_closure", guided_too, WEEKEND)]))
    with pytest.raises(AnswersError, match="on no water"):
        L.alien_weekend(_db([("r6:gone", "g.r1", "angler_closure", ALIEN, WEEKEND)]))


# 3 ---------------------------------------------------------------------------------------------------------------------
def test_every_curated_weekend_closure_to_the_alien_prints_the_books_words():
    syn = under_test.curated().regulations.synopsis
    n, bad = 0, []
    head = L.ALIEN_WEEKEND_SAYS.split(" on ")[0]          # "Angling prohibited for non-guided non-resident aliens"
    for p in sorted(syn.glob("region-*.json")):
        for e in json.loads(p.read_text(encoding="utf-8")).get("entries") or []:
            words = " ".join(r.get("verbatim") or "" for r in e.get("rules") or [])
            for r in e.get("rules") or []:
                to, when = r.get("closed_to") or {}, r.get("when") or {}
                if "non_resident_alien" in (to.get("residency") or []) and {"Saturday", "Sunday"} & set(
                        when.get("weekdays") or []):
                    n += 1
                    if head not in words:
                        bad.append(f"{p.name} {e['entry_id']} {r['rule_id']}")
    assert n >= 10, f"only {n} weekend closures to the alien in the corpus — the corpus moved"
    assert not bad, f"waters whose rows do not print {head!r}: {bad}"


# 4 ---------------------------------------------------------------------------------------------------------------------
AW = {"says": L.ALIEN_WEEKEND_SAYS, "waters": [{"name": "Babine River", "key": "gnis:17687"}]}
U16 = [{"residency": "resident"}]


def test_the_frame_carries_it_as_a_fourth_index_and_decodes_it_back():
    codec = CODECS["licence"]
    values = [{"holds": {"h": 1}, "profiles": [{"p": 1}], "under_16": U16, "alien_weekend": AW},
              {"holds": {"h": 2}, "profiles": [{"p": 1}], "under_16": U16, "alien_weekend": AW},
              {"holds": {"tidal": {}}, "profiles": [{"tidal": True}]}]
    tables, refs = codec.encode(values, None, None)
    assert tables["alien_weekend"] == [AW]                                    # interned: once for every frame
    assert [tables["frames"][r] for r in refs] == [[0, 0, 0, 0], [1, 0, 0, 0], [2, 1]]
    assert [codec.decode_frame(tables, r, None, None) for r in refs] == values
    # the region shard reaches the row (and only the under-16 row through the third index)
    assert shards.frame_refs("licence_frame", [1, 0, 0, 0]) == {
        "licence_hold": [1], "licence_doc": [0], "licence_u16": [0], "licence_alien": [0]}
    assert shards.frame_refs("licence_frame", [2, 1]) == {
        "licence_hold": [2], "licence_doc": [1], "licence_u16": [], "licence_alien": []}
    assert "licence_alien" in shards.SHARD_TABLES


def test_alien_weekend_never_rides_a_frame_without_the_under_16_answer():
    with pytest.raises(ValueError, match="no under_16"):
        CODECS["licence"].encode([{"holds": {}, "profiles": [], "alien_weekend": AW}], None, None)


def test_the_model_refuses_it_on_tidal_water_and_a_water_twice():
    tidal = {"holds": {"tidal": dict(TIDAL_STATE)}, "profiles": [{"tidal": True}] * 60}
    LicenceFrame.model_validate_json(json.dumps(tidal))
    with pytest.raises(ValidationError, match="no alien weekend"):
        LicenceFrame.model_validate_json(json.dumps(dict(tidal, alien_weekend=AW)))
    with pytest.raises(ValidationError, match="listed once"):
        AlienWeekend.model_validate_json(json.dumps(dict(AW, waters=AW["waters"] * 2)))
    with pytest.raises(ValidationError):
        AlienWeekend.model_validate_json(json.dumps(dict(AW, waters=[])))
