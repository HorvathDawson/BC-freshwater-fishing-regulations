"""ANSWERS 2.8 — the quota card is water-first, says each fact once, and a gate proves every number.

NO DATA: every test here runs on `answers28_fixture` (Region 2's trout/char table + one water's lift, the
real reader and the real rows over 15 rule records in git), so CI's no-data tier runs them. One test per
complaint the owner made on the Vedder (mouth part, Oct 10; ROUND9 R1-R6, decisions D1/D2):

  1 a big "4 a day" on a stream whose share is 2            -> headline
  2 "the 2-from-streams limit doesn't apply to them"        -> a lift said positively, with whose rule
  3 "Steelhead count toward the 4" (they share the 2 too)   -> steelhead in the stream share's bucket
  4 "Over 50 cm: at most 2 — only 1 …" against the rows     -> over-50 said once, as the bucket's note
                                                              (ROUND11 A1; it lived on the kind rows)
  5 "Only 1 fish over 50 cm in total (steelhead don't count)" (page checklist) -> no checklist
  6 the same cap stated in several places                   -> each cap stated once
  7 one picture standing for brook, brown and cutthroat     -> every kind row lists its species
  8 lifted fish left out of the stream tally                -> RULINGS Q23b, the worked count
  9 the fishway / hatchery buffers on the gear page         -> "Where you can't fish", Keep side (D2)
and the gate (`card_check.check_card`) on the fixture, mutation-pinned.
"""
from __future__ import annotations

import copy
import re
from pathlib import Path

from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import display as D
from pipeline.tests import answers28_fixture as F

OCT10, JUL15, MAY15 = (10, 10), (7, 15), (5, 15)


def _g(water, md=OCT10):
    out, _P = F.card(water, md)
    return F.trout_group(out)


def _kind(g, S):
    got = [(b, k) for b in g["buckets"] for k in b["kinds"] if S in k["species"]]
    assert len(got) == 1, (S, g["buckets"])
    return got[0]


def _card_text(g, row):
    """Every sentence the card shows for the group (not the sheet behind "Fished elsewhere today?")."""
    out = [g["headline"]["says"]] + ([g["origin"]] if g["origin"] else []) + [L["says"] for L in g["lifts"]]
    out += [g["total_line"]] if g["total_line"] else []
    for b in g["buckets"]:
        out += [b["title"]] if b["title"] else []
        out += [n["says"] for n in b.get("notes") or []] + [n["says"] for n in b.get("sizes") or []]
        for k in b["kinds"]:
            out += [k["name"], k["detail"], *k["notes"], *k["duties"]]
    return [t for t in out if t]


# 1 ---------------------------------------------------------------------------------------------
def test_the_headline_is_this_waters_number_without_a_lift():
    assert _g(F.VEDDER)["headline"] == {"n": 2, "unit": "a day from this stream",
                                         "says": "2 a day from this stream",
                                         "also": [{"n": 4, "says": "4 if they’re hatchery rainbow trout 50 cm "
                                                                   "/ 20 in or less"}]}
    assert _g(F.STREAM)["headline"]["n"] == 2
    assert _g(F.LAKE)["headline"] == {"n": 4, "unit": "a day", "says": "4 a day"}


# 2 ---------------------------------------------------------------------------------------------
def test_a_lift_is_said_positively_naming_whose_rule_with_its_book_page():
    g = _g(F.VEDDER)
    assert [L["says"] for L in g["lifts"]] == [
        "On this river, hatchery rainbow trout 50 cm / 20 in or less: up to 4 a day — this river’s rule "
        "replaces Region 2’s 2-from-streams limit for them."]
    assert g["lifts"][0]["by"] == F.LIFT                      # the page cites its Book p.22 from it
    assert g["total_line"] == "No more than 4 trout and char a day across Region 2, lakes included."
    assert not any("doesn’t apply" in t or "don’t count" in t for t in _card_text(g, None))
    assert _g(F.STREAM)["lifts"] == [] and _g(F.LAKE)["lifts"] == []


# 3 ---------------------------------------------------------------------------------------------
def test_steelhead_share_the_2_from_streams():
    g = _g(F.VEDDER)
    b, st = _kind(g, "ST")
    assert (b["title"], b["cap"], b["by"]) == ("Up to 2 a day between them — all Region 2 streams combined", 2, F.SHARE)
    assert st["n"] == 2 and st["name"] == "Steelhead" and st["detail"] == "hatchery rainbow trout over 50 cm"
    assert not any(re.search(r"[Ss]teelhead.*count toward the 4", t) for t in _card_text(g, None))


# 4 ---------------------------------------------------------------------------------------------
def test_over_50_is_said_once_on_its_bucket():
    """ROUND11 A1 (owner, superseding "over 50 cm lives on the kind rows"): the bucket's note."""
    g = _g(F.VEDDER)
    # ROUND15 1: a size group naming the kinds it binds
    assert [n["says"] for n in _kind(g, "CT")[0]["sizes"]] == [
        "Cutthroat trout, brown trout, Dolly Varden/bull trout, lake trout or brook trout: only 1 over 50 cm "
        "between them"]
    assert _kind(g, "CT")[1]["notes"] == [] and _kind(g, "DV")[1]["notes"] == []
    assert _kind(g, "ST")[1]["notes"] == []
    assert not any("Over 50 cm: at most" in t for t in _card_text(g, None))


# 5 ---------------------------------------------------------------------------------------------
def test_the_page_draws_no_for_every_kind_checklist_beside_the_buckets():
    js = (Path(__file__).parents[1] / "deliver/answers/reference/page_v36.js").read_text("utf-8")
    assert "For every kind" not in js
    assert re.search(r"group\??\.buckets", js)                  # the card reads the answers' buckets


# 6 ---------------------------------------------------------------------------------------------
def test_each_cap_is_stated_once_on_the_card():
    for w in (F.VEDDER, F.STREAM, F.LAKE):
        g = _g(w)
        states = [s["r"] for s in g["states"]]
        assert len(states) == len(set(states)), (w, g["states"])
        txt = _card_text(g, None)
        assert len(txt) == len(set(txt)), (w, txt)


# 7 ---------------------------------------------------------------------------------------------
def test_every_kind_row_lists_each_of_its_species_for_its_own_picture():
    g = _g(F.VEDDER)
    assert _kind(g, "CT")[1]["species"] == ["EB", "GB", "CT"]         # brook, brown, cutthroat
    assert _kind(g, "DV")[1]["species"] == ["DV", "LT"]
    every = [S for b in g["buckets"] for k in b["kinds"] for S in k["species"]]
    assert sorted(every) == sorted(g["kinds"]) and len(every) == len(set(every))


# 8 ---------------------------------------------------------------------------------------------
def test_q23b_the_worked_count_on_a_lifted_stream():
    g = _g(F.VEDDER)
    assert [{k: w[k] for k in ("kept", "q", "a")} for w in g["worked"]] == [
        {"kept": 4, "q": "Keep 4 rainbows here", "a": "that’s your trout and char for the day, everywhere."},
        {"kept": 3, "q": "Keep 3 rainbows here", "a": "1 more: any kind from here, or one from a lake."},
        {"kept": 2, "q": "Keep 2 rainbows here", "a": "no more trout and char from any other stream today; up to 2 more "
                                                      "from here or from lakes."},
        # ROUND12 4: the kept-elsewhere examples the keep-N count does not repeat, in the one list
        {"kept": 1, "q": "Kept 1 at a lake?",
         "a": "Keep 3 more here. Fish from lakes count toward the 4 a day, not toward the 2 from streams."},
        {"kept": 1, "q": "Kept trout or char over 50 cm / 20 in elsewhere today?",
         "a": "It counts here too: at most 2 over 50 cm / 20 in a day in all — only 1 of them may be anything other "
              "than a hatchery steelhead."}]
    assert [w["at"] for w in _g(F.STREAM)["worked"]] == ["stream", "lake", "elsewhere"]
    assert [w["at"] for w in _g(F.LAKE)["worked"]] == ["elsewhere", "elsewhere"]


def test_q23b_lifted_fish_count_toward_the_stream_tally_on_other_streams_only():
    """RULINGS Q23b (refined, owner 2026-10-12): on the lifted water the lifted kind and the rest are
    kept in any order; every OTHER stream counts the lifted fish toward its tally; lakes: the total."""
    _out, P = F.card(F.VEDDER, OCT10)
    E = CC.Engine(P, F.TOTAL)
    rb, ct = ("RB", "hatchery", 40, "here"), ("CT", "hatchery", 40, "stream")
    here_ct = ("CT", "hatchery", 40, "here")
    assert E.legal([ct, ct, rb, rb])                 # another stream's 2 first, then the lift here
    assert E.legal([rb, rb, here_ct, here_ct])       # on the Vedder: any order
    assert not E.legal([rb, rb, here_ct, here_ct, rb])               # the total
    assert not E.legal([rb, rb, ct])                 # 2 rainbows kept here: no other stream today
    assert E.legal([rb, rb, ("CT", "hatchery", 40, "lake")])         # lakes face only the total
    assert not E.legal([rb, rb, rb, rb, ("CT", "hatchery", 40, "lake")])
    # ROUND12 4: the kept-elsewhere examples are the card's one list (`worked`); the stream-tally
    # example is the keep-N count read backwards, so a lifted water drops it (STREAM keeps it)
    w = _g(F.STREAM)["worked"]
    assert [w[0]["q"], w[0]["a"], w[0]["here"]] == ["Kept 2 on another stream?", "None more here", 0]
    # the Q16 cross-water case is in the list, not on the card
    assert [x["a"] for x in _g(F.VEDDER)["worked"]][-1] == (
        "It counts here too: at most 2 over 50 cm / 20 in a day in all — only 1 of them may be anything other "
        "than a hatchery steelhead.")


# 9 ---------------------------------------------------------------------------------------------
def test_the_fishway_and_hatchery_buffers_are_where_you_cant_fish_on_the_keep_side():
    x = {"type": "retention_limit", "family": "retention", "standing": True, "take": 0,
         "may_target": False, "species": ["ALL_GAME_FISH"],
         "verbatim": "Within 23 m downstream of the lower entrance to any fishway, canal, obstacle or leap."}
    assert D.where_of(x) == "cant_fish"
    assert D.CANT_FISH_TITLE == "Where you can’t fish"
    assert D.where_of({"type": "retention_limit", "family": "retention", "take": 2,
                       "species": ["RB"]}) is None
    # the BUNDLE spells the same rule with `may_target` 0 (sqlite), the export's fields `false`: the
    # heading must come from both alike (it did not — d78cda88 tested `is False`, so the bundle's
    # facts lost `where` and the dataflow gate saw a second `may_target` reader)
    assert D.where_of({**x, "may_target": 0}) == "cant_fish"
    assert D.rule_facts({**x, "may_target": 0}) == D.rule_facts(x)
    # a standing rule one MAY fish for is no place you can't fish
    assert D.where_of({**x, "may_target": 1}) is None


# the gate ----------------------------------------------------------------------------------------
def test_check_card_proves_the_fixture_cards():
    for w in (F.VEDDER, F.STREAM, F.LAKE):
        for md in (OCT10, JUL15, MAY15, (6, 15)):
            out, P = F.card(w, md)
            assert CC.check_card(P, out) == [], (w, md)


def test_check_card_fails_when_the_lift_breaks():
    """MUTATION: the ladder no longer lifts the share for the rainbow, the card still says 4."""
    out, P = F.card(F.VEDDER, OCT10)
    lad = copy.deepcopy(P.ladder)
    for o in ("none", "hatchery"):
        lad["RB"][o][F.SHARE] = ["speaks", None, None, None]
    probs = CC.check_card(F.part(F.VEDDER, OCT10, lad), out)
    assert any("RB" in p and "kind" in p for p in probs), probs


def test_check_card_fails_when_the_tally_breaks():
    """MUTATION: a worked count that lets lifted fish skip the stream tally (the old Q23 reading)."""
    out, P = F.card(F.VEDDER, OCT10)
    bad = copy.deepcopy(out)
    g = F.trout_group(bad)
    g["worked"][2]["other"] = 2                          # "Keep 2 rainbows here"
    probs = CC.check_card(P, bad)
    assert any("worked" in p for p in probs), probs


def test_the_headline_says_the_lifted_number_beside_it():
    """Round 9 persona: a bare "2" next to a 4-rainbow lift read as the whole limit."""
    assert _g(F.VEDDER)["headline"].get("also") == [
        {"n": 4, "says": "4 if they’re hatchery rainbow trout 50 cm / 20 in or less"}]
    assert not _g(F.STREAM)["headline"].get("also") and not _g(F.LAKE)["headline"].get("also")
