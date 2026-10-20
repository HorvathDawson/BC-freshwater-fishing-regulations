"""ROUND UR6 (owner, the last prototype fixes before porting; rows 28, display 17).

  1  a size tier's words say no box ("this box" — but since the flat redesign the page draws no box): "Of these, only 1 may be
     over 50 cm (20 in) — 2 if one is a hatchery steelhead" (picked by a fresh-reader single-frame test over naming every fish:
     right on every head frame either way, higher confidence mid-section with the shorter words; the page draws the section's
     extent as a left rule from its head to its last row)
  4  the carry-your-paper-licence line names only fish the water lets you keep: a record rule is listed for a part only where
     a keeping quota for its fish speaks in some verdict of the key (the record duty itself always speaks); the Ape and the
     Bella Coola, closed to steelhead, named hatchery steelhead
  6  a water's own rule under the region total says how it stands to the stream share and the total — Glenn and Hamill read
     the Duncan River's 5 against the head's "2 a day from streams" as a contradiction: `relation` "Not part of the 2 from
     streams; part of the 5 a day across Region 4"
"""
from __future__ import annotations

from types import SimpleNamespace as N

from pipeline.deliver import types as T
from pipeline.deliver.answers import display as D
from pipeline.deliver.answers.common import RuleKey
from pipeline.tests import answers28_fixture as F

OCT10, MAY1 = (10, 10), (5, 1)


def _g(water, md=OCT10):
    out, P = F.card(water, md)
    (g,) = [r["group"] for r in out["rows"] if r.get("group") and set(r["group"]["kinds"]) & {"RB", "EB", "GB"}]
    return g


def _walk(nodes):
    for n in nodes:
        yield n
        yield from _walk(n.get("children") or [])


# 1 ---------------------------------------------------------------------------------------------
def test_1_a_size_tier_says_of_these_not_this_box():
    g = _g(F.VEDDER)
    (size,) = [t for t in _walk(g["tree"]) if t.get("tier") == "size"]
    assert size["says"] == "Of these, only 1 may be over 50 cm (20 in) — 2 if one is a hatchery steelhead"
    g = _g(F.BELLA_MAY_ST, MAY1)
    (size,) = [t for t in _walk(g["tree"]) if t.get("tier") == "size"]
    assert size["says"] == "Of these, only 1 may be over 50 cm (20 in)"


def test_1_no_tier_anywhere_says_box():
    for water in (F.VEDDER, F.STREAM):
        g = _g(water)
        assert not [t["says"] for t in _walk(g["tree"]) if t.get("node") == "tier" and "box" in t["says"]]


# 6 ---------------------------------------------------------------------------------------------
def test_6_an_own_rule_under_the_total_says_it_is_outside_the_share_and_inside_the_total():
    g = _g(F.VEDDER)
    (own,) = [t for t in _walk(g["tree"]) if t.get("tier") == "own"]
    assert own["relation"] == "Not part of the 2 from streams; part of the 4 a day across Region 2"


def test_6_no_relation_where_there_is_no_share_or_the_own_rule_is_inside_it():
    for water, md in ((F.STREAM, OCT10), (F.BELLA_MAY_ST, MAY1)):
        g = _g(water, md)
        assert all(t.get("relation") is None for t in _walk(g["tree"]) if t.get("node") == "tier")


# 4 ---------------------------------------------------------------------------------------------
class _Store:
    """Rules: 0 the steelhead record duty, 1 the chinook record duty, 2 a steelhead quota (2 a day), 3 a chinook quota.
    The record duties always speak; the steelhead quota speaks only where steelhead may be kept."""
    rule_ids = ["e:st::r1", "e:ch::r1", "e:stq::r1", "e:chq::r1"]

    def __init__(self, st_kept: bool):
        speak, lost = sorted(T.SPEAKER_CODES)[0], len(list(T.RuleState)) - 1
        self._rows = {0: ((0, speak, None, None, ()), (2, speak if st_kept else lost, None, None, ())),
                      1: ((1, speak, None, None, ()), (3, speak, None, None, ()))}

    def frames(self, key):
        return {(0, "ST", "hatchery"): 0, (0, "CH", "none"): 1}

    def rows(self, v):
        return self._rows[v]


def _bundle():
    rules = {("e:st", "r1"): {"record_retention": True, "species": ["ST"], "dimension": "daily@origin=hatchery&record"},
             ("e:ch", "r1"): {"record_retention": True, "species": ["CH"], "dimension": "daily@record"},
             ("e:stq", "r1"): {"family": "retention", "species": ["ST"], "take": 2, "dimension": "daily@origin=hatchery"},
             ("e:chq", "r1"): {"family": "retention", "species": ["CH"], "take": 1, "dimension": "daily"},
             ("e:x", "r1"): {"species": ["RB"]}}
    key = RuleKey(7, True, True)
    return N(rules=rules, sets={7: [("e:ch", "r1", "own"), ("e:st", "r1", "own"), ("e:x", "r1", "own")]},
             key_ix={key: 3}), key


def test_4_the_paper_licence_names_only_fish_kept_here():
    B, key = _bundle()
    assert D.paper_licence(B, key, "stream", store=_Store(st_kept=True)) == ["e:ch::r1", "e:st::r1"]
    assert D.paper_licence(B, key, "stream", store=_Store(st_kept=False)) == ["e:ch::r1"]   # closed to steelhead here


def test_4_a_fish_the_book_sets_no_quota_for_keeps_its_duty():
    """Adult chinook: no keeping quota of the book takes part (DFO sets it) — the book cannot say it is shut, the line stays."""
    B, key = _bundle()
    S = _Store(st_kept=False)
    S._rows[1] = ((1, sorted(T.SPEAKER_CODES)[0], None, None, ()),)
    assert D.paper_licence(B, key, "stream", store=S) == ["e:ch::r1"]
