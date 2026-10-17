"""ROUND 18 (owner review of v19, 2026-10-17) — the card as a TREE, no data, CI's no-data tier.

  1  `group.tree`: nested tiers (total → share / a water's own rule → count / size) holding the kinds, each
     kind exactly once under the tightest tier that holds it; a cap spanning tiers is said on the kind
     (`also_counts`). The card gate proves every tier's and kind's number, the sizes, every `also_counts`
     and the numbers in every sentence.
  2  Every size in the tree in cm AND inches (nearest whole inch): 30 cm → 12 in, 50 → 20, 25 → 10, 33 → 13,
     60 → 24; each kind's `sizes` {cm, in}.
  3  The Q16 over-50 pair as ONE size tier: "1 over 50 cm / 20 in — a second may be a hatchery steelhead".
  4  A compact header: "2 a day from this stream · 4 across Region 2, lakes included", tag "hatchery only".
  7  Kind rows as sentences inside their tier: "Of the 2 a day: up to 2 may be brook, brown or cutthroat trout
     (hatchery, 30 cm / 12 in or longer) — only 1 over 50 cm / 20 in, if you haven't already kept a Dolly
     Varden/bull trout or lake trout over 50 cm".
  8  An own-rule tier whose rule reached the water by the tributary walk says why it applies here.
"""
from __future__ import annotations

import copy

from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import common
from pipeline.tests import answers28_fixture as F

OCT10, MAY1 = (10, 10), (5, 1)


def _walk(nodes):
    for n in nodes:
        yield n
        yield from _walk(n.get("children") or [])


def _tree(water, md=OCT10):
    out, P = F.card(water, md)
    (g,) = [r["group"] for r in out["rows"] if r.get("group") and set(r["group"]["kinds"]) & {"RB", "EB", "GB"}]
    return g, out, P


# 2 ---------------------------------------------------------------------------------------------
def test_2_sizes_in_cm_and_inches():
    assert [common.cm_in(c) for c in (30, 50, 25, 33, 60, 90)] == [
        "30 cm / 12 in", "50 cm / 20 in", "25 cm / 10 in", "33 cm / 13 in", "60 cm / 24 in", "90 cm / 35 in"]
    assert common.range_cm_in(25, 33) == "25–33 cm / 10–13 in"


# 1, 3, 7 -------------------------------------------------------------------------------------------
def test_1_the_vedder_tree():
    g, _o, _P = _tree(F.VEDDER)
    (total,) = g["tree"]
    assert (total["tier"], total["n"], total["says"]) == ("total", 4, "4 a day across Region 2, lakes included")
    own, share = total["children"]
    assert (own["tier"], own["n"], own["says"]) == ("own", 4, "This river’s rule: up to 4 a day")
    assert own["replaces"] == "Replaces Region 2’s 2-from-streams limit and 30 cm / 12 in minimum size for these fish"
    assert own["why_here"] is None
    (rb,) = own["children"]
    assert rb["sentence"] == "Of the 4 a day: up to 4 may be rainbow trout (hatchery, any length, 50 cm / 20 in or less)"
    assert rb["sizes"] == {"cm": [[0, 50]], "in": [[0, 20]]}
    assert (share["tier"], share["n"], share["says"]) == ("share", 2, "2 a day from this stream")
    trout, size = share["children"]
    assert trout["sentence"] == ("Of the 2 a day: up to 2 may be brook, brown or cutthroat trout (hatchery, "
                                 "30 cm / 12 in or longer) — only 1 over 50 cm / 20 in, if you haven’t already "
                                 "kept a Dolly Varden/bull trout or lake trout over 50 cm")
    assert trout["sizes"] == {"cm": [[30, None]], "in": [[12, None]]}
    (ac,) = trout["also_counts"]
    assert (ac["tier"], ac["n"], ac["over_cm"]) == (size["id"], 1, 50)
    # 3: the Q16 pair is ONE size tier holding the kinds wholly over the size
    assert (size["tier"], size["n"], size["over_cm"]) == ("size", 1, 50)
    assert size["says"] == "1 over 50 cm / 20 in — a second may be a hatchery steelhead"
    assert size["extra"] == {"n": 1, "species": ["ST"]}
    dv, st = size["children"]
    assert dv["sentence"] == ("Of the 1 over 50 cm / 20 in: up to 1 may be Dolly Varden/bull trout or lake trout "
                              "(hatchery, 60 cm / 24 in or longer)")
    assert st["sentence"] == "Of the 1 over 50 cm / 20 in: up to 2 may be steelhead (hatchery rainbow trout over 50 cm / 20 in)"
    assert st["duties"] == ["10 a year", "stop fishing for the day after 2", "record each one on your licence"]
    assert st["presence"]["line"] == "known_with_rules"


def test_1_each_kind_once_under_its_tier():
    for w, md in ((F.VEDDER, OCT10), (F.STREAM, OCT10), (F.LAKE, OCT10), (F.BELLA_MAY_ST, MAY1), (F.SLOT, OCT10),
                  (F.CLAKE, OCT10), (F.NESTED, OCT10), (F.WAHLEACH, OCT10), (F.COUNT, OCT10), (F.TWO, OCT10)):
        g, _o, _P = _tree(w, md)
        kinds = [S for n in _walk(g["tree"]) if n["node"] == "kind" for S in n["species"]]
        assert sorted(kinds) == sorted(g["kinds"]), w


def test_1_the_bella_coola_may_tree_nests_the_count_and_says_the_size_cap_on_its_kinds():
    g, _o, _P = _tree(F.BELLA_MAY_ST, MAY1)        # the river's row on a Region 2 stream (hatchery, 30 cm floor)
    (total,) = g["tree"]
    (own,) = total["children"]
    assert own["says"] == "This river’s rule: up to 2 a day"
    trout, count, size = own["children"]
    assert trout["sentence"] == ("Of the 2 a day: up to 2 may be brook or brown trout (hatchery, 30 cm / 12 in or "
                                 "longer) — only 1 over 50 cm / 20 in, if you haven’t already kept a Dolly "
                                 "Varden/bull trout or lake trout over 50 cm")
    assert (count["tier"], count["n"], count["says"]) == ("count", 1, "1 between them")
    assert [k["sentence"] for k in count["children"]] == [
        "Of the 1 between them: up to 1 may be cutthroat trout (hatchery, 30–33 cm / 12–13 in)",
        "Of the 1 between them: up to 1 may be rainbow trout (hatchery, 30–50 cm / 12–20 in)"]
    # no steelhead may be kept (closed): the size tier is the plain "1 over 50 cm"
    assert (size["tier"], size["says"], size["extra"]) == ("size", "1 over 50 cm / 20 in", None)


# 4 ---------------------------------------------------------------------------------------------
def test_4_a_compact_header():
    g, _o, _P = _tree(F.VEDDER)
    assert g["header"] == {"says": "2 a day from this stream · 4 across Region 2, lakes included",
                           "tag": "hatchery only"}
    g, _o, _P = _tree(F.LAKE)
    assert g["header"]["says"] == "4 a day"


# 8 ---------------------------------------------------------------------------------------------
def test_8_a_rule_reached_by_the_tributary_walk_says_why_it_applies_here():
    g, _o, _P = _tree(F.VEDDER_TRIB)
    own = g["tree"][0]["children"][0]
    assert own["why_here"] == "its row covers its tributaries, so it applies here"
    assert own["line"] == ("The Chilliwack River’s rule (its row covers its tributaries, so it applies here) · "
                           "Replaces Region 2’s 2-from-streams limit and 30 cm / 12 in minimum size for these fish")


# the gate ----------------------------------------------------------------------------------------
def test_the_gate_proves_every_tree():
    for w, md in ((F.VEDDER, OCT10), (F.STREAM, OCT10), (F.LAKE, OCT10), (F.BELLA_MAY_ST, MAY1), (F.SLOT, OCT10),
                  (F.CLAKE, OCT10), (F.NESTED, OCT10), (F.WAHLEACH, OCT10), (F.COUNT, OCT10), (F.TWO, OCT10),
                  (F.VEDDER_TRIB, OCT10)):
        out, P = F.card(w, md)
        assert CC.check_card(P, out) == [], w


def _mutated(fn, water=F.VEDDER, md=OCT10):
    out, P = F.card(water, md)
    (g,) = [copy.deepcopy(r["group"]) for r in out["rows"] if r.get("group") and "RB" in r["group"]["kinds"]]
    fn(g)
    return CC.check_tree(P, g)


def test_the_gate_refuses_a_wrong_tree():
    def tier_n(g):
        g["tree"][0]["children"][1]["n"] = 3                       # the share tier
    def size_extra(g):
        g["tree"][0]["children"][1]["children"][1]["extra"]["n"] = 2
    def also(g):
        g["tree"][0]["children"][1]["children"][0]["also_counts"][0]["n"] = 2
    def twice(g):
        share = g["tree"][0]["children"][1]
        share["children"].append(copy.deepcopy(share["children"][0]))
    def words(g):
        k = g["tree"][0]["children"][1]["children"][0]
        k["sentence"] = k["sentence"].replace("up to 2", "up to 3")
    def sizes(g):
        g["tree"][0]["children"][0]["children"][0]["sizes"] = {"cm": [[30, 50]], "in": [[12, 20]]}
    def header(g):
        g["header"]["says"] = g["header"]["says"].replace("4 across", "5 across")
    for fn in (tier_n, size_extra, also, twice, words, sizes, header):
        assert _mutated(fn), fn.__name__


def test_8_a_waters_own_total_reached_by_the_walk_says_why_too():
    g, _o, _P = _tree(F.BELLA_TRIB)                # Ape Creek's shape: the Bella Coola River's row by the walk
    (total,) = g["tree"]
    assert total["says"] == "The Bella Coola River’s rule: up to 1 a day"
    assert total["line"] == "The Bella Coola River’s rule (its row covers its tributaries, so it applies here)"
    out, P = F.card(F.BELLA_TRIB, OCT10)
    assert CC.check_card(P, out) == []
