"""ROUND14 1 (owner ruling 2026-10-16, RULINGS T14) — no data, CI's no-data tier.

WHERE TWO ✱ ROWS COMPETE ON A TRIBUTARY, THE ROW OF THE WATER NEAREST THAT STREAM WINS. On the Duncan
River's tributaries the Duncan's "bull trout daily quota = 2 (any size)" beats Kootenay Lake's tributaries'
"Bull trout catch and release": the Duncan drains into Kootenay Lake, so walking down from the creek the
Duncan is met first.

NEARER, precisely (`bundle.nearer`): row A's water is nearer than row B's when A's own water DRAINS INTO
B's own water — following each water's outlet down the flow graph (`item_down`: the first other named water
each outlet meets, mainstem first) reaches one of B's waters, and B's never reaches A's. On a section both
rows reach by the tributary walk, the downstream path is unique, so A's water is the one met first. Rows
whose waters do not nest stay as the ladder leaves them. A closure is never out-ranked this way (an
inherited closure is only lifted, T4/L20).

  graph   `first_named` / `item_down` on a toy graph (creek -> unnamed -> river -> lake, a braid)
  pairs   `nearer_pairs`: Trout Lake -> Duncan -> Kootenay Lake, every co-occurring pair ordered
  reader  `effective_rules_bound`: the nearer row's quota speaks, the farther row's release is displaced
          (`nearer_row`, by the nearer rule) and releases nothing; without the pair both speak
"""
from __future__ import annotations

from types import SimpleNamespace

from pipeline.deliver.bundle import nearer as N
from pipeline.deliver.bundle import read

DUNCAN = "r4:duncan_river@4-19"
KOOT = "r4:kootenay_lake_s_tributaries@4-19+4-7"
ZONE = "z4:daily_quotas"


# graph -------------------------------------------------------------------------------------------
def _graph(edges):
    """A StreamGraph-shaped toy: nodes, edges (from, to, kind), down_adj."""
    nodes = {n: object() for e in edges for n in e[:2]}
    E = [SimpleNamespace(from_node=a, to_node=b, kind=k) for a, b, k in edges]
    down = {}
    for i, e in enumerate(E):
        down.setdefault(e.from_node, []).append(i)
    return SimpleNamespace(nodes=nodes, edges=E, down_adj=down)


def test_graph_each_water_drains_into_the_first_named_water_below_it():
    g = _graph([("c1", "c2", "continuation"), ("c2", "u1", "join"), ("u1", "u2", "continuation"),
                ("u2", "d1", "join"), ("d1", "d2", "continuation"), ("d2", "k1", "lake_in"),
                # a braid leaving the river and rejoining it is no outlet
                ("d1", "b1", "braid"), ("b1", "d2", "braid")])
    own = {"creek": {"c1", "c2"}, "river": {"d1", "d2", "b1"}, "lake": {"k1"}}
    assert N.item_down(g, own) == {"creek": {"river"}, "river": {"lake"}}


def test_graph_the_mainstem_edge_is_followed_first():
    g = _graph([("a1", "x", "continuation"), ("x", "y", "side"), ("x", "z", "continuation"),
                ("y", "s1", "continuation"), ("z", "m1", "continuation")])
    own = {"a": {"a1"}, "side": {"s1"}, "main": {"m1"}}
    assert N.item_down(g, own) == {"a": {"main"}}


# pairs -------------------------------------------------------------------------------------------
def test_pairs_the_upstream_row_is_nearer_on_every_stream_both_reach():
    down = {"trout_lake": {"lardeau"}, "lardeau": {"duncan"}, "duncan": {"kootenay_main"}}
    own = {"T": {"trout_lake"}, "D": {"duncan"}, "K": {"kootenay_main", "kootenay_west"}}
    co = {frozenset({"T", "D"}), frozenset({"D", "K"}), frozenset({"T", "K"})}
    assert N.nearer_pairs(down, own, co) == {("T", "D"), ("D", "K"), ("T", "K")}


def test_pairs_waters_that_do_not_nest_are_not_ordered():
    down = {"a": {"sea"}, "b": {"sea"}}
    assert N.nearer_pairs(down, {"A": {"a"}, "B": {"b"}}, {frozenset({"A", "B"})}) == set()


# reader ------------------------------------------------------------------------------------------
PATH = "fixture:r14_nearer"


def _rule(entry, rule, name, species, take, extents, **f):
    return {"entry_id": entry, "rule_id": rule, "entry": entry, "rule": rule, "entry_name": name,
            "type": "retention_limit", "family": "retention", "dimension": "daily", "period": "daily",
            "species": species, "species_except": [], "take": take, "may_target": None, "uncertain": 0,
            "unresolved": None, "extent_text": None, "undrawn_part": None, "extents": extents,
            "scope": "section", "label": f"{name} {rule}", "verbatim": f"{name} {rule}", **f}


def _seed(pairs):
    rules = [_rule(DUNCAN, "duncan_river.r5", "Duncan River", ["DV"], 2, [{"op": "whole"}]),
             _rule(KOOT, "kootenay_lake_tributaries.r1", "Kootenay Lake's tributaries", ["DV"], 0,
                   [{"op": "whole"}]),
             _rule(ZONE, "trout_char_quota.r1", "Region 4", ["TROUT_CHAR"], 5,
                   [{"op": "within", "area_id": "area:region:4"}], scope="area")]
    got = {(x["entry"], x["rule"]): x for x in rules}
    for x in got.values():
        x["_rank"] = read.source_of(x).rank
    read._RULES_BY_PATH[PATH] = got
    read._NEARER_BY_PATH[PATH] = frozenset(pairs)
    return [(DUNCAN, "duncan_river.r5", "trib"), (KOOT, "kootenay_lake_tributaries.r1", "trib"),
            (ZONE, "trout_char_quota.r1", "reach")]


def _ask(pairs):
    bound = _seed(pairs)
    got = read.effective_rules_bound(bound, False, (6, 15), "DV", PATH, trace=True)
    return {x["rule"]: (x["state"], x.get("reason"), x.get("by")) for x in got}


def test_reader_the_nearer_rows_quota_beats_the_farther_rows_release():
    got = _ask({(DUNCAN, KOOT)})
    assert got["duncan_river.r5"][0] == "speaks"
    assert got["kootenay_lake_tributaries.r1"] == ("displaced", "nearer_row", f"{DUNCAN}::duncan_river.r5")
    # the farther row's release releases nothing: the region's total still counts the bull trout (T3a)
    assert got["trout_char_quota.r1"][0] == "speaks"


def test_reader_without_the_pair_both_rows_speak():
    """MUTATION: no nearness known (waters that do not nest) — the ladder ties them, as before."""
    got = _ask(set())
    assert got["duncan_river.r5"][0] == "speaks" and got["kootenay_lake_tributaries.r1"][0] == "speaks"


def test_reader_the_ruling_is_named_and_has_words():
    from pipeline.deliver.answers import display
    assert read.NEARER_ROW_BEATS_FARTHER is True
    assert read.LOSS_REASONS["nearer_row"] == "displaced"
    assert "nearer_row" in display.reasons_table()
