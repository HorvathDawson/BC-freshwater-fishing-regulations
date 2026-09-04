"""Leaf-first pruning of unnamed headwater capillaries.

The property that matters is not how much it removes — it is what it REFUSES to remove. A
nameless section sitting between two named ones is the connection between them, and losing
it breaks a river into two rivers. The rule protects it without a special case, and these
tests are here to prove that the protection is structural rather than accidental.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from pipeline.deliver.tiles.prune import PruneRule, hops_above_named, prunable


# --- the smallest thing that looks like a StreamGraph to `prune` ------------------------
@dataclass
class FakeNode:
    kind: str = "stream"
    display_name: str = ""
    gnis_id: str | None = None
    stream_magnitude: int = 1
    length_m: float = 500.0


@dataclass
class FakeEdge:
    from_node: str
    to_node: str


@dataclass
class FakeGraph:
    nodes: dict = field(default_factory=dict)
    edges: list = field(default_factory=list)
    up_adj: dict = field(default_factory=dict)


def build(chain: list[tuple[str, dict]], flows: list[tuple[str, str]]) -> FakeGraph:
    """`flows` are (upstream, downstream) pairs — water runs from the first to the second."""
    g = FakeGraph(nodes={n: FakeNode(**kw) for n, kw in chain})
    for up, down in flows:
        g.edges.append(FakeEdge(from_node=up, to_node=down))
        g.up_adj.setdefault(down, []).append(len(g.edges) - 1)
    return g


ALWAYS = PruneRule(max_magnitude=99, min_hops=0, max_length_m=None)


def test_off_when_the_rule_is_none():
    """The switch. `PRUNE = None` in layers.py has to mean the export writes what it did."""
    g = build([("a", {}), ("b", {})], [("a", "b")])
    assert prunable(g, None) == set()


def test_an_unnamed_section_between_two_named_ones_is_never_removed():
    """named -> unnamed -> named, the case the whole design is arranged around.

    `mid` is unnamed and would pass every threshold. It survives because `top` is named,
    a named section is never removed, and so `mid` never becomes a leaf. No special case
    tests for this shape; it falls out of only-ever-removing-leaves.
    """
    g = build(
        [("top", {"display_name": "Upper Creek"}),
         ("mid", {}),
         ("bottom", {"display_name": "Big River"})],
        [("top", "mid"), ("mid", "bottom")])
    assert prunable(g, ALWAYS) == set()


def test_it_peels_a_chain_one_section_at_a_time():
    """Removing a leaf exposes the next one, so a whole nameless branch goes in one call."""
    g = build(
        [("river", {"display_name": "Big River"}),
         ("c1", {}), ("c2", {}), ("c3", {})],
        [("c3", "c2"), ("c2", "c1"), ("c1", "river")])
    assert prunable(g, ALWAYS) == {"c1", "c2", "c3"}


def test_magnitude_stops_the_peel_eating_into_a_real_stream():
    """A leaf is always magnitude 1, so this threshold does nothing on the first pass.

    It bites on the SECOND: once `c2` and `c3` are gone, `c1` becomes a leaf carrying the
    magnitude of everything that used to be above it, and that is what has to stop the peel.
    """
    g = build(
        [("river", {"display_name": "Big River"}),
         ("c1", {"stream_magnitude": 40}), ("c2", {}), ("c3", {})],
        [("c3", "c2"), ("c2", "c1"), ("c1", "river")])
    assert prunable(g, PruneRule(max_magnitude=3, min_hops=0)) == {"c2", "c3"}


def test_hops_protect_the_tributary_somebody_might_be_standing_on():
    """One junction above a named river is a tributary; five is a capillary."""
    g = build(
        [("river", {"display_name": "Big River"}),
         ("c1", {}), ("c2", {}), ("c3", {})],
        [("c3", "c2"), ("c2", "c1"), ("c1", "river")])
    # c1 is 1 hop, c2 is 2, c3 is 3
    assert prunable(g, PruneRule(max_magnitude=99, min_hops=3)) == {"c3"}
    assert prunable(g, PruneRule(max_magnitude=99, min_hops=2)) == {"c2", "c3"}


def test_length_keeps_a_long_nameless_creek():
    g = build(
        [("river", {"display_name": "Big River"}),
         ("short", {"length_m": 300.0}), ("long", {"length_m": 9000.0})],
        [("short", "river"), ("long", "river")])
    assert prunable(g, PruneRule(max_magnitude=99, min_hops=0, max_length_m=1000)) == {"short"}


def test_lakes_and_wetlands_are_never_candidates():
    """A lake with nothing flowing into it is a leaf of this graph, and is also very often
    exactly the pond somebody drove to."""
    g = build(
        [("river", {"display_name": "Big River"}),
         ("pond", {"kind": "lake"}), ("marsh", {"kind": "wetland"})],
        [("pond", "river"), ("marsh", "river")])
    assert prunable(g, ALWAYS) == set()


def test_a_named_leaf_stays_however_small():
    g = build([("river", {"display_name": "Big River"}),
               ("named", {"display_name": "Little Creek"}),
               ("gnis_only", {"gnis_id": "12345"})],
              [("named", "river"), ("gnis_only", "river")])
    assert prunable(g, ALWAYS) == set()


def test_a_branch_with_no_named_water_below_it_is_as_deep_as_it_gets():
    """Unreached by the walk means nothing named lies downstream, so `min_hops` cannot
    protect it — the alternative would be to keep every isolated nameless network forever."""
    g = build([("x", {}), ("y", {})], [("y", "x")])
    assert hops_above_named(g) == {}
    assert prunable(g, PruneRule(max_magnitude=99, min_hops=5)) == {"x", "y"}


def test_hops_are_to_the_NEAREST_named_water():
    """Two named waters below one branch: the count is the shorter of the two, so a section
    close to any named water is protected even when it is far from the one it drains into."""
    g = build(
        [("far", {"display_name": "Far River"}), ("near", {"display_name": "Near Creek"}),
         ("a", {}), ("b", {})],
        [("a", "far"), ("a", "near"), ("b", "a")])
    h = hops_above_named(g)
    assert h["a"] == 1 and h["b"] == 2


@pytest.mark.parametrize("rule,expect", [
    (PruneRule(max_magnitude=3, min_hops=5), "magnitude <= 3, >= 5 hops"),
    (PruneRule(max_magnitude=1, min_hops=3, max_length_m=1000), "<= 1,000 m"),
])
def test_describe_says_what_is_configured(rule, expect):
    """The export prints this beside the count, so a build log records which rule ran."""
    assert expect in rule.describe()
