"""`resolve_carve_outs` — the EXCEPT clauses, and what they take with them.

Public (not a private of `_expander`) because the review app has to SHOW a curator which
streams an EXCEPT removed. If the app recomputed that itself the two could drift, and a
curator would confirm a reach that is not the one that ships.

The behaviour that matters: a carve-out blocks the named water **and everything upstream of
it**. "Except Hunlen Creek upstream of Hunlen Falls" cannot mean "except that one section" —
the water above it drains only through the excepted stretch.
"""

from __future__ import annotations

from pipeline.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.reach.build import resolve_carve_outs


def _n(nid, *, order=1):
    return StreamNode(node_id=nid, kind=NodeKind.stream, blk=nid.split(":")[0],
                      down_m=0.0, up_m=100.0, length_m=100.0, stream_order=order)


def _g(nodes, edges):
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=0.0)
               for a, b, k in edges]
    g.up_adj, g.down_adj = {}, {}
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


class _Item:
    def __init__(self, sections=(), boundaries=()):
        self.section_ids = tuple(sections)
        self.boundaries = tuple(boundaries)
        self.kind = "stream"
        self.name = "x"


def _graph():
    """mainstem <- burnt (the excepted creek) <- burnt_trib, and burnt_above continuing past it.

        m:0  <- burnt:0  <- burnt_trib:0   (tributary edge)
                   ^-------- burnt_above:0 (continuation edge — burnt carrying on upstream)
    """
    nodes = [_n("m:0", order=5), _n("burnt:0", order=2),
             _n("burnt_trib:0", order=1), _n("burnt_above:0", order=2)]
    return _g(nodes, [("burnt:0", "m:0", "tributary"),
                      ("burnt_trib:0", "burnt:0", "tributary"),
                      ("burnt_above:0", "burnt:0", "continuation")])


REG = {"burnt": _Item(["burnt:0"]), "main": _Item(["m:0"])}


def test_no_carve_outs_blocks_nothing():
    detail, blocked = resolve_carve_outs({}, {}, REG, _graph(), ["main"])
    assert detail == [] and blocked == set()


def test_entry_level_except_blocks_the_water_and_everything_above_it():
    """`entry.tributaries.excludes` qualifies EVERY rule in the row, exactly as `entry.scope` does."""
    entry = {"tributaries": {"excludes": [{"op": "whole", "item_id": "burnt", "splits": []}]}}
    detail, blocked = resolve_carve_outs(entry, {}, REG, _graph(), ["main"])

    assert blocked == {"burnt:0", "burnt_trib:0"}, "the creek AND what drains into it"
    assert len(detail) == 1
    assert detail[0]["resolved"] is True
    assert detail[0]["sections"] == ["burnt:0"]     # what the extent itself named
    assert detail[0]["above"] == 1                  # reported separately, so the UI can explain it


def test_a_carve_out_does_not_swallow_the_mainstem_continuing_above_it():
    """Subtle, and deliberate: the block walk will not cross a `continuation` edge OUT of the
    excepted stretch — the same reach-boundary rule the main tributary walk uses.

    It is what makes a PARTIAL carve-out mean what it says. "Except Burnt Bridge Creek
    upstream of Sitkatapa" resolves the upper stretch through the extent's own `upstream_of`
    op; nothing needs inferring. If the block walk also followed continuations, a carve-out
    naming one stretch would silently remove the rest of the creek too."""
    entry = {"tributaries": {"excludes": [{"op": "whole", "item_id": "burnt", "splits": []}]}}
    _detail, blocked = resolve_carve_outs(entry, {}, REG, _graph(), ["main"])

    assert "burnt_above:0" not in blocked


def test_rule_level_except_is_additive_to_the_entry_level_one():
    entry = {"tributaries": {"excludes": [{"op": "whole", "item_id": "burnt", "splits": []}]}}
    rule = {"tributary_excludes": [{"op": "whole", "item_id": "main", "splits": []}]}
    detail, blocked = resolve_carve_outs(entry, rule, REG, _graph(), ["main"])

    assert len(detail) == 2, "both sources apply; neither replaces the other"
    assert "burnt:0" in blocked and "m:0" in blocked


def test_an_unresolvable_carve_out_is_reported_not_silently_dropped():
    """A carve-out naming water that does not resolve must be visible: silently ignoring it
    would WIDEN the closure past what the regulation says."""
    rule = {"tributary_excludes": [{"op": "whole", "item_id": "nope", "splits": []}]}
    detail, blocked = resolve_carve_outs({}, rule, REG, _graph(), ["main"])

    assert len(detail) == 1 and detail[0]["resolved"] is False
    assert blocked == set()
