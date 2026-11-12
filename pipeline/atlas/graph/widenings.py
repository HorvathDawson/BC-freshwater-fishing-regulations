"""A RIVER'S UNNAMED WIDENINGS — the one test of which unnamed polygons a river's main flow runs through (T153).

FWA cuts a blue line at every waterbody it crosses, and the graph makes each one a `lake:{wbk}` node joined to the
pieces either side by `lake_in` / `lake_out` edges. A polygon is a widening of a set of river pieces `selected` when:
  * it is OPEN: unnamed (no GNIS name, no display name), not a water of its own, not already selected (`is_open`);
  * the river's MAIN FLOW runs through it: it drains (`lake_out`) into a selected piece P, and a selected piece on P's
    own blue line, from further up that line, flows into it (`lake_in`), both pieces carrying a MAIN FLOW line
    (`fwa_edge_types.is_main_flow`: 1000/1050/1200/1250). A pond on a side channel, a lake at the reach's top or mouth,
    or one holding a cut is never taken.
Unnamed polygons back to back on the line are taken together (at most `WIDENING_CHAIN_MAX`); a named one breaks it.

Two readers, one test: the registry folds every widening of a stream item's WHOLE course into that item (T679 #5, owner
D1 2026-11-12: the pond is the river for every rule, "in streams" rules included: `registry.flowing.fold_river_
widenings`), and the reach takes the widenings between two pieces a rule selected (`reach.extent.river_widenings`) for
the polygons no item holds.
"""
from __future__ import annotations

from typing import Callable

from pipeline.common.fwa_edge_types import is_main_flow

#: How many unnamed polygons back to back one widening may chain through (a pond draining straight into another pond
#: on the same line). Measured: the longest chain under a regulated row is short; the cap only bounds a pathological
#: graph.
WIDENING_CHAIN_MAX = 8


def widenings(g, selected: set[str], is_open: Callable[[str], bool]) -> set[str]:
    """The polygons the main flow of `selected` runs through (module docstring). `is_open(nid)` says whether a polygon
    may be taken (unnamed, held by no water, not selected)."""
    out: set[str] = set()
    for pid in sorted(selected):
        p = g.nodes.get(pid)
        if p is None or not p.blk or not is_main_flow(p):
            continue
        for ei in sorted(g.up_adj.get(pid, ())):
            e = g.edges[ei]
            if e.kind != "lake_out" or not is_open(e.from_node):
                continue
            stack = [(e.from_node, (e.from_node,))]
            while stack:
                cur, path = stack.pop()
                for ej in sorted(g.up_adj.get(cur, ())):
                    f = g.edges[ej]
                    src = f.from_node
                    s = g.nodes.get(src)
                    if s is None:
                        continue
                    if s.blk:
                        if (f.kind == "lake_in" and src in selected and s.blk == p.blk
                                and s.down_m >= p.up_m - 1.0 and is_main_flow(s)):
                            out.update(path)
                    elif src not in path and len(path) < WIDENING_CHAIN_MAX and is_open(src):
                        stack.append((src, path + (src,)))
    return out


def unnamed(g, nid: str) -> bool:
    """A waterbody node with no name of its own (no GNIS id, no display name) and no blue line."""
    n = g.nodes.get(nid)
    return n is not None and not n.blk and not (n.gnis_id or n.display_name)
