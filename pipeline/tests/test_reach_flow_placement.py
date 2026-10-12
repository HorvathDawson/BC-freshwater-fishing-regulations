"""An OFF-LINE piece is placed by its FLOW PATH (D5, owner 2026-10-12; `extent._by_measure`).

`_by_measure` places a piece that is not on the cut's own blue line through its neighbours — but only
neighbours INSIDE the scoped water. A piece whose link to the rest of its water runs through a lake, a
pond, another named water or the sea touched "nothing", was called detached, and `_bracket` then needed
a placed piece of its own line on BOTH sides by route measure. A piece above a lake (nothing placed
above it) or in a delta (nothing below it) failed that and was reported unplaced, so a rule silently
skipped it: the Cowichan's delta piece missed "No Fishing downstream of the CNR Mile 66 Trestle".

The fix follows the piece's water — downstream through any water, upstream through its own water's
lakes and channels (never up a tributary) — to the first section of the scoped water it reaches, and
takes that section's side. A path that meets the cut's line AT the cut, that never meets the water
(the sea, out of B.C.), or that meets both sides stays UNPLACED with a typed reason (AGENTS 12).

No data: each graph is a few nodes, so this runs in the no-data CI tier.
"""

from __future__ import annotations

from pipeline.atlas.reach import extent as X
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.common.models.enums import BoundaryKind
from pipeline.common.models.registry import RegistryBoundary, RegistryItem
from pipeline.common.models.sections import SectionBoundary

S, L = NodeKind.stream, NodeKind.lake
CUT = SectionBoundary(boundary_id="split:cut", kind=BoundaryKind("split"), route_measure=1000)


def _graph(nodes, edges):
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


def _s(nid, blk, lo, hi, wsc="100", **kw):
    return StreamNode(node_id=nid, kind=S, blk=blk, wsc=wsc, down_m=lo, up_m=hi,
                      length_m=hi - lo, **kw)


def _lake(wbk, wsc="100"):
    return StreamNode(node_id=f"lake:{wbk}", kind=L, wbk=wbk, wsc=wsc)


def _main():
    """The cut's own line M, cut at 1000: M:0 (0-1000) below, M:1000 (1000-2000) above."""
    return [_s("M:0", "M", 0, 1000, upper_bound=CUT), _s("M:1000", "M", 1000, 2000, lower_bound=CUT)]


def _reg(secs):
    return {"gnis:1": RegistryItem(id="gnis:1", name="River", kind="stream", section_ids=tuple(secs),
                                   boundaries=(RegistryBoundary(id="cut", label="cut", kind="split",
                                                                ref="split:cut"),))}


def _resolve(g, op):
    secs = sorted(n for n in g.nodes if not n.startswith("lake:") and n not in NOT_THE_WATER)
    got = X.resolve_extent(_reg(secs), g, ["gnis:1"], {"op": op, "splits": ["cut"]})
    return set(got["sections"]), set(got["unclassified"]), dict(got.get("unclassified_why") or {})


#: Nodes in the fixtures that are NOT part of the scoped water (another named water, a creek).
NOT_THE_WATER = {"creek", "other", "W:out"}


# ------------------------------------------------------------------------------------- the delta
def _delta():
    """The Cowichan's shape: a distributary D leaves M:0 (below the cut), runs through a pond and
    reaches the sea as D:0. D:0 touches only the pond and the sea."""
    return _graph(_main() + [_s("D:500", "D", 500, 900), _lake("pond"), _s("D:0", "D", 0, 380)],
                  [("M:1000", "M:0", "continuation", 1000), ("M:0", "D:500", "confluence", 900),
                   ("D:500", "lake:pond", "lake_in", 500), ("lake:pond", "D:0", "lake_out", 380)])


def test_a_delta_piece_reaching_its_water_only_through_a_pond_is_placed_by_its_flow_path():
    """D:0 drains to the sea; its water came from D:500 through the pond, so it is below the cut."""
    down, unplaced, _ = _resolve(_delta(), "downstream_of")
    assert "D:0" in down and not unplaced
    up, unplaced, _ = _resolve(_delta(), "upstream_of")
    assert "D:0" not in up and not unplaced


# ------------------------------------------------------------------------------ above the lake
def _above_the_lake(lakes: int = 1):
    """The Chilliwack's shape: C joins the cut line M at its top (2000), and above C:0 the river runs
    through `lakes` lakes; the pieces between and above them touch only lakes."""
    nodes = _main() + [_s("C:0", "C", 0, 500)]
    edges = [("M:1000", "M:0", "continuation", 1000), ("C:0", "M:1000", "continuation", 2000)]
    below = "C:0"
    for i in range(lakes):
        lo = 600 + 1000 * i
        nodes += [_lake(f"k{i}"), _s(f"C:{lo}", "C", lo, lo + 900)]
        edges += [(f"lake:k{i}", below, "lake_out", lo - 100), (f"C:{lo}", f"lake:k{i}", "lake_in", lo)]
        below = f"C:{lo}"
    return _graph(nodes, edges)


def test_a_piece_above_a_lake_takes_the_side_of_the_water_it_drains_into():
    """Chilliwack above Chilliwack Lake is OUTSIDE "downstream of Vedder Crossing" — not unplaced."""
    down, unplaced, _ = _resolve(_above_the_lake(), "downstream_of")
    assert down == {"M:0"} and not unplaced
    up, unplaced, _ = _resolve(_above_the_lake(), "upstream_of")
    assert up == {"M:1000", "C:0", "C:600"} and not unplaced


def test_a_river_threading_a_chain_of_lakes_is_placed_piece_by_piece():
    """The South Atnarko / Chowade shape: piece, lake, piece, lake, piece — every piece above."""
    up, unplaced, _ = _resolve(_above_the_lake(lakes=3), "upstream_of")
    assert {"C:600", "C:1600", "C:2600"} <= up and not unplaced


# -------------------------------------------------------------------------------- at the cut
def test_a_piece_whose_water_meets_the_line_at_the_cut_stays_unplaced_with_a_reason():
    """X drains through another water onto the cut line exactly AT the cut: on neither side."""
    g = _graph(_main() + [_s("other", "O", 0, 400, wsc="100-500000"), _s("X:0", "X", 0, 300)],
               [("M:1000", "M:0", "continuation", 1000), ("other", "M:0", "confluence", 1000),
                ("X:0", "other", "confluence", 200)])
    down, unplaced, why = _resolve(g, "downstream_of")
    assert "X:0" not in down and unplaced == {"X:0"}
    assert why == {"X:0": "meets_the_line_at_the_cut"}


def test_a_piece_draining_into_the_lake_at_the_cut_is_above_its_outlet():
    """The Sproat shape: X drains into the lake whose OUTLET is the cut, so X lies above the cut."""
    g = _graph(_main() + [_lake("k"), _s("X:0", "X", 0, 300)],
               [("M:1000", "M:0", "continuation", 1000), ("lake:k", "M:0", "lake_out", 1000),
                ("X:0", "lake:k", "lake_in", 0)])
    down, unplaced, _ = _resolve(g, "downstream_of")
    assert "X:0" not in down and not unplaced
    up, unplaced, _ = _resolve(g, "upstream_of")
    assert "X:0" in up and not unplaced


# ------------------------------------------------------------------------- the sea, the border
def test_a_piece_whose_water_reaches_the_sea_without_meeting_its_water_stays_unplaced():
    """Z flows to the sea and only a creek (a tributary basin, never a way back) flows into it."""
    g = _graph(_main() + [_s("Z:0", "Z", 0, 200), _s("creek", "K", 0, 500, wsc="100-500000")],
               [("M:1000", "M:0", "continuation", 1000), ("creek", "Z:0", "confluence", 100)])
    down, unplaced, why = _resolve(g, "downstream_of")
    assert unplaced == {"Z:0"} and why == {"Z:0": "flows_to_the_sea_unjoined"}


def test_a_flow_path_continues_across_water_outside_bc_and_places_the_piece_where_it_returns():
    """T11: Z drains out of B.C. (W:out, another water) and back in onto M:0, below the cut."""
    g = _graph(_main() + [_s("Z:0", "Z", 0, 200), _s("W:out", "W", 0, 900, out_of_bc=True)],
               [("M:1000", "M:0", "continuation", 1000), ("Z:0", "W:out", "confluence", 800),
                ("W:out", "M:0", "confluence", 400)])
    down, unplaced, _ = _resolve(g, "downstream_of")
    assert "Z:0" in down and not unplaced


def test_a_piece_whose_water_leaves_bc_without_meeting_its_water_stays_unplaced():
    g = _graph(_main() + [_s("Z:0", "Z", 0, 200), _s("W:out", "W", 0, 900, out_of_bc=True)],
               [("M:1000", "M:0", "continuation", 1000), ("Z:0", "W:out", "confluence", 800)])
    down, unplaced, why = _resolve(g, "downstream_of")
    assert unplaced == {"Z:0"} and why == {"Z:0": "leaves_bc_unjoined"}


# ------------------------------------------------------------------------------ a true straddle
def test_a_braid_leaving_above_the_cut_and_rejoining_below_still_straddles_with_a_reason():
    """Not widened: the braid B touches M:1000 (above) and M:0 (below) directly."""
    g = _graph(_main() + [_s("B:0", "B", 0, 700)],
               [("M:1000", "M:0", "continuation", 1000), ("M:1000", "B:0", "confluence", 700),
                ("B:0", "M:0", "confluence", 600)])
    down, unplaced, why = _resolve(g, "downstream_of")
    assert "B:0" not in down and why == {"B:0": "straddles_the_cut"}


# -------------------------------------------------------------------------------- the switch
def test_the_switch_matters(monkeypatch):
    """MUTATION: with the flow path off, the delta piece and the pieces above the lake are unplaced
    again — the defect this file pins."""
    monkeypatch.setattr(X, "OFF_LINE_PIECES_FOLLOW_THEIR_FLOW", False)
    assert _resolve(_delta(), "downstream_of")[1] == {"D:0"}
    assert _resolve(_above_the_lake(), "downstream_of")[1] == {"C:600"}


# =========================================================================== the real atlas (slow)
import os                                                                    # noqa: E402
from pathlib import Path                                                     # noqa: E402

import pytest                                                                # noqa: E402

from pipeline.tests.conftest import need, ATLAS_HINT                         # noqa: E402


@pytest.fixture(scope="module")
def real(request):
    from pipeline.atlas.registry import load_registry
    from pipeline.common.curated import GENERATED
    from pipeline.common.io.serialize import read_artifact
    from pipeline.regs.parsing import io as parse_io
    from pipeline.atlas.reach.models import iter_entries
    b = Path(os.environ.get("ATLAS_BUILD") or GENERATED.build())
    need(request, "atlas", b / "graph.pkl", ATLAS_HINT)
    g = read_artifact(str(b / "graph.pkl"))
    reg = load_registry(str(b / "registry.json"))
    corpus = {e["entry_id"]: e for e in iter_entries(parse_io.read_all_entries(registry=reg))}
    return g, reg, corpus


def _bind(real, eid, rid):
    from pipeline.atlas.reach.build import build_reach
    g, reg, corpus = real
    e = corpus[eid]
    b, d = build_reach(e, next(r for r in e["rules"] if r["rule_id"] == rid), reg, g)
    unplaced = {p for x in d if x.kind == "unclassified" for p in x.payload["pieces"]}
    return set(b.sections), unplaced


COWICHAN = "r1:cowichan_river_see_map_below@1-4"
CHILLIWACK = "r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4"
ATNARKO = "r5:atnarko_bella_coola_rivers_includes_tributaries_except_burnt@5-11+5-6+5-8"


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_the_cowichan_delta_piece_is_inside_no_fishing_downstream_of_the_mile_66_trestle(real):
    """Handle 354088335:0 (sid 716 in the 2026-10-11 atlas) touches only a pond and the sea."""
    got, unplaced = _bind(real, COWICHAN, "cowichan_river.r6")
    assert "354088335:0" in got and "354088335:0" not in unplaced


@pytest.mark.needs_atlas
@pytest.mark.slow
@pytest.mark.parametrize("rid", [f"chilliwack_vedder_rivers.r{i}" for i in range(4, 10)])
def test_the_chilliwack_above_chilliwack_lake_is_outside_the_vedder_crossing_rules(real, rid):
    """Handles 380887781:57967 and :58646 lie above Chilliwack Lake: outside "downstream of Vedder
    Crossing", and no longer reported unplaced."""
    got, unplaced = _bind(real, CHILLIWACK, rid)
    above = {"380887781:57967", "380887781:58646"}
    assert not got & above and not unplaced & above


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_the_atnarko_above_its_lakes_is_placed_by_its_flow(real):
    """The Atnarko above Stillwater Lake is above Goat Creek (outside r12); the South Atnarko between
    Tenas and Rainbow lakes joins below the park boundary (inside r10, downstream of it)."""
    got, unplaced = _bind(real, ATNARKO, "atnarko_bella_coola_rivers.r12")
    assert "360879335:40401" not in got and "360879335:40401" not in unplaced
    got, unplaced = _bind(real, ATNARKO, "atnarko_bella_coola_rivers.r10")
    assert "360878501:2380" in got and "360878501:2380" not in unplaced
