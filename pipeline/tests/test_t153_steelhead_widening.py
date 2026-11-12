"""T153 follow-up (2026-10-23, the card gate on key 2336 — the Kitimat River's pond `lake:329233923`): A STEELHEAD
RIVER'S WIDENING IS ANADROMOUS. An unnamed pond a steelhead row's OWN river takes as its widening
(`extent.river_widenings`) is the river (T153), so a rainbow over 50 cm in it is a steelhead (p.80: "a rainbow trout
longer than 50 cm in waters where anadromous rainbow trout are found"). Before, the pond was known steelhead water
with steelhead rules applying but `anadromous` false (a lake), so the Kitimat's "Hatchery rainbow trout (adipose
clipped, <50 cm)" kept hatchery rainbows to 50 cm while a wild 60 cm rainbow stayed keepable under Region 6's lake
quota ("1 over 50 cm") — on the river 10 m away it is a wild steelhead, released.

One function decides it, `steelhead.Presence.anadromous`, behind `A_STEELHEAD_RIVERS_WIDENING_IS_ANADROMOUS`.

MUTATION PINS — each mutation of `Presence.anadromous` / `Presence.add_row` and the test it fails (run 2026-10-23):
  * `A_STEELHEAD_RIVERS_WIDENING_IS_ANADROMOUS = False`   -> test_a_steelhead_rivers_pond_is_anadromous,
                                                             test_the_report_counts_the_pond
  * `anadromous` reads `kind == "stream"` only            -> the same two
  * drop `kind == "stream"` (widenings only)              -> the same two, test_a_named_lake_on_the_line_stays_a_lake
  NOT caught, on purpose: taking every diagnostic's polygons, or dropping the "inside the row's own water" filter
  — only the own-water binding's `river_widening` diagnostic carries polygons, all inside it; the two
  conditions are belt and braces.

No data: a three-piece river and one pond, so this runs in the no-data CI tier.
"""
from __future__ import annotations

from pipeline.atlas.reach import steelhead as SH
from pipeline.atlas.reach.build import build_reaches
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.common.models.registry import RegistryItem

MAIN = ("1000",)


def _s(nid, lo, hi):
    return StreamNode(node_id=nid, kind=NodeKind.stream, blk="M", wsc="100", down_m=lo, up_m=hi,
                      length_m=hi - lo, edge_types=MAIN, display_name="River")


def _graph(pond: StreamNode) -> StreamGraph:
    """Line M of the River: M:0 (0-1000), the pond (1000-1200), M:1200 (1200-3000)."""
    g = StreamGraph()
    nodes = [_s("M:0", 0, 1000), pond, _s("M:1200", 1200, 3000)]
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in
               (("M:1200", pond.node_id, "lake_in", 1200), (pond.node_id, "M:0", "lake_out", 1000))]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


UNNAMED = StreamNode(node_id="lake:u", kind=NodeKind.lake, wbk="u", wsc="100")
NAMED = StreamNode(node_id="lake:u", kind=NodeKind.lake, wbk="u", wsc="100", gnis_id="9", display_name="Pond Lake")

REG = {"gnis:1": RegistryItem(id="gnis:1", name="River", kind="stream", section_ids=("M:0", "M:1200"),
                              mus=("6-3",)),
       "area:region:6": RegistryItem(id="area:region:6", name="area:region:6", kind="area",
                                     section_ids=("M:0", "M:1200", "lake:u"))}
REG_NAMED = dict(REG, **{"wbk:u": RegistryItem(id="wbk:u", name="Pond Lake", kind="lake",
                                               section_ids=("lake:u",), mus=("6-3",))})

STREAMS = [{"op": "within", "area_id": "area:region:6", "feature_types": ["stream"]}]
TWIN = lambda base: [{"op": "steelhead_waters", "siblings": [base]}]  # noqa: E731


def _province():
    rules = []
    for base in SH.PROVINCE_SET_RULES:
        rid = base.split(":")[-1]
        rules.append({"rule_id": rid, "type": "retention_limit", "species": ["ST"], "verbatim": "steelhead",
                      "water": "stream", "extents": STREAMS})
        rules.append({"rule_id": rid + "b", "type": "retention_limit", "species": ["ST"],
                      "verbatim": "steelhead", "extents": TWIN(rid)})
    return {"entry_id": SH.PROVINCE_STEELHEAD, "matched": [], "rules": rules}


def _river_row(steelhead: bool):
    """The Kitimat's row: hatchery rainbow to 50 cm, hatchery steelhead over 50 cm (p.51) — or, with no
    steelhead line, a plain bait ban."""
    rules = [{"rule_id": "river.r3", "type": "bait_restriction", "verbatim": "Bait ban",
              "gear": [{"slot": "bait", "ban": ["any_bait"]}], "extents": [{"op": "whole"}]}]
    if steelhead:
        rules.append({"rule_id": "river.r5", "type": "retention_limit", "species": ["ST"], "take": 2,
                      "origin": "hatchery", "verbatim": "Hatchery steelhead (adipose clipped, >50 cm) daily quota = 2",
                      "extents": [{"op": "whole"}]})
    return {"entry_id": "r6:river@6-3", "matched": ["gnis:1"], "region": "6", "rules": rules}


def _steelhead(pond=UNNAMED, reg=REG, steelhead=True) -> dict:
    """{section: (steelhead, kind, rules apply, anadromous)} — every presence row of the run."""
    got = build_reaches([_province(), _river_row(steelhead)], reg, _graph(pond))
    return {r["section_id"]: (r["steelhead"], r["kind"], r["rules"], r["anadromous"]) for r in got.steelhead}


def test_a_steelhead_rivers_pond_is_anadromous():
    """The pond is the river: known by the river's own row, steelhead rules apply, and a big rainbow in it is a
    steelhead — exactly as on the river's two pieces."""
    assert _steelhead() == {"M:0": ("known", "stream", True, True),
                            "M:1200": ("known", "stream", True, True),
                            "lake:u": ("known", "lake", True, True)}


def test_the_flag_off_leaves_the_pond_a_lake(monkeypatch):
    monkeypatch.setattr(SH, "A_STEELHEAD_RIVERS_WIDENING_IS_ANADROMOUS", False)
    assert _steelhead()["lake:u"] == ("known", "lake", True, False)


def test_a_pond_on_a_river_with_no_steelhead_row_is_absent():
    """No steelhead row: the river's pieces are only "possible" (the base binds streams) and the pond nothing."""
    assert _steelhead(steelhead=False) == {"M:0": ("possible", "stream", True, False),
                                           "M:1200": ("possible", "stream", True, False)}


def test_a_named_lake_on_the_line_stays_a_lake():
    """A NAMED lake is never the river's widening (T153): the river's row does not take it, so it is not the
    row's own water — no presence row at all."""
    got = _steelhead(NAMED, REG_NAMED)
    assert got == {"M:0": ("known", "stream", True, True), "M:1200": ("known", "stream", True, True)}


def test_one_function_decides_it():
    """`anadromous` is decided in `Presence.anadromous` only, and the flag read only there."""
    import inspect
    src = inspect.getsource(SH)
    assert src.count("A_STEELHEAD_RIVERS_WIDENING_IS_ANADROMOUS") == 2       # the constant and its one read
    assert src.count('"anadromous": self.anadromous(') == 1


def test_the_report_counts_the_pond():
    got = build_reaches([_province(), _river_row(True)], REG, _graph(UNNAMED))
    assert (got.report.steelhead["anadromous"], got.report.steelhead["known_lakes"]) == (3, 1)
