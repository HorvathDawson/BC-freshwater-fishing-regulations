"""T468, T1177: a lake row's through-river above the lake is its tributary — since T1177 (owner 2026-11-26, "everything in
is a tributary and only shouldnt be if an exception is listed saying otherwise") whether or not it has a row of its own;
T468 took it only when it had none.

Owner, 2026-11-07: "powell lake is closed including tributaries in november but powell river (above the
lake) is not showing as closed". FWA draws the Powell River down through Powell Lake on ONE blue line
(blk 360879873), so the river feeding the lake's head shares the lake's OUTLET line and the walk refused
it as "the river continuing through" (`tributaries._through_blks`, the Koocanusa rule). Every other
inflow took the closure; the Powell River and its 640 sections above the lake did not.

The shape here is Powell's: lake L drains out by R:0 (the lower Powell River) and is fed by R:up on the
same line (the upper Powell River, with a creek RT of its own) and by a side creek C.
"""

from __future__ import annotations

import pytest

from pipeline.atlas.graph import tributaries as T
from pipeline.atlas.reach.build import build_reach
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.common.models.registry import RegistryItem

S, L = NodeKind.stream, NodeKind.lake


def _n(nid, kind=S, *, blk="", lo=0.0, hi=100.0, name="", wsc="", order=2):
    return StreamNode(node_id=nid, kind=kind, blk=blk, wsc=wsc, display_name=name, down_m=lo,
                      up_m=hi, length_m=hi - lo, stream_order=order)


@pytest.fixture
def powell():
    nodes = [
        _n("R:0", blk="R", lo=0, hi=1000, name="Powell River", wsc="900-1", order=6),
        _n("lake:L", L, name="Powell Lake", wsc="900-1", order=6),
        _n("R:up", blk="R", lo=50000, hi=60000, name="Powell River", wsc="900-1", order=5),
        _n("RT", blk="RT", name="", wsc="900-1-000800", order=2),
        _n("C", blk="C", name="Side Creek", wsc="900-1-000300", order=2),
    ]
    edges = [("lake:L", "R:0", "lake_out", 1000), ("R:up", "lake:L", "lake_in", 50000),
             ("RT", "R:up", "confluence", 55000), ("C", "lake:L", "lake_in", 20000)]
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    reg = {"wbk:L": RegistryItem(id="wbk:L", name="Powell Lake", kind="lake", section_ids=("lake:L",)),
           "gnis:R": RegistryItem(id="gnis:R", name="Powell River", kind="stream",
                                  section_ids=("R:0", "R:up"))}
    return g, reg


LAKE_ROW = {"entry_id": "r2:powell_lake@2-12", "matched": ["wbk:L"], "includes_tributaries": True}
CLOSURE = {"rule_id": "powell_lake.r1", "verbatim": "No Fishing Nov 1-Mar 31",
           "extents": [{"op": "whole"}]}


def _bind(world, *, entry=LAKE_ROW, rule=CLOSURE, owned=None):
    g, reg = world
    b, d = build_reach(entry, rule, reg, g, owned=owned or {})
    return set(b.sections), [x.payload for x in d if x.kind == "through_river"]


def test_the_walk_alone_refuses_the_through_river(powell):
    """The cause, pinned: at the lake, the inflow on the outlet's line is `RIVER_CONTINUING`."""
    g, _ = powell
    assert T.through_inlets(g, {"lake:L"}, "lake:L") == {"R:up"}
    assert T.expand(g, {"lake:L"}) == {"lake:L", "C"}


def test_an_unrowed_through_river_goes_with_the_lake(powell):
    got, diags = _bind(powell)
    assert got == {"lake:L", "C", "R:up", "RT"}, "the Powell River above the lake and its creek"
    assert "R:0" not in got, "the river BELOW the lake is downstream, never a tributary"
    assert diags == [{"lake": "lake:L", "inlet": "R:up", "water": "Powell River", "own_row": [],
                      "negated": False, "joined": 2}]


def test_a_through_river_with_a_row_of_its_own_is_a_tributary_too(powell):
    """T1177 (O2, Upper Chehalis River; O7, Forsyth Creek above Connor Lake): a row of its own no longer keeps the river
    out — it gets both rows (RULINGS T4); only a printed exception keeps it out. Its own row is still reported."""
    got, diags = _bind(powell, owned={"gnis:R": ("r2:powell_river@2-12",)})
    assert got == {"lake:L", "C", "R:up", "RT"}
    assert diags[0]["own_row"] == ["r2:powell_river@2-12"] and diags[0]["joined"] == 2


def test_the_rules_words_keep_it_out(powell):
    rule = dict(CLOSURE, verbatim="No Fishing, but not including the Powell River")
    got, _ = _bind(powell, rule=rule)
    assert not got & {"R:up", "RT"}


def test_a_lake_at_the_top_of_a_river_reach_still_ends_it(powell):
    """Only the ENTRY'S OWN lake: a river row whose reach stops at a lake does not take its inflows."""
    g, reg = powell
    entry = {"entry_id": "r2:powell_river@2-12", "matched": ["gnis:R"], "includes_tributaries": True}
    rule = {"rule_id": "powell_river.r1", "verbatim": "x",
            "extents": [{"op": "whole", "item_id": "gnis:R"}, {"op": "whole", "item_id": "wbk:L"}]}
    b, d = build_reach(entry, rule, reg, g, owned={})
    assert not [x for x in d if x.kind == "through_river"]


def test_mutation_pin(powell, monkeypatch):
    """False = T468's rule: a through-river with a row of its own stays out (an unrowed one still comes in)."""
    monkeypatch.setattr(T, "EVERY_INFLOW_OF_A_RULES_LAKE_IS_ITS_TRIBUTARY", False)
    got, diags = _bind(powell, owned={"gnis:R": ("r2:powell_river@2-12",)})
    assert got == {"lake:L", "C"} and diags[0]["joined"] == 0
    got, _ = _bind(powell)
    assert {"R:up", "RT"} <= got


# ---------------------------------------------------------------- T743: a DFO lake place (Redsand Lake)
# "Redsand Lake, including tributaries" (DFO Region 6 B(ii), mark 2625) is the curated non-FWA lake wbk:-1 on the
# Kitsumkalum mainstem, 2 km below Kitsumkalum Lake. A DFO place is resolved with no table of rows (`owned` None), so
# T468 read its through-river as unrowed and took the Kitsumkalum above it with Kitsumkalum Lake's whole basin: 2,639
# sections, 3,086 km, for a 40 ha lake. The same shape as Powell's; the rule carries D5's DFO key.

DFO_ROW = {"entry_id": "6:redsand-lake:including-tributaries", "matched": ["wbk:L"], "includes_tributaries": True}


def _dfo_rule(**kw):
    from pipeline.atlas.reach.build import JOINING_WATER_STAYS_OUT_KEY
    return {"rule_id": "6:redsand-lake:including-tributaries#0", "extents": [{"op": "whole", "item_id": "wbk:L"}],
            "includes_tributaries": True, JOINING_WATER_STAYS_OUT_KEY: True, **kw}


def test_a_dfo_lake_places_through_river_is_its_tributary_as_the_books_is(powell):
    """T1177 (owner 2026-11-26: "unless you have exact dfo words saying what they mean by tributaries we should have a
    single tributary logic"; DFO prints none): a DFO lake place walks as a book row does — T743 kept it out."""
    g, reg = powell
    b, d = build_reach(DFO_ROW, _dfo_rule(), reg, g)          # owned None: how `reach.dfo_salmon` resolves a place
    assert set(b.sections) == {"lake:L", "C", "R:up", "RT"}
    assert [x.payload for x in d if x.kind == "through_river"] == [
        {"lake": "lake:L", "inlet": "R:up", "water": "Powell River", "own_row": [], "negated": False, "joined": 2}]


def test_a_dfo_lake_place_whose_words_name_the_river_takes_it(powell):
    g, reg = powell
    b, _ = build_reach(DFO_ROW, _dfo_rule(verbatim="including tributaries, including the Powell River"), reg, g)
    assert {"R:up", "RT"} <= set(b.sections)


def test_a_lake_row_without_the_dfo_key_is_unchanged(powell):
    """The synopsis row (no DFO key) still takes its unrowed through-river (T468)."""
    got, _ = _bind(powell)
    assert {"R:up", "RT"} <= got


def test_dfo_one_logic_mutation_pin(powell, monkeypatch):
    """False = T743: a DFO place's through-river stays out unless its words name it."""
    from pipeline.atlas.reach import build as B
    monkeypatch.setattr(B, "ONE_TRIBUTARY_LOGIC_FOR_DFO_PLACES", False)
    g, reg = powell
    b, d = build_reach(DFO_ROW, _dfo_rule(), reg, g)
    assert set(b.sections) == {"lake:L", "C"}
    assert [x.payload.get("stays_out") for x in d if x.kind == "through_river"] == [True]


# ---------------------------------------------------------------- T772: the through-river's side channels stay out too
# "Kitsumkalum Lake, including tributaries" (DFO Region 6) took 16 side-channel pieces (14.6 km) of the Kitsumkalum
# River above the lake: the walk refused the river's main line, but a side channel of the river — the river's own
# watershed code — reaching the lake by a mouth of its own came in as a lake tributary, with the creek feeding it.

@pytest.fixture
def braided(powell):
    g, reg = powell
    extra = [_n("R:ch", blk="RC", lo=0, hi=400, name="Powell River", wsc="900-1", order=5),
             _n("CT", blk="CT", name="", wsc="900-1-000900", order=1)]
    for n in extra:
        g.nodes[n.node_id] = n
    for a, b, k, m in [("R:up", "R:ch", "confluence", 50000), ("R:ch", "lake:L", "lake_in", 49800),
                       ("CT", "R:ch", "confluence", 200)]:
        g.edges.append(FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m))
        i = len(g.edges) - 1
        g.up_adj.setdefault(b, []).append(i)
        g.down_adj.setdefault(a, []).append(i)
    return g, reg


NOT_THE_RIVER = dict(CLOSURE, verbatim="No Fishing Nov 1-Mar 31, not including the Powell River")


def test_a_through_river_that_stays_out_stays_out_with_its_side_channels(braided):
    """Kept out by the rule's printed words (since T1177 a row of its own keeps nothing out)."""
    got, diags = _bind(braided, rule=NOT_THE_RIVER)
    assert got == {"lake:L", "C"}, "the side channel R:ch and the creek CT feeding it are the river's, not the lake's"
    assert diags[0]["channels_out"] == 1


def test_an_unrowed_through_river_still_comes_in_whole(braided):
    got, _ = _bind(braided)
    assert {"R:up", "RT", "R:ch", "CT"} <= got
    got, _ = _bind(braided, owned={"gnis:R": ("r2:powell_river@2-12",)})
    assert {"R:up", "RT", "R:ch", "CT"} <= got, "T1177: rowed or not, the river comes in with its side channels"


def test_t772_mutation_pin(braided, monkeypatch):
    from pipeline.atlas.reach import build as B
    monkeypatch.setattr(B, "A_THROUGH_RIVER_STAYS_OUT_WITH_ITS_SIDE_CHANNELS", False)
    got, _ = _bind(braided, rule=NOT_THE_RIVER)
    assert {"R:ch", "CT"} <= got


@pytest.mark.needs_reach_run
@pytest.mark.needs_atlas
def test_kitsumkalum_lake_takes_the_kitsumkalum_river_above_it(request):
    """On the built run (T1177, one tributary logic; T743 and T772 kept it out): "Kitsumkalum Lake, including
    tributaries" (DFO Region 6) binds the Kitsumkalum River (gnis:3942) above the lake, the river's side channels
    entering the lake with it, as a book lake row's walk does."""
    import gzip
    import json
    from pipeline.tests import under_test
    from pipeline.tests.conftest import ATLAS_HINT, REACH_HINT, need
    run = need(request, "reach_run", under_test.reach_run(request) / "dfo_salmon.jsonl.gz", REACH_HINT)
    reg = need(request, "atlas", under_test.atlas(request) / "registry.json", ATLAS_HINT)
    items = json.loads(reg.read_text(encoding="utf-8"))["items"]
    items = items.values() if isinstance(items, dict) else items
    river = {s for it in items if it["id"] == "gnis:3942" for s in it["section_ids"]}
    assert river
    with gzip.open(run, "rt", encoding="utf-8") as f:
        lake = next(set(r["sections"]) for r in map(json.loads, f)
                    if r["location_id"] == "6:kitsumkalum-lake:including-tributaries")
    assert lake and lake & river


# ---------------------------------------------------------------- T784: the words come from the place, not a hand-built rule

def _dfo_place(area: str):
    """Redsand Lake's shape as the reach run and the review app hand it in: `entries.to_reach_input`, never a rule dict
    typed here (the old test above passes `verbatim=`, which production never builds)."""
    from pipeline.regs.dfo_salmon import entries as E
    loc = E.EntryLocation.from_dict({
        "location_id": "6:redsand-lake:x", "region": "6", "region_number": 6, "kind": "water", "precedence": 3,
        "water": "Redsand Lake", "water_id": "6:redsand-lake", "source_text": {"specific_area": area},
        "binding": {"extents": [{"op": "whole", "item_id": "wbk:L"}], "tributaries": True}})
    water = E.WaterBinding(water_id="6:redsand-lake", region="6", region_number=6, name="Redsand Lake",
                           item_ids=["wbk:L"])
    return E.to_reach_input(loc, [{}], water)


def test_a_dfo_place_whose_own_words_name_the_through_river_takes_it(powell):
    """T784: "unless DFO's words name it" fires on the place's `specific_area`, handed in as `extent_text`.
    MUTATION: `to_reach_input` without `extent_text` (as before T784) keeps the Powell out here."""
    g, reg = powell
    entry, rules = _dfo_place("including the Powell River above the lake")
    assert rules[0]["extent_text"] == "including the Powell River above the lake"
    b, _ = build_reach(entry, rules[0], reg, g)
    assert {"R:up", "RT"} <= set(b.sections)
    entry, rules = _dfo_place("including tributaries")                    # T1177: in too, by the one logic
    b, _ = build_reach(entry, rules[0], reg, g)
    assert {"R:up", "RT"} <= set(b.sections)
