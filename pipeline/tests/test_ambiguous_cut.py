"""AN AMBIGUOUS CUT takes the candidate NEAREST THE RULE'S OTHER END (owner 2026-10-22, decisions.md
"the audit plan"; `extent.AMBIGUOUS_CUT_NEAREST_THE_OTHER_END`).

A split point can match TWO places on one blue line. A lake the river runs through is the usual
case: "Harrison River … Harrison Lake" meets the lake where the river leaves it (km 20) and where the
line enters it (km 84). The builder used to take the one nearest the mouth, whatever the rule said,
so "upstream of the lake" started at the OUTLET and its stretch spanned the lake — and walked every
stream entering the lake. The owner: "pick the end that is closest to the other extent, because this
will never be a lake regulation and under-lake streams have no regs."

  * `downstream_of` — the other end is the mouth: the LOWEST candidate (unchanged).
  * `upstream_of`   — the other end is the source: the HIGHEST candidate.
  * `between` on one line — the PAIR of candidates nearest each other.
  * `between` across two lines — the upper cut's LOWEST candidate, the lower cut's HIGHEST.

Deterministic, so no review list; the choice is still recorded — every candidate and the one taken —
as an `ambiguous_cut` diagnostic, which the bundle carries (`rule_ambiguous_cut`).

No data: each graph is a few hand-made pieces, so this runs in the no-data CI tier. The real cases
(Harrison, Ash, Mohun, Cayoosh) are at the bottom, marked `needs_atlas`.
"""

from __future__ import annotations

import pytest

from pipeline.atlas.reach import extent as X
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.common.models.enums import BoundaryKind
from pipeline.common.models.registry import RegistryBoundary, RegistryItem
from pipeline.common.models.sections import SectionBoundary

INF = float("inf")


def _b(sid: str, m: float) -> SectionBoundary:
    return SectionBoundary(boundary_id=f"split:{sid}", kind=BoundaryKind("split"), route_measure=m)


def _s(nid, blk, lo, hi, lower=None, upper=None):
    return StreamNode(node_id=nid, kind=NodeKind.stream, blk=blk, wsc="100", down_m=lo, up_m=hi,
                      length_m=hi - lo, lower_bound=lower, upper_bound=upper)


def _graph(nodes, edges=()):
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind="continuation", at_measure=0.0)
               for a, b in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


def _reg(g, cuts):
    return {"gnis:1": RegistryItem(
        id="gnis:1", name="River", kind="stream", section_ids=tuple(sorted(g.nodes)),
        boundaries=tuple(RegistryBoundary(id=c, label=c, kind="split", ref=f"split:{c}")
                         for c in cuts))}


def _river():
    """One blue line M, mouth at 0. A bridge at 1,000; a LAKE the line runs through from 2,000 (its
    outlet) to 6,000 (its inlet), so the one split id `lake` lands at BOTH; falls at 8,000. The
    river's line through the lake (M:2000) is a piece of the river — it is what "spanning the lake"
    takes in."""
    lake_lo, lake_hi = _b("lake", 2000), _b("lake", 6000)
    return _graph([
        _s("M:0", "M", 0, 1000, upper=_b("bridge", 1000)),
        _s("M:1000", "M", 1000, 2000, lower=_b("bridge", 1000), upper=lake_lo),
        _s("M:2000", "M", 2000, 6000, lower=lake_lo, upper=lake_hi),
        _s("M:6000", "M", 6000, 8000, lower=lake_hi, upper=_b("falls", 8000)),
        _s("M:8000", "M", 8000, 9000, lower=_b("falls", 8000)),
    ], [("M:8000", "M:6000"), ("M:6000", "M:2000"), ("M:2000", "M:1000"), ("M:1000", "M:0")])


def _resolve(g, op, *splits):
    return X.resolve_extent(_reg(g, splits), g, ["gnis:1"], {"op": op, "splits": list(splits)})


# ------------------------------------------------------------------------------ one cut

def test_downstream_of_the_lake_starts_at_its_outlet():
    got = _resolve(_river(), "downstream_of", "lake")
    assert got["sections"] == ["M:0", "M:1000"]
    assert got["window"] == ("M", 0.0, 2000)
    assert got["ambiguous_cut"] == [{"split_id": "lake", "used": 2000, "also_at": [6000],
                                     "candidates": [2000, 6000], "toward": "mouth"}]


def test_upstream_of_the_lake_starts_at_its_inlet_and_never_spans_it():
    """The case the ruling changes: the stretch above the lake starts where the line LEAVES the lake
    upstream, so the river's line through the lake is not in it (and the tributary walk, which
    reads `window`, does not take every stream entering the lake)."""
    got = _resolve(_river(), "upstream_of", "lake")
    assert got["sections"] == ["M:6000", "M:8000"]
    assert got["window"] == ("M", 6000, INF)
    assert got["ambiguous_cut"] == [{"split_id": "lake", "used": 6000, "also_at": [2000],
                                     "candidates": [2000, 6000], "toward": "source"}]


def test_between_the_lake_and_the_falls_above_it_starts_at_the_inlet():
    got = _resolve(_river(), "between", "lake", "falls")
    assert got["sections"] == ["M:6000"]
    assert got["window"] == ("M", 6000, 8000)
    assert got["ambiguous_cut"][0]["used"] == 6000
    assert got["ambiguous_cut"][0]["toward"] == "falls"


def test_between_the_bridge_and_the_lake_above_it_ends_at_the_outlet():
    got = _resolve(_river(), "between", "bridge", "lake")
    assert got["sections"] == ["M:1000"]
    assert got["window"] == ("M", 1000, 2000)
    assert got["ambiguous_cut"][0]["used"] == 2000


def test_between_two_lakes_takes_the_link_between_them():
    """Marble River "between Victoria and Alice lakes": BOTH cuts are ambiguous. The pair nearest
    each other is the link river, not a stretch running through either lake."""
    a_lo, a_hi, b_lo, b_hi = _b("alice", 1000), _b("alice", 3000), _b("vic", 5000), _b("vic", 8000)
    g = _graph([
        _s("M:0", "M", 0, 1000, upper=a_lo),
        _s("M:1000", "M", 1000, 3000, lower=a_lo, upper=a_hi),
        _s("M:3000", "M", 3000, 5000, lower=a_hi, upper=b_lo),
        _s("M:5000", "M", 5000, 8000, lower=b_lo, upper=b_hi),
        _s("M:8000", "M", 8000, 9000, lower=b_hi),
    ])
    for order in (("vic", "alice"), ("alice", "vic")):
        got = _resolve(g, "between", *order)
        assert got["sections"] == ["M:3000"]
        assert got["window"] == ("M", 3000, 5000)
        used = {a["split_id"]: (a["used"], a["toward"]) for a in got["ambiguous_cut"]}
        assert used == {"alice": (3000, "vic"), "vic": (5000, "alice")}


def test_an_unambiguous_cut_records_nothing():
    got = _resolve(_river(), "between", "bridge", "falls")
    assert got["ambiguous_cut"] == []
    assert got["window"] == ("M", 1000, 8000)


# -------------------------------------------------------------------- across two blue lines

def _two_lines():
    """An upper river (blk 100) with a bridge at 1,000 flows into a lower river (blk 200) at its
    head. The lower river runs through a lake from 300 (outlet) to 600 (inlet). Water runs
    100:1000 -> 100:0 -> 200:600 -> 200:300 -> 200:0."""
    lo, hi = _b("lake", 300), _b("lake", 600)
    return _graph([
        _s("100:1000", "100", 1000, 2000, lower=_b("bridge", 1000)),
        _s("100:0", "100", 0, 1000, upper=_b("bridge", 1000)),
        _s("200:600", "200", 600, 900, lower=hi),
        _s("200:300", "200", 300, 600, lower=lo, upper=hi),
        _s("200:0", "200", 0, 300, upper=lo),
    ], [("100:1000", "100:0"), ("100:0", "200:600"), ("200:600", "200:300"),
        ("200:300", "200:0")])


def test_between_across_lines_the_lower_cut_takes_its_highest_candidate():
    """"From the bridge down to the lake": the lake is the LOWER cut, so the stretch ends at its
    inlet (600) and does not run on through the lake to its outlet."""
    for order in (("bridge", "lake"), ("lake", "bridge")):
        got = _resolve(_two_lines(), "between", *order)
        assert got["sections"] == ["100:0", "200:600"]
        assert got["ambiguous_cut"] == [{"split_id": "lake", "used": 600, "also_at": [300],
                                         "candidates": [300, 600], "toward": "bridge"}]


# ------------------------------------------------------------------------- mutation pins

def test_the_ruling_is_what_moves_the_cut(monkeypatch):
    """Mutation pin: with the policy off, the old nearest-the-mouth pick comes back and the stretch
    above the lake spans it. If this passes with the flag off, the tests above prove nothing."""
    monkeypatch.setattr(X, "AMBIGUOUS_CUT_NEAREST_THE_OTHER_END", False)
    got = _resolve(_river(), "upstream_of", "lake")
    assert got["window"] == ("M", 2000, INF)
    assert "M:2000" in got["sections"]
    got = _resolve(_two_lines(), "between", "bridge", "lake")
    assert "200:300" in got["sections"]


def test_the_policy_is_pinned_by_the_cache_version():
    """A cached reach built before the ruling must not be served after it."""
    from pipeline.atlas.reach import cache
    assert X.AMBIGUOUS_CUT_NEAREST_THE_OTHER_END is True
    assert int(cache.POLICY_VERSION) >= 20


# ------------------------------------------------------------------------------ real cases

import os                                                                    # noqa: E402
from pathlib import Path                                                     # noqa: E402

from pipeline.tests.conftest import ATLAS_HINT, need                         # noqa: E402


@pytest.fixture(scope="module")
def real(request):
    from pipeline.atlas.reach.models import iter_entries
    from pipeline.atlas.registry import load_registry
    from pipeline.common.curated import GENERATED
    from pipeline.common.io.serialize import read_artifact
    from pipeline.regs.parsing import io as parse_io
    b = Path(os.environ.get("ATLAS_BUILD") or GENERATED.build())
    need(request, "atlas", b / "graph.pkl", ATLAS_HINT)
    g = read_artifact(str(b / "graph.pkl"))
    reg = load_registry(str(b / "registry.json"))
    corpus = {e["entry_id"]: e for e in iter_entries(parse_io.read_all_entries(registry=reg))}
    return g, reg, corpus


def _amb(real, eid, rid):
    """The rule's `ambiguous_cut` diagnostics, keyed by split id."""
    from pipeline.atlas.reach.build import build_reach
    g, reg, corpus = real
    e = corpus[eid]
    _b, diags = build_reach(e, next(r for r in e["rules"] if r["rule_id"] == rid), reg, g)
    return {d.payload["split_id"]: d.payload for d in diags if d.kind == "ambiguous_cut"}


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_harrison_below_the_lake_starts_at_the_outlet(real):
    """The owner's example: km 20 (outlet) and km 84 (where the line enters the lake). "Downstream
    of Harrison Lake" is the river below the outlet — unchanged by the ruling."""
    amb = _amb(real, "r2:harrison_river_from_the_fraser_river_upstream_to_harrison_la@2-18", "harrison_river.r1")["harrison_river__harrison_lake"]
    assert round(amb["used"]) == 20178 and [round(x) for x in amb["also_at"]] == [84129]
    assert amb["toward"] == "mouth"


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_ash_between_elsie_and_dickson_lakes_takes_the_river_between_them(real):
    """Both cuts are lakes on the Ash: the stretch runs from Dickson Lake's inlet (14,995) to Elsie
    Lake's outlet (26,178) — it used to run from Dickson's OUTLET, through the lake."""
    amb = _amb(real, "r1:ash_river@1-7", "ash_river.r1")
    assert round(amb["ash_river__dickson_lake"]["used"]) == 14995
    assert round(amb["ash_river__elsie_lake"]["used"]) == 26178


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_mohun_between_the_bridge_and_morton_lake_ends_at_the_outlet(real):
    amb = _amb(real, MOHUN, "mohun_creek.r2")["mohun_creek__morton_lake"]
    assert round(amb["used"]) == 13250 and [round(x) for x in amb["also_at"]] == [14176]


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_cayoosh_below_the_falls_starts_at_the_lower_crossing(real):
    amb = _amb(real, CAYOOSH, "cayoosh_creek.r2")["cayoosh_creek__cayoosh_falls"]
    assert round(amb["used"]) == 1771 and amb["toward"] == "mouth"


MOHUN = "r1:mohun_creek@1-10"
CAYOOSH = "r3:cayoosh_creek@3-16"
