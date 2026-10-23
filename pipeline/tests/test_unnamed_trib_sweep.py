"""T57 — unnamed streams carry their regulations: a page for every one, and every "and tributaries" rule
that should reach them (`pipeline.tools.unnamed_trib_sweep`).

No data: the check's rules on small graphs, each a whole value (AGENTS 33). THE KEY RULE — an unnamed
stream draining into water that carries a walking rule carries it too — is mutation-pinned twice: on a
stub (drop the binding, the miss appears, exactly) and on the live bundle (drop one real binding, the
sweep finds exactly it).

On the data (`needs_bundle`; the sweep also `needs_atlas` + `needs_reach_run` and is `slow` — it
unpickles the graph): every unnamed section in B.C. has an answer key with cells and a name in the
store the release is cut from (the page T53 opens); the sweep reproduces the committed
`fixtures/t57_trib_sweep.json` and that result has ZERO unexplained misses. `UI_EXPORT_BUNDLE` /
`ATLAS_BUILD` point it at another build (the reach run is the bundle's own, `meta.reach_run`).
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.tools import unnamed_trib_sweep as S

R = ("r1:eve_river@1-10", "eve_river.r1")
Q = ("r1:other@1-10", "other.r1")


def st(order=1, wsc="100-1", blk="b", barrier=False, down_m=0.0, up_m=100.0):
    return ("stream", order, wsc, barrier, blk, down_m, up_m)


def lk(order=1, wsc="100-1"):
    return ("lake", order, wsc, False, "", 0.0, 0.0)


def bound(rules, unnamed, walking=(R, Q), **kw):
    return S.Bound(rules=rules, unnamed=set(unnamed), regions=kw.pop("regions", {}),
                   walking={r: {} for r in walking}, **kw)


# ---- the key rule ------------------------------------------------------------------------------------------------

#  1 the named river, R by its reach  <- 2 unnamed (confluence)  <- 3 unnamed (confluence)
RIVER = S.topo_of({1: st(4, "100", "B"), 2: st(1, "100-7", "u2"), 3: st(1, "100-7-1", "u3")},
                  [(2, 1, "confluence"), (3, 2, "confluence")])


def test_an_unnamed_stream_carries_the_walking_rule_of_the_water_it_drains_into():
    whole = bound({1: {R: "reach"}, 2: {R: "trib"}, 3: {R: "trib"}}, {2, 3})
    assert S.frontier(RIVER, whole) == []
    assert S.sweep(RIVER, whole)["unexplained"] == 0


def test_mutation_drop_the_binding_and_the_miss_is_found_at_its_lowest_section():
    """The pin: the lowest section without R is reported once, with what it costs (itself and the one above)."""
    cut = bound({1: {R: "reach"}, 2: {}, 3: {}}, {2, 3})
    assert S.frontier(RIVER, cut) == [S.Miss(sid=2, rule=R, below=1, via="reach", lake=None)]
    assert S.sweep(RIVER, cut) == {
        "frontier": 1, "by_reason": {"unexplained": 1}, "explained": {}, "unexplained": 1,
        "unexplained_sections": 2,
        "by_rule": {"r1:eve_river@1-10::eve_river.r1": {
            "frontier": 1, "sections": 2, "examples": [{"sid": 2, "below": 1, "via": "reach", "lake": None}]}},
        "by_region": {"?": 1}, "unnamed_lake_at_reach_top": {}}
    # ...and only R: a rule that does not walk hands nothing up
    assert S.frontier(RIVER, bound({1: {R: "reach"}, 2: {}, 3: {}}, {2, 3}, walking=())) == []


def test_a_walk_continues_up_a_tributary_it_holds():
    """`trib` on the section below is handed on whatever the edge: inside a tributary its own continuation is followed."""
    t = S.topo_of({1: st(2, "100-7", "T"), 2: st(1, "100-7", "T", down_m=100.0)}, [(2, 1, "continuation")])
    assert S.frontier(t, bound({1: {R: "trib"}, 2: {}}, {2})) == [S.Miss(2, R, 1, "trib", None)]


# ---- the walk's own stops, applied while looking -----------------------------------------------------------------

def test_the_river_continuing_above_its_reach_is_not_a_tributary():
    t = S.topo_of({1: st(3, "100", "B"), 2: st(3, "100", "B", down_m=100.0)}, [(2, 1, "continuation")])
    assert S.frontier(t, bound({1: {R: "reach"}, 2: {}}, {2})) == []


def test_a_lake_is_climbed_through_and_a_lake_at_the_reach_top_stops_the_walk():
    #  1 river (R by reach) <-lake_out- 2 lake <-lake_in- 3 unnamed stream
    nodes = {1: st(3, "100", "B"), 2: lk(3, "100"), 3: st(1, "100-5", "c")}
    edges = [(2, 1, "lake_out"), (3, 2, "lake_in")]
    t = S.topo_of(nodes, edges)
    top = bound({1: {R: "reach"}, 2: {}, 3: {}}, {3}, unnamed_lakes={2})
    assert S.frontier(t, top) == []                                  # "downstream of the lake": not its inflows
    assert S.sweep(t, top)["unnamed_lake_at_reach_top"] == {"r1:eve_river@1-10::eve_river.r1": 1}
    # the same lake held by the WALK (a tributary lake): climbed, its inflow is a tributary
    assert S.frontier(t, bound({1: {R: "trib"}, 2: {}, 3: {}}, {3})) == [S.Miss(3, R, 1, "trib", 2)]
    # MID-REACH: the reach goes on above the lake on the same line, so the river passes through it
    mid = S.topo_of({**nodes, 4: st(3, "100", "B", down_m=200.0, up_m=300.0)}, edges + [(4, 2, "lake_in")])
    got = S.frontier(mid, bound({1: {R: "reach"}, 2: {}, 3: {}, 4: {R: "reach"}}, {3}))
    assert got == [S.Miss(3, R, 1, "reach", 2)]


# ---- explanations ------------------------------------------------------------------------------------------------

def _one(t, b):
    (m,) = S.frontier(t, b)
    return S.explain(t, b, m)


def test_each_stop_the_walk_makes_on_purpose_explains_a_miss_and_nothing_else_does():
    base = {1: {R: "reach"}, 2: {}}
    two = lambda **kw: S.topo_of({1: st(2, "100", "B"), 2: st(**kw)}, [(2, 1, "confluence")])  # noqa: E731
    assert _one(two(wsc="100-1"), bound(base, {2})) is None
    assert _one(two(wsc="100-1", barrier=True), bound(base, {2})) == "barrier"
    assert _one(two(order=3, wsc="200"), bound(base, {2})) == "strahler"
    assert _one(two(order=3, wsc="100-1"), bound(base, {2})) is None       # the code says tributary: it wins
    assert _one(two(wsc="100-1"), bound(base, {2}, limits={R: frozenset({1})})) == "within_area"
    assert _one(two(wsc="100-1"), bound(base, {2}, limits={R: frozenset({1, 2})})) is None
    clip = {R: {"region_clip"}}
    assert _one(two(wsc="100-1"), bound(base, {2}, facts=clip, regions={2: ("2",)})) == "region_clip"
    assert _one(two(wsc="100-1"), bound(base, {2}, facts=clip, regions={2: ("1",)})) is None
    assert _one(two(wsc="100-1"), bound(base, {2}, kinds={R: frozenset({"lake"})})) == "feature_types"


def test_the_river_through_a_reach_lake_and_a_stem_bounded_at_its_lake():
    # 3 runs on the lake's outlet line "B": the river through the lake, not a tributary of it
    t = S.topo_of({1: st(3, "100", "B"), 2: lk(3, "100"), 3: st(3, "100", "B", down_m=500.0)},
                  [(2, 1, "lake_out"), (3, 2, "lake_in")])
    assert _one(t, bound({1: {}, 2: {R: "reach"}, 3: {}}, {3})) == "through_lake"
    # RU-14: the stem a confluence cut pulls in ends at its first lake
    t2 = S.topo_of({1: st(3, "100", "M"), 2: lk(3, "100"), 3: st(1, "100-4", "c")},
                   [(2, 1, "lake_out"), (3, 2, "lake_in")])
    assert _one(t2, bound({1: {R: "trib"}, 2: {}, 3: {}}, {3}, stems={R: frozenset({1})})) \
        == "stem_ends_at_lake"
    assert _one(t2, bound({1: {R: "trib"}, 2: {}, 3: {}}, {3})) is None


def test_stem_ends_at_lake_explains_only_the_lake_at_the_top_of_the_joining_waters_stem():
    """Review pin: RU-14 bounds ONE joining water's stem at its first lake. A lake on any OTHER tributary the
    same rule walks is climbed through, so an inflow there without the rule is a real miss — the rule having
    a bounded stem somewhere must not launder it."""
    t2 = S.topo_of({1: st(3, "100", "M"), 2: lk(3, "100"), 3: st(1, "100-4", "c")},
                   [(2, 1, "lake_out"), (3, 2, "lake_in")])
    other_stem = bound({1: {R: "trib"}, 2: {}, 3: {}}, {3}, facts={R: {"stem_ends_at_lake"}},
                       stems={R: frozenset({99})})
    assert _one(t2, other_stem) is None


def test_reasons_are_exactly_the_stops_explain_returns():
    assert S.REASONS == ("barrier", "strahler", "through_lake", "stem_ends_at_lake", "within_area",
                         "region_clip", "feature_types")


# ---- MU lines ----------------------------------------------------------------------------------------------------

def test_an_mu_group_rule_binds_its_members_and_a_straddler_is_marked_in():
    sections = {1: ("stream", ("1-6",)), 2: ("stream", ("1-6", "1-10")), 3: ("stream", ("1-10",)),
                4: ("lake", ("1-5",)), 5: ("stream", ("1-10", "1-6"))}
    group = frozenset({"1-1", "1-5", "1-6"})
    # 5 touches 1-6 at a point only (stamped, not a member): not bound, and that is right
    assert S.mu_lines(sections, frozenset({1, 2, 4}), group, frozenset({"stream"}), {1, 2}) == {
        "straddling": 2, "straddling_bound": 1, "bound_not_member": 0, "member_not_bound": 0,
        "stamped_not_member": 1, "member_not_stamped": 0}
    # a member of the right kind left out, and a section bound outside the group, are both counted
    assert S.mu_lines(sections, frozenset({1, 2, 4}), group, None, {1, 3}) == {
        "straddling": 2, "straddling_bound": 0, "bound_not_member": 1, "member_not_bound": 2,
        "stamped_not_member": 1, "member_not_stamped": 0}


# ---- on the data -------------------------------------------------------------------------------------------------

def _paths():
    from pipeline.common.curated import CURATED, GENERATED
    bundle = Path(os.environ.get("UI_EXPORT_BUNDLE") or GENERATED.bundle / "bundle.sqlite")
    build = Path(os.environ.get("ATLAS_BUILD") or GENERATED.build())
    return bundle, build, Path(CURATED.waters.areas)


@pytest.mark.needs_bundle
def test_every_unnamed_section_has_a_page():
    """T53's whole-map count = 0: every unnamed section of the bundle has a key in the store, the key has cells,
    and the store names it ("Unnamed stream · Region N") — the page the water screen opens."""
    from pipeline.deliver.store.decode import Store
    bundle, _, _ = _paths()
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    unnamed = [sid for (sid,) in db.execute("SELECT sid FROM section_ruleset WHERE up IS NOT NULL ORDER BY sid")]
    st = Store(bundle.parent / "regs.sqlite", verify=False)
    assert st.meta["reach_digest"] == dict(db.execute("SELECT k, v FROM meta"))["reach_digest"]
    akeys = st.section_akeys()
    with_cells = {a for (a,) in st.con.execute("SELECT DISTINCT akey FROM cell")}
    no_key = [s for s in unnamed if not akeys.get(s)]
    no_cells = sorted({akeys[s] for s in unnamed if akeys.get(s) and akeys[s] not in with_cells})
    sample = unnamed[:: max(1, len(unnamed) // 500)]
    unnamed_names = sorted({(st.unnamed_name(s) or "").split(" · ")[0] for s in sample})
    assert (len(unnamed) > 1_000_000, no_key, no_cells, unnamed_names) == (
        True, [], [], ["Unnamed lake", "Unnamed stream", "Unnamed wetland"])


@pytest.fixture(scope="module")
def live():
    bundle, build, areas = _paths()
    if not (bundle.exists() and (build / "graph.pkl").exists()):
        return None                     # each test's `need` fails with the make-command
    t = S.topology(build)
    meta = dict(sqlite3.connect(f"file:{bundle}?mode=ro", uri=True).execute("SELECT k, v FROM meta"))
    reach = bundle.parent.parent / "reaches" / meta["reach_run"]
    return t, bundle, build, reach, areas


def _need_all(request, live):
    from pipeline.tests.conftest import ATLAS_HINT, BUNDLE_HINT, REACH_HINT, need
    bundle, build, _ = _paths()
    need(request, "bundle", bundle, BUNDLE_HINT)
    need(request, "atlas", build / "graph.pkl", ATLAS_HINT)
    need(request, "reach_run", live[3] / "rule_diagnostic.jsonl", REACH_HINT)


@pytest.mark.slow
@pytest.mark.needs_bundle
@pytest.mark.needs_atlas
@pytest.mark.needs_reach_run
def test_the_sweep_is_the_committed_result_and_nothing_is_unexplained(request, live):
    _need_all(request, live)
    t, bundle, build, reach, areas = live
    got = S.summary(S.run(build, bundle, reach, areas, t))
    want = json.loads(S.RESULT.read_text(encoding="utf-8"))
    assert got == want, ("the sweep moved: rerun `python -m pipeline.tools.unnamed_trib_sweep --write-result` "
                         "(through scripts/heavy.sh) and explain every new miss in T57")
    assert (got["unexplained"], got["by_rule"]) == (0, {})
    assert {k: (v["bound_not_member"], v["member_not_bound"], v["member_not_stamped"])
            for k, v in got["mu_groups"].items()} == {k: (0, 0, 0) for k in got["mu_groups"]}


@pytest.mark.slow
@pytest.mark.needs_bundle
@pytest.mark.needs_atlas
@pytest.mark.needs_reach_run
def test_mutation_drop_one_real_tributary_binding_and_the_sweep_finds_exactly_it(request, live):
    _need_all(request, live)
    t, bundle, build, reach, _ = live
    b, _ = S.bound_from(bundle, reach, build)
    base = {(m.sid, m.rule) for m in S.frontier(t, b)}
    # the first unnamed stream (by handle) holding a walk's rule that the stream it drains into also holds
    pick = next((u, r) for u in sorted(b.unnamed) for r, via in sorted(b.rules[u].items()) if via == "trib"
                and any(b.rules.get(d, {}).get(r) == "trib" and k == "confluence" for d, k in t.down.get(u, ())))
    u, r = pick
    b.rules = {**b.rules, u: {k: v for k, v in b.rules[u].items() if k != r}}
    added = [m for m in S.frontier(t, b) if (m.sid, m.rule) not in base]
    assert [(m.sid, m.rule, S.explain(t, b, m)) for m in added] == [(u, r, None)]
