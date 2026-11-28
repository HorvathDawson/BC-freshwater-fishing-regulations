"""T1242 (ruling E31): the creek-less braid loops every map build removes, and what it leaves (`--drop-small-loops`)."""

import pytest
from pathlib import Path
from shapely.geometry import LineString

from pipeline.atlas.graph import prune
from pipeline.atlas.graph.prune import drop_small_loops, small_loops
from dataclasses import replace

from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode

MAIN = "100-0001"
TRIB = "100-0001-0002"


def _n(nid, blk, down, up, *, wsc=MAIN, name="Big River"):
    return StreamNode(node_id=nid, kind=NodeKind.stream, blk=blk, wsc=wsc, display_name=name,
                      down_m=float(down), up_m=float(up), length_m=float(up - down))


def _graph(nodes, edges):
    g = StreamGraph()
    for n in nodes:
        g.nodes[n.node_id] = n
    for frm, to, m, x, y in edges:
        g.edges.append(FlowEdge(from_node=frm, to_node=to, at_measure=float(m), x=float(x), y=float(y)))
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


def _river(loop_name="Big River", far=False, creek=True):
    """The river M runs north to south along x = 0 (geometry is stored downstream end first): M:hi 1000-2000 m, M:lo
    0-1000 m. Loop L leaves M:hi at 1500 m, bows 40 m east and rejoins M:lo at 500 m; creek C enters L at (40, 1200).
    With `far`, F leaves M:hi at 1800 m, bows 300 m east and rejoins M:lo at 100 m."""
    nodes = [_n("M:hi", "M", 1000, 2000), _n("M:lo", "M", 0, 1000), _n("L:0", "L", 0, 1100, name=loop_name)]
    edges = [("M:hi", "M:lo", 1000, 0, 1000), ("M:hi", "L:0", 1100, 0, 1500), ("L:0", "M:lo", 500, 0, 500)]
    geoms = {"M:hi": LineString([(0, 1000), (0, 2000)]), "M:lo": LineString([(0, 0), (0, 1000)]),
             "L:0": LineString([(0, 500), (40, 1200), (0, 1500)])}
    if creek:
        nodes.append(_n("C:0", "C", 0, 460, wsc=TRIB, name=""))
        edges.append(("C:0", "L:0", 700, 40, 1200))
        geoms["C:0"] = LineString([(40, 1200), (500, 1200)])
    if far:
        nodes.append(_n("F:0", "F", 0, 2000))
        edges += [("M:hi", "F:0", 2000, 0, 1800), ("F:0", "M:lo", 100, 0, 100)]
        geoms["F:0"] = LineString([(0, 100), (300, 1000), (0, 1800)])
    return _graph(nodes, edges), geoms


def test_the_trial_values_are_pinned():
    """Mutation pins: the trial is 100 m and, by the owner's direction for 0.10.0, a loop with a creek on it stays."""
    assert prune.SMALL_LOOP_TRIAL_M == 100.0
    assert prune.SMALL_LOOP_CREEKS == "keep"


def test_the_pass_is_on_by_default_any_size():
    """Ruling E31: creek-less loops go in every map build, any size; `--keep-small-loops` is the only way to keep them."""
    from pipeline.atlas.build import _parse_args
    assert _parse_args(["--full"]).drop_small_loops == float("inf")
    assert _parse_args(["--full", "--keep-small-loops"]).drop_small_loops is None
    assert _parse_args(["--full", "--drop-small-loops", "100"]).drop_small_loops == 100.0


@pytest.fixture
def tail_mode(monkeypatch):
    monkeypatch.setattr(prune, "SMALL_LOOP_CREEKS", "tail")


def test_the_default_keeps_a_loop_a_creek_flows_into():
    """The owner's 0.10.0 direction: only creek-less loops go."""
    g, geoms = _river(far=True)
    why = {}
    assert small_loops(g, geoms, 100.0, why=why) == [] and why["a creek flows into it"] == 1
    g, geoms = _river(creek=False)
    assert [lp["nodes"] for lp in small_loops(g, geoms, 100.0)] == [["L:0"]]


def test_a_near_loop_goes_and_a_far_one_stays(tail_mode):
    g, geoms = _river(far=True)
    why = {}
    loops = small_loops(g, geoms, 100.0, why=why)
    assert [lp["nodes"] for lp in loops] == [["L:0"]]
    assert loops[0]["offset_m"] == pytest.approx(40.0, abs=0.5) and loops[0]["tribs"] == ["C:0"]
    assert why == {"removed, creek takes the tail": 1, "far side 100 m or more from the river": 1}


def test_the_creek_is_re_homed_onto_the_river_where_it_is_nearest(monkeypatch):
    """Variant (a), "rehome"."""
    monkeypatch.setattr(prune, "SMALL_LOOP_CREEKS", "rehome")
    g, geoms = _river()
    moved = []
    g, n, _ = drop_small_loops(g, geoms, small_loops(g, geoms, 100.0), moved)
    assert n == 1 and "L:0" not in g.nodes and "L:0" not in geoms
    out = [e for e in g.edges if e.from_node == "C:0"]
    assert len(out) == 1 and out[0].to_node == "M:hi"
    assert out[0].at_measure == pytest.approx(1200.0) and (out[0].x, out[0].y) == pytest.approx((0.0, 1200.0))
    assert moved == [("C:0", "L:0", "M:hi", 1200.0, 40.0)]
    assert not any("L:0" in (e.from_node, e.to_node) for e in g.edges)
    assert {g.edges[i].to_node for i in g.down_adj["M:hi"]} == {"M:lo"}, "the river goes on down its own line"


def test_a_named_channel_and_a_kept_blk_are_never_removed():
    g, geoms = _river(loop_name="Seabird Side Channel")
    assert small_loops(g, geoms, 100.0) == []
    g, geoms = _river()
    assert small_loops(g, geoms, 100.0, keep_blks={"L"}) == []


def test_without_rehoming_a_loop_a_creek_enters_stays(monkeypatch):
    monkeypatch.setattr(prune, "SMALL_LOOP_CREEKS", "keep")
    g, geoms = _river()
    why = {}
    assert small_loops(g, geoms, 100.0, why=why) == [] and why == {"a creek flows into it": 1}
    g, geoms = _river(creek=False)
    assert [lp["nodes"] for lp in small_loops(g, geoms, 100.0)] == [["L:0"]]


def test_a_loop_off_a_side_channel_is_not_judged():
    """Only a loop off the river's MAIN line: one hanging off a named (kept) side channel stays."""
    g, geoms = _river(creek=False)
    g.nodes["N:0"] = _n("N:0", "N", 0, 1000, name="Old Channel")
    g.nodes["K:0"] = _n("K:0", "K", 0, 300)
    for frm, to in (("M:hi", "N:0"), ("N:0", "M:lo"), ("N:0", "K:0"), ("K:0", "N:0")):
        g.edges.append(FlowEdge(from_node=frm, to_node=to, at_measure=0.0))
    g.up_adj, g.down_adj = {}, {}
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    geoms["N:0"] = LineString([(0, 200), (-60, 1000), (0, 1900)])
    geoms["K:0"] = LineString([(-60, 1000), (-70, 1100), (-60, 1200)])
    why = {}
    assert [lp["nodes"] for lp in small_loops(g, geoms, 100.0, why=why)] == [["L:0"]]
    assert why["off a side channel or another water, not the main line"] == 1


def test_a_loop_that_rejoins_its_own_piece_goes_without_a_self_loop():
    g = _graph([_n("M:0", "M", 0, 2000), _n("L:0", "L", 0, 900)],
               [("M:0", "L:0", 900, 0, 1500), ("L:0", "M:0", 700, 0, 700)])
    geoms = {"M:0": LineString([(0, 0), (0, 2000)]), "L:0": LineString([(0, 700), (30, 1100), (0, 1500)])}
    loops = small_loops(g, geoms, 100.0)
    assert loops[0]["same_piece"] is True, "T1247's case, flagged so a build can tell it apart"
    g, n, _ = drop_small_loops(g, geoms, loops)
    assert n == 1 and set(g.nodes) == {"M:0"} and g.edges == []


def test_a_loop_that_is_the_rivers_only_way_on_is_put_back():
    """Connectivity is checked: M:top's only way downstream is the loop, so nothing is removed."""
    g = _graph([_n("M:top", "M", 2000, 3000), _n("M:lo", "M", 0, 1000), _n("L:0", "L", 0, 1200)],
               [("M:top", "L:0", 1200, 0, 2000), ("L:0", "M:lo", 1000, 0, 1000)])
    geoms = {"M:top": LineString([(0, 1000), (0, 2000)]), "M:lo": LineString([(0, 0), (0, 1000)]),
             "L:0": LineString([(0, 1000), (20, 1500), (0, 2000)])}
    loops = small_loops(g, geoms, 100.0)
    assert [lp["nodes"] for lp in loops] == [["L:0"]]
    g, n, _ = drop_small_loops(g, geoms, loops)
    assert n == 0 and "L:0" in g.nodes


def test_no_geometry_is_refused_not_guessed():
    g, _geoms = _river()
    with pytest.raises(ValueError):
        small_loops(g, {}, 100.0)


# ---- variant (d): the creek takes the tail -----------------------------------------------------------------------

def test_the_creek_takes_the_loops_lower_part_as_its_own_last_reach(tail_mode):
    """One line the whole way down: L is cut at the creek's junction (700 m up it), the lower 700 m becomes the creek's
    (its code, its name), its mouth is where the loop met the river, and there is no gap."""
    g, geoms = _river()
    moved = []
    loops = small_loops(g, geoms, 100.0)
    assert loops[0]["tail"] == {"L:0": ["C:0", 700.0]}
    g, n, _ = drop_small_loops(g, geoms, loops, moved)
    assert n == 0 and "L:0" in g.nodes, "nothing goes whole: the loop is cut, not removed"
    t = g.nodes["L:0"]
    assert (t.wsc, t.display_name, t.up_m, t.length_m) == (TRIB, "", 700.0, 700.0)
    assert geoms["L:0"].length == pytest.approx(700.0, abs=1.0)
    assert {(e.from_node, e.to_node) for e in g.edges} == {("M:hi", "M:lo"), ("C:0", "L:0"), ("L:0", "M:lo")}
    assert moved == [("C:0", "L:0", "M:lo", 500.0, 0.0)]


def test_two_creeks_on_one_loop_the_bigger_owns_the_shared_reach(tail_mode):
    g, geoms = _river()
    g.nodes["C:0"] = replace(g.nodes["C:0"], stream_magnitude=2)
    g.nodes["K:0"] = replace(_n("K:0", "K", 0, 300, wsc="100-0001-0003", name="Small Creek"), stream_magnitude=9)
    g.edges.append(FlowEdge(from_node="K:0", to_node="L:0", at_measure=300.0, x=17.0, y=800.0))
    _reindex(g)
    geoms["K:0"] = LineString([(17, 800), (300, 800)])
    loops = small_loops(g, geoms, 100.0)
    assert loops[0]["tail"] == {"L:0": ["K:0", 700.0]}, "K is bigger: the reach is K's; C joins it at 700 m"
    g, _n_, _ = drop_small_loops(g, geoms, loops)
    assert g.nodes["L:0"].display_name == "Small Creek"
    assert {e.from_node for e in g.edges if e.to_node == "L:0"} == {"C:0", "K:0"}


def test_a_loop_that_touches_a_lake_stays():
    """The river's feed into a lake (Bulkley, Serpentine): removing it let two closures go."""
    g, geoms = _river(creek=False)
    g.nodes["lake:1"] = StreamNode(node_id="lake:1", kind=NodeKind.lake, wbk="1", wsc=MAIN)
    g.edges.append(FlowEdge(from_node="L:0", to_node="lake:1", at_measure=0.0))
    _reindex(g)
    why = {}
    assert small_loops(g, geoms, 100.0, why=why) == [] and why == {"touches a lake": 1}


def test_part_of_a_longer_blue_line_stays():
    """Attichika Creek: removing one piece of a blue line left its other piece unjoined."""
    g, geoms = _river(creek=False)
    g.nodes["L:5000"] = _n("L:5000", "L", 5000, 5100)
    g.edges.append(FlowEdge(from_node="L:5000", to_node="M:hi", at_measure=1900.0))
    _reindex(g)
    geoms["L:5000"] = LineString([(0, 1900), (-80, 1960)])
    why = {}
    assert small_loops(g, geoms, 100.0, why=why) == [] and why["part of a longer blue line"] == 1


def _reindex(g):
    g.up_adj, g.down_adj = {}, {}
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)


# ---- whose water the tail is, and the order against the leaf prune ------------------------------------------------

def test_the_tail_rule_is_pinned():
    """Mutation pin: (e), the creek takes the tail unless that would take a river rule off it."""
    assert prune.SMALL_LOOP_TAIL_RULES == "unless-looser"


def test_a_tail_on_the_river_list_stays_the_rivers_side_channel(tail_mode):
    """(e): a tail whose renaming would loosen a rule (the keep file's `tail_river`) is cut but keeps the river's
    code and name; the creek's line ends at it."""
    g, geoms = _river()
    loops = small_loops(g, geoms, 100.0, tail_river_blks={"L"})
    assert loops[0]["rename"] is False
    g, _n_, _ = drop_small_loops(g, geoms, loops)
    t = g.nodes["L:0"]
    assert (t.wsc, t.display_name, t.up_m) == (MAIN, "Big River", 700.0)
    g2, geoms2 = _river()
    assert small_loops(g2, geoms2, 100.0, tail_river_blks={"X"})[0]["rename"] is True


def test_tail_rules_river_never_renames(monkeypatch, tail_mode):
    monkeypatch.setattr(prune, "SMALL_LOOP_TAIL_RULES", "river")
    g, geoms = _river()
    loops = small_loops(g, geoms, 100.0)
    g, _n_, _ = drop_small_loops(g, geoms, loops)
    assert g.nodes["L:0"].wsc == MAIN and g.nodes["L:0"].display_name == "Big River"


def test_the_loop_pass_runs_after_the_leaf_prune(tmp_path, tail_mode):
    """Run before it, a tail renamed to an UNNAMED creek put the creek's headwaters a hop further from a named water,
    and the leaf prune (3+ hops) dropped them: 4,197 creek lines in the province. After it, H1 (2 hops above the named
    loop today) stays; H2 (3 hops) goes either way."""
    import json as _json
    from types import SimpleNamespace
    from pipeline.atlas.build import prune_graph
    g, geoms = _river()
    g.nodes["C:0"] = replace(g.nodes["C:0"], stream_magnitude=2)
    g.nodes["H1:0"] = replace(_n("H1:0", "H1", 0, 300, wsc="100-0001-0002-0001", name=""), stream_magnitude=1)
    g.nodes["H2:0"] = replace(_n("H2:0", "H2", 0, 300, wsc="100-0001-0002-0001-0001", name=""), stream_magnitude=1)
    g.edges += [FlowEdge(from_node="H1:0", to_node="C:0", at_measure=460.0),
                FlowEdge(from_node="H2:0", to_node="H1:0", at_measure=300.0)]
    _reindex(g)
    geoms["H1:0"] = LineString([(500, 1200), (800, 1200)])
    geoms["H2:0"] = LineString([(800, 1200), (1100, 1200)])
    ex = tmp_path / "loop_exemptions.json"
    ex.write_text(_json.dumps(_doc([])))
    args = SimpleNamespace(simplify_braids=True, no_leaf_prune=False, drop_small_loops=100.0)
    b = SimpleNamespace(args=args, graph=g, geoms=geoms, fids=[], tick=lambda *_: None, out=tmp_path,
                        loop_keep_paths=(ex, {}))
    prune_graph(b)
    assert "H1:0" in b.graph.nodes, "the creek's headwater survives: the loop pass ran after the leaf prune"
    assert "H2:0" not in b.graph.nodes
    assert b.graph.nodes["L:0"].wsc == TRIB, "and the creek took the tail"


# ---- what keeps a loop: only names and structure, read in the map build (owner 2026-11-27) ------------------------

def _row(water, blks, rid, **kw):
    return {"water": water, "blks": blks, "why": "a book row names it", "id": rid, "words": "", "date": "2026-11-27", **kw}


def _doc(rows):
    return {"_about": "test", "format": 1, "rows": rows}


def _files(tmp_path, exempt=(), **structural):
    import json as _json
    ex = tmp_path / "loop_exemptions.json"
    ex.write_text(_json.dumps(_doc([_row("x", list(exempt) or ["999999999"], "r2:stave_river@2-8/stave_river.r1")])))
    paths = {}
    for reason, blk in structural.items():
        f = tmp_path / f"{reason}.json"
        f.write_text(_json.dumps({"somewhere": {"blk": blk}}))
        paths[reason.replace("_", " ")] = f
    return ex, paths


@pytest.mark.parametrize("kind", ["a_curated_cut", "a_tidal_limit", "a_gauge", "an_owner_call"])
def test_each_structural_keep_keeps_its_loop(tmp_path, kind):
    """A loop a cut, tidal limit, gauge or owner call names stays; take that file away and the same loop goes."""
    from pipeline.atlas.graph.prune import loop_keeps
    g, geoms = _river(creek=False)
    g.nodes["L:0"] = replace(g.nodes["L:0"], blk="123456789")
    ex, paths = _files(tmp_path, **{kind: "123456789"})
    keep = loop_keeps(ex, paths)
    assert keep["123456789"] == [kind.replace("_", " ")]
    assert small_loops(g, geoms, float("inf"), set(keep)) == []
    assert [lp["nodes"] for lp in small_loops(g, geoms, float("inf"), set(loop_keeps(ex, {})))] == [["L:0"]]


def test_a_named_exemption_keeps_its_loop(tmp_path):
    from pipeline.atlas.graph.prune import loop_keeps
    g, geoms = _river(creek=False)
    g.nodes["L:0"] = replace(g.nodes["L:0"], blk="987654321")
    ex, _ = _files(tmp_path, exempt=["987654321"])
    keep = loop_keeps(ex, {})
    assert keep["987654321"][0].startswith("named:")
    assert small_loops(g, geoms, float("inf"), set(keep)) == []
    ex2, _ = _files(tmp_path / ".." / tmp_path.name, exempt=[])
    assert small_loops(g, geoms, float("inf"), set(loop_keeps(ex2, {}))) != []


def test_a_named_exemption_the_atlas_lacks_stops_the_build(tmp_path):
    import json as _json
    from types import SimpleNamespace
    from pipeline.atlas.build import prune_graph
    g, geoms = _river(creek=False)
    ex = tmp_path / "loop_exemptions.json"
    ex.write_text(_json.dumps(_doc([_row("gone", ["111111111"], "x")])))
    args = SimpleNamespace(simplify_braids=True, no_leaf_prune=False, drop_small_loops=float("inf"))
    b = SimpleNamespace(args=args, graph=g, geoms=geoms, fids=[], tick=lambda *_: None, out=tmp_path,
                        loop_keep_paths=(ex, {}))
    with pytest.raises(SystemExit, match="does not have"):
        prune_graph(b)


def test_the_curated_exemptions_file_is_readable(tmp_path):
    """The real file is well formed (`exemption_problems`); the book's and DFO's words are stored whole, never cut."""
    import json as _json
    from pipeline.tests import under_test
    doc = _json.loads(open(under_test.curated().waters.loop_exemptions, encoding="utf-8").read())
    assert prune.exemption_problems(doc) == []
    assert doc["rows"] and all(r["blks"] and r["id"] and r["why"] for r in doc["rows"])
    dfo = [r for r in doc["rows"] if r["id"].startswith("DFO/")]
    assert dfo and all(r["words"] and r["words"] != r["id"].split("/", 1)[1] for r in dfo), "DFO's text, not the slug"
    assert all(r["words"] and r["words"][-1] in ".,;)abcdefghijklmnopqrstuvwxyz0123456789"
               for r in doc["rows"] if r["id"].startswith("r"))
    assert all(b.lstrip("-").isdigit() for r in doc["rows"] for b in r["blks"]), "blue-line keys only (a lake is no loop)"
    assert any(r["id"] == "owner:goat-river" and r["words"] for r in doc["rows"])


# ---- the review of 2026-11-27 (T1242): found names, cuts as placed, the record, the marks ------------------------

def _b(tmp_path, g, geoms, **kw):
    import json as _json
    from types import SimpleNamespace
    ex = tmp_path / "loop_exemptions.json"
    ex.write_text(_json.dumps(_doc([])))
    args = SimpleNamespace(simplify_braids=True, no_leaf_prune=False, drop_small_loops=float("inf"))
    return SimpleNamespace(args=args, graph=g, geoms=geoms, fids=[], tick=lambda *_: None, out=tmp_path,
                           loop_keep_paths=(ex, {}), **kw)


def test_a_loop_an_accepted_found_name_labels_stays(tmp_path):
    """Shovel Creek (blk 355997438, MU 2-18): an owner-accepted found name is a label, not a graph name, so the prune
    could not see it and the loop went. The build keeps a loop any found name labels (from found_names, no row)."""
    from pipeline.atlas.build import _small_loops
    g, geoms = _river(creek=False)
    b = _b(tmp_path, g, geoms, _mined=[{"target": {"blks": ["L"]}, "names": [{"name": "Shovel Creek"}]}])
    _small_loops(b, g, geoms, None)
    assert "L:0" in g.nodes
    g, geoms = _river(creek=False)
    b = _b(tmp_path, g, geoms, _mined=[])
    _small_loops(b, g, geoms, None)
    assert "L:0" not in g.nodes, "without the found name the same loop goes"


def test_a_keep_the_pass_removes_anyway_stops_the_build(tmp_path, monkeypatch):
    """The backstop: a line a keep names (here a found name) that the pass removes stops the build by name, never a
    silent skip downstream (`registry.mined` only counts an absent line)."""
    from pipeline.atlas.build import _small_loops
    real = prune.small_loops
    monkeypatch.setattr(prune, "small_loops", lambda g, geoms, m, keep, **kw: real(g, geoms, m, set(), **kw))
    g, geoms = _river(creek=False)
    b = _b(tmp_path, g, geoms, _mined=[{"target": {"blks": ["L"]}, "names": [{"name": "Shovel Creek"}]}])
    with pytest.raises(SystemExit, match=r"L \(a found name\)"):
        _small_loops(b, g, geoms, None)


def test_a_cut_is_kept_where_the_cut_placement_puts_it(tmp_path, monkeypatch):
    """A cut anchored by coordinate, tributary code, lake, unit or area boundary names no blue line; the pass asks the
    cut placement's own resolver (before any loop goes, with the boundary polygons the sectionizer loads) and keeps
    the loop it lands on. A B.C. border cut is made later by the border pass: one scoped to a blue line keeps it."""
    from types import SimpleNamespace
    from pipeline.atlas import build as B
    from pipeline.atlas.splits import anchors
    seen = {}

    def fake(defs, chains, live_blks=None, mu_polys=None, area_polys=None, **kw):
        seen.update(types=sorted(d.anchor.type for d in defs), live=live_blks, mu=mu_polys, area=area_polys)
        return [SimpleNamespace(blk="L", source="curated"), SimpleNamespace(blk="G", source="gauge")]
    monkeypatch.setattr(anchors, "resolve_split_defs", fake)
    monkeypatch.setattr(B, "get_mu_polys", lambda fwa: {"mu": fwa})
    monkeypatch.setattr(B, "get_area_polys", lambda fwa, specs, bbox=None: {"area": specs})
    a = lambda t, **kw: SimpleNamespace(anchor=SimpleNamespace(type=t, area_layer="parks", area_name_field="n",  # noqa: E731
                                                               area_name="P"), **kw)
    defs = [a(t) for t in ("point", "confluence", "lake", "line", "gauge", "mu_boundary", "area_boundary")]
    defs += [a("border", blk="B1"), a("border", blk=None)]
    g, geoms = _river(creek=False)
    b = _b(tmp_path, g, geoms, splits=defs, chains=[], matches_sha=None, fwa="FWA", bbox=None)
    _ex, files, sets = B.loop_keep_inputs(b, g)
    assert sets["a curated cut"] == {"L", "B1"} and sets["a gauge"] == {"G"}
    assert seen["types"] == ["area_boundary", "confluence", "gauge", "lake", "line", "mu_boundary", "point"]
    assert seen["mu"] == {"mu": "FWA"} and seen["area"] == {"area": [("parks", "n", "P")]}
    assert seen["live"] == {"M", "L"}, "asked on the graph before any loop goes"
    keep = prune.loop_keeps(_ex, {}, sets)
    assert small_loops(g, geoms, float("inf"), set(keep)) == []


def test_every_keep_kind_is_offered_in_a_default_build(tmp_path, monkeypatch):
    """The build's REAL keep list (`loop_keep_inputs`, read as a default build reads it): drop any kind and this fails."""
    from types import SimpleNamespace
    from pipeline.atlas import build as B
    from pipeline.atlas.registry import flowing
    from pipeline.atlas.splits import anchors
    monkeypatch.setattr(anchors, "resolve_split_defs", lambda *a, **k: [])
    monkeypatch.setattr(flowing, "FOLD_CONFIRMED", True)
    g, geoms = _river(creek=False)
    b = SimpleNamespace(args=SimpleNamespace(), _mined=[], chains=[], matches_sha="x",
                        splits=[SimpleNamespace(anchor=SimpleNamespace(type="point"))])
    ex, files, sets = B.loop_keep_inputs(b, g)
    assert {"named"} | set(files) | set(sets) == set(B.LOOP_KEEP_KINDS)
    assert B.LOOP_KEEP_KINDS == ("named", "a found name", "a curated cut", "a gauge", "a tidal limit",
                                 "an owner call", "an owner call (pond)")
    from pipeline.tests import under_test
    assert Path(ex) == Path(under_test.curated().waters.loop_exemptions)


@pytest.mark.parametrize("kind", ["a found name", "a curated cut", "a gauge"])
def test_each_computed_keep_keeps_its_loop(tmp_path, kind):
    g, geoms = _river(creek=False)
    ex, _ = _files(tmp_path)
    keep = prune.loop_keeps(ex, {}, {kind: {"L"}})
    assert keep["L"] == [kind] and small_loops(g, geoms, float("inf"), set(keep)) == []


def test_the_keeps_follow_the_build_flags(tmp_path, monkeypatch):
    """--no-splits: no cut, tidal limit or gauge is read (as `inputs.build_paths` says); FOLD_CONFIRMED off: no slough
    call; --no-gauge-splits (no matches_sha): the match file is not read."""
    from types import SimpleNamespace
    from pipeline.atlas import build as B
    from pipeline.atlas.registry import flowing
    from pipeline.atlas.splits import anchors
    monkeypatch.setattr(anchors, "resolve_split_defs", lambda *a, **k: [])
    g, _ = _river(creek=False)
    monkeypatch.setattr(flowing, "FOLD_CONFIRMED", False)
    _ex, files, sets = B.loop_keep_inputs(SimpleNamespace(_mined=[], splits=None), g)
    assert set(files) == {"an owner call (pond)"} and set(sets) == {"a found name"}
    _ex, files, sets = B.loop_keep_inputs(SimpleNamespace(_mined=[], chains=[], matches_sha=None,
                                                          splits=[SimpleNamespace(anchor=SimpleNamespace(type="point"))]), g)
    assert "a gauge" not in files and "a tidal limit" in files


def test_the_atlas_record_says_whether_the_pass_ran(tmp_path):
    """An atlas built with --keep-small-loops, or with a limit, never passes for a default one: `build.small_loops` in
    graph.pkl.meta.json, read by `inputs.behind`; the exemptions are recorded as not read."""
    import json as _json
    from pipeline.atlas import inputs as I
    from pipeline.atlas.build import _parse_args, small_loops_fact
    from pipeline.common.io.serialize import write_artifact
    assert small_loops_fact(_parse_args(["--full"])) == {"ran": True, "max_offset_m": "inf"}
    off = _parse_args(["--full", "--keep-small-loops"])
    assert small_loops_fact(off) == {"ran": False, "max_offset_m": None}
    assert I.build_paths(off)["curated:waters.loop_exemptions"].startswith("not read (--keep-small-loops")
    assert "curated:waters.loop_exemptions" not in I.build_paths(_parse_args(["--full"]))
    for args, want in ((off, "every braid loop kept"), (_parse_args(["--full", "--drop-small-loops", "100"]), "100.0 m"),
                       (_parse_args(["--full"]), None)):
        d = tmp_path / str(len(list(tmp_path.iterdir())))
        write_artifact({}, str(d / "graph.pkl"), input_hashes={"k": "v"}, build={"small_loops": small_loops_fact(args)})
        assert _json.loads((d / "graph.pkl.meta.json").read_text())["build"]["small_loops"] == small_loops_fact(args)
        got = I.loop_pass_behind(d)
        assert (got == []) if want is None else (len(got) == 1 and want in got[0])


def test_the_exemptions_file_is_validated(tmp_path):
    good = _doc([_row("x", ["123456789"], "a")])
    assert prune.exemption_problems(good) == []
    assert any("unknown key 'note'" in p for p in prune.exemption_problems(_doc([_row("x", ["1"], "a", note="n")])))
    assert any("no `why`" in p for p in prune.exemption_problems(
        _doc([{k: v for k, v in _row("x", ["1"], "a").items() if k != "why"}])))
    assert any("not a blue-line key" in p for p in prune.exemption_problems(_doc([_row("x", ["wbk:329"], "a")])))
    assert any("used twice" in p for p in prune.exemption_problems(_doc([_row("x", ["1"], "a"), _row("y", ["2"], "a")])))
    assert any("unknown key 'extra'" in p for p in prune.exemption_problems({**good, "extra": 1}))
    import json as _json
    ex = tmp_path / "e.json"
    ex.write_text(_json.dumps(_doc([_row("x", ["1"], "a", note="n")])))
    with pytest.raises(SystemExit, match="malformed"):
        prune.loop_keeps(ex, {})


def test_a_removed_loop_a_review_mark_names_refuses_the_promote(tmp_path):
    """Marks are no keep by design (E31) and no build reads them (T266): promote refuses a candidate whose removed
    loop line a mark names. READ ONLY: the store is never written. A sha's digit run is no blue line."""
    import json as _json
    from pipeline.atlas.promote import marked_loops
    cand = tmp_path / "cand"
    cand.mkdir()
    (cand / "small_loops.json").write_text(_json.dumps({"removed": [{"blks": ["355997438"]}, {"blks": ["356000001"]}]}))
    marks = tmp_path / "marks.jsonl"
    marks.write_text(_json.dumps({"note": "the channel blk:355997438 is missing", "pdf_sha256": "a356000001b"}) + "\n")
    before = marks.read_bytes()
    assert marked_loops(cand, lambda *_: None, marks) == ["355997438"]
    assert marks.read_bytes() == before
    assert prune.marked_removed_lines(["356000001"], "sha 0356000001ff and 1234567890123456") == []
