"""T166 step 5 (owner 2026-10-28, RULINGS C-2): THE PIECE BELOW A TIDAL BOUNDARY IS TIDAL WATER.

  limit     a `split` / `cut` row of waters/tidal_limits.json (T326) -> the atlas's `tidal.resolved.json`
            -> the reach run reads the ids from the atlas (`outside.tidal_splits`)
  piece     `downstream_of` the cut on every item that carries it — no tributary
            (`outside.TIDAL_PIECE_HAS_NO_TRIBUTARIES`, `tidal_cut_owner`)
  order     the tributary walk runs on the FULL network; the tidal piece leaves each binding AFTER
            it, so a creek entering tidal water stays its river's tributary (tributary-impact.md)
  loud      a rule left with no water is unresolved `tidal` and LISTED (`report.tidal_emptied`) —
            kitimat_river.r1 lies wholly below its proposed cut
  strict    a tidal split no item carries, or one the resolver cannot place, stops the run

No data: a few hand-made pieces (runs in the no-data tier).
"""
from __future__ import annotations

import json

import pytest

from pipeline.atlas.reach import outside as O
from pipeline.atlas.reach.build import build_reach, build_reaches, tidal_limit
from pipeline.atlas.reach.models import Outcome, Reason
from pipeline.atlas.splits.splits import RESOLVED_KEYS, order_resolved, write_resolved
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.common.models.enums import AnchorType, BoundaryKind
from pipeline.common.models.registry import RegistryBoundary, RegistryItem
from pipeline.common.models.sections import SectionBoundary
from pipeline.common.models.splits import SplitDef, SplitPoint

TIDE = "river__tidal_boundary"


def _b(sid, m):
    return SectionBoundary(boundary_id=f"split:{sid}", kind=BoundaryKind("split"), route_measure=m)


def _s(nid, blk, lo, hi, lower=None, upper=None, wsc="900-100000", order=4):
    return StreamNode(node_id=nid, kind=NodeKind.stream, blk=blk, wsc=wsc, down_m=lo, up_m=hi,
                      length_m=hi - lo, stream_order=order, lower_bound=lower, upper_bound=upper)


def _graph(nodes, edges):
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


@pytest.fixture
def estuary():
    """River R (blk R, mouth at 0) with its tidal boundary at 1,000 and falls at 3,000. Creek T1 joins
    at 500 — INSIDE the tidal piece; creek T2 joins at 2,000, above it."""
    tide, falls = _b(TIDE, 1000), _b("river__falls", 3000)
    g = _graph([
        _s("R:0", "R", 0, 1000, upper=tide, order=5),
        _s("R:1000", "R", 1000, 3000, lower=tide, upper=falls, order=5),
        _s("R:3000", "R", 3000, 5000, lower=falls, order=5),
        _s("T1:0", "T1", 0, 800, wsc="900-100000-050000", order=2),
        _s("T2:0", "T2", 0, 900, wsc="900-100000-400000", order=2),
    ], [("R:3000", "R:1000", "continuation", 3000), ("R:1000", "R:0", "continuation", 1000),
        ("T1:0", "R:0", "confluence", 500), ("T2:0", "R:1000", "confluence", 2000)])
    bds = tuple(RegistryBoundary(id=c, label=c, kind="split", ref=f"split:{c}")
                for c in (TIDE, "river__falls"))
    reg = {"gnis:1": RegistryItem(id="gnis:1", name="River", kind="stream",
                                  section_ids=("R:0", "R:1000", "R:3000"), boundaries=bds),
           "gnis:2": RegistryItem(id="gnis:2", name="One Creek", kind="stream", section_ids=("T1:0",)),
           "gnis:3": RegistryItem(id="gnis:3", name="Two Creek", kind="stream", section_ids=("T2:0",)),
           "area:region:1": RegistryItem(id="area:region:1", name="Region 1", kind="area",
                                         section_ids=("R:0", "R:1000", "R:3000", "T1:0", "T2:0"))}
    return g, reg


def _row(eid, rules, **kw):
    return {"entry_id": eid, "matched": ["gnis:1"], "rules": rules, **kw}


# ------------------------------------------------------------------------------- the piece

def test_the_tidal_piece_is_the_water_below_the_cut_and_no_tributary(estuary):
    g, reg = estuary
    rows: list = []
    got = O.tidal_cut_owner(reg, g, [TIDE], report=rows)
    assert got == {"R:0": f"split:{TIDE}"}                  # T1:0 joins it, and is not tidal
    assert rows == [{"split": TIDE, "items": ["gnis:1"], "sections": 1, "unplaced": [],
                     "ambiguous_cut": []}]
    assert O.is_cut_owner(got["R:0"]) and not O.is_cut_owner("r1:nitinat_lake@1-3")


def test_a_row_marked_tidal_keeps_its_claim_where_both_hold(estuary):
    g, reg = estuary
    row = {"entry_id": "r1:river@1-1", "matched": ["gnis:1"], "tidal": True}
    got = O.tidal_water([row], reg, g, [TIDE])
    assert got == {"R:0": "r1:river@1-1", "R:1000": "r1:river@1-1", "R:3000": "r1:river@1-1"}


def test_a_tidal_split_no_item_carries_stops_the_run(estuary):
    g, reg = estuary
    with pytest.raises(SystemExit, match="no registry item carries it"):
        O.tidal_cut_owner(reg, g, ["not_cut"])
    assert O.tidal_cut_owner(reg, g, ["not_cut"], strict=False) == {}


# ------------------------------------------------------------- walk first, then drop the piece

def _run(estuary, rules, tidal_splits=(TIDE,), **kw):
    g, reg = estuary
    res = build_reaches([_row("r1:river@1-1", rules, **kw)], reg, g, tidal_splits=tidal_splits)
    return res, {b.rule_id: b for b in res.bindings}


def test_a_creek_entering_tidal_water_stays_its_rivers_tributary(estuary):
    res, by = _run(estuary, [{"rule_id": "r1", "extents": [{"op": "whole"}],
                              "includes_tributaries": True}])
    b = by["r1"]
    assert b.outcome is Outcome.bound
    assert set(b.sections) == {"R:1000", "R:3000", "T1:0", "T2:0"}     # R:0 dropped, T1 kept
    assert "T1:0" in b.via_tributary
    assert res.tidal == {"R:0": f"split:{TIDE}"}
    assert res.report.tidal_cuts[0]["sections"] == 1 and res.report.tidal_emptied == []


def test_mutation_walking_the_freshwater_only_network_loses_the_creek(estuary):
    """The order is the requirement: drop the tidal piece BEFORE the walk and the creek that enters
    it is cut off (29 rules on the real corpus; Skeena r1 lost 130 estuary tributaries)."""
    g, reg = estuary
    fresh = _graph([n for nid, n in g.nodes.items() if nid != "R:0"],
                   [("R:3000", "R:1000", "continuation", 3000), ("T2:0", "R:1000", "confluence", 2000)])
    reg2 = dict(reg, **{"gnis:1": RegistryItem(id="gnis:1", name="River", kind="stream",
                                               section_ids=("R:1000", "R:3000"),
                                               boundaries=reg["gnis:1"].boundaries)})
    b, _ = build_reach({"entry_id": "r1:river@1-1", "matched": ["gnis:1"]},
                       {"rule_id": "r1", "extents": [{"op": "whole"}], "includes_tributaries": True},
                       reg2, fresh)
    assert "T1:0" not in b.sections


def test_a_rule_wholly_below_the_cut_is_unresolved_and_listed(estuary):
    """kitimat_river.r1: its whole extent lies below the proposed Kitimat cut."""
    res, by = _run(estuary, [
        {"rule_id": "r1", "extents": [{"op": "downstream_of", "splits": [TIDE]}]},
        {"rule_id": "r2", "extents": [{"op": "upstream_of", "splits": [TIDE]}]}])
    assert by["r1"].outcome is Outcome.unresolved and by["r1"].reason is Reason.tidal
    assert set(by["r2"].sections) == {"R:1000", "R:3000"}
    assert res.report.tidal_emptied == [{"entry_id": "r1:river@1-1", "rule_id": "r1",
                                         "splits": [TIDE]}]
    d = [x for x in res.diagnostics if x.kind == "tidal" and x.rule_id == "r1"]
    assert d and d[0].payload["splits"] == [TIDE]


def test_zone_rules_stop_at_the_tidal_boundary(estuary):
    g, reg = estuary
    zone = {"entry_id": "z1:quota", "matched": [],
            "rules": [{"rule_id": "z", "extents": [{"op": "within", "area_id": "area:region:1"}]}]}
    res = build_reaches([zone], reg, g, tidal_splits=(TIDE,))
    assert set(res.bindings[0].sections) == {"R:1000", "R:3000", "T1:0", "T2:0"}


def test_mutation_without_tidal_splits_the_piece_stays_freshwater(estuary):
    res, by = _run(estuary, [{"rule_id": "r1", "extents": [{"op": "whole"}]}], tidal_splits=())
    assert "R:0" in by["r1"].sections and res.tidal == {}


def test_a_row_marked_tidal_still_loses_the_piece_below_a_cut(estuary):
    """A tidal row keeps its OWN water (Nitinat Lake's note), not a cut's tidal piece."""
    tidal = {"R:0": f"split:{TIDE}", "lake:9": "r1:lagoon@1-3"}
    assert tidal_limit({"entry_id": "r1:lagoon@1-3", "tidal": True}, tidal) == {"R:0": f"split:{TIDE}"}
    assert tidal_limit({"entry_id": "r1:lagoon@1-3", "tidal": True}, frozenset(tidal)) is None
    assert tidal_limit({"entry_id": "z"}, tidal) is tidal


# ------------------------------------------------------------------- the flag through the atlas

def _sidecar(tmp_path, rows):
    from pipeline.atlas.splits.tidal import write_resolved as write_tidal
    write_tidal(tmp_path, rows)


def test_the_tidal_limits_reach_the_run_through_the_atlas_sidecar(tmp_path):
    """T326: which cut is tidal is a row of waters/tidal_limits.json, resolved by the atlas into `tidal.resolved.json`; the
    run reads the `split` / `cut` ids and the `whole` items from it — never a flag on splits.json."""
    _sidecar(tmp_path, [{"blk": "1", "limit": "split", "split_id": TIDE},
                        {"blk": "2", "limit": "cut", "split_id": "creek__tidal_boundary"},
                        {"blk": "3", "limit": "mouth", "alias": "x__tidal_boundary"},
                        {"blk": "4", "limit": "whole", "item": "gnis:2"}])
    assert O.tidal_splits(tmp_path) == ("creek__tidal_boundary", TIDE)
    assert O.tidal_waters(tmp_path) == ("gnis:2",)
    with pytest.raises(SystemExit, match="missing"):
        O.tidal_splits(tmp_path / "nowhere")
    assert "tidal" not in RESOLVED_KEYS
    with pytest.raises(ValueError, match="carries"):
        order_resolved({"split_id": TIDE, "tidal": True})


def test_splits_json_may_not_flag_a_cut_tidal(tmp_path):
    from pipeline.atlas.splits.splits import load_split_defs
    p = tmp_path / "splits.json"
    p.write_text(json.dumps({"waterbodies": [{"name": "R", "applies_to": {"gnis_id": "1"}, "splits": [
        {"id": TIDE, "label": "tidal boundary", "tidal": True,
         "anchor": {"type": "point", "coord": [-123.1, 49.3], "is_lonlat": True}}]}]}))
    with pytest.raises(ValueError, match="waters/tidal.json"):
        load_split_defs(str(p))


def test_an_old_resolved_row_loses_its_tidal_key_in_the_sidecar_migration():
    from pipeline.atlas.sidecars import enrich_resolved
    d = SplitDef.from_dict({"id": "fraser__cpr", "gnis_id": "1", "label": "CPR Bridge",
                            "anchor": {"type": "point", "coord": [-122.3, 49.13], "is_lonlat": True}})
    got = enrich_resolved([{"split_id": "fraser__cpr", "blk": "F", "route_measure": 1.0,
                            "label": "CPR Bridge", "anchor_type": "point", "tidal": True}], [d])
    assert "tidal" not in got[0]


# --------------------------------------------------------------- the preview, before any lands

def test_the_preview_lists_a_rule_wholly_below_a_cut(tmp_path):
    """kitimat_river.r1 (blk 360885824, 4295-5046 m) lies wholly below a cut at 7137 m."""
    import gzip
    from pipeline.tools.audit.tidal_preview import preview
    (tmp_path / "h.txt").write_text("K:0\nK:4295\nK:5046\nK:7137\nK:9000\nO:0\n")
    with gzip.open(tmp_path / "rs.jsonl.gz", "wt") as f:
        for eid, rid, secs in (("r6:kitimat@6-9", "kitimat_river.r1", ["K:4295"]),
                               ("r6:kitimat@6-9", "kitimat_river.r2", ["K:5046", "K:7137", "O:0"]),
                               ("z", "far", ["O:0"])):
            f.write(json.dumps({"entry_id": eid, "rule_id": rid, "sections": secs}) + "\n")
    got = preview([{"proposal": "p", "kind": "new", "split": "kitimat__tidal", "blk": "K",
                    "measure": 7137.0}], tmp_path / "rs.jsonl.gz", tmp_path / "h.txt")
    assert [r["rule_id"] for r in got["lose_all"]] == ["kitimat_river.r1"]
    assert got["lose_some"] == [{"entry_id": "r6:kitimat@6-9", "rule_id": "kitimat_river.r2",
                                 "sections": 3, "tidal": 1, "straddled": 0,
                                 "splits": ["kitimat__tidal"]}]


# ------------------------------------------------- off the map: the tiles, the area, the bundle's proof

def test_the_tiles_leave_off_the_reach_runs_own_set(estuary, tmp_path):
    from pipeline.deliver.tiles.tidal import tidal_pieces
    g, reg = estuary
    _sidecar(tmp_path, [{"blk": "R", "limit": "split", "split_id": TIDE}])
    assert tidal_pieces(tmp_path, graph=g, registry=reg, whole=()) == frozenset(O.tidal_cut_owner(reg, g, [TIDE]))
    _sidecar(tmp_path, [])
    assert tidal_pieces(tmp_path, graph=g, registry=reg, whole=()) == frozenset()
    # T212: a whole tidal water comes off the map too
    assert tidal_pieces(tmp_path, graph=g, registry=reg, whole=("gnis:2",)) == frozenset({"T1:0"})


def test_the_tidal_area_is_the_dfo_polygon_plus_pieces_outside_it():
    """A cut above the polygon's edge (Capilano's CN bridge) leaves a removed piece in neither: its
    footprint joins the area; a piece inside the polygon adds nothing."""
    from shapely.geometry import LineString, MultiPolygon, box
    from pipeline.deliver.tiles.tidal import PIECE_BUFFER_M, area_features
    sea = MultiPolygon([box(0, 0, 1000, 1000), box(5000, 0, 6000, 1000)])
    inside = LineString([(100, 100), (200, 200)])
    above = LineString([(500, 1000), (500, 1300)])          # runs 300 m up out of the sea
    got = area_features(sea, [inside, above])
    assert len(got) == 3                                      # two sea parts + one footprint
    extra = got[2]
    assert extra.bounds[3] == pytest.approx(1300 + PIECE_BUFFER_M, abs=1)
    assert extra.bounds[1] >= 1000 - 1e-6                     # nothing of the sea drawn twice
    assert area_features(None, [above])[0].area > 0


def test_the_bundle_proves_the_tiles_left_off_exactly_its_tidal_pieces():
    import sqlite3
    from pipeline.deliver.bundle import undrawn as U
    db = sqlite3.connect(":memory:")
    db.executescript(
        "CREATE TABLE tidal (sid INTEGER, entry_id TEXT); CREATE TABLE item (item_id TEXT, kind TEXT);"
        "CREATE TABLE entry (entry_id TEXT, item_id TEXT); CREATE TABLE ruleset (set_id INT, entry_id TEXT);"
        "CREATE TABLE item_section (ord INT, sid INT); CREATE TABLE section_gauge (sid INT, station TEXT, trust TEXT, mag REAL);"
        "CREATE TABLE section_ruleset (sid INT, set_id INT);"
        "INSERT INTO tidal VALUES (1, 'split:r__tide'), (2, 'split:r__tide'), (9, 'r1:nitinat_lake@1-3');"
        "INSERT INTO item_section VALUES (1, 1), (1, 2); INSERT INTO section_gauge VALUES (2, '08GA999', 'x', 0);")
    assert U.problems(db, {"tidal": [1, 2], "wetland": []}) == []   # in items, gauged: allowed
    assert U.tidal_gauges(db, {"tidal": [1, 2]}) == [(2, "08GA999")]
    assert any("still drawn" in x for x in U.problems(db, {"tidal": [1]}))
    assert any("no tidal piece" in x for x in U.problems(db, {"tidal": [1, 2, 9]}))


# ---------------------------------------------------------- T212: a whole water that is tidal (owner 2026-10-29)

def test_a_whole_tidal_water_is_every_section_of_it_and_off_the_map(estuary):
    """Parsons / Annacis / Cannery / Morey Channels: "this entire channel is tidal"."""
    g, reg = estuary
    rows: list = []
    got = O.tidal_whole_owner(reg, ["gnis:2"], report=rows)
    assert got == {"T1:0": "water:gnis:2"} and O.is_cut_owner("water:gnis:2")
    assert rows[0]["sections"] == 1
    with pytest.raises(SystemExit, match="not in the registry"):
        O.tidal_whole_owner(reg, ["gnis:404"])
    res = build_reaches([_row("r1:river@1-1", [{"rule_id": "r1", "extents": [{"op": "whole"}],
                                                "includes_tributaries": True}])],
                        reg, g, tidal_splits=(), tidal_waters=("gnis:2",))
    b = res.bindings[0]
    assert "T1:0" not in b.sections and {"R:0", "R:1000", "T2:0"} <= set(b.sections)
    assert res.tidal == {"T1:0": "water:gnis:2"}


def test_a_rule_on_a_whole_tidal_water_is_unresolved_and_listed(estuary):
    g, reg = estuary
    res = build_reaches([{"entry_id": "r1:one@1-1", "matched": ["gnis:2"],
                          "rules": [{"rule_id": "c", "extents": [{"op": "whole"}]}]}],
                        reg, g, tidal_waters=("gnis:2",))
    assert res.bindings[0].reason is Reason.tidal
    assert res.report.tidal_emptied == [{"entry_id": "r1:one@1-1", "rule_id": "c", "splits": ["water:gnis:2"]}]


def test_the_curated_whole_tidal_waters_are_the_owners_sixteen_fraser_delta_waters():
    """The 4 channels the owner marked (2026-10-29) + the 6 Fraser-code items below Mission he confirmed the same day
    (T326: `whole` rows of waters/tidal_limits.json, the owner's own calls) + the owner's whole-water slough list of
    2026-11-12 (RULINGS E24, T685: Deas, Woodward, McDonald, Tamboline, London) and Ewen Slough (owner A, 2026-11-11).
    Every other slough is cut at the river's edge by the coast rule."""
    from pipeline.atlas.splits import tidal as T
    whole = {r.item: r.name for r in T.load() if r.limit == "whole"}
    assert sorted(whole) == [
        "gnis:10494", "gnis:11566", "gnis:14010", "gnis:14905", "gnis:16081", "gnis:17368", "gnis:17805", "gnis:18526",
        "gnis:21224", "gnis:22631", "gnis:22880", "gnis:23895", "gnis:25152", "gnis:29381", "gnis:5325", "gnis:8341"]
    assert sorted(n for n in whole.values() if "Slough" in n) == [
        "Deas Slough", "Ewen Slough", "Gilmour Slough", "London Slough", "McDonald Slough", "Tamboline Slough",
        "Tilbury Slough", "Williamson Slough", "Woodward Slough"]


def test_a_row_marked_tidal_keeps_its_own_water_when_a_cut_exists_elsewhere(estuary):
    """Nitinat Lake's own row lost its lake once any tidal split existed (_without_tidal got the whole
    tidal map instead of the row's limit)."""
    g, reg = estuary
    row = {"entry_id": "r1:two@1-1", "matched": ["gnis:3"], "tidal": True,
           "rules": [{"rule_id": "n", "extents": [{"op": "whole"}]}]}
    res = build_reaches([row], reg, g, tidal_splits=(TIDE,))
    b = res.bindings[0]
    assert b.outcome is Outcome.bound and set(b.sections) == {"T2:0"}
    assert res.tidal["T2:0"] == "r1:two@1-1" and res.tidal["R:0"] == f"split:{TIDE}"


# ------------------------------------------- T660: a stream entering tidal water ends at its own shore

SHORE = "split:shore:C1:300"


def _shore(m, sid=SHORE):
    return SectionBoundary(boundary_id=sid, kind=BoundaryKind("split"), route_measure=m, source="shore")


@pytest.fixture
def crossings(estuary):
    """The estuary plus three creeks drawn on across the water to the river's line (FWA's water crossing):
    C1 enters the TIDAL piece R:0 (its crossing 0-300 m cut at its shore), C2 enters the FRESH piece R:1000 the
    same way, C3 enters C1's crossing (a crossing into a crossing). Their edge types are the parent's, mixed, as
    every cut piece's are (`sectionizer._split_one`); only the shore cut says where the crossing ends."""
    g, reg = estuary
    mixed = ("1000", "1450")
    up1, up2 = _shore(300), _shore(250, "split:shore:C2:250")
    nodes = list(g.nodes.values()) + [
        _s("C1:0", "C1", 0, 300, upper=up1, wsc="900-100000-020000", order=1),
        _s("C1:300", "C1", 300, 2300, lower=up1, wsc="900-100000-020000", order=1),
        _s("C2:0", "C2", 0, 250, upper=up2, wsc="900-100000-500000", order=1),
        _s("C2:250", "C2", 250, 1250, lower=up2, wsc="900-100000-500000", order=1),
        _s("C3:0", "C3", 0, 120, wsc="900-100000-020000-001000", order=1),
    ]
    nodes = [n if not n.blk.startswith("C") else
             StreamNode(**{**n.__dict__, "edge_types": ("1450",) if n.blk == "C3" else mixed}) for n in nodes]
    edges = [(e.from_node, e.to_node, e.kind, e.at_measure) for e in g.edges] + [
        ("C1:300", "C1:0", "continuation", 300), ("C1:0", "R:0", "confluence", 400),
        ("C2:250", "C2:0", "continuation", 250), ("C2:0", "R:1000", "confluence", 2500),
        ("C3:0", "C1:0", "confluence", 100)]
    reg = dict(reg, **{"area:region:1": RegistryItem(
        id="area:region:1", name="Region 1", kind="area",
        section_ids=tuple(sorted(set(reg["area:region:1"].section_ids) | {n.node_id for n in nodes})))})
    return _graph(nodes, edges), reg


def test_a_crossing_into_tidal_water_is_tidal_and_the_creek_above_its_shore_is_not(crossings):
    g, reg = crossings
    got = O.tidal_water([], reg, g, [TIDE])
    assert got == {"R:0": f"split:{TIDE}", "C1:0": "crossing:C1:0", "C3:0": "crossing:C3:0"}
    assert O.is_cut_owner("crossing:C1:0") and O.is_crossing_owner("crossing:C1:0")
    assert not O.is_crossing_owner(f"split:{TIDE}")
    # C2's crossing ends in FRESH water (above the tidal boundary): it stays freshwater, as does every creek above
    assert O.crossing_pieces(g) == frozenset({"C1:0", "C2:0", "C3:0"})


def test_a_zone_rule_keeps_the_creek_and_loses_its_crossing(crossings):
    g, reg = crossings
    zone = {"entry_id": "z1:quota", "matched": [],
            "rules": [{"rule_id": "z", "extents": [{"op": "within", "area_id": "area:region:1"}]}]}
    res = build_reaches([zone], reg, g, tidal_splits=(TIDE,))
    assert set(res.bindings[0].sections) == {"R:1000", "R:3000", "T1:0", "T2:0", "C1:300", "C2:0", "C2:250"}
    diag = [d.payload for d in res.diagnostics if d.kind == "tidal"][0]
    assert diag["splits"] == [TIDE] and diag["crossings"] == 2          # counted, never listed as cuts
    assert {"split": O.CROSSING_REPORT, "items": [], "sections": 2, "unplaced": [],
            "ambiguous_cut": []} in res.report.tidal_cuts


def test_mutation_without_the_shore_cut_the_mixed_creek_stays_freshwater(crossings):
    """A cut piece carries its parent's edge types: they cannot say where the crossing ends. Take the shore
    boundary away and only the line that is a crossing through and through (C3) is one."""
    g, reg = crossings
    bare = {nid: StreamNode(**{**n.__dict__, "upper_bound": None, "lower_bound": None}) if n.blk in ("C1", "C2")
            else n for nid, n in g.nodes.items()}
    g2 = _graph(list(bare.values()), [(e.from_node, e.to_node, e.kind, e.at_measure) for e in g.edges])
    assert O.crossing_pieces(g2) == frozenset({"C3:0"})
    # C3 now enters C1:0, which is fresh: nothing but the tidal piece is tidal
    assert O.tidal_water([], reg, g2, [TIDE]) == {"R:0": f"split:{TIDE}"}


def test_a_crossing_into_a_row_marked_tidal_is_not_taken(crossings):
    """A row marked tidal (Nitinat Lake) stays drawn, so the tiles cannot take its crossings: neither does the run."""
    g, reg = crossings
    row = {"entry_id": "r1:river@1-1", "matched": ["gnis:1"], "tidal": True}
    got = O.tidal_water([row], reg, g, ())
    assert not any(O.is_crossing_owner(o) for o in got.values())


def test_the_tiles_leave_off_the_crossings_too(crossings, tmp_path):
    from pipeline.deliver.tiles.tidal import tidal_pieces
    g, reg = crossings
    _sidecar(tmp_path, [{"blk": "R", "limit": "split", "split_id": TIDE}])
    run = O.tidal_water([], reg, g, [TIDE])
    assert tidal_pieces(tmp_path, graph=g, registry=reg, whole=()) == frozenset(run) == \
        frozenset({"R:0", "C1:0", "C3:0"})
