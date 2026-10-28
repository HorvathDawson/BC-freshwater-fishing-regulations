"""T166 step 5 (owner 2026-10-28, RULINGS C-2): THE PIECE BELOW A TIDAL BOUNDARY IS TIDAL WATER.

  flag      a curated split carries `tidal: true` (splits.json) -> the atlas writes it on its
            `splits.resolved.json` rows (and the sidecar migration for a reused cut) -> the reach
            run reads the ids from the atlas (`outside.tidal_splits`)
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

def test_the_flag_rides_from_splits_json_to_the_resolved_rows(tmp_path):
    d = SplitDef.from_dict({"id": TIDE, "gnis_id": "1", "tidal": True, "label": "tidal boundary",
                            "anchor": {"type": "point", "coord": [-123.1, 49.3], "is_lonlat": True}})
    assert d.tidal is True
    assert SplitDef.from_dict({"id": "x", "gnis_id": "1", "anchor": {
        "type": "point", "coord": [-123.1, 49.3], "is_lonlat": True}}).tidal is False
    with pytest.raises(ValueError, match="tidal must be true"):
        SplitDef.from_dict({"id": "x", "gnis_id": "1", "tidal": "yes", "anchor": {
            "type": "point", "coord": [-123.1, 49.3], "is_lonlat": True}})
    p = tmp_path / "splits.resolved.json"
    write_resolved([SplitPoint(TIDE, "R", 1000.0, "", "tidal boundary", AnchorType.point,
                               source="curated", tidal=True),
                    SplitPoint("river__falls", "R", 3000.0, "", "falls", AnchorType.point,
                               source="curated")], str(p))
    rows = json.loads(p.read_text())
    assert rows[0]["tidal"] is True and "tidal" not in rows[1] and "tidal" in RESOLVED_KEYS
    assert O.tidal_splits(tmp_path) == (TIDE,)
    with pytest.raises(SystemExit, match="missing"):
        O.tidal_splits(tmp_path / "nowhere")
    assert order_resolved(rows[0])["tidal"] is True


def test_a_reused_cut_flagged_after_the_build_reaches_the_sidecar():
    from pipeline.atlas.sidecars import enrich_resolved
    d = SplitDef.from_dict({"id": "fraser__cpr", "gnis_id": "1", "tidal": True, "label": "CPR Bridge",
                            "anchor": {"type": "point", "coord": [-122.3, 49.13], "is_lonlat": True}})
    got = enrich_resolved([{"split_id": "fraser__cpr", "blk": "F", "route_measure": 1.0,
                            "label": "CPR Bridge", "anchor_type": "point"}], [d])
    assert got[0]["tidal"] is True
    plain = SplitDef.from_dict({**{"id": "fraser__cpr", "gnis_id": "1", "label": "CPR Bridge"},
                                "anchor": {"type": "point", "coord": [-122.3, 49.13], "is_lonlat": True}})
    assert "tidal" not in enrich_resolved([dict(got[0])], [plain])[0]


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
    write_resolved([SplitPoint(TIDE, "R", 1000.0, "", "tidal boundary", AnchorType.point,
                               source="curated", tidal=True)], str(tmp_path / "splits.resolved.json"))
    assert tidal_pieces(tmp_path, graph=g, registry=reg) == frozenset(O.tidal_cut_owner(reg, g, [TIDE]))
    write_resolved([], str(tmp_path / "splits.resolved.json"))
    assert tidal_pieces(tmp_path, graph=g, registry=reg) == frozenset()


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
