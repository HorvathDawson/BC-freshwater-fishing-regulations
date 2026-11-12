"""The next atlas rebuild (T664 + T665 + the nine DFO cuts, `handoff/PLAN/app-integration/next-rebuild.md`), points a-e.

a. the shore cuts are decided on the reach's own tidal set (`shore.reach_tidal`), and the build stops if the registry
   it writes disagrees;
b. a held line keeps the cut build 27 made (`shore.HELD_KEEPS_ITS_CUT`);
e. promote lists and refuses a curated item id the candidate lost or absorbed, and lists lost boundaries by family;
and the owner's ruling of 2026-11-11 on the eight still-water oxbow polygons (`one_water_takes`).

Synthetic graphs and registries only: no data.
"""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from pipeline.atlas import promote as PR
from pipeline.atlas.registry import build_registry
from pipeline.atlas.registry import one_water as OW
from pipeline.atlas.splits import shore as S
from pipeline.atlas.splits.sectionizer import place_points, split_graph_at
from pipeline.atlas.splits.tidal import Limit
from pipeline.common.models import AnchorType, RegistryItem, SplitPoint
from pipeline.tests.test_shore_cuts import SEA, _built, _fid

NAME = "Okanagan River Oxbows"


# ---------------------------------------------------------------------------------------------- b. held lines
def test_a_held_line_keeps_the_cut_it_has_and_no_other():
    """C is held. Without a kept cut it is not cut at all; with one it is cut there, not at its polygon exit (300).
    Mutation: drop the `kept_here` branch -> no cut."""
    g, geoms, _, _ = _built()
    inside = S.polygon_inside(S.graph_lines(g, geoms), SEA)
    pts, _, rows = S.polygon_rule(g, inside, {"X:0"}, held={"C": "C Creek"}, kept={})
    assert pts == [] and {r["blk"]: r for r in rows}["C"]["not_cut"] == S.HELD
    pts, _, rows = S.polygon_rule(g, inside, {"X:0"}, held={"C": "C Creek"}, kept={"C": 150.0})
    row = {r["blk"]: r for r in rows}["C"]
    assert [(p.split_id, p.route_measure) for p in pts] == [("shore:C:150", 150.0)]
    assert row["held"] == S.HELD_KEPT and row["cut_m"] == 150.0 and "not_cut" not in row


def test_a_held_line_keeps_its_cut_even_where_the_piece_below_stays_fresh():
    """Build 27 has 7 held lines whose crossing is not tidal: the cut stays, the reach says fresh, as today."""
    g, geoms, _, _ = _built()
    inside = S.polygon_inside(S.graph_lines(g, geoms), SEA)
    pts, _, rows = S.polygon_rule(g, inside, set(), held={"C": "C Creek"}, kept={"C": 150.0})
    assert [p.split_id for p in pts] == ["shore:C:150"]
    assert {r["blk"]: r for r in rows}["C"]["below_cut_tidal"] is False


def test_a_kept_cut_needs_a_held_line_and_a_place_on_it():
    g, geoms, _, _ = _built()
    inside = S.polygon_inside(S.graph_lines(g, geoms), SEA)
    with pytest.raises(SystemExit, match="not held"):
        S.polygon_rule(g, inside, {"X:0"}, held={}, kept={"C": 150.0})
    with pytest.raises(SystemExit, match="cannot keep its cut"):
        S.polygon_rule(g, inside, {"X:0"}, held={"C": "C Creek"}, kept={"C": 5000.0})


def test_the_kept_cuts_are_held_lines_of_the_second_group():
    """28 of the 64 held lines carry a build 27 cut; none is one of the eight main rivers (they have no cut today),
    nor the creek held for its braid (not cut in build 27)."""
    held = S.held_lines()
    assert len(held) == 64 and len(S.HELD_KEEPS_ITS_CUT) == 28
    assert (len(S.HELD_FOR_OWNER), len(S.HELD_UP_THE_CHANNEL), len(S.HELD_UP_THE_CHANNEL_SINCE),
            len(S.HELD_A_BRAID_SPANS_THE_EXIT)) == (8, 53, 2, 1)
    assert set(S.HELD_KEEPS_ITS_CUT) <= set(S.HELD_UP_THE_CHANNEL) | set(S.HELD_UP_THE_CHANNEL_SINCE)
    assert all(m > 5.0 for m in S.HELD_KEEPS_ITS_CUT.values())


def test_a_ticked_line_leaves_the_kept_cuts_with_the_held_list(monkeypatch):
    """Removing a line from the held lists is the tick: its kept cut goes with it (the default reads only held rows)."""
    g, geoms, _, _ = _built()
    inside = S.polygon_inside(S.graph_lines(g, geoms), SEA)
    monkeypatch.setattr(S, "HELD_FOR_OWNER", {})
    monkeypatch.setattr(S, "HELD_UP_THE_CHANNEL", {})
    monkeypatch.setattr(S, "HELD_A_BRAID_SPANS_THE_EXIT", {})
    monkeypatch.setattr(S, "HELD_UP_THE_CHANNEL_SINCE", {})
    monkeypatch.setattr(S, "HELD_KEEPS_ITS_CUT", {"C": 150.0})
    pts, _, _ = S.polygon_rule(g, inside, {"X:0"})
    assert [p.split_id for p in pts] == ["shore:C:300"]


# ---------------------------------------------------------------------------------------------- a. the reach's tidal set
#: a side channel of X River with no name of its own (X's watershed code), leaving X at x = 1500 and rejoining at x = 500,
#: over land north of the sea (y > 300) on the way: the registry folds it into X River; the graph alone (`atlas_tidal`:
#: the pieces that carry X's gnis) does not see it
SIDE = _fid("S1", "S", "50-100", [(500, 0), (1000, 400), (1500, 0)], 0, 1280)


def _cut_x(extra):
    g, geoms, idx, fids = _built(extra)
    sp = SplitPoint(split_id="x_tidal", blk="X", route_measure=1000.0, fid="", label="tidal boundary",
                    anchor_type=AnchorType.point, source="curated")
    split_graph_at(g, geoms, place_points(g, [sp]), idx)
    return g, geoms, idx, fids


def test_reach_tidal_is_the_reach_runs_own_answer():
    """The piece below the limit, by the reach's `tidal_cut_owner` on the registry made from the graph."""
    from pipeline.atlas.reach import outside as O
    g, _, _, _ = _cut_x(())
    limits = [Limit(blk="X", limit="split", split="x_tidal")]
    reg = build_registry(g)
    got = S.reach_tidal(reg, g, limits, {})
    assert got == set(O.tidal_cut_owner(reg, g, ["x_tidal"])) and "X:0" in got and "X:1000" not in got


def test_a_limit_no_item_carries_is_refused():
    g, _, _, _ = _built()
    with pytest.raises(SystemExit, match="no registry item"):
        S.reach_tidal(build_registry(g), g, [Limit(blk="X", limit="split", split="x_tidal")], {})


def _stage(tmp_path, monkeypatch, extra, limits):
    from pipeline.atlas import build as B
    from pipeline.atlas.graph.blk_chains import build_blk_chains
    g, geoms, idx, fids = _cut_x(extra)
    monkeypatch.setattr(S, "read_tidal_polygon", lambda gpkg, bbox=None: SEA)
    b = SimpleNamespace(args=SimpleNamespace(gpkg="unused.gpkg", out=str(tmp_path)), graph=g, geoms=geoms,
                        fid_index=idx, applied_splits=[], fids=fids, chains=build_blk_chains(fids, {}), bbox=None,
                        tidal_limits=limits, tidal_ids={}, tick=lambda label: None, splits=[], lake_names={},
                        wetland_names={}, _nv_all=[], wb_polys={}, _mined=[], added_lake_parts={}, area_defs=[])
    B.cut_shores(b)
    return b


def test_the_build_cuts_by_the_registrys_tidal_water(tmp_path, monkeypatch):
    """The atlas stage with a limit: the tidal set it cuts by is the reach's on the registry made before the cuts
    (`build.water_registry`), it is kept for the gate, and the record counts it. Mutation: the switch off -> the
    graph's reading (`atlas_tidal`) is used instead."""
    limits = [Limit(blk="X", limit="split", split="x_tidal")]
    b = _stage(tmp_path, monkeypatch, (), limits)
    reg = build_registry(b.graph)
    assert b.shore_tidal == S.reach_tidal(reg, b.graph, limits, {})
    assert "X:0" in b.shore_tidal and all(not s.startswith("C:") for s in b.shore_tidal)
    rec = json.loads((tmp_path / S.RECORD).read_text())
    assert rec["tidal_sections"] == len(b.shore_tidal) and rec["rule"] == S.RULE
    assert "shore:C:300" in [p.split_id for p in b.applied_splits]
    monkeypatch.setattr(S, "THE_ATLAS_CUTS_BY_THE_REACHS_TIDAL_SET", False)
    b2 = _stage(tmp_path, monkeypatch, (), limits)
    assert b2.shore_tidal == S.atlas_tidal(b2.graph, limits, {})


def test_a_side_channel_of_a_whole_tidal_river_is_tidal_water_not_a_creek_to_cut(tmp_path, monkeypatch):
    """The dry run's gap in one case (its "3 cuts only with the atlas set", Williamson Slough): X River is tidal whole;
    its unnamed side channel S is X River in the registry, so the reach takes S as tidal and the build leaves it
    alone. Read off the graph alone S is a creek into tidal water, cut where it leaves the polygon (and the braid
    sweep stops the build on that cut)."""
    limits = [Limit(blk="X", limit="whole", item="gnis:8")]
    b = _stage(tmp_path, monkeypatch, (SIDE,), limits)
    assert "S:0" in b.shore_tidal and "S:0" not in S.atlas_tidal(b.graph, limits, {})
    assert [p.split_id for p in b.applied_splits if p.blk == "S"] == []
    assert {n for n in b.graph.nodes if n.startswith("S:")} == {"S:0"}
    monkeypatch.setattr(S, "THE_ATLAS_CUTS_BY_THE_REACHS_TIDAL_SET", False)
    # read off the graph, S is cut at its polygon exit, across X River's own braid: the build stops on it
    with pytest.raises(SystemExit, match="meet a braid"):
        _stage(tmp_path, monkeypatch, (SIDE,), limits)
    rows = json.loads((tmp_path / S.RECORD).read_text())["runs"]
    assert [r["blk"] for r in rows if "cut_m" in r and r["blk"] == "S"] == ["S"]


# ---------------------------------------------------------------------------------------------- the oxbow polygons
def _it(iid, name, kind, secs, mus=("8-1",)):
    return RegistryItem(id=iid, name=name, kind=kind, variants=(name,), mus=tuple(mus), section_ids=tuple(secs),
                        ref_ids=(iid,))


def _reg():
    return {
        "wsc:300-1-100": _it("wsc:300-1-100", NAME, "stream", ["b1:0", "b1:50"]),
        "wsc:300-1-200": _it("wsc:300-1-200", NAME, "stream", ["b2:0"]),
        "wbk:8": _it("wbk:8", "OKANAGAN RIVER OXBOWS", "wetland", ["lake:8"]),
        "wbk:9": _it("wbk:9", "OKANAGAN RIVER OXBOWS", "lake", ["lake:9"]),
        "wbk:5": _it("wbk:5", "Vaseux Lake", "lake", ["lake:5"]),
    }


def _nv(takes):
    return [{"target": {"wscs": ["300-1-100"]}, "names": [
        {"name": NAME, "one_water": True, **({"one_water_takes": takes} if takes is not None else {})}]}]


def test_a_listed_still_water_item_joins_the_one_water_as_a_stream_polygon():
    """Owner 2026-11-11: "They aren't lakes, they are considered streams." Only the listed one moves. Mutation: ignore
    `takes` -> wbk:8 stays a wetland item."""
    rep: dict = {}
    nv = _nv(["wbk:8"])
    out = OW.merge_one_waters(_reg(), OW.one_water_names(nv), rep, OW.one_water_takes(nv))
    assert set(out) == {"wsc:300-1-100", "wbk:9", "wbk:5"}
    one = out["wsc:300-1-100"]
    assert one.kind == "stream" and set(one.section_ids) == {"b1:0", "b1:50", "b2:0", "lake:8"}
    assert set(one.aliases) == {"wsc:300-1-200", "wbk:8"} and "wbk:8" in one.ref_ids
    assert rep[NAME]["took_still_water"] == ["wbk:8"] and rep[NAME]["kept_apart"] == ["wbk:9"]
    assert out["wbk:9"].kind == "lake"                       # not listed: untouched


def test_all_listed_gives_one_item_and_running_it_again_changes_nothing():
    nv = _nv(["wbk:8", "wbk:9"])
    out = OW.merge_one_waters(_reg(), OW.one_water_names(nv), {}, OW.one_water_takes(nv))
    assert [i for i, it in out.items() if NAME.lower() in it.name.lower()] == ["wsc:300-1-100"]
    again = OW.merge_one_waters(out, OW.one_water_names(nv), {}, OW.one_water_takes(nv))     # the sidecar pass
    assert again == out


def test_a_listed_id_that_is_not_an_oxbow_item_is_refused():
    nv = _nv(["wbk:5"])                                       # Vaseux Lake does not hold the name
    with pytest.raises(ValueError, match="one_water_takes"):
        OW.merge_one_waters(_reg(), OW.one_water_names(nv), {}, OW.one_water_takes(nv))
    nv = _nv(["wbk:404"])                                     # not in the registry
    with pytest.raises(ValueError, match="one_water_takes"):
        OW.merge_one_waters(_reg(), OW.one_water_names(nv), {}, OW.one_water_takes(nv))


def test_the_list_is_wbk_ids_on_a_one_water_record():
    with pytest.raises(ValueError):
        OW.one_water_takes([{"names": [{"name": NAME, "one_water_takes": ["wbk:8"]}]}])       # no one_water flag
    with pytest.raises(ValueError):
        OW.one_water_takes(_nv(["gnis:1"]))
    assert OW.one_water_takes(_nv(None)) == {}


# ---------------------------------------------------------------------------------------------- e. promote
def _atlas(d, items):
    d.mkdir(parents=True)
    (d / "registry.json").write_text(json.dumps({"items": items}))
    return d


def test_promote_names_every_curated_id_the_candidate_lost_or_absorbed(tmp_path):
    """Mutation: scan only `matched` -> the notice water's id is missed."""
    old = _atlas(tmp_path / "full", [{"id": "wsc:1"}, {"id": "wsc:2"}, {"id": "wbk:3"}, {"id": "gnis:4"}, {"id": "gnis:5"}])
    new = _atlas(tmp_path / "next", [{"id": "wsc:1", "aliases": ["wsc:2"]}, {"id": "gnis:4"}])
    root = tmp_path / "curated"
    (root / "regulations").mkdir(parents=True)
    (root / "inseason").mkdir()
    (root / "review").mkdir()
    (root / "regulations" / "r8.json").write_text(json.dumps([{"entry_id": "r8:x", "matched": ["wsc:1", "wsc:2", "gnis:4"]}]))
    (root / "inseason" / "notice.json").write_text(json.dumps({"m": {"extents": [{"item_ids": ["wbk:3", "wbk:3"]}]}}))
    (root / "review" / "log.json").write_text(json.dumps(["gnis:5"]))          # the review log is not read
    dead, absorbed = PR.orphaned_ids(old, new, root)
    assert dead == ["wbk:3: inseason/notice.json x2"]
    assert absorbed == ["wsc:2 -> wsc:1: regulations/r8.json x1"]
    assert PR.orphaned_ids(old, old, root) == ([], [])


def test_lost_boundaries_are_listed_by_family(tmp_path):
    def it(iid, bs):
        return {"id": iid, "boundaries": [{"id": i, "ref": r} for i, r in bs]}
    old = _atlas(tmp_path / "full", [it("gnis:1", [("shore:1:50", "split:shore:1:50"), ("a__bridge", "split:a__bridge"),
                                                   ("a__lake_x", "lake:7"), ("length:1:900", "split:length:1:900")])])
    new = _atlas(tmp_path / "next", [it("gnis:1", [("shore:1:80", "split:shore:1:80")])])
    assert PR.lost_boundaries(old, new) == {"shore": ["shore:1:50"], "curated": ["a__bridge"], "lake": ["a__lake_x"],
                                            "length": ["length:1:900"]}
