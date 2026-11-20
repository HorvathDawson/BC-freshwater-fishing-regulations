"""The next atlas rebuild (T664 + T665 + the nine DFO cuts, `handoff/PLAN/app-integration/next-rebuild.md`), points a-e.

a. the coast cut is decided on the tidal water rank 1 makes, resolved by the atlas (`tidal.rank1_owners`, T678 plan 2),
   and the build stops if the registry it writes disagrees;
(b., the polygon rule's held lines, retired with it by T678 on 2026-11-20: `waters/shore_held.json` is the owner's.)
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
from pipeline.atlas.splits import tidal as T
from pipeline.atlas.splits.sectionizer import place_points, split_graph_at
from pipeline.atlas.splits.tidal import Limit
from pipeline.common.models import AnchorType, RegistryItem, SplitPoint
from pipeline.tests.test_shore_cuts import SEA, _built, _fid

NAME = "Okanagan River Oxbows"


@pytest.fixture(autouse=True)
def _no_pfma_layer_needed(monkeypatch):
    """A synthetic graph has no PFMA drainage: the province's basins are not read (the build refuses to skip them),
    as in `test_registry.py` — without it `build_registry` opens the source gpkg and the file fails on a no-data clone."""
    from pipeline.atlas.splits import pfma_drainage
    monkeypatch.setattr(pfma_drainage, "basin_areas", lambda *a, **kw: {})


# ---------------------------------------------------------------------------------------------- a. rank 1's tidal water
#: a side channel of X River with no name of its own (X's watershed code), leaving X at x = 1500 and rejoining at x = 500,
#: over land north of the sea (y > 300) on the way: the registry folds it into X River; the graph alone (the pieces that
#: carry X's gnis) does not see it
SIDE = _fid("S1", "S", "50-100", [(500, 0), (1000, 400), (1500, 0)], 0, 1280)


def _cut_x(extra):
    g, geoms, idx, fids = _built(extra)
    sp = SplitPoint(split_id="x_tidal", blk="X", route_measure=1000.0, fid="", label="tidal boundary",
                    anchor_type=AnchorType.point, source="curated")
    split_graph_at(g, geoms, place_points(g, [sp]), idx)
    return g, geoms, idx, fids


def test_rank1_is_the_piece_below_the_limit_resolved_by_the_atlas():
    """The piece below the limit, by the atlas's own `tidal_cut_owner` on the registry made from the graph (T678
    plan 2: the reach resolves nothing)."""
    g, _, _, _ = _cut_x(())
    limits = [Limit(blk="X", limit="split", split="x_tidal")]
    reg = build_registry(g)
    got = T.rank1_owners(reg, g, limits, {})
    assert got == T.tidal_cut_owner(reg, g, ["x_tidal"]) and "X:0" in got and "X:1000" not in got


def test_a_limit_no_item_carries_is_refused():
    g, _, _, _ = _built()
    with pytest.raises(SystemExit, match="no registry item"):
        T.rank1_owners(build_registry(g), g, [Limit(blk="X", limit="split", split="x_tidal")], {})


def _stage(tmp_path, monkeypatch, extra, limits, held=None):
    from pipeline.atlas import build as B
    from pipeline.atlas.graph.blk_chains import build_blk_chains
    g, geoms, idx, fids = _cut_x(extra)
    from pipeline.tests.test_coast_cut import patch_stage
    patch_stage(monkeypatch, SEA, held)           # T678: the stage is the coast cut; its coast is the polygon here
    b = SimpleNamespace(args=SimpleNamespace(gpkg="unused.gpkg", coast_gpkg="unused-coast.gpkg", out=str(tmp_path)), graph=g, geoms=geoms,
                        fid_index=idx, applied_splits=[], fids=fids, chains=build_blk_chains(fids, {}), bbox=None,
                        tidal_limits=limits, tidal_ids={}, tick=lambda label: None, splits=[], lake_names={},
                        wetland_names={}, _nv_all=[], wb_polys={}, _mined=[], added_lake_parts={}, area_defs=[])
    B.cut_shores(b)
    return b


def test_the_build_cuts_by_the_registrys_tidal_water(tmp_path, monkeypatch):
    """The atlas stage with a limit: the tidal set it cuts by is the reach's on the registry made before the cuts
    (`build.water_registry`), it is kept for the gate (as water: `coast.water_cover`), and the record counts it.
    Mutation: no tidal water -> C's receiver is fresh and C is not cut. (T678: the coast cut; D's two crossings
    disagree, so D is the owner's and held.)"""
    from pipeline.atlas.splits import coast as C
    limits = [Limit(blk="X", limit="split", split="x_tidal")]
    held = {"D": {"keep": "fresh"}}
    b = _stage(tmp_path, monkeypatch, (), limits, held)
    reg = build_registry(b.graph)
    assert b.shore_tidal == frozenset(T.rank1_owners(reg, b.graph, limits, {}))
    assert "X:0" in b.shore_tidal and all(not s.startswith("C:") for s in b.shore_tidal)
    assert b.shore_tidal_cover == C.water_cover(b.graph, b.shore_tidal)
    rec = json.loads((tmp_path / S.RECORD).read_text())
    assert rec["tidal_sections"] == len(b.shore_tidal) and rec["rule"] == C.RULE
    assert "shore:C:300" in [p.split_id for p in b.applied_splits]
    monkeypatch.setattr(T, "rank1_owners", lambda *a, **k: {})
    b2 = _stage(tmp_path, monkeypatch, (), limits, held)
    assert "shore:C:300" not in [p.split_id for p in b2.applied_splits]


def test_the_registry_before_the_cuts_asks_for_no_area_item(tmp_path, monkeypatch):
    """Before the shore cuts no section carries an area flag, so no closure area is an item; `outside_area_items`
    refuses a missing area and would stop every full build there. Mutation: `areas=True` in `cut_shores` -> SystemExit."""
    from pipeline.atlas import build as B
    limits = [Limit(blk="X", limit="split", split="x_tidal")]
    area_defs = [{"id": "parks_nat", "outside_items": {"some_park": ["gnis:8"]}}]
    orig = _stage.__globals__["SimpleNamespace"]

    def with_areas(**kw):
        return orig(**{**kw, "area_defs": area_defs})
    monkeypatch.setitem(_stage.__globals__, "SimpleNamespace", with_areas)
    b = _stage(tmp_path, monkeypatch, (), limits)
    assert "X:0" in b.shore_tidal
    with pytest.raises(SystemExit, match="is not an area of this build"):
        B.water_registry(b, b.graph)                              # the full pass, as `build_registry` runs it


def test_a_side_channel_of_a_whole_tidal_river_is_tidal_water_not_a_creek_to_cut(tmp_path, monkeypatch):
    """The dry run's gap in one case (its "3 cuts only with the atlas set", Williamson Slough): X River is tidal whole;
    its unnamed side channel S is X River in the registry, so the reach takes S as tidal and the build leaves it
    alone: S is governed by X's rank-1 row (`shore.owner_limited`), never cut, and not in the coast record."""
    limits = [Limit(blk="X", limit="whole", item="gnis:8")]
    b = _stage(tmp_path, monkeypatch, (SIDE,), limits, {"D": {"keep": "fresh"}})
    assert "S:0" in b.shore_tidal
    assert [p.split_id for p in b.applied_splits if p.blk == "S"] == []
    assert {n for n in b.graph.nodes if n.startswith("S:")} == {"S:0"}
    rows = json.loads((tmp_path / S.RECORD).read_text())["runs"]
    assert "S" not in [r["blk"] for r in rows]


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
    (root / "inseason" / "notice.json").write_text(json.dumps({
        "m": {"extents": [{"item_ids": ["wbk:3", "wbk:3"]}]},
        "history": [{"extents": [{"item_id": "wsc:2"}]}],            # what a match named once: a log, not a reference
        "names": [{"one_water_takes": ["wsc:2"]}]}))                 # the instruction that absorbs it
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
