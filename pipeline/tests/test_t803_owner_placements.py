"""T803 / T805 / T806 — the owner's placement answers of 2026-11-16, pinned on the curated files the next map rebuild
reads (each read through the loader the build uses, not as raw JSON where a loader exists).

  Gitnadoix   "the powerline is. the mouth essentially": the powerline cut lands ON the river's own mouth (an alias on
              the mouth boundary, `at_edge`), never a new cut; the coho place is upstream of it (= the whole river)
  Sheldens    "Extend to the bridge": the creek runs on down a hand-drawn estuary channel to the road bridge, the
              low-confidence tidal cut (`test_sheldens_estuary_channel.py`); the place stays the creek
  Mara Bridge "at the bridge but make sure the cut name has the fact signs are nearby"
  Shuswap     Middle Shuswap (Shuswap Falls to Mabel Lake): "Main river only"
  Cowichan    the water's "including tributaries" flag: "Turn it off"; no live place changes with it
  François    T805 D, "Outlet area only": every line of the p.51 row is held on the outlet area, as r1 always was, so
              none of them governs the main lake (it reads Region 6's rules)
"""
from __future__ import annotations

import json

from pipeline.common.curated import CURATED


def _places(slug: str):
    from pipeline.regs.dfo_salmon import entries as E
    return E.load(slug)


def _place(ef, location_id: str):
    got = [l for l in ef.locations if l.location_id == location_id]
    assert len(got) == 1, location_id
    return got[0]


def _dump(extents) -> list:
    return [e.model_dump(mode="json", exclude_defaults=True) for e in extents]


# --------------------------------------------------------------------------------------------- Gitnadoix
GITNADOIX_CUT = "gitnadoix_river__powerline_crossing_near_the_mouth"
#: the Gitnadoix's own FWA mouth (blk 360887399, downstream route measure 0), lon/lat
GITNADOIX_MOUTH = (-129.207645, 54.317405)


def test_gitnadoix_powerline_is_the_rivers_own_mouth_not_a_new_cut():
    from shapely.geometry import LineString
    from pipeline.atlas.splits import anchors as A
    from pipeline.atlas.splits import sectionizer as SZ
    from pipeline.atlas.splits.splits import load_split_defs
    from pipeline.common.models import BlkChain
    from pipeline.common import tolerances as tol

    sd = next(d for d in load_split_defs(CURATED.waters.cuts) if d.id == GITNADOIX_CUT)
    assert sd.gnis_id == "3508" and sd.at_edge and sd.anchor.type.value == "point"
    # a Gitnadoix line drawn from its mouth upstream: the cut projects onto the line's own lower end
    x, y = A._pt(GITNADOIX_MOUTH, True).coords[0]
    g = LineString([(x, y), (x, y - 2000.0)])
    chain = BlkChain(blk="360887399", fwa_watershed_code="400-1", fids=(), geometry=g, mouth_measure=0.0,
                     length_m=g.length, name_tuples=(), gnis_id="3508")
    pts = A.resolve_split_defs([sd], [chain])
    assert [(p.blk, p.split_id) for p in pts] == [("360887399", GITNADOIX_CUT)]
    # on the end (within one spot), so the sectionizer takes the natural end: an alias, no sliver, no new cut
    assert pts[0].route_measure <= tol.SAME_PLACE_M < SZ.AT_EDGE_M


def test_gitnadoix_coho_place_is_upstream_of_the_mouth_alias():
    loc = _place(_places("6"), "6:gitnadoix-river:upstream-of-powerline-crossing-nea")
    assert loc.status == "active"
    assert _dump(loc.binding.extents) == [{"op": "upstream_of", "splits": [GITNADOIX_CUT], "item_id": "gnis:3508"}]
    assert loc.binding.tributaries is True          # unchanged: the whole river with its side streams


# --------------------------------------------------------------------------------------------- Sheldens
def test_sheldens_place_is_the_creek_above_the_bridge():
    """The tidal boundary is a cut at the bridge on the hand-drawn estuary channel (`test_sheldens_estuary_channel`);
    the place stays the creek, and the tidal subtraction leaves only the water above the bridge in it.

    The creek is TWO registry items: the FWA line is named only by a mined name (`found_names.json`), which is a label
    and never a grouping key (`registry.mined`), so it is `blk:360846083`, while the hand-drawn channel carries its own
    name and is `wsc:950-016367` (rebuild of 2026-11-17). The place binds both, so the channel above the bridge is in
    it; one item is a later structural fix."""
    loc = _place(_places("6"), "6:sheldens-creek:upstream-of-white-triangular-fishi")
    assert _dump(loc.binding.extents) == [{"op": "whole", "item_ids": ["blk:360846083", "wsc:950-016367"]}]
    assert _places("6").water("6:sheldens-creek").item_ids == ["blk:360846083", "wsc:950-016367"]


# --------------------------------------------------------------------------------------------- Mara Bridge
def test_mara_bridge_cut_name_says_the_signs_are_nearby():
    from pipeline.atlas.splits.splits import label_problem, load_split_defs
    sd = next(d for d in load_split_defs(CURATED.waters.cuts) if d.id == "shuswap_river__mara_bridge")
    assert sd.label == "Mara Bridge (fishing boundary signs nearby)" and label_problem(sd.label) == ""
    assert sd.anchor.coord == (-119.06995, 50.6848)                 # still at the bridge


# --------------------------------------------------------------------------------------------- side streams
def _reach_flag(slug: str, location_id: str) -> bool:
    from pipeline.regs.dfo_salmon import entries as E
    ef = _places(slug)
    loc = _place(ef, location_id)
    entry, _ = E.to_reach_input(loc, [], ef.water(loc.water_id))
    return entry["includes_tributaries"]


def test_middle_shuswap_is_the_main_river_only():
    ef = _places("8")
    for lid in ("8:shuswap-river:middle-between-shuswap-falls-and-m",
                "8:shuswap-river:middle-between-shuswap-falls-and-m~2"):
        assert _place(ef, lid).binding.tributaries is False, lid
    assert _reach_flag("8", "8:shuswap-river:middle-between-shuswap-falls-and-m~2") is False


def test_cowichan_water_flag_is_off_and_no_live_place_takes_side_streams():
    ef = _places("1")
    assert ef.water("1:cowichan-river").tributaries is False
    live = [l for l in ef.locations if l.water_id == "1:cowichan-river" and l.status == "active"]
    assert live and all(_reach_flag("1", l.location_id) is False for l in live)


# --------------------------------------------------------------------------------------------- François Lake
def test_francois_lake_every_line_is_held_on_the_outlet_area():
    from pipeline.deliver.bundle.read.facts import not_yet_mapped
    doc = json.loads((CURATED.regulations.synopsis / "region-6.json").read_text(encoding="utf-8"))
    e = next(x for x in doc["entries"] if x["entry_id"] == "r6:fran_ois_lake@6-4")
    part = "the area at the outlet of François Lake described on map 1"
    assert [r["verbatim"] for r in e["rules"]] == [
        "No Fishing Nov 15-May 31", "char daily quota = 2", "no more than\n1 over 50 cm",
        "rainbow trout catch and release", "bait ban", "single barbless hook", "no set lining"]
    assert all(not_yet_mapped(r) and r["undrawn_part"] == part and r["extents"] == [{"op": "whole"}]
               for r in e["rules"])
