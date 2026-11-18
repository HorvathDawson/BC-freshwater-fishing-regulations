"""T803 / T805 / T806 — the owner's placement answers of 2026-11-16, pinned on the curated files the next map rebuild
reads (each read through the loader the build uses, not as raw JSON where a loader exists).

  Gitnadoix   "the powerline is. the mouth essentially": the powerline cut lands ON the river's own mouth (an alias on
              the mouth boundary, `at_edge`), never a new cut; the coho place is upstream of it (= the whole river,
              which it binds since T833 kept a cut on a line's end from dropping the end piece's side channels)
  Sheldens    "Extend to the bridge": the creek runs on down a hand-drawn estuary channel to the road bridge, the
              low-confidence tidal cut (`test_sheldens_estuary_channel.py`); the place stays the creek, ONE item
  Mara Bridge "at the bridge but make sure the cut name has the fact signs are nearby"
  Shuswap     Middle Shuswap (Shuswap Falls to Mabel Lake): "Main river only"
  Cowichan    the water's "including tributaries" flag: "Turn it off"; no live place changes with it
  François    T805 D, "Outlet area only": every line of the p.51 row is held on the outlet area, as r1 always was, so
              none of them governs the main lake (it reads Region 6's rules)
"""
from __future__ import annotations

import json

import pytest

from pipeline.tests.under_test import curated


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
GITNADOIX_PLACE = "6:gitnadoix-river:upstream-of-powerline-crossing-nea"


def test_gitnadoix_powerline_is_the_rivers_own_mouth_not_a_new_cut():
    from shapely.geometry import LineString
    from pipeline.atlas.splits import anchors as A
    from pipeline.atlas.splits import sectionizer as SZ
    from pipeline.atlas.splits.splits import load_split_defs
    from pipeline.common.models import BlkChain
    from pipeline.common import tolerances as tol

    sd = next(d for d in load_split_defs(curated().waters.cuts) if d.id == GITNADOIX_CUT)
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
    loc = _place(_places("6"), GITNADOIX_PLACE)
    assert loc.status == "active"
    assert _dump(loc.binding.extents) == [{"op": "upstream_of", "splits": [GITNADOIX_CUT], "item_id": "gnis:3508"}]
    # unchanged: the whole river with its side streams — T807: by its heading "Gitnadoix River (including tributaries)",
    # so the file stores no setting of its own
    from pipeline.regs.dfo_salmon import side_streams as SS
    assert loc.binding.tributaries is None and SS.effective(loc).tributaries is True


def _salmon_sections(request, *location_ids: str) -> dict[str, set[str]]:
    import gzip
    from pipeline.tests import under_test
    from pipeline.tests.conftest import REACH_HINT, need
    p = need(request, "reach_run", under_test.reach_run(request) / "dfo_salmon.jsonl.gz", REACH_HINT)
    out: dict[str, set[str]] = {}
    with gzip.open(p, "rt", encoding="utf-8") as f:
        for line in f:
            if any(i in line for i in location_ids):
                r = json.loads(line)
                if r["location_id"] in location_ids:
                    out[r["location_id"]] = set(r.get("sections") or ())
    return out


@pytest.mark.needs_reach_run
def test_gitnadoix_coho_place_binds_what_the_whole_river_place_binds(request):
    """On the built run: the coho place, `upstream_of` the mouth alias, binds what the Gitnadoix's `whole` place binds
    (1,000 sections on the 2026-11-17 rebuild). Before T833 it bound 991: a cut on a line's end seeded every line
    touching the 0-23.6 km end piece as across the junction, so 360215450:0, 360215474:0, the six dead-end side
    channels and 360742973:0 went outside (`reach.extent.A_CUT_ON_A_LINE_END_IS_ACROSS_ONLY_AT_ITS_POINT`)."""
    got = _salmon_sections(request, GITNADOIX_PLACE, "6:gitnadoix-river:whole")
    assert set(got) == {GITNADOIX_PLACE, "6:gitnadoix-river:whole"}
    assert got[GITNADOIX_PLACE] and got[GITNADOIX_PLACE] == got["6:gitnadoix-river:whole"]
    assert {"360215450:0", "360230202:0", "360742973:0"} <= got[GITNADOIX_PLACE]


# --------------------------------------------------------------------------------------------- Sheldens
SHELDENS = "wsc:950-016367"


def test_sheldens_place_is_the_creek_above_the_bridge():
    """The tidal boundary is a cut at the bridge on the hand-drawn estuary channel (`test_sheldens_estuary_channel`);
    the place stays the creek, and the tidal subtraction leaves only the water above the bridge in it.

    The creek is ONE registry item, `wsc:950-016367`: the FWA line (blue line 360846083) carries its name as a
    `name_variants.json` record (a graph name, which groups), so it joins the hand-drawn channel of the same code, as
    Squatters Creek's and Kask's Camp Creek's FWA lines join their added channels."""
    loc = _place(_places("6"), "6:sheldens-creek:upstream-of-white-triangular-fishi")
    assert _dump(loc.binding.extents) == [{"op": "whole", "item_id": SHELDENS}]
    assert _places("6").water("6:sheldens-creek").item_ids == [SHELDENS]


def test_sheldens_name_is_a_name_variant_not_a_found_name():
    nv = json.loads(curated().waters.name_variants.read_text(encoding="utf-8"))
    got = [n for e in nv if (e.get("target") or {}).get("blks") == ["360846083"] for n in e["names"]]
    assert [(n["name"], n["source"], n.get("display")) for n in got] == [("Sheldens Creek", "dfo", True)]
    fn = json.loads(curated().waters.found_names.read_text(encoding="utf-8"))["entries"]
    assert not [e for e in fn if "360846083" in ((e.get("target") or {}).get("blks") or ())]


def test_a_found_name_curation_gives_is_not_written_twice(monkeypatch):
    """`mined_names.CURATED_NAME_IS_THE_ONE_SOURCE`: the owner's accept call on the Sheldens candidate stands, and the
    writer leaves the found name out because name_variants.json gives the same name on the same line (and only
    then: another name, or a line curation names on only part of the target, is still written). False is the
    mutation pin."""
    from pipeline.regs.review import names as N
    from pipeline.tools import mined_names as MN
    assert N.owner_calls()["blk:360846083~sheldens-creek"]["call"] == "accept"
    named = MN._curated_line_names(json.loads(curated().waters.name_variants.read_text(encoding="utf-8")))
    recs = [{"name": "Sheldens Creek"}, {"name": "Shelden Bay Creek"}]
    assert MN.not_curated(["360846083"], recs, named) == [{"name": "Shelden Bay Creek"}]
    assert MN.not_curated(["360846083", "1"], recs, named) == recs
    monkeypatch.setattr(MN, "CURATED_NAME_IS_THE_ONE_SOURCE", False)
    assert MN.not_curated(["360846083"], recs, named) == recs


@pytest.mark.needs_atlas
def test_search_finds_one_sheldens_creek(request):
    """On the built registry: one item answers to "Sheldens Creek", and it holds the FWA line and the channel (and, as
    a named stream, its unnamed widening pond lake:329381266, 3.5 ha, by the widening fold: T679 #5)."""
    from pipeline.tests import under_test
    from pipeline.tests.conftest import ATLAS_HINT, need
    from pipeline.common.names import key
    p = need(request, "atlas", under_test.atlas(request) / "registry.json", ATLAS_HINT)
    items = json.loads(p.read_text(encoding="utf-8"))["items"]
    items = items.values() if isinstance(items, dict) else items
    hits = [it for it in items if it.get("kind") != "area"
            and key("Sheldens Creek") in {key(x) for x in (it.get("name"), *(it.get("variants") or ())) if x}]
    assert [it["id"] for it in hits] == [SHELDENS]
    assert {s.split(":")[0] for s in hits[0]["section_ids"] if not s.startswith("lake:")} == {"360846083", "-2400000001"}


# --------------------------------------------------------------------------------------------- Mara Bridge
def test_mara_bridge_cut_name_says_the_signs_are_nearby():
    from pipeline.atlas.splits.splits import label_problem, load_split_defs
    sd = next(d for d in load_split_defs(curated().waters.cuts) if d.id == "shuswap_river__mara_bridge")
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
        # T807: main river only because DFO's words for it say nothing of side streams (heading "Shuswap River")
        from pipeline.regs.dfo_salmon import side_streams as SS
        assert SS.effective(_place(ef, lid)).tributaries is False, lid
    assert _reach_flag("8", "8:shuswap-river:middle-between-shuswap-falls-and-m~2") is False


def test_cowichan_water_flag_is_off_and_no_live_place_takes_side_streams():
    ef = _places("1")
    assert ef.water("1:cowichan-river").tributaries is False
    live = [l for l in ef.locations if l.water_id == "1:cowichan-river" and l.status == "active"]
    assert live and all(_reach_flag("1", l.location_id) is False for l in live)


# --------------------------------------------------------------------------------------------- François Lake
def test_francois_lake_every_line_is_held_on_the_outlet_area():
    from pipeline.deliver.bundle.read.facts import not_yet_mapped
    doc = json.loads((curated().regulations.synopsis / "region-6.json").read_text(encoding="utf-8"))
    e = next(x for x in doc["entries"] if x["entry_id"] == "r6:fran_ois_lake@6-4")
    part = "the area at the outlet of François Lake described on map 1"
    assert [r["verbatim"] for r in e["rules"]] == [
        "No Fishing Nov 15-May 31", "char daily quota = 2", "no more than\n1 over 50 cm",
        "rainbow trout catch and release", "bait ban", "single barbless hook", "no set lining"]
    assert all(not_yet_mapped(r) and r["undrawn_part"] == part and r["extents"] == [{"op": "whole"}]
               for r in e["rules"])
