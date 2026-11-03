"""T355: the review app's new-split PREVIEW is the build's own resolver (`split_preview.resolve`):
`resolve_split_defs` on the build's chains for the one water, read from the FWA source — so the cut it
shows is the cut the build makes. Pinned against the served atlas where the build already made one
(the Moyie's U.S. border, Little Qualicum Falls Park). Needs the FWA source and the served atlas."""
from __future__ import annotations

import pytest

from pipeline.common.curated import SOURCE
from pipeline.regs.review import marks as M
from pipeline.tests.conftest import GPKG_HINT, need

BACKEND = __import__("pathlib").Path(__file__).resolve().parents[2] / "curation-review" / "backend"
LQ = "gnis:10886"                                          # Little Qualicum River
pytestmark = [pytest.mark.needs_source, pytest.mark.needs_atlas]


@pytest.fixture(autouse=True)
def _the_gpkg(request):
    """FAILS without the FWA gpkg, naming the command that fetches it (the atlas is the conftest's own check)."""
    need(request, "source", SOURCE / "bc_fisheries_data.gpkg", GPKG_HINT)


@pytest.fixture(scope="module")
def sp():
    mp = pytest.MonkeyPatch()
    mp.syspath_prepend(str(BACKEND))
    import split_preview
    yield split_preview
    mp.undo()


def _ns(**kw):
    return M.NewSplit(ref="n1", **kw)


def test_the_border_lands_on_the_atlas_s_own_border_cut(sp):
    got = sp.resolve(_ns(anchor="border", on_item="gnis:15783", coord=(-116.17899, 49.00046)))
    assert got["ok"], got["why"]
    (c,) = got["cuts"]
    assert c["blk"] == "356570342" and abs(c["measure"] - 42640.55) < 0.1   # splits.resolved.json


def test_a_park_boundary_cuts_where_the_water_enters_and_leaves(sp):
    both = sp.resolve(_ns(anchor="area_boundary", area="LITTLE QUALICUM FALLS PARK", on_item=LQ))
    assert both["ok"] and len(both["cuts"]) == 2 and both["outline"]
    down = sp.resolve(_ns(anchor="area_boundary", area="LITTLE QUALICUM FALLS PARK", area_end="downstream",
                          on_item=LQ))
    assert [c["measure"] for c in down["cuts"]] == [min(c["measure"] for c in both["cuts"])]
    nope = sp.resolve(_ns(anchor="area_boundary", area="NO SUCH PARK", on_item=LQ))
    assert not nope["ok"] and "no polygon named" in nope["why"]


def test_an_offset_cut_shows_its_reference(sp):
    got = sp.resolve(_ns(anchor="coord", on_item=LQ, coord=(-124.555678, 49.317797), offset_m=-100))
    assert got["ok"]
    (c,), (r,) = got["cuts"], got["reference"]
    assert c["blk"] == r["blk"] and abs((r["measure"] - c["measure"]) - 100.0) < 0.5


def test_a_confluence_is_picked_by_its_mouth_and_offset_along_the_water(sp):
    tribs = {t["name"]: t for t in sp.tributaries(LQ)}
    kinkade = tribs["Kinkade Creek"]
    hit = sp.pick("tributary", LQ, tuple(kinkade["mouth"]))
    assert hit["ok"] and hit["hits"][0]["id"] == kinkade["id"]
    got = sp.resolve(_ns(anchor="confluence", on_item=LQ, item_id=kinkade["id"], name="Kinkade Creek", offset_m=50))
    assert got["ok"]
    assert abs(got["cuts"][0]["measure"] - got["reference"][0]["measure"] - 50.0) < 0.5


def test_area_names_autocomplete_prefix_first_then_nearest_in_the_layer_s_spelling(sp):
    got = sp.area_names("parks_bc", "qualicum", LQ)
    assert got[0]["name"] == "LITTLE QUALICUM FALLS PARK" and got[0]["field"] == "PROTECTED_LANDS_NAME"
    near = sp.area_names("parks_bc", "park", LQ)
    assert len(near) <= sp.NAME_HITS and near[0]["name"].casefold().startswith("park")
    rest = [r["distance_km"] for r in near if not r["name"].casefold().startswith("park")]
    assert rest == sorted(rest)                              # after the prefix hits: nearest first
    assert sp.area_names("parks_nat", "PACIFIC")[0]["name"].startswith("Pacific Rim")
    with pytest.raises(ValueError):
        sp.area_names("wmu", "x")


def test_every_proposal_is_shown_under_its_real_id_and_the_build_lands_the_same(sp):
    """T355: the review app's ids (`dfo.real_ids`) are the build's (`proposals.build`) — never a local ref."""
    import dfo
    from pipeline.regs.review import export as E
    from pipeline.regs.review import proposals as P
    ids = dfo.real_ids()
    assert ids and not [v for v in ids.values() if P.is_local_ref(v)]
    landed = {p["split_id"] for p in P.build(E.load(None), P.Sources.live()) if p["kind"] == "new_split"}
    assert landed <= set(ids.values())
