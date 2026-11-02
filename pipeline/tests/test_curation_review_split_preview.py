"""T355: the review app's new-split PREVIEW is the build's own resolver (`split_preview.resolve`):
`resolve_split_defs` on the build's chains for the one water, read from the FWA source — so the cut it
shows is the cut the build makes. Pinned against the served atlas where the build already made one
(the Moyie's U.S. border, Little Qualicum Falls Park). Needs the FWA source and the served atlas."""
from __future__ import annotations

import pytest

from pipeline.common.curated import GENERATED, SOURCE
from pipeline.regs.review import marks as M

BACKEND = __import__("pathlib").Path(__file__).resolve().parents[2] / "curation-review" / "backend"
LQ = "gnis:10886"                                          # Little Qualicum River
pytestmark = pytest.mark.skipif(not ((SOURCE / "bc_fisheries_data.gpkg").exists()
                                     and (GENERATED.build() / "registry.json").exists()),
                                reason="needs the FWA source and the served atlas")


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
