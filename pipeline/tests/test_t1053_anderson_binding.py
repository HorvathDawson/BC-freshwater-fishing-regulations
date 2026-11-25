"""T1053 (owner 2026-11-25: "Yes, fix it in 0.9.1 (Recommended)"): the Region 1 row “ANDERSON” LAKE (printed p.14, MU 1-3)
is "Unnamed lake in the Walbran Creek Watershed approximately 7 km west/southwest of Mt. Walbran" — FWA `wbk:329504289`,
the polygon FWA gives the BC Lakes Database WBID 00105SANJ the row's own note cites. It was pinned to the gazetted Anderson
Lake near Courtenay (gnis 1657 / `wbk:329095566`, MU 1-6, on Dove Creek), whose catch-and-release ✱ then reopened Dove
Creek under the Tsolum River's closure. The binding lives in two curated files; both are pinned here."""
from __future__ import annotations

import json

from pipeline.tests import under_test

WALBRAN, COURTENAY = "329504289", "329095566"


def _override():
    links = json.loads(under_test.curated().regulations.book_name_links.read_text())
    hit = [o for o in links if (o.get("criteria") or {}).get("name_verbatim") == '"ANDERSON" LAKE']
    assert len(hit) == 1, hit
    return hit[0]


def test_the_anderson_row_is_matched_to_the_walbran_lake():
    o = _override()
    assert o.get("waterbody_keys") == [WALBRAN]
    assert not o.get("gnis_ids"), "never the Courtenay lake's gnis 1657"
    assert o["criteria"]["mus"] == ["1-3"]


def test_the_regulation_name_sits_on_the_walbran_polygon_not_the_courtenay_lake():
    variants = json.loads(under_test.curated().waters.name_variants.read_text())
    on = {w: [n["source"] for e in variants if w in (e["target"].get("wbks") or []) for n in e["names"]
              if n["name"].upper() == "ANDERSON LAKE"] for w in (WALBRAN, COURTENAY)}
    assert on[WALBRAN] == ["regulation"]
    assert "regulation" not in on[COURTENAY]
