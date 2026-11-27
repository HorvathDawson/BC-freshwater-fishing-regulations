"""T1177: the owner's one-off rulings, pinned on the curated files under test (no build needed).

O4 Hatton Creek takes the Caycuse River's fly-only rule (RULINGS E29); O13 Healy Lake's outlet stream is closed below
the lake by the ROW's extent; O3 the Whistler Lost Lake carries the electric-motor rule; D6 Twin Lakes 4-34 names
exactly the pair inside MU 4-34. Each test fails if its edit is reverted (checked by reverting each once, 2026-11-26).
"""
from __future__ import annotations

import json

from pipeline.tests import under_test

CATALOGUE = under_test.curated().regulations.synopsis
LINKS = under_test.curated().regulations.book_name_links


def _entry(entry_id: str) -> dict:
    region = entry_id.split(":", 1)[0][1:]
    doc = json.loads((CATALOGUE / f"region-{region}.json").read_text(encoding="utf-8"))
    found = [e for e in doc["entries"] if e["entry_id"] == entry_id]
    assert len(found) == 1, f"{entry_id}: {len(found)} entries"
    return found[0]


def _rule(entry: dict, rule_id: str) -> dict:
    return next(r for r in entry["rules"] if r["rule_id"] == rule_id)


def _override(name: str, mu: str) -> dict:
    hits = [o for o in json.loads(LINKS.read_text(encoding="utf-8"))
            if o.get("type") == "override" and o["criteria"]["name_verbatim"] == name and mu in o["criteria"]["mus"]]
    assert len(hits) == 1, f"{name} {mu}: {len(hits)} overrides"
    return hits[0]


def test_o4_hatton_creek_is_in_the_caycuse_fly_only_rule_and_no_row_scope_clips_it():
    """"Fly fishing only upstream of and including Hatton Creek": the rule binds the Caycuse above the confluence AND
    Hatton Creek. A row-level `whole` scope would clip every rule to the Caycuse and drop Hatton (measured in the side
    reach run), so the row states none."""
    e = _entry("r1:caycuse_river@1-3")
    r2 = _rule(e, "caycuse_river.r2")
    assert {"op": "whole", "item_id": "gnis:16117"} in r2["extents"]
    assert {"op": "upstream_of", "splits": ["caycuse_river__hatton_creek_into_caycuse_river"]} in r2["extents"]
    assert "when" not in r2, "the book dates the fly-only clause nowhere: all year (owner: \"it is all year\")"
    assert not any(x.get("op") == "whole" and not x.get("item_id") for x in e.get("extents") or ()), \
        "a row-level whole scope clips Hatton Creek away"


def test_o13_healy_lakes_outlet_stream_is_closed_below_the_lake_by_the_rows_extent():
    """Owner: "use global extent for this not rule level": the row (entry) is `downstream_of` the South Englishman
    River's Healy Lake boundary; the No Fishing rule itself stays `whole`."""
    e = _entry("r1:healy_lake_s_outlet_stream@1-5")
    assert e["extents"] == [{"op": "downstream_of", "splits": ["south_englishman_river__healy_lake"]}]
    assert _rule(e, "healy_lake_s_outlet_stream.r1")["extents"] == [{"op": "whole"}]


def test_o3_the_electric_motor_rule_is_on_the_whistler_lost_lake():
    """LOST LAKE (near Whistler) 2-8 (a misprint: Whistler is MU 2-9) is wbk:329303502, never the Malcolm Knapp
    Lost Lake wbk:329292352."""
    assert _entry("r2:lost_lake_near_whistler@2-8")["matched"] == ["wbk:329303502"]
    assert _override("LOST LAKE (near Whistler)", "2-8").get("waterbody_keys") == ["329303502"]


def test_d6_twin_lakes_4_34_names_exactly_the_pair_inside_the_unit():
    """Twin Lakes 4-34 is the gazetted pair beside Botts Lake (federal Sch.3 item 62, "west of Brisco"), and no
    longer Yankee and Canuck lakes by Premier Lake."""
    want = ["wbk:329094185", "wbk:329094397"]
    assert _entry("r4:twin_lakes@4-34")["matched"] == want
    o = _override("TWIN LAKES", "4-34")
    assert o.get("waterbody_keys") == ["329094185", "329094397"] and "gnis_ids" not in o
