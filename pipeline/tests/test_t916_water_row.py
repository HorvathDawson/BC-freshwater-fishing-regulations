"""T916 — "This water in the rule book p.7" on Sweltzer River (owner 2026-11-19: "Has page 7 link at top (which is wrong)
with real page 26 at bottom"). The app's line (core `waterBookOf`, T474) linked the pages of every rank-0 rule on the part.
Rank 0 is the LADDER's place for any rule bound to a water (`region/water` and `province/water` both rank 0), so a B.C. table
line bound to a whole watershed sat there too: the White Sturgeon Conservation Licence (`zp:white_sturgeon_licence`, printed
p.7, the licences page) binds the Fraser watershed from Mission to the Williams Lake River, Sweltzer River included.

ONE DECISION, ONE PLACE: whether a rule's entry is a ROW OF A REGION'S WATER TABLE (the book's entry for a water) is the
export's entry kind ("water"), the same fact `applies_here` words as "This water’s own row in the …". The display facts carry
it as `row: true` (absent otherwise); the app's line reads it and never re-derives it from an id or a rank.

Mutations, each failing the named test:
  * `add_sources` never sets `row`                 -> test_a_water_rows_rule_is_marked_a_row
  * `add_sources` sets `row` on every rule          -> test_a_province_line_bound_to_a_watershed_is_no_row,
                                                      test_a_zone_line_bound_to_a_water_is_no_row
"""
from __future__ import annotations

import json

import pytest

from pipeline.deliver.answers import model as M
from pipeline.deliver.answers.display import add_sources
from pipeline.deliver.answers.display import decided as DEC

SWELTZER = "r2:sweltzer_creek@2-3"
STURGEON = "zp:white_sturgeon_licence"
RUBBLE = "z2:rubble_creek"
RUBBLE_SAYS = ("NOTICE TO ANGLERS The area known as the Rubble Creek Landslide Hazard Area is a high risk slide area. People "
               "who fish in this area do so at THEIR OWN RISK.")


def _rule(entry: str, rule: str, authority: str, verbatim: str = "") -> dict:
    return {"id": f"{entry}::{rule}", "entry_id": entry, "rule_id": rule, "fields": {}, "verbatim": verbatim,
            "provenance": {"rank": 0, "authority": authority, "binds_to": "water", "entry_name": entry}}


@pytest.fixture
def facts(monkeypatch):
    """`add_sources` over three rank-0 rules: Sweltzer River's own row (p.26), the B.C. sturgeon licence line (p.7), and a
    Region 2 table line naming one water (p.21). No row boxes are read (the sidecar is the book's; not this test's)."""
    monkeypatch.setitem(DEC._BOXES, "rows", {})
    # (T1032) a zone or province line carries the book's words, as the export does: its highlight is located by them
    rules = [_rule(SWELTZER, "sweltzer_creek.r1", "region"),
             _rule(STURGEON, "white_sturgeon_licence.r2", "province", "This is a catch-and-release only fishery."),
             _rule(RUBBLE, "rubble_creek.r1", "region", RUBBLE_SAYS)]
    doc = {"entries": {SWELTZER: {"name": "Sweltzer River", "full_name": "SWELTZER CREEK", "kind": "water", "pages": [26]},
                       STURGEON: {"name": "Fraser watershed, Mission to Williams Lake River", "kind": "province",
                                  "pages": [7]},
                       RUBBLE: {"name": "Rubble Creek", "kind": "zone", "pages": [21]}},
           "rules": {x["id"]: x for x in rules}, "rulesets": {}, "waters": {},
           "guide": {"pdf_pages": {"7": 9, "21": 23, "26": 28}}}
    out = [{"kind": "gate", "title": "No fishing"} for _ in rules]
    add_sources(out, doc, [x["id"] for x in rules])
    for f in out:
        M.RuleFacts.model_validate_json(json.dumps(f))   # the field is in the shipped schema
    return dict(zip((SWELTZER, STURGEON, RUBBLE), out))


def test_a_water_rows_rule_is_marked_a_row(facts):
    f = facts[SWELTZER]
    assert f["row"] is True and f["book"] == [[26, 28]]


def test_a_province_line_bound_to_a_watershed_is_no_row(facts):
    f = facts[STURGEON]
    assert "row" not in f and f["book"] == [[7, 9]]


def test_a_zone_line_bound_to_a_water_is_no_row(facts):
    assert "row" not in facts[RUBBLE]
