"""T230 owner add-on: the in-season AUTO matcher (`pipeline.regs.inseason.auto`) — locators, split matching,
ambiguity → guess, owner wins, determinism. A tiny hand registry; no build needed."""
from __future__ import annotations

import pytest

from pipeline.common.models.registry import RegistryItem
from pipeline.regs.inseason import auto as A
from pipeline.regs.inseason import matches as M
from pipeline.regs.matching.matcher import build_id_index, build_name_index, build_override_index

AT = "2026-10-28T12:00:00Z"


def _ctx(verify=None) -> A.Context:
    reg = {
        "gnis:1": RegistryItem(id="gnis:1", name="Peace River", kind="stream", mus=("7-30",), ref_ids=("gnis:1",)),
        "gnis:2": RegistryItem(id="gnis:2", name="Perry Creek", kind="stream", mus=("4-16",),
                               ref_ids=("gnis:2", "wsc:300-1-2")),
        "gnis:3": RegistryItem(id="gnis:3", name="Perry Creek", kind="stream", mus=("4-20",),
                               ref_ids=("gnis:3", "wsc:300-9-5")),
        "gnis:4": RegistryItem(id="gnis:4", name="St. Mary River", kind="stream", mus=("4-20",),
                               ref_ids=("gnis:4", "wsc:300-9")),
        "gnis:5": RegistryItem(id="gnis:5", name="Lardeau River", kind="stream", mus=("4-30",), ref_ids=("gnis:5",)),
    }
    splits = {"waterbodies": [
        {"name": "PEACE RIVER", "applies_to": {"gnis_id": "1"}, "splits": [
            {"id": "peace__canyon_dam", "label": "Peace Canyon Dam"},
            {"id": "peace__hwy_29", "label": "Hwy. 29 bridge"},
            {"id": "peace__site_c", "label": "Site C dam"}]},
        {"name": "LARDEAU RIVER", "applies_to": {"gnis_id": "5"}, "splits": [
            {"id": "lardeau__mobbs", "label": "fishing boundary signs near the Mobbs Creek confluence (600 m below)"}]},
    ]}
    return A.Context(reg, build_name_index(reg), build_id_index(reg), build_override_index([]), splits, verify)


def _row(rid, water, change, region="7B"):
    return {"id": rid, "region": region, "water": water, "change": change}


def _empty() -> M.Matches:
    return M.Matches.model_validate({"format": "inseason-matches/1", "matches": {}})


# --------------------------------------------------------------------------- locators

@pytest.mark.parametrize("cell,change,want", [
    ("Peace River", "No fishing between Peace Canyon Dam and Hwy 29 Bridge",
     [("between", ["Peace Canyon Dam", "Hwy 29 Bridge"])]),
    ("Alexander Creek (upstream of the easternmost Hwy 3 bridge)", "Clarification: licence",
     [("upstream_of", ["the easternmost Hwy 3 bridge"])]),
    ("Fraser River", "No fishing for steelhead in the Fraser River (mainstem) from Hells Gate to Spuzzum Creek "
     "confluence August 15 to December 31. No fishing upstream of Hells Gate January 1 to December 31.",
     [("between", ["Hells Gate", "Spuzzum Creek confluence"]), ("upstream_of", ["Hells Gate"])]),
    ("Heber Creek", "No fishing upstream of top of lower canyon, located 1.3 km upstream of the Gold River "
     "confluence November 1 to August 31.", [("upstream_of", ["top of lower canyon"])]),
    ("Lardeau River", "No fishing downstream of fishing boundary signs at Trout Lake outlet, to fishing boundary "
     "signs approximately 600m downstream near the confluence of Mobbs Creek Exempt from spring closure",
     [("between", ["fishing boundary signs at Trout Lake outlet",
                   "fishing boundary signs approximately 600m downstream near the confluence of Mobbs Creek"])]),
    ("Granby River", "No fishing July 30 to September 15 from 12:00 pm to midnight.", []),
    ("Salmon River", "No fishing July 30 to September 15, including tributaries.", []),
])
def test_locators(cell, change, want):
    assert [(loc.op, loc.places) for loc in A.locators(cell, change)] == want


def test_split_match_same_words_subset_and_ambiguity():
    sp = [{"id": "a", "label": "Hell's Gate"}, {"id": "b", "label": "signs 2800 m downstream of Hell's Gate"},
          {"id": "c", "label": "Spuzzum Creek → Fraser River"}, {"id": "d", "label": "easternmost Hwy 3"}]
    assert A.match_split("Hells Gate", sp, "Fraser River")[:2] == ("a", "high")          # same words beat a subset
    assert A.match_split("Spuzzum Creek confluence", sp, "Fraser River")[:2] == ("c", "high")
    assert A.match_split("the easternmost Hwy 3 bridge", sp, "Alexander Creek")[:2] == ("d", "high")
    sid, why, _ = A.match_split("the Region 5 boundary", sp, "Fraser River")
    assert sid is None and why == "no split on this water reads 'the Region 5 boundary'"
    # only generic words shared: medium
    assert A.match_split("falls near the km 26 post", [{"id": "f", "label": "Falls"}], "Lodgepole Creek")[:2] == \
        ("f", "medium")


# --------------------------------------------------------------------------- proposals

def test_unambiguous_between_is_auto():
    p = A.propose(_row("is-000000000001", "Peace River", "No fishing between Peace Canyon Dam and Hwy 29 Bridge"),
                  _ctx())
    assert p.auto and p.confidence == "high"
    assert [e.model_dump() for e in p.extents] == [
        {"op": "between", "item_id": "gnis:1", "splits": ["peace__canyon_dam", "peace__hwy_29"]}]


def test_boundary_without_split_is_a_guess_with_best_extents():
    p = A.propose(_row("is-000000000002", "Lardeau River",
                       "No fishing downstream of fishing boundary signs at Trout Lake outlet, to fishing boundary "
                       "signs approximately 600m downstream near the confluence of Mobbs Creek", "4"), _ctx())
    assert not p.auto
    assert p.why == ("'fishing boundary signs at Trout Lake outlet': no split on this water reads "
                     "'fishing boundary signs at Trout Lake outlet'")
    assert [e.op for e in p.extents] == ["whole"]


def test_two_same_named_waters_disambiguated_only_by_the_printed_licence_river():
    ctx = _ctx()
    p = A.propose(_row("is-000000000003", "Perry Creek",
                       "Clarification: St. Mary River classified licence required", "4"), ctx)
    assert p.auto and p.confidence == "medium" and p.extents[0].item_id == "gnis:3"
    q = A.propose(_row("is-000000000004", "Perry Creek", "No fishing", "4"), ctx)
    assert not q.auto and q.why == "2 waters named 'Perry Creek' in the region"


def test_tributaries_only_and_other_water_are_guesses():
    ctx = _ctx()
    assert not A.propose(_row("is-000000000005", "Peace River's Tributaries", "No fishing"), ctx).auto
    p = A.propose(_row("is-000000000006", "Peace River", "No Fishing in the Northrop Spawning Channel"), ctx)
    assert not p.auto and "another water" in p.why


def test_a_proposal_that_binds_nothing_is_a_guess():
    p = A.propose(_row("is-000000000007", "Peace River", "No fishing"), _ctx(verify=lambda e: (False, "no sections")))
    assert not p.auto and p.why == "the extent binds no section: no sections"


# --------------------------------------------------------------------------- apply: owner wins, deterministic

FEED = {"rows": [_row("is-000000000001", "Peace River", "No fishing between Peace Canyon Dam and Hwy 29 Bridge"),
                 _row("is-000000000004", "Perry Creek", "No fishing", "4")]}


def test_apply_writes_auto_and_guess_and_is_idempotent():
    doc, props = A.apply(_empty(), FEED, _ctx(), AT)
    m = doc.matches["is-000000000001"]
    assert m.source == "auto" and m.by == "auto" and m.confidence == "high"
    assert m.evidence["splits"] == {"Peace Canyon Dam": "peace__canyon_dam", "Hwy 29 Bridge": "peace__hwy_29"}
    assert "is-000000000004" not in doc.matches and doc.guesses["is-000000000004"]["why"].startswith("2 waters")
    assert [h.action for h in doc.history] == ["auto"]
    text = M.dumps(doc)
    again, _ = A.apply(M.parse(text), FEED, _ctx(), "2030-01-01T00:00:00Z")
    assert M.dumps(again) == text                 # nothing changed: no new history, no new clock


def test_owner_match_is_never_overwritten():
    owner = M.Match(extents=[M.InSeasonExtent(op="whole", item_id="gnis:1")], note="mine", by="owner", at=AT)
    doc = M.set_match(_empty(), "is-000000000001", owner, at=AT, by="owner", note="mine")
    new, props = A.apply(doc, FEED, _ctx(), AT)
    assert new.matches["is-000000000001"] == doc.matches["is-000000000001"]
    assert [p.rid for p in props] == ["is-000000000004"]
    assert "is-000000000001" not in new.guesses


def test_an_auto_match_that_no_longer_holds_becomes_a_guess():
    doc, _ = A.apply(_empty(), FEED, _ctx(), AT)
    moved = {"rows": [_row("is-000000000001", "Peace River", "No fishing between Peace Canyon Dam and Nowhere Bridge")]}
    new, _ = A.apply(doc, moved, _ctx(), AT)
    assert "is-000000000001" not in new.matches
    assert new.history[-1].action == "clear" and new.history[-1].by == "auto"
    assert "Nowhere Bridge" in new.guesses["is-000000000001"]["why"]


def test_owner_save_drops_the_guess_and_is_owner():
    doc, _ = A.apply(_empty(), FEED, _ctx(), AT)
    m = M.Match(extents=[M.InSeasonExtent(op="whole", item_id="gnis:3")], note="checked")
    new = M.set_match(doc, "is-000000000004", m, at=AT, by="owner", note="checked")
    assert new.matches["is-000000000004"].source == "owner" and "is-000000000004" not in new.guesses
