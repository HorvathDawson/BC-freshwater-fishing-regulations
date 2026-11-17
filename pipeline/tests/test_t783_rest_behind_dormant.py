"""T783 (general audit 2026-11-16) — a "rest of" place stands behind EVERY place it lists or leaves out, live or dormant.

T761 made a dormant listed place take nothing, so the rest binds the dormant place's sections too. The rest moved
behind its LIVE listed places only (the reach row's `behind`); against a dormant one plain size order decided, and the
smaller rest sorted first (Stamp River chains 20, 55, 56). The day DFO prints the dormant wording again the live feed
gives it rules, and the rest's opening would answer first on the dormant place's stretch until the next release.
"""
from __future__ import annotations

from types import SimpleNamespace

from pipeline.deliver import salmon as S
from pipeline.regs.dfo_salmon import entries as E

JUL_20 = 720


def _place(lid, extents, fp, status="active"):
    return E.EntryLocation.from_dict({
        "location_id": lid, "region": "1", "region_number": 1, "kind": "water", "precedence": 3, "water": "River",
        "water_id": "1:river", "status": status, "fingerprints": [fp], "source_text": {"specific_area": lid},
        "binding": {"extents": extents, "tributaries": False}})


def _coho(rid, take, may_target=None):
    r = {"rule_id": rid, "type": "retention_limit", "verbatim": rid, "species": ["CO"], "take": take,
         "when": {"dates": [{"from_month": 1, "from_day": 1, "to_month": 12, "to_day": 31}]}}
    if may_target is not None:
        r["may_target"] = may_target
    return r


def _inputs(dormant_rules):
    """The Stamp shape: the rest (2 sections: s1, s2) lists a DORMANT place (s2..s8, so it is larger); the dormant
    place takes nothing out of the rest, so both cover s2. `dormant_rules`: what the feed gives the dormant place (the
    release: nothing; the live feed, the day DFO prints its wording again: its rows)."""
    rest = _place("1:river:a-rest", [{"op": "rest", "siblings": ["1:river:z-dormant"]}], "fr")
    dormant = _place("1:river:z-dormant", [{"op": "whole", "item_id": "gnis:river"}], "fd", status="dormant")
    feed = {"locators": [{"fingerprint": "fr", "rules": [_coho("2 per day", 2)]},
                         {"fingerprint": "fd", "rules": dormant_rules}]}
    secs = [f"s{i}" for i in range(1, 9)]
    placed = [{"location_id": rest.location_id, "mapped": True, "sections": ["s1", "s2"]},
              {"location_id": dormant.location_id, "mapped": True, "sections": secs[1:]}]
    ef = SimpleNamespace(region_name="R1", locations=[rest, dormant], water=lambda wid: None)
    return {"1": ef}, {"1": feed}, placed, {s: i for i, s in enumerate(secs, 1)}


def _chain(payload, sections, sid):
    return payload["chains"][dict(sections)[sid]]


def _coho_on(payload, chain, day):
    runs = chain["sp"][S.SPECIES.index("CO")]
    i = S.DAYS.index(day)
    return payload["verdicts"][next(runs[k + 1] for k in range(len(runs) - 2, -1, -2) if runs[k] <= i)]["s"]


def test_the_rest_stands_behind_a_dormant_listed_place():
    """In the release the dormant place sits AHEAD of the rest on its sections (it says nothing, so the rest answers);
    the day it gains rules it answers first. MUTATION: `dormant_listed` returning nothing puts the smaller rest first
    and its opening speaks on the stretch DFO closes."""
    payload, sections = S.build(*_inputs([]), section_regions=None)
    assert _chain(payload, sections, 2)["l"] == ["1:river:z-dormant", "1:river:a-rest"]
    assert _coho_on(payload, _chain(payload, sections, 2), JUL_20) == "open", "dormant: the rest answers"
    assert _chain(payload, sections, 1)["l"] == ["1:river:a-rest"]
    payload, sections = S.build(*_inputs([_coho("No fishing", 0, False)]), section_regions=None)
    assert _coho_on(payload, _chain(payload, sections, 2), JUL_20) == "closed", "listed again: it answers first"


def test_dormant_listed_names_only_a_rests_dormant_places():
    ef, *_ = _inputs([])
    assert S.dormant_listed(ef["1"].locations) == {"1:river:a-rest": frozenset({"1:river:z-dormant"})}
    plain = SimpleNamespace(location_id="x")                   # a place with no binding (a closed-area piece, a fixture)
    assert S.dormant_listed([plain]) == {}
