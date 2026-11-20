"""T761 (owner 2026-11-15) — a DFO "rest of" place: its own bounds, the places it leaves out, and DFO's "areas AND times".

  the model      a DFO rest may carry `bounds` (a stretch) and its own `siblings` list; a rule-book rest takes no bounds
  the reach      bounds less the LIVE, drawn listed places; a dormant one takes nothing; one that does not bind is
                 skipped and named (never `complement_unknown` for the whole rest); the listed places' sections inside
                 the bounds are reported (`rest_behind`)
  the chain      on those sections the rest answers AFTER its listed places, whatever the sizes (the Little Qualicum
                 shape: the listed closure in its season, the rest's limit off-season)
  the review     an extent fix says bounds + listed places, and the place's extent opens in the editor again

Every test stands on the four-piece river of `test_complement_rest` (its graph, registry and walk); each guard is
pinned by a mutation.
"""
from __future__ import annotations

import pytest

from pipeline.atlas.reach import build as B
from pipeline.atlas.reach.build import build_reach
from pipeline.atlas.reach.models import Outcome, Reason
from pipeline.deliver import salmon as S
from pipeline.regs.dfo_salmon import entries as E
from pipeline.regs.parsing.entry_models import Extent
from pipeline.regs.review import marks as M
from pipeline.regs.review import proposals as P
from pipeline.tests.test_complement_rest import G, REG, _item, _walk  # noqa: F401  (the river and its walk)

#: the river (m1..m4), the reach (m2, m3) and one more listed stretch on m4
REG2 = {**REG, "gnis:low": _item("gnis:low", ["m4"])}
WATER = E.WaterBinding(water_id="1:river", region="1", region_number=1, name="River", item_ids=["gnis:river"])
#: the bounds "a to b": m2..m4 (a cut stands in for the resolver's route measures, as `test_complement_rest` does)
BOUNDS = {"op": "between", "splits": ["a", "b"]}


@pytest.fixture(autouse=True)
def _cuts(monkeypatch):
    real = B._resolve.resolve_extent

    def resolve(reg, g, covered, ex, reasons=None):
        if ex.get("op") == "between" and list(ex.get("splits") or ()) == ["a", "b"]:
            return {"sections": ["m2", "m3", "m4"]}
        return real(reg, g, covered, ex, reasons)
    monkeypatch.setattr(B._resolve, "resolve_extent", resolve)


def _place(lid, extents, status="active", area=""):
    return E.EntryLocation.from_dict({
        "location_id": lid, "region": "1", "region_number": 1, "kind": "water", "precedence": 3, "water": "River",
        "water_id": "1:river", "status": status, "source_text": {"specific_area": area},
        "binding": {"extents": extents, "tributaries": False}})


def _places(rest_ex, *, reach_status="active", low=None):
    out = {"1:river:rest": _place("1:river:rest", [rest_ex], area="excluding the areas below"),
           "1:river:reach": _place("1:river:reach", [{"op": "whole", "item_id": "gnis:reach"}], reach_status,
                                   area="the reach"),
           "1:river:low": _place("1:river:low", low if low is not None else [{"op": "whole", "item_id": "gnis:low"}],
                                 area="the low stretch")}
    return out


def _bind(places, lid="1:river:rest"):
    entry, rules = E.to_reach_input(places[lid], [{}], WATER, siblings=places)
    return build_reach(entry, rules[0], REG2, G)


def _diag(diags, kind):
    return next(d.payload for d in diags if d.kind == kind)


# --------------------------------------------------------------------------------------------------------- model

def test_the_model_takes_bounds_on_a_dfo_rest_only():
    ex = Extent.model_validate({"op": "rest", "bounds": BOUNDS, "siblings": ["x"]}, context={"rest_at_build": True})
    assert ex.model_dump(mode="json", exclude_defaults=True) == {"op": "rest", "siblings": ["x"], "bounds": BOUNDS}
    with pytest.raises(ValueError, match="belong to a DFO place"):
        Extent.model_validate({"op": "rest", "bounds": BOUNDS, "siblings": ["x"]})     # a rule-book rest: unchanged
    with pytest.raises(ValueError, match="bounds belong to op rest"):
        Extent.model_validate({"op": "whole", "bounds": BOUNDS}, context={"rest_at_build": True})
    with pytest.raises(ValueError, match="only an op and its cuts"):
        Extent.model_validate({"op": "rest", "bounds": {**BOUNDS, "item_id": "gnis:1"}}, context={"rest_at_build": True})
    with pytest.raises(ValueError, match="a stretch"):
        Extent.model_validate({"op": "rest", "bounds": {"op": "within", "area_id": "area:x"}},
                              context={"rest_at_build": True})


# --------------------------------------------------------------------------------------------------------- reach

def test_the_rest_is_its_bounds_less_its_listed_places(monkeypatch):
    """bounds m2..m4 less the reach (m2, m3) and the low stretch (m4)... with only the reach listed: m4.
    MUTATION: `DFO_REST_HAS_ITS_OWN_BOUNDS` off and it is the whole river less the reach (m1, m4)."""
    ps = _places({"op": "rest", "bounds": BOUNDS, "siblings": ["1:river:reach"]})
    b, diags = _bind(ps)
    assert b.outcome is Outcome.bound and b.sections == ("m4",)
    assert _diag(diags, "complement")["bounds"] == BOUNDS
    assert _diag(diags, B.REST_BEHIND) == {"behind": {"1:river:reach": ["m2", "m3"]}}
    monkeypatch.setattr(B, "DFO_REST_HAS_ITS_OWN_BOUNDS", False)
    assert _bind(ps)[0].sections == ("m1", "m4")


def test_its_own_list_is_honoured_and_no_list_is_every_other_live_place():
    named = _places({"op": "rest", "siblings": ["1:river:reach"]})
    assert _bind(named)[0].sections == ("m1", "m4"), "the low stretch is not listed: it stays in the rest"
    every = _places({"op": "rest"})
    assert _bind(every)[0].sections == ("m1",), "no list: every other live place of the water comes out"
    with pytest.raises(ValueError, match="no other place of its region"):
        _bind(_places({"op": "rest", "siblings": ["1:river:nowhere"]}))


def test_a_dormant_listed_place_takes_nothing(monkeypatch):
    """Nanaimo / Somass / Stamp. MUTATION: `A_DORMANT_LISTED_PLACE_TAKES_NOTHING` off and the retired reach takes its
    water out again."""
    ps = _places({"op": "rest"}, reach_status="dormant")
    b, diags = _bind(ps)
    assert b.sections == ("m1", "m2", "m3")
    assert _diag(diags, "complement")["dormant"] == ["1:river:reach"]
    assert [r["state"] for r in E.answers_before(ps["1:river:rest"], ps)] == ["answers_first", "dormant"]
    monkeypatch.setattr(E, "A_DORMANT_LISTED_PLACE_TAKES_NOTHING", False)
    assert _bind(ps)[0].sections == ("m1",)


def test_a_listed_place_that_does_not_bind_is_skipped_and_named(monkeypatch):
    """Nanaimo's stretch whose cut is not built: skipped, named — never the whole rest unknown. A place with no extent
    yet is named too. MUTATION: `DFO_REST_SKIPS_A_LISTED_PLACE_THAT_DOES_NOT_BIND` off and the rest goes unknown."""
    ps = _places({"op": "rest"}, low=[{"op": "whole", "item_id": "gnis:missing"}])
    ps["1:river:new"] = _place("1:river:new", [], area="a place not placed yet")
    b, diags = _bind(ps)
    assert b.outcome is Outcome.bound and b.sections == ("m1", "m4")
    skipped = _diag(diags, "complement")["skipped"]
    assert list(skipped) == ["1:river:low"] and "does not bind" in skipped["1:river:low"]
    named = _places({"op": "rest", "siblings": ["1:river:reach", "1:river:new"]})
    named["1:river:new"] = _place("1:river:new", [], area="a place not placed yet")
    got = _diag(_bind(named)[1], "complement")["skipped"]
    assert got == {"1:river:new": "it has no extent yet"}
    assert [r["state"] for r in E.answers_before(named["1:river:rest"], named)] == ["answers_first", "undrawn"]
    monkeypatch.setattr(B, "DFO_REST_SKIPS_A_LISTED_PLACE_THAT_DOES_NOT_BIND", False)
    b, _ = _bind(ps)
    assert b.outcome is Outcome.unresolved and b.reason is Reason.complement_unknown


def test_a_rule_book_rest_is_unchanged():
    """AGENTS 13: a synopsis rest carries no DFO mark — an unbound sibling still leaves it unknown, no `rest_behind`."""
    r1 = {"rule_id": "r1", "extents": [{"op": "whole", "item_id": "gnis:missing"}]}
    r2 = {"rule_id": "r2", "extents": [{"op": "rest", "siblings": ["r1"]}]}
    b, diags = build_reach({"entry_id": "r1:x", "matched": ["gnis:river"], "rules": [r1, r2]}, r2, REG2, G)
    assert b.reason is Reason.complement_unknown and not any(d.kind == B.REST_BEHIND for d in diags)


# --------------------------------------------------------------------------------------------------------- chain

JUL_20, SEP_15 = 720, 915


def _rule(rid, take, frm, to, may_target=None):
    r = {"rule_id": rid, "type": "retention_limit", "verbatim": rid, "species": ["CO"], "take": take,
         "when": {"dates": [{"from_month": frm[0], "from_day": frm[1], "to_month": to[0], "to_day": to[1]}]}}
    if may_target is not None:
        r["may_target"] = may_target
    return r


def _salmon():
    """The Little Qualicum shape: the rest (one section of its own, s1) and a LARGER listed stretch (s2, s3; s3 outside
    the rest's bounds) closed Jul 1-Aug 31; the rest's coho limit all year."""
    from types import SimpleNamespace
    locs = [SimpleNamespace(location_id=l, precedence=3, fingerprints=[f], water="River", water_id="1:river",
                            source_text={"specific_area": l}, tidal_water=False, region_number=1)
            for l, f in (("1:river:a-rest", "fr"), ("1:river:z-listed", "fl"))]
    feed = {"locators": [{"fingerprint": "fr", "rules": [_rule("2 per day", 2, (1, 1), (12, 31))]},
                         {"fingerprint": "fl", "rules": [_rule("No fishing", 0, (7, 1), (8, 31), False)]}]}
    placed = [{"location_id": "1:river:a-rest", "mapped": True, "sections": ["s1"],
               "behind": {"1:river:z-listed": ["s2"]}},
              {"location_id": "1:river:z-listed", "mapped": True, "sections": ["s2", "s3"]}]
    ef = SimpleNamespace(region_name="R1", locations=locs, water=lambda wid: None)
    return {"1": ef}, {"1": feed}, placed, {"s1": 1, "s2": 2, "s3": 3}


def _coho(payload, sections, sid, day):
    c = payload["chains"][dict(sections)[sid]]
    runs = c["sp"][S.SPECIES.index("CO")]
    i = S.DAYS.index(day)
    v = next(runs[k + 1] for k in range(len(runs) - 2, -1, -2) if runs[k] <= i)
    return c["l"], payload["verdicts"][v]["s"]


def test_on_a_listed_stretch_the_rest_answers_after_it(monkeypatch):
    """The listed closure in its season, the rest's limit off-season, on the stretch; the rest alone on its own water;
    the listed place alone outside the bounds. MUTATION: `rest_after` a no-op and the smaller rest sorts first and
    opens coho the listed stretch closes on Jul 20."""
    payload, sections = S.build(*_salmon(), section_regions=None)
    assert _coho(payload, sections, 2, JUL_20) == (["1:river:z-listed", "1:river:a-rest"], "closed")
    assert _coho(payload, sections, 2, SEP_15)[1] == "open"
    assert _coho(payload, sections, 1, JUL_20) == (["1:river:a-rest"], "open")
    assert _coho(payload, sections, 3, SEP_15) == (["1:river:z-listed"], "closed")
    monkeypatch.setattr(S, "rest_after", lambda key, after: tuple(key))
    payload, sections = S.build(*_salmon(), section_regions=None)
    assert _coho(payload, sections, 2, JUL_20) == (["1:river:a-rest", "1:river:z-listed"], "open")


def test_a_rest_its_listed_places_take_whole_still_answers_behind_them():
    """Somass: the whole-river bait ban is listed, so the rest binds no section of its own (`mapped: false`) — it is
    no undrawn part of its water, it answers behind the listed place."""
    files, feeds, placed, sid_of = _salmon()
    placed[0] = {"location_id": "1:river:a-rest", "mapped": False, "sections": [],
                 "behind": {"1:river:z-listed": ["s2", "s3"]}}
    payload, sections = S.build(files, feeds, placed, sid_of, section_regions=None)
    assert payload["unmapped"] == []
    assert _coho(payload, sections, 3, SEP_15) == (["1:river:z-listed", "1:river:a-rest"], "open")


def test_rest_after_moves_only_a_rest_whose_listed_place_is_in_the_chain():
    after = {"r": frozenset({"l1", "l2"})}
    assert S.rest_after(("r", "l1", "x", "l2", "y"), after) == ("l1", "x", "l2", "r", "y")
    assert S.rest_after(("r", "x"), after) == ("r", "x")


# --------------------------------------------------------------------------------------------------------- review

def test_an_extent_fix_says_bounds_and_listed_places_and_opens_again():
    fix = M.ExtentFix.model_validate({"scope": "entry", "op": "rest", "bounds": "between", "items": ["gnis:2936"],
                                      "siblings": ["6:a", "6:b"],
                                      "points": [{"kind": "split", "split_id": "a"}, {"kind": "split", "split_id": "b"}]})
    ex, probs = P.extent_of(fix, lambda i, p: p.split_id, rest_at_build=True)
    assert probs == [] and ex == {"op": "rest", "bounds": BOUNDS, "siblings": ["6:a", "6:b"], "item_id": "gnis:2936"}
    E.Binding.from_dict({"extents": [ex]})                                  # the curated model takes it
    assert P.extent_of(fix, lambda i, p: p.split_id)[1], "a catalogue rule's rest has no bounds: a hand edit"
    back, why = P.fixes_of_extents([ex], None)
    assert why == [] and back == [fix], "the editor opens on the extent the place has — never 'cannot edit'"
    with pytest.raises(ValueError, match="takes 2 split points"):
        M.ExtentFix.model_validate({"scope": "entry", "op": "rest", "bounds": "between", "points": []})
    with pytest.raises(ValueError, match="only"):
        M.ExtentFix.model_validate({"scope": "entry", "op": "whole", "siblings": ["6:a"]})
    with pytest.raises(ValueError, match="at least one"):
        M.ExtentFix.model_validate({"scope": "entry", "op": "rest", "siblings": []})


def test_a_mark_saved_before_these_fields_is_the_same_line():
    old = {"scope": "entry", "op": "rest", "points": [], "items": []}
    m = M.Mark.model_validate({
        "scope": "dfo", "target": "1:x", "verdict": "verified", "answers": {"T": "y", "R": "y", "P": "y"},
        "reviewer": "dawson", "date": "2026-11-15", "pdf_sha256": "0" * 64, "extent_fix": [old],
        "prints": {"text": "t", "records": [{"key": "entry", "meaning": "m", "binding": "b"}]}})
    assert '"bounds"' not in M.line(m) and '"siblings"' not in M.line(m)
    new = M.Mark.model_validate({**m.model_dump(mode="json"), "extent_fix": [{**old, "siblings": ["1:y"]}]})
    assert '"siblings":["1:y"]' in M.line(new) and M.Mark.model_validate_json(M.line(new)) == new
