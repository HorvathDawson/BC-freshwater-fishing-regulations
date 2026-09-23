"""The gear model, pinned by the records it must REFUSE.

Every case here validated cleanly until 2026-09-23. They were found by constructing wrong records
against the validators rather than by reading them, which is the only way this class of hole shows
itself: each one is a plausible thing a curator would write, and each one states something other
than what the book says.
"""
import pytest

from pipeline.regs.parsing.catalogue import (CONDUCT_ACTS, CatalogueRule, GearClause, GearSpec,
                                             GearWhen, Method, RuleType, Slot)

_BARE = dict(type=RuleType.method_rule, verbatim="v")
_METHOD_RULE = dict(_BARE, gear=[{"slot": "method", "allow": ["set_lining"]}])


def test_an_escape_with_no_condition_is_refused():
    """`GearWhen()` with every field defaulted is a valid object, so "(this does not apply to
    downrigger weights)" with the one key dropped becomes "there is no weight limit in B.C." — and
    a dropped key is what a parse that scanned the parenthetical and matched nothing produces.
    `is_empty()` was written for this and never called."""
    with pytest.raises(ValueError, match="lifts the clause everywhere"):
        GearClause(slot=Slot.weight_per_line_kg, max=1, unless=[GearWhen()])
    with pytest.raises(ValueError, match="states nothing"):
        GearClause(slot=Slot.downrigger, requires=GearSpec())


@pytest.mark.parametrize("key", ["allow", "only", "ban"])
def test_no_set_bound_may_be_empty(key):
    """An empty list is what a dropped key, a failed parse and a half-filled field all produce.
    `allow: []` was the SANCTIONED spelling of "everything in this slot is banned" — the widest
    statement in the schema, written in the one shape three accidents also write."""
    with pytest.raises(ValueError, match="what a dropped key also writes"):
        GearClause(slot=Slot.bait, **{key: []})
    assert GearClause(slot=Slot.bait, **{key: ["any_bait"]}).slot is Slot.bait


def test_a_spec_slot_takes_no_number():
    """`light: {max: 1}` read as "at most one light" when the printed 1 is METRES — "within 1 m of
    the hook". `light` was the one slot admitted to a number with no unit in its name, which is
    this enum's founding complaint."""
    with pytest.raises(ValueError, match="says HOW the thing must be"):
        GearClause(slot=Slot.light, max=1)


def test_a_counted_slot_takes_no_set_bound():
    """Only `allow` was guarded, so `only` and `ban` rode straight onto counted slots.
    `{hooks_per_line, only: ["single"], max: 1}` rebuilds the exact collapse this enum ends — a
    statement about hook TYPE on the slot that COUNTS hooks."""
    for bound in ({"ban": ["treble"]}, {"only": ["single"]}, {"allow": ["x"]}):
        with pytest.raises(ValueError, match="use max/min"):
            GearClause(slot=Slot.points_per_hook, max=1, **bound)


def test_members_qualifies_a_bound_and_never_replaces_one():
    """Without this, "only one hook, one lure OR one fly is attached" — the basic licence
    entitlement, binding every angler on every water — inverts into UNLIMITED terminal tackle."""
    with pytest.raises(ValueError, match="needs a max, a min or `unlimited`"):
        GearClause(slot=Slot.terminal_attachments_per_line,
                   members=["hook", "artificial_lure", "artificial_fly"])
    ok = GearClause(slot=Slot.terminal_attachments_per_line, max=1,
                    members=["hook", "artificial_lure", "artificial_fly"])
    assert ok.max == 1


def test_a_hook_gap_smaller_than_the_book_prints_is_refused():
    """The synopsis states this in BOTH units — "gap not less than 3 cm" on a set line, "no hooks
    greater than 15 mm from point to shank" on a hook. One unit in the slot name is what stops
    them diverging, and the cost is that an unconverted 3 is a legal value meaning no constraint."""
    with pytest.raises(ValueError, match="confirm the unit"):
        GearClause(slot=Slot.hook_gap_mm, min=3)
    assert GearClause(slot=Slot.hook_gap_mm, min=30).min == 30


def test_a_carve_out_belongs_to_a_ban():
    """"parts of fin fish OTHER THAN ROE is prohibited" is one bound and one carve-out, and `gear`
    being a map means a ban and an allow cannot both sit on `bait` in one rule. Without `except`
    the exception produced NO FIELD and its whole force fell on one register line."""
    got = GearClause(slot=Slot.bait, ban=["fin_fish"], **{"except": ["roe"]})
    assert got.except_ == ["roe"]
    with pytest.raises(ValueError, match="carves members out of a ban"):
        GearClause(slot=Slot.bait, allow=["roe"], **{"except": ["x"]})


def test_a_device_is_not_a_way_of_fishing():
    """`downrigger` and `light` were members of `method`. The two-rules pattern is polarity-blind,
    so "angle with a downrigger, PROVIDED…" (an allowable list) and "Use a light… UNLESS…" (from
    a list headed "It is UNLAWFUL to") both produced `method: {allow: [...]}` — and a lake
    answered "what may I fish with here" with the one piece of tackle the province forbids."""
    assert "downrigger" not in {m.value for m in Method}
    assert "light" not in {m.value for m in Method}


def test_a_circumstance_must_name_a_means_or_a_spec_slot():
    """`while` draws from method members AND spec-slot names, because a spec slot's name IS a
    means token. That is what lets `while: ["downrigger"]` have a referent without the device
    being a way of fishing."""
    assert CatalogueRule(rule_id="a", **_METHOD_RULE, **{"while": ["downrigger"]}).while_
    assert CatalogueRule(rule_id="b", **_METHOD_RULE, **{"while": ["set_lining"]}).while_
    with pytest.raises(ValueError, match="not a means of fishing"):
        CatalogueRule(rule_id="c", **_METHOD_RULE, **{"while": ["trolling"]})


def test_an_exemption_must_say_when_it_lifts():
    """A lift with no circumstance applies everywhere and deletes the rule it narrows. Region 6's
    steelhead closure carried an exemption whose PLACE could not be drawn, was applied everywhere,
    and cost the Babine a season; "dead fin fish when set lining" applied everywhere took a
    water's bait tile from "banned" to "no rule at all"."""
    with pytest.raises(ValueError, match="must say WHERE or WHEN"):
        CatalogueRule(rule_id="d", **_BARE,
                      gear=[GearClause(slot=Slot.bait, ban=["fin_fish"])],
                      exempts=[{"target": "bait.r1"}])
    # a CIRCUMSTANCE scopes it…
    assert CatalogueRule(rule_id="e", **_BARE, **{"while": ["set_lining"]},
                         gear=[GearClause(slot=Slot.bait, ban=["fin_fish"])],
                         exempts=[{"target": "bait.r1"}])
    # …and so does a PLACE. `zp:set_lining.r1` permits set lining in the lakes of Region 6 and
    # 7A and lifts the province-wide ban; its extents are what narrow it, and it needs no
    # circumstance because it names where instead. The first draft of this guard demanded a
    # circumstance and fired on real data.
    assert CatalogueRule(rule_id="f2", **_BARE,
                         extents=[{"op": "within", "area_id": "area:region:6"}],
                         gear=[GearClause(slot=Slot.bait, ban=["fin_fish"])],
                         exempts=[{"target": "bait.r1"}])


def test_there_is_no_boolean_left_to_invert():
    """`submerged: Optional[bool]` was the last polarity flag — `false` reads as "a light is
    lawful only if it is NOT submerged", the inverse of the printed clause. It was evicted from
    `GearClause` and re-admitted one object down in `GearSpec`. A state the thing must be IN is a
    `must_be` token, where negation is unwriteable."""
    assert "submerged" not in GearSpec.model_fields
    assert GearClause(slot=Slot.light, must_be=["submerged", "attached_to_line"]).must_be


def test_an_aliased_field_survives_a_round_trip():
    """`while`, `except` and `with` are Python keywords, so the field is `while_` internally and
    `while` in JSON. `model_dump()` emits the python name and `extra="forbid"` then refuses it, so
    a rule carrying one ingested fine, wrote fine, and failed on being read back."""
    r = CatalogueRule(rule_id="f", **_METHOD_RULE, **{"while": ["set_lining"]})
    assert CatalogueRule.model_validate(r.model_dump()).while_ == ["set_lining"]
    assert CatalogueRule.model_validate(r.model_dump(by_alias=True)).while_ == ["set_lining"]


def test_a_conduct_act_carries_its_own_direction_and_must_be_registered():
    """The act token IS the direction — `do_not_waste_catch`, never `waste_catch` plus a flag.
    A `must`/`must_not` key beside it would be a second place to state polarity, and
    `must: "do_not_waste_catch"` a double negative that reads as law.

    The vocabulary is open by necessity — each edition can print a new duty — so it is held by a
    registry rather than a type, and an unregistered token is refused rather than absorbed. That
    is what stops it becoming `reason` under a new name."""
    ok = CatalogueRule(rule_id="g", type=RuleType.handling_rule, verbatim="Waste the fish.",
                       conduct=["do_not_waste_catch"])
    assert ok.conduct == ["do_not_waste_catch"]
    assert all(a.startswith(("do_not_", "no_")) or " " not in a for a in CONDUCT_ACTS)
    with pytest.raises(ValueError, match="not a registered act"):
        CatalogueRule(rule_id="h", type=RuleType.handling_rule, verbatim="v",
                      conduct=["waste_catch"])


def test_the_converted_provincial_rules_say_what_the_book_says():
    """The six conversions that were materially wrong before, read back off the entries."""
    import json
    from pipeline.common.curated import CURATED
    d = json.loads((CURATED.regulations.entries.catalogue / "region-provincial.json").read_text())
    by = {r["rule_id"]: r for e in d["entries"] for r in e.get("rules", [])}

    # was {barbless: true, required: false} — "barbed hooks are legal in every B.C. stream".
    # `only`, not `allow`: `barb` has two members and the law CLOSES the set — "Single barbless
    # hook must be used in all streams". `allow` renders "barbless may be used", which is the
    # permission reading of a prohibition and the very failure this field replaced.
    assert by["barbless_single_hook_streams.r1"]["gear"] == [
        {"slot": "barb", "only": ["barbless"]}]
    # was {required: false} and NOTHING else; the submersion and the 1 m were gone
    assert {c["slot"] for c in by["terminal_tackle.r5"]["gear"]} == {"light", "light_to_hook_mm"}
    # was hook_count: 1 — capped hooks and left a lure AND a fly addable
    assert by["terminal_tackle.r6"]["gear"][0]["slot"] == "terminal_attachments_per_line"
    # was {} — the quick-release proviso was its whole content and it was stored nowhere
    assert by["allowable_methods.r1"]["gear"] == [
        {"slot": "downrigger", "must_be": ["quick_release_to_line"]}]
    # was two rules with identical extents that nothing could rank
    lines = by["terminal_tackle.r1"]["gear"]
    assert [c["max"] for c in lines] == [2, 1], "narrow first, general last"
    assert lines[0]["when"] and "when" not in lines[1]
    # was {bait: roe, allowed: true} — a "you must not have more than 1 kg" stored as a permission
    assert by["bait.r6"]["gear"] == [{"slot": "bait_possession_kg", "of": ["roe"], "max": 1}]
    # "…or parts of fin fish OTHER THAN ROE is prohibited"
    assert by["bait.r1"]["gear"] == [
        {"slot": "bait", "ban": ["fin_fish"], "except": ["roe"]}]


def test_unlimited_is_a_bound_said_outright():
    """Kootenay Lake's "unlimited number of rods" was `max_lines: 0` — a zero standing for infinity —
    and converted to "no lines at all". `unlimited` says it; it is refused wherever it is not a
    lifted ceiling on a count."""
    ok = GearClause(slot=Slot.lines_per_angler, unlimited=True)
    assert ok.max is None and ok.model_dump(mode="json") == {"slot": "lines_per_angler",
                                                              "unlimited": True}
    for bad in (dict(slot=Slot.lines_per_angler, unlimited=True, max=2),
                dict(slot=Slot.hook_gap_mm, unlimited=True),
                dict(slot=Slot.bait, unlimited=True, ban=["any_bait"])):
        with pytest.raises(ValueError):
            GearClause(**bad)
    import json
    from pipeline.common.curated import CURATED
    d = json.loads((CURATED.regulations.entries.catalogue / "region-4.json").read_text())
    x = next(r for e in d["entries"] for r in e.get("rules", [])
             if r["rule_id"] == "kootenay_lake_main_body.r1")
    assert CatalogueRule.model_validate(x).gear[0].unlimited
