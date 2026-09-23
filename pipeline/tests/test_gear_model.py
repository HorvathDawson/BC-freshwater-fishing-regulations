"""The gear model, pinned by the records it must REFUSE.

Every case here validated cleanly until 2026-09-23. They were found by constructing wrong records
against the validators rather than by reading them, which is the only way this class of hole shows
itself: each one is a plausible thing a curator would write, and each one states something other
than what the book says.
"""
import pytest

from pipeline.regs.parsing.catalogue import (CatalogueRule, Conduct, GearClause, GearSpec,
                                             GearWhen, Method, RuleType, Slot)

_METHOD_RULE = dict(type=RuleType.method_rule, verbatim="v",
                    method=Method.set_lining, permitted=True)


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
    with pytest.raises(ValueError, match="needs a max or a min"):
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
    with pytest.raises(ValueError, match="must say WHEN it lifts"):
        CatalogueRule(rule_id="d", **_METHOD_RULE,
                      gear=[GearClause(slot=Slot.bait, ban=["fin_fish"])],
                      exempts=[{"target": "bait.r1"}])
    assert CatalogueRule(rule_id="e", **_METHOD_RULE, **{"while": ["set_lining"]},
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


def test_a_conduct_act_runs_one_way():
    with pytest.raises(ValueError, match="exactly one"):
        Conduct(must="a", must_not="b")
    with pytest.raises(ValueError, match="exactly one"):
        Conduct()
