"""Contract tests for the rebuilt-parser output models (entry_models.py)."""

import pytest
from pydantic import ValidationError

from pipeline.parsing.entry_models import (
    Entry, EntryFile, Extent, Identity, Op, Rule, Tributaries, validate_entry_splits,
)


# --- Extent arity ---------------------------------------------------------

def test_extent_arity():
    assert Extent(op=Op.WHOLE).splits == []
    assert len(Extent(op=Op.UPSTREAM_OF, splits=["falls"]).splits) == 1
    assert len(Extent(op=Op.BETWEEN, splits=["a", "b"]).splits) == 2
    assert Extent(op=Op.WITHIN, area="GARIBALDI PARK").area == "GARIBALDI PARK"
    for bad in (
        dict(op=Op.UPSTREAM_OF, splits=[]),          # needs 1
        dict(op=Op.UPSTREAM_OF, splits=["a", "b"]),  # too many
        dict(op=Op.BETWEEN, splits=["a"]),           # needs 2
        dict(op=Op.WHOLE, splits=["a"]),             # takes none
        dict(op=Op.WITHIN),                          # needs area or splits
    ):
        with pytest.raises(ValidationError):
            Extent(**bad)


# --- Rule verbatim chain --------------------------------------------------

def _rule(**kw):
    base = dict(rule_id="e.r1", restriction_type="closure", details="No fishing",
                rule_text="No fishing upstream of Hunlen Falls from Apr 1 - Jun 30.",
                extents=[Extent(op=Op.UPSTREAM_OF, splits=["hunlen_falls"])])
    base.update(kw)
    return Rule(**base)


def test_rule_valid_and_chain():
    r = _rule(location_text="upstream of Hunlen Falls", dates=["Apr 1 - Jun 30"])
    assert r.extents[0].op == Op.UPSTREAM_OF

def test_rule_location_text_must_be_substring():
    with pytest.raises(ValidationError):
        _rule(location_text="downstream of Hunlen Falls")   # not in rule_text

def test_rule_date_must_be_substring():
    with pytest.raises(ValidationError):
        _rule(dates=["Jan 1 - Feb 2"])

def test_rule_no_ellipsis_in_verbatim():
    with pytest.raises(ValidationError):
        _rule(rule_text="No fishing ... Hunlen Falls")

def test_rule_needs_review_requires_reason():
    # needs_review with no extents is allowed, but requires a reason
    Rule(rule_id="e.r1", restriction_type="note", details="unclear", rule_text="the 2nd bridge",
         extents=[], needs_review=True, review_reason="no split matches 'the 2nd bridge'")
    with pytest.raises(ValidationError):
        Rule(rule_id="e.r1", restriction_type="note", details="unclear", rule_text="the 2nd bridge",
             extents=[], needs_review=True)

def test_rule_confident_needs_binding():
    with pytest.raises(ValidationError):   # no extents, no override, not needs_review
        Rule(rule_id="e.r1", restriction_type="closure", details="x", rule_text="No fishing here", extents=[])

def test_rule_whole_scope_is_explicit():
    r = _rule(extents=[Extent(op=Op.WHOLE)], location_text="")
    assert r.extents[0].op == Op.WHOLE


# --- Entry ----------------------------------------------------------------

def _entry(**kw):
    base = dict(
        entry_id="atnarko_main",
        identity=Identity(name="Atnarko River", region="5", mus=["5-4"]),
        regs_verbatim="No fishing upstream of Hunlen Falls from Apr 1 - Jun 30. No powered boats.",
        tributaries=Tributaries(included=True),
        rules=[
            _rule(location_text="upstream of Hunlen Falls", dates=["Apr 1 - Jun 30"]),
            Rule(rule_id="atnarko_main.r2", restriction_type="vessel_restriction", details="No powered boats",
                 rule_text="No powered boats.", extents=[Extent(op=Op.WHOLE)], includes_tributaries=False),
        ],
    )
    base.update(kw)
    return Entry(**base)


def test_entry_valid():
    e = _entry()
    assert e.entry_id == "atnarko_main" and len(e.rules) == 2 and e.matched == []
    assert e.tributaries.included is True

def test_entry_rule_text_must_be_in_regs():
    with pytest.raises(ValidationError):
        _entry(rules=[_rule(rule_text="No fishing near the moon.", location_text="")])

def test_entry_duplicate_rule_ids():
    with pytest.raises(ValidationError):
        _entry(rules=[_rule(), _rule()])   # both rule_id 'e.r1'

def test_entry_keyword_coverage():
    # regs mentions 'no powered boats' but no rule captures it
    with pytest.raises(ValidationError):
        _entry(rules=[_rule(location_text="upstream of Hunlen Falls", dates=["Apr 1 - Jun 30"])])

def test_tributary_only_implies_included():
    e = _entry(tributaries=Tributaries(only=True, included=False))
    assert e.tributaries.included is True


# --- EntryFile + split-id validation --------------------------------------

def test_entryfile_unique_entry_ids():
    with pytest.raises(ValidationError):
        EntryFile(region="5", entries=[_entry(), _entry()])

def test_validate_entry_splits():
    e = _entry()
    assert validate_entry_splits(e, {"hunlen_falls"}) == []
    errs = validate_entry_splits(e, {"some_other_split"})
    assert errs and "hunlen_falls" in errs[0]


def test_excludes_hand_curated_trib_carveout():
    # a curator subtracts 'Burnt Bridge upstream of Sitkatapa' from the inherited trib set;
    # the excepted item is inferred from the split's own scope, so no `item` is needed.
    e = _entry(tributaries=Tributaries(
        included=True, excludes=[Extent(op=Op.UPSTREAM_OF, splits=["sitkatapa_creek_confluence"])]))
    assert e.tributaries.excludes[0].splits == ["sitkatapa_creek_confluence"]
    assert validate_entry_splits(e, {"hunlen_falls", "sitkatapa_creek_confluence"}) == []
    errs = validate_entry_splits(e, {"hunlen_falls"})   # exclude's split not in allowed set
    assert errs and "sitkatapa_creek_confluence" in errs[0]
