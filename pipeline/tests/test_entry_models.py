"""Contract tests for the rebuilt-parser output models (entry_models.py)."""

import pytest
from pydantic import ValidationError

from pipeline.parsing.entry_models import (
    Entry, EntryFile, Extent, Identity, Op, Rule, Tributaries,
    unused_splits, validate_entry_splits,
)


# --- Extent arity ---------------------------------------------------------

def test_extent_arity():
    assert Extent(op=Op.WHOLE).splits == []
    assert len(Extent(op=Op.UPSTREAM_OF, splits=["falls"]).splits) == 1
    assert len(Extent(op=Op.BETWEEN, splits=["a", "b"]).splits) == 2
    assert Extent(op=Op.WITHIN, area_id="GARIBALDI PARK").area_id == "GARIBALDI PARK"
    assert Extent(op=Op.WITHIN, area_id="area:park:wells_gray", feature_types=["lake", "wetland"]).feature_types == ["lake", "wetland"]
    with pytest.raises(ValidationError):
        Extent(op=Op.WITHIN, area_id="x", feature_types=["fish"])       # invalid feature kind
    with pytest.raises(ValidationError):
        Extent(op=Op.WHOLE, feature_types=["lake"])                  # feature_types only for within
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
    assert e.locked is False                      # fresh parse; a curator flips this True to freeze the entry
    assert _entry(locked=True).locked is True

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


# --- display_location / unresolved_locators / species (parser failure surface) ---

def test_display_location_not_verbatim_constrained():
    # display_location is user-facing + curator-editable — free text, unlike location_text
    r = _rule(display_location="Above the Talchako confluence", location_text="")
    assert r.display_location == "Above the Talchako confluence"

def test_display_location_kept_while_whole_reach():
    # the curation escape hatch: readable label preserved, rule falls back to the whole stream
    r = _rule(extents=[Extent(op=Op.WHOLE)], location_text="",
              display_location="500 m below the falls (locator not found)")
    assert r.extents[0].op == Op.WHOLE and r.display_location.startswith("500 m")

def test_unresolved_locators_force_review():
    # an unbound locator with needs_review=False is rejected — cannot hide as a confident parse
    with pytest.raises(ValidationError):
        _rule(extents=[Extent(op=Op.WHOLE)], location_text="",
              unresolved_locators=["signs 500 m below the outlet"])
    # allowed when routed to review
    r = Rule(rule_id="e.r1", restriction_type="closure", details="No fishing",
             rule_text="No fishing above the outlet.", extents=[], needs_review=True,
             review_reason="outlet locator has no curated split",
             unresolved_locators=["above the outlet"])
    assert r.unresolved_locators == ["above the outlet"]

def test_species_codes_validated():
    r = _rule(species=["ST", "BT"])
    assert r.species == ["ST", "BT"]
    with pytest.raises(ValidationError):
        _rule(species=["NOTAFISH"])

def test_rule_dates_parse_to_windows():
    from pipeline.parsing.dates import DateWindow
    r = _rule(location_text="upstream of Hunlen Falls", dates=["Apr 1 - Jun 30"])
    assert r.date_windows() == [DateWindow(4, 1, 6, 30)]

def test_rule_rejects_hallucinated_date():
    # date is a verbatim substring of rule_text but not a real calendar date -> rejected
    with pytest.raises(ValidationError):
        _rule(rule_text="No fishing from Jun 31 onward.", location_text="", dates=["Jun 31"],
              extents=[Extent(op=Op.WHOLE)])


def test_unused_splits_coverage_advisory():
    e = _entry()   # rules reference only 'hunlen_falls'
    assert unused_splits(e, {"hunlen_falls"}) == []
    assert unused_splits(e, {"hunlen_falls", "goat_confluence"}) == ["goat_confluence"]


def test_excludes_hand_curated_trib_carveout():
    # a curator subtracts 'Burnt Bridge upstream of Sitkatapa' from the inherited trib set;
    # the excepted item is inferred from the split's own scope, so no `item` is needed.
    e = _entry(tributaries=Tributaries(
        included=True, excludes=[Extent(op=Op.UPSTREAM_OF, splits=["sitkatapa_creek_confluence"])]))
    assert e.tributaries.excludes[0].splits == ["sitkatapa_creek_confluence"]
    assert validate_entry_splits(e, {"hunlen_falls", "sitkatapa_creek_confluence"}) == []
    errs = validate_entry_splits(e, {"hunlen_falls"})   # exclude's split not in allowed set
    assert errs and "sitkatapa_creek_confluence" in errs[0]


def test_rule_tributary_excludes_defaults_empty_and_validates():
    # additive per-rule carve-out (e.g. 'No Fishing in any tributaries except Quinsam River');
    # defaults to [] so existing parses need no reparse, and its split ids are checked.
    assert _rule().tributary_excludes == []
    e = _entry(
        regs_verbatim="No fishing upstream of Hunlen Falls",
        tributaries=Tributaries(included=True),
        rules=[Rule(
            rule_id="atnarko_main.r1", restriction_type="closure", details="No fishing",
            rule_text="No fishing upstream of Hunlen Falls", extents=[Extent(op=Op.WHOLE)],
            includes_tributaries=True,
            tributary_excludes=[Extent(op=Op.UPSTREAM_OF, splits=["sitkatapa_creek_confluence"])])],
    )
    assert e.rules[0].tributary_excludes[0].splits == ["sitkatapa_creek_confluence"]
    assert validate_entry_splits(e, {"hunlen_falls", "sitkatapa_creek_confluence"}) == []
    errs = validate_entry_splits(e, {"hunlen_falls"})   # rule exclude's split not allowed
    assert errs and "tributary_excludes" in errs[0] and "sitkatapa_creek_confluence" in errs[0]

