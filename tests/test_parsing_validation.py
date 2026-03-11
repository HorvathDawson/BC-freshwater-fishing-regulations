"""Tests for synopsis parsing model validation — completeness checks.

Covers:
  1. Restriction-details-in-rule-text check (catches compound (a)/(b) splits)
  2. Keyword coverage check (catches missing classified water, etc.)
"""

import pytest
from synopsis_pipeline.models import (
    RuleGroup,
    ParsedWaterbody,
    ScopeObject,
    RestrictionObject,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_scope(**overrides):
    defaults = dict(
        type="WHOLE_SYSTEM",
        lookup_name="ALL",
        location_verbatim=None,
        landmark_verbatim=None,
        landmark_end_verbatim=None,
        direction=None,
        includes_tributaries=None,
    )
    defaults.update(overrides)
    return defaults


def _make_rule(rule_text, restriction_type, details, dates=None, **scope_kw):
    return {
        "rule_text_verbatim": rule_text,
        "scope": _make_scope(**scope_kw),
        "restriction": {"type": restriction_type, "details": details, "dates": dates},
    }


def _make_waterbody(regs_verbatim, rules, name="TEST RIVER"):
    return {
        "identity": {
            "name_verbatim": name,
            "lookup_name": name,
            "identity_type": "STREAM",
            "component_waterbodies": [],
            "alternate_names": [],
            "location_descriptor": None,
            "notes": None,
            "global_scope": _make_scope(),
            "exclusions": [],
            "inclusions": [],
        },
        "regs_verbatim": regs_verbatim,
        "audit_log": [],
        "rules": rules,
    }


# ===================================================================
#  1. Restriction details in rule_text_verbatim (RuleGroup-level)
# ===================================================================


class TestRestrictionDetailsInRuleText:
    """restriction.details core must appear in rule_text_verbatim."""

    def test_matching_details_passes(self):
        """Normal case — 'Fly fishing only' appears in the rule_text."""
        rule = RuleGroup.from_dict(
            _make_rule(
                rule_text="Fly fishing only from** (a)** Anahim Lake to Iltasyuko River, June 15- Mar 31, and** (b)** Crag Creek to fishing boundary signs approximately 500 m\nupstream of canyon, July 15-Sept 30",
                restriction_type="gear_restriction",
                details="Fly fishing only",
                dates=["June 15- Mar 31"],
            )
        )
        errors = rule.validate(parent_regs_verbatim=rule.rule_text_verbatim)
        detail_errors = [e for e in errors if "restriction.details core" in e]
        assert detail_errors == []

    def test_compound_split_fails(self):
        """Compound (a)/(b) fragment missing the restriction keyword."""
        rule = RuleGroup.from_dict(
            _make_rule(
                rule_text="Crag Creek to fishing boundary signs approximately 500 m\nupstream of canyon, July 15-Sept 30",
                restriction_type="gear_restriction",
                details="Fly fishing only",
                dates=["July 15-Sept 30"],
            )
        )
        parent = (
            "Fly fishing only from** (a)** Anahim Lake to Iltasyuko River, June 15- Mar 31, "
            "and** (b)** Crag Creek to fishing boundary signs approximately 500 m\nupstream of canyon, July 15-Sept 30"
        )
        errors = rule.validate(parent_regs_verbatim=parent)
        detail_errors = [e for e in errors if "restriction.details core" in e]
        assert len(detail_errors) == 1
        assert "Fly fishing only" in detail_errors[0]

    def test_bait_ban_matches(self):
        rule = RuleGroup.from_dict(
            _make_rule(
                rule_text="Bait ban all parts[Includes Tributaries]",
                restriction_type="gear_restriction",
                details="Bait ban",
            )
        )
        errors = rule.validate(parent_regs_verbatim=rule.rule_text_verbatim)
        detail_errors = [e for e in errors if "restriction.details core" in e]
        assert detail_errors == []

    def test_no_fishing_matches(self):
        rule = RuleGroup.from_dict(
            _make_rule(
                rule_text="**No Fishing** upstream of Iltasyuko River, Apr 1-June 14",
                restriction_type="closure",
                details="No Fishing",
                dates=["Apr 1-June 14"],
            )
        )
        errors = rule.validate(parent_regs_verbatim=rule.rule_text_verbatim)
        detail_errors = [e for e in errors if "restriction.details core" in e]
        assert detail_errors == []

    def test_note_type_skipped(self):
        """Note-type restrictions are exempt from the check."""
        rule = RuleGroup.from_dict(
            _make_rule(
                rule_text='NOTE: "canyon" means lower canyon 3-5 km from river\'s mouth',
                restriction_type="note",
                details="canyon means lower canyon 3-5 km from river's mouth",
            )
        )
        errors = rule.validate(parent_regs_verbatim=rule.rule_text_verbatim)
        detail_errors = [e for e in errors if "restriction.details core" in e]
        assert detail_errors == []

    def test_licensing_with_parenthetical(self):
        """Classified water details like 'Class II water (notes...)' — core is before '('."""
        rule = RuleGroup.from_dict(
            _make_rule(
                rule_text="**From Anahim Lake to Iltasyuko River**[Includes Tributaries]**: Class II water June 16-Oct 31 (see map below); Steelhead Stamp not required**",
                restriction_type="licensing",
                details="Class II water (see map below)",
                dates=["June 16-Oct 31"],
            )
        )
        errors = rule.validate(parent_regs_verbatim=rule.rule_text_verbatim)
        detail_errors = [e for e in errors if "restriction.details core" in e]
        assert detail_errors == []

    def test_licensing_missing_from_fragment(self):
        """Fragment that doesn't contain the restriction keyword."""
        rule = RuleGroup.from_dict(
            _make_rule(
                rule_text="see map below; Steelhead Stamp not required",
                restriction_type="licensing",
                details="Class II water",
                dates=["June 16-Oct 31"],
            )
        )
        parent = "**From Anahim Lake to Iltasyuko River**: Class II water June 16-Oct 31 (see map below); Steelhead Stamp not required"
        errors = rule.validate(parent_regs_verbatim=parent)
        detail_errors = [e for e in errors if "restriction.details core" in e]
        assert len(detail_errors) == 1


# ===================================================================
#  2. Keyword coverage (ParsedWaterbody-level)
# ===================================================================

DEAN_RIVER_REGS = (
    "**No Fishing** upstream of Iltasyuko River[Includes Tributaries], including upstream of Anahim Lake[Includes Tributaries], Apr 1-June 14\n"
    "**No Fishing** from Iltasyuko River to Crag Creek[Includes Tributaries]\n"
    "**No Fishing** from Crag Creek to fishing boundary signs approximately 500 m upstream of canyon[Includes Tributaries], Oct 1-May 31\n"
    "**No Fishing** from fishing boundary signs approximately 500 m upstream of canyon to signs 100 m downstream of canyon\n"
    "**No Fishing** from fishing boundary signs approximately 100 m downstream of canyon to tidal boundary, Oct 1 - May 31\n"
    "Trout/char daily quota = 1 (none under 35 cm)\n"
    "Bait ban all parts[Includes Tributaries]\n"
    "Fly fishing only from** (a)** Anahim Lake to Iltasyuko River, June 15- Mar 31, and** (b)** Crag Creek to fishing boundary signs approximately 500 m\n"
    "upstream of canyon, July 15-Sept 30\n"
    "No powered boats between signs 0.5 km and 3.5 km upstream of canyon, Aug 1-Aug 31\n"
    "**From Crag Creek to signs 500 m upstream of the canyon**[Includes Tributaries]**: Class I water June 1-Sept 30 and Steelhead Stamp mandatory June 1-Sept 30; see map below (Non-residents see notice on **page 42**)\n"
    "From signs 100 m downstream of canyon to tidal boundary**[Includes Tributaries]**: Class I water June 1-Sept 30 and Steelhead Stamp mandatory June 1-Sept 30: see map below\n"
    "From Anahim Lake to Iltasyuko River**[Includes Tributaries]**: Class II water June 16-Oct 31 (see map below); Steelhead Stamp not required**\n"
    'NOTE: "canyon" means lower canyon 3-5 km from river\'s mouth'
)


class TestKeywordCoverage:
    """regs_verbatim keywords must be covered by at least one rule."""

    def _rules_covering_closures_and_basics(self):
        """Rules that cover closures, harvest, gear, boats — but NOT classified water."""
        return [
            _make_rule(
                "**No Fishing** upstream of Iltasyuko River[Includes Tributaries], including upstream of Anahim Lake[Includes Tributaries], Apr 1-June 14",
                "closure",
                "No Fishing",
                ["Apr 1-June 14"],
            ),
            _make_rule(
                "**No Fishing** from Iltasyuko River to Crag Creek[Includes Tributaries]",
                "closure",
                "No Fishing",
            ),
            _make_rule(
                "**No Fishing** from Crag Creek to fishing boundary signs approximately 500 m upstream of canyon[Includes Tributaries], Oct 1-May 31",
                "closure",
                "No Fishing",
                ["Oct 1-May 31"],
            ),
            _make_rule(
                "**No Fishing** from fishing boundary signs approximately 500 m upstream of canyon to signs 100 m downstream of canyon",
                "closure",
                "No Fishing",
            ),
            _make_rule(
                "**No Fishing** from fishing boundary signs approximately 100 m downstream of canyon to tidal boundary, Oct 1 - May 31",
                "closure",
                "No Fishing",
                ["Oct 1 - May 31"],
            ),
            _make_rule(
                "Trout/char daily quota = 1 (none under 35 cm)",
                "harvest",
                "Trout/char daily quota = 1 (none under 35 cm)",
            ),
            _make_rule(
                "Bait ban all parts[Includes Tributaries]",
                "gear_restriction",
                "Bait ban",
            ),
            _make_rule(
                "Fly fishing only from** (a)** Anahim Lake to Iltasyuko River, June 15- Mar 31, and** (b)** Crag Creek to fishing boundary signs approximately 500 m\nupstream of canyon, July 15-Sept 30",
                "gear_restriction",
                "Fly fishing only",
                ["June 15- Mar 31"],
            ),
            _make_rule(
                "Fly fishing only from** (a)** Anahim Lake to Iltasyuko River, June 15- Mar 31, and** (b)** Crag Creek to fishing boundary signs approximately 500 m\nupstream of canyon, July 15-Sept 30",
                "gear_restriction",
                "Fly fishing only",
                ["July 15-Sept 30"],
            ),
            _make_rule(
                "No powered boats between signs 0.5 km and 3.5 km upstream of canyon, Aug 1-Aug 31",
                "vessel_restriction",
                "No powered boats",
                ["Aug 1-Aug 31"],
            ),
            _make_rule(
                'NOTE: "canyon" means lower canyon 3-5 km from river\'s mouth',
                "note",
                "canyon means lower canyon 3-5 km from river's mouth",
            ),
        ]

    def test_missing_classified_water_flagged(self):
        """Dean River with no classified water rules → validation flags missing keywords."""
        rules = self._rules_covering_closures_and_basics()
        wb = ParsedWaterbody.from_dict(
            _make_waterbody(DEAN_RIVER_REGS, rules, "DEAN RIVER")
        )
        errors = wb.validate(
            expected_name="DEAN RIVER", expected_raw_text=DEAN_RIVER_REGS
        )
        kw_errors = [e for e in errors if "Regulation keyword" in e]
        # Should flag class i water, class ii water, and steelhead stamp mandatory
        flagged_keywords = {e.split("'")[1] for e in kw_errors}
        assert "class i water" in flagged_keywords
        assert "class ii water" in flagged_keywords
        assert "steelhead stamp mandatory" in flagged_keywords

    def test_full_coverage_passes(self):
        """All keywords covered → no coverage errors."""
        rules = self._rules_covering_closures_and_basics()
        # Add the missing classified water rules
        rules.extend(
            [
                _make_rule(
                    "**From Crag Creek to signs 500 m upstream of the canyon**[Includes Tributaries]**: Class I water June 1-Sept 30 and Steelhead Stamp mandatory June 1-Sept 30; see map below (Non-residents see notice on **page 42**)",
                    "licensing",
                    "Class I water",
                    ["June 1-Sept 30"],
                ),
                _make_rule(
                    "From signs 100 m downstream of canyon to tidal boundary**[Includes Tributaries]**: Class I water June 1-Sept 30 and Steelhead Stamp mandatory June 1-Sept 30: see map below",
                    "licensing",
                    "Class I water",
                    ["June 1-Sept 30"],
                ),
                _make_rule(
                    "From Anahim Lake to Iltasyuko River**[Includes Tributaries]**: Class II water June 16-Oct 31 (see map below); Steelhead Stamp not required**",
                    "licensing",
                    "Class II water",
                    ["June 16-Oct 31"],
                ),
            ]
        )
        wb = ParsedWaterbody.from_dict(
            _make_waterbody(DEAN_RIVER_REGS, rules, "DEAN RIVER")
        )
        errors = wb.validate(
            expected_name="DEAN RIVER", expected_raw_text=DEAN_RIVER_REGS
        )
        kw_errors = [e for e in errors if "Regulation keyword" in e]
        assert kw_errors == []

    def test_simple_waterbody_no_false_positives(self):
        """Simple regs text with no special keywords → no coverage errors."""
        regs = "Trout/char daily quota = 2 (none over 50 cm)"
        rules = [
            _make_rule(regs, "harvest", "Trout/char daily quota = 2 (none over 50 cm)")
        ]
        wb = ParsedWaterbody.from_dict(_make_waterbody(regs, rules, "SIMPLE LAKE"))
        errors = wb.validate(expected_name="SIMPLE LAKE", expected_raw_text=regs)
        kw_errors = [e for e in errors if "Regulation keyword" in e]
        assert kw_errors == []

    def test_bait_ban_missing_flagged(self):
        """regs_verbatim has 'bait ban' but no rule covers it."""
        regs = "Trout/char daily quota = 1\nBait ban"
        rules = [
            _make_rule(
                "Trout/char daily quota = 1", "harvest", "Trout/char daily quota = 1"
            )
        ]
        wb = ParsedWaterbody.from_dict(_make_waterbody(regs, rules, "TEST CREEK"))
        errors = wb.validate(expected_name="TEST CREEK", expected_raw_text=regs)
        kw_errors = [e for e in errors if "Regulation keyword" in e]
        assert len(kw_errors) == 1
        assert "bait ban" in kw_errors[0]

    def test_compound_sentence_correct_verbatim_passes(self):
        """Compound (a)/(b) using ENTIRE sentence for both rules → passes both checks."""
        sentence = "Fly fishing only from** (a)** A to B, June 15-Mar 31, and** (b)** C to D, July 15-Sept 30"
        regs = sentence
        rules = [
            _make_rule(
                sentence, "gear_restriction", "Fly fishing only", ["June 15-Mar 31"]
            ),
            _make_rule(
                sentence, "gear_restriction", "Fly fishing only", ["July 15-Sept 30"]
            ),
        ]
        wb = ParsedWaterbody.from_dict(_make_waterbody(regs, rules, "TEST RIVER"))
        errors = wb.validate(expected_name="TEST RIVER", expected_raw_text=regs)
        detail_errors = [e for e in errors if "restriction.details core" in e]
        kw_errors = [e for e in errors if "Regulation keyword" in e]
        assert detail_errors == []
        assert kw_errors == []
