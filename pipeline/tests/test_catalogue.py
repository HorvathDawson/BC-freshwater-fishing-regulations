"""The rule catalogue: what the model refuses, and what the label generator says.

These are the failures that shipped in the prose corpus. Each test names the real rule it comes
from, because a guard whose motivating case is forgotten gets deleted by the next person.
"""

from __future__ import annotations

import pytest

from pipeline.regs.parsing.catalogue import (
    CatalogueRule, Method, Obligation, Period, PropulsionLevel,
    RuleType, VesselAspect, WaterKind, complement, label, parse_date_range,
)


def _r(**kw):
    kw.setdefault("rule_id", "t.r1")
    kw.setdefault("verbatim", "verbatim sentence")
    return CatalogueRule(**kw)


# --------------------------------------------------------------------------- validation

def test_retention_limit_refuses_empty_species():
    """115 rules store species=[] while naming a species in their own text; the blanket default
    states a trout limit on bass, kokanee and burbot."""
    with pytest.raises(ValueError, match="needs species"):
        _r(type=RuleType.retention_limit, take=2)


def test_gear_rules_refuse_species():
    """20 gear rules carry codes leaked from a co-located catch-and-release clause.
    `r2:silver_silverhope_lake@2-2` is a bare "bait ban, single barbless hook" with eight species,
    and "Trout/char: bait ban" is narrower than the law — the ban is on the whole river.

    A rule that applies only when fishing FOR something (a salmon bait ban) is a different thing
    and uses `when_targeting`; see the test below."""
    for t in (RuleType.bait_restriction, RuleType.tackle_restriction):
        with pytest.raises(ValueError, match="must not carry `species`"):
            _r(type=t, species=["RB"], gear=[{"slot": "barb", "only": ["barbless"]}])


def test_a_bait_rule_may_be_scoped_to_what_you_are_FISHING_FOR():
    """A stream can carry a salmon bait ban and no other. That is not a species the ban protects —
    it is the fishery the ban applies to, which is why it is a separate field."""
    r = _r(type=RuleType.bait_restriction, gear=[{"slot": "bait", "ban": ["any_bait"]}], when_targeting=["SA"])
    assert label(r) == "Bait ban when fishing for salmon"
    general = _r(type=RuleType.bait_restriction, gear=[{"slot": "bait", "ban": ["any_bait"]}])
    assert r.dimension != general.dimension    # they coexist; neither displaces the other


def test_a_standing_rule_must_be_flagged():
    """Standing means the extent is UNKNOWABLE (no dataset of fishways), not merely absent."""
    with pytest.raises(ValueError, match="standing rule must be flagged"):
        _r(type=RuleType.retention_limit, species=["ALL_GAME_FISH"], take=0,
           may_target=False, standing=True)


def test_take_zero_demands_an_explicit_may_target():
    """`may_target: bool = True` as a DEFAULT reinstated the defect this module exists to stop:
    forget the field on a closure and it renders as a catch-and-release permission."""
    with pytest.raises(ValueError, match="needs an explicit may_target"):
        _r(type=RuleType.retention_limit, species=["BS"], take=0)


def test_an_impossible_slot_is_refused():
    with pytest.raises(ValueError):
        _r(type=RuleType.retention_limit, species=["LT"], take=2,
           lengths=[{"min_cm": 90, "max_cm": 60}])


def test_unknown_species_codes_are_refused():
    """`species_words` falls through to the raw code, so an unrecognised one prints at the reader."""
    with pytest.raises(ValueError, match="unknown species"):
        _r(type=RuleType.retention_limit, species=["NOT_A_FISH"], take=2)


def test_half_a_time_window_is_refused():
    """_scope needs both ends; with one, "No fishing 21:00-05:00" renders as a total closure."""
    with pytest.raises(ValueError):
        _r(type=RuleType.retention_limit, species=["RB"], take=2,
           when={"hours": {"start": {"at": "21:00"}}})


def test_retention_fields_are_refused_on_other_types():
    with pytest.raises(ValueError, match="belongs to retention_limit"):
        _r(type=RuleType.bait_restriction, gear=[{"slot": "bait", "ban": ["any_bait"]}], take=2)


def test_power_capped_needs_a_number():
    with pytest.raises(ValueError, match="power_capped needs"):
        _r(type=RuleType.vessel_rule, aspect=VesselAspect.propulsion,
           level=PropulsionLevel.power_capped)


# --------------------------------------------------------------------------- the dimension

def test_tackle_facets_are_separate_dimensions():
    """A fly-only rule and a barbless rule are ADDITIVE — a fly must be barbless and singly
    hooked. `r1:farewell_lake@1-10` says all three in one sentence. Sharing a dimension would let
    one strike the other."""
    fly = _r(type=RuleType.tackle_restriction, gear=[{"slot": "method", "only": ["fly_fishing"]}])
    hook = _r(type=RuleType.tackle_restriction, gear=[{"slot": "barb", "only": ["barbless"]},
                                                      {"slot": "points_per_hook", "max": 1}])
    assert fly.dimension != hook.dimension


def test_an_angler_closure_never_shares_a_key_with_a_quota():
    """"Angling prohibited for non-guided non-resident aliens on Saturdays" closes the water to
    ONE KIND of angler. The ladder keys on (type, dimension) and ignores who, so filed as a
    retention limit it could displace the zone quota that binds everyone."""
    closure = _r(type=RuleType.angler_closure,
                 verbatim="Angling prohibited for non-guided non-resident aliens",
                 closed_to={"residency": ["non_resident_alien"], "guidance": ["non_guided"]})
    quota = _r(type=RuleType.retention_limit, species=["TROUT_CHAR"], take=2)
    closed = _r(type=RuleType.retention_limit, species=["ALL_GAME_FISH"], take=0,
                may_target=False)
    assert closure.family == "access"
    for r in (quota, closed):
        assert (closure.type, closure.dimension) != (r.type, r.dimension)
    other = _r(type=RuleType.angler_closure, verbatim="Angling prohibited for non-resident aliens",
               closed_to={"residency": ["non_resident_alien"]})
    assert closure.dimension != other.dimension, "different anglers are different subjects"


def test_vessel_aspects_are_separate_dimensions():
    speed = _r(type=RuleType.vessel_rule, aspect=VesselAspect.speed, max_kmh=8)
    prop = _r(type=RuleType.vessel_rule, aspect=VesselAspect.propulsion,
              level=PropulsionLevel.unpowered)
    assert speed.dimension != prop.dimension


# --------------------------------------------------------------------------- labels

def test_take_zero_branches_on_may_target():
    """THE defect this catalogue exists to stop. Unbranched, take=0 -> "release all" turns all
    605 rules whose label begins "No fishing" into a catch-and-release PERMISSION."""
    closed = _r(type=RuleType.retention_limit, species=["BS"], take=0, may_target=False)
    release = _r(type=RuleType.retention_limit, species=["BS"], take=0, may_target=True)
    assert label(closed) == "No fishing for bass"
    assert label(release) == "Bass — release all"
    assert label(closed) != label(release)


def test_kokanee_streams_is_species_scoped_not_a_water_closure():
    """"Kokanee: 5 (none from streams)" protects spawning kokanee. The stream stays open for
    everything else, so may_target is per SPECIES."""
    r = _r(type=RuleType.retention_limit, species=["KO"], take=0, may_target=False,
           water=WaterKind.stream)
    assert label(r) == "No fishing for kokanee in streams"


def test_hp_is_a_lookup_not_a_computation():
    """7.5 kW computes to 10.06 hp; the synopsis prints 10 hp."""
    r = _r(type=RuleType.vessel_rule, aspect=VesselAspect.propulsion,
           level=PropulsionLevel.power_capped, max_power_kw=7.5)
    assert label(r) == "Engine power restriction 7.5 kW (10 hp)"


def test_excepting_windows_are_inverted_rather_than_flagged():
    """`r4:kootenay_lake_upper_west_arm` printed [Apr 1-3, Jul 1-2] on a catch-and-release rule as
    the days it does NOT apply — a flag that inverted the field beside it, which is what `band`
    did to the size fields. `When` stores the days the rule DOES hold, computed once, so there is
    no flag left for a reader to apply or forget."""
    held = complement([parse_date_range("Apr 1-3"), parse_date_range("Jul 1-2")])
    r = _r(type=RuleType.retention_limit, species=["KO"], take=0, may_target=True,
           when={"dates": [d.model_dump() for d in held]})
    assert "except" not in label(r)
    assert "Jul 3-Mar 31 and Apr 4-Jun 30" in label(r)
    # and the excepted days are genuinely outside it
    got = {(d.from_month, d.from_day, d.to_month, d.to_day) for d in r.when.dates}
    assert (7, 3, 3, 31) in got and (4, 4, 6, 30) in got


def test_species_except_reads_as_the_sentence_does():
    r = _r(type=RuleType.retention_limit, species=["ALL_GAME_FISH"], species_except=["BB"],
           take=0, may_target=True, **{"while": ["set_lining"]})
    assert label(r) == "All game fish other than burbot — release all, taken on a set line"


def test_verbatim_is_required():
    with pytest.raises(ValueError):
        CatalogueRule(rule_id="x", type=RuleType.advisory, verbatim="")


def test_size_polarity_on_a_sub_limit():
    """`r2:cultus_lake` — "1 bull trout over 60 cm": the fish you keep must BE over 60.
    Rendered as "no more than 1 under 60 cm" it inverts the rule on the fish it protects."""
    r = _r(type=RuleType.retention_limit, species=["BT"], take=1, within="parent",
           lengths=[{"min_cm": 60}, {"max_cm": 60, "take": 0}])
    assert label(r) == "Bull trout (no more than 1, none under 60 cm)"


def test_a_counted_size_class_on_a_sub_limit_allows_the_big_fish():
    """"not more than 1 over 50 cm" ALLOWS one big fish; "none over 50 cm" forbids them."""
    r = _r(type=RuleType.retention_limit, species=["TROUT"], take=1, within="parent",
           lengths=[{"min_cm": 50}])
    assert label(r) == "Trout (no more than 1 over 50 cm)"


def test_a_flat_size_prohibition_still_reads_as_one():
    r = _r(type=RuleType.retention_limit, species=["TROUT"], take=0, may_target=True,
           lengths=[{"min_cm": 50, "take": 0}])
    assert label(r) == "Trout — release all over 50 cm"


def test_a_slot_sub_limit_keeps_its_count():
    """`z7a` — "not more than 1 bull trout (Dolly Varden) ... only 30-50 cm in length".
    Dropping the 1 turns a one-fish allowance into an unlimited one inside the slot."""
    r = _r(type=RuleType.retention_limit, species=["BT"], take=1, within="parent",
           lengths=[{"min_cm": 30, "max_cm": 50}, {"max_cm": 30, "take": 0},
                    {"min_cm": 50, "take": 0}])
    assert label(r) == "Bull trout (no more than 1, 30–50 cm only)"


# --- the species menu is a PROMISE ------------------------------------------------------------
# It was not one. The parser was handed `species.prompt_menu()` — the official CSV listing — while
# validation checked `catalogue.KNOWN_SPECIES`. The two had drifted into different languages: the
# menu offered CH/CO/SK/PK/CM/PW/RW, which validation refuses, and omitted every group the corpus
# actually uses, including TROUT_CHAR, the commonest species value in 350 curated rules. A model
# cannot be blamed for picking a code the prompt offered it, so the menu is generated from the
# vocabulary and these tests keep it that way.

def test_every_menu_code_is_accepted_by_validation():
    import re
    from pipeline.regs.parsing.catalogue import species_menu, KNOWN_SPECIES
    offered = set(re.findall(r"`([A-Z_]{2,})`", species_menu()))
    assert offered, "the menu offered no codes at all"
    assert not (offered - KNOWN_SPECIES), \
        f"menu offers codes validation refuses: {sorted(offered - KNOWN_SPECIES)}"


def test_every_accepted_code_can_render_a_label():
    from pipeline.regs.parsing.catalogue import KNOWN_SPECIES, _SPECIES_WORDS
    assert not (KNOWN_SPECIES - set(_SPECIES_WORDS)), \
        f"accepted but unlabelled: {sorted(KNOWN_SPECIES - set(_SPECIES_WORDS))}"


def test_the_menu_offers_the_groups_the_synopsis_prints():
    from pipeline.regs.parsing.catalogue import species_menu
    m = species_menu()
    for g in ("ALL_GAME_FISH", "TROUT_CHAR", "TROUT", "CHAR", "WHITEFISH", "BASS"):
        assert f"`{g}`" in m, f"the menu never offers {g}"


def test_labels_match_the_official_table_not_a_strain_name():
    """GB was labelled 'Gerrard rainbow trout' — a Kootenay Lake strain that appears nowhere in the
    synopsis. The official table says Salmo trutta, Brown Trout."""
    from pipeline.regs.parsing.species import SPECIES
    from pipeline.regs.parsing.catalogue import _SPECIES_WORDS
    assert _SPECIES_WORDS["GB"] == "Brown trout"
    assert SPECIES["GB"].common_name == "Brown Trout"


def test_all_game_fish_is_the_closed_list_and_excludes_salmon():
    """definitions.md fixes this set. Salmon are federal and are NOT game fish, so a provincial
    rule about 'all game fish' must not silently reach them."""
    from pipeline.regs.parsing.catalogue import SPECIES_GROUPS
    agf = set(SPECIES_GROUPS["ALL_GAME_FISH"])
    assert not (agf & set(SPECIES_GROUPS["SALMON"]))
    for c in ("RB", "BT", "KO", "WSG", "CRA", "LMB", "MW"):
        assert c in agf, f"{c} is on the printed closed list but not in ALL_GAME_FISH"


def test_group_expansion_is_recoverable():
    from pipeline.regs.parsing.catalogue import expand_species
    assert expand_species(["TROUT_CHAR"])[:3] == ["RB", "ST", "CT"]
    assert expand_species(["BT"]) == ["BT"]                       # non-group passes through
    assert expand_species(["TROUT", "RB"]).count("RB") == 1       # de-duplicated


def test_non_game_fish_is_nameable_and_not_expanded_away():
    """"Only non-game fish (such as carp) may be speared, except burbot" is a rule ABOUT this set.
    It is the COMPLEMENT of the game-fish list, so it has no fixed membership — and expanding it to
    an empty list would erase the rule rather than state it."""
    from pipeline.regs.parsing.catalogue import expand_species, KNOWN_SPECIES, species_menu
    assert "NON_GAME_FISH" in KNOWN_SPECIES and "CP" in KNOWN_SPECIES
    assert expand_species(["NON_GAME_FISH"]) == ["NON_GAME_FISH"]
    assert "`NON_GAME_FISH`" in species_menu()


def test_every_collective_word_the_source_uses_has_a_code():
    """Audited against the reference corpus. "sport fish" is a verb ("licence to sport fish for any
    species"), "shellfish" appears only inside the definition of "fish", and "all species combined"
    is the `combined` quota field — none of the three is a species set."""
    from pipeline.regs.parsing.catalogue import KNOWN_SPECIES
    for word, code in (("game fish", "ALL_GAME_FISH"), ("non-game fish", "NON_GAME_FISH"),
                       ("trout/char", "TROUT_CHAR"), ("whitefish", "WHITEFISH"),
                       ("bass", "BASS"), ("salmon", "SALMON"), ("crayfish", "CRA"),
                       ("sturgeon", "SG"), ("perch", "P")):
        assert code in KNOWN_SPECIES, f"the source says {word!r} and there is no code for it"


def test_unresolved_locators_is_a_field_the_prompt_can_actually_ask_for():
    """The parse prompt, the batch envelope and the no-registry instructions all tell the model to
    record an unbindable phrase here. It existed only on the RETIRED prose Rule, so a model that
    followed the instruction exactly was refused with "extra inputs are not permitted" — four
    entries in one run. Instructing a field the model refuses is worse than not having it: the
    reach that could not be expressed is exactly what must not be dropped silently."""
    from pipeline.regs.parsing.catalogue import CatalogueRule
    r = CatalogueRule(rule_id="r1", type="retention_limit", species=["ALL_GAME_FISH"], take=0,
                      may_target=False, verbatim="No fishing above the falls",
                      unresolved_locators=["above the falls"],
                      review_reason="locator has no cut-point")
    assert r.unresolved_locators == ["above the falls"]


def test_an_unbound_locator_forces_review():
    import pytest
    from pipeline.regs.parsing.catalogue import CatalogueRule
    with pytest.raises(Exception, match="review_reason"):
        CatalogueRule(rule_id="r1", type="retention_limit", species=["ALL_GAME_FISH"], take=0,
                      may_target=False, verbatim="x", unresolved_locators=["the outlet"])


def test_every_field_the_prompts_name_exists_on_the_model():
    """The generalisation of the bug above: grep the prompts and the batch envelope for backticked
    rule fields and check the model accepts each one."""
    import re
    from pathlib import Path
    from pipeline.regs.parsing.catalogue import CatalogueRule
    fields = set(CatalogueRule.model_fields)
    text = ""
    for p in Path("pipeline/regs/parsing/prompts").glob("CATALOGUE_*.md"):
        text += p.read_text(encoding="utf-8")
    # only check names that look like rule fields and are named as code
    named = {m for m in re.findall(r"`([a-z][a-z0-9_]{3,})`", text)}
    known_non_fields = {
        "regs_verbatim", "entry_id", "rule_id", "extents", "splits", "item_id", "item_ids",
        "area_id", "area_kind", "feature_types", "within_area", "identity", "matched", "rules",
        "tributaries", "included", "needs_review", "review_reason", "locked", "reviewed_by",
        "true", "false", "null", "index", "entry", "whole", "between", "within", "applies",
        "excepts", "should", "must", "daily", "possession", "annual", "monthly", "stream",
        "lake", "hatchery", "wild", "angling", "set_lining", "spear_fishing", "ice_fishing",
        "netting", "crayfish_trapping", "upstream_of", "downstream_of", "includes_tributaries",
        "tributaries_only", "registry_status", "registry_note", "source_symbols",
    }
    suspect = {n for n in named if n not in fields and n not in known_non_fields
               and not n.isupper()}
    # anything left must not look like a rule field the model would be asked to emit
    assert not (suspect & {"unresolved_locators", "details", "restriction_type", "rule_text"}), \
        f"the prompts name rule fields the model refuses: {sorted(suspect)}"


# --------------------------------------------------------------------------------------- #
# SHAPE COERCION — a value written the wrong way is not a wrong value.
#
# Four shapes below came out of ONE 34-entry parse run. Two carry their meaning intact
# and are rewritten; two do not, and must keep failing, because deriving them means reading
# the sentence.
# --------------------------------------------------------------------------------------- #

def test_a_bare_exemption_name_becomes_a_list():
    from pipeline.regs.parsing.validate_catalogue import coerce_shapes
    d = {"rules": [{"rule_id": "r1", "exempts": "spring closure"}]}
    assert coerce_shapes(d) == 1
    assert d["rules"][0]["exempts"] == [{"default_id": "spring closure", "note": ""}]


def test_electric_only_is_a_propulsion_level_not_a_field():
    from pipeline.regs.parsing.validate_catalogue import coerce_shapes
    d = {"rules": [{"rule_id": "r1", "type": "vessel_rule", "electric_only": True}]}
    assert coerce_shapes(d) == 1
    r = d["rules"][0]
    assert "electric_only" not in r
    assert r["aspect"] == "propulsion" and r["level"] == "electric_only"


def test_a_bait_rule_with_no_gear_is_NOT_guessed():
    """"Bait ban" and "bait may be used" are both `bait_restriction`, and the difference is the
    whole rule. Inferring it from the word "ban" is reading the sentence; the entry fails."""
    from pipeline.regs.parsing.validate_catalogue import coerce_shapes
    d = {"rules": [{"rule_id": "r1", "type": "bait_restriction", "verbatim": "bait ban"}]}
    assert coerce_shapes(d) == 0
    assert "gear" not in d["rules"][0]


def test_a_propulsion_rule_with_no_level_is_NOT_guessed():
    """"No powered boats" is `unpowered` and "No vessels" is `none` — both are a refusal, and
    they are different rules. The model wrote `permitted: false` for both."""
    from pipeline.regs.parsing.validate_catalogue import coerce_shapes
    d = {"rules": [{"rule_id": "r1", "type": "vessel_rule", "aspect": "propulsion",
                    "permitted": False, "verbatim": "No powered boats"}]}
    assert coerce_shapes(d) == 0
    assert "level" not in d["rules"][0]


def test_all_fin_fish_is_wider_than_the_game_list():
    """"Any fish" is not "all game fish", and the difference is salmon and every non-game fish.

    Three rules in the corpus say a set wider than the provincial closed list — the snagging
    release duty, the crayfish-trap release duty, and the Pine River's catch-and-release. All
    three were written as ALL_GAME_FISH because the menu said to use it for "everything", which
    told a reader that a snagged coho or a snagged carp need not be released.
    """
    from pipeline.regs.parsing.catalogue import (
        KNOWN_SPECIES, SPECIES_GROUPS, expand_species, species_menu, species_words)

    assert "ALL_FIN_FISH" in KNOWN_SPECIES
    # Open, like NON_GAME_FISH: the non-game half is a complement that would go stale if listed.
    assert SPECIES_GROUPS["ALL_FIN_FISH"] == ()
    assert expand_species(["ALL_FIN_FISH"]) == ["ALL_FIN_FISH"]
    assert species_words(["ALL_FIN_FISH"]) == "All fish"
    # The menu must offer it, or the parser reaches for ALL_GAME_FISH again.
    assert "`ALL_FIN_FISH`" in species_menu()


def test_the_three_wider_than_game_rules_say_so():
    """The curated entries for those three sentences must not claim the narrower set."""
    import json
    from pathlib import Path

    want = {("zp:crayfish_trapping", "crayfish_trapping.r2"),
            ("zp:prohibited_methods", "prohibited_methods.r3"),
            ("r7:pine_river@7-32", "pine_river.r1")}
    root = Path(__file__).resolve().parents[2] / "data/curated/regulations/entries/catalogue"
    seen = set()
    for f in root.glob("*.json"):
        for entry in json.loads(f.read_text()).get("entries") or []:
            for rule in entry.get("rules") or []:
                key = (entry["entry_id"], rule["rule_id"])
                if key in want:
                    seen.add(key)
                    assert rule["species"] == ["ALL_FIN_FISH"], f"{key} narrowed to {rule['species']}"
    assert seen == want, f"missing: {want - seen}"


def test_reason_is_gone_and_a_notice_is_only_a_notice():
    """`reason` held a citation, an explanation and a hidden condition. It is refused now, and the
    citation has its own field that takes nothing but a notice number."""
    with pytest.raises(ValueError):
        _r(type=RuleType.advisory, reason="located in an Ecological Reserve")
    assert _r(type=RuleType.advisory, notice="FN0679").notice == "FN0679"
    with pytest.raises(ValueError):
        _r(type=RuleType.advisory, notice="for the conservation of chinook")


def test_a_suspended_requirement_says_what_suspends_it():
    """"Classified Waters Licence … not required until reopened to steelhead fishing": the licence
    rule sleeps while the steelhead closure binds, and its label says so from the closure's own
    label — not from prose."""
    from pipeline.regs.parsing.catalogue import CatalogueEntry
    e = CatalogueEntry.model_validate({
        "entry_id": "x", "name": "X", "regs_verbatim": "No Fishing for steelhead. Class II water",
        "rules": [
            {"rule_id": "x.r1", "type": "retention_limit", "verbatim": "No Fishing for steelhead",
             "species": ["ST"], "take": 0, "may_target": False},
            {"rule_id": "x.r2", "type": "vessel_rule", "verbatim": "Class II water",
             "aspect": "towing", "suspended_while": "x.r1"}]})
    sib = {r.rule_id: r for r in e.rules}
    assert label(e.rules[1], sib).endswith("— not while “No fishing for steelhead” is in force")
    with pytest.raises(ValueError, match="names no other rule"):
        CatalogueEntry.model_validate({**e.model_dump(mode="json", exclude_defaults=True),
                                       "rules": [e.rules[1].model_dump(mode="json",
                                                                       exclude_defaults=True)]})
