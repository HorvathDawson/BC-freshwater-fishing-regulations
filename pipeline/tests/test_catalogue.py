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
    assert label(r) == "Bait ban, when fishing for salmon"
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
    # CT is a group (decision 11): "cutthroat" is westslope and coastal cutthroat
    assert expand_species(["TROUT_CHAR"])[:4] == ["RB", "ST", "WCT", "CCT"]
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


def test_every_name_the_prompts_use_is_current():
    """The generalisation of the bug above: every backticked name in the parse and review
    prompts must be something the CURRENT model knows — a field, an alias, or a value.

    A prompt that names a retired field primes the model to write it: listing what "no longer
    exists" teaches the old shape to a reader who never saw it. So the prompts teach the current
    shape only, and this pins that nothing else is named."""
    import enum
    import re
    import typing
    from pathlib import Path

    import pydantic

    import pipeline.regs.parsing.catalogue as C
    from pipeline.regs.parsing.entry_models import Extent, Op

    vocab: set[str] = set(C.CONDUCT_ACTS) | {o.value for o in Op} | set(Extent.model_fields)

    def literals(t):
        if typing.get_origin(t) is typing.Literal:
            vocab.update(str(a) for a in typing.get_args(t))
        for a in typing.get_args(t):
            literals(a)

    for obj in vars(C).values():
        if isinstance(obj, type) and issubclass(obj, pydantic.BaseModel):
            for name, f in obj.model_fields.items():
                vocab.add(name)
                if f.alias:
                    vocab.add(f.alias)
                literals(f.annotation)
        elif isinstance(obj, type) and issubclass(obj, enum.Enum):
            vocab.update(str(m.value) for m in obj)

    text = "".join(p.read_text(encoding="utf-8")
                   for p in Path("pipeline/regs/parsing/prompts").glob("CATALOGUE_*.md"))
    named = set(re.findall(r"`([a-z][a-z0-9_]{3,})`", text))
    example_ids = {"okanagan_river__mcintyre_dam"}       # a split id in a worked example
    assert not (named - vocab - example_ids), sorted(named - vocab - example_ids)


# --------------------------------------------------------------------------------------- #
# NO SHAPE REPAIR. `coerce_shapes` rewrote two shapes the model reached for — a bare-string
# `exempts`, and `electric_only: true` — and silently dropped `electric_only: false`. It is
# gone: a shape the schema does not take is refused, and the entry is re-parsed.
# --------------------------------------------------------------------------------------- #

def _refused(rule: dict) -> str:
    import pytest
    from pipeline.regs.parsing.validate_catalogue import check_entry
    entry = {"entry_id": "e1", "name": "X", "regs_verbatim": rule["verbatim"],
             "rules": [dict(rule, rule_id="r1")]}
    got, errors = check_entry(entry, rule["verbatim"])
    if got is not None:
        pytest.fail(f"accepted {rule}")
    return " ".join(errors)


def test_a_bare_exemption_name_is_refused_not_wrapped():
    assert "exempts" in _refused({"type": "advisory", "verbatim": "Exempt from spring closure",
                                  "exempts": "spring closure"})


def test_electric_only_as_a_field_is_refused_not_translated():
    for on in (True, False):
        assert "electric_only" in _refused({"type": "vessel_rule", "electric_only": on,
                                            "verbatim": "Electric motor only"})


def test_a_bait_rule_with_no_gear_is_NOT_guessed():
    """"Bait ban" and "bait may be used" are both `bait_restriction`, and the difference is the
    whole rule. Inferring it from the word "ban" is reading the sentence; the entry fails."""
    assert "gear" in _refused({"type": "bait_restriction", "verbatim": "bait ban"})


def test_a_propulsion_rule_with_no_level_is_NOT_guessed():
    """"No powered boats" is `unpowered` and "No vessels" is `none` — both are a refusal, and
    they are different rules."""
    assert "level" in _refused({"type": "vessel_rule", "aspect": "propulsion",
                                "verbatim": "No powered boats"})


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
             "species": ["ST"], "take": 0, "may_target": False, "extents": [{"op": "whole"}]},
            {"rule_id": "x.r2", "type": "vessel_rule", "verbatim": "Class II water",
             "aspect": "towing", "suspended_while": "x.r1", "extents": [{"op": "whole"}]}]})
    sib = {r.rule_id: r for r in e.rules}
    assert label(e.rules[1], sib).endswith("— not while “No fishing for steelhead” is in force")
    with pytest.raises(ValueError, match="names no other rule"):
        CatalogueEntry.model_validate({**e.model_dump(mode="json", exclude_defaults=True),
                                       "rules": [e.rules[1].model_dump(mode="json",
                                                                       exclude_defaults=True)]})


def test_exempts_names_one_thing_and_names_it_by_id():
    """`default_id` is a zone default's slug, `target` a rule id (in `entry_id` when another
    entry's). Prose in `target` ("Columbia Lake's tributaries closure") named nothing a reader could
    find, and a rule id was filed as a `default_id` (`set_lining.r1b`)."""
    import pytest
    from pipeline.regs.parsing.catalogue import Exempts

    assert Exempts(default_id="spring_stream_closure").target is None
    assert Exempts(target="species_quotas.r5", entry_id="z4:species_quotas").entry_id
    for bad in ({}, {"default_id": "a", "target": "a.r1"},
                {"target": "Columbia Lake's tributaries closure"},
                {"default_id": "set_lining.r1b"},
                {"default_id": "spring_stream_closure", "entry_id": "z3:x"},
                {"target": "a.r1", "entryid": "typo"}):
        with pytest.raises(Exception):
            Exempts(**bad)


def test_an_exemption_must_name_a_registered_default_or_a_rule_that_exists():
    """Five exemptions resolved to nothing: prose in `target`, a rule id in `default_id`, and a
    zone rule named bare from another entry. Each shape is refused where it can be seen."""
    import pytest
    from pipeline.regs.parsing.catalogue import (
        EXEMPTABLE_DEFAULTS, CatalogueEntry, CatalogueFile, Exempts)

    assert "spring_stream_closure" in EXEMPTABLE_DEFAULTS
    with pytest.raises(Exception, match="not a registered zone default"):
        Exempts(default_id="spring_closure")

    def entry(eid, rules):
        v = " ".join(r["verbatim"] for r in rules)
        return {"entry_id": eid, "name": "X", "regs_verbatim": v, "rules": rules}

    lift = {"rule_id": "x.r1", "type": "retention_limit", "species": ["ALL_GAME_FISH"],
            "verbatim": "Exempt from it.", "extents": [{"op": "whole"}]}
    closed = {"rule_id": "x.r2", "type": "retention_limit", "species": ["ALL_GAME_FISH"],
              "take": 0, "may_target": False, "verbatim": "No fishing.",
              "extents": [{"op": "whole"}]}
    ok = entry("r4:x@4-1", [{**lift, "exempts": [{"target": "x.r2"}]}, closed])
    CatalogueEntry.model_validate(ok)
    for bad, why in (([{"target": "x.r9"}], "names no other rule"),
                     ([{"target": "x.r1"}], "names no other rule"),                # itself
                     ([{"target": "x.r2", "entry_id": "r4:x@4-1"}], "its own entry")):
        with pytest.raises(Exception, match=why):
            CatalogueEntry.model_validate(entry("r4:x@4-1", [{**lift, "exempts": bad}, closed]))

    other = entry("r4:y@4-1", [{**closed, "rule_id": "y.r1"}])
    good = entry("r4:x@4-1", [{**lift, "exempts": [{"target": "y.r1", "entry_id": "r4:y@4-1"}]}])
    CatalogueFile.model_validate({"region": "4", "entries": [good, other]})
    wrong = entry("r4:x@4-1", [{**lift, "exempts": [{"target": "y.r7", "entry_id": "r4:y@4-1"}]}])
    with pytest.raises(Exception, match="name no rule of that entry"):
        CatalogueFile.model_validate({"region": "4", "entries": [wrong, other]})


def test_every_rule_says_where_it_is_and_nothing_inherits_the_entry():
    """`None inherits the entry` was the documented reading of a rule with no extents, and nothing
    implemented it: the reach builder left 110 such rules unbound on entries that had extents, and
    provenance filled them in from the entry. Now a rule states its reach — `extents`, or the words
    for a place nothing can draw — and an entry holding one that says nothing is refused."""
    import pytest
    from pipeline.regs.parsing.catalogue import CatalogueEntry

    base = {"entry_id": "r4:x@4-1", "name": "X", "regs_verbatim": "Bait ban.",
            "extents": [{"op": "whole"}]}
    rule = {"rule_id": "x.r1", "type": "bait_restriction", "verbatim": "Bait ban.",
            "gear": [{"slot": "bait", "ban": ["any_bait"]}]}
    with pytest.raises(ValueError, match="says nothing about where it applies"):
        CatalogueEntry.model_validate({**base, "rules": [rule]})       # the entry's do not count
    for said in ({"extents": [{"op": "whole"}]}, {"extent_text": "below the falls"},
                 {"unresolved_locators": ["the falls"], "review_reason": "no cut at the falls"}):
        CatalogueEntry.model_validate({**base, "rules": [{**rule, **said}]})


def test_the_corpus_has_no_rule_that_says_nothing_about_where():
    """Against the curated files: every rule states extents, or a place in words."""
    from pipeline.regs.parsing import io
    from pipeline.regs.parsing.catalogue import CatalogueEntry
    bare = [(e["entry_id"], r.rule_id) for e in io.read_entries_dir().values()
            for r in CatalogueEntry.model_validate(e).rules
            if not r.extents and not r.extent_text.strip() and not r.unresolved_locators]
    assert bare == []


def test_angling_from_powered_boats_says_powered():
    """"No angling from powered boats" shipped as the unqualified boat ban on seven rules —
    forbidding a canoe the book allows. It is a method ban on angling `when: {angler:
    in_powered_boat}`; the sentence's own word is checked against an `in_boat` clause."""
    import pytest
    from pipeline.regs.parsing.catalogue import CatalogueRule, label

    def ban(angler, verbatim):
        return {"rule_id": "x.r1", "type": "method_rule", "verbatim": verbatim,
                "extents": [{"op": "whole"}],
                "gear": [{"slot": "method", "ban": ["angling"], "when": {"angler": angler}}]}
    powered = "No angling from powered boats upstream of dyke gates"
    with pytest.raises(ValueError, match="POWERED"):
        CatalogueRule.model_validate(ban("in_boat", powered))
    r = CatalogueRule.model_validate(ban("in_powered_boat", powered))
    assert label(r) == "No angling (from a powered boat)"
    assert label(CatalogueRule.model_validate(ban("in_boat", "No angling from boats"))) \
        == "No angling (from a boat)"
    with pytest.raises(ValueError, match="level belongs to vessel_rule"):
        CatalogueRule.model_validate({**ban("in_powered_boat", powered), "level": "unpowered"})
    with pytest.raises(ValueError, match="level belongs to"):
        CatalogueRule.model_validate({"rule_id": "x.r1", "type": "advisory", "verbatim": "x",
                                      "level": "unpowered"})


def test_the_boat_type_is_retired_and_refused():
    import pytest
    from pipeline.regs.parsing.catalogue import CatalogueRule
    with pytest.raises(ValueError):
        CatalogueRule.model_validate({"rule_id": "x.r1", "type": "angling_from_vessel_prohibited",
                                      "verbatim": "No angling from boats",
                                      "extents": [{"op": "whole"}]})


def test_a_conditional_ban_never_displaces_an_unconditional_allow():
    """The province allows angling (`zp:terminal_tackle.r7`, method allow angling). A water's "no
    angling from boats" keyed "method:angling" would share its (type, dimension) and DISPLACE it —
    shore angling would read as not allowed. The clause's condition is part of the key."""
    from pipeline.regs.parsing.catalogue import CatalogueRule
    allow = CatalogueRule.model_validate({
        "rule_id": "p.r1", "type": "method_rule", "verbatim": "angle", "extents": [{"op": "whole"}],
        "gear": [{"slot": "method", "allow": ["angling"]}]})
    boats = CatalogueRule.model_validate({
        "rule_id": "w.r1", "type": "method_rule", "verbatim": "No angling from boats",
        "extents": [{"op": "whole"}],
        "gear": [{"slot": "method", "ban": ["angling"], "when": {"angler": "in_boat"}}]})
    plain = CatalogueRule.model_validate({
        "rule_id": "w.r2", "type": "method_rule", "verbatim": "No angling",
        "extents": [{"op": "whole"}], "gear": [{"slot": "method", "ban": ["angling"]}]})
    assert (allow.type, allow.dimension) != (boats.type, boats.dimension)
    assert boats.dimension == "method:angling@angler=in_boat"
    # an unconditional ban still competes with the unconditional allow, as before
    assert allow.dimension == plain.dimension == "method:angling"


# --------------------------------------------------------------------------- where a rule is

_CLOSED = dict(type=RuleType.retention_limit, species=["ALL_GAME_FISH"], take=0, may_target=False)


def test_a_bare_whole_beside_a_place_in_words_is_refused():
    """"No Fishing in Salmon Arm Bay" was stored as `whole` with the bay in `extent_text`; the
    builder reads only `whole`, so the closure bound all of Shuswap Lake. Seventeen part-lake rules
    were in that shape. A part nothing can draw keeps its words and NO extents."""
    with pytest.raises(ValueError, match="extent_text names a place"):
        _r(**_CLOSED, extents=[{"op": "whole"}], extent_text="Salmon Arm Bay")
    # the unbound shape is the right one
    assert _r(**_CLOSED, extent_text="Salmon Arm Bay", review_reason="not drawn").extents is None


def test_a_qualified_whole_keeps_its_description():
    """Mutation guard on the refusal: a `whole` that names an item, an area or a kind IS a place,
    and its `extent_text` only describes it ("lakes of the Fraser watershed")."""
    for ex in ({"op": "whole", "item_id": "gnis:39325"},
               {"op": "whole", "within_area": "area:region:5"},
               {"op": "whole", "item_id": "gnis:2936", "feature_types": ["lake"]}):
        assert _r(**_CLOSED, extents=[ex], extent_text="the watershed").extent_text


def test_extent_text_is_a_place_not_a_list_item():
    for bad in ("3. Within 23 m", "(b) Chimdemash Creek", "• the outlet"):
        with pytest.raises(ValueError, match="list marker"):
            _r(**_CLOSED, extent_text=bad, review_reason="x")
    assert _r(**_CLOSED, extent_text="(map A) west of the signs", review_reason="x")


def test_includes_tributaries_inside_a_rule_extent_is_refused():
    """The builder reads the flag on the rule, never on an extent: nineteen watershed rules
    ("Skeena River, including tributaries") bound the mainstem alone that way."""
    with pytest.raises(ValueError, match="includes_tributaries"):
        _r(**_CLOSED, extents=[{"op": "whole", "item_id": "gnis:2936",
                                "includes_tributaries": True}])
    assert _r(**_CLOSED, extents=[{"op": "whole", "item_id": "gnis:2936"}],
              includes_tributaries=True).includes_tributaries


def test_feature_types_on_some_extents_and_not_others_is_refused():
    """The builder applies one kind filter to the rule's whole reach, after the walk."""
    with pytest.raises(ValueError, match="feature_types is set on some extents"):
        _r(**_CLOSED, extents=[{"op": "whole", "item_id": "a", "feature_types": ["lake"]},
                               {"op": "whole", "item_id": "b"}])


# --------------------------------------------------------------------------- the label's place

_LIST_MARKER = r"^\s*(\d{1,2}[.)]|\([a-z0-9ivx]{1,3}\)|[•–-]\s)"


def test_a_label_names_the_place_its_extents_draw():
    """226 labels read exactly "No fishing": a split or an area never reached the label."""
    r = _r(**_CLOSED, extents=[{"op": "upstream_of", "splits": ["x__falls"]}])
    assert label(r) == "No fishing"
    assert label(r, place_of=lambda ex: "upstream of the falls") == \
        "No fishing — upstream of the falls"


def test_the_book_s_words_win_for_a_whole_it_describes():
    """"lakes of the Fraser watershed" says it better than an item name."""
    r = _r(**_CLOSED, extents=[{"op": "whole", "item_id": "gnis:39325", "feature_types": ["lake"]}],
           extent_text="lakes of the Fraser watershed")
    assert label(r, place_of=lambda ex: "Fraser River") == \
        "No fishing — lakes of the Fraser watershed"


def test_the_book_s_words_win_over_a_drawn_cut_point():
    """Kokish r2 read "between log boom (90 m upstream) and signs at the tail of the canyon pool
    (300 m downstream)" — the curator's offsets, where the book prints "approximately 100 m" and
    "250 m". A rule that carries the page's phrase is named by it; the atlas names only fill in a
    rule that has none."""
    r = _r(**_CLOSED, extents=[{"op": "between", "splits": ["a", "b"]}],
           extent_text="from the log boom upstream of the IPP intake to signs at the tail of the "
                       "canyon pool")
    said = label(r, place_of=lambda ex: "between log boom (90 m upstream) and signs (300 m down)")
    assert said == ("No fishing — from the log boom upstream of the IPP intake to signs at the "
                    "tail of the canyon pool")


def test_a_place_the_namer_cannot_name_falls_back_to_the_words():
    r = _r(**_CLOSED, extents=[{"op": "between", "splits": ["a", "b"]}],
           extent_text="from the CNR Bridge to the CNR Bridge")
    assert label(r, place_of=lambda ex: None) == "No fishing — from the CNR Bridge to the CNR Bridge"


def test_a_label_never_starts_with_a_list_marker():
    """The verbatim keeps the book's numbering ("3. Within 23 m …"); a generated label is not a
    list item, and neither is its place. Pinned over every rule in the corpus."""
    import json
    import re
    from pipeline.common.curated import CURATED
    from pipeline.regs.parsing.catalogue import CatalogueFile
    bad = []
    for p in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json")):
        for e in CatalogueFile.model_validate(json.loads(p.read_text())).entries:
            sib = {r.rule_id: r for r in e.rules}
            for r in e.rules:
                if re.match(_LIST_MARKER, label(r, sib)):
                    bad.append(f"{e.entry_id}::{r.rule_id}")
    assert not bad, bad[:10]
    # mutation: a label built from a marked place would start with one — the check can fail
    assert re.match(_LIST_MARKER, "3. Within 23 m")


def test_a_place_in_words_loses_its_list_marker_in_the_label():
    from pipeline.regs.parsing.catalogue import strip_list_marker
    assert strip_list_marker("(b) Chimdemash Creek") == "Chimdemash Creek"
    assert strip_list_marker("4. Within a 100 m radius") == "Within a 100 m radius"
    assert strip_list_marker("(map A) west") == "(map A) west"


def test_no_verbatim_is_cut_at_an_abbreviation():
    """"… rearing fish (e." and "… is attached (i." were cut by a sentence splitter at "e.g." and
    "i.e."; a verbatim must be the book's whole sentence."""
    import json
    import re
    from pipeline.common.curated import CURATED
    from pipeline.regs.parsing.catalogue import CatalogueFile
    cut = re.compile(r"(\((e|i)\.|\b(e\.g|i\.e)\.)\s*$")
    bad = []
    for p in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json")):
        for e in CatalogueFile.model_validate(json.loads(p.read_text())).entries:
            for r in e.rules:
                if cut.search(r.verbatim):
                    bad.append(f"{e.entry_id}::{r.rule_id}")
            for x in e.licensing:
                if cut.search(x.verbatim):
                    bad.append(f"{e.entry_id}#{x.id}")
    assert not bad, bad
    assert cut.search("operated for counting, passing or rearing fish (e.")


def test_no_verbatim_carries_a_known_extraction_garble():
    """The extractor interleaved a wrapped line into "Folpye fnishing" (Fly fishing + open)."""
    import json
    from pipeline.common.curated import CURATED
    for p in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json")):
        assert "Folpye" not in p.read_text(), p.name
