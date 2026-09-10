"""The rule catalogue: what the model refuses, and what the label generator says.

These are the failures that shipped in the prose corpus. Each test names the real rule it comes
from, because a guard whose motivating case is forgotten gets deleted by the next person.
"""

from __future__ import annotations

import pytest

from pipeline.regs.parsing.catalogue import (
    Bait, CatalogueRule, Document, Lure, Method, Obligation, Period, PropulsionLevel,
    RuleType, VesselAspect, WaterKind, WindowsAre, label,
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
            _r(type=t, species=["RB"], allowed=False, barbless=True)


def test_a_bait_rule_may_be_scoped_to_what_you_are_FISHING_FOR():
    """A stream can carry a salmon bait ban and no other. That is not a species the ban protects —
    it is the fishery the ban applies to, which is why it is a separate field."""
    r = _r(type=RuleType.bait_restriction, bait=Bait.any, allowed=False, when_targeting=["SA"])
    assert label(r) == "Bait ban when fishing for salmon"
    general = _r(type=RuleType.bait_restriction, bait=Bait.any, allowed=False)
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
    with pytest.raises(ValueError, match="impossible slot"):
        _r(type=RuleType.retention_limit, species=["LT"], take=2, under_cm=90, over_cm=60)


def test_unknown_species_codes_are_refused():
    """`species_words` falls through to the raw code, so an unrecognised one prints at the reader."""
    with pytest.raises(ValueError, match="unknown species"):
        _r(type=RuleType.retention_limit, species=["NOT_A_FISH"], take=2)


def test_half_a_time_window_is_refused():
    """_scope needs both ends; with one, "No fishing 21:00-05:00" renders as a total closure."""
    with pytest.raises(ValueError, match="both ends"):
        _r(type=RuleType.retention_limit, species=["RB"], take=2, from_time="21:00")


def test_retention_fields_are_refused_on_other_types():
    with pytest.raises(ValueError, match="belongs to retention_limit"):
        _r(type=RuleType.bait_restriction, allowed=False, take=2)


def test_power_capped_needs_a_number():
    with pytest.raises(ValueError, match="power_capped needs"):
        _r(type=RuleType.vessel_rule, aspect=VesselAspect.propulsion,
           level=PropulsionLevel.power_capped)


def test_band_needs_both_bounds():
    with pytest.raises(ValueError, match="band needs"):
        _r(type=RuleType.retention_limit, species=["LT"], take=1, over_cm=90, band=True)


# --------------------------------------------------------------------------- the dimension

def test_tackle_facets_are_separate_dimensions():
    """A fly-only rule and a barbless rule are ADDITIVE — a fly must be barbless and singly
    hooked. `r1:farewell_lake@1-10` says all three in one sentence. Sharing a dimension would let
    one strike the other."""
    fly = _r(type=RuleType.tackle_restriction, lure=Lure.fly_fishing)
    hook = _r(type=RuleType.tackle_restriction, hook_count=1, barbless=True)
    assert fly.dimension != hook.dimension


def test_two_documents_do_not_collide():
    """44 entries need a classified licence AND a stamp at once."""
    a = _r(type=RuleType.document_required, document=Document.classified_waters_licence)
    b = _r(type=RuleType.document_required, document=Document.steelhead_stamp)
    assert a.dimension != b.dimension


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


def test_excepting_windows_say_except():
    """`r4:kootenay_lake_upper_west_arm` stores [Apr 1-3, Jul 1-2] on a catch-and-release rule:
    those are the days it does NOT apply."""
    r = _r(type=RuleType.retention_limit, species=["KO"], take=0, may_target=True,
           windows=["Apr 1-3", "Jul 1-2"], windows_are=WindowsAre.excepts)
    assert "except Apr 1-3 and Jul 1-2" in label(r)


def test_species_except_reads_as_the_sentence_does():
    r = _r(type=RuleType.retention_limit, species=["ALL_GAME_FISH"], species_except=["BB"],
           take=0, may_target=True, method=Method.set_lining)
    assert label(r) == "All game fish other than burbot — release all, taken on a set line"


def test_verbatim_is_required():
    with pytest.raises(ValueError):
        CatalogueRule(rule_id="x", type=RuleType.advisory, verbatim="")


def test_size_polarity_on_a_sub_limit():
    """`r2:cultus_lake` — "1 bull trout over 60 cm": the fish you keep must BE over 60.
    Rendered as "no more than 1 under 60 cm" it inverts the rule on the fish it protects."""
    r = _r(type=RuleType.retention_limit, species=["BT"], take=1, under_cm=60, within="parent")
    assert label(r) == "Bull trout (no more than 1, none under 60 cm)"


def test_over_cm_on_a_sub_limit_allows_the_big_fish():
    """"not more than 1 over 50 cm" ALLOWS one big fish; "none over 50 cm" forbids them."""
    r = _r(type=RuleType.retention_limit, species=["TROUT"], take=1, over_cm=50, within="parent")
    assert label(r) == "Trout (no more than 1 over 50 cm)"


def test_a_flat_size_prohibition_still_reads_as_one():
    r = _r(type=RuleType.retention_limit, species=["TROUT"], take=0, may_target=True, over_cm=50)
    assert label(r) == "Trout — release all over 50 cm"


def test_a_slot_sub_limit_keeps_its_count():
    """`z7a` — "not more than 1 bull trout (Dolly Varden) ... only 30-50 cm in length".
    Dropping the 1 turns a one-fish allowance into an unlimited one inside the slot."""
    r = _r(type=RuleType.retention_limit, species=["BT"], take=1, under_cm=30, over_cm=50,
           within="parent")
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
