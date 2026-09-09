"""BC species reference table (pipeline/regs/parsing/species.py), loaded from the authoritative CSV."""

from pipeline.regs.parsing.species import (
    COMMON_NAME, GROUPS, KNOWN_SPECIES_CODES, SPECIES, expand_group,
    normalize_species, resolve_species_phrase,
)


def test_authoritative_codes_loaded():
    # straight from bc_species.csv — these are stable, official codes
    assert SPECIES["RB"].common_name == "Rainbow Trout"
    assert SPECIES["BT"].common_name == "Bull Trout"
    assert SPECIES["ST"].common_name == "Steelhead"
    assert len(KNOWN_SPECIES_CODES) > 150


def test_group_membership_derived_from_genus():
    # 'Char, General' (SLV) = genus Salvelinus -> the chars + bull/brook/lake trout + splake
    chars = expand_group("SLV")
    for c in ("AC", "BT", "DV", "EB", "LT", "SPK"):   # arctic char, bull, dolly, brook, lake, splake
        assert c in chars
    # a plain species expands to itself
    assert expand_group("RB") == frozenset({"RB"})


def test_salmon_override_excludes_trout():
    # genus Oncorhynchus also covers RB/CT, so the salmon group is pinned to the 5 Pacific salmon
    sa = expand_group("SA")
    assert sa == {"CH", "CM", "CO", "PK", "SK"}
    assert "RB" not in sa and "CT" not in sa


def test_resolve_specific_and_group_and_collective():
    assert resolve_species_phrase("Bull Trout") == ["BT"]
    assert resolve_species_phrase("steelhead") == ["ST"]
    assert resolve_species_phrase("char") == ["SLV"]                # a group code
    # A GROUP CODE, NOT SIX SPECIES, and that changed deliberately.
    #
    # "Trout: 4" is one claim about trout. Written as six codes it becomes six claims that
    # happen to coincide: a correction has to find all six, and the stored rule no longer
    # resembles the sentence it came from. `TRT` is synthetic — the official table has 27
    # "General" rows and none is trout, because trout is not a taxon — and `expand_group`
    # turns it back into species for anything that needs them.
    assert resolve_species_phrase("trout") == ["TRT"]
    assert expand_group("TRT") == frozenset({"RB", "CT", "WCT", "CCT", "GB", "GT"})
    # "Trout and Char" is the commonest line in the synopsis and used to resolve to nothing,
    # so every parse composed it by hand from the two halves — three chances to differ.
    assert resolve_species_phrase("Trout and Char") == ["TRT", "SLV"]
    assert resolve_species_phrase("spacefish") == []               # unknown -> empty, no guess


def test_normalize_unknown_returns_none():
    assert normalize_species("dragonfish") is None
    assert normalize_species("") is None
    assert normalize_species("RB") == "RB"                          # already a code


def test_groups_and_names_are_consistent():
    # every group member is a real code (Rule.species validation would reject otherwise)
    for members in GROUPS.values():
        assert members <= KNOWN_SPECIES_CODES
    assert set(COMMON_NAME) == set(KNOWN_SPECIES_CODES)
