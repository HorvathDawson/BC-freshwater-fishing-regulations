"""Splitting a rule that bundles several independent restrictions into one rule each.

The synopsis writes a whole paragraph of restrictions as one phrase — "fly fishing only, bait ban,
hatchery rainbow trout catch and release (50 cm or less), and hatchery cutthroat catch and release" —
and the parser keeps that shape. They have to be separate rules: separately searchable, separately
displayable, each with its OWN species.

The danger runs both ways, so both directions are tested here: splitting too little leaves the bundle,
and splitting too much invents restrictions that the synopsis never stated.
"""

from __future__ import annotations

from pipeline.parsing.split_bundled_gear import classify, split_entry


def _details(parts) -> list[str]:
    return [n for n, _t, _s in parts]


def _entry(rule: dict, locked: bool = False) -> dict:
    return {"entry_id": "e1", "locked": locked, "rules": [{"rule_id": "e1.r1", "species": [],
                                                           "extents": [], **rule}]}


# --- what MUST split -------------------------------------------------------------------------

def test_gear_and_release_bundle_splits_with_species_per_clause():
    """The Chilliwack/Vedder rule: two gear restrictions and two releases, in one sentence."""
    parts = classify("Fly fishing only, bait ban; hatchery rainbow trout (50 cm or less) "
                     "and hatchery cutthroat catch and release")
    assert _details(parts) == [
        "Fly fishing only",
        "Bait ban",
        "Hatchery rainbow trout (50 cm or less): catch and release",
        "Hatchery cutthroat: catch and release",
    ]
    assert [t for _n, t, _s in parts] == ["gear_restriction", "gear_restriction", "harvest", "harvest"]
    # the size limit binds the rainbow only, and each release carries just its own species
    assert [s for _n, _t, s in parts] == [None, None, ["RB"], ["CT"]]


def test_two_quotas_over_two_species_split_and_narrow_their_species():
    """Inheriting the parent's [BS, YP] onto both halves would say bass have a quota of 20."""
    parts = classify("Bass daily quota 8; yellow perch daily quota 20")
    assert _details(parts) == ["Bass daily quota = 8", "Yellow perch daily quota = 20"]
    assert [s for _n, _t, s in parts] == [["BS"], ["YP"]]


def test_release_beside_gear_becomes_a_harvest_rule_and_a_gear_rule():
    parts = classify("Trout/char catch and release, bait ban, artificial fly only")
    assert [t for _n, t, _s in parts] == ["harvest", "gear_restriction", "gear_restriction"]


def test_and_still_separates_two_gear_restrictions():
    """Protecting "catch and release" must not stop "and" separating genuine gear clauses."""
    assert _details(classify("Bait ban and single barbless hook")) == ["Bait ban", "Single barbless hook"]


# --- what MUST NOT split ---------------------------------------------------------------------

def test_one_restriction_over_two_species_stays_one_rule():
    """"Cutthroat trout and bull trout catch and release" is ONE restriction. Splitting it produces
    two identical rules that differ only in species — which is what the species list is for."""
    parts = classify("Cutthroat trout and bull trout catch and release")
    assert parts is None or _details(parts) == ["Cutthroat trout and bull trout: catch and release"]
    if parts:
        assert [s for _n, _t, s in parts] == [["CT", "BT"]]


def test_release_with_no_subject_is_left_alone():
    """"Catch and release; single barbless hook" never says WHAT is released, so the species of the
    new harvest rule would have to be guessed."""
    assert classify("Catch and release; single barbless hook") is None


def test_unknown_subject_refuses_the_whole_rule():
    assert classify("Wolffish catch and release, bait ban") is None


def test_a_trailing_date_is_not_a_restriction():
    assert classify("Max hook size 15 mm, Oct 1-May 31") is None


def test_a_qualifier_clause_is_not_a_restriction():
    assert classify("Bait ban, all year") is None
    assert classify("Trout daily quota = 1, none over 50 cm") is None


def test_a_carve_out_is_never_chopped_into_nonsense():
    assert classify("Bait ban except when fishing for sturgeon") is None


def test_single_restriction_is_left_alone():
    assert classify("Bait ban") is None


# --- locked entries --------------------------------------------------------------------------

def test_locked_entry_is_not_reinterpreted_by_default():
    """A split that changes restriction_type or species reinterprets a decision the curator already
    confirmed, so it is reported instead of applied."""
    e = _entry({"restriction_type": "gear_restriction",
                "details": "Trout/char catch and release, artificial fly only",
                "species": ["RB"]}, locked=True)
    notes, held, unlocked = split_entry(e)
    assert notes == [] and len(held) == 1 and unlocked == []
    assert len(e["rules"]) == 1, "the locked rule must be left exactly as it was"
    assert e["locked"] is True


def test_locked_entry_splits_when_it_is_a_pure_re_expression():
    """Two gear restrictions of the same type and species is the same statement written twice — no
    reinterpretation, so it applies even when locked."""
    e = _entry({"restriction_type": "gear_restriction",
                "details": "Bait ban and single barbless hook", "species": []}, locked=True)
    notes, held, unlocked = split_entry(e)
    assert held == [] and len(notes) == 1 and unlocked == []
    assert e["locked"] is True, "a pure re-expression does not disturb the lock"
    assert [r["details"] for r in e["rules"]] == ["Bait ban", "Single barbless hook"]


def test_split_preserves_everything_but_details_type_species_and_id():
    e = _entry({"restriction_type": "gear_restriction",
                "details": "Trout/char catch and release, bait ban",
                "species": ["RB"], "dates": ["May 1-31"], "rule_text": "verbatim",
                "extents": [{"op": "whole", "splits": []}]})
    split_entry(e)
    assert len(e["rules"]) == 2
    for r in e["rules"]:
        assert r["dates"] == ["May 1-31"]
        assert r["rule_text"] == "verbatim"
        assert r["extents"] == [{"op": "whole", "splits": []}]
    assert [r["rule_id"] for r in e["rules"]] == ["e1.r1a", "e1.r1b"]
    assert e["rules"][0]["species"] == ["RB", "CT", "WCT", "CCT", "GB", "GT", "SLV"]
    assert e["rules"][1]["species"] == ["RB"], "a gear clause keeps the parent's species"


def test_include_locked_unlocks_and_flags_for_re_review():
    """Rewriting a confirmed rule into a different restriction_type is a reinterpretation. The entry
    must not keep its "confirmed" stamp on content nobody has read — it goes back in the queue."""
    e = _entry({"restriction_type": "gear_restriction",
                "details": "Trout/char catch and release, artificial fly only",
                "species": ["RB"]}, locked=True)
    notes, held, unlocked = split_entry(e, include_locked=True)
    assert held == [] and len(notes) == 1 and unlocked == ["e1.r1"]
    assert e["locked"] is False
    assert len(e["rules"]) == 2
    assert all(r["needs_review"] for r in e["rules"])
    assert all("split out of e1.r1" in r["review_reason"] for r in e["rules"])
    assert any("unlocked" in line for line in e["audit_log"])


def test_a_vessel_clause_splits_into_one_rule_per_restriction():
    """"no vessels on parts, no powered boats on parts, no towing on parts" is THREE rules.

    Two things stopped it. The table had no "No vessels"/"No towing", and `split_entry`'s gate only
    offered `gear_restriction` and `harvest` rules to `classify` — excluding the very type these
    clauses carry. ELK LAKE sat at 4 rules where the curator had confirmed 6.
    """
    got = classify("No vessels, no powered boats, no towing (parts)")
    assert got == [("No vessels", "vessel_restriction", None),
                   ("No powered boats", "vessel_restriction", None),
                   ("No towing (parts)", "vessel_restriction", None)]

    entry = {"rules": [{"rule_id": "x.r1", "restriction_type": "vessel_restriction",
                        "details": "No vessels, no powered boats, no towing (parts)",
                        "rule_text": "no vessels on parts, no powered boats on parts, no towing on parts",
                        "extents": [{"op": "whole"}]}]}
    split_entry(entry)
    assert [r["details"] for r in entry["rules"]] == ["No vessels", "No powered boats", "No towing (parts)"]
    assert {r["restriction_type"] for r in entry["rules"]} == {"vessel_restriction"}


def test_a_size_limit_and_a_quota_are_separate_rules_over_separate_species():
    """"No wild trout over 50 cm, 1 bull trout over 60 cm" is two rules about two different fish.

    Left as one rule it carried the UNION of both species lists, which says bull trout may not
    exceed 50 cm and every trout has a quota of 1 — neither of which the synopsis says.
    """
    got = classify("No wild trout over 50 cm; bull trout daily quota 1 (over 60 cm)")
    assert got == [("No wild trout over 50 cm", "harvest", ["RB", "CT", "WCT", "CCT", "GB", "GT"]),
                   ("Bull trout daily quota = 1 (over 60 cm)", "harvest", ["BT"])]


def test_a_qualifier_that_binds_its_own_clause_is_never_split_off():
    """"Trout daily quota = 1, none over 50 cm" is ONE rule — the size limit qualifies the quota.

    The comma here is not a separator between restrictions, and treating it as one would invent a
    standalone "none over 50 cm" rule that applies to nothing.
    """
    assert classify("Trout daily quota = 1, none over 50 cm") is None


def test_the_rule_standard_is_actually_delivered_to_both_agents():
    """RULE_STANDARDS.md is cited BY NAME in both prompts; a spec the agent cannot see is not a spec.

    The review checklist tells the reviewer to check `details` "against RULE_STANDARDS.md §3", and
    the parse prompt defers the canonical forms to it. Both loaders read only their own file, so the
    citation pointed at nothing until the standard was appended to each.
    """
    from pipeline.parsing.parse_context import load_system_prompt
    from pipeline.parsing.review_exporter import render_review_prompt

    for text in (load_system_prompt(), render_review_prompt([], {})):
        assert "Rule statement standards" in text
        assert "daily quota = " in text
        assert "restriction_type` is decided by what the rule DOES" in text
