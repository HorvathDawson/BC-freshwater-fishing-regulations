"""EVERY TEACHING OF THE PARSE PROMPT VALIDATES — an entry written exactly as
`CATALOGUE_PARSE_PROMPT.md` instructs passes `CatalogueEntry` and `post_model_checks`.

A review (2026-09-29, model-export ME-01…ME-11) wrote 23 entries to the letter of the prompt and
five were refused: the lift shape (`exempts`) was never shown, a lift-only quota's `species` was
never required, a bare "Catch and release" needed a crayfish exception nobody mentioned, a
designation outside Region 4 needed a stamp clause, and a list number in a verbatim was refused.
The prompt now teaches each; this pins that what it teaches is what the model accepts — and, for
each, that the prompt still SAYS it (a teaching deleted from the prompt fails here too).

Where the prompt prints the JSON itself (the Output example, the lift example), the test reads it
out of the prompt rather than copying it.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from pipeline.regs.parsing.catalogue import CatalogueEntry, label
from pipeline.regs.parsing.validate_catalogue import post_model_checks

PROMPT = (Path(__file__).resolve().parents[1] / "regs" / "parsing" / "prompts"
          / "CATALOGUE_PARSE_PROMPT.md").read_text(encoding="utf-8")


def _entry(eid: str, verb: str, rules: list, **kw) -> dict:
    e = {"entry_id": eid, "name": eid.split(":")[1].split("@")[0].upper(),
         "display_name": eid.split(":")[1].split("@")[0].replace("_", " ").title(),
         "region": eid[1:eid.index(":")], "regs_verbatim": verb, "source_pages": [29],
         "matched": ["gnis:1"], "extents": [{"op": "whole"}], "rules": rules}
    e.update(kw)
    return e


def _ok(e: dict) -> CatalogueEntry:
    m = CatalogueEntry.model_validate(e)
    post = post_model_checks(m)
    assert not post, post
    return m


def _refused(e: dict, words: str) -> None:
    with pytest.raises(Exception) as got:
        _ok(e)
    assert words in str(got.value), str(got.value)[:400]


def _says(*phrases: str) -> None:
    for p in phrases:
        assert p in PROMPT, f"the prompt no longer teaches: {p!r}"


def _json_block_after(marker: str) -> dict:
    """The first ```json block after `marker` in the prompt, parsed."""
    at = PROMPT.index(marker)
    m = re.search(r"```json\n(.*?)```", PROMPT[at:], re.S)
    assert m, marker
    text = m.group(1).strip()
    return json.loads(text if text.startswith("{") else "{" + text + "}")   # a fragment of keys


# --------------------------------------------------------------------------- ME-01 exemptions
def test_the_lift_example_the_prompt_prints_validates():
    rule = _json_block_after("## Exemptions — `exempts`")
    _ok(_entry("r1:oyster_river@1-6", rule["verbatim"], [rule]))


@pytest.mark.parametrize("slug", ["spring_stream_closure", "summer_stream_closure",
                                  "steelhead_stream_closure", "trout_char_winter_release",
                                  "bait_ban_streams", "single_barbless_hook"])
def test_every_default_the_prompt_lists_is_registered(slug):
    from pipeline.regs.parsing.catalogue import EXEMPTABLE_DEFAULTS
    _says(slug)
    assert slug in EXEMPTABLE_DEFAULTS


def test_the_prompt_lists_exactly_the_registered_defaults():
    from pipeline.regs.parsing.catalogue import EXEMPTABLE_DEFAULTS
    block = PROMPT[PROMPT.index("## Exemptions"):PROMPT.index("## Rules the page imposes")]
    listed = set(re.findall(r"\b([a-z_]+_(?:closure|release|streams|hook))\b", block))
    assert EXEMPTABLE_DEFAULTS <= listed


def test_a_lift_only_quota_carries_species_as_taught():
    _says("A lift-only `retention_limit` STILL carries `species`")
    verb = "Exempt from spring closure"
    lift = {"rule_id": "g_river.r1", "type": "retention_limit", "verbatim": verb,
            "exempts": [{"default_id": "spring_stream_closure"}], "extents": [{"op": "whole"}]}
    _refused(_entry("r3:g_river@3-1", verb, [lift]), "retention_limit needs species")
    _ok(_entry("r3:g_river@3-1", verb, [dict(lift, species=["ALL_GAME_FISH"])]))
    # the book's own words are not a slug
    _refused(_entry("r3:g_river@3-1", verb, [dict(lift, species=["ALL_GAME_FISH"],
                                                   exempts=[{"default_id": "spring_closure"}])]),
             "not a registered zone default")


def test_a_lift_of_a_rule_elsewhere_names_its_entry():
    _says('{"target": "<rule_id>", "entry_id": "<id>"}')
    verb = "EXEMPT from Columbia Lake's tributaries closure"
    _ok(_entry("r4:x_creek@4-1", verb, [
        {"rule_id": "x_creek.r1", "type": "retention_limit", "verbatim": verb,
         "species": ["ALL_GAME_FISH"], "extents": [{"op": "whole"}],
         "exempts": [{"target": "columbia_lake.r3", "entry_id": "r4:columbia_lake@4-25"}]}]))


def test_a_bait_ban_lift_and_a_hook_lift_as_taught():
    _says('`gear: [{"slot": "bait", "allow":\n  ["any_bait"]}]`',
          "a lift of the single-barbless-hook default is `tackle_restriction` with no\n  `gear`")
    verb = "EXEMPT from bait ban; EXEMPT from single barbless hooks"
    _ok(_entry("r7:m_creek@7-30", verb, [
        {"rule_id": "m_creek.r1", "type": "bait_restriction", "verbatim": "EXEMPT from bait ban",
         "gear": [{"slot": "bait", "allow": ["any_bait"]}],
         "exempts": [{"default_id": "bait_ban_streams"}], "extents": [{"op": "whole"}]},
        {"rule_id": "m_creek.r2", "type": "tackle_restriction",
         "verbatim": "EXEMPT from single barbless hooks",
         "exempts": [{"default_id": "single_barbless_hook"}], "extents": [{"op": "whole"}]}]))


# --------------------------------------------------------------------------- ME-02 youth
def test_youth_disabled_accompanied_water_is_two_rules_as_taught():
    _says('`closed_to: {"age": ["16_plus"]}`',
          '`closed_to_except: [{"residency": ["resident"], "status": ["disabled"]}, '
          '{"role": ["companion"]}]`')
    verb = "Youth/Disabled Accompanied Water year round (see page 4)"
    m = _ok(_entry("r1:b_lake@1-7", verb, [
        {"rule_id": "b_lake.r1", "type": "program_membership", "verbatim": verb,
         "extents": [{"op": "whole"}]},
        {"rule_id": "b_lake.r2", "type": "angler_closure", "verbatim": verb,
         "closed_to": {"age": ["16_plus"]},
         "closed_to_except": [{"residency": ["resident"], "status": ["disabled"]},
                              {"role": ["companion"]}],
         "extents": [{"op": "whole"}]}]))
    assert "companion" in label(m.rules[1]).lower()


# --------------------------------------------------------------------------- ME-03 bare C&R
def test_a_bare_catch_and_release_excepts_crayfish_as_taught():
    _says('`species: ["ALL_GAME_FISH"], species_except: ["CRA"], take: 0, may_target: true`')
    rule = {"rule_id": "z_creek.r1", "type": "retention_limit", "verbatim": "Catch and release",
            "species": ["ALL_GAME_FISH"], "take": 0, "may_target": True,
            "extents": [{"op": "whole"}]}
    _refused(_entry("r5:z_creek@5-1", "Catch and release", [rule]), "CRA")
    _ok(_entry("r5:z_creek@5-1", "Catch and release", [dict(rule, species_except=["CRA"])]))


# --------------------------------------------------------------------------- ME-04 no registry
def test_a_no_registry_row_writes_whole_with_a_review_reason_as_taught():
    from pipeline.regs.parsing.parse_context import build_no_registry_context, render_user_message
    _says("**A row with NO REGISTRY MATCH**")
    ctx = build_no_registry_context(entry_id="r5:frog_lake@5-6", name="FROG LAKE",
                                    raw_regs="No powered boats", registry_note="unmatched",
                                    region="5", mus=("5-6",))
    envelope = render_user_message(ctx)
    # the envelope teaches what the model accepts: `whole` + a review_reason, never `extents: []`
    assert "Do NOT invent split ids" in envelope and "extents: []" not in envelope
    assert '[{"op": "whole"}]' in envelope and "review_reason" in envelope
    why = "no registry match — attach an item and bind extents"
    verb = "No powered boats; speed restriction on parts (8 km/h)"
    e = _entry("r5:frog_lake@5-6", verb, [
        {"rule_id": "frog_lake.r1", "type": "vessel_rule", "verbatim": "No powered boats",
         "aspect": "propulsion", "level": "unpowered", "extents": [{"op": "whole"}],
         "review_reason": why},
        {"rule_id": "frog_lake.r2", "type": "vessel_rule",
         "verbatim": "speed restriction on parts (8 km/h)", "aspect": "speed", "max_kmh": 8,
         "undrawn_part": "on parts", "extents": [{"op": "whole"}], "review_reason": why}],
        matched=[])
    e.pop("extents")
    _ok(e)


# --------------------------------------------------------------------------- ME-05 stamp
def test_a_designation_outside_region_4_carries_the_stamp_or_a_reason_as_taught():
    _says("**Outside Region 4 a `designation` must say what the row prints about the Steelhead Stamp**")
    d = {"kind": "designation", "id": "h_river", "classified": "II", "unit": "h_river",
         "unit_name": "H River",
         "when": {"dates": [{"from_month": 9, "from_day": 1, "to_month": 4, "to_day": 30}]},
         "extents": [{"op": "whole"}], "verbatim": "Class II water Sept 1-Apr 30"}
    plain = d["verbatim"]
    _refused(_entry("r3:h_river@3-1", plain, [], licensing=[d]), "steelhead country")
    verb = "Class II water Sept 1-Apr 30; Steelhead Stamp mandatory Dec 1-Apr 30"
    _ok(_entry("r3:h_river@3-1", verb, [], licensing=[dict(d, steelhead_stamp_during={
        "when": {"dates": [{"from_month": 12, "from_day": 1, "to_month": 4, "to_day": 30}]},
        "verbatim": "Steelhead Stamp mandatory Dec 1-Apr 30"})]))
    _ok(_entry("r3:h_river@3-1", plain, [], licensing=[dict(
        d, review_reason="the row prints no stamp clause for this designation")]))
    _ok(_entry("r4:h_river@4-1", plain, [], licensing=[d]))      # Region 4 prints none


# --------------------------------------------------------------------------- ME-06 list marker
def test_a_verbatim_quotes_the_sentence_not_its_list_number():
    _says("**Quote from the sentence, never from its list number or\n   bullet**")
    verb = "1. No fishing within 100 m of the dam"
    rule = {"rule_id": "l_lake.r1", "type": "retention_limit", "species": ["ALL_GAME_FISH"],
            "take": 0, "may_target": False, "extents": [{"op": "whole"}],
            "undrawn_part": "within 100 m of the dam",
            "review_reason": "the dam needs a cut-point to draw 100 m"}
    _refused(_entry("r8:l_lake@8-1", verb, [dict(rule, verbatim=verb)]), "list marker")
    _ok(_entry("r8:l_lake@8-1", verb, [dict(rule, verbatim=verb[3:])]))


# --------------------------------------------------------------------------- ME-07 fields
def test_the_curator_only_fields_are_named_and_the_parsers_are_not():
    block = PROMPT[PROMPT.index("## Fields the CURATOR sets"):PROMPT.index("## Output")]
    for f in ("closure_kind", "derived_from", "condition_of", "standing", "authority", "notice",
              "scope_note", "anadromous_rainbow", "watershed", "outside_items"):
        assert f"`{f}`" in block or f" {f}" in block, f
    _says("record_retention: true", "tributaries_only  true")


def test_record_retention_of_adult_chinook_as_taught():
    verb = "record your retention of adult chinook salmon"
    _ok(_entry("r2:f_river@2-1", verb, [
        {"rule_id": "f_river.r1", "type": "retention_limit", "verbatim": verb,
         "species": ["CH"], "life_stage": "adult", "record_retention": True,
         "extents": [{"op": "whole"}]}]))


# --------------------------------------------------------------------------- ME-08 count
def test_the_prompt_counts_the_models_types():
    from pipeline.regs.parsing.catalogue import RuleType
    words = {13: "thirteen"}
    assert len(RuleType) == 13
    _says(f"six families, {words[len(RuleType)]} types")
    for t in RuleType:
        assert t.value in PROMPT, t.value


# --------------------------------------------------------------------------- ME-09 rest
def test_the_rest_example_the_prompt_prints_validates_and_labels_other_parts():
    """ME-09: `rest` with `extent_text` beside it looked like a contradiction of the field's
    definition; the definition now says why (the label needs the book's word), and this pins it."""
    ext = _json_block_after('**"Other parts", "all other parts", "remainder" — `rest`.**')
    assert ext["extents"][0]["op"] == "rest" and ext["extent_text"] == "other parts"
    verb = "Bull trout catch and release upstream of the falls. Other parts: trout/char daily quota = 1"
    m = _ok(_entry("r4:bull_river@4-22", verb, [
        {"rule_id": "bull_river.r1", "type": "retention_limit",
         "verbatim": "Bull trout catch and release upstream of the falls", "species": ["DV"],
         "take": 0, "may_target": True,
         "extents": [{"op": "upstream_of", "splits": ["falls"]}]},
        {"rule_id": "bull_river.r2", "type": "retention_limit",
         "verbatim": "trout/char daily quota = 1", "species": ["TROUT_CHAR"], "take": 1,
         **ext}]))
    assert "other parts" in label(m.rules[1]).lower()


# --------------------------------------------------------------------------- ME-10 pre-reads
def test_the_prompt_is_the_contract_not_its_background_reading():
    _says("This prompt and the validator are the contract")


# --------------------------------------------------------------------------- the rest
def test_the_output_example_the_prompt_prints_validates():
    at = PROMPT.index("## Output")
    m = re.search(r"```json\n(.*?)```", PROMPT[at:], re.S)
    e = json.loads(m.group(1).replace("<the printed row, unedited>",
                                      "Rainbow trout daily quota = 8"))
    e["rules"][0]["extents"] = [{"op": "whole"}]            # "Write it out every time"
    _says('"extents": [{"op": "whole"}]', "Write it out every time")
    _ok(e)


@pytest.mark.parametrize("name,verb,rule,taught", [
    ("no fishing for kokanee", "No fishing for kokanee",
     {"species": ["KO"], "take": 0, "may_target": False}, "`take=0, may_target=false`"),
    ("bull trout is DV", "Bull trout catch and release",
     {"species": ["DV"], "take": 0, "may_target": True}, "*\"Bull trout catch and release\"* is `species: [\"DV\"]`"),
    ("trout is TROUT_CHAR", "Trout daily quota = 2",
     {"species": ["TROUT_CHAR"], "take": 2}, "are all\n  `TROUT_CHAR`"),
    ("whole-water closure", "No Fishing",
     {"species": ["ALL_GAME_FISH"], "take": 0, "may_target": False},
     "A whole-water closure is `species=ALL_GAME_FISH, take=0, may_target=false`"),
    ("no trout over 50 cm", "No trout over 50 cm",
     {"species": ["TROUT_CHAR"], "may_target": True,
      "lengths": [{"min_cm": 50, "take": 0}]}, '[{"min_cm": 50, "take": 0}]            no take'),
])
def test_a_one_rule_teaching_validates(name, verb, rule, taught):
    _says(taught)
    _ok(_entry("r3:t_water@3-1", verb, [dict(
        {"rule_id": "t_water.r1", "type": "retention_limit", "verbatim": verb,
         "extents": [{"op": "whole"}]}, **rule)]))


def test_a_semicolon_ends_the_dating_run_as_taught():
    _says("**A `;` ends the\nrun.**")
    verb = "Trout/char catch and release; bait ban, June 15-Oct 31"
    _ok(_entry("r3:v_creek@3-1", verb, [
        {"rule_id": "v_creek.r1", "type": "retention_limit",
         "verbatim": "Trout/char catch and release", "species": ["TROUT_CHAR"], "take": 0,
         "may_target": True, "extents": [{"op": "whole"}]},
        {"rule_id": "v_creek.r2", "type": "bait_restriction", "verbatim": "bait ban, June 15-Oct 31",
         "gear": [{"slot": "bait", "ban": ["any_bait"]}],
         "when": {"dates": [{"from_month": 6, "from_day": 15, "to_month": 10, "to_day": 31}]},
         "extents": [{"op": "whole"}]}]))


def test_a_pointer_is_see_not_a_rule_as_taught():
    _says('`"see": [{"verbatim": "See Lonzo Creek", "entry_ids": ["r2:lonzo_marshall_creek@2-4"]}]`')
    _ok(_entry("r2:lonzo@2-4", "See Lonzo Creek", [],
               see=[{"verbatim": "See Lonzo Creek", "entry_ids": ["r2:lonzo_marshall_creek@2-4"]}]))


# --------------------------------------------------------------------------- validators
def test_a_gear_note_costs_a_review_reason():
    """VA-01: `GearWhen.note` says it costs a review_reason; now it is enforced."""
    verb = "Bait ban except when ice fishing"
    rule = {"rule_id": "j_lake.r1", "type": "bait_restriction", "verbatim": verb,
            "gear": [{"slot": "bait", "ban": ["any_bait"], "when": {"note": "except when ice fishing"}}],
            "extents": [{"op": "whole"}]}
    _refused(_entry("r8:j_lake@8-1", verb, [rule]), "review_reason")
    _ok(_entry("r8:j_lake@8-1", verb, [dict(rule, review_reason="ice fishing is a method; "
                                                   "write when.method once confirmed")]))


def test_a_conditional_bait_ban_reads_bait_ban():
    """VA-02: a total bait ban with a condition labelled "No any bait"."""
    verb = "Bait ban in streams"
    m = _ok(_entry("r8:k_lake@8-1", verb, [
        {"rule_id": "k_lake.r1", "type": "bait_restriction", "verbatim": verb,
         "gear": [{"slot": "bait", "ban": ["any_bait"], "when": {"water": "stream"}}],
         "extents": [{"op": "whole"}]}]))
    assert label(m.rules[0]).startswith("Bait ban (stream)"), label(m.rules[0])
    assert "any bait" not in label(m.rules[0])


def test_lengths_are_refused_off_retention():
    """VA-03: a size on a bait rule validated and vanished from its label."""
    verb = "Bait ban over 50 cm"
    _refused(_entry("r8:m_lake@8-1", verb, [
        {"rule_id": "m_lake.r1", "type": "bait_restriction", "verbatim": verb,
         "gear": [{"slot": "bait", "ban": ["any_bait"]}], "lengths": [{"min_cm": 50}],
         "extents": [{"op": "whole"}]}]), "lengths belongs to retention_limit")
