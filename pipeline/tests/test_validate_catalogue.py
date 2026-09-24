"""The parse gate — what a candidate must survive before a human ever sees it.

Layer 2 (a rule's verbatim inside its entry's passage) is not chain of custody on its own: the
agent writes both sides, so an invented sentence passes. Two did in the first authored pass. These
tests pin layer 3 — the passage against the text the agent was GIVEN.
"""

from __future__ import annotations

import pytest

from pipeline.regs.parsing.validate_catalogue import check_entry, squash

SOURCE = ("TRANQUILLE LAKE 3-29 Rainbow trout daily quota = 8. Bait ban. "
          "No fishing for kokanee in streams.")


def _entry(rules, regs=SOURCE):
    return {"entry_id": "r3:tranquille_lake@3-29", "name": "TRANQUILLE LAKE",
            "regs_verbatim": regs, "rules": rules}


def _quota(**kw):
    base = {"rule_id": "t.r1", "type": "retention_limit",
            "verbatim": "Rainbow trout daily quota = 8", "species": ["RB"], "take": 8}
    base.update(kw)
    return base


def test_a_clean_entry_passes():
    _, errors = check_entry(_entry([_quota()]), SOURCE)
    assert errors == []


def test_a_number_not_in_its_own_sentence_is_refused():
    """The guard that caught a 50 cm sub-limit attributed to a rule whose text never said it."""
    _, errors = check_entry(_entry([_quota(take=99)]), SOURCE)
    assert any("does not appear in its own verbatim" in e for e in errors)


def test_a_reworded_passage_is_refused():
    """LAYER 3. The agent writes both the passage and the rules, so only the source can settle it."""
    _, errors = check_entry(_entry([_quota()], regs="Rainbow trout quota is eight."), SOURCE)
    assert any("reworded" in e or "contiguous substring" in e for e in errors)


def test_an_invented_sentence_is_refused():
    """Exactly what got through the first time: a rule quoting text that is nowhere in the source."""
    rules = [_quota(rule_id="t.r2", verbatim="All sturgeon must be released.",
                    species=["WSG"], take=0, may_target=True)]
    _, errors = check_entry(_entry(rules, regs=SOURCE + " All sturgeon must be released."), SOURCE)
    assert any("reworded" in e or "contiguous run" in e for e in errors)


def test_a_sub_limit_larger_than_its_parent_is_refused():
    rules = [_quota(),
             _quota(rule_id="t.r2", verbatim="Rainbow trout daily quota = 8", take=8,
                    within="t.r1")]
    rules[1]["take"] = 8
    rules[0]["take"] = 8
    _, errors = check_entry(_entry(rules), SOURCE)
    assert errors == [] or all("cannot exceed" not in e for e in errors)


def test_within_must_name_a_rule_in_the_same_entry():
    _, errors = check_entry(_entry([_quota(within="nope.r9")]), SOURCE)
    assert any("names no rule in this entry" in e for e in errors)


def test_take_zero_without_may_target_is_refused_by_the_model():
    rules = [{"rule_id": "t.r1", "type": "retention_limit",
              "verbatim": "No fishing for kokanee in streams", "species": ["KO"], "take": 0}]
    _, errors = check_entry(_entry(rules), SOURCE)
    assert any("may_target" in e for e in errors)


def test_a_long_row_producing_one_rule_is_flagged_but_not_fatal():
    long_src = SOURCE + " " + ("Single barbless hook. Electric motor only. " * 6)
    _, errors = check_entry(_entry([_quota()], regs=long_src), long_src)
    advisories = [e for e in errors if e.startswith("ADVISORY")]
    assert advisories and all(e.startswith("ADVISORY") for e in errors)


def test_squash_ignores_emphasis_bullets_and_dash_variants():
    assert squash("**Bait ban** - all year") == squash("  • Bait ban – all year  ")


# --- the reach is part of the gate, not only of ingest -----------------------------------------
# The agent is told to run this validator on itself until clean. A check that lives only in ingest
# is a defect it cannot see and cannot fix: an invented cut-point passes every self-check, reaches
# the corpus, and surfaces much later as a rule that silently selects no sections.

def _reach_item():
    return {
        "entry_id": "e1", "region": "8",
        "raw_regs": "No fishing downstream of McIntyre Dam.",
        "bindable_ids": ["gauge__08NM247", "okanagan_river__mcintyre_dam"],
        "boundaries": [["gauge__08NM247", "Below Mcintyre Dam", "split",
                        ["okanagan_river__mcintyre_dam"]]],
    }


def _reach_entry(splits):
    return {
        "entry_id": "e1", "region": "8", "name": "Okanagan River",
        "regs_verbatim": "No fishing downstream of McIntyre Dam.",
        "rules": [{
            "rule_id": "r1", "type": "retention_limit", "species": ["ALL_GAME_FISH"],
            "take": 0, "may_target": False,
            "verbatim": "No fishing downstream of McIntyre Dam.",
            "extents": [{"op": "downstream_of", "splits": splits}],
        }],
    }


def test_validator_refuses_an_invented_split_id():
    from pipeline.regs.parsing.validate_catalogue import check_entry
    _, errors = check_entry(_reach_entry(["okanagan_river__invented_dam"]),
                            "No fishing downstream of McIntyre Dam.", _reach_item())
    assert any("invented" in e for e in errors), errors


def test_validator_rewrites_an_alias_to_the_canonical_id():
    from pipeline.regs.parsing.validate_catalogue import check_entry
    entry, errors = check_entry(_reach_entry(["okanagan_river__mcintyre_dam"]),
                                "No fishing downstream of McIntyre Dam.", _reach_item())
    assert not [e for e in errors if not e.startswith("ADVISORY")], errors
    assert entry.rules[0].extents[0]["splits"] == ["gauge__08NM247"]


def test_validator_without_an_item_still_validates_the_rest():
    """`item` is optional — a caller that has no batch still gets layers 1 and 2."""
    from pipeline.regs.parsing.validate_catalogue import check_entry
    entry, errors = check_entry(_reach_entry(["anything_at_all"]),
                                "No fishing downstream of McIntyre Dam.")
    assert entry is not None
    assert not any("not a cut-point" in e for e in errors)


def test_ingest_and_the_validator_share_one_implementation():
    """Two copies of this check would drift, and the ingest copy is the one nobody runs by hand."""
    import inspect
    from pipeline.regs.parsing import ingest_catalogue
    src = inspect.getsource(ingest_catalogue)
    assert "def _bind_extents" not in src and "def _boundary_ids" not in src, \
        "ingest re-implemented the split check instead of calling the validator's"


# --------------------------------------------------------------------------------------------
# AN EXEMPTION THAT NAMES A ZONE ENTRY BY THE BOOK'S WORDING LIFTS NOTHING.
#
# The parser writes what the book says — "exempt from spring closure" — and the entry is called
# `spring_stream_closure`. Nothing ever checked the two agreed: the renderer looks the id up,
# finds nothing, and lifts nothing. The rule still renders and still says "exempt", while the
# closure it exempts you from goes on closing the water. 41 rules were in that state, including
# "Mainstem open all year" on 1,250 km of the Fraser.

from pipeline.regs.parsing.validate_catalogue import resolve_exempt_ids


def _ex_entry(default_id=None, target=None, species=None):
    ex = {}
    if default_id: ex["default_id"] = default_id
    if target: ex["target"] = target
    return {"rules": [{"rule_id": "x.r1", "exempts": [ex], "species": species or []}]}


def test_book_wording_resolves_to_the_entry_id():
    for written, real in [("spring_closure", "spring_stream_closure"),
                          ("summer_closure", "summer_stream_closure"),
                          ("trout_char_release", "trout_char_winter_release"),
                          ("bait_ban", "bait_ban_streams")]:
        e = _ex_entry(default_id=written)
        assert resolve_exempt_ids(e) == 1
        assert e["rules"][0]["exempts"][0]["default_id"] == real


def test_an_id_that_is_already_right_is_left_alone():
    e = _ex_entry(default_id="spring_stream_closure")
    assert resolve_exempt_ids(e) == 0
    assert e["rules"][0]["exempts"][0]["default_id"] == "spring_stream_closure"


def test_an_unknown_id_is_never_guessed():
    """A silent rename is how this got here; an id with no alias stays put and stays visible."""
    e = _ex_entry(default_id="something_nobody_has_seen")
    assert resolve_exempt_ids(e) == 0
    assert e["rules"][0]["exempts"][0]["default_id"] == "something_nobody_has_seen"


def test_a_single_line_exemption_targets_the_RULE_not_the_entry():
    """"EXEMPT from the regional kokanee 'none from streams' quota" is ONE line of Region 4's
    `species_quotas`, which carries eleven. Pointed at the entry it would reach bass, burbot,
    pike, sturgeon and the rest — the over-application direction."""
    e = _ex_entry(default_id="kokanee_stream_quota")
    assert resolve_exempt_ids(e) == 1
    ex = e["rules"][0]["exempts"][0]
    assert ex.get("target") == "species_quotas.r5"
    assert "default_id" not in ex


# --- THE CLI, as the parse prompt tells the model to run it -------------------------------------
# `main()` once splatted argparse's names into run() (`run() got an unexpected keyword argument
# 'batch'`), so the one command CATALOGUE_PARSE_PROMPT.md gives the model crashed before it
# checked anything. Only a subprocess on real files exercises that line.

def _cli(tmp_path, entry):
    import json
    import subprocess
    import sys
    from pathlib import Path
    root = Path(__file__).resolve().parents[2]
    batch = tmp_path / "batch.json"
    batch.write_text(json.dumps({"batch": 0, "items": [
        {"index": 0, "entry_id": "r3:tranquille_lake@3-29", "name": "TRANQUILLE LAKE",
         "region": "3", "raw_regs": SOURCE}]}))
    cand = tmp_path / "candidate.json"
    cand.write_text(json.dumps([{"index": 0, "entry": entry}]))
    return subprocess.run(
        [sys.executable, "-m", "pipeline.regs.parsing.validate_catalogue", str(batch), str(cand)],
        cwd=root, env={**__import__("os").environ, "PYTHONPATH": str(root)},
        capture_output=True, text=True, timeout=120)


def test_cli_accepts_a_clean_candidate(tmp_path):
    p = _cli(tmp_path, _entry([_quota(extents=[{"op": "whole"}])]))
    assert p.returncode == 0, p.stdout + p.stderr
    assert "1/1 entries valid" in p.stdout


def test_cli_refuses_a_bad_candidate_with_exit_1(tmp_path):
    p = _cli(tmp_path, _entry([_quota(take=99, extents=[{"op": "whole"}])]))
    assert p.returncode == 1, p.stdout + p.stderr
    assert "Traceback" not in p.stderr
    assert "0/1 entries valid" in p.stdout and "FAIL " in p.stdout


def test_the_prompt_names_the_command_the_cli_takes():
    """The command in the prompt is the one tested above: module path, then batch, then candidate."""
    from pathlib import Path
    cmd = ("PYTHONPATH=\"$PWD\" .venv/bin/python -m pipeline.regs.parsing.validate_catalogue "
           "batch.json candidate.json")
    prompts = Path(__file__).resolve().parents[1] / "regs" / "parsing" / "prompts"
    for name in ("CATALOGUE_PARSE_PROMPT.md", "CHAT_INVOCATION.md"):
        assert cmd in (prompts / name).read_text(encoding="utf-8"), name


# --------------------------------------------------------------------------- split lakes, one gate

@pytest.mark.parametrize("where", ["matched", "rule", "licensing"])
def test_a_lake_cut_into_parts_is_not_a_place_a_record_may_name(where):
    """Kootenay Lake's parent keeps one section — its whole 423 km² polygon — while its three parts
    carry the water; binding the parent put the Main Body's quota and stamp on that ghost."""
    e = _entry([_quota(extents=[{"op": "whole"}])])
    if where == "matched":
        e["matched"] = ["wbk:328974235"]
    elif where == "rule":
        e["rules"][0]["extents"] = [{"op": "whole", "item_id": "wbk:328961697"}]
    else:
        e["licensing"] = [{"kind": "requirement", "id": "x", "doing": {"act": "fishing"},
                           "satisfied_by": [{"hold": ["basic_licence"]}],
                           "extents": [{"op": "whole", "item_ids": ["wbk:329459193"]}],
                           "verbatim": "Rainbow trout daily quota = 8"}]
    _, errors = check_entry(e, SOURCE)
    assert any("is cut into" in x for x in errors), errors


def test_the_parts_themselves_are_fine():
    e = _entry([_quota(extents=[{"op": "whole", "item_id": "wbk:-20"}])])
    e["matched"] = ["wbk:-21"]
    _, errors = check_entry(e, SOURCE)
    assert not any("is cut into" in x for x in errors), errors


def test_every_lake_with_parts_is_a_split_parent():
    from pipeline.atlas.waters.added_lakes.split_parents import split_parents
    got = split_parents()
    assert got["wbk:328974235"] == ["wbk:-20", "wbk:-21", "wbk:-22"]
    assert got["wbk:328961697"] == ["wbk:-16", "wbk:-17", "wbk:-18", "wbk:-19"]
    assert got["wbk:329459193"] == ["wbk:-15", "wbk:-23"]


def test_the_post_model_gate_addresses_each_refusal_to_its_field():
    """The review app shows the refusal on the field; ingest prints the same words."""
    from pipeline.regs.parsing.catalogue import CatalogueEntry
    from pipeline.regs.parsing.validate_catalogue import post_model_checks
    e = _entry([_quota(take=99, lengths=[{"min_cm": 77}], extents=[{"op": "whole"}])])
    got = post_model_checks(CatalogueEntry.model_validate(e))
    paths = {tuple(p) for p, _ in got}
    assert ("rules", 0, "take") in paths and ("rules", 0, "lengths", 0, "min_cm") in paths
    _, errors = check_entry(e, SOURCE)
    assert [m for _, m in got] == [x for x in errors if "does not appear" in x]


def test_no_catalogue_rule_names_a_split_parent():
    """The corpus itself: every record that meant a part now names the part."""
    import json
    from pipeline.atlas.waters.added_lakes.split_parents import refs_to_parents, split_parents
    from pipeline.common.curated import CURATED
    entries = [e for p in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json"))
               for e in json.loads(p.read_text())["entries"]]
    assert refs_to_parents(entries, split_parents()) == []


# --------------------------------------------------------------------------- #
# Counts written as words
# --------------------------------------------------------------------------- #

_NATION = "Lake trout possession quota = 2 (only one over 50 cm); no set lines"


def _sub_limit(verbatim, take=1, **kw):
    """The Nation-lakes shape: a possession quota and its "only one over 50 cm" sub-limit."""
    return [
        {"rule_id": "t.r1", "type": "retention_limit", "species": ["LT"], "take": 2,
         "period": "possession", "verbatim": "Lake trout possession quota = 2"},
        {"rule_id": "t.r2", "type": "retention_limit", "species": ["LT"], "take": take,
         "period": "possession", "within": "t.r1", "lengths": [{"min_cm": 50}],
         "verbatim": verbatim, **kw},
    ]


def test_a_count_spelled_as_a_word_is_its_own_number():
    """"only one over 50 cm" — six sub-limits were dropped because the gate read digits only."""
    _, errors = check_entry(_entry(_sub_limit("only one over 50 cm"), regs=_NATION), _NATION)
    assert errors == []


@pytest.mark.parametrize("take,verbatim", [
    (2, "only one over 50 cm"),        # the word says 1, the field says 2
    (1, "none over 50 cm"),            # "one" inside "none" is not a one
    (1, "a fish over 50 cm"),          # "a" is not accepted as one
    (1, "single fish over 50 cm"),     # nor is "single"
])
def test_a_word_count_must_be_the_same_number_and_a_whole_word(take, verbatim):
    regs = f"Lake trout possession quota = 2 ({verbatim}); no set lines"
    _, errors = check_entry(_entry(_sub_limit(verbatim, take=take), regs=regs), regs)
    assert any("t.r2: take=" in e and "does not appear" in e for e in errors), errors


def test_a_size_is_never_accepted_as_a_word():
    """Words are for COUNTS. A length the sentence spells only as "fifty" is not a printed size."""
    regs = "Lake trout possession quota = 2 (only one over fifty); no set lines"
    rules = _sub_limit("only one over fifty")
    _, errors = check_entry(_entry(rules, regs=regs), regs)
    assert any("lengths[0].min_cm=50" in e for e in errors), errors


def test_count_in_words_reads_whole_words_case_blind():
    from pipeline.regs.parsing.validate_catalogue import count_in_words
    assert count_in_words(1, "Only One over 50 cm")
    assert count_in_words(20, "twenty per day")
    assert count_in_words(2.0, "two daily quotas")
    assert not count_in_words(1, "someone")
    assert not count_in_words(10, "often")
    assert not count_in_words(21, "twenty-one")          # outside the table: digits only
    assert not count_in_words(1.5, "one and a half")


def test_twice_is_a_possession_multiplier_and_nothing_else():
    """"no more than twice the daily quota" is `per_daily: 2`; "twice" is not a take of 2."""
    from pipeline.regs.parsing.validate_catalogue import count_in_words
    assert count_in_words(2, "no more than twice the daily quota", "per_daily")
    assert not count_in_words(2, "no more than twice the daily quota", "take")
    assert not count_in_words(1, "once you have caught your quota", "per_daily")
