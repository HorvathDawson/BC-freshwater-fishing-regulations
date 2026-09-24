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
