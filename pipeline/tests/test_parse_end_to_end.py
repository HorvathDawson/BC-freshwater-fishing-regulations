"""The whole path, in one test: prompt -> agent output -> gate -> a file on disk.

Every other test checks one link. This one checks that the links join, because each of them has
been individually correct while the chain was broken — the prompt described a format the ingest
did not accept, and the ingest wrote a shape the model would not revalidate.
"""

from __future__ import annotations

import json

from pipeline.regs.parsing.catalogue import CatalogueFile, label
from pipeline.regs.parsing.ingest_catalogue import ingest, write
from pipeline.regs.parsing.parse_context import load_system_prompt

#: A printed row of the kind the exporter hands over as `raw_regs`.
ROW = ("TRANQUILLE LAKE 3-29 Rainbow trout daily quota = 8. Bait ban. "
       "No fishing for kokanee in streams. Electric motor only.")


def test_the_prompt_describes_the_format_the_gate_accepts():
    """The prompt names every type the model defines, and no type it does not."""
    from pipeline.regs.parsing.catalogue import RuleType

    prompt = load_system_prompt()
    for t in RuleType:
        assert t.value in prompt, f"the prompt never mentions {t.value}"
    assert "PARSE_PROMPT" not in prompt and "RULE_STANDARDS" not in prompt


def test_the_prompt_teaches_the_two_conditions_most_often_got_wrong():
    prompt = load_system_prompt()
    assert "may_target" in prompt
    assert "No fishing for" in prompt and "Catch and Release" in prompt
    assert "not more than 1 over 50 cm" in prompt      # size polarity


def test_an_agent_response_in_the_documented_shape_reaches_disk(tmp_path):
    """Exactly the JSON the prompt's Output section asks for."""
    candidate = {
        "entry_id": "r3:tranquille_lake@3-29",
        "name": "TRANQUILLE LAKE",
        "display_name": "Tranquille Lake",
        "region": "3",
        "regs_verbatim": "the model's copy, which ingest must discard",
        "source_pages": [29],
        "matched": ["gnis:12345"],
        "extents": [{"op": "whole"}],
        "rules": [
            {"rule_id": "tranquille_lake.r1", "type": "retention_limit",
             "verbatim": "Rainbow trout daily quota = 8", "species": ["RB"], "take": 8},
            {"rule_id": "tranquille_lake.r2", "type": "bait_restriction",
             "verbatim": "Bait ban.", "bait": "any", "allowed": False},
            {"rule_id": "tranquille_lake.r3", "type": "retention_limit",
             "verbatim": "No fishing for kokanee in streams.", "species": ["KO"],
             "take": 0, "may_target": False, "water": "stream"},
            {"rule_id": "tranquille_lake.r4", "type": "vessel_rule",
             "verbatim": "Electric motor only.", "aspect": "propulsion",
             "level": "electric_only"},
        ],
    }
    batch = {"r3:tranquille_lake@3-29": {"entry_id": "r3:tranquille_lake@3-29", "raw_regs": ROW}}

    accepted, problems = ingest([candidate], batch)
    assert not [p for p in problems if not p.startswith("ADVISORY")], problems
    assert list(accepted) == ["r3:tranquille_lake@3-29"]

    out = tmp_path / "catalogue"; out.mkdir()
    assert write(accepted, out) == {"region-3.json": 1}

    # and it round-trips: what was written revalidates, and every rule renders
    f = CatalogueFile.model_validate(json.loads((out / "region-3.json").read_text()))
    entry = f.entries[0]
    assert entry.regs_verbatim == ROW, "the passage must come from the batch, not the model"
    labels = [label(r) for r in entry.rules]
    assert labels == [
        "Rainbow trout — 8 per day",
        "Bait ban",
        "No fishing for kokanee in streams",
        "Electric motor only (max 7.5 kW)",
    ], labels


def test_the_gate_refuses_the_mistakes_the_prompt_warns_about(tmp_path):
    batch = {"x@1-1": {"entry_id": "x@1-1", "raw_regs": ROW}}

    def cand(rule):
        return {"entry_id": "x@1-1", "name": "X", "region": "1", "rules": [rule]}

    # take=0 with no may_target — the 605-rule defect
    acc, _ = ingest([cand({"rule_id": "x.r1", "type": "retention_limit",
                           "verbatim": "No fishing for kokanee in streams.",
                           "species": ["KO"], "take": 0})], batch)
    assert not acc

    # a species on a bait rule — the ban is the whole river
    acc, _ = ingest([cand({"rule_id": "x.r1", "type": "bait_restriction", "verbatim": "Bait ban.",
                           "bait": "any", "allowed": False, "species": ["RB"]})], batch)
    assert not acc

    # a number that is not in the rule's own sentence
    acc, _ = ingest([cand({"rule_id": "x.r1", "type": "retention_limit",
                           "verbatim": "Rainbow trout daily quota = 8",
                           "species": ["RB"], "take": 80})], batch)
    assert not acc
