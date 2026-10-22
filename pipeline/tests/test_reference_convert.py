"""The reference page's converter (`pipeline/deliver/answers/reference/convert.py`), which `page_data.py` still
uses to cut page v36's data block (the app's conformance oracle). Moved from test_answers_reference.py when the
page v35 harness was archived (audit F12, 2026-10-22); the assertions are unchanged."""
from __future__ import annotations

import pytest


def test_convert_gives_the_page_a_caught_condition_as_while():
    """v35 predates `caught` (answers 2.3, Q38: a snagged fish is released): read raw, the snag
    rule is `take: 0` on ALL_FIN_FISH with no condition the page knows, so the page released every
    fish in B.C. The converter hands the page the condition in its own word (`while`, which its
    `release` test reads, page_v35.js `n.release`); a rule without `caught` is untouched."""
    from pipeline.deliver.answers.reference.convert import rule_out
    prov = {"rank": 4, "who": "Provincial", "authority": "province", "binds_to": "region",
            "uncertain": False, "why": None, "entry_name": "Everywhere in B.C.", "scope": "area"}
    snag = {"entry_id": "zp:prohibited_methods", "rule_id": "prohibited_methods.r3",
            "type": "retention_limit", "family": "retention", "dimension": "daily@caught=foul_hooked",
            "label": "x", "parts": {}, "verbatim": "v", "binds": "sections", "provenance": prov,
            "fields": {"caught": ["foul_hooked"], "species": ["ALL_FIN_FISH"], "take": 0,
                       "period": "daily", "may_target": True}}
    got = rule_out(snag)
    assert got["fields"] == {"while": ["foul_hooked"], "species": ["ALL_FIN_FISH"], "take": 0,
                             "period": "daily", "may_target": True}
    assert snag["fields"]["caught"] == ["foul_hooked"] and "while" not in snag["fields"]  # not mutated
    plain = {**snag, "fields": {"species": ["RB"], "take": 0, "period": "daily"}}
    assert rule_out(plain)["fields"] == {"species": ["RB"], "take": 0, "period": "daily"}
    both = {**snag, "fields": {"caught": ["foul_hooked"], "while": ["angling"], "take": 0}}
    with pytest.raises(ValueError, match="both"):
        rule_out(both)
