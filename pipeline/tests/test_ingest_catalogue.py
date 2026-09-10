"""What may be written to disk, and what may not.

The gate exists because an agent writes BOTH the passage and the rules that quote it. Left to
itself that makes the chain of custody self-referential — an invented sentence validates against
its own invention, and two did. So ingest takes `regs_verbatim` from the batch and throws away
whatever the model supplied.
"""

from __future__ import annotations

import json

import pytest

from pipeline.regs.parsing.ingest_catalogue import ingest, load_batch, write

SRC = "TRANQUILLE LAKE 3-29 Rainbow trout daily quota = 8. Bait ban."
BATCH = {"r3:tranquille@3-29": {"entry_id": "r3:tranquille@3-29", "raw_regs": SRC}}


def _cand(**over):
    base = {"entry_id": "r3:tranquille@3-29", "name": "TRANQUILLE LAKE", "region": "3",
            "regs_verbatim": SRC,
            "rules": [{"rule_id": "t.r1", "type": "retention_limit",
                       "verbatim": "Rainbow trout daily quota = 8", "species": ["RB"], "take": 8}]}
    base.update(over)
    return base


def test_a_clean_candidate_is_accepted():
    accepted, problems = ingest([_cand()], BATCH)
    assert list(accepted) == ["r3:tranquille@3-29"]
    assert not [p for p in problems if not p.startswith("ADVISORY")]


def test_regs_verbatim_comes_from_the_batch_not_the_model():
    """THE POINT OF THE GATE. A model-supplied passage makes the substring check meaningless."""
    accepted, _ = ingest([_cand(regs_verbatim="whatever the model felt like writing")], BATCH)
    assert accepted["r3:tranquille@3-29"].regs_verbatim == SRC


def test_a_number_not_in_the_source_is_rejected():
    accepted, problems = ingest([_cand(rules=[
        {"rule_id": "t.r1", "type": "retention_limit",
         "verbatim": "Rainbow trout daily quota = 99", "species": ["RB"], "take": 99}])], BATCH)
    assert not accepted and problems


def test_an_entry_id_not_in_the_batch_is_rejected():
    """An invented or altered entry_id would write a regulation onto a water nobody asked about."""
    accepted, problems = ingest([_cand(entry_id="r3:somewhere_else@3-29")], BATCH)
    assert not accepted
    assert any("not in the batch" in p for p in problems)


def test_nothing_partial_is_written():
    """One good entry and one bad one: the good one lands, the bad one does not, and neither is
    half-written. A water with some of its regulations is indistinguishable from one with none."""
    bad = _cand(entry_id="r3:nope@3-29")
    accepted, _ = ingest([_cand(), bad], BATCH)
    assert list(accepted) == ["r3:tranquille@3-29"]


def test_write_validates_the_WHOLE_file_not_just_the_new_rows(tmp_path):
    """A duplicate entry_id or a broken neighbour is a failure of the file; writing it ships the
    break."""
    accepted, _ = ingest([_cand()], BATCH)
    out = tmp_path / "cat"
    out.mkdir()
    (out / "region-3.json").write_text(json.dumps(
        {"region": "3", "entries": [{"entry_id": "r3:other@3-1", "name": "Other",
                                     "regs_verbatim": "Bait ban.",
                                     "rules": [{"rule_id": "o.r1", "type": "bait_restriction",
                                                "verbatim": "Bait ban.", "allowed": False}]}]}))
    written = write(accepted, out)
    assert written == {"region-3.json": 1}
    both = json.loads((out / "region-3.json").read_text())["entries"]
    assert {e["entry_id"] for e in both} == {"r3:other@3-1", "r3:tranquille@3-29"}


def test_reingesting_replaces_rather_than_duplicates(tmp_path):
    accepted, _ = ingest([_cand()], BATCH)
    out = tmp_path / "cat"; out.mkdir()
    write(accepted, out)
    write(accepted, out)
    entries = json.loads((out / "region-3.json").read_text())["entries"]
    assert len(entries) == 1


def test_dry_run_writes_nothing(tmp_path):
    accepted, _ = ingest([_cand()], BATCH)
    out = tmp_path / "cat"; out.mkdir()
    write(accepted, out, dry_run=True)
    assert not list(out.glob("*.json"))
