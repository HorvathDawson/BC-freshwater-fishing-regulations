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


# --- extents: aliases are canonicalised, invented ids are refused ------------------------------

def _batch_item(**kw):
    it = {
        "entry_id": "e1", "item_id": "gnis:1", "name": "Okanagan River", "region": "8",
        "raw_regs": "No fishing downstream of McIntyre Dam.",
        "bindable_ids": ["gauge__08NM247", "okanagan_river__mcintyre_dam"],
        "boundaries": [["gauge__08NM247", "Below Mcintyre Dam", "split",
                        ["okanagan_river__mcintyre_dam"]]],
    }
    it.update(kw)
    return it


def _candidate(splits):
    return {
        "entry_id": "e1", "region": "8", "name": "Okanagan River",
        "regs_verbatim": "No fishing downstream of McIntyre Dam.",
        "rules": [{
            "rule_id": "r1", "type": "retention_limit", "species": ["ALL_GAME_FISH"], "take": 0,
            "may_target": False,
            "verbatim": "No fishing downstream of McIntyre Dam.",
            "extents": [{"op": "downstream_of", "splits": splits}],
        }],
    }


def _splits_of(entry):
    return entry.rules[0].extents[0]["splits"]


def test_an_alias_is_rewritten_to_the_canonical_id():
    """The page says "McIntyre Dam"; the surviving boundary id is a gauge number. Both bind, but
    only one spelling is stored, or two rules about one point never compare equal."""
    from pipeline.regs.parsing.ingest_catalogue import ingest
    accepted, problems = ingest([_candidate(["okanagan_river__mcintyre_dam"])],
                                {"e1": _batch_item()})
    assert not [p for p in problems if not p.startswith("ADVISORY")], problems
    assert _splits_of(accepted["e1"]) == ["gauge__08NM247"]


def test_the_canonical_id_passes_through_unchanged():
    from pipeline.regs.parsing.ingest_catalogue import ingest
    accepted, _ = ingest([_candidate(["gauge__08NM247"])], {"e1": _batch_item()})
    assert _splits_of(accepted["e1"]) == ["gauge__08NM247"]


def test_an_invented_split_id_is_refused():
    """The catalogue path had no split check at all — an invented cut-point reached the corpus and
    surfaced much later as a rule that silently selected nothing."""
    from pipeline.regs.parsing.ingest_catalogue import ingest
    accepted, problems = ingest([_candidate(["okanagan_river__invented_dam"])],
                                {"e1": _batch_item()})
    assert "e1" not in accepted
    assert any("invented" in p for p in problems), problems


def test_a_no_registry_row_is_not_split_checked():
    from pipeline.regs.parsing.ingest_catalogue import ingest
    item = _batch_item(no_registry=True, bindable_ids=[], boundaries=[])
    cand = _candidate([])
    cand["rules"][0]["extents"] = []
    cand["rules"][0]["needs_review"] = True
    cand["rules"][0]["review_reason"] = "no registry match — attach an item and bind extents"
    accepted, problems = ingest([cand], {"e1": item})
    assert not [p for p in problems if not p.startswith("ADVISORY")], problems


def test_within_area_survives_ingest():
    """`within_area` limits an extent to a polygon and is applied after the tributary walk. It is
    a plain dict field, and the one failure mode that matters is silent loss: an extent that loses
    it resolves province-wide instead of bounded. entry_models.Extent DOES drop it — the catalogue
    stores extents as dicts precisely so it cannot."""
    from pipeline.regs.parsing.ingest_catalogue import ingest
    cand = _candidate([])
    cand["rules"][0]["extents"] = [{"op": "whole", "within_area": "area:region:5"}]
    accepted, problems = ingest([cand], {"e1": _batch_item()})
    assert not [p for p in problems if not p.startswith("ADVISORY")], problems
    assert accepted["e1"].rules[0].extents[0]["within_area"] == "area:region:5"


def test_within_area_survives_alongside_a_bound_reach():
    from pipeline.regs.parsing.ingest_catalogue import ingest
    cand = _candidate([])
    cand["rules"][0]["extents"] = [{"op": "downstream_of",
                                    "splits": ["okanagan_river__mcintyre_dam"],
                                    "within_area": "area:region:8"}]
    accepted, _ = ingest([cand], {"e1": _batch_item()})
    ex = accepted["e1"].rules[0].extents[0]
    assert ex["splits"] == ["gauge__08NM247"]        # alias canonicalised
    assert ex["within_area"] == "area:region:8"      # and the limiter kept


def test_a_prose_era_response_is_skipped_not_ingested():
    """The work dir survives between runs and dispatch skips a batch that already has a response —
    which is what makes a run resumable, and also what let 22 files from the retired prose parser
    be ingested as catalogue output. 669 rules then failed as "extra inputs are not permitted",
    with nothing in the output naming the real cause."""
    from pipeline.regs.parsing.ingest_catalogue import is_stale
    prose = [{"index": 1, "entry": {"entry_id": "e1", "rules": [
        {"rule_id": "r1", "restriction_type": "closure", "details": "No fishing"}]}}]
    catalogue = [{"index": 1, "entry": {"entry_id": "e1", "rules": [
        {"rule_id": "r1", "type": "retention_limit", "verbatim": "No fishing"}]}}]
    assert is_stale(prose)
    assert not is_stale(catalogue)
    assert not is_stale([])


def test_entry_id_comes_from_the_batch_not_the_model():
    """The agent retypes entry_id and drops the `@MU` suffix a third of the time. Stored under the
    truncated id, `--skip-existing` stops recognising the row: a resume re-parses waters already
    done and writes each one twice, under two ids that name one water."""
    from pipeline.regs.parsing.ingest_catalogue import ingest
    cand = _candidate([])
    cand["entry_id"] = "e1"                       # model drops the qualifier
    item = _batch_item(entry_id="e1@8-9", index=7)
    cand["_batch_index"] = 7                      # what run() attaches when it unwraps the envelope
    accepted, problems = ingest([cand], {"e1@8-9": item, "#7": item})
    assert not [p for p in problems if not p.startswith("ADVISORY")], problems
    assert "e1@8-9" in accepted, f"stored under {list(accepted)}"
    assert accepted["e1@8-9"].entry_id == "e1@8-9"
