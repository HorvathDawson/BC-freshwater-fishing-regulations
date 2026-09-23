"""End-to-end agent-parse flow: registry_io -> matcher -> batch_exporter -> validate -> ingest.
Synthetic registry + rows; no FWA data, no network."""

import json

from pipeline.common.models import RegistryBoundary, RegistryItem
from pipeline.regs.parsing.batch_exporter import export
from pipeline.regs.matching.matcher import match_rows
from pipeline.atlas.registry import load_registry, write_registry


def _registry():
    river = RegistryItem(
        id="gnis:1", name="Atnarko River", kind="stream", variants=("Atnarko",), mus=("5-4",),
        section_ids=("n1", "n2"),
        boundaries=(RegistryBoundary(id="hunlen_falls", label="Hunlen Falls", kind="confluence",
                                     ref="split:hunlen_falls"),),
    )
    lake = RegistryItem(id="wbk:9", name="Charlotte Lake", kind="lake", mus=("5-4",), section_ids=("l1",))
    return {river.id: river, lake.id: lake}


def _rows():
    return [
        {"water": "Atnarko River", "region": "REGION 5 - Cariboo", "mu": "5-4",
         "raw_regs": "No fishing upstream of Hunlen Falls. No powered boats.", "symbols": []},
        {"water": "Nonexistent Creek", "region": "REGION 5", "mu": "5-4", "raw_regs": "Closed.", "symbols": []},
    ]


def _candidate_entry(index: int) -> dict:
    """What the parser returns for the Atnarko row, in the envelope dispatch writes."""
    return {
        "index": index,
        "entry": {
            "entry_id": "r5:atnarko_river@5-4",
            "name": "Atnarko River",
            "regs_verbatim": "the model's copy — ingest replaces it with the batch's",
            "rules": [
                {"rule_id": "atnarko_river.r1", "type": "retention_limit",
                 "species": ["ALL_GAME_FISH"], "take": 0, "may_target": False,
                 "verbatim": "No fishing upstream of Hunlen Falls.",
                 "extents": [{"op": "upstream_of", "splits": ["hunlen_falls"]}]},
                {"rule_id": "atnarko_river.r2", "type": "vessel_rule", "aspect": "propulsion",
                 "level": "unpowered", "verbatim": "No powered boats.",
                 "extents": [{"op": "whole"}]},
            ],
        },
    }


def test_export_then_ingest_writes_the_entry(tmp_path):
    """The whole flow on files: export a batch, answer it, ingest the answer."""
    from pipeline.regs.parsing.ingest_catalogue import run
    export(_rows(), _registry(), tmp_path, batch_size=40, overrides={}, existing_ids=set(),
           force=False)
    batch = tmp_path / "batches" / "batch_000.json"
    item = next(it for it in json.loads(batch.read_text())["items"]
                if it["entry_id"] == "r5:atnarko_river@5-4")
    response = tmp_path / "responses" / "batch_000.json"
    response.parent.mkdir()
    response.write_text(json.dumps([_candidate_entry(item["index"])]))
    out = tmp_path / "catalogue"
    assert run([str(batch)], [str(response)], str(out), ledger=tmp_path / "ingested.json") == 0
    written = json.loads((out / "region-5.json").read_text())["entries"]
    assert [e["entry_id"] for e in written] == ["r5:atnarko_river@5-4"]
    assert written[0]["regs_verbatim"] == item["raw_regs"]
    assert written[0]["matched"] == ["gnis:1"]


def test_registry_io_round_trip(tmp_path):
    reg = _registry()
    p = write_registry(reg, tmp_path / "registry.json")
    back = load_registry(p)
    assert set(back) == set(reg)
    assert back["gnis:1"].boundaries[0].id == "hunlen_falls"
    assert back["gnis:1"].variants == ("Atnarko",)


def test_matcher_hits_and_misses():
    results = match_rows(_rows(), _registry())
    assert results[0].item_id == "gnis:1" and results[0].status in ("matched",)
    assert results[1].item_id is None and results[1].status == "unmatched"


def test_batch_layout_is_stable_regardless_of_existing_entries(tmp_path):
    # The batch layout MUST be a pure function of (rows, registry) — else a re-run that already
    # ingested some rows would renumber the batches and desync them from responses/ (the bug that
    # left region 3 unfinished). By default existing_ids must NOT change the layout.
    reg = _registry()
    m_fresh = export(_rows(), reg, tmp_path / "a", batch_size=40, overrides={},
                     existing_ids=set(), force=False)
    m_with_done = export(_rows(), reg, tmp_path / "b", batch_size=40, overrides={},
                         existing_ids={"r5:atnarko_river@5-4"}, force=False)   # pretend it's ingested
    assert m_with_done["pending_count"] == m_fresh["pending_count"]
    assert len(m_with_done["batches"]) == len(m_fresh["batches"])
    assert m_with_done["skipped_existing"] == []                        # opt-in only
    # opt-in skip_existing DOES drop it (deliberate fresh export)
    m_skip = export(_rows(), reg, tmp_path / "c", batch_size=40, overrides={},
                    existing_ids={"r5:atnarko_river@5-4"}, force=False, skip_existing=True)
    assert m_skip["pending_count"] == m_fresh["pending_count"] - 1


def test_two_rows_sharing_one_item_with_identical_regs_are_both_exported(tmp_path):
    """The exporter must NEVER drop a synopsis row because another row says the same thing.

    It used to dedupe on `(item_id, raw_regs)`, a key blind to region, MU and the verbatim
    name — so it silently deleted five real rows: PECKHAMS LAKE (a different water that merely
    shares "No powered boats" with NORBURY LAKE), BLACKWATER RIVER's "See West Road River"
    pointer printed in BOTH region 5 and region 7, and lakes listed under both their names.
    Redundancy is the CURATOR's call, made where it is visible.
    """
    reg = _registry()
    rows = [
        {"water": "Atnarko River", "region": "REGION 5 - Cariboo", "mu": "5-4",
         "raw_regs": "No powered boats.", "symbols": []},
        {"water": "Atnarko", "region": "REGION 5 - Cariboo", "mu": "5-4",
         "raw_regs": "No powered boats.", "symbols": []},        # same item, byte-identical regs
    ]
    m = export(rows, reg, tmp_path / "d", batch_size=40, overrides={}, existing_ids=set(), force=False)
    assert m["pending_count"] == 2 and m["skipped_existing"] == []
    items = json.loads((tmp_path / "d" / "batches" / "batch_000.json").read_text())["items"]
    ids = [it["entry_id"] for it in items]
    assert ids == ["r5:atnarko_river@5-4", "r5:atnarko@5-4"]      # kept apart by the row, not the item
    assert len(set(ids)) == 2
