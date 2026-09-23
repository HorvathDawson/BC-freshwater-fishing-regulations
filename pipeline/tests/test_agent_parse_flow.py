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


def _candidate_entry():
    return {
        "index": 0,
        "entry": {
            "entry_id": "gnis:1",
            "identity": {"name": "Atnarko River", "region": "5", "mus": ["5-4"]},
            "regs_verbatim": "WILL BE INJECTED",
            "rules": [
                {"rule_id": "gnis:1.r1", "restriction_type": "closure", "details": "No fishing",
                 "rule_text": "No fishing upstream of Hunlen Falls.", "location_text": "upstream of Hunlen Falls",
                 "extents": [{"op": "upstream_of", "splits": ["hunlen_falls"]}]},
                {"rule_id": "gnis:1.r2", "restriction_type": "vessel_restriction", "details": "No powered boats",
                 "rule_text": "No powered boats.", "extents": [{"op": "whole"}]},
            ],
        },
    }


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
    Redundancy is the CURATOR's call (`reference_only`), made where it is visible.
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
