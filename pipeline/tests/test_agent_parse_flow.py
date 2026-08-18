"""End-to-end agent-parse flow: registry_io -> matcher -> batch_exporter -> validate -> ingest.
Synthetic registry + rows; no FWA data, no network."""

import json

from pipeline.models import RegistryBoundary, RegistryItem
from pipeline.parsing import ingest as ingest_mod
from pipeline.parsing import validate as validate_mod
from pipeline.parsing.batch_exporter import export
from pipeline.matching.matcher import match_rows
from pipeline.registry import load_registry, write_registry


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


def test_export_validate_ingest_and_locked(tmp_path):
    reg = _registry()
    out_dir = tmp_path / "parse"
    manifest = export(_rows(), reg, out_dir, batch_size=40, overrides={}, existing_ids=set(), force=False)
    assert manifest["pending_count"] == 2                 # matched row + unmatched row (content-only)
    assert manifest["no_registry_count"] == 1             # the unmatched row is parsed, flagged no_registry
    assert len(manifest["unmatched"]) == 1                # still reported for visibility

    batch_file = out_dir / "batches" / "batch_000.json"
    assert batch_file.exists() and (out_dir / "batches" / "batch_000.prompt.txt").exists()
    items = json.loads(batch_file.read_text())["items"]
    assert items[0]["bindable_ids"] == ["hunlen_falls"] and items[0]["raw_regs"].startswith("No fishing")
    assert items[0]["no_registry"] is False
    # the unmatched row exported as a no-registry item: no boundaries, reason recorded
    nr = items[1]
    assert nr["no_registry"] is True and nr["bindable_ids"] == [] and nr["item_id"] is None
    assert "unmatched" in nr["registry_note"] and nr["entry_id"].startswith("noreg_")

    # agent's candidate response -> self-check passes
    cand = tmp_path / "resp.json"
    cand.write_text(json.dumps([_candidate_entry()]))
    assert validate_mod.run(str(batch_file), str(cand)) == 0

    # ingest -> region-5.json written with the entry
    batch_items = validate_mod.load_batch_items(batch_file)
    accepted, report = ingest_mod.ingest([cand.read_text()], batch_items)
    assert report["accepted"] == [0] and not report["failed"]
    entries_dir = tmp_path / "entries"
    written = ingest_mod.write_entry_files(accepted, batch_items, entries_dir)
    assert written["5"]["entries"] == 1
    region_file = entries_dir / "region-5.json"
    assert json.loads(region_file.read_text())["entries"][0]["entry_id"] == "gnis:1"

    # lock it, then a re-ingest with a changed entry must NOT overwrite the locked one
    data = json.loads(region_file.read_text())
    data["entries"][0]["locked"] = True
    region_file.write_text(json.dumps(data))
    changed = _candidate_entry()
    changed["entry"]["rules"][0]["details"] = "Angling closed"   # a valid but different re-parse
    accepted2, _ = ingest_mod.ingest([json.dumps([changed])], batch_items)
    assert accepted2                                             # the changed entry IS valid
    written2 = ingest_mod.write_entry_files(accepted2, batch_items, entries_dir)
    assert written2["5"]["kept_locked"] == 1
    # locked original preserved on disk — the re-parse did NOT overwrite it
    assert json.loads(region_file.read_text())["entries"][0]["rules"][0]["details"] == "No fishing"


def test_no_registry_row_parses_content_only(tmp_path):
    # An unmatched row is exported as a no_registry item; its regs still split into rules (each flagged
    # needs_review, no extents), and ingest injects registry_status/note authoritatively.
    reg = _registry()
    out_dir = tmp_path / "parse"
    export(_rows(), reg, out_dir, batch_size=40, overrides={}, existing_ids=set(), force=False)
    batch_file = out_dir / "batches" / "batch_000.json"
    batch_items = validate_mod.load_batch_items(batch_file)
    nr_index = next(i for i, it in batch_items.items() if it["no_registry"])

    cand = {"index": nr_index, "entry": {
        "entry_id": "IGNORED — ingest injects the real id",
        "identity": {"name": "Nonexistent Creek", "region": "5", "mus": ["5-4"]},
        "regs_verbatim": "WILL BE INJECTED",
        "rules": [{"rule_id": "r1", "restriction_type": "closure", "details": "Closed",
                   "rule_text": "Closed.", "extents": [], "needs_review": True,
                   "review_reason": "no registry match — attach an item and bind extents"}],
    }}
    accepted, report = ingest_mod.ingest([json.dumps([cand])], batch_items)
    assert report["accepted"] == [nr_index] and not report["failed"], report
    entry = accepted[nr_index]
    assert entry.registry_status == "no_registry" and "unmatched" in entry.registry_note
    assert entry.entry_id.startswith("noreg_") and entry.matched == []
    assert entry.rules[0].needs_review and entry.rules[0].extents == []


def test_ingest_rejects_bad_split(tmp_path):
    reg = _registry()
    out_dir = tmp_path / "parse"
    export(_rows(), reg, out_dir, batch_size=40, overrides={}, existing_ids=set(), force=False)
    batch_file = out_dir / "batches" / "batch_000.json"
    bad = _candidate_entry()
    bad["entry"]["rules"][0]["extents"] = [{"op": "upstream_of", "splits": ["not_a_boundary"]}]
    batch_items = validate_mod.load_batch_items(batch_file)
    accepted, report = ingest_mod.ingest([json.dumps([bad])], batch_items)
    assert not accepted and report["failed"] and report["failed"][0]["index"] == 0
