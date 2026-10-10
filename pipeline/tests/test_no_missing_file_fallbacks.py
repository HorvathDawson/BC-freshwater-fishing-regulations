"""P2 guardrail: NO MISSING-FILE FALLBACKS. A loader that read a missing input as empty built an
artifact that looked healthy and was wrong (no gauge tables, every lake a pond, a corrupt region
file silently dropped from the corpus). Each one now raises, naming the file; only a writer about
to create a region file may read it as empty (`missing_ok`)."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from pipeline.atlas.graph.names import load_name_variants
from pipeline.atlas.splits.area_splits import load_area_split_defs
from pipeline.deliver.bundle import rules as BR
from pipeline.deliver.bundle.place_names import area_names
from pipeline.gauges.matches import read_match
from pipeline.regs.parsing import io as PIO


def test_a_missing_region_file_raises_unless_a_writer_says_so(tmp_path: Path):
    with pytest.raises(FileNotFoundError, match="no region file"):
        PIO.read_entryfile(tmp_path / "region-9.json")
    assert PIO.read_entryfile(tmp_path / "region-9.json", missing_ok=True) == {}


def test_a_corrupt_region_file_is_not_silently_dropped_from_the_sources(tmp_path: Path):
    (tmp_path / "region-1.json").write_text("{ not json")
    with pytest.raises(ValueError):
        PIO._holds_entries(tmp_path)


def test_missing_curated_waters_files_raise(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load_name_variants(tmp_path / "name_variants.json")
    with pytest.raises(FileNotFoundError):
        load_area_split_defs(str(tmp_path / "areas.json"))


def test_missing_atlas_files_raise(tmp_path: Path):
    with pytest.raises(FileNotFoundError, match="area_catalog.gpkg"):
        area_names(tmp_path)


def test_missing_gauge_match_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError, match="pipeline.gauges.generate.match"):
        read_match(tmp_path / "matches.json")


def test_a_reach_run_without_rule_sections_stops_the_bundle(tmp_path: Path):
    """Skipped, the rule tables were empty and every water in the province rendered open."""
    with pytest.raises(SystemExit, match="rule_section.jsonl"):
        BR.write(sqlite3.connect(":memory:"), tmp_path, tmp_path, cov=None)


# ---------------------------------------------------------------------------------------------
# The reach run and the bundle read ONE corpus (P2: the entries digest)
# ---------------------------------------------------------------------------------------------
def _run(tmp_path: Path, entries) -> Path:
    from pipeline.atlas.reach.io import write_run
    from pipeline.atlas.reach.build import ReachResult
    from pipeline.atlas.reach.models import BuildReport
    write_run(tmp_path, ReachResult([], [], BuildReport(build="b", handles="0" * 16)), entries)
    return tmp_path


def test_the_reach_run_stamps_its_corpus_and_the_bundle_checks_it(tmp_path: Path):
    entries = {"r1:a@1-1": {"entry_id": "r1:a@1-1", "rules": [{"rule_id": "a.r1", "verbatim": "x"}]}}
    run = _run(tmp_path, entries)
    assert BR.check_corpus(run, entries) == PIO.corpus_digest(entries)


def test_mutation_an_edited_rule_with_unchanged_ids_is_refused(tmp_path: Path):
    entries = {"r1:a@1-1": {"entry_id": "r1:a@1-1", "rules": [{"rule_id": "a.r1", "verbatim": "x"}]}}
    run = _run(tmp_path, entries)
    edited = {"r1:a@1-1": {"entry_id": "r1:a@1-1", "rules": [{"rule_id": "a.r1", "verbatim": "y"}]}}
    with pytest.raises(SystemExit, match="re-run the reach builder"):
        BR.check_corpus(run, edited)


def test_a_run_that_predates_the_stamp_is_refused(tmp_path: Path):
    (tmp_path / "report.json").write_text("{}")
    with pytest.raises(SystemExit, match="predates the entries digest"):
        BR.check_corpus(tmp_path, {})
