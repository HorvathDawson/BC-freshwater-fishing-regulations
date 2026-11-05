"""T331: ONE SYNC LIST — the GitHub workflow and the Mac script put the same staged set (`pipeline.deliver.sync_inputs`).

They were two hand-kept lists and drifted: the workflow never synced `dfo_salmon/wording.json`. Now neither names a
data file; both run the module and put what its manifest lists.
"""
from __future__ import annotations

import json

import yaml
from pathlib import Path

from pipeline.common.curated import CURATED, REPO_ROOT
from pipeline.deliver import sync_inputs as S

WORKFLOW = REPO_ROOT / ".github/workflows/feeds-sync-inputs.yml"
MAC = REPO_ROOT / "deploy/staging/sync-inputs.sh"


def test_both_uploaders_stage_through_the_module_and_name_no_data_file():
    for p in (WORKFLOW, MAC):
        text = p.read_text(encoding="utf-8")
        body = "\n".join(ln for ln in text.splitlines() if not ln.lstrip().startswith("#"))
        if p == WORKFLOW:                     # T324: the push trigger's `paths:` name the inputs; the JOB names none
            body = body[body.index("\njobs:"):]
        assert "python -m pipeline.deliver.sync_inputs" in body or "-m pipeline.deliver.sync_inputs" in body, p
        assert "data/curated" not in body and "bc_hydrometric_stations" not in body, p


def test_the_workflow_choices_are_the_list():
    doc = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    on = doc.get("on", doc.get(True))
    assert on["workflow_dispatch"]["inputs"]["what"]["options"] == ["all", *S.NAMES]


def test_the_list_holds_the_wording_registry(tmp_path):
    got = S.stage(tmp_path, ("dfo-entries", "wording", "roster"))
    keys = [k for k, _ in got]
    assert "private/feeds/dfo/wording.json" in keys
    assert (tmp_path / "private/feeds/dfo/wording.json").read_bytes() == CURATED.dfo_salmon.wording.read_bytes()
    assert sum(k.startswith("private/feeds/dfo/entries/region-") for k in keys) == len(
        list(CURATED.dfo_salmon.places.glob("region-*.json")))
    roster = json.loads((tmp_path / "private/feeds/gauges/roster.json").read_text())
    assert roster and set(roster[0]) == {"station", "realtime"}
    assert all(c is None for _, c in got)                 # only the public in-season links carry a cache header


def test_an_unknown_input_is_refused(tmp_path):
    import pytest
    with pytest.raises(SystemExit):
        S.stage(tmp_path, ("wordings",))


def test_the_push_trigger_watches_every_input_and_targets_staging_only():
    """T324 (owner 2026-11-05): a push of a curated input syncs STAGING by itself; never a schedule."""
    from pipeline.common.curated import CURATED, REPO_ROOT as R, SOURCE
    doc = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    on = doc.get("on", doc.get(True))
    assert "schedule" not in on
    paths = set(on["push"]["paths"])
    rel = lambda p: str(Path(p).relative_to(R))
    for want in (rel(CURATED.dfo_salmon.places) + "/**", rel(CURATED.dfo_salmon.wording),
                 rel(CURATED.inseason.notice_waters), rel(SOURCE / "bc_hydrometric_stations.json")):
        assert want in paths, want
    env = doc["jobs"]["sync"]["env"]["BUCKET"]
    assert "github.event_name == 'workflow_dispatch' && inputs.target == 'production'" in env
