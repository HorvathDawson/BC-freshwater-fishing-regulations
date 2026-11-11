"""T331: ONE SYNC LIST — the uploaders put the staged set of `pipeline.deliver.sync_inputs` and name no data file.

(T520, owner 2026-11-08) Publishing is from the Mac only: the GitHub workflow (`feeds-sync-inputs.yml`) is deleted —
its runner had no atlas, so it failed every run once publish needed one. `deploy/staging/sync-inputs.sh` is the path.
"""
from __future__ import annotations

import json

from pathlib import Path

from pipeline.common.curated import REPO_ROOT
from pipeline.deliver import sync_inputs as S
from pipeline.tests import under_test

MAC = REPO_ROOT / "deploy/staging/sync-inputs.sh"
PROD = REPO_ROOT / "deploy/prod/sync-inputs.sh"


def test_the_uploaders_stage_through_the_module():
    for p in (MAC, PROD):
        body = "\n".join(ln for ln in p.read_text(encoding="utf-8").splitlines() if not ln.lstrip().startswith("#"))
        assert "-m pipeline.deliver.sync_inputs" in body, p
        assert "bc_hydrometric_stations" not in body and "dfo_salmon" not in body, p


def test_publishing_is_from_the_mac_only():
    """(T520) no GitHub workflow puts the inputs: the deleted one, and no other naming the module."""
    wf = REPO_ROOT / ".github/workflows"
    assert not (wf / "feeds-sync-inputs.yml").exists()
    assert not [p.name for p in wf.glob("*.yml") if "pipeline.deliver.sync_inputs" in p.read_text(encoding="utf-8")]


def test_the_list_holds_the_wording_registry(tmp_path):
    got = S.stage(tmp_path, ("dfo-entries", "wording", "roster"))
    keys = [k for k, _ in got]
    assert "private/feeds/dfo/wording.json" in keys
    assert (tmp_path / "private/feeds/dfo/wording.json").read_bytes() == under_test.curated().dfo_salmon.wording.read_bytes()
    assert sum(k.startswith("private/feeds/dfo/entries/region-") for k in keys) == len(
        list(under_test.curated().dfo_salmon.places.glob("region-*.json")))
    roster = json.loads((tmp_path / "private/feeds/gauges/roster.json").read_text())
    assert roster and set(roster[0]) == {"station", "realtime"}
    assert all(c is None for _, c in got)                 # only the public in-season links carry a cache header


def test_an_unknown_input_is_refused(tmp_path):
    import pytest
    with pytest.raises(SystemExit):
        S.stage(tmp_path, ("wordings",))


def test_one_refused_input_does_not_hold_back_the_others(tmp_path, monkeypatch, capsys):
    """(T520) each input stages on its own: an in-season refusal is named (FAILED …, exit 1) and the DFO entries,
    wording and roster are still in the manifest."""
    def refuse(out):
        raise SystemExit("inseason.publish: refused — is-a: unresolved")
    monkeypatch.setattr(S, "INPUTS", tuple(S.Input(i.name, refuse if i.name == "inseason-matches" else i.stage, i.cache)
                                           for i in S.INPUTS))
    assert S.main(["--out", str(tmp_path)]) == 1
    got = capsys.readouterr()
    assert "private/feeds/dfo/wording.json" in got.out and "feeds/inseason/matches.json" not in got.out
    assert "FAILED inseason-matches: inseason.publish: refused — is-a: unresolved" in got.err
