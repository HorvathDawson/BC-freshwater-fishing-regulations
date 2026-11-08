"""T520 (owner 2026-11-08: publish from the Mac only): a saved input runs THE Mac path (deploy/staging/sync-inputs.sh)
in the background — debounced, one at a time, its outcome said (status) and a failure logged, never silent. Tests run
a stand-in command; nothing reaches R2."""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "curation-review" / "backend"))

import inputs_sync as IS  # noqa: E402


def _sync(tmp_path, script: str) -> IS.InputsSync:
    cmd = tmp_path / "cmd.sh"
    cmd.write_text("#!/usr/bin/env bash\n" + script)
    cmd.chmod(0o755)
    return IS.InputsSync(tmp_path, enabled=True, cmd=[str(cmd)], log_path=tmp_path / "log.jsonl", debounce=0.05)


def test_a_save_runs_the_mac_path_once_for_several(tmp_path):
    s = _sync(tmp_path, f'echo run >> "{tmp_path}/runs"; echo "inputs in sync"\n')
    for r in ("inseason-match", "inseason-match", "write-through"):
        s.request(r)
    assert s.wait_idle(10)
    st = s.status()
    assert st["state"] == "done" and st["rc"] == 0 and "inputs in sync" in st["output"]
    assert (tmp_path / "runs").read_text().count("run") == 1          # debounced: one run
    assert not (tmp_path / "log.jsonl").exists()


def test_a_refusal_is_said_and_logged(tmp_path):
    s = _sync(tmp_path, 'echo "inseason.publish: refused — is-a: unresolved" >&2; exit 1\n')
    s.request("inseason-match")
    assert s.wait_idle(10)
    st = s.status()
    assert st["state"] == "failed" and "refused" in st["output"]
    (row,) = [json.loads(x) for x in (tmp_path / "log.jsonl").read_text().splitlines()]
    assert row["rc"] == 1 and row["reasons"] == ["inseason-match"]


def test_off_says_so_and_runs_nothing(tmp_path):
    s = IS.InputsSync(tmp_path, enabled=False)
    assert s.request("inseason-match")["state"] == "off"


def test_which_files_are_inputs():
    from pipeline.common.curated import CURATED
    assert IS.touches([CURATED.inseason.notice_waters], REPO)
    assert IS.touches([Path(CURATED.dfo_salmon.places) / "region-2.json"], REPO)
    assert not IS.touches([REPO / "data/curated/waters/cuts.json"], REPO)


def test_the_default_command_is_the_staging_script():
    s = IS.InputsSync(REPO, enabled=False)
    assert s.cmd == [str(REPO / "deploy/staging/sync-inputs.sh")]
    assert "bc-fishing-regulations-staging" in (REPO / "deploy/staging/sync-inputs.sh").read_text()
