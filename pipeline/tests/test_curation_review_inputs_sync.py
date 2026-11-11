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
from pipeline.tests import under_test


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
    assert IS.touches([under_test.curated().inseason.notice_waters], REPO)
    assert IS.touches([Path(under_test.curated().dfo_salmon.places) / "region-2.json"], REPO)
    assert not IS.touches([under_test.curated().waters.cuts], REPO)


def test_the_default_command_is_the_staging_script():
    s = IS.InputsSync(REPO, enabled=False)
    assert s.cmd == [str(REPO / "deploy/staging/sync-inputs.sh")]
    assert "bc-fishing-regulations-staging" in (REPO / "deploy/staging/sync-inputs.sh").read_text()


def test_each_input_that_did_not_go_up_is_named(tmp_path):
    s = _sync(tmp_path, 'echo "FAILED inseason-matches: refused — is-a: unresolved" >&2; exit 1\n')
    s.request("inseason-match")
    assert s.wait_idle(10)
    assert s.status()["failed"] == {"inseason-matches": "refused — is-a: unresolved"}


def test_it_runs_from_the_review_app_mode(tmp_path):
    """The script is told it runs from the review app: it then refuses to rewrite the matches (re-resolve here)."""
    s = _sync(tmp_path, f'echo "$SYNC_INPUTS_FROM_REVIEW" > "{tmp_path}/mode"\n')
    s.request("inseason-match")
    assert s.wait_idle(10) and (tmp_path / "mode").read_text().strip() == "1"


def test_a_save_after_a_run_ends_starts_a_new_run(tmp_path):
    """The worker clears itself under the lock as it ends: a request right after is never dropped."""
    s = _sync(tmp_path, f'echo run >> "{tmp_path}/runs"\n')
    for _ in range(3):
        s.request("inseason-match")
        assert s.wait_idle(10)
    assert (tmp_path / "runs").read_text().count("run") == 3


def test_it_fails_closed(monkeypatch):
    """Off unless CURATION_SYNC_INPUTS=1, and never over a sandboxed curated tree."""
    for env, on in (({}, False), ({"CURATION_SYNC_INPUTS": "0"}, False), ({"CURATION_SYNC_INPUTS": "1"}, True),
                    ({"CURATION_SYNC_INPUTS": "1", "CURATED_ROOT": "/tmp/x"}, False)):
        for k in ("CURATION_SYNC_INPUTS", "CURATED_ROOT"):
            monkeypatch.delenv(k, raising=False)
        for k, v in env.items():
            monkeypatch.setenv(k, v)
        assert IS.from_env(REPO).enabled is on, env


def test_it_is_spawned_never_forked(tmp_path, monkeypatch):
    """A fork would run PROJ's atfork child handler before exec and could crash there (rc -11, no output: it did, once
    an earlier test had loaded pyproj). With fork made impossible, the run still happens, in the app's root."""
    import subprocess

    def no_fork(*_a, **_k):
        raise AssertionError("inputs sync forked: it must posix_spawn")
    monkeypatch.setattr(subprocess, "_fork_exec", no_fork)
    s = _sync(tmp_path, 'pwd -P > where\n')
    s.request("inseason-match")
    assert s.wait_idle(10)
    assert s.status()["state"] == "done", s.status()
    assert (tmp_path / "where").read_text().strip() == str(tmp_path.resolve())
