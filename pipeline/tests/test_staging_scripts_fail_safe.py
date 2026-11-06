"""T433 (audit 2026-11-05): the staging scripts fail safe.

  upload        the bucket is cleared AFTER the pointer flip, sparing what the new release names (a failure above it
                once left staging with no manifest)
  sync-inputs   a failed rclone copy stops the script (run, against a stub rclone); it once printed "in sync"
  hydat         fetch_data's hydat layer downloads through the checked scripts/hydat-fetch.sh and raises on an
                unreadable listing
  check-public  a failed secret read is not swallowed (a secret it cannot read is one it cannot look for)
  testflight    --production needs CONFIRM_PROD, before the heavy queue (run)
"""
from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _exe(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)
    p.chmod(p.stat().st_mode | stat.S_IXUSR)


def test_upload_clears_after_the_pointer_and_spares_the_release():
    lines = (ROOT / "deploy/staging/upload.sh").read_text().splitlines()
    run = [ln for ln in lines if not ln.lstrip().startswith("#")]
    pointer = next(i for i, ln in enumerate(run) if 'cp_ "$M" release/manifest.json' in ln)
    clear = [i for i, ln in enumerate(run) if ln.startswith("rclone delete")]
    assert clear and all(i > pointer for i in clear), "the clear must come after the pointer flip"
    cmd = " ".join(run[clear[0]:clear[0] + 3])
    for spared in ('"release/manifest.json"', '"release/tiles/$TILES/**"', '"release/$REL/**"', '"release/$BM"',
                   '"release/$SYN"', '"feeds/**"', '"private/**"', '"runtiming/**"', '"test/**"'):
        assert f"--exclude {spared}" in cmd, spared


PY_STUB = """#!/usr/bin/env bash
# pipeline.deliver.sync_inputs --out <dir>: two inputs
out=$4; mkdir -p "$out/feeds/inseason" "$out/dfo"
echo '{"matches": [1, 2]}' > "$out/feeds/inseason/matches.json"; echo '{}' > "$out/dfo/a.json"
printf 'dfo/a.json\\t-\\nfeeds/inseason/matches.json\\tno-cache\\n'
"""


@pytest.fixture
def sync_tree(tmp_path):
    (tmp_path / "deploy/staging").mkdir(parents=True)
    shutil.copy(ROOT / "deploy/staging/sync-inputs.sh", tmp_path / "deploy/staging/sync-inputs.sh")
    _exe(tmp_path / ".venv/bin/python", PY_STUB)
    return tmp_path


def _sync(tree: Path, rclone_rc: int) -> subprocess.CompletedProcess:
    _exe(tree / "bin/rclone", f'#!/usr/bin/env bash\necho "$*" >> "{tree}/rclone.log"\n'
                              f'echo "ERROR : boom" >&2\nexit {rclone_rc}\n' if rclone_rc else
         f'#!/usr/bin/env bash\necho "$*" >> "{tree}/rclone.log"\necho "INFO  : x: Copied (new)" >&2\n')
    env = {**os.environ, "PATH": f"{tree / 'bin'}:{os.environ['PATH']}"}
    return subprocess.run(["bash", str(tree / "deploy/staging/sync-inputs.sh")], env=env, capture_output=True,
                          text=True)


def test_sync_inputs_stops_on_a_failed_copy(sync_tree):
    r = _sync(sync_tree, 1)
    assert r.returncode != 0
    assert "inputs in sync" not in r.stdout
    assert "sync-inputs: dfo/a.json failed" in r.stderr
    assert len((sync_tree / "rclone.log").read_text().splitlines()) == 1     # nothing after the failed one


def test_sync_inputs_reports_a_clean_copy(sync_tree):
    r = _sync(sync_tree, 0)
    assert r.returncode == 0, r.stderr
    assert "inputs in sync: r2:bc-fishing-regulations-staging (2 objects, in-season matches 2)" in r.stdout


@pytest.fixture
def fetch_data(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "data"))
    import fetch_data as F
    return F


def test_hydat_layer_uses_the_checked_downloader(fetch_data, tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(fetch_data, "hydat_latest_release", lambda: "20261020")
    monkeypatch.setattr(subprocess, "run", lambda cmd, **k: calls.append((cmd, k)))
    fetch_data.fetch_hydat("hydat", tmp_path / "hydat.sqlite3")
    assert calls == [(["bash", str(ROOT / "scripts/hydat-fetch.sh"), "20261020", str(tmp_path / "hydat-download"),
                       str(tmp_path / "hydat.sqlite3")], {"check": True})]


def test_hydat_layer_raises_on_an_unreadable_listing(fetch_data, tmp_path, monkeypatch):
    monkeypatch.setattr(fetch_data, "hydat_latest_release", lambda: None)
    with pytest.raises(RuntimeError, match="could not read the HYDAT listing"):
        fetch_data.fetch_hydat("hydat", tmp_path / "hydat.sqlite3")


def test_check_public_does_not_swallow_a_failed_secret_read():
    src = (ROOT / "deploy/staging/check-public.mjs").read_text()
    read = src[src.index("function readPrivate"):src.index("/** the secret VALUES")]
    assert "catch" not in read, "readPrivate swallows a failed read"
    dump = src[src.index("'config', 'dump'") - 200:src.index("'config', 'dump'")]
    assert "try {" not in dump.splitlines()[-1], "a failed rclone config dump is swallowed"
    assert "e.code !== 'ENOENT'" in src                        # only a missing .run-token is a skip


def test_testflight_production_needs_confirm_prod():
    env = {k: v for k, v in os.environ.items() if k not in ("CONFIRM_PROD", "BUILD_IOS_INNER")}
    # --dry-run: without the guard this only prints its plan (exit 0); never a build, never the heavy queue
    r = subprocess.run(["bash", str(ROOT / "deploy/staging/build-ios-testflight.sh"), "--production", "--dry-run"], env=env,
                       capture_output=True, text=True, timeout=30)
    assert r.returncode == 2
    assert "set CONFIRM_PROD=canifishthis.ca" in r.stderr
