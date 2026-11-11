"""T554 item 4: a release upload never puts an older HYDAT's clim.json back over R2's newer one."""

from __future__ import annotations

import json
import os
import subprocess
import sys

from pipeline.gauges.feed.clim_guard import decide, release_of_head


def test_decide_puts_newer_or_same_and_skips_older_or_unknown():
    assert decide("20261019", None, False)[0] is True                     # nothing on R2
    assert decide("20261019", "20260717", True)[0] is True
    assert decide("20261019", "20261019", True)[0] is True                # same HYDAT: --checksum decides
    put, why = decide("20260717", "20261019", True)
    assert not put and "newer than the local 20260717" in why and "scripts/update-hydat.sh" in why
    assert decide("unknown", "20261019", True)[0] is False
    assert decide("20261019", None, True)[0] is False                     # R2's names none: never overwritten


def test_the_release_is_read_from_the_head():
    assert release_of_head('{"release": "20261019", "percentiles": [') == "20261019"
    assert release_of_head('{"percentiles": [1, 2') is None


def _run(tmp_path, remote_head: str | None, local: str = "20260717"):
    (tmp_path / "bin").mkdir(exist_ok=True)
    rc = tmp_path / "bin/rclone"
    if remote_head is None:
        rc.write_text('#!/usr/bin/env bash\necho "directory not found" >&2\nexit 3\n')
    else:
        (tmp_path / "head.txt").write_text(remote_head)
        rc.write_text(f'#!/usr/bin/env bash\nif [[ $1 == lsf ]]; then echo clim.json; else cat "{tmp_path}/head.txt"; fi\n')
    rc.chmod(0o755)
    (tmp_path / "clim.json").write_text(json.dumps({"release": local, "stations": {}}))
    env = {**os.environ, "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}"}
    return subprocess.run([sys.executable, "-m", "pipeline.gauges.feed.clim_guard", "--local", str(tmp_path / "clim.json"),
                           "--remote", "r2:b/feeds/live/clim.json"], capture_output=True, text=True, env=env)


def test_the_cli_against_a_stub_rclone(tmp_path):
    older = _run(tmp_path, '{"release": "20261019", "percentiles"')
    assert older.returncode == 3 and "newer than the local 20260717" in older.stderr
    assert _run(tmp_path, '{"release": "20250101", "x"').returncode == 0
    assert _run(tmp_path, None).returncode == 0                            # R2 holds none
