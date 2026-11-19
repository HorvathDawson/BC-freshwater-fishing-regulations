"""T918: the deploy scripts' own node tests, run from pytest so CI's tiers carry them.

`deploy/staging/testflight-distribute.test.mjs`: the TestFlight External-group step with App Store Connect mocked (token
shape, group picking, no-op reruns, the bounded processing wait). No network, no real key.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_testflight_distribute_node_tests_pass():
    r = subprocess.run(["node", "--test", "deploy/staging/testflight-distribute.test.mjs"], capture_output=True,
                       text=True, cwd=ROOT)
    assert r.returncode == 0, r.stdout[-3000:] + r.stderr[-2000:]
