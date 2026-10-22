"""`scripts/update-hydat.sh` RUN, against stubs (owner 2026-10-22 late: HYDAT first).

The script is copied into a throwaway tree whose `.venv/bin/python` and `scripts/heavy.sh` are stubs that only log
what they were asked, from where. Nothing is downloaded, built or sent. What is pinned is what the owner's one command
actually does: every HYDAT consumer in order, on the SERVED feed directory (the one `pipeline build` packs clim.json
from), and it stops before rebuilding anything when the fetch did not land the newest HYDAT (fetch_data.py reports a
failed listing or download on stdout and still exits 0).
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SERVED = "data/generated/gauges/feeds"

PYTHON_STUB = """#!/usr/bin/env bash
here=$(pwd); echo "${here#$TREE}|$*" >> "$TREE/log"
case "$*" in
  *"fetch_data.py --layers hydat"*) [ -n "$LANDS" ] && printf %s "$LANDS" > source/hydat.release ;;
  *hydat_latest_release*) printf '%s\\n' "$LISTED" ;;
esac
exit 0
"""
HEAVY_STUB = """#!/usr/bin/env bash
echo "heavy|$*" >> "$TREE/log"
"""


def _exe(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)
    p.chmod(p.stat().st_mode | stat.S_IXUSR)


@pytest.fixture
def tree(tmp_path):
    (tmp_path / "scripts").mkdir()
    shutil.copy(ROOT / "scripts/update-hydat.sh", tmp_path / "scripts/update-hydat.sh")
    _exe(tmp_path / ".venv/bin/python", PYTHON_STUB)
    _exe(tmp_path / "scripts/heavy.sh", HEAVY_STUB)
    (tmp_path / "data/source").mkdir(parents=True)
    (tmp_path / "data/source/hydat.release").write_text("20260717")
    return tmp_path


def _run(tree: Path, lands: str, listed: str, **env) -> subprocess.CompletedProcess:
    e = {**os.environ, "TREE": str(tree), "LANDS": lands, "LISTED": listed, **env}
    return subprocess.run(["bash", str(tree / "scripts/update-hydat.sh")], env=e, capture_output=True, text=True)


def _log(tree: Path) -> list[str]:
    lines = (tree / "log").read_text().splitlines()
    # the listing check is a python -c one-liner: pin that it asks hydat_latest_release, not its exact text
    return ["|-c <latest>" if "|-c " in ln and "hydat_latest_release" in ln else ln for ln in lines]


def test_a_new_hydat_reruns_every_consumer_in_order_on_the_served_feed(tree):
    r = _run(tree, lands="20261020", listed="20261020")
    assert r.returncode == 0, r.stderr
    assert _log(tree) == [
        "/data|fetch_data.py --layers hydat",
        "|-c <latest>",
        f"|-m pipeline.gauges.feed.climatology --hydat data/source/hydat.sqlite3 --out {SERVED}/clim.json",
        f"|-m pipeline.gauges.feed.publish --out {SERVED} --clim {SERVED}/clim.json",
        "heavy|build 40 -- .venv/bin/python -m pipeline build",
    ]


def test_another_feed_dir_is_not_honoured(tree):
    # `pipeline build` packs the bundle from the SERVED clim.json only; an envelope written elsewhere would leave the
    # bundle on the old HYDAT while the script reported it rebuilt. The committed dev fixture is never touched.
    r = _run(tree, lands="20261020", listed="20261020", FEED="app/packages/data/dev/feeds")
    assert r.returncode == 0, r.stderr
    assert not any("app/packages/data/dev/feeds" in ln for ln in _log(tree))
    assert f"|-m pipeline.gauges.feed.publish --out {SERVED} --clim {SERVED}/clim.json" in _log(tree)


def test_a_fetch_that_did_not_land_the_newest_stops_before_any_rebuild(tree):
    # the download failed (fetch_data prints and exits 0): the marker is still the old release
    r = _run(tree, lands="", listed="20261020")
    assert r.returncode != 0
    assert _log(tree) == ["/data|fetch_data.py --layers hydat", "|-c <latest>"]
    assert "HYDAT 20261020 is listed but data/source holds 20260717" in r.stderr


def test_an_unreadable_listing_stops_before_any_rebuild(tree):
    r = _run(tree, lands="", listed="")
    assert r.returncode != 0
    assert _log(tree) == ["/data|fetch_data.py --layers hydat", "|-c <latest>"]
    assert "could not read the HYDAT listing" in r.stderr
