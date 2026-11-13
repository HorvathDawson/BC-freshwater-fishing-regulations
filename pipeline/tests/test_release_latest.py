"""T72: the release LATEST names is current, whatever a later touch does to an older folder's mtime; the prune keeps
LATEST, PUBLISHED, one previous and --keep, and removes only the rest."""
import json
import os
import time

import pytest

from pipeline.deliver.release.manifest import latest, live, write_latest
from pipeline.deliver.release import prune as P


def _rel(root, rid, at):
    d = root / rid
    d.mkdir(parents=True)
    (d / "manifest.json").write_text(json.dumps({"data_release": rid}))
    os.utime(d / "manifest.json", (at, at))
    return d


@pytest.fixture
def root(tmp_path):
    now = time.time()
    for i, rid in enumerate(["r-old", "r-mid", "r-new", "r-pub"]):
        _rel(tmp_path, rid, now - 1000 + i * 100)
    return tmp_path


def test_a_touched_older_release_is_never_picked(root):
    write_latest(root, "r-mid")
    os.utime(root / "r-old" / "manifest.json")          # touched after the release step: newest by mtime
    assert latest(root).name == "r-mid"


def test_without_a_pointer_the_newest_manifest_stands_in(root):
    assert latest(root).name == "r-pub"


def test_a_pointer_to_a_missing_release_is_refused(root):
    write_latest(root, "r-gone")
    with pytest.raises(SystemExit):
        latest(root)


def test_the_prune_keeps_latest_published_previous_and_keep(root):
    _rel(root, "r-extra", time.time() - 5000)
    write_latest(root, "r-mid")
    (root / "PUBLISHED").write_text("r-old\n")
    kept, gone = P.plan(root, keep=["r-extra"])
    assert {d.name for d in kept} == {"r-mid", "r-old", "r-pub", "r-extra"}   # r-pub: the newest other one
    assert [d.name for d in gone] == ["r-new"]


def test_the_dry_run_removes_nothing(root, capsys):
    write_latest(root, "r-new")
    P.main(["--root", str(root)])
    assert sorted(p.name for p in root.glob("r-*")) == ["r-mid", "r-new", "r-old", "r-pub"]
    assert "would remove r-old" in capsys.readouterr().out
    P.main(["--root", str(root), "--yes"])
    assert sorted(p.name for p in root.glob("r-*")) == ["r-new", "r-pub"]


def test_the_live_release_has_no_mtime_fallback(root):
    """T560 item 6 / T601 item 2: `live` reads only the pointer, and refuses what `latest` would guess or half-read."""
    with pytest.raises(SystemExit, match="no release is live"):
        live(root)                                       # `latest` would answer r-pub by mtime
    write_latest(root, "r-mid")
    assert live(root).name == "r-mid"
    write_latest(root, "r-gone")
    with pytest.raises(SystemExit, match="has no manifest"):
        live(root)


def test_no_pipeline_module_reads_the_latest_pointer_by_hand():
    """T560 item 6: the live release is read through `manifest.live` / `latest`, never `(root / "LATEST").read_text()`
    (those readers went on with a pruned or half-copied folder and failed later, or read a partial release)."""
    import re
    from pipeline.common.curated import REPO_ROOT
    pat = re.compile(r"""["']LATEST["']\s*\)\s*\.read_text""")
    hits = [str(p.relative_to(REPO_ROOT)) for p in sorted((REPO_ROOT / "pipeline").rglob("*.py"))
            if "tests" not in p.parts and p.name != "manifest.py" and pat.search(p.read_text(encoding="utf-8"))]
    assert not hits, f"read the live release through pipeline.deliver.release.manifest.live: {hits}"
