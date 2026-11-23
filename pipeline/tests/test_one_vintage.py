"""T1001: `conftest.one_vintage` — a test that resolves on the release under test from the served atlas fails with ONE
line naming both vintages and what to run when they differ (between a promote and a release), and passes on a matching
pair. On an invented atlas and release (no data)."""
from __future__ import annotations

import json

import pytest

from pipeline.common import section_handles as H
from pipeline.tests.conftest import one_vintage


def _pair(tmp_path, release_handles: str | None = None):
    """An atlas and a release; the release's core names `release_handles`, else the atlas's own handles."""
    atlas = tmp_path / "full"
    atlas.mkdir()
    (atlas / H.FILENAME).write_text("A:0\nlake:1\n")
    rel = tmp_path / "r-000000000001"
    rel.mkdir()
    (rel / "manifest.json").write_text(json.dumps(
        {"files": {"core": {"section_handles": release_handles or H.digest_for(atlas)}}}))
    return atlas, rel


def test_a_matching_pair_passes(tmp_path):
    one_vintage(*_pair(tmp_path))


def test_an_atlas_ahead_of_the_release_fails_naming_both_and_what_to_run(tmp_path):
    atlas, rel = _pair(tmp_path, "0123456789abcdef")
    with pytest.raises(pytest.fail.Exception) as e:
        one_vintage(atlas, rel)
    msg = str(e.value)
    assert msg == (f"atlas build full (section handles {H.digest_for(atlas)}) is not the release under test "
                   "r-000000000001's (tiles 0123456789abcdef): between a promote and a release that is expected — "
                   "release it (python -m pipeline release --staging), or point RELEASE_DIR at the candidate")
