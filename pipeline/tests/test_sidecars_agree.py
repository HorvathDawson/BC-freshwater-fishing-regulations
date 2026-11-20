"""T679 #15 (owner kept, 2026-11-20): `atlas.sidecars` is a SUPPORTED LABEL-ONLY PATH, and its files must be the atlas
build's own: on an atlas built by this code from today's curated files, a sidecar run writes every file of `SIDECARS` byte
for byte as the build did (`disagreements`). The no-data tests pin the comparison; the slow one runs it on the atlas under
test (`ATLAS_BUILD` names a side build)."""
from __future__ import annotations

import pytest

from pipeline.atlas import sidecars as SC
from pipeline.tests import under_test


def test_the_comparison_names_a_differing_and_a_missing_file(tmp_path):
    a, b = tmp_path / "build", tmp_path / "side"
    a.mkdir()
    b.mkdir()
    for d in (a, b):
        (d / "registry.json").write_text("{}")
    assert SC.disagreements(a, b) == []
    (b / "registry.json").write_text('{"items": []}')
    (a / "region_home.json").write_text("{}")
    assert SC.disagreements(a, b) == [f"region_home.json: missing in {b}", "registry.json"]


def test_every_file_a_sidecar_run_writes_is_a_sidecar_never_a_link_written_through():
    """`widening_fold_report.json` was written through its link into the source atlas (T679 #15)."""
    import inspect
    import re
    src = inspect.getsource(SC.write_sidecars)
    written = set(re.findall(r'dest / "([a-z_]+\.json)"', src)) | {"registry.json", "region_home.json"}
    assert written <= set(SC.SIDECARS), sorted(written - set(SC.SIDECARS))


@pytest.mark.slow
@pytest.mark.needs_atlas
def test_the_two_writers_agree_on_the_atlas_under_test(request, tmp_path):
    """The atlas under test must be built by this code from today's curated files (a side build, or the served atlas
    right after its build): a sidecar run beside it writes every sidecar byte for byte as the build did."""
    build = under_test.atlas(request)
    SC.write_sidecars(build, tmp_path / "side", log=lambda *_: None)
    assert SC.disagreements(build, tmp_path / "side") == []
