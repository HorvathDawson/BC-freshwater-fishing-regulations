"""T667: `pipeline/tests/under_test.py` — section names over a handle table, and which release is under test.

Mutation style: a RENUMBERED table (other sections added, every handle shifted) changes no named answer; a section that
MOVED fails by its name. The release: the candidate the environment names, else LATEST, never a guess."""
from __future__ import annotations

import pytest

from pipeline.common import section_handles as H
from pipeline.tests import under_test

#: a small atlas: two blue lines and a lake
IDS = ["356075101:0", "356075101:1200", "356075101:5400", "380887781:0", "380887781:58646", "lake:-1174"]


def build(tmp_path, name, ids):
    d = tmp_path / name
    d.mkdir()
    H.write(ids, d)
    return d


def named(n: under_test.SectionNames) -> list[str]:
    hs = [*n.streams([(356075101, 1200), (380887781, 58646)]), n.lake(-1174)]
    return [n.name(h) for h in hs]


def test_a_renumbered_table_gives_the_same_named_sections(tmp_path):
    a = under_test.sections(build=build(tmp_path, "a", IDS))
    # the next build cut other lines: every handle after them moved
    b = under_test.sections(build=build(tmp_path, "b", ["100000001:0", "356075101:300", *IDS, "lake:-9"]))
    assert a.stream(356075101, 1200) == 2 and b.stream(356075101, 1200) == 4      # the NUMBER moved
    assert a.lake(-1174) != b.lake(-1174)
    assert named(a) == named(b) == ["356075101:1200", "380887781:58646", "lake:-1174"]   # the NAME did not


def test_a_moved_section_fails_by_name(tmp_path):
    moved = [i if i != "356075101:1200" else "356075101:1300" for i in IDS]      # the cut moved 100 m
    n = under_test.sections(build=build(tmp_path, "moved", moved))
    with pytest.raises(under_test.NoSuchSection, match=r"no stream section starts at 356075101:1200 in .*moved"):
        n.stream(356075101, 1200)
    with pytest.raises(under_test.NoSuchSection, match="356075101:1200"):
        n.streams([(356075101, 0), (356075101, 1200)])
    assert n.stream(356075101, 1300) == 2                                           # named where it starts now
    with pytest.raises(under_test.NoSuchSection, match=r"no lake section lake:-5 in"):
        n.lake(-5)


def release_in(root, rid):
    (root / rid).mkdir(parents=True)
    (root / rid / "manifest.json").write_text("{}")
    return root / rid


def test_the_release_under_test_is_the_candidate_when_named_else_latest(tmp_path):
    live, cand = release_in(tmp_path, "r-live"), release_in(tmp_path, "r-cand")
    (tmp_path / "LATEST").write_text("r-live\n")
    (tmp_path / "CANDIDATE").write_text("r-cand\n")                   # a waiting candidate is not picked by itself
    assert under_test.release_at({}, tmp_path) == live
    assert under_test.release_at({under_test.RELEASE_ENV: str(cand)}, tmp_path) == cand


def test_no_release_is_never_answered_by_another(tmp_path):
    release_in(tmp_path, "r-newest")
    with pytest.raises(LookupError, match="LATEST is missing and RELEASE_DIR is unset"):
        under_test.release_at({}, tmp_path)                           # no pointer: not the newest folder
    (tmp_path / "LATEST").write_text("r-newest\n")
    with pytest.raises(LookupError, match=r"RELEASE_DIR=.*r-gone: no release there"):
        under_test.release_at({under_test.RELEASE_ENV: str(tmp_path / "r-gone")}, tmp_path)   # not passed over for LATEST
    (tmp_path / "LATEST").write_text("r-gone\n")
    with pytest.raises(LookupError, match="names r-gone, which has no manifest"):
        under_test.release_at({}, tmp_path)


def test_an_accessor_refuses_a_test_without_its_marker(request):
    """Every accessor goes through `need`: this test carries no `needs_bundle`, so the read is refused."""
    with pytest.raises(pytest.fail.Exception, match="without the `needs_bundle` marker"):
        under_test.bundle(request)
    with pytest.raises(pytest.fail.Exception, match="without the `needs_atlas` marker"):
        under_test.sections(request)
    with pytest.raises(pytest.fail.Exception, match="without the `needs_bundle` marker"):
        under_test.release(request)


def test_the_overrides_move_what_is_under_test(tmp_path, monkeypatch):
    for k in ("UI_EXPORT_BUNDLE", "UI_EXPORT_VERDICTS", "STATUS_INDEX", "ANSWERS_EXPORT_DIR", "UI_EXPORT_DIR",
              "ATLAS_BUILD", "UI_EXPORT_REACHES", "UI_EXPORT_TILES"):
        monkeypatch.delenv(k, raising=False)
    live = {k: under_test.path(k) for k in ("bundle", "verdicts", "status_index", "export_dir", "atlas", "reach_run")}
    assert live["verdicts"] == live["bundle"].with_name("verdicts.sqlite")
    assert live["reach_run"].name == live["atlas"].name                     # the run named for the build
    monkeypatch.setenv("UI_EXPORT_BUNDLE", str(tmp_path / "bundle.sqlite"))
    assert under_test.path("verdicts") == tmp_path / "verdicts.sqlite"     # a side bundle brings its own kin
    assert under_test.path("status_index") == tmp_path / "status_index.bin"
    monkeypatch.setenv("STATUS_INDEX", str(tmp_path / "si.bin"))
    assert under_test.path("status_index") == tmp_path / "si.bin"
    monkeypatch.setenv("UI_EXPORT_DIR", str(tmp_path / "out"))
    assert under_test.release_root() == tmp_path / "out" / "release"
    assert under_test.path("export_dir") == tmp_path / "out" / "regs"
    monkeypatch.setenv("ATLAS_BUILD", str(tmp_path / "side"))
    assert under_test.path("atlas") == tmp_path / "side" and under_test.path("reach_run").name == "side"
    with pytest.raises(KeyError, match="no 'bundel'"):
        under_test.path("bundel")


@pytest.mark.needs_reach_run
def test_a_run_named_by_a_bundle_is_found_by_that_name_and_a_side_run_wins(tmp_path, monkeypatch, request):
    monkeypatch.delenv("UI_EXPORT_REACHES", raising=False)
    with pytest.raises(pytest.fail.Exception, match=r"missing reach_run: .*reaches/no_such_run_t667"):
        under_test.reach_run_named("no_such_run_t667", request)        # by the NAME given, not the atlas's
    monkeypatch.setenv("UI_EXPORT_REACHES", str(tmp_path))
    assert under_test.reach_run_named("no_such_run_t667", request) == tmp_path
