"""T666 (one release path): the table of what follows the release, the tests found by rule, and the pieces the release
command stands on that touch real git / real scripts — each on a scratch repo or a stand-in tool, never this checkout's
state and never a real release. The orchestration itself is `test_release_command.py`."""
from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import types
from pathlib import Path

import pytest

from pipeline import release as R
from pipeline import release_follows as RF

REPO = Path(R.REPO_ROOT)


# ---------------------------------------------------------------------------------------------------------- the table
def test_every_row_names_paths_that_exist_and_no_two_rows_share_one():
    tracked = set(subprocess.run(["git", "ls-files"], cwd=REPO, capture_output=True, text=True, check=True).stdout.split("\n"))
    seen: dict[str, str] = {}
    for row in RF.FOLLOWS:
        for p in row.paths:
            assert p in tracked or any(t.startswith(p + "/") for t in tracked), f"{row.name}: {p} is not in git"
            assert p not in seen, f"{p} is in two rows ({seen.get(p)}, {row.name})"
            seen[p] = row.name
        assert row.refresh in (RF.RECUT, RF.INSEASON, None)
    assert [r.name for r in RF.FOLLOWS] == ["goldens", "page-oracle", "regs-fixture", "golden-pages", "inseason", "baseline"]


def test_the_table_is_the_source_the_release_and_the_tools_agree_with():
    """One table: the release's in-season file, the goldens check's taps.json and the re-cut's own outputs are rows."""
    assert RF.paths_of(RF.INSEASON) == (str(Path(R.INSEASON).resolve().relative_to(REPO.resolve())),)
    recut = RF.paths_of(RF.RECUT)
    sync = (REPO / "app/tools/regs-recut/sync.mjs").read_text(encoding="utf-8")
    for p in recut:                                           # every re-cut path is one sync.mjs says it writes
        assert p.removeprefix("app/packages/data/") in sync or p in sync, p
    paired = (REPO / "scripts/check-goldens-paired.py").read_text(encoding="utf-8")
    assert "app/fixtures/golden/taps.json" in recut and "app/fixtures/golden/taps.json" in paired
    assert RF.refresh_cmd(RF.RECUT, Path("/r/r-x")) == ["node", "app/tools/regs-recut/sync.mjs", "--release", "/r/r-x"]
    assert RF.refresh_cmd(RF.INSEASON, Path("/r/r-x"))[1:] == ["-m", "pipeline.regs.inseason.resolve", "--release", "/r/r-x"]
    with pytest.raises(KeyError):
        RF.refresh_cmd("nope", Path("/r"))


def test_only_a_golden_page_is_an_answer_change():
    """T667 D5: renumber-only changes are committed; a golden page's words, or the page oracle, stop the release."""
    renumber = ["app/fixtures/golden/taps.json", "app/fixtures/golden/hydrate.json",
                "app/packages/data/dev/regs-release/r-1/water-aa.sqlite.gz", "app/packages/data/dev/section-lines.json"]
    assert RF.answer_paths(renumber) == []
    page = "app/packages/ui-native/src/regs/fixtures/glenn_501.json"
    assert RF.answer_paths(renumber + [page]) == [page]
    # (T667 5b) page v36's code over the release names rules by durable key: it moves only when an answer does
    oracle = "app/fixtures/golden/page-oracle.json"
    assert RF.answer_paths(renumber + [oracle]) == [oracle] and RF.row_of(oracle).name == "page-oracle"
    assert RF.row_of(page).name == "golden-pages" and RF.row_of("app/packages/ui-native/src/regs/fixturesX.json") is None
    assert RF.row_of("pipeline/release.py") is None


# ------------------------------------------------------------------------------------------- the tests, found by rule
def test_the_tests_that_compared_with_latest_are_found_by_the_rule():
    """Build 27's red-until-promote tests ask for the release under test now; the rule finds them with no list."""
    py = RF.pipeline_tests()
    assert {"pipeline/tests/test_paint_sync.py", "pipeline/tests/test_unnamed_trib_sweep.py"} <= set(py)
    app = RF.app_tests()
    assert {"conformance/src/regs/golden.test.ts", "tools/release-boot.test.ts"} <= set(app)
    assert all(not a.startswith("app/") and "node_modules" not in a for a in app)
    assert RF.app_tests_cmd(app)[:6] == ["pnpm", "-C", "app", "exec", "vitest", "run"]
    assert RF.test_env(Path("/r/r-x")) == {"RELEASE_DIR": "/r/r-x", "REGS_STORE_REQUIRED": "1"}


def test_a_new_test_that_asks_for_the_release_needs_no_list_edit(tmp_path):
    (tmp_path / "pipeline/tests").mkdir(parents=True)
    (tmp_path / "pipeline/tests/test_new.py").write_text("def test_x(request):\n    d = under_test.release(request)\n")
    (tmp_path / "pipeline/tests/test_plain.py").write_text("def test_y():\n    assert released == 1\n")
    (tmp_path / "pipeline/tests/helper.py").write_text("under_test.release()\n")
    assert RF.pipeline_tests(tmp_path) == ["pipeline/tests/test_new.py"]
    for rel_, body in (("app/tools/a.test.ts", "const d = liveRelease();"), ("app/x/b.release.test.ts", "it('x')"),
                       ("app/x/c.test.tsx", "const d = namedRelease();"), ("app/x/plain.test.ts", "it('y')"),
                       ("app/node_modules/p/d.test.ts", "liveRelease()"), ("app/x/notatest.ts", "liveRelease()")):
        f = tmp_path / rel_
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(body)
    assert RF.app_tests(tmp_path) == ["tools/a.test.ts", "x/b.release.test.ts", "x/c.test.tsx"]


# ------------------------------------------------------------------------------------------- real git, a scratch repo
def _git(d: Path, *a: str) -> str:
    return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *a], cwd=d, capture_output=True,
                          text=True, check=True).stdout.strip()


@pytest.fixture
def repo(tmp_path, monkeypatch):
    d = tmp_path / "repo"
    (d / "app/fixtures/golden").mkdir(parents=True)
    (d / "app/packages/ui-native/src/regs/fixtures").mkdir(parents=True)
    (d / "app/fixtures/golden/taps.json").write_text("{}\n")
    (d / "app/packages/ui-native/src/regs/fixtures/a.json").write_text("{}\n")
    (d / "other.py").write_text("x = 1\n")
    _git(d, "init", "-q")
    _git(d, "add", "-A")
    _git(d, "commit", "-q", "-m", "first")
    monkeypatch.setattr(R, "REPO_ROOT", d)
    monkeypatch.setattr(RF, "REPO_ROOT", d)
    for k in ("GIT_AUTHOR_NAME", "GIT_COMMITTER_NAME"):
        monkeypatch.setenv(k, "t")
    for k in ("GIT_AUTHOR_EMAIL", "GIT_COMMITTER_EMAIL"):
        monkeypatch.setenv(k, "t@t")
    return d


def test_the_tree_key_follows_content_not_head(repo):
    """T666 item 5: fast-fail's pass is keyed on what it reads. A new HEAD over the same content keeps the key; any
    edit — committed, uncommitted or a new untracked file — moves it; undoing the edit brings it back."""
    k0, h0 = R.tree_key(), _git(repo, "rev-parse", "HEAD")
    _git(repo, "commit", "-q", "--allow-empty", "-m", "a message only")
    assert _git(repo, "rev-parse", "HEAD") != h0 and R.tree_key() == k0
    (repo / "other.py").write_text("x = 2\n")
    k1 = R.tree_key()
    assert k1 != k0 and R.dirty_paths() == ["other.py"] and R.changed_since(h0) == {"other.py"}
    _git(repo, "commit", "-q", "-am", "the edit, committed")
    assert R.tree_key() == k1 and R.dirty_paths() == [] and R.changed_since(h0) == {"other.py"}
    (repo / "new.txt").write_text("untracked\n")
    k2 = R.tree_key()
    assert k2 != k1
    (repo / "new.txt").write_text("untracked, edited\n")
    assert R.tree_key() not in (k1, k2)
    (repo / "new.txt").unlink()
    assert R.tree_key() == k1


def test_the_recut_commit_takes_only_the_tables_paths(repo):
    """Several lanes share the checkout: the release's commit carries the re-cut fixtures and nothing else."""
    (repo / "app/fixtures/golden/taps.json").write_text('{"cut": 2}\n')
    (repo / "app/packages/ui-native/src/regs/fixtures/a.json").unlink()
    (repo / "app/packages/ui-native/src/regs/fixtures/b.json").write_text("{}\n")
    (repo / "other.py").write_text("x = 'another lane, half done'\n")
    changed = RF.changed(RF.RECUT)
    assert changed == ["app/fixtures/golden/taps.json", "app/packages/ui-native/src/regs/fixtures/a.json",
                       "app/packages/ui-native/src/regs/fixtures/b.json"]
    assert RF.answer_paths(changed) == changed[1:]
    t = RF.content_token(changed)
    (repo / "app/fixtures/golden/taps.json").write_text('{"cut": 3}\n')
    assert RF.content_token(changed) != t                     # another re-cut: its accept does not carry over
    head = RF.commit(RF.RECUT, "r-abc", "renumber only")
    assert head == _git(repo, "rev-parse", "HEAD") and RF.changed(RF.RECUT) == []
    assert _git(repo, "show", "--name-only", "--no-renames", "--format=%s", "HEAD").splitlines() == [
        "release r-abc: fixtures re-cut on the candidate (renumber only; pipeline release --staging, T666)", "",
        *changed]
    assert _git(repo, "status", "--porcelain") == "M other.py"            # the other lane's edit is still its own
    assert "Co-Authored-By: Claude <noreply@anthropic.com>" in _git(repo, "log", "-1", "--format=%B")


# ------------------------------------------------------------------------------------------------- the real scripts
def _run(cmd, **env):
    return subprocess.run(cmd, cwd=REPO, capture_output=True, text=True, timeout=120,
                          env={**{k: v for k, v in os.environ.items() if not k.startswith("FAST_FAIL_")}, **env})


def test_fast_fail_stops_at_the_first_failure_or_lists_them_all(tmp_path):
    """T666 item 3: FAST_FAIL_ALL goes on past a failed step (which still stops at ITS first failing command) and ends
    with every failed step; without it the first failure ends the run, as before."""
    first = _run(["bash", "scripts/fast-fail.sh"], FAST_FAIL_SELFTEST="1")
    assert first.returncode == 1 and "selftest: a step that passes" not in first.stdout
    report = tmp_path / "failed.txt"
    every = _run(["bash", "scripts/fast-fail.sh"], FAST_FAIL_SELFTEST="1", FAST_FAIL_ALL="1", FAST_FAIL_REPORT=str(report))
    assert every.returncode == 1 and "NOT REACHED" not in every.stdout
    assert report.read_text().splitlines() == ["selftest: a step that fails", "selftest: a second step that fails"]
    assert "2 step(s) FAILED of 3 run" in every.stderr and "── selftest: a step that passes\n   ok" in every.stdout


def test_run_fast_fail_asks_for_the_whole_list_and_never_a_partial_run(monkeypatch):
    seen = {}

    def run_cmd(cmd, env=None):
        seen.update(cmd=cmd, env=env)
        Path(env["FAST_FAIL_REPORT"]).write_text("format pairs\nno conflict markers\n")
        return 1
    monkeypatch.setattr(R, "run_cmd", run_cmd)
    assert R.run_fast_fail("c0ffee", {"RELEASE_DIR": "/r/r-x"}) == (1, ["format pairs", "no conflict markers"])
    env = seen["env"]
    assert (env["FAST_FAIL_BASE"], env["FAST_FAIL_ALL"], env["FAST_FAIL_ONLY"], env["RELEASE_DIR"]) == ("c0ffee", "1", "", "/r/r-x")
    monkeypatch.setattr(R, "run_cmd", lambda cmd, env=None: 0)
    assert R.run_fast_fail(None) == (0, [])


def test_the_workflow_start_is_retried_bounded_and_said_out_loud():
    """T666 item 6: scripts/start-workflow.sh's own shell test (a stand-in gh; no network)."""
    got = _run(["bash", "scripts/tests/start-workflow.test.sh"])
    assert got.returncode == 0 and "start-workflow.test: 0 failure(s)" in got.stdout, got.stdout + got.stderr
    assert got.stdout.count("\nok ") + got.stdout.startswith("ok ") >= 12 and "FAIL" not in got.stdout


def test_the_recut_refuses_a_named_release_that_is_not_there():
    """`--release` / RELEASE_DIR naming no release is an error before any step runs — never passed over for LATEST."""
    for cmd, env in ((["node", "app/tools/regs-recut/sync.mjs", "--release", "data/release/r-not-there"], {}),
                     (["node", "app/tools/regs-recut/sync.mjs"], {"RELEASE_DIR": "data/release/r-not-there"})):
        got = _run(cmd, **env)
        assert got.returncode == 1 and "no release at" in got.stderr and "r-not-there" in got.stderr, got.stderr
        assert "regs-recut 1/4" not in got.stdout


def _goldens():
    spec = importlib.util.spec_from_file_location("check_goldens_paired", REPO / "scripts/check-goldens-paired.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_the_goldens_pair_with_the_release_under_test(tmp_path, monkeypatch):
    G = _goldens()
    monkeypatch.setattr(G, "RELEASE", tmp_path)
    assert G.under_test({}) == (None, "LATEST")               # a machine with no release: said, not failed
    for rid in ("r-live", "r-cand"):
        (tmp_path / rid).mkdir()
        (tmp_path / rid / "manifest.json").write_text("{}")
    (tmp_path / "LATEST").write_text("r-live\n")
    assert G.under_test({}) == (tmp_path / "r-live", "LATEST")
    assert G.under_test({"RELEASE_DIR": str(tmp_path / "r-cand")}) == (tmp_path / "r-cand", "RELEASE_DIR")
    with pytest.raises(LookupError, match="no release there"):
        G.under_test({"RELEASE_DIR": str(tmp_path / "r-gone")})
    live = dict.fromkeys(G.KEYS, "a")
    assert G.problems("r-cand", live, dict(live), "RELEASE_DIR") == []
    said = G.problems("r-cand", live, {**live, "reach_digest": "b"}, "RELEASE_DIR")[0]
    assert "another release than RELEASE_DIR r-cand" in said and "sync.mjs --release data/release/r-cand" in said


# ------------------------------------------------------------------------------------------- the rehearsal's questions
def test_the_inseason_rehearsal_tells_a_change_from_a_match_that_does_not_resolve(monkeypatch):
    def says(line, rc):
        monkeypatch.setattr(R, "inseason_cmd", lambda rid, check=False: ["bash", "-c", f"echo noise; echo '{line}'; exit {rc}"])
        return R.inseason_check("r-x")
    assert says("in-season matches on r-x: unchanged", 0) == ("same", "in-season matches on r-x: unchanged")
    assert says("in-season matches on r-x: WOULD CHANGE", 1)[0] == "change"
    assert says("in-season matches on r-x: WOULD CHANGE, 2 unresolved", 1)[0] == "unresolved"
    assert says("in-season matches on r-x: unchanged, 1 unresolved", 1)[0] == "unresolved"
    how, why = says("Traceback: boom", 1)
    assert how == "error" and "rc=1" in why and "boom" in why
    assert R.inseason_cmd.__name__ == "<lambda>"               # (the stand-in; the real one is checked next)


def test_the_inseason_commands_are_the_tables_and_the_check_writes_nothing():
    real = R.inseason_cmd("r-x")
    assert real[:5] == ["bash", "scripts/heavy.sh", "inseason-resolve", "30", "--"] and "--check" not in real
    assert real[5:] == RF.refresh_cmd(RF.INSEASON, R.release_root() / "r-x")
    assert R.inseason_cmd("r-x", check=True) == real + ["--check"]


def test_the_uploads_first_refusals(tmp_path, monkeypatch):
    """T666 item 6: what deploy/staging/upload.sh refuses first, asked before it starts and in the rehearsal."""
    feed = tmp_path / "feed" / "release"
    feed.mkdir(parents=True)
    monkeypatch.setattr(R, "GENERATED", types.SimpleNamespace(runtiming=tmp_path))
    monkeypatch.setattr(R, "manifest_of", lambda rid: {"tiles": {"current": {"id": "t-new.pmtiles"}}})
    assert R.tool_says(["no-such-tool-t666"])[0] == 127       # a tool that is not installed is a problem, not a crash
    tools = {"gh": (0, "Logged in"), "rclone": (0, "r2:\nother:")}
    monkeypatch.setattr(R, "tool_says", lambda cmd: tools[cmd[0]])
    assert len(R.staging_prechecks("r-x")) == 1 and "no run-timing base" in R.staging_prechecks("r-x")[0]
    (feed / "base.json").write_text(json.dumps({"tile_version": "t-old.pmtiles", "base": "b.json.gz"}))
    assert "is for tiles t-old.pmtiles, the release's tiles are t-new.pmtiles" in R.staging_prechecks("r-x")[0]
    (feed / "base.json").write_text(json.dumps({"tile_version": "t-new.pmtiles", "base": "b.json.gz"}))
    assert R.staging_prechecks("r-x") == []
    tools.update(gh=(1, "You are not logged into any GitHub hosts"), rclone=(0, "s3:"))
    bad = R.staging_prechecks("r-x")
    assert len(bad) == 2 and "gh auth status" in bad[0] and "not logged into" in bad[0] and "`r2:` is not configured" in bad[1]
    up = (REPO / "deploy/staging/upload.sh").read_text(encoding="utf-8")
    for refused in ('RT/base.json', "tile_version", "r2:$BUCKET", "gh "):
        assert refused in up                                  # the script still makes these refusals itself
