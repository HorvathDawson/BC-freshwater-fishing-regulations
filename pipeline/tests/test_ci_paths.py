"""`scripts/ci_paths.py` (T995): the trunk and the release-branch names, CI's areas (one list for the workflows' push
filters, their PR `changes` jobs and `before_push.py --changed`), and the `ok` job's verdict — the required check on
`main`. Pinned by mutation: a skipped job passes, a failed or cancelled one fails, a PR outside the area skips."""
from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

from pipeline.common.curated import REPO_ROOT

ROOT = Path(REPO_ROOT)
spec = importlib.util.spec_from_file_location("ci_paths", ROOT / "scripts" / "ci_paths.py")
CP = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = CP
spec.loader.exec_module(CP)


@pytest.mark.parametrize("path, pattern, hit", [
    ("app/x/y.ts", "app/**", True),
    ("app", "app/**", False),
    ("apps/x.ts", "app/**", False),                                   # a prefix is not the folder
    ("app/packages/core/src/a.generated.ts", "app/packages/core/src/*.generated.*", True),
    ("app/packages/core/src/sub/a.generated.ts", "app/packages/core/src/*.generated.*", False),   # `*` stops at /
    ("app/tools/gauge-feed-contract.test.ts", "app/tools/gauge-feed-contract*", True),
    ("AGENTS.md", "AGENTS.md", True),
    ("x/AGENTS.md", "AGENTS.md", False),
    ("AGENTSxmd", "AGENTS.md", False),                                # `.` is literal
])
def test_the_match_is_githubs_glob(path, pattern, hit):
    assert CP.matches(path, [pattern]) is hit


def test_areas_of_a_change():
    assert CP.areas_of(["handoff/PLAN/STATUS.md"]) == {"docs"}
    assert CP.areas_of(["NOTICE.txt"]) == set()
    assert CP.areas_of(["README.md"]) == {"pipeline"}                    # (T995 review) test_doc_drift reads it
    assert CP.areas_of([".github/workflows/feeds-hydat.yml"]) == {"pipeline"}   # test_workflow_pins reads it
    assert CP.areas_of(["deploy/staging/upload.sh"]) >= {"pipeline", "feeds-worker"}
    assert CP.areas_of(["app/workers/feeds/src/index.ts"]) == {"app", "feeds-worker", "pipeline"}
    assert CP.areas_of(["pipeline/gauges/x.py"]) == {"pipeline", "feeds-worker"}
    assert CP.areas_of(["scripts/ci_paths.py"]) == {"docs", "app", "feeds-worker", "pipeline"}    # shared: all run


def test_a_push_runs_everything_and_a_pr_only_its_area():
    paths = lambda base, head: ["handoff/PLAN/STATUS.md", "AGENTS.md"]          # noqa: E731
    assert CP.github("app", {"EVENT_NAME": "push"}, paths)[0] is True
    assert CP.github("pipeline", {"EVENT_NAME": "workflow_dispatch"}, paths)[0] is True
    pr = {"EVENT_NAME": "pull_request", "BASE_SHA": "b", "HEAD_SHA": "h"}
    run, why = CP.github("app", pr, paths)
    assert run is False and why == "the pull request changes 2 path(s), none in app: its jobs are skipped (a pass)"
    run, why = CP.github("docs", pr, paths)
    assert run is True and why == "2 of the pull request's 2 path(s) are in docs: handoff/PLAN/STATUS.md, AGENTS.md"
    with pytest.raises(SystemExit, match="needs BASE_SHA and HEAD_SHA"):
        CP.github("app", {"EVENT_NAME": "pull_request"}, paths)
    with pytest.raises(SystemExit, match="no area 'worker'"):
        CP.github("worker", pr, paths)


def test_the_pr_diff_is_the_merge_base_with_the_base(tmp_path, monkeypatch):
    """On real git: a change on the base after the PR branched is not the PR's (three-dot diff); a git failure stops."""
    git = ["git", "-c", "user.email=t@t", "-c", "user.name=t", "-c", "init.defaultBranch=main"]
    run = lambda *a: subprocess.run([*git, *a], cwd=tmp_path, check=True, capture_output=True, text=True).stdout  # noqa
    run("init", "-q")
    (tmp_path / "a").write_text("1")
    run("add", "a")
    run("commit", "-qm", "base")
    run("switch", "-qc", "lane")
    (tmp_path / "handoff").mkdir()
    (tmp_path / "handoff" / "x.md").write_text("x")
    run("add", ".")
    run("commit", "-qm", "lane")
    head = run("rev-parse", "HEAD").strip()
    run("switch", "-q", "main")
    (tmp_path / "app").mkdir()
    (tmp_path / "app" / "y.ts").write_text("y")
    run("add", ".")
    run("commit", "-qm", "main moved")
    base = run("rev-parse", "HEAD").strip()
    monkeypatch.chdir(tmp_path)
    assert CP.pr_paths(base, head) == ["handoff/x.md"]
    # (T995 review) a file moved out of an area names its old path too: git's rename detection names only the new one
    run("switch", "-q", "lane")
    (tmp_path / "pipeline").mkdir()
    (tmp_path / "pipeline" / "m.py").write_text("\n".join(map(str, range(50))))
    run("add", ".")
    run("commit", "-qm", "add")
    before = run("rev-parse", "HEAD").strip()
    run("mv", "pipeline/m.py", "m.py")
    run("commit", "-qm", "move")
    assert CP.pr_paths(before, run("rev-parse", "HEAD").strip()) == ["m.py", "pipeline/m.py"]
    with pytest.raises(SystemExit, match="git diff --name-only"):
        CP.pr_paths("deadbeef", head)


@pytest.mark.parametrize("results, ok", [
    ({"changes": "success", "gates": "success"}, True),
    ({"changes": "success", "gates": "skipped"}, True),               # skipped by its `if:` = a pass
    ({"changes": "success", "gates": "failure"}, False),
    ({"changes": "success", "gates": "cancelled"}, False),
    ({"changes": "failure", "gates": "skipped"}, False),               # the decider failed: its skips are no pass
    ({"changes": "success", "gates": "timed_out"}, False),            # anything else fails
    ({}, False),                                                      # nothing ran: never a pass
])
def test_the_ok_verdict(results, ok):
    assert CP.verdict({j: {"result": r, "outputs": {}} for j, r in results.items()})[0] is ok


def test_the_cli_writes_githubs_output_and_the_verdict_exits(tmp_path, monkeypatch, capsys):
    out = tmp_path / "out"
    monkeypatch.setenv("GITHUB_OUTPUT", str(out))
    monkeypatch.setenv("EVENT_NAME", "push")
    assert CP.main(["github", "docs"]) == 0 and out.read_text() == "run=true\n"
    monkeypatch.setenv("NEEDS", json.dumps({"changes": {"result": "success"}, "ledger": {"result": "failure"}}))
    assert CP.main(["verdict"]) == 1 and "ledger: failure" in capsys.readouterr().out
    monkeypatch.setenv("NEEDS", json.dumps({"changes": {"result": "success"}, "ledger": {"result": "skipped"}}))
    assert CP.main(["verdict"]) == 0
    monkeypatch.delenv("NEEDS")
    assert CP.main(["verdict"]) == 2


def test_the_branch_names(tmp_path):
    assert CP.TRUNK == "main" and CP.release_branch("0.9.0") == "release/0.9.0"
    app = tmp_path / "app.json"
    app.write_text(json.dumps({"expo": {"version": "1.2.3"}}))
    assert CP.app_version(app) == "1.2.3"
    app.write_text(json.dumps({"expo": {"version": "1.2"}}))
    with pytest.raises(SystemExit, match="is not x.y.z"):
        CP.app_version(app)
    with pytest.raises(SystemExit, match="cannot read expo.version"):
        CP.app_version(tmp_path / "none.json")
    assert re.fullmatch(r"\d+\.\d+\.\d+", CP.app_version())                 # the real app.json


def test_typescript_names_the_same_trunk():
    """AGENTS 40: a value on both sides of the boundary may not drift. TypeScript's one copy is core/src/repo.ts."""
    ts = (ROOT / "app" / "packages" / "core" / "src" / "repo.ts").read_text(encoding="utf-8")
    assert re.search(r'^export const TRUNK = "([^"]+)";$', ts, re.M).group(1) == CP.TRUNK


def test_no_script_or_doc_names_the_old_dev_branch():
    """(T995 acceptance) nothing names the retired dev branch — in what this suite's area covers (its own input, so a
    change that brings the name back runs it: the code, the workflows, deploy, the app, the README; handoff/ history is
    not in it). The app's two names are pinned by its own tests too (legal.test.ts REPO_BLOB, dispatch DEFAULT_REF)."""
    old = "redesign/" + "stream-sections"
    specs = [f":(glob){p}" for p in CP.AREAS["pipeline"]] + [":(exclude)handoff/**"]
    got = subprocess.run(["git", "grep", "-n", "-F", old, "--", *specs], cwd=ROOT, capture_output=True, text=True)
    assert got.returncode in (0, 1), got.stderr
    assert got.stdout == ""
