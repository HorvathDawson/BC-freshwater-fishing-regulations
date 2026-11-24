"""`scripts/mutation_report.py` and `.github/workflows/mutation.yml` (T1041, owner 2026-11-23 "Report only"): the changed
lines only, the survivors listed, a broken tool said, never a red PR and never a required check. The tools are stood in
for (each real run is a minute or more; the measured runs are in the ticket)."""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import yaml

from pipeline.common.curated import REPO_ROOT

ROOT = Path(REPO_ROOT)
spec = importlib.util.spec_from_file_location("mutation_report", ROOT / "scripts" / "mutation_report.py")
MR = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = MR
spec.loader.exec_module(MR)

DIFF = """diff --git a/pipeline/a.py b/pipeline/a.py
--- a/pipeline/a.py
+++ b/pipeline/a.py
@@ -3,0 +4,2 @@ def f():
+    x = 1
+    y = 2
@@ -10 +12 @@ def g():
-    return 0
+    return 1
@@ -20,3 +22,0 @@ def h():
diff --git a/pipeline/gone.py b/pipeline/gone.py
--- a/pipeline/gone.py
+++ /dev/null
@@ -1,2 +0,0 @@
"""


def test_the_changed_lines_are_the_ones_the_diff_adds():
    """A hunk's `+start,count` (count 1 when left out, 0 adds nothing); a deleted file adds nothing."""
    assert MR.added_lines(DIFF) == {"pipeline/a.py": [4, 5, 12]}
    assert MR.ranges([12, 4, 5, 6, 9]) == [(4, 6), (9, 9), (12, 12)]
    kept, left = MR.cap_lines({"b.py": [1, 2, 3], "a.py": [7, 8]}, cap=3)
    assert kept == {"a.py": [7, 8], "b.py": [1]} and left == ["b.py (lines after 1)"]
    kept, left = MR.cap_lines({"b.py": [1], "a.py": [7, 8]}, cap=2)
    assert kept == {"a.py": [7, 8]} and left == ["b.py (1 lines)"]


XML = """<?xml version="1.0" ?>
<testsuites><testsuite name="mutmut" tests="3">
<testcase name="Mutant #1" file="pipeline/a.py" line="4"/>
<testcase name="Mutant #2" file="pipeline/a.py" line="12"><failure type="failure" message="bad_survived">--- a
+++ b
-    return 1
+    return 2</failure><system-out>    return 1</system-out></testcase>
<testcase name="Mutant #3" file="pipeline/a.py" line="5"><error type="timeout" message="bad_timeout">t</error></testcase>
<testcase name="Mutant #4" file="pipeline/a.py" line="5"><skipped type="skipped" message="untested">u</skipped></testcase>
<testcase name="Mutant #5" file="pipeline/a.py" line="4"><failure message="ok_suspicious">s</failure></testcase>
</testsuite></testsuites>"""


def test_mutmut_and_stryker_results_name_each_survivor():
    # (CI 2026-11-23) a mutant a stopped run never reached is untested, never "killed"; a timeout is an <error>
    counts, surv = MR.mutmut_survivors(XML)
    assert counts == {"killed": 1, "survived": 1, "timeout": 1, "suspicious": 1, "untested": 1}
    assert surv[0] == ("pipeline/a.py:12", "survived: `return 1`", "--- a\n+++ b\n-    return 1\n+    return 2")
    assert surv[1][1].startswith("suspicious") and len(surv) == 2
    report = {"files": {"packages/core/src/x.ts": {"mutants": [
        {"status": "Killed", "mutatorName": "A", "replacement": "1", "location": {"start": {"line": 1, "column": 2}}},
        {"status": "Survived", "mutatorName": "ConditionalExpression", "replacement": "false",
         "location": {"start": {"line": 3, "column": 5}}},
        {"status": "NoCoverage", "mutatorName": "B", "replacement": "{}", "location": {"start": {"line": 9}}}]}}}
    counts, surv = MR.stryker_survivors(report)
    assert counts == {"Killed": 1, "Survived": 1, "NoCoverage": 1}
    assert [s[0] for s in surv] == ["app/packages/core/src/x.ts:3", "app/packages/core/src/x.ts:9"]
    assert surv[0][1] == "Survived: ConditionalExpression → `false`"
    md = MR.Report("t", survivors=surv, counts=counts).markdown(5)
    assert "### 2 survived" in md and "| `app/packages/core/src/x.ts:3` |" in md and "never fails the PR" in md


def _git_with(diff: str):
    return lambda *a: diff if a[0] == "diff" else ""


def test_the_python_part_runs_each_file_against_its_importers_and_says_every_gap(monkeypatch, tmp_path):
    """No test imports it: said, not run. A run stopped at the budget, a fatal mutmut and unreadable results: each a
    problem in the report. Mutation: drop the `rc & 1` branch -> the fatal case reads as a clean run, fails here."""
    files = {"pipeline/a.py": [4], "pipeline/b.py": [7], "pipeline/c.py": [1]}
    monkeypatch.setattr(MR, "py_files", lambda changed: files)
    monkeypatch.setattr(MR, "git", lambda *a: "")
    related = {"pipeline/a.py": ["pipeline/tests/test_a.py"], "pipeline/b.py": ["pipeline/tests/test_b.py"],
               "pipeline/c.py": []}
    monkeypatch.setattr(MR.RT, "pytest_related", lambda paths, root: (related[paths[0]], []))
    monkeypatch.setattr(MR, "ROOT", tmp_path)
    (tmp_path / "pipeline").mkdir()
    for f in files:
        (tmp_path / f).write_text("x = 1\n")
    calls = []

    def runner(cmd, secs, cwd, env=None):
        calls.append(cmd)
        if cmd[1] == "junitxml":
            return (0, XML) if len([c for c in calls if c[1] == "run"]) == 1 else (1, "no cache")
        return (0, "") if "pipeline/a.py" in cmd else (1, "baseline failed")
    rep = MR.Report("py")
    MR.run_py("b", "h", 900, rep, runner=runner, mutmut="mutmut")
    run = [c for c in calls if c[1] == "run"]
    assert [c[c.index("--paths-to-mutate") + 1] for c in run] == ["pipeline/a.py", "pipeline/b.py"]
    assert "pipeline/tests/test_a.py" in run[0][run[0].index("--runner") + 1]
    assert " -x " in run[0][run[0].index("--runner") + 1] and "--use-patch-file" in run[0]
    assert any("`pipeline/c.py`" in x and "NO test imports or names it" in x for x in rep.lines)
    assert rep.counts == {"killed": 1, "survived": 1, "timeout": 1, "suspicious": 1, "untested": 1}
    assert len(rep.survivors) == 2
    assert [c for c in calls if c[1] == "junitxml"][0][2:] == ["--untested-policy", "skipped", "--suspicious-policy",
                                                               "failure"]
    assert any("`pipeline/b.py`: mutmut stopped (rc=1): baseline failed" in p for p in rep.problems)
    assert any("`pipeline/b.py`: no mutmut results" in p for p in rep.problems)
    rep2 = MR.Report("py")
    MR.run_py("b", "h", 900, rep2, runner=lambda cmd, secs, cwd, env=None: (124, "") if cmd[1] == "run" else (0, XML),
              mutmut="mutmut")
    assert any("stopped at its share of the budget" in p for p in rep2.problems)
    # (CI 2026-11-23: one 190-line file took the whole 900 s) each file gets an equal share of what is left, the
    # smaller change first. Mutation: give the first file all that is left -> fails.
    secs = {}
    def timing(cmd, s, cwd, env=None):
        if cmd[1] == "run":
            secs[cmd[cmd.index("--paths-to-mutate") + 1]] = s
        return (0, "") if cmd[1] == "run" else (0, XML)
    monkeypatch.setattr(MR, "py_files", lambda changed: {"pipeline/a.py": [1, 2, 3], "pipeline/b.py": [7]})
    MR.run_py("b", "h", 900, MR.Report("py"), runner=timing, mutmut="mutmut")
    assert list(secs) == ["pipeline/b.py", "pipeline/a.py"] and 440 <= secs["pipeline/b.py"] <= 450
    assert secs["pipeline/a.py"] >= 890                       # the first finished at once: the rest is the last one's


def test_the_typescript_part_mutates_line_ranges_against_nearby_tests(monkeypatch, tmp_path):
    for rel in ("app/packages/core/src/a.ts", "app/packages/core/src/a.test.ts", "app/packages/ui/src/b.ts",
                "app/packages/ui-native/src/C.tsx", "app/packages/ui-native/src/D.test.tsx"):
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text("x\n")
    diff = "".join(f"+++ b/{f}\n@@ -1 +{a},{n} @@\n" for f, a, n in (
        ("app/packages/core/src/a.ts", 3, 2), ("app/packages/core/src/a.test.ts", 1, 1), ("app/packages/ui/src/b.ts", 5, 1),
        ("app/packages/ui-native/src/C.tsx", 10, 1), ("app/packages/ui-native/src/D.test.tsx", 2, 1)))
    monkeypatch.setattr(MR, "git", _git_with(diff))
    monkeypatch.setattr(MR, "ROOT", tmp_path)
    seen = {}

    def runner(cmd, secs, cwd, env=None):
        seen.update(cmd=cmd, tests=json.loads(env["MUTATION_TESTS"]), cfg=json.loads((cwd / cmd[2]).read_text()),
                    vcfg=(cwd / ".mutation-vitest.config.mts").read_text())
        (cwd / ".mutation-report.json").write_text(json.dumps({"files": {}}))
        return 0, "table"
    rep = MR.Report("ts")
    MR.run_ts("b", "h", 900, rep, runner=runner)
    # b.ts has no sibling test; the PR's changed tests (a.test.ts, D.test.tsx) go with every mutated file
    assert seen["cmd"][-1] == "packages/core/src/a.ts:3-4,packages/ui-native/src/C.tsx:10-10,packages/ui/src/b.ts:5-5"
    assert seen["tests"] == ["packages/core/src/a.test.ts", "packages/ui-native/src/D.test.tsx"]
    assert seen["cfg"]["inPlace"] is True and seen["cfg"]["vitest"] == {"configFile": ".mutation-vitest.config.mts",
                                                                         "related": False}
    assert "matchesGlob" in seen["vcfg"] and not list((tmp_path / "app").glob(".mutation*"))   # removed after
    rep2 = MR.Report("ts")
    MR.run_ts("b", "h", 900, rep2, runner=lambda cmd, secs, cwd, env=None: (124, ""))
    assert any("stopped at the 900 s budget" in p for p in rep2.problems)
    rep3 = MR.Report("ts")
    MR.run_ts("b", "h", 900, rep3, runner=lambda cmd, secs, cwd, env=None: (1, "Something went wrong"))
    assert any("Stryker failed (rc=1): Something went wrong" in p for p in rep3.problems)


def test_a_broken_report_is_said_and_never_fails_the_pr(monkeypatch, capsys, tmp_path):
    """Mutation: let the exception out -> main exits non-zero and the PR job goes red."""
    def boom(*a, **k):
        raise RuntimeError("git diff failed")
    monkeypatch.setattr(MR, "run_py", boom)
    out = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(out))
    assert MR.main(["py", "--base", "b", "--head", "h"]) == 0
    assert "the report itself failed: RuntimeError: git diff failed" in out.read_text()
    assert "::warning title=mutation report::" in capsys.readouterr().out
    assert MR.main(["py", "--base", "b", "--head", "h", "--budget", "10"]) == 2


def test_the_workflow_is_report_only_pinned_and_on_pull_requests_alone():
    doc = yaml.safe_load((ROOT / ".github" / "workflows" / "mutation.yml").read_text())
    on = doc.get("on", doc.get(True))
    assert list(on) == ["pull_request"]                       # no merge_group: a queue waits on required checks only
    from importlib import util
    s = util.spec_from_file_location("ci_paths", ROOT / "scripts" / "ci_paths.py")
    cp = util.module_from_spec(s)
    s.loader.exec_module(cp)
    names = {j.get("name", k) for k, j in doc["jobs"].items()}
    assert not names & set(cp.REQUIRED.values()) and "mutation.yml" not in cp.WORKFLOWS.values()
    for job in doc["jobs"].values():
        assert job["continue-on-error"] is True and job["timeout-minutes"] * 60 > 900
        assert job["steps"][0]["with"]["ref"] == "${{ github.event.pull_request.head.sha }}"
        assert job["steps"][-1]["run"].endswith("--budget 900")
    runs = "\n".join(st.get("run", "") for j in doc["jobs"].values() for st in j["steps"])
    assert f"@stryker-mutator/core@{MR.STRYKER} @stryker-mutator/vitest-runner@{MR.STRYKER}" in runs
    assert f"mutmut=={MR.MUTMUT}\n" in (ROOT / "scripts" / "mutation-requirements.txt").read_text()
    reqs = [ln for ln in (ROOT / "scripts" / "mutation-requirements.txt").read_text().splitlines()
            if ln and not ln.startswith("#")]
    assert all("==" in ln for ln in reqs), reqs                # every version exact


def test_the_python_part_picks_pipeline_and_scripts_sources(monkeypatch, tmp_path):
    """pipeline/**.py and scripts/*.py are mutated; tests, conftest and deleted files are not."""
    for rel in ("pipeline/a.py", "scripts/b.py", "pipeline/tests/test_a.py", "pipeline/conftest.py", "app/x.py"):
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text("x")
    (tmp_path / "pytest.ini").write_text("[pytest]\ntestpaths = pipeline/tests\n")
    monkeypatch.setattr(MR, "ROOT", tmp_path)
    changed = {p: [1] for p in ("pipeline/a.py", "scripts/b.py", "pipeline/tests/test_a.py", "pipeline/conftest.py",
                                "app/x.py", "pipeline/gone.py")}
    assert sorted(MR.py_files(changed)) == ["pipeline/a.py", "scripts/b.py"]


def test_a_stopped_tool_gets_a_ctrl_c_first(tmp_path):
    """bounded(): at its time a tool is sent SIGINT first (mutmut keeps what it finished, as on Ctrl-C), 124 back.
    Mutation: TERM first -> the child's 'saved' line never prints."""
    child = tmp_path / "child.py"
    child.write_text("import time, sys\ntry:\n    time.sleep(60)\nexcept KeyboardInterrupt:\n    print('saved')\n")
    rc, out = MR.bounded([sys.executable, str(child)], 1, tmp_path)
    assert rc == 124 and "saved" in out
    assert MR.bounded([sys.executable, "-c", "print('ok')"], 30, tmp_path) == (0, "ok\n")


def test_a_mutated_file_is_put_back_as_it_was_uncommitted_edits_too(monkeypatch, tmp_path):
    """A stopped mutmut can leave its mutant on disk: the file is written back from the bytes read before the run,
    never `git checkout` (which threw away a local uncommitted fix, 2026-11-23). Mutation: skip the write -> fails."""
    (tmp_path / "pipeline").mkdir()
    (tmp_path / "pipeline" / "a.py").write_text("x = 1  # an uncommitted edit\n")
    monkeypatch.setattr(MR, "ROOT", tmp_path)
    monkeypatch.setattr(MR, "py_files", lambda changed: {"pipeline/a.py": [1]})
    monkeypatch.setattr(MR, "git", lambda *a: "")
    monkeypatch.setattr(MR.RT, "pytest_related", lambda paths, root: (["pipeline/tests/test_a.py"], []))

    def runner(cmd, secs, cwd, env=None):
        if cmd[1] == "run":
            (tmp_path / "pipeline" / "a.py").write_text("x = 2  # the mutant, left by a stopped run\n")
            return 124, ""
        return 0, XML
    MR.run_py("b", "h", 900, MR.Report("py"), runner=runner, mutmut="mutmut")
    assert (tmp_path / "pipeline" / "a.py").read_text() == "x = 1  # an uncommitted edit\n"
