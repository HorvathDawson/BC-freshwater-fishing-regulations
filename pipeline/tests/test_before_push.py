"""`scripts/before_push.py` (T993 item 7, T791 #2): one command before a push that asks what GitHub's runs ask — the
step list read from pipeline-ci.yml, every failure listed, the hook printed and never installed. No step is run here
(each is a real check of its own); the runner is stood in for."""
from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

from pipeline.common.curated import REPO_ROOT

ROOT = Path(REPO_ROOT)
spec = importlib.util.spec_from_file_location("before_push", ROOT / "scripts" / "before_push.py")
BP = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = BP           # its dataclasses look their module up by name
spec.loader.exec_module(BP)


def test_every_pipeline_ci_run_step_is_run_or_named_with_its_reason():
    """Read from the workflow, as fast-fail's ci_step: a step added to pipeline-ci.yml joins by itself."""
    steps = BP.workflow_steps()
    names = {s.name for s in BP.plan()}
    for s in steps:
        if s.get("name") and "run" in s:
            assert f"CI: {s['name']}" in names or s["name"] in BP.SKIP or s["name"] == BP.TESTS, s["name"]
    assert set(BP.SKIP) <= {s.get("name") for s in steps}, "a SKIP entry names no workflow step"
    by_name = {s.name: s for s in BP.plan()}
    mini = by_name["CI: The committed mini release matches a rebuild"]
    assert mini.script == next(s["run"] for s in steps if s.get("name") == "The committed mini release matches a rebuild")


def test_a_step_added_to_the_workflow_joins_and_the_tier_reads_its_markers():
    steps = BP.workflow_steps() + [{"name": "A new guard", "run": "python -m pipeline.tools.new_guard --check"}]
    plan = BP.plan(steps=steps)
    assert any(s.name == "CI: A new guard" and "new_guard" in s.script for s in plan)
    tier = next(s for s in plan if s.name.startswith("CI: Tests"))
    assert "scripts/heavy.sh" in tier.cmd and "scripts/pytest-chunked.sh" in tier.cmd
    assert tier.cmd[tier.cmd.index("-m") + 1] == BP.tier_markers(steps) and "not needs_bundle" in BP.tier_markers(steps)


def test_it_asks_what_app_ci_asks_beyond_pnpm_check():
    plan = {s.name: s for s in BP.plan()}
    assert plan["app gates (pnpm check:fast)"].cmd[-1] == "check:fast"
    assert "workers/feeds" in plan["feeds worker tests"].cmd
    scanners = plan["app tree scanners (app/tools tests that read the tree)"].cmd
    assert "tools/failures.test.ts" in scanners and "tools/back-layers.test.ts" in scanners
    no_tier = {s.name: s for s in BP.plan(tier=False)}
    assert not any(n.startswith("CI: Tests") for n in no_tier)
    assert "pipeline/tests/test_liveness.py" in no_tier["the pipeline's source scanners (--no-tier)"].cmd


def test_every_failure_is_listed_and_the_exit_says_so(capsys):
    def runner(step):
        return 1 if step.name in ("app gates (pnpm check:fast)", "feeds worker tests") else 0
    assert BP.main([], runner=runner) == 1
    err = capsys.readouterr().err
    assert "2 of" in err and "- app gates (pnpm check:fast)" in err and "- feeds worker tests" in err
    assert "--hook prints how" in err
    assert BP.main(["--only", "feeds"], runner=lambda s: 0) == 0
    assert "PARTIAL (--only): 1 step(s) passed" in capsys.readouterr().out
    ran = []
    assert BP.main(["--only", "no-such-step"], runner=lambda s: ran.append(s) or 0) == 2 and ran == []
    assert "matches no step" in capsys.readouterr().err                   # never a pass on nothing run


def test_the_hook_is_printed_never_installed(capsys, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    ran = []
    assert BP.main(["--hook"], runner=lambda s: ran.append(s) or 0) == 0
    out = capsys.readouterr().out
    assert "\ngit config core.hooksPath .githooks\n" in out and "git push --no-verify" in out and ran == []
    assert not any(tmp_path.iterdir())


def _changed(monkeypatch, capsys, paths, argv=()):
    """(T995) `--changed` over these paths: the names of the steps it would run."""
    monkeypatch.delenv(BP.SKIP_ENV, raising=False)
    monkeypatch.setattr(BP, "changed_paths", lambda: ("b" * 40, list(paths)))
    ran = []
    assert BP.main(["--changed", "--no-tier", "--serial", *argv], runner=lambda s: ran.append(s.name) or 0) == 0
    capsys.readouterr()
    return ran


def test_changed_runs_only_the_areas_the_branch_changed(monkeypatch, capsys):
    """(T995) the same areas as CI's `changes` jobs (scripts/ci_paths.py): a docs-only push is the ticket guard alone;
    a pipeline change adds the pipeline's steps; an app change runs the app gates (the ticket guard is inside them)."""
    guard = "ticket guard (node app/tools/check-tickets.mjs)"
    docs = "docs: the ticket guard's tests and the start-here page (docs-ci)"
    msgs = BP.COMMITS                                                     # (T998) first, whatever changed
    assert _changed(monkeypatch, capsys, ["handoff/PLAN/STATUS.md", "AGENTS.md"]) == [msgs, guard, docs]
    assert _changed(monkeypatch, capsys, ["NOTICE.txt"]) == [msgs, guard]
    # (T995 review) what docs-ci runs on a handoff change, the hook runs: its vitest step, word for word
    import yaml
    ledger = yaml.safe_load((ROOT / ".github" / "workflows" / "docs-ci.yml").read_text())["jobs"]["ledger"]["steps"]
    assert f"pnpm exec vitest run {' '.join(BP.DOCS_TESTS)}" in [s.get("run") for s in ledger]
    ran = _changed(monkeypatch, capsys, ["pipeline/release.py"])
    assert ran[:2] == [msgs, guard] and "the pipeline's source scanners (--no-tier)" in ran
    assert not any(n.startswith(("app", "feeds worker")) for n in ran)
    ran = _changed(monkeypatch, capsys, ["app/workers/feeds/src/index.ts"])
    assert guard not in ran and "app gates (pnpm check:fast)" in ran and "feeds worker tests" in ran
    assert "the pipeline's source scanners (--no-tier)" in ran            # (T995 review) the pipeline tests read app/
    ran = _changed(monkeypatch, capsys, ["pipeline/regs/review/x.py"])
    assert not any(n.startswith(("app", "feeds worker", "docs: ")) for n in ran) and ran[0] == msgs
    # every area: the whole plan, (T1041) the tree scanners step become `vitest related`, the pipeline's related tests
    # after its source scanners
    full = [BP.APP_RELATED if n == BP.SCANNERS else n for s in BP.plan(tier=False) for n in
            ([s.name, BP.PY_RELATED] if s.name == BP.NO_TIER else [s.name])]
    assert _changed(monkeypatch, capsys, ["pipeline/x.py", "app/x.ts", "app/workers/feeds/x.ts", "AGENTS.md"]) == full
    # every step names its area
    assert {BP.step_area(s) for s in BP.plan(app_suite=True)} == {"pipeline", "app", "feeds-worker", "docs"}


def test_the_release_commands_own_push_skips_saying_why(monkeypatch, capsys):
    monkeypatch.setenv(BP.SKIP_ENV, "the release command's own push")
    ran = []
    assert BP.main(["--changed"], runner=lambda s: ran.append(s) or 0) == 0 and ran == []
    assert "SKIPPED (nothing run): the release command's own push" in capsys.readouterr().out
    monkeypatch.setenv(BP.SKIP_ENV, " ")
    assert BP.main(["--changed"], runner=lambda s: ran.append(s) or 0) == 2 and ran == []
    assert "needs a reason" in capsys.readouterr().err


def test_the_versioned_hooks():
    """(T995) .githooks/pre-push runs `--changed` (seconds for a docs push); (T998) commit-msg runs the Conventional
    Commits check (app/tools/check-commits.test.ts runs it on real messages: it needs app/node_modules); an empty
    message is git's own refusal, so the hook passes it."""
    import os
    import subprocess
    hooks = ROOT / ".githooks"
    pre = (hooks / "pre-push").read_text()
    assert 'exec "$py" scripts/before_push.py --changed --pre-push' in pre and "git config core.hooksPath .githooks" in pre
    assert os.access(hooks / "pre-push", os.X_OK) and os.access(hooks / "commit-msg", os.X_OK)
    assert 'exec node app/tools/check-commits.mjs --edit "$msg"' in (hooks / "commit-msg").read_text()
    assert subprocess.run(["sh", str(hooks / "commit-msg"), "/dev/null"]).returncode == 0


def _repo(tmp_path, monkeypatch):
    """A clone with an origin whose main has one commit; BP's git runs in it."""
    import subprocess
    git = ["git", "-c", "user.email=t@t", "-c", "user.name=t", "-c", "init.defaultBranch=main"]
    origin, here = tmp_path / "origin.git", tmp_path / "here"
    subprocess.run([*git, "init", "-q", "--bare", str(origin)], check=True)
    subprocess.run([*git, "init", "-q", str(here)], check=True)

    def run(*a):
        return subprocess.run([*git, *a], cwd=here, check=True, capture_output=True, text=True).stdout.strip()
    (here / "pipeline").mkdir()
    (here / "pipeline" / "a.py").write_text("1")
    run("add", ".")
    run("commit", "-qm", "base")
    run("remote", "add", "origin", str(origin))
    run("push", "-q", "origin", "HEAD:refs/heads/main")
    run("fetch", "-q", "origin")
    monkeypatch.setattr(BP, "ROOT", here)
    return here, run


def test_changed_holds_commits_the_working_tree_and_untracked_files(tmp_path, monkeypatch):
    """(T995 review) the unpinned mutation: untracked files are part of the change."""
    here, run = _repo(tmp_path, monkeypatch)
    run("switch", "-qc", "lane")
    (here / "handoff").mkdir()
    (here / "handoff" / "x.md").write_text("x")
    run("add", ".")
    run("commit", "-qm", "docs")
    (here / "pipeline" / "a.py").write_text("2")                           # uncommitted
    (here / "app").mkdir()
    (here / "app" / "new.ts").write_text("n")                              # untracked
    base, paths = BP.changed_paths()
    assert base == run("rev-parse", "origin/main") and paths == ["app/new.ts", "handoff/x.md", "pipeline/a.py"]


def test_the_hook_checks_the_pushed_commits_not_the_tree(tmp_path, monkeypatch, capsys):
    """(T995 review) the refs on stdin: a tag or a delete checks nothing; an existing branch its new commits; a new
    branch everything since its merge base with origin/main; never the working tree."""
    here, run = _repo(tmp_path, monkeypatch)
    z = BP.ZERO
    run("switch", "-qc", "lane")
    (here / "handoff").mkdir()
    (here / "handoff" / "x.md").write_text("x")
    run("add", ".")
    run("commit", "-qm", "docs")
    one = run("rev-parse", "HEAD")
    (here / "app").mkdir()
    (here / "app" / "y.ts").write_text("y")
    run("add", ".")
    run("commit", "-qm", "app")
    two = run("rev-parse", "HEAD")
    (here / "pipeline" / "a.py").write_text("dirty")                       # the tree: never the hook's change
    said, paths = BP.pushed_paths([f"refs/heads/lane {two} refs/heads/lane {z}"])
    assert paths == ["app/y.ts", "handoff/x.md"] and "since the merge base with origin/main" in said[0]
    said, paths = BP.pushed_paths([f"refs/heads/lane {two} refs/heads/lane {one}"])
    assert paths == ["app/y.ts"] and said == [f"refs/heads/lane: 1 path(s) in {one[:12]}..{two[:12]}"]
    assert BP.pushed_paths([f"refs/tags/v1 {two} refs/tags/v1 {z}", f"(delete) {z} refs/heads/old {one}"]) == (
        ["refs/tags/v1: a tag, nothing to check", "refs/heads/old: a delete, nothing to check"], [])
    import io
    monkeypatch.delenv(BP.SKIP_ENV, raising=False)
    ran = []
    assert BP.main(["--changed", "--pre-push"], runner=lambda s: ran.append(s) or 0,
                   stdin=io.StringIO(f"refs/tags/v1 {two} refs/tags/v1 {z}\n")) == 0
    assert ran == [] and "only tags / deletes pushed: nothing to check" in capsys.readouterr().out
    assert BP.main(["--changed", "--pre-push", "--no-tier", "--serial"], runner=lambda s: ran.append(s.name) or 0,
                   stdin=io.StringIO(f"refs/heads/lane {one} refs/heads/lane {z}\n")) == 0
    assert ran == [BP.COMMITS, "ticket guard (node app/tools/check-tickets.mjs)",
                   "docs: the ticket guard's tests and the start-here page (docs-ci)"]     # docs only, not the tree


def test_the_commit_messages_of_what_is_pushed_are_checked_first(tmp_path, monkeypatch):
    """(T998) the hook checks the messages of the commits each pushed branch sends (`<base>..<sha>`), a tag none;
    by hand (or --changed) the branch's commits the trunk does not have."""
    here, run = _repo(tmp_path, monkeypatch)
    z = BP.ZERO
    base = run("rev-parse", "HEAD")
    run("switch", "-qc", "lane")
    (here / "handoff").mkdir()
    (here / "handoff" / "x.md").write_text("x")
    run("add", ".")
    run("commit", "-qm", "docs(handoff): x")
    one = run("rev-parse", "HEAD")
    said, paths, ranges = BP.pushed([f"refs/heads/lane {one} refs/heads/lane {z}", f"refs/tags/v1 {one} refs/tags/v1 {z}"])
    assert ranges == [f"{base}..{one}"]
    # (T1019) an UPDATED branch: still the branch's own commits since the trunk (docs-ci's range), never `<old tip>..`
    (here / "handoff" / "y.md").write_text("y")
    run("add", ".")
    run("commit", "-qm", "docs(handoff): y")
    two = run("rev-parse", "HEAD")
    said2, paths2, ranges2 = BP.pushed([f"refs/heads/lane {two} refs/heads/lane {one}"])
    assert ranges2 == [f"{base}..{two}"] and paths2 == ["handoff/y.md"]
    step = BP.plan(tier=False, areas={"docs"}, ranges=ranges)[0]
    assert step == BP.Step(BP.COMMITS, ("node", "app/tools/check-commits.mjs", "--range", f"{base}..{one}"))
    assert BP.plan(tier=False)[0].cmd == ("node", "app/tools/check-commits.mjs", "--range", "origin/main..HEAD")
    assert BP.step_area(step) == "docs"



def test_a_step_runs_without_gits_repository_variables(tmp_path):
    """(T995, 2026-11-22) A pre-push hook run from a LINKED WORKTREE has GIT_DIR=<repo>/.git/worktrees/<name>. A step's
    test making its scratch clone (`git init <tmp>/here`, test_before_push / test_release_command) then re-initialised
    THAT gitdir instead, as bare, and the write landed in the shared config: core.bare = true, every worktree broke. Here: with the
    hook's env the hazard is real; through `step_env` it is not. Mutation: return the env unstripped -> fails."""
    import subprocess as sp
    clean = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    a, wt = tmp_path / "a", tmp_path / "wt"
    git = lambda *x, env=clean: sp.run(["git", *x], env=env, check=True, capture_output=True, text=True).stdout.strip()
    git("init", "-q", str(a))
    git("-C", str(a), "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "x")
    git("-C", str(a), "worktree", "add", "-q", str(wt), "-b", "side")
    hook_env = {**os.environ, "GIT_DIR": str(a / ".git" / "worktrees" / "wt")}   # what git sets for the hook
    bare = lambda: git("-C", str(a), "config", "core.bare")
    git("init", "-q", str(tmp_path / "here1"), env=hook_env)                   # the hazard, as the tests met it
    assert bare() == "true"
    git("-C", str(a), "config", "core.bare", "false")
    env = BP.step_env(hook_env)
    assert "GIT_DIR" not in env and not (set(env) & BP._git_local_vars())
    git("init", "-q", str(tmp_path / "here2"), env=env)
    assert bare() == "false" and (tmp_path / "here2" / ".git").is_dir()


def test_the_pre_push_hook_drops_gits_repository_variables():
    """The hook itself unsets them before it starts before_push.py (the same set, git's own list)."""
    hook = (Path(__file__).resolve().parents[2] / ".githooks" / "pre-push").read_text()
    assert "unset $(git rev-parse --local-env-vars) GIT_QUARANTINE_PATH" in hook
    assert hook.index("unset $(git rev-parse --local-env-vars)") < hook.index("exec ")


def test_the_pre_push_hook_stays_fast():
    """(T995) the hook leaves the whole no-data tier to the PR's required check: git's connection to GitHub, held open
    while the hook runs, did not outlive a ~10 min tier (SIGPIPE). Mutation: drop --no-tier -> fails."""
    hook = (Path(__file__).resolve().parents[2] / ".githooks" / "pre-push").read_text()
    assert 'scripts/before_push.py --changed --pre-push --no-tier' in hook


# ── (T1041) THE TESTS A CHANGE REACHES, AND THE TWO LANES ──────────────────────────────────────────────────────────────

def _plan(paths, **kw):
    return {s.name: s for s in BP.plan(tier=False, areas=BP.CP.areas_of(paths), paths=list(paths), **kw)}


def test_a_core_change_runs_the_tests_of_every_package_that_imports_it():
    """(T1041) #63, 2026-11-23: app/packages/core/src/tabSources.ts changed and ui-native's WaterScreen.tidal.test.tsx
    failed on CI ("regsSourcesOf: the rule index has no byKey") — the hook had run only the tree scanners. Now the
    app's test step is `vitest related` over the changed code (vitest's import graph crosses packages), the scanners
    with it. Mutation: keep the scanners step in --changed -> fails."""
    plan = _plan(["app/packages/core/src/tabSources.ts", "app/packages/data/dev/mini-release/LATEST"])
    assert BP.SCANNERS not in plan
    cmd = plan[BP.APP_RELATED].cmd
    assert cmd[:8] == ("pnpm", "-C", "app", "exec", "vitest", "related", "--run", "--passWithNoTests")
    assert cmd[8] == "packages/core/src/tabSources.ts" and set(BP.app_scanners()) <= set(cmd[9:])
    assert any("not code" in n and "mini-release/LATEST" in n for n in plan[BP.APP_RELATED].note)   # said, not dropped
    # a full run (no change list) and --app-suite keep the plain scanners: the whole suite holds every related test
    assert BP.APP_RELATED not in {s.name for s in BP.plan(tier=False)}
    assert BP.SCANNERS in _plan(["app/x.ts"], app_suite=True) and BP.APP_RELATED not in _plan(["app/x.ts"], app_suite=True)


def test_a_pipeline_change_runs_the_tests_that_import_it_as_ci_does():
    """(T1041) #67: decided.py changed; test_local_closures.py, which imports it, failed in the no-data tier the hook
    leaves to the PR. Now its direct importers run, CI's markers, in chunks. Mutation: drop the step -> fails."""
    import re
    plan = _plan(["pipeline/deliver/answers/display/decided.py"])
    step = plan[BP.PY_RELATED]
    assert step.cmd[:3] == ("bash", "scripts/pytest-chunked.sh", "--only")
    only = re.compile(step.cmd[3])
    assert only.search("pipeline/tests/test_local_closures.py") and not only.search("pipeline/tests/test_liveness.py")
    assert step.cmd[step.cmd.index("-m") + 1] == BP.tier_markers(BP.workflow_steps())
    assert list(plan).index(BP.PY_RELATED) == list(plan).index(BP.NO_TIER) + 1
    # no test reaches the change: the step runs nothing and says so (never a silent pass of nothing)
    none = _plan(["README.md"])[BP.PY_RELATED]
    assert none.cmd == () and "no pipeline test file changed or imports a changed module" in none.note
    assert BP.run_step(none) == 0
    # with the whole tier (no --no-tier) it holds them: no related step
    assert BP.PY_RELATED not in {s.name for s in BP.plan(areas={"pipeline"}, paths=["pipeline/release.py"])}


def test_the_two_lanes_run_at_once_and_each_steps_output_is_printed_whole(capsys):
    """(T1041) the pipeline's steps and the rest run at once (the hook takes the longer lane, not the sum); a step's
    output is held and printed whole with its header; the failures are listed in the plan's order; a runner that
    raises fails its step and its lane goes on. Mutation: run the lanes one after another -> the overlap fails."""
    import threading
    import time
    steps = [BP.Step("app gates (pnpm check:fast)", ("x",)), BP.Step("app: second", ("x",)),
             BP.Step("CI: a pipeline step", ("x",), note=("a note",)), BP.Step("CI: fails", ("x",)),
             BP.Step("CI: raises", ("x",))]
    assert [[s.name for s in g] for g in BP.lanes(steps)] == [["app gates (pnpm check:fast)", "app: second"],
                                                             ["CI: a pipeline step", "CI: fails", "CI: raises"]]
    live, seen = set(), []
    gate = threading.Barrier(2, timeout=5)

    def runner(s):
        live.add(s.name)
        if s.name in ("app gates (pnpm check:fast)", "CI: a pipeline step"):
            gate.wait()                          # both lanes are inside a step at the same moment, or this times out
        seen.append(set(live))
        print(f"out of {s.name}", file=BP._OUT.f)
        time.sleep(0.01)
        live.discard(s.name)
        if s.name == "CI: raises":
            raise RuntimeError("boom")
        return 1 if s.name == "CI: fails" else 0
    rcs = BP.run_all(steps, runner, parallel=True)
    assert rcs == {"app gates (pnpm check:fast)": 0, "app: second": 0, "CI: a pipeline step": 0, "CI: fails": 1,
                   "CI: raises": 1}
    out = capsys.readouterr().out
    block = out[out.index("── CI: a pipeline step"):]
    assert block.startswith("── CI: a pipeline step\n   a note\nout of CI: a pipeline step\n   ok (")
    assert "raised RuntimeError('boom')" in out and "2 lanes at once" in out
    # serial: one after another, nothing held
    capsys.readouterr()
    order = []
    assert BP.run_all(steps[:2], lambda s: order.append(s.name) or 0, parallel=False) == {s.name: 0 for s in steps[:2]}
    assert order == ["app gates (pnpm check:fast)", "app: second"]


def test_main_lists_the_failures_in_the_plans_order_whatever_lane_ended_first(monkeypatch, capsys):
    monkeypatch.delenv(BP.SKIP_ENV, raising=False)
    monkeypatch.setattr(BP, "changed_paths", lambda: ("b" * 40, ["pipeline/release.py", "app/x.ts"]))
    bad = {"the pipeline's source scanners (--no-tier)", "app gates (pnpm check:fast)"}
    assert BP.main(["--changed", "--no-tier"], runner=lambda s: 1 if s.name in bad else 0) == 1
    err = capsys.readouterr().err
    assert err.index("- the pipeline's source scanners") < err.index("- app gates (pnpm check:fast)")
